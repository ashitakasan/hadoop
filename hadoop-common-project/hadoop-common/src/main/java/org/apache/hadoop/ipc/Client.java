/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
@Public
@InterfaceStability.Evolving
public class Client implements AutoCloseable {                                      // IPC 服务的客户端
  
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  /** A counter for generating call IDs. */
  private static final AtomicInteger callIdCounter = new AtomicInteger();           // RPC 调用计数器，会储存在 callId ThreadLocal 中

  private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();    // 当前线程的 RPC 调用的 callId
  private static final ThreadLocal<Integer> retryCount = new ThreadLocal<Integer>();// RPC 请求重复次数计数器，用来限制每个 rpc 请求重复次数
  private static final ThreadLocal<Object> EXTERNAL_CALL_HANDLER
      = new ThreadLocal<>();
  private static final ThreadLocal<AsyncGet<? extends Writable, IOException>>
      ASYNC_RPC_RESPONSE = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> asynchronousMode =                      // 当前线程的 RPC 调用是异步的还是同步的，发 rpc 请求线程是调用线程
      new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return false;
        }
      };

  @SuppressWarnings("unchecked")
  @Unstable
  public static <T extends Writable> AsyncGet<T, IOException>
      getAsyncRpcResponse() {
    return (AsyncGet<T, IOException>) ASYNC_RPC_RESPONSE.get();
  }

  /** Set call id and retry count for the next call. */
  public static void setCallIdAndRetryCount(int cid, int rc,
                                            Object externalHandler) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);

    callId.set(cid);
    retryCount.set(rc);
    EXTERNAL_CALL_HANDLER.set(externalHandler);
  }

  private ConcurrentMap<ConnectionId, Connection> connections =
      new ConcurrentHashMap<>();                                                    // 保存了一个连接池，ConnectionId <-> Connection，以复用连接

  private Class<? extends Writable> valueClass;   // class of call values
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf;

  private SocketFactory socketFactory;           // how to create sockets
  private int refCount = 1;                                                         // 当前 client 的引用计数，用于缓存 client

  private final int connectionTimeout;                                              // socket 连接超时，默认 20s

  private final boolean fallbackAllowed;                                            // 如果认证失败，默认不允许降级到 SIMPLE
  private final byte[] clientId;
  private final int maxAsyncCalls;
  private final AtomicInteger asyncCallCounter = new AtomicInteger(0);              // 记录异步调用的计数器

  /**
   * Executor on which IPC calls' parameters are sent.
   * Deferring the sending of parameters to a separate
   * thread isolates them from thread interruptions in the
   * calling code.
   */
  private final ExecutorService sendParamsExecutor;                                 // 发送 rpc 请求数据的线程池，全 jvm 唯一
  private final static ClientExecutorServiceFactory clientExcecutorFactory =
      new ClientExecutorServiceFactory();                                           // 构造 发送数据的线程池 的工厂类，全 jvm 唯一

  private static class ClientExecutorServiceFactory {
    private int executorRefCount = 0;
    private ExecutorService clientExecutor = null;
    
    /**
     * Get Executor on which IPC calls' parameters are sent.
     * If the internal reference counter is zero, this method
     * creates the instance of Executor. If not, this method
     * just returns the reference of clientExecutor.
     * 
     * @return An ExecutorService instance
     */
    synchronized ExecutorService refAndGetInstance() {                              // 获取发送数据的线程池，每个客户端都会记录引用计数
      if (executorRefCount == 0) {
        clientExecutor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("IPC Parameter Sending Thread #%d")
            .build());
      }
      executorRefCount++;
      
      return clientExecutor;
    }
    
    /**
     * Cleanup Executor on which IPC calls' parameters are sent.
     * If reference counter is zero, this method discards the
     * instance of the Executor. If not, this method
     * just decrements the internal reference counter.
     * 
     * @return An ExecutorService instance if it exists.
     *   Null is returned if not.
     */
    synchronized ExecutorService unrefAndCleanup() {                                // 如果 clientExecutor 没有客户端使用了，则停止它
      executorRefCount--;
      assert(executorRefCount >= 0);
      
      if (executorRefCount == 0) {
        clientExecutor.shutdown();
        try {
          if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
            clientExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for clientExecutor" +
              " to stop");
          clientExecutor.shutdownNow();
          Thread.currentThread().interrupt();
        }
        clientExecutor = null;
      }
      
      return clientExecutor;
    }
  };
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  public static final void setPingInterval(Configuration conf,
      int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  public static final int getPingInterval(Configuration conf) {                     // ping 请求时间间隔
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   * @deprecated use {@link #getRpcTimeout(Configuration)} instead
   */
  @Deprecated
  final public static int getTimeout(Configuration conf) {                          // 获取配置的 rpc 超时时间
    int timeout = getRpcTimeout(conf);
    if (timeout > 0)  {
      return timeout;
    }
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
        CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT)) {                         // doPing ? min(pingInterval, rpcTimeout) : rpcTimeout
      return getPingInterval(conf);
    }
    return -1;
  }

  /**
   * The time after which a RPC will timeout.
   *
   * @param conf Configuration
   * @return the timeout period in milliseconds.
   */
  public static final int getRpcTimeout(Configuration conf) {                       // 配置的 rpc 请求的超时时间，默认没有超时时间
    int timeout =
        conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
    return (timeout < 0) ? 0 : timeout;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(Configuration conf, int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  @VisibleForTesting
  public static final ExecutorService getClientExecutor() {
    return Client.clientExcecutorFactory.clientExecutor;
  }
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  /** Check the rpc response header. */
  void checkResponse(RpcResponseHeaderProto header) throws IOException {            // 检查 rpc 请求头
    if (header == null) {
      throw new EOFException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final byte[] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {                                         // 如果响应中的 client 不是虚拟客户端，则必须是当前 clientId
          throw new IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in response="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }

  Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {                       // 在当前 rpc 调用线程内创建 RPC Call
    return new Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   */
  static class Call {                                                               // 表示一个 RPC 请求，包含以下字段：
    final int id;               // call id                                          // callId、retryCount、rpc请求数据、rpc响应、异常、rpc类型、完成标志
    final int retry;           // retry count
    final Writable rpcRequest;  // the serialized rpc request
    Writable rpcResponse;       // null if rpc has error
    IOException error;          // exception, null if success
    final RPC.RpcKind rpcKind;      // Rpc EngineKind
    boolean done;               // true when call is done
    private final Object externalHandler;

    private Call(RPC.RpcKind rpcKind, Writable param) {                             // 构造 Call，param 即 rpcRequest
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      final Integer id = callId.get();
      if (id == null) {
        this.id = nextCallId();
      } else {
        callId.set(null);                                                           // RPC 请求线程复用，如果该线程曾执行 rpc 调用，则清空其 callId
        this.id = id;
      }
      
      final Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }

      this.externalHandler = EXTERNAL_CALL_HANDLER.get();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + id;
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {                                    // 表明 rpc 调用已完成或出错，这里通知调用者
      this.done = true;
      notify();                                 // notify caller

      if (externalHandler != null) {
        synchronized (externalHandler) {
          externalHandler.notify();
        }
      }
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {                      // 调用出错时设置异常，并通知调用者
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    public synchronized void setRpcResponse(Writable rpcResponse) {                 // 设置 rpc 请求的响应，并通知调用者
      this.rpcResponse = rpcResponse;
      callComplete();
    }
    
    public synchronized Writable getRpcResponse() {
      return rpcResponse;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {                                         // 用于读取响应并通知调用者的线程，包含一个 socket，可认为是 rpc 连接
    private InetSocketAddress server;             // server ip:port                 // 主要包含以下字段：
    private final ConnectionId remoteId;                // connection id            //   服务端 ip:port、connectionId、authMethod、authProtocol、
    private AuthMethod authMethod; // authentication method                         //   sasl客户端、ipcStreams、socket、当前活跃的 rpcCall 等
    private AuthProtocol authProtocol;
    private int serviceClass;
    private SaslRpcClient saslRpcClient;
    
    private Socket socket = null;                 // connected socket
    private IpcStreams ipcStreams;
    private final int maxResponseLength;                                            // rpc 响应最大长度
    private final int rpcTimeout;                                                   // rpc 超时时间
    private int maxIdleTime; //connections will be culled if it was idle for        // 线程最长空闲等待时间
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;                                // 非超时导致的连接失败的重试策略
    private final int maxRetriesOnSasl;                                             // sasl 最大重试次数
    private int maxRetriesOnSocketTimeouts;                                         // socket 超时最大重试次数
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm        // 是否启动 TCP Nagle 算法
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message                 // 是否需要发送 ping 请求
    private final int pingInterval; // how often sends ping to the server           // ping 时间间隔
    private final int soTimeout; // used by ipc ping and rpc timeout                // rpc 超时时间
    private byte[] pingRequest; // ping message

    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();        // 当前连接线程中所有活跃的 rpc call
    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason
    
    private final Object sendRpcRequestLock = new Object();                         // rpc 请求锁，确保该连接线程内 其他线程发送 rpc 请求数据是同步(串行)的

    public Connection(ConnectionId remoteId, int serviceClass) throws IOException { // 主要从 ConnectionId 中取数据，serviceClass 仅仅类似于版本号
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw NetUtils.wrapException(server.getHostName(),
            server.getPort(),
            null,
            0,
            new UnknownHostException());
      }
      this.maxResponseLength = remoteId.conf.getInt(
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);             // 默认 rpc 响应最大长度为 128M
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;                  // 非连接超时导致的连接失败的重试策略
      this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();                       // client 认证(连接)失败后最大重试次数
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();   // rpc Timeout(socket Timeout) 后最大重试次数
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.tcpLowLatency = remoteId.getTcpLowLatency();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        // construct a RPC header with the callId as the ping callId
        ResponseBuffer buf = new ResponseBuffer();
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        pingHeader.writeDelimitedTo(buf);
        pingRequest = buf.toByteArray();                                            // 构造 ping 请求数据，
      }
      this.pingInterval = remoteId.getPingInterval();
      if (rpcTimeout > 0) {
        // effective rpc timeout is rounded up to multiple of pingInterval
        // if pingInterval < rpcTimeout.
        this.soTimeout = (doPing && pingInterval < rpcTimeout) ?
            pingInterval : rpcTimeout;                                              // doPing ? min(pingInterval, rpcTimeout) : rpcTimeout
      } else {
        this.soTimeout = pingInterval;                                              // 如果不 doPing，则 soTimeout = 0
      }
      this.serviceClass = serviceClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }

      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;          // 如果启用了安全性，或者 ugi 包含 token，则尝试使用 SASL
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);                                                         // 防止因为连接线程退出缓慢导致 jvm 延迟结束
    }

    /** Update lastActivity with the current time. */
    private void touch() {                                                          // 更新最近一次 rpc 连接活跃时间
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {                               // 在此连接的调用队列中添加一个 rpc call，并通知该线程的监听者
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {                       // 当读取超时时，这个类发送一个 ping 到远端，如果没有失败则一直重试
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * the RPC is not timed out yet, send a ping.
       */
      private void handleTimeout(SocketTimeoutException e, int waiting)
          throws IOException {                                                      // 处理 ping 请求的 socket timeout
        if (shouldCloseConnection.get() || !running.get() ||
            (0 < rpcTimeout && rpcTimeout <= waiting)) {                            // 连接关闭、client 停止、ping 时间大于 rpcTimeout 则抛出异常
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {                                        // 从流中读取一个字节，失败则重试并发送 ping 请求，直到 rpcTimeout
        int waiting = 0;
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {            // 从流的 off 处读取 len 字节数据到 buf 中，失败则发送 ping
        int waiting = 0;
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl() {                                       // 释放 saslClient 占用的资源，关闭 saslClient
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ignored) {
        }
      }
    }
    
    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {   // 判断请求是否需要通过 kerberos 认证
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }

    private synchronized AuthMethod setupSaslConnection(IpcStreams streams)
        throws IOException {                                                        // 建立 saslRpcClient 到服务端的连接认证
      // Do not use Client.conf here! We must use ConnectionId.conf, since the
      // Client object is cached and shared between all RPC clients, even those
      // for separate services.
      saslRpcClient = new SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), remoteId.conf);
      return saslRpcClient.saslConnect(streams);                                    // 通过 streams 建立 saslRpcClient 到服务端的连接认证
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized boolean updateAddress() throws IOException {               // 如果对应主机名的地址发生更改，则更新服务器地址，并重置重试次数
      // Do a fresh lookup with the old host name.
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + server.toString() +
                                 " New: " + currentAddr.toString());
        server = currentAddr;
        return true;
      }
      return false;
    }
    
    private synchronized void setupConnection(
        UserGroupInformation ticket) throws IOException {                           // 使用 ticket 建立连接
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();                               // 创建 socket 并配置 tcp 一些参数
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.setKeepAlive(true);
          
          if (tcpLowLatency) {
            /*
             * This allows intermediate switches to shape IPC traffic
             * differently from Shuffle/HDFS DataStreamer traffic.
             *
             * IPTOS_RELIABILITY (0x04) | IPTOS_LOWDELAY (0x10)
             *
             * Prefer to optimize connect() speed & response latency over net
             * throughput.
             */
            this.socket.setTrafficClass(0x04 | 0x10);
            this.socket.setPerformancePreferences(1, 2, 0);
          }

          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          InetSocketAddress bindAddr = null;
          if (ticket != null && ticket.hasKerberosCredentials()) {                  // 将 socket 绑定到客户端 principal 中指定的主机，确保地址匹配
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null) {
              String principal = ticket.getUserName();
              String host = SecurityUtil.getHostFromPrincipal(principal);
              // If host name is a valid local address then bind socket to it
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.setReuseAddress(true);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Binding " + principal + " to " + localAddr);
                }
                bindAddr = new InetSocketAddress(localAddr, 0);
              }
            }
          }
          
          NetUtils.connect(this.socket, server, bindAddr, connectionTimeout);       // 建立到 server 的 rpc 连接，设置 socket 超时时间
          this.socket.setSoTimeout(soTimeout);
          return;
        } catch (ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);                                     // 连接超时属于 socket 超时，这里处理 SocketTimeout
        } catch (IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);                                // 这里处理非连接超时导致的失败
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries, final int maxRetries, final Exception ex,
        final Random rand, final UserGroupInformation ugi) throws IOException,
        InterruptedException {                                                      // 处理 sasl 连接认证失败
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          final short MAX_BACKOFF = 5000;
          closeConnection();                                                        // 关闭 socket 连接，关闭 saslClient，释放 saslClient 占用的资源
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to "
                    + "the server : " + ex);
              }
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();            // 重新向服务器发起认证
              } else if (UserGroupInformation.isLoginTicketBased()) {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));                        // 这里是建立到服务端的连接，因此可以在这里 sleep
              return null;
            } else {                                                                // 如果超过最大重试次数，则抛出异常
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg, ex);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to "
                + "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException) ex;
          throw new IOException(ex);
        }
      });
    }

    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    private synchronized void setupIOstreams(
        AtomicBoolean fallbackToSimpleAuth) {                                       // 建立客户端到服务端的连接并建立 i/o 流，发送请求头并启动连接线程
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }
      UserGroupInformation ticket = remoteId.getTicket();
      if (ticket != null) {
        final UserGroupInformation realUser = ticket.getRealUser();                 // rpc 请求需要拿到真实的用户 realUser
        if (realUser != null) {
          ticket = realUser;
        }
      }
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        Span span = Tracer.getCurrentSpan();
        if (span != null) {
          span.addTimelineAnnotation("IPC client connecting to " + server);
        }
        short numRetries = 0;
        Random rand = null;
        while (true) {
          setupConnection(ticket);                                                  // 使用 ticket 建立客户端到 server 的连接
          ipcStreams = new IpcStreams(socket, maxResponseLength);
          writeConnectionHeader(ipcStreams);
          if (authProtocol == AuthProtocol.SASL) {
            try {
              authMethod = ticket
                  .doAs(new PrivilegedExceptionAction<AuthMethod>() {               // 建立 saslRpcClient 到服务端的连接认证
                    @Override
                    public AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(ipcStreams);
                    }
                  });
            } catch (IOException ex) {
              if (saslRpcClient == null) {
                // whatever happened -it can't be handled, so rethrow
                throw ex;
              }
              // otherwise, assume a connection problem
              authMethod = saslRpcClient.getAuthMethod();                           // saslRpcClient 认证失败时可能会降级，这里需要重新获取 authMethod
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
                  rand, ticket);                                                    // 这里处理 sasl 客户端连接认证失败
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              ipcStreams.setSaslClient(saslRpcClient);                              // 表明连接认证成功，建立 IPC 的 I/O 流，此时 in/out 为空
              // for testing
              remoteId.saslQop =
                  (String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
              LOG.debug("Negotiated QOP is :" + remoteId.saslQop);
              if (fallbackToSimpleAuth != null) {
                fallbackToSimpleAuth.set(false);
              }
            } else if (UserGroupInformation.isSecurityEnabled()) {                  // 如果因为认证失败导致 认证降级，根据 rpc 是否允许来决定是否抛出异常
              if (!fallbackAllowed) {
                throw new IOException("Server asks us to fall back to SIMPLE " +
                    "auth, but this client is configured to only allow secure " +
                    "connections.");
              }
              if (fallbackToSimpleAuth != null) {                                   // 通知调用者连接认证被降级了
                fallbackToSimpleAuth.set(true);
              }
            }
          }

          if (doPing) {
            ipcStreams.setInputStream(new PingInputStream(ipcStreams.in));          // 设置输入流为 PingInputStream
          }

          writeConnectionContext(remoteId, authMethod);                             // 写入 rpc ConnectionContext，包含当前连接的具体信息

          // update last activity time
          touch();

          span = Tracer.getCurrentSpan();
          if (span != null) {
            span.addTimelineAnnotation("IPC client connected to " + server);
          }

          // start the receiver thread after the socket connection has been set
          // up
          start();                                                                  // 建立 socket 连接后，启动当前线程，开始接收 rpc 数据
          return;
        }
      } catch (Throwable t) {                                                       // 连接过程中发送任何异常，都要结束当前连接线程，最后关闭当前连接
        if (t instanceof IOException) {
          markClosed((IOException)t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams: " + t, t));
        }
        close();
      }
    }
    
    private void closeConnection() {                                                // 关闭当前 connection 的 socket 连接，可重新连接
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionTimeout(
        int curRetries, int maxRetries, IOException ioe) throws IOException {       // 处理连接超时，先关闭 socket 连接，再重新连接

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {                                               // 如果重试次数超过最大次数，则停止重试并抛出异常
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(int curRetries, IOException ioe
        ) throws IOException {                                                      // 处理非连接超时导致的连接失败，先关闭连接，再重试
      closeConnection();

      final RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);       // 根据 retryPolicy 决定是否继续重试连接，需要关闭 socket 连接
      } catch(Exception e) {
        throw e instanceof IOException? (IOException)e: new IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to connect to server: " + server + ": "
                    + action.reason, ioe);
          }
        }
        throw ioe;
      }

      // Throw the exception if the thread is interrupted
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Interrupted while trying for connection");
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);                                           // 根据 retryPolicy，sleep 指定时间后再进行重试
      } catch (InterruptedException e) {
        throw (IOException)new InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(IpcStreams streams)
        throws IOException {                                                        // 写入 rpc 请求头，只在连接建立时才写入 rpc 流
      // Write out the header, version and authentication method.
      // The output stream is buffered but we must not flush it yet.  The
      // connection setup protocol requires the client to send multiple
      // messages before reading a response.
      //
      //   insecure: send header+context+call, read
      //   secure  : send header+negotiate, read, (sasl), context+call, read
      //
      // The client must flush only when it's prepared to read.  Otherwise
      // "broken pipe" exceptions occur if the server closes the connection
      // before all messages are sent.
      final DataOutputStream out = streams.out;
      synchronized (out) {
        out.write(RpcConstants.HEADER.array());
        out.write(RpcConstants.CURRENT_VERSION);                                    // 这里只写入 int 的低 8 位
        out.write(serviceClass);
        out.write(authProtocol.callId);
      }
    }

    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionContext(ConnectionId remoteId,
                                        AuthMethod authMethod)
                                            throws IOException {                    // 将 connection context header 写入 rpc 流，每个连接线程只写一次
      // Write out the ConnectionHeader
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);                          // 构造 RpcRequestHeaderProto，并写入 ipcStreams 输出流中
      // do not flush.  the context and first ipc call request must be sent
      // together to avoid possibility of broken pipes upon authz failure.
      // see writeConnectionHeader
      final ResponseBuffer buf = new ResponseBuffer();
      connectionContextHeader.writeDelimitedTo(buf);
      message.writeDelimitedTo(buf);
      synchronized (ipcStreams.out) {
        ipcStreams.sendRequest(buf.toByteArray());
      }
    }

    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized boolean waitForWork() {                                    // 连接线程一直在这里 wait 等待，直到有工作可做或者空闲超时
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);                                                          // 如果没有工作可做，则一直再这里 wait，直到其他线程唤醒它
          } catch (InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {                                     // 非正常情况被唤醒，需要关闭连接线程
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing() throws IOException {                       // 向服务端发送 ping 请求，更新 rpc 连接活跃时间
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (ipcStreams.out) {
          ipcStreams.sendRequest(pingRequest);
          ipcStreams.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveRpcResponse();                                                     // 连接线程的主要工作，读取 rpc 响应，一旦出错则关闭连接
        }
      } catch (Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call - the rpc request
     */
    public void sendRpcRequest(final Call call)
        throws InterruptedException, IOException {                                  // 向服务器发送 rpc 请求发起 rpc 调用，这是其他线程做的，不是连接线程
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the sendParamsExecutor thread,
      
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here. 
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId);

      final ResponseBuffer buf = new ResponseBuffer();
      header.writeDelimitedTo(buf);
      RpcWritable.wrap(call.rpcRequest).writeTo(buf);                               // 构造 rpc 请求的 proto，包含请求头（call 信息）、请求数据等

      synchronized (sendRpcRequestLock) {
        Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {         // 序列化 call 是调用者线程，发送数据是 sendParamsExecutor 线程池
          @Override
          public void run() {
            try {
              synchronized (ipcStreams.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug(getName() + " sending #" + call.id
                      + " " + call.rpcRequest);
                }
                // RpcRequestHeader + RpcRequest
                ipcStreams.sendRequest(buf.toByteArray());                          // 真正的发送 rpc 请求数据的方法，直到发送完数据才释放 out 同步锁
                ipcStreams.flush();
              }
            } catch (IOException e) {
              // exception at this point would leave the connection in an
              // unrecoverable state (eg half a call left on the wire).
              // So, close the connection, killing any outstanding calls
              markClosed(e);                                                        // 异常可能导致连接无法恢复，因此这里关闭当前连接
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              IOUtils.closeStream(buf);
            }
          }
        });
      
        try {
          senderFuture.get();                                                       // 调用者线程在这里等待 rpc 请求发送完成，异常将抛出到调用者线程
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          
          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else {
            throw new RuntimeException("unexpected checked exception", cause);
          }
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    private void receiveRpcResponse() {                                             // 从 ipcStreams 流中读取 rpc 响应
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        ByteBuffer bb = ipcStreams.readResponse();
        RpcWritable.Buffer packet = RpcWritable.Buffer.wrap(bb);
        RpcResponseHeaderProto header =
            packet.getValue(RpcResponseHeaderProto.getDefaultInstance());           // 通过流中读取的字节构造 proto
        checkResponse(header);

        int callId = header.getCallId();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        RpcStatusProto status = header.getStatus();
        if (status == RpcStatusProto.SUCCESS) {                                     // 请求成功，则取出响应数据，从 calls 中移除 call
          Writable value = packet.newInstance(valueClass, conf);
          final Call call = calls.remove(callId);
          call.setRpcResponse(value);
        }
        // verify that packet length was correct
        if (packet.remaining() > 0) {
          throw new RpcClientException("RPC response length mismatch");
        }
        if (status != RpcStatusProto.SUCCESS) { // Rpc Request failed               // 如果请求失败，则构造 RemoteException，关闭当前连接线程
          final String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";
          final String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          RemoteException re = new RemoteException(exceptionClassName, errorMsg, erCode);
          if (status == RpcStatusProto.ERROR) {                                     // 如果 rpc ERROR，只是失败 call，如果是 FATAL，则关闭当前连接
            final Call call = calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {                           // 设置连接关闭状态，通知其他线程逐步关闭该连接上的资源
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {                                             // 关闭当前连接，移除连接池，并释放占用的资源
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // We have marked this connection as closed. Other thread could have
      // already known it and replace this closedConnection with a new one.
      // We should only remove this closedConnection.
      connections.remove(remoteId, this);                                           // 移除当前 ConnectionId，以便其他线程可以新建连接线程

      // close the streams and therefore the socket
      IOUtils.closeStream(ipcStreams);                                              // 关闭 ipcStreams 流，释放 saslClient 占用的资源
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();                                                             // 清理当前连接内的 rpc call，最后关闭 socket 连接
      }
      closeConnection();
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {                                                   // 清理当前连接线程下的 calls，并设置每个 call 异常结束
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {                                                      // 构造一个 RPC 客户端，初始化一些配置参数
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.clientId = ClientId.getClientId();
    this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
    this.maxAsyncCalls = conf.getInt(
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_DEFAULT);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {                                                              // 关闭当前客户端，这里是异步关闭的
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    for (Connection conn : connections.values()) {
      conn.interrupt();
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {                                                // 等待所有的连接线程逐步关闭
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
    
    clientExcecutorFactory.unrefAndCleanup();
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
      fallbackToSimpleAuth);
  }

  private void checkAsyncCall() throws IOException {
    if (isAsynchronousMode()) {
      if (asyncCallCounter.incrementAndGet() > maxAsyncCalls) {                     // 限制异步调用的请求数量，默认同时最大 100 个异步调用
        asyncCallCounter.decrementAndGet();
        String errMsg = String.format(
            "Exceeded limit of max asynchronous calls: %d, " +
            "please configure %s to adjust it.",
            maxAsyncCalls,
            CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY);
        throw new AsyncCallLimitExceededException(errMsg);
      }
    }
  }

  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc response.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, int serviceClass,
      AtomicBoolean fallbackToSimpleAuth) throws IOException {                      // 客户端核心方法，发送 rpcRequest 到 remoteId，返回 rpcResponse
    final Call call = createCall(rpcKind, rpcRequest);
    final Connection connection = getConnection(remoteId, call, serviceClass,
        fallbackToSimpleAuth);

    try {
      checkAsyncCall();
      try {
        connection.sendRpcRequest(call);                 // send the rpc request    // 通过 connection 发送 rpc 请求 call，阻塞直到请求发送完成
      } catch (RejectedExecutionException e) {
        throw new IOException("connection has been closed", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("interrupted waiting to send rpc request to server", e);
        throw new IOException(e);
      }
    } catch(Exception e) {
      if (isAsynchronousMode()) {                                                   // 如果是异步获取响应，发生异常时需要释放异步计数器
        releaseAsyncCall();
      }
      throw e;
    }

    if (isAsynchronousMode()) {
      final AsyncGet<Writable, IOException> asyncGet
          = new AsyncGet<Writable, IOException>() {                                 // 异步调用模式下，调用者线程会调用 asyncGet.get，获取 rpc 响应
        @Override
        public Writable get(long timeout, TimeUnit unit)
            throws IOException, TimeoutException{                                   // 如果 timeout < 0，则没有超时时间，永久循环
          boolean done = true;
          try {
            final Writable w = getRpcResponse(call, connection, timeout, unit);
            if (w == null) {
              done = false;
              throw new TimeoutException(call + " timed out "
                  + timeout + " " + unit);
            }
            return w;
          } finally {
            if (done) {
              releaseAsyncCall();                                                   // 必须释放异步获取响应的计数器
            }
          }
        }

        @Override
        public boolean isDone() {
          synchronized (call) {
            return call.done;
          }
        }
      };

      ASYNC_RPC_RESPONSE.set(asyncGet);                                             // 在当前线程（调用线程）中异步获取 rpc 响应
      return null;
    } else {
      return getRpcResponse(call, connection, -1, null);                            // 同步的 RPC 请求，没有超时时间
    }
  }

  /**
   * Check if RPC is in asynchronous mode or not.
   *
   * @returns true, if RPC is in asynchronous mode, otherwise false for
   *          synchronous mode.
   */
  @Unstable
  public static boolean isAsynchronousMode() {                                      // 当前线程的 RPC 调用是否是异步的
    return asynchronousMode.get();
  }

  /**
   * Set RPC to asynchronous or synchronous mode.
   *
   * @param async
   *          true, RPC will be in asynchronous mode, otherwise false for
   *          synchronous mode
   */
  @Unstable
  public static void setAsynchronousMode(boolean async) {
    asynchronousMode.set(async);
  }

  private void releaseAsyncCall() {
    asyncCallCounter.decrementAndGet();
  }

  @VisibleForTesting
  int getAsyncCallCount() {
    return asyncCallCounter.get();
  }

  /** @return the rpc response or, in case of timeout, null. */
  private Writable getRpcResponse(final Call call, final Connection connection,
      final long timeout, final TimeUnit unit) throws IOException {                 // 从 call 中获取 rpc 响应，最多等待 timeout
    synchronized (call) {
      while (!call.done) {
        try {
          AsyncGet.Util.wait(call, timeout, unit);                                  // 在 call 上 wait timeout 时间，等待被 connection 线程唤醒
          if (timeout >= 0 && !call.done) {                                         // wait 会释放 call 锁，被唤醒时则重新获得 call 锁
            return null;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Call interrupted");
        }
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }
      } else {
        return call.getRpcResponse();
      }
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  Set<ConnectionId> getConnectionIds() {
    return connections.keySet();
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  private Connection getConnection(ConnectionId remoteId,
      Call call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {                                                          // 从 connections 池中获取一个连接线程
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    while (true) {
      // These lines below can be shorten with computeIfAbsent in Java8
      connection = connections.get(remoteId);                                       // 每个 remoteId 只会对于一个连接线程 connection
      if (connection == null) {
        connection = new Connection(remoteId, serviceClass);
        Connection existing = connections.putIfAbsent(remoteId, connection);        // 别的调用线程可能已经存入了一个 connection
        if (existing != null) {
          connection = existing;
        }
      }

      if (connection.addCall(call)) {
        break;
      } else {
        // This connection is closed, should be removed. But other thread could
        // have already known this closedConnection, and replace it with a new
        // connection. So we should call conditional remove to make sure we only
        // remove this closedConnection.
        connections.remove(remoteId, connection);                                   // 表明 connection 这个连接已经关闭了，需要手动删除它
      }
    }

    // If the server happens to be slow, the method below will take longer to
    // establish a connection.
    connection.setupIOstreams(fallbackToSimpleAuth);                                // 建立客户端到服务端的连接并建立 i/o 流，发送请求头并启动连接线程
    return connection;
  }
  
  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {                                                // 包含服务端address、用户ugi，用于表示一个 RPC connection
    InetSocketAddress address;                                                      // <address, protocol, ticket> 唯一表示一个 RPC connection
    UserGroupInformation ticket;
    final Class<?> protocol;
    private static final int PRIME = 16777619;                                      // 32位字符串 hash 使用的质数
    private final int rpcTimeout;
    private final int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    // the max. no. of retries for socket connections on time out exceptions
    private final int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server in msecs
    private String saslQop; // here for testing
    private final Configuration conf; // used to get the expected kerberos principal name
    
    ConnectionId(InetSocketAddress address, Class<?> protocol, 
                 UserGroupInformation ticket, int rpcTimeout,
                 RetryPolicy connectionRetryPolicy, Configuration conf) {
      this.protocol = protocol;                                                     // RPC 协议的实现类
      this.address = address;                                                       // 服务端 ip:port
      this.ticket = ticket;                                                         // ugi，即通过认证的用户
      this.rpcTimeout = rpcTimeout;                                                 // rpc 请求超时时间
      this.connectionRetryPolicy = connectionRetryPolicy;                           // 非连接超时导致的连接失败的重试策略

      this.maxIdleTime = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);   // 默认线程最大空闲等待时间 10s
      this.maxRetriesOnSasl = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);    // 默认 sasl 最大重试次数 5 次
      this.maxRetriesOnSocketTimeouts = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT); // 默认 socket 超时最大重试次数 45 次
      this.tcpNoDelay = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);               // 默认不启动 Nagle 算法
      this.tcpLowLatency = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY,
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY_DEFAULT
          );
      this.doPing = conf.getBoolean(
          CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
          CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
      this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);              // 默认 每分钟执行 ping 请求一次，如果不发送 ping，则该值为 0
      this.conf = conf;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }
    
    Class<?> getProtocol() {
      return protocol;
    }
    
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    private int getRpcTimeout() {
      return rpcTimeout;
    }
    
    int getMaxIdleTime() {
      return maxIdleTime;
    }
    
    public int getMaxRetriesOnSasl() {
      return maxRetriesOnSasl;
    }

    /** max connection retries on socket time outs */
    public int getMaxRetriesOnSocketTimeouts() {
      return maxRetriesOnSocketTimeouts;
    }

    /** disable nagle's algorithm */
    boolean getTcpNoDelay() {
      return tcpNoDelay;
    }

    /** use low-latency QoS bits over TCP */
    boolean getTcpLowLatency() {
      return tcpLowLatency;
    }

    boolean getDoPing() {
      return doPing;
    }
    
    int getPingInterval() {
      return pingInterval;
    }
    
    @VisibleForTesting
    String getSaslQop() {
      return saslQop;
    }
    
    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException { // 创建 ConnectionId 的唯一方法

      if (connectionRetryPolicy == null) {
        final int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        final int retryInterval = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
            CommonConfigurationKeysPublic
                .IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);

        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, retryInterval, TimeUnit.MILLISECONDS);                             // 默认的 rpc 连接失败重试策略：每秒钟重试一次，最多 10 次
      }

      return new ConnectionId(addr, protocol, ticket, rpcTimeout,
          connectionRetryPolicy, conf);
    }
    
    static boolean isEqual(Object a, Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof ConnectionId) {
        ConnectionId that = (ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public String toString() {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static int nextCallId() {                                                  // 获取一个 rpc 请求的唯一 id，目前只能是正数
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }

  @Override
  @Unstable
  public void close() throws Exception {
    stop();
  }

  /** Manages the input and output streams for an IPC connection.
   *  Only exposed for use by SaslRpcClient.
   */
  @InterfaceAudience.Private
  public static class IpcStreams implements Closeable, Flushable {                  // 管理 IPC 连接的输入和输出流，只供 Sasl Rpc 客户端使用
    private DataInputStream in;
    public DataOutputStream out;
    private int maxResponseLength;
    private boolean firstResponse = true;

    IpcStreams(Socket socket, int maxResponseLength) throws IOException {           // 从 socket 构造 rpc 的输入和输出流
      this.maxResponseLength = maxResponseLength;
      setInputStream(
          new BufferedInputStream(NetUtils.getInputStream(socket)));
      setOutputStream(
          new BufferedOutputStream(NetUtils.getOutputStream(socket)));
    }

    void setSaslClient(SaslRpcClient client) throws IOException {                   // 直接从客户端构造输入输出流，复用 socket 流
      // Wrap the input stream in a BufferedInputStream to fill the buffer
      // before reading its length (HADOOP-14062).
      setInputStream(new BufferedInputStream(client.getInputStream(in)));
      setOutputStream(client.getOutputStream(out));
    }

    private void setInputStream(InputStream is) {                                   // 设置 ipc 输入输出流，这里只是对流做一个包装
      this.in = (is instanceof DataInputStream)
          ? (DataInputStream)is : new DataInputStream(is);
    }

    private void setOutputStream(OutputStream os) {
      this.out = (os instanceof DataOutputStream)
          ? (DataOutputStream)os : new DataOutputStream(os);
    }

    public ByteBuffer readResponse() throws IOException {                           // ipc 请求是连续的，通过一次方法调用可以读取到完整的 proto
      int length = in.readInt();
      if (firstResponse) {
        firstResponse = false;
        // pre-rpcv9 exception, almost certainly a version mismatch.
        if (length == -1) {
          in.readInt(); // ignore fatal/error status, it's fatal for us.            // 首次请求可能会输出第一个字节是 -1
          throw new RemoteException(WritableUtils.readString(in),
                                    WritableUtils.readString(in));
        }
      }
      if (length <= 0) {
        throw new RpcException("RPC response has invalid length");
      }
      if (maxResponseLength > 0 && length > maxResponseLength) {
        throw new RpcException("RPC response exceeds maximum data length");
      }
      ByteBuffer bb = ByteBuffer.allocate(length);
      in.readFully(bb.array());
      return bb;
    }

    public void sendRequest(byte[] buf) throws IOException {
      out.write(buf);
    }

    @Override
    public void flush() throws IOException {                                        // flush 之后数据才会发送出去
      out.flush();
    }

    @Override
    public void close() {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
    }
  }
}
