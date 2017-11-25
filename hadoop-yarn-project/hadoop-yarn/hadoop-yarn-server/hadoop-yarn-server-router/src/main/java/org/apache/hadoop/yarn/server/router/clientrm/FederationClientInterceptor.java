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

package org.apache.hadoop.yarn.server.router.clientrm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceProfilesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetResourceProfileResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.federation.failover.FederationProxyProviderUtil;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.RouterPolicyFacade;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;
import org.apache.hadoop.yarn.server.router.RouterMetrics;
import org.apache.hadoop.yarn.server.router.RouterServerUtil;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Extends the {@code AbstractRequestInterceptorClient} class and provides an
 * implementation for federation of YARN RM and scaling an application across
 * multiple YARN SubClusters. All the federation specific implementation is
 * encapsulated in this class. This is always the last intercepter in the chain.
 */
public class FederationClientInterceptor
    extends AbstractClientRequestInterceptor {

  /*
   * TODO YARN-6740 Federation Router (hiding multiple RMs for
   * ApplicationClientProtocol) phase 2.
   *
   * The current implementation finalized the main 4 calls (getNewApplication,
   * submitApplication, forceKillApplication and getApplicationReport). Those
   * allow us to execute applications E2E.
   */

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationClientInterceptor.class);

  private int numSubmitRetries;                                                     // 提交作业重试次数，可向多个 cluster 提交作业
  private Map<SubClusterId, ApplicationClientProtocol> clientRMProxies;             // 每个 cluster 都有一个 clientRMProxy
  private FederationStateStoreFacade federationFacade;                              // FD store 实现
  private Random rand;
  private RouterPolicyFacade policyFacade;                                          // 路由策略
  private RouterMetrics routerMetrics;
  private final Clock clock = new MonotonicClock();

  @Override
  public void init(String userName) {
    super.init(userName);

    federationFacade = FederationStateStoreFacade.getInstance();
    rand = new Random(System.currentTimeMillis());

    final Configuration conf = this.getConf();

    try {
      policyFacade = new RouterPolicyFacade(conf, federationFacade,
          this.federationFacade.getSubClusterResolver(), null);
    } catch (FederationPolicyInitializationException e) {
      LOG.error(e.getMessage());
    }

    numSubmitRetries =
        conf.getInt(YarnConfiguration.ROUTER_CLIENTRM_SUBMIT_RETRY,
            YarnConfiguration.DEFAULT_ROUTER_CLIENTRM_SUBMIT_RETRY);

    clientRMProxies =
        new ConcurrentHashMap<SubClusterId, ApplicationClientProtocol>();
    routerMetrics = RouterMetrics.getMetrics();
  }

  @Override
  public void setNextInterceptor(ClientRequestInterceptor next) {                   // 不应该在 FD 拦截器上调用这个方法
    throw new YarnRuntimeException("setNextInterceptor is being called on "
        + "FederationClientRequestInterceptor, which should be the last one "
        + "in the chain. Check if the interceptor pipeline configuration "
        + "is correct");
  }

  @VisibleForTesting
  protected ApplicationClientProtocol getClientRMProxyForSubCluster(
      SubClusterId subClusterId) throws YarnException {

    if (clientRMProxies.containsKey(subClusterId)) {                                // 选取 clusterId 对应的 RMProxy，proxy 内部会向对应的 RM 发请求
      return clientRMProxies.get(subClusterId);
    }

    ApplicationClientProtocol clientRMProxy = null;
    try {
      clientRMProxy = FederationProxyProviderUtil.createRMProxy(getConf(),
          ApplicationClientProtocol.class, subClusterId, user);                     // 为 user 创建 subCluster 的 RM 代理，并储存
    } catch (Exception e) {
      RouterServerUtil.logAndThrowException(
          "Unable to create the interface to reach the SubCluster "
              + subClusterId,
          e);
    }

    clientRMProxies.put(subClusterId, clientRMProxy);
    return clientRMProxy;
  }

  private SubClusterId getRandomActiveSubCluster(
      Map<SubClusterId, SubClusterInfo> activeSubclusters)
      throws YarnException {

    if (activeSubclusters == null || activeSubclusters.size() < 1) {
      RouterServerUtil.logAndThrowException(
          FederationPolicyUtils.NO_ACTIVE_SUBCLUSTER_AVAILABLE, null);
    }
    List<SubClusterId> list = new ArrayList<>(activeSubclusters.keySet());

    return list.get(rand.nextInt(list.size()));                                     // 从所有 active 的 cluster 中随机选取一个
  }

  /**
   * YARN Router forwards every getNewApplication requests to any RM. During
   * this operation there will be no communication with the State Store. The
   * Router will forward the requests to any SubCluster. The Router will retry
   * to submit the request on #numSubmitRetries different SubClusters. The
   * SubClusters are randomly chosen from the active ones.
   *
   * Possible failures and behaviors:                                               // 故障及行为：client 故障无影响，Router 故障 client 会超时并重试
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit.
   *
   * ResourceManager: the Router will timeout and contacts another RM.              // RM 故障则 Router 会超时并连接其他的 RM
   *
   * StateStore: not in the execution.
   */
  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnException, IOException {         // new app 请求会发送到任意一个 sub cluster 上

    long startTime = clock.getTime();

    Map<SubClusterId, SubClusterInfo> subClustersActive =
        federationFacade.getSubClusters(true);

    for (int i = 0; i < numSubmitRetries; ++i) {                                    // 重试次数中的每次选取 RM 都是随机的
      SubClusterId subClusterId = getRandomActiveSubCluster(subClustersActive);
      LOG.debug(
          "getNewApplication try #" + i + " on SubCluster " + subClusterId);
      ApplicationClientProtocol clientRMProxy =
          getClientRMProxyForSubCluster(subClusterId);
      GetNewApplicationResponse response = null;
      try {
        response = clientRMProxy.getNewApplication(request);
      } catch (Exception e) {
        LOG.warn("Unable to create a new ApplicationId in SubCluster "
            + subClusterId.getId(), e);
      }

      if (response != null) {

        long stopTime = clock.getTime();
        routerMetrics.succeededAppsCreated(stopTime - startTime);
        return response;
      } else {
        // Empty response from the ResourceManager.
        // Blacklist this subcluster for this request.
        subClustersActive.remove(subClusterId);                                     // 如果 new app 失败，则将 sub cluster 加入黑名单
      }

    }

    routerMetrics.incrAppsFailedCreated();
    String errMsg = "Fail to create a new application.";
    LOG.error(errMsg);
    throw new YarnException(errMsg);
  }

  /**
   * Today, in YARN there are no checks of any applicationId submitted.
   *
   * Base scenarios:                                                                // 基本情形：
   *
   * The Client submits an application to the Router. • The Router selects one      //    1. client 提交一个 app 到 Router
   * SubCluster to forward the request. • The Router inserts a tuple into           //    2. Router 选择一个 cluster 转发请求
   * StateStore with the selected SubCluster (e.g. SC1) and the appId. • The        //    3. Router 将 appid <-> cluster 映射信息保存到 store 中
   * State Store replies with the selected SubCluster (e.g. SC1). • The Router      //    4. store 响应 cluster
   * submits the request to the selected SubCluster.                                //    5. Router 提交请求到选择的 cluster
   *
   * In case of State Store failure:                                                // 如果 store 故障：
   *
   * The client submits an application to the Router. • The Router selects one      //    1. client 提交一个 app 到 Router
   * SubCluster to forward the request. • The Router inserts a tuple into State     //    2. Router 选择一个 cluster 转发请求
   * Store with the selected SubCluster (e.g. SC1) and the appId. • Due to the      //    3. Router 将 appid <-> cluster 映射信息保存到 store 中
   * State Store down the Router times out and it will retry depending on the       //    4. 由于 store 故障，Router 会重试
   * FederationFacade settings. • The Router replies to the client with an error    //    5. Router 响应 client 错误信息
   * message.
   *
   * If State Store fails after inserting the tuple: identical behavior as
   * {@code ClientRMService}.
   *
   * In case of Router failure:
   *
   * Scenario 1 – Crash before submission to the ResourceManager                    // 提交 app 到 RM 之前 Router 故障：
   *
   * The Client submits an application to the Router. • The Router selects one      //    1. client 提交一个 app 到 Router
   * SubCluster to forward the request. • The Router inserts a tuple into State     //    2. Router 选择一个 cluster 转发请求
   * Store with the selected SubCluster (e.g. SC1) and the appId. • The Router      //    3. Router 将 appid <-> cluster 映射信息保存到 store 中
   * crashes. • The Client timeouts and resubmits the application. • The Router     //    4. Router 故障，client 超时并重新提交作业
   * selects one SubCluster to forward the request. • The Router inserts a tuple    //    5. Router(new) 选择一个 cluster 转发请求
   * into State Store with the selected SubCluster (e.g. SC2) and the appId. •      //    6. Router 将新的 appid <-> cluster 映射信息保存到 store 中
   * Because the tuple is already inserted in the State Store, it returns the       //    7. 由于 store 已经有该 appid 信息，store 返回之前选择的 cluster
   * previous selected SubCluster (e.g. SC1). • The Router submits the request      //    8. Router 提交请求到返回的 cluster 上
   * to the selected SubCluster (e.g. SC1).
   *
   * Scenario 2 – Crash after submission to the ResourceManager                     // 提交 app 到 RM 之后 Router 故障
   *
   * • The Client submits an application to the Router. • The Router selects one    //    1. client 提交一个 app 到 Router
   * SubCluster to forward the request. • The Router inserts a tuple into State     //    2. Router 选择一个 cluster 转发请求
   * Store with the selected SubCluster (e.g. SC1) and the appId. • The Router      //    3. Router 将 appid <-> cluster 映射信息保存到 store 中
   * submits the request to the selected SubCluster. • The Router crashes. • The    //    4. Router 提交请求到选择的 cluster 上
   * Client timeouts and resubmit the application. • The Router selects one         //    5. Router 故障，client 超时并重新提交作业
   * SubCluster to forward the request. • The Router inserts a tuple into State     //    6. Router 选择一个 cluster 转发请求
   * Store with the selected SubCluster (e.g. SC2) and the appId. • The State       //    7. Router 将新的 appid <-> cluster 映射信息保存到 store 中
   * Store replies with the selected SubCluster (e.g. SC1). • The Router submits    //    8. 由于 store 已经有该 appid 信息，store 返回之前选择的 cluster
   * the request to the selected SubCluster (e.g. SC1). When a client re-submits    //    9. 如果 client 重新提交请求到相同的 RM，RM 不会异常并返回成功消息
   * the same application to the same RM, it does not raise an exception and
   * replies with operation successful message.
   *
   * In case of Client failure: identical behavior as {@code ClientRMService}.
   *
   * In case of ResourceManager failure:                                            // 如果 RM 故障：
   *
   * The Client submits an application to the Router. • The Router selects one      //    1. client 提交一个 app 到 Router
   * SubCluster to forward the request. • The Router inserts a tuple into State     //    2. Router 选择一个 cluster 转发请求（根据策略）
   * Store with the selected SubCluster (e.g. SC1) and the appId. • The Router      //    3. Router 将 appid <-> cluster 映射信息保存到 store 中
   * submits the request to the selected SubCluster. • The entire SubCluster is     //    4. Router 提交请求到选择的 cluster 上
   * down – all the RMs in HA or the master RM is not reachable. • The Router       //    5. cluster RM 故障，并且 RM 的 HA 也故障
   * times out. • The Router selects a new SubCluster to forward the request. •     //    6. Router 超时，Router 选择新的 cluster 转发请求
   * The Router update a tuple into State Store with the selected SubCluster        //    7. Router 将新的 appid <-> cluster 映射信息保存到 store 中
   * (e.g. SC2) and the appId. • The State Store replies with OK answer. • The      //    8. store 返回 OK，Router 提交请求到选择的 cluster 中
   * Router submits the request to the selected SubCluster (e.g. SC2).
   */
  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnException, IOException {         // client 通过 Router 提交 app 到 RM

    long startTime = clock.getTime();

    if (request == null || request.getApplicationSubmissionContext() == null
        || request.getApplicationSubmissionContext()
            .getApplicationId() == null) {
      routerMetrics.incrAppsFailedSubmitted();
      RouterServerUtil
          .logAndThrowException("Missing submitApplication request or "
              + "applicationSubmissionContex information.", null);
    }

    ApplicationId applicationId =
        request.getApplicationSubmissionContext().getApplicationId();

    List<SubClusterId> blacklist = new ArrayList<SubClusterId>();

    for (int i = 0; i < numSubmitRetries; ++i) {

      SubClusterId subClusterId = policyFacade.getHomeSubcluster(
          request.getApplicationSubmissionContext(), blacklist);                    // 从 策略包装中获取提交请求对应的 cluster
      LOG.info("submitApplication appId" + applicationId + " try #" + i
          + " on SubCluster " + subClusterId);

      ApplicationHomeSubCluster appHomeSubCluster =
          ApplicationHomeSubCluster.newInstance(applicationId, subClusterId);       // pb 消息内部包含 appid、clusterid

      if (i == 0) {
        try {
          // persist the mapping of applicationId and the subClusterId which has
          // been selected as its home
          subClusterId =
              federationFacade.addApplicationHomeSubCluster(appHomeSubCluster);     // 在 store 中储存 appid <-> clusterid 映射信息
        } catch (YarnException e) {
          routerMetrics.incrAppsFailedSubmitted();
          String message = "Unable to insert the ApplicationId " + applicationId
              + " into the FederationStateStore";
          RouterServerUtil.logAndThrowException(message, e);
        }
      } else {
        try {
          // update the mapping of applicationId and the home subClusterId to
          // the new subClusterId we have selected
          federationFacade.updateApplicationHomeSubCluster(appHomeSubCluster);      // 如果是重试，则更新 store 中 appid <-> clusterid 映射信息
        } catch (YarnException e) {
          String message = "Unable to update the ApplicationId " + applicationId
              + " into the FederationStateStore";
          SubClusterId subClusterIdInStateStore =
              federationFacade.getApplicationHomeSubCluster(applicationId);
          if (subClusterId == subClusterIdInStateStore) {
            LOG.info("Application " + applicationId
                + " already submitted on SubCluster " + subClusterId);
          } else {
            routerMetrics.incrAppsFailedSubmitted();
            RouterServerUtil.logAndThrowException(message, e);
          }
        }
      }

      ApplicationClientProtocol clientRMProxy =
          getClientRMProxyForSubCluster(subClusterId);                              // 获取指定 cluster 的代理，并开始提交 app

      SubmitApplicationResponse response = null;
      try {
        response = clientRMProxy.submitApplication(request);
      } catch (Exception e) {
        LOG.warn("Unable to submit the application " + applicationId
            + "to SubCluster " + subClusterId.getId(), e);
      }

      if (response != null) {
        LOG.info("Application "
            + request.getApplicationSubmissionContext().getApplicationName()
            + " with appId " + applicationId + " submitted on " + subClusterId);
        long stopTime = clock.getTime();
        routerMetrics.succeededAppsSubmitted(stopTime - startTime);
        return response;
      } else {
        // Empty response from the ResourceManager.
        // Blacklist this subcluster for this request.
        blacklist.add(subClusterId);                                                // 如果 RM 没有响应，则将其放入黑名单，该黑名单仅仅本次请求有效
      }
    }

    routerMetrics.incrAppsFailedSubmitted();                                        // 达到重试次数后，此次作业提交失败
    String errMsg = "Application "
        + request.getApplicationSubmissionContext().getApplicationName()
        + " with appId " + applicationId + " failed to be submitted.";
    LOG.error(errMsg);
    throw new YarnException(errMsg);
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   *
   * Possible failures and behaviors:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router will timeout and the call will fail.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnException, IOException {

    long startTime = clock.getTime();

    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppsFailedKilled();
      RouterServerUtil.logAndThrowException(
          "Missing forceKillApplication request or ApplicationId.", null);
    }
    ApplicationId applicationId = request.getApplicationId();
    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(request.getApplicationId());
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedKilled();
      RouterServerUtil.logAndThrowException("Application " + applicationId
          + " does not exist in FederationStateStore", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    KillApplicationResponse response = null;
    try {
      LOG.info("forceKillApplication " + applicationId + " on SubCluster "
          + subClusterId);
      response = clientRMProxy.forceKillApplication(request);
    } catch (Exception e) {
      routerMetrics.incrAppsFailedKilled();
      LOG.error("Unable to kill the application report for "
          + request.getApplicationId() + "to SubCluster "
          + subClusterId.getId(), e);
      throw e;
    }

    if (response == null) {
      LOG.error("No response when attempting to kill the application "
          + applicationId + " to SubCluster " + subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppsKilled(stopTime - startTime);
    return response;
  }

  /**
   * The YARN Router will forward to the respective YARN RM in which the AM is
   * running.
   *
   * Possible failure:
   *
   * Client: identical behavior as {@code ClientRMService}.
   *
   * Router: the Client will timeout and resubmit the request.
   *
   * ResourceManager: the Router will timeout and the call will fail.
   *
   * State Store: the Router will timeout and it will retry depending on the
   * FederationFacade settings - if the failure happened before the select
   * operation.
   */
  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnException, IOException {

    long startTime = clock.getTime();

    if (request == null || request.getApplicationId() == null) {
      routerMetrics.incrAppsFailedRetrieved();
      RouterServerUtil.logAndThrowException(
          "Missing getApplicationReport request or applicationId information.",
          null);
    }

    SubClusterId subClusterId = null;

    try {
      subClusterId = federationFacade
          .getApplicationHomeSubCluster(request.getApplicationId());
    } catch (YarnException e) {
      routerMetrics.incrAppsFailedRetrieved();
      RouterServerUtil
          .logAndThrowException("Application " + request.getApplicationId()
              + " does not exist in FederationStateStore", e);
    }

    ApplicationClientProtocol clientRMProxy =
        getClientRMProxyForSubCluster(subClusterId);

    GetApplicationReportResponse response = null;
    try {
      response = clientRMProxy.getApplicationReport(request);
    } catch (Exception e) {
      routerMetrics.incrAppsFailedRetrieved();
      LOG.error("Unable to get the application report for "
          + request.getApplicationId() + "to SubCluster "
          + subClusterId.getId(), e);
      throw e;
    }

    if (response == null) {
      LOG.error("No response when attempting to retrieve the report of "
          + "the application " + request.getApplicationId() + " to SubCluster "
          + subClusterId.getId());
    }

    long stopTime = clock.getTime();
    routerMetrics.succeededAppsRetrieved(stopTime - startTime);
    return response;
  }

  @Override
  public GetApplicationsResponse getApplications(GetApplicationsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public MoveApplicationAcrossQueuesResponse moveApplicationAcrossQueues(
      MoveApplicationAcrossQueuesRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetNewReservationResponse getNewReservation(
      GetNewReservationRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationSubmissionResponse submitReservation(
      ReservationSubmissionRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationListResponse listReservations(
      ReservationListRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationUpdateResponse updateReservation(
      ReservationUpdateRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public ReservationDeleteResponse deleteReservation(
      ReservationDeleteRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetNodesToLabelsResponse getNodeToLabels(
      GetNodesToLabelsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetLabelsToNodesResponse getLabelsToNodes(
      GetLabelsToNodesRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetClusterNodeLabelsResponse getClusterNodeLabels(
      GetClusterNodeLabelsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetApplicationAttemptReportResponse getApplicationAttemptReport(
      GetApplicationAttemptReportRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetApplicationAttemptsResponse getApplicationAttempts(
      GetApplicationAttemptsRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetContainerReportResponse getContainerReport(
      GetContainerReportRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetContainersResponse getContainers(GetContainersRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public RenewDelegationTokenResponse renewDelegationToken(
      RenewDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public CancelDelegationTokenResponse cancelDelegationToken(
      CancelDelegationTokenRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public FailApplicationAttemptResponse failApplicationAttempt(
      FailApplicationAttemptRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public UpdateApplicationPriorityResponse updateApplicationPriority(
      UpdateApplicationPriorityRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(
      UpdateApplicationTimeoutsRequest request)
      throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetAllResourceProfilesResponse getResourceProfiles(
      GetAllResourceProfilesRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetResourceProfileResponse getResourceProfile(
      GetResourceProfileRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }

  @Override
  public GetAllResourceTypeInfoResponse getResourceTypeInfo(
      GetAllResourceTypeInfoRequest request) throws YarnException, IOException {
    throw new NotImplementedException();
  }
}
