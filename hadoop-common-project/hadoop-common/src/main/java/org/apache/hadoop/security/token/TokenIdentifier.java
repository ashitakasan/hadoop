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

package org.apache.hadoop.security.token;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An identifier that identifies a token, may contain public information 
 * about a token, including its kind (or type).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenIdentifier implements Writable {                         // token 的标识符，可以包含有关令牌的公共信息，包括其种类、类型

  private String trackingId = null;

  /**
   * Get the token kind
   * @return the kind of the token
   */
  public abstract Text getKind();

  /**
   * Get the Ugi with the username encoded in the token identifier
   * 
   * @return the username. null is returned if username in the identifier is
   *         empty or null.
   */
  public abstract UserGroupInformation getUser();                                   // 用 token 中编码的用户名获取 ugi

  /**
   * Get the bytes for the token identifier
   * @return the bytes of the identifier
   */
  public byte[] getBytes() {                                                        // 获取 token 的字节数组，目前默认为 4096 字节
    DataOutputBuffer buf = new DataOutputBuffer(4096);
    try {
      this.write(buf);
    } catch (IOException ie) {
      throw new RuntimeException("i/o error in getBytes", ie);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  /**
   * Returns a tracking identifier that can be used to associate usages of a
   * token across multiple client sessions.
   *
   * Currently, this function just returns an MD5 of {{@link #getBytes()}.
   *
   * @return tracking identifier
   */
  public String getTrackingId() {                                                   // 返回可用于在多个客户端会话间关联 token 使用的 跟踪标识符
    if (trackingId == null) {
      trackingId = DigestUtils.md5Hex(getBytes());
    }
    return trackingId;
  }
}
