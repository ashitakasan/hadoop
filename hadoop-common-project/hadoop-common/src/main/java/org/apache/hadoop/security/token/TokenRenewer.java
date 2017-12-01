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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * This is the interface for plugins that handle tokens.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class TokenRenewer {                                                // 处理 token 更新的接口

  /**
   * Does this renewer handle this kind of token?
   * @param kind the kind of the token
   * @return true if this renewer can renew it
   */
  public abstract boolean handleKind(Text kind);                                    // 这个 renewer 是否接受该类型的 token

  /**
   * Is the given token managed? Only managed tokens may be renewed or
   * cancelled.
   * @param token the token being checked
   * @return true if the token may be renewed or cancelled
   * @throws IOException
   */
  public abstract boolean isManaged(Token<?> token) throws IOException;             // 是否管理给定的令牌，只有受管理的 token 才能 renewed 或 失效
  
  /**
   * Renew the given token.
   * @return the new expiration time
   * @throws IOException
   * @throws InterruptedException 
   */
  public abstract long renew(Token<?> token,
                             Configuration conf
                             ) throws IOException, InterruptedException;
  
  /**
   * Cancel the given token
   * @throws IOException
   * @throws InterruptedException 
   */
  public abstract void cancel(Token<?> token,
                              Configuration conf
                              ) throws IOException, InterruptedException;
}
