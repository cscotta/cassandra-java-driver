/*
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
package com.datastax.shim.bridge;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;

/**
 * A {@link CqlSessionBuilder} that builds a {@link ShimDriverContext} (instead of the stock {@code
 * DefaultDriverContext}) so a user-supplied 3.x {@code NettyOptions}/{@code ThreadingOptions}/{@code
 * TimestampGenerator}/{@code ReconnectionPolicy}/{@code RetryPolicy} is honored (and per-request
 * retry works). Used for all shim sessions; each bridged option falls back to stock 4.x when unset.
 */
public class ShimCqlSessionBuilder extends CqlSessionBuilder {

  private final com.datastax.driver.core.Cluster shimCluster;
  private final com.datastax.driver.core.NettyOptions v3Netty;
  private final com.datastax.driver.core.ThreadingOptions v3Threading;
  private final com.datastax.driver.core.TimestampGenerator v3Timestamp;
  private final com.datastax.driver.core.policies.ReconnectionPolicy v3Reconnection;
  private final com.datastax.driver.core.policies.RetryPolicy v3Retry;
  private final com.datastax.driver.core.EndPointFactory v3EndPointFactory;

  public ShimCqlSessionBuilder(
      com.datastax.driver.core.Cluster shimCluster,
      com.datastax.driver.core.NettyOptions v3Netty,
      com.datastax.driver.core.ThreadingOptions v3Threading,
      com.datastax.driver.core.TimestampGenerator v3Timestamp,
      com.datastax.driver.core.policies.ReconnectionPolicy v3Reconnection,
      com.datastax.driver.core.policies.RetryPolicy v3Retry,
      com.datastax.driver.core.EndPointFactory v3EndPointFactory) {
    this.shimCluster = shimCluster;
    this.v3Netty = v3Netty;
    this.v3Threading = v3Threading;
    this.v3Timestamp = v3Timestamp;
    this.v3Reconnection = v3Reconnection;
    this.v3Retry = v3Retry;
    this.v3EndPointFactory = v3EndPointFactory;
  }

  @Override
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    return new ShimDriverContext(
        configLoader,
        programmaticArguments,
        shimCluster,
        v3Netty,
        v3Threading,
        v3Timestamp,
        v3Reconnection,
        v3Retry,
        v3EndPointFactory);
  }
}
