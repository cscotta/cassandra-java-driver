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
// Shim provenance: net-new bridge — package com.datastax.shim.* is internal shim support,
// not part of the 3.x ABI (japicmp compares only com.datastax.driver.*). See PROVENANCE.md.
package com.datastax.shim.bridge;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link DefaultDriverContext} that bridges the user-supplied 3.x policy/option beans onto the 4.x
 * engine. Each override returns {@code super}'s result when the corresponding 3.x option is absent,
 * so an unconfigured cluster behaves exactly like stock 4.x. This context is used for <b>all</b>
 * shim sessions (so per-request retry works universally), which is safe because every override is a
 * no-op fallback when nothing is set.
 *
 * <p>The build methods are resolved lazily (via {@code LazyReference}) after construction, so the
 * fields set here are available when they run.
 */
public class ShimDriverContext extends DefaultDriverContext {

  private final com.datastax.driver.core.Cluster shimCluster;
  private final com.datastax.driver.core.NettyOptions v3Netty;
  private final com.datastax.driver.core.ThreadingOptions v3Threading;
  private final com.datastax.driver.core.TimestampGenerator v3Timestamp;
  private final com.datastax.driver.core.policies.ReconnectionPolicy v3Reconnection;
  private final com.datastax.driver.core.policies.RetryPolicy v3Retry;
  private final com.datastax.driver.core.EndPointFactory v3EndPointFactory;

  public ShimDriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      com.datastax.driver.core.Cluster shimCluster,
      com.datastax.driver.core.NettyOptions v3Netty,
      com.datastax.driver.core.ThreadingOptions v3Threading,
      com.datastax.driver.core.TimestampGenerator v3Timestamp,
      com.datastax.driver.core.policies.ReconnectionPolicy v3Reconnection,
      com.datastax.driver.core.policies.RetryPolicy v3Retry,
      com.datastax.driver.core.EndPointFactory v3EndPointFactory) {
    super(configLoader, programmaticArguments);
    this.shimCluster = shimCluster;
    this.v3Netty = v3Netty;
    this.v3Threading = v3Threading;
    this.v3Timestamp = v3Timestamp;
    this.v3Reconnection = v3Reconnection;
    this.v3Retry = v3Retry;
    this.v3EndPointFactory = v3EndPointFactory;
  }

  @Override
  protected NettyOptions buildNettyOptions() {
    if (v3Netty == null && v3Threading == null) {
      return super.buildNettyOptions();
    }
    com.datastax.driver.core.NettyOptions effectiveNetty =
        (v3Netty != null) ? v3Netty : com.datastax.driver.core.NettyOptions.DEFAULT_INSTANCE;
    return new Shim3xNettyOptions(
        getSessionName(), getConfig().getDefaultProfile(), effectiveNetty, v3Threading);
  }

  @Override
  protected TimestampGenerator buildTimestampGenerator() {
    if (v3Timestamp == null) {
      return super.buildTimestampGenerator();
    }
    return new Shim3xTimestampGenerator(v3Timestamp);
  }

  @Override
  protected ReconnectionPolicy buildReconnectionPolicy() {
    if (v3Reconnection == null) {
      return super.buildReconnectionPolicy();
    }
    return new Shim3xReconnectionPolicy(v3Reconnection, shimCluster);
  }

  @Override
  protected Map<String, RetryPolicy> buildRetryPolicies() {
    // Always install the dispatcher (per-request retry can be set on any statement). It falls back
    // to the real 4.x policy per profile when neither a per-request nor a cluster-level 3.x policy
    // applies, so the unset case matches stock 4.x behavior.
    Map<String, RetryPolicy> v4Defaults = super.buildRetryPolicies();
    Map<String, RetryPolicy> result = new LinkedHashMap<String, RetryPolicy>();
    for (Map.Entry<String, RetryPolicy> e : v4Defaults.entrySet()) {
      result.put(e.getKey(), new Shim3xRetryPolicy(v3Retry, e.getValue(), shimCluster));
    }
    return result;
  }

  @Override
  protected TopologyMonitor buildTopologyMonitor() {
    if (v3EndPointFactory == null) {
      return super.buildTopologyMonitor();
    }
    // Honor the user's 3.x EndPointFactory via the 4.x-documented buildNodeEndPoint override.
    return new ShimTopologyMonitor(this, shimCluster, v3EndPointFactory);
  }
}
