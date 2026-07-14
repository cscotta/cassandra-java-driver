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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NodeHostBridge;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy.SpeculativeExecutionPlan;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 4.x {@link SpeculativeExecutionPolicy} that delegates every decision to a user-supplied 3.x
 * {@code com.datastax.driver.core.policies.SpeculativeExecutionPolicy}, preserving the 3.x behavior
 * ({@code PercentileSpeculativeExecutionPolicy}'s percentile math, the stateful per-request
 * counter, {@code maxSpeculativeExecutions}, etc.) — the adapter only <b>drives</b> the 3.x code.
 *
 * <p><b>Instance hand-off.</b> 4.x instantiates the speculative-execution policy reflectively from
 * {@code advanced.speculative-execution-policy.class}; there is no API to pass an instance. The
 * shim registers the user's policy (with the shim {@code Cluster} it needs at {@code init}) in
 * {@link #REGISTRY} under a fresh token written to the programmatic config under {@link
 * #TOKEN_OPTION}; this class reads the token back and recovers the registration (mirrors {@code
 * Shim3xLoadBalancingPolicy}).
 *
 * <p>{@code v3.init(cluster)} is called exactly once per shim {@code Cluster} (guarded on the
 * registration), so a {@code PercentileSpeculativeExecutionPolicy} registers its tracker once via
 * {@code cluster.register(tracker)} — the shim {@code Cluster} then feeds that tracker from a
 * build-time 4.x {@code RequestTracker} ({@code ShimLatencyRequestTracker}).
 */
public class Shim3xSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

  /** Path of the custom config option carrying the registry token. */
  public static final String TOKEN_PATH = "shim.speculative-execution-policy-token";

  /** The custom {@link DriverOption} used to inject / read the registry token. */
  public static final DriverOption TOKEN_OPTION =
      new DriverOption() {
        @Override
        public String getPath() {
          return TOKEN_PATH;
        }
      };

  private static final ConcurrentHashMap<String, Registration> REGISTRY =
      new ConcurrentHashMap<String, Registration>();

  /** Bundle recovered from a token: shim Cluster + user's 3.x policy + one-shot init guard. */
  public static final class Registration {
    final Cluster cluster;
    final com.datastax.driver.core.policies.SpeculativeExecutionPolicy v3;
    final AtomicBoolean initialized = new AtomicBoolean(false);

    Registration(Cluster cluster, com.datastax.driver.core.policies.SpeculativeExecutionPolicy v3) {
      this.cluster = cluster;
      this.v3 = v3;
    }
  }

  public static void register(
      String token, Cluster cluster, com.datastax.driver.core.policies.SpeculativeExecutionPolicy v3) {
    REGISTRY.put(token, new Registration(cluster, v3));
  }

  public static void deregister(String token) {
    if (token != null) {
      REGISTRY.remove(token);
    }
  }

  private final com.datastax.driver.core.policies.SpeculativeExecutionPolicy v3;

  // One 3.x plan per in-flight request; weak keys so completed requests don't leak.
  private final Map<Object, SpeculativeExecutionPlan> plans =
      Collections.synchronizedMap(new WeakHashMap<Object, SpeculativeExecutionPlan>());

  public Shim3xSpeculativeExecutionPolicy(DriverContext context) {
    this(context, null);
  }

  public Shim3xSpeculativeExecutionPolicy(DriverContext context, String profileName) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    String token = profile.isDefined(TOKEN_OPTION) ? profile.getString(TOKEN_OPTION) : null;
    Registration reg = token == null ? null : REGISTRY.get(token);
    if (reg == null) {
      throw new IllegalStateException(
          "No 3.x speculative-execution policy registered for shim token '"
              + token
              + "'; the shim could not delegate to the user-supplied policy.");
    }
    this.v3 = reg.v3;
    // Match 3.x: init(cluster) exactly once per Cluster, so the policy can register its tracker.
    if (reg.initialized.compareAndSet(false, true)) {
      reg.v3.init(reg.cluster);
    }
  }

  @Override
  public long nextExecution(Node node, CqlIdentifier keyspace, Request request, int runningExecutions) {
    SpeculativeExecutionPlan plan;
    synchronized (plans) {
      plan = plans.get(request);
      if (plan == null) {
        String loggedKeyspace = keyspace == null ? null : keyspace.asInternal();
        plan = v3.newPlan(loggedKeyspace, RoutingBridge.placeholderStatement(request));
        plans.put(request, plan);
      }
    }
    // 3.x plan.nextExecution(lastQueried): the percentile-latency delay in ms, or < 0 to stop
    // (preserving the stateful counter / maxSpeculativeExecutions). 4.x uses the same ms/stop
    // convention, so the value passes through directly.
    long delayMs = plan.nextExecution(NodeHostBridge.toHost(node));
    if (delayMs < 0) {
      synchronized (plans) {
        plans.remove(request);
      }
      return -1;
    }
    return delayMs;
  }

  @Override
  public void close() {
    v3.close();
  }
}
