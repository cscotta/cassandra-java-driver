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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.NodeHostBridge;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 4.x {@link LoadBalancingPolicy} that delegates every routing decision to a user-supplied 3.x
 * {@code com.datastax.driver.core.policies.LoadBalancingPolicy}. This lets an arbitrary 3.x policy
 * passed to {@code Cluster.Builder.withLoadBalancingPolicy(...)} be honored on the 4.x engine.
 *
 * <p><b>Instance hand-off.</b> 4.x instantiates the load-balancing policy reflectively from the
 * config option {@code basic.load-balancing-policy.class}; there is no API to pass a policy
 * instance. The shim therefore registers the user's 3.x policy (together with the shim {@code
 * Cluster} that the policy's {@code init} needs) in {@link #REGISTRY} under a fresh token, and
 * writes that token into the programmatic config under {@link #TOKEN_OPTION}. This class, when
 * built by 4.x, reads the token back and recovers the registration.
 *
 * <p>The class is instantiated through the two-arg {@code (DriverContext, String)} constructor for
 * the load-balancing SPI (see {@code Reflection.buildFromConfigProfiles}); the one-arg {@code
 * (DriverContext)} constructor is provided for the global-policy convention as well.
 *
 * <p><b>Fidelity notes.</b> The 4.x {@code DistanceReporter} is push-based whereas 3.x
 * {@code distance(Host)} is pull-based, so distances are pushed at {@code init} time and re-pushed
 * on every topology event. {@link #newQueryPlan} carries the request's routing key, routing
 * keyspace, and consistency level into a faithful 3.x {@code SimpleStatement}, and forwards the
 * session keyspace as the logged keyspace, so token-aware 3.x policies route on the same key the
 * 4.x engine computed.
 */
public class Shim3xLoadBalancingPolicy implements LoadBalancingPolicy {

  /** Path of the custom config option carrying the registry token. */
  public static final String TOKEN_PATH = "shim.load-balancing-policy-token";

  /** The custom {@link DriverOption} used to inject / read the registry token. */
  public static final DriverOption TOKEN_OPTION =
      new DriverOption() {
        @Override
        public String getPath() {
          return TOKEN_PATH;
        }
      };

  /** token -&gt; (shim Cluster + user's 3.x policy). Populated by the shim Cluster at build time. */
  private static final ConcurrentHashMap<String, Registration> REGISTRY =
      new ConcurrentHashMap<String, Registration>();

  /** Bundle of the state the reflective instance needs to recover from a token. */
  public static final class Registration {
    final Cluster cluster;
    final com.datastax.driver.core.policies.LoadBalancingPolicy v3;

    Registration(Cluster cluster, com.datastax.driver.core.policies.LoadBalancingPolicy v3) {
      this.cluster = cluster;
      this.v3 = v3;
    }
  }

  /** Registers a user 3.x policy under {@code token}. Called by the shim {@code Cluster}. */
  public static void register(
      String token, Cluster cluster, com.datastax.driver.core.policies.LoadBalancingPolicy v3) {
    REGISTRY.put(token, new Registration(cluster, v3));
  }

  /** Drops the registration for {@code token} (called when the shim {@code Cluster} closes). */
  public static void deregister(String token) {
    if (token != null) {
      REGISTRY.remove(token);
    }
  }

  private final com.datastax.driver.core.policies.LoadBalancingPolicy v3;
  private final Cluster shimCluster;
  private volatile DistanceReporter reporter;

  /** Global-policy convention (profileName null). */
  public Shim3xLoadBalancingPolicy(DriverContext context) {
    this(context, null);
  }

  /** Per-profile convention used by the load-balancing SPI. */
  public Shim3xLoadBalancingPolicy(DriverContext context, String profileName) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    String token = profile.isDefined(TOKEN_OPTION) ? profile.getString(TOKEN_OPTION) : null;
    Registration reg = token == null ? null : REGISTRY.get(token);
    if (reg == null) {
      throw new IllegalStateException(
          "No 3.x load-balancing policy registered for shim token '"
              + token
              + "'; the shim could not delegate to the user-supplied policy.");
    }
    this.v3 = reg.v3;
    this.shimCluster = reg.cluster;
  }

  @Override
  public void init(Map<UUID, Node> nodes, DistanceReporter distanceReporter) {
    this.reporter = distanceReporter;
    Collection<Host> hosts = new ArrayList<Host>(nodes.size());
    for (Node node : nodes.values()) {
      hosts.add(NodeHostBridge.toHost(node));
    }
    v3.init(shimCluster, hosts);
    for (Node node : nodes.values()) {
      distanceReporter.setDistance(node, dist(v3.distance(NodeHostBridge.toHost(node))));
    }
  }

  @Override
  public Queue<Node> newQueryPlan(Request request, Session session) {
    // 3.x separates the logged (session) keyspace from the statement's own keyspace; a 3.x
    // TokenAwarePolicy falls back to the logged keyspace when the statement keyspace is null. The
    // placeholder statement carries the request's routing key / keyspace / CL so a token-aware 3.x
    // policy routes on the same key the 4.x engine computed.
    String loggedKeyspace = RoutingBridge.loggedKeyspace(session);
    SimpleStatement stmt = RoutingBridge.placeholderStatement(request);
    Iterator<Host> it = v3.newQueryPlan(loggedKeyspace, stmt);
    Queue<Node> plan = new ArrayDeque<Node>();
    while (it.hasNext()) {
      Node node = NodeHostBridge.toNode(it.next());
      if (node != null) {
        plan.add(node);
      }
    }
    return plan;
  }

  @Override
  public void onAdd(Node node) {
    Host host = NodeHostBridge.toHost(node);
    v3.onAdd(host);
    report(node, host);
  }

  @Override
  public void onUp(Node node) {
    Host host = NodeHostBridge.toHost(node);
    v3.onUp(host);
    report(node, host);
  }

  @Override
  public void onDown(Node node) {
    Host host = NodeHostBridge.toHost(node);
    v3.onDown(host);
    report(node, host);
  }

  @Override
  public void onRemove(Node node) {
    Host host = NodeHostBridge.toHost(node);
    v3.onRemove(host);
    // No distance to re-report: the node is leaving the cluster.
  }

  @Override
  public void close() {
    v3.close();
  }

  private void report(Node node, Host host) {
    DistanceReporter r = reporter;
    if (r != null) {
      r.setDistance(node, dist(v3.distance(host)));
    }
  }

  private static NodeDistance dist(HostDistance d) {
    if (d == null) {
      return NodeDistance.LOCAL;
    }
    switch (d) {
      case REMOTE:
        return NodeDistance.REMOTE;
      case IGNORED:
        return NodeDistance.IGNORED;
      case LOCAL:
      default:
        return NodeDistance.LOCAL;
    }
  }
}
