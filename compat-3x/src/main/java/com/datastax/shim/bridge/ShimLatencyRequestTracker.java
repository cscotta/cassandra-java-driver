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

import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.NodeHostBridge;
import com.datastax.driver.core.Statement;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import java.util.List;

/**
 * Build-time 4.x {@link RequestTracker} that fans per-node request latencies out to the 3.x {@link
 * LatencyTracker}s registered on the shim {@code Cluster} (via {@code Cluster.register(tracker)}).
 * This is what makes {@code Cluster.register(LatencyTracker)} functional and, in particular, feeds
 * a {@code PercentileTracker} used by {@code PercentileSpeculativeExecutionPolicy}.
 *
 * <p>The tracker list is the live, mutable list owned by the {@code Cluster}, so trackers
 * registered after the session was built are still fed. The 3.x {@code update} takes a {@code
 * Statement}; the placeholder built from the request carries routing info but not the original CQL
 * (latency trackers such as {@code PerHostPercentileTracker} categorize by host, so this is
 * immaterial there).
 */
public final class ShimLatencyRequestTracker implements RequestTracker {

  private final List<LatencyTracker> trackers;

  public ShimLatencyRequestTracker(List<LatencyTracker> trackers) {
    this.trackers = trackers;
  }

  @Override
  public void onNodeSuccess(
      Request request, long latencyNanos, DriverExecutionProfile profile, Node node) {
    feed(request, node, null, latencyNanos);
  }

  @Override
  public void onNodeError(
      Request request,
      Throwable error,
      long latencyNanos,
      DriverExecutionProfile profile,
      Node node) {
    feed(request, node, error, latencyNanos);
  }

  private void feed(Request request, Node node, Throwable error, long latencyNanos) {
    if (trackers.isEmpty()) {
      return;
    }
    com.datastax.driver.core.Host host = NodeHostBridge.toHost(node);
    Statement stmt = RoutingBridge.placeholderStatement(request);
    // 3.x PercentileTracker.include() filters on 3.x exception classes; translate so that filter
    // stays faithful. Successful requests pass a null exception.
    Exception exception = toV3Exception(error);
    for (LatencyTracker tracker : trackers) {
      try {
        tracker.update(host, stmt, exception, latencyNanos);
      } catch (RuntimeException ignored) {
        // A misbehaving tracker must not break request handling.
      }
    }
  }

  private static Exception toV3Exception(Throwable error) {
    if (error == null) {
      return null;
    }
    try {
      return ExceptionBridge.toV3(error);
    } catch (RuntimeException e) {
      return (error instanceof Exception) ? (Exception) error : new RuntimeException(error);
    }
  }

  @Override
  public void close() {
    // Nothing to release; the tracker list is owned by the Cluster.
  }
}
