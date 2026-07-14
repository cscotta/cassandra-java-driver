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
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * A single 4.x {@link RetryPolicy} per profile that dispatches each callback to the effective 3.x
 * {@code com.datastax.driver.core.policies.RetryPolicy}, resolved in this order:
 *
 * <ol>
 *   <li>a per-request 3.x policy set via {@code Statement.setRetryPolicy(...)} (registered by {@code
 *       StatementShimAccess} when the 4.x statement is materialized, keyed by 4.x request identity);
 *   <li>the cluster-level 3.x policy set via {@code Cluster.Builder.withRetryPolicy(...)};
 *   <li>the real 4.x default policy (so the unset case matches 4.x/3.x default behavior).
 * </ol>
 *
 * <p>The driver invokes the {@code *Verdict} methods; the default interface implementations delegate
 * to the (deprecated) decision methods implemented here. A 3.x {@code RetryDecision} is mapped to a
 * 4.x {@link RetryDecision}; a 3.x decision that changes the target consistency level is a
 * documented lossy point (the CL change is not carried into the 4.x retry).
 */
public class Shim3xRetryPolicy implements RetryPolicy {

  /** 4.x request identity -&gt; per-request 3.x policy + the 3.x statement it came from. */
  private static final Map<Request, PerRequest> REGISTRY =
      Collections.synchronizedMap(new WeakHashMap<Request, PerRequest>());

  /** Identity set of 3.x policies whose {@code init(cluster)} has already run. */
  private static final Set<com.datastax.driver.core.policies.RetryPolicy> INITED =
      Collections.synchronizedSet(
          Collections.newSetFromMap(
              new IdentityHashMap<com.datastax.driver.core.policies.RetryPolicy, Boolean>()));

  private static final class PerRequest {
    final com.datastax.driver.core.policies.RetryPolicy policy;
    final com.datastax.driver.core.Statement statement;

    PerRequest(
        com.datastax.driver.core.policies.RetryPolicy policy,
        com.datastax.driver.core.Statement statement) {
      this.policy = policy;
      this.statement = statement;
    }
  }

  /** Called by {@code StatementShimAccess} when a 3.x statement carries a per-request retry policy. */
  public static void registerPerRequest(
      Request v4Statement,
      com.datastax.driver.core.policies.RetryPolicy v3Policy,
      com.datastax.driver.core.Statement v3Statement) {
    if (v4Statement != null && v3Policy != null) {
      REGISTRY.put(v4Statement, new PerRequest(v3Policy, v3Statement));
    }
  }

  private final com.datastax.driver.core.policies.RetryPolicy clusterV3;
  private final RetryPolicy v4Fallback;
  private final Cluster shimCluster;

  public Shim3xRetryPolicy(
      com.datastax.driver.core.policies.RetryPolicy clusterV3,
      RetryPolicy v4Fallback,
      Cluster shimCluster) {
    this.clusterV3 = clusterV3;
    this.v4Fallback = v4Fallback;
    this.shimCluster = shimCluster;
    if (clusterV3 != null) {
      ensureInit(clusterV3);
    }
  }

  private void ensureInit(com.datastax.driver.core.policies.RetryPolicy v3) {
    if (INITED.add(v3)) {
      try {
        v3.init(shimCluster);
      } catch (RuntimeException e) {
        // A policy that drives I/O from init is a documented caveat; don't fail the build.
      }
    }
  }

  /**
   * Resolves the effective 3.x policy + statement for a request, or {@code null} to signal that the
   * 4.x fallback should handle it.
   */
  private PerRequest resolve(Request request) {
    PerRequest perRequest = REGISTRY.get(request);
    if (perRequest != null) {
      ensureInit(perRequest.policy);
      return perRequest;
    }
    if (clusterV3 != null) {
      return new PerRequest(clusterV3, RoutingBridge.placeholderStatement(request));
    }
    return null;
  }

  private static RetryDecision toV4(
      com.datastax.driver.core.policies.RetryPolicy.RetryDecision d) {
    if (d == null) {
      return RetryDecision.RETHROW;
    }
    switch (d.getType()) {
      case IGNORE:
        return RetryDecision.IGNORE;
      case RETRY:
        // A changed target consistency level (d.getRetryConsistencyLevel()) is not carried into the
        // 4.x retry — documented lossy point.
        return d.isRetryCurrent() ? RetryDecision.RETRY_SAME : RetryDecision.RETRY_NEXT;
      case RETHROW:
      default:
        return RetryDecision.RETHROW;
    }
  }

  private com.datastax.driver.core.ConsistencyLevel errorCl(Request request) {
    if (request instanceof com.datastax.oss.driver.api.core.cql.Statement) {
      return EnumBridge.clV3(
          ((com.datastax.oss.driver.api.core.cql.Statement<?>) request).getConsistencyLevel());
    }
    return com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
  }

  @Override
  public RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    PerRequest r = resolve(request);
    if (r == null) {
      return v4Fallback
          .onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount)
          .getRetryDecision();
    }
    return toV4(
        r.policy.onReadTimeout(
            r.statement, EnumBridge.clV3(cl), blockFor, received, dataPresent, retryCount));
  }

  @Override
  public RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel cl,
      WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    PerRequest r = resolve(request);
    if (r == null) {
      return v4Fallback
          .onWriteTimeoutVerdict(request, cl, writeType, blockFor, received, retryCount)
          .getRetryDecision();
    }
    return toV4(
        r.policy.onWriteTimeout(
            r.statement,
            EnumBridge.clV3(cl),
            EnumBridge.wtV3(writeType),
            blockFor,
            received,
            retryCount));
  }

  @Override
  public RetryDecision onUnavailable(
      Request request,
      ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    PerRequest r = resolve(request);
    if (r == null) {
      return v4Fallback
          .onUnavailableVerdict(request, cl, required, alive, retryCount)
          .getRetryDecision();
    }
    return toV4(
        r.policy.onUnavailable(r.statement, EnumBridge.clV3(cl), required, alive, retryCount));
  }

  @Override
  public RetryDecision onRequestAborted(
      Request request, Throwable error, int retryCount) {
    PerRequest r = resolve(request);
    if (r == null) {
      return v4Fallback.onRequestAbortedVerdict(request, error, retryCount).getRetryDecision();
    }
    return toV4(
        r.policy.onRequestError(r.statement, errorCl(request), toV3(error), retryCount));
  }

  @Override
  public RetryDecision onErrorResponse(
      Request request, CoordinatorException error, int retryCount) {
    PerRequest r = resolve(request);
    if (r == null) {
      return v4Fallback.onErrorResponseVerdict(request, error, retryCount).getRetryDecision();
    }
    return toV4(
        r.policy.onRequestError(r.statement, errorCl(request), toV3(error), retryCount));
  }

  private static DriverException toV3(Throwable error) {
    RuntimeException v3 = ExceptionBridge.toV3(error);
    return (v3 instanceof DriverException) ? (DriverException) v3 : new DriverException(error);
  }

  @Override
  public void close() {
    if (clusterV3 != null) {
      try {
        clusterV3.close();
      } catch (RuntimeException e) {
        // best effort
      }
    }
  }
}
