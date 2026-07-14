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
// Shim provenance: 3.12.1 base + shim facade edits — net-new shim lines are layered on
// top of the carried-forward 3.12.1 source. For the exact added/changed lines see
// PROVENANCE.md and `src/test/scripts/provenance-diff.sh` (diff vs tag shim-3x-vendor-3.12.1).
package com.datastax.driver.core;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;

/**
 * Metrics exposed by the driver, over the <a href="http://metrics.dropwizard.io/">Dropwizard
 * Metrics</a> library.
 *
 * <p>Shim over the 4.x driver, reporting REAL values: {@link #getRegistry()} returns the same {@code
 * com.codahale.metrics.MetricRegistry} the connected 4.x session uses, {@link #getRequestsTimer()}
 * is the 4.x session {@code cql-requests} Timer, {@link #getBytesSent()}/{@link #getBytesReceived()}
 * are the session byte meters, and the {@link Errors} counters sum the corresponding 4.x per-node
 * counters (3.x reports them cluster-wide). The {@code known-hosts}/{@code connected-to} gauges are
 * backed by cluster metadata; open-connections / in-flight are summed from the per-node gauges.
 *
 * <p>A few 3.x session-aggregate gauges have no 4.x equivalent and still report zero: trashed
 * connections, request/executor/blocking-executor queue depths, and the reconnection/task scheduler
 * queue sizes. Likewise {@code retries/ignores-on-client-timeout} and {@code
 * retries/ignores-on-connection-error} have no 4.x counterpart and report zero.
 */
public class Metrics {

  private final Cluster cluster;
  private final com.datastax.oss.driver.api.core.metrics.Metrics v4; // null if metrics unavailable
  private final CqlSession session;
  private final MetricRegistry fallbackRegistry;
  private final Errors errors;

  private static final Timer EMPTY_TIMER = new Timer();
  private static final Meter EMPTY_METER = new Meter();

  private final Gauge<Integer> knownHosts = hostCountGauge();
  private final Gauge<Integer> connectedTo = hostCountGauge();
  private final Gauge<Integer> openConnections;
  private final Gauge<Integer> inFlightRequests;
  private final Gauge<Integer> trashedConnections = zero();
  private final Gauge<Integer> requestQueueDepth = zero();
  private final Gauge<Integer> executorQueueDepth = zero();
  private final Gauge<Integer> blockingExecutorQueueDepth = zero();
  private final Gauge<Integer> reconnectionSchedulerQueueSize = zero();
  private final Gauge<Integer> taskSchedulerQueueSize = zero();

  Metrics(Cluster cluster, CqlSession session) {
    this.cluster = cluster;
    this.session = session;
    com.datastax.oss.driver.api.core.metrics.Metrics m = null;
    try {
      m = session.getMetrics().orElse(null);
    } catch (RuntimeException ignored) {
      // leave null; fall back to an empty registry
    }
    this.v4 = m;
    this.fallbackRegistry = (m == null) ? new MetricRegistry() : null;
    this.openConnections = nodeSumGauge(DefaultNodeMetric.OPEN_CONNECTIONS);
    this.inFlightRequests = nodeSumGauge(DefaultNodeMetric.IN_FLIGHT);
    // Build Errors last, so its NodeSumCounters capture the (now-set) v4/session.
    this.errors = new Errors();
  }

  public MetricRegistry getRegistry() {
    return v4 != null ? v4.getRegistry() : fallbackRegistry;
  }

  public Timer getRequestsTimer() {
    Metric m = sessionMetric(DefaultSessionMetric.CQL_REQUESTS);
    return (m instanceof Timer) ? (Timer) m : EMPTY_TIMER;
  }

  public Errors getErrorMetrics() {
    return errors;
  }

  public Gauge<Integer> getKnownHosts() {
    return knownHosts;
  }

  public Gauge<Integer> getConnectedToHosts() {
    return connectedTo;
  }

  public Gauge<Integer> getOpenConnections() {
    return openConnections;
  }

  public Gauge<Integer> getTrashedConnections() {
    return trashedConnections;
  }

  public Gauge<Integer> getInFlightRequests() {
    return inFlightRequests;
  }

  public Gauge<Integer> getRequestQueueDepth() {
    return requestQueueDepth;
  }

  public Gauge<Integer> getExecutorQueueDepth() {
    return executorQueueDepth;
  }

  public Gauge<Integer> getBlockingExecutorQueueDepth() {
    return blockingExecutorQueueDepth;
  }

  public Gauge<Integer> getReconnectionSchedulerQueueSize() {
    return reconnectionSchedulerQueueSize;
  }

  public Gauge<Integer> getTaskSchedulerQueueSize() {
    return taskSchedulerQueueSize;
  }

  public Meter getBytesSent() {
    Metric m = sessionMetric(DefaultSessionMetric.BYTES_SENT);
    return (m instanceof Meter) ? (Meter) m : EMPTY_METER;
  }

  public Meter getBytesReceived() {
    Metric m = sessionMetric(DefaultSessionMetric.BYTES_RECEIVED);
    return (m instanceof Meter) ? (Meter) m : EMPTY_METER;
  }

  // ---- 4.x lookups ---------------------------------------------------------------------------

  private Metric sessionMetric(DefaultSessionMetric metric) {
    if (v4 == null) {
      return null;
    }
    try {
      return v4.getSessionMetric(metric).orElse(null);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private Gauge<Integer> hostCountGauge() {
    return new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        try {
          return cluster.getMetadata().getAllHosts().size();
        } catch (RuntimeException e) {
          return 0;
        }
      }
    };
  }

  /** A gauge summing an integer-valued 4.x per-node gauge across all nodes. */
  private Gauge<Integer> nodeSumGauge(final DefaultNodeMetric metric) {
    return new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        if (v4 == null || session == null) {
          return 0;
        }
        int sum = 0;
        try {
          for (Node node : session.getMetadata().getNodes().values()) {
            Metric m = v4.getNodeMetric(node, metric).orElse(null);
            if (m instanceof Gauge) {
              Object v = ((Gauge<?>) m).getValue();
              if (v instanceof Number) {
                sum += ((Number) v).intValue();
              }
            }
          }
        } catch (RuntimeException e) {
          return 0;
        }
        return sum;
      }
    };
  }

  private static Gauge<Integer> zero() {
    return new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 0;
      }
    };
  }

  /**
   * A {@link Counter} whose {@code getCount()} sums the corresponding 4.x per-node counter across
   * all nodes (3.x tracks these cluster-wide). Increment/decrement are inherited but unused.
   */
  private static final class NodeSumCounter extends Counter {
    private final com.datastax.oss.driver.api.core.metrics.Metrics v4;
    private final CqlSession session;
    private final DefaultNodeMetric metric;

    NodeSumCounter(
        com.datastax.oss.driver.api.core.metrics.Metrics v4,
        CqlSession session,
        DefaultNodeMetric metric) {
      this.v4 = v4;
      this.session = session;
      this.metric = metric;
    }

    @Override
    public long getCount() {
      if (v4 == null || session == null) {
        return 0L;
      }
      long sum = 0L;
      try {
        for (Node node : session.getMetadata().getNodes().values()) {
          Metric m = v4.getNodeMetric(node, metric).orElse(null);
          if (m instanceof Counter) {
            sum += ((Counter) m).getCount();
          }
        }
      } catch (RuntimeException e) {
        return 0L;
      }
      return sum;
    }
  }

  private Counter nodeCounter(DefaultNodeMetric metric) {
    return new NodeSumCounter(v4, session, metric);
  }

  /** A session-level counter (e.g. cql-client-timeouts), or a zero counter if unavailable. */
  private Counter sessionCounter(DefaultSessionMetric metric) {
    Metric m = sessionMetric(metric);
    return (m instanceof Counter) ? (Counter) m : new Counter();
  }

  /**
   * Metrics on errors encountered. Counters that have a 4.x per-node equivalent report the live
   * cluster-wide sum; those without one report zero (see the class notes).
   */
  public class Errors {

    private final Counter connectionErrors = nodeCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS);
    private final Counter authenticationErrors =
        nodeCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS);

    private final Counter writeTimeouts = nodeCounter(DefaultNodeMetric.WRITE_TIMEOUTS);
    private final Counter readTimeouts = nodeCounter(DefaultNodeMetric.READ_TIMEOUTS);
    private final Counter unavailables = nodeCounter(DefaultNodeMetric.UNAVAILABLES);
    private final Counter clientTimeouts = sessionCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS);

    private final Counter otherErrors = nodeCounter(DefaultNodeMetric.OTHER_ERRORS);

    private final Counter retries = nodeCounter(DefaultNodeMetric.RETRIES);
    private final Counter retriesOnWriteTimeout =
        nodeCounter(DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT);
    private final Counter retriesOnReadTimeout =
        nodeCounter(DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT);
    private final Counter retriesOnUnavailable =
        nodeCounter(DefaultNodeMetric.RETRIES_ON_UNAVAILABLE);
    private final Counter retriesOnClientTimeout = new Counter(); // no 4.x equivalent
    private final Counter retriesOnConnectionError = new Counter(); // no 4.x equivalent
    private final Counter retriesOnOtherErrors =
        nodeCounter(DefaultNodeMetric.RETRIES_ON_OTHER_ERROR);

    private final Counter ignores = nodeCounter(DefaultNodeMetric.IGNORES);
    private final Counter ignoresOnWriteTimeout =
        nodeCounter(DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT);
    private final Counter ignoresOnReadTimeout =
        nodeCounter(DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT);
    private final Counter ignoresOnUnavailable =
        nodeCounter(DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
    private final Counter ignoresOnClientTimeout = new Counter(); // no 4.x equivalent
    private final Counter ignoresOnConnectionError = new Counter(); // no 4.x equivalent
    private final Counter ignoresOnOtherErrors =
        nodeCounter(DefaultNodeMetric.IGNORES_ON_OTHER_ERROR);

    private final Counter speculativeExecutions =
        nodeCounter(DefaultNodeMetric.SPECULATIVE_EXECUTIONS);

    public Counter getConnectionErrors() {
      return connectionErrors;
    }

    public Counter getAuthenticationErrors() {
      return authenticationErrors;
    }

    public Counter getWriteTimeouts() {
      return writeTimeouts;
    }

    public Counter getReadTimeouts() {
      return readTimeouts;
    }

    public Counter getUnavailables() {
      return unavailables;
    }

    public Counter getClientTimeouts() {
      return clientTimeouts;
    }

    public Counter getOthers() {
      return otherErrors;
    }

    public Counter getRetries() {
      return retries;
    }

    public Counter getRetriesOnReadTimeout() {
      return retriesOnReadTimeout;
    }

    public Counter getRetriesOnWriteTimeout() {
      return retriesOnWriteTimeout;
    }

    public Counter getRetriesOnUnavailable() {
      return retriesOnUnavailable;
    }

    public Counter getRetriesOnClientTimeout() {
      return retriesOnClientTimeout;
    }

    public Counter getRetriesOnConnectionError() {
      return retriesOnConnectionError;
    }

    public Counter getRetriesOnOtherErrors() {
      return retriesOnOtherErrors;
    }

    public Counter getIgnores() {
      return ignores;
    }

    public Counter getIgnoresOnReadTimeout() {
      return ignoresOnReadTimeout;
    }

    public Counter getIgnoresOnWriteTimeout() {
      return ignoresOnWriteTimeout;
    }

    public Counter getIgnoresOnUnavailable() {
      return ignoresOnUnavailable;
    }

    public Counter getIgnoresOnClientTimeout() {
      return ignoresOnClientTimeout;
    }

    public Counter getIgnoresOnConnectionError() {
      return ignoresOnConnectionError;
    }

    public Counter getIgnoresOnOtherErrors() {
      return ignoresOnOtherErrors;
    }

    public Counter getSpeculativeExecutions() {
      return speculativeExecutions;
    }
  }
}
