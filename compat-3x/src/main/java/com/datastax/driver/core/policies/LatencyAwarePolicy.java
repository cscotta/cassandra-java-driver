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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper load balancing policy that adds latency awareness to a child policy.
 *
 * <p>When used, this policy will collect the latencies of the queries to each Cassandra node and
 * maintain a per-node latency score (an average). Based on these scores, the policy will penalize
 * (technically, it will ignore them, unless no other nodes are up) the nodes that are slower than
 * the best performing node by more than some configurable amount (the exclusion threshold).
 *
 * <p>Note: in the 4.x-backed shim, latency awareness is folded into the default policy's
 * slow-replica avoidance; this wrapper is an inert value object delegating routing to its child.
 */
public class LatencyAwarePolicy implements ChainableLoadBalancingPolicy {

  private final LoadBalancingPolicy childPolicy;
  private final double exclusionThreshold;
  private final long scale;
  private final long retryPeriod;
  private final long updateRate;
  private final int minMeasure;

  private LatencyAwarePolicy(
      LoadBalancingPolicy childPolicy,
      double exclusionThreshold,
      long scale,
      long retryPeriod,
      long updateRate,
      int minMeasure) {
    this.childPolicy = childPolicy;
    this.exclusionThreshold = exclusionThreshold;
    this.scale = scale;
    this.retryPeriod = retryPeriod;
    this.updateRate = updateRate;
    this.minMeasure = minMeasure;
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  /**
   * Creates a new latency aware policy builder given the child policy that the resulting policy
   * wraps.
   *
   * @param childPolicy the load balancing policy to wrap with latency awareness.
   * @return the created builder.
   */
  public static Builder builder(LoadBalancingPolicy childPolicy) {
    return new Builder(childPolicy);
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    childPolicy.init(cluster, hosts);
  }

  @Override
  public HostDistance distance(Host host) {
    return childPolicy.distance(host);
  }

  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    return childPolicy.newQueryPlan(loggedKeyspace, statement);
  }

  /**
   * Returns a snapshot of the scores (latency averages) maintained by this policy.
   *
   * @return a new (immutable) {@link Snapshot} object containing the current latency scores.
   */
  public Snapshot getScoresSnapshot() {
    return new Snapshot(Collections.<Host, Snapshot.Stats>emptyMap());
  }

  @Override
  public void onUp(Host host) {
    childPolicy.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    childPolicy.onDown(host);
  }

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
  }

  @Override
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }

  /** An immutable snapshot of the per-host latency scores. */
  public static class Snapshot {
    private final Map<Host, Stats> stats;

    private Snapshot(Map<Host, Stats> stats) {
      this.stats = stats;
    }

    /**
     * A map of the statistics for all hosts tracked by this snapshot.
     *
     * @return the immutable map.
     */
    public Map<Host, Stats> getAllStats() {
      return stats;
    }

    /**
     * The {@code Stats} object for a given host.
     *
     * @param host the host to return the stats of.
     * @return the {@code Stats} for {@code host} in this snapshot or {@code null} if not present.
     */
    public Stats getStats(Host host) {
      return stats.get(host);
    }

    /** A set of statistics about latencies to a given host. */
    public static class Stats {
      private final long lastUpdatedSince;
      private final long average;
      private final long nbMeasurements;

      private Stats(long lastUpdatedSince, long average, long nbMeasurements) {
        this.lastUpdatedSince = lastUpdatedSince;
        this.average = average;
        this.nbMeasurements = nbMeasurements;
      }

      /**
       * The number of nanoseconds since the last latency update was recorded.
       *
       * @return the elapsed time in nanoseconds.
       */
      public long lastUpdatedSince() {
        return lastUpdatedSince;
      }

      /**
       * The latency score for the host this is the statistics of, in nanoseconds.
       *
       * @return the latency score, or {@code -1L} if not enough measurements have been recorded.
       */
      public long getLatencyScore() {
        return average;
      }

      /**
       * The number of recorded latency measurements for the host this is the statistics of.
       *
       * @return the number of measurements.
       */
      public long getMeasurementsCount() {
        return nbMeasurements;
      }
    }
  }

  /** Helper builder object to create a latency aware policy. */
  public static class Builder {

    public static final double DEFAULT_EXCLUSION_THRESHOLD = 2.0;
    public static final long DEFAULT_SCALE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    public static final long DEFAULT_RETRY_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(10);
    public static final long DEFAULT_UPDATE_RATE_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    public static final int DEFAULT_MIN_MEASURE = 50;

    private final LoadBalancingPolicy childPolicy;
    private double exclusionThreshold = DEFAULT_EXCLUSION_THRESHOLD;
    private long scale = DEFAULT_SCALE_NANOS;
    private long retryPeriod = DEFAULT_RETRY_PERIOD_NANOS;
    private long updateRate = DEFAULT_UPDATE_RATE_NANOS;
    private int minMeasure = DEFAULT_MIN_MEASURE;

    /**
     * Creates a new latency aware policy builder given the child policy that the resulting policy
     * should wrap.
     *
     * @param childPolicy the load balancing policy to wrap with latency awareness.
     */
    public Builder(LoadBalancingPolicy childPolicy) {
      this.childPolicy = childPolicy;
    }

    /**
     * Sets the exclusion threshold to use for the resulting latency aware policy.
     *
     * @param exclusionThreshold the exclusion threshold to use.
     * @return this builder.
     */
    public Builder withExclusionThreshold(double exclusionThreshold) {
      if (exclusionThreshold < 1d)
        throw new IllegalArgumentException(
            "Invalid exclusion threshold, must be greater than 1.");
      this.exclusionThreshold = exclusionThreshold;
      return this;
    }

    /**
     * Sets the scale to use for the resulting latency aware policy.
     *
     * @param scale the scale to use.
     * @param unit the unit of {@code scale}.
     * @return this builder.
     */
    public Builder withScale(long scale, TimeUnit unit) {
      if (scale <= 0)
        throw new IllegalArgumentException("Invalid scale value, must be strictly positive");
      this.scale = unit.toNanos(scale);
      return this;
    }

    /**
     * Sets the retry period for the resulting latency aware policy.
     *
     * @param retryPeriod the retry period to use.
     * @param unit the unit for {@code retryPeriod}.
     * @return this builder.
     */
    public Builder withRetryPeriod(long retryPeriod, TimeUnit unit) {
      if (retryPeriod < 0)
        throw new IllegalArgumentException("Invalid retry period value, must be positive");
      this.retryPeriod = unit.toNanos(retryPeriod);
      return this;
    }

    /**
     * Sets the update rate for the resulting latency aware policy.
     *
     * @param updateRate the update rate to use.
     * @param unit the unit for {@code updateRate}.
     * @return this builder.
     */
    public Builder withUpdateRate(long updateRate, TimeUnit unit) {
      if (updateRate <= 0)
        throw new IllegalArgumentException("Invalid update rate value, must be strictly positive");
      this.updateRate = unit.toNanos(updateRate);
      return this;
    }

    /**
     * Sets the minimum number of measurements per-host to consider for the resulting latency aware
     * policy.
     *
     * @param minMeasure the minimum measurements to consider.
     * @return this builder.
     */
    public Builder withMininumMeasurements(int minMeasure) {
      if (minMeasure < 0)
        throw new IllegalArgumentException("Invalid minimum measurements value, must be positive");
      this.minMeasure = minMeasure;
      return this;
    }

    /**
     * Builds a new latency aware policy using the options set on this builder.
     *
     * @return the newly created {@code LatencyAwarePolicy}.
     */
    public LatencyAwarePolicy build() {
      return new LatencyAwarePolicy(
          childPolicy, exclusionThreshold, scale, retryPeriod, updateRate, minMeasure);
    }
  }
}
