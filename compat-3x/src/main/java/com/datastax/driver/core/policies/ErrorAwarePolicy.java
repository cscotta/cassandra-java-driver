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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * A wrapper load balancing policy that adds error awareness to a child policy.
 *
 * <p>The policy tracks the number of errors on each host over a sliding time window, and excludes
 * from query plans hosts whose error rate exceeds a configurable threshold, for a configurable
 * retry period.
 *
 * <p>Note: in the 4.x-backed shim, error-rate exclusion has no equivalent; this wrapper is an inert
 * value object delegating routing to its child.
 */
public class ErrorAwarePolicy implements ChainableLoadBalancingPolicy {

  private final LoadBalancingPolicy childPolicy;
  private final int maxErrorsPerMinute;
  private final long retryPeriodNanos;
  private final ErrorFilter errorFilter;

  private ErrorAwarePolicy(Builder builder) {
    this.childPolicy = builder.childPolicy;
    this.maxErrorsPerMinute = builder.maxErrorsPerMinute;
    this.retryPeriodNanos = builder.retryPeriodNanos;
    this.errorFilter = builder.errorFilter;
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
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

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
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
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  /**
   * Creates a new error aware policy builder given the child policy that the resulting policy
   * wraps.
   *
   * @param childPolicy the load balancing policy to wrap with error awareness.
   * @return the created builder.
   */
  public static Builder builder(LoadBalancingPolicy childPolicy) {
    return new Builder(childPolicy);
  }

  @Override
  public void close() {
    childPolicy.close();
  }

  /** Utility class to create a {@link ErrorAwarePolicy}. */
  public static class Builder {
    final LoadBalancingPolicy childPolicy;

    private int maxErrorsPerMinute = 1;
    private long retryPeriodNanos = NANOSECONDS.convert(2, MINUTES);

    private ErrorFilter errorFilter = new DefaultErrorFilter();

    /**
     * Creates a {@link Builder} instance.
     *
     * @param childPolicy the load balancing policy to wrap with error awareness.
     */
    public Builder(LoadBalancingPolicy childPolicy) {
      this.childPolicy = childPolicy;
    }

    /**
     * Defines the maximum number of errors allowed per minute for each host.
     *
     * @param maxErrorsPerMinute the number.
     * @return this {@link Builder} instance, for method chaining.
     */
    public Builder withMaxErrorsPerMinute(int maxErrorsPerMinute) {
      this.maxErrorsPerMinute = maxErrorsPerMinute;
      return this;
    }

    /**
     * Defines the time during which a host is excluded by the policy once it has exceeded {@link
     * #withMaxErrorsPerMinute(int)}.
     *
     * @param retryPeriod the period of exclusion for a host.
     * @param retryPeriodTimeUnit the time unit for the retry period.
     * @return this {@link Builder} instance, for method chaining.
     */
    public Builder withRetryPeriod(long retryPeriod, TimeUnit retryPeriodTimeUnit) {
      this.retryPeriodNanos = retryPeriodTimeUnit.toNanos(retryPeriod);
      return this;
    }

    /**
     * Provides a filter that will decide which errors are counted towards {@link
     * #withMaxErrorsPerMinute(int)}.
     *
     * @param errorFilter the filter class that the policy will use.
     * @return this {@link Builder} instance, for method chaining.
     */
    public Builder withErrorsFilter(ErrorFilter errorFilter) {
      this.errorFilter = errorFilter;
      return this;
    }

    /**
     * Creates the {@link ErrorAwarePolicy} instance.
     *
     * @return the newly created {@link ErrorAwarePolicy}.
     */
    public ErrorAwarePolicy build() {
      return new ErrorAwarePolicy(this);
    }
  }

  private static class DefaultErrorFilter implements ErrorFilter {
    @Override
    public boolean shouldConsiderError(Exception e, Host host, Statement statement) {
      return true;
    }
  }

  /** A filter that decides which errors are counted towards the per-host error rate. */
  public interface ErrorFilter {
    /**
     * Whether an error should be counted in the host's error rate.
     *
     * @param e the exception.
     * @param host the host.
     * @param statement the statement that caused the exception.
     * @return {@code true} if the exception should be counted.
     */
    boolean shouldConsiderError(Exception e, Host host, Statement statement);
  }
}
