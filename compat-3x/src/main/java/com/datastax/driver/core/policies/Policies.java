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

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.DefaultEndPointFactory;
import com.datastax.driver.core.EndPointFactory;
import com.datastax.driver.core.TimestampGenerator;

/** Policies configured for a {@link com.datastax.driver.core.Cluster} instance. */
public class Policies {

  private final LoadBalancingPolicy loadBalancingPolicy;
  private final ReconnectionPolicy reconnectionPolicy;
  private final RetryPolicy retryPolicy;
  private final AddressTranslator addressTranslator;
  private final TimestampGenerator timestampGenerator;
  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;
  private final EndPointFactory endPointFactory;

  private Policies(
      LoadBalancingPolicy loadBalancingPolicy,
      ReconnectionPolicy reconnectionPolicy,
      RetryPolicy retryPolicy,
      AddressTranslator addressTranslator,
      TimestampGenerator timestampGenerator,
      SpeculativeExecutionPolicy speculativeExecutionPolicy,
      EndPointFactory endPointFactory) {
    this.loadBalancingPolicy = loadBalancingPolicy;
    this.reconnectionPolicy = reconnectionPolicy;
    this.retryPolicy = retryPolicy;
    this.addressTranslator = addressTranslator;
    this.timestampGenerator = timestampGenerator;
    this.speculativeExecutionPolicy = speculativeExecutionPolicy;
    this.endPointFactory = endPointFactory;
  }

  /**
   * Returns a builder to create a new {@code Policies} object.
   *
   * @return the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * The default load balancing policy.
   *
   * @return the default load balancing policy.
   */
  public static LoadBalancingPolicy defaultLoadBalancingPolicy() {
    return new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());
  }

  /**
   * The default reconnection policy.
   *
   * @return the default reconnection policy.
   */
  public static ReconnectionPolicy defaultReconnectionPolicy() {
    return new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);
  }

  /**
   * The default retry policy.
   *
   * @return the default retry policy.
   */
  public static RetryPolicy defaultRetryPolicy() {
    return DefaultRetryPolicy.INSTANCE;
  }

  /**
   * The default address translator.
   *
   * @return the default address translator.
   */
  public static AddressTranslator defaultAddressTranslator() {
    return new IdentityTranslator();
  }

  /**
   * The default timestamp generator.
   *
   * @return the default timestamp generator.
   */
  public static TimestampGenerator defaultTimestampGenerator() {
    return new AtomicMonotonicTimestampGenerator();
  }

  /**
   * The default speculative execution policy.
   *
   * @return the default speculative execution policy.
   */
  public static SpeculativeExecutionPolicy defaultSpeculativeExecutionPolicy() {
    return NoSpeculativeExecutionPolicy.INSTANCE;
  }

  /**
   * The default endpoint factory.
   *
   * @return the default endpoint factory.
   */
  public static EndPointFactory defaultEndPointFactory() {
    return new DefaultEndPointFactory();
  }

  /**
   * The load balancing policy in use.
   *
   * @return the load balancing policy in use.
   */
  public LoadBalancingPolicy getLoadBalancingPolicy() {
    return loadBalancingPolicy;
  }

  /**
   * The reconnection policy in use.
   *
   * @return the reconnection policy in use.
   */
  public ReconnectionPolicy getReconnectionPolicy() {
    return reconnectionPolicy;
  }

  /**
   * The retry policy in use.
   *
   * @return the retry policy in use.
   */
  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * The address translator in use.
   *
   * @return the address translator in use.
   */
  public AddressTranslator getAddressTranslator() {
    return addressTranslator;
  }

  /**
   * The timestamp generator in use.
   *
   * @return the timestamp generator in use.
   */
  public TimestampGenerator getTimestampGenerator() {
    return timestampGenerator;
  }

  /**
   * The speculative execution policy in use.
   *
   * @return the speculative execution policy in use.
   */
  public SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
    return speculativeExecutionPolicy;
  }

  /**
   * The endpoint factory in use.
   *
   * @return the endpoint factory in use.
   */
  public EndPointFactory getEndPointFactory() {
    return endPointFactory;
  }

  /** A builder to create a new {@link Policies} object. */
  public static class Builder {
    private LoadBalancingPolicy loadBalancingPolicy;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;
    private AddressTranslator addressTranslator;
    private TimestampGenerator timestampGenerator;
    private SpeculativeExecutionPolicy speculativeExecutionPolicy;
    private EndPointFactory endPointFactory;

    /** Creates a new {@code Policies.Builder} instance. */
    public Builder() {}

    /**
     * Sets the load balancing policy.
     *
     * @param loadBalancingPolicy the policy.
     * @return this builder.
     */
    public Builder withLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
      this.loadBalancingPolicy = loadBalancingPolicy;
      return this;
    }

    /**
     * Sets the reconnection policy.
     *
     * @param reconnectionPolicy the policy.
     * @return this builder.
     */
    public Builder withReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
      this.reconnectionPolicy = reconnectionPolicy;
      return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retryPolicy the policy.
     * @return this builder.
     */
    public Builder withRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Sets the address translator.
     *
     * @param addressTranslator the translator.
     * @return this builder.
     */
    public Builder withAddressTranslator(AddressTranslator addressTranslator) {
      this.addressTranslator = addressTranslator;
      return this;
    }

    /**
     * Sets the timestamp generator.
     *
     * @param timestampGenerator the generator.
     * @return this builder.
     */
    public Builder withTimestampGenerator(TimestampGenerator timestampGenerator) {
      this.timestampGenerator = timestampGenerator;
      return this;
    }

    /**
     * Sets the speculative execution policy.
     *
     * @param speculativeExecutionPolicy the policy.
     * @return this builder.
     */
    public Builder withSpeculativeExecutionPolicy(
        SpeculativeExecutionPolicy speculativeExecutionPolicy) {
      this.speculativeExecutionPolicy = speculativeExecutionPolicy;
      return this;
    }

    /**
     * Sets the endpoint factory.
     *
     * @param endPointFactory the factory.
     * @return this builder.
     */
    public Builder withEndPointFactory(EndPointFactory endPointFactory) {
      this.endPointFactory = endPointFactory;
      return this;
    }

    /**
     * Builds a new {@code Policies} object, filling unset fields with their defaults.
     *
     * @return the newly built {@code Policies} object.
     */
    public Policies build() {
      return new Policies(
          loadBalancingPolicy == null ? defaultLoadBalancingPolicy() : loadBalancingPolicy,
          reconnectionPolicy == null ? defaultReconnectionPolicy() : reconnectionPolicy,
          retryPolicy == null ? defaultRetryPolicy() : retryPolicy,
          addressTranslator == null ? defaultAddressTranslator() : addressTranslator,
          timestampGenerator == null ? defaultTimestampGenerator() : timestampGenerator,
          speculativeExecutionPolicy == null
              ? defaultSpeculativeExecutionPolicy()
              : speculativeExecutionPolicy,
          endPointFactory == null ? defaultEndPointFactory() : endPointFactory);
    }
  }
}
