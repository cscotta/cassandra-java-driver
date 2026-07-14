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

import com.datastax.driver.core.policies.Policies;

/**
 * The configuration of a {@link Cluster}.
 *
 * <p>Shim-owned aggregate of the 3.x option sub-objects. The lifecycle tier returns whatever option
 * beans the option tiers construct; here only the container shape and accessor signatures are
 * pinned. Values are assembled from the {@link Cluster.Builder} state (or supplied through {@link
 * Configuration.Builder}).
 */
public class Configuration {

  private final Policies policies;
  private final ProtocolOptions protocolOptions;
  private final PoolingOptions poolingOptions;
  private final SocketOptions socketOptions;
  private final MetricsOptions metricsOptions;
  private final QueryOptions queryOptions;
  private final ThreadingOptions threadingOptions;
  private final NettyOptions nettyOptions;
  private final CodecRegistry codecRegistry;
  private final String defaultKeyspace;

  Configuration(
      Policies policies,
      ProtocolOptions protocolOptions,
      PoolingOptions poolingOptions,
      SocketOptions socketOptions,
      MetricsOptions metricsOptions,
      QueryOptions queryOptions,
      ThreadingOptions threadingOptions,
      NettyOptions nettyOptions,
      CodecRegistry codecRegistry,
      String defaultKeyspace) {
    this.policies = policies;
    this.protocolOptions = protocolOptions;
    this.poolingOptions = poolingOptions;
    this.socketOptions = socketOptions;
    this.metricsOptions = metricsOptions;
    this.queryOptions = queryOptions;
    this.threadingOptions = threadingOptions;
    this.nettyOptions = nettyOptions;
    this.codecRegistry = codecRegistry;
    this.defaultKeyspace = defaultKeyspace;
  }

  /** Copy constructor, for subclasses. */
  protected Configuration(Configuration toCopy) {
    this(
        toCopy.policies,
        toCopy.protocolOptions,
        toCopy.poolingOptions,
        toCopy.socketOptions,
        toCopy.metricsOptions,
        toCopy.queryOptions,
        toCopy.threadingOptions,
        toCopy.nettyOptions,
        toCopy.codecRegistry,
        toCopy.defaultKeyspace);
  }

  /** Returns a builder to create a new {@code Configuration}. */
  public static Builder builder() {
    return new Builder();
  }

  public Policies getPolicies() {
    return policies;
  }

  public SocketOptions getSocketOptions() {
    return socketOptions;
  }

  public ProtocolOptions getProtocolOptions() {
    return protocolOptions;
  }

  public PoolingOptions getPoolingOptions() {
    return poolingOptions;
  }

  public MetricsOptions getMetricsOptions() {
    return metricsOptions;
  }

  public QueryOptions getQueryOptions() {
    return queryOptions;
  }

  public ThreadingOptions getThreadingOptions() {
    return threadingOptions;
  }

  public NettyOptions getNettyOptions() {
    return nettyOptions;
  }

  public String getDefaultKeyspace() {
    return defaultKeyspace;
  }

  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  /** A builder to create a new {@code Configuration}. */
  public static class Builder {

    private Policies policies;
    private ProtocolOptions protocolOptions;
    private PoolingOptions poolingOptions;
    private SocketOptions socketOptions;
    private MetricsOptions metricsOptions;
    private QueryOptions queryOptions;
    private ThreadingOptions threadingOptions;
    private NettyOptions nettyOptions;
    private CodecRegistry codecRegistry;
    private String defaultKeyspace;

    public Builder() {}

    public Builder withPolicies(Policies policies) {
      this.policies = policies;
      return this;
    }

    public Builder withProtocolOptions(ProtocolOptions protocolOptions) {
      this.protocolOptions = protocolOptions;
      return this;
    }

    public Builder withPoolingOptions(PoolingOptions poolingOptions) {
      this.poolingOptions = poolingOptions;
      return this;
    }

    public Builder withSocketOptions(SocketOptions socketOptions) {
      this.socketOptions = socketOptions;
      return this;
    }

    public Builder withMetricsOptions(MetricsOptions metricsOptions) {
      this.metricsOptions = metricsOptions;
      return this;
    }

    public Builder withQueryOptions(QueryOptions queryOptions) {
      this.queryOptions = queryOptions;
      return this;
    }

    public Builder withThreadingOptions(ThreadingOptions threadingOptions) {
      this.threadingOptions = threadingOptions;
      return this;
    }

    public Builder withNettyOptions(NettyOptions nettyOptions) {
      this.nettyOptions = nettyOptions;
      return this;
    }

    public Builder withCodecRegistry(CodecRegistry codecRegistry) {
      this.codecRegistry = codecRegistry;
      return this;
    }

    public Builder withDefaultKeyspace(String defaultKeyspace) {
      this.defaultKeyspace = defaultKeyspace;
      return this;
    }

    public Configuration build() {
      return new Configuration(
          policies,
          protocolOptions,
          poolingOptions,
          socketOptions,
          metricsOptions,
          queryOptions,
          threadingOptions,
          nettyOptions,
          codecRegistry,
          defaultKeyspace);
    }
  }
}
