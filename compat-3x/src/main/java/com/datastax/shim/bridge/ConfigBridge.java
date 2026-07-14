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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ExtendedAuthProvider;
import com.datastax.driver.core.ExtendedRemoteEndpointAwareSslOptions;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLEngine;

/**
 * Translates the 3.x option beans held by {@code Cluster.Builder} into a 4.x {@link
 * DriverConfigLoader} that is applied when the underlying {@code CqlSession} is built.
 *
 * <p>Only values the user actually supplied are written, so the common (unconfigured) path leaves
 * the 4.x defaults untouched — this keeps the shim's connection behavior aligned with the real
 * 3.12.1 driver. Members with no 4.x equivalent are left inert (see the per-bean catalogue).
 */
public final class ConfigBridge {

  private ConfigBridge() {}

  /**
   * Builds a config loader from the supplied beans, or returns {@code null} if nothing needs to be
   * overridden (in which case the session should be built with 4.x defaults).
   *
   * <p>When {@code lbPolicyToken} is non-null, the loader points {@code
   * basic.load-balancing-policy.class} at {@link Shim3xLoadBalancingPolicy}; when {@code
   * specexPolicyToken} is non-null it points {@code advanced.speculative-execution-policy.class} at
   * {@link Shim3xSpeculativeExecutionPolicy}. Each token is injected so the reflective instance can
   * recover the user's 3.x policy from the static registry.
   */
  public static DriverConfigLoader buildConfigLoader(
      SocketOptions socketOptions,
      PoolingOptions poolingOptions,
      QueryOptions queryOptions,
      ProtocolOptions.Compression compression,
      ProtocolVersion protocolVersion,
      String lbPolicyToken,
      String specexPolicyToken,
      boolean metricsEnabled) {

    ProgrammaticDriverConfigLoaderBuilder b = DriverConfigLoader.programmaticBuilder();
    boolean any = false;

    if (metricsEnabled) {
      // 3.x had metrics ON by default; enable an equivalent set on 4.x so the shim Metrics getters
      // have live data. The default metrics factory selects Dropwizard when it is on the classpath
      // (it is), exposing a com.codahale.metrics.MetricRegistry.
      b.withStringList(
          DefaultDriverOption.METRICS_SESSION_ENABLED,
          java.util.Arrays.asList(
              "bytes-sent", "bytes-received", "connected-nodes", "cql-requests",
              "cql-client-timeouts"));
      b.withStringList(
          DefaultDriverOption.METRICS_NODE_ENABLED,
          java.util.Arrays.asList(
              "pool.open-connections", "pool.in-flight", "bytes-sent", "bytes-received",
              "cql-messages", "errors.request.write-timeouts", "errors.request.read-timeouts",
              "errors.request.unavailables", "errors.request.others", "retries.total",
              "retries.read-timeout", "retries.write-timeout", "retries.unavailable",
              "retries.other", "ignores.total", "ignores.read-timeout", "ignores.write-timeout",
              "ignores.unavailable", "ignores.other", "speculative-executions",
              "errors.connection.init", "errors.connection.auth"));
      any = true;
    }

    if (lbPolicyToken != null) {
      b.withString(
          DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
          Shim3xLoadBalancingPolicy.class.getName());
      b.withString(Shim3xLoadBalancingPolicy.TOKEN_OPTION, lbPolicyToken);
      any = true;
    }

    if (specexPolicyToken != null) {
      b.withString(
          DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
          Shim3xSpeculativeExecutionPolicy.class.getName());
      b.withString(Shim3xSpeculativeExecutionPolicy.TOKEN_OPTION, specexPolicyToken);
      any = true;
    }

    if (protocolVersion != null) {
      b.withString(DefaultDriverOption.PROTOCOL_VERSION, protocolVersion.name());
      any = true;
    }

    if (compression != null && compression != ProtocolOptions.Compression.NONE) {
      // 4.x expects "snappy" / "lz4"; Compression.toString() yields exactly that label.
      b.withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression.toString());
      any = true;
    }

    if (socketOptions != null) {
      b.withDuration(
          DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
          Duration.ofMillis(Math.max(1, socketOptions.getConnectTimeoutMillis())));
      int readTimeout = socketOptions.getReadTimeoutMillis();
      if (readTimeout > 0) {
        b.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(readTimeout));
      }
      if (socketOptions.getKeepAlive() != null) {
        b.withBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE, socketOptions.getKeepAlive());
      }
      if (socketOptions.getReuseAddress() != null) {
        b.withBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, socketOptions.getReuseAddress());
      }
      if (socketOptions.getTcpNoDelay() != null) {
        b.withBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY, socketOptions.getTcpNoDelay());
      }
      if (socketOptions.getSoLinger() != null) {
        b.withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL, socketOptions.getSoLinger());
      }
      if (socketOptions.getReceiveBufferSize() != null) {
        b.withInt(
            DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE, socketOptions.getReceiveBufferSize());
      }
      if (socketOptions.getSendBufferSize() != null) {
        b.withInt(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE, socketOptions.getSendBufferSize());
      }
      any = true;
    }

    if (queryOptions != null) {
      ConsistencyLevel cl = queryOptions.getConsistencyLevel();
      if (cl != null) {
        b.withString(DefaultDriverOption.REQUEST_CONSISTENCY, cl.name());
      }
      ConsistencyLevel serial = queryOptions.getSerialConsistencyLevel();
      if (serial != null) {
        b.withString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY, serial.name());
      }
      b.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, queryOptions.getFetchSize());
      b.withBoolean(
          DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, queryOptions.getDefaultIdempotence());
      b.withBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES, queryOptions.isPrepareOnAllHosts());
      b.withBoolean(DefaultDriverOption.REPREPARE_ENABLED, queryOptions.isReprepareOnUp());
      b.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, queryOptions.isMetadataEnabled());
      // 3.x node/schema refresh debouncing -> 4.x metadata debouncer windows / max-events.
      b.withDuration(
          DefaultDriverOption.METADATA_TOPOLOGY_WINDOW,
          Duration.ofMillis(Math.max(0, queryOptions.getRefreshNodeIntervalMillis())));
      b.withInt(
          DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS,
          Math.max(1, queryOptions.getMaxPendingRefreshNodeRequests()));
      b.withDuration(
          DefaultDriverOption.METADATA_SCHEMA_WINDOW,
          Duration.ofMillis(Math.max(0, queryOptions.getRefreshSchemaIntervalMillis())));
      b.withInt(
          DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS,
          Math.max(1, queryOptions.getMaxPendingRefreshSchemaRequests()));
      any = true;
    }

    if (poolingOptions != null) {
      int localMax = poolingOptions.getMaxConnectionsPerHost(HostDistance.LOCAL);
      if (localMax != PoolingOptions.UNSET) {
        b.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, localMax);
      }
      int remoteMax = poolingOptions.getMaxConnectionsPerHost(HostDistance.REMOTE);
      if (remoteMax != PoolingOptions.UNSET) {
        b.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, remoteMax);
      }
      int maxReq = poolingOptions.getMaxRequestsPerConnection(HostDistance.LOCAL);
      if (maxReq != PoolingOptions.UNSET) {
        b.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, maxReq);
      }
      b.withDuration(
          DefaultDriverOption.HEARTBEAT_INTERVAL,
          Duration.ofSeconds(Math.max(1, poolingOptions.getHeartbeatIntervalSeconds())));
      any = true;
    }

    return any ? b.build() : null;
  }

  // ---- auth ----------------------------------------------------------------------------------

  /** Adapts a 3.x {@link com.datastax.driver.core.AuthProvider} to a 4.x {@link AuthProvider}. */
  public static AuthProvider toV4AuthProvider(com.datastax.driver.core.AuthProvider provider) {
    return new V4AuthProvider(provider);
  }

  private static final class V4AuthProvider implements AuthProvider {

    private final com.datastax.driver.core.AuthProvider delegate;

    V4AuthProvider(com.datastax.driver.core.AuthProvider delegate) {
      this.delegate = delegate;
    }

    @Override
    public Authenticator newAuthenticator(
        com.datastax.oss.driver.api.core.metadata.EndPoint endPoint, String serverAuthenticator) {
      InetSocketAddress address = (InetSocketAddress) endPoint.resolve();
      com.datastax.driver.core.Authenticator authenticator;
      if (delegate instanceof ExtendedAuthProvider) {
        authenticator =
            ((ExtendedAuthProvider) delegate)
                .newAuthenticator(new InetSocketAddressEndPoint(address), serverAuthenticator);
      } else {
        authenticator = delegate.newAuthenticator(address, serverAuthenticator);
      }
      return new V4Authenticator(authenticator);
    }

    @Override
    public void onMissingChallenge(com.datastax.oss.driver.api.core.metadata.EndPoint endPoint) {
      // 3.x has no equivalent callback; matching the 4.x default this is a no-op (the connection
      // proceeds without a challenge).
    }

    @Override
    public void close() {
      // 3.x AuthProvider has no lifecycle to release.
    }
  }

  private static final class V4Authenticator implements Authenticator {

    private final com.datastax.driver.core.Authenticator delegate;

    V4Authenticator(com.datastax.driver.core.Authenticator delegate) {
      this.delegate = delegate;
    }

    @Override
    public CompletionStage<ByteBuffer> initialResponse() {
      return CompletableFuture.completedFuture(wrap(delegate.initialResponse()));
    }

    @Override
    public CompletionStage<ByteBuffer> evaluateChallenge(ByteBuffer challenge) {
      return CompletableFuture.completedFuture(wrap(delegate.evaluateChallenge(toBytes(challenge))));
    }

    @Override
    public CompletionStage<Void> onAuthenticationSuccess(ByteBuffer token) {
      delegate.onAuthenticationSuccess(toBytes(token));
      return CompletableFuture.completedFuture(null);
    }

    private static ByteBuffer wrap(byte[] bytes) {
      return bytes == null ? null : ByteBuffer.wrap(bytes);
    }

    private static byte[] toBytes(ByteBuffer buffer) {
      if (buffer == null) {
        return null;
      }
      byte[] bytes = new byte[buffer.remaining()];
      buffer.duplicate().get(bytes);
      return bytes;
    }
  }

  // ---- SSL -----------------------------------------------------------------------------------

  /** Adapts a 3.x {@link SSLOptions} to a 4.x {@link SslEngineFactory}. */
  public static SslEngineFactory toV4SslEngineFactory(SSLOptions options) {
    return new V4SslEngineFactory(options);
  }

  private static final class V4SslEngineFactory implements SslEngineFactory {

    private final SSLOptions delegate;

    V4SslEngineFactory(SSLOptions delegate) {
      this.delegate = delegate;
    }

    @Override
    public SSLEngine newSslEngine(com.datastax.oss.driver.api.core.metadata.EndPoint remoteEndpoint) {
      SocketAddress resolved = remoteEndpoint.resolve();
      InetSocketAddress address =
          (resolved instanceof InetSocketAddress) ? (InetSocketAddress) resolved : null;
      // 3.x SSLOptions produce a Netty SslHandler; the JDK-based options ignore the channel, but the
      // Netty-based ones use channel.alloc(), so we hand them an unconnected NioSocketChannel and
      // extract the engine the handler wraps.
      NioSocketChannel channel = new NioSocketChannel();
      try {
        SslHandler handler;
        EndPoint endPoint = address == null ? null : new InetSocketAddressEndPoint(address);
        if (delegate instanceof ExtendedRemoteEndpointAwareSslOptions) {
          handler =
              ((ExtendedRemoteEndpointAwareSslOptions) delegate).newSSLHandler(channel, endPoint);
        } else if (delegate instanceof RemoteEndpointAwareSSLOptions) {
          handler = ((RemoteEndpointAwareSSLOptions) delegate).newSSLHandler(channel, address);
        } else {
          handler = delegate.newSSLHandler(channel);
        }
        return handler.engine();
      } finally {
        try {
          channel.close();
        } catch (RuntimeException ignored) {
          // best effort; the channel was never connected
        }
      }
    }

    @Override
    public void close() {
      // 3.x SSLOptions has no lifecycle to release.
    }
  }
}
