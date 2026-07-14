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

import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.shim.bridge.ConfigBridge;
import com.datastax.shim.bridge.ExceptionBridge;
import com.datastax.shim.bridge.FutureBridge;
import com.datastax.shim.bridge.InetSocketAddressEndPoint;
import com.datastax.shim.bridge.Shim3xLoadBalancingPolicy;
import com.datastax.shim.bridge.Shim3xSpeculativeExecutionPolicy;
import com.datastax.shim.bridge.ShimCqlSessionBuilder;
import com.datastax.shim.bridge.ShimLatencyRequestTracker;
import com.datastax.shim.bridge.ShimNodeStateListener;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

/**
 * Informations and known state of a Cassandra cluster.
 *
 * <p>Shim facade over a 4.x {@code CqlSession}. 3.x separates {@code Cluster} (config + lifecycle
 * owner) from {@code Session}; the shim keeps the accumulated builder state here and builds a 4.x
 * {@code CqlSession} on each {@code connect}/{@code init}.
 */
public class Cluster implements Closeable {

  private static final int DEFAULT_PORT = 9042;
  static final String DEFAULT_DATACENTER = "datacenter1";

  private final String clusterName;
  private final List<EndPoint> contactPoints;
  private final int port;
  private final String localDatacenter;
  private final String username;
  private final String password;
  private final File cloudBundleFile;
  private final URL cloudBundleUrl;
  private final InputStream cloudBundleStream;
  private final Collection<Host.StateListener> initialListeners;
  private Configuration configuration;

  // Option-bean state captured from the Builder, applied to the 4.x session via ConfigBridge and
  // surfaced through getConfiguration().
  private final ProtocolVersion protocolVersion;
  private final ProtocolOptions.Compression compression;
  private final PoolingOptions poolingOptions;
  private final SocketOptions socketOptions;
  private final QueryOptions queryOptions;
  private final ThreadingOptions threadingOptions;
  private final NettyOptions nettyOptions;
  private final CodecRegistry codecRegistry;
  private final AuthProvider authProvider;
  private final SSLOptions sslOptions;
  private final boolean ssl;

  // Policy graph captured from the Builder. Surfaced through getConfiguration().getPolicies() so
  // introspection matches 3.x (which always returns a populated Policies). NOTE: cluster-level
  // retry/reconnection/speculative/timestamp runtime translation to the 4.x SPI is not wired (it
  // remains inert, as documented); these are retained for faithful configuration introspection.
  private final LoadBalancingPolicy loadBalancingPolicy;
  private final ReconnectionPolicy reconnectionPolicy;
  private final RetryPolicy retryPolicy;
  private final AddressTranslator addressTranslator;
  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;
  private final TimestampGenerator timestampGenerator;
  private final EndPointFactory endPointFactory;

  // Registry token used to hand the user-supplied 3.x load-balancing policy to the reflectively
  // instantiated 4.x adapter (Shim3xLoadBalancingPolicy). Non-null iff a user policy was supplied.
  private final String lbPolicyToken;

  // Registry token for a delegated 3.x speculative-execution policy (Percentile or any custom,
  // non-Constant/non-No policy), handed to Shim3xSpeculativeExecutionPolicy. Non-null iff delegated.
  private final String specexPolicyToken;

  // Live list of user-registered LatencyTrackers, fed by a build-time 4.x RequestTracker
  // (ShimLatencyRequestTracker) installed on every session built by this Cluster.
  private final List<LatencyTracker> latencyTrackers = new CopyOnWriteArrayList<LatencyTracker>();

  // Live lists of user-registered node-state / schema-change listeners, fed by build-time 4.x
  // NodeStateListener / SchemaChangeListener adapters installed on every session.
  private final List<Host.StateListener> hostStateListeners =
      new CopyOnWriteArrayList<Host.StateListener>();
  final List<SchemaChangeListener> schemaChangeListeners =
      new CopyOnWriteArrayList<SchemaChangeListener>();

  private final List<CqlSession> openSessions = new ArrayList<CqlSession>();
  private CqlSession controlSession;
  private volatile boolean closed;
  private Metrics metrics;

  // 3.x had metrics + JMX reporting ON by default; toggled off by withoutMetrics()/withoutJMXReporting().
  private final boolean metricsEnabled;
  private final boolean jmxEnabled;
  private com.codahale.metrics.jmx.JmxReporter jmxReporter;
  private static final java.util.concurrent.atomic.AtomicInteger JMX_DOMAIN_SEQ =
      new java.util.concurrent.atomic.AtomicInteger();

  /**
   * Constructs a new Cluster instance.
   *
   * <p>Tolerates a {@code null} configuration and never opens resources (used by {@link
   * DelegatingCluster}, which passes {@code null}).
   */
  protected Cluster(String name, List<EndPoint> contactPoints, Configuration configuration) {
    this.clusterName = name;
    this.contactPoints =
        contactPoints == null
            ? new ArrayList<EndPoint>()
            : new ArrayList<EndPoint>(contactPoints);
    this.port = DEFAULT_PORT;
    this.localDatacenter = null;
    this.username = null;
    this.password = null;
    this.cloudBundleFile = null;
    this.cloudBundleUrl = null;
    this.cloudBundleStream = null;
    this.initialListeners = Collections.emptyList();
    this.configuration = configuration;
    this.protocolVersion = null;
    this.compression = null;
    this.poolingOptions = null;
    this.socketOptions = null;
    this.queryOptions = null;
    this.threadingOptions = null;
    this.nettyOptions = null;
    this.codecRegistry = null;
    this.authProvider = null;
    this.sslOptions = null;
    this.ssl = false;
    this.loadBalancingPolicy = null;
    this.reconnectionPolicy = null;
    this.retryPolicy = null;
    this.addressTranslator = null;
    this.speculativeExecutionPolicy = null;
    this.timestampGenerator = null;
    this.endPointFactory = null;
    this.lbPolicyToken = null;
    this.specexPolicyToken = null;
    this.metricsEnabled = true;
    this.jmxEnabled = true;
  }

  /** Constructs a new Cluster from an {@link Initializer}. */
  protected Cluster(Initializer initializer) {
    this.clusterName = initializer.getClusterName();
    List<EndPoint> cps = initializer.getContactPoints();
    this.contactPoints =
        cps == null ? new ArrayList<EndPoint>() : new ArrayList<EndPoint>(cps);
    Collection<Host.StateListener> listeners = initializer.getInitialListeners();
    this.initialListeners =
        listeners == null ? Collections.<Host.StateListener>emptyList() : listeners;
    this.configuration = initializer.getConfiguration();
    if (initializer instanceof Builder) {
      Builder b = (Builder) initializer;
      this.port = b.port;
      this.localDatacenter = b.localDatacenter;
      this.username = b.username;
      this.password = b.password;
      this.cloudBundleFile = b.cloudBundleFile;
      this.cloudBundleUrl = b.cloudBundleUrl;
      this.cloudBundleStream = b.cloudBundleStream;
      this.protocolVersion = b.protocolVersion;
      this.compression = b.compression;
      this.poolingOptions = b.poolingOptions;
      this.socketOptions = b.socketOptions;
      this.queryOptions = b.queryOptions;
      this.threadingOptions = b.threadingOptions;
      this.nettyOptions = b.nettyOptions;
      this.codecRegistry = b.codecRegistry;
      this.authProvider = b.authProvider;
      this.sslOptions = b.sslOptions;
      this.ssl = b.ssl;
      this.loadBalancingPolicy = b.loadBalancingPolicy;
      this.reconnectionPolicy = b.reconnectionPolicy;
      this.retryPolicy = b.retryPolicy;
      this.addressTranslator = b.addressTranslator;
      this.speculativeExecutionPolicy = b.speculativeExecutionPolicy;
      this.timestampGenerator = b.timestampGenerator;
      this.endPointFactory = b.endPointFactory;
      this.metricsEnabled = b.metricsEnabled;
      this.jmxEnabled = b.jmxEnabled;
    } else {
      this.port = DEFAULT_PORT;
      this.localDatacenter = null;
      this.username = null;
      this.password = null;
      this.cloudBundleFile = null;
      this.cloudBundleUrl = null;
      this.cloudBundleStream = null;
      this.protocolVersion = null;
      this.compression = null;
      this.poolingOptions = null;
      this.socketOptions = null;
      this.queryOptions = null;
      this.threadingOptions = null;
      this.nettyOptions = null;
      this.codecRegistry = null;
      this.authProvider = null;
      this.sslOptions = null;
      this.ssl = false;
      this.loadBalancingPolicy = null;
      this.reconnectionPolicy = null;
      this.retryPolicy = null;
      this.addressTranslator = null;
      this.speculativeExecutionPolicy = null;
      this.timestampGenerator = null;
    this.endPointFactory = null;
      this.metricsEnabled = true;
      this.jmxEnabled = true;
    }
    // Honor an arbitrary user-supplied 3.x load-balancing policy by delegating to it through the
    // 4.x SPI: register it under a fresh token that newCqlSessionBuilder() injects into the
    // programmatic config (Shim3xLoadBalancingPolicy reads the token back and recovers the policy).
    if (this.loadBalancingPolicy != null) {
      String token = "shim-lbp-" + java.util.UUID.randomUUID();
      Shim3xLoadBalancingPolicy.register(token, this, this.loadBalancingPolicy);
      this.lbPolicyToken = token;
    } else {
      this.lbPolicyToken = null;
    }
    // Delegate a Percentile or any custom (non-Constant/non-No) 3.x speculative-execution policy to
    // the 4.x SPI via the adapter; Constant/No keep their existing (inert) translation.
    if (this.speculativeExecutionPolicy != null
        && !(this.speculativeExecutionPolicy instanceof ConstantSpeculativeExecutionPolicy)
        && !(this.speculativeExecutionPolicy instanceof NoSpeculativeExecutionPolicy)) {
      String token = "shim-specex-" + java.util.UUID.randomUUID();
      Shim3xSpeculativeExecutionPolicy.register(token, this, this.speculativeExecutionPolicy);
      this.specexPolicyToken = token;
    } else {
      this.specexPolicyToken = null;
    }
    // Honor withInitialListeners(...): register each at build time and fire onRegister (3.x parity).
    for (Host.StateListener listener : this.initialListeners) {
      if (listener != null && !hostStateListeners.contains(listener)) {
        hostStateListeners.add(listener);
        listener.onRegister(this);
      }
    }
  }

  // ---- static factories ----------------------------------------------------------------------

  /** Creates a {@link Builder} to configure and build a {@code Cluster} instance. */
  public static Builder builder() {
    return new Builder();
  }

  /** Creates a {@code Cluster} from the provided {@link Initializer}. */
  public static Cluster buildFrom(Initializer initializer) {
    return new Cluster(initializer);
  }

  /** Returns the version of the underlying driver. */
  public static String getDriverVersion() {
    return com.datastax.oss.driver.api.core.session.Session.OSS_DRIVER_COORDINATES
        .getVersion()
        .toString();
  }

  /** Logs the driver version. */
  public static void logDriverVersion() {
    // No-op / signature match: 4.x has no equivalent hook.
  }

  // ---- lifecycle -----------------------------------------------------------------------------

  private CqlSessionBuilder newCqlSessionBuilder() {
    // All shim sessions build through ShimCqlSessionBuilder / ShimDriverContext so 3.x
    // policy/option beans (Netty, threading, timestamp, reconnection, and per-request/cluster retry)
    // are bridged onto 4.x. Each bridged option falls back to stock 4.x behavior when it is unset,
    // so an unconfigured cluster behaves exactly as before; per-request retry needs this universally.
    CqlSessionBuilder b =
        new ShimCqlSessionBuilder(
            this,
            nettyOptions,
            threadingOptions,
            timestampGenerator,
            reconnectionPolicy,
            retryPolicy,
            endPointFactory);
    if (cloudBundleFile != null) {
      b.withCloudSecureConnectBundle(cloudBundleFile.toPath());
    } else if (cloudBundleUrl != null) {
      b.withCloudSecureConnectBundle(cloudBundleUrl);
    } else if (cloudBundleStream != null) {
      b.withCloudSecureConnectBundle(cloudBundleStream);
    } else {
      List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
      for (EndPoint ep : contactPoints) {
        addrs.add(ep.resolve());
      }
      if (!addrs.isEmpty()) {
        b.addContactPoints(addrs);
      }
      b.withLocalDatacenter(localDatacenter != null ? localDatacenter : DEFAULT_DATACENTER);
    }
    if (authProvider != null) {
      b.withAuthProvider(ConfigBridge.toV4AuthProvider(authProvider));
    } else if (username != null) {
      b.withAuthCredentials(username, password);
    }
    if (sslOptions != null) {
      b.withSslEngineFactory(ConfigBridge.toV4SslEngineFactory(sslOptions));
    } else if (ssl) {
      // withSSL() with no options: 3.x uses a JDK context with default (JSSE system property)
      // settings; reproduce that by wrapping the default JdkSSLOptions.
      b.withSslEngineFactory(
          ConfigBridge.toV4SslEngineFactory(RemoteEndpointAwareJdkSSLOptions.builder().build()));
    }
    DriverConfigLoader loader =
        ConfigBridge.buildConfigLoader(
            socketOptions,
            poolingOptions,
            queryOptions,
            compression,
            protocolVersion,
            lbPolicyToken,
            specexPolicyToken,
            metricsEnabled);
    if (loader != null) {
      b.withConfigLoader(loader);
    }
    // Feed user-registered LatencyTrackers (and thus PercentileTracker-backed speculative execution)
    // from the 4.x per-node request callbacks. Installed unconditionally so trackers registered
    // after connect() are still fed; the fan-out is a no-op while no tracker is registered.
    b.withRequestTracker(new ShimLatencyRequestTracker(latencyTrackers));
    // Fan node-state and schema-change events out to user-registered 3.x listeners (no-op while
    // none are registered).
    b.withNodeStateListener(new ShimNodeStateListener(hostStateListeners));
    b.withSchemaChangeListener(new ShimSchemaChangeAdapter(this));
    return b;
  }

  private synchronized CqlSession registerSession(CqlSession s) {
    openSessions.add(s);
    return s;
  }

  /** Initializes this Cluster (builds the underlying control session). */
  public Cluster init() {
    ensureControl();
    return this;
  }

  private synchronized CqlSession ensureControl() {
    checkNotClosed();
    if (controlSession == null) {
      if (!openSessions.isEmpty()) {
        controlSession = openSessions.get(0);
      } else {
        try {
          controlSession = newCqlSessionBuilder().build();
          openSessions.add(controlSession);
        } catch (RuntimeException e) {
          throw ExceptionBridge.toV3(e);
        }
      }
    }
    return controlSession;
  }

  /** Creates a new session on this cluster (eagerly built by the shim). */
  public Session newSession() {
    return connect();
  }

  /** Creates a new session on this cluster and initializes it. */
  public Session connect() {
    checkNotClosed();
    try {
      return new ShimSession(this, registerSession(newCqlSessionBuilder().build()));
    } catch (RuntimeException e) {
      throw ExceptionBridge.toV3(e);
    }
  }

  /** Creates a new session, initializes it and sets the keyspace to {@code keyspace}. */
  public Session connect(String keyspace) {
    checkNotClosed();
    try {
      return new ShimSession(
          this, registerSession(newCqlSessionBuilder().withKeyspace(keyspace).build()));
    } catch (RuntimeException e) {
      throw ExceptionBridge.toV3(e);
    }
  }

  /** Creates a new session on this cluster and initializes it asynchronously. */
  public ListenableFuture<Session> connectAsync() {
    return connectAsync(null);
  }

  /** Creates a new session, initializes it and sets the keyspace, asynchronously. */
  public ListenableFuture<Session> connectAsync(String keyspace) {
    checkNotClosed();
    CompletionStage<CqlSession> stage;
    try {
      CqlSessionBuilder b = newCqlSessionBuilder();
      if (keyspace != null) {
        b.withKeyspace(keyspace);
      }
      stage = b.buildAsync();
    } catch (RuntimeException e) {
      throw ExceptionBridge.toV3(e);
    }
    CompletionStage<Session> mapped =
        stage.handle(
            (cql, error) -> {
              if (error != null) {
                throw ExceptionBridge.toV3(error);
              }
              return (Session) new ShimSession(this, registerSession(cql));
            });
    return FutureBridge.toListenableFuture(mapped);
  }

  // ---- accessors -----------------------------------------------------------------------------

  /** The name of this cluster (best-effort; see class notes). */
  public String getClusterName() {
    if (clusterName != null) {
      return clusterName;
    }
    return ensureControl().getName();
  }

  /** Returns read-only metadata on the connected cluster. */
  public Metadata getMetadata() {
    CqlSession s = ensureControl();
    return new Metadata(s.getMetadata(), s);
  }

  /** The cluster configuration. */
  public Configuration getConfiguration() {
    if (configuration != null && configuration.getProtocolOptions() != null) {
      return configuration;
    }
    ProtocolOptions protocolOptions =
        new ProtocolOptions(
            port,
            protocolVersion,
            ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS,
            sslOptions,
            authProvider);
    if (compression != null) {
      protocolOptions.setCompression(compression);
    }
    protocolOptions.shimCluster = this;
    configuration =
        Configuration.builder()
            .withPolicies(
                buildPolicies(
                    loadBalancingPolicy,
                    reconnectionPolicy,
                    retryPolicy,
                    addressTranslator,
                    speculativeExecutionPolicy,
                    timestampGenerator))
            .withProtocolOptions(protocolOptions)
            .withPoolingOptions(poolingOptions != null ? poolingOptions : new PoolingOptions())
            .withSocketOptions(socketOptions != null ? socketOptions : new SocketOptions())
            .withQueryOptions(queryOptions != null ? queryOptions : new QueryOptions())
            .withMetricsOptions(new MetricsOptions(metricsEnabled, jmxEnabled))
            .withThreadingOptions(
                threadingOptions != null ? threadingOptions : new ThreadingOptions())
            .withNettyOptions(nettyOptions != null ? nettyOptions : NettyOptions.DEFAULT_INSTANCE)
            .withCodecRegistry(codecRegistry != null ? codecRegistry : CodecRegistry.DEFAULT_INSTANCE)
            .build();
    return configuration;
  }

  /**
   * Builds a {@link com.datastax.driver.core.policies.Policies} from the user-configured policies,
   * filling any unset slot with the same defaults 3.x uses, so {@code getConfiguration().getPolicies()}
   * is never null and returns the caller's policies where supplied (matching 3.12.1's
   * {@code Builder.getConfiguration()} which always calls {@code withPolicies(...)}).
   */
  private static com.datastax.driver.core.policies.Policies buildPolicies(
      LoadBalancingPolicy loadBalancingPolicy,
      ReconnectionPolicy reconnectionPolicy,
      RetryPolicy retryPolicy,
      AddressTranslator addressTranslator,
      SpeculativeExecutionPolicy speculativeExecutionPolicy,
      TimestampGenerator timestampGenerator) {
    com.datastax.driver.core.policies.Policies.Builder b =
        com.datastax.driver.core.policies.Policies.builder();
    if (loadBalancingPolicy != null) {
      b.withLoadBalancingPolicy(loadBalancingPolicy);
    }
    if (reconnectionPolicy != null) {
      b.withReconnectionPolicy(reconnectionPolicy);
    }
    if (retryPolicy != null) {
      b.withRetryPolicy(retryPolicy);
    }
    if (addressTranslator != null) {
      b.withAddressTranslator(addressTranslator);
    }
    if (speculativeExecutionPolicy != null) {
      b.withSpeculativeExecutionPolicy(speculativeExecutionPolicy);
    }
    if (timestampGenerator != null) {
      b.withTimestampGenerator(timestampGenerator);
    }
    return b.build();
  }

  /**
   * The cluster metrics, backed by the live 4.x Dropwizard registry of the connected session, or
   * {@code null} if metrics were disabled via {@link Builder#withoutMetrics()} (3.x semantics).
   */
  public Metrics getMetrics() {
    if (!metricsEnabled) {
      return null;
    }
    synchronized (this) {
      if (metrics == null) {
        CqlSession session = ensureControl();
        metrics = new Metrics(this, session);
        maybeStartJmx(session);
      }
      return metrics;
    }
  }

  /**
   * Attaches a Dropwizard {@link com.codahale.metrics.jmx.JmxReporter} to the session's metric
   * registry (once) when JMX reporting is enabled. 4.x has no built-in JMX reporter, so the shim
   * publishes MBeans itself, matching 3.x (which reported over JMX by default). A per-cluster domain
   * avoids MBean name collisions between the multiple sessions/clusters an app may open.
   */
  private void maybeStartJmx(CqlSession session) {
    if (!jmxEnabled || jmxReporter != null) {
      return;
    }
    try {
      com.datastax.oss.driver.api.core.metrics.Metrics v4 = session.getMetrics().orElse(null);
      if (v4 == null) {
        return;
      }
      com.codahale.metrics.jmx.JmxReporter reporter =
          com.codahale.metrics.jmx.JmxReporter.forRegistry(v4.getRegistry())
              .inDomain("com.datastax.shim.metrics-" + JMX_DOMAIN_SEQ.incrementAndGet())
              .build();
      reporter.start();
      jmxReporter = reporter;
    } catch (RuntimeException e) {
      // JMX publication is best-effort; never fail the caller over it.
    }
  }

  /**
   * Returns the native-protocol version negotiated by the underlying 4.x session, or {@code null}
   * if no session has connected yet. Used by {@link ProtocolOptions#getProtocolVersion()}.
   */
  ProtocolVersion negotiatedProtocolVersion() {
    CqlSession session;
    synchronized (this) {
      if (controlSession != null) {
        session = controlSession;
      } else if (!openSessions.isEmpty()) {
        session = openSessions.get(0);
      } else {
        session = null;
      }
    }
    if (session == null) {
      return null;
    }
    try {
      return ProtocolVersion.fromInt(session.getContext().getProtocolVersion().getCode());
    } catch (RuntimeException e) {
      return null;
    }
  }

  public Cluster register(Host.StateListener listener) {
    if (listener != null && !hostStateListeners.contains(listener)) {
      hostStateListeners.add(listener);
      listener.onRegister(this);
    }
    return this;
  }

  public Cluster unregister(Host.StateListener listener) {
    if (listener != null && hostStateListeners.remove(listener)) {
      listener.onUnregister(this);
    }
    return this;
  }

  public Cluster register(LatencyTracker tracker) {
    if (tracker != null && !latencyTrackers.contains(tracker)) {
      latencyTrackers.add(tracker);
      tracker.onRegister(this);
    }
    return this;
  }

  public Cluster unregister(LatencyTracker tracker) {
    if (tracker != null && latencyTrackers.remove(tracker)) {
      tracker.onUnregister(this);
    }
    return this;
  }

  public Cluster register(SchemaChangeListener listener) {
    if (listener != null && !schemaChangeListeners.contains(listener)) {
      schemaChangeListeners.add(listener);
      listener.onRegister(this);
    }
    return this;
  }

  public Cluster unregister(SchemaChangeListener listener) {
    if (listener != null && schemaChangeListeners.remove(listener)) {
      listener.onUnregister(this);
    }
    return this;
  }

  /** Initiates a shutdown of this cluster instance. */
  public CloseFuture closeAsync() {
    List<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
    List<CqlSession> owners = new ArrayList<CqlSession>();
    synchronized (this) {
      closed = true;
      for (CqlSession s : openSessions) {
        owners.add(s);
        futures.add(s.closeAsync().toCompletableFuture());
      }
      // Stop the JMX reporter (unregisters its MBeans); listener-based, so no thread to join.
      if (jmxReporter != null) {
        try {
          jmxReporter.stop();
        } catch (RuntimeException ignored) {
          // best effort
        }
        jmxReporter = null;
      }
    }
    // Drop the load-balancing delegation registration (if any); the underlying 4.x sessions have
    // been asked to close, so the adapter's close() (which forwards to the 3.x policy) has run.
    Shim3xLoadBalancingPolicy.deregister(lbPolicyToken);
    Shim3xSpeculativeExecutionPolicy.deregister(specexPolicyToken);
    if (futures.isEmpty()) {
      return ShimCloseFuture.immediate();
    }
    CompletableFuture<Void> all =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    // 3.x ClusterCloseFuture.force() forwards force() to every child session close future; pass the
    // open sessions so force() can call forceCloseAsync() on each and complete promptly.
    return new ShimCloseFuture(all, owners);
  }

  /** Closes this cluster instance, blocking until shutdown completes. */
  @Override
  public void close() {
    try {
      Uninterruptibles.getUninterruptibly(closeAsync());
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause == null ? e : cause);
    }
  }

  /** Whether this cluster instance has been closed. */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Matches 3.x {@code Manager.checkNotClosed()}: throws synchronously if this cluster was already
   * closed, so {@code connect()}/{@code init()} on a closed cluster fails fast instead of building a
   * new (leaked) session.
   */
  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException(
          "Can't use this cluster instance because it was previously closed");
    }
  }

  // ============================================================================================
  //  Initializer
  // ============================================================================================

  /** An interface providing the initialization data required to build a {@link Cluster}. */
  public interface Initializer {

    String getClusterName();

    List<EndPoint> getContactPoints();

    Configuration getConfiguration();

    Collection<Host.StateListener> getInitialListeners();
  }

  // ============================================================================================
  //  Builder
  // ============================================================================================

  /** Helper class to build a {@link Cluster} instance. */
  public static class Builder implements Initializer {

    private String clusterName;
    private final List<EndPoint> contactPoints = new ArrayList<EndPoint>();
    // Raw contact points, resolved to endpoints at build time (getContactPoints) so a withPort()
    // called after addContactPoint applies, matching 3.12.1. rawHostContactPoints get the builder's
    // final port; rawHostAndPortContactPoints carry their own explicit port.
    private final List<InetAddress> rawHostContactPoints = new ArrayList<InetAddress>();
    private final List<InetSocketAddress> rawHostAndPortContactPoints =
        new ArrayList<InetSocketAddress>();
    int port = DEFAULT_PORT;
    String localDatacenter;
    String username;
    String password;
    File cloudBundleFile;
    URL cloudBundleUrl;
    InputStream cloudBundleStream;
    private final Collection<Host.StateListener> listeners = new ArrayList<Host.StateListener>();

    // stored option beans (for getConfiguration())
    private LoadBalancingPolicy loadBalancingPolicy;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;
    private AddressTranslator addressTranslator;
    private SpeculativeExecutionPolicy speculativeExecutionPolicy;
    private TimestampGenerator timestampGenerator;
    private EndPointFactory endPointFactory;
    private boolean metricsEnabled = true;
    private boolean jmxEnabled = true;
    private CodecRegistry codecRegistry;
    private AuthProvider authProvider;
    private SSLOptions sslOptions;
    private boolean ssl;
    private ProtocolVersion protocolVersion;
    private ProtocolOptions.Compression compression;
    private PoolingOptions poolingOptions;
    private SocketOptions socketOptions;
    private QueryOptions queryOptions;
    private ThreadingOptions threadingOptions;
    private NettyOptions nettyOptions;

    public Builder() {}

    @Override
    public String getClusterName() {
      return clusterName;
    }

    @Override
    public List<EndPoint> getContactPoints() {
      // Resolve raw hosts to endpoints at build time, applying the final port. Use a set to dedupe
      // (mirrors 3.12.1 Builder.getContactPoints).
      java.util.Set<EndPoint> all = new java.util.LinkedHashSet<EndPoint>(contactPoints);
      for (InetAddress address : rawHostContactPoints) {
        all.add(new InetSocketAddressEndPoint(new InetSocketAddress(address, port)));
      }
      for (InetSocketAddress socketAddress : rawHostAndPortContactPoints) {
        all.add(new InetSocketAddressEndPoint(socketAddress));
      }
      return new ArrayList<EndPoint>(all);
    }

    public Builder withClusterName(String name) {
      this.clusterName = name;
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      return this;
    }

    public Builder allowBetaProtocolVersion() {
      return this;
    }

    public Builder withMaxSchemaAgreementWaitSeconds(int maxSchemaAgreementWaitSeconds) {
      return this;
    }

    public Builder withProtocolVersion(ProtocolVersion version) {
      this.protocolVersion = version;
      return this;
    }

    public Builder addContactPoint(String address) {
      // Mirror 3.12.1: eagerly resolve the host (expanding multi-A-record hostnames) but defer the
      // port to build time, so a later withPort() is honored. The whole string is treated as a
      // hostname (no host:port parsing), as in 3.x.
      if (address == null) {
        throw new NullPointerException();
      }
      try {
        InetAddress[] allByName = InetAddress.getAllByName(address);
        java.util.Collections.addAll(rawHostContactPoints, allByName);
      } catch (java.net.UnknownHostException e) {
        throw new IllegalArgumentException("Failed to add contact point: " + address, e);
      }
      return this;
    }

    public Builder addContactPoint(EndPoint contactPoint) {
      contactPoints.add(contactPoint);
      return this;
    }

    public Builder addContactPoints(String... addresses) {
      for (String address : addresses) {
        addContactPoint(address);
      }
      return this;
    }

    public Builder addContactPoints(InetAddress... addresses) {
      java.util.Collections.addAll(rawHostContactPoints, addresses);
      return this;
    }

    public Builder addContactPoints(Collection<InetAddress> addresses) {
      rawHostContactPoints.addAll(addresses);
      return this;
    }

    public Builder addContactPointsWithPorts(InetSocketAddress... addresses) {
      java.util.Collections.addAll(rawHostAndPortContactPoints, addresses);
      return this;
    }

    public Builder addContactPointsWithPorts(Collection<InetSocketAddress> addresses) {
      rawHostAndPortContactPoints.addAll(addresses);
      return this;
    }

    public Builder withLoadBalancingPolicy(LoadBalancingPolicy policy) {
      this.loadBalancingPolicy = policy;
      // Unwrap the standard wrapper chain (TokenAware/HostFilter/LatencyAware/ErrorAware all
      // implement ChainableLoadBalancingPolicy) down to the routing child, so a DCAware nested
      // inside e.g. TokenAware still yields its local DC (matching ADAPTERS section 8). Without this
      // the local DC would be lost and the 4.x session would fall back to "datacenter1".
      LoadBalancingPolicy current = policy;
      while (current != null) {
        if (current instanceof DCAwareRoundRobinPolicy) {
          String dc = ((DCAwareRoundRobinPolicy) current).getLocalDc();
          if (dc != null) {
            this.localDatacenter = dc;
          }
          break;
        }
        if (current
            instanceof com.datastax.driver.core.policies.ChainableLoadBalancingPolicy) {
          current =
              ((com.datastax.driver.core.policies.ChainableLoadBalancingPolicy) current)
                  .getChildPolicy();
        } else {
          break;
        }
      }
      return this;
    }

    public Builder withReconnectionPolicy(ReconnectionPolicy policy) {
      this.reconnectionPolicy = policy;
      return this;
    }

    public Builder withRetryPolicy(RetryPolicy policy) {
      this.retryPolicy = policy;
      return this;
    }

    public Builder withAddressTranslator(AddressTranslator translator) {
      this.addressTranslator = translator;
      return this;
    }

    public Builder withTimestampGenerator(TimestampGenerator timestampGenerator) {
      this.timestampGenerator = timestampGenerator;
      return this;
    }

    public Builder withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy policy) {
      this.speculativeExecutionPolicy = policy;
      return this;
    }

    public Builder withEndPointFactory(EndPointFactory endPointFactory) {
      this.endPointFactory = endPointFactory;
      return this;
    }

    public Builder withCodecRegistry(CodecRegistry codecRegistry) {
      this.codecRegistry = codecRegistry;
      return this;
    }

    public Builder withCredentials(String username, String password) {
      this.username = username;
      this.password = password;
      return this;
    }

    public Builder withAuthProvider(AuthProvider authProvider) {
      this.authProvider = authProvider;
      return this;
    }

    public Builder withCompression(ProtocolOptions.Compression compression) {
      this.compression = compression;
      return this;
    }

    public Builder withoutMetrics() {
      this.metricsEnabled = false;
      this.jmxEnabled = false;
      return this;
    }

    public Builder withSSL() {
      this.ssl = true;
      return this;
    }

    public Builder withSSL(SSLOptions sslOptions) {
      this.ssl = true;
      this.sslOptions = sslOptions;
      return this;
    }

    public Builder withInitialListeners(Collection<Host.StateListener> listeners) {
      this.listeners.addAll(listeners);
      return this;
    }

    public Builder withoutJMXReporting() {
      this.jmxEnabled = false;
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

    public Builder withNoCompact() {
      return this;
    }

    public Builder withCloudSecureConnectBundle(File cloudConfig) {
      this.cloudBundleFile = cloudConfig;
      return this;
    }

    public Builder withCloudSecureConnectBundle(URL cloudConfig) {
      this.cloudBundleUrl = cloudConfig;
      return this;
    }

    public Builder withCloudSecureConnectBundle(InputStream cloudConfig) {
      this.cloudBundleStream = cloudConfig;
      return this;
    }

    @Override
    public Configuration getConfiguration() {
      return Configuration.builder()
          .withPolicies(
              buildPolicies(
                  loadBalancingPolicy,
                  reconnectionPolicy,
                  retryPolicy,
                  addressTranslator,
                  speculativeExecutionPolicy,
                  timestampGenerator))
          .withPoolingOptions(poolingOptions)
          .withSocketOptions(socketOptions)
          .withQueryOptions(queryOptions)
          .withThreadingOptions(threadingOptions)
          .withNettyOptions(nettyOptions)
          .withCodecRegistry(codecRegistry)
          .build();
    }

    @Override
    public Collection<Host.StateListener> getInitialListeners() {
      return listeners;
    }

    /** Builds the cluster with the configured set of initial contact points and policies. */
    public Cluster build() {
      return Cluster.buildFrom(this);
    }
  }
}
