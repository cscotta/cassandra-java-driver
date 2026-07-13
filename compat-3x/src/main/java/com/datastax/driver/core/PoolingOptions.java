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
package com.datastax.driver.core;

import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.HostDistance.REMOTE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options related to connection pooling. Shim port of the 3.12.1 value bean.
 *
 * <p>The state round-trips through the getters/setters. Several members map only partially (or not
 * at all) onto 4.x, whose pools have a fixed size and no idle/borrow-timeout concept; see {@link
 * com.datastax.shim.bridge.ConfigBridge} for the mapping actually applied to a {@code CqlSession}.
 */
public class PoolingOptions {

  private static final Logger logger = LoggerFactory.getLogger(PoolingOptions.class);
  // Warn once per method so a caller in a loop does not flood the logs.
  private static final AtomicBoolean REFRESH_ALL_WARNED = new AtomicBoolean();
  private static final AtomicBoolean REFRESH_ONE_WARNED = new AtomicBoolean();
  // Knobs with no 4.x equivalent: the value round-trips via the getter, but is inert at runtime; each
  // setter logs a WARN once (guarded here) to explain the no-op.
  private static final AtomicBoolean NEW_CONN_THRESHOLD_WARNED = new AtomicBoolean();
  private static final AtomicBoolean IDLE_TIMEOUT_WARNED = new AtomicBoolean();
  private static final AtomicBoolean POOL_TIMEOUT_WARNED = new AtomicBoolean();
  private static final AtomicBoolean MAX_QUEUE_SIZE_WARNED = new AtomicBoolean();
  private static final AtomicBoolean INIT_EXECUTOR_WARNED = new AtomicBoolean();

  /**
   * The value returned for connection options when they have not been set by the client, and the
   * protocol version is not known yet.
   */
  public static final int UNSET = Integer.MIN_VALUE;

  public static final String CORE_POOL_LOCAL_KEY = "corePoolLocal";
  public static final String MAX_POOL_LOCAL_KEY = "maxPoolLocal";
  public static final String CORE_POOL_REMOTE_KEY = "corePoolRemote";
  public static final String MAX_POOL_REMOTE_KEY = "maxPoolRemote";
  public static final String NEW_CONNECTION_THRESHOLD_LOCAL_KEY = "newConnectionThresholdLocal";
  public static final String NEW_CONNECTION_THRESHOLD_REMOTE_KEY = "newConnectionThresholdRemote";
  public static final String MAX_REQUESTS_PER_CONNECTION_LOCAL_KEY =
      "maxRequestsPerConnectionLocal";
  public static final String MAX_REQUESTS_PER_CONNECTION_REMOTE_KEY =
      "maxRequestsPerConnectionRemote";

  /** The default values for connection options, that depend on the native protocol version. */
  public static final Map<ProtocolVersion, Map<String, Integer>> DEFAULTS =
      ImmutableMap.<ProtocolVersion, Map<String, Integer>>of(
          ProtocolVersion.V1,
              ImmutableMap.<String, Integer>builder()
                  .put(CORE_POOL_LOCAL_KEY, 2)
                  .put(MAX_POOL_LOCAL_KEY, 8)
                  .put(CORE_POOL_REMOTE_KEY, 1)
                  .put(MAX_POOL_REMOTE_KEY, 2)
                  .put(NEW_CONNECTION_THRESHOLD_LOCAL_KEY, 100)
                  .put(NEW_CONNECTION_THRESHOLD_REMOTE_KEY, 100)
                  .put(MAX_REQUESTS_PER_CONNECTION_LOCAL_KEY, 128)
                  .put(MAX_REQUESTS_PER_CONNECTION_REMOTE_KEY, 128)
                  .build(),
          ProtocolVersion.V3,
              ImmutableMap.<String, Integer>builder()
                  .put(CORE_POOL_LOCAL_KEY, 1)
                  .put(MAX_POOL_LOCAL_KEY, 1)
                  .put(CORE_POOL_REMOTE_KEY, 1)
                  .put(MAX_POOL_REMOTE_KEY, 1)
                  .put(NEW_CONNECTION_THRESHOLD_LOCAL_KEY, 800)
                  .put(NEW_CONNECTION_THRESHOLD_REMOTE_KEY, 200)
                  .put(MAX_REQUESTS_PER_CONNECTION_LOCAL_KEY, 1024)
                  .put(MAX_REQUESTS_PER_CONNECTION_REMOTE_KEY, 256)
                  .build());

  /** The default value for {@link #getIdleTimeoutSeconds()} ({@value}). */
  public static final int DEFAULT_IDLE_TIMEOUT_SECONDS = 120;

  /** The default value for {@link #getPoolTimeoutMillis()} ({@value}). */
  public static final int DEFAULT_POOL_TIMEOUT_MILLIS = 5000;

  /** The default value for {@link #getMaxQueueSize()} ({@value}). */
  public static final int DEFAULT_MAX_QUEUE_SIZE = 256;

  /** The default value for {@link #getHeartbeatIntervalSeconds()} ({@value}). */
  public static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30;

  // Upper bounds on requests per connection, per protocol version (from the 3.x StreamIdGenerator).
  private static final int MAX_STREAM_PER_CONNECTION_V2 = 128;
  private static final int MAX_STREAM_PER_CONNECTION_V3 = 32768;

  private static final Executor DEFAULT_INITIALIZATION_EXECUTOR = MoreExecutors.directExecutor();

  private volatile ProtocolVersion protocolVersion;

  private final int[] coreConnections = new int[] {UNSET, UNSET, 0};
  private final int[] maxConnections = new int[] {UNSET, UNSET, 0};
  private final int[] newConnectionThreshold = new int[] {UNSET, UNSET, 0};
  private volatile int maxRequestsPerConnectionLocal = UNSET;
  private volatile int maxRequestsPerConnectionRemote = UNSET;

  private volatile int idleTimeoutSeconds = DEFAULT_IDLE_TIMEOUT_SECONDS;
  private volatile int poolTimeoutMillis = DEFAULT_POOL_TIMEOUT_MILLIS;
  private volatile int maxQueueSize = DEFAULT_MAX_QUEUE_SIZE;
  private volatile int heartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

  private volatile Executor initializationExecutor = DEFAULT_INITIALIZATION_EXECUTOR;

  public PoolingOptions() {}

  public int getCoreConnectionsPerHost(HostDistance distance) {
    return coreConnections[distance.ordinal()];
  }

  public synchronized PoolingOptions setCoreConnectionsPerHost(
      HostDistance distance, int newCoreConnections) {
    if (distance == HostDistance.IGNORED)
      throw new IllegalArgumentException(
          "Cannot set core connections per host for " + distance + " hosts");
    Preconditions.checkArgument(
        newCoreConnections >= 0, "core number of connections must be positive");

    if (maxConnections[distance.ordinal()] != UNSET)
      checkConnectionsPerHostOrder(
          newCoreConnections, maxConnections[distance.ordinal()], distance);

    coreConnections[distance.ordinal()] = newCoreConnections;
    return this;
  }

  public int getMaxConnectionsPerHost(HostDistance distance) {
    return maxConnections[distance.ordinal()];
  }

  public synchronized PoolingOptions setMaxConnectionsPerHost(
      HostDistance distance, int newMaxConnections) {
    if (distance == HostDistance.IGNORED)
      throw new IllegalArgumentException(
          "Cannot set max connections per host for " + distance + " hosts");
    Preconditions.checkArgument(
        newMaxConnections >= 0, "max number of connections must be positive");

    if (coreConnections[distance.ordinal()] != UNSET)
      checkConnectionsPerHostOrder(
          coreConnections[distance.ordinal()], newMaxConnections, distance);

    maxConnections[distance.ordinal()] = newMaxConnections;
    return this;
  }

  public synchronized PoolingOptions setConnectionsPerHost(
      HostDistance distance, int core, int max) {
    if (distance == HostDistance.IGNORED)
      throw new IllegalArgumentException(
          "Cannot set connections per host for " + distance + " hosts");
    Preconditions.checkArgument(core >= 0, "core number of connections must be positive");
    Preconditions.checkArgument(max >= 0, "max number of connections must be positive");

    checkConnectionsPerHostOrder(core, max, distance);
    coreConnections[distance.ordinal()] = core;
    maxConnections[distance.ordinal()] = max;
    return this;
  }

  public int getNewConnectionThreshold(HostDistance distance) {
    return newConnectionThreshold[distance.ordinal()];
  }

  public synchronized PoolingOptions setNewConnectionThreshold(
      HostDistance distance, int newValue) {
    if (distance == HostDistance.IGNORED)
      throw new IllegalArgumentException(
          "Cannot set new connection threshold for " + distance + " hosts");

    checkRequestsPerConnectionRange(newValue, "New connection threshold", distance);
    if (NEW_CONN_THRESHOLD_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.setNewConnectionThreshold(...) is a no-op on the 3.x-compatibility shim: "
              + "4.x pools have a fixed size and open no extra connections on demand. The value is "
              + "retained for getter round-trip but has no runtime effect.");
    }
    newConnectionThreshold[distance.ordinal()] = newValue;
    return this;
  }

  public int getMaxRequestsPerConnection(HostDistance distance) {
    switch (distance) {
      case LOCAL:
        return maxRequestsPerConnectionLocal;
      case REMOTE:
        return maxRequestsPerConnectionRemote;
      default:
        return 0;
    }
  }

  public PoolingOptions setMaxRequestsPerConnection(HostDistance distance, int newMaxRequests) {
    checkRequestsPerConnectionRange(newMaxRequests, "Max requests per connection", distance);

    switch (distance) {
      case LOCAL:
        maxRequestsPerConnectionLocal = newMaxRequests;
        break;
      case REMOTE:
        maxRequestsPerConnectionRemote = newMaxRequests;
        break;
      default:
        throw new IllegalArgumentException(
            "Cannot set max requests per host for " + distance + " hosts");
    }
    return this;
  }

  public int getIdleTimeoutSeconds() {
    return idleTimeoutSeconds;
  }

  public PoolingOptions setIdleTimeoutSeconds(int idleTimeoutSeconds) {
    if (idleTimeoutSeconds < 0) throw new IllegalArgumentException("Idle timeout must be positive");
    if (IDLE_TIMEOUT_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.setIdleTimeoutSeconds(...) is a no-op on the 3.x-compatibility shim: 4.x "
              + "does not trash idle connections from fixed-size pools. The value is retained for "
              + "getter round-trip but has no runtime effect.");
    }
    this.idleTimeoutSeconds = idleTimeoutSeconds;
    return this;
  }

  public int getPoolTimeoutMillis() {
    return poolTimeoutMillis;
  }

  public PoolingOptions setPoolTimeoutMillis(int poolTimeoutMillis) {
    if (poolTimeoutMillis < 0) throw new IllegalArgumentException("Pool timeout must be positive");
    if (POOL_TIMEOUT_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.setPoolTimeoutMillis(...) is a no-op on the 3.x-compatibility shim: 4.x "
              + "has no borrow/acquisition timeout knob. The value is retained for getter round-trip "
              + "but has no runtime effect.");
    }
    this.poolTimeoutMillis = poolTimeoutMillis;
    return this;
  }

  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  public PoolingOptions setMaxQueueSize(int maxQueueSize) {
    if (maxQueueSize < 0) throw new IllegalArgumentException("Max queue size must be positive");
    if (MAX_QUEUE_SIZE_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.setMaxQueueSize(...) is a no-op on the 3.x-compatibility shim: 4.x has no "
              + "such acquisition-queue knob. The value is retained for getter round-trip but has no "
              + "runtime effect.");
    }
    this.maxQueueSize = maxQueueSize;
    return this;
  }

  public int getHeartbeatIntervalSeconds() {
    return heartbeatIntervalSeconds;
  }

  public PoolingOptions setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) {
    if (heartbeatIntervalSeconds < 0)
      throw new IllegalArgumentException("Heartbeat interval must be positive");

    this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
    return this;
  }

  public Executor getInitializationExecutor() {
    return initializationExecutor;
  }

  public PoolingOptions setInitializationExecutor(Executor initializationExecutor) {
    Preconditions.checkNotNull(initializationExecutor);
    if (INIT_EXECUTOR_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.setInitializationExecutor(...) is a no-op on the 3.x-compatibility shim: "
              + "4.x manages pool initialization on its own event-loop/admin executors. The value is "
              + "retained for getter round-trip but has no runtime effect.");
    }
    this.initializationExecutor = initializationExecutor;
    return this;
  }

  /**
   * UNMAPPED against 4.x (no runtime pool-refresh API). No-op in the shim; kept for ABI
   * compatibility.
   */
  public void refreshConnectedHosts() {
    // No-op on 4.x (which manages pools/node-distance internally). Warn once instead of throwing so
    // 3.x code that calls this on a schedule keeps working without a hard failure or log flood.
    if (REFRESH_ALL_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.refreshConnectedHosts() is a no-op on the 3.x-compatibility shim: the 4.x "
              + "driver manages connection pools and node distance internally, so there is no runtime "
              + "pool-refresh to trigger. This call is ignored (this warning is logged once).");
    }
  }

  /** No-op against 4.x (no runtime pool-refresh API); logs a deprecation/no-op warning once. */
  public void refreshConnectedHost(Host host) {
    if (REFRESH_ONE_WARNED.compareAndSet(false, true)) {
      logger.warn(
          "PoolingOptions.refreshConnectedHost(Host) is a no-op on the 3.x-compatibility shim: the "
              + "4.x driver manages connection pools internally. This call is ignored (this warning "
              + "is logged once).");
    }
  }

  private void checkRequestsPerConnectionRange(
      int value, String description, HostDistance distance) {
    int max =
        (protocolVersion == null || protocolVersion.compareTo(ProtocolVersion.V3) >= 0)
            ? MAX_STREAM_PER_CONNECTION_V3
            : MAX_STREAM_PER_CONNECTION_V2;

    if (value < 0 || value > max)
      throw new IllegalArgumentException(
          String.format(
              "%s for %s hosts must be in the range (0, %d)", description, distance, max));
  }

  private static void checkConnectionsPerHostOrder(int core, int max, HostDistance distance) {
    if (core > max)
      throw new IllegalArgumentException(
          String.format(
              "Core connections for %s hosts must be less than max (%d > %d)",
              distance, core, max));
  }
}
