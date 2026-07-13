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

/**
 * Options related to defaults for individual queries. Shim port of the 3.12.1 value bean.
 *
 * <p>State round-trips through the getters/setters and {@link #equals(Object)}/{@link #hashCode()}
 * are reimplemented over the same fields as 3.12.1. The {@link com.datastax.shim.bridge.ConfigBridge}
 * maps the held values onto the equivalent 4.x request/metadata config keys.
 */
public class QueryOptions {

  /** The default consistency level for queries: {@link ConsistencyLevel#LOCAL_ONE}. */
  public static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.LOCAL_ONE;

  /** The default serial consistency level for conditional updates: {@link ConsistencyLevel#SERIAL}. */
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY_LEVEL = ConsistencyLevel.SERIAL;

  /** The default fetch size for SELECT queries: 5000. */
  public static final int DEFAULT_FETCH_SIZE = 5000;

  /** The default value for {@link #getDefaultIdempotence()}: {@code false}. */
  public static final boolean DEFAULT_IDEMPOTENCE = false;

  public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS = 20;
  public static final int DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS = 20;
  public static final int DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS = 20;
  public static final int DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS = 1000;
  public static final int DEFAULT_REFRESH_NODE_INTERVAL_MILLIS = 1000;
  public static final int DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS = 1000;

  private volatile ConsistencyLevel consistency = DEFAULT_CONSISTENCY_LEVEL;
  private volatile ConsistencyLevel serialConsistency = DEFAULT_SERIAL_CONSISTENCY_LEVEL;
  private volatile int fetchSize = DEFAULT_FETCH_SIZE;
  private volatile boolean defaultIdempotence = DEFAULT_IDEMPOTENCE;

  private volatile boolean consistencySet = false;
  private volatile boolean metadataEnabled = true;

  private volatile int maxPendingRefreshNodeListRequests =
      DEFAULT_MAX_PENDING_REFRESH_NODE_LIST_REQUESTS;
  private volatile int maxPendingRefreshNodeRequests = DEFAULT_MAX_PENDING_REFRESH_NODE_REQUESTS;
  private volatile int maxPendingRefreshSchemaRequests =
      DEFAULT_MAX_PENDING_REFRESH_SCHEMA_REQUESTS;

  private volatile int refreshNodeListIntervalMillis = DEFAULT_REFRESH_NODE_LIST_INTERVAL_MILLIS;
  private volatile int refreshNodeIntervalMillis = DEFAULT_REFRESH_NODE_INTERVAL_MILLIS;
  private volatile int refreshSchemaIntervalMillis = DEFAULT_REFRESH_SCHEMA_INTERVAL_MILLIS;

  private volatile boolean reprepareOnUp = true;
  private volatile boolean prepareOnAllHosts = true;

  public QueryOptions() {}

  public QueryOptions setConsistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencySet = true;
    this.consistency = consistencyLevel;
    return this;
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistency;
  }

  public QueryOptions setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
    this.serialConsistency = serialConsistencyLevel;
    return this;
  }

  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistency;
  }

  public QueryOptions setFetchSize(int fetchSize) {
    if (fetchSize <= 0)
      throw new IllegalArgumentException("Invalid fetchSize, should be > 0, got " + fetchSize);
    this.fetchSize = fetchSize;
    return this;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public QueryOptions setDefaultIdempotence(boolean defaultIdempotence) {
    this.defaultIdempotence = defaultIdempotence;
    return this;
  }

  public boolean getDefaultIdempotence() {
    return defaultIdempotence;
  }

  public QueryOptions setPrepareOnAllHosts(boolean prepareOnAllHosts) {
    this.prepareOnAllHosts = prepareOnAllHosts;
    return this;
  }

  public boolean isPrepareOnAllHosts() {
    return this.prepareOnAllHosts;
  }

  public QueryOptions setReprepareOnUp(boolean reprepareOnUp) {
    this.reprepareOnUp = reprepareOnUp;
    return this;
  }

  public boolean isReprepareOnUp() {
    return this.reprepareOnUp;
  }

  public QueryOptions setMetadataEnabled(boolean enabled) {
    this.metadataEnabled = enabled;
    return this;
  }

  public boolean isMetadataEnabled() {
    return metadataEnabled;
  }

  public QueryOptions setRefreshSchemaIntervalMillis(int refreshSchemaIntervalMillis) {
    this.refreshSchemaIntervalMillis = refreshSchemaIntervalMillis;
    return this;
  }

  public int getRefreshSchemaIntervalMillis() {
    return refreshSchemaIntervalMillis;
  }

  public QueryOptions setMaxPendingRefreshSchemaRequests(int maxPendingRefreshSchemaRequests) {
    this.maxPendingRefreshSchemaRequests = maxPendingRefreshSchemaRequests;
    return this;
  }

  public int getMaxPendingRefreshSchemaRequests() {
    return maxPendingRefreshSchemaRequests;
  }

  public QueryOptions setRefreshNodeListIntervalMillis(int refreshNodeListIntervalMillis) {
    this.refreshNodeListIntervalMillis = refreshNodeListIntervalMillis;
    return this;
  }

  public int getRefreshNodeListIntervalMillis() {
    return refreshNodeListIntervalMillis;
  }

  public QueryOptions setMaxPendingRefreshNodeListRequests(int maxPendingRefreshNodeListRequests) {
    this.maxPendingRefreshNodeListRequests = maxPendingRefreshNodeListRequests;
    return this;
  }

  public int getMaxPendingRefreshNodeListRequests() {
    return maxPendingRefreshNodeListRequests;
  }

  public QueryOptions setRefreshNodeIntervalMillis(int refreshNodeIntervalMillis) {
    this.refreshNodeIntervalMillis = refreshNodeIntervalMillis;
    return this;
  }

  public int getRefreshNodeIntervalMillis() {
    return refreshNodeIntervalMillis;
  }

  public QueryOptions setMaxPendingRefreshNodeRequests(int maxPendingRefreshNodeRequests) {
    this.maxPendingRefreshNodeRequests = maxPendingRefreshNodeRequests;
    return this;
  }

  public int getMaxPendingRefreshNodeRequests() {
    return maxPendingRefreshNodeRequests;
  }

  @Override
  public boolean equals(Object that) {
    if (that == null || !(that instanceof QueryOptions)) {
      return false;
    }

    QueryOptions other = (QueryOptions) that;

    return (this.consistency.equals(other.consistency)
        && this.serialConsistency.equals(other.serialConsistency)
        && this.fetchSize == other.fetchSize
        && this.defaultIdempotence == other.defaultIdempotence
        && this.metadataEnabled == other.metadataEnabled
        && this.maxPendingRefreshNodeListRequests == other.maxPendingRefreshNodeListRequests
        && this.maxPendingRefreshNodeRequests == other.maxPendingRefreshNodeRequests
        && this.maxPendingRefreshSchemaRequests == other.maxPendingRefreshSchemaRequests
        && this.refreshNodeListIntervalMillis == other.refreshNodeListIntervalMillis
        && this.refreshNodeIntervalMillis == other.refreshNodeIntervalMillis
        && this.refreshSchemaIntervalMillis == other.refreshSchemaIntervalMillis
        && this.reprepareOnUp == other.reprepareOnUp
        && this.prepareOnAllHosts == other.prepareOnAllHosts);
  }

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(
        consistency,
        serialConsistency,
        fetchSize,
        defaultIdempotence,
        metadataEnabled,
        maxPendingRefreshNodeListRequests,
        maxPendingRefreshNodeRequests,
        maxPendingRefreshSchemaRequests,
        refreshNodeListIntervalMillis,
        refreshNodeIntervalMillis,
        refreshSchemaIntervalMillis,
        reprepareOnUp,
        prepareOnAllHosts);
  }

  public boolean isConsistencySet() {
    return consistencySet;
  }
}
