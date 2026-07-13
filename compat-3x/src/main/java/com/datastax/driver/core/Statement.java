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

import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

/**
 * An executable query.
 *
 * <p>This represents either a {@link RegularStatement}, a {@link BoundStatement} or a {@link
 * BatchStatement} along with the querying options (consistency level, whether to trace the query,
 * ...).
 */
public abstract class Statement {

  /**
   * A special ByteBuffer value that can be used with custom payloads to denote a null value in a
   * payload map.
   */
  public static final ByteBuffer NULL_PAYLOAD_VALUE = ByteBuffer.allocate(0);

  // An exception to the RegularStatement, BoundStatement or BatchStatement rule above. This is
  // used when preparing a statement and for other internal queries. Do not expose publicly.
  static final Statement DEFAULT =
      new Statement() {
        @Override
        public ByteBuffer getRoutingKey(
            ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
          return null;
        }

        @Override
        public String getKeyspace() {
          return null;
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
          return ConsistencyLevel.ONE;
        }
      };

  private volatile ConsistencyLevel consistency;
  private volatile ConsistencyLevel serialConsistency;
  private volatile boolean traceQuery;
  private volatile int fetchSize;
  private volatile long defaultTimestamp = Long.MIN_VALUE;
  private volatile int readTimeoutMillis = Integer.MIN_VALUE;
  private volatile RetryPolicy retryPolicy;
  private volatile ByteBuffer pagingState;
  protected volatile Boolean idempotent;
  private volatile Map<String, ByteBuffer> outgoingPayload;
  private volatile Host host;
  private volatile int nowInSeconds = Integer.MIN_VALUE;

  // We don't want to expose the constructor, because the code relies on this being only sub-classed
  // by RegularStatement, BoundStatement and BatchStatement
  Statement() {}

  /**
   * Sets the consistency level for the query.
   *
   * @param consistency the consistency level to set.
   * @return this {@code Statement} object.
   */
  public Statement setConsistencyLevel(ConsistencyLevel consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * The consistency level for this query.
   *
   * @return the consistency level for this query, or {@code null} if no consistency level has been
   *     specified (through {@code setConsistencyLevel}). In the latter case, the default
   *     consistency level will be used.
   */
  public ConsistencyLevel getConsistencyLevel() {
    return consistency;
  }

  /**
   * Sets the serial consistency level for the query.
   *
   * <p>The serial consistency can only be one of {@code ConsistencyLevel.SERIAL} or {@code
   * ConsistencyLevel.LOCAL_SERIAL}.
   *
   * @param serialConsistency the serial consistency level to set.
   * @return this {@code Statement} object.
   * @throws IllegalArgumentException if {@code serialConsistency} is not one of {@code
   *     ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
   */
  public Statement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
    if (!serialConsistency.isSerial())
      throw new IllegalArgumentException(
          "Supplied consistency level is not serial: " + serialConsistency);
    this.serialConsistency = serialConsistency;
    return this;
  }

  /**
   * The serial consistency level for this query.
   *
   * @return the serial consistency level for this query, or {@code null} if no serial consistency
   *     level has been specified. In the latter case, the default serial consistency level will be
   *     used.
   */
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistency;
  }

  /**
   * Enables tracing for this query.
   *
   * @return this {@code Statement} object.
   */
  public Statement enableTracing() {
    this.traceQuery = true;
    return this;
  }

  /**
   * Disables tracing for this query.
   *
   * @return this {@code Statement} object.
   */
  public Statement disableTracing() {
    this.traceQuery = false;
    return this;
  }

  /**
   * Returns whether tracing is enabled for this query or not.
   *
   * @return {@code true} if this query has tracing enabled, {@code false} otherwise.
   */
  public boolean isTracing() {
    return traceQuery;
  }

  /**
   * Returns the routing key (in binary raw form) to use for token aware routing of this query.
   *
   * @param protocolVersion the protocol version that will be used if the actual implementation
   *     needs to serialize something to compute the key.
   * @param codecRegistry the codec registry that will be used if the actual implementation needs to
   *     serialize something to compute this key.
   * @return the routing key for this query or {@code null}.
   */
  public abstract ByteBuffer getRoutingKey(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

  /**
   * Returns the keyspace this query operates on.
   *
   * @return the keyspace this query operate on if relevant or {@code null}.
   */
  public abstract String getKeyspace();

  /**
   * Sets the retry policy to use for this query.
   *
   * @param policy the retry policy to use for this query.
   * @return this {@code Statement} object.
   */
  public Statement setRetryPolicy(RetryPolicy policy) {
    this.retryPolicy = policy;
    return this;
  }

  /**
   * Returns the retry policy sets for this query, if any.
   *
   * @return the retry policy sets specifically for this query or {@code null} if no query specific
   *     retry policy has been set.
   */
  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * Sets the query fetch size.
   *
   * @param fetchSize the fetch size to use. If {@code fetchSize &lte; 0}, the default fetch size
   *     will be used.
   * @return this {@code Statement} object.
   */
  public Statement setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  /**
   * The fetch size for this query.
   *
   * @return the fetch size for this query.
   */
  public int getFetchSize() {
    return fetchSize;
  }

  /**
   * Sets the default timestamp for this query (in microseconds since the epoch).
   *
   * @param defaultTimestamp the default timestamp for this query (must be strictly positive).
   * @return this {@code Statement} object.
   */
  public Statement setDefaultTimestamp(long defaultTimestamp) {
    this.defaultTimestamp = defaultTimestamp;
    return this;
  }

  /**
   * The default timestamp for this query.
   *
   * @return the default timestamp (in microseconds since the epoch).
   */
  public long getDefaultTimestamp() {
    return defaultTimestamp;
  }

  /**
   * Overrides the default per-host read timeout for this statement.
   *
   * @param readTimeoutMillis the timeout to set. Negative values are not allowed. If it is 0, the
   *     read timeout will be disabled for this statement.
   * @return this {@code Statement} object.
   */
  public Statement setReadTimeoutMillis(int readTimeoutMillis) {
    Preconditions.checkArgument(readTimeoutMillis >= 0, "read timeout must be >= 0");
    this.readTimeoutMillis = readTimeoutMillis;
    return this;
  }

  /**
   * Return the per-host read timeout that was set for this statement.
   *
   * @return the timeout. Note that a negative value means that the default will be used.
   */
  public int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  /**
   * Sets the paging state.
   *
   * @param pagingState the paging state to set, or {@code null} to remove any state that was
   *     previously set on this statement.
   * @param codecRegistry the codec registry that will be used if this method needs to serialize the
   *     statement's values in order to check that the paging state matches.
   * @return this {@code Statement} object.
   * @throws PagingStateException if the paging state does not match this statement.
   * @see #setPagingState(PagingState)
   */
  public Statement setPagingState(PagingState pagingState, CodecRegistry codecRegistry) {
    if (this instanceof BatchStatement) {
      throw new UnsupportedOperationException("Cannot set the paging state on a batch statement");
    } else {
      if (pagingState == null) {
        this.pagingState = null;
      } else if (pagingState.matches(this, codecRegistry)) {
        this.pagingState = pagingState.getRawState();
      } else {
        throw new PagingStateException(
            "Paging state mismatch, "
                + "this means that either the paging state contents were altered, "
                + "or you're trying to apply it to a different statement");
      }
    }
    return this;
  }

  /**
   * Sets the paging state.
   *
   * <p>This method calls {@link #setPagingState(PagingState, CodecRegistry)} with {@link
   * CodecRegistry#DEFAULT_INSTANCE}.
   *
   * @param pagingState the paging state to set, or {@code null} to remove any state that was
   *     previously set on this statement.
   */
  public Statement setPagingState(PagingState pagingState) {
    return setPagingState(pagingState, CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * Sets the paging state.
   *
   * <p>Contrary to {@link #setPagingState(PagingState)}, this method takes the "raw" form of the
   * paging state.
   *
   * @param pagingState the paging state to set, or {@code null} to remove any state that was
   *     previously set on this statement.
   * @return this {@code Statement} object.
   */
  public Statement setPagingStateUnsafe(byte[] pagingState) {
    if (pagingState == null) {
      this.pagingState = null;
    } else {
      this.pagingState = ByteBuffer.wrap(pagingState);
    }
    return this;
  }

  ByteBuffer getPagingState() {
    return pagingState;
  }

  /**
   * Sets whether this statement is idempotent.
   *
   * @param idempotent the new value.
   * @return this {@code Statement} object.
   */
  public Statement setIdempotent(boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  /**
   * Whether this statement is idempotent, i.e. whether it can be applied multiple times without
   * changing the result beyond the initial application.
   *
   * @return whether this statement is idempotent, or {@code null} to use {@link
   *     QueryOptions#getDefaultIdempotence()}.
   */
  public Boolean isIdempotent() {
    return idempotent;
  }

  boolean isIdempotentWithDefault(QueryOptions queryOptions) {
    Boolean myValue = this.isIdempotent();
    if (myValue != null) return myValue;
    else return queryOptions.getDefaultIdempotence();
  }

  /**
   * Returns this statement's outgoing payload.
   *
   * @return the outgoing payload to include with this statement, or {@code null} if no payload has
   *     been set.
   * @since 2.2
   */
  public Map<String, ByteBuffer> getOutgoingPayload() {
    return outgoingPayload;
  }

  /**
   * Set the given outgoing payload on this statement.
   *
   * @param payload the outgoing payload to include with this statement, or {@code null} to clear
   *     any previously entered payload.
   * @return this {@link Statement} object.
   * @since 2.2
   */
  public Statement setOutgoingPayload(Map<String, ByteBuffer> payload) {
    this.outgoingPayload = payload == null ? null : ImmutableMap.copyOf(payload);
    return this;
  }

  /**
   * Returns the number of bytes required to encode this statement.
   *
   * @return the number of bytes required to encode this statement.
   */
  public int requestSizeInBytes(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    return -1;
  }

  protected static Boolean isBatchIdempotent(Collection<? extends Statement> statements) {
    boolean hasNullIdempotentStatements = false;
    for (Statement statement : statements) {
      Boolean innerIdempotent = statement.isIdempotent();
      if (innerIdempotent == null) {
        hasNullIdempotentStatements = true;
      } else if (!innerIdempotent) {
        return false;
      }
    }
    return (hasNullIdempotentStatements) ? null : true;
  }

  /**
   * @return The host configured on this statement, or null if none is configured.
   * @see #setHost(Host)
   */
  public Host getHost() {
    return host;
  }

  /**
   * Sets the {@link Host} that should handle this query.
   *
   * @param host The host that should be used to handle executions of this statement or null to
   *     delegate to the configured load balancing policy.
   * @return this {@code Statement} object.
   */
  public Statement setHost(Host host) {
    this.host = host;
    return this;
  }

  /**
   * @return a custom "now in seconds" to use when applying the request (for testing purposes).
   *     {@link Integer#MIN_VALUE} means "no value".
   */
  public int getNowInSeconds() {
    return nowInSeconds;
  }

  /**
   * Sets the "now in seconds" to use when applying the request (for testing purposes). {@link
   * Integer#MIN_VALUE} means "no value".
   */
  public Statement setNowInSeconds(int nowInSeconds) {
    this.nowInSeconds = nowInSeconds;
    return this;
  }
}
