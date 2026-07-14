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

import com.datastax.driver.core.policies.RetryPolicy;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Represents a prepared statement, a query with bound variables that has been prepared (pre-parsed)
 * by the database.
 *
 * <p>A prepared statement can be executed once concrete values have been provided for the bound
 * variables.
 */
public interface PreparedStatement {

  /**
   * Returns metadata on the bounded variables of this prepared statement.
   *
   * @return the variables bounded in this prepared statement.
   */
  public ColumnDefinitions getVariables();

  /**
   * Creates a new BoundStatement object and bind its variables to the provided values.
   *
   * @param values the values to bind to the variables of the newly created BoundStatement.
   * @return the newly created {@code BoundStatement} with its variables bound to {@code values}.
   * @see BoundStatement#bind
   */
  public BoundStatement bind(Object... values);

  /**
   * Creates a new BoundStatement object for this prepared statement.
   *
   * @return the newly created {@code BoundStatement}.
   */
  public BoundStatement bind();

  /**
   * Sets the routing key for this prepared statement.
   *
   * @param routingKey the raw (binary) value to use as routing key.
   * @return this {@code PreparedStatement} object.
   */
  public PreparedStatement setRoutingKey(ByteBuffer routingKey);

  /**
   * Sets the routing key for this query.
   *
   * @param routingKeyComponents the raw (binary) values to compose to obtain the routing key.
   * @return this {@code PreparedStatement} object.
   */
  public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents);

  /**
   * Returns the routing key set for this query.
   *
   * @return the routing key for this query or {@code null} if none has been explicitly set on this
   *     PreparedStatement.
   */
  public ByteBuffer getRoutingKey();

  /**
   * Sets a default consistency level for all bound statements created from this prepared statement.
   *
   * @param consistency the default consistency level to set.
   * @return this {@code PreparedStatement} object.
   */
  public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Returns the default consistency level set through {@link #setConsistencyLevel}.
   *
   * @return the default consistency level.
   */
  public ConsistencyLevel getConsistencyLevel();

  /**
   * Sets a default serial consistency level for all bound statements created from this prepared
   * statement.
   *
   * @param serialConsistency the default serial consistency level to set.
   * @return this {@code PreparedStatement} object.
   * @throws IllegalArgumentException if {@code serialConsistency} is not one of {@code
   *     ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
   */
  public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency);

  /**
   * Returns the default serial consistency level set through {@link #setSerialConsistencyLevel}.
   *
   * @return the default serial consistency level.
   */
  public ConsistencyLevel getSerialConsistencyLevel();

  /**
   * Returns the string of the query that was prepared to yield this {@code PreparedStatement}.
   *
   * @return the query that was prepared to yield this {@code PreparedStatement}.
   */
  public String getQueryString();

  /**
   * Returns the keyspace at the time that this prepared statement was prepared.
   *
   * @return the keyspace at the time that this statement was prepared or {@code null}.
   */
  public String getQueryKeyspace();

  /**
   * Convenience method to enables tracing for all bound statements created from this prepared
   * statement.
   *
   * @return this {@code Query} object.
   */
  public PreparedStatement enableTracing();

  /**
   * Convenience method to disable tracing for all bound statements created from this prepared
   * statement.
   *
   * @return this {@code PreparedStatement} object.
   */
  public PreparedStatement disableTracing();

  /**
   * Returns whether tracing is enabled for this prepared statement.
   *
   * @return {@code true} if this prepared statement has tracing enabled, {@code false} otherwise.
   */
  public boolean isTracing();

  /**
   * Convenience method to set a default retry policy for the {@code BoundStatement} created from
   * this prepared statement.
   *
   * @param policy the retry policy to use for this prepared statement.
   * @return this {@code PreparedStatement} object.
   */
  public PreparedStatement setRetryPolicy(RetryPolicy policy);

  /**
   * Returns the retry policy sets for this prepared statement, if any.
   *
   * @return the retry policy sets specifically for this prepared statement or {@code null}.
   */
  public RetryPolicy getRetryPolicy();

  /**
   * Returns the prepared Id for this statement.
   *
   * @return the PreparedId corresponding to this statement.
   */
  public PreparedId getPreparedId();

  /**
   * Return the incoming payload, that is, the payload that the server sent back with its {@code
   * PREPARED} response, if any, or {@code null}.
   *
   * @return the custom payload that the server sent back with its response, if any, or {@code
   *     null}.
   * @since 2.2
   */
  public Map<String, ByteBuffer> getIncomingPayload();

  /**
   * Return the outgoing payload currently associated with this statement.
   *
   * @return this statement's outgoing payload, if any, or {@code null} if no outgoing payload is
   *     set
   * @since 2.2
   */
  public Map<String, ByteBuffer> getOutgoingPayload();

  /**
   * Associate the given payload with this prepared statement.
   *
   * @param payload the outgoing payload to associate with this statement, or {@code null} to clear
   *     any previously associated payload.
   * @return this {@link Statement} object.
   * @since 2.2
   */
  public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload);

  /**
   * Return the {@link CodecRegistry} instance associated with this prepared statement.
   *
   * @return the {@link CodecRegistry} instance associated with this prepared statement.
   */
  public CodecRegistry getCodecRegistry();

  /**
   * Sets whether this statement is idempotent.
   *
   * @param idempotent the new value.
   * @return this {@code IdempotenceAwarePreparedStatement} object.
   */
  public PreparedStatement setIdempotent(Boolean idempotent);

  /**
   * Whether this statement is idempotent.
   *
   * @return whether this statement is idempotent, or {@code null} to use {@link
   *     QueryOptions#getDefaultIdempotence()}.
   */
  public Boolean isIdempotent();
}
