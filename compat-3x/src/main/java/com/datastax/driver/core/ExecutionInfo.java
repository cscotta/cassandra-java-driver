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

import com.datastax.driver.core.utils.Bytes;
import com.google.common.util.concurrent.ListenableFuture;
import com.datastax.shim.bridge.FutureBridge;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Basic information on the execution of a query. */
public class ExecutionInfo {

  // Populated on the public-constructor path (user-created ExecutionInfo).
  private final int speculativeExecutions;
  private final int successfulExecutionIndex;
  private final List<Host> triedHosts;
  private final ConsistencyLevel achievedConsistencyLevel;
  private final Map<String, ByteBuffer> customPayload;

  // Populated on the facade path (wrapping a 4.x ExecutionInfo). Null on the public-ctor path.
  private final com.datastax.oss.driver.api.core.cql.ExecutionInfo delegate;

  // The originating 3.x statement, threaded through by the execute path so getStatement() returns
  // the exact object the caller executed. The 3.x object-mapper relies on
  // `getExecutionInfo().getStatement() instanceof MapperBoundStatement` to enable aliased reads.
  private Statement statement;

  /** Package-private: attach the originating 3.x statement. Returns this for chaining. */
  ExecutionInfo setStatement(Statement statement) {
    this.statement = statement;
    return this;
  }

  /**
   * Public constructor mirroring the 3.12.1 public ABI. Instances created this way carry only the
   * provided values and are not backed by a live 4.x execution.
   */
  public ExecutionInfo(
      int speculativeExecutions,
      int successfulExecutionIndex,
      List<Host> triedHosts,
      ConsistencyLevel achievedConsistency,
      Map<String, ByteBuffer> customPayload) {
    this.speculativeExecutions = speculativeExecutions;
    this.successfulExecutionIndex = successfulExecutionIndex;
    this.triedHosts = triedHosts;
    this.achievedConsistencyLevel = achievedConsistency;
    this.customPayload = customPayload;
    this.delegate = null;
  }

  // Facade path (used by ResultSetBridge.toV3Info).
  ExecutionInfo(com.datastax.oss.driver.api.core.cql.ExecutionInfo delegate) {
    this.delegate = delegate;
    this.speculativeExecutions = 0;
    this.successfulExecutionIndex = 0;
    this.triedHosts = null;
    // No 4.x concept for downgrading-retry achieved consistency.
    this.achievedConsistencyLevel = null;
    this.customPayload = null;
  }

  /**
   * The list of tried hosts for this query.
   *
   * <p>The 4.x driver only exposes the coordinator (not the full tried-hosts list), so on the
   * facade path this returns a singleton list of the coordinator.
   *
   * @return the list of tried hosts.
   */
  public List<Host> getTriedHosts() {
    if (delegate != null) {
      Host coordinator = SchemaBridge.toHost(delegate.getCoordinator());
      return coordinator == null
          ? Collections.<Host>emptyList()
          : Collections.singletonList(coordinator);
    }
    return triedHosts;
  }

  /**
   * The host that coordinated this query.
   *
   * @return the coordinator host, or {@code null} if unknown.
   */
  public Host getQueriedHost() {
    if (delegate != null) {
      return SchemaBridge.toHost(delegate.getCoordinator());
    }
    return (triedHosts == null || triedHosts.isEmpty())
        ? null
        : triedHosts.get(triedHosts.size() - 1);
  }

  /**
   * The number of speculative executions that were started for this query.
   *
   * @return the number of speculative executions.
   */
  public int getSpeculativeExecutions() {
    return delegate != null ? delegate.getSpeculativeExecutionCount() : speculativeExecutions;
  }

  /**
   * The index of the execution that completed this query.
   *
   * @return the index of the successful execution.
   */
  public int getSuccessfulExecutionIndex() {
    return delegate != null ? delegate.getSuccessfulExecutionIndex() : successfulExecutionIndex;
  }

  /**
   * The consistency level achieved for this query.
   *
   * <p>The 4.x driver removed the downgrading-retry concept, so this always returns {@code null} on
   * the facade path (matching 3.x behavior with the default retry policy).
   *
   * @return the achieved consistency level, or {@code null}.
   */
  public ConsistencyLevel getAchievedConsistencyLevel() {
    return achievedConsistencyLevel;
  }

  /**
   * The query trace for this query, if tracing was enabled.
   *
   * @return the query trace, or {@code null} if tracing was not enabled.
   */
  public QueryTrace getQueryTrace() {
    if (delegate == null || delegate.getTracingId() == null) return null;
    return new QueryTrace(delegate.getQueryTrace());
  }

  /**
   * The query trace for this query, fetched asynchronously.
   *
   * @return a future on the query trace.
   */
  public ListenableFuture<QueryTrace> getQueryTraceAsync() {
    if (delegate == null || delegate.getTracingId() == null) {
      return com.google.common.util.concurrent.Futures.immediateFuture((QueryTrace) null);
    }
    return FutureBridge.toListenableFuture(
        delegate
            .getQueryTraceAsync()
            .thenApply(
                new java.util.function.Function<
                    com.datastax.oss.driver.api.core.cql.QueryTrace, QueryTrace>() {
                  @Override
                  public QueryTrace apply(
                      com.datastax.oss.driver.api.core.cql.QueryTrace t) {
                    return t == null ? null : new QueryTrace(t);
                  }
                }));
  }

  /**
   * The paging state of the query.
   *
   * <p>Adapts the 4.x raw paging state to a 3.x {@link PagingState} bound to the originating
   * statement, so it round-trips through {@link Statement#setPagingState(PagingState)} (whose match
   * check re-hashes over the same statement, protocol version and codec registry). Returns {@code
   * null} when there is no next page, or when the originating statement is unavailable.
   *
   * @return the paging state, or {@code null}.
   */
  public PagingState getPagingState() {
    if (delegate == null || statement == null) return null;
    ByteBuffer raw = delegate.getPagingState();
    if (raw == null) return null;
    return new PagingState(
        raw, statement, ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * The raw paging state of the query.
   *
   * @return the paging state as a byte array, or {@code null}.
   */
  public byte[] getPagingStateUnsafe() {
    if (delegate == null) return null;
    ByteBuffer bytes = delegate.getPagingState();
    return bytes == null ? null : Bytes.getArray(bytes);
  }

  /**
   * Whether the cluster had reached schema agreement after the execution of this query.
   *
   * @return whether the cluster reached schema agreement.
   */
  public boolean isSchemaInAgreement() {
    return delegate == null || delegate.isSchemaInAgreement();
  }

  /**
   * The server-side warnings for this query.
   *
   * @return the warnings, or an empty list if there are none.
   */
  public List<String> getWarnings() {
    return delegate != null ? delegate.getWarnings() : Collections.<String>emptyList();
  }

  /**
   * The custom payload sent back by the server with the response, if any.
   *
   * @return the custom payload.
   */
  public Map<String, ByteBuffer> getIncomingPayload() {
    return delegate != null ? delegate.getIncomingPayload() : customPayload;
  }

  /**
   * The statement whose execution this object represents.
   *
   * @return the statement, or {@code null} if this info was not produced by an execute path that
   *     threaded the originating statement.
   */
  public Statement getStatement() {
    return statement;
  }
}
