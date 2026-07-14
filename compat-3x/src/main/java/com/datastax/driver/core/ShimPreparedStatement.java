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
// Shim provenance: net-new — no 3.12.1 counterpart; written for the shim. See PROVENANCE.md.
package com.datastax.driver.core;

import com.datastax.driver.core.policies.RetryPolicy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Package-private shim implementation of the 3.x {@link PreparedStatement} interface. It wraps a 4.x
 * {@code PreparedStatement} and carries the 3.x default options (consistency, tracing, idempotence,
 * routing key, payloads, retry policy) that the 3.x contract propagates to every {@link
 * BoundStatement} created from it. Not part of the 3.x public ABI (japicmp only inspects public
 * types; this class is package-private).
 */
class ShimPreparedStatement implements PreparedStatement {

  final com.datastax.oss.driver.api.core.cql.PreparedStatement v4;
  private final ColumnDefinitions variables;
  private final PreparedId preparedId;
  private final CodecRegistry codecRegistry;
  private final String queryKeyspace;
  private final Map<String, ByteBuffer> incomingPayload;

  private volatile ByteBuffer routingKey;
  private volatile ConsistencyLevel consistency;
  private volatile ConsistencyLevel serialConsistency;
  private volatile boolean tracing;
  private volatile RetryPolicy retryPolicy;
  private volatile Map<String, ByteBuffer> outgoingPayload;
  private volatile Boolean idempotent;

  ShimPreparedStatement(
      com.datastax.oss.driver.api.core.cql.PreparedStatement v4,
      ColumnDefinitions variables,
      PreparedId preparedId,
      CodecRegistry codecRegistry,
      String queryKeyspace,
      Map<String, ByteBuffer> incomingPayload) {
    this.v4 = v4;
    this.variables = variables;
    this.preparedId = preparedId;
    this.codecRegistry = codecRegistry;
    this.queryKeyspace = queryKeyspace;
    this.incomingPayload = incomingPayload;
  }

  @Override
  public ColumnDefinitions getVariables() {
    return variables;
  }

  @Override
  public BoundStatement bind(Object... values) {
    return new BoundStatement(this).bind(values);
  }

  @Override
  public BoundStatement bind() {
    return new BoundStatement(this);
  }

  @Override
  public PreparedStatement setRoutingKey(ByteBuffer routingKey) {
    this.routingKey = routingKey;
    return this;
  }

  @Override
  public PreparedStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
    this.routingKey = SimpleStatement.compose(routingKeyComponents);
    return this;
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return routingKey;
  }

  @Override
  public PreparedStatement setConsistencyLevel(ConsistencyLevel consistency) {
    this.consistency = consistency;
    return this;
  }

  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistency;
  }

  @Override
  public PreparedStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
    if (serialConsistency != null && !serialConsistency.isSerial())
      throw new IllegalArgumentException(
          "Supplied consistency level is not serial: " + serialConsistency);
    this.serialConsistency = serialConsistency;
    return this;
  }

  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistency;
  }

  @Override
  public String getQueryString() {
    return v4.getQuery();
  }

  @Override
  public String getQueryKeyspace() {
    return queryKeyspace;
  }

  @Override
  public PreparedStatement enableTracing() {
    this.tracing = true;
    return this;
  }

  @Override
  public PreparedStatement disableTracing() {
    this.tracing = false;
    return this;
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public PreparedStatement setRetryPolicy(RetryPolicy policy) {
    this.retryPolicy = policy;
    return this;
  }

  @Override
  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  @Override
  public PreparedId getPreparedId() {
    return preparedId;
  }

  @Override
  public Map<String, ByteBuffer> getIncomingPayload() {
    return incomingPayload;
  }

  @Override
  public Map<String, ByteBuffer> getOutgoingPayload() {
    return outgoingPayload;
  }

  @Override
  public PreparedStatement setOutgoingPayload(Map<String, ByteBuffer> payload) {
    this.outgoingPayload =
        payload == null ? null : com.google.common.collect.ImmutableMap.copyOf(payload);
    return this;
  }

  @Override
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @Override
  public PreparedStatement setIdempotent(Boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  static int[] toIntArray(List<Integer> indices) {
    if (indices == null || indices.isEmpty()) return null;
    int[] out = new int[indices.size()];
    for (int i = 0; i < out.length; i++) out[i] = indices.get(i);
    return out;
  }
}
