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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Public seam (in {@code com.datastax.driver.core}) that the {@code com.datastax.shim.bridge}
 * package and the session/cluster tier use to translate between the mutable 3.x shim statement
 * state-holders and the immutable 4.x driver statements. It lives in this package because it needs
 * the package-private state of the shim statements (paging state, bound value buffers, batch
 * children, the prepared-id carrier, and {@link ConsistencyLevel#code}).
 *
 * <p>Not part of the 3.x public ABI: japicmp runs with {@code --only-incompatible}, so this added
 * type is never reported (adding public API is binary-compatible). The catalogue statement FQNs are
 * unaffected.
 *
 * <p>The shim statement types accumulate 3.x options in fields; a 4.x {@code Statement<?>} is
 * materialized here by reading those fields and calling the 4.x setters in sequence.
 */
public final class StatementShimAccess {

  private StatementShimAccess() {}

  /** Materializes an immutable 4.x {@code Statement<?>} from an accumulated shim {@link Statement}. */
  public static com.datastax.oss.driver.api.core.cql.Statement<?> toV4(
      Statement shim, CqlSession session) {
    if (shim instanceof StatementWrapper) {
      shim = ((StatementWrapper) shim).getWrappedStatement();
    }
    if (shim instanceof BoundStatement) {
      return toV4Bound((BoundStatement) shim, session);
    } else if (shim instanceof BatchStatement) {
      return toV4Batch((BatchStatement) shim, session);
    } else if (shim instanceof RegularStatement) {
      return toV4Regular((RegularStatement) shim, session);
    }
    throw new UnsupportedOperationException(
        "Cannot materialize statement of type " + shim.getClass().getName());
  }

  /** Wraps a 4.x {@code PreparedStatement} in a shim {@link PreparedStatement}. */
  public static PreparedStatement toV3Prepared(
      com.datastax.oss.driver.api.core.cql.PreparedStatement v4, CqlSession session) {
    ColumnDefinitions variables = ResultSetBridge.toV3Defs(v4.getVariableDefinitions());
    ProtocolVersion pv =
        ProtocolVersion.fromInt(session.getContext().getProtocolVersion().getCode());
    int[] routingKeyIndexes = ShimPreparedStatement.toIntArray(v4.getPartitionKeyIndices());
    PreparedId preparedId = new PreparedId(pv, routingKeyIndexes);
    String queryKeyspace = variables.size() == 0 ? null : variables.getKeyspace(0);
    if (queryKeyspace != null && queryKeyspace.isEmpty()) {
      queryKeyspace = null;
    }
    return new ShimPreparedStatement(
        v4,
        variables,
        preparedId,
        CodecRegistry.DEFAULT_INSTANCE,
        queryKeyspace,
        /* incomingPayload */ null);
  }

  // ---- SimpleStatement / RegularStatement ----------------------------------------------------

  private static com.datastax.oss.driver.api.core.cql.SimpleStatement toV4Regular(
      RegularStatement shim, CqlSession session) {
    String query = shim.getQueryString();
    // Serialize the values on the 3.x side using the shim CodecRegistry (which reproduces the
    // 3.12.1 type system, incl. java.util.Date, shim LocalDate/Duration/UDTValue/TupleValue/Token),
    // exactly as the real 3.x driver does before sending an (unprepared) RegularStatement. The
    // resulting ByteBuffers are handed to 4.x as positional/named values; 4.x's blob codec passes a
    // ByteBuffer value through unchanged (BlobCodec.encode returns value.duplicate()), so the wire
    // bytes match 3.x rather than being re-encoded by the 4.x session registry (which lacks codecs
    // for those Java types). This covers both SimpleStatement and query-builder BuiltStatement,
    // whose non-inlined values would otherwise be dropped.
    ProtocolVersion pv = ProtocolVersion.NEWEST_SUPPORTED;
    CodecRegistry registry = CodecRegistry.DEFAULT_INSTANCE;
    com.datastax.oss.driver.api.core.cql.SimpleStatement v4;
    if (shim.usesNamedValues()) {
      Map<String, ByteBuffer> named = shim.getNamedValues(pv, registry);
      if (named == null || named.isEmpty()) {
        v4 = com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query);
      } else {
        Map<String, Object> namedValues = new HashMap<String, Object>(named);
        v4 = com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query, namedValues);
      }
    } else {
      ByteBuffer[] values = shim.getValues(pv, registry);
      if (values == null || values.length == 0) {
        v4 = com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query);
      } else {
        Object[] positional = new Object[values.length];
        System.arraycopy(values, 0, positional, 0, values.length);
        v4 = com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query, positional);
      }
    }
    return applyCommon(v4, shim, /* withRoutingKey */ true);
  }

  // ---- BoundStatement ------------------------------------------------------------------------

  private static com.datastax.oss.driver.api.core.cql.BoundStatement toV4Bound(
      BoundStatement shim, CqlSession session) {
    ShimPreparedStatement prep = (ShimPreparedStatement) shim.preparedStatement();
    BoundStatementBuilder builder = prep.v4.boundStatementBuilder();
    ByteBuffer[] values = shim.wrapper.values;
    for (int i = 0; i < values.length; i++) {
      ByteBuffer v = values[i];
      if (v == BoundStatement.UNSET) {
        continue; // leave unset (4.x builders start unset)
      }
      builder = builder.setBytesUnsafe(i, v); // v may be null -> explicit null
    }
    return applyCommon(builder.build(), shim, /* withRoutingKey */ true);
  }

  // ---- BatchStatement ------------------------------------------------------------------------

  private static com.datastax.oss.driver.api.core.cql.BatchStatement toV4Batch(
      BatchStatement shim, CqlSession session) {
    com.datastax.oss.driver.api.core.cql.BatchStatement v4 =
        com.datastax.oss.driver.api.core.cql.BatchStatement.newInstance(
            toV4BatchType(shim.batchType));
    List<BatchableStatement<?>> children = new ArrayList<BatchableStatement<?>>();
    for (Statement child : shim.getStatements()) {
      children.add(toV4Batchable(child, session));
    }
    v4 = v4.addAll(children);
    return applyCommon(v4, shim, /* withRoutingKey */ false);
  }

  private static BatchableStatement<?> toV4Batchable(Statement child, CqlSession session) {
    if (child instanceof StatementWrapper) {
      child = ((StatementWrapper) child).getWrappedStatement();
    }
    if (child instanceof BoundStatement) {
      return toV4Bound((BoundStatement) child, session);
    }
    // RegularStatement (child batch options are ignored per 3.x semantics)
    return toV4Regular((RegularStatement) child, session);
  }

  private static DefaultBatchType toV4BatchType(BatchStatement.Type type) {
    switch (type) {
      case UNLOGGED:
        return DefaultBatchType.UNLOGGED;
      case COUNTER:
        return DefaultBatchType.COUNTER;
      case LOGGED:
      default:
        return DefaultBatchType.LOGGED;
    }
  }

  // ---- common options --------------------------------------------------------------------------

  /**
   * Applies the common {@link Statement} options accumulated on the shim to a freshly-created 4.x
   * statement. The 4.x setters are immutable (return a new instance of the concrete self type), so
   * this is written generically over {@code S}.
   */
  private static <S extends com.datastax.oss.driver.api.core.cql.Statement<S>> S applyCommon(
      S v4, Statement shim, boolean withRoutingKey) {
    ConsistencyLevel cl = shim.getConsistencyLevel();
    if (cl != null) {
      v4 = v4.setConsistencyLevel(DefaultConsistencyLevel.fromCode(cl.code));
    }
    ConsistencyLevel serial = shim.getSerialConsistencyLevel();
    if (serial != null) {
      v4 = v4.setSerialConsistencyLevel(DefaultConsistencyLevel.fromCode(serial.code));
    }
    if (shim.isTracing()) {
      v4 = v4.setTracing(true);
    }
    int fetchSize = shim.getFetchSize();
    if (fetchSize > 0) {
      v4 = v4.setPageSize(fetchSize);
    }
    long timestamp = shim.getDefaultTimestamp();
    if (timestamp != Long.MIN_VALUE) {
      v4 = v4.setQueryTimestamp(timestamp);
    }
    int readTimeout = shim.getReadTimeoutMillis();
    if (readTimeout >= 0) {
      // 3.x semantics: a value of 0 DISABLES the per-statement read timeout. Duration.ZERO is
      // non-null so 4.x resolveRequestTimeout returns it and scheduleTimeout leaves the timer off
      // (toNanos() is not > 0), matching 3.x. A negative value (the Integer.MIN_VALUE default) is
      // skipped, correctly falling back to the profile default.
      v4 = v4.setTimeout(Duration.ofMillis(readTimeout));
    }
    Boolean idempotent = shim.isIdempotent();
    if (idempotent != null) {
      v4 = v4.setIdempotent(idempotent);
    }
    ByteBuffer pagingState = shim.getPagingState();
    if (pagingState != null) {
      v4 = v4.setPagingState(pagingState);
    }
    Map<String, ByteBuffer> payload = shim.getOutgoingPayload();
    if (payload != null) {
      v4 = v4.setCustomPayload(payload);
    }
    int nowInSeconds = shim.getNowInSeconds();
    if (nowInSeconds != Integer.MIN_VALUE) {
      v4 = v4.setNowInSeconds(nowInSeconds);
    }
    String keyspace = shim.getKeyspace();
    if (keyspace != null) {
      v4 = v4.setRoutingKeyspace(keyspace);
    }
    if (withRoutingKey) {
      ByteBuffer rk =
          shim.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE);
      if (rk != null) {
        v4 = v4.setRoutingKey(rk);
      }
    }
    // Per-request RetryPolicy (shim.getRetryPolicy()) is bridged: register the final 4.x statement
    // identity -> (3.x RetryPolicy, 3.x Statement) so the Shim3xRetryPolicy dispatcher applies it at
    // execution. A pinned Host (shim.getHost()) still has no 4.x per-request equivalent (round-trips
    // but is inert).
    com.datastax.driver.core.policies.RetryPolicy perRequestRetry = shim.getRetryPolicy();
    if (perRequestRetry != null) {
      com.datastax.shim.bridge.Shim3xRetryPolicy.registerPerRequest(v4, perRequestRetry, shim);
    }
    return v4;
  }
}
