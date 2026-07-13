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

import com.datastax.shim.bridge.FutureBridge;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Construction-bearing bridge for the results tier. Lives in {@code com.datastax.driver.core}
 * (package-private, like {@link SchemaBridge}) because it needs the package-private constructors of
 * the shim result types plus {@link SchemaBridge#toShimDataType}. The package-agnostic
 * {@code CompletionStage}&lt;-&gt;Guava adapter lives in {@code com.datastax.shim.bridge.FutureBridge};
 * this class supplies the {@code AsyncResultSet -> ResultSet} pager to it.
 */
final class ResultSetBridge {

  private ResultSetBridge() {}

  // ---- ColumnDefinitions ---------------------------------------------------------------------

  static ColumnDefinitions toV3Defs(
      com.datastax.oss.driver.api.core.cql.ColumnDefinitions defs) {
    int n = defs.size();
    ColumnDefinitions.Definition[] out = new ColumnDefinitions.Definition[n];
    for (int i = 0; i < n; i++) {
      ColumnDefinition d = defs.get(i);
      out[i] =
          new ColumnDefinitions.Definition(
              asInternal(d.getKeyspace()),
              asInternal(d.getTable()),
              asInternal(d.getName()),
              SchemaBridge.toShimDataType(d.getType()));
    }
    return new ColumnDefinitions(out, CodecRegistry.DEFAULT_INSTANCE);
  }

  private static String asInternal(CqlIdentifier id) {
    return id == null ? "" : id.asInternal();
  }

  // ---- Row -----------------------------------------------------------------------------------

  static Row toV3Row(com.datastax.oss.driver.api.core.cql.Row row) {
    return toV3Row(toV3Defs(row.getColumnDefinitions()), row);
  }

  static Row toV3Row(ColumnDefinitions defs, com.datastax.oss.driver.api.core.cql.Row row) {
    int n = defs.size();
    List<ByteBuffer> data = new ArrayList<ByteBuffer>(n);
    for (int i = 0; i < n; i++) {
      data.add(row.getBytesUnsafe(i));
    }
    ProtocolVersion pv = ProtocolVersion.fromInt(row.protocolVersion().getCode());
    return new ShimRow(defs, pv, data);
  }

  // ---- ExecutionInfo -------------------------------------------------------------------------

  static ExecutionInfo toV3Info(com.datastax.oss.driver.api.core.cql.ExecutionInfo info) {
    return toV3Info(info, null);
  }

  static ExecutionInfo toV3Info(
      com.datastax.oss.driver.api.core.cql.ExecutionInfo info, Statement statement) {
    return info == null ? null : new ExecutionInfo(info).setStatement(statement);
  }

  // ---- ResultSet -----------------------------------------------------------------------------

  /** Wraps a 4.x (auto-paging, synchronous) {@code ResultSet} in a 3.x {@link ResultSet}. */
  static ResultSet wrap(com.datastax.oss.driver.api.core.cql.ResultSet rs) {
    return wrap(rs, null);
  }

  static ResultSet wrap(com.datastax.oss.driver.api.core.cql.ResultSet rs, Statement statement) {
    return new SyncResultSet(rs, statement);
  }

  /** Wraps a 4.x {@code AsyncResultSet} in a 3.x {@link ResultSet} that pages transparently. */
  static ResultSet wrapAsyncPaged(AsyncResultSet first) {
    return wrapAsyncPaged(first, null);
  }

  static ResultSet wrapAsyncPaged(AsyncResultSet first, Statement statement) {
    return new AsyncPagedResultSet(first, statement);
  }

  /** Builds a 3.x {@link ResultSetFuture} over {@code CqlSession.executeAsync}. */
  static ResultSetFuture toFuture(CompletionStage<AsyncResultSet> stage) {
    return FutureBridge.toResultSetFuture(
        stage,
        new Function<AsyncResultSet, ResultSet>() {
          @Override
          public ResultSet apply(AsyncResultSet async) {
            return wrapAsyncPaged(async);
          }
        });
  }
}
