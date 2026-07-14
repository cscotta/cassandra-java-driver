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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 3.x {@link ResultSet} facade over a 4.x synchronous (auto-paging) {@code ResultSet}. A single
 * cursor over the 4.x rows is shared by {@link #one()}, {@link #all()} and {@link #iterator()},
 * matching 3.x consume-once semantics.
 */
final class SyncResultSet implements ResultSet {

  private final com.datastax.oss.driver.api.core.cql.ResultSet delegate;
  private ColumnDefinitions definitions;
  private Iterator<com.datastax.oss.driver.api.core.cql.Row> cursor;
  private final Statement statement;

  SyncResultSet(com.datastax.oss.driver.api.core.cql.ResultSet delegate) {
    this(delegate, null);
  }

  SyncResultSet(com.datastax.oss.driver.api.core.cql.ResultSet delegate, Statement statement) {
    this.delegate = delegate;
    this.statement = statement;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    if (definitions == null) {
      definitions = ResultSetBridge.toV3Defs(delegate.getColumnDefinitions());
    }
    return definitions;
  }

  private Iterator<com.datastax.oss.driver.api.core.cql.Row> cursor() {
    if (cursor == null) {
      cursor = delegate.iterator();
    }
    return cursor;
  }

  @Override
  public Row one() {
    Iterator<com.datastax.oss.driver.api.core.cql.Row> it = cursor();
    if (!it.hasNext()) return null;
    return ResultSetBridge.toV3Row(getColumnDefinitions(), it.next());
  }

  @Override
  public List<Row> all() {
    List<Row> result = new ArrayList<Row>(getAvailableWithoutFetching());
    Row row;
    while ((row = one()) != null) {
      result.add(row);
    }
    return result;
  }

  @Override
  public Iterator<Row> iterator() {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return cursor().hasNext();
      }

      @Override
      public Row next() {
        return ResultSetBridge.toV3Row(getColumnDefinitions(), cursor().next());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public boolean isExhausted() {
    // 3.x semantics: peek through any empty trailing page(s). The 4.x auto-paging iterator's
    // hasNext() fetches the next page(s) as needed and caches the peeked row, so this advances past
    // empty trailing pages without consuming a real row (matching 3.x MultiPage.prepareNextRow).
    return !cursor().hasNext();
  }

  @Override
  public boolean isFullyFetched() {
    return delegate.isFullyFetched();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return delegate.getAvailableWithoutFetching();
  }

  @Override
  public ListenableFuture<ResultSet> fetchMoreResults() {
    // 4.x drives paging transparently on iteration; there is no separate prefetch control.
    return Futures.immediateFuture((ResultSet) this);
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return ResultSetBridge.toV3Info(delegate.getExecutionInfo(), statement);
  }

  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    List<com.datastax.oss.driver.api.core.cql.ExecutionInfo> src = delegate.getExecutionInfos();
    List<ExecutionInfo> result = new ArrayList<ExecutionInfo>(src.size());
    for (com.datastax.oss.driver.api.core.cql.ExecutionInfo info : src) {
      result.add(ResultSetBridge.toV3Info(info, statement));
    }
    return result;
  }

  @Override
  public boolean wasApplied() {
    return delegate.wasApplied();
  }

  @Override
  public String toString() {
    return "ResultSet[ exhausted: " + isExhausted() + ", " + getColumnDefinitions() + "]";
  }
}
