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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * 3.x {@link ResultSet} facade over a 4.x {@code AsyncResultSet}. The 4.x async API does not
 * auto-page; this class reproduces 3.x transparent paging by blocking on {@code fetchNextPage()}
 * when the current page is exhausted. Used for the {@code executeAsync} / {@link ResultSetFuture}
 * path once the first page has resolved.
 */
final class AsyncPagedResultSet implements ResultSet {

  private com.datastax.oss.driver.api.core.cql.AsyncResultSet current;
  private Iterator<com.datastax.oss.driver.api.core.cql.Row> pageCursor;
  private final boolean wasApplied;
  private ColumnDefinitions definitions;
  private final List<com.datastax.oss.driver.api.core.cql.ExecutionInfo> executionInfos =
      new ArrayList<com.datastax.oss.driver.api.core.cql.ExecutionInfo>();
  private final Statement statement;

  AsyncPagedResultSet(com.datastax.oss.driver.api.core.cql.AsyncResultSet first) {
    this(first, null);
  }

  AsyncPagedResultSet(
      com.datastax.oss.driver.api.core.cql.AsyncResultSet first, Statement statement) {
    this.current = first;
    this.wasApplied = first.wasApplied();
    this.pageCursor = first.currentPage().iterator();
    this.executionInfos.add(first.getExecutionInfo());
    this.statement = statement;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    if (definitions == null) {
      definitions = ResultSetBridge.toV3Defs(current.getColumnDefinitions());
    }
    return definitions;
  }

  // Advances across pages until the cursor has a row or the result is fully consumed.
  private boolean ensureAvailable() {
    while (!pageCursor.hasNext()) {
      if (!current.hasMorePages()) return false;
      try {
        current = Uninterruptibles.getUninterruptibly(current.fetchNextPage().toCompletableFuture());
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) throw (RuntimeException) cause;
        if (cause instanceof Error) throw (Error) cause;
        throw new RuntimeException(cause == null ? e : cause);
      }
      executionInfos.add(current.getExecutionInfo());
      pageCursor = current.currentPage().iterator();
    }
    return true;
  }

  @Override
  public Row one() {
    if (!ensureAvailable()) return null;
    return ResultSetBridge.toV3Row(getColumnDefinitions(), pageCursor.next());
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
        return ensureAvailable();
      }

      @Override
      public Row next() {
        // Match 3.x iterator().next() (delegates to one()): cross page boundaries so a next()
        // without a preceding hasNext() still advances to the next page instead of throwing.
        if (!ensureAvailable()) {
          throw new java.util.NoSuchElementException();
        }
        return ResultSetBridge.toV3Row(getColumnDefinitions(), pageCursor.next());
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public boolean isExhausted() {
    // 3.x semantics: fetch through empty trailing page(s) to decide. ensureAvailable() advances
    // across pages (blocking on fetchNextPage) until a row is available or the result is fully
    // consumed, caching the peeked row, so an empty final page reports exhausted (as 3.x does).
    return !ensureAvailable();
  }

  @Override
  public boolean isFullyFetched() {
    return !current.hasMorePages();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return current.remaining();
  }

  @Override
  public ListenableFuture<ResultSet> fetchMoreResults() {
    return Futures.immediateFuture((ResultSet) this);
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return ResultSetBridge.toV3Info(executionInfos.get(executionInfos.size() - 1), statement);
  }

  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    List<ExecutionInfo> result = new ArrayList<ExecutionInfo>(executionInfos.size());
    for (com.datastax.oss.driver.api.core.cql.ExecutionInfo info : executionInfos) {
      result.add(ResultSetBridge.toV3Info(info, statement));
    }
    return result;
  }

  @Override
  public boolean wasApplied() {
    return wasApplied;
  }

  @Override
  public String toString() {
    return "ResultSet[ exhausted: " + isExhausted() + ", " + getColumnDefinitions() + "]";
  }
}
