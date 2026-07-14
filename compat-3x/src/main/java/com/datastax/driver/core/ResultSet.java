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

/**
 * The result of a query.
 *
 * <p>The retrieval of the rows of a {@code ResultSet} is generally paged (a first page of result is
 * fetched and the next one is only fetched once all the results of the first page have been
 * consumed). The size of the pages can be configured on the statement.
 */
public interface ResultSet extends PagingIterable<ResultSet, Row> {

  /**
   * Returns the next result, or {@code null} if this result set is exhausted.
   *
   * @return the next row, or {@code null}.
   */
  @Override
  public Row one();

  /**
   * Returns the columns returned in this ResultSet.
   *
   * @return the columns returned in this ResultSet.
   */
  public ColumnDefinitions getColumnDefinitions();

  /**
   * If the query that produced this ResultSet was a conditional update, returns whether it was
   * successfully applied.
   *
   * @return whether the conditional update was applied.
   */
  public boolean wasApplied();
}
