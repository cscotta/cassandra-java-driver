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

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Iterator;
import java.util.List;

/**
 * An iterable that is the result of a query, and can be paged.
 *
 * @param <S> the concrete type of the paging iterable (self-type).
 * @param <T> the type of the elements returned in the iterable.
 */
public interface PagingIterable<S extends PagingIterable<S, T>, T> extends Iterable<T> {

  /**
   * Returns whether this result set has more results.
   *
   * @return whether this result set has more results.
   */
  public boolean isExhausted();

  /**
   * Whether all results from this result set have been fetched from the database.
   *
   * @return whether all results have been fetched.
   */
  public boolean isFullyFetched();

  /**
   * The number of elements that can be retrieved from this result set without triggering a blocking
   * background fetch.
   *
   * @return the number of results that can be retrieved without a background fetch.
   */
  public int getAvailableWithoutFetching();

  /**
   * Force fetching the content of the next page of results, if any.
   *
   * @return a future on the completion of fetching the next page.
   */
  public ListenableFuture<S> fetchMoreResults();

  /**
   * Returns the next result, or {@code null} if this result set is exhausted.
   *
   * @return the next result, or {@code null}.
   */
  public T one();

  /**
   * Returns all the remaining results as a list.
   *
   * @return a list containing the remaining results.
   */
  public List<T> all();

  /**
   * Returns an iterator over the results.
   *
   * @return an iterator over the results.
   */
  @Override
  public Iterator<T> iterator();

  /**
   * Returns information on the execution of the last query made for this iterable.
   *
   * @return the execution info of the last query.
   */
  public ExecutionInfo getExecutionInfo();

  /**
   * Returns the execution information for all queries made to retrieve this iterable.
   *
   * @return a list of the execution info.
   */
  public List<ExecutionInfo> getAllExecutionInfo();
}
