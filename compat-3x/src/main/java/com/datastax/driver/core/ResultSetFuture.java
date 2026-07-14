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

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** A future on a {@link ResultSet}. */
public interface ResultSetFuture extends ListenableFuture<ResultSet> {

  /**
   * Waits for the query to return and return its result.
   *
   * @return the query result.
   */
  public ResultSet getUninterruptibly();

  /**
   * Waits for the provided time for the query to return and return its result if available.
   *
   * @param timeout the maximum time to wait.
   * @param unit the unit for {@code timeout}.
   * @return the query result.
   * @throws TimeoutException if the wait timed out.
   */
  public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Attempts to cancel the execution of the request corresponding to this future.
   *
   * @param mayInterruptIfRunning whether the thread executing the task should be interrupted.
   * @return {@code false} if the task could not be cancelled, {@code true} otherwise.
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning);
}
