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
// Shim provenance: net-new bridge — package com.datastax.shim.* is internal shim support,
// not part of the 3.x ABI (japicmp compares only com.datastax.driver.*). See PROVENANCE.md.
package com.datastax.shim.bridge;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Package-agnostic adapters between Java 8 {@link CompletionStage} and Guava's {@link
 * ListenableFuture} / {@link com.google.common.util.concurrent.Future}. Lives outside {@code
 * com.datastax.driver.*} so it is invisible to the 3.x ABI (the 3.x public {@code ResultSetFuture}
 * and its Guava supertype are reproduced by {@code ShimResultSetFuture} defined here).
 */
public final class FutureBridge {

  private FutureBridge() {}

  /** Adapts a {@link CompletionStage} to a Guava {@link ListenableFuture}. */
  public static <T> ListenableFuture<T> toListenableFuture(CompletionStage<T> stage) {
    return new StageBackedFuture<T>(stage);
  }

  /**
   * Builds a 3.x {@link ResultSetFuture} over a {@code CompletionStage<AsyncResultSet>}, using the
   * supplied {@code pager} to turn the resolved (first-page) {@code AsyncResultSet} into a fully
   * paging 3.x {@link ResultSet}.
   */
  public static ResultSetFuture toResultSetFuture(
      CompletionStage<AsyncResultSet> stage, Function<AsyncResultSet, ResultSet> pager) {
    return new ShimResultSetFuture(stage, pager);
  }

  /** Unwraps an {@link ExecutionException} into an unchecked exception (3.x {@code getUninterruptibly} contract). */
  static RuntimeException unwrap(ExecutionException e) {
    Throwable cause = e.getCause();
    // Mirror 3.x DriverThrowables.propagateCause: rethrow a fresh copy so the stack trace points at
    // the calling thread and repeated getUninterruptibly() calls return distinct instances.
    if (cause instanceof com.datastax.driver.core.exceptions.DriverException) {
      return ((com.datastax.driver.core.exceptions.DriverException) cause).copy();
    }
    if (cause instanceof RuntimeException) return (RuntimeException) cause;
    if (cause instanceof Error) throw (Error) cause;
    return new RuntimeException(cause == null ? e : cause);
  }

  /** Guava future backed by a {@link CompletionStage}. */
  private static final class StageBackedFuture<T> extends AbstractFuture<T> {
    private final CompletionStage<T> stage;

    StageBackedFuture(CompletionStage<T> stage) {
      this.stage = stage;
      stage.whenComplete(
          (value, error) -> {
            if (error != null) {
              setException(unwrapCompletion(error));
            } else {
              set(value);
            }
          });
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        stage.toCompletableFuture().cancel(mayInterruptIfRunning);
      }
      return cancelled;
    }
  }

  static Throwable unwrapCompletion(Throwable error) {
    if (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) {
      return error.getCause();
    }
    return error;
  }

  /**
   * 3.x {@link ResultSetFuture} over a {@code CompletionStage<AsyncResultSet>}. The resolved value
   * is the fully-paging 3.x {@link ResultSet} produced by the {@code pager}.
   */
  private static final class ShimResultSetFuture extends AbstractFuture<ResultSet>
      implements ResultSetFuture {

    private final CompletionStage<AsyncResultSet> stage;

    ShimResultSetFuture(
        CompletionStage<AsyncResultSet> stage, Function<AsyncResultSet, ResultSet> pager) {
      this.stage = stage;
      stage.whenComplete(
          (async, error) -> {
            if (error != null) {
              setException(unwrapCompletion(error));
            } else {
              try {
                set(pager.apply(async));
              } catch (RuntimeException | Error e) {
                setException(e);
              }
            }
          });
    }

    @Override
    public ResultSet getUninterruptibly() {
      try {
        return Uninterruptibles.getUninterruptibly(this);
      } catch (ExecutionException e) {
        throw unwrap(e);
      }
    }

    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
      try {
        return Uninterruptibles.getUninterruptibly(this, timeout, unit);
      } catch (ExecutionException e) {
        throw unwrap(e);
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        stage.toCompletableFuture().cancel(mayInterruptIfRunning);
      }
      return cancelled;
    }
  }
}
