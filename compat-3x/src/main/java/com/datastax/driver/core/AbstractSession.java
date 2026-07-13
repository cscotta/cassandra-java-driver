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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract implementation of the {@link Session} interface.
 *
 * <p>Reproduces the 3.12.1 {@code AbstractSession}: the concrete {@code execute*}/{@code prepare*}
 * methods funnel into the abstract {@link #executeAsync(Statement)} and {@link #prepareAsync(String,
 * Map)} that subclasses implement. Bodies operate purely on shim types.
 */
public abstract class AbstractSession implements Session {

  public AbstractSession() {}

  @Override
  public ResultSet execute(String query) {
    return execute(new SimpleStatement(query));
  }

  @Override
  public ResultSet execute(String query, Object... values) {
    return execute(new SimpleStatement(query, values));
  }

  @Override
  public ResultSet execute(String query, Map<String, Object> values) {
    return execute(new SimpleStatement(query, values));
  }

  @Override
  public ResultSet execute(Statement statement) {
    checkNotInEventLoop();
    return executeAsync(statement).getUninterruptibly();
  }

  @Override
  public ResultSetFuture executeAsync(String query) {
    return executeAsync(new SimpleStatement(query));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    return executeAsync(new SimpleStatement(query, values));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    return executeAsync(new SimpleStatement(query, values));
  }

  @Override
  public PreparedStatement prepare(String query) {
    try {
      return Uninterruptibles.getUninterruptibly(prepareAsync(query));
    } catch (ExecutionException e) {
      throw unwrap(e);
    }
  }

  @Override
  public PreparedStatement prepare(RegularStatement statement) {
    try {
      return Uninterruptibles.getUninterruptibly(prepareAsync(statement));
    } catch (ExecutionException e) {
      throw unwrap(e);
    }
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return prepareAsync(query, null);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(final RegularStatement statement) {
    if (statement.hasValues()) {
      throw new IllegalArgumentException("A statement to prepare should not have values");
    }
    ListenableFuture<PreparedStatement> prepared =
        prepareAsync(statement.getQueryString(), statement.getOutgoingPayload());
    return Futures.transform(
        prepared,
        new Function<PreparedStatement, PreparedStatement>() {
          @Override
          public PreparedStatement apply(PreparedStatement prepared) {
            ConsistencyLevel cl = statement.getConsistencyLevel();
            if (cl != null) {
              prepared.setConsistencyLevel(cl);
            }
            ConsistencyLevel serial = statement.getSerialConsistencyLevel();
            if (serial != null) {
              prepared.setSerialConsistencyLevel(serial);
            }
            if (statement.isTracing()) {
              prepared.enableTracing();
            }
            prepared.setRetryPolicy(statement.getRetryPolicy());
            Boolean idempotent = statement.isIdempotent();
            if (idempotent != null) {
              prepared.setIdempotent(idempotent);
            }
            return prepared;
          }
        },
        MoreExecutors.directExecutor());
  }

  /**
   * Prepares the provided query string, attaching the given custom payload to the {@code PREPARE}
   * request. This is the single abstract method through which all {@code prepare*} calls funnel.
   */
  protected abstract ListenableFuture<PreparedStatement> prepareAsync(
      String query, Map<String, ByteBuffer> customPayload);

  @Override
  public void close() {
    try {
      Uninterruptibles.getUninterruptibly(closeAsync());
    } catch (ExecutionException e) {
      throw unwrap(e);
    }
  }

  /**
   * Checks that the current thread is not one of the driver's I/O event-loop threads.
   *
   * <p>Shim: implemented as a no-op — the shim has no analogue of the 3.x {@code
   * connectionFactory.eventLoopGroup} to inspect.
   */
  public void checkNotInEventLoop() {}

  private static RuntimeException unwrap(ExecutionException e) {
    Throwable cause = e.getCause();
    // Mirror 3.x DriverThrowables.propagateCause: rethrow a fresh copy so the stack trace points at
    // the calling thread and repeated blocking gets return distinct instances.
    if (cause instanceof com.datastax.driver.core.exceptions.DriverException) {
      return ((com.datastax.driver.core.exceptions.DriverException) cause).copy();
    }
    if (cause instanceof RuntimeException) {
      return (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    return new RuntimeException(cause == null ? e : cause);
  }
}
