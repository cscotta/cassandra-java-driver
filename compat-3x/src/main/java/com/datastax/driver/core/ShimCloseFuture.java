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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Concrete package-private {@link CloseFuture} backing every {@code closeAsync()} in the shim. It
 * wraps a 4.x {@code CompletionStage<Void>} (from {@code AsyncAutoCloseable.closeAsync()}) and, when
 * that stage completes, drives the inherited Guava {@code AbstractFuture} state via {@code
 * set(null)}/{@code setException(t)}. {@link #force()} triggers {@code CqlSession.forceCloseAsync()}
 * on every owning session (mirroring 3.x {@code ClusterCloseFuture.force()} forwarding to each child)
 * and returns {@code this}.
 */
final class ShimCloseFuture extends CloseFuture {

  private final List<CqlSession> owners; // may be empty, never null

  ShimCloseFuture(CompletionStage<Void> stage, CqlSession owner) {
    this.owners =
        owner == null ? Collections.<CqlSession>emptyList() : Collections.singletonList(owner);
    whenComplete(stage);
  }

  ShimCloseFuture(CompletionStage<Void> stage, List<CqlSession> owners) {
    this.owners = owners == null ? Collections.<CqlSession>emptyList() : owners;
    whenComplete(stage);
  }

  private void whenComplete(CompletionStage<Void> stage) {
    stage.whenComplete(
        (v, t) -> {
          if (t != null) {
            setException(unwrap(t));
          } else {
            set(null);
          }
        });
  }

  private ShimCloseFuture() {
    this.owners = Collections.emptyList();
    set(null);
  }

  /** An already-completed close future, for a Cluster/Session that never opened resources. */
  static CloseFuture immediate() {
    return new ShimCloseFuture();
  }

  @Override
  public CloseFuture force() {
    for (CqlSession owner : owners) {
      if (owner != null) {
        owner.forceCloseAsync();
      }
    }
    return this;
  }

  private static Throwable unwrap(Throwable t) {
    if (t instanceof java.util.concurrent.CompletionException && t.getCause() != null) {
      return t.getCause();
    }
    return t;
  }

  /** Convenience for building from a {@link CompletableFuture} shaped stage. */
  static CloseFuture of(CompletionStage<Void> stage, CqlSession owner) {
    return new ShimCloseFuture(stage, owner);
  }
}
