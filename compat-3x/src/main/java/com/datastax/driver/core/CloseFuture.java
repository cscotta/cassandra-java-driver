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

import com.google.common.util.concurrent.AbstractFuture;

/**
 * A future on the shutdown of a {@link Cluster} or {@link Session} instance.
 *
 * <p>Reproduces the 3.12.1 public shape: a {@code public abstract class} extending Guava's {@code
 * AbstractFuture<Void>} with a single public abstract {@link #force()} method. The concrete shim
 * implementation is {@link ShimCloseFuture}, which bridges a 4.x {@code
 * CompletionStage<Void>} (from {@code AsyncAutoCloseable.closeAsync()}) into this Guava future.
 */
public abstract class CloseFuture extends AbstractFuture<Void> {

  CloseFuture() {}

  /**
   * Try to force the completion of the shutdown this is a future of.
   *
   * @return this {@code CloseFuture}.
   */
  public abstract CloseFuture force();
}
