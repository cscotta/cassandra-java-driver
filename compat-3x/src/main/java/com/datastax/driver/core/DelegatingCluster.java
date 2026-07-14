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
import java.util.Collections;

/**
 * Base class for custom {@link Cluster} implementations that wrap another instance.
 *
 * <p>All public methods forward to {@link #delegate()}. The constructor builds a dummy base {@code
 * Cluster} that never allocates resources (null configuration, empty contact points) and eagerly
 * marks it closed via {@code super.closeAsync()}.
 */
public abstract class DelegatingCluster extends Cluster {

  protected DelegatingCluster() {
    super("delegating_cluster", Collections.<EndPoint>emptyList(), null);
    // Make sure the underlying dummy Cluster never manages any resource.
    super.closeAsync().force();
  }

  /** Returns the delegate instance that all methods forward to. */
  protected abstract Cluster delegate();

  @Override
  public Cluster init() {
    delegate().init();
    return this;
  }

  @Override
  public Session newSession() {
    return delegate().newSession();
  }

  @Override
  public Session connect() {
    return delegate().connect();
  }

  @Override
  public Session connect(String keyspace) {
    return delegate().connect(keyspace);
  }

  @Override
  public ListenableFuture<Session> connectAsync() {
    return delegate().connectAsync();
  }

  @Override
  public ListenableFuture<Session> connectAsync(String keyspace) {
    return delegate().connectAsync(keyspace);
  }

  @Override
  public Metadata getMetadata() {
    return delegate().getMetadata();
  }

  @Override
  public Configuration getConfiguration() {
    return delegate().getConfiguration();
  }

  @Override
  public Metrics getMetrics() {
    return delegate().getMetrics();
  }

  @Override
  public Cluster register(Host.StateListener listener) {
    delegate().register(listener);
    return this;
  }

  @Override
  public Cluster unregister(Host.StateListener listener) {
    delegate().unregister(listener);
    return this;
  }

  @Override
  public Cluster register(LatencyTracker tracker) {
    delegate().register(tracker);
    return this;
  }

  @Override
  public Cluster unregister(LatencyTracker tracker) {
    delegate().unregister(tracker);
    return this;
  }

  @Override
  public Cluster register(SchemaChangeListener listener) {
    delegate().register(listener);
    return this;
  }

  @Override
  public Cluster unregister(SchemaChangeListener listener) {
    delegate().unregister(listener);
    return this;
  }

  @Override
  public CloseFuture closeAsync() {
    return delegate().closeAsync();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public boolean isClosed() {
    return delegate().isClosed();
  }
}
