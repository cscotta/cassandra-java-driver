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
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

/**
 * A session holds connections to a Cassandra cluster, allowing it to be queried.
 *
 * <p>Reproduces the 3.12.1 {@code com.datastax.driver.core.Session} public interface (extends {@code
 * java.io.Closeable}). Backed at runtime by a 4.x {@code CqlSession} (see the shim {@code
 * ShimSession}).
 */
public interface Session extends Closeable {

  String getLoggedKeyspace();

  Session init();

  ListenableFuture<Session> initAsync();

  ResultSet execute(String query);

  ResultSet execute(String query, Object... values);

  ResultSet execute(String query, Map<String, Object> values);

  ResultSet execute(Statement statement);

  ResultSetFuture executeAsync(String query);

  ResultSetFuture executeAsync(String query, Object... values);

  ResultSetFuture executeAsync(String query, Map<String, Object> values);

  ResultSetFuture executeAsync(Statement statement);

  PreparedStatement prepare(String query);

  PreparedStatement prepare(RegularStatement statement);

  ListenableFuture<PreparedStatement> prepareAsync(String query);

  ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement);

  CloseFuture closeAsync();

  @Override
  void close();

  boolean isClosed();

  Cluster getCluster();

  State getState();

  /** The state of a {@link Session} as far as the connection pools are concerned. */
  interface State {

    Session getSession();

    Collection<Host> getConnectedHosts();

    int getOpenConnections(Host host);

    int getTrashedConnections(Host host);

    int getInFlightQueries(Host host);
  }
}
