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
// Shim provenance: net-new — no 3.12.1 counterpart; written for the shim. See PROVENANCE.md.
package com.datastax.driver.core;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.shim.bridge.ExceptionBridge;
import com.datastax.shim.bridge.FutureBridge;
import com.datastax.shim.bridge.StatementBridge;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Concrete {@link Session} over a 4.x {@code CqlSession}. Every request path funnels through {@link
 * AbstractSession} into {@link #executeAsync(Statement)} / {@link #prepareAsync(String, Map)}, which
 * translate to 4.x, execute, and translate results and errors back to 3.x.
 */
final class ShimSession extends AbstractSession {

  private final Cluster cluster;
  final CqlSession session;

  ShimSession(Cluster cluster, CqlSession session) {
    this.cluster = cluster;
    this.session = session;
  }

  @Override
  public String getLoggedKeyspace() {
    Optional<CqlIdentifier> ks = session.getKeyspace();
    return ks.map(CqlIdentifier::asInternal).orElse(null);
  }

  @Override
  public Session init() {
    return this;
  }

  @Override
  public ListenableFuture<Session> initAsync() {
    return Futures.immediateFuture((Session) this);
  }

  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    CompletionStage<AsyncResultSet> stage;
    try {
      com.datastax.oss.driver.api.core.cql.Statement<?> v4 =
          StatementBridge.toV4(statement, session);
      stage = session.executeAsync(v4);
    } catch (RuntimeException e) {
      throw ExceptionBridge.toV3(e);
    }
    // Thread the originating 3.x statement into the ResultSet's ExecutionInfo so
    // getExecutionInfo().getStatement() returns it (the 3.x object-mapper depends on this).
    return FutureBridge.toResultSetFuture(
        translate(stage), async -> ResultSetBridge.wrapAsyncPaged(async, statement));
  }

  @Override
  protected ListenableFuture<PreparedStatement> prepareAsync(
      String query, Map<String, ByteBuffer> customPayload) {
    CompletionStage<com.datastax.oss.driver.api.core.cql.PreparedStatement> stage;
    try {
      if (customPayload != null && !customPayload.isEmpty()) {
        com.datastax.oss.driver.api.core.cql.SimpleStatement ss =
            com.datastax.oss.driver.api.core.cql.SimpleStatement.newInstance(query)
                .setCustomPayload(customPayload);
        stage = session.prepareAsync(ss);
      } else {
        stage = session.prepareAsync(query);
      }
    } catch (RuntimeException e) {
      throw ExceptionBridge.toV3(e);
    }
    CompletionStage<PreparedStatement> mapped =
        translate(stage).thenApply(v4 -> StatementShimAccess.toV3Prepared(v4, session));
    return FutureBridge.toListenableFuture(mapped);
  }

  @Override
  public CloseFuture closeAsync() {
    return new ShimCloseFuture(session.closeAsync(), session);
  }

  @Override
  public boolean isClosed() {
    return session.isClosed();
  }

  @Override
  public Cluster getCluster() {
    return cluster;
  }

  @Override
  public State getState() {
    return new ShimState(this);
  }

  /** Wraps a 4.x completion stage so that any failure is re-thrown as a 3.x shim exception. */
  private static <T> CompletionStage<T> translate(CompletionStage<T> stage) {
    return stage.handle(
        (value, error) -> {
          if (error != null) {
            throw ExceptionBridge.toV3(error);
          }
          return value;
        });
  }

  /** {@link Session.State} derived from the 4.x metadata node view. */
  private static final class ShimState implements State {

    private final ShimSession owner;

    ShimState(ShimSession owner) {
      this.owner = owner;
    }

    @Override
    public Session getSession() {
      return owner;
    }

    @Override
    public Collection<Host> getConnectedHosts() {
      Collection<Host> hosts = new ArrayList<Host>();
      for (Map.Entry<UUID, Node> e : owner.session.getMetadata().getNodes().entrySet()) {
        Node node = e.getValue();
        if (node.getOpenConnections() > 0) {
          hosts.add(new Host(node));
        }
      }
      return hosts;
    }

    @Override
    public int getOpenConnections(Host host) {
      return host == null || host.node == null ? 0 : host.node.getOpenConnections();
    }

    @Override
    public int getTrashedConnections(Host host) {
      return 0;
    }

    @Override
    public int getInFlightQueries(Host host) {
      return 0;
    }
  }
}
