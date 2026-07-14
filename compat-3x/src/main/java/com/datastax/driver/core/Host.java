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

import com.datastax.shim.bridge.MetadataBridge;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * A Cassandra node, as seen by the driver. Shim facade over the 4.x {@link Node}. A single {@code
 * Host} instance is cached per {@code Node} (see {@code SchemaBridge}) so that {@code equals}/{@code
 * hashCode} are stable across calls.
 */
public class Host {

  final Node node;

  Host(Node node) {
    this.node = node;
  }

  /** The information the driver uses to connect to the node. */
  public EndPoint getEndPoint() {
    final com.datastax.oss.driver.api.core.metadata.EndPoint delegate = node.getEndPoint();
    return new EndPoint() {
      @Override
      public InetSocketAddress resolve() {
        SocketAddress address = delegate.resolve();
        return (address instanceof InetSocketAddress) ? (InetSocketAddress) address : null;
      }

      @Override
      public String toString() {
        return delegate.toString();
      }
    };
  }

  /** The node address, or {@code null} if it cannot be resolved. */
  public InetAddress getAddress() {
    InetSocketAddress socketAddress = getSocketAddress();
    return socketAddress == null ? null : socketAddress.getAddress();
  }

  /** The node socket address. */
  public InetSocketAddress getSocketAddress() {
    SocketAddress address = node.getEndPoint().resolve();
    return (address instanceof InetSocketAddress) ? (InetSocketAddress) address : null;
  }

  /** The node's broadcast RPC address (the address clients use), or {@code null}. */
  public InetSocketAddress getBroadcastRpcAddress() {
    return node.getBroadcastRpcAddress().orElse(null);
  }

  /** The node's broadcast address, or {@code null}. */
  public InetAddress getBroadcastAddress() {
    return node.getBroadcastAddress().map(InetSocketAddress::getAddress).orElse(null);
  }

  /** The node's broadcast socket address, or {@code null}. */
  public InetSocketAddress getBroadcastSocketAddress() {
    return node.getBroadcastAddress().orElse(null);
  }

  /** The node's listen address, or {@code null}. */
  public InetAddress getListenAddress() {
    return node.getListenAddress().map(InetSocketAddress::getAddress).orElse(null);
  }

  /** The node's listen socket address, or {@code null}. */
  public InetSocketAddress getListenSocketAddress() {
    return node.getListenAddress().orElse(null);
  }

  /** The datacenter the node is in. */
  public String getDatacenter() {
    return node.getDatacenter();
  }

  /** The rack the node is in. */
  public String getRack() {
    return node.getRack();
  }

  /** The Cassandra version the node runs, or {@code null} if unknown. */
  public VersionNumber getCassandraVersion() {
    return MetadataBridge.toVersionNumber(node.getCassandraVersion());
  }

  /**
   * UNMAPPED: DSE metadata is not exposed by OSS 4.x. Always returns {@code null} (a DSE driver is
   * required to obtain the DSE version).
   */
  public VersionNumber getDseVersion() {
    return null;
  }

  /** UNMAPPED: DSE metadata is not exposed by OSS 4.x. Always returns {@code null}. */
  public String getDseWorkload() {
    return null;
  }

  /** UNMAPPED: DSE metadata is not exposed by OSS 4.x. Always returns {@code false}. */
  public boolean isDseGraphEnabled() {
    return false;
  }

  /** The node's host id. */
  public UUID getHostId() {
    return node.getHostId();
  }

  /** The node's schema version. */
  public UUID getSchemaVersion() {
    return node.getSchemaVersion();
  }

  /**
   * UNMAPPED: OSS 4.x exposes token ranges (via {@code Metadata.getTokenMap()}), not a per-node raw
   * token set. Returns an empty set.
   */
  public Set<Token> getTokens() {
    return Collections.emptySet();
  }

  /** Whether the node is currently considered up. */
  public boolean isUp() {
    return node.getState() == NodeState.UP;
  }

  /** The node's current state, as a string ({@code ADDED}/{@code UP}/{@code DOWN}). */
  public String getState() {
    return MetadataBridge.stateName(node.getState());
  }

  /**
   * UNMAPPED: 4.x manages reconnection internally and exposes no future. Returns an
   * already-completed future.
   */
  public ListenableFuture<?> getReconnectionAttemptFuture() {
    return Futures.immediateFuture(null);
  }

  /** UNMAPPED: no 4.x equivalent (reconnection is managed internally). No-op. */
  public void tryReconnectOnce() {
    // no-op
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof Host)) return false;
    Host that = (Host) other;
    return this.node.getEndPoint().equals(that.node.getEndPoint());
  }

  @Override
  public int hashCode() {
    return node.getEndPoint().hashCode();
  }

  @Override
  public String toString() {
    return node.toString();
  }

  /**
   * Interface for objects that are interested in tracking the state of the nodes of a cluster.
   *
   * <p>Shim: an adapter over the 4.x {@code NodeStateListener} forwards node events (see {@code
   * SchemaBridge}); {@code onRegister}/{@code onUnregister} are driven by the shim {@code Cluster}.
   */
  public interface StateListener {

    void onAdd(Host host);

    void onUp(Host host);

    void onDown(Host host);

    void onRemove(Host host);

    void onRegister(Cluster cluster);

    void onUnregister(Cluster cluster);
  }

  /**
   * A {@link StateListener} that is also notified when it is registered with, or unregistered from,
   * a {@link Cluster}.
   */
  public interface LifecycleAwareStateListener extends StateListener {

    @Override
    void onRegister(Cluster cluster);

    @Override
    void onUnregister(Cluster cluster);
  }
}
