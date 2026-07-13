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
package com.datastax.shim.bridge;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.NodeHostBridge;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import java.util.List;

/**
 * Build-time 4.x {@link NodeStateListener} that fans node add/up/down/remove events out to the 3.x
 * {@link Host.StateListener}s registered on the shim {@code Cluster} (via {@code
 * Cluster.register(Host.StateListener)}), translating {@code Node} -&gt; {@code Host} through the
 * cached bridge. Mirrors {@code ShimLatencyRequestTracker}: the list is the live, mutable list owned
 * by the {@code Cluster}, so listeners registered after {@code connect()} are still notified.
 */
public final class ShimNodeStateListener implements NodeStateListener {

  private final List<Host.StateListener> listeners;

  public ShimNodeStateListener(List<Host.StateListener> listeners) {
    this.listeners = listeners;
  }

  @Override
  public void onAdd(Node node) {
    if (listeners.isEmpty()) {
      return;
    }
    Host host = NodeHostBridge.toHost(node);
    for (Host.StateListener l : listeners) {
      try {
        l.onAdd(host);
      } catch (RuntimeException ignored) {
        // a misbehaving listener must not break topology processing
      }
    }
  }

  @Override
  public void onUp(Node node) {
    if (listeners.isEmpty()) {
      return;
    }
    Host host = NodeHostBridge.toHost(node);
    for (Host.StateListener l : listeners) {
      try {
        l.onUp(host);
      } catch (RuntimeException ignored) {
        // ignore
      }
    }
  }

  @Override
  public void onDown(Node node) {
    if (listeners.isEmpty()) {
      return;
    }
    Host host = NodeHostBridge.toHost(node);
    for (Host.StateListener l : listeners) {
      try {
        l.onDown(host);
      } catch (RuntimeException ignored) {
        // ignore
      }
    }
  }

  @Override
  public void onRemove(Node node) {
    if (listeners.isEmpty()) {
      return;
    }
    Host host = NodeHostBridge.toHost(node);
    for (Host.StateListener l : listeners) {
      try {
        l.onRemove(host);
      } catch (RuntimeException ignored) {
        // ignore
      }
    }
  }

  @Override
  public void close() {
    // The listener list is owned by the Cluster; nothing to release here.
  }
}
