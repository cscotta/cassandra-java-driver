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

import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Shim-internal accessor that exposes the package-private {@code Node}&lt;-&gt;{@code Host} mapping
 * to the 4.x load-balancing adapter ({@code com.datastax.shim.bridge.Shim3xLoadBalancingPolicy}),
 * which lives in a different package and so cannot reach {@link Host}'s package-private constructor
 * or {@code node} field directly.
 *
 * <p>Not part of the 3.12.1 ABI: this class has no counterpart in the reference 3.12.1 jars, so it
 * is a purely additive (binary- and source-compatible) class and does not affect the japicmp
 * comparison, which only reports removals/modifications of pre-existing members.
 */
public final class NodeHostBridge {

  private NodeHostBridge() {}

  /**
   * Wraps a 4.x {@link Node} in the cached shim {@link Host} (stable identity per node, shared with
   * the schema-metadata shim).
   */
  public static Host toHost(Node node) {
    return SchemaBridge.toHost(node);
  }

  /** Returns the 4.x {@link Node} backing a shim {@link Host}, or {@code null}. */
  public static Node toNode(Host host) {
    return host == null ? null : host.node;
  }
}
