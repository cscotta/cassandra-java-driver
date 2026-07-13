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

import com.datastax.driver.core.SimpleStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Shared helpers for the policy adapters ({@code Shim3xLoadBalancingPolicy}, {@code
 * Shim3xSpeculativeExecutionPolicy}, {@code ShimLatencyRequestTracker}) that need to hand a 4.x
 * {@link Request} to a 3.x policy as a {@code com.datastax.driver.core.Statement}.
 *
 * <p>The 4.x {@code Request} has no faithful 3.x {@code Statement} projection, so a lightweight
 * {@link SimpleStatement} placeholder is built that carries the request's routing key, routing
 * keyspace, and consistency level — enough for a token-aware 3.x policy to route on the same key the
 * 4.x engine computed. Only values that are actually present are copied (3.x {@code unset == null}
 * triggers the policy's own fallback).
 */
public final class RoutingBridge {

  private RoutingBridge() {}

  /** The session (logged) keyspace as a 3.x internal name, or {@code null}. */
  public static String loggedKeyspace(Session session) {
    if (session == null) {
      return null;
    }
    Optional<CqlIdentifier> keyspace = session.getKeyspace();
    return keyspace.isPresent() ? keyspace.get().asInternal() : null;
  }

  /** A faithful 3.x {@link SimpleStatement} carrying the request's routing key / keyspace / CL. */
  public static SimpleStatement placeholderStatement(Request request) {
    SimpleStatement stmt = new SimpleStatement("");
    if (request != null) {
      ByteBuffer routingKey = request.getRoutingKey();
      if (routingKey != null) {
        stmt.setRoutingKey(routingKey);
      }
      CqlIdentifier routingKeyspace = request.getRoutingKeyspace();
      if (routingKeyspace == null) {
        routingKeyspace = request.getKeyspace();
      }
      if (routingKeyspace != null) {
        stmt.setKeyspace(routingKeyspace.asInternal());
      }
      if (request instanceof com.datastax.oss.driver.api.core.cql.Statement) {
        com.datastax.oss.driver.api.core.ConsistencyLevel cl =
            ((com.datastax.oss.driver.api.core.cql.Statement<?>) request).getConsistencyLevel();
        com.datastax.driver.core.ConsistencyLevel v3cl = toV3ConsistencyLevel(cl);
        if (v3cl != null) {
          stmt.setConsistencyLevel(v3cl);
        }
      }
    }
    return stmt;
  }

  /**
   * Maps a 4.x consistency level to its 3.x namesake (both enums share names), or {@code null} when
   * unset or unknown, so the placeholder statement leaves it unset and the 3.x policy applies its
   * own fallback.
   */
  public static com.datastax.driver.core.ConsistencyLevel toV3ConsistencyLevel(
      com.datastax.oss.driver.api.core.ConsistencyLevel cl) {
    if (cl == null) {
      return null;
    }
    try {
      return com.datastax.driver.core.ConsistencyLevel.valueOf(cl.name());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
