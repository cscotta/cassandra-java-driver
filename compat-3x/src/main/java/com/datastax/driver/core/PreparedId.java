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

/**
 * Identifies a prepared statement in a Cassandra cluster.
 *
 * <p>3.x exposed no public members on this type; it exists only as the return type of {@link
 * PreparedStatement#getPreparedId()}. The shim keeps the surface empty and carries, as
 * package-private state, the protocol version and routing-key indexes that {@link BoundStatement}
 * needs when computing routing keys / binding values.
 */
public class PreparedId {

  // Package-private carrier state (not part of the 3.x public ABI).
  final ProtocolVersion protocolVersion;
  final int[] routingKeyIndexes;

  PreparedId(ProtocolVersion protocolVersion, int[] routingKeyIndexes) {
    this.protocolVersion = protocolVersion;
    this.routingKeyIndexes = routingKeyIndexes;
  }
}
