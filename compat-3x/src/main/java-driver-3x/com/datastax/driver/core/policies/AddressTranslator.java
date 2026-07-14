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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import java.net.InetSocketAddress;

/**
 * Translates IP addresses received from Cassandra nodes into locally queriable addresses.
 *
 * <p>The driver auto-detects new Cassandra nodes added to the cluster. For each node, the address
 * the driver receives corresponds to the {@code rpc_address} set in the node yaml file. This
 * interface allows translating such an address to another address to be used by the driver for
 * connection.
 */
public interface AddressTranslator {

  /**
   * Initializes this address translator.
   *
   * @param cluster the {@code Cluster} instance for which the translator is created.
   */
  void init(Cluster cluster);

  /**
   * Translates a Cassandra {@code rpc_address} to another address if necessary.
   *
   * @param address the address of a node as returned by Cassandra.
   * @return the address the driver should actually use to connect to the node designated by {@code
   *     address}.
   */
  InetSocketAddress translate(InetSocketAddress address);

  /** Called at {@link Cluster} shutdown. */
  void close();
}
