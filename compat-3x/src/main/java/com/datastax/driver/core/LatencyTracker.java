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
 * Interface for objects that are interested in tracking the latencies of the driver's requests to
 * each Cassandra node.
 *
 * <p>An implementation of this interface can be registered against a {@link Cluster} object through
 * the {@link Cluster#register(LatencyTracker)} method.
 */
public interface LatencyTracker {

  /**
   * A method that is called after each request to a Cassandra node with the duration of that
   * operation.
   *
   * @param host the Cassandra host on which a request has been performed.
   * @param statement the {@link Statement} that has been executed.
   * @param exception an {@link Exception} thrown when receiving the response, or {@code null} if
   *     the response was successful.
   * @param newLatencyNanos the latency in nanoseconds.
   */
  void update(Host host, Statement statement, Exception exception, long newLatencyNanos);

  /**
   * Gets invoked when the tracker is registered with a cluster, or at cluster startup if the
   * tracker was registered at initialization with {@link
   * com.datastax.driver.core.Cluster.Initializer#getInitialListeners()}.
   *
   * @param cluster the cluster that this tracker is registered with.
   */
  void onRegister(Cluster cluster);

  /**
   * Gets invoked when the tracker is unregistered from a cluster, or at cluster shutdown if the
   * tracker was not unregistered.
   *
   * @param cluster the cluster that this tracker was registered with.
   */
  void onUnregister(Cluster cluster);
}
