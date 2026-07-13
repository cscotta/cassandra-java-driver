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

/**
 * Policy that decides how often the reconnection to a dead node is attempted.
 *
 * <p>Each time a node is detected dead, a new {@code ReconnectionSchedule} instance is created
 * (through {@link #newSchedule()}). Then each call to the {@link ReconnectionSchedule#nextDelayMs}
 * method of this instance will decide when the next reconnection attempt to this node will be
 * tried.
 */
public interface ReconnectionPolicy {

  /**
   * Creates a new schedule for reconnection attempts.
   *
   * @return the created schedule.
   */
  public ReconnectionSchedule newSchedule();

  /** Schedules reconnection attempts to a node. */
  public interface ReconnectionSchedule {

    /**
     * When to attempt the next reconnection.
     *
     * @return a time in milliseconds to wait before attempting the next reconnection.
     */
    public long nextDelayMs();
  }

  /**
   * Gets invoked at cluster startup.
   *
   * @param cluster the cluster that this policy is associated with.
   */
  void init(Cluster cluster);

  /** Gets invoked at cluster shutdown. */
  void close();
}
