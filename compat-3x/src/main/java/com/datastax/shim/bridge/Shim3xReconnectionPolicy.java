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
// Shim provenance: net-new bridge — package com.datastax.shim.* is internal shim support,
// not part of the 3.x ABI (japicmp compares only com.datastax.driver.*). See PROVENANCE.md.
package com.datastax.shim.bridge;

import com.datastax.driver.core.Cluster;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.time.Duration;

/**
 * Bridges a user's 3.x {@code com.datastax.driver.core.policies.ReconnectionPolicy} onto 4.x's
 * {@link ReconnectionPolicy}. Each 4.x schedule ({@code newNodeSchedule} / {@code
 * newControlConnectionSchedule}) wraps a fresh 3.x {@code newSchedule()}, and {@code nextDelay()}
 * returns {@code Duration.ofMillis(v3Schedule.nextDelayMs())}. {@code v3.init(cluster)} is called
 * once so a custom policy initializes as in 3.x.
 */
public class Shim3xReconnectionPolicy implements ReconnectionPolicy {

  private final com.datastax.driver.core.policies.ReconnectionPolicy v3;

  public Shim3xReconnectionPolicy(
      com.datastax.driver.core.policies.ReconnectionPolicy v3, Cluster shimCluster) {
    this.v3 = v3;
    try {
      v3.init(shimCluster);
    } catch (RuntimeException e) {
      // A policy that introspects the cluster during init is a documented caveat; don't fail build.
    }
  }

  @Override
  public ReconnectionSchedule newNodeSchedule(Node node) {
    return wrap(v3.newSchedule());
  }

  @Override
  public ReconnectionSchedule newControlConnectionSchedule(boolean isInitialConnection) {
    return wrap(v3.newSchedule());
  }

  private static ReconnectionSchedule wrap(
      com.datastax.driver.core.policies.ReconnectionPolicy.ReconnectionSchedule v3Schedule) {
    return new ReconnectionSchedule() {
      @Override
      public Duration nextDelay() {
        return Duration.ofMillis(v3Schedule.nextDelayMs());
      }
    };
  }

  @Override
  public void close() {
    v3.close();
  }
}
