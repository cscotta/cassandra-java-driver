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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import java.util.Collection;
import java.util.Iterator;

/**
 * The policy that decides which Cassandra hosts to contact for each new query.
 *
 * <p>The {@code LoadBalancingPolicy} is informed of hosts up/down events. For efficiency purposes,
 * the policy is expected to exclude down hosts from query plans.
 */
public interface LoadBalancingPolicy {

  /**
   * Initialize this load balancing policy.
   *
   * @param cluster the {@code Cluster} instance for which the policy is created.
   * @param hosts the initial hosts to use.
   */
  public void init(Cluster cluster, Collection<Host> hosts);

  /**
   * Returns the distance assigned by this policy to the provided host.
   *
   * @param host the host of which to return the distance of.
   * @return the HostDistance to {@code host}.
   */
  public HostDistance distance(Host host);

  /**
   * Returns the hosts to use for a new query.
   *
   * @param loggedKeyspace the currently logged keyspace.
   * @param statement the query for which to build a plan.
   * @return an iterator of Host.
   */
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement);

  /**
   * Called when a new node is added to the cluster.
   *
   * @param host the host that has been newly added.
   */
  void onAdd(Host host);

  /**
   * Called when a node is determined to be up.
   *
   * @param host the host that has been detected up.
   */
  void onUp(Host host);

  /**
   * Called when a node is determined to be down.
   *
   * @param host the host that has been detected down.
   */
  void onDown(Host host);

  /**
   * Called when a node is removed from the cluster.
   *
   * @param host the removed host.
   */
  void onRemove(Host host);

  /** Gets invoked at cluster shutdown. */
  void close();
}
