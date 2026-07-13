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
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data-center aware Round-robin load balancing policy.
 *
 * <p>This policy provides round-robin queries over the node of the local data center. It also
 * includes in the query plans returned a configurable number of hosts in the remote data centers,
 * but those are always tried after the local nodes.
 */
public class DCAwareRoundRobinPolicy implements LoadBalancingPolicy {

  private static final Logger logger = LoggerFactory.getLogger(DCAwareRoundRobinPolicy.class);

  /**
   * Returns a builder to create a new instance.
   *
   * @return the builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final String UNSET = "";

  private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts =
      new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
  private final AtomicInteger index = new AtomicInteger();

  volatile String localDc;

  private final int usedHostsPerRemoteDc;
  private final boolean dontHopForLocalCL;

  private volatile Configuration configuration;

  private DCAwareRoundRobinPolicy(
      String localDc,
      int usedHostsPerRemoteDc,
      boolean allowRemoteDCsForLocalConsistencyLevel,
      boolean allowEmptyLocalDc) {
    if (!allowEmptyLocalDc && Strings.isNullOrEmpty(localDc))
      throw new IllegalArgumentException("Null or empty data center specified for DC-aware policy");
    this.localDc = localDc == null ? UNSET : localDc;
    this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
    this.dontHopForLocalCL = !allowRemoteDCsForLocalConsistencyLevel;
  }

  /**
   * The configured local datacenter, or {@code null} if it is to be inferred from contact points.
   *
   * <p>Shim-only accessor used by {@code Cluster.Builder} to translate this policy into the 4.x
   * {@code basic.load-balancing-policy.local-datacenter} option. Not part of the 3.x public API.
   */
  public String getLocalDc() {
    //noinspection StringEquality
    return localDc == UNSET ? null : localDc;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    //noinspection StringEquality
    if (localDc != UNSET)
      logger.info("Using provided data-center name '{}' for DCAwareRoundRobinPolicy", localDc);

    this.configuration = cluster.getConfiguration();

    ArrayList<String> notInLocalDC = new ArrayList<String>();

    for (Host host : hosts) {
      String dc = dc(host);

      // If the localDC was in "auto-discover" mode and it's the first host for which we have a DC,
      // use it.
      //noinspection StringEquality
      if (localDc == UNSET && dc != UNSET) {
        logger.info(
            "Using data-center name '{}' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)",
            dc);
        localDc = dc;
      } else if (!dc.equals(localDc))
        notInLocalDC.add(String.format("%s (%s)", host.toString(), dc));

      CopyOnWriteArrayList<Host> prev = perDcLiveHosts.get(dc);
      if (prev == null)
        perDcLiveHosts.put(dc, new CopyOnWriteArrayList<Host>(Collections.singletonList(host)));
      else prev.addIfAbsent(host);
    }

    if (notInLocalDC.size() > 0) {
      String nonLocalHosts = Joiner.on(",").join(notInLocalDC);
      logger.warn(
          "Some contact points don't match local data center. Local DC = {}. Non-conforming contact points: {}",
          localDc,
          nonLocalHosts);
    }

    this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
  }

  private String dc(Host host) {
    String dc = host.getDatacenter();
    return dc == null ? localDc : dc;
  }

  @SuppressWarnings("unchecked")
  private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
    return (CopyOnWriteArrayList<Host>) list.clone();
  }

  @Override
  public HostDistance distance(Host host) {
    String dc = dc(host);
    //noinspection StringEquality
    if (dc == UNSET || dc.equals(localDc)) return HostDistance.LOCAL;

    CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
    if (dcHosts == null || usedHostsPerRemoteDc == 0) return HostDistance.IGNORED;

    // We need to clone, otherwise our subList call is not thread safe
    dcHosts = cloneList(dcHosts);
    return dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc)).contains(host)
        ? HostDistance.REMOTE
        : HostDistance.IGNORED;
  }

  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, final Statement statement) {

    CopyOnWriteArrayList<Host> localLiveHosts = perDcLiveHosts.get(localDc);
    final List<Host> hosts =
        localLiveHosts == null ? Collections.<Host>emptyList() : cloneList(localLiveHosts);
    final int startIdx = index.getAndIncrement();

    return new AbstractIterator<Host>() {

      private int idx = startIdx;
      private int remainingLocal = hosts.size();

      // For remote Dcs
      private Iterator<String> remoteDcs;
      private List<Host> currentDcHosts;
      private int currentDcRemaining;

      @Override
      protected Host computeNext() {
        while (true) {
          if (remainingLocal > 0) {
            remainingLocal--;
            int c = idx++ % hosts.size();
            if (c < 0) {
              c += hosts.size();
            }
            return hosts.get(c);
          }

          if (currentDcHosts != null && currentDcRemaining > 0) {
            currentDcRemaining--;
            int c = idx++ % currentDcHosts.size();
            if (c < 0) {
              c += currentDcHosts.size();
            }
            return currentDcHosts.get(c);
          }

          ConsistencyLevel cl =
              statement.getConsistencyLevel() == null
                  ? configuration.getQueryOptions().getConsistencyLevel()
                  : statement.getConsistencyLevel();

          if (dontHopForLocalCL && cl.isDCLocal()) return endOfData();

          if (remoteDcs == null) {
            Set<String> copy = new HashSet<String>(perDcLiveHosts.keySet());
            copy.remove(localDc);
            remoteDcs = copy.iterator();
          }

          if (!remoteDcs.hasNext()) break;

          String nextRemoteDc = remoteDcs.next();
          CopyOnWriteArrayList<Host> nextDcHosts = perDcLiveHosts.get(nextRemoteDc);
          if (nextDcHosts != null) {
            // Clone for thread safety
            List<Host> dcHosts = cloneList(nextDcHosts);
            currentDcHosts = dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc));
            currentDcRemaining = currentDcHosts.size();
          }
        }
        return endOfData();
      }
    };
  }

  @Override
  public void onUp(Host host) {
    String dc = dc(host);

    // If the localDC was in "auto-discover" mode and it's the first host for which we have a DC,
    // use it.
    //noinspection StringEquality
    if (localDc == UNSET && dc != UNSET) {
      logger.info(
          "Using data-center name '{}' for DCAwareRoundRobinPolicy (if this is incorrect, please provide the correct datacenter name with DCAwareRoundRobinPolicy constructor)",
          dc);
      localDc = dc;
    }

    CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
    if (dcHosts == null) {
      CopyOnWriteArrayList<Host> newMap =
          new CopyOnWriteArrayList<Host>(Collections.singletonList(host));
      dcHosts = perDcLiveHosts.putIfAbsent(dc, newMap);
      // If we've successfully put our new host, we're good, otherwise we've been beaten so continue
      if (dcHosts == null) return;
    }
    dcHosts.addIfAbsent(host);
  }

  @Override
  public void onDown(Host host) {
    CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
    if (dcHosts != null) dcHosts.remove(host);
  }

  @Override
  public void onAdd(Host host) {
    onUp(host);
  }

  @Override
  public void onRemove(Host host) {
    onDown(host);
  }

  @Override
  public void close() {
    // nothing to do
  }

  /** Helper class to build the policy. */
  public static class Builder {
    private String localDc;
    private int usedHostsPerRemoteDc;
    private boolean allowRemoteDCsForLocalConsistencyLevel;

    /**
     * Sets the name of the datacenter that will be considered "local" by the policy.
     *
     * @param localDc the name of the datacenter. It should not be {@code null}.
     * @return this builder.
     */
    public Builder withLocalDc(String localDc) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(localDc),
          "localDc name can't be null or empty. If you want to let the policy autodetect the datacenter, don't call Builder.withLocalDC");
      this.localDc = localDc;
      return this;
    }

    /**
     * Sets the number of hosts per remote datacenter that the policy should consider.
     *
     * @param usedHostsPerRemoteDc the number.
     * @return this builder.
     * @deprecated This functionality will be removed in the next major release of the driver.
     */
    @Deprecated
    public Builder withUsedHostsPerRemoteDc(int usedHostsPerRemoteDc) {
      Preconditions.checkArgument(
          usedHostsPerRemoteDc >= 0, "usedHostsPerRemoteDc must be equal or greater than 0");
      this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
      return this;
    }

    /**
     * Allows the policy to return remote hosts when building query plans for queries having
     * consistency level {@code LOCAL_ONE} or {@code LOCAL_QUORUM}.
     *
     * @return this builder.
     * @deprecated This functionality will be removed in the next major release of the driver.
     */
    @Deprecated
    public Builder allowRemoteDCsForLocalConsistencyLevel() {
      this.allowRemoteDCsForLocalConsistencyLevel = true;
      return this;
    }

    /**
     * Builds the policy configured by this builder.
     *
     * @return the policy.
     */
    public DCAwareRoundRobinPolicy build() {
      if (usedHostsPerRemoteDc == 0 && allowRemoteDCsForLocalConsistencyLevel) {
        logger.warn(
            "Setting allowRemoteDCsForLocalConsistencyLevel has no effect if usedHostsPerRemoteDc = 0. "
                + "This setting will be ignored");
      }
      return new DCAwareRoundRobinPolicy(
          localDc, usedHostsPerRemoteDc, allowRemoteDCsForLocalConsistencyLevel, true);
    }
  }
}
