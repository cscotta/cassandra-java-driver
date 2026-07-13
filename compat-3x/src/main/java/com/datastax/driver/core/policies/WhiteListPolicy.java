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

import com.datastax.driver.core.Host;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A load balancing policy wrapper that ensure that only hosts from a provided white list will ever
 * be returned.
 *
 * @see HostFilterPolicy
 */
public class WhiteListPolicy extends HostFilterPolicy {

  /**
   * Creates a new policy that wraps the provided child policy but only "allows" hosts from the
   * provided white list.
   *
   * @param childPolicy the wrapped policy.
   * @param whiteList the white listed hosts.
   */
  public WhiteListPolicy(LoadBalancingPolicy childPolicy, Collection<InetSocketAddress> whiteList) {
    super(childPolicy, buildPredicate(whiteList));
  }

  /**
   * Private constructor solely for maintaining type from policy created by {@link
   * #ofHosts(LoadBalancingPolicy, String...)}.
   */
  private WhiteListPolicy(LoadBalancingPolicy childPolicy, Predicate<Host> predicate) {
    super(childPolicy, predicate);
  }

  private static Predicate<Host> buildPredicate(Collection<InetSocketAddress> whiteList) {
    final ImmutableSet<InetSocketAddress> hosts = ImmutableSet.copyOf(whiteList);
    return new Predicate<Host>() {
      @Override
      public boolean apply(Host host) {
        InetSocketAddress socketAddress = host.getEndPoint().resolve();
        return hosts.contains(socketAddress);
      }
    };
  }

  /**
   * Creates a new policy with the given host names.
   *
   * <p>See {@link #ofHosts(LoadBalancingPolicy, Iterable)} for more details.
   */
  public static WhiteListPolicy ofHosts(LoadBalancingPolicy childPolicy, String... hostnames) {
    return ofHosts(childPolicy, Arrays.asList(hostnames));
  }

  /**
   * Creates a new policy that wraps the provided child policy but only "allows" hosts having
   * addresses that match those from the resolved input host names.
   *
   * @param childPolicy the wrapped policy.
   * @param hostnames list of host names to resolve whitelisted addresses from.
   * @throws IllegalArgumentException if any of the given {@code hostnames} could not be resolved.
   * @throws NullPointerException If null was provided for a hostname.
   */
  public static WhiteListPolicy ofHosts(
      LoadBalancingPolicy childPolicy, Iterable<String> hostnames) {
    ImmutableSet.Builder<InetAddress> builder = ImmutableSet.builder();
    for (String hostname : hostnames) {
      try {
        if (hostname == null) throw new NullPointerException();
        builder.add(InetAddress.getAllByName(hostname));
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Failed to resolve: " + hostname, e);
      }
    }
    final ImmutableSet<InetAddress> addresses = builder.build();
    return new WhiteListPolicy(
        childPolicy,
        new Predicate<Host>() {
          @Override
          public boolean apply(Host host) {
            InetSocketAddress socketAddress = host.getEndPoint().resolve();
            return addresses.contains(socketAddress.getAddress());
          }
        });
  }
}
