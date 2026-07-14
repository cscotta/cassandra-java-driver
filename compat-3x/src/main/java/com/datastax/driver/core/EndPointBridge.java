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
// Shim provenance: net-new — no 3.12.1 counterpart; written for the shim. See PROVENANCE.md.
package com.datastax.driver.core;

import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Public bridge letting the shim topology monitor (in {@code com.datastax.shim.bridge}) drive a
 * user's 3.x {@link EndPointFactory}, which requires the package-private {@link AdminRowShim} /
 * {@link V4EndPointAdapter} wrappers that live here in {@code com.datastax.driver.core}.
 *
 * <p>Not part of the 3.12.1 ABI: this class has no counterpart in the reference jars, so it is a
 * purely additive class and does not affect the japicmp comparison.
 */
public final class EndPointBridge {

  private EndPointBridge() {}

  private static final Set<EndPointFactory> INITED =
      Collections.synchronizedSet(
          Collections.newSetFromMap(new IdentityHashMap<EndPointFactory, Boolean>()));

  /** Calls {@code factory.init(cluster)} exactly once per factory instance (3.x semantics). */
  public static void initFactory(EndPointFactory factory, Cluster cluster) {
    if (factory != null && INITED.add(factory)) {
      try {
        factory.init(cluster);
      } catch (RuntimeException e) {
        // A factory that drives I/O from init is a documented caveat; don't fail the build.
      }
    }
  }

  /**
   * Wraps the peer {@link AdminRow} as a 3.x {@link Row}, invokes {@code factory.create(...)}, and
   * adapts the returned 3.x {@link EndPoint} to a 4.x endpoint (or {@code null} if the factory
   * decided the peer is unusable).
   */
  public static com.datastax.oss.driver.api.core.metadata.EndPoint buildV4EndPoint(
      EndPointFactory factory, AdminRow row) {
    EndPoint v3 = factory.create(new AdminRowShim(row));
    return v3 == null ? null : new V4EndPointAdapter(v3);
  }
}
