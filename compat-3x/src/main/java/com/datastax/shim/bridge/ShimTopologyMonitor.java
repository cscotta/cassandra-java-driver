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
import com.datastax.driver.core.EndPointBridge;
import com.datastax.driver.core.EndPointFactory;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import java.net.InetSocketAddress;

/**
 * A {@link DefaultTopologyMonitor} that honors a user's 3.x {@code EndPointFactory} by overriding
 * the 4.x-documented {@code buildNodeEndPoint} extension point: for a peer row, it hands the row to
 * the 3.x factory (via {@link EndPointBridge}) and returns the adapted endpoint. Control/local rows
 * and the case where the factory returns {@code null} fall back to the stock implementation.
 */
public class ShimTopologyMonitor extends DefaultTopologyMonitor {

  private final EndPointFactory v3Factory;

  public ShimTopologyMonitor(
      InternalDriverContext context, Cluster shimCluster, EndPointFactory v3Factory) {
    super(context);
    this.v3Factory = v3Factory;
    // Initialize the factory once, as 3.x does at cluster startup, before it builds endpoints.
    EndPointBridge.initFactory(v3Factory, shimCluster);
  }

  @Override
  protected EndPoint buildNodeEndPoint(
      AdminRow row, InetSocketAddress broadcastRpcAddress, EndPoint localEndPoint) {
    if (v3Factory != null && row != null && row.contains("peer")) {
      EndPoint adapted = EndPointBridge.buildV4EndPoint(v3Factory, row);
      if (adapted != null) {
        return adapted;
      }
    }
    return super.buildNodeEndPoint(row, broadcastRpcAddress, localEndPoint);
  }
}
