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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

/**
 * Adapts a 3.x {@link EndPoint} (produced by a user's {@code EndPointFactory}) to the 4.x {@code
 * EndPoint} SPI, so the shim topology monitor can return it. {@link #resolve()} returns the 3.x
 * endpoint's socket address; the metric prefix / equality derive from that resolved address.
 */
final class V4EndPointAdapter implements com.datastax.oss.driver.api.core.metadata.EndPoint {

  private final EndPoint v3;

  V4EndPointAdapter(EndPoint v3) {
    this.v3 = v3;
  }

  @Override
  public SocketAddress resolve() {
    InetSocketAddress address = v3.resolve();
    if (address == null) {
      throw new IllegalStateException("3.x EndPoint " + v3 + " resolved to a null address");
    }
    return address;
  }

  @Override
  public String asMetricPrefix() {
    InetSocketAddress address = v3.resolve();
    if (address == null) {
      return "unknown";
    }
    String host =
        (address.getAddress() != null)
            ? address.getAddress().getHostAddress()
            : address.getHostString();
    return (host + '_' + address.getPort()).replace('.', '_').replace(':', '_');
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof V4EndPointAdapter)) {
      return false;
    }
    return Objects.equals(v3.resolve(), ((V4EndPointAdapter) other).v3.resolve());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(v3.resolve());
  }

  @Override
  public String toString() {
    return String.valueOf(v3.resolve());
  }
}
