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

import com.datastax.driver.core.EndPoint;
import java.net.InetSocketAddress;

/** Simple {@link EndPoint} backed by a fixed {@link InetSocketAddress}. Not part of the 3.x ABI. */
public final class InetSocketAddressEndPoint implements EndPoint {

  private final InetSocketAddress address;

  public InetSocketAddressEndPoint(InetSocketAddress address) {
    this.address = address;
  }

  @Override
  public InetSocketAddress resolve() {
    return address;
  }

  @Override
  public String toString() {
    return String.valueOf(address);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof InetSocketAddressEndPoint)) return false;
    InetSocketAddress other = ((InetSocketAddressEndPoint) o).address;
    return address == null ? other == null : address.equals(other);
  }

  @Override
  public int hashCode() {
    return address == null ? 0 : address.hashCode();
  }
}
