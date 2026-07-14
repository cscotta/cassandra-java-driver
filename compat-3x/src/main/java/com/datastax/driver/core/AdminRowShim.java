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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Adapts a 4.x {@code system.peers}/{@code system.peers_v2} {@link AdminRow} to the 3.x {@link Row}
 * interface, so a user's 3.x {@code EndPointFactory} can read a peer row when the shim's
 * {@code ShimTopologyMonitor} builds node endpoints.
 *
 * <p>Only the accessors that a peer-row {@code EndPointFactory} actually uses are backed:
 * {@link #getColumnDefinitions()} {@code .contains(name)}, {@link #getInet}, {@link #getInt},
 * {@link #getString}, {@link #getUUID}, {@link #getBytes} and {@link #isNull} — all by column name,
 * delegating to {@link AdminRow}. {@code AdminRow} is name-keyed with no stable column index, so the
 * index-based getter chain (and any name-based accessor not listed above) throws {@link
 * UnsupportedOperationException} — a documented limitation of this bridge.
 */
final class AdminRowShim extends AbstractGettableData implements Row {

  private final AdminRow row;
  private final ColumnDefinitions defs;

  AdminRowShim(AdminRow row) {
    super(ProtocolVersion.NEWEST_SUPPORTED);
    this.row = row;
    this.defs = new AdminRowColumnDefinitions(row);
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return defs;
  }

  @Override
  public boolean isNull(String name) {
    return !row.contains(name) || row.isNull(name);
  }

  @Override
  public int getInt(String name) {
    Integer v = row.getInteger(name);
    return v == null ? 0 : v;
  }

  @Override
  public InetAddress getInet(String name) {
    return row.getInetAddress(name);
  }

  @Override
  public String getString(String name) {
    return row.getString(name);
  }

  @Override
  public UUID getUUID(String name) {
    return row.getUuid(name);
  }

  @Override
  public ByteBuffer getBytes(String name) {
    return row.getByteBuffer(name);
  }

  @Override
  public Token getToken(int i) {
    throw unsupported();
  }

  @Override
  public Token getToken(String name) {
    throw unsupported();
  }

  @Override
  public Token getPartitionKeyToken() {
    throw unsupported();
  }

  // ---- index-based hooks: AdminRow exposes no stable index, so these are unsupported -----------

  @Override
  protected DataType getType(int i) {
    throw unsupported();
  }

  @Override
  protected String getName(int i) {
    throw unsupported();
  }

  @Override
  protected ByteBuffer getValue(int i) {
    throw unsupported();
  }

  @Override
  protected CodecRegistry getCodecRegistry() {
    return CodecRegistry.DEFAULT_INSTANCE;
  }

  @Override
  protected int getIndexOf(String name) {
    throw unsupported();
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException(
        "The shim EndPointFactory bridge only exposes the peer-row columns an EndPointFactory reads "
            + "(getInet/getInt/getString/getUUID/getBytes/isNull by name); this accessor is not "
            + "supported on an AdminRow-backed Row.");
  }

  /** A {@link ColumnDefinitions} whose {@code contains(name)} reflects the backing {@link AdminRow}. */
  private static final class AdminRowColumnDefinitions extends ColumnDefinitions {
    private final AdminRow row;

    AdminRowColumnDefinitions(AdminRow row) {
      super(new Definition[0], CodecRegistry.DEFAULT_INSTANCE);
      this.row = row;
    }

    @Override
    public boolean contains(String name) {
      return row.contains(name);
    }
  }
}
