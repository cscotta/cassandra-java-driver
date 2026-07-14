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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Concrete {@link Row} backed by a snapshot of the column definitions plus the raw (unsafe) bytes
 * copied out of a 4.x {@code Row}. Reuses the data-values getter chain ({@link
 * AbstractGettableData}).
 */
final class ShimRow extends AbstractGettableData implements Row {

  private static final Pattern TOKEN_COLUMN_NAME = Pattern.compile("(system\\.)?token(.*)");

  private final ColumnDefinitions metadata;
  private final List<ByteBuffer> data;

  ShimRow(ColumnDefinitions metadata, ProtocolVersion protocolVersion, List<ByteBuffer> data) {
    super(protocolVersion);
    this.metadata = metadata;
    this.data = data;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return metadata;
  }

  @Override
  protected DataType getType(int i) {
    return metadata.getType(i);
  }

  @Override
  protected String getName(int i) {
    return metadata.getName(i);
  }

  @Override
  protected ByteBuffer getValue(int i) {
    return data.get(i);
  }

  @Override
  protected CodecRegistry getCodecRegistry() {
    return metadata.codecRegistry;
  }

  @Override
  protected int getIndexOf(String name) {
    return metadata.getFirstIdx(name);
  }

  @Override
  public Token getToken(int i) {
    Token.Factory factory = tokenFactoryFor(metadata.getType(i));
    checkType(i, factory.getTokenType().getName());

    ByteBuffer value = getValue(i);
    if (value == null || value.remaining() == 0) return null;

    return factory.deserialize(value, protocolVersion);
  }

  @Override
  public Token getToken(String name) {
    return getToken(metadata.getFirstIdx(name));
  }

  @Override
  public Token getPartitionKeyToken() {
    for (int i = 0; i < metadata.size(); i++) {
      if (TOKEN_COLUMN_NAME.matcher(metadata.getName(i)).matches()) return getToken(i);
    }
    throw new IllegalStateException(
        "Found no column named 'token(...)'. If the column is aliased, use getToken(String).");
  }

  // Pick the token factory matching the CQL type of the token column: Murmur3 (bigint),
  // RandomPartitioner (varint) or OrderedPartitioner (blob).
  private static Token.Factory tokenFactoryFor(DataType type) {
    switch (type.getName()) {
      case BIGINT:
        return Token.M3PToken.FACTORY;
      case VARINT:
        return Token.RPToken.FACTORY;
      case BLOB:
        return Token.OPPToken.FACTORY;
      default:
        throw new IllegalStateException(
            "Cannot convert column of type " + type + " to a token; expected bigint, varint or blob");
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Row[");
    for (int i = 0; i < metadata.size(); i++) {
      if (i != 0) sb.append(", ");
      ByteBuffer bb = data.get(i);
      if (bb == null) sb.append("NULL");
      else {
        Object o = getCodecRegistry().codecFor(metadata.getType(i)).deserialize(bb, protocolVersion);
        sb.append(o == null ? "NULL" : o.toString());
      }
    }
    sb.append(']');
    return sb.toString();
  }
}
