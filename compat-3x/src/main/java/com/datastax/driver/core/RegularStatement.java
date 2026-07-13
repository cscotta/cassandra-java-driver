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
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A regular (non-prepared and non batched) CQL statement.
 *
 * <p>This class represents a query string along with query options (and optionally binary values,
 * see {@code getValues}). It can be extended but {@link SimpleStatement} is provided as a simple
 * implementation to build a {@code RegularStatement} directly from its query string.
 */
public abstract class RegularStatement extends Statement {

  /** Creates a new RegularStatement. */
  protected RegularStatement() {}

  /**
   * Returns the query string for this statement.
   *
   * @param codecRegistry the codec registry that will be used if the actual implementation needs to
   *     serialize Java objects in the process of generating the query.
   * @return a valid CQL query string.
   * @see #getQueryString()
   */
  public abstract String getQueryString(CodecRegistry codecRegistry);

  /**
   * Returns the query string for this statement.
   *
   * <p>This method calls {@link #getQueryString(CodecRegistry)} with {@link
   * CodecRegistry#DEFAULT_INSTANCE}.
   *
   * @return a valid CQL query string.
   */
  public String getQueryString() {
    return getQueryString(CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * The positional values to use for this statement.
   *
   * @param protocolVersion the protocol version that will be used to serialize the values.
   * @param codecRegistry the codec registry that will be used to serialize the values.
   * @see SimpleStatement#SimpleStatement(String, Object...)
   */
  public abstract ByteBuffer[] getValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

  /**
   * The named values to use for this statement.
   *
   * @param protocolVersion the protocol version that will be used to serialize the values.
   * @param codecRegistry the codec registry that will be used to serialize the values.
   * @return the named values.
   * @see SimpleStatement#SimpleStatement(String, Map)
   */
  public abstract Map<String, ByteBuffer> getNamedValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry);

  /**
   * Whether or not this statement has values, that is if {@code getValues} will return {@code null}
   * or not.
   *
   * @param codecRegistry the codec registry that will be used if the actual implementation needs to
   *     serialize Java objects in the process of determining if the query has values.
   * @return {@code false} if both {@link #getValues(ProtocolVersion, CodecRegistry)} and {@link
   *     #getNamedValues(ProtocolVersion, CodecRegistry)} return {@code null}, {@code true}
   *     otherwise.
   * @see #hasValues()
   */
  public abstract boolean hasValues(CodecRegistry codecRegistry);

  /**
   * Whether this statement uses named values.
   *
   * @return {@code false} if {@link #getNamedValues(ProtocolVersion, CodecRegistry)} returns {@code
   *     null}, {@code true} otherwise.
   */
  public abstract boolean usesNamedValues();

  /**
   * Whether or not this statement has values, that is if {@code getValues} will return {@code null}
   * or not.
   *
   * <p>This method calls {@link #hasValues(CodecRegistry)} with {@link
   * CodecRegistry#DEFAULT_INSTANCE}.
   *
   * @return {@code false} if {@link #getValues} returns {@code null}, {@code true} otherwise.
   */
  public boolean hasValues() {
    return hasValues(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Override
  public int requestSizeInBytes(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    // The exact 3.x computation relies on internal wire encoders (CBUtil / Frame.Header /
    // Requests.QueryFlag) that are not part of the 4.x public surface. The 3.x contract allows
    // returning -1 when the size cannot be calculated.
    return -1;
  }

  /**
   * Returns this statement as a CQL query string.
   *
   * @return this statement as a CQL query string.
   * @see #getQueryString()
   */
  @Override
  public String toString() {
    return getQueryString();
  }
}
