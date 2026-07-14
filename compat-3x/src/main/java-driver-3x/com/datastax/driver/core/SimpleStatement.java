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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** A simple {@code RegularStatement} implementation built directly from a query string. */
public class SimpleStatement extends RegularStatement {

  private final String query;
  private final Object[] values;
  private final Map<String, Object> namedValues;

  private volatile ByteBuffer routingKey;
  private volatile String keyspace;

  /**
   * Creates a new {@code SimpleStatement} with the provided query string (and no values).
   *
   * @param query the query string.
   */
  public SimpleStatement(String query) {
    this(query, (Object[]) null);
  }

  /**
   * Creates a new {@code SimpleStatement} with the provided query string and values.
   *
   * @param query the query string.
   * @param values values required for the execution of {@code query}.
   * @throws IllegalArgumentException if the number of values is greater than 65535.
   */
  public SimpleStatement(String query, Object... values) {
    if (values != null && values.length > 65535)
      throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
    this.query = query;
    this.values = values;
    this.namedValues = null;
  }

  /**
   * Creates a new {@code SimpleStatement} with the provided query string and named values.
   *
   * @param query the query string.
   * @param values named values required for the execution of {@code query}.
   * @throws IllegalArgumentException if the number of values is greater than 65535.
   */
  public SimpleStatement(String query, Map<String, Object> values) {
    if (values.size() > 65535)
      throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
    this.query = query;
    this.values = null;
    this.namedValues = values;
  }

  @Override
  public String getQueryString(CodecRegistry codecRegistry) {
    return query;
  }

  @Override
  public ByteBuffer[] getValues(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    if (values == null) return null;
    return convert(values, protocolVersion, codecRegistry);
  }

  @Override
  public Map<String, ByteBuffer> getNamedValues(
      ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    if (namedValues == null) return null;
    return convert(namedValues, protocolVersion, codecRegistry);
  }

  /**
   * The number of values for this statement, that is the size of the array that will be returned by
   * {@code getValues}.
   *
   * @return the number of values.
   */
  public int valuesCount() {
    if (values != null) return values.length;
    else if (namedValues != null) return namedValues.size();
    else return 0;
  }

  @Override
  public boolean hasValues(CodecRegistry codecRegistry) {
    return (values != null && values.length > 0) || (namedValues != null && namedValues.size() > 0);
  }

  @Override
  public boolean usesNamedValues() {
    return namedValues != null && namedValues.size() > 0;
  }

  /**
   * Returns the {@code i}th positional value as the Java type matching its CQL type.
   *
   * @param i the index to retrieve.
   * @return the {@code i}th value of this statement.
   * @throws IllegalStateException if this statement does not have positional values.
   * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
   */
  public Object getObject(int i) {
    if (values == null)
      throw new IllegalStateException("This statement does not have positional values");
    if (i < 0 || i >= values.length) throw new ArrayIndexOutOfBoundsException(i);
    return values[i];
  }

  /**
   * Returns a named value as the Java type matching its CQL type.
   *
   * @param name the name of the value to retrieve.
   * @return the value that matches the name, or {@code null} if there is no such name.
   * @throws IllegalStateException if this statement does not have named values.
   */
  public Object getObject(String name) {
    if (namedValues == null)
      throw new IllegalStateException("This statement does not have named values");
    return namedValues.get(name);
  }

  /**
   * Returns the names of the named values of this statement.
   *
   * @return the names of the named values of this statement.
   * @throws IllegalStateException if this statement does not have named values.
   */
  public Set<String> getValueNames() {
    if (namedValues == null)
      throw new IllegalStateException("This statement does not have named values");
    return Collections.unmodifiableSet(namedValues.keySet());
  }

  /**
   * Returns the routing key for the query.
   *
   * @param protocolVersion unused by this implementation.
   * @param codecRegistry unused by this implementation.
   * @return the routing key set through {@link #setRoutingKey} if such a key was set, {@code null}
   *     otherwise.
   * @see Statement#getRoutingKey
   */
  @Override
  public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    return routingKey;
  }

  /**
   * Sets the routing key for this query.
   *
   * @param routingKey the raw (binary) value to use as routing key.
   * @return this {@code SimpleStatement} object.
   * @see Statement#getRoutingKey
   */
  public SimpleStatement setRoutingKey(ByteBuffer routingKey) {
    this.routingKey = routingKey;
    return this;
  }

  /**
   * Returns the keyspace this query operates on.
   *
   * @return the keyspace set through {@link #setKeyspace} if such keyspace was set, {@code null}
   *     otherwise.
   * @see Statement#getKeyspace
   */
  @Override
  public String getKeyspace() {
    return keyspace;
  }

  /**
   * Sets the keyspace this query operates on.
   *
   * @param keyspace the name of the keyspace this query operates on.
   * @return this {@code SimpleStatement} object.
   * @see Statement#getKeyspace
   */
  public SimpleStatement setKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  /**
   * Sets the routing key for this query.
   *
   * @param routingKeyComponents the raw (binary) values to compose to obtain the routing key.
   * @return this {@code SimpleStatement} object.
   * @see Statement#getRoutingKey
   */
  public SimpleStatement setRoutingKey(ByteBuffer... routingKeyComponents) {
    this.routingKey = compose(routingKeyComponents);
    return this;
  }

  /*
   * This method performs a best-effort heuristic to guess which codec to use.
   */
  private static ByteBuffer[] convert(
      Object[] values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    ByteBuffer[] serializedValues = new ByteBuffer[values.length];
    for (int i = 0; i < values.length; i++) {
      Object value = values[i];
      if (value == null) {
        serializedValues[i] = null;
      } else {
        if (value instanceof Token) {
          serializedValues[i] = ((Token) value).serialize(protocolVersion);
        } else {
          try {
            TypeCodec<Object> codec = codecRegistry.codecFor(value);
            serializedValues[i] = codec.serialize(value, protocolVersion);
          } catch (Exception e) {
            throw new InvalidTypeException(
                String.format(
                    "Value %d of type %s does not correspond to any CQL3 type",
                    i, value.getClass()),
                e);
          }
        }
      }
    }
    return serializedValues;
  }

  private static Map<String, ByteBuffer> convert(
      Map<String, Object> values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    Map<String, ByteBuffer> serializedValues = new HashMap<String, ByteBuffer>();
    for (Map.Entry<String, Object> entry : values.entrySet()) {
      String name = entry.getKey();
      Object value = entry.getValue();
      if (value == null) {
        serializedValues.put(name, null);
      } else {
        if (value instanceof Token) {
          serializedValues.put(name, ((Token) value).serialize(protocolVersion));
        } else {
          try {
            TypeCodec<Object> codec = codecRegistry.codecFor(value);
            serializedValues.put(name, codec.serialize(value, protocolVersion));
          } catch (Exception e) {
            throw new InvalidTypeException(
                String.format(
                    "Value '%s' of type %s does not correspond to any CQL3 type",
                    name, value.getClass()),
                e);
          }
        }
      }
    }
    return serializedValues;
  }

  /**
   * Utility method to assemble different routing key components into a single {@link ByteBuffer}.
   *
   * @param buffers the components of the routing key.
   * @return A ByteBuffer containing the serialized routing key
   */
  static ByteBuffer compose(ByteBuffer... buffers) {
    if (buffers.length == 1) return buffers[0];

    int totalLength = 0;
    for (ByteBuffer bb : buffers) totalLength += 2 + bb.remaining() + 1;

    ByteBuffer out = ByteBuffer.allocate(totalLength);
    for (ByteBuffer buffer : buffers) {
      ByteBuffer bb = buffer.duplicate();
      putShortLength(out, bb.remaining());
      out.put(bb);
      out.put((byte) 0);
    }
    out.flip();
    return out;
  }

  static void putShortLength(ByteBuffer bb, int length) {
    bb.put((byte) ((length >> 8) & 0xFF));
    bb.put((byte) (length & 0xFF));
  }
}
