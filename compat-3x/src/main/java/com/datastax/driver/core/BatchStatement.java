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
// Shim provenance: 3.12.1 base + shim facade edits — net-new shim lines are layered on
// top of the carried-forward 3.12.1 source. For the exact added/changed lines see
// PROVENANCE.md and `src/test/scripts/provenance-diff.sh` (diff vs tag shim-3x-vendor-3.12.1).
package com.datastax.driver.core;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A statement that groups a number of {@link Statement} so they get executed as a batch.
 *
 * <p>Setting a BatchStatement's serial consistency level is only supported with the native protocol
 * version 3 or higher (see {@link #setSerialConsistencyLevel(ConsistencyLevel)}).
 */
public class BatchStatement extends Statement {

  /** The type of batch to use. */
  public enum Type {
    /**
     * A logged batch: Cassandra will first write the batch to its distributed batch log to ensure
     * the atomicity of the batch.
     */
    LOGGED,

    /**
     * A batch that doesn't use Cassandra's distributed batch log. Such batch are not guaranteed to
     * be atomic.
     */
    UNLOGGED,

    /**
     * A counter batch. Note that such batch is the only type that can contain counter operations
     * and it can only contain these.
     */
    COUNTER
  }

  final Type batchType;
  private final List<Statement> statements = new ArrayList<Statement>();

  /** Creates a new {@code LOGGED} batch statement. */
  public BatchStatement() {
    this(Type.LOGGED);
  }

  /**
   * Creates a new batch statement of the provided type.
   *
   * @param batchType the type of batch.
   */
  public BatchStatement(Type batchType) {
    this.batchType = batchType;
  }

  /**
   * Adds a new statement to this batch.
   *
   * @param statement the new statement to add.
   * @return this batch statement.
   * @throws IllegalStateException if adding the new statement means that this {@code
   *     BatchStatement} has more than 65536 statements.
   * @throws IllegalArgumentException if adding a regular statement that uses named values.
   */
  public BatchStatement add(Statement statement) {
    if (statement instanceof StatementWrapper) {
      statement = ((StatementWrapper) statement).getWrappedStatement();
    }
    if ((statement instanceof RegularStatement)
        && ((RegularStatement) statement).usesNamedValues()) {
      throw new IllegalArgumentException(
          "Batch statement cannot contain regular statements with named values ("
              + ((RegularStatement) statement).getQueryString()
              + ")");
    }

    // We handle BatchStatement here (rather than in getIdAndValues) as it make it slightly
    // easier to avoid endless loops if the user mistakenly passes a batch that depends on this
    // object (or this directly).
    if (statement instanceof BatchStatement) {
      for (Statement subStatements : ((BatchStatement) statement).statements) {
        add(subStatements);
      }
    } else {
      if (statements.size() >= 0xFFFF)
        throw new IllegalStateException(
            "Batch statement cannot contain more than " + 0xFFFF + " statements.");
      statements.add(statement);
    }
    return this;
  }

  /**
   * Adds multiple statements to this batch.
   *
   * @param statements the statements to add.
   * @return this batch statement.
   */
  public BatchStatement addAll(Iterable<? extends Statement> statements) {
    for (Statement statement : statements) add(statement);
    return this;
  }

  /**
   * The statements that have been added to this batch so far.
   *
   * @return an (immutable) collection of the statements that have been added to this batch so far.
   */
  public Collection<Statement> getStatements() {
    return ImmutableList.copyOf(statements);
  }

  /**
   * Clears this batch, removing all statements added so far.
   *
   * @return this (now empty) {@code BatchStatement}.
   */
  public BatchStatement clear() {
    statements.clear();
    return this;
  }

  /**
   * Returns the number of elements in this batch.
   *
   * @return the number of elements in this batch.
   */
  public int size() {
    return statements.size();
  }

  @Override
  public int requestSizeInBytes(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    // Exact wire size relies on internal 3.x encoders not present in the 4.x public surface; the
    // 3.x contract allows returning -1 when the size cannot be calculated.
    return -1;
  }

  /**
   * Sets the serial consistency level for the query.
   *
   * @param serialConsistency the serial consistency level to set.
   * @return this {@code Statement} object.
   * @throws IllegalArgumentException if {@code serialConsistency} is not one of {@code
   *     ConsistencyLevel.SERIAL} or {@code ConsistencyLevel.LOCAL_SERIAL}.
   * @see Statement#setSerialConsistencyLevel(ConsistencyLevel)
   */
  @Override
  public BatchStatement setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
    return (BatchStatement) super.setSerialConsistencyLevel(serialConsistency);
  }

  @Override
  public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    for (Statement statement : statements) {
      if (statement instanceof StatementWrapper)
        statement = ((StatementWrapper) statement).getWrappedStatement();
      ByteBuffer rk = statement.getRoutingKey(protocolVersion, codecRegistry);
      if (rk != null) return rk;
    }
    return null;
  }

  @Override
  public String getKeyspace() {
    for (Statement statement : statements) {
      String keyspace = statement.getKeyspace();
      if (keyspace != null) return keyspace;
    }
    return null;
  }

  @Override
  public Boolean isIdempotent() {
    if (idempotent != null) {
      return idempotent;
    }
    return isBatchIdempotent(statements);
  }

  void ensureAllSet() {
    for (Statement statement : statements)
      if (statement instanceof BoundStatement) ((BoundStatement) statement).ensureAllSet();
  }
}
