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

import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Describes a keyspace. Shim facade over the 4.x {@code KeyspaceMetadata}. */
public class KeyspaceMetadata {

  public static final String KS_NAME = "keyspace_name";

  final com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata delegate;
  private final Metadata root;

  KeyspaceMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata delegate, Metadata root) {
    this.delegate = delegate;
    this.root = root;
  }

  /** The name of this keyspace. */
  public String getName() {
    return delegate.getName().asInternal();
  }

  /** Whether durable writes are set on this keyspace. */
  public boolean isDurableWrites() {
    return delegate.isDurableWrites();
  }

  /** Whether this keyspace is a virtual keyspace. */
  public boolean isVirtual() {
    return delegate.isVirtual();
  }

  /** The replication options for this keyspace. */
  public Map<String, String> getReplication() {
    return delegate.getReplication();
  }

  /** Returns the table with the given name, or {@code null} if it doesn't exist. */
  public TableMetadata getTable(String name) {
    com.datastax.oss.driver.api.core.metadata.schema.TableMetadata t =
        delegate.getTables().get(CqlIdentifier.fromInternal(Metadata.handleId(name)));
    return t == null ? null : new TableMetadata(t, this);
  }

  /** Returns the tables of this keyspace. */
  public Collection<TableMetadata> getTables() {
    List<TableMetadata> result = new ArrayList<TableMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.TableMetadata t :
        delegate.getTables().values()) {
      result.add(new TableMetadata(t, this));
    }
    return result;
  }

  /** Returns the materialized view with the given name, or {@code null} if it doesn't exist. */
  public MaterializedViewMetadata getMaterializedView(String name) {
    com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata v =
        delegate.getViews().get(CqlIdentifier.fromInternal(Metadata.handleId(name)));
    return v == null ? null : new MaterializedViewMetadata(v, this);
  }

  /** Returns the materialized views of this keyspace. */
  public Collection<MaterializedViewMetadata> getMaterializedViews() {
    List<MaterializedViewMetadata> result = new ArrayList<MaterializedViewMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata v :
        delegate.getViews().values()) {
      result.add(new MaterializedViewMetadata(v, this));
    }
    return result;
  }

  /** Returns the user-defined type with the given name, or {@code null} if it doesn't exist. */
  public UserType getUserType(String name) {
    return delegate
        .getUserDefinedType(CqlIdentifier.fromInternal(Metadata.handleId(name)))
        .map(SchemaBridge::toShimUserType)
        .orElse(null);
  }

  /** Returns the user-defined types of this keyspace. */
  public Collection<UserType> getUserTypes() {
    List<UserType> result = new ArrayList<UserType>();
    for (com.datastax.oss.driver.api.core.type.UserDefinedType t :
        delegate.getUserDefinedTypes().values()) {
      result.add(SchemaBridge.toShimUserType(t));
    }
    return result;
  }

  /** Returns the function with the given name and argument types, or {@code null}. */
  public FunctionMetadata getFunction(String name, Collection<DataType> argumentTypes) {
    for (com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata f :
        delegate.getFunctions().values()) {
      if (matches(f.getSignature(), name, argumentTypes)) {
        return new FunctionMetadata(f, this);
      }
    }
    return null;
  }

  /** Returns the function with the given name and argument types, or {@code null}. */
  public FunctionMetadata getFunction(String name, DataType... argumentTypes) {
    return getFunction(name, Arrays.asList(argumentTypes));
  }

  /** Returns the functions of this keyspace. */
  public Collection<FunctionMetadata> getFunctions() {
    List<FunctionMetadata> result = new ArrayList<FunctionMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata f :
        delegate.getFunctions().values()) {
      result.add(new FunctionMetadata(f, this));
    }
    return result;
  }

  /** Returns the aggregate with the given name and argument types, or {@code null}. */
  public AggregateMetadata getAggregate(String name, Collection<DataType> argumentTypes) {
    for (com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata a :
        delegate.getAggregates().values()) {
      if (matches(a.getSignature(), name, argumentTypes)) {
        return new AggregateMetadata(a, this);
      }
    }
    return null;
  }

  /** Returns the aggregate with the given name and argument types, or {@code null}. */
  public AggregateMetadata getAggregate(String name, DataType... argumentTypes) {
    return getAggregate(name, Arrays.asList(argumentTypes));
  }

  /** Returns the aggregates of this keyspace. */
  public Collection<AggregateMetadata> getAggregates() {
    List<AggregateMetadata> result = new ArrayList<AggregateMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata a :
        delegate.getAggregates().values()) {
      result.add(new AggregateMetadata(a, this));
    }
    return result;
  }

  private static boolean matches(
      com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature signature,
      String name,
      Collection<DataType> argumentTypes) {
    if (!signature.getName().asInternal().equals(Metadata.handleId(name))) {
      return false;
    }
    List<com.datastax.oss.driver.api.core.type.DataType> params = signature.getParameterTypes();
    if (params.size() != argumentTypes.size()) {
      return false;
    }
    int i = 0;
    for (DataType arg : argumentTypes) {
      DataType paramShim = SchemaBridge.toShimDataType(params.get(i++));
      if (!paramShim.asFunctionParameterString().equals(arg.asFunctionParameterString())) {
        return false;
      }
    }
    return true;
  }

  /** Returns a CQL query representing this keyspace, including all children. */
  public String exportAsString() {
    return delegate.describeWithChildren(true);
  }

  /** Returns a single CQL statement representing this keyspace (without children). */
  public String asCQLQuery() {
    return delegate.describe(false);
  }

  @Override
  public String toString() {
    return asCQLQuery();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof KeyspaceMetadata)) return false;
    KeyspaceMetadata that = (KeyspaceMetadata) other;
    return MoreObjects.equal(this.getName(), that.getName());
  }

  @Override
  public int hashCode() {
    return getName() == null ? 0 : getName().hashCode();
  }
}
