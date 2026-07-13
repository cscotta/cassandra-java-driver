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

import com.datastax.driver.core.utils.MoreObjects;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Describes a user-defined aggregate. Shim facade over the 4.x {@code AggregateMetadata}. */
public class AggregateMetadata {

  private final com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata delegate;
  private final KeyspaceMetadata keyspace;

  AggregateMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata delegate,
      KeyspaceMetadata keyspace) {
    this.delegate = delegate;
    this.keyspace = keyspace;
  }

  /** Returns a CQL query representing this aggregate. */
  public String exportAsString() {
    return delegate.describeWithChildren(true);
  }

  /** Returns a single CQL statement representing this aggregate. */
  public String asCQLQuery() {
    return delegate.describe(false);
  }

  @Override
  public String toString() {
    return asCQLQuery();
  }

  /** The keyspace this aggregate belongs to. */
  public KeyspaceMetadata getKeyspace() {
    return keyspace;
  }

  /** The CQL signature of this aggregate, e.g. {@code sum(int)}. */
  public String getSignature() {
    StringBuilder sb = new StringBuilder();
    sb.append(delegate.getSignature().getName().asInternal()).append('(');
    List<com.datastax.oss.driver.api.core.type.DataType> types =
        delegate.getSignature().getParameterTypes();
    for (int i = 0; i < types.size(); i++) {
      if (i > 0) sb.append(',');
      sb.append(SchemaBridge.toShimDataType(types.get(i)).asFunctionParameterString());
    }
    sb.append(')');
    return sb.toString();
  }

  /** The simple name of this aggregate (without arguments). */
  public String getSimpleName() {
    return delegate.getSignature().getName().asInternal();
  }

  /** The argument types of this aggregate. */
  public List<DataType> getArgumentTypes() {
    List<com.datastax.oss.driver.api.core.type.DataType> types =
        delegate.getSignature().getParameterTypes();
    List<DataType> result = new ArrayList<DataType>(types.size());
    for (com.datastax.oss.driver.api.core.type.DataType t : types) {
      result.add(SchemaBridge.toShimDataType(t));
    }
    return result;
  }

  /** The final function of this aggregate, or {@code null} if there is none. */
  public FunctionMetadata getFinalFunc() {
    Optional<FunctionSignature> sig = delegate.getFinalFuncSignature();
    return sig.isPresent() ? resolveFunction(sig.get()) : null;
  }

  /** The initial condition of this aggregate, or {@code null} if there is none. */
  public Object getInitCond() {
    return delegate.getInitCond().orElse(null);
  }

  /** The return type of this aggregate. */
  public DataType getReturnType() {
    return SchemaBridge.toShimDataType(delegate.getReturnType());
  }

  /** The state function of this aggregate. */
  public FunctionMetadata getStateFunc() {
    return resolveFunction(delegate.getStateFuncSignature());
  }

  /** The state type of this aggregate. */
  public DataType getStateType() {
    return SchemaBridge.toShimDataType(delegate.getStateType());
  }

  private FunctionMetadata resolveFunction(FunctionSignature signature) {
    if (keyspace == null) return null;
    com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata f =
        keyspace.delegate.getFunctions().get(signature);
    return f == null ? null : new FunctionMetadata(f, keyspace);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof AggregateMetadata)) return false;
    AggregateMetadata that = (AggregateMetadata) other;
    return MoreObjects.equal(this.getSignature(), that.getSignature())
        && MoreObjects.equal(ksName(this), ksName(that));
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(getSignature(), ksName(this));
  }

  private static String ksName(AggregateMetadata a) {
    return a.keyspace == null ? null : a.keyspace.getName();
  }
}
