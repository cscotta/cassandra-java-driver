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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Describes a user-defined function. Shim facade over the 4.x {@code FunctionMetadata}. */
public class FunctionMetadata {

  private final com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata delegate;
  private final KeyspaceMetadata keyspace;

  FunctionMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata delegate,
      KeyspaceMetadata keyspace) {
    this.delegate = delegate;
    this.keyspace = keyspace;
  }

  /** Returns a CQL query representing this function. */
  public String exportAsString() {
    return delegate.describeWithChildren(true);
  }

  /** Returns a single CQL statement representing this function. */
  public String asCQLQuery() {
    return delegate.describe(false);
  }

  @Override
  public String toString() {
    return asCQLQuery();
  }

  /** The keyspace this function belongs to. */
  public KeyspaceMetadata getKeyspace() {
    return keyspace;
  }

  /** The CQL signature of this function, e.g. {@code sum(int,int)}. */
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

  /** The simple name of this function (without arguments). */
  public String getSimpleName() {
    return delegate.getSignature().getName().asInternal();
  }

  /** The arguments of this function, as an ordered map of name to type. */
  public Map<String, DataType> getArguments() {
    Map<String, DataType> result = new LinkedHashMap<String, DataType>();
    List<CqlIdentifier> names = delegate.getParameterNames();
    List<com.datastax.oss.driver.api.core.type.DataType> types =
        delegate.getSignature().getParameterTypes();
    for (int i = 0; i < names.size(); i++) {
      result.put(names.get(i).asInternal(), SchemaBridge.toShimDataType(types.get(i)));
    }
    return result;
  }

  /** The body of this function. */
  public String getBody() {
    return delegate.getBody();
  }

  /** Whether this function is called when its input is null. */
  public boolean isCalledOnNullInput() {
    return delegate.isCalledOnNullInput();
  }

  /** The programming language of the body of this function. */
  public String getLanguage() {
    return delegate.getLanguage();
  }

  /** The return type of this function. */
  public DataType getReturnType() {
    return SchemaBridge.toShimDataType(delegate.getReturnType());
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof FunctionMetadata)) return false;
    FunctionMetadata that = (FunctionMetadata) other;
    return MoreObjects.equal(this.getSignature(), that.getSignature())
        && MoreObjects.equal(ksName(this), ksName(that));
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(getSignature(), ksName(this));
  }

  private static String ksName(FunctionMetadata f) {
    return f.keyspace == null ? null : f.keyspace.getName();
  }
}
