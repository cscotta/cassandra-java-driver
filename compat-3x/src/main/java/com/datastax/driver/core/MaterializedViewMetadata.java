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

/** Describes a materialized view. Shim facade over the 4.x {@code ViewMetadata}. */
public class MaterializedViewMetadata extends AbstractTableMetadata {

  private final com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata delegate;

  MaterializedViewMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata delegate,
      KeyspaceMetadata keyspace) {
    this(delegate, keyspace, SchemaBridge.buildTableFields(delegate));
  }

  private MaterializedViewMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata delegate,
      KeyspaceMetadata keyspace,
      SchemaBridge.TableFields f) {
    super(
        keyspace,
        f.name,
        f.id,
        f.partitionKey,
        f.clusteringColumns,
        f.columns,
        f.options,
        f.clusteringOrder,
        f.cassandraVersion);
    this.delegate = delegate;
    this.relationDelegate = delegate;
    for (ColumnMetadata c : columns.values()) {
      c.parent = this;
    }
  }

  /** Returns the table this materialized view is based on, or {@code null} if not resolvable. */
  public TableMetadata getBaseTable() {
    if (keyspace == null) return null;
    return keyspace.getTable(delegate.getBaseTable().asInternal());
  }

  @Override
  protected String asCQLQuery(boolean formatted) {
    return delegate.describe(formatted);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof MaterializedViewMetadata)) return false;
    MaterializedViewMetadata that = (MaterializedViewMetadata) other;
    return MoreObjects.equal(this.name, that.name)
        && MoreObjects.equal(keyspaceName(this), keyspaceName(that));
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(name, keyspaceName(this));
  }

  private static String keyspaceName(MaterializedViewMetadata v) {
    return v.keyspace == null ? null : v.keyspace.getName();
  }
}
