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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Describes a table. Shim facade over the 4.x {@code TableMetadata}. */
public class TableMetadata extends AbstractTableMetadata {

  private final com.datastax.oss.driver.api.core.metadata.schema.TableMetadata delegate;

  TableMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata delegate,
      KeyspaceMetadata keyspace) {
    this(delegate, keyspace, SchemaBridge.buildTableFields(delegate));
  }

  private TableMetadata(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata delegate,
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

  /** Returns metadata for the index with the given name, or {@code null} if it doesn't exist. */
  public IndexMetadata getIndex(String name) {
    com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata i =
        delegate
            .getIndexes()
            .get(
                com.datastax.oss.driver.api.core.CqlIdentifier.fromInternal(
                    Metadata.handleId(name)));
    return i == null ? null : new IndexMetadata(this, i);
  }

  /** Returns the indexes on this table. */
  public Collection<IndexMetadata> getIndexes() {
    Collection<com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata> raw =
        delegate.getIndexes().values();
    List<IndexMetadata> result = new ArrayList<IndexMetadata>(raw.size());
    for (com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata i : raw) {
      result.add(new IndexMetadata(this, i));
    }
    return result;
  }

  /** Returns the materialized view with the given name that is based on this table, or {@code null}. */
  public MaterializedViewMetadata getView(String name) {
    String normalized = Metadata.handleId(name);
    for (MaterializedViewMetadata view : getViews()) {
      if (view.getName().equals(normalized)) {
        return view;
      }
    }
    return null;
  }

  /** Returns the materialized views based on this table. */
  public Collection<MaterializedViewMetadata> getViews() {
    List<MaterializedViewMetadata> result = new ArrayList<MaterializedViewMetadata>();
    if (keyspace != null) {
      for (MaterializedViewMetadata view : keyspace.getMaterializedViews()) {
        TableMetadata base = view.getBaseTable();
        if (base != null && this.name.equals(base.name)) {
          result.add(view);
        }
      }
    }
    return result;
  }

  @Override
  public String exportAsString() {
    return delegate.describeWithChildren(true);
  }

  @Override
  protected String asCQLQuery(boolean formatted) {
    return delegate.describe(formatted);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof TableMetadata)) return false;
    TableMetadata that = (TableMetadata) other;
    return MoreObjects.equal(this.name, that.name)
        && MoreObjects.equal(keyspaceName(this), keyspaceName(that));
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(name, keyspaceName(this));
  }

  private static String keyspaceName(TableMetadata t) {
    return t.keyspace == null ? null : t.keyspace.getName();
  }

  // --- package-private CQL-formatting helpers relied on by other 3.x metadata types ----------

  static StringBuilder newLine(StringBuilder sb, boolean formatted) {
    if (formatted) sb.append('\n');
    return sb;
  }

  static StringBuilder spaceOrNewLine(StringBuilder sb, boolean formatted) {
    sb.append(formatted ? "\n    " : ' ');
    return sb;
  }
}
