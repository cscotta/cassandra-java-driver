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

import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Base class for the metadata of a table or materialized view.
 *
 * <p>Shim: the 3.12.1 ABI exposes nine {@code protected final} fields and a nine-argument {@code
 * protected} constructor. These are reproduced verbatim. Values are translated eagerly from the 4.x
 * {@code RelationMetadata} delegate by the concrete subclasses ({@link TableMetadata}, {@link
 * MaterializedViewMetadata}), which set the column back-references after {@code super(...)}.
 */
public abstract class AbstractTableMetadata {

  protected final KeyspaceMetadata keyspace;
  protected final String name;
  protected final UUID id;
  protected final List<ColumnMetadata> partitionKey;
  protected final List<ColumnMetadata> clusteringColumns;
  protected final Map<String, ColumnMetadata> columns;
  protected final TableOptionsMetadata options;
  protected final List<ClusteringOrder> clusteringOrder;
  protected final VersionNumber cassandraVersion;

  // Non-ABI: 4.x delegate used for CQL rendering (describe/describeWithChildren) and isVirtual.
  RelationMetadata relationDelegate;

  protected AbstractTableMetadata(
      KeyspaceMetadata keyspace,
      String name,
      UUID id,
      List<ColumnMetadata> partitionKey,
      List<ColumnMetadata> clusteringColumns,
      Map<String, ColumnMetadata> columns,
      TableOptionsMetadata options,
      List<ClusteringOrder> clusteringOrder,
      VersionNumber cassandraVersion) {
    this.keyspace = keyspace;
    this.name = name;
    this.id = id;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.columns = columns;
    this.options = options;
    this.clusteringOrder = clusteringOrder;
    this.cassandraVersion = cassandraVersion;
  }

  /** The name of this table. */
  public String getName() {
    return name;
  }

  /** The unique id of this table, or {@code null} if not available. */
  public UUID getId() {
    return id;
  }

  /** The keyspace this table belongs to. */
  public KeyspaceMetadata getKeyspace() {
    return keyspace;
  }

  /** Returns the metadata for a column by name, or {@code null} if not found. */
  public ColumnMetadata getColumn(String name) {
    return columns.get(Metadata.handleId(name));
  }

  /** The columns of this table. */
  public List<ColumnMetadata> getColumns() {
    return new ArrayList<ColumnMetadata>(columns.values());
  }

  /** The primary key (partition key followed by clustering columns) of this table. */
  public List<ColumnMetadata> getPrimaryKey() {
    List<ColumnMetadata> pk = new ArrayList<ColumnMetadata>(partitionKey.size() + clusteringColumns.size());
    pk.addAll(partitionKey);
    pk.addAll(clusteringColumns);
    return pk;
  }

  /** The partition key columns of this table. */
  public List<ColumnMetadata> getPartitionKey() {
    return new ArrayList<ColumnMetadata>(partitionKey);
  }

  /** The clustering columns of this table. */
  public List<ColumnMetadata> getClusteringColumns() {
    return new ArrayList<ColumnMetadata>(clusteringColumns);
  }

  /** The clustering order of the clustering columns of this table. */
  public List<ClusteringOrder> getClusteringOrder() {
    return new ArrayList<ClusteringOrder>(clusteringOrder);
  }

  /** The options of this table. */
  public TableOptionsMetadata getOptions() {
    return options;
  }

  /** Whether this table is a virtual table. */
  public boolean isVirtual() {
    return (relationDelegate instanceof com.datastax.oss.driver.api.core.metadata.schema.TableMetadata)
        && ((com.datastax.oss.driver.api.core.metadata.schema.TableMetadata) relationDelegate)
            .isVirtual();
  }

  /** Returns a CQL query representing this table, including all children (indexes, views). */
  public String exportAsString() {
    return relationDelegate == null ? asCQLQuery(true) : relationDelegate.describeWithChildren(true);
  }

  /** Returns a single CQL statement representing this table. */
  public String asCQLQuery() {
    return asCQLQuery(false);
  }

  protected abstract String asCQLQuery(boolean formatted);

  /**
   * UNMAPPED: the 3.x internal CQL option-rendering helper has no 4.x hook (options are rendered by
   * the 4.x {@code Describable}). Kept for ABI; appends nothing.
   */
  protected StringBuilder appendOptions(StringBuilder sb, boolean formatted) {
    return sb;
  }

  @Override
  public String toString() {
    return asCQLQuery();
  }
}
