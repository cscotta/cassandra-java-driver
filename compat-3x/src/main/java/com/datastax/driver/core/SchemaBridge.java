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

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.ContainerType;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Package-private, non-ABI workhorse for the schema-metadata shim. Holds the construction-bearing
 * bridge logic that needs the package-private constructors of the 3.x metadata types: the
 * Node&lt;-&gt;Host identity cache, the metadata-tree wrappers, 4.x-&gt;3.x {@code DataType}
 * conversion, and the eager field translation for {@link AbstractTableMetadata} subclasses. Stateless
 * value conversions live in {@code com.datastax.shim.bridge.MetadataBridge}, which this reuses.
 */
final class SchemaBridge {

  private SchemaBridge() {}

  // ---- Node <-> Host identity cache (stable equals/hashCode across calls) -------------------

  private static final ConcurrentHashMap<Node, Host> HOSTS = new ConcurrentHashMap<Node, Host>();

  static Host toHost(Node node) {
    if (node == null) return null;
    Host existing = HOSTS.get(node);
    if (existing != null) return existing;
    Host created = new Host(node);
    Host race = HOSTS.putIfAbsent(node, created);
    return race != null ? race : created;
  }

  // ---- 4.x DataType -> 3.x DataType ----------------------------------------------------------

  static DataType toShimDataType(com.datastax.oss.driver.api.core.type.DataType type) {
    if (type instanceof ListType) {
      ListType t = (ListType) type;
      return DataType.list(toShimDataType(t.getElementType()), t.isFrozen());
    }
    if (type instanceof SetType) {
      SetType t = (SetType) type;
      return DataType.set(toShimDataType(t.getElementType()), t.isFrozen());
    }
    if (type instanceof MapType) {
      MapType t = (MapType) type;
      return DataType.map(toShimDataType(t.getKeyType()), toShimDataType(t.getValueType()), t.isFrozen());
    }
    if (type instanceof com.datastax.oss.driver.api.core.type.TupleType) {
      com.datastax.oss.driver.api.core.type.TupleType t =
          (com.datastax.oss.driver.api.core.type.TupleType) type;
      List<com.datastax.oss.driver.api.core.type.DataType> comps = t.getComponentTypes();
      DataType[] shim = new DataType[comps.size()];
      for (int i = 0; i < comps.size(); i++) shim[i] = toShimDataType(comps.get(i));
      return TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, shim);
    }
    if (type instanceof UserDefinedType) {
      return toShimUserType((UserDefinedType) type);
    }
    if (type instanceof CustomType) {
      return DataType.custom(((CustomType) type).getClassName());
    }
    if (type instanceof ContainerType) {
      // Vector or any future single-element container with no 3.x analogue: render as custom.
      return DataType.custom(type.asCql(false, false));
    }
    // Primitive: protocol codes are shared 1:1 between 3.x and 4.x.
    return primitiveFromProtocolCode(type.getProtocolCode());
  }

  static UserType toShimUserType(UserDefinedType udt) {
    List<com.datastax.oss.driver.api.core.CqlIdentifier> names = udt.getFieldNames();
    List<com.datastax.oss.driver.api.core.type.DataType> types = udt.getFieldTypes();
    List<UserType.Field> fields = new ArrayList<UserType.Field>(names.size());
    for (int i = 0; i < names.size(); i++) {
      fields.add(new UserType.Field(names.get(i).asInternal(), toShimDataType(types.get(i))));
    }
    String keyspace = udt.getKeyspace() == null ? null : udt.getKeyspace().asInternal();
    return new UserType(
        keyspace,
        udt.getName().asInternal(),
        udt.isFrozen(),
        fields,
        ProtocolVersion.NEWEST_SUPPORTED,
        CodecRegistry.DEFAULT_INSTANCE);
  }

  private static DataType primitiveFromProtocolCode(int code) {
    switch (code) {
      case ProtocolConstants.DataType.ASCII:
        return DataType.ascii();
      case ProtocolConstants.DataType.BIGINT:
        return DataType.bigint();
      case ProtocolConstants.DataType.BLOB:
        return DataType.blob();
      case ProtocolConstants.DataType.BOOLEAN:
        return DataType.cboolean();
      case ProtocolConstants.DataType.COUNTER:
        return DataType.counter();
      case ProtocolConstants.DataType.DECIMAL:
        return DataType.decimal();
      case ProtocolConstants.DataType.DOUBLE:
        return DataType.cdouble();
      case ProtocolConstants.DataType.FLOAT:
        return DataType.cfloat();
      case ProtocolConstants.DataType.INT:
        return DataType.cint();
      case ProtocolConstants.DataType.TIMESTAMP:
        return DataType.timestamp();
      case ProtocolConstants.DataType.UUID:
        return DataType.uuid();
      case ProtocolConstants.DataType.VARCHAR:
        return DataType.varchar();
      case ProtocolConstants.DataType.VARINT:
        return DataType.varint();
      case ProtocolConstants.DataType.TIMEUUID:
        return DataType.timeuuid();
      case ProtocolConstants.DataType.INET:
        return DataType.inet();
      case ProtocolConstants.DataType.DATE:
        return DataType.date();
      case ProtocolConstants.DataType.TIME:
        return DataType.time();
      case ProtocolConstants.DataType.SMALLINT:
        return DataType.smallint();
      case ProtocolConstants.DataType.TINYINT:
        return DataType.tinyint();
      case ProtocolConstants.DataType.DURATION:
        return DataType.duration();
      default:
        throw new DriverInternalError("Unsupported data type protocol code: " + code);
    }
  }

  // ---- Eager AbstractTableMetadata field translation ----------------------------------------

  /** Holder for the nine {@link AbstractTableMetadata} constructor arguments. */
  static final class TableFields {
    final String name;
    final UUID id;
    final List<ColumnMetadata> partitionKey;
    final List<ColumnMetadata> clusteringColumns;
    final Map<String, ColumnMetadata> columns;
    final TableOptionsMetadata options;
    final List<ClusteringOrder> clusteringOrder;
    final VersionNumber cassandraVersion;

    TableFields(
        String name,
        UUID id,
        List<ColumnMetadata> partitionKey,
        List<ColumnMetadata> clusteringColumns,
        Map<String, ColumnMetadata> columns,
        TableOptionsMetadata options,
        List<ClusteringOrder> clusteringOrder,
        VersionNumber cassandraVersion) {
      this.name = name;
      this.id = id;
      this.partitionKey = partitionKey;
      this.clusteringColumns = clusteringColumns;
      this.columns = columns;
      this.options = options;
      this.clusteringOrder = clusteringOrder;
      this.cassandraVersion = cassandraVersion;
    }
  }

  static TableFields buildTableFields(
      com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata delegate) {
    String name = delegate.getName().asInternal();
    UUID id = delegate.getId().orElse(null);

    Map<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();
    for (Map.Entry<
            com.datastax.oss.driver.api.core.CqlIdentifier,
            com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata>
        e : delegate.getColumns().entrySet()) {
      com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata c = e.getValue();
      columns.put(
          e.getKey().asInternal(),
          new ColumnMetadata(c.getName().asInternal(), toShimDataType(c.getType()), c.isStatic()));
    }

    List<ColumnMetadata> partitionKey = new ArrayList<ColumnMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata c :
        delegate.getPartitionKey()) {
      partitionKey.add(columns.get(c.getName().asInternal()));
    }

    List<ColumnMetadata> clusteringColumns = new ArrayList<ColumnMetadata>();
    List<ClusteringOrder> clusteringOrder = new ArrayList<ClusteringOrder>();
    for (Map.Entry<
            com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata,
            com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder>
        e : delegate.getClusteringColumns().entrySet()) {
      clusteringColumns.add(columns.get(e.getKey().getName().asInternal()));
      clusteringOrder.add(ClusteringOrder.valueOf(e.getValue().name()));
    }

    boolean compact =
        (delegate instanceof com.datastax.oss.driver.api.core.metadata.schema.TableMetadata)
            && ((com.datastax.oss.driver.api.core.metadata.schema.TableMetadata) delegate)
                .isCompactStorage();
    Map<String, Object> optionMap = new LinkedHashMap<String, Object>();
    for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, Object> e :
        delegate.getOptions().entrySet()) {
      optionMap.put(e.getKey().asInternal(), e.getValue());
    }
    TableOptionsMetadata options = new TableOptionsMetadata(optionMap, compact);

    return new TableFields(
        name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, null);
  }
}
