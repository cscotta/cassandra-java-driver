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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.session.Session;
import java.util.List;

/**
 * Build-time 4.x {@code SchemaChangeListener} that translates 4.x schema-change events into the 3.x
 * {@code Added/Removed/Changed} vocabulary and fans them out to the shim {@link SchemaChangeListener}s
 * registered on the {@link Cluster}. Lives in {@code com.datastax.driver.core} so it can use the
 * package-private constructors of the 3.x metadata wrappers and the {@link Metadata} root.
 *
 * <p>4.x delivers the changed metadata object; wrappers that need a parent keyspace (table, function,
 * aggregate, view) are built by looking the (still-present) keyspace up in the session's live
 * metadata and wrapping the delivered object as the child's delegate. If the parent keyspace can't
 * be resolved (e.g. the keyspace itself was dropped), that per-object event is skipped.
 */
final class ShimSchemaChangeAdapter
    implements com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener {

  private final Cluster cluster;
  private volatile Session session;

  ShimSchemaChangeAdapter(Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public void onSessionReady(Session session) {
    this.session = session;
  }

  private List<SchemaChangeListener> listeners() {
    return cluster.schemaChangeListeners;
  }

  private Metadata root() {
    Session s = session;
    return s == null ? null : new Metadata(s.getMetadata(), s);
  }

  /** Wraps a delivered 4.x keyspace. */
  private KeyspaceMetadata km(
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata ks) {
    return new KeyspaceMetadata(ks, root());
  }

  /** Resolves the shim keyspace wrapper for a child object's keyspace name, or {@code null}. */
  private KeyspaceMetadata parent(CqlIdentifier keyspaceName) {
    Session s = session;
    if (s == null || keyspaceName == null) {
      return null;
    }
    com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata ks =
        s.getMetadata().getKeyspace(keyspaceName).orElse(null);
    return ks == null ? null : new KeyspaceMetadata(ks, new Metadata(s.getMetadata(), s));
  }

  // ---- keyspace ------------------------------------------------------------------------------

  @Override
  public void onKeyspaceCreated(
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata keyspace) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata k = km(keyspace);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onKeyspaceAdded(k));
  }

  @Override
  public void onKeyspaceDropped(
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata keyspace) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata k = km(keyspace);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onKeyspaceRemoved(k));
  }

  @Override
  public void onKeyspaceUpdated(
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata current,
      com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata previous) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata c = km(current);
    KeyspaceMetadata p = km(previous);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onKeyspaceChanged(c, p));
  }

  // ---- table ---------------------------------------------------------------------------------

  @Override
  public void onTableCreated(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(table.getKeyspace());
    if (parent == null) return;
    TableMetadata t = new TableMetadata(table, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onTableAdded(t));
  }

  @Override
  public void onTableDropped(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata table) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(table.getKeyspace());
    if (parent == null) return;
    TableMetadata t = new TableMetadata(table, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onTableRemoved(t));
  }

  @Override
  public void onTableUpdated(
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata current,
      com.datastax.oss.driver.api.core.metadata.schema.TableMetadata previous) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(current.getKeyspace());
    if (parent == null) return;
    TableMetadata c = new TableMetadata(current, parent);
    TableMetadata p = new TableMetadata(previous, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onTableChanged(c, p));
  }

  // ---- user-defined type ---------------------------------------------------------------------

  @Override
  public void onUserDefinedTypeCreated(
      com.datastax.oss.driver.api.core.type.UserDefinedType type) {
    if (listeners().isEmpty()) return;
    UserType t = SchemaBridge.toShimUserType(type);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onUserTypeAdded(t));
  }

  @Override
  public void onUserDefinedTypeDropped(
      com.datastax.oss.driver.api.core.type.UserDefinedType type) {
    if (listeners().isEmpty()) return;
    UserType t = SchemaBridge.toShimUserType(type);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onUserTypeRemoved(t));
  }

  @Override
  public void onUserDefinedTypeUpdated(
      com.datastax.oss.driver.api.core.type.UserDefinedType current,
      com.datastax.oss.driver.api.core.type.UserDefinedType previous) {
    if (listeners().isEmpty()) return;
    UserType c = SchemaBridge.toShimUserType(current);
    UserType p = SchemaBridge.toShimUserType(previous);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onUserTypeChanged(c, p));
  }

  // ---- function ------------------------------------------------------------------------------

  @Override
  public void onFunctionCreated(
      com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata function) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(function.getKeyspace());
    if (parent == null) return;
    FunctionMetadata f = new FunctionMetadata(function, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onFunctionAdded(f));
  }

  @Override
  public void onFunctionDropped(
      com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata function) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(function.getKeyspace());
    if (parent == null) return;
    FunctionMetadata f = new FunctionMetadata(function, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onFunctionRemoved(f));
  }

  @Override
  public void onFunctionUpdated(
      com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata current,
      com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata previous) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(current.getKeyspace());
    if (parent == null) return;
    FunctionMetadata c = new FunctionMetadata(current, parent);
    FunctionMetadata p = new FunctionMetadata(previous, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onFunctionChanged(c, p));
  }

  // ---- aggregate -----------------------------------------------------------------------------

  @Override
  public void onAggregateCreated(
      com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata aggregate) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(aggregate.getKeyspace());
    if (parent == null) return;
    AggregateMetadata a = new AggregateMetadata(aggregate, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onAggregateAdded(a));
  }

  @Override
  public void onAggregateDropped(
      com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata aggregate) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(aggregate.getKeyspace());
    if (parent == null) return;
    AggregateMetadata a = new AggregateMetadata(aggregate, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onAggregateRemoved(a));
  }

  @Override
  public void onAggregateUpdated(
      com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata current,
      com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata previous) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(current.getKeyspace());
    if (parent == null) return;
    AggregateMetadata c = new AggregateMetadata(current, parent);
    AggregateMetadata p = new AggregateMetadata(previous, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onAggregateChanged(c, p));
  }

  // ---- materialized view ---------------------------------------------------------------------

  @Override
  public void onViewCreated(com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata view) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(view.getKeyspace());
    if (parent == null) return;
    MaterializedViewMetadata v = new MaterializedViewMetadata(view, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onMaterializedViewAdded(v));
  }

  @Override
  public void onViewDropped(com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata view) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(view.getKeyspace());
    if (parent == null) return;
    MaterializedViewMetadata v = new MaterializedViewMetadata(view, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onMaterializedViewRemoved(v));
  }

  @Override
  public void onViewUpdated(
      com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata current,
      com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata previous) {
    if (listeners().isEmpty()) return;
    KeyspaceMetadata parent = parent(current.getKeyspace());
    if (parent == null) return;
    MaterializedViewMetadata c = new MaterializedViewMetadata(current, parent);
    MaterializedViewMetadata p = new MaterializedViewMetadata(previous, parent);
    for (SchemaChangeListener l : listeners()) safe(() -> l.onMaterializedViewChanged(c, p));
  }

  @Override
  public void close() {
    // The listener list is owned by the Cluster; nothing to release here.
  }

  private static void safe(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException ignored) {
      // a misbehaving listener must not break schema-event processing
    }
  }
}
