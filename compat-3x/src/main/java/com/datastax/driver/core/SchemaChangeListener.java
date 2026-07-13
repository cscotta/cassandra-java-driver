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

/**
 * Interface for objects that are interested in tracking schema change events in the cluster.
 *
 * <p>Shim: a {@code SchemaBridge} adapter implements the 4.x {@code SchemaChangeListener} and
 * forwards events (translating 4.x {@code Created/Dropped/Updated} to the 3.x {@code
 * Added/Removed/Changed} names) to registered 3.x listeners. {@code onRegister}/{@code onUnregister}
 * are driven by the shim {@code Cluster} register path.
 */
public interface SchemaChangeListener {

  void onKeyspaceAdded(KeyspaceMetadata keyspace);

  void onKeyspaceRemoved(KeyspaceMetadata keyspace);

  void onKeyspaceChanged(KeyspaceMetadata current, KeyspaceMetadata previous);

  void onTableAdded(TableMetadata table);

  void onTableRemoved(TableMetadata table);

  void onTableChanged(TableMetadata current, TableMetadata previous);

  void onUserTypeAdded(UserType type);

  void onUserTypeRemoved(UserType type);

  void onUserTypeChanged(UserType current, UserType previous);

  void onFunctionAdded(FunctionMetadata function);

  void onFunctionRemoved(FunctionMetadata function);

  void onFunctionChanged(FunctionMetadata current, FunctionMetadata previous);

  void onAggregateAdded(AggregateMetadata aggregate);

  void onAggregateRemoved(AggregateMetadata aggregate);

  void onAggregateChanged(AggregateMetadata current, AggregateMetadata previous);

  void onMaterializedViewAdded(MaterializedViewMetadata view);

  void onMaterializedViewRemoved(MaterializedViewMetadata view);

  void onMaterializedViewChanged(MaterializedViewMetadata current, MaterializedViewMetadata previous);

  void onRegister(Cluster cluster);

  void onUnregister(Cluster cluster);
}
