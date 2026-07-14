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

/** Describes a secondary index on a table. Shim facade over the 4.x index metadata. */
public class IndexMetadata {

  public static final String TARGET_OPTION_NAME = "target";
  public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";
  public static final String INDEX_KEYS_OPTION_NAME = "index_keys";
  public static final String INDEX_ENTRIES_OPTION_NAME = "index_keys_and_values";

  private final TableMetadata table;
  private final com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata delegate;

  IndexMetadata(
      TableMetadata table,
      com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata delegate) {
    this.table = table;
    this.delegate = delegate;
  }

  /** The table this index is part of. */
  public TableMetadata getTable() {
    return table;
  }

  /** The index name. */
  public String getName() {
    return delegate.getName().asInternal();
  }

  /** The index kind. */
  public Kind getKind() {
    switch (delegate.getKind()) {
      case KEYS:
        return Kind.KEYS;
      case CUSTOM:
        return Kind.CUSTOM;
      case COMPOSITES:
      default:
        return Kind.COMPOSITES;
    }
  }

  /** The index target (the indexed column or expression). */
  public String getTarget() {
    return delegate.getTarget();
  }

  /** Whether this is a custom (SASI or third-party) index. */
  public boolean isCustomIndex() {
    return delegate.getKind() == com.datastax.oss.driver.api.core.metadata.schema.IndexKind.CUSTOM;
  }

  /** The name of the custom index class, or {@code null} if this is not a custom index. */
  public String getIndexClassName() {
    return delegate.getOptions().get(CUSTOM_INDEX_OPTION_NAME);
  }

  /** Returns the value of a given index option, or {@code null} if not present. */
  public String getOption(String name) {
    return delegate.getOptions().get(name);
  }

  /** Returns a CQL query representing this index. */
  public String asCQLQuery() {
    return delegate.describe(false);
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(getName(), getTarget(), getKind());
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof IndexMetadata)) return false;
    IndexMetadata that = (IndexMetadata) other;
    return MoreObjects.equal(this.getName(), that.getName())
        && MoreObjects.equal(this.getTarget(), that.getTarget())
        && MoreObjects.equal(this.getKind(), that.getKind());
  }

  /** The kind of an index. */
  public enum Kind {
    KEYS,
    CUSTOM,
    COMPOSITES
  }
}
