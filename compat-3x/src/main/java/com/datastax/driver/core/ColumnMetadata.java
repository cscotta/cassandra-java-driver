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

/** Describes a column defined in a table (or view). Shim facade over the 4.x column metadata. */
public class ColumnMetadata {

  private final String name;
  private final DataType type;
  private final boolean isStatic;
  // Back-reference to the owning table/view; set at construction (may be assigned after super()
  // in the parent constructor, hence not final).
  AbstractTableMetadata parent;

  ColumnMetadata(String name, DataType type, boolean isStatic) {
    this.name = name;
    this.type = type;
    this.isStatic = isStatic;
  }

  /** The name of the column. */
  public String getName() {
    return name;
  }

  /** The metadata of the table/view this column is part of. */
  public AbstractTableMetadata getParent() {
    return parent;
  }

  /** The type of the column. */
  public DataType getType() {
    return type;
  }

  /** Whether this column is a static column. */
  public boolean isStatic() {
    return isStatic;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof ColumnMetadata)) return false;
    ColumnMetadata that = (ColumnMetadata) other;
    return MoreObjects.equal(this.name, that.name)
        && MoreObjects.equal(this.type, that.type)
        && this.isStatic == that.isStatic;
  }

  @Override
  public int hashCode() {
    return MoreObjects.hashCode(name, type, isStatic);
  }

  @Override
  public String toString() {
    return name + " " + type;
  }

  /**
   * Package-private internal parser type in 3.12.1. Not part of the public ABI ({@code Raw} itself
   * is package-private), but its nested {@code Kind} enum is a public class file, so it is
   * reproduced to keep japicmp clean. Unused by the shim (sentinel only).
   */
  static class Raw {

    /** The kind of a column, as seen by the 3.x schema parser. */
    public enum Kind {
      PARTITION_KEY,
      CLUSTERING_COLUMN,
      REGULAR,
      COMPACT_VALUE,
      STATIC
    }
  }
}
