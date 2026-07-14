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
// Shim provenance: net-new bridge — package com.datastax.shim.* is internal shim support,
// not part of the 3.x ABI (japicmp compares only com.datastax.driver.*). See PROVENANCE.md.
package com.datastax.shim.bridge;

import com.datastax.driver.core.VersionNumber;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.NodeState;

/**
 * Stateless converters shared by the schema-metadata shim (package {@code com.datastax.driver.core})
 * and, later, the session-cluster integration tier. Lives outside {@code com.datastax.driver.*} so
 * it is invisible to the 3.x ABI (japicmp only compares {@code com.datastax.driver.*}).
 *
 * <p>The construction-bearing wrapping (Node&lt;-&gt;Host identity cache, metadata-tree wrappers,
 * 4.x&lt;-&gt;3.x DataType conversion and the {@code NodeStateListener}/{@code SchemaChangeListener}
 * adapters) needs the package-private constructors of the 3.x metadata types, so it lives in the
 * package-private {@code com.datastax.driver.core.SchemaBridge}. This class holds only the
 * package-agnostic value conversions, which that helper reuses.
 */
public final class MetadataBridge {

  private MetadataBridge() {}

  /** 4.x {@code CqlIdentifier} -&gt; 3.x internal (case-sensitive) name. */
  public static String asInternal(CqlIdentifier id) {
    return id == null ? null : id.asInternal();
  }

  /**
   * 3.x metadata name -&gt; 4.x {@code CqlIdentifier}. 3.x metadata lookups are case-sensitive on the
   * name as stored internally, so {@link CqlIdentifier#fromInternal(String)} is used.
   */
  public static CqlIdentifier fromInternal(String name) {
    return name == null ? null : CqlIdentifier.fromInternal(name);
  }

  /** 4.x {@code Version} -&gt; 3.x {@code VersionNumber} (via 3.x parser; {@code null}-safe). */
  public static VersionNumber toVersionNumber(Version version) {
    return version == null ? null : VersionNumber.parse(version.toString());
  }

  /**
   * 4.x {@link NodeState} -&gt; 3.x {@code Host} state string. 3.x uses {@code ADDED/UP/DOWN}; 4.x
   * adds {@code UNKNOWN/FORCED_DOWN}. Map {@code UNKNOWN}-&gt;{@code ADDED} and
   * {@code FORCED_DOWN}-&gt;{@code DOWN} for closest fidelity.
   */
  public static String stateName(NodeState state) {
    if (state == null) return "ADDED";
    switch (state) {
      case UP:
        return "UP";
      case DOWN:
      case FORCED_DOWN:
        return "DOWN";
      case UNKNOWN:
      default:
        return "ADDED";
    }
  }
}
