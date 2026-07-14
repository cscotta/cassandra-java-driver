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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.WriteType;

/**
 * 4.x -&gt; 3.x enum conversions shared by the policy adapters. Both driver generations use the same
 * enum names, so the mapping is by name (with a safe fallback if a 4.x name has no 3.x analogue).
 */
public final class EnumBridge {

  private EnumBridge() {}

  /** 4.x consistency level -&gt; 3.x, defaulting to {@link ConsistencyLevel#ONE} if unknown/null. */
  public static ConsistencyLevel clV3(com.datastax.oss.driver.api.core.ConsistencyLevel v4) {
    if (v4 == null) {
      return ConsistencyLevel.LOCAL_ONE;
    }
    try {
      return ConsistencyLevel.valueOf(v4.name());
    } catch (RuntimeException e) {
      return ConsistencyLevel.ONE;
    }
  }

  /** 4.x write type -&gt; 3.x, defaulting to {@link WriteType#SIMPLE} if unknown/null. */
  public static WriteType wtV3(com.datastax.oss.driver.api.core.servererrors.WriteType v4) {
    if (v4 == null) {
      return WriteType.SIMPLE;
    }
    try {
      return WriteType.valueOf(v4.name());
    } catch (RuntimeException e) {
      return WriteType.SIMPLE;
    }
  }
}
