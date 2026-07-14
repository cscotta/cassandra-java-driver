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

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementShimAccess;
import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Package-visible entry point (used by the session/cluster tier) for translating between the 3.x
 * shim statement types and the 4.x driver statements. It delegates to the package-private
 * construction bridge {@code com.datastax.driver.core.StatementCoreBridge} (reached via {@code
 * StatementShimAccess}), which owns the package-private state of the mutable shim statement
 * state-holders. Not part of the 3.x public ABI (this package is not inspected by japicmp).
 */
public final class StatementBridge {

  private StatementBridge() {}

  /** Materializes an immutable 4.x {@code Statement<?>} from an accumulated shim {@link Statement}. */
  public static com.datastax.oss.driver.api.core.cql.Statement<?> toV4(
      Statement shim, CqlSession session) {
    return StatementShimAccess.toV4(shim, session);
  }

  /** Wraps a 4.x {@code PreparedStatement} in a shim {@link com.datastax.driver.core.PreparedStatement}. */
  public static com.datastax.driver.core.PreparedStatement toV3Prepared(
      com.datastax.oss.driver.api.core.cql.PreparedStatement v4, CqlSession session) {
    return StatementShimAccess.toV3Prepared(v4, session);
  }
}
