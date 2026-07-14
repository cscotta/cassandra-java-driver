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

import com.datastax.oss.driver.api.core.time.TimestampGenerator;

/**
 * Bridges a user's 3.x {@code com.datastax.driver.core.TimestampGenerator} onto 4.x's {@link
 * TimestampGenerator}. Both SPIs are {@code long next()} returning microseconds (with {@code
 * Long.MIN_VALUE} meaning "let Cassandra assign a server-side timestamp"), so {@link #next()} is a
 * direct pass-through.
 */
public class Shim3xTimestampGenerator implements TimestampGenerator {

  private final com.datastax.driver.core.TimestampGenerator v3;

  public Shim3xTimestampGenerator(com.datastax.driver.core.TimestampGenerator v3) {
    this.v3 = v3;
  }

  @Override
  public long next() {
    return v3.next();
  }

  @Override
  public void close() {
    // 3.x TimestampGenerator has no lifecycle to release.
  }
}
