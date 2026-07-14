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

/** Options of the Cassandra native binary protocol. Shim port of the 3.12.1 value bean. */
public class ProtocolOptions {

  /** Compression supported by the Cassandra binary protocol. */
  public enum Compression {
    /** No compression */
    NONE("") {},
    /** Snappy compression */
    SNAPPY("snappy") {},
    /** LZ4 compression */
    LZ4("lz4") {};

    final String protocolName;

    private Compression(String protocolName) {
      this.protocolName = protocolName;
    }

    static Compression fromString(String str) {
      for (Compression c : values()) {
        if (c.protocolName.equalsIgnoreCase(str)) return c;
      }
      return null;
    }

    @Override
    public String toString() {
      return protocolName;
    }
  };

  /** The default port for Cassandra native binary protocol: 9042. */
  public static final int DEFAULT_PORT = 9042;

  /** The default value for {@link #getMaxSchemaAgreementWaitSeconds()}: 10. */
  public static final int DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS = 10;

  private final int port;
  final ProtocolVersion initialProtocolVersion;

  volatile int maxSchemaAgreementWaitSeconds;

  private final SSLOptions sslOptions;
  private final AuthProvider authProvider;

  private final boolean noCompact;

  private volatile Compression compression = Compression.NONE;

  // Shim link to the owning Cluster; used to report the negotiated protocol version once the
  // underlying CqlSession is connected. Not part of the 3.x ABI.
  volatile Cluster shimCluster;

  public ProtocolOptions() {
    this(DEFAULT_PORT);
  }

  public ProtocolOptions(int port) {
    this(port, null, DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS, null, null, false);
  }

  public ProtocolOptions(
      int port,
      ProtocolVersion protocolVersion,
      int maxSchemaAgreementWaitSeconds,
      SSLOptions sslOptions,
      AuthProvider authProvider) {
    this(port, protocolVersion, maxSchemaAgreementWaitSeconds, sslOptions, authProvider, false);
  }

  public ProtocolOptions(
      int port,
      ProtocolVersion protocolVersion,
      int maxSchemaAgreementWaitSeconds,
      SSLOptions sslOptions,
      AuthProvider authProvider,
      boolean noCompact) {
    this.port = port;
    this.initialProtocolVersion = protocolVersion;
    this.maxSchemaAgreementWaitSeconds = maxSchemaAgreementWaitSeconds;
    this.sslOptions = sslOptions;
    this.authProvider = authProvider;
    this.noCompact = noCompact;
  }

  public int getPort() {
    return port;
  }

  /**
   * The protocol version used by the Cluster instance.
   *
   * <p>Returns the version negotiated by the underlying 4.x session once connected; before the
   * cluster connects (and if no version was forced), this returns {@code null}, matching 3.12.1.
   */
  public ProtocolVersion getProtocolVersion() {
    if (shimCluster != null) {
      ProtocolVersion negotiated = shimCluster.negotiatedProtocolVersion();
      if (negotiated != null) {
        return negotiated;
      }
    }
    return initialProtocolVersion;
  }

  public Compression getCompression() {
    return compression;
  }

  public ProtocolOptions setCompression(Compression compression) {
    this.compression = compression;
    return this;
  }

  public int getMaxSchemaAgreementWaitSeconds() {
    return maxSchemaAgreementWaitSeconds;
  }

  public SSLOptions getSSLOptions() {
    return sslOptions;
  }

  public AuthProvider getAuthProvider() {
    return authProvider;
  }

  /** @return Whether or not to include the NO_COMPACT startup option (UNMAPPED against 4.x). */
  public boolean isNoCompact() {
    return noCompact;
  }
}
