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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.session.Session;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Keeps metadata on the connected cluster, including known nodes and schema definitions.
 *
 * <p>Shim: the instance API is a facade over the 4.x {@code Metadata} (schema + nodes) and the 4.x
 * {@code TokenMap} (token/replica queries). Token objects are the shim's own native {@link Token}
 * types, rebuilt from the 4.x token map's partitioner and string form. The static identifier
 * utilities ({@code quoteIfNecessary}, {@code quote}, {@code isReservedCqlKeyword}, {@code
 * handleId}) are self-contained reimplementations copied from the 3.12.1 source.
 */
public class Metadata {

  // Non-ABI: 4.x delegates threaded in by the session-cluster tier.
  final com.datastax.oss.driver.api.core.metadata.Metadata delegate;
  private final Session session;

  Metadata(com.datastax.oss.driver.api.core.metadata.Metadata delegate, Session session) {
    this.delegate = delegate;
    this.session = session;
  }

  private static final Set<String> RESERVED_KEYWORDS =
      new HashSet<String>(
          Arrays.asList(
              "add",
              "allow",
              "alter",
              "and",
              "any",
              "apply",
              "asc",
              "authorize",
              "batch",
              "begin",
              "by",
              "columnfamily",
              "create",
              "delete",
              "desc",
              "drop",
              "each_quorum",
              "from",
              "grant",
              "in",
              "index",
              "inet",
              "infinity",
              "insert",
              "into",
              "keyspace",
              "keyspaces",
              "limit",
              "local_one",
              "local_quorum",
              "modify",
              "nan",
              "norecursive",
              "of",
              "on",
              "one",
              "order",
              "password",
              "primary",
              "quorum",
              "rename",
              "revoke",
              "schema",
              "select",
              "set",
              "table",
              "to",
              "token",
              "three",
              "truncate",
              "two",
              "unlogged",
              "update",
              "use",
              "using",
              "where",
              "with"));

  /**
   * Takes a CQL identifier as provided by a user and normalizes it to the form Cassandra stores
   * internally (unquoted identifiers are lower-cased; quoted identifiers are unwrapped).
   */
  static String handleId(String id) {
    // Shouldn't really happen for this method, but no reason to fail here
    if (id == null) return null;

    boolean isAlphanumericLowCase = true;
    boolean isAlphanumeric = true;
    for (int i = 0; i < id.length(); i++) {
      char c = id.charAt(i);
      if (c >= 65 && c <= 90) { // A-Z
        isAlphanumericLowCase = false;
      } else if (!((c >= 48 && c <= 57) // 0-9
          || (c == 95) // _ (underscore)
          || (c >= 97 && c <= 122) // a-z
      )) {
        isAlphanumeric = false;
        isAlphanumericLowCase = false;
        break;
      }
    }

    if (isAlphanumericLowCase) {
      return id;
    }
    if (isAlphanumeric) {
      return id.toLowerCase();
    }

    // Check if it's enclosed in quotes. If it is, remove them and unescape internal double quotes
    return ParseUtils.unDoubleQuote(id);
  }

  /**
   * Quotes a CQL identifier if necessary.
   *
   * @param id the "internal" form of the identifier.
   * @return the identifier as it would appear in a CQL query string.
   */
  public static String quoteIfNecessary(String id) {
    return needsQuote(id) ? quote(id) : id;
  }

  private static boolean needsQuote(String s) {
    // this method should only be called for C*-provided identifiers,
    // so we expect it to be non-null
    assert s != null;
    if (s.isEmpty()) return true;
    char c = s.charAt(0);
    if (!(c >= 97 && c <= 122)) // a-z
    return true;
    for (int i = 1; i < s.length(); i++) {
      c = s.charAt(i);
      if (!((c >= 48 && c <= 57) // 0-9
          || (c == 95) // _
          || (c >= 97 && c <= 122) // a-z
      )) {
        return true;
      }
    }
    return isReservedCqlKeyword(s);
  }

  /**
   * Quote a keyspace, table or column identifier to make it case sensitive.
   *
   * @param id the keyspace or table identifier.
   * @return {@code id} enclosed in double-quotes.
   */
  public static String quote(String id) {
    return ParseUtils.doubleQuote(id);
  }

  /**
   * Checks whether an identifier is a known reserved CQL keyword or not. The check is
   * case-insensitive.
   *
   * @param id the identifier to check; should not be {@code null}.
   * @return {@code true} if the given identifier is a known reserved CQL keyword.
   */
  public static boolean isReservedCqlKeyword(String id) {
    if (id == null) {
      return false;
    }
    return RESERVED_KEYWORDS.contains(id.toLowerCase());
  }

  // ---- Instance API (facade over 4.x Metadata / TokenMap / Session) --------------------------

  private TokenMap tokenMap() {
    return delegate.getTokenMap().orElse(null);
  }

  private Token.Factory tokenFactory() {
    TokenMap tm = tokenMap();
    return tm == null ? null : Token.getFactory(tm.getPartitionerName());
  }

  private Token toShimToken(Token.Factory factory, com.datastax.oss.driver.api.core.metadata.token.Token token) {
    TokenMap tm = tokenMap();
    if (tm == null || factory == null) return null;
    return factory.fromString(tm.format(token));
  }

  private TokenRange toShimRange(
      Token.Factory factory, com.datastax.oss.driver.api.core.metadata.token.TokenRange range) {
    return new TokenRange(
        toShimToken(factory, range.getStart()), toShimToken(factory, range.getEnd()), factory);
  }

  /** Returns the token ranges that define data distribution in the ring. */
  public Set<TokenRange> getTokenRanges() {
    TokenMap tm = tokenMap();
    if (tm == null) return Collections.emptySet();
    Token.Factory factory = tokenFactory();
    Set<TokenRange> result = new LinkedHashSet<TokenRange>();
    for (com.datastax.oss.driver.api.core.metadata.token.TokenRange r : tm.getTokenRanges()) {
      result.add(toShimRange(factory, r));
    }
    return result;
  }

  /** Returns the token ranges that are replicated on the given host, for the given keyspace. */
  public Set<TokenRange> getTokenRanges(String keyspace, Host host) {
    TokenMap tm = tokenMap();
    if (tm == null) return Collections.emptySet();
    Token.Factory factory = tokenFactory();
    Set<TokenRange> result = new LinkedHashSet<TokenRange>();
    for (com.datastax.oss.driver.api.core.metadata.token.TokenRange r :
        tm.getTokenRanges(CqlIdentifier.fromInternal(handleId(keyspace)), host.node)) {
      result.add(toShimRange(factory, r));
    }
    return result;
  }

  /** Returns the set of hosts that are replicas for a given partition key. */
  public Set<Host> getReplicas(String keyspace, ByteBuffer partitionKey) {
    TokenMap tm = tokenMap();
    if (tm == null) return Collections.emptySet();
    return toHosts(tm.getReplicas(CqlIdentifier.fromInternal(handleId(keyspace)), partitionKey));
  }

  /** Returns the set of hosts that are replicas for a given token range. */
  public Set<Host> getReplicas(String keyspace, TokenRange range) {
    TokenMap tm = tokenMap();
    if (tm == null || range == null || range.getEnd() == null) return Collections.emptySet();
    com.datastax.oss.driver.api.core.metadata.token.Token end = tm.parse(range.getEnd().toString());
    return toHosts(tm.getReplicas(CqlIdentifier.fromInternal(handleId(keyspace)), end));
  }

  private static Set<Host> toHosts(Set<Node> nodes) {
    Set<Host> result = new LinkedHashSet<Host>();
    for (Node node : nodes) {
      result.add(SchemaBridge.toHost(node));
    }
    return result;
  }

  /** The Cassandra cluster name, or {@code null} if it is not known. */
  public String getClusterName() {
    return delegate.getClusterName().orElse(null);
  }

  /** The partitioner in use in the cluster, or {@code null} if it is not known. */
  public String getPartitioner() {
    TokenMap tm = tokenMap();
    return tm == null ? null : tm.getPartitionerName();
  }

  /** Returns all known hosts of the cluster. */
  public Set<Host> getAllHosts() {
    Set<Host> result = new LinkedHashSet<Host>();
    for (Node node : delegate.getNodes().values()) {
      result.add(SchemaBridge.toHost(node));
    }
    return result;
  }

  /** Checks whether hosts that are currently up agree on the schema definition. */
  public boolean checkSchemaAgreement() {
    return session != null && session.checkSchemaAgreement();
  }

  /** Returns the metadata of a keyspace given its name, or {@code null} if it doesn't exist. */
  public KeyspaceMetadata getKeyspace(String keyspace) {
    return delegate
        .getKeyspace(CqlIdentifier.fromInternal(handleId(keyspace)))
        .map(ks -> new KeyspaceMetadata(ks, this))
        .orElse(null);
  }

  /** Returns a list of all the defined keyspaces. */
  public List<KeyspaceMetadata> getKeyspaces() {
    List<KeyspaceMetadata> result = new ArrayList<KeyspaceMetadata>();
    for (com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata ks :
        delegate.getKeyspaces().values()) {
      result.add(new KeyspaceMetadata(ks, this));
    }
    return result;
  }

  /** Exports the current schema as a CQL string. */
  public String exportSchemaAsString() {
    StringBuilder sb = new StringBuilder();
    for (com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata ks :
        delegate.getKeyspaces().values()) {
      sb.append(ks.describeWithChildren(true)).append('\n');
    }
    return sb.toString();
  }

  /** Builds a new, empty tuple type from the given component types. */
  public TupleType newTupleType(DataType... types) {
    return TupleType.of(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE, types);
  }

  /** Builds a new, empty tuple type from the given component types. */
  public TupleType newTupleType(List<DataType> types) {
    return newTupleType(types.toArray(new DataType[types.size()]));
  }

  /** Builds a token from its string representation. */
  public Token newToken(String tokenStr) {
    Token.Factory factory = tokenFactory();
    return factory == null ? null : factory.fromString(tokenStr);
  }

  /** Builds a token from the values that compose a partition key. */
  public Token newToken(ByteBuffer... values) {
    TokenMap tm = tokenMap();
    Token.Factory factory = tokenFactory();
    if (tm == null || factory == null) return null;
    return toShimToken(factory, tm.newToken(values));
  }

  /** Builds a token range from a start and end token. */
  public TokenRange newTokenRange(Token start, Token end) {
    return new TokenRange(start, end, tokenFactory());
  }
}
