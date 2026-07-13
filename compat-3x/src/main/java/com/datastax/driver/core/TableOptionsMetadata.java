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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The options of a table (or view). Shim facade: 4.x collapses all options into {@code
 * RelationMetadata.getOptions(): Map<CqlIdentifier,Object>}; each getter here reads a well-known key
 * of that map (translated to {@code Map<String,Object>} at construction).
 */
public class TableOptionsMetadata {

  private final Map<String, Object> options;
  private final boolean isCompactStorage;

  TableOptionsMetadata(Map<String, Object> options, boolean isCompactStorage) {
    this.options =
        options == null
            ? Collections.<String, Object>emptyMap()
            : new LinkedHashMap<String, Object>(options);
    this.isCompactStorage = isCompactStorage;
  }

  private String asString(String key) {
    Object v = options.get(key);
    return v == null ? null : v.toString();
  }

  private double asDouble(String key) {
    Object v = options.get(key);
    return (v instanceof Number) ? ((Number) v).doubleValue() : 0.0;
  }

  private int asInt(String key) {
    Object v = options.get(key);
    return (v instanceof Number) ? ((Number) v).intValue() : 0;
  }

  private Integer asBoxedInt(String key) {
    Object v = options.get(key);
    return (v instanceof Number) ? ((Number) v).intValue() : null;
  }

  private boolean asBool(String key) {
    Object v = options.get(key);
    return (v instanceof Boolean) && (Boolean) v;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> asStringMap(String key) {
    Object v = options.get(key);
    if (v instanceof Map) {
      Map<String, String> result = new LinkedHashMap<String, String>();
      for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
        result.put(String.valueOf(e.getKey()), e.getValue() == null ? null : e.getValue().toString());
      }
      return result;
    }
    return Collections.emptyMap();
  }

  /** Whether the table uses the (legacy) {@code COMPACT STORAGE} option. */
  public boolean isCompactStorage() {
    return isCompactStorage;
  }

  /** The commentary set for this table. */
  public String getComment() {
    String c = asString("comment");
    return c == null ? "" : c;
  }

  /** The chance with which a read repair is triggered for this table. */
  public double getReadRepairChance() {
    return asDouble("read_repair_chance");
  }

  /** The read repair strategy (newer Cassandra), or {@code null}. */
  public String getReadRepair() {
    return asString("read_repair");
  }

  /** The (cluster) local read repair chance set for this table. */
  public double getLocalReadRepairChance() {
    return asDouble("dclocal_read_repair_chance");
  }

  /**
   * UNMAPPED: legacy Cassandra 1.x option, absent from 4.x metadata. Returns {@code true} (the 3.x
   * default).
   */
  public boolean getReplicateOnWrite() {
    return true;
  }

  /** The tombstone garbage collection grace time in seconds for this table. */
  public int getGcGraceInSeconds() {
    return asInt("gc_grace_seconds");
  }

  /** The false positive chance for the Bloom filter of this table. */
  public double getBloomFilterFalsePositiveChance() {
    return asDouble("bloom_filter_fp_chance");
  }

  /** The caching option for this table. */
  public Map<String, String> getCaching() {
    return asStringMap("caching");
  }

  /**
   * UNMAPPED: legacy option, absent from 4.x metadata. Returns {@code false}.
   */
  public boolean getPopulateIOCacheOnFlush() {
    return false;
  }

  /** The memtable flush period (in milliseconds) option for this table. */
  public int getMemtableFlushPeriodInMs() {
    return asInt("memtable_flush_period_in_ms");
  }

  /** The default TTL for this table. */
  public int getDefaultTimeToLive() {
    return asInt("default_time_to_live");
  }

  /** The speculative retry option for this table. */
  public String getSpeculativeRetry() {
    return asString("speculative_retry");
  }

  /** The index interval option, or {@code null} if not present. */
  public Integer getIndexInterval() {
    return asBoxedInt("index_interval");
  }

  /** The minimum index interval option, or {@code null} if not present. */
  public Integer getMinIndexInterval() {
    return asBoxedInt("min_index_interval");
  }

  /** The maximum index interval option, or {@code null} if not present. */
  public Integer getMaxIndexInterval() {
    return asBoxedInt("max_index_interval");
  }

  /** The CRC check chance option, or {@code null} if not present. */
  public Double getCrcCheckChance() {
    Object v = options.get("crc_check_chance");
    return (v instanceof Number) ? ((Number) v).doubleValue() : null;
  }

  /** The compaction options for this table. */
  public Map<String, String> getCompaction() {
    return asStringMap("compaction");
  }

  /** The compression options for this table. */
  public Map<String, String> getCompression() {
    return asStringMap("compression");
  }

  /** The extension options for this table. */
  @SuppressWarnings("unchecked")
  public Map<String, ByteBuffer> getExtensions() {
    Object v = options.get("extensions");
    if (v instanceof Map) {
      return (Map<String, ByteBuffer>) v;
    }
    return Collections.emptyMap();
  }

  /** Whether change data capture is enabled on this table. */
  public boolean isCDC() {
    return asBool("cdc");
  }

  /** The additional write policy (Cassandra 4.0+), or {@code null}. */
  public String getAdditionalWritePolicy() {
    return asString("additional_write_policy");
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) return true;
    if (!(other instanceof TableOptionsMetadata)) return false;
    TableOptionsMetadata that = (TableOptionsMetadata) other;
    return this.isCompactStorage == that.isCompactStorage && this.options.equals(that.options);
  }

  @Override
  public int hashCode() {
    return options.hashCode() * 31 + (isCompactStorage ? 1 : 0);
  }
}
