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
package com.datastax.driver.shim.unit;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.VersionNumber;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: deterministic driver utilities — {@code UUIDs}, {@code Bytes},
 * {@code VersionNumber}. No server; asserts against the shim's own classes. Mirrors the differential
 * {@code ParityMain} type/hex observations and the 3.x {@code UUIDsTest} / {@code VersionNumberTest}.
 */
public class UtilsTest {

  // ---- Bytes ----

  @Test(groups = "unit")
  public void bytes_should_render_hex() {
    ByteBuffer bb = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
    assertThat(Bytes.toHexString(bb)).isEqualTo("0x01020304");
  }

  @Test(groups = "unit")
  public void bytes_should_round_trip_hex() {
    byte[] raw = {0x00, 0x7f, (byte) 0x80, (byte) 0xff, 0x2a};
    String hex = Bytes.toHexString(ByteBuffer.wrap(raw));
    ByteBuffer back = Bytes.fromHexString(hex);
    assertThat(Bytes.getArray(back)).isEqualTo(raw);
  }

  // ---- UUIDs (deterministic surface) ----

  @Test(groups = "unit")
  public void uuids_startOf_endOf_should_be_version1_with_matching_timestamp() {
    long ts = 1234567890000L;
    UUID start = UUIDs.startOf(ts);
    UUID end = UUIDs.endOf(ts);
    assertThat(start.version()).isEqualTo(1);
    assertThat(end.version()).isEqualTo(1);
    // unixTimestamp(startOf(t)) == t (mirrors 3.x UUIDsTest.startEndOfTest invariants).
    assertThat(UUIDs.unixTimestamp(start)).isEqualTo(ts);
    assertThat(UUIDs.unixTimestamp(end)).isEqualTo(ts);
  }

  @Test(groups = "unit")
  public void uuids_timeBased_should_be_version1_variant2() {
    UUID u = UUIDs.timeBased();
    assertThat(u.version()).isEqualTo(1);
    assertThat(u.variant()).isEqualTo(2);
  }

  @Test(groups = "unit")
  public void uuids_unixTimestamp_should_track_current_time() {
    long now = System.currentTimeMillis();
    long tstamp = UUIDs.unixTimestamp(UUIDs.timeBased());
    // Same 10ms window the 3.x UUIDsTest.conformanceTest asserts.
    assertThat(tstamp).isGreaterThanOrEqualTo(now - 10).isLessThanOrEqualTo(now + 10);
  }

  // ---- VersionNumber ----

  @Test(groups = "unit")
  public void version_should_parse_release() {
    VersionNumber v = VersionNumber.parse("1.2.19");
    assertThat(v.getMajor()).isEqualTo(1);
    assertThat(v.getMinor()).isEqualTo(2);
    assertThat(v.getPatch()).isEqualTo(19);
    assertThat(v.getDSEPatch()).isEqualTo(-1);
    assertThat(v.toString()).isEqualTo("1.2.19");
  }

  @Test(groups = "unit")
  public void version_should_parse_pre_release_labels_and_next_stable() {
    VersionNumber v = VersionNumber.parse("1.2.0-beta1-SNAPSHOT");
    assertThat(v.getPreReleaseLabels()).isEqualTo(Arrays.asList("beta1", "SNAPSHOT"));
    assertThat(v.nextStable().toString()).isEqualTo("1.2.0");
  }

  @Test(groups = "unit")
  public void version_should_order() {
    // Mirrors VersionNumberTest.should_order_versions.
    assertThat(VersionNumber.parse("1.2.0").compareTo(VersionNumber.parse("2.0.0"))).isEqualTo(-1);
    assertThat(VersionNumber.parse("2.0").compareTo(VersionNumber.parse("2.0.0"))).isEqualTo(0);
    assertThat(VersionNumber.parse("2.0.0-beta1").compareTo(VersionNumber.parse("2.0.0")))
        .isEqualTo(-1);
  }

  @Test(groups = "unit")
  public void version_should_implement_equals_and_hashCode() {
    VersionNumber a = VersionNumber.parse("3.0.15-SNAPSHOT");
    VersionNumber b = VersionNumber.parse("3.0.15-SNAPSHOT");
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());
  }
}
