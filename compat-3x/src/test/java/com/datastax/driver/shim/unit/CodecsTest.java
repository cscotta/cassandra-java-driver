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

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.arrays.IntArrayCodec;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.extras.codecs.enums.EnumOrdinalCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: {@code TypeCodec} format/parse and serialize/deserialize
 * round-trips for base types plus the extras codecs (enums / date / arrays). No server; asserts
 * against the shim's own codec classes. Values mirror the differential {@code ParityMain.scExtras}
 * and the 3.x {@code TypeCodecTest} / extras codec tests (verified against the shim by {@code
 * UpstreamSuiteIT}).
 */
public class CodecsTest {

  private static final ProtocolVersion V = ProtocolVersion.NEWEST_SUPPORTED;

  @Test(groups = "unit")
  public void should_format_base_types() {
    assertThat(TypeCodec.cint().format(42)).isEqualTo("42");
    assertThat(TypeCodec.bigint().format(42L)).isEqualTo("42");
    assertThat(TypeCodec.cboolean().format(true)).isEqualTo("true");
    assertThat(TypeCodec.varchar().format("foo")).isEqualTo("'foo'");
    // Single quotes are doubled when escaping a CQL string literal.
    assertThat(TypeCodec.varchar().format("fo'o")).isEqualTo("'fo''o'");
    assertThat(TypeCodec.varint().format(new BigInteger("99999999999999999999")))
        .isEqualTo("99999999999999999999");
    assertThat(TypeCodec.decimal().format(new BigDecimal("12345.6789"))).isEqualTo("12345.6789");
    // NULL rendering.
    assertThat(TypeCodec.varchar().format(null)).isEqualTo("NULL");
    assertThat(TypeCodec.cint().format(null)).isEqualTo("NULL");
  }

  @Test(groups = "unit")
  public void should_parse_base_types() {
    assertThat(TypeCodec.cint().parse("42")).isEqualTo(42);
    assertThat(TypeCodec.bigint().parse("91294377723")).isEqualTo(91294377723L);
    assertThat(TypeCodec.varchar().parse("'foo'")).isEqualTo("foo");
    assertThat(TypeCodec.varchar().parse("'fo''o'")).isEqualTo("fo'o");
    assertThat(TypeCodec.cboolean().parse("true")).isEqualTo(true);
    // null / NULL literals parse to null.
    assertThat(TypeCodec.varchar().parse("NULL")).isNull();
    assertThat(TypeCodec.cint().parse("null")).isNull();
  }

  @Test(groups = "unit")
  public void should_round_trip_serialize_deserialize_int() {
    ByteBuffer bb = TypeCodec.cint().serialize(42, V);
    assertThat(TypeCodec.cint().deserialize(bb, V)).isEqualTo(42);
  }

  @Test(groups = "unit")
  public void should_round_trip_serialize_deserialize_text() {
    ByteBuffer bb = TypeCodec.varchar().serialize("héllo", V);
    assertThat(TypeCodec.varchar().deserialize(bb, V)).isEqualTo("héllo");
  }

  @Test(groups = "unit")
  public void should_round_trip_serialize_deserialize_uuid() {
    UUID u = UUID.fromString("00000000-0000-0000-0000-000000000001");
    ByteBuffer bb = TypeCodec.uuid().serialize(u, V);
    assertThat(TypeCodec.uuid().deserialize(bb, V)).isEqualTo(u);
  }

  @Test(groups = "unit")
  public void should_round_trip_list_codec() {
    TypeCodec<List<String>> codec = TypeCodec.list(TypeCodec.varchar());
    List<String> value = Arrays.asList("x", "y", "z");
    ByteBuffer bb = codec.serialize(value, V);
    assertThat(codec.deserialize(bb, V)).isEqualTo(value);
  }

  @Test(groups = "unit")
  public void should_format_enum_name_codec() {
    // Mirrors ParityMain.scExtras.
    EnumNameCodec<TimeUnit> codec = new EnumNameCodec<>(TimeUnit.class);
    assertThat(codec.format(TimeUnit.SECONDS)).isEqualTo("'SECONDS'");
    assertThat(codec.parse("'MINUTES'")).isEqualTo(TimeUnit.MINUTES);
  }

  @Test(groups = "unit")
  public void should_format_enum_ordinal_codec() {
    EnumOrdinalCodec<TimeUnit> codec = new EnumOrdinalCodec<>(TimeUnit.class);
    // TimeUnit.SECONDS has ordinal 3.
    assertThat(codec.format(TimeUnit.SECONDS)).isEqualTo("3");
  }

  @Test(groups = "unit")
  public void should_round_trip_simple_timestamp_codec() {
    // SimpleTimestampCodec maps the CQL `timestamp` type to a Java long (millis).
    long millis = 1234567890000L;
    ByteBuffer bb = SimpleTimestampCodec.instance.serialize(millis, V);
    assertThat(SimpleTimestampCodec.instance.deserialize(bb, V)).isEqualTo(millis);
  }

  @Test(groups = "unit")
  public void should_round_trip_int_array_codec() {
    IntArrayCodec codec = IntArrayCodec.instance;
    int[] value = {1, 2, 3, 4};
    ByteBuffer bb = codec.serialize(value, V);
    assertThat(codec.deserialize(bb, V)).isEqualTo(value);
  }
}
