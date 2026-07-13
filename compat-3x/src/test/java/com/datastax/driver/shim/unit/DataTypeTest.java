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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeTokens;
import com.google.common.reflect.TypeToken;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: {@code DataType} rendering / identity and {@code TypeTokens}. No
 * server; asserts against the shim's own classes. Mirrors the differential {@code
 * ParityMain.scMetadata} type observations and the 3.x {@code DataTypeTest}.
 */
public class DataTypeTest {

  @Test(groups = "unit")
  public void should_render_primitive_toString() {
    assertThat(DataType.cint().toString()).isEqualTo("int");
    assertThat(DataType.text().toString()).isEqualTo("text");
    assertThat(DataType.bigint().toString()).isEqualTo("bigint");
  }

  @Test(groups = "unit")
  public void should_render_collection_toString() {
    assertThat(DataType.list(DataType.text()).toString()).isEqualTo("list<text>");
    assertThat(DataType.frozenList(DataType.text()).toString()).isEqualTo("frozen<list<text>>");
    assertThat(DataType.map(DataType.text(), DataType.cint()).toString()).isEqualTo("map<text, int>");
  }

  @Test(groups = "unit")
  public void should_expose_name() {
    assertThat(DataType.cint().getName()).isEqualTo(DataType.Name.INT);
    // text() and varchar() report distinct names but compare equal (3.x behavior).
    assertThat(DataType.text().getName()).isEqualTo(DataType.Name.TEXT);
    assertThat(DataType.varchar().getName()).isEqualTo(DataType.Name.VARCHAR);
    assertThat(DataType.text()).isEqualTo(DataType.varchar());
    assertThat(DataType.Name.INT.toString()).isEqualTo("int");
  }

  @Test(groups = "unit")
  public void should_render_as_function_parameter_string() {
    assertThat(DataType.cint().asFunctionParameterString()).isEqualTo("int");
    assertThat(DataType.list(DataType.text()).asFunctionParameterString()).isEqualTo("list<text>");
  }

  @Test(groups = "unit")
  public void should_implement_equals_and_hashCode() {
    assertThat(DataType.cint()).isEqualTo(DataType.cint());
    assertThat(DataType.cint()).isNotEqualTo(DataType.text());
    assertThat(DataType.cint().hashCode()).isEqualTo(DataType.cint().hashCode());
    assertThat(DataType.list(DataType.text())).isEqualTo(DataType.list(DataType.text()));
    assertThat(DataType.list(DataType.text())).isNotEqualTo(DataType.list(DataType.cint()));
  }

  @Test(groups = "unit")
  public void should_build_type_tokens() {
    TypeToken<List<String>> listOf = TypeTokens.listOf(String.class);
    TypeToken<Set<Integer>> setOf = TypeTokens.setOf(Integer.class);
    TypeToken<Map<String, Integer>> mapOf = TypeTokens.mapOf(String.class, Integer.class);
    assertThat(listOf.toString()).isEqualTo("java.util.List<java.lang.String>");
    assertThat(setOf.toString()).isEqualTo("java.util.Set<java.lang.Integer>");
    assertThat(mapOf.toString()).isEqualTo("java.util.Map<java.lang.String, java.lang.Integer>");
    assertThat(listOf).isEqualTo(TypeTokens.listOf(String.class));
  }
}
