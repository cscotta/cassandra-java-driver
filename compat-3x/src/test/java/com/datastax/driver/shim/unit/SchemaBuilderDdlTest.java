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

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createIndex;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.createTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.dropTable;
import static com.datastax.driver.core.schemabuilder.SchemaBuilder.frozen;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: the shim's {@code SchemaBuilder} DDL rendering. No server; asserts
 * against the shim's own classes. Expected strings mirror the 3.x {@code CreateTest} / {@code
 * CreateIndexTest} / {@code DropTest} (verified passing against the shim by {@code UpstreamSuiteIT})
 * and the differential {@code ParityMain.scSchemaBuilder}.
 */
public class SchemaBuilderDdlTest {

  @Test(groups = "unit")
  public void should_render_create_simple_table() {
    SchemaStatement statement =
        createTable("test").addPartitionKey("id", DataType.bigint()).addColumn("name", DataType.text());
    assertThat(statement.getQueryString())
        .isEqualTo("\n\tCREATE TABLE test(\n\t\tid bigint,\n\t\tname text,\n\t\tPRIMARY KEY(id))");
  }

  @Test(groups = "unit")
  public void should_render_create_table_with_keyspace() {
    SchemaStatement statement =
        createTable("ks", "test")
            .addPartitionKey("id", DataType.bigint())
            .addColumn("name", DataType.text());
    assertThat(statement.getQueryString())
        .isEqualTo(
            "\n\tCREATE TABLE ks.test(\n\t\tid bigint,\n\t\tname text,\n\t\tPRIMARY KEY(id))");
  }

  @Test(groups = "unit")
  public void should_render_create_table_with_udt_partition_key() {
    SchemaStatement statement = createTable("test").addUDTPartitionKey("u", frozen("user"));
    assertThat(statement.getQueryString())
        .isEqualTo("\n\tCREATE TABLE test(\n\t\tu frozen<user>,\n\t\tPRIMARY KEY(u))");
  }

  @Test(groups = "unit")
  public void should_fail_creating_table_without_partition_key() {
    try {
      createTable("test").addColumn("name", DataType.text()).getQueryString();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // 3.x contract: a table needs at least one partition key.
    }
  }

  @Test(groups = "unit")
  public void should_render_create_index() {
    // Mirrors ParityMain.scSchemaBuilder: createIndex(...).onTable(...).andColumn(...).
    SchemaStatement statement = createIndex("idx_sb").onTable("parity_ks", "sb_t").andColumn("name");
    assertThat(statement.getQueryString())
        .isEqualTo("\n\tCREATE INDEX idx_sb ON parity_ks.sb_t(name)");
  }

  @Test(groups = "unit")
  public void should_render_create_index_if_not_exists() {
    // Mirrors CreateIndexTest.
    SchemaStatement statement =
        createIndex("myIndex").ifNotExists().onTable("ks", "test").andColumn("col");
    assertThat(statement.getQueryString())
        .isEqualTo("\n\tCREATE INDEX IF NOT EXISTS myIndex ON ks.test(col)");
  }

  @Test(groups = "unit")
  public void should_render_drop_table_if_exists() {
    // Mirrors ParityMain.scSchemaBuilder (sb.dropTable) and DropTest.
    assertThat(dropTable("parity_ks", "sb_t").ifExists().getQueryString())
        .isEqualTo("DROP TABLE IF EXISTS parity_ks.sb_t");
  }

  @Test(groups = "unit")
  public void should_render_drop_table_plain() {
    assertThat(dropTable("ks", "test").getQueryString()).isEqualTo("DROP TABLE ks.test");
  }
}
