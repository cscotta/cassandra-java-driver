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

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.quote;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import java.net.InetAddress;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: the shim's {@code QueryBuilder} must render byte-for-byte the same
 * CQL as the 3.12.1 driver. No Cassandra, no real 3.x driver; asserts against the shim's own classes.
 *
 * <p>The expected strings mirror the cases in the differential {@code ParityMain.scQueryBuilder} (the
 * {@code parity_ks.t} form, verified IDENTICAL real-vs-shim) and the 3.x {@code QueryBuilderTest}
 * (verified passing against the shim by {@code UpstreamSuiteIT}).
 */
public class QueryBuilderCqlTest {

  private static final String KS = "parity_ks";

  @Test(groups = "unit")
  public void should_render_select_all_with_eq() {
    assertThat(select().all().from(KS, "t").where(eq("id", 1)).getQueryString())
        .isEqualTo("SELECT * FROM parity_ks.t WHERE id=1;");
  }

  @Test(groups = "unit")
  public void should_render_select_columns_with_limit() {
    assertThat(select("id", "name").from(KS, "t").limit(5).getQueryString())
        .isEqualTo("SELECT id,name FROM parity_ks.t LIMIT 5;");
  }

  @Test(groups = "unit")
  public void should_render_insert_with_bound_value() {
    // A String value is not inlined by QueryBuilder; it travels as a bind value, rendering '?'.
    assertThat(insertInto(KS, "t").value("id", 5).value("name", "e").getQueryString())
        .isEqualTo("INSERT INTO parity_ks.t (id,name) VALUES (5,?);");
  }

  @Test(groups = "unit")
  public void should_render_update_with_set_and_where() {
    assertThat(update(KS, "t").with(set("name", "z")).where(eq("id", 5)).getQueryString())
        .isEqualTo("UPDATE parity_ks.t SET name=? WHERE id=5;");
  }

  @Test(groups = "unit")
  public void should_render_delete_with_where() {
    assertThat(delete().from(KS, "t").where(eq("id", 5)).getQueryString())
        .isEqualTo("DELETE FROM parity_ks.t WHERE id=5;");
  }

  @Test(groups = "unit")
  public void should_render_in_clause_with_inlined_ints() {
    assertThat(select().all().from(KS, "t").where(in("id", 1, 2, 3)).getQueryString())
        .isEqualTo("SELECT * FROM parity_ks.t WHERE id IN (1,2,3);");
  }

  @Test(groups = "unit")
  public void should_render_bind_marker() {
    BuiltStatement sel = select().all().from(KS, "t").where(eq("name", bindMarker()));
    assertThat(sel.getQueryString()).contains("?");
    assertThat(sel.getQueryString()).isEqualTo("SELECT * FROM parity_ks.t WHERE name=?;");
  }

  @Test(groups = "unit")
  public void should_render_ordering_and_quoting() throws Exception {
    // Mirrors QueryBuilderTest.selectTest: quoted identifiers, IN of inet, ORDER BY, LIMIT.
    // The 3.x QueryBuilderTest asserts on Statement.toString(), which inlines literal values
    // (getQueryString() instead emits bind markers for non-inlineable values).
    String query =
        "SELECT a,b,\"C\" FROM foo WHERE a IN ('127.0.0.1','127.0.0.3') AND \"C\"='foo' "
            + "ORDER BY a ASC,b DESC LIMIT 42;";
    BuiltStatement select =
        select("a", "b", quote("C"))
            .from("foo")
            .where(in("a", InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.3")))
            .and(eq(quote("C"), "foo"))
            .orderBy(asc("a"), desc("b"))
            .limit(42);
    assertThat(select.toString()).isEqualTo(query);
  }

  @Test(groups = "unit")
  public void should_render_range_conditions() {
    // Mirrors QueryBuilderTest.selectTest first case (asserted via toString(), which inlines).
    BuiltStatement select =
        select().all().from("foo").where(eq("k", 4)).and(gt("c", "a")).and(lte("c", "z"));
    assertThat(select.toString()).isEqualTo("SELECT * FROM foo WHERE k=4 AND c>'a' AND c<='z';");
  }
}
