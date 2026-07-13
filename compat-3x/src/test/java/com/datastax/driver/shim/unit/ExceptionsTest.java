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

import com.datastax.driver.core.exceptions.CoordinatorException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.testng.annotations.Test;

/**
 * Layer 1 (standalone) unit test: the shim's 3.x exception hierarchy. Class relationships (superclass
 * chains, {@code CoordinatorException} being an interface) are part of the public ABI that
 * applications catch against. Mirrors {@code ParityMain.scExceptions} (exc.hierarchy) and the 3.x
 * {@code ExceptionsTest}.
 */
public class ExceptionsTest {

  @Test(groups = "unit")
  public void should_have_driver_exception_extend_runtime_exception() {
    // Matches ParityMain: NoHostAvailableException.getSuperclass() == DriverException, and
    // DriverException is unchecked.
    assertThat(DriverException.class.getSuperclass()).isEqualTo(RuntimeException.class);
    assertThat(NoHostAvailableException.class.getSuperclass()).isEqualTo(DriverException.class);
  }

  @Test(groups = "unit")
  public void should_chain_query_validation_exceptions() {
    assertThat(InvalidQueryException.class.getSuperclass()).isEqualTo(QueryValidationException.class);
    assertThat(SyntaxError.class.getSuperclass()).isEqualTo(QueryValidationException.class);
    assertThat(QueryValidationException.class.getSuperclass()).isEqualTo(DriverException.class);
  }

  @Test(groups = "unit")
  public void should_chain_query_execution_exceptions() {
    assertThat(QueryConsistencyException.class.getSuperclass())
        .isEqualTo(QueryExecutionException.class);
    assertThat(QueryExecutionException.class.getSuperclass()).isEqualTo(DriverException.class);
  }

  @Test(groups = "unit")
  public void coordinator_exception_should_be_an_interface() {
    assertThat(CoordinatorException.class.isInterface()).isTrue();
    assertThat(CoordinatorException.class.isAssignableFrom(SyntaxError.class)).isTrue();
  }

  @Test(groups = "unit")
  public void should_be_assignable_to_driver_exception() {
    DriverException e = new InvalidQueryException("boom");
    assertThat(e).isInstanceOf(DriverException.class);
    assertThat(e).isInstanceOf(QueryValidationException.class);
  }

  @Test(groups = "unit")
  public void should_preserve_message() {
    assertThat(new InvalidQueryException("boom").getMessage()).isEqualTo("boom");
    assertThat(new InvalidQueryException("boom", new RuntimeException("cause")).getCause())
        .isInstanceOf(RuntimeException.class);
  }
}
