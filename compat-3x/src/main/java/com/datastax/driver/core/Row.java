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

/** A CQL Row returned in a {@link ResultSet}. */
public interface Row extends GettableData {

  /**
   * Returns the columns contained in this Row.
   *
   * @return the columns contained in this Row.
   */
  public ColumnDefinitions getColumnDefinitions();

  /**
   * Returns the {@code i}th value of this row as a token.
   *
   * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
   * @return the value of the {@code i}th column as a token.
   */
  public Token getToken(int i);

  /**
   * Returns the value of column {@code name} as a token.
   *
   * @param name the name of the column to retrieve.
   * @return the value of column {@code name} as a token.
   */
  public Token getToken(String name);

  /**
   * Returns the value of the first column containing a token as a token.
   *
   * @return the value of column of the partition key token, or {@code null} if the row does not
   *     contain a token column.
   */
  public Token getPartitionKeyToken();
}
