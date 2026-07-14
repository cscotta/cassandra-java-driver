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

/** {@link Metrics} options. Shim port of the 3.12.1 value bean. */
public class MetricsOptions {

  private final boolean metricsEnabled;
  private final boolean jmxEnabled;

  /**
   * Creates a new {@code MetricsOptions} object with default values (metrics enabled, JMX reporting
   * enabled).
   */
  public MetricsOptions() {
    this(true, true);
  }

  /** Creates a new {@code MetricsOptions} object. */
  public MetricsOptions(boolean enabled, boolean jmxEnabled) {
    this.metricsEnabled = enabled;
    this.jmxEnabled = jmxEnabled;
  }

  /** Returns whether metrics are enabled. */
  public boolean isEnabled() {
    return metricsEnabled;
  }

  /**
   * Returns whether JMX reporting is enabled.
   *
   * <p>UNMAPPED against 4.x (no built-in JMX reporter); the value round-trips but is inert.
   */
  public boolean isJMXReportingEnabled() {
    return jmxEnabled;
  }
}
