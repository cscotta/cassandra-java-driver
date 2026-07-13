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

/**
 * A small abstraction around the system clock, providing timestamps in microseconds.
 *
 * <p>Package-private internal support type, identical to the 3.12.1 driver's {@code Clock}. It is
 * not part of the public 3.x ABI (japicmp compares only public/protected members), but the monotonic
 * timestamp generators use it as an injectable test seam, exactly as the 3.x driver did.
 */
interface Clock {

  /**
   * Returns the current time in microseconds.
   *
   * @return the difference, measured in microseconds, between the current time and the Epoch (that
   *     is, midnight, January 1, 1970 UTC).
   */
  long currentTimeMicros();
}
