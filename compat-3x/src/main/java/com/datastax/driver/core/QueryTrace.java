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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** The Cassandra trace for a query. */
public class QueryTrace {

  private final com.datastax.oss.driver.api.core.cql.QueryTrace delegate;

  QueryTrace(com.datastax.oss.driver.api.core.cql.QueryTrace delegate) {
    this.delegate = delegate;
  }

  /**
   * The id of this trace.
   *
   * @return the id of this trace.
   */
  public UUID getTraceId() {
    return delegate.getTracingId();
  }

  /**
   * The type of request.
   *
   * @return the type of request.
   */
  public String getRequestType() {
    return delegate.getRequestType();
  }

  /**
   * The duration of the operation in microseconds.
   *
   * @return the duration of the operation in microseconds.
   */
  public int getDurationMicros() {
    return delegate.getDurationMicros();
  }

  /**
   * The coordinator host of the operation traced by this object.
   *
   * @return the coordinator host.
   */
  @SuppressWarnings("deprecation")
  public InetAddress getCoordinator() {
    return delegate.getCoordinator();
  }

  /**
   * The parameters attached to this trace.
   *
   * @return the parameters attached to this trace.
   */
  public Map<String, String> getParameters() {
    return delegate.getParameters();
  }

  /**
   * The server-side timestamp of the beginning of this operation.
   *
   * @return the timestamp of the beginning of the operation.
   */
  public long getStartedAt() {
    return delegate.getStartedAt();
  }

  /**
   * The events contained in this trace.
   *
   * @return the events of this trace.
   */
  public List<Event> getEvents() {
    List<com.datastax.oss.driver.api.core.cql.TraceEvent> src = delegate.getEvents();
    List<Event> result = new ArrayList<Event>(src.size());
    for (com.datastax.oss.driver.api.core.cql.TraceEvent e : src) {
      result.add(new Event(e));
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s [%s] - %dµs", getRequestType(), getTraceId(), getDurationMicros());
  }

  /** A trace event. */
  public static class Event {

    private final com.datastax.oss.driver.api.core.cql.TraceEvent delegate;

    Event(com.datastax.oss.driver.api.core.cql.TraceEvent delegate) {
      this.delegate = delegate;
    }

    /**
     * The event description, that is which activity this event corresponds to.
     *
     * @return the event description.
     */
    public String getDescription() {
      return delegate.getActivity();
    }

    /**
     * The server side timestamp of the event.
     *
     * @return the server side timestamp of the event.
     */
    public long getTimestamp() {
      return delegate.getTimestamp();
    }

    /**
     * The address of the host having generated this event.
     *
     * @return the address of the host having generated this event.
     */
    @SuppressWarnings("deprecation")
    public InetAddress getSource() {
      return delegate.getSource();
    }

    /**
     * The number of microseconds elapsed on the source when this event occurred since the moment
     * when the source started handling the query.
     *
     * @return the elapsed time on the source host when that event happened in microseconds.
     */
    public int getSourceElapsedMicros() {
      return delegate.getSourceElapsedMicros();
    }

    /**
     * The name of the thread on which this event occurred.
     *
     * @return the name of the thread on which this event occurred.
     */
    public String getThreadName() {
      return delegate.getThreadName();
    }

    @Override
    public String toString() {
      return String.format("%s on %s[%s] at %tT", getDescription(), getSource(), getThreadName(),
          new java.util.Date(getTimestamp()));
    }
  }
}
