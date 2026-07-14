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
 * Options to configure low-level socket options for the connections kept to the Cassandra hosts.
 *
 * <p>Shim port of the 3.12.1 value bean. State round-trips through the getters/setters; the {@link
 * com.datastax.shim.bridge.ConfigBridge} maps the held values onto the equivalent 4.x driver config
 * keys when a {@code CqlSession} is built.
 */
public class SocketOptions {

  /** The default connection timeout in milliseconds ({@value}). */
  public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

  /** The default read timeout in milliseconds ({@value}). */
  public static final int DEFAULT_READ_TIMEOUT_MILLIS = 12000;

  private volatile int connectTimeoutMillis = DEFAULT_CONNECT_TIMEOUT_MILLIS;
  private volatile int readTimeoutMillis = DEFAULT_READ_TIMEOUT_MILLIS;
  private volatile Boolean keepAlive;
  private volatile Boolean reuseAddress;
  private volatile Integer soLinger;
  private volatile Boolean tcpNoDelay = Boolean.TRUE;
  private volatile Integer receiveBufferSize;
  private volatile Integer sendBufferSize;

  /** Creates a new {@code SocketOptions} instance with default values. */
  public SocketOptions() {}

  public int getConnectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  public SocketOptions setConnectTimeoutMillis(int connectTimeoutMillis) {
    this.connectTimeoutMillis = connectTimeoutMillis;
    return this;
  }

  public int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  public SocketOptions setReadTimeoutMillis(int readTimeoutMillis) {
    this.readTimeoutMillis = readTimeoutMillis;
    return this;
  }

  public Boolean getKeepAlive() {
    return keepAlive;
  }

  public SocketOptions setKeepAlive(boolean keepAlive) {
    this.keepAlive = keepAlive;
    return this;
  }

  public Boolean getReuseAddress() {
    return reuseAddress;
  }

  public SocketOptions setReuseAddress(boolean reuseAddress) {
    this.reuseAddress = reuseAddress;
    return this;
  }

  public Integer getSoLinger() {
    return soLinger;
  }

  public SocketOptions setSoLinger(int soLinger) {
    this.soLinger = soLinger;
    return this;
  }

  public Boolean getTcpNoDelay() {
    return tcpNoDelay;
  }

  public SocketOptions setTcpNoDelay(boolean tcpNoDelay) {
    this.tcpNoDelay = tcpNoDelay;
    return this;
  }

  public Integer getReceiveBufferSize() {
    return receiveBufferSize;
  }

  public SocketOptions setReceiveBufferSize(int receiveBufferSize) {
    this.receiveBufferSize = receiveBufferSize;
    return this;
  }

  public Integer getSendBufferSize() {
    return sendBufferSize;
  }

  public SocketOptions setSendBufferSize(int sendBufferSize) {
    this.sendBufferSize = sendBufferSize;
    return this;
  }
}
