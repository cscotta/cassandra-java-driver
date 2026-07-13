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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.concurrent.ThreadFactory;

/**
 * A set of hooks that allow clients to customize the driver's underlying Netty layer. Shim port of
 * the 3.12.1 defaults.
 *
 * <p>4.x builds and owns its own Netty layer (via {@code NETTY_*} config) and exposes no equivalent
 * hooks, so a user-supplied {@code NettyOptions} has no effect on the underlying {@code CqlSession}.
 * The default behavior below is reproduced for standalone use and ABI parity.
 */
public class NettyOptions {

  /** The default instance of {@link NettyOptions} to use. */
  public static final NettyOptions DEFAULT_INSTANCE = new NettyOptions();

  public EventLoopGroup eventLoopGroup(ThreadFactory threadFactory) {
    return new NioEventLoopGroup(0, threadFactory);
  }

  public Class<? extends SocketChannel> channelClass() {
    return NioSocketChannel.class;
  }

  public void afterBootstrapInitialized(Bootstrap bootstrap) {
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
  }

  public void afterChannelInitialized(SocketChannel channel) throws Exception {
    // noop
  }

  public void onClusterClose(EventLoopGroup eventLoopGroup) {
    eventLoopGroup.shutdownGracefully().syncUninterruptibly();
  }

  public Timer timer(ThreadFactory threadFactory) {
    return new HashedWheelTimer(threadFactory);
  }

  public void onClusterClose(Timer timer) {
    timer.stop();
  }
}
