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
package com.datastax.shim.bridge;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.time.Duration;
import java.util.concurrent.ThreadFactory;

/**
 * Bridges a user's 3.x {@code com.datastax.driver.core.NettyOptions} (and optional 3.x {@code
 * ThreadingOptions}) onto 4.x's internal {@link NettyOptions} SPI, so {@code
 * Cluster.Builder.withNettyOptions(...)} / {@code withThreadingOptions(...)} take effect on the 4.x
 * engine while preserving 3.x behavior.
 *
 * <p>The 3.x and 4.x SPIs are near-1:1: the io {@link EventLoopGroup}, {@code channelClass}, {@link
 * Timer}, and the {@code afterBootstrapInitialized}/{@code afterChannelInitialized} hooks come
 * straight from the 3.x options. 4.x additionally requires an admin {@link EventExecutorGroup} and a
 * {@link ByteBufAllocator}, for which 3.x has no analogue, so a small daemon admin group and the 4.x
 * default allocator are supplied. {@code afterBootstrapInitialized} first applies the driver's own
 * socket options from config (as 4.x's {@code DefaultNettyOptions} does, and as 3.x applies its
 * {@code SocketOptions} before invoking the hook) and then runs the user hook.
 *
 * <p>{@link #onClose()} mirrors {@code DefaultNettyOptions.onClose()} but drives the 3.x shutdown
 * hooks: it shuts the io group down via {@code v3.onClusterClose(ioGroup)}, stops the timer via
 * {@code v3.onClusterClose(timer)}, shuts the admin group down, and completes a single {@link
 * Future} when all finish. This is critical: the io group / timer threads are non-daemon when the
 * user's {@code ThreadingOptions} factory is (as in 3.x), so failing to shut them down would hang
 * the JVM.
 */
public class Shim3xNettyOptions implements NettyOptions {

  private final com.datastax.driver.core.NettyOptions v3Netty;
  private final DriverExecutionProfile config;
  private final String clusterName;
  private final EventLoopGroup ioGroup;
  private final Class<? extends Channel> channelClass;
  private final EventExecutorGroup adminGroup;
  private final Timer timer;

  public Shim3xNettyOptions(
      String clusterName,
      DriverExecutionProfile config,
      com.datastax.driver.core.NettyOptions v3Netty,
      com.datastax.driver.core.ThreadingOptions v3Threading) {
    this.v3Netty = v3Netty;
    this.config = config;
    this.clusterName = clusterName;
    // 3.x sourced its event-loop threads from ThreadingOptions.createThreadFactory when set; fall
    // back to a daemon factory otherwise. Reused for the timer (as 3.x's Timer is created likewise).
    ThreadFactory ioThreadFactory =
        (v3Threading != null)
            ? v3Threading.createThreadFactory(clusterName, "nio-worker")
            : new DefaultThreadFactory(clusterName + "-nio-worker", /* daemon */ true);
    this.ioGroup = v3Netty.eventLoopGroup(ioThreadFactory);
    this.channelClass = v3Netty.channelClass();
    this.timer = v3Netty.timer(ioThreadFactory);
    // 4.x uses a dedicated admin group for non-I/O tasks; 3.x has none, so create a small daemon one.
    this.adminGroup =
        new DefaultEventExecutorGroup(
            2, new DefaultThreadFactory(clusterName + "-admin", /* daemon */ true));
  }

  @Override
  public EventLoopGroup ioEventLoopGroup() {
    return ioGroup;
  }

  @Override
  public Class<? extends Channel> channelClass() {
    return channelClass;
  }

  @Override
  public EventExecutorGroup adminEventExecutorGroup() {
    return adminGroup;
  }

  @Override
  public ByteBufAllocator allocator() {
    return ByteBufAllocator.DEFAULT;
  }

  @Override
  public void afterBootstrapInitialized(Bootstrap bootstrap) {
    // Apply the driver's socket options from config first (mirrors DefaultNettyOptions, and 3.x
    // which applies SocketOptions before invoking the user hook), then run the user's 3.x hook.
    applySocketOptions(bootstrap);
    v3Netty.afterBootstrapInitialized(bootstrap);
  }

  private void applySocketOptions(Bootstrap bootstrap) {
    bootstrap.option(
        ChannelOption.TCP_NODELAY, config.getBoolean(DefaultDriverOption.SOCKET_TCP_NODELAY));
    if (config.isDefined(DefaultDriverOption.SOCKET_KEEP_ALIVE)) {
      bootstrap.option(
          ChannelOption.SO_KEEPALIVE, config.getBoolean(DefaultDriverOption.SOCKET_KEEP_ALIVE));
    }
    if (config.isDefined(DefaultDriverOption.SOCKET_REUSE_ADDRESS)) {
      bootstrap.option(
          ChannelOption.SO_REUSEADDR, config.getBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS));
    }
    if (config.isDefined(DefaultDriverOption.SOCKET_LINGER_INTERVAL)) {
      bootstrap.option(
          ChannelOption.SO_LINGER, config.getInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL));
    }
    if (config.isDefined(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE)) {
      int receiveBufferSize = config.getInt(DefaultDriverOption.SOCKET_RECEIVE_BUFFER_SIZE);
      bootstrap
          .option(ChannelOption.SO_RCVBUF, receiveBufferSize)
          .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(receiveBufferSize));
    }
    if (config.isDefined(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE)) {
      bootstrap.option(
          ChannelOption.SO_SNDBUF, config.getInt(DefaultDriverOption.SOCKET_SEND_BUFFER_SIZE));
    }
    if (config.isDefined(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT)) {
      Duration connectTimeout = config.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT);
      bootstrap.option(
          ChannelOption.CONNECT_TIMEOUT_MILLIS,
          Long.valueOf(connectTimeout.toMillis()).intValue());
    }
  }

  @Override
  public void afterChannelInitialized(Channel channel) {
    if (channel instanceof SocketChannel) {
      try {
        v3Netty.afterChannelInitialized((SocketChannel) channel);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Future<Void> onClose() {
    final DefaultPromise<Void> closeFuture = new DefaultPromise<Void>(GlobalEventExecutor.INSTANCE);
    // Run the (blocking) 3.x shutdown hooks on a dedicated daemon thread — NOT on
    // GlobalEventExecutor, because the io/admin groups' termination futures are themselves tied to
    // GlobalEventExecutor, and Netty's deadlock guard throws BlockingOperationException if
    // syncUninterruptibly() is invoked from that executor's thread.
    Thread closer =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                Throwable failure = null;
                try {
                  v3Netty.onClusterClose(ioGroup); // shuts down the io group (blocks until done)
                } catch (Throwable t) {
                  failure = t;
                }
                try {
                  v3Netty.onClusterClose(timer); // stops the timer
                } catch (Throwable t) {
                  if (failure == null) {
                    failure = t;
                  }
                }
                try {
                  adminGroup.shutdownGracefully().syncUninterruptibly();
                } catch (Throwable t) {
                  if (failure == null) {
                    failure = t;
                  }
                }
                if (failure == null) {
                  closeFuture.trySuccess(null);
                } else {
                  closeFuture.tryFailure(failure);
                }
              }
            },
            clusterName + "-shim-netty-close");
    closer.setDaemon(true);
    closer.start();
    return closeFuture;
  }

  @Override
  public Timer getTimer() {
    return timer;
  }
}
