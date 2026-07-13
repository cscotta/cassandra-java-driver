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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A set of hooks that allow clients to customize the driver's internal executors. Shim port of the
 * 3.12.1 defaults.
 *
 * <p>4.x manages its own executors (via {@code NETTY_*} config), so these factory methods are not
 * wired into the underlying {@code CqlSession}; they exist for API/ABI parity and reproduce the 3.x
 * default behavior as standalone factories.
 */
public class ThreadingOptions {

  private static final int NON_BLOCKING_EXECUTOR_SIZE =
      Integer.getInteger(
          "com.datastax.driver.NON_BLOCKING_EXECUTOR_SIZE",
          Runtime.getRuntime().availableProcessors());
  private static final int DEFAULT_THREAD_KEEP_ALIVE_SECONDS = 30;

  public ThreadFactory createThreadFactory(String clusterName, String executorName) {
    return new ThreadFactoryBuilder()
        .setNameFormat(clusterName + "-" + executorName + "-%d")
        .setThreadFactory(new DefaultThreadFactory("ignored name"))
        .build();
  }

  public ExecutorService createExecutor(String clusterName) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            NON_BLOCKING_EXECUTOR_SIZE,
            NON_BLOCKING_EXECUTOR_SIZE,
            DEFAULT_THREAD_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            createThreadFactory(clusterName, "worker"));
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  public ExecutorService createBlockingExecutor(String clusterName) {
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(
            2,
            2,
            DEFAULT_THREAD_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(),
            createThreadFactory(clusterName, "blocking-task-worker"));
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  public ScheduledExecutorService createReconnectionExecutor(String clusterName) {
    return new ScheduledThreadPoolExecutor(2, createThreadFactory(clusterName, "reconnection"));
  }

  public ScheduledExecutorService createScheduledTasksExecutor(String clusterName) {
    return new ScheduledThreadPoolExecutor(
        1, createThreadFactory(clusterName, "scheduled-task-worker"));
  }

  public ScheduledExecutorService createReaperExecutor(String clusterName) {
    return new ScheduledThreadPoolExecutor(
        1, createThreadFactory(clusterName, "connection-reaper"));
  }
}
