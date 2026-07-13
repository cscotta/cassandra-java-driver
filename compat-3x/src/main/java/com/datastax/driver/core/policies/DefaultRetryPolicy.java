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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.WriteFailureException;

/**
 * The default retry policy.
 *
 * <p>This policy retries queries in only two cases: on a read timeout, retries once on the same
 * host if enough replicas replied but data was not retrieved; on a write timeout, retries once on
 * the same host if the timeout occurred while writing the batch log; on an unavailable exception,
 * retries once on the next host; on a request error, retries on the next host, except on read/write
 * failures.
 */
public class DefaultRetryPolicy implements RetryPolicy {

  public static final DefaultRetryPolicy INSTANCE = new DefaultRetryPolicy();

  private DefaultRetryPolicy() {}

  @Override
  public RetryDecision onReadTimeout(
      Statement statement,
      ConsistencyLevel cl,
      int requiredResponses,
      int receivedResponses,
      boolean dataRetrieved,
      int nbRetry) {
    if (nbRetry != 0) return RetryDecision.rethrow();

    return receivedResponses >= requiredResponses && !dataRetrieved
        ? RetryDecision.retry(cl)
        : RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onWriteTimeout(
      Statement statement,
      ConsistencyLevel cl,
      WriteType writeType,
      int requiredAcks,
      int receivedAcks,
      int nbRetry) {
    if (nbRetry != 0) return RetryDecision.rethrow();

    return writeType == WriteType.BATCH_LOG ? RetryDecision.retry(cl) : RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onUnavailable(
      Statement statement,
      ConsistencyLevel cl,
      int requiredReplica,
      int aliveReplica,
      int nbRetry) {
    return (nbRetry == 0) ? RetryDecision.tryNextHost(null) : RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onRequestError(
      Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
    if (e instanceof WriteFailureException || e instanceof ReadFailureException) {
      return RetryDecision.rethrow();
    }
    return RetryDecision.tryNextHost(cl);
  }

  @Override
  public void init(Cluster cluster) {
    // nothing to do
  }

  @Override
  public void close() {
    // nothing to do
  }
}
