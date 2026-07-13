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
 * A retry policy that sometimes retries with a lower consistency level than the one initially
 * requested.
 *
 * @deprecated as of version 3.5.0, this retry policy has been deprecated, and it will be removed in
 *     4.0.0.
 */
@Deprecated
@SuppressWarnings("DeprecatedIsStillUsed")
public class DowngradingConsistencyRetryPolicy implements RetryPolicy {

  public static final DowngradingConsistencyRetryPolicy INSTANCE =
      new DowngradingConsistencyRetryPolicy();

  private DowngradingConsistencyRetryPolicy() {}

  private RetryDecision maxLikelyToWorkCL(int knownOk, ConsistencyLevel currentCL) {
    if (knownOk >= 3) return RetryDecision.retry(ConsistencyLevel.THREE);

    if (knownOk == 2) return RetryDecision.retry(ConsistencyLevel.TWO);

    // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
    // so even if we get 0 alive replicas, there might be
    // a node up in some other datacenter
    if (knownOk == 1 || currentCL == ConsistencyLevel.EACH_QUORUM)
      return RetryDecision.retry(ConsistencyLevel.ONE);

    return RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onReadTimeout(
      Statement statement,
      ConsistencyLevel cl,
      int requiredResponses,
      int receivedResponses,
      boolean dataRetrieved,
      int nbRetry) {
    if (nbRetry != 0) return RetryDecision.rethrow();

    if (cl.isSerial()) return RetryDecision.rethrow();

    if (receivedResponses < requiredResponses) {
      // Tries the biggest CL that is expected to work
      return maxLikelyToWorkCL(receivedResponses, cl);
    }

    return !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
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

    switch (writeType) {
      case SIMPLE:
      case BATCH:
        // Since we provide atomicity there is no point in retrying
        return receivedAcks > 0 ? RetryDecision.ignore() : RetryDecision.rethrow();
      case UNLOGGED_BATCH:
        // Since only part of the batch could have been persisted,
        // retry with whatever consistency should allow to persist all
        return maxLikelyToWorkCL(receivedAcks, cl);
      case BATCH_LOG:
        return RetryDecision.retry(cl);
    }
    // We want to rethrow on COUNTER and CAS, because in those case "we don't know" and don't want
    // to guess
    return RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onUnavailable(
      Statement statement,
      ConsistencyLevel cl,
      int requiredReplica,
      int aliveReplica,
      int nbRetry) {
    if (nbRetry != 0) return RetryDecision.rethrow();

    if (cl.isSerial()) return RetryDecision.tryNextHost(null);

    // Tries the biggest CL that is expected to work
    return maxLikelyToWorkCL(aliveReplica, cl);
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
