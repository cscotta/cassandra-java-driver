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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.BootstrappingException;
import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.CASWriteUnknownException;
import com.datastax.driver.core.exceptions.CDCWriteException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.exceptions.FunctionExecutionException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ProtocolError;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TransportException;
import com.datastax.driver.core.exceptions.TruncateException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * One-way translation of 4.x driver exceptions into the shim's 3.x {@code
 * com.datastax.driver.core.exceptions.*} value types. Applied at every adapter boundary (execute /
 * prepare / connect / close). No shim exception wraps a 4.x exception.
 */
public final class ExceptionBridge {

  private ExceptionBridge() {}

  /** Translates a 4.x throwable into the matching 3.x shim {@link RuntimeException}. */
  public static RuntimeException toV3(Throwable t) {
    t = unwrap(t);

    if (t instanceof DriverException) {
      // already a shim exception (e.g. thrown by shim-internal code)
      return (DriverException) t;
    }

    // ---- AllNodesFailedException -> NoHostAvailableException ----------------------------------
    if (t instanceof AllNodesFailedException) {
      Map<EndPoint, Throwable> errors = new LinkedHashMap<EndPoint, Throwable>();
      try {
        for (Map.Entry<Node, List<Throwable>> e :
            ((AllNodesFailedException) t).getAllErrors().entrySet()) {
          EndPoint ep = endPoint(e.getKey());
          List<Throwable> causes = e.getValue();
          Throwable cause = (causes == null || causes.isEmpty()) ? t : causes.get(0);
          if (ep != null) {
            errors.put(ep, toV3(cause));
          }
        }
      } catch (RuntimeException ignore) {
        // best effort
      }
      return new NoHostAvailableException(errors);
    }

    // ---- timeout -----------------------------------------------------------------------------
    if (t instanceof DriverTimeoutException) {
      return new OperationTimedOutException((EndPoint) null, t.getMessage());
    }

    // ---- coordinator (server) errors ---------------------------------------------------------
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.CoordinatorException) {
      return fromCoordinator(
          (com.datastax.oss.driver.api.core.servererrors.CoordinatorException) t);
    }

    // ---- authentication ----------------------------------------------------------------------
    if (t instanceof com.datastax.oss.driver.api.core.auth.AuthenticationException) {
      EndPoint ep =
          endPoint(
              ((com.datastax.oss.driver.api.core.auth.AuthenticationException) t).getEndPoint());
      return new AuthenticationException(ep, t.getMessage());
    }

    // ---- connection / protocol (PARTIAL: 4.x carries less context; see mapping spec) ----------
    if (t instanceof com.datastax.oss.driver.api.core.connection.FrameTooLongException) {
      // 4.x exposes (SocketAddress,message); 3.x carries a streamId (disjoint) -> lossy (-1).
      return new FrameTooLongException(-1);
    }
    if (t instanceof com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException) {
      com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException e =
          (com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException) t;
      EndPoint ep = endPoint(e.getEndPoint());
      // 4.x exposes only getAttemptedVersions(); 3.x wants an "unsupported" and a "server" version.
      ProtocolVersion attempted = firstProtocolVersion(e.getAttemptedVersions());
      return new UnsupportedProtocolVersionException(ep, attempted, attempted, e);
    }
    if (t instanceof com.datastax.oss.driver.api.core.connection.HeartbeatException) {
      EndPoint ep =
          endPoint(((com.datastax.oss.driver.api.core.connection.HeartbeatException) t).getAddress());
      return new TransportException(ep, t.getMessage(), t);
    }
    if (t instanceof com.datastax.oss.driver.api.core.connection.BusyConnectionException) {
      return new BusyConnectionException((EndPoint) null, t);
    }
    if (t instanceof com.datastax.oss.driver.api.core.connection.ClosedConnectionException) {
      return new TransportException((EndPoint) null, t.getMessage(), t);
    }

    // ---- fallback ----------------------------------------------------------------------------
    if (t instanceof RuntimeException) {
      return new DriverInternalError(t.getMessage(), t);
    }
    return new DriverException(t.getMessage(), t);
  }

  private static RuntimeException fromCoordinator(
      com.datastax.oss.driver.api.core.servererrors.CoordinatorException t) {
    EndPoint ep = endPoint(t.getCoordinator());
    String msg = t.getMessage();

    if (t instanceof com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException) {
      com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException e =
          (com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException) t;
      return new ReadTimeoutException(
          ep, cl(e.getConsistencyLevel()), e.getReceived(), e.getBlockFor(), e.wasDataPresent());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException) {
      com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException e =
          (com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException) t;
      return new WriteTimeoutException(
          ep, cl(e.getConsistencyLevel()), wt(e.getWriteType()), e.getReceived(), e.getBlockFor());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.ReadFailureException) {
      com.datastax.oss.driver.api.core.servererrors.ReadFailureException e =
          (com.datastax.oss.driver.api.core.servererrors.ReadFailureException) t;
      return new ReadFailureException(
          ep,
          cl(e.getConsistencyLevel()),
          e.getReceived(),
          e.getBlockFor(),
          e.getNumFailures(),
          e.getReasonMap(),
          e.wasDataPresent());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.WriteFailureException) {
      com.datastax.oss.driver.api.core.servererrors.WriteFailureException e =
          (com.datastax.oss.driver.api.core.servererrors.WriteFailureException) t;
      return new WriteFailureException(
          ep,
          cl(e.getConsistencyLevel()),
          wt(e.getWriteType()),
          e.getReceived(),
          e.getBlockFor(),
          e.getNumFailures(),
          e.getReasonMap());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.UnavailableException) {
      com.datastax.oss.driver.api.core.servererrors.UnavailableException e =
          (com.datastax.oss.driver.api.core.servererrors.UnavailableException) t;
      return new UnavailableException(cl(e.getConsistencyLevel()), e.getRequired(), e.getAlive());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.TruncateException) {
      return new TruncateException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.OverloadedException) {
      return new OverloadedException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.BootstrappingException) {
      return new BootstrappingException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.ServerError) {
      return new ServerError(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.ProtocolError) {
      return new ProtocolError(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.SyntaxError) {
      return new SyntaxError(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.UnauthorizedException) {
      return new UnauthorizedException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException) {
      return new InvalidConfigurationInQueryException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException) {
      return new AlreadyExistsException(ep, "", "");
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.FunctionFailureException) {
      return new FunctionExecutionException(ep, msg);
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException) {
      com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException e =
          (com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException) t;
      return new CASWriteUnknownException(
          ep, cl(e.getConsistencyLevel()), e.getReceived(), e.getBlockFor());
    }
    if (t instanceof com.datastax.oss.driver.api.core.servererrors.CDCWriteFailureException) {
      return new CDCWriteException(ep, msg);
    }
    // QueryValidationException / InvalidQueryException and anything else coordinator-shaped
    return new InvalidQueryException(ep, msg);
  }

  private static ConsistencyLevel cl(
      com.datastax.oss.driver.api.core.ConsistencyLevel v4) {
    try {
      return ConsistencyLevel.valueOf(v4.name());
    } catch (RuntimeException e) {
      return ConsistencyLevel.ONE;
    }
  }

  private static WriteType wt(
      com.datastax.oss.driver.api.core.servererrors.WriteType v4) {
    try {
      return WriteType.valueOf(v4.name());
    } catch (RuntimeException e) {
      return WriteType.SIMPLE;
    }
  }

  private static EndPoint endPoint(Node node) {
    if (node == null) {
      return null;
    }
    try {
      return endPoint(node.getEndPoint());
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static EndPoint endPoint(com.datastax.oss.driver.api.core.metadata.EndPoint v4) {
    if (v4 == null) {
      return null;
    }
    try {
      SocketAddress addr = v4.resolve();
      if (addr instanceof InetSocketAddress) {
        return new InetSocketAddressEndPoint((InetSocketAddress) addr);
      }
    } catch (RuntimeException e) {
      // fall through
    }
    return null;
  }

  private static EndPoint endPoint(SocketAddress addr) {
    if (addr instanceof InetSocketAddress) {
      return new InetSocketAddressEndPoint((InetSocketAddress) addr);
    }
    return null;
  }

  private static ProtocolVersion firstProtocolVersion(
      List<com.datastax.oss.driver.api.core.ProtocolVersion> versions) {
    if (versions == null || versions.isEmpty()) {
      return null;
    }
    try {
      return ProtocolVersion.fromInt(versions.get(0).getCode());
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static Throwable unwrap(Throwable t) {
    while ((t instanceof CompletionException || t instanceof ExecutionException)
        && t.getCause() != null
        && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }
}
