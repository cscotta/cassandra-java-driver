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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configurable {@link LatencyTracker} that logs all executed statements. Shim port of the 3.12.1
 * implementation, reimplemented over the shim statement types.
 *
 * <p>Registration hooks into the shim {@link Cluster}'s latency-tracker machinery. The logging
 * logic itself is self-contained and does not depend on the 4.x {@code RequestLogger}.
 */
public abstract class QueryLogger implements LatencyTracker {

  public static final long DEFAULT_SLOW_QUERY_THRESHOLD_MS = 5000;

  public static final double DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE = 99.0;

  public static final int DEFAULT_MAX_QUERY_STRING_LENGTH = 500;

  public static final int DEFAULT_MAX_PARAMETER_VALUE_LENGTH = 50;

  public static final int DEFAULT_MAX_LOGGED_PARAMETERS = 50;

  public static final Logger NORMAL_LOGGER =
      LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.NORMAL");

  public static final Logger SLOW_LOGGER =
      LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.SLOW");

  public static final Logger ERROR_LOGGER =
      LoggerFactory.getLogger("com.datastax.driver.core.QueryLogger.ERROR");

  private static final String NORMAL_TEMPLATE =
      "[%s] [%s] Query completed normally, took %s ms: %s";

  private static final String SLOW_TEMPLATE_MILLIS = "[%s] [%s] Query too slow, took %s ms: %s";

  private static final String SLOW_TEMPLATE_PERCENTILE =
      "[%s] [%s] Query too slow, took %s ms (%s percentile = %s ms): %s";

  private static final String ERROR_TEMPLATE = "[%s] [%s] Query error after %s ms: %s";

  @VisibleForTesting static final String TRUNCATED_OUTPUT = "... [truncated output]";

  @VisibleForTesting static final String FURTHER_PARAMS_OMITTED = " [further parameters omitted]";

  protected volatile Cluster cluster;

  private volatile ProtocolVersion protocolVersion;

  protected volatile int maxQueryStringLength;

  protected volatile int maxParameterValueLength;

  protected volatile int maxLoggedParameters;

  private QueryLogger(
      int maxQueryStringLength, int maxParameterValueLength, int maxLoggedParameters) {
    this.maxQueryStringLength = maxQueryStringLength;
    this.maxParameterValueLength = maxParameterValueLength;
    this.maxLoggedParameters = maxLoggedParameters;
  }

  public static QueryLogger.Builder builder() {
    return new QueryLogger.Builder();
  }

  @Override
  public void onRegister(Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public void onUnregister(Cluster cluster) {
    // nothing to do
  }

  /** A QueryLogger that uses a constant threshold in milliseconds to track slow queries. */
  public static class ConstantThresholdQueryLogger extends QueryLogger {

    private volatile long slowQueryLatencyThresholdMillis;

    private ConstantThresholdQueryLogger(
        int maxQueryStringLength,
        int maxParameterValueLength,
        int maxLoggedParameters,
        long slowQueryLatencyThresholdMillis) {
      super(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters);
      this.setSlowQueryLatencyThresholdMillis(slowQueryLatencyThresholdMillis);
    }

    public long getSlowQueryLatencyThresholdMillis() {
      return slowQueryLatencyThresholdMillis;
    }

    public void setSlowQueryLatencyThresholdMillis(long slowQueryLatencyThresholdMillis) {
      if (slowQueryLatencyThresholdMillis <= 0)
        throw new IllegalArgumentException(
            "Invalid slowQueryLatencyThresholdMillis, should be > 0, got "
                + slowQueryLatencyThresholdMillis);
      this.slowQueryLatencyThresholdMillis = slowQueryLatencyThresholdMillis;
    }

    @Override
    protected void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs) {
      if (latencyMs > slowQueryLatencyThresholdMillis) {
        maybeLogSlowQuery(host, statement, latencyMs);
      } else {
        maybeLogNormalQuery(host, statement, latencyMs);
      }
    }

    protected void maybeLogSlowQuery(Host host, Statement statement, long latencyMs) {
      if (SLOW_LOGGER.isDebugEnabled()) {
        String message =
            String.format(
                SLOW_TEMPLATE_MILLIS,
                cluster.getClusterName(),
                host,
                latencyMs,
                statementAsString(statement));
        logQuery(statement, null, SLOW_LOGGER, message);
      }
    }
  }

  /** A QueryLogger that uses a dynamic threshold in milliseconds to track slow queries. */
  public static class DynamicThresholdQueryLogger extends QueryLogger {

    private volatile double slowQueryLatencyThresholdPercentile;

    private volatile PercentileTracker percentileLatencyTracker;

    private DynamicThresholdQueryLogger(
        int maxQueryStringLength,
        int maxParameterValueLength,
        int maxLoggedParameters,
        double slowQueryLatencyThresholdPercentile,
        PercentileTracker percentileLatencyTracker) {
      super(maxQueryStringLength, maxParameterValueLength, maxLoggedParameters);
      this.setSlowQueryLatencyThresholdPercentile(slowQueryLatencyThresholdPercentile);
      this.setPercentileLatencyTracker(percentileLatencyTracker);
    }

    public PercentileTracker getPercentileLatencyTracker() {
      return percentileLatencyTracker;
    }

    public void setPercentileLatencyTracker(PercentileTracker percentileLatencyTracker) {
      if (percentileLatencyTracker == null)
        throw new IllegalArgumentException("perHostPercentileLatencyTracker cannot be null");
      this.percentileLatencyTracker = percentileLatencyTracker;
    }

    public double getSlowQueryLatencyThresholdPercentile() {
      return slowQueryLatencyThresholdPercentile;
    }

    public void setSlowQueryLatencyThresholdPercentile(double slowQueryLatencyThresholdPercentile) {
      if (slowQueryLatencyThresholdPercentile < 0.0 || slowQueryLatencyThresholdPercentile >= 100.0)
        throw new IllegalArgumentException(
            "Invalid slowQueryLatencyThresholdPercentile, should be >= 0 and < 100, got "
                + slowQueryLatencyThresholdPercentile);
      this.slowQueryLatencyThresholdPercentile = slowQueryLatencyThresholdPercentile;
    }

    @Override
    protected void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs) {
      long threshold =
          percentileLatencyTracker.getLatencyAtPercentile(
              host, statement, null, slowQueryLatencyThresholdPercentile);
      if (threshold >= 0 && latencyMs > threshold) {
        maybeLogSlowQuery(host, statement, latencyMs, threshold);
      } else {
        maybeLogNormalQuery(host, statement, latencyMs);
      }
    }

    protected void maybeLogSlowQuery(
        Host host, Statement statement, long latencyMs, long threshold) {
      if (SLOW_LOGGER.isDebugEnabled()) {
        String message =
            String.format(
                SLOW_TEMPLATE_PERCENTILE,
                cluster.getClusterName(),
                host,
                latencyMs,
                slowQueryLatencyThresholdPercentile,
                threshold,
                statementAsString(statement));
        logQuery(statement, null, SLOW_LOGGER, message);
      }
    }

    @Override
    public void onRegister(Cluster cluster) {
      super.onRegister(cluster);
      cluster.register(percentileLatencyTracker);
    }
  }

  /** Helper class to build {@link QueryLogger} instances with a fluent API. */
  public static class Builder {

    private int maxQueryStringLength = DEFAULT_MAX_QUERY_STRING_LENGTH;

    private int maxParameterValueLength = DEFAULT_MAX_PARAMETER_VALUE_LENGTH;

    private int maxLoggedParameters = DEFAULT_MAX_LOGGED_PARAMETERS;

    private long slowQueryLatencyThresholdMillis = DEFAULT_SLOW_QUERY_THRESHOLD_MS;

    private double slowQueryLatencyThresholdPercentile = DEFAULT_SLOW_QUERY_THRESHOLD_PERCENTILE;

    private PercentileTracker percentileLatencyTracker;

    private boolean constantThreshold = true;

    public Builder withConstantThreshold(long slowQueryLatencyThresholdMillis) {
      this.slowQueryLatencyThresholdMillis = slowQueryLatencyThresholdMillis;
      constantThreshold = true;
      return this;
    }

    public Builder withDynamicThreshold(
        PercentileTracker percentileLatencyTracker, double slowQueryLatencyThresholdPercentile) {
      this.percentileLatencyTracker = percentileLatencyTracker;
      this.slowQueryLatencyThresholdPercentile = slowQueryLatencyThresholdPercentile;
      constantThreshold = false;
      return this;
    }

    public Builder withMaxQueryStringLength(int maxQueryStringLength) {
      this.maxQueryStringLength = maxQueryStringLength;
      return this;
    }

    public Builder withMaxParameterValueLength(int maxParameterValueLength) {
      this.maxParameterValueLength = maxParameterValueLength;
      return this;
    }

    public Builder withMaxLoggedParameters(int maxLoggedParameters) {
      this.maxLoggedParameters = maxLoggedParameters;
      return this;
    }

    public QueryLogger build() {
      if (constantThreshold) {
        return new ConstantThresholdQueryLogger(
            maxQueryStringLength,
            maxParameterValueLength,
            maxLoggedParameters,
            slowQueryLatencyThresholdMillis);
      } else {
        return new DynamicThresholdQueryLogger(
            maxQueryStringLength,
            maxParameterValueLength,
            maxLoggedParameters,
            slowQueryLatencyThresholdPercentile,
            percentileLatencyTracker);
      }
    }
  }

  public int getMaxQueryStringLength() {
    return maxQueryStringLength;
  }

  public void setMaxQueryStringLength(int maxQueryStringLength) {
    if (maxQueryStringLength <= 0 && maxQueryStringLength != -1)
      throw new IllegalArgumentException(
          "Invalid maxQueryStringLength, should be > 0 or -1, got " + maxQueryStringLength);
    this.maxQueryStringLength = maxQueryStringLength;
  }

  public int getMaxParameterValueLength() {
    return maxParameterValueLength;
  }

  public void setMaxParameterValueLength(int maxParameterValueLength) {
    if (maxParameterValueLength <= 0 && maxParameterValueLength != -1)
      throw new IllegalArgumentException(
          "Invalid maxParameterValueLength, should be > 0 or -1, got " + maxParameterValueLength);
    this.maxParameterValueLength = maxParameterValueLength;
  }

  public int getMaxLoggedParameters() {
    return maxLoggedParameters;
  }

  public void setMaxLoggedParameters(int maxLoggedParameters) {
    if (maxLoggedParameters <= 0 && maxLoggedParameters != -1)
      throw new IllegalArgumentException(
          "Invalid maxLoggedParameters, should be > 0 or -1, got " + maxLoggedParameters);
    this.maxLoggedParameters = maxLoggedParameters;
  }

  @Override
  public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
    if (cluster == null)
      throw new IllegalStateException(
          "This method should only be called after the logger has been registered with a cluster");

    if (statement instanceof StatementWrapper)
      statement = ((StatementWrapper) statement).getWrappedStatement();

    long latencyMs = NANOSECONDS.toMillis(newLatencyNanos);
    if (exception == null) {
      maybeLogNormalOrSlowQuery(host, statement, latencyMs);
    } else {
      maybeLogErrorQuery(host, statement, exception, latencyMs);
    }
  }

  protected abstract void maybeLogNormalOrSlowQuery(Host host, Statement statement, long latencyMs);

  protected void maybeLogNormalQuery(Host host, Statement statement, long latencyMs) {
    if (NORMAL_LOGGER.isDebugEnabled()) {
      String message =
          String.format(
              NORMAL_TEMPLATE,
              cluster.getClusterName(),
              host,
              latencyMs,
              statementAsString(statement));
      logQuery(statement, null, NORMAL_LOGGER, message);
    }
  }

  protected void maybeLogErrorQuery(
      Host host, Statement statement, Exception exception, long latencyMs) {
    if (ERROR_LOGGER.isDebugEnabled()
        && !(exception instanceof CancelledSpeculativeExecutionException)) {
      String message =
          String.format(
              ERROR_TEMPLATE,
              cluster.getClusterName(),
              host,
              latencyMs,
              statementAsString(statement));
      logQuery(statement, exception, ERROR_LOGGER, message);
    }
  }

  protected void logQuery(Statement statement, Exception exception, Logger logger, String message) {
    boolean showParameterValues = logger.isTraceEnabled();
    if (showParameterValues) {
      StringBuilder params = new StringBuilder();
      if (statement instanceof BoundStatement) {
        appendParameters((BoundStatement) statement, params, maxLoggedParameters);
      } else if (statement instanceof SimpleStatement) {
        appendParameters((SimpleStatement) statement, params, maxLoggedParameters);
      } else if (statement instanceof BatchStatement) {
        BatchStatement batchStatement = (BatchStatement) statement;
        int remaining = maxLoggedParameters;
        for (Statement inner : batchStatement.getStatements()) {
          if (inner instanceof BoundStatement) {
            remaining = appendParameters((BoundStatement) inner, params, remaining);
          } else if (inner instanceof SimpleStatement) {
            remaining = appendParameters((SimpleStatement) inner, params, remaining);
          }
        }
      } else if (statement instanceof BuiltStatement) {
        appendParameters((BuiltStatement) statement, params, maxLoggedParameters);
      }
      if (params.length() > 0) params.append("]");
      logger.trace(message + params, exception);
    } else {
      logger.debug(message, exception);
    }
  }

  protected String statementAsString(Statement statement) {
    StringBuilder sb = new StringBuilder();
    if (statement instanceof BatchStatement) {
      BatchStatement bs = (BatchStatement) statement;
      int statements = bs.getStatements().size();
      int boundValues = countBoundValues(bs);
      sb.append("[" + statements + " statements, " + boundValues + " bound values] ");
    } else if (statement instanceof BoundStatement) {
      int boundValues = ((BoundStatement) statement).wrapper.values.length;
      sb.append("[" + boundValues + " bound values] ");
    } else if (statement instanceof SimpleStatement) {
      int boundValues = ((SimpleStatement) statement).valuesCount();
      sb.append("[" + boundValues + " bound values] ");
    }

    append(statement, sb, maxQueryStringLength);
    return sb.toString();
  }

  protected int countBoundValues(BatchStatement bs) {
    int count = 0;
    for (Statement s : bs.getStatements()) {
      if (s instanceof BoundStatement) count += ((BoundStatement) s).wrapper.values.length;
      else if (s instanceof SimpleStatement) count += ((SimpleStatement) s).valuesCount();
    }
    return count;
  }

  protected int appendParameters(BoundStatement statement, StringBuilder buffer, int remaining) {
    if (remaining == 0) return 0;
    ColumnDefinitions metadata = statement.preparedStatement().getVariables();
    int numberOfParameters = metadata.size();
    if (numberOfParameters > 0) {
      List<ColumnDefinitions.Definition> definitions = metadata.asList();
      int numberOfLoggedParameters;
      if (remaining == -1) {
        numberOfLoggedParameters = numberOfParameters;
      } else {
        numberOfLoggedParameters = Math.min(remaining, numberOfParameters);
        remaining -= numberOfLoggedParameters;
      }
      for (int i = 0; i < numberOfLoggedParameters; i++) {
        if (buffer.length() == 0) buffer.append(" [");
        else buffer.append(", ");
        String value =
            statement.isSet(i)
                ? parameterValueAsString(definitions.get(i), statement.wrapper.values[i])
                : "<UNSET>";
        buffer.append(String.format("%s:%s", metadata.getName(i), value));
      }
      if (numberOfLoggedParameters < numberOfParameters) {
        buffer.append(FURTHER_PARAMS_OMITTED);
      }
    }
    return remaining;
  }

  protected String parameterValueAsString(ColumnDefinitions.Definition definition, ByteBuffer raw) {
    String valueStr;
    if (raw == null || raw.remaining() == 0) {
      valueStr = "NULL";
    } else {
      DataType type = definition.getType();
      CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
      TypeCodec<Object> codec = codecRegistry.codecFor(type);
      int maxParameterValueLength = this.maxParameterValueLength;
      if (type.equals(DataType.blob()) && maxParameterValueLength != -1) {
        int maxBufferLength = Math.max(2, (maxParameterValueLength - 2) / 2);
        boolean bufferTooLarge = raw.remaining() > maxBufferLength;
        if (bufferTooLarge) {
          raw = (ByteBuffer) raw.duplicate().limit(maxBufferLength);
        }
        Object value = codec.deserialize(raw, protocolVersion());
        valueStr = codec.format(value);
        if (bufferTooLarge) {
          valueStr = valueStr + TRUNCATED_OUTPUT;
        }
      } else {
        Object value = codec.deserialize(raw, protocolVersion());
        valueStr = codec.format(value);
        if (maxParameterValueLength != -1 && valueStr.length() > maxParameterValueLength) {
          valueStr = valueStr.substring(0, maxParameterValueLength) + TRUNCATED_OUTPUT;
        }
      }
    }
    return valueStr;
  }

  protected int appendParameters(SimpleStatement statement, StringBuilder buffer, int remaining) {
    if (remaining == 0) return 0;
    int numberOfParameters = statement.valuesCount();
    if (numberOfParameters > 0) {
      int numberOfLoggedParameters;
      if (remaining == -1) {
        numberOfLoggedParameters = numberOfParameters;
      } else {
        numberOfLoggedParameters = remaining > numberOfParameters ? numberOfParameters : remaining;
        remaining -= numberOfLoggedParameters;
      }
      Iterator<String> valueNames = null;
      if (statement.usesNamedValues()) {
        valueNames = statement.getValueNames().iterator();
      }
      for (int i = 0; i < numberOfLoggedParameters; i++) {
        if (buffer.length() == 0) buffer.append(" [");
        else buffer.append(", ");
        if (valueNames != null && valueNames.hasNext()) {
          String valueName = valueNames.next();
          buffer.append(
              String.format(
                  "%s:%s", valueName, parameterValueAsString(statement.getObject(valueName))));
        } else {
          buffer.append(parameterValueAsString(statement.getObject(i)));
        }
      }
      if (numberOfLoggedParameters < numberOfParameters) {
        buffer.append(FURTHER_PARAMS_OMITTED);
      }
    }
    return remaining;
  }

  protected String parameterValueAsString(Object value) {
    String valueStr;
    if (value == null) {
      valueStr = "NULL";
    } else {
      CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();
      TypeCodec<Object> codec = codecRegistry.codecFor(value);
      int maxParameterValueLength = this.maxParameterValueLength;
      if (codec.cqlType.equals(DataType.blob()) && maxParameterValueLength != -1) {
        ByteBuffer buf = (ByteBuffer) value;
        int maxBufferLength = Math.max(2, (maxParameterValueLength - 2) / 2);
        boolean bufferTooLarge = buf.remaining() > maxBufferLength;
        if (bufferTooLarge) {
          value = (ByteBuffer) buf.duplicate().limit(maxBufferLength);
        }
        valueStr = codec.format(value);
        if (bufferTooLarge) {
          valueStr = valueStr + TRUNCATED_OUTPUT;
        }
      } else {
        valueStr = codec.format(value);
        if (maxParameterValueLength != -1 && valueStr.length() > maxParameterValueLength) {
          valueStr = valueStr.substring(0, maxParameterValueLength) + TRUNCATED_OUTPUT;
        }
      }
    }
    return valueStr;
  }

  protected int appendParameters(BuiltStatement statement, StringBuilder buffer, int remaining) {
    if (remaining == 0) {
      return 0;
    }
    ByteBuffer[] values =
        statement.getValues(protocolVersion(), cluster.getConfiguration().getCodecRegistry());
    int numberOfParameters = values == null ? 0 : values.length;
    if (numberOfParameters > 0) {
      int numberOfLoggedParameters;
      if (remaining == -1) {
        numberOfLoggedParameters = numberOfParameters;
      } else {
        numberOfLoggedParameters = remaining > numberOfParameters ? numberOfParameters : remaining;
        remaining -= numberOfLoggedParameters;
      }

      for (int i = 0; i < numberOfLoggedParameters; i++) {
        if (buffer.length() == 0) {
          buffer.append(" [");
        } else {
          buffer.append(", ");
        }

        buffer.append(parameterValueAsString(statement.getObject(i)));
      }
      if (numberOfLoggedParameters < numberOfParameters) {
        buffer.append(FURTHER_PARAMS_OMITTED);
      }
    }
    return remaining;
  }

  private ProtocolVersion protocolVersion() {
    if (protocolVersion == null) {
      protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    }
    return protocolVersion;
  }

  protected int append(Statement statement, StringBuilder buffer, int remaining) {
    if (statement instanceof RegularStatement) {
      RegularStatement rs = (RegularStatement) statement;
      String query = rs.getQueryString();
      remaining = append(query.trim(), buffer, remaining);
    } else if (statement instanceof BoundStatement) {
      remaining =
          append(
              ((BoundStatement) statement).preparedStatement().getQueryString().trim(),
              buffer,
              remaining);
    } else if (statement instanceof BatchStatement) {
      BatchStatement batchStatement = (BatchStatement) statement;
      remaining = append("BEGIN", buffer, remaining);
      switch (batchStatement.batchType) {
        case UNLOGGED:
          append(" UNLOGGED", buffer, remaining);
          break;
        case COUNTER:
          append(" COUNTER", buffer, remaining);
          break;
      }
      remaining = append(" BATCH", buffer, remaining);
      for (Statement stmt : batchStatement.getStatements()) {
        remaining = append(" ", buffer, remaining);
        remaining = append(stmt, buffer, remaining);
      }
      remaining = append(" APPLY BATCH", buffer, remaining);
    } else {
      remaining = append(statement.toString(), buffer, remaining);
    }
    if (buffer.charAt(buffer.length() - 1) != ';') {
      remaining = append(";", buffer, remaining);
    }
    return remaining;
  }

  protected int append(CharSequence str, StringBuilder buffer, int remaining) {
    if (remaining == -2) {
      // capacity exceeded
    } else if (remaining == -1) {
      buffer.append(str);
    } else if (str.length() > remaining) {
      buffer.append(str, 0, remaining).append(TRUNCATED_OUTPUT);
      remaining = -2;
    } else {
      buffer.append(str);
      remaining -= str.length();
    }
    return remaining;
  }
}
