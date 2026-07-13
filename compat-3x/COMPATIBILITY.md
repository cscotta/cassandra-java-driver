# Cassandra Java Driver 3.12.1 → 4.x Compatibility Shim

This module (`cassandra-driver-shim`) reimplements the **3.12.1** driver public API
(`com.datastax.driver.*` — core, mapping, and extras) on top of the **4.x** driver
(`com.datastax.oss.driver.*` / `java-driver-core`). It lets an application written against
`cassandra-driver-{core,mapping,extras}:3.12.1` run on the 4.x engine as a **drop-in dependency
swap**, without source changes.

## What is guaranteed

- **Binary compatibility (ABI).** Verified with `japicmp` against the real 3.12.1 jars:
  **0 incompatible** class/method/field changes across `core`, `mapping`, and `extras`
  (public + protected, generic signatures, constant values, and synthetic bridge methods compared).
  Code compiled against 3.12.1 links against this shim unchanged.
- **Behavioral parity.** A differential suite runs the same program against the real 3.12.1 driver
  and against this shim, both pointed at the same live Cassandra, and diffs the observations.
  All 149 checks are identical — connect/protocol, CRUD, prepared/batch, the CQL type set
  (incl. date/time/smallint/tinyint, nested collections, UDT, tuple), collections, paging, schema
  metadata, exceptions, the QueryBuilder/SchemaBuilder CQL DSLs (byte-for-byte), async
  (`ResultSetFuture`), consistency levels, the extra codecs, the runtime object mapper
  (`MappingManager`/`Mapper` save/get/delete), lightweight transactions, counters, TTL, static
  columns, an arbitrary custom load-balancing policy honored through the 4.x SPI (including the
  per-request routing key it observes), speculative execution — a custom `SpeculativeExecutionPolicy`
  driven through the 4.x SPI and a `PercentileSpeculativeExecutionPolicy` whose tracker is fed and
  warmed — custom `NettyOptions`/`ThreadingOptions` hooks bridged onto the 4.x internal Netty SPI, a
  custom `TimestampGenerator` (verified via `WRITETIME`), cluster/per-request `RetryPolicy` and
  `ReconnectionPolicy` wiring, node-state and schema-change listeners (a `CREATE TABLE` fires
  `onTableAdded`), the `QueryOptions` refresh-debouncer knobs, `PoolingOptions` round-trips, real
  Dropwizard metrics (live registry + recorded `cql-requests`, JMX reporting on by default), and a
  custom `EndPointFactory` bridged onto the 4.x topology monitor.
- **Verified against the 3.12.1 driver's own test suite.** The 3.12.1 driver's own TestNG unit suite
  (`@Test(groups = "unit")`) was run against the shim with the real 3.x driver **absent** from the
  classpath — the 3.x-compiled test bytecode links against the shim purely through binary
  compatibility. **64 public-API unit classes / 695 test methods pass against the shim, identical to
  the real 3.12.1 driver** (core 47/427, extras 15/207, mapping 2/61). Excluded classes are
  **internal-bound** (they target package-private 3.x internals the shim deliberately omits — the
  protocol-v5 `Segment*`/`Frame` framing, `Connection`/`AbstractReconnectionHandler`,
  `ReplicationStrategy`/`ReplicationFactor`/`Cluster.Manager`, `DirectedGraph`, `SimpleJSONParser`,
  `StreamIdGenerator`, `ClockFactory`+`Native`, `EventDebouncer`, `RollingCount`) and **impl-detail**
  (`StatementSizeTest` — the shim does not recompute the 3.x request wire-frame size, so
  `Statement.requestSizeInBytes(...)` returns `-1`). The full results table, categorized exclusion
  list, and reproduction harness live in [`parity/upstream-tests/`](parity/upstream-tests/)
  (`RESULTS.md`, `exclusions.txt`, `run-upstream.sh`). Three package-private shim gaps that the
  upstream tests surfaced were fixed (`PoolingOptions.setProtocolVersion`, the injectable `Clock` seam
  on the monotonic timestamp generators, and the internal `SystemProperties` helper); all are additive
  and invisible to `japicmp`.

## Requirements

- **JDK 8+** (the shim is compiled to Java 8 bytecode, matching 3.12.1).
- Transitively pulls `java-driver-core` (4.x) and **unshaded Guava** (3.12.1 exposed Guava types
  such as `ListenableFuture`, `TypeToken`, `Optional`, `ImmutableList` in its public API, so the
  shim links plain Guava; the 4.x driver's own Guava is shaded and does not conflict).

## Behavioral differences

Every 3.12.1 public type and member exists at the ABI level. Most APIs that 4.x reorganized, made
config-driven, or hid behind internal SPIs are **bridged** and behave as they did in 3.x — see
**Supported via delegation** below (several of those bridges reach into 4.x *internal* SPIs, an
approved trade-off noted per entry). The remaining items are genuinely absent in 4.x; per the
project's chosen policy they **fail fast** (throw `UnsupportedOperationException` with a migration
note), **no-op with a one-time warning**, or are **inert/lossy** — never silent misbehavior — and
are listed after the supported set.

### Supported via delegation (with caveats)

- **Arbitrary custom load-balancing policies.** `Cluster.Builder.withLoadBalancingPolicy(...)` honors
  any user-written 3.x `LoadBalancingPolicy` by delegating to it through the 4.x load-balancing SPI.
  4.x instantiates the policy reflectively from `basic.load-balancing-policy.class` (there is no API
  to pass an instance), so the shim registers the user's policy instance under a fresh token, points
  that class option at an adapter (`com.datastax.shim.bridge.Shim3xLoadBalancingPolicy`), and injects
  the token into the programmatic config. The adapter recovers the instance, maps the 4.x `Node`s to
  the shim's cached 3.x `Host`s, and forwards `init` / `distance` / `newQueryPlan` /
  `onAdd`/`onUp`/`onDown`/`onRemove` / `close`. Per-request token-aware routing is preserved:
  `newQueryPlan` copies the request's routing key, routing keyspace, and consistency level into the
  3.x statement handed to the policy, and forwards the session keyspace as the logged keyspace, so a
  token-aware 3.x policy routes on the same key the 4.x engine computed (the differential suite
  asserts an identical routing key reaches the policy under both engines). The
  `DCAwareRoundRobinPolicy` local-DC translation to `withLocalDatacenter` still applies
  (belt-and-suspenders), including when wrapped in `TokenAwarePolicy`. Caveats:
  - *Distance is push (4.x) vs pull (3.x).* The adapter pushes each node's distance to the 4.x
    `DistanceReporter` at `init` and re-pushes on every topology event; a policy that varies distance
    purely over time (with no add/up/down/remove) will not have the change observed until the next
    event.
  - *`Cluster` introspection during `init`.* The 3.x `Cluster` handed to `init` is the shim facade;
    `getConfiguration()` works, but a policy that drives I/O (`connect()`/`getMetadata()`) from within
    `init` is unsupported (it would re-enter session construction).

- **`PercentileSpeculativeExecutionPolicy` and arbitrary custom speculative-execution policies.**
  `Cluster.Builder.withSpeculativeExecutionPolicy(...)` honors any non-`Constant`/non-`No` 3.x
  `SpeculativeExecutionPolicy` by delegating to it through the 4.x specex SPI (adapter
  `com.datastax.shim.bridge.Shim3xSpeculativeExecutionPolicy`, same config-token hand-off as the
  load-balancing adapter). The adapter preserves 3.x behavior — it does not reimplement the
  percentile math; it keeps one 3.x `SpeculativeExecutionPlan` per in-flight request (weak-keyed to
  avoid leaks), forwards `plan.nextExecution(host)` (the percentile-latency delay in ms, or a
  negative value to stop, preserving `maxSpeculativeExecutions`), and calls `v3.init(cluster)` once so
  a `PercentileSpeculativeExecutionPolicy` can register its tracker. Latencies reach that tracker
  because the shim installs one build-time 4.x `RequestTracker` that fans per-node success/error
  latencies out to the `LatencyTracker`s registered on the `Cluster` (see below). `Constant`/`No`
  speculative-execution policies keep their existing (inert) translation. Caveats:
  - *Percentile warm-up.* Statistics are unavailable until a full interval has elapsed with at least
    `minRecordedValues` samples (as in 3.x); until then `getLatencyAtPercentile` returns a negative
    value and no speculative execution is triggered.
  - *Single-node speculation is not observable.* With one reachable node there is no other node to
    speculate to, so the differential suite asserts the wiring (custom `newPlan` invoked, tracker
    warmed) rather than a fired cross-node speculation.
  - *`LatencyTracker` feed statement.* The 3.x `LatencyTracker.update` receives a placeholder
    statement carrying the request's routing info but not the original CQL; host-categorizing
    trackers (e.g. `PerHostPercentileTracker`) are unaffected.

- **`Cluster.register/unregister(LatencyTracker)`.** Functional: registered trackers are held on the
  `Cluster` and fed the per-node request latency (host, placeholder statement, translated 3.x
  exception or `null`, latency-nanos) from the build-time 4.x `RequestTracker` installed on every
  session. Trackers registered after `connect()` are still fed. `onRegister`/`onUnregister` fire on
  (un)registration.

- **`Cluster.register/unregister(Host.StateListener)`.** Functional: a build-time 4.x
  `NodeStateListener` fans node add/up/down/remove events (translating `Node` → `Host`) out to the
  registered 3.x listeners; `register`/`unregister` fire `onRegister`/`onUnregister(cluster)`, and
  `Cluster.Builder.withInitialListeners(...)` registers at build time. (Node up/down failure events
  aren't induced against a single healthy node, so the differential suite asserts the
  register/unregister wiring.)

- **`Cluster.register/unregister(SchemaChangeListener)`.** Functional: a build-time 4.x
  `SchemaChangeListener` translates 4.x Created/Dropped/Updated events into the 3.x
  Added/Removed/Changed vocabulary for keyspaces, tables, UDTs (`UserDefinedType` → `UserType`),
  functions, aggregates, and views (`ViewMetadata` → `MaterializedViewMetadata`) — reusing the
  existing metadata wrappers — and fans them out to registered listeners. Verified end-to-end
  (a `CREATE TABLE` fires `onTableAdded`). Child wrappers (table/function/aggregate/view) resolve
  their parent keyspace from live metadata; if the parent keyspace can't be resolved (e.g. the
  keyspace itself was dropped) that per-object event is skipped.

- **`QueryOptions` node/schema refresh debouncing.** `refreshNodeIntervalMillis` /
  `maxPendingRefreshNodeRequests` map to `advanced.metadata.topology-event-debouncer.window` /
  `.max-events`, and `refreshSchemaIntervalMillis` / `maxPendingRefreshSchemaRequests` map to
  `advanced.metadata.schema.debouncer.window` / `.max-events`. The getters round-trip.

- **`Cluster.Builder.withTimestampGenerator(TimestampGenerator)`.** Functional: bridged onto 4.x's
  `TimestampGenerator` SPI (both are `long next()` in microseconds, with `Long.MIN_VALUE` meaning
  "server-side timestamp"), so the client-side query timestamp is generated by the 3.x generator.
  Verified end-to-end via `WRITETIME`.

- **`Cluster.Builder.withReconnectionPolicy(ReconnectionPolicy)`.** Functional: bridged onto 4.x's
  `ReconnectionPolicy` SPI; each 4.x schedule wraps a fresh 3.x `newSchedule()` and maps
  `nextDelay()` = `Duration.ofMillis(v3Schedule.nextDelayMs())`; `v3.init(cluster)` runs once. (The
  reconnection failure path itself is not observable against a single healthy node; the instance
  round-trips through `getConfiguration().getPolicies()` and the schedule wiring is in place.)

- **Per-request retry (`Statement.setRetryPolicy`) and cluster-level `withRetryPolicy`.** Functional:
  every shim session installs one 4.x `RetryPolicy` dispatcher per profile (via a `ShimDriverContext`
  used for *all* sessions). On each retry callback it resolves the effective 3.x policy — first a
  per-request policy threaded from the statement (registered by `StatementShimAccess` keyed by 4.x
  request identity in a weak map), then the cluster-level policy, then the real 4.x default (so the
  unset case matches stock behavior) — calls the matching 3.x method (mapping consistency
  level/write type by name), and maps the 3.x `RetryDecision` to a 4.x `RetryVerdict`
  (`RETRY` same/next → `RETRY_SAME`/`RETRY_NEXT`, `RETHROW` → `RETHROW`, `IGNORE` → `IGNORE`).
  `init(cluster)` runs once per policy. Caveats:
  - *Retry consistency-level change is lossy.* A 3.x decision that retries at a *different* target
    consistency level (`RetryDecision.getRetryConsistencyLevel()`) is applied as a plain
    RETRY_SAME/RETRY_NEXT; the changed CL is not carried into the 4.x retry.
  - *Failure paths are not observable on a single healthy node.* No read/write-timeout or unavailable
    error is induced against one healthy node, so the differential suite asserts the wiring
    (`init` invoked, per-request round-trip) rather than a fired retry decision.

- **`Cluster.Builder.withNettyOptions(NettyOptions)` / `withThreadingOptions(ThreadingOptions)`.**
  Functional, by bridging the 3.x options onto 4.x's **internal** Netty SPI
  (`com.datastax.oss.driver.internal.core.context.NettyOptions`), which is a near-1:1 match. When
  either option is set, the shim builds the session through a `ShimCqlSessionBuilder` whose
  `ShimDriverContext` (a subclass of the internal `DefaultDriverContext`) returns a
  `Shim3xNettyOptions`. That adapter sources the io `EventLoopGroup` from
  `v3Netty.eventLoopGroup(threadFactory)` (the thread factory coming from
  `v3Threading.createThreadFactory(clusterName, "nio-worker")` when a `ThreadingOptions` is set, else
  a daemon factory), the `channelClass` and `Timer` from the 3.x options, and forwards
  `afterBootstrapInitialized` (after applying the driver's socket options from config, as 3.x does)
  and `afterChannelInitialized`. `onClose()` drives the 3.x shutdown hooks
  (`onClusterClose(ioGroup)`, `onClusterClose(timer)`) plus the admin group on a dedicated daemon
  thread and completes a single future when all shut down, so the injected groups/timer are released
  and the JVM does not hang. Caveats:
  - *Depends on 4.x internals.* This uses `internal.core.context.NettyOptions` and subclasses
    `DefaultDriverContext`; a 4.x upgrade that changes those may require adjustment.
  - *Only the event-loop thread factory is taken from `ThreadingOptions`.* 4.x has no separate
    blocking/reconnection/reaper executors, so `createBlockingExecutor`/`createReconnectionExecutor`/
    `createScheduledTasksExecutor`/`createReaperExecutor`/`createExecutor` are not mapped (4.x uses a
    single admin event-executor group instead).

- **`Metrics` / `Metrics.Errors` and `MetricsOptions.isJMXReportingEnabled()`.** Real Dropwizard
  metrics: metrics are enabled by default (3.x parity) by turning on an equivalent set of 4.x
  session + node metrics (the default metrics factory selects Dropwizard, which is on the classpath);
  `withoutMetrics()` disables them and `getMetrics()` then returns `null`. `Metrics.getRegistry()`
  returns the live `com.codahale.metrics.MetricRegistry` the connected session uses;
  `getRequestsTimer()` is the session `cql-requests` Timer; `getBytesSent()`/`getBytesReceived()` are
  the session byte meters; the `Errors` counters and `open-connections`/`in-flight` gauges sum the
  corresponding 4.x per-node metrics (3.x reported them cluster-wide); `known-hosts`/`connected-to`
  are backed by cluster metadata. JMX reporting is on by default (off after `withoutJMXReporting()`):
  the shim attaches a `com.codahale.metrics.jmx.JmxReporter` (4.x removed its built-in JMX) to that
  registry on connect and stops it on close. Still with no 4.x equivalent (report zero):
  `getTrashedConnections`, `getRequestQueueDepth`, `getExecutorQueueDepth`,
  `getBlockingExecutorQueueDepth`, `getReconnectionSchedulerQueueSize`, `getTaskSchedulerQueueSize`,
  and the `retries/ignores-on-client-timeout` and `retries/ignores-on-connection-error` counters.

- **`Cluster.Builder.withEndPointFactory(EndPointFactory)` and `DefaultEndPointFactory.create(Row)`.**
  Functional, via the 4.x-**documented** extension point: `ShimTopologyMonitor` extends the internal
  `DefaultTopologyMonitor` and overrides `protected buildNodeEndPoint(AdminRow, InetSocketAddress,
  EndPoint)` (whose own javadoc says to extend the class and override this method for custom
  endpoints). For a peer row it wraps the 4.x `AdminRow` as a 3.x `Row`, calls the user's
  `EndPointFactory.create(row)`, and adapts the returned 3.x `EndPoint` to a 4.x endpoint; control/
  local rows and the no-factory case use the stock implementation. `factory.init(cluster)` runs once
  at startup. Verified end-to-end on a 2-node cluster: the factory's `create(...)` is invoked for the
  one peer (`createCalls=1`) and the peer is discovered, identically to the real 3.12.1 driver.
  Caveat: the `AdminRow`-backed `Row` only exposes the peer-row accessors an `EndPointFactory` reads
  (`getColumnDefinitions().contains`, `getInet`, `getInt`, `getString`, `getUUID`, `getBytes`,
  `isNull`, by column name); the index-based getter chain and other column types throw
  `UnsupportedOperationException`.

### Fail-fast (throws `UnsupportedOperationException`)

- **`ProtocolVersion.V1` / `V2`** cross-driver use — 4.x has no V1/V2. The enum constants exist; the
  adapter throws when asked to negotiate them.

### Inert (value round-trips via getters/setters, but has no runtime effect on 4.x)

- **`ProtocolOptions.isNoCompact()`** (NO_COMPACT removed).

### No-op + logs a warning once (4.x has no equivalent)

These setters accept and store the value (the getter round-trips, `getX == setX`), but the value is
inert on 4.x; each logs a one-time SLF4J WARN explaining why. Getters stay silent.

- **`PoolingOptions.setNewConnectionThreshold(HostDistance,int)` / `setIdleTimeoutSeconds(int)` /
  `setPoolTimeoutMillis(int)` / `setMaxQueueSize(int)` / `setInitializationExecutor(Executor)`** — 4.x
  pools are fixed-size with no on-demand growth, idle-trashing, borrow timeout, acquisition queue, or
  pluggable init executor. (Core/max pool size and max-requests-per-connection remain
  functional/mapped.)
- **`PoolingOptions.refreshConnectedHosts()` / `refreshConnectedHost(Host)`** — 4.x manages pools and
  node distance internally, so there is no runtime pool-refresh to trigger.

### Lossy / approximate (returns a synthetic or reduced value)

- **Exception translation lossiness:** `AlreadyExistsException.getKeyspace()/getTable()` parsed from
  the message; `FrameTooLongException.getStreamId()`; `UnsupportedProtocolVersionException`
  server/unsupported version pair (4.x exposes only attempted versions); `EndPoint` on
  timeout/connection exceptions supplied from request context.
- **`ExecutionInfo.getAchievedConsistencyLevel()`** → `null` (4.x removed downgrading retry; matches
  3.x default when no downgrade occurred); **`getTriedHosts()`** → coordinator only.
- **`Host.getTokens()`** → empty (4.x exposes ranges, not per-host tokens);
  **`getDseVersion()/getDseWorkload()/isDseGraphEnabled()`** → null/false on OSS 4.x.
- **`Session.State.getTrashedConnections/getInFlightQueries`** → 0.
- **`PagingState`** is reimplemented with the 3.x layout; a state produced by this shim is **not**
  wire-compatible with a state produced by the real 3.12.1 driver (both resume correctly within
  their own driver). Do not persist a paging state from one and resume it in the other.

(`00-MAPPING-SPEC.md` is the original per-member design catalogue produced before implementation;
many paths it initially marked UNMAPPED — retry/reconnection/timestamp policies, speculative
execution, the tracker/state/schema listeners, Netty/threading options, `QueryOptions` debouncing,
and metrics/JMX — have since been bridged into **Supported via delegation** above. The sections in
this document reflect the current, verified state.)

## Excluded internal classes

These 3.12.1 classes are `public` in bytecode but are internal utilities with no stable contract and
no 4.x counterpart. They are intentionally **not** reproduced and are excluded from the japicmp
comparison; no documented 3.x application references them:
`Native`, `GuavaCompatibility` (reproduced internally where the object mapper needs it),
`MetricsUtil`, `DefaultPreparedStatement` (the shim supplies its own `PreparedStatement`
implementation), `FramingFormatHandler`, `IgnoreJDK6Requirement`. The public utilities
`CodecUtils`, `ParseUtils`, and `utils.Bytes` **are** reproduced and compared.

## Known cosmetic differences (no behavioral impact)

- The runtime object mapper assigns internal column aliases (`col1`, `col2`, …) in its generated
  `SELECT ... AS ...` queries in a different order than the real 3.12.1 driver. The mapper is
  internally self-consistent, so `save`/`get`/`delete` results are identical; only the (internal,
  non-user-facing) generated CQL alias labels differ.

## Packaging notes

The shim is built as a single jar containing all three package trees. For a strict per-artifact
drop-in (replacing `cassandra-driver-core`, `-mapping`, and `-extras` independently), it can be
split into three jars by package, or republished under the original GAVs with a transitive
dependency on `java-driver-core`.
