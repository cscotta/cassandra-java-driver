# 00 — Consolidated 3.x → 4.x Mapping Spec

Binary-compatible shim of the DataStax Java Driver **3.12.1** public API implemented on top of
the **4.x** driver (repo HEAD). This document reconciles the 18 per-area catalogues into one
mapping spec grouped by shim package.

- Reference ABI: `abi-3121-core.txt`, `abi-3121-mapping.txt`, `abi-3121-extras.txt`.
- Target: `com.datastax.oss.driver` 4.x (`java-driver-core`, `java-driver-query-builder`,
  `java-driver-mapper-runtime`), **unshaded** Guava on the shim classpath.
- japicmp compares the shim jar against the real 3.12.1 jars. Every public/protected type,
  member, modifier, generic signature, and constant value below must match 3.12.1. The
  4.x "target" column is a **runtime** delegation target, not an ABI constraint.

## Totals

- Public/protected types cataloged: **354** across 18 areas.
- Fail-fast / lossy translation paths (UNMAPPED at behavior level): **61** (see §UNMAPPED).
- No public type is UNMAPPED at the *signature* level — every 3.12.1 public type is reproduced.
  "UNMAPPED" always means the runtime translation to 4.x has no target (throws,
  no-ops, or returns a lossy/synthetic value); the ABI surface is preserved regardless.

## Global mapping rules (apply to every area)

1. **Reproduce 3.x kind, never inherit 4.x.** Where 3.x is a class/enum/abstract-class and 4.x
   is an interface (DataType, TypeCodec, CodecRegistry, ConsistencyLevel, WriteType,
   ProtocolVersion, BatchStatement.Type, ColumnDefinitions, Token, all metadata types, all
   policy SPIs), the shim keeps the 3.x kind and holds/creates a 4.x delegate.
2. **Enums stay enums, mapped by `name()` never ordinal.** 4.x reorders constants
   (`DefaultConsistencyLevel` puts `LOCAL_ONE` at position 6 vs 3.x ordinal 10). Adapters
   translate by name.
3. **Guava is UNSHADED.** Any public signature exposing `com.google.common.*` (TypeToken,
   ListenableFuture, AbstractFuture, Predicate, Optional, ImmutableList/Map) forces the shim jar
   to link plain Guava. 4.x `GenericType` wraps *shaded* Guava — every crossing converts through
   `java.lang.reflect.Type`.
4. **Covariant/self-type returns and synthetic bridge methods are reproduced by declaring the
   identical generic hierarchy in source**, never hand-written. Adding or omitting a covariant
   override changes the emitted bridges and breaks japicmp.
5. **Protected members, protected/private constructor visibility, public constant fields and
   their inlined values, and `throws` clauses are ABI-significant** and reproduced verbatim.
6. **Package-private types are outside the ABI** (japicmp `-protected` compares public+protected
   only) and are reproduced only where they appear in a public extends/implements chain.

---

## Mapping status legend

- **FULL** — signature reproduced and runtime backed by a 4.x delegate (or a
  self-contained reimplementation with byte-identical behavior).
- **PARTIAL** — signature reproduced; runtime translation exists but is lossy, approximate, or
  reimplemented (behavioral-parity risk, not an ABI risk).
- **UNMAPPED** — signature reproduced; no 4.x runtime target — the member throws,
  no-ops, or returns a synthetic value. Listed in §UNMAPPED.

---

## Package `com.datastax.driver.core` — enums (area: enums, 5 types)

| Type | Kind | Status | 4.x target |
|---|---|---|---|
| `ConsistencyLevel` | enum | FULL | `api.core.ConsistencyLevel`/`DefaultConsistencyLevel` (by name) |
| `WriteType` | enum | FULL | `api.core.servererrors.WriteType`/`DefaultWriteType` (by name) |
| `HostDistance` | enum | FULL | `api.core.loadbalancing.NodeDistance` (by name) |
| `ClusteringOrder` | enum | FULL | `api.core.metadata.schema.ClusteringOrder` (by name) |
| `ProtocolVersion` | enum | PARTIAL | `api.core.ProtocolVersion`/`DefaultProtocolVersion` (V3–V6; V1/V2 unmapped) |

Keep 3.x method casing `isDCLocal` (4.x renamed `isDcLocal`); keep public constants
`ProtocolVersion.NEWEST_SUPPORTED=V5`, `NEWEST_BETA=V6` as shim enum constants.

## Package `com.datastax.driver.core.exceptions` (area: exceptions, 41 types)

Hierarchy reimplemented as value objects; no 4.x inheritance. `ExceptionAdapter`
performs one-way 4.x→3.x translation at the boundary.

| Type | Kind | Status | Note |
|---|---|---|---|
| `DriverException` | class (concrete) | FULL | 4.x abstract → shim concrete; concrete `copy()` |
| `DriverInternalError` | class | UNMAPPED | no 4.x equivalent; emitted by the shim |
| `CoordinatorException` | **interface** | FULL | 4.x is abstract class; shim keeps interface |
| `QueryExecutionException`/`QueryValidationException`/`QueryConsistencyException` | class | FULL | protected ctors stay protected |
| `ReadTimeoutException`,`WriteTimeoutException`,`ReadFailureException`,`WriteFailureException`,`CASWriteUnknownException`,`UnavailableException`,`OverloadedException`,`BootstrappingException`,`TruncateException`,`FunctionExecutionException`,`CDCWriteException` | class | FULL | `UnavailableException extends QueryExecutionException` |
| `InvalidQueryException`,`InvalidConfigurationInQueryException`,`SyntaxError`,`UnauthorizedException` | class | FULL | |
| `ServerError`,`ProtocolError` | class | FULL | reproduce 3.x superclass chain (extends `DriverInternalError`) + extra bridges |
| `AuthenticationException` | class | FULL | 3.x extends DriverException+CoordinatorException; NOT 4.x RuntimeException |
| `CrcMismatchException` | class | FULL | |
| `AlreadyExistsException` | class | PARTIAL | 4.x has no keyspace/table getters; parse from message |
| `NoHostAvailableException` | class | PARTIAL | unwrap 4.x `AllNodesFailedException`; `Map<Node,Throwable>`→`Map<EndPoint,Throwable>` |
| `CodecNotFoundException` | class | PARTIAL | `GenericType`→Guava `TypeToken`; message-only ctor |
| `OperationTimedOutException`,`ConnectionException`,`TransportException`,`BusyConnectionException` | class | PARTIAL | 4.x carries no EndPoint; supplied from context |
| `FrameTooLongException` | class | PARTIAL | streamId vs address disjoint |
| `UnsupportedProtocolVersionException` | class | PARTIAL | 4.x `getAttemptedVersions():List` only |
| `UnsupportedFeatureException` | class | UNMAPPED | 4.x throws IllegalArgumentException |
| `UnpreparedException` | class | UNMAPPED | 4.x re-prepares on its own |
| `BusyPoolException` | class | UNMAPPED | no 4.x pool exception |
| `InvalidTypeException` | class | UNMAPPED | 4.x throws IllegalArgumentException/ClassCastException; caught+rethrown by codec adapters |
| `PagingStateException` | class | UNMAPPED | emitted by shim PagingState |
| `TraceRetrievalException` | class | UNMAPPED | emitted by shim QueryTrace |
| `UnresolvedUserTypeException` | class | UNMAPPED | 3.x-internal UDT metadata error |

## Package `com.datastax.driver.core` — type system & codecs (area: type-system, 22 types)

Standalone class hierarchy; wrap 4.x. `equals`/`hashCode`/`toString`/CQL formatting
reimplemented from 3.x (TEXT≡VARCHAR aliasing, `frozen<…>` templates).

| Type | Kind | Status | 4.x target |
|---|---|---|---|
| `DataType` | abstract class | FULL | `api.core.type.DataType` (interface) via `DataTypeAdapter` |
| `DataType$Name` | enum (27 constants) | FULL | none (self-contained; protocol-code table internal) |
| `DataType$NativeType`,`$CollectionType`,`$CustomType` | class | FULL | `DataTypes.*` |
| `TupleType` | class | PARTIAL | `api.core.type.TupleType` (PV+registry stored, unused by 4.x) |
| `UserType`,`UserType$Field` | class | PARTIAL/FULL | `UserDefinedType`; `CqlIdentifier`↔String |
| `CodecRegistry` | final class | PARTIAL | `MutableCodecRegistry`/`DefaultCodecRegistry`; `register` self-return; `DEFAULT_INSTANCE` |
| `TypeCodec<T>` | abstract class | PARTIAL | `api.core.type.codec.TypeCodec` (interface); serialize→encode |
| `TypeCodec$Abstract{Collection,Map,Tuple,UDT}Codec` | abstract class | PARTIAL | no 4.x base — serialization bodies ported from 3.x |
| `TypeCodec$Primitive{Boolean,Byte,Double,Float,Int,Long,Short}Codec` | abstract class | PARTIAL | 4.x interfaces; `serializeNoBoxing`→`encodePrimitive` |
| `TypeTokens` | final class | FULL | none — unshaded-Guava reimplementation |

## Package `com.datastax.driver.core` — data accessors & value holders (area: data-values, 11 types)

Reproduce the package-private abstract hierarchy with the original FQNs
(`AbstractGettableByIndexData` → `AbstractAddressableByIndexData<T>` → `AbstractData<T>`, plus
public `AbstractGettableData`); back protected `getValue`/`setValue` hooks with a wrapped 4.x
value holder via `getBytesUnsafe`/`setBytesUnsafe`; copy 3.x typed-accessor bodies verbatim.

| Type | Kind | Status |
|---|---|---|
| `GettableData`,`SettableData` | interface | FULL |
| `GettableByIndexData`,`GettableByNameData`,`SettableByIndexData`,`SettableByNameData` | interface | PARTIAL |
| `AbstractGettableData` | abstract class | PARTIAL |
| `TupleValue` (extends `AbstractAddressableByIndexData<TupleValue>`), `UDTValue` (extends `AbstractData<UDTValue>`) | class | PARTIAL |
| `LocalDate`,`Duration` | class | FULL — copied wholesale as `com.datastax.driver.core.*`, NOT `java.time`/`CqlDuration` |

## Package `com.datastax.driver.core` — statements (area: statements, 10 types)

Standalone **mutable state-holders**; do NOT wrap a 4.x object. An immutable `4.x Statement<?>`
is materialized at execute time by the Session shim.

| Type | Kind | Status | Note |
|---|---|---|---|
| `Statement` | abstract class | PARTIAL | `NULL_PAYLOAD_VALUE` public const; protected volatile `idempotent` |
| `RegularStatement`,`SimpleStatement`,`BoundStatement`,`BatchStatement`,`StatementWrapper` | class | PARTIAL | BoundStatement implements `SettableData<BoundStatement>`,`GettableData` (~80 bridges) |
| `PreparedStatement` | interface | PARTIAL | |
| `PreparedId` | class | FULL | empty public class |
| `BatchStatement$Type` | enum | FULL | 4.x `BatchType` interface |
| `PagingState` | class | PARTIAL | reimplemented standalone (3.x MD5 layout; incompatible 4.x format) |

## Package `com.datastax.driver.core` — results & paging (area: results, 11 types)

| Type | Kind | Status | 4.x target |
|---|---|---|---|
| `PagingIterable<S extends PagingIterable<S,T>,T>` | interface | PARTIAL | 4.x single-param `PagingIterable` (declare own) |
| `ResultSet` | interface | FULL | `api.core.cql.ResultSet` (auto-paging) |
| `ResultSetFuture` | interface (extends Guava `ListenableFuture<ResultSet>`) | PARTIAL | `CompletionStage<AsyncResultSet>` via future adapter + pager |
| `Row` | interface (extends `GettableData`) | FULL | `api.core.cql.Row` |
| `ColumnDefinitions`,`ColumnDefinitions$Definition` | class | PARTIAL | 4.x interfaces (`equals`/`hashCode` final on Definition) |
| `ExecutionInfo` | class | PARTIAL | one public ctor `(int,int,List<Host>,ConsistencyLevel,Map)` |
| `QueryTrace`,`QueryTrace$Event` | class | FULL | |
| `Token` | **abstract class** (public no-arg ctor) | PARTIAL | 4.x `Token` empty interface + internal token types |
| `TokenRange` | final class | FULL | `api.core.metadata.token.TokenRange` |

## Package `com.datastax.driver.core` — cluster/session lifecycle (area: session-cluster, 10 types)

Merge 3.x Cluster+Session onto a single 4.x `CqlSession`.

| Type | Kind | Status |
|---|---|---|
| `Cluster` | class (implements Closeable) | PARTIAL |
| `Cluster$Builder`,`Cluster$Initializer` | class/interface | PARTIAL |
| `Session`,`Session$State` | interface | PARTIAL |
| `AbstractSession` | abstract class | PARTIAL |
| `DelegatingCluster` | class | PARTIAL — `super(dummy,null)` must not allocate |
| `CloseFuture` | abstract class (extends Guava `AbstractFuture<Void>`) | PARTIAL |
| `Configuration`,`Configuration$Builder` | class | PARTIAL |

## Package `com.datastax.driver.core` — options & metrics (area: options-config, 14 types)

Self-contained mutable state holders; Cluster area consumes them into a 4.x `OptionsMap`.

| Type | Kind | Status |
|---|---|---|
| `SocketOptions` | class | FULL |
| `QueryOptions` | class | FULL (equals/hashCode) — refresh-node knobs UNMAPPED |
| `ProtocolOptions` | class | FULL — `isNoCompact` UNMAPPED |
| `ProtocolOptions$Compression` | enum (renders as abstract-class-extends-Enum) | FULL |
| `PoolingOptions` | class | PARTIAL — core/max split, thresholds UNMAPPED |
| `ThreadingOptions`,`NettyOptions` | class | PARTIAL/UNMAPPED — reproduced inert |
| `MetricsOptions` | class | FULL — `isJMXReportingEnabled` UNMAPPED |
| `Metrics`,`Metrics$Errors` | class (Errors non-static inner) | PARTIAL — Dropwizard; several gauges UNMAPPED (zero) |
| `QueryLogger`,`$Builder`,`$ConstantThresholdQueryLogger`,`$DynamicThresholdQueryLogger` | class | FULL/PARTIAL — logic reimplemented over shim statements |

## Package `com.datastax.driver.core` — auth & SSL (area: auth-ssl, 15 types)

Reimplemented as shim code (no 4.x type in any signature). Reproduce the hierarchy so bridges regenerate.

| Type | Kind | Status |
|---|---|---|
| `AuthProvider`,`Authenticator`,`ExtendedAuthProvider` (+`$NoAuthProvider`,`$TransitionalModePlainTextAuthenticator`) | interface/class | FULL |
| `PlainTextAuthProvider` | class | FULL |
| `SSLOptions`,`RemoteEndpointAwareSSLOptions`,`ExtendedRemoteEndpointAwareSslOptions` | interface | FULL |
| `JdkSSLOptions`(+`$Builder`),`RemoteEndpointAwareJdkSSLOptions`(+`$Builder`),`SniSSLOptions`(+`$Builder`) | class | FULL |
| `NettySSLOptions`,`RemoteEndpointAwareNettySSLOptions` | class | PARTIAL — Netty `SslContext`→`SSLEngine` runtime bridge |

## Package `com.datastax.driver.core` — schema metadata (area: metadata, 17 types)

Reproduce as classes wrapping 4.x interface delegates; **eager** field translation for
`AbstractTableMetadata` protected fields; parent back-refs set at construction.

| Type | Kind | Status |
|---|---|---|
| `Metadata` | class | PARTIAL — holds session handle; token map via 4.x `Optional<TokenMap>` |
| `KeyspaceMetadata`,`AbstractTableMetadata`,`TableMetadata`,`TableOptionsMetadata`,`ColumnMetadata`,`IndexMetadata`,`MaterializedViewMetadata`,`FunctionMetadata`,`AggregateMetadata` | class/abstract | PARTIAL |
| `IndexMetadata$Kind` | enum | FULL |
| `Host`,`Host$StateListener`,`Host$LifecycleAwareStateListener` | class/interface | PARTIAL — `Host` wraps 4.x `Node` |
| `VersionNumber` | class (Comparable only, NOT Serializable) | FULL — shim-native |
| `SchemaChangeListener`,`SchemaChangeListenerBase` | interface/class | PARTIAL/FULL — Created/Dropped/Updated→Added/Removed/Changed |

## Package `com.datastax.driver.core` — timestamp/endpoint/latency (area: misc-core, 18 types)

SPIs re-declared verbatim (4.x signatures differ); math reimplemented in pure Java.

| Type | Kind | Status |
|---|---|---|
| `TimestampGenerator`,`AbstractMonotonicTimestampGenerator`,`LoggingMonotonicTimestampGenerator`,`AtomicMonotonicTimestampGenerator`,`ThreadLocalMonotonicTimestampGenerator`,`ServerSideTimestampGenerator` | interface/class | FULL/PARTIAL |
| `EndPoint`,`EndPointFactory`,`DefaultEndPointFactory`,`SniEndPoint`,`SniEndPointFactory` | interface/class | FULL (DefaultEndPointFactory.create(Row) PARTIAL/UNMAPPED) |
| `LatencyTracker` | interface | FULL |
| `PercentileTracker`(+`$Builder`),`PerHostPercentileTracker`(+`$Builder`),`ClusterWidePercentileTracker`(+`$Builder`) | class | FULL/PARTIAL — reimplemented over HdrHistogram |

## Package `com.datastax.driver.core.policies` — LB (area: policies-lb, 16 types)

Inert value/marker objects; translation happens in `Cluster.Builder.withLoadBalancingPolicy`.

| Type | Kind | Status |
|---|---|---|
| `LoadBalancingPolicy`,`ChainableLoadBalancingPolicy` | interface | PARTIAL |
| `RoundRobinPolicy`,`DCAwareRoundRobinPolicy`(+`$Builder`),`TokenAwarePolicy`,`WhiteListPolicy`,`HostFilterPolicy`,`LatencyAwarePolicy`(+`$Builder`),`ErrorAwarePolicy`(+`$Builder`) | class | PARTIAL |
| `TokenAwarePolicy$ReplicaOrdering` | enum | UNMAPPED (no 4.x knob) |
| `LatencyAwarePolicy$Snapshot`,`$Snapshot$Stats`,`ErrorAwarePolicy$ErrorFilter` | class/interface | UNMAPPED (empty/zeroed) |

## Package `com.datastax.driver.core.policies` — retry/reconnect/specex/translate (area: policies-other, 22 types)

Self-contained reimplementations; 3.x decision logic ported 1:1. A 4.x-side bridge (owned by
session-cluster) forwards callbacks.

| Type | Kind | Status |
|---|---|---|
| `RetryPolicy`(+`$RetryDecision`,`$RetryDecision$Type`),`DefaultRetryPolicy`,`DowngradingConsistencyRetryPolicy`,`FallthroughRetryPolicy`,`LoggingRetryPolicy`,`IdempotenceAwareRetryPolicy` | interface/class/enum | PARTIAL |
| `ReconnectionPolicy`(+`$ReconnectionSchedule`),`ConstantReconnectionPolicy`,`ExponentialReconnectionPolicy` | interface/class | PARTIAL |
| `SpeculativeExecutionPolicy`(+`$SpeculativeExecutionPlan`),`ConstantSpeculativeExecutionPolicy`,`NoSpeculativeExecutionPolicy` | interface/class | PARTIAL |
| `PercentileSpeculativeExecutionPolicy` | class | UNMAPPED — 4.x removed percentile subsystem; ctor refs shim `PercentileTracker` |
| `AddressTranslator`,`IdentityTranslator`,`EC2MultiRegionAddressTranslator` | interface/class | PARTIAL |
| `Policies`,`Policies$Builder` | class | PARTIAL — value holder |

## Package `com.datastax.driver.core.querybuilder` (area: querybuilder, 29 types)

**PORT 3.x sources verbatim** onto the shim's 3.x base types. Only 4.x seam: execute-time
conversion of a built statement to a 4.x SimpleStatement/BatchStatement (in session-cluster).

All 29 types FULL (PORTED). PARTIAL only where fidelity is delegated to adjacent areas:
`BuiltStatement.getValues`/`getRoutingKey` (TypeCodec.serialize, Token.serialize),
`cast`/`Select$Selection.cast` (DataType rendering), value inlining (TypeCodec.format),
`from`/`insertInto` on metadata types.

## Package `com.datastax.driver.core.schemabuilder` (area: schemabuilder, 39 types)

**REIMPLEMENT verbatim** (pure CQL string builders). All 39 types FULL. Exposed unshaded-Guava
`Optional<String> keyspaceName` protected field on `AbstractCreateStatement`. Preserve raw
generic bounds and misspelled factory `sizedTieredStategy()`.

## Package `com.datastax.driver.core.utils` (area: utils, 6 types)

| Type | Kind | Status |
|---|---|---|
| `UUIDs` | final class | FULL — delegates to `api.core.uuid.Uuids`; `PID_SYSTEM_PROPERTY` local literal `com.datastax.driver.PID` |
| `Bytes` | final class | FULL — reimplemented (avoid `internal` dependency) |
| `MoreFutures`(+`$SuccessCallback`,`$FailureCallback`) | class | FULL — pure Guava |
| `MoreObjects` | class | FULL |

## Package `com.datastax.driver.mapping` + `.mapping.annotations` (area: mapping, 41 types)

**Compile the 3.12.1 `driver-mapping` source VERBATIM** against the shim's reproduced 3.x core.
No 4.x mapper to delegate to (4.x uses a compile-time annotation processor). All types FULL at
signature level; behavioral types (`Mapper`,`MappingManager`,`Result`,`Mapper$Option`) PARTIAL
via dependence on the shimmed core. `Mapper$Option$Type` (pkg-private enum leaked via
`getType()`) reproduced with the original FQN + constant order.

## Package `com.datastax.driver.extras.codecs.*` (area: extras, 27 types)

**Compile the 3.12.1 `driver-extras` source VERBATIM** against the shim's reproduced `TypeCodec`
bases (incl. nested `PrimitiveIntCodec`/`PrimitiveLongCodec`/`AbstractTupleCodec`). All 27 types
FULL. jdk8/joda singleton codecs keep private ctors + public static `instance`. Optional deps:
Jackson, javax.json, joda-time.

---

## UNMAPPED (fail-fast / lossy) APIs — consolidated (61 entries)

Signatures exist for ABI; runtime translation throws, no-ops, or returns synthetic/lossy values.

### exceptions
- `UnpreparedException` (type) — 4.x re-prepares on its own; adapter never emits it.
- `BusyPoolException` (type) — no 4.x pool-exhaustion exception.
- `InvalidTypeException` (type) — 4.x throws IllegalArgumentException/ClassCastException; codec adapters catch & rethrow.
- `DriverInternalError` (type) — no 4.x equivalent; emitted by the shim.
- `PagingStateException` (type) — no 4.x equivalent; emitted by shim PagingState.
- `TraceRetrievalException` (type) — no 4.x equivalent; emitted by shim QueryTrace.
- `UnresolvedUserTypeException` (type) — 3.x-internal UDT cycle error.
- `UnsupportedFeatureException` (type) — 4.x throws IllegalArgumentException; uses 3.x ProtocolVersion enum.
- `AlreadyExistsException.getKeyspace()/getTable()/wasTableCreation()` (translation) — 4.x exposes no getters; parse from message or null.
- `FrameTooLongException.getStreamId()/ctor(int)` (translation) — 4.x uses (SocketAddress,String); disjoint data.
- `UnsupportedProtocolVersionException.getServerVersion()/getUnsupportedVersion()` (translation) — 4.x exposes only `getAttemptedVersions()`.
- `OperationTimedOutException/ConnectionException/TransportException/BusyConnectionException` EndPoint (translation) — 4.x carries no EndPoint; supplied from request/Node context.

### enums
- `ProtocolVersion.V1`, `ProtocolVersion.V2` (cross-driver translation) — 4.x has no V1/V2 instance; adapter throws UnsupportedOperationException/IllegalArgumentException. Shim enum retains both constants; shim-internal `toInt()/fromInt()/getLowerSupported()` still handle them.

### statements
- `Statement.setRetryPolicy/getRetryPolicy` (also on PreparedStatement) — 4.x removed per-request retry; value round-trips, inert.
- `Statement.requestSizeInBytes(ProtocolVersion,CodecRegistry)` (+ overrides) — internal 3.x encoders; return -1/best-effort.
- `PreparedStatement.getQueryKeyspace()` — 4.x exposes none; derive or null.
- `PreparedStatement.getIncomingPayload()` — return shim-stored or null.
- `BoundStatement.setPartitionKeyToken(Token)` — no clean 4.x equivalent; best-effort (return this).

### results
- `ExecutionInfo.getAchievedConsistencyLevel()` — 4.x removed downgrading retry; null (matches 3.x default).
- `ExecutionInfo.getTriedHosts()` — 4.x exposes only coordinator; singleton list (lossy).
- `PagingIterable.fetchMoreResults()` — 4.x sync auto-pages; already-completed future.
- `Token.serialize(ProtocolVersion)` — reimplemented via 4.x TypeCodec on internal token value.

### session-cluster
- `Cluster.register/unregister(LatencyTracker)` — 4.x RequestTracker is build-time only; store/no-op, return this.
- `Cluster.Builder.withEndPointFactory` — no public 4.x equivalent.
- `Cluster.Builder.withThreadingOptions` — no 4.x threading injection.
- `Cluster.Builder.withNettyOptions` — no 4.x Netty hook.
- `Session.State.getTrashedConnections(Host)` — return 0.
- `Session.State.getInFlightQueries(Host)` — return 0.
- `AbstractSession.checkNotInEventLoop()` — no shim internal handle; no-op.
- `Configuration.getThreadingOptions()/getNettyOptions()` — default shim instances.

### options-config
- `PoolingOptions.getNewConnectionThreshold/setNewConnectionThreshold` — inert.
- `PoolingOptions.getIdleTimeoutSeconds/getPoolTimeoutMillis/getMaxQueueSize/getInitializationExecutor` (+setters) — inert.
- `PoolingOptions.refreshConnectedHosts()/refreshConnectedHost(Host)` — UnsupportedOperationException/no-op.
- `QueryOptions.set/getRefreshNodeIntervalMillis`, `set/getMaxPendingRefreshNodeRequests` — inert.
- `ProtocolOptions.isNoCompact()` — bean state only (NO_COMPACT removed).
- `MetricsOptions.isJMXReportingEnabled()` — bean state only.
- `NettyOptions.*` (eventLoopGroup/channelClass/afterBootstrapInitialized/afterChannelInitialized/timer/onClusterClose×2) — inert.
- `Metrics.getKnownHosts/getTrashedConnections/getRequestQueueDepth/getExecutorQueueDepth/getBlockingExecutorQueueDepth/getReconnectionSchedulerQueueSize/getTaskSchedulerQueueSize` — synthetic zero gauge.
- `Metrics$Errors.getRetriesOnClientTimeout/getIgnoresOnClientTimeout` — zero counter.

### metadata
- `Host.getTokens()` — 4.x exposes ranges only; empty set/throw.
- `Host.getReconnectionAttemptFuture()` — completed Guava future/throw (forces unshaded Guava).
- `Host.tryReconnectOnce()` — no-op.
- `Host.getDseVersion()/getDseWorkload()/isDseGraphEnabled()` — null/false (OSS 4.x has no DSE metadata).
- `TableOptionsMetadata.getReplicateOnWrite()` — legacy; return true.
- `TableOptionsMetadata.getPopulateIOCacheOnFlush()` — legacy; return false.
- `AbstractTableMetadata.appendOptions(StringBuilder,boolean)` — reimplement shim-native.
- `SchemaChangeListener.onRegister(Cluster)/onUnregister(Cluster)` — driven by shim Cluster register path.
- `Metadata.quoteIfNecessary(String)/isReservedCqlKeyword(String)` — reimplement with 3.x reserved-word list.

### misc-core
- `DefaultEndPointFactory.create(Row)` — internal address translator; reproduce peers-row parsing or UnsupportedOperationException.
- `EndPointFactory` (init/create SPI) — no 4.x SPI; interface for ABI only.
- `PercentileTracker` family as 4.x delegate — no 4.x object; reimplemented over HdrHistogram.

### policies-lb
- `DCAwareRoundRobinPolicy$Builder.withUsedHostsPerRemoteDc(int)/allowRemoteDCsForLocalConsistencyLevel()` — 4.x removed remote-DC failover; stored, ignored (warn).
- `TokenAwarePolicy$ReplicaOrdering` — no 4.x knob; enum for ABI only.
- `LatencyAwarePolicy.getScoresSnapshot()/Snapshot.getAllStats/getStats/Stats.*` — empty/zeroed.
- `LatencyAwarePolicy$Builder.withExclusionThreshold/withScale/withRetryPeriod/withUpdateRate/withMininumMeasurements` — stored, dropped (warn).
- `ErrorAwarePolicy$Builder.withMaxErrorsPerMinute/withRetryPeriod/withErrorsFilter` + `ErrorFilter.shouldConsiderError` — dropped (warn).
- `HostFilterPolicy(Predicate<Host>)` arbitrary predicate + `fromDCWhiteList/fromDCBlackList`; `WhiteListPolicy` address whitelist — DC/address map to NodeDistanceEvaluator best-effort; arbitrary predicates UNMAPPED.

### policies-other
- `PercentileSpeculativeExecutionPolicy(PercentileTracker,double,int)` + `newPlan` — 4.x removed percentile subsystem; self-contained port or UnsupportedOperationException; ctor requires shimmed `PercentileTracker` FQN.

### mapping
- Mapper schema-change cache eviction (`SchemaChangeListenerBase` callbacks) — depends on shim Cluster firing 4.x schema events as 3.x callbacks; behavioral no-op if unavailable (signature unaffected).
