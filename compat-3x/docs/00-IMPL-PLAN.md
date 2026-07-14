# 00 — Implementation Plan

Dependency-ordered plan to build the binary-compatible 3.12.1 shim on 4.x, the Maven module
setup, and the japicmp verification approach.

---

## 1. Maven module setup

Single artifact `cassandra-driver-shim` (matching 3.12.1 coordinates so drop-in replacement
works: `com.datastax.cassandra:cassandra-driver-core:3.12.1` etc., or a shaded/relocated build
depending on deployment). One module compiles all shimmed packages together; the shim is one
compilation unit, so intra-shim forward references are free — "dependency order" below is the
**implement-and-test** order, not a set of separate compilation units.

### Bytecode / toolchain
- **JDK 8 bytecode**: `maven.compiler.release=8` (or `source`/`target=1.8` with a JDK8 toolchain).
  3.12.1 targeted Java 6/8; the ABI must load on a JDK8 consumer.
- 4.x `java-driver-core` requires JDK8+ at runtime — compatible.

### Dependencies
- `com.datastax.oss:java-driver-core` (4.x HEAD) — primary delegation target.
- `com.datastax.oss:java-driver-query-builder` (4.x) — for the execute-time SimpleStatement path
  and any 4.x builder use.
- `com.datastax.oss:java-driver-mapper-runtime` (4.x) — declared per project requirement; note the
  shim mapper does NOT delegate to it (it compiles 3.12.1 `driver-mapping` verbatim), so this is a
  transitive/compat dependency only.
- **UNSHADED Guava** (`com.google.guava:guava`) — hard, whole-jar constraint. 3.12.1 exposed
  unshaded Guava in public signatures (`ListenableFuture`, `AbstractFuture`, `TypeToken`,
  `TypeParameter`, `Predicate`, `Optional`, `ImmutableList`/`ImmutableMap`). 4.x's `GenericType`
  wraps *shaded* Guava, so the shim keeps a separate unshaded Guava and bridges through
  `java.lang.reflect.Type`. Pin a Guava major compatible with the 3.12.1 signatures.
- `org.hdrhistogram:HdrHistogram` — `PercentileTracker` family (misc-core). Pin to the coordinates
  3.12.1 used (behavioral parity).
- `io.dropwizard.metrics:metrics-core` (`com.codahale.metrics.*`) — `Metrics`/`QueryLogger`
  (options-config), exposed in public signatures.
- `io.netty:netty-*` — `NettySSLOptions`/`NettyOptions` expose `io.netty.*` in signatures; align
  the Netty artifact/version so descriptors match and runtime handlers agree with 4.x.
- `org.slf4j:slf4j-api` — `QueryLogger` logger-name constants.
- Optional (`provided`/optional scope, extras): `com.fasterxml.jackson.core:jackson-databind`,
  `javax.json:javax.json-api` (+impl), `joda-time:joda-time`.

### Guava exposure inventory (drives the unshaded-Guava decision)
`ListenableFuture`/`AbstractFuture` (results `ResultSetFuture`/`PagingIterable`, session-cluster
`CloseFuture`/`connectAsync`/`prepareAsync`, metadata `Host.getReconnectionAttemptFuture`, utils
`MoreFutures`); `TypeToken`/`TypeParameter` (type-system `TypeCodec`/`TypeTokens`/`CodecRegistry`,
data-values getters/setters, exceptions `CodecNotFoundException`, extras codecs, mapping);
`Predicate<Host>` (policies-lb `HostFilterPolicy`); `Optional<String>` (schemabuilder
`AbstractCreateStatement`); `ImmutableList`/`ImmutableMap` (statements `BoundStatement`).

---

## 2. Dependency-ordered implementation sequence

Build+test tiers. Each tier is landable and japicmp-checkable for the packages it completes.

**Tier 0 — Scaffolding.** Maven module, deps, JDK8 toolchain, japicmp harness with the reference
3.12.1 jars, `com.datastax.shim.bridge` skeleton with `FutureBridge`, `EnumBridge`. De-risks the
async model early (see §4).

**Tier 1 — Leaf types (no intra-shim deps).**
1. enums (ConsistencyLevel, WriteType, ProtocolVersion, HostDistance, ClusteringOrder) + EnumBridge.
2. utils (Bytes, UUIDs, MoreFutures, MoreObjects) — depends only on Guava.
3. `EndPoint` bare interface + `SniEndPoint` (misc-core) — needed by exceptions/auth.
4. `TypeTokens` (unshaded-Guava; standalone).

**Tier 2 — exceptions.** Whole hierarchy + `ExceptionBridge`. Depends on enums, EndPoint, Guava
TypeToken, and references `DataType` (CodecNotFoundException.getCqlType). Land the type shells;
`ExceptionBridge`'s codec paths finish after Tier 3.

**Tier 3 — type system.** `DataType` hierarchy, `TypeCodec` + `Abstract*`/`Primitive*` bases,
`CodecRegistry`, + `DataTypeBridge`/`CodecBridge`/`GenericTypeBridge`/`CodecRegistryBridge`. Ports
the serialization bodies for the abstract bases (largest single implementation effort). References
exceptions (InvalidTypeException/CodecNotFoundException) — resolves the Tier 2↔3 cycle within the
module.

**Tier 4 — data-values.** Gettable/Settable interfaces + package-private abstract chain,
`TupleValue`/`UDTValue`/`LocalDate`/`Duration`, `TimeValueBridge`/`ValueHolderBridge`. Depends on
type-system.

**Tier 5 — metadata.** `Host`(wraps Node)/`VersionNumber`/`Metadata`/keyspace/table/column/index/
view/function/aggregate + `MetadataBridge`. Depends on type-system, data-values (ClusteringOrder,
LocalDate), EndPoint.

**Tier 6 — misc-core (remainder).** Timestamp generators, `LatencyTracker`, PercentileTracker
family (HdrHistogram), EndPointFactory impls, `TrackerBridge`. Depends on metadata (Host), results
(Row — DefaultEndPointFactory.create(Row)), statements — wire the SPI shells first, fill wiring
adapters after Tier 8.

**Tier 7 — results.** `Row`/`ResultSet`/`ResultSetFuture`/`ColumnDefinitions`/`ExecutionInfo`/
`QueryTrace`/`Token`/`TokenRange` + `ResultSetBridge`. Depends on data-values (Row on GettableData),
FutureBridge, metadata (Host), type-system.

**Tier 8 — statements.** Statement family, `PagingState`, `PreparedId` + `StatementBridge`.
Depends on data-values, type-system, results (ColumnDefinitions), enums.

**Tier 9 — policies.** retry/reconnect/specex/address + `Policies`; then LB policies + config
bridge hooks. Depends on enums, statements, metadata (Host), exceptions, PercentileTracker
(policies-other `PercentileSpeculativeExecutionPolicy` ctor).

**Tier 10 — options-config.** SocketOptions…Metrics, QueryLogger. Depends on enums, statements,
metadata (Host), ColumnDefinitions, LatencyTracker, and (later) querybuilder BuiltStatement for
`QueryLogger.appendParameters`.

**Tier 11 — auth-ssl.** Native reimplementations. Depends on EndPoint, AuthenticationException.

**Tier 12 — session-cluster (integration hub).** `Cluster`/`Session`/`AbstractSession`/
`DelegatingCluster`/`CloseFuture`/`Configuration` + the materialize-at-execute path
(`StatementBridge.toV4`) + `ConfigBridge` (options + policy graph → 4.x). Consumes most
prior tiers. This is where the driver runs.

**Tier 13 — querybuilder.** Port 3.x sources verbatim. Depends on statements, type-system,
metadata, enums, policies. Execute-time conversion seam already provided by Tier 12.

**Tier 14 — schemabuilder.** Reimplement verbatim. Depends on type-system (DataType.toString),
statements (RegularStatement), metadata (isReservedCqlKeyword).

**Tier 15 — mapping.** Compile 3.12.1 `driver-mapping` verbatim against shim core. Depends on
type-system (AbstractUDTCodec), statements (subclassable BoundStatement), results, session-cluster
(AbstractSession.checkNotInEventLoop), metadata, querybuilder, enums, Guava.

**Tier 16 — extras.** Compile 3.12.1 `driver-extras` verbatim. Depends on type-system bases +
`ParseUtils`/`CodecUtils`/`Bytes`. Independent of mapping — can run in parallel with Tier 15.

---

## 3. japicmp verification approach

Run `japicmp-maven-plugin` in `verify`, comparing the **old** = real 3.12.1 jars against **new** =
the shim jar, with `onlyModified` off and `breakBuildOnBinaryIncompatibleModifications=true`.
Three comparisons (core, mapping, extras) against `abi-3121-core.txt`/`-mapping.txt`/`-extras.txt`.

Configuration:
- `accessModifier=PROTECTED` (compare public + protected; matches how the ABI dumps were taken).
- Compare generic signatures (Signature attribute), not just erased descriptors — several areas
  (exceptions Map type args, data-values self-type bounds, results PagingIterable) depend on it.
- Include synthetic bridge methods in the comparison — bridge fidelity is the highest-churn risk
  (exceptions `copy()`, BoundStatement's ~80 SettableData bridges, builder covariant returns).
- Verify public constant field **values** (japicmp compares inlined constants): IndexMetadata
  `*_OPTION_NAME`, KeyspaceMetadata.KS_NAME, LatencyAwarePolicy `DEFAULT_EXCLUSION_THRESHOLD=2.0`/
  `DEFAULT_MIN_MEASURE=50`, UUIDs.PID_SYSTEM_PROPERTY=`com.datastax.driver.PID`, ProtocolVersion
  NEWEST_SUPPORTED/NEWEST_BETA, Statement.NULL_PAYLOAD_VALUE, ProtocolOptions.Compression labels.

### Internal-but-public 3.x classes to EXCLUDE from the comparison

These are `public` in the 3.12.1 jar but internal utilities with no stable contract; the 4.x
delegate has no counterpart. Exclude via japicmp `<excludes>` (class regex) so their absence from
the shim does not raise false "class removed" diffs. Excluding them is safe because they are not
part of the documented 3.x API and no reference application links them.

| Class | Justification |
|---|---|
| `com.datastax.driver.core.Native` | JNR/native-clock helper; no public contract, no 4.x analogue. |
| `com.datastax.driver.core.GuavaCompatibility` | internal Guava-version shim; obsolete under a single pinned unshaded Guava. |
| `com.datastax.driver.core.MetricsUtil` | internal metrics naming helper. |
| `com.datastax.driver.core.DefaultPreparedStatement` | internal impl of the `PreparedStatement` interface; shim provides its own impl. |
| `com.datastax.driver.core.FramingFormatHandler` | internal protocol framing. |
| `com.datastax.driver.core.IgnoreJDK6Requirement` | CLASS-retention annotation; out of scope for binary comparison (animal-sniffer only). |
| `com.datastax.driver.core.ColumnMetadata$Raw` / `$Raw$Kind` | `Raw` is package-private; `Raw$Kind` is a public enum nested in a pkg-private class. Keep a pkg-private shell + standby `Raw$Kind` enum; exclude unless japicmp flags the class file. |

`com.datastax.driver.core.CodecUtils` and `com.datastax.driver.core.ParseUtils` are public-but-
internal utilities, BUT the extras codecs port them verbatim and depend on them, so they are
**reproduced in the shim** (ported from 3.12.1) and **retained in the comparison** — a verbatim
port matches the reference ABI, so no exclusion is needed. `com.datastax.driver.core.utils.Bytes`
is a documented public utility and is reproduced + compared.

Package-private types (e.g. `ArrayBackedResultSet`, `ArrayBackedRow`, `DefaultResultSetFuture`,
`Token$Factory`, `UserType$Shallow`, `WrappingEndPoint`, LB/query-builder helper classes) are
outside a `-protected` comparison and need no explicit exclude, except where they
appear in a public extends/implements chain (querybuilder `Utils$Appendeable`,
`BuiltStatement$ForwardingStatement`, data-values abstract parents, auth package-private
authenticators) — those ARE reproduced with identical FQNs/access so the public chains resolve.

### Parity test suite (separate from japicmp)

japicmp proves ABI; a behavioral parity suite (Tier ≥12) runs the real 3.12.1 client code against
a live Cassandra through the shim and asserts runtime behavior for the lossy paths flagged
throughout: exception translation lossiness, PagingState round-trip, query/schema-builder CQL
string output (byte-for-byte vs 3.12.1), UUIDs, codec wire formats, metadata CQL export.

---

## 4. Highest-risk areas and de-risking

**Async model (Guava `ListenableFuture`/`AbstractFuture` ↔ `CompletionStage`).** The single most
pervasive constraint: unshaded Guava must match the 3.12.1 signatures, and `CloseFuture` must
extend `AbstractFuture<Void>`. A shaded Guava breaks every future-typed signature. De-risk: build
`FutureBridge` + `CloseFuture`/`ResultSetFuture` in Tier 0 and japicmp the future-bearing
signatures before building consumers; pin Guava major up front. Also verify
`DelegatingCluster()` construction (`super(dummy,null)` then `super.closeAsync()`) returns a
completed CloseFuture without allocating a session.

**Load-balancing policies.** 3.x and 4.x LB SPIs are disjoint by design (pull-Iterator +
programmatic chaining vs push-DistanceReporter + config-driven single policy). 4.x never invokes a
shim policy method. De-risk: treat shim LB types as inert value/marker objects (signature-compat
only); do translation in `Cluster.Builder.withLoadBalancingPolicy` by unwrapping the policy graph
to a routing child and mapping DCAware.localDc → `LOAD_BALANCING_LOCAL_DATACENTER`; custom
user LBPs throw `UnsupportedOperationException` at build time (documented hard break). Parity tests
assert signature compat, not routing behavior.

**Object mapper.** No 4.x runtime mapper (4.x uses a compile-time processor). De-risk: compile the
3.12.1 `driver-mapping` source verbatim against the shim core — it touches only 3.x core, so every
public/protected signature and package-private helper reproduces, and runtime flows through
the shimmed core into `CqlSession`. Risk concentrates in three shim-core seams the mapper needs:
`TypeCodec.AbstractUDTCodec` (5 protected abstract methods), subclassable `BoundStatement`
(`MapperBoundStatement`), and `AbstractSession.checkNotInEventLoop()`. Build those first; schema-
change cache eviction degrades to a no-op if the shim Cluster cannot surface 4.x schema events.

**Query / schema builders.** 4.x builders are immutable/fluent with different types and CQL
formatting — cannot satisfy 3.x mutable-builder signatures or byte-for-byte CQL. De-risk: port
3.x sources verbatim (pure string manipulation, no live-cluster dependency); the only 4.x seam is
execute-time conversion of a built 3.x statement to a 4.x SimpleStatement/BatchStatement, already
owned by session-cluster. CQL-string parity then reduces to the fidelity of `TypeCodec.format` /
`DataType.toString` / `Metadata.quoteIfNecessary` in adjacent areas.

Secondary risks: bridge-method fidelity across exceptions/statements/builders (declare identical
generic hierarchies, never hand-write bridges); PagingState MD5 round-trip; Metrics fidelity
(4.x is per-node/opt-in — back getters with best-effort aggregation or zero instances); Netty
artifact/version alignment for the SSL-options descriptors; HdrHistogram/Dropwizard coordinate
pinning for behavioral parity.
