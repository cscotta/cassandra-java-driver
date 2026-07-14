# cassandra-driver-shim — Design & Upgrade Guide

This guide is for engineers running an application built against the **DataStax/Apache Cassandra
Java driver 3.12.1** (`com.datastax.driver.*`) who want to move onto the **4.x** driver
(`com.datastax.oss.driver.*` / `java-driver-core`) **without changing their source code**.

`cassandra-driver-shim` re-implements the entire 3.12.1 public API — core, mapping, and extras — on
top of the 4.x engine and is **binary-compatible** with 3.12.1: an application compiled against
`cassandra-driver-{core,mapping,extras}:3.12.1` links and runs unchanged after you swap this artifact
in. It is a **drop-in dependency swap, not a rewrite.**

> **How to read this guide.** [At a glance](#at-a-glance) is the 60-second summary. [Design](#1-design--architecture)
> explains *how* the shim works. [Carried forward verbatim](#2-what-was-carried-forward-from-3121-verbatim)
> and [Modified](#4-what-was-carried-forward-but-modified-the-hybrids) tell you which parts of your
> 3.x behavior are literally the original code vs. re-pointed at 4.x. [How APIs are mapped](#3-how-each-api-is-mapped-onto-4x)
> is the reference for each subsystem. **If you are short on time, read [At a glance](#at-a-glance),
> [Caveats & upgrade considerations](#5-caveats--upgrade-considerations), and the
> [checklist](#6-before-you-upgrade-checklist).**

---

## At a glance

| | |
|---|---|
| **What you get** | The 3.12.1 public API (`com.datastax.driver.*`), backed at runtime by the 4.x driver. |
| **Binary compatibility** | `japicmp` reports **0 / 0 / 0** incompatible class/method/field changes vs the real 3.12.1 jars, across core, mapping, and extras. Code compiled against 3.12.1 links unchanged. |
| **Behavioral parity** | Verified by a differential suite (same program run under real-3.12.1 and the shim against the same live Cassandra — **149/149 identical**) and by running the 3.12.1 driver's own unit suite against the shim (**64 classes / 695 methods pass**). |
| **How much is original 3.x code** | **239 of 332 source files (~72%)** are 3.12.1 source carried forward verbatim; the other 93 hold net-new shim code — including 59 "hybrid" files that keep much of their original 3.x body under facade edits, so by lines even more of the shim traces back to 3.12.1. |
| **Upgrade effort** | Swap the dependency. No source changes. A handful of genuinely-removed 4.x features fail-fast, no-op, or return reduced values — all listed in §5, none silent. |
| **Requirements** | JDK 8+; **unshaded Guava 19** on the classpath (3.x exposed Guava in its ABI); the 4.x `java-driver-core` comes in transitively. |
| **The one hard incompatibility** | `ProtocolVersion.V1` / `V2` cannot be negotiated (4.x has no v1/v2 protocol). Target v3+. |

---

## 1. Design & architecture

### 1.1 One hard invariant: the ABI must match 3.12.1 exactly

The shim is built around a single non-negotiable rule: **`japicmp` must report zero
binary-incompatible changes** — 0 class, 0 method, 0 field diffs — when the shim jar is compared
against the real 3.12.1 `core`, `mapping`, and `extras` jars (public **and** protected members,
generic signatures, inlined constant values, and synthetic bridge methods all included).

The design decouples two concerns that 3.x fused together:

- **ABI fidelity** — every public/protected type, member, modifier, generic signature, and constant
  value is reproduced *exactly* as 3.12.1 emitted it. This is what makes the linkage work.
- **Runtime behavior** — what a member *does* is re-pointed at the 4.x engine. The 4.x type a member
  delegates to is a runtime target, **not** an ABI constraint.

Because these are separated, every 3.12.1 type is reproduced in one of two modes:

| Mode | Applies to | How |
|---|---|---|
| **Facade over a 4.x delegate** | Runtime-connected types that need a live cluster: `Cluster`, `Session`, `ResultSet`, `Row`, `Statement`, `Metadata`, `Host`, `DataType`, exceptions, policies | The 3.x type is reproduced in source, holds/creates a 4.x delegate internally, and translates at the boundary through the bridge layer |
| **Verbatim copy** | Self-contained value types and pure-logic subsystems: the exception hierarchy, the type/codec system, `LocalDate`/`Duration`/`VersionNumber`, the QueryBuilder & SchemaBuilder DSLs, the object mapper, the extras codecs | The 3.12.1 source is carried forward byte-for-byte, so no 4.x type appears in any signature |

A useful consequence: **no public type is ever "unmapped" at the signature level** — every 3.12.1
public type exists. When this guide says an API is "fail-fast", "no-op", or "lossy", it means the
*runtime translation* to 4.x has no faithful target; the ABI surface is always present.

### 1.2 The object-model bridge: 3.x `Cluster` + `Session` → one 4.x `CqlSession`

The most consequential design decision is the lifecycle merge. In 3.x, `Cluster` (connection/config
lifecycle) and `Session` (per-keyspace query execution) are separate objects; in 4.x there is only
`CqlSession`. The shim maps both onto one `CqlSession`:

- The shim **`Cluster`** is a facade that captures all `Builder` state (contact points, option beans,
  the whole policy graph) and, at `connect()`/`init()`, builds a **4.x `CqlSession`**. Contact
  points, local datacenter, cloud secure-connect bundle, auth, and SSL are applied to the builder;
  the option beans are translated into a 4.x programmatic `DriverConfigLoader` (see §3).
- Shim **statements are standalone mutable state-holders — they do not wrap a 4.x statement.** The
  immutable 4.x `Statement<?>` is materialized *once, at execute time*, by reading the accumulated
  3.x fields. (A QueryBuilder `BuiltStatement` — which *is* a 3.x `RegularStatement` — is converted
  through its query string + values.)
- **Results flow back the other way:** a 4.x `ResultSet`/`AsyncResultSet` is wrapped as a 3.x
  auto-paging `ResultSet`, and each 4.x `Row` is presented as a 3.x `Row`.
- **Async model:** 3.x exposes Guava `ListenableFuture`/`AbstractFuture`; 4.x uses `CompletionStage`.
  A future bridge converts both directions and reconstructs `CloseFuture` and `ResultSetFuture`.

The 3.x value-access chain (`AbstractGettableByIndexData → AbstractAddressableByIndexData<T> →
AbstractData<T>`, plus `AbstractGettableData`) is reproduced with **exact fully-qualified names**, and
its `getValue(int)`/`setValue(int,ByteBuffer)` hooks are backed by a wrapped 4.x value holder. `Row`,
`BoundStatement`, `TupleValue`, and `UDTValue` all sit on this one chain, so codec dispatch and
exception semantics are identical to 3.x.

### 1.3 The internal bridge package, and the deliberate use of 4.x internals

All 3.x↔4.x conversion lives in one package, **`com.datastax.shim.bridge`**, which is invisible to
the ABI (japicmp compares only `com.datastax.driver.*`). It is the single home for every conversion,
so no two subsystems can translate the same type differently. Two correctness-critical invariants
live here: codec adaptation must be **idempotent** (a 3x-over-4x-over-3x codec chain would otherwise
recurse forever) and `Node`↔`Host` mapping must be **identity-cached** (unstable identity would break
3.x `equals()`/`hashCode()` semantics).

To re-expose 3.x extension points that 4.x deleted from its *public* API, the bridge deliberately
reaches into **4.x `internal` SPIs**:

- **`ShimDriverContext extends DefaultDriverContext`** overrides the context's `build*` factory
  methods (`buildNettyOptions`, `buildTimestampGenerator`, `buildReconnectionPolicy`,
  `buildRetryPolicies`, `buildTopologyMonitor`) to splice user 3.x beans into the engine. **Every
  override returns `super`'s result when the corresponding 3.x bean is unset, so an unconfigured
  cluster behaves exactly like stock 4.x.** It is installed for *all* shim sessions (via
  `ShimCqlSessionBuilder`), which is what lets per-request retry work universally.
- **The internal `NettyOptions` SPI** lets a user's 3.x `NettyOptions`/`ThreadingOptions` (event-loop
  group, channel class, timer, bootstrap/channel hooks) map onto 4.x's near-1:1 internal Netty
  contract.
- **`DefaultTopologyMonitor`** is subclassed to honor a 3.x `EndPointFactory` through the
  4.x-*documented* `buildNodeEndPoint(...)` extension point.

**The trade-off, stated plainly:** these internal types are not part of 4.x's stable public contract,
so the shim is coupled to 4.x internals and *could* need adjustment across a 4.x minor upgrade where
a purely-public shim would not. The project accepts this deliberately — it is the only way to surface
3.x hooks (custom Netty/threading, timestamp generator, reconnection policy, per-request retry,
endpoint factory) that 4.x removed from public view. Where not even an internal seam exists, the
member is left inert/lossy rather than faked.

### 1.4 The config-token instance-handoff pattern

4.x instantiates several policies **reflectively from config** — `basic.load-balancing-policy.class`
and `advanced.speculative-execution-policy.class` name a class that 4.x constructs itself. There is
**no API to pass a policy *instance*.** But a 3.x user calls `withLoadBalancingPolicy(myPolicy)` with
a live object. The shim bridges the gap with a static registry keyed by a per-cluster token:

1. **Register** — the shim `Cluster` mints a fresh token (`"shim-lbp-" + UUID`) and registers the
   user's policy instance (plus the shim `Cluster` its `init` needs).
2. **Inject** — the config points the 4.x `*.class` option at the shim adapter and writes the token
   into a custom config option.
3. **Recover** — 4.x reflectively constructs the adapter, which reads the token back and looks up the
   instance, then implements the 4.x SPI by delegating to the user's 3.x policy.
4. **Deregister** — the registration is dropped on `Cluster.close()` to avoid leaks.

This is the general escape hatch for injecting a live 3.x instance into any 4.x subsystem that only
accepts a reflectively-constructed class name.

### 1.5 Source layout and provenance

Because most of the module (239 of 332 files) is original 3.x code, provenance is made
**structurally obvious**: the two
kinds of source live in separate directories (package names are unchanged — the ABI requires
`com.datastax.driver.*` — so this is a *directory* split, not a package rename).

| Location | Contents |
|---|---|
| `src/main/java-driver-3x/` | The **239 carried-forward** files: pristine 3.12.1 source, identical modulo comments, **no shim code**, **no** provenance header. |
| `src/main/java/` | The **93 files containing net-new shim code**: the hybrids, the net-new glue classes, and the `com.datastax.shim.*` bridge. **Every file carries a `// Shim provenance:` header.** |

Three tools (under `src/test/scripts/`, documented in [`PROVENANCE.md`](PROVENANCE.md)) maintain and
expose this: `provenance-manifest.sh` (classifies every file), `provenance-check.sh` (CI guard for
the invariant), and `provenance-diff.sh` (shows the exact line-level delta the shim adds on top of
3.12.1, *including inside hybrid files*, by diffing the `shim-3x-vendor-3.12.1` baseline tag).

### 1.6 Packaging & build constraints

- **Unshaded Guava 19 is a hard, whole-jar constraint.** 3.12.1 exposed *unshaded* Guava types
  directly in its public ABI (`ListenableFuture`, `AbstractFuture`, `TypeToken`, `Predicate<Host>`,
  `Optional<String>`, `ImmutableList`/`ImmutableMap`). Reproducing those signatures forces the shim
  to link plain Guava. 4.x's own Guava is **shaded**, so the two coexist; every crossing round-trips
  through `java.lang.reflect.Type`.
- **JDK 8 bytecode** (3.12.1 targeted Java 6/8; 4.x also runs on JDK 8+).
- **Single jar** (`cassandra-driver-shim`) containing all three package trees, kept **out of the root
  reactor** so the verbatim-ported sources are not subjected to the 4.x checkstyle/format/license
  plugins.

---

## 2. What was carried forward from 3.12.1 verbatim

**239 files (~38.7k lines)** are the *original* 3.12.1 source, compiled unchanged against the 4.x
classpath and moved into `src/main/java-driver-3x/`:

| Category | Files | Meaning |
|---|---:|---|
| **Verbatim** (byte-identical to 3.12.1) | 217 | Every signature, constant, `serialVersionUID`, and method body is exactly the original. |
| **Near-verbatim** (identical *modulo comments*) | 22 | Differ from 3.12.1 only in comments/Javadoc — **no code change** (no signature, statement, or constant altered). |

**Why carry them byte-for-byte rather than re-point them at 4.x?** These classes are self-contained
value types and pure logic — they compute over Java types, `ByteBuffer`s, and each other, and never
touch the transport/session internals that had to be rewritten. Carrying them byte-identical is
exactly what keeps `japicmp` at 0/0/0 on them. They therefore carry **no `// Shim provenance:`
header**; the guard `provenance-check.sh` enforces that a file in `java-driver-3x/` has no header and
is identical to 3.12.1 modulo comments.

### Inventory by area (authoritative counts from the manifest)

| Area | Verbatim | Near-verbatim | Notes |
|---|---:|---:|---|
| **Exceptions** — `core/exceptions/*` | 41 | 0 | The entire hierarchy: `DriverException`, `QueryExecutionException`/`QueryValidationException`, `InvalidQueryException`, `NoHostAvailableException`, `Read/Write/Timeout/Failure` exceptions, `UnavailableException`, `AlreadyExistsException`, `CASWriteUnknownException`, `CDCWriteException`, etc. (Only `BusyPoolException` and `OperationTimedOutException` are lightly modified — see §4.) |
| **QueryBuilder DSL** — `core/querybuilder/*` | 15 | 0 | **Whole package verbatim**: `QueryBuilder`, `BuiltStatement`, `Select`, `Insert`, `Update`, `Delete`, `Batch`, `Truncate`, `Clause`, `Ordering`, `Using`, `BindMarker`, … |
| **SchemaBuilder DSL** — `core/schemabuilder/*` | 18 | 0 | **Whole package verbatim**: `SchemaBuilder`, `Create`/`CreateIndex`/`CreateKeyspace`/`CreateType`, `Alter`, `Drop`, `TableOptions`, `ColumnType`, … |
| **Object mapper** — `mapping/*` | 31 | 0 | **Whole subtree verbatim**: `Mapper`, `MappingManager`, `EntityMapper`, `AnnotationParser`, `PropertyMapper`, `NamingStrategy`/`NamingConventions`, `Result`, `MappedUDTCodec`, … |
| **Mapper annotations** — `mapping/annotations/*` | 16 | 0 | **Whole package verbatim**: `@Table`, `@Column`, `@PartitionKey`, `@ClusteringColumn`, `@Computed`, `@Transient`, `@Frozen`, `@UDT`, `@Accessor`, `@Query`, `@Param`, … |
| **Extras codecs** — `extras/codecs/**` | 35 | 0 | **Whole subtree verbatim**: `MappingCodec`/`ParsingCodec` plus `arrays/`, `date/`, `enums/`, `guava/`, `jdk8/`, `joda/`, `json/`. |
| **Core value/type system** — `core/*` | 57 | 6 | The type/codec machinery (`TypeCodec`, `CodecRegistry`, `TypeTokens`, `CodecUtils`, `ParseUtils`), values (`Duration`, `LocalDate`, `TupleType`/`TupleValue`, `UDTValue`), the `Gettable`/`Settable` data-access chain, enums & beans (`ConsistencyLevel`, `HostDistance`, `WriteType`, `ProtocolVersion`, `VersionNumber`, `PagingState`, `Token`/`TokenRange`), auth/SSL, timestamp generators, percentile trackers. *(Near-verbatim here: `ColumnDefinitions`, `SimpleStatement`, `SocketOptions`, `MetricsOptions`, `PercentileTracker`, `SystemProperties` — comment-only edits.)* |
| **Policies** — `core/policies/*` | 2 | 16 | `ChainableLoadBalancingPolicy`, `NoSpeculativeExecutionPolicy` are byte-verbatim; the policy interfaces and simple implementations (`LoadBalancingPolicy`, `ReconnectionPolicy` + constant/exponential, the concrete retry policies `DefaultRetryPolicy`/`DowngradingConsistencyRetryPolicy`/`FallthroughRetryPolicy`/`IdempotenceAwareRetryPolicy`, `SpeculativeExecutionPolicy` + constant, `RoundRobinPolicy`, `TokenAwarePolicy`, `WhiteListPolicy`, `HostFilterPolicy`, `AddressTranslator`/`IdentityTranslator`) are near-verbatim (comment-only). *(The policies that actively bridge to 4.x — including the `RetryPolicy` interface and `LoggingRetryPolicy` — are hybrids; see §4.)* |
| **Utils** — `core/utils/*` | 2 | 0 | `Bytes`, `MoreObjects` verbatim. (`UUIDs`, `MoreFutures` are hybrids.) |

**Bottom line for readers:** anything under `src/main/java-driver-3x/` is stock 3.12.1 behavior. If
you relied on a 3.12.1 exception type, the QueryBuilder/SchemaBuilder DSLs, the object mapper and its
annotations, the extras codecs, or the core value/enum/type classes, the shim gives you the *literal
original class*.

---

## 3. How each API is mapped onto 4.x

Two mechanisms recur (described in §1.3–§1.4): the universal **`ShimDriverContext`** (splices 3.x
beans into the engine, falling back to stock 4.x when unset) and the **config-token hand-off** (for
policies 4.x builds reflectively). The subsystems below reference them.

### Cluster / Session lifecycle & builder

| 3.x surface | 4.x target | Mechanism / notes |
|---|---|---|
| `Cluster.builder()` / `buildFrom(Initializer)` | `CqlSession.builder()` via `ShimCqlSessionBuilder` | Builder accumulates 3.x option beans; the `CqlSessionBuilder` is assembled lazily at connect. |
| `connect()` / `connect(ks)` / `newSession()` | `CqlSessionBuilder.build()` (+ `withKeyspace`), wrapped in a `ShimSession` | Each built session is tracked for later close. |
| `connectAsync(...)` | `buildAsync()` → `CompletionStage<CqlSession>` | Adapted to a Guava `ListenableFuture`; failures translated to 3.x exceptions. |
| contact points / `withPort` | `addContactPoints(...)` | Cloud secure-connect bundle handled first. |
| local DC | `withLocalDatacenter(...)` | Also where a `DCAwareRoundRobinPolicy`'s local DC is translated. |
| `withCredentials` / `withAuthProvider` | `withAuthCredentials` / 4.x `AuthProvider` | 3.x `AuthProvider`/`Authenticator` adapted (incl. `ExtendedAuthProvider`). |
| `withSSL()` / `withSSL(SSLOptions)` | `withSslEngineFactory(...)` | Drives the 3.x `SSLOptions.newSSLHandler(channel)` and extracts the `SSLEngine`; handles the remote-endpoint-aware variants. |
| `close()` / `closeAsync()` | `CqlSession.closeAsync()` per session | Closes all sessions, stops the JMX reporter, drops policy registrations; returns a `CloseFuture` whose `force()` forwards `forceCloseAsync()`. |

**Option beans → 4.x config.** Only values you actually set are written, so the unconfigured path
leaves 4.x defaults intact. Highlights: `SocketOptions` connect/read timeouts, keep-alive, TCP-no-delay,
buffers → the matching `SOCKET_*`/`CONNECTION_*`/`REQUEST_TIMEOUT` options; `QueryOptions` consistency,
serial CL, fetch size, default idempotence, prepare-on-all/reprepare, metadata-enabled, and the
node/schema refresh debouncers; `PoolingOptions` core/max connections, max requests, heartbeat;
`ProtocolOptions` compression (`snappy`/`lz4`) and protocol version.

### Statement execution

Every request funnels through the async path (`executeAsync`/`prepareAsync`); synchronous `execute()`
is built on it.

| 3.x surface | 4.x target | Notes |
|---|---|---|
| `SimpleStatement` / `RegularStatement` / QueryBuilder `BuiltStatement` | 4.x `SimpleStatement` | Query string used as-is; **bound values are serialized on the 3.x side** with the shim codec registry, then handed to 4.x as positional/named values (see *Type system* for why). |
| `BoundStatement` | 4.x `BoundStatement` | Materialized from the wrapped 4.x `PreparedStatement`; `UNSET` sentinels skipped, explicit `null` preserved. |
| `BatchStatement` | 4.x `BatchStatement` | Type `LOGGED`/`UNLOGGED`/`COUNTER` mapped; children converted individually. |
| per-statement options | copied at execute time | Consistency & serial CL, tracing, page size, query timestamp, per-statement read timeout (**`0` disables**), idempotence, paging state, custom payload, routing keyspace, and (non-batch) the **routing key** computed with the 3.x registry. A per-request `RetryPolicy` is registered here. |
| `ResultSet` (sync/auto-paging) | 4.x `ResultSet`/`AsyncResultSet` | Transparent paging reproduced by blocking on `fetchNextPage()` when a page is exhausted; `isExhausted()` peeks across trailing empty pages to match 3.x. |
| `Row` / `ColumnDefinitions` | 4.x `Row` / `ColumnDefinitions` | Raw `getBytesUnsafe(i)` bytes copied into a shim `Row`, decoded lazily by 3.x codecs; 4.x `DataType` → 3.x `DataType`. |
| `ExecutionInfo` (+ `getStatement()`) | 4.x `ExecutionInfo` | The **originating 3.x statement** is threaded back in — the object mapper depends on `getExecutionInfo().getStatement()`. |
| `ResultSetFuture` / `ListenableFuture` | 4.x `CompletionStage` | Bridged both directions; `getUninterruptibly()` re-throws a fresh `copy()` of the 3.x exception so the stack points at the caller. |

### PreparedStatement / prepare

`prepare(query | SimpleStatement)` → `CqlSession.prepareAsync(...)` (with custom payload if present).
The resolved 4.x `PreparedStatement` is wrapped, capturing the 3.x `ColumnDefinitions`, negotiated
`ProtocolVersion`, partition-key indices (`PreparedId`), codec registry, and keyspace. `bind(...)`
produces a shim `BoundStatement` re-materialized against the 4.x prepared statement at execution.

### Type system & codecs (runs unchanged — not delegated to 4.x)

The 3.12.1 type/codec system is carried **verbatim**: `CodecRegistry`, `TypeCodec`, the value-access
chain, `Token`, `TupleType`, `UDTValue`, and the entire extras codec family. Consequences:

- **Custom `TypeCodec`s and extras codecs work as in 3.x.** Register on `CodecRegistry.DEFAULT_INSTANCE`
  or supply your own via `withCodecRegistry(...)`.
- **Writes:** values are encoded to `ByteBuffer` *on the 3.x side* (including 3.x-only Java types like
  `java.util.Date` and the shim `LocalDate`/`Duration`/`UDTValue`/`TupleValue`), then passed to 4.x,
  whose blob codec passes the bytes through unchanged — so the wire bytes match 3.x exactly.
- **Reads:** raw bytes are decoded lazily with the 3.x registry over the reused getter chain.
- **4.x `DataType` → 3.x `DataType`** conversion recurses through collections/tuples; `UserDefinedType`
  → 3.x `UserType`; custom/other container types → `DataType.custom(...)`.

### Schema metadata & token/routing

`Metadata` is a facade over 4.x `Metadata` + `TokenMap`; `KeyspaceMetadata`/`TableMetadata`/
`ColumnMetadata`/etc. wrap the corresponding 4.x types with a **stable `Node`↔`Host` identity cache**.
Static identifier helpers (`quote`, `quoteIfNecessary`, `isReservedCqlKeyword`, `handleId`) are
self-contained 3.12.1 reimplementations. Token objects are the shim's own `Token` types rebuilt from
the 4.x partitioner; `getTokenRanges`, `getReplicas`, `newToken(...)`, `getPartitioner`,
`checkSchemaAgreement`, `exportSchemaAsString` all map onto `TokenMap`/`Session`/`KeyspaceMetadata`.

### Exceptions

`ExceptionBridge.toV3(...)` is a **one-way** translation applied at every boundary
(execute/prepare/connect/close); no shim exception ever wraps a 4.x exception. It unwraps
`CompletionException`/`ExecutionException`, passes an already-3.x exception through, and maps:
`AllNodesFailedException`→`NoHostAvailableException` (per-node error map rebuilt);
`DriverTimeoutException`→`OperationTimedOutException`; the server read/write timeout & failure
exceptions to their 3.x namesakes (CL/write-type by name, counts preserved);
`UnavailableException`/`TruncateException`/`OverloadedException`/`ServerError`/`SyntaxError`/… to the
same 3.x names; and a `DriverInternalError` fallback. A few translations are **lossy** — see §5.4.

### Load balancing (arbitrary custom 3.x policies)

`withLoadBalancingPolicy(...)` honors any user 3.x policy (including `DCAwareRoundRobinPolicy`,
`TokenAwarePolicy`, `RoundRobinPolicy`) through the 4.x LB SPI via the config-token hand-off. The
adapter maps `Node`→`Host`, forwards the full lifecycle (`init`/`distance`/`newQueryPlan`/
`onAdd`/`onUp`/`onDown`/`onRemove`/`close`), and **preserves per-request token-aware routing**: it
copies the request's routing key, routing keyspace, and consistency level into the 3.x statement, so a
token-aware policy routes on the same key the 4.x engine computed. Caveats:

- *Distance is push (4.x) vs pull (3.x)* — a policy that varies distance purely over time with no
  topology event won't have the change observed until the next event.
- *`Cluster` introspection during `init`* — `getConfiguration()` works, but driving I/O
  (`connect()`/`getMetadata()`) from `init` is unsupported (it would re-enter session construction).

### Speculative execution (incl. `PercentileSpeculativeExecutionPolicy`)

Any non-`Constant`/non-`No` 3.x policy is honored through the 4.x specex SPI. The adapter **does not
reimplement the percentile math** — it drives the 3.x code: it keeps one 3.x `SpeculativeExecutionPlan`
per in-flight request and forwards `plan.nextExecution(host)` (the delay in ms, or negative to stop,
preserving `maxSpeculativeExecutions`). `v3.init(cluster)` runs once so a
`PercentileSpeculativeExecutionPolicy` can register its tracker; latencies reach that tracker because
the shim installs a build-time 4.x `RequestTracker` that fans per-node latencies to the registered 3.x
`LatencyTracker`s. Caveats: percentile warm-up (no speculation until a full interval with
≥ `minRecordedValues` samples); single-node speculation isn't observable; the `LatencyTracker.update`
feed statement carries routing info, not the original CQL.

### Retry / Reconnection / TimestampGenerator / Netty / Threading (via `ShimDriverContext`)

- **RetryPolicy** — a dispatcher is installed on *every* profile, wrapping the real 4.x default. It
  resolves the effective policy in order: per-request (set on the statement) → cluster-level → real 4.x
  default. It maps the 3.x `RetryDecision` to a 4.x `RetryVerdict`. *Lossy:* a decision that retries at
  a **different consistency level** is applied as a plain RETRY (the changed CL is dropped).
- **ReconnectionPolicy** — each 4.x schedule wraps a fresh 3.x `newSchedule()`;
  `nextDelay()` = `Duration.ofMillis(nextDelayMs())`.
- **TimestampGenerator** — direct pass-through (both are `long next()` microseconds, `Long.MIN_VALUE`
  = server-side). Verified via `WRITETIME`.
- **NettyOptions / ThreadingOptions** — bridged onto 4.x's internal Netty SPI (event-loop group,
  channel class, timer, bootstrap/channel hooks). *Only the event-loop thread factory* is taken from
  `ThreadingOptions` (4.x has no separate blocking/reconnection/reaper executors). Shutdown hooks run
  on a dedicated daemon thread so the JVM does not hang.

### Listeners

`register/unregister(LatencyTracker | Host.StateListener | SchemaChangeListener)` are all functional,
each backed by a build-time 4.x adapter that fans events out to the registered 3.x listeners
(translating `Node`→`Host`, and 4.x Created/Dropped/Updated → 3.x Added/Removed/Changed for
keyspaces/tables/UDTs/functions/aggregates/views). Trackers registered *after* `connect()` are still
fed; `withInitialListeners(...)` registers at build time. Verified end-to-end (a `CREATE TABLE` fires
`onTableAdded`).

### Metrics (real Dropwizard + JMX)

`getMetrics()` returns a `Metrics`/`Metrics.Errors` backed by the connected 4.x session's **live
Dropwizard `MetricRegistry`**. Metrics are **on by default** (3.x parity); `withoutMetrics()` disables
them (then `getMetrics()` returns `null`). `getRegistry()`, the `cql-requests` timer, and bytes
sent/received are functional; error counters and open-connections/in-flight **sum** the 4.x per-node
metrics (3.x reported them cluster-wide). **JMX is on by default** (a `JmxReporter` is attached on
connect, stopped on close; `withoutJMXReporting()` disables it). Some gauges have no 4.x equivalent
and report zero — see §5.4.

### `withEndPointFactory`

A user 3.x `EndPointFactory` is honored via the 4.x-documented `DefaultTopologyMonitor.buildNodeEndPoint`
override: for a peer row the raw 4.x `AdminRow` is wrapped as a 3.x `Row`, handed to `factory.create(row)`,
and the returned 3.x `EndPoint` adapted back. `factory.init(cluster)` runs once. Verified on a 2-node
cluster. Caveat: the `AdminRow`-backed `Row` exposes only the peer-row accessors an `EndPointFactory`
reads (by column name); other accessors throw.

---

## 4. What was carried forward but modified (the "hybrids")

**59 files** were carried forward from 3.12.1 but **modified in place**: the 3.12.1 file was copied to
the shim's source path, its **public shape preserved** (class/interface declarations, method
signatures, field visibility, constants — so the ABI still matches), and its **method bodies rewritten
to delegate to 4.x**. All 59 live in `src/main/java/` and carry a `// Shim provenance:` header.

### The editing pattern

- **Signatures/ABI preserved** — public/protected members keep their 3.12.1 names, parameters, return
  types, and modifiers. Constructors that were public in 3.x stay public even where the shim no longer
  needs them (kept as ABI sentinels).
- **Bodies re-pointed at 4.x** — a facade typically gains a package-private `delegate` field of the
  matching 4.x type and a wrapping constructor.
- **Large deletions of 3.x internal machinery** — whole inner classes, background threads, connection-
  pool bookkeeping, and wire encoders are removed because 4.x owns that logic now. Across the 59
  hybrids the delta is **≈ +2,650 / −12,600** lines — deletions dominate. (The whole shim-code tree,
  `src/main/java`, is ≈ +7,400 / −12,600; the entire `src/main` change, including the comment-only
  edits to carried-forward files, is +7,460 / −13,432.)
- **Small additions of delegation glue** — 4.x/bridge imports, the delegate field, and occasional
  shim-only accessors.

You can see the exact added/changed lines for any hybrid with
`src/test/scripts/provenance-diff.sh <path>` (diff against the `shim-3x-vendor-3.12.1` baseline).

### The most significant hybrids

Magnitudes are `+added / −deleted` vs the pristine 3.12.1 baseline.

| File (`core/…`) | Δ | What changed / how it behaves now |
|---|---|---|
| `Cluster.java` | **+733 / −2964** | The biggest rewrite. No longer owns an internal control-connection engine; it captures all `Builder` state and builds a **4.x `CqlSession` on each `connect()`/`init()`**, translating options through the config bridge. `getConfiguration()` reconstructs populated 3.x config for faithful introspection. |
| `Metadata.java` | +190 / −819 | Facade over 4.x `Metadata` + `TokenMap`; static identifier helpers reimplemented verbatim from 3.12.1. |
| `Metrics.java` | +206 / −482 | Re-backed by the connected 4.x session's Dropwizard registry (semantics shift cluster-wide → summed-per-node; some gauges report zero). |
| `Host.java` | +84 / −457 | Facade over 4.x `Node` (one cached `Host` per `Node`). Several 3.x-only methods are unmapped (see below). |
| `TableMetadata.java` | +81 / −560 | Facade over 4.x `TableMetadata`. |
| `KeyspaceMetadata.java` | +115 / −361 | Facade over 4.x `KeyspaceMetadata`. |
| `TableOptionsMetadata.java` | +100 / −375 | 4.x collapses table options into a map; legacy Cassandra-1.x options return 3.x-style defaults. |
| `ExecutionInfo.java` | +110 / −205 | Dual-path: a public 3.x value constructor + a package-private facade over 4.x `ExecutionInfo`. |
| `PoolingOptions.java` | +83 / −339 | Value bean; several knobs are inert no-ops that warn once (§5.2). |
| `Configuration.java` | +41 / −206 | Reconstructed aggregate of the 3.x option sub-objects, assembled from builder state. |
| `policies/LatencyAwarePolicy.java` | **+40 / −572** | Gutted to an **inert** `ChainableLoadBalancingPolicy` wrapper — tracker/updater/executor deleted; routing just delegates to the child (latency-awareness is folded into the 4.x default policy). |
| `policies/DCAwareRoundRobinPolicy.java` | +22 / −71 | Routing largely retained, plus a shim-only `getLocalDc()` used to translate into the 4.x `local-datacenter` option. |
| `policies/Policies.java` | +48 / −63 | Default holder; `getX()` return the same 3.x defaults. |

*(`ColumnMetadata`, `AggregateMetadata`, `FunctionMetadata`, `IndexMetadata`,
`MaterializedViewMetadata`, `QueryTrace`, `AbstractSession`, `QueryOptions`, `ProtocolOptions`,
`SocketOptions`, `QueryLogger`, `NettyOptions`, `ThreadingOptions`, `ErrorAwarePolicy`, `RetryPolicy`,
`DataType`, `UserType`, `UUIDs`, and more follow the same pattern with smaller deltas.)*

### Subtle behavior changes an upgrader should know

- **`Metrics`** is now the 4.x session's Dropwizard registry; error/connection counts are **summed per
  node** (were cluster-wide), and several 3.x aggregates permanently report **zero** (§5.4). JMX is now
  wired at the `Cluster` level, not inside `Metrics`.
- **`PoolingOptions`** dynamic-growth/idle/queue/timeout/init-executor knobs are inert no-ops that warn
  once; core/max connections and max-requests *are* honored.
- **`Configuration`** is a reconstructed aggregate — introspection matches 3.x, but mutating a returned
  option bean after `connect()` does not re-tune the running session.
- **`Host`** DSE/token methods return empty/null on OSS 4.x (`getTokens()` empty — 4.x exposes ranges;
  `getDseVersion()` null; `isDseGraphEnabled()` false).
- **`ExecutionInfo`** on the live path exposes only the coordinator (`getTriedHosts()` singleton) and
  `getAchievedConsistencyLevel()` is `null` (4.x removed downgrading retry).
- **`LatencyAwarePolicy`/`ErrorAwarePolicy`** are inert wrappers — they construct and delegate routing,
  but their tuning knobs have no runtime effect.

---

## 5. Caveats & upgrade considerations

The shim's guiding policy is **never silent misbehavior.** The few APIs 4.x genuinely dropped fall
into exactly one of four visible buckets. The source of record is
[`COMPATIBILITY.md`](COMPATIBILITY.md).

### 5.1 Hard incompatibility — fail-fast (throws)

| 3.x API | Behavior | What to do |
|---|---|---|
| `ProtocolVersion.V1` / `V2` **as the negotiated version** (e.g. `withProtocolVersion(V1/V2)`) | Throws — 4.x has no native protocol v1/v2 | **Target v3+** (v3/v4/v5 negotiate normally). Drop any defensive v1/v2 pin. Merely *referencing* the enum constants (e.g. in a `switch`) is fine — they exist at the ABI level. |

### 5.2 No-op + one-time WARN — accepted but does nothing

These store the value (getter round-trips) but have no runtime effect; each logs a one-time SLF4J
`WARN` on first use. **Safe to leave in your code** — they never throw. Remove only to quiet the log.

| 3.x API | Reason |
|---|---|
| `PoolingOptions.setNewConnectionThreshold(...)` | 4.x pools are fixed-size (no on-demand growth). |
| `PoolingOptions.setIdleTimeoutSeconds(...)` | 4.x has no idle-connection trashing. |
| `PoolingOptions.setPoolTimeoutMillis(...)` | 4.x has no borrow/acquisition timeout. |
| `PoolingOptions.setMaxQueueSize(...)` | 4.x has no client-side acquisition queue. |
| `PoolingOptions.setInitializationExecutor(...)` | 4.x initializes pools on its own executors. |
| `PoolingOptions.refreshConnectedHosts()` / `refreshConnectedHost(Host)` | 4.x manages pools/distance internally — nothing to refresh. |

*(Core/max pool size and max-requests-per-connection **are** mapped and functional.)*

### 5.3 Inert — round-trips, no engine effect (silent)

| 3.x API | Reason |
|---|---|
| `ProtocolOptions.isNoCompact()` (the `NO_COMPACT` startup option) | 4.x removed the flag. Reading/writing it is harmless; if you relied on it to see COMPACT-STORAGE tables in legacy shape, that server-negotiation behavior is gone — plan schema accordingly. |

### 5.4 Lossy / approximate — reduced or synthetic values

Watch for code that depends on the *exact* value.

| 3.x API | What you get | Why |
|---|---|---|
| `AlreadyExistsException.getKeyspace()` / `getTable()` | Parsed from the error **message** | 4.x doesn't surface the structured fields. |
| `FrameTooLongException.getStreamId()` | Synthetic | Not carried by 4.x's exception. |
| `UnsupportedProtocolVersionException` version pair | Only the **attempted** version(s) | 4.x exposes attempted versions, not the full pair. |
| `EndPoint` on timeout/connection exceptions | From request context | Approximate origin address. |
| `ExecutionInfo.getAchievedConsistencyLevel()` | `null` | 4.x removed downgrading retry; matches the 3.x no-downgrade default. Don't treat `null` as an error. |
| `ExecutionInfo.getTriedHosts()` | Coordinator only | 4.x doesn't expose the full chain. |
| `Host.getTokens()` | Empty set | 4.x exposes token **ranges**, not per-host tokens. |
| `Host.getDseVersion()` / `getDseWorkload()` / `isDseGraphEnabled()` | `null` / `null` / `false` | OSS 4.x has no DSE fields. |
| `Session.State.getTrashedConnections/getInFlightQueries` | `0` | No 4.x equivalent. |
| `RetryDecision.getRetryConsistencyLevel()` (retry-at-different-CL) | Retry fires at the **original** CL | 4.x's retry verdict has no "retry at a different CL" variant — the CL change is dropped. |
| **`PagingState`** | Resumes correctly **within the shim**, but is **not wire-compatible** with a real-3.12.1 `PagingState` | Reimplemented with the 3.x layout. **Do not persist a paging state from one driver and resume it in the other.** In a mixed real-3.x / shim deployment, drain in-flight paged reads before cutover or key stored states by driver build. |
| `Statement.requestSizeInBytes(...)` | `-1` | Relies on 3.x wire encoders absent from the 4.x public surface. |

**Metrics that report a flat zero** (no 4.x equivalent): `getTrashedConnections`, all queue-depth
gauges (`getRequestQueueDepth`, `getExecutorQueueDepth`, `getBlockingExecutorQueueDepth`,
`getReconnectionSchedulerQueueSize`, `getTaskSchedulerQueueSize`), and the
`retries/ignores-on-client-timeout` and `retries/ignores-on-connection-error` counters. If a dashboard
keys off these, expect a zero series rather than a missing metric.

### 5.5 Excluded internal classes (usually harmless)

A set of 3.12.1 classes are `public` in bytecode but are **internal utilities with no stable contract
and no 4.x counterpart** — intentionally not reproduced and excluded from the japicmp comparison:
`Native`, `GuavaCompatibility` (reproduced internally only where the mapper needs it), `MetricsUtil`,
`DefaultPreparedStatement` (the shim supplies its own), `FramingFormatHandler`, `IgnoreJDK6Requirement`.
If your code links one directly you'll get a **loud** `NoClassDefFoundError`/`NoSuchMethodError` at
link time — not silent breakage. (The genuinely-public utilities `CodecUtils`, `ParseUtils`, and
`utils.Bytes` **are** reproduced and ABI-compared.)

### 5.6 Environment & packaging

- **JDK 8+.** The shim is Java 8 bytecode, matching 3.12.1.
- **The 4.x `java-driver-core` comes in transitively** — you don't add it yourself.
- **Unshaded Guava 19 must be on the classpath** — the *one classpath gotcha*. 3.12.1's public API
  exposed Guava types (`ListenableFuture`, `TypeToken`, `Optional`, `ImmutableList`, …), so they are
  part of the ABI the shim reproduces. 4.x's own Guava is *shaded*, so the two coexist. **If you had
  removed Guava expecting 4.x to shade everything, add it back.** The tell-tale of a missing one is
  `NoClassDefFoundError: com/google/common/...` on `ListenableFuture`/`TypeToken`/`Optional`.
- **Single jar** containing core+mapping+extras. For a strict per-artifact drop-in it can be split by
  package or republished under the original GAVs.
- **Netty/threading options** reach 4.x *internal* SPIs — functional today, but a 4.x version bump that
  changes those internals may require the shim to be adjusted.

---

## 6. Before-you-upgrade checklist

- [ ] **Guava on the classpath.** Ensure an **unshaded Guava (≈19)** is present — add it back if you
      dropped it. Verify `ListenableFuture`/`TypeToken`/`Optional` resolve.
- [ ] **Drop v1/v2 protocol pins.** Remove any `withProtocolVersion(V1/V2)`; target v3+ (§5.1) — the
      only hard fail-fast.
- [ ] **Audit `PagingState` persistence.** Don't interchange paging states between the real 3.x driver
      and the shim (§5.4). Drain paged reads across a mixed deployment, or key stored states by build.
- [ ] **Review custom `RetryPolicy`.** If any decision retries at a *different* CL, know the CL change
      is dropped (the retry still fires at the original CL).
- [ ] **Check dashboards/alerts** for the zero-value metrics (§5.4); repoint at 4.x-native equivalents.
- [ ] **Expect one-time WARN logs** from the inert `PoolingOptions` knobs (§5.2) — safe to leave.
- [ ] **Don't rely on `requestSizeInBytes(...)`** — it returns `-1`.
- [ ] **Don't reference excluded internals** (§5.5) — you'll get a loud link error if you did.
- [ ] **Note DSE/token accessors** return empty/null on OSS 4.x — don't treat as failures.
- [ ] **Pin the Guava and 4.x versions** in your dependency management to avoid a transitive downgrade.

---

## 7. Verifying & exploring for yourself

- [`COMPATIBILITY.md`](COMPATIBILITY.md) — the authoritative per-API status (supported-via-delegation /
  fail-fast / inert / no-op / lossy) with the exact caveats.
- [`PROVENANCE.md`](PROVENANCE.md) — the per-file provenance manifest and the ported-vs-net-new split.
- [`README.md`](README.md) — build and test instructions.
- [`docs/`](docs/) — the design corpus (`00-MAPPING-SPEC.md` per-member mapping catalogue,
  `00-ADAPTERS.md` bridge design, `00-IMPL-PLAN.md` implementation plan).
- Tooling (`src/test/scripts/`):
  - `provenance-manifest.sh` — classify every source file (`--list VERBATIM|NEAR-VERBATIM|HYBRID|NETNEW|NETNEW-PKG`).
  - `provenance-diff.sh <path>` — see exactly which lines the shim added on top of 3.12.1, including
    inside a hybrid file.
  - `provenance-check.sh` — the CI guard for the ported/net-new invariant.
