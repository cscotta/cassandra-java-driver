# 00 — Shared Shim-Internal Support / Bridge Package

Package: `com.datastax.shim.bridge` (package-private / not part of the shimmed 3.x public ABI —
japicmp compares only `com.datastax.driver.*`, so nothing here is ABI-visible). This is the
single home for every 3.x↔4.x conversion. Areas call into it rather than each rolling their own.

This document also records the **canonical decision** for each type that more than one area
touches (DataType, TypeCodec, Row/data, Statement, exceptions, Metadata, futures, enums), so the
areas cannot diverge.

---

## Canonical cross-area decisions (one ruling each)

**DataType** — Reproduce the full 3.x `DataType` hierarchy as standalone classes in
`com.datastax.driver.core`; never subclass 4.x. Each shim `DataType` may cache a 4.x `DataType`
for conversion. `equals`/`hashCode`/`toString`/`asFunctionParameterString`/`asCQLQuery` are
reimplemented from 3.x source (TEXT≡VARCHAR aliasing, `frozen<…>` templates) — NOT delegated to
4.x `asCql`/`equals`. All crossings go through `DataTypeBridge`. Owner: type-system. Consumers
(data-values, results, statements, metadata, querybuilder, schemabuilder, extras) use the bridge,
never their own conversion.

**TypeCodec** — Reproduce 3.x `TypeCodec<T>` and its nested `Abstract*`/`Primitive*` bases as
abstract classes. Concrete factory codecs wrap a 4.x `TypeCodec` and map `serialize→encode`,
`deserialize→decode`, `format`/`parse` 1:1. The `Abstract*Codec` and `Primitive*Codec` bases have
NO 4.x public counterpart, so their serialization bodies are ported from 3.x. All crossings go
through `CodecBridge`, which is **idempotent** (unwraps a 3x-over-4x-over-3x chain). Owner:
type-system. Consumers (data-values, statements, results, extras subclasses, mapping
`MappedUDTCodec`) use the bridge.

**Row / GettableData / SettableData** — One canonical value-access implementation. Reproduce the
package-private abstract chain with exact FQNs: `AbstractGettableByIndexData` →
`AbstractAddressableByIndexData<T>` → `AbstractData<T>`, plus public `AbstractGettableData`. The
abstract protected hooks `getValue(int)`/`setValue(int,ByteBuffer)` are backed by a wrapped 4.x
`GettableByIndex`/`SettableByIndex` (or an `AccessibleByName` holder) via
`getBytesUnsafe`/`setBytesUnsafe`; the 3.x typed-accessor bodies are copied verbatim so codec
dispatch and exception semantics are identical. `Row`, `BoundStatement`, `TupleValue`, `UDTValue`
all sit on this one chain. Owner: data-values. Consumers (results `Row`, statements
`BoundStatement`) reuse it — no parallel accessor logic.

**Statement** — Shim statements are standalone **mutable state-holders**; they do NOT wrap a 4.x
statement. The immutable `4.x Statement<?>` is built once, at execute time, by the Session shim
reading the accumulated fields (`StatementBridge.toV4`). `querybuilder.BuiltStatement` IS a 3.x
`RegularStatement`; the same execute-time path converts it via `getQueryString()`+`getValues()`.
Owner: statements + session-cluster (materialize). Consumers never construct 4.x statements
directly.

**Exceptions** — Reimplement the entire 3.x hierarchy natively as value objects. No shim
exception extends or wraps a 4.x exception. Translation is one-way (4.x thrown → 3.x shim) via
`ExceptionBridge` at every adapter boundary. Owner: exceptions.

**Metadata** — Reproduce each 3.x metadata type as a class wrapping a 4.x interface delegate.
`AbstractTableMetadata`'s 9 protected final fields + 9-arg ctor force **eager** translation:
subclasses compute field values from the 4.x delegate and call `super(...)`. Parent back-refs are
set at construction (4.x returns `CqlIdentifier`, not the parent object). `Host` wraps 4.x `Node`.
`VersionNumber` is shim-native (Comparable only, not Serializable). Owner: metadata.

**Futures / async** — Guava is UNSHADED. A single `FutureBridge` converts
`CompletionStage`↔Guava `ListenableFuture` both directions and builds the `CloseFuture` (a
`com.google.common.util.concurrent.AbstractFuture<Void>` subclass) and `ResultSetFuture`. Owner:
bridge package; used by session-cluster, results, mapping, misc-core.

**Enums** — Keep every 3.x enum as an enum; adapters map by `name()` (never ordinal). `EnumBridge`
holds all conversions. Owner: enums.

---

## 1. `FutureBridge` — Guava ListenableFuture ↔ CompletionStage

```java
final class FutureBridge {
  private FutureBridge() {}

  // CompletionStage -> Guava ListenableFuture (via SettableFuture + whenComplete)
  static <T> com.google.common.util.concurrent.ListenableFuture<T>
      toListenable(java.util.concurrent.CompletionStage<T> stage);

  // element-mapping variant (e.g. CompletionStage<CqlSession> -> ListenableFuture<Session>)
  static <U, V> com.google.common.util.concurrent.ListenableFuture<V>
      toListenable(java.util.concurrent.CompletionStage<U> stage,
                   java.util.function.Function<? super U, ? extends V> mapper);

  // Guava ListenableFuture -> CompletionStage (addListener + directExecutor)
  static <T> java.util.concurrent.CompletionStage<T>
      toCompletionStage(com.google.common.util.concurrent.ListenableFuture<T> future);

  // uninterruptible blocking get for ResultSetFuture.getUninterruptibly
  static <T> T getUninterruptibly(com.google.common.util.concurrent.ListenableFuture<T> f);
  static <T> T getUninterruptibly(com.google.common.util.concurrent.ListenableFuture<T> f,
                                  long timeout, java.util.concurrent.TimeUnit unit)
      throws java.util.concurrent.TimeoutException;
}
```

### 1a. `CloseFuture` bridge

`com.datastax.driver.core.CloseFuture` is `public abstract class extends
com.google.common.util.concurrent.AbstractFuture<Void>` with abstract `CloseFuture force()`.
Concrete shim impl (package-private) wraps a `CompletionStage<Void>` and the owning `CqlSession`:

```java
final class ShimCloseFuture extends com.datastax.driver.core.CloseFuture {
  ShimCloseFuture(java.util.concurrent.CompletionStage<Void> stage,
                  com.datastax.oss.driver.api.core.CqlSession owner /*nullable*/);
  // stage.whenComplete((v,t) -> { if (t!=null) setException(t); else set(null); })
  @Override public com.datastax.driver.core.CloseFuture force(); // owner.forceCloseAsync(); return this
  static com.datastax.driver.core.CloseFuture immediate();       // already-completed (unbuilt Cluster)
}
```

### 1b. `ResultSetFuture` bridge

`ResultSetFuture extends ListenableFuture<ResultSet>`. Built from `CompletionStage<AsyncResultSet>`;
the resolved value is wrapped in an auto-paging shim `ResultSet` (see `ResultSetBridge`):

```java
final class ShimResultSetFuture
    extends com.google.common.util.concurrent.AbstractFuture<com.datastax.driver.core.ResultSet>
    implements com.datastax.driver.core.ResultSetFuture {
  ShimResultSetFuture(java.util.concurrent.CompletionStage<
      com.datastax.oss.driver.api.core.cql.AsyncResultSet> stage);
  @Override public com.datastax.driver.core.ResultSet getUninterruptibly();
  @Override public com.datastax.driver.core.ResultSet getUninterruptibly(long t, TimeUnit u)
      throws java.util.concurrent.TimeoutException;
}
```

---

## 2. Type & codec bridging

### 2a. `DataTypeBridge`

```java
final class DataTypeBridge {
  static com.datastax.oss.driver.api.core.type.DataType toV4(com.datastax.driver.core.DataType d);
  static com.datastax.driver.core.DataType toV3(com.datastax.oss.driver.api.core.type.DataType d);
  // recursive: primitives via Name/protocol-code table; list/set/map -> DataTypes.listOf/setOf/mapOf;
  // custom -> DataTypes.custom; tuple -> DataTypes.tupleOf; UDT -> UserDefinedType.
}
```

### 2b. `GenericTypeBridge` — unshaded Guava `TypeToken` ↔ 4.x `GenericType`

The two Guava copies are not assignment-compatible; round-trip through `java.lang.reflect.Type`.

```java
final class GenericTypeBridge {
  static <T> com.datastax.oss.driver.api.core.type.reflect.GenericType<T>
      toGeneric(com.google.common.reflect.TypeToken<T> token);   // GenericType.of(token.getType())
  static <T> com.google.common.reflect.TypeToken<T>
      toToken(com.datastax.oss.driver.api.core.type.reflect.GenericType<T> g); // TypeToken.of(g.getType())
}
```

### 2c. `CodecBridge` — idempotent, both directions

```java
final class CodecBridge {
  // wrap a 4.x codec behind the 3.x abstract class (serialize->encode, deserialize->decode)
  static <T> com.datastax.driver.core.TypeCodec<T>
      toV3(com.datastax.oss.driver.api.core.type.codec.TypeCodec<T> v4);
  // expose a 3.x codec to a 4.x MutableCodecRegistry (encode->serialize, decode->deserialize)
  static <T> com.datastax.oss.driver.api.core.type.codec.TypeCodec<T>
      toV4(com.datastax.driver.core.TypeCodec<T> v3);
  // idempotent unwrap: if v4 is itself a wrapped 3x codec, return the original 3x instance
  static <T> com.datastax.driver.core.TypeCodec<T> unwrapOrWrapV3(Object codec);
}
```

`ShimTypeCodec<T>` (the concrete wrapper returned by factories) extends `TypeCodec<T>` and, where
the Java type differs, applies a value converter (§3d) inside `serialize`/`deserialize`/`format`/
`parse`, catching 4.x `IllegalArgumentException`/`ClassCastException` and rethrowing 3.x
`InvalidTypeException` (via `ExceptionBridge`).

### 2d. `CodecRegistryBridge`

Shim `CodecRegistry` wraps a 4.x `MutableCodecRegistry` (`DefaultCodecRegistry`).
`register(...)` adapts the 3.x codec via `CodecBridge.toV4`, calls 4.x `register`, returns `this`.
`codecFor(DataType, TypeToken)` converts via `GenericTypeBridge`. 4.x `CodecNotFoundException` →
3.x `CodecNotFoundException` via `ExceptionBridge`.

---

## 3. Row / data bridging

### 3a. Value backing

`AbstractData<T>` and friends hold a wrapped 4.x `GettableByIndex`+`SettableByIndex` (for
BoundStatement/TupleValue) or `GettableByName`+`SettableByName`. Protected hooks resolve through
raw bytes so all typed accessors reuse the 3.x code path:

```java
abstract class DataBacking {
  java.nio.ByteBuffer getValue(int i);            // -> v4.getBytesUnsafe(i)
  void setValue(int i, java.nio.ByteBuffer v);    // -> v4.setBytesUnsafe(i, v)
  int getIndexOf(String name);                    // 3.x name-resolution (dup/quoting via handleId)
  java.util.List<Integer> getAllIndexesOf(String name);
}
```

### 3b. `RowBridge` / `ResultSetBridge`

```java
final class ResultSetBridge {
  static com.datastax.driver.core.Row toV3Row(com.datastax.oss.driver.api.core.cql.Row v4);
  // sync auto-paging wrapper over 4.x ResultSet
  static com.datastax.driver.core.ResultSet wrap(com.datastax.oss.driver.api.core.cql.ResultSet v4);
  // AsyncResultSet + transparent pager (fetchNextPage) presented as a 3.x auto-paging ResultSet
  static com.datastax.driver.core.ResultSet wrapAsyncPaged(
      com.datastax.oss.driver.api.core.cql.AsyncResultSet first);
  static com.datastax.driver.core.ColumnDefinitions toV3Defs(
      com.datastax.oss.driver.api.core.cql.ColumnDefinitions v4); // CqlIdentifier -> asInternal()
  static com.datastax.driver.core.ExecutionInfo toV3Info(
      com.datastax.oss.driver.api.core.cql.ExecutionInfo v4);
}
```

### 3c. Value-holder wrap/unwrap

```java
final class ValueHolderBridge {
  static com.datastax.driver.core.TupleValue toV3(com.datastax.oss.driver.api.core.data.TupleValue v);
  static com.datastax.oss.driver.api.core.data.TupleValue toV4(com.datastax.driver.core.TupleValue v);
  static com.datastax.driver.core.UDTValue toV3(com.datastax.oss.driver.api.core.data.UdtValue v);
  static com.datastax.oss.driver.api.core.data.UdtValue toV4(com.datastax.driver.core.UDTValue v);
}
```

### 3d. `TimeValueBridge` — value converters

```java
final class TimeValueBridge {
  static java.time.Instant toInstant(java.util.Date d);            // timestamp codec
  static java.util.Date toDate(java.time.Instant i);
  static java.time.LocalDate toJavaLocalDate(com.datastax.driver.core.LocalDate d); // date codec
  static com.datastax.driver.core.LocalDate toShimLocalDate(java.time.LocalDate d);
  static java.time.LocalTime toLocalTime(long nanosSinceMidnight);  // time codec (PrimitiveLong)
  static long toNanos(java.time.LocalTime t);
  static com.datastax.driver.core.Duration toShimDuration(
      com.datastax.oss.driver.api.core.data.CqlDuration d);
  static com.datastax.oss.driver.api.core.data.CqlDuration toV4Duration(
      com.datastax.driver.core.Duration d);
}
```

---

## 4. Statement bridging (mutable 3.x → immutable 4.x)

Built at execute time from the accumulated 3.x fields; 3.x setters never touch 4.x.

```java
final class StatementBridge {
  // dispatch on shim statement kind, produce an immutable 4.x request
  static com.datastax.oss.driver.api.core.cql.Statement<?> toV4(
      com.datastax.driver.core.Statement s,
      com.datastax.oss.driver.api.core.CqlSession session);

  // SimpleStatement:  SimpleStatement.builder(query).addPositionalValues(...) / named ...
  //                   .setConsistencyLevel(EnumBridge.cl(...)).setPageSize(...).setIdempotent(...)
  //                   .setPagingState(...).setRoutingKey(...).setKeyspace(CqlIdentifier)...build()
  // BoundStatement:   prepared.bind() then copy set*/get* backing bytes + per-statement options
  // BatchStatement:   BatchStatement.builder(BatchTypeBridge.toV4(type)).addStatements(...)...
  // RegularStatement (BuiltStatement): SimpleStatement.newInstance(getQueryString(), getValues(...))

  static com.datastax.driver.core.PreparedStatement toV3Prepared(
      com.datastax.oss.driver.api.core.cql.PreparedStatement v4,
      /*default options carried from 3.x*/ Object shimDefaults);
}
```

Notes: per-request `RetryPolicy` and `requestSizeInBytes` are inert/UNMAPPED (see mapping spec).
`PagingState` is reimplemented standalone (3.x MD5 layout); `setPagingState` matching reproduces
3.x hashing over queryString + encoded values + raw state (depends on `CodecBridge` producing the
same ByteBuffers).

---

## 5. Exception translation — `ExceptionBridge`

One-way 4.x → 3.x. `instanceof` cascade; unwraps `AllNodesFailedException` to per-node 3.x
coordinator exceptions.

```java
final class ExceptionBridge {
  static RuntimeException toV3(Throwable v4);   // returns a com.datastax.driver.core.exceptions.*

  // AllNodesFailedException -> NoHostAvailableException with Map<EndPoint,Throwable>
  //   (recursively translate each per-node cause; Node -> EndPoint via MetadataBridge)
  // DriverTimeoutException -> OperationTimedOutException (EndPoint supplied from context)
  // servererrors.* (ReadTimeout/WriteTimeout/Unavailable/ReadFailure/WriteFailure/
  //   CASWriteUnknown/Overloaded/... ) -> matching 3.x subclass
  //   (ConsistencyLevel via EnumBridge, WriteType via EnumBridge)
  // AlreadyExistsException -> parse keyspace/table from message (getters lossy)
  // auth.AuthenticationException -> exceptions.AuthenticationException
  // codec IllegalArgumentException/ClassCastException (from a codec call) -> InvalidTypeException
  // 4.x CodecNotFoundException -> exceptions.CodecNotFoundException (GenericType->TypeToken; DataType->3x)
}
```

Types with no 4.x source (`UnpreparedException`, `BusyPoolException`, `DriverInternalError`,
`PagingStateException`, `TraceRetrievalException`, `UnresolvedUserTypeException`,
`UnsupportedFeatureException`) are never produced by the bridge; they exist for ABI and are
thrown only by shim-internal code.

---

## 6. Metadata / Host ↔ Node — `MetadataBridge`

```java
final class MetadataBridge {
  static com.datastax.driver.core.Host toHost(com.datastax.oss.driver.api.core.metadata.Node n);
  static com.datastax.driver.core.EndPoint toEndPoint(
      com.datastax.oss.driver.api.core.metadata.Node n);      // Node -> 3.x EndPoint (getCoordinator)
  static com.datastax.driver.core.EndPoint toEndPoint(
      com.datastax.oss.driver.api.core.metadata.EndPoint v4);
  static com.datastax.driver.core.VersionNumber toVersion(
      com.datastax.oss.driver.api.core.Version v);            // via shim-native VersionNumber.parse
  static String toName(com.datastax.oss.driver.api.core.CqlIdentifier id);   // asInternal()
  static com.datastax.oss.driver.api.core.CqlIdentifier fromName(String s);  // fromInternal()
  // caching so repeated getAllHosts()/getColumns() return equals/hashCode-stable shim instances
}
```

Host/Node caching is required to preserve 3.x identity semantics used inside `equals()`.
`NodeStateListener` and 4.x `SchemaChangeListener` adapters forward events to the shim
`Host$StateListener` / `SchemaChangeListener` (Created/Dropped/Updated → Added/Removed/Changed).

---

## 7. Enum & ConsistencyLevel/WriteType mapping — `EnumBridge`

All translation by `name()`, never ordinal.

```java
final class EnumBridge {
  static com.datastax.oss.driver.api.core.ConsistencyLevel cl(com.datastax.driver.core.ConsistencyLevel c);
  static com.datastax.driver.core.ConsistencyLevel cl(com.datastax.oss.driver.api.core.ConsistencyLevel c);
  static com.datastax.oss.driver.api.core.servererrors.WriteType wt(com.datastax.driver.core.WriteType w);
  static com.datastax.driver.core.WriteType wt(com.datastax.oss.driver.api.core.servererrors.WriteType w);
  static com.datastax.oss.driver.api.core.loadbalancing.NodeDistance dist(
      com.datastax.driver.core.HostDistance d);
  static com.datastax.driver.core.HostDistance dist(
      com.datastax.oss.driver.api.core.loadbalancing.NodeDistance d);
  static com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder order(
      com.datastax.driver.core.ClusteringOrder o);
  static com.datastax.oss.driver.api.core.ProtocolVersion pv(com.datastax.driver.core.ProtocolVersion v);
  //   pv: V1/V2 -> throw UnsupportedOperationException with migration note
  static com.datastax.driver.core.ProtocolVersion pv(com.datastax.oss.driver.api.core.ProtocolVersion v);
  static com.datastax.oss.driver.api.core.cql.BatchType batchType(
      com.datastax.driver.core.BatchStatement.Type t);
}
```

---

## 8. Config bridge (options + policies → 4.x) — `ConfigBridge`

Owned/consumed by session-cluster; converts the mutable option beans and the policy graph into a
4.x `ProgrammaticDriverConfigLoaderBuilder` / `OptionsMap`.

```java
final class ConfigBridge {
  static void applySocketOptions(builder, com.datastax.driver.core.SocketOptions o);
  static void applyQueryOptions(builder, com.datastax.driver.core.QueryOptions o);
  static void applyPoolingOptions(builder, com.datastax.driver.core.PoolingOptions o); // core/max collapse
  static void applyProtocolOptions(builder, com.datastax.driver.core.ProtocolOptions o); // Compression->string
  // LB policy graph: unwrap TokenAware/HostFilter/LatencyAware/ErrorAware to routing child,
  //   map DCAware.localDc -> LOAD_BALANCING_LOCAL_DATACENTER; arbitrary custom LBP -> UnsupportedOperationException
  static void applyLoadBalancing(builder, com.datastax.driver.core.policies.LoadBalancingPolicy p);
}
```

4.x-side **policy bridge adapters** (implement the 4.x RetryPolicy/RequestTracker/etc. SPI and
forward to the user's 3.x instance) also live here; the `RetryDecision`→`RetryVerdict` mapping is
lossy on consistency level (retry→RETRY_SAME, tryNextHost→RETRY_NEXT).

---

## 9. Endpoint / latency wiring — `TrackerBridge`

```java
final class TrackerBridge {
  // 3.x LatencyTracker -> 4.x RequestTracker (onNodeSuccess/onNodeError -> update)
  static com.datastax.oss.driver.api.core.tracker.RequestTracker toRequestTracker(
      com.datastax.driver.core.LatencyTracker t);
  // 3.x EndPoint (resolve():InetSocketAddress) <-> 4.x EndPoint (resolve():SocketAddress)
  static com.datastax.oss.driver.api.core.metadata.EndPoint toV4(com.datastax.driver.core.EndPoint e);
  static com.datastax.driver.core.EndPoint toV3(com.datastax.oss.driver.api.core.metadata.EndPoint e);
}
```

---

## Dependency notes for the bridge package

- Depends on the shim public types (it constructs them) and on 4.x + unshaded Guava.
- HdrHistogram and Dropwizard/Netty/slf4j are runtime deps of the areas that expose them
  (misc-core percentile trackers; options-config Metrics/QueryLogger/NettyOptions), not of the
  bridge itself.
- `CodecBridge` idempotency and `MetadataBridge` caching are the two correctness-critical
  invariants: double-adaptation causes infinite delegation; unstable Host/metadata identity breaks
  3.x `equals()` semantics.
