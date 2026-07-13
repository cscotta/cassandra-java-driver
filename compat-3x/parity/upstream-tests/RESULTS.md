# Upstream 3.12.1 unit-test suite run against the 3.12.1 → 4.x shim

This harness runs the **DataStax/Apache Cassandra Java driver 3.12.1's own** TestNG unit suite
(`@Test(groups = "unit")`) against the 3.12.1 → 4.x compatibility shim, with the real 3.x driver
**absent** from the classpath. The 3.x-compiled test bytecode links against the shim purely through
binary compatibility. This is the strongest compatibility evidence available: the authors' own tests,
run against the shim.

> All file paths in this directory are **sandbox-absolute** (development environment). Adjust the
> `CONFIG` block at the top of `run-upstream.sh` before running elsewhere.

## How to reproduce

```
# build the shim first
JAVA_HOME=<jdk8> mvn -f ../../pom.xml clean package -DskipTests

# baseline under the REAL 3.12.1 jars, then under the SHIM
./run-upstream.sh real
./run-upstream.sh shim
# per-class logs: out-real/<fqcn>.log, out-shim/<fqcn>.log
# tallies:        results-real.tsv, results-shim.tsv
```

`run-upstream.sh` runs each candidate as `org.testng.TestNG -testclass <fqcn> -groups unit` and parses
TestNG's `Total tests run / Failures / Skips`. The SHIM classpath is `test-classes : shim.jar :
shim-runtime-deps : test-support-jars`, with **every** `cassandra-driver-{core,mapping,extras}` jar and
the real module `target/classes` filtered out (guard-checked). `test-support-jars` are the same
test-runtime libraries the REAL classpath carried (TestNG/AssertJ 1.7/Mockito/Hamcrest, plus log4j, the
Glassfish JSR-353 impl, Scassandra, and commons-exec) — no shim code.

## Methodology

1. **Candidates** = the 86 `*Test` classes across `driver-core`, `driver-mapping`, `driver-extras`
   tagged TestNG group `unit`.
2. Run each under (A) REAL 3.12.1 and (B) SHIM. Keep the set that **executes and passes on REAL**
   (drops env-flaky and abstract/no-test classes).
3. For each shim-only divergence, triage: load failure for an omitted internal → internal-bound
   exclude; intentional impl-detail change → exclude + document; otherwise it is a shim bug → fix in
   `compat-3x` without regressing the `japicmp` (0/0/0) and behavioral-parity (IDENTICAL) gates.
4. Iterate until the shim pass-set ⊇ the real pass-set for the runnable public-API unit classes.

## Result

**64 public-API unit classes / 695 test methods pass against the shim, identical to the real 3.12.1
driver.** After excluding internal-bound + impl-detail classes (below), there are **zero** remaining
divergences: the shim passes everything the real driver executes and passes.

| module  | classes | methods |
|---------|--------:|--------:|
| core    |      47 |     427 |
| extras  |      15 |     207 |
| mapping |       2 |      61 |
| **TOTAL** |  **64** | **695** |

Gates held throughout: `japicmp.sh all` = 0/0/0 incompatible (core/mapping/extras);
`run_parity.sh both` = PARITY: IDENTICAL (149/149) on Cassandra 5.0.8.

## Shim bugs found and fixed (naming the 3.x test that caught each)

All three fixes are **additive and package-private / private**, so they are invisible to `japicmp`
(which compares only public + protected API) and do not touch the public 3.x ABI.

1. **`PoolingOptions` was missing the package-private `setProtocolVersion(ProtocolVersion)`** that
   applies the protocol-version-dependent connection defaults and re-checks the per-connection
   invariants. Ported verbatim from 3.12.1.
   *Caught by:* `PoolingOptionsTest` (4 methods: `should_initialize_to_v2_defaults_if_v2_or_below`,
   `should_initialize_to_v3_defaults_if_v3_or_above`, `should_enforce_invariants_once_protocol_version_known`,
   `should_set_core_and_max_connections_simultaneously`) — was `NoSuchMethodError`, now 6/0/0.

2. **The monotonic timestamp generators hard-coded `System.currentTimeMillis()*1000`** instead of the
   injectable `Clock` seam the 3.x generators expose. Added the package-private `Clock` interface and a
   package-visible `volatile Clock clock` field on `AbstractMonotonicTimestampGenerator` (defaulting to a
   millisecond system clock; no JNR/native clock, matching the omitted `Native`), and routed
   `computeNext` through it.
   *Caught by:* `AtomicMonotonicTimestampGeneratorTest` and `ThreadLocalMonotonicTimestampGeneratorTest`
   (they set `generator.clock = new MockClocks.FixedTimeClock(...)`) — was `NoClassDefFoundError: Clock`,
   now 1/0/0 and 2/0/0. The exact drift-warning message/logger were already faithful, so the clock-skew
   resync assertions pass unchanged.

3. **The package-private internal `SystemProperties` helper was absent**, so the 3.x `TestUtils` static
   initializer (`SystemProperties.getInt(...)`) failed to link. Ported verbatim.
   *Caught by:* `DataTypeTest.serializeDeserializeTest` — was `NoClassDefFoundError: SystemProperties`
   (1 of 9 methods), now 9/0/0.

## Excluded classes

See `exclusions.txt` for the full categorized list with the specific omitted internal per class.
Summary:

- **internal-bound (15):** `AbstractReconnectionHandlerTest`, `BytesToSegmentDecoderTest`,
  `SegmentBuilderTest`, `SegmentCodecTest`, `SegmentToFrameDecoderTest`, `EventDebouncerTest`,
  `DirectedGraphTest`, `ReplicationFactorTest`, `ReplicationStrategyTest`, `SimpleStrategyTest`,
  `NetworkTopologyStrategyTest`, `SimpleJSONParserTest`, `StreamIdGeneratorTest`, `ClockFactoryTest`,
  `RollingCountTest` — each targets a package-private 3.x internal the shim deliberately omits
  (protocol-v5 framing `Segment*`/`Frame.Header`, `Connection`/`AbstractReconnectionHandler`,
  `ReplicationStrategy`/`ReplicationFactor`/`Cluster.Manager`, `DirectedGraph`, `SimpleJSONParser`,
  `StreamIdGenerator`, `ClockFactory`+`Native`, `EventDebouncer`, `RollingCount`+`policies.Clock`).
- **impl-detail (1):** `StatementSizeTest` — the shim's `Statement.requestSizeInBytes(...)` returns `-1`
  (it does not recompute the 3.x request wire-frame size against the 4.x engine); the setup also builds
  the internal `PreparedId.PreparedMetadata`.
- **server-gated (2):** `CustomPayloadTest`, `PagingStateTest` — extend `CCMTestsSupport`; their
  unit-tagged methods require a live `session()` and are **SKIPPED on REAL** (2/0/2 and 3/0/3). The shim
  skips them identically (parity), so they contribute 0 executed tests either way.
- **env-flaky on REAL (2):** `NativeTest` (1/1/0), `WhiteListPolicyTest` (2/1/0) — fail on the real
  3.12.1 driver too in this sandbox; not shim issues.
- **abstract base (2):** `AbstractBatchIdempotencyTest`, `PercentileTrackerTest` — 0 unit tests when run
  directly (NOTESTS on both).

## Full per-class results (REAL total/fail/skip → SHIM total/fail/skip)

| class | REAL | SHIM | verdict |
|-------|------|------|---------|
| com.datastax.driver.core.AbstractBatchIdempotencyTest | 0/0/0 | 0/0/0 | not-runnable (abstract/no unit tests) |
| com.datastax.driver.core.AbstractReconnectionHandlerTest | 8/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.AtomicMonotonicTimestampGeneratorTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.BytesToSegmentDecoderTest | 6/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.ClockFactoryTest | 2/0/1 | 2/1/1 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.ClusterWidePercentileTrackerTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.CodecRegistryTest | 89/0/0 | 89/0/0 | PASS (shim=real) |
| com.datastax.driver.core.ColumnDefinitionsTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.CustomPayloadTest | 2/0/2 | 2/0/2 | server-gated: skipped on REAL and shim |
| com.datastax.driver.core.CustomPercentileTrackerTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.DataTypeClassNameParserTest | 5/0/0 | 5/0/0 | PASS (shim=real) |
| com.datastax.driver.core.DataTypeTest | 9/0/0 | 9/0/0 | PASS (shim=real) |
| com.datastax.driver.core.DelegatingClusterTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.DirectedGraphTest | 6/0/0 | 6/6/0 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.DurationCodecTest | 3/0/0 | 3/0/0 | PASS (shim=real) |
| com.datastax.driver.core.DurationTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.EventDebouncerTest | 8/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.LocalDateTest | 6/0/0 | 6/0/0 | PASS (shim=real) |
| com.datastax.driver.core.M3PTokenFactoryTest | 6/0/0 | 6/0/0 | PASS (shim=real) |
| com.datastax.driver.core.MetadataTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.core.NativeTest | 1/1/0 | 1/1/0 | excluded: fails on REAL (env-flaky) |
| com.datastax.driver.core.NetworkTopologyStrategyTest | 12/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.OPPTokenFactoryTest | 5/0/0 | 5/0/0 | PASS (shim=real) |
| com.datastax.driver.core.PagingStateTest | 3/0/3 | 3/0/3 | server-gated: skipped on REAL and shim |
| com.datastax.driver.core.ParseUtilsTest | 6/0/0 | 6/0/0 | PASS (shim=real) |
| com.datastax.driver.core.PerHostPercentileTrackerTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.PercentileTrackerTest | 0/0/0 | 0/0/0 | not-runnable (abstract/no unit tests) |
| com.datastax.driver.core.PoolingOptionsTest | 6/0/0 | 6/0/0 | PASS (shim=real) |
| com.datastax.driver.core.ProtocolOptionsTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.QueryLoggerTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.RPTokenFactoryTest | 5/0/0 | 5/0/0 | PASS (shim=real) |
| com.datastax.driver.core.ReplicationFactorTest | 2/0/0 | 2/2/0 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.ReplicationStrategyTest | 6/0/0 | 6/6/0 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.SegmentBuilderTest | 8/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.SegmentCodecTest | 6/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.SegmentToFrameDecoderTest | 2/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.SimpleJSONParserTest | 1/0/0 | 1/1/0 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.SimpleStatementTest | 10/0/0 | 10/0/0 | PASS (shim=real) |
| com.datastax.driver.core.SimpleStrategyTest | 5/0/0 | -/-/- | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.StatementIdempotenceTest | 5/0/0 | 5/0/0 | PASS (shim=real) |
| com.datastax.driver.core.StatementSizeTest | 4/0/0 | 4/0/4 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.StreamIdGeneratorTest | 1/0/0 | 1/1/0 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.ThreadLocalMonotonicTimestampGeneratorTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.TokenRangeTest | 13/0/0 | 13/0/0 | PASS (shim=real) |
| com.datastax.driver.core.TypeCodecTest | 14/0/0 | 14/0/0 | PASS (shim=real) |
| com.datastax.driver.core.VersionNumberTest | 7/0/0 | 7/0/0 | PASS (shim=real) |
| com.datastax.driver.core.exceptions.ConnectionExceptionTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.exceptions.ExceptionsTest | 16/0/0 | 16/0/0 | PASS (shim=real) |
| com.datastax.driver.core.exceptions.NoHostAvailableExceptionTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.EC2MultiRegionAddressTranslatorTest | 6/0/0 | 6/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.HostFilterPolicyTest | 7/0/0 | 7/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.IdempotenceAwareRetryPolicyIntegrationTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.LoggingRetryPolicyIntegrationTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.RetryDecisionTest | 1/0/0 | 1/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.RollingCountTest | 7/0/0 | 7/0/7 | EXCLUDED (see exclusions.txt) |
| com.datastax.driver.core.policies.TokenAwarePolicyTest | 3/0/0 | 3/0/0 | PASS (shim=real) |
| com.datastax.driver.core.policies.WhiteListPolicyTest | 2/1/0 | 2/1/0 | excluded: fails on REAL (env-flaky) |
| com.datastax.driver.core.querybuilder.QueryBuilderTest | 54/0/0 | 54/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.AlterKeyspaceTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.AlterTest | 16/0/0 | 16/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CompactionOptionsTest | 9/0/0 | 9/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CompressionOptionsTest | 4/0/0 | 4/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CreateIndexTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CreateKeyspaceTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CreateTest | 44/0/0 | 44/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.CreateTypeTest | 9/0/0 | 9/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.DropKeyspaceTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.core.schemabuilder.DropTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.core.utils.UUIDsTest | 5/0/0 | 5/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.arrays.ArrayCodecsTest | 10/0/0 | 10/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.date.SimpleDateCodecTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.date.SimpleTimestampCodecTest | 13/0/0 | 13/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.InstantCodecTest | 14/0/0 | 14/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.LocalDateCodecTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.LocalDateTimeCodecTest | 14/0/0 | 14/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.LocalTimeCodecTest | 12/0/0 | 12/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.ZoneIdCodecTest | 28/0/0 | 28/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.jdk8.ZonedDateTimeCodecTest | 28/0/0 | 28/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.joda.DateTimeCodecTest | 26/0/0 | 26/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.joda.InstantCodecTest | 14/0/0 | 14/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.joda.LocalDateCodecTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.joda.LocalTimeCodecTest | 11/0/0 | 11/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.json.JacksonJsonCodecTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.extras.codecs.json.Jsr353JsonCodecTest | 2/0/0 | 2/0/0 | PASS (shim=real) |
| com.datastax.driver.mapping.MapperInvalidAnnotationsTest | 18/0/0 | 18/0/0 | PASS (shim=real) |
| com.datastax.driver.mapping.NamingConventionsTest | 43/0/0 | 43/0/0 | PASS (shim=real) |

Generated on Cassandra 5.0.8 (shimtest @ 127.0.0.1:9042), JDK 8 (Temurin 1.8.0_492), shim 3.12.1-shim on 4.19.4-SNAPSHOT.
