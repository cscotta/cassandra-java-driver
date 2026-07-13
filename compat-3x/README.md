# cassandra-driver-shim — 3.12.1 → 4.x binary-compatibility shim

This module reimplements the DataStax/Apache Cassandra Java driver **3.12.1** public API
(`com.datastax.driver.*` — core, mapping, and extras) on top of the **4.x** driver
(`org.apache.cassandra:java-driver-core`). An application compiled against
`cassandra-driver-{core,mapping,extras}:3.12.1` runs unchanged after swapping this artifact in.

It is intentionally kept **out of the root reactor** (`<modules>`) so the verbatim-ported 3.x
sources are not subject to the 4.x checkstyle/format/license plugins.

## Guarantees (verified)

- **Binary compatibility:** `japicmp` reports **0 incompatible** class/method/field changes vs the
  real 3.12.1 jars across `core`, `mapping`, and `extras` (public + protected, generic signatures,
  constant values, synthetic bridge methods).
- **Behavioral parity:** a differential suite compiles one program against the 3.12.1
  API and runs it under both the real 3.12.1 driver and this shim against the same live Cassandra,
  diffing the observations — **149/149 identical** on Cassandra 5.0.8.
- **The 3.12.1 driver's own unit suite:** run against the shim (real 3.x absent from the classpath),
  **64 public-API classes / 695 methods pass, identical to the real driver**.

See [`COMPATIBILITY.md`](COMPATIBILITY.md) for the full per-API status (Supported via delegation /
fail-fast / no-op / lossy) and [`docs/`](docs/) for the design (mapping spec, adapter design,
implementation plan).

## Build

```
JAVA_HOME=<jdk8> mvn -f pom.xml clean package -DskipTests
```

Depends on the 4.x driver artifacts being installed locally (`java-driver-{core,query-builder,
mapper-runtime}`) and **unshaded Guava 19** (3.12.1 exposed Guava types in its public ABI).

## Tests

The verification harness is a proper Maven TestNG suite in two layers:

- **`mvn test`** — Layer 1 standalone unit tests (surefire, `*Test` under
  `src/test/java/com/datastax/driver/shim/unit/`). Deterministic pure-logic checks — QueryBuilder /
  SchemaBuilder CQL, codecs, `DataType`/`TypeTokens`, exception hierarchy, `UUIDs`/`Bytes`/
  `VersionNumber`. **No Cassandra, no real 3.x driver.**

  ```
  JAVA_HOME=<jdk8> mvn -f pom.xml test
  ```

- **`mvn verify -Pit`** — Layer 2 integration tests (failsafe, `*IT` under
  `src/test/java/com/datastax/driver/shim/it/`), gated on a live Cassandra + the real 3.12.1 jars:
  - `DifferentialParityIT` — the real-vs-shim differential cross-check (149 scenarios, per-scenario
    reporting), reusing `ParityMain.java`.
  - `UpstreamSuiteIT` — runs the 3.12.1 driver's own unit suite under real and shim classpaths and
    asserts shim ⊇ real per candidate class (64 classes / 695 methods).

  Both **skip gracefully** if the contact point or the real jars are unavailable.

  ```
  JAVA_HOME=<jdk8> mvn -f pom.xml verify -Pit \
      -Dshim.contactPoint=127.0.0.1:9042 \
      -Dshim.cjd=<path to a 3.12.1 driver source checkout>
  ```

The harness assets live under the module test tree:

- `src/test/resources/parity/ParityMain.java` — the differential compatibility program (written
  against the 3.12.1 API; compiled once, run under both classpaths).
- `src/test/resources/parity/upstream-tests/` — `candidates.txt`, `exclusions.txt`, `RESULTS.md`, and
  the reference tallies.
- `src/test/scripts/` — the parameterized runner scripts (`run_parity.sh`, `run-upstream.sh`,
  `annotate.sh`, `japicmp.sh`), driven by the ITs. Paths are overridable via `SHIM_*` env vars /
  `-Dshim.*` system properties; the real-3.x and shim classpaths are resolved into `target/`.

The internal support/bridge layer lives in `com.datastax.shim.bridge` (package-private to the 3.x
ABI; `japicmp` compares only `com.datastax.driver.*`). Some bridges deliberately use 4.x *internal*
SPIs (`DefaultDriverContext`, internal `NettyOptions`, `DefaultTopologyMonitor`) to expose 3.x
behavior that 4.x otherwise hides — see `COMPATIBILITY.md` for where and why.
