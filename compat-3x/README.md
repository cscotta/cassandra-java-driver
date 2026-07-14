# cassandra-driver-shim — 3.12.1 → 4.x binary-compatibility shim

This module reimplements the DataStax/Apache Cassandra Java driver 3.12.1 public API
(`com.datastax.driver.*` — core, mapping, and extras) on top of the 4.x driver
(`org.apache.cassandra:java-driver-core`). An application compiled against
`cassandra-driver-{core,mapping,extras}:3.12.1` runs unchanged after swapping this artifact in.

New to the shim? [`GUIDE.md`](GUIDE.md) is the full design & upgrade guide — how the shim works,
what was carried forward from 3.x (verbatim vs. modified), how each API maps onto 4.x, and the
caveats + checklist for upgraders.

It is kept out of the root reactor (`<modules>`) so the verbatim-ported 3.x
sources are not subject to the 4.x checkstyle/format/license plugins.

## Guarantees (verified)

- **Binary compatibility:** `japicmp` reports 0 incompatible class/method/field changes vs the
  real 3.12.1 jars across `core`, `mapping`, and `extras` (public + protected, generic signatures,
  constant values, synthetic bridge methods).
- **Behavioral parity:** a differential suite compiles one program against the 3.12.1
  API and runs it under both the real 3.12.1 driver and this shim against the same live Cassandra,
  diffing the observations — 149/149 identical on Cassandra 5.0.8.
- **The 3.12.1 driver's own unit suite:** run against the shim (real 3.x absent from the classpath),
  64 public-API classes / 695 methods pass, identical to the real driver.

See [`COMPATIBILITY.md`](COMPATIBILITY.md) for the full per-API status (Supported via delegation /
fail-fast / no-op / lossy), [`GUIDE.md`](GUIDE.md) for the design & upgrade guide, and
[`docs/`](docs/) for the design corpus (mapping spec, adapter design, implementation plan).

## Build

```
JAVA_HOME=<jdk8> mvn -f pom.xml clean package -DskipTests
```

Depends on the 4.x driver artifacts being installed in the local Maven repository (`java-driver-{core,query-builder,
mapper-runtime}`) and unshaded Guava 19 (3.12.1 exposed Guava types in its public ABI).

## Tests

The verification harness is a proper Maven TestNG suite in two layers:

- **`mvn test`** — Layer 1 standalone unit tests (surefire, `*Test` under
  `src/test/java/com/datastax/driver/shim/unit/`). Deterministic pure-logic checks — QueryBuilder /
  SchemaBuilder CQL, codecs, `DataType`/`TypeTokens`, exception hierarchy, `UUIDs`/`Bytes`/
  `VersionNumber`. No Cassandra, no real 3.x driver.

  ```
  JAVA_HOME=<jdk8> mvn -f pom.xml test
  ```

- **`mvn verify -Pit`** — Layer 2 integration tests (failsafe, `*IT` under
  `src/test/java/com/datastax/driver/shim/it/`), gated on a live Cassandra + the real 3.12.1 jars:
  - `DifferentialParityIT` — the real-vs-shim differential cross-check (149 scenarios, per-scenario
    reporting), reusing `ParityMain.java`.
  - `UpstreamSuiteIT` — runs the 3.12.1 driver's own unit suite under real and shim classpaths and
    asserts shim ⊇ real per candidate class (64 classes / 695 methods).

  Both skip if the contact point or the real jars are unavailable.

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
ABI; `japicmp` compares only `com.datastax.driver.*`). Some bridges use 4.x *internal*
SPIs (`DefaultDriverContext`, internal `NettyOptions`, `DefaultTopologyMonitor`) to expose 3.x
behavior that 4.x hides — see `COMPATIBILITY.md` for where and why.

## Source layout: ported-3.x vs net-new shim code

Most of this module — 239 of 332 source files — is Cassandra Java driver 3.12.1 source
carried forward verbatim; the rest is net-new shim code. The two are split by source
directory so provenance is visible at a glance (package names stay `com.datastax.driver.*` — the ABI
requires them, so this is a directory split, not a package rename):

- **`src/main/java-driver-3x/`** — 3.12.1 source carried forward. Every file is identical to its
  3.12.1 counterpart modulo comments, contains no shim code, and carries no provenance
  header (preserving that byte-identity is the point). Compiled in via
  `build-helper-maven-plugin`'s `add-source` (base `<build>`, so it applies to every build).
- **`src/main/java/`** — every file that contains net-new shim code: the *hybrids* (3.12.1 classes
  with 4.x-delegating facade edits, e.g. `Cluster`, `Metadata`, `Metrics`), the net-new glue classes,
  and the `com.datastax.shim.bridge` layer. Every file here carries a `// Shim provenance:` header.

Three tools maintain and expose this (all under `src/test/scripts/`, see also
[`PROVENANCE.md`](PROVENANCE.md)):

- `provenance-manifest.sh` — classifies every main source file against the 3.12.1 sources and
  regenerates `PROVENANCE.md` (`--md PROVENANCE.md`).
- `provenance-check.sh` — CI guard: asserts *file in `java/` ⇔ has a header* and *file in
  `java-driver-3x/` ⇔ no header and identical-to-3.12.1-modulo-comments*.
- `provenance-diff.sh` — shows the line-level delta the shim adds on top of 3.12.1, including
  the hybrid files, by diffing against the orphan tag `shim-3x-vendor-3.12.1` (pristine
  3.12.1 sources committed at the shim's own paths). This is diff-based, not `git blame`-based — see
  `PROVENANCE.md`.
