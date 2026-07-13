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
- **Behavioral parity:** a differential suite (`parity/`) compiles one program against the 3.12.1
  API and runs it under both the real 3.12.1 driver and this shim against the same live Cassandra,
  diffing the observations — **149/149 identical** on Cassandra 5.0.8.

See [`COMPATIBILITY.md`](COMPATIBILITY.md) for the full per-API status (Supported via delegation /
fail-fast / no-op / lossy) and [`docs/`](docs/) for the design (mapping spec, adapter design,
implementation plan).

## Build

```
JAVA_HOME=<jdk8> mvn -f pom.xml clean package -DskipTests
```

Depends on the 4.x driver artifacts being installed locally (`java-driver-{core,query-builder,
mapper-runtime}`) and **unshaded Guava 19** (3.12.1 exposed Guava types in its public ABI).

## Verification tooling (`parity/`)

- `ParityMain.java` — the differential compatibility suite (written against the 3.12.1 API).
- `run_parity.sh` — compiles it once and runs it under the real-3.12.1 and shim classpaths, then
  diffs. **Contains absolute paths from the development sandbox** — adjust the path variables at the
  top before running elsewhere.
- `japicmp.sh` — runs the ABI comparison against the reference 3.12.1 jars (also uses sandbox paths).

The internal support/bridge layer lives in `com.datastax.shim.bridge` (package-private to the 3.x
ABI; `japicmp` compares only `com.datastax.driver.*`). Some bridges deliberately use 4.x *internal*
SPIs (`DefaultDriverContext`, internal `NettyOptions`, `DefaultTopologyMonitor`) to expose 3.x
behavior that 4.x otherwise hides — see `COMPATIBILITY.md` for where and why.
