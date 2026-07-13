#!/usr/bin/env bash
###############################################################################
# run-upstream.sh <real|shim> [candidate-list-file]
#
# Runs the DataStax/Apache Cassandra Java driver 3.12.1's OWN unit test suite
# (TestNG group "unit") under one of two classpaths:
#
#   real  -- the real cassandra-driver-{core,mapping,extras}-3.12.1 jars
#   shim  -- the 3.12.1->4.x compatibility shim jar (compat-3x), with NO real
#            com.datastax.driver.* on the classpath. The 3.x-compiled test
#            bytecode links against the shim via binary compatibility.
#
# For each candidate *Test class it runs:
#     org.testng.TestNG -testclass <fqcn> -groups unit
# and parses TestNG's "Total tests run: N, Failures: F, Skips: S" line.
#
# Output: <workdir>/out-<real|shim>/<fqcn>.log  (per class)
#         <workdir>/results-<real|shim>.tsv      (fqcn TAB total F S status verdict)
#
# Normally invoked by UpstreamSuiteIT; also runnable standalone.
#
# All paths are OVERRIDABLE via environment variables (defaults suit the dev
# sandbox). The script self-locates the module:
#   SHIM_JDK8   JDK 8 home
#   SHIM_M2     local Maven repository (default: ~/.m2/repository)
#   SHIM_CJD    3.12.1 driver source checkout (provides the 3.x test-classes
#               + the real/test transitive classpaths via dependency:build-classpath)
#   SHIM_JAR    built shim jar (default: <module>/target/cassandra-driver-shim.jar)
#   SHIM_WORKDIR     output/scratch dir (default: <module>/target/upstream-tests)
#   PER_CLASS_TIMEOUT  seconds per class (default 180)
###############################################################################
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$SCRIPT_DIR/../../.." && pwd)"           # compat-3x
REPO="$(cd "$MODULE/.." && pwd)"                        # cassandra-java-driver

JDK8="${SHIM_JDK8:-/usr/lib/jvm/jdk8u492-b09}"
M2="${SHIM_M2:-$HOME/.m2/repository}"
CJD="${SHIM_CJD:-/home/agent/projects/cjd-3.12.1}"
CASS="$M2/org/apache/cassandra"
SHIM_JAR="${SHIM_JAR:-$MODULE/target/cassandra-driver-shim.jar}"
# SHIM_WORKDIR is the base output dir (default target); upstream artifacts live in its
# upstream-tests/ subdir.
WORKDIR="${SHIM_WORKDIR:-$MODULE/target}/upstream-tests"
RESOURCES="$MODULE/src/test/resources/parity/upstream-tests"
PER_CLASS_TIMEOUT="${PER_CLASS_TIMEOUT:-180}"

MODE="${1:-}"
CAND="${2:-$RESOURCES/candidates.txt}"
[ "$MODE" = real ] || [ "$MODE" = shim ] || { echo "usage: $0 <real|shim> [candidate-list]"; exit 2; }
[ -f "$CAND" ] || { echo "candidate list not found: $CAND"; exit 2; }
mkdir -p "$WORKDIR"

TESTCLASSES="$CJD/driver-core/target/test-classes:$CJD/driver-mapping/target/test-classes:$CJD/driver-extras/target/test-classes"

# Refresh the maven-derived classpath fragments (cached in the workdir) if missing.
for m in driver-core driver-mapping driver-extras; do
  f="$WORKDIR/cptest_$m.txt"
  [ -f "$f" ] || (cd "$CJD" && JAVA_HOME=$JDK8 mvn -q -o -pl $m dependency:build-classpath \
      -Dmdep.outputFile="$f" -DincludeScope=test >/dev/null 2>&1)
done

if [ "$MODE" = real ]; then
  REAL_JARS="$CASS/cassandra-driver-core/3.12.1/cassandra-driver-core-3.12.1.jar:$CASS/cassandra-driver-mapping/3.12.1/cassandra-driver-mapping-3.12.1.jar:$CASS/cassandra-driver-extras/3.12.1/cassandra-driver-extras-3.12.1.jar"
  ALLDEPS=$(cat "$WORKDIR"/cptest_driver-core.txt "$WORKDIR"/cptest_driver-mapping.txt "$WORKDIR"/cptest_driver-extras.txt \
              | tr ':' '\n' | awk '!seen[$0]++' | grep -v '^$' | tr '\n' ':')
  CP="$TESTCLASSES:$REAL_JARS:$ALLDEPS"
else
  SHIM_DEPS_FILE="$WORKDIR/cpshim.txt"
  [ -f "$SHIM_DEPS_FILE" ] || JAVA_HOME=$JDK8 mvn -q -o -f "$MODULE/pom.xml" \
      dependency:build-classpath -Dmdep.outputFile="$SHIM_DEPS_FILE" -DincludeScope=runtime >/dev/null 2>&1
  SHIM_RT=$(cat "$SHIM_DEPS_FILE")
  [ -f "$SHIM_JAR" ] || { echo "shim jar not built: $SHIM_JAR"; exit 1; }
  # test-only extras from the 3.x test cp (drop any real cassandra-driver-*;
  # the shim + its runtime deps already provide guava/netty/metrics/etc).
  # Includes the same test-support libs the REAL classpath carried (log4j binding +
  # glassfish JSR-353 impl for the JSON codec test); these are test-runtime deps, not
  # shim code. Any real cassandra-driver-* is filtered out below.
  TESTONLY=$(cat "$WORKDIR"/cptest_driver-core.txt "$WORKDIR"/cptest_driver-mapping.txt "$WORKDIR"/cptest_driver-extras.txt \
              | tr ':' '\n' | awk '!seen[$0]++' \
              | grep -iE 'testng|jcommander|assertj|mockito|hamcrest|scassandra|cql-antlr|scala-library|scala-reflect|typesafe|/config/|netty|log4j|slf4j-log4j|glassfish|javax.json|commons-exec' \
              | grep -viE 'cassandra-driver' | tr '\n' ':')
  # test-classes + shim FIRST so the shim's com.datastax.driver.* always wins.
  CP="$TESTCLASSES:$SHIM_JAR:$SHIM_RT:$TESTONLY"
fi

# Guard: no real cassandra-driver-{core,mapping,extras} may appear on the shim cp.
if [ "$MODE" = shim ]; then
  if echo "$CP" | tr ':' '\n' | grep -qiE 'cassandra-driver-(core|mapping|extras)-3'; then
    echo "FATAL: real cassandra-driver jar leaked onto shim classpath"; exit 1
  fi
fi

OUT="$WORKDIR/out-$MODE"
RES="$WORKDIR/results-$MODE.tsv"
mkdir -p "$OUT"
: > "$RES"
echo "# mode=$MODE  cp-entries=$(echo "$CP" | tr ':' '\n' | grep -c .)  $(date)" >> "$RES"

while read -r fqcn; do
  [ -z "$fqcn" ] && continue
  case "$fqcn" in \#*) continue ;; esac
  log="$OUT/$fqcn.log"
  timeout "$PER_CLASS_TIMEOUT" "$JDK8/bin/java" -cp "$CP" \
      org.testng.TestNG -testclass "$fqcn" -groups unit > "$log" 2>&1
  rc=$?
  line=$(grep -E 'Total tests run:' "$log" | tail -1)
  if [ $rc -eq 124 ]; then
    printf '%s\t-\t-\t-\tTIMEOUT\n' "$fqcn" >> "$RES"
  elif [ -n "$line" ]; then
    tot=$(echo "$line" | sed -E 's/.*Total tests run: ([0-9]+).*/\1/')
    fail=$(echo "$line" | sed -E 's/.*Failures: ([0-9]+).*/\1/')
    skip=$(echo "$line" | sed -E 's/.*Skips: ([0-9]+).*/\1/')
    if [ "$fail" = 0 ] && [ "$tot" -gt 0 ] 2>/dev/null; then st=PASS
    elif [ "$tot" = 0 ]; then st=NOTESTS
    else st=FAIL; fi
    printf '%s\t%s\t%s\t%s\t%s\n' "$fqcn" "$tot" "$fail" "$skip" "$st" >> "$RES"
  else
    # No TestNG summary => class failed to load / init (NoClassDefFound etc.)
    reason=$(grep -oE '(NoClassDefFoundError|NoSuchMethodError|ClassNotFoundException|NoSuchFieldError|ExceptionInInitializerError|IncompatibleClassChangeError)[^ ]*' "$log" | head -1)
    printf '%s\t-\t-\t-\tLOADFAIL:%s\n' "$fqcn" "${reason:-unknown}" >> "$RES"
  fi
  printf '%-70s %s\n' "$fqcn" "$(tail -1 "$RES" | cut -f2-5 | tr '\t' ' ')"
done < "$CAND"

echo "== wrote $RES =="
# Annotate the raw tally with an explicit verdict column (PASS / EXCLUDED:<reason>) so the file is
# self-documenting: a FAIL/LOADFAIL on an EXCLUDED (internal-bound/etc.) class is expected evidence
# for its exclusion, not a shim defect.
SHIM_EXCLUSIONS="$RESOURCES/exclusions.txt" SHIM_RESULTS_DIR="$WORKDIR" "$SCRIPT_DIR/annotate.sh" || true
