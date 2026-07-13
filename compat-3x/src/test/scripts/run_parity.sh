#!/usr/bin/env bash
###############################################################################
# run_parity.sh [real|shim|both]   (default both)
#
# Differential parity runner. Compiles ParityMain ONCE against the real 3.12.1
# public API, then runs it under (A) the real 3.12.1 driver and (B) the shim+4.x,
# both against the same live Cassandra node, and diffs the KEY=VALUE observations.
# Identical output => behavioral parity (and, implicitly, binary compatibility:
# 3.x-compiled bytecode linking against the shim at runtime).
#
# Normally invoked by DifferentialParityIT; also runnable standalone.
#
# All paths are OVERRIDABLE via environment variables (defaults suit the dev
# sandbox). The script self-locates the module so it is not tied to a checkout
# location:
#   SHIM_JDK8   JDK 8 home (javac/java that speak 1.8 bytecode)
#   SHIM_M2     local Maven repository (default: ~/.m2/repository)
#   SHIM_HOST   Cassandra contact point (default: 127.0.0.1)
#   SHIM_CJD    3.12.1 driver source checkout, used ONLY to resolve the real
#               driver's transitive runtime classpath via dependency:build-classpath
#   SHIM_JAR    built shim jar (default: <module>/target/cassandra-driver-shim.jar)
#   SHIM_PARITY_SRC  ParityMain.java (default: <module>/src/test/resources/parity/ParityMain.java)
#   SHIM_WORKDIR     output/scratch dir (default: <module>/target/parity)
###############################################################################
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$SCRIPT_DIR/../../.." && pwd)"           # compat-3x
REPO="$(cd "$MODULE/.." && pwd)"                        # cassandra-java-driver

JDK8="${SHIM_JDK8:-/usr/lib/jvm/jdk8u492-b09}"
M2="${SHIM_M2:-$HOME/.m2/repository}"
HOST="${SHIM_HOST:-127.0.0.1}"
CJD="${SHIM_CJD:-/home/agent/projects/cjd-3.12.1}"
SHIM_JAR="${SHIM_JAR:-$MODULE/target/cassandra-driver-shim.jar}"
PARITY_SRC="${SHIM_PARITY_SRC:-$MODULE/src/test/resources/parity/ParityMain.java}"
# SHIM_WORKDIR is the base output dir (default target); parity artifacts live in its parity/ subdir.
DIR="${SHIM_WORKDIR:-$MODULE/target}/parity"
mkdir -p "$DIR/classes"

REAL_CORE="$M2/org/apache/cassandra/cassandra-driver-core/3.12.1/cassandra-driver-core-3.12.1.jar"
REAL_MAP="$M2/org/apache/cassandra/cassandra-driver-mapping/3.12.1/cassandra-driver-mapping-3.12.1.jar"
REAL_EXT="$M2/org/apache/cassandra/cassandra-driver-extras/3.12.1/cassandra-driver-extras-3.12.1.jar"

# The real driver's transitive runtime deps are not standalone-resolvable from a
# bare ~/.m2, so we derive them from the 3.12.1 source checkout (SHIM_CJD). Cached.
REAL_DEPS_FILE="$DIR/cp-real-deps.txt"
if [ ! -f "$REAL_DEPS_FILE" ]; then
  ( cd "$CJD" && JAVA_HOME=$JDK8 mvn -q -o -pl driver-core dependency:build-classpath \
      -Dmdep.outputFile="$REAL_DEPS_FILE" -DincludeScope=runtime >/dev/null 2>&1 )
fi
[ -f "$REAL_DEPS_FILE" ] || { echo "FATAL: could not resolve real 3.12.1 runtime classpath (SHIM_CJD=$CJD)"; exit 3; }
REAL_CP="$(cat "$REAL_DEPS_FILE"):$REAL_CORE:$REAL_MAP:$REAL_EXT"

compile() {
  echo ">> compiling ParityMain against real 3.12.1 API"
  "$JDK8/bin/javac" -encoding UTF-8 -cp "$REAL_CP" -d "$DIR/classes" "$PARITY_SRC" \
    || { echo "COMPILE FAILED"; exit 1; }
}
run_real() {
  echo ">> run REAL 3.12.1"
  "$JDK8/bin/java" -Dfile.encoding=UTF-8 -cp "$DIR/classes:$REAL_CP" ParityMain "$HOST" \
      > "$DIR/parity_real.txt" 2>"$DIR/parity_real.err"
  echo "   -> $DIR/parity_real.txt ($(wc -l < "$DIR/parity_real.txt") lines)"
}
run_shim() {
  [ -f "$SHIM_JAR" ] || { echo "shim jar not built yet: $SHIM_JAR"; return 1; }
  SHIM_DEPS_FILE="$DIR/cp-shim-deps.txt"
  [ -f "$SHIM_DEPS_FILE" ] || JAVA_HOME=$JDK8 mvn -q -o -f "$MODULE/pom.xml" \
      dependency:build-classpath -Dmdep.outputFile="$SHIM_DEPS_FILE" -DincludeScope=runtime >/dev/null 2>&1
  SHIM_CP="$(cat "$SHIM_DEPS_FILE"):$SHIM_JAR"
  # Guard: no real cassandra-driver-{core,mapping,extras} may leak onto the shim cp.
  if echo "$SHIM_CP" | tr ':' '\n' | grep -qiE 'cassandra-driver-(core|mapping|extras)-3'; then
    echo "FATAL: real cassandra-driver jar leaked onto shim classpath"; exit 1
  fi
  echo ">> run SHIM+4.x"
  "$JDK8/bin/java" -Dfile.encoding=UTF-8 -cp "$DIR/classes:$SHIM_CP" ParityMain "$HOST" \
      > "$DIR/parity_shim.txt" 2>"$DIR/parity_shim.err"
  echo "   -> $DIR/parity_shim.txt ($(wc -l < "$DIR/parity_shim.txt") lines)"
}

compile
case "${1:-both}" in
  real) run_real ;;
  shim) run_shim ;;
  both) run_real; run_shim
        echo ">> DIFF (real vs shim) — empty means PARITY:"
        diff "$DIR/parity_real.txt" "$DIR/parity_shim.txt" && echo "PARITY: IDENTICAL" || echo "PARITY: DIFFERENCES ABOVE" ;;
esac
