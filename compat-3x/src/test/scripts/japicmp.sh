#!/usr/bin/env bash
# Run japicmp comparing the built shim jar against the real 3.12.1 reference jars.
# Usage: japicmp.sh [core|mapping|extras|all]   (default: all)
# Overridable via env: SHIM_JDK8, SHIM_M2, SHIM_JAR, SHIM_WORKDIR. Self-locates the module.
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$SCRIPT_DIR/../../.." && pwd)"           # compat-3x
JDK="${SHIM_JDK8:-/usr/lib/jvm/jdk8u492-b09}"
M2R="${SHIM_M2:-$HOME/.m2/repository}"
JAPICMP=$(ls "$M2R"/com/github/siom79/japicmp/japicmp/*/japicmp-*-jar-with-dependencies.jar 2>/dev/null | head -1)
M2="$M2R/org/apache/cassandra"
SHIM="${SHIM_JAR:-$MODULE/target/cassandra-driver-shim.jar}"
OUT="${SHIM_WORKDIR:-$MODULE/target/japicmp}"
mkdir -p "$OUT"

# Internal-but-public 3.x classes with no stable contract / no 4.x analogue (see 00-IMPL-PLAN.md).
EXCLUDES='com.datastax.driver.core.Native;com.datastax.driver.core.GuavaCompatibility;com.datastax.driver.core.MetricsUtil;com.datastax.driver.core.DefaultPreparedStatement;com.datastax.driver.core.FramingFormatHandler;com.datastax.driver.core.IgnoreJDK6Requirement'

run() {
  local mod="$1"
  local old="$M2/cassandra-driver-$mod/3.12.1/cassandra-driver-$mod-3.12.1.jar"
  [ -f "$old" ] || { echo "MISSING reference jar: $old"; return 1; }
  [ -f "$SHIM" ] || { echo "MISSING shim jar (build first): $SHIM"; return 1; }
  echo "===== japicmp: $mod (old=3.12.1  new=shim) ====="
  "$JDK/bin/java" -jar "$JAPICMP" \
    -o "$old" -n "$SHIM" \
    -a PROTECTED \
    --ignore-missing-classes \
    --only-incompatible \
    --exclude "$EXCLUDES" > "$OUT/$mod.txt" 2>"$OUT/$mod.err"
  # Tally incompatible findings by kind
  echo "  incompatible CLASS removals : $(grep -cE 'REMOVED CLASS' "$OUT/$mod.txt")"
  echo "  incompatible CLASS modified : $(grep -cE 'MODIFIED (CLASS|INTERFACE|SUPERCLASS)' "$OUT/$mod.txt")"
  echo "  incompatible METHOD issues  : $(grep -cE '(REMOVED|MODIFIED) (METHOD|CONSTRUCTOR)' "$OUT/$mod.txt")"
  echo "  incompatible FIELD issues   : $(grep -cE '(REMOVED|MODIFIED) FIELD' "$OUT/$mod.txt")"
  echo "  --- first findings ---"
  grep -E 'CLASS|METHOD|FIELD|CONSTRUCTOR|INTERFACE' "$OUT/$mod.txt" | grep -E '!|REMOVED|MODIFIED' | head -40
  echo "(full report: $OUT/$mod.txt ; stderr: $OUT/$mod.err)"
}

case "${1:-all}" in
  core) run core ;;
  mapping) run mapping ;;
  extras) run extras ;;
  all) run core; run mapping; run extras ;;
  *) echo "usage: $0 [core|mapping|extras|all]"; exit 2 ;;
esac
