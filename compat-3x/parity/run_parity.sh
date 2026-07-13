#!/usr/bin/env bash
# Differential parity runner: compile ParityMain once against the real 3.12.1 API,
# then run under (A) real 3.12.1 and (B) shim+4.x classpaths against the live C* node, and diff.
# Usage: run_parity.sh [real|shim|both]   (default both)
set -u
JDK8=/usr/lib/jvm/jdk8u492-b09
REPO=/home/agent/projects/cassandra-java-driver
DIR=/home/agent/projects/shim-work/parity
M2=$HOME/.m2/repository
HOST=127.0.0.1
mkdir -p "$DIR/classes"

REAL_CORE="$M2/org/apache/cassandra/cassandra-driver-core/3.12.1/cassandra-driver-core-3.12.1.jar"
REAL_MAP="$M2/org/apache/cassandra/cassandra-driver-mapping/3.12.1/cassandra-driver-mapping-3.12.1.jar"
REAL_EXT="$M2/org/apache/cassandra/cassandra-driver-extras/3.12.1/cassandra-driver-extras-3.12.1.jar"
[ -f /tmp/cp3.txt ] || (cd /home/agent/projects/cjd-3.12.1 && JAVA_HOME=$JDK8 mvn -q -pl driver-core dependency:build-classpath -Dmdep.outputFile=/tmp/cp3.txt -DincludeScope=runtime >/dev/null 2>&1)
REAL_CP="$(cat /tmp/cp3.txt):$REAL_CORE:$REAL_MAP:$REAL_EXT"

SHIM_JAR="$REPO/compat-3x/target/cassandra-driver-shim.jar"

compile() {
  echo ">> compiling ParityMain against real 3.12.1 API"
  "$JDK8/bin/javac" -encoding UTF-8 -cp "$REAL_CP" -d "$DIR/classes" "$DIR/ParityMain.java" || { echo "COMPILE FAILED"; exit 1; }
}
run_real() {
  echo ">> run REAL 3.12.1"
  "$JDK8/bin/java" -Dfile.encoding=UTF-8 -cp "$DIR/classes:$REAL_CP" ParityMain "$HOST" > "$DIR/parity_real.txt" 2>"$DIR/parity_real.err"
  echo "   -> $DIR/parity_real.txt ($(wc -l < "$DIR/parity_real.txt") lines)"
}
run_shim() {
  [ -f "$SHIM_JAR" ] || { echo "shim jar not built yet: $SHIM_JAR"; return 1; }
  JAVA_HOME=$JDK8 mvn -q -f "$REPO/compat-3x/pom.xml" dependency:build-classpath -Dmdep.outputFile=/tmp/cpshim.txt -DincludeScope=runtime >/dev/null 2>&1
  SHIM_CP="$(cat /tmp/cpshim.txt):$SHIM_JAR"
  echo ">> run SHIM+4.x"
  "$JDK8/bin/java" -Dfile.encoding=UTF-8 -cp "$DIR/classes:$SHIM_CP" ParityMain "$HOST" > "$DIR/parity_shim.txt" 2>"$DIR/parity_shim.err"
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
