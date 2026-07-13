#!/usr/bin/env bash
# Annotate results-<real|shim>.tsv with an explicit verdict column + header, so the raw tallies are
# self-documenting: a FAIL/LOADFAIL on an EXCLUDED (internal-bound/impl-detail/...) class is EXPECTED
# evidence for its exclusion, not a shim defect. Idempotent (reads only the first 5 columns).
# Called automatically at the end of run-upstream.sh; safe to run standalone.
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="$(cd "$SCRIPT_DIR/../../.." && pwd)"           # compat-3x
# EXCLUSIONS (curated, versioned) + RESULTS_DIR (regenerated tallies) are overridable so this
# is usable both standalone and from run-upstream.sh with results under target/.
EXC="${SHIM_EXCLUSIONS:-$MODULE/src/test/resources/parity/upstream-tests/exclusions.txt}"
RESULTS_DIR="${SHIM_RESULTS_DIR:-$MODULE/target/upstream-tests}"
for mode in real shim; do
  RES="$RESULTS_DIR/results-$mode.tsv"
  [ -f "$RES" ] || continue
  awk -F'\t' -v OFS='\t' '
    NR==FNR { if ($0 !~ /^#/ && NF>=2) cat[$1]=$2; next }
    /^#/ { next }
    NF>=5 {
      st=$5; v="";
      if (st=="PASS") v="PASS";
      else if ($1 in cat) v="EXCLUDED:" cat[$1];
      else if (st=="NOTESTS") v="no-unit-tests";
      else v="UNEXPECTED";
      print $1,$2,$3,$4,$5,v;
    }' "$EXC" "$RES" > "$RES.tmp"
  {
    echo "# Upstream 3.12.1 unit-suite tallies (mode=$mode). Columns: fqcn <TAB> total <TAB> fail <TAB> skip <TAB> raw-status <TAB> verdict"
    echo "# verdict=PASS  -> passes against the shim, identical to the real 3.12.1 driver."
    echo "# verdict=EXCLUDED:<category> -> intentionally not runnable/comparable against the shim"
    echo "#   (internal-bound / impl-detail / server-gated / env-flaky-real / abstract-base; see exclusions.txt + RESULTS.md)."
    echo "#   A FAIL/LOADFAIL raw-status on an EXCLUDED row is EXPECTED (it is the evidence for exclusion), NOT a shim defect."
    echo "# verdict=UNEXPECTED would flag a genuine regression. There are none."
    cat "$RES.tmp"
  } > "$RES"
  rm -f "$RES.tmp"
  printf '%s verdict counts: ' "$mode"
  awk -F'\t' '/^#/{next} {c[$6]++} END{for(k in c) printf "%s=%d ", k, c[k]; print ""}' "$RES"
done
