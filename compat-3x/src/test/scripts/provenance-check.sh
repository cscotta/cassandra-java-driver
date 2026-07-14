#!/usr/bin/env bash
# provenance-check.sh — CI guard that keeps the ported/shim-code split honest.
#
# Enforces the invariant that makes provenance easy to differentiate:
#
#   1. Every file under src/main/java/            MUST carry a "// Shim provenance:" header.
#      (These are the files that contain net-new shim code: hybrids + net-new + bridge.)
#   2. Every file under src/main/java-driver-3x/  MUST NOT carry that header, AND MUST be
#      identical to its pristine 3.12.1 counterpart modulo comments/whitespace.
#      (These are carried-forward 3.12.1 sources with no shim code.)
#   3. The classifier and the on-disk layout must agree: nothing classified as carried-forward
#      may sit in java/, and nothing classified as shim-code may sit in java-driver-3x/.
#
# Exit 0 = clean; exit 1 = one or more violations (printed). Read-only.
#
# Overridable via env: SHIM_MODULE (compat-3x dir), CJD (3.12.1 worktree).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_MODULE="${SHIM_MODULE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CJD="${CJD:-/home/agent/projects/cjd-3.12.1}"
MANIFEST="$SCRIPT_DIR/provenance-manifest.sh"

[ -d "$CJD" ] || { echo "provenance-check: CJD not found: $CJD (cannot verify carried-forward identity)" >&2; exit 2; }

# --- Fail closed: the guard must never report OK by silently checking nothing. Assert both
#     source roots exist and are non-trivially populated before we trust a green result. The
#     floors are deliberately loose (a refactor may legitimately shift the exact counts) — they
#     only catch a misresolved SHIM_MODULE or a whole root gone missing, which would otherwise
#     turn every per-file check into a vacuous pass. ---
JAVA_DIR="$SHIM_MODULE/src/main/java"
PORTED_DIR="$SHIM_MODULE/src/main/java-driver-3x"
MARKER="// Shim provenance:"
FLOOR_JAVA="${FLOOR_JAVA:-50}"      # 93 shim-code files today
FLOOR_PORTED="${FLOOR_PORTED:-150}" # 239 carried-forward files today

preflight() {
  local d="$1" floor="$2" label="$3" n
  [ -d "$d" ] || { echo "provenance-check: FATAL — $label root missing: $d" >&2; exit 2; }
  n=$(find "$d" -name '*.java' | wc -l)
  [ "$n" -ge "$floor" ] || { echo "provenance-check: FATAL — $label root has only $n .java files (expected >= $floor); refusing to pass vacuously: $d" >&2; exit 2; }
}
preflight "$JAVA_DIR"   "$FLOOR_JAVA"   "java"
preflight "$PORTED_DIR" "$FLOOR_PORTED" "java-driver-3x"

violations=0
note() { echo "  ✗ $*"; violations=$((violations+1)); }

# Header present anywhere in the file. The marker string is shim-invented and never appears in
# pristine 3.12.1 source, so a whole-file scan has no false positives and — unlike a preamble-only
# scan — also catches a header mistakenly placed BELOW the package declaration (both directions of
# the java/ <-> java-driver-3x/ invariant).
has_header() {  # $1 = file
  grep -qF "$MARKER" "$1"
}

real_for() {  # $1 = relpath -> pristine 3.12.1 path or ""
  local rel="$1" b
  for b in driver-core driver-mapping driver-extras; do
    [ -f "$CJD/$b/src/main/java/$rel" ] && { echo "$CJD/$b/src/main/java/$rel"; return; }
  done
  echo ""
}

# comment/whitespace-insensitive equality (string/char-literal aware) — same logic as the manifest
same_modulo_comments() {
  python3 - "$1" "$2" <<'PY'
import sys
def strip(path):
    s = open(path, encoding="utf-8", errors="replace").read()
    out=[]; i=0; n=len(s)
    while i<n:
        c=s[i]
        if c=='/' and i+1<n and s[i+1]=='/':
            i+=2
            while i<n and s[i]!='\n': i+=1
            out.append(' '); continue
        if c=='/' and i+1<n and s[i+1]=='*':
            i+=2
            while i+1<n and not(s[i]=='*' and s[i+1]=='/'): i+=1
            i+=2; out.append(' '); continue
        if c=='"':
            out.append(c); i+=1
            while i<n:
                out.append(s[i])
                if s[i]=='\\' and i+1<n: out.append(s[i+1]); i+=2; continue
                if s[i]=='"': i+=1; break
                i+=1
            continue
        if c=="'":
            out.append(c); i+=1
            while i<n:
                out.append(s[i])
                if s[i]=='\\' and i+1<n: out.append(s[i+1]); i+=2; continue
                if s[i]=="'": i+=1; break
                i+=1
            continue
        out.append(c); i+=1
    return "\n".join(" ".join(t.split()) for t in "".join(out).splitlines() if t.split())
sys.exit(0 if strip(sys.argv[1])==strip(sys.argv[2]) else 1)
PY
}

echo "provenance-check: java/ files must carry a header ..."
while IFS= read -r f; do
  has_header "$f" || note "missing provenance header: ${f#"$SHIM_MODULE"/}"
done < <(find "$JAVA_DIR" -name '*.java')

echo "provenance-check: java-driver-3x/ files must have NO header and be 3.12.1 modulo comments ..."
while IFS= read -r f; do
  rel="${f#"$PORTED_DIR"/}"
  if has_header "$f"; then note "carried-forward file has a shim header (should not): $rel"; fi
  rf="$(real_for "$rel")"
  if [ -z "$rf" ]; then
    note "carried-forward file has NO 3.12.1 counterpart (is it really ported?): $rel"
  elif ! same_modulo_comments "$f" "$rf"; then
    note "carried-forward file differs from 3.12.1 in CODE (not just comments): $rel"
  fi
done < <(find "$PORTED_DIR" -name '*.java')

echo "provenance-check: classifier ↔ layout agreement ..."
# The manifest is a subprocess in a process substitution, where `set -e` cannot see its exit
# status. Materialize it first and check rc explicitly, so a classifier crash fails the guard
# instead of silently disabling this whole invariant.
MAN_OUT="$(mktemp)"; trap 'rm -f "$MAN_OUT"' EXIT
if ! "$MANIFEST" > "$MAN_OUT"; then
  echo "provenance-check: FATAL — classifier ($MANIFEST) failed; cannot verify layout agreement." >&2
  exit 2
fi
# carried-forward category must live in java-driver-3x; shim-code category must live in java
while IFS=$'\t' read -r catg _a _d rel; do
  case "$catg" in
    VERBATIM|NEAR-VERBATIM)
      [ -f "$PORTED_DIR/$rel" ] || note "classified $catg but not in java-driver-3x/: $rel" ;;
    HYBRID|NETNEW|NETNEW-PKG)
      [ -f "$JAVA_DIR/$rel" ] || note "classified $catg but not in java/: $rel" ;;
  esac
done < "$MAN_OUT"

echo
if [ "$violations" -eq 0 ]; then
  echo "provenance-check: OK — segregation invariant holds."
else
  echo "provenance-check: FAILED with $violations violation(s)."
  exit 1
fi
