#!/usr/bin/env bash
# provenance-manifest.sh — classify every shim MAIN source file by provenance
# relative to the pristine 3.12.1 driver sources, and (optionally) regenerate
# PROVENANCE.md.
#
# The classifier is the single source of truth for the segregation work: the
# move step, the header-stamp step, the orphan baseline, and provenance-check.sh
# all consume the categories it emits. It scans BOTH source roots
# (src/main/java and src/main/java-driver-3x) so it is correct before AND after
# the files are moved — i.e. it is idempotent and safe to re-run for manifest
# regeneration.
#
# Categories (shim file S vs 3.12.1 counterpart R at the same package relpath):
#   VERBATIM      byte-identical to 3.12.1
#   NEAR-VERBATIM differs from 3.12.1 only in comments / whitespace
#   HYBRID        3.12.1 base with net-new shim code edits
#   NETNEW        no 3.12.1 counterpart, package com.datastax.driver.*
#   NETNEW-PKG    no 3.12.1 counterpart, package com.datastax.shim.*
#
# VERBATIM + NEAR-VERBATIM  => belong in src/main/java-driver-3x (no shim code)
# HYBRID + NETNEW + NETNEW-PKG => belong in src/main/java (contain shim code)
#
# Usage:
#   provenance-manifest.sh                 # print the TSV manifest to stdout
#   provenance-manifest.sh --list <CAT>    # print relpaths in one category
#   provenance-manifest.sh --ported        # relpaths that belong in java-driver-3x
#   provenance-manifest.sh --shimcode      # relpaths that belong in java
#   provenance-manifest.sh --md <file>     # write PROVENANCE.md
#
# Overridable via env: SHIM_MODULE (compat-3x dir), CJD (3.12.1 worktree).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_MODULE="${SHIM_MODULE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CJD="${CJD:-/home/agent/projects/cjd-3.12.1}"

ROOTS=("$SHIM_MODULE/src/main/java" "$SHIM_MODULE/src/main/java-driver-3x")
CJD_SRCS=(
  "$CJD/driver-core/src/main/java"
  "$CJD/driver-mapping/src/main/java"
  "$CJD/driver-extras/src/main/java"
)

[ -d "$CJD" ] || { echo "provenance-manifest: CJD not found: $CJD" >&2; exit 2; }

# Resolve a package relpath (com/datastax/...) to its pristine 3.12.1 file, or "".
real_for() {
  local rel="$1" base
  for base in "${CJD_SRCS[@]}"; do
    [ -f "$base/$rel" ] && { echo "$base/$rel"; return; }
  done
  echo ""
}

# Comment/whitespace-insensitive equality of two Java files.
# A proper string/char-literal-aware stripper (so "//" or "/*" inside a literal
# is never mistaken for a comment); then whitespace is normalized away. Applied
# identically to both files, so any real *code* difference survives.
same_modulo_comments() {
  python3 - "$1" "$2" <<'PY'
import sys

def strip(path):
    s = open(path, encoding="utf-8", errors="replace").read()
    out = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        # line comment
        if c == '/' and i + 1 < n and s[i+1] == '/':
            i += 2
            while i < n and s[i] != '\n':
                i += 1
            out.append(' '); continue
        # block comment
        if c == '/' and i + 1 < n and s[i+1] == '*':
            i += 2
            while i + 1 < n and not (s[i] == '*' and s[i+1] == '/'):
                i += 1
            i += 2
            out.append(' '); continue
        # string literal
        if c == '"':
            out.append(c); i += 1
            while i < n:
                out.append(s[i])
                if s[i] == '\\' and i + 1 < n:
                    out.append(s[i+1]); i += 2; continue
                if s[i] == '"':
                    i += 1; break
                i += 1
            continue
        # char literal
        if c == "'":
            out.append(c); i += 1
            while i < n:
                out.append(s[i])
                if s[i] == '\\' and i + 1 < n:
                    out.append(s[i+1]); i += 2; continue
                if s[i] == "'":
                    i += 1; break
                i += 1
            continue
        out.append(c); i += 1
    # normalize whitespace: collapse runs, drop blank lines
    return "\n".join(" ".join(t.split()) for t in "".join(out).splitlines() if t.split())

sys.exit(0 if strip(sys.argv[1]) == strip(sys.argv[2]) else 1)
PY
}

classify_one() {  # $1 = shim file path -> prints "CAT<TAB>added<TAB>removed<TAB>relpath"
  local sf="$1" root rel rf add del
  for root in "${ROOTS[@]}"; do
    case "$sf" in "$root"/*) rel="${sf#"$root"/}"; break;; esac
  done
  rf="$(real_for "$rel")"
  if [ -z "$rf" ]; then
    case "$rel" in
      com/datastax/shim/*) printf 'NETNEW-PKG\t0\t0\t%s\n' "$rel";;
      *)                   printf 'NETNEW\t0\t0\t%s\n' "$rel";;
    esac
    return
  fi
  if cmp -s "$sf" "$rf"; then
    printf 'VERBATIM\t0\t0\t%s\n' "$rel"; return
  fi
  add=$(diff "$rf" "$sf" | grep -c '^>' || true)
  del=$(diff "$rf" "$sf" | grep -c '^<' || true)
  if same_modulo_comments "$sf" "$rf"; then
    printf 'NEAR-VERBATIM\t%s\t%s\t%s\n' "$add" "$del" "$rel"
  else
    printf 'HYBRID\t%s\t%s\t%s\n' "$add" "$del" "$rel"
  fi
}

manifest() {  # emit the full TSV manifest, sorted by relpath
  local sf
  for root in "${ROOTS[@]}"; do
    [ -d "$root" ] || continue
    while IFS= read -r sf; do classify_one "$sf"; done \
      < <(find "$root" -name '*.java' | sort)
  done | sort -t$'\t' -k4
}

MODE="${1:-manifest}"
case "$MODE" in
  manifest) manifest ;;
  --list)   manifest | awk -F'\t' -v c="$2" '$1==c{print $4}' ;;
  --ported) manifest | awk -F'\t' '$1=="VERBATIM"||$1=="NEAR-VERBATIM"{print $4}' ;;
  --shimcode) manifest | awk -F'\t' '$1=="HYBRID"||$1=="NETNEW"||$1=="NETNEW-PKG"{print $4}' ;;
  --md)
    OUT="$2"
    M="$(manifest)"
    tally() { echo "$M" | awk -F'\t' -v c="$1" '$1==c{n++} END{print n+0}'; }
    lines_in() {  # sum of line counts of files in a category, from the live tree
      echo "$M" | awk -F'\t' -v c="$1" '$1==c{print $4}' | while IFS= read -r rel; do
        for root in "${ROOTS[@]}"; do [ -f "$root/$rel" ] && wc -l < "$root/$rel" && break; done
      done | awk '{s+=$1} END{print s+0}'
    }
    {
      echo "<!-- GENERATED by src/test/scripts/provenance-manifest.sh --md — do not edit by hand. -->"
      echo "# Provenance of \`compat-3x\` source"
      echo
      echo "Every main source file is classified against the pristine Cassandra Java driver"
      echo "3.12.1 sources (tag \`3.12.1\`, commit \`873e6f7\`). The split is by directory:"
      echo
      echo "| Location | Meaning |"
      echo "|---|---|"
      echo "| \`src/main/java-driver-3x/\` | 3.12.1 source carried forward — identical to 3.12.1 modulo comments. No shim code. No provenance header (byte-identity is the point). |"
      echo "| \`src/main/java/\` | Contains net-new shim code (hybrids + net-new classes + the \`com.datastax.shim.*\` bridge). Every file carries a \`// Shim provenance:\` header. |"
      echo
      echo "Package names are unchanged (\`com.datastax.driver.*\`) — the split is by"
      echo "source directory, because the ABI requires the original packages (japicmp 0/0/0)."
      echo
      echo "## Categories"
      echo
      echo "| Category | Files | Lines | Location |"
      echo "|---|---:|---:|---|"
      printf '| Verbatim (byte-identical to 3.12.1) | %s | %s | `java-driver-3x/` |\n' "$(tally VERBATIM)" "$(lines_in VERBATIM)"
      printf '| Near-verbatim (identical modulo comments) | %s | %s | `java-driver-3x/` |\n' "$(tally NEAR-VERBATIM)" "$(lines_in NEAR-VERBATIM)"
      printf '| Hybrid (3.12.1 base + shim facade edits) | %s | %s | `java/` |\n' "$(tally HYBRID)" "$(lines_in HYBRID)"
      printf '| Net-new (`com.datastax.driver.*`) | %s | %s | `java/` |\n' "$(tally NETNEW)" "$(lines_in NETNEW)"
      printf '| Net-new bridge (`com.datastax.shim.*`) | %s | %s | `java/` |\n' "$(tally NETNEW-PKG)" "$(lines_in NETNEW-PKG)"
      echo
      echo "## Line-level attribution for hybrid files"
      echo
      echo "The 3.12.1 sources are also committed pristine at the shim's own paths under the"
      echo "orphan tag \`shim-3x-vendor-3.12.1\`. To see which lines the shim added or"
      echo "changed on top of 3.12.1 (including inside the hybrid files):"
      echo
      echo '```'
      echo "src/test/scripts/provenance-diff.sh            # whole shim delta vs 3.12.1"
      echo "src/test/scripts/provenance-diff.sh <path>     # one file"
      echo '```'
      echo
      echo "This attribution is diff-based, not \`git blame\`-based: \`git blame\` on a hybrid"
      echo "points every line at the shim import commit, not at 3.12.1. Recovering true per-line"
      echo "blame back to 3.12.1 would require rewriting the (already-published) branch history,"
      echo "which is not done — the vendor-baseline diff is the supported substitute."
      echo
      echo "## Regenerating / checking"
      echo
      echo '```'
      echo "src/test/scripts/provenance-manifest.sh --md PROVENANCE.md   # regenerate this file"
      echo "src/test/scripts/provenance-check.sh                         # CI guard (see below)"
      echo '```'
      echo
      echo "\`provenance-check.sh\` enforces the invariant that keeps the split honest:"
      echo "a file in \`java/\` must carry a provenance header; a file in \`java-driver-3x/\`"
      echo "must carry none, and be identical to its 3.12.1 counterpart modulo comments."
      echo
      echo "## Full manifest"
      echo
      echo "| Category | +add | -del | File |"
      echo "|---|---:|---:|---|"
      echo "$M" | awk -F'\t' '{printf "| %s | %s | %s | `%s` |\n",$1,$2,$3,$4}'
    } > "$OUT"
    echo "wrote $OUT"
    ;;
  *) echo "unknown mode: $MODE" >&2; exit 2 ;;
esac
