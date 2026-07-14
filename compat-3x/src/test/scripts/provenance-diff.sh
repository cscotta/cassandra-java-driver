#!/usr/bin/env bash
# provenance-diff.sh — show the exact line-level delta the shim adds on top of the
# pristine Cassandra Java driver 3.12.1 sources.
#
# The orphan tag `shim-3x-vendor-3.12.1` holds the pristine 3.12.1 sources at the
# shim's own final source paths (carried-forward -> src/main/java-driver-3x/,
# hybrids -> src/main/java/, net-new absent). Diffing the working tree against it
# yields precisely the shim's additions/changes — including the net-new lines
# woven into the hybrid files (e.g. Cluster.java), which no other view surfaces.
#
# Usage:
#   provenance-diff.sh                 # whole shim delta over 3.12.1 (main sources)
#   provenance-diff.sh <path...>       # restrict to one or more files/dirs
#   provenance-diff.sh --stat          # summary (files changed / +added / -deleted)
#   provenance-diff.sh --added         # list only the net-new files (no 3.12.1 origin)
#   provenance-diff.sh --hybrids       # delta for the hybrid files only (java/ dir)
#
# NOTE: this is DIFF-based attribution, not `git blame`-based. `git blame` on a
# hybrid points every line at the shim import commit, not at 3.12.1; recovering
# true per-line blame to 3.12.1 would require rewriting published history, which
# is deliberately not done. See PROVENANCE.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SHIM_MODULE="${SHIM_MODULE:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
TAG="${SHIM_VENDOR_TAG:-shim-3x-vendor-3.12.1}"
MAIN="$SHIM_MODULE/src/main"

git -C "$SHIM_MODULE" rev-parse -q --verify "refs/tags/$TAG" >/dev/null 2>&1 \
  || { echo "provenance-diff: tag '$TAG' not found. It is created by the segregation setup; see PROVENANCE.md." >&2; exit 2; }

mode="${1:-}"

# Resolve user-supplied paths to absolute so they work regardless of the caller's
# cwd (git runs with -C "$SHIM_MODULE", under which a bare relative path would
# otherwise be misinterpreted).
abspaths() {
  local p
  for p in "$@"; do
    case "$p" in
      /*) printf '%s\n' "$p" ;;
      *)  if [ -e "$p" ]; then printf '%s\n' "$(cd "$(dirname "$p")" && pwd)/$(basename "$p")"
          else
            # Not found relative to cwd — fall back to a module-relative path, but warn: a typo'd
            # path would otherwise produce an empty diff and a misleading exit 0.
            [ -e "$SHIM_MODULE/$p" ] || echo "provenance-diff: warning: path not found: $p (trying $SHIM_MODULE/$p)" >&2
            printf '%s\n' "$SHIM_MODULE/$p"
          fi ;;
    esac
  done
}

case "$mode" in
  --stat)    shift
             if [ "$#" -gt 0 ]; then mapfile -t P < <(abspaths "$@"); else P=("$MAIN"); fi
             exec git -C "$SHIM_MODULE" diff --stat --find-renames "$TAG" -- "${P[@]}" ;;
  --added)   git -C "$SHIM_MODULE" diff --diff-filter=A --name-only "$TAG" -- "$MAIN"; exit 0 ;;
  --hybrids) exec git -C "$SHIM_MODULE" diff --find-renames "$TAG" -- "$MAIN/java" ;;
  "")        exec git -C "$SHIM_MODULE" diff --find-renames "$TAG" -- "$MAIN" ;;
  *)         mapfile -t P < <(abspaths "$@")
             exec git -C "$SHIM_MODULE" diff --find-renames "$TAG" -- "${P[@]}" ;;
esac
