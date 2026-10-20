#!/usr/bin/env bash
# paired_ab.sh: the #765 PAIRED A/B driver (coordinator ruling, #765).
#
# On a host that cannot be made quiet, the absolute figures are meaningless,
# but two builds measured back to back, alternating, in the same window, can
# still be compared by RATIOS. This driver:
#   1. builds one tree per arm from git archives:
#        H0      = the harness SHA itself (main + harness), or with -b <sha>
#                  that SHA with the harness overlaid (main just before a fix);
#        <ARM>   = the arm's SHA (a fix branch on the same main) with the
#                  harness overlaid (scale765/ and dispatchers.go from the
#                  harness SHA). It refuses when the arm changed dispatchers.go
#                  since the common base, because the overlay would drop that change;
#   2. compiles TestScale765_Point once per tree;
#   3. runs PAIRS times: H0 then ARM on odd pairs, ARM then H0 on even pairs
#      (drift cancels), each run writing <arm>-<n>.json;
#   4. summarises with paired_summary.py: per-run granted CPU and ratios, a
#      pair compared only when the granted CPU of its two runs is within 15%.
#
# -s <n>: number the pairs from n (add pairs to an existing -o dir; the summary covers 1..n+pairs-1).
# -d: dry run (build trees and binaries, check the refusal, print the plan; no measurement).
# usage (from the repo root):
#   go/snowplow/internal/handlers/dispatchers/scale765/paired_ab.sh \
#     -h <harness-sha> [-b <h0-base-sha>] -a <arm-name>=<arm-sha> [-n pairs] [-m mode] [-r rate] -o <out-dir>
# env passed through: SCALE765_WARMUP, SCALE765_MEASURE, SCALE765_PLATFORM.
set -u
HARNESS="" H0BASE="" ARM="" PAIRS=3 MODE=annotation RATE=10 OUT="" DRY=0 START=1
while getopts "h:b:a:n:m:r:o:s:d" o; do
  case $o in
    h) HARNESS=$OPTARG ;; b) H0BASE=$OPTARG ;; a) ARM=$OPTARG ;; n) PAIRS=$OPTARG ;;
    m) MODE=$OPTARG ;; r) RATE=$OPTARG ;; o) OUT=$OPTARG ;; s) START=$OPTARG ;; d) DRY=1 ;;
    *) exit 2 ;;
  esac
done
[ -n "$HARNESS" ] && [ -n "$ARM" ] && [ -n "$OUT" ] || { echo "usage: -h <sha> -a <name>=<sha> -o <dir> [-n pairs] [-m mode] [-r rate]"; exit 2; }
ARM_NAME=${ARM%%=*}; ARM_SHA=${ARM#*=}
REPO=$(git rev-parse --show-toplevel) || exit 2
PKG=go/snowplow/internal/handlers/dispatchers/scale765
DISP=go/snowplow/internal/handlers/dispatchers/dispatchers.go
mkdir -p "$OUT" || exit 2
OUT=$(cd "$OUT" && pwd)

refuse_if_overlay_drops() { # <name> <sha>
  local base
  base=$(git -C "$REPO" merge-base "$HARNESS" "$2") || exit 2
  if ! git -C "$REPO" diff --quiet "$base" "$2" -- "$DISP"; then
    echo "refusing: $1 ($2) changed $DISP since $base; the harness overlay would drop it"; exit 3
  fi
}
refuse_if_overlay_drops "$ARM_NAME" "$ARM_SHA"
if [ -n "$H0BASE" ]; then refuse_if_overlay_drops H0 "$H0BASE"; fi

build() { # build <name> <tree-sha> [overlay-sha]
  local d=$OUT/tree-$1
  rm -rf "$d" && mkdir -p "$d"
  git -C "$REPO" archive "$2" | tar -x -C "$d" || exit 4
  if [ -n "${3:-}" ]; then
    rm -rf "$d/$PKG"
    git -C "$REPO" archive "$3" "$PKG" "$DISP" | tar -x -C "$d" || exit 4
  fi
  (cd "$d/go/snowplow" && go test -c -tags scale765 -o "$OUT/$1.test" ./internal/handlers/dispatchers/scale765/) || exit 5
  echo "built $1 from $2${3:+ + harness $3}"
}
if [ -n "$H0BASE" ]; then build H0 "$H0BASE" "$HARNESS"; else build H0 "$HARNESS"; fi
build "$ARM_NAME" "$ARM_SHA" "$HARNESS"

load() { python3 -I -c "import os; print(round(os.getloadavg()[0],2))"; }
run() { # run <name> <n>
  local label=$1-$2
  echo "run $label start $(date -u +%FT%TZ) load=$(load)"
  (cd "$OUT/tree-$1/$PKG" && SCALE765_OUT="$OUT" SCALE765_LABEL="$label" SCALE765_MODE="$MODE" \
     SCALE765_RATE="$RATE" SCALE765_MAX_LOAD="${SCALE765_MAX_LOAD:-1000}" \
     "$OUT/$1.test" -test.count=1 -test.run 'TestScale765_Point$' -test.timeout 2h -test.v > "$OUT/$label.log" 2>&1)
  local rc=$?
  echo "run $label end $(date -u +%FT%TZ) load=$(load) rc=$rc"
}
if [ "$DRY" = 1 ]; then # -d: build and check only; print the plan, measure nothing
  for i in $(seq "$START" $((START + PAIRS - 1))); do
    if [ $((i % 2)) -eq 1 ]; then echo "plan pair $i: H0 then $ARM_NAME"; else echo "plan pair $i: $ARM_NAME then H0"; fi
  done
  for b in H0 "$ARM_NAME"; do "$OUT/$b.test" -test.list 'TestScale765_Point' | grep -qx TestScale765_Point || { echo "$b.test lacks TestScale765_Point"; exit 6; }; done
  echo "dry run OK: both binaries built and list TestScale765_Point"; exit 0
fi
for i in $(seq "$START" $((START + PAIRS - 1))); do
  if [ $((i % 2)) -eq 1 ]; then run H0 "$i"; run "$ARM_NAME" "$i"; else run "$ARM_NAME" "$i"; run H0 "$i"; fi
done
python3 -I "$REPO/$PKG/paired_summary.py" "$OUT" H0 "$ARM_NAME" $((START + PAIRS - 1)) | tee "$OUT/summary.txt"
