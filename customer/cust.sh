#!/bin/bash
# #854 customer arm. Per rate and round: main, branch, main again (the two main
# runs give the main/main spread). GOMAXPROCS=4 = the chart's CPU limit (Go sets
# GOMAXPROCS from it) = the refresher's 4 workers. Background load is sampled.
# Usage: cust.sh OUTDIR SECONDS ROUNDS "RATES"
set -u
D=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/paired854
OUT=$D/$1; SECS=$2; ROUNDS=$3; RATES=$4
mkdir -p "$OUT"
rm -f "$OUT/.done"
( while [ ! -f "$OUT/.done" ]; do
    echo "$(date -u +%H:%M:%S) other%=$(ps -Ao pcpu,comm | grep -v 'bin-main\|bin-branch' | awk 'NR>1{s+=$1}END{printf "%d", s}') $(sysctl -n vm.loadavg)"
    sleep 10
  done ) > "$OUT/load.log" &
for rate in $RATES; do
  for round in $(seq 1 "$ROUNDS"); do
    for leg in main branch main2; do
      bin=main; [ "$leg" = branch ] && bin=branch
      tag="r${rate}-${round}-${leg}"
      GOMAXPROCS=4 PAIRED854_N=50000 PAIRED854_RATE=$rate PAIRED854_SECONDS=$SECS \
        PAIRED854_OUT="$OUT/$tag.json" PAIRED854_PROF="$OUT/$tag.cpu.pb.gz" \
        "$D/bin-$bin" -test.run TestPaired854 -test.count=1 -test.timeout 30m > "$OUT/$tag.log" 2>&1
      echo "$tag exit=$? $(date -u +%H:%M:%S)" >> "$OUT/runs.log"
    done
  done
done
touch "$OUT/.done"
wait
echo CUST-DONE >> "$OUT/runs.log"
