#!/bin/bash
# #865 customer arm on the REAL refresher. Per rate and round: main, branch,
# main again (the two main legs give the main/main spread). GOMAXPROCS=4 = the
# chart's CPU limit = the refresher's 4 workers. Background load is sampled.
# Usage: cust865.sh OUTDIR SECONDS ROUNDS "RATES" DEP_CUSTOMERS UNREL_CUSTOMERS THINK_MS
set -u
D=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/paired854
OUT=$D/$1; SECS=$2; ROUNDS=$3; RATES=$4; DEP=$5; UNREL=$6; THINK=$7
mkdir -p "$OUT"
rm -f "$OUT/.done"
( while [ ! -f "$OUT/.done" ]; do
    echo "$(date -u +%H:%M:%S) other%=$(ps -Ao pcpu,comm | grep -v 'bin865-' | awk 'NR>1{s+=$1}END{printf "%d", s}') $(sysctl -n vm.loadavg)"
    sleep 10
  done ) > "$OUT/load.log" &
for rate in $RATES; do
  for round in $(seq 1 "$ROUNDS"); do
    for leg in main branch main2; do
      bin=main; [ "$leg" = branch ] && bin=branch
      tag="r${rate}-${round}-${leg}"
      GOMAXPROCS=4 PAIRED865_N=50000 PAIRED865_RATE=$rate PAIRED865_SECONDS=$SECS \
        PAIRED865_CUST_DEP=$DEP PAIRED865_CUST_UNREL=$UNREL PAIRED865_THINK_MS=$THINK \
        PAIRED865_OUT="$OUT/$tag.json" PAIRED865_PROF="$OUT/$tag.cpu.pb.gz" \
        "$D/bin865-$bin" -test.run TestPaired865Customer -test.count=1 -test.timeout 30m > "$OUT/$tag.log" 2>&1
      echo "$tag exit=$? $(date -u +%H:%M:%S) warn=$(grep -c '"level":"WARN"' "$OUT/$tag.log")" >> "$OUT/runs.log"
      gzip -f "$OUT/$tag.log"
    done
  done
done
touch "$OUT/.done"
wait
echo CUST-DONE >> "$OUT/runs.log"
