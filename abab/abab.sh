#!/bin/bash
# #854 binding paired arm: ABAB (main, branch) x3 per event rate, 50K objects.
set -u
D=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/paired854
cd "$D"
mkdir -p runs
for rate in 1 10 50; do
  for round in 1 2 3; do
    for side in main branch; do
      tag="r${rate}-${round}-${side}"
      PAIRED854_N=50000 PAIRED854_RATE=$rate PAIRED854_SECONDS=30 \
        PAIRED854_OUT="runs/$tag.json" PAIRED854_PROF="runs/$tag.cpu.pb.gz" \
        "./bin-$side" -test.run TestPaired854 -test.count=1 -test.timeout 30m > "runs/$tag.log" 2>&1
      echo "$tag exit=$? $(date -u +%H:%M:%S)"
    done
  done
done
echo ABAB-DONE
