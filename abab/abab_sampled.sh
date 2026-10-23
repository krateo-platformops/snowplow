#!/bin/bash
# abab.sh with a machine-load sampler (total %CPU of OTHER processes + loadavg, every 10 s).
D=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/paired854
mkdir -p "$D/runs"
( while [ ! -f "$D/runs/.done" ]; do
    echo "$(date -u +%H:%M:%S) other%=$(ps -Ao pcpu,comm | grep -v 'bin-main\|bin-branch' | awk 'NR>1{s+=$1}END{printf "%d", s}') $(sysctl -n vm.loadavg)"
    sleep 10
  done ) > "$D/runs/load.log" &
bash "$D/abab.sh" > "$D/abab.log" 2>&1
touch "$D/runs/.done"
wait
