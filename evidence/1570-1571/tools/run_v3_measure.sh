#!/usr/bin/env bash
# #1581 v3 re-measure, VM-exclusive window (2026-11-23): #865 split at 10 and 50 events/s x3, then freshness x6.
S=$SCRATCH
cd "$S" || exit 1
echo "START $(date -u +%FT%TZ)"
python3 -I "$S/rb_paired.py" "$S/arms-v3-865.json" "$S/v3-865-r10" TestPaired865Customer 3 PAIRED865_RATE=10 PAIRED865_PROBE=2 PAIRED865_HITBENCH=2000
echo "PHASE r10 done rc=$? $(date -u +%FT%TZ)"
python3 -I "$S/rb_paired.py" "$S/arms-v3-865.json" "$S/v3-865-r50" TestPaired865Customer 3 PAIRED865_RATE=50 PAIRED865_PROBE=2 PAIRED865_HITBENCH=2000
echo "PHASE r50 done rc=$? $(date -u +%FT%TZ)"
python3 -I "$S/rb_paired.py" "$S/arms-v3-fresh.json" "$S/v3-fresh" TestPairedFresh1570 6
echo "PHASE fresh done rc=$? $(date -u +%FT%TZ)"
echo ALL-DONE
