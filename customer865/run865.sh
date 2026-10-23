#!/bin/bash
# #865 binding customer arms on the real refresher, 5 rounds of (main, branch, main2):
#   A  normal:    1/10/50 ev/s, 2 customers on dependents + 4 on an unrelated RA, 50 ms think
#   B  sustained: 10/50 ev/s,   8 + 4 customers, 0 ms think (a request always in flight:
#                 the refresher's yield runs to its 5 s cap)
D=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/paired854
bash "$D/cust865.sh" cust865-A 30 5 "1 10 50" 2 4 50
bash "$D/cust865.sh" cust865-B 30 5 "10 50" 8 4 0
echo RUN865-DONE > "$D/run865.done"
