#!/bin/bash
# #854 gate: whole module plain, then the CI invocation itself (whole module,
# -race, unit+integration tags, -p 1), then every scripts/check_*.sh.
S=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad
cd "$S/wt-854/go/snowplow" || exit 1
rm -f $S/g854-*.log
git rev-parse HEAD > $S/g854-head.log
git status --short >> $S/g854-head.log
go test ./... -count=1 > $S/g854-all.log 2>&1; echo "exit=$?" >> $S/g854-all.log
go test -race -tags=unit,integration -p 1 -count=1 -timeout 120m ./... > $S/g854-race-ci.log 2>&1; echo "exit=$?" >> $S/g854-race-ci.log
for c in scripts/check_*.sh; do bash $c > $S/g854-$(basename $c).log 2>&1; echo "$c exit=$?" >> $S/g854-checks.log; done
echo DONE >> $S/g854-checks.log
