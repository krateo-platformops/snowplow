#!/bin/bash
# Every #854 arm x3 under -race with the CI tags, on the committed head.
S=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad
cd "$S/wt-854/go/snowplow" || exit 1
git rev-parse HEAD > $S/arms854.log
git status --short >> $S/arms854.log
go test -race -tags=unit,integration -count=3 -v -timeout 60m \
  -run 'Test\w*854\w*|TestIssue710_D2_17i_|TestContentFill710_FMEM_SeedChargeCoversAnAnchoringFill' \
  ./internal/cache/ ./internal/handlers/dispatchers/ ./internal/resolvers/restactions/api/ >> $S/arms854-raw.log 2>&1
echo "exit=$?" >> $S/arms854.log
grep -E '^(ok|FAIL)|^--- ' $S/arms854-raw.log | sort | uniq -c >> $S/arms854.log
grep -E 'anchoring P-seed|^\s+list_cell_854_f6_test.go:[0-9]+: F6:' $S/arms854-raw.log >> $S/arms854.log
echo DONE >> $S/arms854.log
