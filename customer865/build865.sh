#!/bin/bash
# Build the #865 customer-arm binaries: the branch's harness copied byte-identically
# to the main tree (plus main's nil stats stub), one test binary per tree.
set -e
S=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad
D=$S/paired854
H=go/snowplow/internal/handlers/dispatchers/paired_865_test.go
cp "$S/wt-854/$H" "$S/wt-854main/$H"
cp "$D/p865_stats_main_stub.go.txt" "$S/wt-854main/go/snowplow/internal/handlers/dispatchers/paired_865_stats_test.go"
cmp "$S/wt-854/$H" "$S/wt-854main/$H" && echo HARNESS_IDENTICAL
(cd "$S/wt-854main/go/snowplow/internal/handlers/dispatchers" && go test -c -tags paired854 -o "$D/bin865-main" .)
(cd "$S/wt-854/go/snowplow/internal/handlers/dispatchers" && go test -c -tags paired854 -o "$D/bin865-branch" .)
echo BUILT
