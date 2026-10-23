#!/bin/bash
# Copy the branch's harness to the main tree (must be byte-identical), build both binaries.
set -e
S=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad
H=go/snowplow/internal/resolvers/restactions/api/paired_854_test.go
cp "$S/wt-854/$H" "$S/wt-854main/$H"
cmp "$S/wt-854/$H" "$S/wt-854main/$H" && echo HARNESS_IDENTICAL
cd "$S/wt-854main/go/snowplow/internal/resolvers/restactions/api" && go test -c -tags paired854 -o "$S/paired854/bin-main" .
cd "$S/wt-854/go/snowplow/internal/resolvers/restactions/api" && go test -c -tags paired854 -o "$S/paired854/bin-branch" .
shasum -a 256 "$S/$H" 2>/dev/null; shasum -a 256 "$S/wt-854/$H" | cut -c1-16
echo BUILT
