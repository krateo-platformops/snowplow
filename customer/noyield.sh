#!/bin/bash
# Supplementary arm: the customer arm WITHOUT the refresher's customer yield
# (scratch variant of the committed harness: yield() returns at once). Builds
# bin-*-noyield, restores the committed harness, then runs rate 10 x 3 rounds.
set -eu
S=/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad
D=$S/paired854
H=go/snowplow/internal/resolvers/restactions/api/paired_854_test.go
cp "$S/wt-854/$H" "$D/paired_854_test.go.committed"
perl -0pe 's/(yield := func\(\) \{\n)/$1\t\treturn \/\/ NO-YIELD VARIANT\n/' "$D/paired_854_test.go.committed" > "$D/paired_854_test.go.noyield"
grep -c 'NO-YIELD VARIANT' "$D/paired_854_test.go.noyield"
for t in wt-854 wt-854main; do cp "$D/paired_854_test.go.noyield" "$S/$t/$H"; done
(cd "$S/wt-854main/go/snowplow/internal/resolvers/restactions/api" && go test -c -tags paired854 -o "$D/bin-main-noyield" .)
(cd "$S/wt-854/go/snowplow/internal/resolvers/restactions/api" && go test -c -tags paired854 -o "$D/bin-branch-noyield" .)
for t in wt-854 wt-854main; do cp "$D/paired_854_test.go.committed" "$S/$t/$H"; done
cmp "$D/paired_854_test.go.committed" "$S/wt-854/$H" && echo RESTORED
OUT=$D/noyield-v1
mkdir -p "$OUT"
for round in 1 2 3; do
  for leg in main branch main2; do
    bin=main; [ "$leg" = branch ] && bin=branch
    tag="r10-${round}-${leg}"
    GOMAXPROCS=4 PAIRED854_N=50000 PAIRED854_RATE=10 PAIRED854_SECONDS=30 \
      PAIRED854_OUT="$OUT/$tag.json" "$D/bin-$bin-noyield" -test.run TestPaired854 -test.count=1 -test.timeout 30m > "$OUT/$tag.log" 2>&1
    echo "$tag exit=$? $(date -u +%H:%M:%S)" >> "$OUT/runs.log"
  done
done
echo NOYIELD-DONE >> "$OUT/runs.log"
