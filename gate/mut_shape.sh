#!/bin/bash
# #854 shape/pairing mutations: apply, run the named arms, revert, verify revert.
cd /private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/wt-854/go/snowplow || exit 1
F=internal/cache/list_cell.go
cp "$F" /private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/list_cell.go.orig

run() { # name, perl-substitution, go test args...
  name=$1; sub=$2; shift 2
  perl -0pi -e "$sub" "$F"
  if cmp -s "$F" /private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/list_cell.go.orig; then echo "$name: MUTATION NOT APPLIED"; return; fi
  out=$(go test -count=1 "$@" 2>&1)
  if echo "$out" | grep -q -E '^(FAIL|--- FAIL)'; then echo "$name: RED"; echo "$out" | grep -E 'RED|--- FAIL' | head -3; else echo "$name: SURVIVED"; fi
  cp /private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/list_cell.go.orig "$F"
}

run M24b 's/ \|\| !shape\.holds\(a\.Envelope, keyed\)//' -tags integration -run 'TestListCell854_F1r_' ./internal/resolvers/restactions/api/
run M25 's/len\(sh\.sep\) != 1 \|\|\n\t\t//' -run 'TestListCell854_SH' ./internal/cache/
run M26 's/(func \(sh envelopeShape\) holds\(env \[\]byte, keyed \[\]keyedRaw\) bool \{\n)/$1\treturn true\n/' -run 'TestListCell854_SH' ./internal/cache/
run M27 's/case !m && out == nil:/case !m \&\& out == nil \&\& false:/' -tags integration -run 'TestListCell854_F1_' ./internal/resolvers/restactions/api/
cmp "$F" /private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/list_cell.go.orig && echo REVERTED_OK
