#!/usr/bin/env bash
# cfg1_falsifier.sh — HG-321 (Ship CFG-1 / 0.30.163) integration falsifier,
# made STRUCTURAL in #192 (1.12.6 commit C0).
#
# CONTRACT (unchanged): under CACHE_ENABLED=false / unset / invalid the cache
# subsystem does not exist and NO cache expvar key is registered at
# /debug/vars; under CACHE_ENABLED=true every one of them is.
#
# WHAT CHANGED. Until #192 this script asserted a hand-maintained list of
# FIVE keys while 18 files (65 keys) were gated on cache.Disabled(). The
# matrix now lives in a Go test that DERIVES the gated key set from source
# (go/parser: every init() with `if Disabled() { return }` that publishes
# expvar keys), asserts the probe binary imports every gated package, fails
# on an empty or shrunken derivation (non-exercise floor), keeps the five
# legacy names as a subset assertion, and spawns one process per env value
# exactly as before (expvar has no Unpublish; init() reads the env once).
#
# This wrapper is kept so the entry point and the exit-code contract stay the
# same for CI and for the runbook:
#   ./cfg1_falsifier.sh        # exit 0 = contract holds for every derived key
#
# See e2e/bench/cfg1_probe/cfg1_structural_test.go for the arms.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." &>/dev/null && pwd)"

echo "==> CFG-1 structural falsifier (derived key set, 4-value CACHE_ENABLED matrix)"
cd "${REPO_ROOT}"
if go test -count=1 -v -run 'TestCFG1_Structural_' ./e2e/bench/cfg1_probe/; then
  echo "HG-321/#192 PASS: every derived cache key follows the CFG-1 contract"
  exit 0
fi
echo "HG-321/#192 FAIL: see the arm output above"
exit 1
