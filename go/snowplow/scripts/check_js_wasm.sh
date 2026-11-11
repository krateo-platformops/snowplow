#!/usr/bin/env bash
# check_js_wasm.sh: the browser build (GOOS=js GOARCH=wasm, the Agentiko sandbox) gate.
#
#   1. the whole program compiles for js/wasm;
#   2. go vet passes for js/wasm on the packages that carry browser-only code;
#   3. the browser bridge's tests run for js/wasm under Go's own wasm runner (Node.js);
#   4. the server build contains none of the browser code (main_browser_build_test.go:
#      no internal/browserbridge, no syscall/js, credhttp.BrowserEgress nil).
#
# Run from go/snowplow. Needs node on PATH for step 3.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== js/wasm: build"
GOOS=js GOARCH=wasm go build -o /dev/null .

echo "== js/wasm: vet"
GOOS=js GOARCH=wasm go vet . ./internal/browserbridge ./internal/credhttp

echo "== js/wasm: bridge tests under Node"
command -v node >/dev/null || { echo "node is required to run the js/wasm tests" >&2; exit 1; }
GOOS=js GOARCH=wasm go test -count=1 -timeout 120s \
  -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./internal/browserbridge

echo "== server build: contains none of the browser code"
go test -count=1 -run 'TestBrowserBuild_' .

echo "check_js_wasm: OK"
