#!/usr/bin/env bash
# check_js_wasm.sh: the browser build (GOOS=js GOARCH=wasm, the Agentiko sandbox) gate.
#
#   1. the whole program compiles for js/wasm;
#   2. go vet passes for js/wasm on the packages that carry browser-only code;
#   3. the browser bridge's and the sandbox fake apiserver's tests run for js/wasm under Go's
#      own wasm runner (Node.js);
#   4. the egress tests run under both of net/http's js/wasm modes, the fetch-mode
#      ones against a real Node HTTP server through Node's real fetch;
#   5. the fake apiserver's field selectors equal kube-apiserver's (scripts/checkfieldlabels);
#   6. the server build contains none of the browser code (main_browser_build_test.go:
#      no internal/browserbridge, no internal/sandbox/..., no syscall/js, credhttp.BrowserEgress nil,
#      credhttp's plumbing clients plumbing's own).
#
# Run from go/snowplow. Needs node on PATH for steps 3 and 4.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== js/wasm: build"
GOOS=js GOARCH=wasm go build -o /dev/null .

echo "== js/wasm: vet"
GOOS=js GOARCH=wasm go vet . ./internal/browserbridge ./internal/sandbox/... ./internal/credhttp ./internal/testsupport/jsfetch

echo "== js/wasm: bridge and fake apiserver tests under Node"
command -v node >/dev/null || { echo "node is required to run the js/wasm tests" >&2; exit 1; }
GOOS=js GOARCH=wasm go test -count=1 -timeout 120s \
  -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./internal/browserbridge ./internal/sandbox/...

# The egress tests under both of net/http's js/wasm modes: plain `node` (argv0
# "node": Go's in-process network, where BrowserEgress and credhttp's plumbing clients must
# be the identity) and a renamed argv0 (net/http uses Node's fetch, as a
# browser's). In fetch mode no TestBrowserFetch_ test may skip: a skip there
# means the mode was not the one this script meant, and nothing was checked.
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
RUNNER="$(go env GOROOT)/lib/wasm/wasm_exec_node.js"
EGRESS_TESTS='^TestBrowser(Egress|Fetch)_'

run_node() { # mode binary
  SNOWPLOW_JS_MODE=node node --stack-size=8192 "$RUNNER" "$2" -test.run "$EGRESS_TESTS" -test.v -test.count=1 -test.timeout=120s >"$TMP/out" 2>&1 || { cat "$TMP/out"; return 1; }
  grep -E '^(--- |ok|PASS|FAIL)' "$TMP/out"
}
run_fetch() { # binary
  SNOWPLOW_JS_MODE=fetch bash -c 'exec -a snowplow-js-fetch node --stack-size=8192 "$0" "$@"' "$RUNNER" "$1" -test.run "$EGRESS_TESTS" -test.v -test.count=1 -test.timeout=120s >"$TMP/out" 2>&1 || { cat "$TMP/out"; return 1; }
  grep -E '^(--- |ok|PASS|FAIL)' "$TMP/out"
  if grep -E '^--- SKIP: TestBrowserFetch_' "$TMP/out"; then
    echo "a fetch-mode test skipped in fetch mode" >&2
    return 1
  fi
  grep -qE '^--- PASS: TestBrowserFetch_' "$TMP/out" || { echo "no fetch-mode test ran" >&2; return 1; }
}

for pkg in ./internal/credhttp ./internal/resolvers/restactions/api; do
  echo "== js/wasm: browser egress under both net/http modes: $pkg"
  GOOS=js GOARCH=wasm go test -c -o "$TMP/pkg.test" "$pkg"
  run_node node "$TMP/pkg.test"
  run_fetch "$TMP/pkg.test"
done

# The fake apiserver's field selectors (fieldlabels.json) against kube-apiserver's own source for
# the version the table names: every v1 AddFieldLabelConversionFunc, read with go/ast.
echo "== fake apiserver: field selectors equal kube-apiserver's"
K8S_VERSION="$(sed -n 's/^[[:space:]]*"upstream": "k8s.io\/kubernetes \(v[0-9.]*\)",$/\1/p' internal/sandbox/fakeapiserver/fieldlabels.json)"
[ -n "$K8S_VERSION" ] || { echo "fieldlabels.json names no upstream version" >&2; exit 1; }
K8S_DIR="$(go mod download -json "k8s.io/kubernetes@$K8S_VERSION" | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')"
[ -d "$K8S_DIR" ] || { echo "could not download k8s.io/kubernetes@$K8S_VERSION" >&2; exit 1; }
go run ./scripts/checkfieldlabels "$K8S_DIR" internal/sandbox/fakeapiserver/fieldlabels.json

echo "== server build: contains none of the browser code"
go test -count=1 -run 'TestBrowserBuild_' . ./internal/credhttp

echo "check_js_wasm: OK"
