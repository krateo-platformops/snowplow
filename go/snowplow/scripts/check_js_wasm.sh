#!/usr/bin/env bash
# check_js_wasm.sh: the browser build (GOOS=js GOARCH=wasm, the Agentiko sandbox) gate.
#
#   1. the whole program compiles for js/wasm;
#   2. go vet passes for js/wasm on the packages that carry browser-only code;
#   3. the browser bridge's tests run for js/wasm under Go's own wasm runner (Node.js);
#   4. the egress tests run under both of net/http's js/wasm modes, the fetch-mode
#      ones against a real Node HTTP server through Node's real fetch;
#   5. gosec, run as the browser build (GOOS=js GOARCH=wasm), finds nothing in the packages
#      whose browser build compiles other files than their server build;
#   6. the server build contains none of the browser code (main_browser_build_test.go:
#      no internal/browserbridge, no syscall/js, credhttp.BrowserEgress nil,
#      credhttp's guardedTransport dialguard's own).
#
# Run from go/snowplow. Needs node on PATH for steps 3 and 4.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== js/wasm: build"
GOOS=js GOARCH=wasm go build -o /dev/null .

echo "== js/wasm: vet"
GOOS=js GOARCH=wasm go vet . ./internal/browserbridge ./internal/credhttp ./internal/testsupport/jsfetch

echo "== js/wasm: bridge tests under Node"
command -v node >/dev/null || { echo "node is required to run the js/wasm tests" >&2; exit 1; }
GOOS=js GOARCH=wasm go test -count=1 -timeout 120s \
  -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./internal/browserbridge

# The egress tests under both of net/http's js/wasm modes: plain `node` (argv0
# "node": Go's in-process network, where BrowserEgress must
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

# gosec over the browser build: the org's security workflow runs gosec with the default GOOS, so it
# never sees a file only the browser build compiles. This runs it with GOOS=js GOARCH=wasm over every
# package whose browser build compiles other files than its server build, and fails on any finding.
# The scope is the go command's own file selection, so every way a file reaches the browser build
# counts: a *_js.go or *_wasm.go name with no build line, a //go:build line naming js or wasm, a
# negated one (!unix, !linux) the browser satisfies. A package that compiles the same files for both
# is the server build's code, which the org workflow scans; third-party dependencies are not in scope.
# The tool is pinned by version and its module checksum is checked against the one recorded here
# (sum.golang.org's, also verified by the go command through GOSUMDB).
GOSEC_VERSION=v2.30.0
GOSEC_SUM="h1:67aF6LHa7SAL6QEc4es857GrXTT50gUqmrl0XPhtDx0="
echo "== js/wasm: gosec $GOSEC_VERSION"
sum="$(go mod download -json "github.com/securego/gosec/v2@$GOSEC_VERSION" | sed -n 's/^[[:space:]]*"Sum": "\(.*\)",$/\1/p')"
[ "$sum" = "$GOSEC_SUM" ] || { echo "gosec $GOSEC_VERSION module checksum is $sum, want $GOSEC_SUM" >&2; exit 1; }
GOBIN="$TMP/bin" go install "github.com/securego/gosec/v2/cmd/gosec@$GOSEC_VERSION"
MODDIR="$(go list -m -f '{{.Dir}}')"
pkg_files() { # GOOS GOARCH: one line per package, its directory (relative) and the files it compiles
  GOOS="$1" GOARCH="$2" go list -e -f '{{.Dir}} {{.GoFiles}}' ./... | sed "s|^$MODDIR|.|" | LC_ALL=C sort
}
pkg_files js wasm >"$TMP/files.js"
pkg_files linux amd64 >"$TMP/files.linux"
LC_ALL=C comm -23 "$TMP/files.js" "$TMP/files.linux" >"$TMP/files.browser"
[ -s "$TMP/files.browser" ] || { echo "no package compiles other files for js/wasm than for linux/amd64: the gosec scope is empty" >&2; exit 1; }
echo "gosec scope: the packages whose js/wasm files differ from their linux/amd64 ones (js/wasm files shown):"
sed 's/^/  /' "$TMP/files.browser"
JS_PKGS="$(cut -d' ' -f1 "$TMP/files.browser")"
# shellcheck disable=SC2086
GOOS=js GOARCH=wasm "$TMP/bin/gosec" -fmt text $JS_PKGS

echo "== server build: contains none of the browser code"
go test -count=1 -run 'TestBrowserBuild_' . ./internal/credhttp

echo "check_js_wasm: OK"
