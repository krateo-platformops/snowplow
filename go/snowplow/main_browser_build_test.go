//go:build !(js && wasm)

package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// The browser build (js/wasm, the Agentiko sandbox) adds code only in files
// constrained to js && wasm. These tests prove the server build contains none
// of it: neither the bridge package, the sandbox's fake apiserver
// (internal/sandbox/...) nor syscall/js, and the seams the server build does
// compile install nothing there: credhttp.BrowserEgress is nil here, and
// credhttp's own TestBrowserBuild_ test shows guardedTransport is
// dialguard's transport unchanged (egress_server_test.go).

const (
	browserBridgePkg = "github.com/krateo-platformops/snowplow/internal/browserbridge"
	fakeApiserverPkg = "github.com/krateo-platformops/snowplow/internal/sandbox/fakeapiserver"
	sandboxPkgPrefix = "github.com/krateo-platformops/snowplow/internal/sandbox/"
)

// goList runs `go list` for the module with GOOS/GOARCH set, so the answer is
// the named build's and not the test binary's.
func goList(t *testing.T, goos, goarch string, args ...string) []string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command on PATH")
	}
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list %v (GOOS=%s GOARCH=%s): %v\n%s", args, goos, goarch, err, ee.Stderr)
		}
		t.Fatalf("go list %v: %v", args, err)
	}
	return strings.Fields(string(out))
}

func TestBrowserBuild_ServerBuildContainsNoneOfIt(t *testing.T) {
	// The js/wasm build is the control: the same queries must find the
	// bridge there, or an empty answer below proves nothing.
	jsDeps := goList(t, "js", "wasm", "-deps", ".")
	for _, want := range []string{browserBridgePkg, fakeApiserverPkg, "syscall/js"} {
		if !contains(jsDeps, want) {
			t.Fatalf("the js/wasm build does not depend on %s: the control failed, so the server-build checks below would be vacuous", want)
		}
	}

	for _, target := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}} {
		deps := goList(t, target[0], target[1], "-deps", ".")
		if len(deps) < 100 {
			t.Fatalf("%s/%s: go list -deps returned %d packages; the query is broken", target[0], target[1], len(deps))
		}
		for _, banned := range []string{browserBridgePkg, "syscall/js"} {
			if contains(deps, banned) {
				t.Errorf("%s/%s: the server build depends on %s, which is browser-only", target[0], target[1], banned)
			}
		}
		for _, d := range deps {
			if strings.HasPrefix(d, sandboxPkgPrefix) {
				t.Errorf("%s/%s: the server build depends on %s: the sandbox's packages are browser-only", target[0], target[1], d)
			}
		}
	}
}

func TestBrowserBuild_EgressSeamIsNilOnTheServerBuild(t *testing.T) {
	if credhttp.BrowserEgress != nil {
		t.Fatal("credhttp.BrowserEgress is set on a server build: every hardened config and owned endpoint client would leave through it instead of dialling")
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
