//go:build !(js && wasm)

package credhttp

import (
	"net"
	"net/http"
	"testing"
)

// The server build gains nothing from the browser build's seams (#1359): every
// plumbing client credhttp hands out is plumbing's own, and guardedTransport
// is dialguard's transport itself. scripts/check_js_wasm.sh runs this with
// main's TestBrowserBuild_ tests.
func TestBrowserBuild_PlumbingClientIsTheIdentityOnTheServerBuild(t *testing.T) {
	if browserFetchOptions != nil {
		t.Fatal("browserFetchOptions is set on a server build: guardedTransport would wrap dialguard's transport")
	}
	if _, ok := guardedTransport.(*http.Transport); !ok {
		t.Fatalf("guardedTransport is %T on a server build, want dialguard's *http.Transport itself", guardedTransport)
	}
	rt := http.RoundTripper(&http.Transport{DialContext: (&net.Dialer{}).DialContext})
	c := &http.Client{Transport: rt}
	if got := plumbingClient(c); got != c || c.Transport != rt {
		t.Fatalf("plumbingClient changed a client on a server build (transport %T)", c.Transport)
	}
}
