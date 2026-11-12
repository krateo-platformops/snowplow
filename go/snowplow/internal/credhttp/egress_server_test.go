//go:build !(js && wasm)

package credhttp

import (
	"net/http"
	"testing"
)

// The server build gains nothing from the browser build (#1359):
// guardedTransport is dialguard's transport itself, dialguard.RoundTripper
// being the identity there. scripts/check_js_wasm.sh runs this with main's
// TestBrowserBuild_ tests.
func TestBrowserBuild_GuardedTransportIsDialguardsOnTheServerBuild(t *testing.T) {
	if _, ok := guardedTransport.(*http.Transport); !ok {
		t.Fatalf("guardedTransport is %T on a server build, want dialguard's *http.Transport itself", guardedTransport)
	}
}
