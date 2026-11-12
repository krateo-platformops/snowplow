//go:build js && wasm

package api

// The stage fetch's two branches on the js/wasm build, in fetch mode: Node's
// real fetch to a real Node HTTP server (internal/testsupport/jsfetch).
// scripts/check_js_wasm.sh runs these with a renamed argv0, where net/http
// sends through fetch as it does in a browser.

import (
	"context"
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"

	"github.com/krateo-platformops/snowplow/internal/testsupport/jsfetch"
)

// stageGet sends a GET through the stage fetch's client for ep.
func stageGet(t *testing.T, ep *endpoints.Endpoint, url string) error {
	t.Helper()
	cli, err := httpClientForEndpoint(context.Background(), ep, &httpcall.RequestInfo{Verb: ptr.To(http.MethodGet)})
	if err != nil {
		t.Fatalf("httpClientForEndpoint: %v", err)
	}
	resp, err := cli.Get(url)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

// B2c (N5): the owned branch builds its transport with client-go, whose
// dialler is snowplow's (DialHolder). On js/wasm a transport with a dial
// function never uses fetch: it dials Go's in-process network, where nothing
// outside the program listens. BrowserEgress below the credential wrappers is
// what sends it through fetch instead, with the credential still on it.
func TestBrowserFetch_OwnedStageFetchLeavesThroughFetch(t *testing.T) {
	jsfetch.FetchOnly(t)
	if !endpointOwnedShape(&endpoints.Endpoint{Token: "t"}) {
		t.Fatal("PRE: a token endpoint with no CA is the owned branch's shape")
	}
	srv := jsfetch.Start(t)
	spy := jsfetch.Spy(t)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "owned-token"}

	if err := stageGet(t, ep, srv.URL+"/owned"); err != nil {
		t.Fatalf("the owned branch's request failed: %v; it did not leave through fetch", err)
	}
	if n := srv.Hits("/owned"); n != 1 {
		t.Fatalf("the server was asked %d times, want 1", n)
	}
	if got := srv.Header("/owned", "authorization"); got != "Bearer owned-token" {
		t.Errorf("the server saw Authorization %q: the credential wrappers above BrowserEgress did not run", got)
	}
	assertFetchOptions(t, spy, srv.URL+"/owned")

	if err := stageGet(t, ep, srv.URL+"/redirect"); err == nil {
		t.Fatal("the owned branch followed a redirect")
	}
	if n := srv.Hits("/target"); n != 0 {
		t.Fatalf("the redirect's target was asked %d times through the owned branch", n)
	}
}

// L4: the plumbing branch (here a password-only basic endpoint, which the
// owned branch does not take) is plumbing's client
// (credhttp.PlumbingEndpointClient), on which plumbing's dialguard.RoundTripper
// sets fetch's redirect and credentials modes.
func TestBrowserFetch_PlumbingStageFetchOmitsCredentials(t *testing.T) {
	jsfetch.FetchOnly(t)
	ep := &endpoints.Endpoint{Password: "p"}
	if endpointOwnedShape(ep) {
		t.Fatal("PRE: a password-only basic endpoint is the plumbing branch's shape")
	}
	srv := jsfetch.Start(t)
	spy := jsfetch.Spy(t)
	ep.ServerURL = srv.URL

	if err := stageGet(t, ep, srv.URL+"/plumbing"); err != nil {
		t.Fatalf("the plumbing branch's request failed: %v", err)
	}
	if got := srv.Header("/plumbing", "authorization"); got == "" {
		t.Error("the server saw no Authorization: the request did not carry plumbing's credential")
	}
	assertFetchOptions(t, spy, srv.URL+"/plumbing")
}

func assertFetchOptions(t *testing.T, spy *jsfetch.FetchSpy, url string) {
	t.Helper()
	calls := spy.CallsTo(url)
	if len(calls) == 0 {
		t.Fatalf("fetch was never called for %s", url)
	}
	for _, c := range calls {
		if c.Credentials != "omit" || c.Redirect != "error" {
			t.Errorf("fetch was called for %s with credentials %q and redirect %q, want omit and error", url, c.Credentials, c.Redirect)
		}
	}
}
