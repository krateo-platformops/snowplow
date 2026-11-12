//go:build js && wasm

package credhttp

// scripts/check_js_wasm.sh runs these under both of net/http's js/wasm modes:
// plain `node` (argv0 "node": Go's in-process network, SNOWPLOW_JS_MODE=node)
// and a renamed argv0 (Node's fetch, as in a browser, SNOWPLOW_JS_MODE=fetch).
// In fetch mode every request goes through Node's real fetch to a real Node
// HTTP server (internal/testsupport/jsfetch): Go's in-process network,
// where a Go test server would listen, is exactly what fetch never reaches.

import (
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/testsupport/jsfetch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

type egressRecordingRT struct{ got *http.Request }

func (r *egressRecordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r.got = req
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

// usesFetch must be net/http's own choice, in both directions: if it said
// fetch under Node, every hardened request would carry option headers Node's
// net/http rejects; if it said no fetch in a browser, BrowserEgress would leave
// client-go's dialling transport in place, and every request would dial an
// in-process network where nothing listens. A future Go that changes net/http's
// condition fails this, whatever the script believes the mode is.
func TestBrowserEgress_UsesFetchIsNetHTTPsChoice(t *testing.T) {
	observed := jsfetch.NetHTTPUsesFetch(t)
	if usesFetch != observed {
		t.Fatalf("usesFetch = %v, but net/http uses fetch = %v: it no longer mirrors net/http's choice (roundtrip_js.go)", usesFetch, observed)
	}
	// The harness check: the mode the script meant to run is the one net/http
	// is in, so the two runs cover both modes rather than one twice.
	if mode := os.Getenv("SNOWPLOW_JS_MODE"); mode != "" && observed != (mode == "fetch") {
		t.Fatalf("scripts/check_js_wasm.sh ran this as %s, but net/http uses fetch = %v", mode, observed)
	}
}

func TestBrowserEgress_IdentityWhereFetchIsNotUsed(t *testing.T) {
	if usesFetch {
		t.Skip("fetch is used here; the TestBrowserFetch_ tests cover it")
	}
	rec := &egressRecordingRT{}
	if got := BrowserEgress(rec); got != http.RoundTripper(rec) {
		t.Fatalf("BrowserEgress replaced the transport where net/http dials (%T)", got)
	}

	// And a hardened chain still reaches a server on the in-process network.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	rt := transport.Wrappers(BrowserEgress)(&http.Transport{DialContext: (&net.Dialer{}).DialContext})
	req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("a request through BrowserEgress failed where net/http dials: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "ok" {
		t.Fatalf("body %q", b)
	}
}

// B2a: every client built here that names no transport of its own gets
// guardedTransport (authn's, the RUM forwarder's, ...). fetch follows a
// redirect itself, where neither CheckRedirect nor RefuseRedirectHops sees the
// hop, so a real 302 must fail the request inside fetch and its target must
// never be asked.
func TestBrowserFetch_GuardedTransportRefusesARealRedirect(t *testing.T) {
	jsfetch.FetchOnly(t)
	srv := jsfetch.Start(t)

	resp, err := Client(&http.Client{}).Get(srv.URL + "/redirect")
	if err == nil {
		resp.Body.Close()
		t.Fatalf("a client on guardedTransport followed a redirect: status %d", resp.StatusCode)
	}
	if srv.Hits("/redirect") != 1 {
		t.Fatalf("the redirecting server was asked %d times, want 1: the request never reached it (%v)", srv.Hits("/redirect"), err)
	}
	if n := srv.Hits("/target"); n != 0 {
		t.Fatalf("the redirect's target was asked %d times: fetch followed the redirect", n)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/redirect", nil)
	if resp, err := guardedTransport.RoundTrip(req); err == nil {
		resp.Body.Close()
		t.Fatalf("guardedTransport followed a redirect: status %d", resp.StatusCode)
	}
	if n := srv.Hits("/target"); n != 0 {
		t.Fatalf("the redirect's target was asked %d times through guardedTransport", n)
	}
}

// B2c (N4): BrowserEgress is installed BELOW the wrappers rc already carries.
// Installed above them, it would replace them along with the dial: the request
// would leave without what they add.
func TestBrowserFetch_HardenKeepsTheWrappersRcCarried(t *testing.T) {
	jsfetch.FetchOnly(t)
	srv := jsfetch.Start(t)

	rc := &rest.Config{Host: srv.URL, WrapTransport: func(rt http.RoundTripper) http.RoundTripper {
		return headerRT{name: "X-Rc-Wrapper", value: "kept", next: rt}
	}}
	hc, err := rest.HTTPClientFor(Harden(rc))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Get(srv.URL + "/echo")
	if err != nil {
		t.Fatalf("a hardened config's request failed through fetch: %v", err)
	}
	resp.Body.Close()
	if got := srv.Header("/echo", "x-rc-wrapper"); got != "kept" {
		t.Fatalf("the server saw X-Rc-Wrapper %q, want kept: Harden dropped a wrapper the config carried", got)
	}

	// And the hardened client refuses a redirect inside fetch too.
	if resp, err := hc.Get(srv.URL + "/redirect"); err == nil {
		resp.Body.Close()
		t.Fatalf("a hardened client followed a redirect: status %d", resp.StatusCode)
	}
	if n := srv.Hits("/target"); n != 0 {
		t.Fatalf("the redirect's target was asked %d times through a hardened client", n)
	}
}

// L4: every way a request reaches fetch sets credentials "omit" (and redirect
// "error"): guardedTransport, a hardened config (BrowserEgress), the /call
// passthrough (Do, plumbing's own client), the JWKS key
// source main builds (JWKSKeySource) and the stage fetch's plumbing branch
// (PlumbingEndpointClient). The fetch spy records the options fetch was
// actually called with.
func TestBrowserFetch_EveryPathOmitsCredentials(t *testing.T) {
	jsfetch.FetchOnly(t)
	srv := jsfetch.Start(t)
	spy := jsfetch.Spy(t)

	paths := map[string]func(){
		"/guarded": func() {
			if resp, err := Client(&http.Client{}).Get(srv.URL + "/guarded"); err == nil {
				resp.Body.Close()
			}
		},
		"/hardened": func() {
			hc, err := rest.HTTPClientFor(Harden(&rest.Config{Host: srv.URL}))
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := hc.Get(srv.URL + "/hardened"); err == nil {
				resp.Body.Close()
			}
		},
		"/call": func() {
			Do(t.Context(), request.RequestOptions{
				RequestInfo: request.RequestInfo{Path: "/call", Verb: ptr.To(http.MethodGet)},
				Endpoint:    &endpoints.Endpoint{ServerURL: srv.URL, Token: "t"},
			})
		},
		"/jwks.json": func() {
			// The key source main builds; the server's answer is no key set,
			// so the lookup fails after the fetch, which is all this needs.
			_, _ = JWKSKeySource(srv.URL+"/jwks.json", 5*time.Second).PublicKey("")
		},
		"/stage-plumbing": func() {
			cli, err := PlumbingEndpointClient(&endpoints.Endpoint{ServerURL: srv.URL, Password: "p"}, &request.RequestInfo{})
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := cli.Get(srv.URL + "/stage-plumbing"); err == nil {
				resp.Body.Close()
			}
		},
	}
	for path, send := range paths {
		send()
		calls := spy.CallsTo(srv.URL + path)
		if len(calls) == 0 {
			t.Errorf("%s: fetch was never called: the path does not leave through fetch", path)
			continue
		}
		for _, c := range calls {
			if c.Credentials != "omit" {
				t.Errorf("%s: fetch was called with credentials %q, want omit", path, c.Credentials)
			}
			if c.Redirect != "error" {
				t.Errorf("%s: fetch was called with redirect %q, want error", path, c.Redirect)
			}
		}
	}
}

type headerRT struct {
	name, value string
	next        http.RoundTripper
}

func (h headerRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Set(h.name, h.value)
	return h.next.RoundTrip(r)
}
