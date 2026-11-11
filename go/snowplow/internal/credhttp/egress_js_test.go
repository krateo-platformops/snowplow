//go:build js && wasm

package credhttp

// scripts/check_js_wasm.sh runs these under both of net/http's js/wasm modes:
// plain `node` (argv0 "node": Go's in-process network, SNOWPLOW_JS_MODE=node)
// and a renamed argv0 (fetch, SNOWPLOW_JS_MODE=fetch).

import (
	"io"
	"net"
	"net/http"
	"os"
	"testing"

	"k8s.io/client-go/transport"
)

type egressRecordingRT struct{ got *http.Request }

func (r *egressRecordingRT) RoundTrip(req *http.Request) (*http.Response, error) {
	r.got = req
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

// usesFetch must be net/http's own choice, in both directions: if it said
// fetch under Node, every hardened request would carry option headers Node's
// net/http rejects; if it said no fetch in a browser, every request would dial
// an in-process network where nothing listens.
func TestBrowserEgress_MirrorsNetHTTPsFetchChoice(t *testing.T) {
	mode := os.Getenv("SNOWPLOW_JS_MODE")
	if mode == "" {
		t.Skip("run by scripts/check_js_wasm.sh, which names the mode")
	}
	if usesFetch != (mode == "fetch") {
		t.Fatalf("usesFetch = %v under %s: it no longer mirrors net/http's choice (roundtrip_js.go)", usesFetch, mode)
	}
}

func TestBrowserEgress_IdentityWhereFetchIsNotUsed(t *testing.T) {
	if usesFetch {
		t.Skip("fetch is used here; TestBrowserEgress_SetsFetchOptionsOnAClone covers it")
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

func TestBrowserEgress_SetsFetchOptionsOnAClone(t *testing.T) {
	if !usesFetch {
		t.Skip("fetch is not used here")
	}
	if _, ok := BrowserEgress(&egressRecordingRT{}).(browserFetch); !ok {
		t.Fatal("BrowserEgress did not install the fetch transport where fetch is used")
	}
	rec := &egressRecordingRT{}
	req, _ := http.NewRequest(http.MethodGet, "https://kubernetes.sandbox.invalid/api", nil)
	req.Header.Set(fetchRedirectOption, "follow")
	if _, err := (browserFetch{next: rec}).RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got := rec.got.Header.Get(fetchRedirectOption); got != "error" {
		t.Errorf("redirect mode %q, want error", got)
	}
	if got := rec.got.Header.Get(fetchCredentialsOption); got != "omit" {
		t.Errorf("credentials mode %q, want omit", got)
	}
	if got := req.Header.Get(fetchRedirectOption); got != "follow" {
		t.Errorf("the caller's request was modified: %q", got)
	}
}
