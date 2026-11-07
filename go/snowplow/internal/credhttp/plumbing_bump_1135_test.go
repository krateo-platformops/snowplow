package credhttp

// plumbing_bump_1135_test.go — #1135: the behaviour plumbing v1.15.1 → v1.28.1
// changed under snowplow, at snowplow's edge.
//
//   - plumbing's round-trippers now attach a credential only on the origin of
//     ep.ServerURL. Every spelling of that origin snowplow can send must keep it
//     (a false refusal is a silent 401).
//   - plumbing's pass-credentials-on-redirect opt-in opens nothing in Send.
//   - the endpoint identity folds the field plumbing added to Endpoint.
//   - the two layers differ on purpose: plumbing permits http → https on one
//     host, Send refuses it.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// Every ServerURL spelling of one origin keeps the endpoint's credential on the
// plumbing-built client: the anchor is read from ep.ServerURL and compared with
// the URL RequestURL builds from it, on the path (/call, credhttp.Do) every
// endpoint shape takes.
func TestIssue1135_PlumbingAnchorKeepsTheCredentialOnEverySpellingOfTheOrigin(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	_, port, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")

	for name, ep := range map[string]*endpoints.Endpoint{
		"plain":          {ServerURL: srv.URL, Token: "tok"},
		"trailing_slash": {ServerURL: srv.URL + "/", Token: "tok"},
		"base_path":      {ServerURL: srv.URL + "/base/", Token: "tok"},
		"upper_case":     {ServerURL: "http://LOCALHOST:" + port, Token: "tok"},
	} {
		t.Run(name, func(t *testing.T) {
			got.Store("")
			handled := false
			st := Do(context.Background(), request.RequestOptions{
				RequestInfo:     request.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodGet)},
				Endpoint:        ep,
				ResponseHandler: func(rc io.ReadCloser) error { handled = true; return rc.Close() },
			})
			if !handled {
				t.Fatalf("PRE: the request was not answered: %+v", st)
			}
			if got.Load() != "Bearer tok" {
				t.Fatalf("RED: the endpoint's own origin got no credential (Authorization = %q): plumbing's anchor refused a spelling of it", got.Load())
			}
		})
	}
}

// The control for the arm above: a request to any origin other than ep.ServerURL
// carries no credential, so the arm above can fail.
func TestIssue1135_PlumbingAnchorControl_AnotherOriginGetsNoCredential(t *testing.T) {
	var got atomic.Value
	got.Store("unset")
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
	}))
	t.Cleanup(srv.Close)
	cli, err := request.HTTPClientForEndpoint(&endpoints.Endpoint{ServerURL: "http://api.example.com", Token: "tok"}, &request.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/x", nil)
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got.Load() != "" {
		t.Fatalf("plumbing attached the credential off its anchor: Authorization = %q", got.Load())
	}
}

// pass-credentials-on-redirect is plumbing's opt-in. Send does not read it: a
// cross-origin hop is refused with the key set, whatever carries the credential.
func TestIssue1135_PassCredentialsOnRedirectOpensNothingInSend(t *testing.T) {
	var otherHits atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { otherHits.Add(1) }))
	t.Cleanup(other.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, strings.Replace(other.URL, "127.0.0.1", "localhost", 1)+"/stolen", http.StatusFound)
	}))
	t.Cleanup(api.Close)

	ep := &endpoints.Endpoint{ServerURL: api.URL, Token: "tok", PassCredentialsOnRedirect: true}
	cli, err := request.HTTPClientForEndpoint(ep, &request.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, api.URL+"/x", nil)
	req.Header.Set("X-Zq7-Tenant-Cred", "KEY-canary")
	resp, err := Send(SendSiteStage, cli, req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusFound || otherHits.Load() != 0 {
		t.Fatalf("RED: the opt-in opened the hop (status %d, %d request(s) at the other host)", resp.StatusCode, otherHits.Load())
	}
}

// The two layers differ on purpose (howto/restactions.md): plumbing lets a
// credential follow http → https on one host, Send refuses every change of
// origin.
func TestIssue1135_SendRefusesTheSchemeUpgradePlumbingPermits(t *testing.T) {
	own := Origin(mustURL(t, "http://api.example.com:8443/x"))
	if !refusesHop(own, mustURL(t, "https://api.example.com:8443/x")) {
		t.Fatal("RED: an http → https hop on one host:port is another origin, and Send refuses it")
	}
	if refusesHop(own, mustURL(t, "http://api.example.com:8443/y")) {
		t.Fatal("control: a hop within the origin is followed")
	}
}

// Two endpoints that differ only in the field plumbing added are two identities:
// a client kept for one is never handed to the other.
func TestIssue1135_EndpointKeyFoldsTheFieldPlumbingAdded(t *testing.T) {
	a := &endpoints.Endpoint{ServerURL: "https://x", Token: "t"}
	b := &endpoints.Endpoint{ServerURL: "https://x", Token: "t", PassCredentialsOnRedirect: true}
	if EndpointKeyOf(a) == EndpointKeyOf(b) {
		t.Fatal("RED: PassCredentialsOnRedirect is not folded into the endpoint identity")
	}
	if !endpointKeyExact {
		t.Fatal("PRE: the fold must stay exact, or no client is kept per identity")
	}
}

// plumbing's default transport carries the destination policy since v1.25.0, so
// every client it builds refuses such an address on the dialled IP. The address
// never receives a packet: the refusal is in the dialer's control hook.
func TestIssue1135_PlumbingBuiltClientRefusesLinkLocal(t *testing.T) {
	ep := &endpoints.Endpoint{ServerURL: "http://169.254.169.254", Token: "tok"}
	cli, err := request.HTTPClientForEndpoint(ep, &request.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://169.254.169.254/latest", nil)
	resp, err := Send(SendSiteStage, cli, req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("RED: the link-local address answered (status %d)", resp.StatusCode)
	}
	var opErr interface{ Timeout() bool }
	if !errors.As(err, &opErr) || !strings.Contains(err.Error(), "destination policy") {
		t.Fatalf("RED: want the destination policy's refusal, got %v", err)
	}
}
