package credhttp

// endpoint_redirect_1106_test.go — the unit arms of #1106's redirect policy and
// #1112 D4's wire dump (the resolver-level arms are in
// internal/resolvers/restactions/api, issue1106_* and issue1112_d4_*).

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestIssue1106_Origin(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		same bool
	}{
		{"https://api.example.com/x", "https://API.example.com:443/y?q=1", true},
		{"http://api.example.com/x", "http://api.example.com:80/y", true},
		{"https://api.example.com", "http://api.example.com", false}, // downgrade
		{"https://api.example.com", "https://api.example.com:8443", false},
		{"https://api.example.com", "https://evil.api.example.com", false}, // a subdomain is another host
		{"https://api.example.com", "https://example.com", false},
	} {
		if got := Origin(mustURL(t, tc.a)) == Origin(mustURL(t, tc.b)); got != tc.same {
			t.Errorf("%s vs %s: same origin = %v, want %v", tc.a, tc.b, got, tc.same)
		}
	}
	if Origin(mustURL(t, "/relative")) != "" {
		t.Error("a URL with no scheme or host has no origin")
	}
}

func TestIssue1106_RefusesEveryHopToAnotherOrigin(t *testing.T) {
	own := "https://api.example.com:443"
	for _, tc := range []struct {
		u    string
		want bool
	}{
		{"https://api.example.com/moved", false},
		{"https://API.example.com:443/moved?x=1", false},
		{"https://evil.example.net/x", true},
		{"https://evil.api.example.com/x", true}, // net/http keeps credential headers on a subdomain (oasgen A1)
		{"https://api.example.com:8443/x", true}, // ... and on another port of the same hostname (oasgen 7, 9)
		{"http://api.example.com/x", true},       // a downgrade (oasgen 10, 12)
		// The rows of oasgen's host-policy conformance list that apply to an
		// origin-equality rule:
		{"https://api.example.com.attacker.net/x", true},     // A2: no suffix or substring match
		{"https://api.example.com@evil.example.net/x", true}, // 20: userinfo cannot stand in for the host
		{"https://collaborators/x", true},                    // 23: a single-label name (a search-domain Service)
		{"https://api.example.com:443/moved", false},         // 23: a spelled-out default port is the same origin
		{"https://api.example.com./moved", false},            // 17: a trailing dot is the same name to DNS
		{"https://API.EXAMPLE.COM/moved", false},             // 18: case
		{"http://api.example.com:443/x", true},               // scheme participates even on the same port
	} {
		if got := refusesHop(own, mustURL(t, tc.u)); got != tc.want {
			t.Errorf("%s: refused=%v, want %v", tc.u, got, tc.want)
		}
	}
	// oasgen 3/4: the zero value refuses. An origin snowplow could not read
	// (no scheme, no host, a bare trailing dot) refuses every hop.
	for _, own := range []string{"", Origin(mustURL(t, "/relative")), Origin(mustURL(t, "https://./x"))} {
		if !refusesHop(own, mustURL(t, "https://api.example.com/x")) {
			t.Errorf("own=%q: an unknown origin refuses every hop", own)
		}
	}
}

// The hop is refused WHATEVER its headers: a stage may carry a credential in
// any header (X-Api-Key, X-Auth-Token, PRIVATE-TOKEN), which net/http keeps on
// a cross-host redirect. plumbing's redirect guard strips Authorization alone
// (#1135), so the last header is a deliberately odd name: nothing in the stack
// strips it by name, so only the refused hop can keep it from the other host.
func TestIssue1106_SendRefusesACrossOriginHopCarryingAnyHeader(t *testing.T) {
	var otherHits []http.Header
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		otherHits = append(otherHits, r.Header.Clone())
	}))
	t.Cleanup(other.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, strings.Replace(other.URL, "127.0.0.1", "localhost", 1)+"/stolen", http.StatusFound)
	}))
	t.Cleanup(api.Close)
	for _, h := range []string{"X-Api-Key", "X-Auth-Token", "Private-Token", "X-Zq7-Tenant-Cred"} {
		req, _ := http.NewRequest(http.MethodGet, api.URL+"/x", nil)
		req.Header.Set(h, "KEY-canary")
		resp, err := Send(SendSiteStage, &http.Client{}, req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusFound || len(otherHits) != 0 {
			t.Fatalf("%s: RED — a cross-host hop was sent (status %d, %d request(s) at the other host)", h, resp.StatusCode, len(otherHits))
		}
	}
}

func TestIssue1106_SameOriginOnly_BoundBackstopAndInnerPolicy(t *testing.T) {
	own := "https://api.example.com:443"
	c := SameOriginOnly(&http.Client{}, own)
	via := make([]*http.Request, 10)
	if err := c.CheckRedirect(&http.Request{URL: mustURL(t, "https://api.example.com/x")}, via); err == nil {
		t.Fatal("setting CheckRedirect replaces net/http's 10-redirect bound: it must be kept")
	}
	// A held client's refuse-all policy is still asked for a same-origin hop.
	held := SameOriginOnly(Client(&http.Client{}), own)
	if err := held.CheckRedirect(&http.Request{URL: mustURL(t, "https://api.example.com/x")}, nil); err != http.ErrUseLastResponse {
		t.Fatalf("a held client still refuses a same-origin redirect (#723), got %v", err)
	}
	// The backstop refuses the hop when CheckRedirect is dropped from a copy.
	rt := c.Transport.(*refuseCrossOriginHops)
	_, err := rt.RoundTrip(&http.Request{Method: http.MethodGet, URL: mustURL(t, "https://evil.example.net/x"),
		Header: http.Header{}, Response: &http.Response{StatusCode: http.StatusTemporaryRedirect}})
	var refused *RedirectRefusedError
	if !errors.As(err, &refused) || refused.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("want the hop refused with the 307 named, got %v", err)
	}
	orig := &http.Client{}
	_ = SameOriginOnly(orig, own)
	if orig.CheckRedirect != nil || orig.Transport != nil {
		t.Fatal("SameOriginOnly must not modify the client it is handed")
	}
}

func TestIssue1112_D4_DumpHeadersRedactsEveryCredentialValue(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer TOKEN-canary")
	h.Set("Proxy-Authorization", "Basic PROXY-canary")
	h.Set("Cookie", "sid=COOKIE-canary")
	h.Set("Set-Cookie", "sid=SETCOOKIE-canary")
	h.Set("Impersonate-User", "IDENTITY-canary")
	h.Set("X-Api-Key", "APIKEY-canary")
	h.Set("Location", "https://u:PASS-canary@h/x?sig=SIG-canary")
	h.Set("Content-Type", "application/json")
	got := strings.Join(dumpHeaders(h), "\n")
	for _, c := range []string{"TOKEN-", "PROXY-", "COOKIE-", "SETCOOKIE-", "IDENTITY-", "APIKEY-", "PASS-", "SIG-"} {
		if strings.Contains(got, c) {
			t.Errorf("RED: %s… is in the dumped headers:\n%s", c, got)
		}
	}
	// Presence only: the named headers by the dump's own constant, the rest
	// counted (X-Api-Key), and never a value, not even a descriptive one.
	want := "Authorization\nContent-Type\nCookie\nImpersonate-User\nLocation\nProxy-Authorization\nSet-Cookie\n+1 other"
	if got != want {
		t.Errorf("dumped headers:\n%s\nwant:\n%s", got, want)
	}
	if strings.Contains(got, "application/json") {
		t.Error("RED: a header value is in the dump")
	}
}

func TestIssue1112_D4_CallPassthroughDumpHoldsNoCredentialOrBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"echo":"RESPBODY-canary"}`))
	}))
	t.Cleanup(srv.Close)
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(lg))
	for _, ep := range []*endpoints.Endpoint{
		{ServerURL: srv.URL, Token: "TOKEN-canary", Debug: true},
		{ServerURL: srv.URL, Password: "TOKEN-canary", Debug: true},
	} {
		buf.Reset()
		st := Do(ctx, request.RequestOptions{
			RequestInfo: request.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodPost), Payload: ptr.To(`{"b":"REQBODY-canary"}`),
				Headers: []string{"Content-Type: application/json"}},
			Endpoint: ep,
		})
		if st == nil || st.Code >= 300 {
			t.Fatalf("PRE: the call succeeds, got %+v", st)
		}
		logs := buf.String()
		if !strings.Contains(logs, `"msg":"request details"`) {
			t.Fatalf("PRE: the Debug endpoint's dump is present; logs: %s", logs)
		}
		for _, c := range []string{"TOKEN-canary", "REQBODY-canary", "RESPBODY-canary"} {
			if strings.Contains(logs, c) {
				t.Errorf("RED: %s is in the /call wire dump (password-only=%v)", c, ep.Token == "")
			}
		}
		if strings.Contains(logs, "VE9LRU4tY2FuYXJ5") { // base64("TOKEN-canary") inside a Basic header
			t.Errorf("RED: the basic credential is in the /call wire dump")
		}
	}
	orig := &endpoints.Endpoint{Debug: true}
	if WithoutPlumbingWireDump(orig).Debug || !orig.Debug {
		t.Fatal("plumbing is handed a copy without Debug, and the caller's endpoint is not changed")
	}
}
