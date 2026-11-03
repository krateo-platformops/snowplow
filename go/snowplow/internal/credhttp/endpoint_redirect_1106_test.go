package credhttp

// endpoint_redirect_1106_test.go — the unit arms of #1106's redirect policy and
// #1112 D4's wire dump (the resolver-level arms are in
// internal/resolvers/restactions/api, issue1106_* and issue1112_d4_*).

import (
	"bytes"
	"context"
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

func TestIssue1106_EndpointPresentsCredential(t *testing.T) {
	for _, tc := range []struct {
		name string
		ep   *endpoints.Endpoint
		want bool
	}{
		{"none", &endpoints.Endpoint{ServerURL: "https://h"}, false},
		{"bearer", &endpoints.Endpoint{ServerURL: "https://h", Token: "t"}, true},
		{"basic_username", &endpoints.Endpoint{ServerURL: "https://h", Username: "u"}, true},
		{"basic_password", &endpoints.Endpoint{ServerURL: "https://h", Password: "p"}, true},
		{"client_cert", &endpoints.Endpoint{ServerURL: "https://h", ClientCertificateData: "c", ClientKeyData: "k"}, true},
		{"aws", &endpoints.Endpoint{ServerURL: "https://h", AwsAccessKey: "a", AwsSecretKey: "s", AwsRegion: "r"}, true},
		{"userinfo", &endpoints.Endpoint{ServerURL: "https://u:p@h"}, true},
		{"nil", nil, false},
	} {
		if got := EndpointPresentsCredential(tc.ep); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIssue1106_PolicyRefusesOnlyACredentialedHopToAnotherOrigin(t *testing.T) {
	own := "https://api.example.com:443"
	other := mustURL(t, "https://evil.example.net/x")
	same := mustURL(t, "https://api.example.com/moved")
	cookie := http.Header{"Cookie": []string{"sid=1"}}
	for _, tc := range []struct {
		name string
		p    endpointRedirectPolicy
		u    *url.URL
		h    http.Header
		want bool
	}{
		{"credentialed_other", endpointRedirectPolicy{own: own, credentialed: true}, other, nil, true},
		{"credentialed_same", endpointRedirectPolicy{own: own, credentialed: true}, same, nil, false},
		{"header_other", endpointRedirectPolicy{own: own}, other, cookie, true},
		{"header_same", endpointRedirectPolicy{own: own}, same, cookie, false},
		{"bare_other", endpointRedirectPolicy{own: own}, other, http.Header{}, false},
		{"credentialed_no_origin", endpointRedirectPolicy{credentialed: true}, same, nil, true},
	} {
		if got := tc.p.refuses(tc.u, tc.h); got != tc.want {
			t.Errorf("%s: refuses=%v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIssue1106_CheckRedirectKeepsTheTenHopBound(t *testing.T) {
	c := EndpointClient(&http.Client{}, &endpoints.Endpoint{ServerURL: "https://api.example.com"})
	via := make([]*http.Request, 10)
	if err := c.CheckRedirect(&http.Request{URL: mustURL(t, "https://api.example.com/x"), Header: http.Header{}}, via); err == nil {
		t.Fatal("setting CheckRedirect replaces net/http's 10-redirect bound: it must be kept")
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
	for _, want := range []string{"Authorization: [REDACTED]", "X-Api-Key: [REDACTED]", "Content-Type: application/json"} {
		if !strings.Contains(got, want) {
			t.Errorf("want %q in:\n%s", want, got)
		}
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
