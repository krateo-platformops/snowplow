package api

// host_pin_730_test.go — #730: an isolated arm for #721's SA-dial host pin
// (hostPinnedTransport / pinSADialHost).
//
// In the full dial path #727's credhttp client refuses every redirect
// (CheckRedirect = ErrUseLastResponse), so a mutation that removes the pin
// survives there: the hop never reaches the transport. These arms drive the pin
// through a BARE http.Client with NO CheckRedirect — Go's default policy follows
// up to 10 redirects — over the same layer order an SA-endpoint dial uses:
//
//	pinSADialHost (outermost) → impersonateEndpointTransport → SA bearer → network
//
// so the pin alone stands between a cross-host hop and the credentials.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"k8s.io/client-go/rest"
)

type rec730 struct{ path, auth, impUser string }

type server730 struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []rec730
}

// newServer730 records every request. A request to /redirect is answered with
// code → target; anything else with 200.
func newServer730(t *testing.T, code int, target func() string) *server730 {
	t.Helper()
	s := &server730{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.reqs = append(s.reqs, rec730{r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Impersonate-User")})
		s.mu.Unlock()
		if r.URL.Path == "/redirect" && target != nil {
			http.Redirect(w, r, target(), code)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *server730) requests() []rec730 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rec730(nil), s.reqs...)
}

// bearer730 stands in for plumbing's bearer layer: it adds the SA credential on
// EVERY request it carries, as the real one does on each redirect hop.
type bearer730 struct{ delegate http.RoundTripper }

func (b bearer730) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.Header.Set("Authorization", "Bearer sa-730")
	return b.delegate.RoundTrip(r)
}

// client730 is a bare client (no CheckRedirect) with the SA-dial layer order,
// pinned to first.
func client730(t *testing.T, first string) *http.Client {
	t.Helper()
	u, err := url.Parse(first)
	if err != nil {
		t.Fatal(err)
	}
	ctx := withEndpointImpersonation(context.Background(), rest.ImpersonationConfig{UserName: "cohort-730", Groups: []string{"g-730"}})
	rt := http.RoundTripper(bearer730{delegate: http.DefaultTransport})
	rt = impersonateEndpointTransport(ctx, rt)
	return &http.Client{Transport: pinSADialHost(u, rt)} // CheckRedirect deliberately unset
}

func TestIssue730_HostPin_RefusesCrossHostRedirect_WithoutCheckRedirect(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			other := newServer730(t, 0, nil)
			api := newServer730(t, code, func() string { return other.srv.URL + "/stolen" })

			resp, err := client730(t, api.srv.URL).Get(api.srv.URL + "/redirect")
			if err == nil {
				resp.Body.Close()
			}
			for _, r := range other.requests() {
				t.Fatalf("#730 RED: with no CheckRedirect, the %d hop reached another host carrying Authorization=%q "+
					"Impersonate-User=%q — the host pin is the only layer that stops it", code, r.auth, r.impUser)
			}
			if err == nil || !strings.Contains(err.Error(), "another host") {
				t.Fatalf("the cross-host hop must fail with the pin's refusal, got err=%v", err)
			}
			// The first request — to the pinned host — carried the credentials.
			reqs := api.requests()
			if len(reqs) != 1 || reqs[0].auth != "Bearer sa-730" || reqs[0].impUser != "cohort-730" {
				t.Fatalf("precondition: the pinned host must get exactly the first request, with the SA bearer and the "+
					"cohort's impersonation; got %+v", reqs)
			}
		})
	}
}

// The pin refuses a host change, not a redirect: a same-host hop (explicit
// default port included) is still followed.
func TestIssue730_HostPin_FollowsSameHostRedirect(t *testing.T) {
	var api *server730
	api = newServer730(t, http.StatusTemporaryRedirect, func() string { return api.srv.URL + "/landed" })
	resp, err := client730(t, api.srv.URL).Get(api.srv.URL + "/redirect")
	if err != nil {
		t.Fatalf("a same-host redirect must be followed, got %v", err)
	}
	resp.Body.Close()
	reqs := api.requests()
	if len(reqs) != 2 || reqs[1].path != "/landed" {
		t.Fatalf("want /redirect then /landed on the same host, got %+v", reqs)
	}
	for _, c := range []struct{ a, b string }{
		{"https://h.example", "https://h.example:443"},
		{"http://H.example", "http://h.example:80"},
	} {
		ua, _ := url.Parse(c.a)
		ub, _ := url.Parse(c.b)
		if canonicalHostPort(ua) != canonicalHostPort(ub) {
			t.Fatalf("canonicalHostPort(%q) != canonicalHostPort(%q)", c.a, c.b)
		}
	}
	for _, c := range []struct{ a, b string }{
		{"https://h.example", "http://h.example"},
		{"https://h.example", "https://h.example:6443"},
		{"https://h.example", "https://i.example"},
	} {
		ua, _ := url.Parse(c.a)
		ub, _ := url.Parse(c.b)
		if canonicalHostPort(ua) == canonicalHostPort(ub) {
			t.Fatalf("canonicalHostPort(%q) == canonicalHostPort(%q): different servers must differ", c.a, c.b)
		}
	}
}
