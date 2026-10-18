package handlers

// call_726_test.go — #726: the /call raw passthrough dials the CALLER's own
// clientconfig, a credential snowplow holds on their behalf. A GET whose
// apiserver answers 302/307 to another host: no request reaches the other
// host, the redirected body is never served, and the refused 3xx is surfaced
// as 502 Bad Gateway naming the 3xx, with no Location (#729's convention).

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
)

func TestIssue726_CallPassthroughNeverFollowsARedirect(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			prev := xenv.TestMode()
			xenv.SetTestMode(true) // plumbing's UserConfig keeps the injected ServerURL
			t.Cleanup(func() { xenv.SetTestMode(prev) })

			var foreign atomic.Int64
			other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				foreign.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"from-the-other-host"}}`))
			}))
			defer other.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", other.URL+r.URL.Path)
				w.WriteHeader(code)
			}))
			defer origin.Close()

			// A token-auth clientconfig: plumbing re-adds the bearer on every hop.
			rec := serve443(Call(), endpoints.Endpoint{ServerURL: origin.URL, Token: "alices-own-token"},
				http.MethodGet, "/call?"+cmQuery443, "")

			if n := foreign.Load(); n != 0 {
				t.Fatalf("#726 RED: %d request(s) carrying the caller's token reached the redirect's other host", n)
			}
			if strings.Contains(rec.Body.String(), "from-the-other-host") {
				t.Fatalf("#726 RED: the redirected body was served as the answer: %s", rec.Body.String())
			}
			if rec.Code != http.StatusBadGateway || rec.Header().Get("Location") != "" ||
				!strings.Contains(rec.Body.String(), "refused to follow a "+strconv.Itoa(code)+" redirect") {
				t.Fatalf("#726: /call answered %d (Location %q), want 502 naming the refused %d (#729 convention); body %s",
					rec.Code, rec.Header().Get("Location"), code, rec.Body.String())
			}
		})
	}
}
