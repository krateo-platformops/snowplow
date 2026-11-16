//go:build unit

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/rest"
)

// A held dial and a named-ref dial of a field-identical endpoint never share a
// client: the held one refuses a redirect, the named ref follows it, in either
// order. The redirect stays on the endpoint's own origin: since #1106 a named
// ref follows no redirect to another origin either (credhttp.Send).
func TestRev1101_HeldAndNamedRefNeverShareAClient(t *testing.T) {
	for _, order := range []string{"named-first", "held-first"} {
		t.Run(order, func(t *testing.T) {
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/y" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"kind":"X","id":"followed"}`))
					return
				}
				http.Redirect(w, r, "/y", http.StatusFound)
			}))
			defer origin.Close()
			ep := endpoints.Endpoint{ServerURL: origin.URL, Token: "same-token-" + order}
			held := credhttp.WithHeldCredential(context.Background())
			check := func(ctx context.Context, isHeld bool) {
				got := revGetID(t, ctx, ep, "GET")
				if isHeld && got == "followed" {
					t.Fatalf("a held dial followed a redirect (it reused the named ref's client)")
				}
				if !isHeld && got != "followed" {
					t.Fatalf("a named-ref dial did not follow its redirect (it reused the held client): %q", got)
				}
			}
			if order == "named-first" {
				check(context.Background(), false)
				check(held, true)
			} else {
				check(held, true)
				check(context.Background(), false)
			}
		})
	}
}

// Two SA dials impersonating two cohorts, sequentially from a cold cache: each
// request carries only its own cohort (the per-fetch wrapper never sticks to the
// kept client).
func TestRev1101_ImpersonationNeverSticksToKeptClient(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, strings.Join(r.Header.Values("Impersonate-User"), ","))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"X","id":"ok"}`))
	}))
	defer srv.Close()
	ep := endpoints.Endpoint{ServerURL: srv.URL, Token: "sa-imp-cold-key"}
	sa := cache.WithServiceAccountDial(context.Background())
	for i, cohort := range []string{"cohort-a", "cohort-b", "", "cohort-c"} {
		ctx := sa
		if cohort != "" {
			ctx = withEndpointImpersonation(sa, rest.ImpersonationConfig{UserName: cohort})
		}
		if got := revGetID(t, ctx, ep, "GET"); got != "ok" {
			t.Fatalf("fetch %d: %q", i, got)
		}
		mu.Lock()
		last := seen[len(seen)-1]
		mu.Unlock()
		if last != cohort {
			t.Fatalf("fetch %d as %q carried Impersonate-User %q", i, cohort, last)
		}
	}
	_ = fmt.Sprint()
}
