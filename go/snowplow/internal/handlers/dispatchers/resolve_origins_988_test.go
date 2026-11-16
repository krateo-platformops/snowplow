package dispatchers

// resolve_origins_988_test.go — #988: every resolve origin marks its ctx, so
// the LISTs its resolves send are counted by snowplow_resolve_collection_lists.
//
// Driven through the REAL production context builders of the background
// origins this package owns — the boot prewarm discovery walk, the
// content-prewarm pass (and the cluster-list prewarm, which uses it), the
// per-cohort seed (and the reseed engine, which uses it) — and the /call
// route middleware with every route scope main.go registers. The refresher's
// scope is pinned in internal/cache (resolve_lists_988_test.go REF), where it
// is stamped. Each origin's ctx sends one LIST through a client hardened the
// way every resolve dial is, and that LIST must be counted.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func TestIssue988_EveryResolveOriginMarksItsContext(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // else WithFallthroughScope is a no-op (CFG-1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMapList","apiVersion":"v1","items":[]}`))
	}))
	t.Cleanup(srv.Close)
	base := xcontext.BuildContext(context.Background())

	origins := map[string]context.Context{
		"boot prewarm walk (withPhase1SAContext)":          withPhase1SAContext(base, endpoints.Endpoint{}, nil),
		"content prewarm (withContentPrewarmSAContext)":    withContentPrewarmSAContext(base, endpoints.Endpoint{}, nil),
		"cohort seed (withCohortSeedContext)":              withCohortSeedContext(base, seedTarget{Username: "admin"}, endpoints.Endpoint{}, nil),
		"cohort seed under the walk (rePrewarmBootScoped)": withCohortSeedContext(withPhase1SAContext(base, endpoints.Endpoint{}, nil), seedTarget{Username: "admin"}, endpoints.Endpoint{}, nil),
	}
	for _, scope := range []string{
		cache.ScopeCallRestactions, cache.ScopeCallWidgets, cache.ScopeCallGeneric,
		cache.ScopeList, cache.ScopePlurals,
	} {
		var seen context.Context
		h := cache.FallthroughScopeMiddleware(scope)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			seen = r.Context()
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/call", nil))
		origins["route middleware "+scope] = seen
	}

	for name, ctx := range origins {
		t.Run(name, func(t *testing.T) {
			if ctx == nil || !credhttp.ResolveMarked(ctx) {
				t.Fatalf("%s: the ctx is not marked — every LIST its resolves send is invisible to "+
					"snowplow_resolve_collection_lists", name)
			}
			credhttp.ResetResolveCollectionListsForTest()
			t.Cleanup(credhttp.ResetResolveCollectionListsForTest)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/namespaces/n/configmaps", nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := credhttp.Client(&http.Client{}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if n := credhttp.ResolveCollectionListCount("/v1, Resource=configmaps", credhttp.ReasonUnattributed); n != 1 {
				t.Fatalf("%s: the LIST was not counted: %v", name, credhttp.ResolveCollectionLists())
			}
		})
	}
}
