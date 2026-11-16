package metrics

// resolve_collection_lists_otlp_988_test.go — #988: the OTLP mirror of
// snowplow_resolve_collection_lists, through the REAL registerInstruments
// callback over a ManualReader.
//
//	PARITY  the series equal the cells /debug/vars publishes, one per
//	        (gvr, reason).
//	F8      the only attribute keys are gvr and reason; reason is the closed
//	        FallthroughReason enum or "unattributed"; nothing driven as an
//	        identity (user, namespace, object name) reaches a value.
//	CFG-1   under cache-off the series is absent.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

const rcl988 = "snowplow_resolve_collection_lists_total"

func drive988(t *testing.T) {
	t.Helper()
	credhttp.ResetResolveCollectionListsForTest()
	t.Cleanup(credhttp.ResetResolveCollectionListsForTest)
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	// An identity on the ctx, and a namespace and name on the path: none may
	// reach an attribute.
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "m988-user", Groups: []string{"m988-devs"}}))
	ctx = cache.WithFallthroughScope(ctx, cache.ScopeCallRestactions)
	send := func(ctx context.Context, path string) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+path, nil)
		resp, err := credhttp.Client(&http.Client{}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	send(cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, "x"), "/apis/widgets.templates.krateo.io/v1beta1/namespaces/m988-ns/widgets")
	send(ctx, "/api/v1/namespaces/m988-ns/configmaps")
	send(ctx, "/api/v1/namespaces/m988-ns/configmaps")
}

func TestIssue988_OTLP_ParityHygieneAndCacheOff(t *testing.T) {
	t.Run("cache on", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		drive988(t)
		pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), rcl988)
		want := map[string]int64{
			"widgets.templates.krateo.io/v1beta1, Resource=widgets|" + string(cache.ReasonInformerNotServable): 1,
			"/v1, Resource=configmaps|" + credhttp.ReasonUnattributed:                                          2,
		}
		if len(pts) != len(want) {
			t.Fatalf("%s emitted %d points; want %d (%v)", rcl988, len(pts), len(want), pts)
		}
		for _, p := range pts {
			if len(p.attrs) != 2 || p.attrs["gvr"] == "" || p.attrs["reason"] == "" {
				t.Errorf("F8: %s carries attributes %v; want exactly gvr and reason", rcl988, p.attrs)
			}
			if w, ok := want[p.attrs["gvr"]+"|"+p.attrs["reason"]]; !ok || w != p.value {
				t.Errorf("parity: point %v=%d is not the expvar cell (want %v)", p.attrs, p.value, want)
			}
			for k, v := range p.attrs {
				if strings.Contains(v, "m988") {
					t.Errorf("F8: %s attribute %s=%q carries identity or object material", rcl988, k, v)
				}
			}
		}
	})
	for _, v := range []string{"false", ""} {
		t.Run("CACHE_ENABLED="+v, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", v)
			all := flatten(collectViaRealCallback(t, "deadbeef"))
			if len(all) == 0 {
				t.Fatal("non-exercise guard: nothing collected at all under cache-off")
			}
			if pts := pointsFor(all, rcl988); len(pts) != 0 {
				t.Fatalf("CFG-1: %s exported %d points under cache-off", rcl988, len(pts))
			}
		})
	}
}
