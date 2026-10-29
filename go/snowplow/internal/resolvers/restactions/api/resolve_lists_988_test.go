package api

// resolve_lists_988_test.go — #988 through the REAL api.Resolve stage loop
// against a recording apiserver: a stage that falls through sends its LIST
// with the reason the cache decided, and snowplow_resolve_collection_lists
// counts it under that reason — never as unattributed — while the same stage
// resolved outside any resolve scope is not counted at all.
//
//	LQ   a list query the informer cannot reproduce: the content layer's own
//	     decision (apistageContentServe, before any flight) →
//	     informer-fallthrough-list-query.
//	NS   a GVR whose informer is not servable: the content-fill LEADER's
//	     dispatchViaInformer gate decides (inside the fill ctx), and only the
//	     reason crosses back to the stage's dispatch ctx →
//	     informer-fallthrough-not-servable.
//	OFF  the LQ stage under a ctx with no resolve scope: the apiserver still
//	     receives the LIST; the counter does not.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

func resolve988(t *testing.T, scoped bool, path string) int64 {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","items":[]}`))
	}))
	t.Cleanup(srv.Close)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: q618User}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
	if scoped {
		ctx = cache.WithFallthroughScope(ctx, cache.ScopeCallRestactions)
	}
	credhttp.ResetResolveCollectionListsForTest()
	t.Cleanup(credhttp.ResetResolveCollectionListsForTest)
	q618Resolve(t, ctx, &templates.API{Name: "s", Path: path, Verb: ptr.To(http.MethodGet)})
	return hits.Load()
}

func want988(t *testing.T, reason cache.FallthroughReason) {
	t.Helper()
	got := credhttp.ResolveCollectionLists()
	k := credhttp.ResolveListKey{GVR: f1WidgetsGVR.String(), Reason: string(reason)}
	if len(got) != 1 || got[k] != 1 {
		t.Fatalf("want exactly {%s|%s}=1 — the stage's LIST labelled with the cache's decision; got %v",
			k.GVR, k.Reason, got)
	}
}

func TestIssue988_LQ_ListQueryFallthroughIsCountedWithItsReason(t *testing.T) {
	newQ618Watcher(t)
	if hits := resolve988(t, true, q618ClusterPath+"?limit=5"); hits != 1 {
		t.Fatalf("PRECONDITION: the stage must fall through to the apiserver once; hits=%d", hits)
	}
	want988(t, cache.ReasonInformerListQuery)
}

func TestIssue988_NS_NotServableFallthroughIsCountedWithTheLeadersReason(t *testing.T) {
	rw := newQ618Watcher(t)
	rw.FireWatchError(f1WidgetsGVR)
	t.Cleanup(func() { rw.MarkWatchRecovered(f1WidgetsGVR) })
	if rw.IsServable(f1WidgetsGVR) {
		t.Fatal("PRECONDITION: the GVR must not be servable")
	}
	if hits := resolve988(t, true, q618ClusterPath); hits != 1 {
		t.Fatalf("PRECONDITION: the stage must fall through to the apiserver once; hits=%d", hits)
	}
	want988(t, cache.ReasonInformerNotServable)
}

func TestIssue988_OFF_AnUnscopedResolveIsNotCounted(t *testing.T) {
	newQ618Watcher(t)
	if hits := resolve988(t, false, q618ClusterPath+"?limit=5"); hits != 1 {
		t.Fatalf("PRECONDITION: the stage must fall through to the apiserver once; hits=%d", hits)
	}
	if got := credhttp.ResolveCollectionLists(); len(got) != 0 {
		t.Fatalf("a LIST from a ctx no resolve origin scoped was counted: %v", got)
	}
}
