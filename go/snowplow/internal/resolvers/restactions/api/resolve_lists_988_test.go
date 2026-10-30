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
//
// The #991 review arms follow: EP (a named endpointRef's LIST is counted),
// JOIN (a flight joiner carries the leader's reason), DIRECT (the pivot branch
// with the resolved cache off carries its reason).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
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

// --- #991 review findings ------------------------------------------------

// TestIssue988_EP_ANamedEndpointRefListIsCounted is the reviewer's case
// (finding 1): a stage whose endpointRef carries a third party's own credential
// (no held mark, no SA dial) and points at the apiserver sends a marked LIST.
// It is a resolve's LIST and must be counted, once. A held dial, whose chain
// already counts through credhttp.Client, must not count twice. The named
// endpoint keeps net/http's redirect behaviour (no credhttp.Client).
func TestIssue988_EP_ANamedEndpointRefListIsCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMapList","apiVersion":"v1","items":[]}`))
	}))
	t.Cleanup(srv.Close)
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	marked := credhttp.MarkResolve(context.Background())
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		named bool
	}{
		{"named endpointRef", marked, true},
		{"held credential", credhttp.WithHeldCredential(marked), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credhttp.ResetResolveCollectionListsForTest()
			t.Cleanup(credhttp.ResetResolveCollectionListsForTest)
			cli, err := httpClientForEndpoint(tc.ctx, ep, &httpcall.RequestInfo{Verb: ptr.To(http.MethodGet)})
			if err != nil {
				t.Fatal(err)
			}
			if tc.named && cli.CheckRedirect != nil {
				t.Fatal("a named endpointRef must keep net/http's redirect behaviour; the counter is not a redirect policy")
			}
			req, _ := http.NewRequestWithContext(tc.ctx, http.MethodGet, srv.URL+"/api/v1/namespaces/n/configmaps", nil)
			resp, err := cli.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			got := credhttp.ResolveCollectionLists()
			k := credhttp.ResolveListKey{GVR: "/v1, Resource=configmaps", Reason: credhttp.ReasonUnattributed}
			if len(got) != 1 || got[k] != 1 {
				t.Fatalf("%s: want exactly {%s|%s}=1; got %v", tc.name, k.GVR, k.Reason, got)
			}
		})
	}
}

// TestIssue988_JOIN_AJoinerCarriesTheLeadersReason pins the content-fill flight
// handoff (finding 2, MY3/MY3b). A reader that JOINS a flight whose leader's
// dispatch fell through falls through on that decision, without dispatching
// itself, and its fall-through ctx carries the leader's reason.
func TestIssue988_JOIN_AJoinerCarriesTheLeadersReason(t *testing.T) {
	newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	var arrived atomic.Int64
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool, context.Context) {
		arrived.Add(1)
		<-gate
		return nil, false, cache.RecordApiserverFallthrough(ctx, cache.ReasonInformerNotServable, "x")
	}

	scoped := cache.WithFallthroughScope(r1Ctx(), cache.ScopeCallRestactions)
	type res struct {
		ok      bool
		fallCtx context.Context
	}
	serve := func() chan res {
		ch := make(chan res, 1)
		go func() {
			_, _, ok, fctx, _ := apistageContentServe(scoped, store, fill710ListCall(), false)
			ch <- res{ok, fctx}
		}()
		return ch
	}
	leader := serve()
	if !fill710WaitFor(10*time.Second, func() bool { return arrived.Load() == 1 }) {
		t.Fatal("PRECONDITION: the leader never dispatched")
	}
	joiner := serve()
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == 1 }) {
		t.Fatalf("PRECONDITION: the second reader is not waiting on the flight (%d)", ContentFillStatsSnapshot().JoinWaiting)
	}
	release()
	for _, w := range []struct {
		name string
		ch   chan res
	}{{"leader", leader}, {"joiner", joiner}} {
		select {
		case r := <-w.ch:
			if r.ok {
				t.Fatalf("%s: PRECONDITION: the flight must not be servable", w.name)
			}
			if got := cache.FallthroughReasonOf(r.fallCtx); got != cache.ReasonInformerNotServable {
				t.Fatalf("%s falls through with reason %q, want %q: its apiserver LIST would be unattributed",
					w.name, got, cache.ReasonInformerNotServable)
			}
			if !credhttp.ResolveMarked(r.fallCtx) {
				t.Fatalf("%s: the fall-through ctx lost the reader's resolve marker", w.name)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never returned", w.name)
		}
	}
	if n := arrived.Load(); n != 1 {
		t.Fatalf("the joiner dispatched itself (%d dispatches): it must fall through on the leader's decision", n)
	}
}

// TestIssue988_DIRECT_ThePivotBranchCarriesItsReason pins the stage loop's
// direct informer-pivot branch (finding 3, MY7). With the resolved cache off
// there is no content layer: dispatchViaInformer decides, and its reason must
// reach the apiserver LIST.
func TestIssue988_DIRECT_ThePivotBranchCarriesItsReason(t *testing.T) {
	newQ618Watcher(t)
	t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	cache.ResetResolvedCacheForTest()
	if cache.ResolvedCache() != nil {
		t.Fatal("PRECONDITION: the resolved cache (and with it the content layer) must be off")
	}
	if hits := resolve988(t, true, q618ClusterPath+"?limit=5"); hits != 1 {
		t.Fatalf("PRECONDITION: the stage must fall through to the apiserver once; hits=%d", hits)
	}
	want988(t, cache.ReasonInformerListQuery)
}
