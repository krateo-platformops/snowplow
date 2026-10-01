// issue376_clusterlist_notouch_test.go — #376: the cluster-list content reads that are
// NOT the customer serve must not stamp the shared content cell. attemptClusterListCollapse
// (:282) is a collapse-DECISION warmth probe and populateClusterListCellSync (:357) is a
// populate-worker re-check — neither is a serve, so both use GetNoTouch and leave hit/miss
// (and lastRead) untouched while still RECOGNIZING warmth. Reuses the warm-fallback harness.

package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func issue376ClusterListSetup(t *testing.T) (*cache.ResolvedCacheStore, *templates.API, context.Context, string) {
	t.Helper()
	_ = newClusterListWatcher(t)
	withClusterListCollapseEnabledForTest(t)
	cache.ResetClusterListCellCountersForTest()
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		time.Sleep(200 * time.Millisecond)
		ResetClusterListAsyncStateForTest()
	})
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache nil")
	}
	apiCall := &templates.API{
		Name: "widgets-cluster-collapse",
		Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets" }`,
		Verb: ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[{"ns":"team-a"}]`),
		},
	}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: clusterListBroadUser}),
	)
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	// Make the cell resident the deterministic way the harness uses (dispatch populate).
	clusterCall := buildClusterListCall(apiCall, endpoints.Endpoint{}, f1WidgetsGVR)
	if !populateClusterListCellSync(ctx, clusterListLogger(t), apiCall, f1WidgetsGVR, contentKey, clusterCall, store) {
		t.Fatal("setup: populateClusterListCellSync must warm the cell")
	}
	return store, apiCall, ctx, contentKey
}

// :282 — attemptClusterListCollapse's warmth probe recognizes the warm cell (useCluster=true)
// but does NOT stamp hit/miss (GetNoTouch). RED pre-fix: a plain Get bumps hit_total.
func TestIssue376_ClusterListCollapse_DecisionProbeIsNoTouch(t *testing.T) {
	store, apiCall, ctx, _ := issue376ClusterListSetup(t)

	h0, m0 := store.Stats().HitTotal, store.Stats().MissTotal
	_, useCluster, gate := attemptClusterListCollapse(
		ctx, clusterListLogger(t), apiCall, map[string]any{},
		endpoints.Endpoint{}, store, true)
	if !useCluster {
		t.Fatalf("warm cell must be RECOGNIZED by the collapse probe (useCluster=true); gate=%d", gate)
	}
	if s := store.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
		t.Fatalf("#376: the collapse decision probe must NOT stamp hit/miss (hit %d->%d miss %d->%d)", h0, s.HitTotal, m0, s.MissTotal)
	}
}

// :357 — populateClusterListCellSync's belt-and-braces re-check over an already-resident
// cell returns true (warm) but does NOT stamp hit/miss (GetNoTouch). RED pre-fix.
func TestIssue376_PopulateClusterListCellSync_RecheckIsNoTouch(t *testing.T) {
	store, apiCall, ctx, contentKey := issue376ClusterListSetup(t)

	clusterCall := buildClusterListCall(apiCall, endpoints.Endpoint{}, f1WidgetsGVR)
	h0, m0 := store.Stats().HitTotal, store.Stats().MissTotal
	if !populateClusterListCellSync(ctx, clusterListLogger(t), apiCall, f1WidgetsGVR, contentKey, clusterCall, store) {
		t.Fatal("re-check over a resident cell must return true (already warm)")
	}
	if s := store.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
		t.Fatalf("#376: the populate-worker re-check must NOT stamp hit/miss (hit %d->%d miss %d->%d)", h0, s.HitTotal, m0, s.MissTotal)
	}
}
