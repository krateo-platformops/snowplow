// cluster_list_warm_fallback_test.go — Path 3.2 / 0.30.218 — cell-warm
// fast-path + cold-miss fallback falsifiers for attemptClusterListCollapse.
//
// AC-P3.2.6 — when the cluster_list cell is warm, the helper returns
// useCluster=true + gate=0 (cell-warm fast-path) — no synchronous
// decode on the customer goroutine.
//
// AC-P3.2.1 (mechanism) — when the cell is cold, the helper returns
// useCluster=false + gate=8 (cold-fallback). The customer goroutine
// pays NO decode cost (this is the customer-priority invariant per
// `feedback_customer_priority_over_refresher`).
//
// AC-P3.2.10 — counter observability: clusterListCellWarmTotal +
// clusterListCellColdFallbackTotal increment correctly per code path.

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

// TestClusterListCollapse_ColdMissReturnsGate8 — the FIRST customer
// call hits an unpopulated cell. The helper MUST return useCluster=false
// + gate=8 (cold-fallback). The cold-fallback counter increments by 1.
func TestClusterListCollapse_ColdMissReturnsGate8(t *testing.T) {
	rw := newClusterListWatcher(t)
	_ = rw
	withClusterListCollapseEnabledForTest(t)
	cache.ResetClusterListCellCountersForTest()
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		// Wait briefly for any in-flight async populate goroutine to
		// finish (lifetime bounded by clusterListAsyncPopulateTimeout
		// or the dispatched goroutine's natural exit; we give it
		// 200ms to let the populate complete cleanly), then reset.
		time.Sleep(200 * time.Millisecond)
		ResetClusterListAsyncStateForTest()
	})

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
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

	tmp, useCluster, gate := attemptClusterListCollapse(
		ownClusterCtx(ctx), clusterListLogger(t), apiCall, map[string]any{},
		endpoints.Endpoint{}, store, true)

	if useCluster {
		t.Fatalf("cold-miss: expected useCluster=false; got useCluster=true gate=%d", gate)
	}
	if gate != 8 {
		t.Fatalf("cold-miss: expected gate=8; got gate=%d (tmp=%v)", gate, tmp)
	}
	warm, cold := cache.ClusterListCellCounters()
	if warm != 0 {
		t.Errorf("cell-warm counter bumped on cold-miss path: warm=%d want 0", warm)
	}
	if cold != 1 {
		t.Errorf("cell-cold counter: got %d want 1", cold)
	}

	// Wait briefly for the async populate to land — assert it warms
	// the cell, so a SECOND call hits the warm fast-path. The
	// production-grade test doesn't depend on timing; we drive
	// populateClusterListCellSync directly to make this deterministic.
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	clusterCall := buildClusterListCall(apiCall, endpoints.Endpoint{}, f1WidgetsGVR)
	if !populateClusterListCellSync(ctx, clusterListLogger(t), apiCall,
		f1WidgetsGVR, contentKey, clusterCall, store) {
		t.Fatalf("populateClusterListCellSync returned false — async populate path would also fail")
	}

	// Second call — cell-warm fast-path.
	tmp2, useCluster2, gate2 := attemptClusterListCollapse(
		ownClusterCtx(ctx), clusterListLogger(t), apiCall, map[string]any{},
		endpoints.Endpoint{}, store, true)
	if !useCluster2 {
		t.Fatalf("post-populate: expected useCluster=true; got useCluster=false gate=%d", gate2)
	}
	if gate2 != 0 {
		t.Fatalf("post-populate: expected gate=0 (cell-warm); got gate=%d", gate2)
	}
	if len(tmp2) != 1 {
		t.Fatalf("post-populate: expected 1 cluster-scope call; got %d", len(tmp2))
	}
	warm2, _ := cache.ClusterListCellCounters()
	if warm2 != 1 {
		t.Errorf("cell-warm counter after warm hit: got %d want 1", warm2)
	}
}

// TestClusterListCollapse_AsyncPopulateBoundedConcurrency — the
// inflight-cell dedup prevents concurrent populate goroutines from
// spawning for the same cell. We can't directly assert no second
// goroutine without instrumenting; instead, we assert the FIRST
// customer's cold-miss return is non-blocking (returns within ms,
// not waiting on the populate). The customer goroutine's wall-clock
// MUST stay tiny regardless of populate cost.
func TestClusterListCollapse_CustomerPathNonBlocking(t *testing.T) {
	rw := newClusterListWatcher(t)
	_ = rw
	withClusterListCollapseEnabledForTest(t)
	cache.ResetClusterListCellCountersForTest()
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		// Wait briefly for any in-flight async populate goroutine to
		// finish (lifetime bounded by clusterListAsyncPopulateTimeout
		// or the dispatched goroutine's natural exit; we give it
		// 200ms to let the populate complete cleanly), then reset.
		time.Sleep(200 * time.Millisecond)
		ResetClusterListAsyncStateForTest()
	})

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
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

	t0 := time.Now()
	_, _, gate := attemptClusterListCollapse(
		ownClusterCtx(ctx), clusterListLogger(t), apiCall, map[string]any{},
		endpoints.Endpoint{}, store, true)
	elapsed := time.Since(t0)
	// AC-P3.2.1 reference: customer goroutine MUST return in well
	// under 100ms on the cold-miss path (sync.Map.Load + log emit only).
	const customerPathCeiling = 100 * time.Millisecond
	if elapsed > customerPathCeiling {
		t.Fatalf("AC-P3.2.1 mechanism FAIL: customer cold-miss returned in %v > %v — "+
			"customer goroutine likely blocked on synchronous decode. gate=%d",
			elapsed, customerPathCeiling, gate)
	}
	if gate != 8 {
		t.Errorf("cold-miss: expected gate=8; got gate=%d", gate)
	}
}
