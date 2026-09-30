// cluster_list_s323_gen_guard_test.go — #323 per-carrier real-race REFUSAL arm
// for the cluster_list content-cell carrier (populateClusterListCellSync:
// CaptureGen(contentKey) before the defensive resolve → PutIfGen at the tail).
//
// The dispatchViaInformerFn seam sits between the capture and the PutIfGen. The
// stub runs the REAL dispatch (correct envelope, so the shape check + materialise
// reach the tail), then DELETE-evicts the content key — bumping its generation
// past the captured value — so the tail PutIfGen REFUSES: the populate DECLINES
// (returns false), the pre-delete cluster-LIST body is not resurrected, and the
// refusal counter bumps. The cluster_list content Put is UN-GATED (identity-free
// substrate, no stage/external/uaf sinks), so a refusal is unambiguously the
// generation guard. Discriminator is the refusal (counter + cell-absent + ok=false),
// body-independent; catches a wrong capture key (M1) and a capture-after-resolve
// (M2). RED-captured by neutering the guard.

package api

import (
	"context"
	"net/http"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestClusterListCellSync_S323_ResolveRacingDelete_RefusedNotResurrected(t *testing.T) {
	rw := newClusterListWatcher(t)
	_ = rw
	withClusterListCollapseEnabledForTest(t)

	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	refusedBefore := store.Stats().PutRefusedGenerationMovedTotal

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
	log := clusterListLogger(t)
	clusterCall := buildClusterListCall(apiCall, endpoints.Endpoint{}, f1WidgetsGVR)

	// Seam the defensive resolve: run the REAL dispatch (correct envelope so the
	// shape check + materialise reach the tail), then DELETE-evict the content key
	// — landing between CaptureGen and the tail PutIfGen — bumping the generation
	// past the captured value.
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(dctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := dispatchViaInformer(dctx, call)
		store.Put(contentKey, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
		store.DeleteForTest(contentKey)
		return raw, ok
	}

	if populateClusterListCellSync(ctx, log, apiCall, f1WidgetsGVR, contentKey, clusterCall, store) {
		t.Fatalf("#323 cluster_list A5 RED: populateClusterListCellSync returned ok=true — a DELETE-eviction during the defensive resolve must DECLINE the populate (ok=false), not resurrect the pre-delete body")
	}
	if got := store.Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
		t.Fatalf("#323 cluster_list A5 RED: the tail PutIfGen was NOT refused (refused_total %d->%d) — a wrong capture key (M1) or a capture-after-resolve (M2) resurrects here.", refusedBefore, got)
	}
	if _, present := store.Get(contentKey); present {
		t.Fatalf("#323 cluster_list A5 RED: the DELETE-evicted cluster-LIST cell was RESURRECTED under key %q", contentKey)
	}
}
