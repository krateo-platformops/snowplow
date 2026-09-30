// apistage_s189_gen_guard_test.go — #189 per-carrier real-race REFUSAL arm for
// the apistage content-serve carrier (apistage.go:604 CaptureGen → :648 PutIfGen).
//
// Mirrors the widgets/restactions httptest arms. The apistage MISS-branch resolve
// runs through the dispatchViaInformerFn seam, which sits EXACTLY between
// CaptureGen(contentKey) and PutIfGen(contentKey, …, contentGen0). The stub runs
// the REAL dispatch (so the request still serves a correct envelope), then
// DELETE-evicts the content key — bumping its generation past the captured value
// — so the tail PutIfGen MUST refuse: the pre-delete body is not resurrected and
// put_refused_generation_moved bumps. The apistage content Put is UN-GATED
// (identity-free envelope, no stage/external/uaf sinks), so a refusal is
// unambiguously the generation guard. Discriminator is the refusal (counter +
// cell-absent), body-independent; catches a wrong capture key (M1) and a
// capture-after-dispatch (M2). RED-captured by neutering the guard.

package api

import (
	"context"
	"net/http"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestApistageContentServe_S189_ResolveRacingDelete_RefusedNotResurrected(t *testing.T) {
	rw := newF1Watcher(t)
	_ = rw
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	refusedBefore := store.Stats().PutRefusedGenerationMovedTotal

	// Seam the MISS-branch resolve: run the REAL dispatch (correct envelope, so the
	// request still serves), then DELETE-evict the content key — this lands BETWEEN
	// the MISS branch's CaptureGen(contentKey) and its PutIfGen, bumping the
	// generation past the captured value.
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := dispatchViaInformer(ctx, call)
		store.Put(contentKey, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
		store.DeleteForTest(contentKey)
		return raw, ok
	}

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}))
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/" + f1WidgetsGVR.Group + "/" + f1WidgetsGVR.Version + "/" + f1WidgetsGVR.Resource,
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, served, ok := apistageContentServe(ctx, store, call, false)
	if !ok {
		t.Fatalf("#189 apistage A5: apistageContentServe ok=false; setup broken (served=%v)", served)
	}
	if !served {
		t.Fatalf("#189 apistage A5: the request must still SERVE from the fresh resolve (served=false)")
	}
	if got := store.Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
		t.Fatalf("#189 apistage A5 RED: the content PutIfGen (apistage.go:648) was NOT refused (refused_total %d->%d) — a DELETE during the resolve must refuse the write; a wrong key (M1) or capture-after-dispatch (M2) resurrects here.", refusedBefore, got)
	}
	if _, present := store.Get(contentKey); present {
		t.Fatalf("#189 apistage A5 RED: the DELETE-evicted content cell was RESURRECTED under key %q", contentKey)
	}
}
