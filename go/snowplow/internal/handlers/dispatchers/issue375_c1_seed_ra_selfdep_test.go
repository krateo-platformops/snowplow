// issue375_c1_seed_ra_selfdep_test.go — #375 C1 (review of #395): the SEED RESTAction's
// CR self-dep.
//
// seedOneRestaction reads the RESTAction (seedObjectsGetFn) on the OUTER ctx, before the
// L1 key is known, and Records its self-dep only AFTER the terminal Put. With resCtx built
// by WithL1KeyContext the sink's startSeq came after that read and the RA coordinate was
// not in the sink, so for the post-readyz modes (keepwarm, gvr-discovered, whose terminal
// Put is a PutIfGen since #394) an edit of the RA in [read, PutIfGen] was invisible on a
// cold cell: the self dirty-mark finds no edge, and the old-spec body is Put with no
// remark. The fix mirrors the customer handler: DepGenEpochNow() before the read, and
// WithL1KeyContextFromEpoch(…, the RA's own DepKey) at the resCtx site.
//
// The arm drives the REAL seedOneRestaction (real tail seedRestactionResolveAndPutProd,
// real resolved-L1 store, real informer) with a REAL RESTAction UPDATE through the dynamic
// client between the read and the Put. NEUTER: resCtx back to WithL1KeyContext(ctx, key)
// → the edit sub-arms go RED (0 remarks).

package dispatchers

import (
	"context"
	"sync"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
)

func TestIssue375_C1_SeedRESTActionSelfDep_EditInReadToPutWindow_Remarks(t *testing.T) {
	for _, mode := range []seedScopeMode{seedModeKeepwarm, seedModeGVRDiscovered} {
		for _, edit := range []bool{true, false} {
			name := mode.String() + "/no-edit_zero-remarks"
			if edit {
				name = mode.String() + "/edit-in-window_remarks-once"
			}
			t.Run(name, func(t *testing.T) {
				dyn := h1BuildWatcherWithRA(t)
				var mu sync.Mutex
				remarks := map[string]map[string]int{}
				t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
					mu.Lock()
					defer mu.Unlock()
					if remarks[k] == nil {
						remarks[k] = map[string]int{}
					}
					remarks[k][reason]++
				}))
				cache.ResetUnguardedPutTotalForTest()

				ref := templatesv1.ObjectReference{
					Reference:  templatesv1.Reference{Name: h1RAName, Namespace: h1NS},
					APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
					Resource:   h1RAGVR.Resource,
				}
				origGet, origTail := seedObjectsGetFn, seedRestactionResolveAndPutFn
				t.Cleanup(func() { seedObjectsGetFn, seedRestactionResolveAndPutFn = origGet, origTail })
				reads := 0
				seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
					reads++
					return objects.Result{GVR: h1RAGVR, Unstructured: h1RAUnstructured()} // the RA@old
				}
				var seedKey string
				seedRestactionResolveAndPutFn = func(
					ctx, resCtx context.Context, cr *templatesv1.RESTAction, r templatesv1.ObjectReference,
					authnNS, key string, handle cacheHandle, inputs *cache.ResolvedKeyInputs, got objects.Result,
					stageErrSink *cache.StageErrorSink, extTouchedSink *cache.ExternalTouchedSink,
				) error {
					seedKey = key
					if edit {
						// The user edits the RA AFTER the seed read it and BEFORE the Put.
						editRA(t, dyn, "new")
					}
					return seedRestactionResolveAndPutProd(ctx, resCtx, cr, r, authnNS, key, handle, inputs, got, stageErrSink, extTouchedSink)
				}

				if err := seedOneRestaction(seedCohortCtx(h1User), "cohort-h1", ref, h1NS, mode); err != nil {
					t.Fatalf("seedOneRestaction(%v): %v", mode, err)
				}
				if reads != 1 || seedKey == "" {
					t.Fatalf("PRECONDITION: the seed must read the RA once and reach its Put tail; reads=%d key=%q", reads, seedKey)
				}
				if _, ok := cache.ResolvedCache().GetNoTouch(seedKey); !ok {
					t.Fatalf("PRECONDITION: the cold seed's terminal PutIfGen must be ACCEPTED (the remark only runs on accept)")
				}
				mu.Lock()
				defer mu.Unlock()
				if n := cache.UnguardedPutTotal(); n != 0 {
					t.Fatalf("#375 C1: the seed's terminal Put ran on a nil sink (unguarded_put_total=%d)", n)
				}
				got := remarks[seedKey]
				if edit {
					if got["moved"] != 1 || got["nil_sink"] != 0 {
						t.Fatalf("#375 C1 RED (%v): the RESTAction was edited in [seed read, PutIfGen] on a COLD cell, but "+
							"the seed Put remarked %v (want exactly 1 'moved'). The sink must start before the RA read and "+
							"pre-declare the RA's own coordinate", mode, got)
					}
					return
				}
				if len(got) != 0 {
					t.Fatalf("#375 C1 anti-amp RED (%v): no RA edit, yet the seed Put remarked %v", mode, got)
				}
			})
		}
	}
}
