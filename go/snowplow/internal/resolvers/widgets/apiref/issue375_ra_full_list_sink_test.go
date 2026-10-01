// issue375_ra_full_list_sink_test.go — #375 (TL-approved scope note 1): the RAFullList
// cell (raKey) is Put under fullCtx, whose dep-gen sink holds the UNPAGINATED resolve's
// deps (the body the cell is built from) and whose startSeq is that resolve's entry.
// Pre-fix the two PutRAFullListIfGen calls (ra_full_list.go:401/:540) passed the request
// (widget) ctx: the wrong sink.
//
// The resolveRA param is the real seam between raFullListServe's raKey capture and its
// tail PutRAFullListIfGen, the same one the #323 arm uses. Dep events are real
// OnObjectEvents (OnUpdate shim). NEUTER: pass ctx instead of fullCtx at :401/:540.

package apiref

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var g375Dep = schema.GroupVersionResource{Group: "deps.example.io", Version: "v1", Resource: "things"}

func raKey375(raName string) string {
	return cache.ComputeKey(cache.RAFullListKeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		"krateo-system", raName, "C:crb-a-f6-uid", nil))
}

type remarkTally struct {
	mu      sync.Mutex
	byKey   map[string]int
	reasons map[string]map[string]int
}

func tally375(t *testing.T) *remarkTally {
	r := &remarkTally{byKey: map[string]int{}, reasons: map[string]map[string]int{}}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, reason string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.byKey[k]++
		if r.reasons[k] == nil {
			r.reasons[k] = map[string]int{}
		}
		r.reasons[k][reason]++
	}))
	return r
}

// (1) The unpaginated resolve reads a dep, the dep churns inside it, and the raKey Put
// must remark raKey as "moved" through its OWN sink. With no outer sink on the request
// ctx, the neutered Put (request ctx) is a nil-sink drift instead (reason nil_sink,
// unguarded_put_total+1), which fails the assertions → RED.
func TestIssue375_RAFullListCellPut_ChecksFullCtxSink_Moved(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	newF6Watcher(t, f6BuildFixture()...)
	rem := tally375(t)
	const raName = "compositions-panels-375a"
	raKey := raKey375(raName)

	var calls atomic.Int64
	base := stubResolveRA(t, panelDict(40), &calls)
	resolve := func(rctx context.Context, perPage, page int) (map[string]any, error) {
		full, err := base(rctx, perPage, page)
		if perPage == 0 { // the UNPAGINATED full resolve under fullCtx
			cache.Deps().Record(rctx, cache.L1KeyFromContext(rctx), g375Dep, "ns", "thing")
			cache.Deps().OnUpdate(g375Dep, "ns", "thing") // churn inside [read, Put]
		}
		return full, err
	}
	unguarded0 := cache.Deps().Stats().UnguardedPutTotal
	if _, _, err := raFullListServe(ctxWithUser(t), gvr(), "krateo-system", raName, ra(raSliceJQ), 10, 1, nil, resolve); err != nil {
		t.Fatalf("raFullListServe: %v", err)
	}
	if _, present := cache.ResolvedCache().Get(raKey); !present {
		t.Fatalf("setup: the raKey cell was not stored")
	}
	if got := rem.reasons[raKey]["moved"]; got != 1 {
		t.Fatalf("#375 RAFullList RED: the raKey Put must remark once as 'moved' from the fullCtx sink "+
			"(the unpaginated resolve's own dep churned), got reasons=%v", rem.reasons[raKey])
	}
	if d := cache.Deps().Stats().UnguardedPutTotal - unguarded0; d != 0 {
		t.Fatalf("#375 RAFullList RED: the raKey Put was a NIL-sink drift (+%d unguarded) — it must be Put "+
			"under fullCtx (WithL1KeyContext(raKey)), not the request ctx", d)
	}
}

// (2) Precision: the WIDGET resolve's own dep churns BEFORE raFullListServe starts its
// unpaginated resolve, and the raKey cell never read it. The raKey Put must NOT remark.
// Under the neuter it checks the widget sink (earlier startSeq, holding the widget dep),
// so it remarks a false positive → RED.
func TestIssue375_RAFullListCellPut_ChecksFullCtxSink_NoWidgetDepFP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	newF6Watcher(t, f6BuildFixture()...)
	rem := tally375(t)
	const raName = "compositions-panels-375b"
	raKey := raKey375(raName)

	wctx := cache.WithL1KeyContext(ctxWithUser(t), "widget-375b") // the widget resolve entry
	cache.Deps().Record(wctx, "widget-375b", g375Dep, "ns", "widget-only")
	cache.Deps().OnUpdate(g375Dep, "ns", "widget-only") // moves AFTER the widget's startSeq

	var calls atomic.Int64
	base := stubResolveRA(t, panelDict(40), &calls)
	if _, _, err := raFullListServe(wctx, gvr(), "krateo-system", raName, ra(raSliceJQ), 10, 1, nil, base); err != nil {
		t.Fatalf("raFullListServe: %v", err)
	}
	if _, present := cache.ResolvedCache().Get(raKey); !present {
		t.Fatalf("setup: the raKey cell was not stored")
	}
	if got := rem.byKey[raKey]; got != 0 {
		t.Fatalf("#375 RAFullList RED: the raKey Put remarked %d times for a dep only the WIDGET resolve "+
			"read (moved before the unpaginated resolve began) — it is checking the widget's sink, not fullCtx's",
			got)
	}
}

// (3) #375 C2 — the RAFullList cell's RESTAction SELF-dep. The RA is fetched under the
// WIDGET ctx (apiref resolve.go) BEFORE fullCtx exists, and raKey Records the RA's own
// coordinate only AFTER the PutRAFullListIfGen. Pre-fix fullCtx's sink started at its own
// creation and never held the RA coordinate, so an RA spec edit during a COLD raKey
// resolve left raKey stale with no remark (its dirty-mark finds no edge on a cold cell),
// and the widget's later re-resolve fast-paths and slices the stale cell.
//
// Two windows, both with a real dep event on the RA coordinate:
//   - during-unpaginated-resolve: the edit lands inside resolveRA(perPage=0) under fullCtx;
//   - before-fullCtx: the edit lands after the widget sink started (≈ the RA read under
//     the widget ctx) but before raFullListServe builds fullCtx — only an epoch inherited
//     from the widget sink sees it.
//
// Each must remark raKey exactly once as "moved", through the refresh hook, while raKey
// is RESIDENT, carrying the RA's GVR as a trigger. That is what converges raKey fresh:
// the refresher re-resolves a resident RAFullList cell from a fresh RA fetch
// (resolve_populate.go resolveRAFullListForRefresh) instead of skipping it.
//
// NEUTER: fullCtx back to WithL1KeyContext(ctx, raKey) → both windows RED (0 remarks).
func TestIssue375_C2_RAFullListCell_RASelfDepEdit_RemarksRAKey(t *testing.T) {
	for _, tc := range []struct {
		name          string
		editInResolve bool
	}{
		{"during-unpaginated-resolve", true},
		{"before-fullCtx_after-widget-read", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			t.Cleanup(cache.ResetDepsForTest)
			newF6Watcher(t, f6BuildFixture()...)
			rem := tally375(t)
			raName := "compositions-panels-375c2-" + tc.name
			raKey := raKey375(raName)

			type hookEv struct {
				gvr      schema.GroupVersionResource
				resident bool
			}
			var hmu sync.Mutex
			var hookEvs []hookEv
			cache.Deps().SetRefreshHook(func(k string, g schema.GroupVersionResource) {
				if k != raKey {
					return
				}
				_, resident := cache.ResolvedCache().GetNoTouch(k)
				hmu.Lock()
				hookEvs = append(hookEvs, hookEv{g, resident})
				hmu.Unlock()
			})

			// The widget resolve entry: its sink starts here, before the RA read.
			wctx := cache.WithL1KeyContext(ctxWithUser(t), "widget-375c2")
			if !tc.editInResolve {
				cache.Deps().OnUpdate(gvr(), "krateo-system", raName) // RA edited after the widget read it
			}
			var calls atomic.Int64
			base := stubResolveRA(t, panelDict(40), &calls)
			resolve := func(rctx context.Context, perPage, page int) (map[string]any, error) {
				full, err := base(rctx, perPage, page)
				if perPage == 0 && tc.editInResolve {
					cache.Deps().OnUpdate(gvr(), "krateo-system", raName) // RA edited mid cold resolve
				}
				return full, err
			}
			if _, present := cache.ResolvedCache().GetNoTouch(raKey); present {
				t.Fatalf("PRECONDITION: raKey must be COLD")
			}
			if _, _, err := raFullListServe(wctx, gvr(), "krateo-system", raName, ra(raSliceJQ), 10, 1, nil, resolve); err != nil {
				t.Fatalf("raFullListServe: %v", err)
			}
			if _, present := cache.ResolvedCache().GetNoTouch(raKey); !present {
				t.Fatalf("PRECONDITION: the raKey cell was not stored (the remark only runs on an accepted Put)")
			}
			rem.mu.Lock()
			got := rem.reasons[raKey]
			rem.mu.Unlock()
			if got["moved"] != 1 || got["nil_sink"] != 0 {
				t.Fatalf("#375 C2 RED (%s): the RESTAction was edited during a COLD raKey fill but the raKey Put "+
					"remarked %v (want exactly 1 'moved'): fullCtx must inherit the widget sink's epoch and "+
					"pre-declare the RA's own coordinate", tc.name, got)
			}
			hmu.Lock()
			defer hmu.Unlock()
			sawTrigger := false
			for _, ev := range hookEvs {
				if ev.gvr == gvr() && ev.resident {
					sawTrigger = true
				}
			}
			if !sawTrigger {
				t.Fatalf("#375 C2 (%s): the raKey remark must reach the refresh hook while raKey is RESIDENT with the "+
					"RA GVR as trigger (else the refresher skips it and raKey never converges); hook events=%v",
					tc.name, hookEvs)
			}
		})
	}
}
