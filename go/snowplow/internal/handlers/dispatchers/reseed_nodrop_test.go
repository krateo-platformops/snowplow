package dispatchers

// reseed_nodrop_test.go — #258 C5 "edge-triggered, no dropped rotation" arms,
// adopted from reviewer-416's probes (zz_probe416_test.go).
//
//   - TestProbe416_MidRunFlushNotLost: a flush that lands MID-RUN of the
//     rbac-shift scope re-arms exactly one follow-up and the follow-up's Drain
//     carries it (and only it). Drives the REAL engine hook, the REAL client-go
//     workqueue (dirty re-add of the in-flight item) and the REAL scope handler.
//     RED: reset the accumulator after the run instead of at take (M6c).
//   - TestProbe416_EnumerationCutDoesNotDropRotation: a scope ctx cut DURING the
//     enumeration (e.g. the 8m budget spent parked in engineYieldCheckpoint under
//     sustained customer traffic) must not drop the drained rotation. RED on
//     05506586 (re-merge only when the reseed loop returned a tail).

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestProbe416_MidRunFlushNotLost(t *testing.T) {
	s258BuildWatcher(t)
	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	e := prewarmEngineSingleton()
	e.rbacShift.Drain()
	for e.queue.Len() > 0 {
		it, _ := e.queue.Get()
		e.queue.Forget(it)
		e.queue.Done(it)
	}
	registerEngineRBACShiftHook(e)

	navHarv := newNavWidgetHarvester()
	w := reseedWidgetEntry()
	navHarv.harvestNavWidget(w.W, w.GVR, w.PerPage, w.Page, w.KeyPerPage, w.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	orig := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = orig })
	calls := 0
	seedOneWidgetFn = func(context.Context, navWidgetEntry, string, seedScopeMode) error {
		calls++
		if calls == 1 {
			cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: "bob"}}) // mid-run flush
		}
		return nil
	}

	// burst coalescing: two flushes before the worker picks up -> ONE queued item.
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: "dave"}})
	if n := e.queue.Len(); n != 1 {
		t.Fatalf("burst: queue len=%d want 1", n)
	}
	item, _ := e.queue.Get()
	if err := makeBootScopeHandler(deps)(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected alice's one resident target to be reseeded; calls=%d", calls)
	}
	e.queue.Done(item)
	if n := e.queue.Len(); n != 1 {
		t.Fatalf("mid-run flush must re-arm exactly one follow-up; queue len=%d", n)
	}
	item2, _ := e.queue.Get()
	defer e.queue.Done(item2)
	got := e.rbacShift.Drain()
	if !got.Rotated("bob", nil) {
		t.Fatalf("mid-run flush LOST: follow-up drain does not carry bob (len=%d)", got.Len())
	}
	if got.Rotated(a1Alice, nil) || got.Rotated("dave", nil) {
		t.Fatalf("follow-up drain re-carries already-processed subjects")
	}
}

func TestProbe416_EnumerationCutDoesNotDropRotation(t *testing.T) {
	s258BuildWatcher(t)
	e := prewarmEngineSingleton()
	e.rbacShift.Drain()
	t.Cleanup(func() {
		// the fix re-arms a scope on the shared singleton queue: leave it clean.
		e.rbacShift.Drain()
		for e.queue.Len() > 0 {
			it, _ := e.queue.Get()
			e.queue.Forget(it)
			e.queue.Done(it)
		}
	})
	navHarv := newNavWidgetHarvester()
	w := reseedWidgetEntry()
	navHarv.harvestNavWidget(w.W, w.GVR, w.PerPage, w.Page, w.KeyPerPage, w.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	var rs cache.RotatedSubjectSet
	cache.RegisterRBACShiftHook(func(r cache.RotatedSubjectSet) { rs = r })
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})
	if n := len(enumerateRotatedResidentTargets(context.Background(), deps, rs)); n != 1 {
		t.Fatalf("precondition: alice has %d resident targets, want 1", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = rePrewarmRBACShift(ctx, deps, rs)
	if e.rbacShift.Len() == 0 {
		t.Fatalf("DROPPED: the drained rotation (alice) was neither reseeded nor re-merged after a ctx cut during enumeration")
	}
}
