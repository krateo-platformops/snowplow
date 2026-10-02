package dispatchers

// reseed_nodrop_test.go — #258 accumulator NO-DROP arm (C5 "edge-triggered, no
// dropped event"). A second RBAC flush that arrives WHILE an rbac-shift reseed
// run is draining must not be lost: its subjects end up reseeded, here in the
// coalesced follow-up run that client-go's dirty-on-re-add schedules after the
// current run's Done.
//
// Driven through the REAL pieces: the production engine hook
// (registerEngineRBACShiftHook → accumulator Merge + payload-free enqueueScope),
// the real client-go workqueue (Get / Done, dirty re-queue of an in-flight item),
// and the real scope handler (makeBootScopeHandler → accumulator Drain →
// rePrewarmRBACShift → enumerateRotatedResidentTargets over the live binding
// index → reseedTargets). Only the seed primitive is stubbed, to record which
// identity each reseed ran under and to fire the second flush mid-run.
//
// RED: reset the accumulator after the run instead of at take (handler:
// Drain → run → Drain-and-discard) — carol's mid-run merge is wiped, the dirty
// follow-up drains an empty set, and carol is never reseeded.

import (
	"context"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/util/workqueue"
)

func TestS258_AccumulatorNoDrop_SecondFlushMidDrain(t *testing.T) {
	s258BuildWatcher(t, s258WideningUser) // alice AND carol each hold a binding
	stubWidgetResolve(t)

	// An isolated engine for this test: fresh queue + accumulator, swapped in as
	// the singleton the hook and the handler both resolve. Restored on cleanup.
	_ = prewarmEngineSingleton() // make sure the Once has fired before the swap
	prev := prewarmEngineInstance
	e := &prewarmEngine{
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[prewarmScope](),
		),
		rbacShift: cache.NewRBACShiftAccumulator(),
		yieldPoll: defaultEngineYieldPoll,
	}
	prewarmEngineInstance = e
	t.Cleanup(func() {
		e.queue.ShutDown()
		prewarmEngineInstance = prev
	})
	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	registerEngineRBACShiftHook(e)

	navHarv := newNavWidgetHarvester()
	w := reseedWidgetEntry()
	navHarv.harvestNavWidget(w.W, w.GVR, w.PerPage, w.Page, w.KeyPerPage, w.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}
	handler := makeBootScopeHandler(deps)

	// Seed stub: record each reseed's identity; on the FIRST reseed (alice, run 1,
	// i.e. after run 1 drained the accumulator) fire the second flush (carol).
	var mu sync.Mutex
	var seededUsers []string
	fired := false
	orig := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = orig })
	seedOneWidgetFn = func(ctx context.Context, _ navWidgetEntry, _ string, _ seedScopeMode) error {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			t.Errorf("reseed ran without a cohort identity: %v", err)
			return nil
		}
		mu.Lock()
		seededUsers = append(seededUsers, ui.Username)
		fireNow := !fired
		fired = true
		mu.Unlock()
		if fireNow {
			if n := e.rbacShift.Len(); n != 0 {
				t.Errorf("precondition: run 1 must have drained the accumulator before reseeding; pending=%d", n)
			}
			cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: s258WideningUser}})
		}
		return nil
	}

	// Flush 1: alice rotates.
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})
	if got := e.queue.Len(); got != 1 {
		t.Fatalf("flush 1 must enqueue exactly one rbac-shift scope; queue len=%d", got)
	}

	// Worker loop: process until the queue is empty (bounded).
	runs := 0
	for e.queue.Len() > 0 {
		if runs >= 5 {
			t.Fatalf("rbac-shift did not settle within 5 runs (no coalescing?)")
		}
		item, shutdown := e.queue.Get()
		if shutdown {
			t.Fatal("queue shut down unexpectedly")
		}
		if item.kind != scopeKindRBACShift {
			t.Fatalf("unexpected scope %q", item.key())
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := handler(ctx, item); err != nil {
			t.Fatalf("handler run %d: %v", runs+1, err)
		}
		cancel()
		e.queue.Done(item) // a mid-run re-add was marked dirty → re-queued here
		runs++
	}

	mu.Lock()
	defer mu.Unlock()
	count := map[string]int{}
	for _, u := range seededUsers {
		count[u]++
	}
	if count[a1Alice] != 1 {
		t.Errorf("alice (flush 1) reseeded %d times, want 1 (seeded=%v)", count[a1Alice], seededUsers)
	}
	if count[s258WideningUser] != 1 {
		t.Fatalf("NO-DROP RED: carol's flush arrived mid-drain and was reseeded %d times, want 1 — the "+
			"rotation was lost (seeded=%v, runs=%d)", count[s258WideningUser], seededUsers, runs)
	}
	if runs != 2 {
		t.Errorf("want exactly 2 runs (the drain + one coalesced follow-up), got %d", runs)
	}
	if n := e.rbacShift.Len(); n != 0 {
		t.Errorf("accumulator must be empty after the follow-up run; pending=%d", n)
	}
}
