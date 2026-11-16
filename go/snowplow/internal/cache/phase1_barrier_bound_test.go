// phase1_barrier_bound_test.go — Fix 1b (task #43) falsifiers for the
// BOUNDED WaitAllInformersSynced exit policy.
//
// The defect Fix 1b cures: the barrier blocked in WaitForCacheSync on the
// OUTER PHASE1_TIMEOUT_SECONDS ctx, so a SINGLE never-HasSynced informer
// held /readyz 503 for the full 900s budget (krateo-enterprise: observed
// prewarm_complete.elapsed_ms = 900463 ms, == the 900s default to the ms).
//
// C2 (the discriminating control): with a deliberately-never-syncing
// informer in the set, the barrier MUST return at ~ (passGrace +
// quiescence), NOT at ~ PHASE1_TIMEOUT*1000 and NOT ∞. A single-snapshot /
// outer-ctx-only implementation FAILS this — it would block on the outer
// ctx for the whole budget. This is the test that would have caught the
// 900s defect.
//
// C9 (-race): the per-pass child ctx + the set-stability last-change
// timestamp + the post-pass unsynced-GVR snapshot all touch shared rw
// state while late EnsureResourceType registrations land concurrently —
// run under -race to prove no data race (feedback_shared_vs_copy_is_a_
// concurrency_change).

package cache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// stuckGVR is a composition-style GVR whose initial LIST always ERRORS —
// modeling the krateo-enterprise `informer-fallthrough-not-synced`
// informer whose initial LIST never succeeds (403/stall), so its Reflector
// retries with backoff forever and HasSynced() stays false.
//
// NOTE on WHY error-not-block: the dynamicfake client serializes every
// action across ALL informers under one process-wide `Fake` mutex
// (client-go testing/fake.go Invokes holds c.Lock() for the whole reactor
// body). A reactor that BLOCKS would hold that lock and starve the HEALTHY
// informers' LISTs too — a test-harness artifact, not real behavior (in a
// real cluster each informer has independent transport). Returning an
// error releases the lock immediately, so the stuck informer never syncs
// while the healthy ones sync normally — the fidelity the C2 control needs.
func stuckGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1",
		Resource: "stuckthing",
	}
}

// barrierBoundListKinds = the phase1 seeds (which sync normally) PLUS the
// stuck GVR's List kind, so registering its informer does NOT panic the
// fake client — the informer is created and starts, but its LIST errors.
func barrierBoundListKinds() map[schema.GroupVersionResource]string {
	m := phase1ListKinds()
	m[stuckGVR()] = "StuckThingList"
	return m
}

// stuckListReactor returns a LIST reactor that always errors (never holds
// the shared Fake lock across a block), so the stuck GVR's informer never
// reaches HasSynced while every other informer syncs normally.
func stuckListReactor() k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: stuckGVR().Group, Resource: stuckGVR().Resource},
			"", context.Canceled)
	}
}

// TestWaitAllInformersSynced_GoodEnoughBoundsAStuckInformer is C2 — the
// discriminating control. One informer never syncs; the rest do. The
// barrier must return nil ("good enough") in O(passGrace + quiescence),
// far below the outer PHASE1_TIMEOUT budget, naming the stuck GVR as
// deferred to the lazy-register fallthrough.
func TestWaitAllInformersSynced_GoodEnoughBoundsAStuckInformer(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	// Bounds: per-pass grace 3s comfortably exceeds the healthy fake
	// informers' sync time (they LIST successfully and reach HasSynced
	// independent of the erroring stuck informer); quiescence 1s. The
	// OUTER budget is a deliberately LARGE 60s so a regression that blocks
	// on the outer ctx (the pre-1b defect) lands at ~60s and trips the 15s
	// ceiling below.
	t.Setenv("PHASE1_SYNC_PASS_GRACE_SECONDS", "3")
	t.Setenv("PHASE1_SYNC_QUIESCENCE_SECONDS", "1")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())

	// The stuck GVR's LIST always errors → its Reflector never reaches
	// HasSynced, while the healthy seeds sync normally (the error reactor
	// does not hold the shared Fake lock across a block — see stuckGVR doc).
	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// Healthy seeds (sync normally) + the stuck informer.
	rw.RegisterMetaQuerySeeds(context.Background())
	rw.EnsureResourceType(stuckGVR())

	// OUTER budget = 60s backstop. If the barrier were still blocking on
	// the outer ctx (the pre-1b defect), it would not return until 60s and
	// trip the ceiling below.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	if err := rw.WaitAllInformersSynced(ctx); err != nil {
		t.Fatalf("barrier must return nil (good enough) with a stuck informer; got %v", err)
	}
	elapsed := time.Since(start)

	// DISCRIMINATING ceiling: ~ grace(1s)+quiescence(1s) plus pass/poll
	// slack. A pre-1b (outer-ctx-only) implementation returns at ~60s and
	// fails here. Generous 15s ceiling still proves O(tens of seconds),
	// NOT O(PHASE1_TIMEOUT).
	if elapsed > 15*time.Second {
		t.Fatalf("barrier returned in %v — expected ~grace+quiescence (~2s), "+
			"NOT the outer budget. The pre-1b defect (block on outer ctx) would "+
			"land at ~60s.", elapsed)
	}

	// The stuck GVR must still be registered (deferred, not dropped) and
	// must still be UNSYNCED — confirming the barrier returned "good
	// enough" rather than waiting it out.
	if !rw.IsRegistered(stuckGVR()) {
		t.Fatalf("stuck GVR must remain registered (deferred to lazy-register fallthrough)")
	}
	if rw.IsSynced(stuckGVR()) {
		t.Fatalf("test setup error: stuck GVR unexpectedly synced — the blocking LIST reactor did not engage")
	}

	// The healthy seeds MUST be synced (good-enough is not a free pass —
	// every informer that CAN sync has).
	for _, g := range cache.MetaQuerySeeds() {
		if !rw.IsSynced(g) {
			t.Fatalf("good-enough must still require every sync-able informer to be synced; %v is not", g)
		}
	}
}

// TestWaitAllInformersSynced_HappyPathUnchanged guards against a
// regression where the new exit policy breaks the all-synced fast path:
// with NO stuck informer the barrier must still return via the
// sync_wait_complete path quickly.
func TestWaitAllInformersSynced_HappyPathUnchanged(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("PHASE1_SYNC_PASS_GRACE_SECONDS", "5")
	t.Setenv("PHASE1_SYNC_QUIESCENCE_SECONDS", "1")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), phase1ListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.RegisterMetaQuerySeeds(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	if err := rw.WaitAllInformersSynced(ctx); err != nil {
		t.Fatalf("happy-path barrier must return nil; got %v", err)
	}
	// All informers sync in the fake client well under the 1s quiescence +
	// a single grace pass — assert it returned promptly (no good-enough
	// quiescence stall needed when everything is already synced).
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("happy path returned in %v — too slow; the all-synced fast path regressed", elapsed)
	}
	for _, g := range cache.MetaQuerySeeds() {
		if !rw.IsSynced(g) {
			t.Fatalf("happy path: %v must be synced at barrier return", g)
		}
	}
}

// TestWaitAllInformersSynced_RaceWithStuckAndLateRegistrations is C9 — the
// -race falsifier. It combines the stuck informer (forcing the per-pass
// child-ctx + good-enough path) with concurrent late EnsureResourceType
// registrations (forcing the set-stability last-change-timestamp re-pass
// path). Run with -race: the per-pass cancel, the lastChange touch, and
// the post-pass unsynced-GVR snapshot all read/write shared rw state while
// the late-register goroutine mutates rw.informers concurrently.
func TestWaitAllInformersSynced_RaceWithStuckAndLateRegistrations(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("PHASE1_SYNC_PASS_GRACE_SECONDS", "1")
	t.Setenv("PHASE1_SYNC_QUIESCENCE_SECONDS", "1")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), barrierBoundListKinds())

	dyn.PrependReactor("list", stuckGVR().Resource, stuckListReactor())

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	rw.RegisterMetaQuerySeeds(context.Background())
	rw.EnsureResourceType(stuckGVR())

	// Concurrently register late GVRs while the barrier runs — exercises
	// the set-stability re-pass + lastChange touch against concurrent
	// rw.informers mutation.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, g := range lateGVRs() {
			rw.EnsureResourceType(g)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// OUTER budget = 60s backstop; the barrier must return on the
	// grace/quiescence policy far below it even while registrations are
	// still landing and one informer is permanently stuck.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	start := time.Now()
	if err := rw.WaitAllInformersSynced(ctx); err != nil {
		t.Fatalf("barrier must return nil (good enough) under concurrent registration + stuck informer; got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("barrier returned in %v under concurrent registration — expected the bounded grace/quiescence policy, not the outer budget", elapsed)
	}
	wg.Wait()

	// CONTRACT under good-enough: the barrier does NOT guarantee every
	// late informer finished syncing (that is the whole point — it defers
	// the still-syncing/stuck ones to lazy-register fallthrough + the
	// refresher). What it DOES guarantee: it returned bounded, and every
	// late GVR is at least REGISTERED (so the substrate exists for the
	// fallthrough/refresher to converge). The permanently-stuck GVR must
	// remain registered-but-unsynced.
	for _, g := range lateGVRs() {
		if !rw.IsRegistered(g) {
			t.Fatalf("late GVR %v must be registered — test setup error", g)
		}
	}
	if !rw.IsRegistered(stuckGVR()) {
		t.Fatalf("stuck GVR must remain registered (deferred to lazy-register fallthrough)")
	}
	if rw.IsSynced(stuckGVR()) {
		t.Fatalf("test setup error: stuck GVR unexpectedly synced")
	}
}
