// issue1126_c3_reconcile_test.go — 1.12.6 C3 arms (design §5, §10 rows
// D2 / D2b, the off-path and the lock-order arm): the sampled reconcile audit.
//
// THE STRANDED PREMISE. Every eviction arm first MANUFACTURES a lost DELETE
// through the real pipeline: the dep-event worker is stopped (its queue
// shut), the object is deleted on the fake cluster, the real informer
// handler submits the coordinate — onto a shut queue, so it is dropped —
// and the entry is asserted STILL RESIDENT. That resident-after-delete
// state is the #187 shape and the behavioural RED on origin/main, where
// nothing but the TTL will ever touch it (the same premise, without the C3
// symbols, is RED there: see the report's c3-red-on-main transcript).
//
//	D2   — the periodic ticker (StartDepsReconcile, period 1 s) finds the
//	       stranded entry within one tick and the worker evicts it; a live
//	       sibling survives; evict_delete_total moves by exactly one.
//	D2c  — reconcileOnce counts exactly: sampled / probed (unique
//	       coordinates, cohort copies share one probe) / divergent /
//	       unknown (unregistered GVR is SKIPPED, never guessed) / a LIST
//	       entry (no name) is not probed; the second pass finds nothing.
//	OFF  — period "0" builds no ticker (the stranded entry stays);
//	       CACHE_ENABLED=false builds nothing and spawns no goroutine.
//	LOCK — a goroutine holding rw.mu and taking the store lock inside it
//	       runs concurrently with reconcileOnce; the audit must complete,
//	       which it can only do if it never nests the two locks.

package cache

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func c3Setup(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	resetDepWatchForTest()
	ResetDepsForTest()
	resetDepsReconcileForTest()
	t.Cleanup(func() {
		resetDepWatchForTest()
		ResetDepsForTest()
		resetDepsReconcileForTest()
	})
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {}) // dirty-marks go nowhere: no refresher
}

func c3Put(store *ResolvedCacheStore, key string, gvr schema.GroupVersionResource, ns, name string) {
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"k":"` + key + `"}`), Inputs: widgetInputs(gvr, ns, name)})
	Deps().Record(key, gvr, ns, name)
}

// c3Strand deletes ns/name on the fake cluster while the dep-event worker is
// STOPPED, so the real handler's submit lands on a shut queue and is lost.
// Returns with the indexer no longer holding the object and key asserted
// STILL RESIDENT (the stranded premise). The dep-watch singleton is then
// replaced so the audit's own submit reaches a live queue.
//
// created is the number of objects the arm created before calling: the
// informer delivers ADD notifications ASYNCHRONOUSLY after the indexer
// update createObj waits on, so the strand first waits until every ADD has
// been submitted and probed (the worker is then provably STARTED and its
// startOnce spent). Stopping before that would let a late ADD start the
// worker after the stop, and the DELETE would be delivered normally.
func c3Strand(t *testing.T, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string, store *ResolvedCacheStore, key string, created uint64) {
	t.Helper()
	deadline := time.Now().Add(harnessWaitBound)
	for DepWatchStatsSnapshot().ProbeExists < created {
		if time.Now().After(deadline) {
			t.Fatalf("premise: only %d of %d ADDs reached the worker within %s", DepWatchStatsSnapshot().ProbeExists, created, harnessWaitBound)
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitQueueIdle(t, harnessWaitBound)
	depWatchSingleton().stopWorker()
	deleteObj(t, rw, dyn, gvr, ns, name) // waits until the indexer no longer holds it
	time.Sleep(300 * time.Millisecond)   // give a wrongly-alive worker time to evict
	if _, alive := store.Get(key); !alive {
		t.Fatalf("premise: %s was evicted although the worker was stopped — the arm is not modelling "+
			"a lost DELETE", key)
	}
	if got := rw.probeObjectState(gvr, ns, name); got != objAbsent {
		t.Fatalf("premise: probe=%v, want objAbsent (the object is gone from a synced indexer)", got)
	}
	resetDepWatchForTest()
}

// --- D2 — the ticker finds the stranded entry and the worker evicts it -----

func TestIssue1126_D2_ReconcileTickerEvictsAStrandedEntry(t *testing.T) {
	c3Setup(t)
	t.Setenv(envDepsReconcilePeriodSeconds, "1")
	t.Setenv(envDepsReconcileSample, "512")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	// The ticker audits the PROCESS store (ResolvedCache()), like production.
	store := ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true")
	}
	Deps().SetStore(store)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	createObj(t, rw, dyn, gvr, "demo-system", "flex-y", "y")
	c3Put(store, "L1_flex-x", gvr, "demo-system", "flex-x")
	c3Put(store, "L1_flex-y", gvr, "demo-system", "flex-y")

	c3Strand(t, rw, dyn, gvr, "demo-system", "flex-x", store, "L1_flex-x", 2)
	before := Deps().Stats().EvictDeleteTotal

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartDepsReconcile(ctx)
	if !DepsReconcileStatsSnapshot().Started {
		t.Fatalf("D2: StartDepsReconcile did not start the ticker with period=1 and the cache on")
	}

	if !c2WaitGone(store, "L1_flex-x", harnessWaitBound) {
		s := DepsReconcileStatsSnapshot()
		t.Fatalf("D2 RED: the stranded entry L1_flex-x is still RESIDENT after %s of the reconcile ticker "+
			"(ticks=%d probed=%d divergence=%d unknown=%d panics=%d). Its DELETE was lost and no audit "+
			"caught it: the entry serves a deleted object until TTL (#187)",
			harnessWaitBound, s.Ticks, s.Probed, s.Divergence, s.Unknown, s.Panics)
	}
	if _, alive := store.Get("L1_flex-y"); !alive {
		t.Fatalf("D2 over-eviction: L1_flex-y (object PRESENT) was evicted by the audit")
	}
	s := DepsReconcileStatsSnapshot()
	if s.Ticks == 0 || s.Divergence == 0 {
		t.Fatalf("D2: ticks=%d divergence=%d — the eviction did not come through the audit", s.Ticks, s.Divergence)
	}
	if s.Panics != 0 {
		t.Fatalf("D2: reconcile_panics_total=%d", s.Panics)
	}
	// Bounded wait, not a single read: the worker counts AFTER deleteForDep.
	if got := c3WaitEvictDelete(before+1, harnessWaitBound); got != before+1 {
		t.Fatalf("D2: evict_delete_total moved by %d, want 1 — the audit must evict through the worker's "+
			"ABSENT verdict (the same path as an informer DELETE)", got-before)
	}
}

// --- D2c — reconcileOnce is exact ----------------------------------------------

func TestIssue1126_D2c_ReconcileOnceCountsExactlyAndSkipsUnknown(t *testing.T) {
	c3Setup(t)
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)

	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	unregistered := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}

	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	createObj(t, rw, dyn, gvr, "demo-system", "flex-y", "y")
	c3Put(store, "L1_flex-x_cohortA", gvr, "demo-system", "flex-x") // two cohorts, one coordinate
	c3Put(store, "L1_flex-x_cohortB", gvr, "demo-system", "flex-x")
	c3Put(store, "L1_flex-y", gvr, "demo-system", "flex-y")
	c3Put(store, "L1_button-z", unregistered, "demo-system", "button-z")                   // GVR never registered → UNKNOWN
	store.Put("L1_list", &ResolvedEntry{RawJSON: []byte(`[]`), Inputs: &ResolvedKeyInputs{ // LIST-scope: no name
		CacheEntryClass: "widgets", Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource, Namespace: "demo-system",
	}})

	c3Strand(t, rw, dyn, gvr, "demo-system", "flex-x", store, "L1_flex-x_cohortA", 2)
	if _, alive := store.Get("L1_flex-x_cohortB"); !alive {
		t.Fatalf("premise: cohort B evicted before the audit")
	}

	rep := reconcileOnce(store, rw, 512)
	if rep.Sampled != 5 {
		t.Fatalf("D2c: sampled=%d, want 5 (every resident entry visited)", rep.Sampled)
	}
	if rep.Probed != 3 {
		t.Fatalf("D2c: probed=%d, want 3 — unique coordinates flex-x, flex-y, button-z (the LIST entry has "+
			"no name and is not probed; cohort copies share one probe)", rep.Probed)
	}
	if rep.Divergent != 1 || rep.Unknown != 1 {
		t.Fatalf("D2c: divergent=%d unknown=%d, want 1/1 — flex-x is ABSENT; button-z's GVR is not watched "+
			"so its state is UNKNOWN and must be SKIPPED, never treated as absent", rep.Divergent, rep.Unknown)
	}
	if len(rep.Entries) != 2 {
		t.Fatalf("D2c: %d divergent entries, want 2 (both cohort copies of flex-x): %+v", len(rep.Entries), rep.Entries)
	}
	for _, e := range rep.Entries {
		if e.Name != "flex-x" || e.Namespace != "demo-system" || e.CacheEntryClass != "widgets" || e.KeyHash == "" {
			t.Fatalf("D2c: divergent row %+v is not the flex-x self entry", e)
		}
	}

	// The worker evicts both cohort copies off the single submitted coordinate.
	for _, k := range []string{"L1_flex-x_cohortA", "L1_flex-x_cohortB"} {
		if !c2WaitGone(store, k, harnessWaitBound) {
			t.Fatalf("D2c: %s still resident after the audit submitted its coordinate", k)
		}
	}
	for _, k := range []string{"L1_flex-y", "L1_button-z", "L1_list"} {
		if _, alive := store.Get(k); !alive {
			t.Fatalf("D2c over-eviction: %s evicted (present / unknown / list entries must be untouched)", k)
		}
	}
	// A second pass finds nothing divergent: the audit converges.
	rep2 := reconcileOnce(store, rw, 512)
	if rep2.Divergent != 0 || rep2.Sampled != 3 {
		t.Fatalf("D2c: second pass divergent=%d sampled=%d, want 0/3", rep2.Divergent, rep2.Sampled)
	}
	// The bounded sampler honours its limit.
	rep3 := reconcileOnce(store, rw, 2)
	if rep3.Sampled != 2 {
		t.Fatalf("D2c: sample limit 2 visited %d entries", rep3.Sampled)
	}
}

// --- OFF — period 0 builds no ticker; cache-off builds nothing --------------

func TestIssue1126_C3_Off_PeriodZeroBuildsNoTicker(t *testing.T) {
	c3Setup(t)
	t.Setenv(envDepsReconcilePeriodSeconds, "0")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	store := ResolvedCache()
	Deps().SetStore(store)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	c3Put(store, "L1_flex-x", gvr, "demo-system", "flex-x")
	c3Strand(t, rw, dyn, gvr, "demo-system", "flex-x", store, "L1_flex-x", 1)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartDepsReconcile(ctx)
	if DepsReconcileStatsSnapshot().Started {
		t.Fatalf("OFF: period \"0\" started the ticker")
	}
	time.Sleep(1500 * time.Millisecond)
	if s := DepsReconcileStatsSnapshot(); s.Ticks != 0 || s.Probed != 0 {
		t.Fatalf("OFF: ticks=%d probed=%d with the period at 0", s.Ticks, s.Probed)
	}
	if _, alive := store.Get("L1_flex-x"); !alive {
		t.Fatalf("OFF: the stranded entry was evicted with the audit disabled — something else ran")
	}
	// The on-demand full walk still works with the ticker off.
	rep, ok := ReconcileFull()
	if !ok || rep.Divergent != 1 {
		t.Fatalf("OFF: ReconcileFull ok=%v divergent=%d, want true/1", ok, rep.Divergent)
	}
	if !c2WaitGone(store, "L1_flex-x", harnessWaitBound) {
		t.Fatalf("OFF: the on-demand walk did not evict the stranded entry")
	}
}

func TestIssue1126_C3_Off_CacheDisabledBuildsNothing(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv(envDepsReconcilePeriodSeconds, "1")
	resetDepsReconcileForTest()
	t.Cleanup(resetDepsReconcileForTest)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartDepsReconcile(ctx)
	if DepsReconcileStatsSnapshot().Started {
		t.Fatalf("cache-off: StartDepsReconcile started a ticker")
	}
	time.Sleep(50 * time.Millisecond)
	// The check is SPECIFIC to the audit's goroutine, not a raw NumGoroutine
	// diff: in the full package run other tests' goroutines are still
	// winding down, and the stats read below touches refresher accessors
	// that construct on cache-off (#203, outside C3's scope).
	if n := c3ReconcileGoroutines(); n != 0 {
		t.Fatalf("cache-off: %d reconcile ticker goroutine(s) running — StartDepsReconcile built one", n)
	}
	if rep, ok := ReconcileFull(); ok || rep.Sampled != 0 {
		t.Fatalf("cache-off: ReconcileFull ok=%v sampled=%d, want false/0", ok, rep.Sampled)
	}
	// The stats read must not construct the ticker either (architect N1 discipline).
	for _, k := range []string{"reconcile_ticks_total", "reconcile_sampled_total", "reconcile_probed_total",
		"reconcile_divergence_total", "reconcile_unknown_total", "reconcile_skipped_no_edge_total", "reconcile_panics_total"} {
		if v, ok := DepsStatsByStat()[k]; !ok || v != 0 {
			t.Fatalf("cache-off: DepsStatsByStat()[%q] = %d,%v want 0,true", k, v, ok)
		}
	}
	if n := c3ReconcileGoroutines(); n != 0 {
		t.Fatalf("cache-off: %d reconcile ticker goroutine(s) after the stats read", n)
	}
}

// c3ReconcileGoroutines counts goroutines currently inside the audit's
// ticker loop (depsReconcile.run) from a full stack dump.
func c3ReconcileGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "(*depsReconcile).run(")
}

// --- LOCK — the audit never nests the store lock inside the watcher lock -----

func TestIssue1126_C3_LockOrder_ReconcileNeverNestsStoreAndWatcherLocks(t *testing.T) {
	c3Setup(t)
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo-system", "flex-y", "y")
	c3Put(store, "L1_flex-y", gvr, "demo-system", "flex-y")

	// The adversary: watcher lock OUTSIDE, store lock INSIDE (the order the
	// dep-tracker eviction path already uses via rw handlers → store). If
	// the audit ever took c.mu and then rw.mu, this pairing deadlocks.
	stop := make(chan struct{})
	adversaryDone := make(chan struct{})
	go func() {
		defer close(adversaryDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rw.mu.Lock()
			store.Get("L1_flex-y")
			rw.mu.Unlock()
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			reconcileOnce(store, rw, 512)
		}
	}()
	select {
	case <-done:
	case <-time.After(harnessWaitBound):
		t.Fatalf("LOCK: reconcileOnce did not complete 200 passes within %s while a goroutine held "+
			"rw.mu → store lock — the audit nests the locks", harnessWaitBound)
	}
	close(stop)
	<-adversaryDone
	if _, alive := store.Get("L1_flex-y"); !alive {
		t.Fatalf("LOCK: the live entry was evicted")
	}
}
