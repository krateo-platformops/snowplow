// issue238_dryrun_falsifier_test.go — #238: /debug/reconcile ?dryRun=1 must
// REPORT the divergent set WITHOUT evicting it, so an operator can observe a
// stranded entry before anything repairs it (the #237 "observe-before-repair"
// need).
//
// THE PAIR (feedback_arm_that_cannot_fail_is_not_coverage): both arms build the
// IDENTICAL stranded fixture (a resident L1 entry whose own object is ABSENT
// from a synced indexer, with a live dep edge) via the same c3* harness the
// 1.12.6 C3 tests use — hermetic (dynamicfake, no kind):
//
//	ARM A (dryRun)  — the entry is in Entries, rep.DryRun==true, and it is STILL
//	                  RESIDENT afterwards, with the worker's EvictDeleteTotal AND
//	                  reconcile_divergence_total UNCHANGED (side-effect-free, PM
//	                  C1 — counter-based, not timing). Reports without repairing.
//	ARM B (control) — SAME fixture, dryRun=false: the entry is in Entries AND is
//	                  EVICTED (c2WaitGone) AND EvictDeleteTotal moves by 1 AND
//	                  reconcile_divergence_total moves by 1. Proves the fixture is
//	                  genuinely divergent and the real path evicts it — so ARM A's
//	                  preservation is dryRun CHANGING behaviour, not a no-op that
//	                  always passes.
//
// RED-first (feedback_falsifier_first_before_ship): written before the
// deps_reconcile.go change; ReconcileFullDryRun does not exist yet, so the file
// does not compile — the RED-for-the-right-reason signal that the mode is
// absent (same posture as #272's new-API RED).

package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// issue238StrandFlexX builds the stranded fixture ReconcileFull* will walk (it
// reads the PROCESS store + watcher, so the fixture must live in the process
// globals, exactly like TestIssue1126_D2 / the OFF test). Returns the process
// store and the resident key whose object is ABSENT.
func issue238StrandFlexX(t *testing.T) (*ResolvedCacheStore, schema.GroupVersionResource, string) {
	t.Helper()
	c3Setup(t)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	store := ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true")
	}
	Deps().SetStore(store)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	c3Put(store, "L1_flex-x", gvr, "demo-system", "flex-x")
	// Strand it: delete the object with the worker stopped so the DELETE is
	// lost, leaving the entry resident but its coordinate ABSENT; the worker is
	// reset to a live queue so the audit's own submit (arm B) can reach it.
	c3Strand(t, rw, dyn, gvr, "demo-system", "flex-x", store, "L1_flex-x", 1)
	return store, gvr, "L1_flex-x"
}

// ARM A — dryRun reports the divergent entry but does NOT evict it or move any
// counter.
func TestIssue238_DryRun_ReportsButPreservesAndIsSideEffectFree(t *testing.T) {
	store, _, key := issue238StrandFlexX(t)

	evictBefore := Deps().Stats().EvictDeleteTotal
	divBefore := DepsReconcileStatsSnapshot().Divergence

	rep, ok := ReconcileFullDryRun(true) // dryRun
	if !ok {
		t.Fatalf("ARM A: ReconcileFullDryRun(true) ok=false, want true (cache is on)")
	}
	if !rep.DryRun {
		t.Fatalf("#238 FAIL: rep.DryRun=false under dryRun — a captured body must state its mode")
	}
	if rep.Divergent != 1 || len(rep.Entries) != 1 {
		t.Fatalf("#238 FAIL: dryRun must still REPORT the divergent set (divergent=%d entries=%d, want 1/1): %+v",
			rep.Divergent, len(rep.Entries), rep.Entries)
	}
	if e := rep.Entries[0]; e.Name != "flex-x" || e.Namespace != "demo-system" {
		t.Fatalf("#238 FAIL: reported entry is not the stranded flex-x: %+v", e)
	}

	// Drain any worker work (there should be none — dryRun submitted nothing);
	// if dryRun had wrongly submitted, this drains it and the entry would be
	// gone below.
	waitQueueIdle(t, harnessWaitBound)

	if _, alive := store.Get(key); !alive {
		t.Fatalf("#238 FAIL: %s was EVICTED under dryRun — the whole point is observe-before-repair", key)
	}
	if got := Deps().Stats().EvictDeleteTotal; got != evictBefore {
		t.Fatalf("#238 FAIL: dryRun moved evict_delete_total by %d, want 0 (nothing handed to the worker)", got-evictBefore)
	}
	if got := DepsReconcileStatsSnapshot().Divergence; got != divBefore {
		t.Fatalf("#238 FAIL: dryRun moved reconcile_divergence_total by %d, want 0 — an observe-only call must "+
			"not fire the lost-DELETE operator signal", got-divBefore)
	}
}

// ARM B — the non-dryRun control on the IDENTICAL fixture DOES evict and DOES
// move the counters, proving arm A's preservation is dryRun changing behaviour.
func TestIssue238_NonDryRun_Control_ReportsAndEvicts(t *testing.T) {
	store, _, key := issue238StrandFlexX(t)

	evictBefore := Deps().Stats().EvictDeleteTotal
	divBefore := DepsReconcileStatsSnapshot().Divergence

	rep, ok := ReconcileFullDryRun(false) // real reconcile — today's behaviour
	if !ok {
		t.Fatalf("ARM B: ReconcileFullDryRun(false) ok=false, want true")
	}
	if rep.DryRun {
		t.Fatalf("#238 FAIL: rep.DryRun=true for a non-dryRun call")
	}
	if rep.Divergent != 1 || len(rep.Entries) != 1 {
		t.Fatalf("ARM B premise: divergent=%d entries=%d, want 1/1", rep.Divergent, len(rep.Entries))
	}
	if !c2WaitGone(store, key, harnessWaitBound) {
		t.Fatalf("#238 control FAIL: %s still resident — the real path must evict via the worker; the fixture "+
			"is then not genuinely divergent and arm A would pass vacuously", key)
	}
	if got := c3WaitEvictDelete(evictBefore+1, harnessWaitBound); got != evictBefore+1 {
		t.Fatalf("ARM B: evict_delete_total moved by %d, want 1", got-evictBefore)
	}
	if got := DepsReconcileStatsSnapshot().Divergence; got != divBefore+1 {
		t.Fatalf("ARM B: reconcile_divergence_total moved by %d, want 1 (the real path accounts divergence)", got-divBefore)
	}
}
