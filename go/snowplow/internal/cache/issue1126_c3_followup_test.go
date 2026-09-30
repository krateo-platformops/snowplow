// issue1126_c3_followup_test.go — 1.12.6 C3 review follow-ups (arch N2 /
// N5, PM condition 3). FU1 and FU2 use ONLY symbols that exist on the
// pre-follow-up tree 2000cba (string expvar keys, ReconcileReport fields
// C3 shipped with, reconcileOnce, RangeMetadata*), so they compile there and
// each is a BEHAVIOURAL RED on 2000cba and GREEN here. FU4 is the EXCEPTION
// (see its note): its bounded-hold property is only observable as a wall-clock
// HOLD DURATION with 2000cba-only symbols — both the chunked and the
// pre-chunking walk snapshot keys once and release c.mu during probes, so the
// meta-collection hold length is the only difference — which made a same-run
// RATIO the only available signal, and it flaked. #326 replaces it with a
// deterministic mechanism assertion built on the follow-up's batched API, so
// FU4 is now COMPILE-RED on 2000cba (like the fields file), not a runtime twin:
// a full chunking revert is caught at compile time, a keep-the-API hold is
// caught by the deterministic guard.
//
//	FU1 — reconcile_sampled_total publishes Sampled and reconcile_probed_total
//	      publishes Probed (arch N2: 2000cba published Probed under the
//	      sampled name and had no probed key).
//	FU2 — an ABSENT entry with NO self dep edge (its Record dropped at
//	      DEPS_MAX_RECORDS — dropped_cap) is counted under
//	      reconcile_skipped_no_edge_total, NOT as divergence, on every pass;
//	      the edged sibling is divergent exactly once and evicted (arch N5:
//	      on 2000cba the no-edge entry pinned divergence non-zero forever).
//	FU4 — the full walk over a 20K-entry store holds the store lock per BATCH
//	      and releases it between batches (PM condition 3), asserted
//	      DETERMINISTICALLY with no wall-clock ratio: the walk runs in exactly
//	      ceil(N/512) acquisitions, store.mu is free during every between-batch
//	      gap (a race-free TryLock — so a hold spanning several batches is
//	      caught even when the count stays intact), and a concurrent customer
//	      Get completes in every gap within a generous backstop. The removed
//	      fu4Ratio/fu4Rounds/fu4ExclusiveWalk/fu4Measure/fu4Hammer were the
//	      ratio's machinery (#326).
//
// The arms that read the follow-up's NEW report fields live in
// issue1126_c3_followup_fields_test.go.

package cache

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// --- FU1 — each expvar key publishes the field it is named after -------------

func TestIssue1126_C3_FU1_ExpvarPublishesSampledAndProbedUnderTheirOwnNames(t *testing.T) {
	c3Setup(t)
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	createObj(t, rw, dyn, gvr, "demo-system", "flex-y", "y")
	c3Put(store, "L1_flex-x_cohortA", gvr, "demo-system", "flex-x") // two cohorts, one coordinate
	c3Put(store, "L1_flex-x_cohortB", gvr, "demo-system", "flex-x")
	c3Put(store, "L1_flex-y", gvr, "demo-system", "flex-y")

	rep := reconcileOnce(store, rw, 512)
	if rep.Sampled != 3 || rep.Probed != 2 || rep.Divergent != 0 {
		t.Fatalf("premise: sampled=%d probed=%d divergent=%d, want 3/2/0", rep.Sampled, rep.Probed, rep.Divergent)
	}
	depsReconcileInstance.account(rep)

	stats := DepsStatsByStat()
	probed, ok := stats["reconcile_probed_total"]
	if !ok {
		t.Fatalf("RED: snowplow_deps has no reconcile_probed_total key — the probe count (the divergence "+
			"denominator) is published under another key's name (arch N2). Keys: %v", stats)
	}
	if probed != 2 {
		t.Fatalf("reconcile_probed_total = %d, want 2 (unique coordinates; cohort copies share one probe)", probed)
	}
	if got := stats["reconcile_sampled_total"]; got != 3 {
		t.Fatalf("RED: reconcile_sampled_total = %d, want 3 (entries visited) — the key publishes Probed, not "+
			"Sampled (arch N2: /debug/reconcile exposes both numbers and expvar published one under the "+
			"other's name)", got)
	}
}

// --- FU2 — a no-edge entry is counted apart, never as divergence --------------

// fu2Strand strands TWO objects at once: waits for both ADDs to reach the
// worker, stops it, deletes both, asserts both entries still resident and
// both probes ABSENT, then installs a fresh live worker.
func fu2Strand(t *testing.T, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, store *ResolvedCacheStore, names, keys []string) {
	t.Helper()
	deadline := time.Now().Add(harnessWaitBound)
	for DepWatchStatsSnapshot().ProbeExists < uint64(len(names)) {
		if time.Now().After(deadline) {
			t.Fatalf("premise: only %d of %d ADDs reached the worker", DepWatchStatsSnapshot().ProbeExists, len(names))
		}
		time.Sleep(10 * time.Millisecond)
	}
	waitQueueIdle(t, harnessWaitBound)
	depWatchSingleton().stopWorker()
	for _, n := range names {
		deleteObj(t, rw, dyn, gvr, "demo-system", n)
	}
	time.Sleep(300 * time.Millisecond)
	for i, k := range keys {
		if _, alive := store.Get(k); !alive {
			t.Fatalf("premise: %s evicted although the worker was stopped", k)
		}
		if got := rw.probeObjectState(gvr, "demo-system", names[i]); got != objAbsent {
			t.Fatalf("premise: probe(%s)=%v, want objAbsent", names[i], got)
		}
	}
	resetDepWatchForTest()
}

func TestIssue1126_C3_FU2_NoEdgeEntryIsCountedApartAndNeverPinsDivergence(t *testing.T) {
	// A tracker that holds exactly ONE edge: the second Record is dropped
	// at the cap through the production path (dropped_cap moves), which is
	// how a no-edge entry comes to exist on a real cluster.
	t.Setenv(envDepsMaxRecords, "1")
	c3Setup(t)
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	createObj(t, rw, dyn, gvr, "demo-system", "flex-z", "z")
	c3Put(store, "L1_flex-x", gvr, "demo-system", "flex-x") // edge recorded
	c3Put(store, "L1_flex-z", gvr, "demo-system", "flex-z") // Record DROPPED at the cap: no edge
	if s := Deps().Stats(); s.RecordDroppedCap != 1 || s.RecordTotal != 1 {
		t.Fatalf("premise: dropped_cap=%d record_total=%d, want 1/1 — the cap did not drop the second edge", s.RecordDroppedCap, s.RecordTotal)
	}
	fu2Strand(t, rw, dyn, gvr, store, []string{"flex-x", "flex-z"}, []string{"L1_flex-x", "L1_flex-z"})
	before := Deps().Stats().EvictDeleteTotal

	rep := reconcileOnce(store, rw, 512)
	depsReconcileInstance.account(rep)
	if rep.Sampled != 2 || rep.Probed != 2 {
		t.Fatalf("FU2: sampled=%d probed=%d, want 2/2", rep.Sampled, rep.Probed)
	}
	if rep.Divergent != 1 {
		t.Fatalf("RED: divergent=%d, want 1 — the no-edge entry L1_flex-z (dropped_cap) is counted as divergence "+
			"although the worker cannot evict it (arch N5): reconcile_divergence_total will stay non-zero "+
			"forever and the observability.md alert fires on a dropped_cap, not on a lost DELETE", rep.Divergent)
	}
	if len(rep.Entries) != 1 || rep.Entries[0].Name != "flex-x" {
		t.Fatalf("FU2: divergent rows %+v, want exactly the edged flex-x", rep.Entries)
	}
	stats := DepsStatsByStat()
	skipped, ok := stats["reconcile_skipped_no_edge_total"]
	if !ok {
		t.Fatalf("RED: snowplow_deps has no reconcile_skipped_no_edge_total key — the dropped_cap case has no "+
			"counter of its own (arch N5). Keys: %v", stats)
	}
	if skipped != 1 || stats["reconcile_divergence_total"] != 1 {
		t.Fatalf("FU2: skipped_no_edge=%d divergence=%d after pass 1, want 1/1", skipped, stats["reconcile_divergence_total"])
	}

	// The edged entry is evicted through the worker; the no-edge entry
	// stays (nothing can evict it but its TTL — the documented limit).
	if !c2WaitGone(store, "L1_flex-x", harnessWaitBound) {
		t.Fatalf("FU2: L1_flex-x still resident after the audit submitted its coordinate")
	}
	if _, alive := store.Get("L1_flex-z"); !alive {
		t.Fatalf("FU2: L1_flex-z (no edge) was evicted — by what? the worker has no edge to it")
	}
	if got := c3WaitEvictDelete(before+1, harnessWaitBound); got != before+1 {
		t.Fatalf("FU2: evict_delete_total moved by %d, want 1", got-before)
	}

	// Pass 2: divergence does NOT move (the guidance "a rate that does not
	// fall back to zero = a lost DELETE" is true); the no-edge counter does.
	rep2 := reconcileOnce(store, rw, 512)
	depsReconcileInstance.account(rep2)
	if rep2.Divergent != 0 {
		t.Fatalf("RED: pass 2 divergent=%d, want 0 — the no-edge entry pins divergence every pass", rep2.Divergent)
	}
	stats = DepsStatsByStat()
	if stats["reconcile_divergence_total"] != 1 || stats["reconcile_skipped_no_edge_total"] != 2 {
		t.Fatalf("FU2: after pass 2 divergence=%d skipped_no_edge=%d, want 1/2",
			stats["reconcile_divergence_total"], stats["reconcile_skipped_no_edge_total"])
	}
}

// c3WaitEvictDelete polls Deps().Stats().EvictDeleteTotal until it reaches
// want or the bound passes, and returns the last value read. The worker's
// runEvictionBatch increments the counter AFTER deleteForDep, so "entry
// gone" is observable a few µs before the counter moves — a single read
// right after c2WaitGone races it (seen once under -race with a parallel
// suite on the machine).
func c3WaitEvictDelete(want uint64, within time.Duration) uint64 {
	deadline := time.Now().Add(within)
	for {
		got := Deps().Stats().EvictDeleteTotal
		if got >= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- FU4 — the full walk never stalls a customer Get for the whole residency --

const fu4Entries = 20000

// fu4Store builds a store of fu4Entries widget entries whose GVR is NOT
// watched, so every probe is a cheap UNKNOWN (no submits, no worker
// traffic) and the walk's cost is the store-mutex hold under test.
func fu4Store(t *testing.T) (*ResolvedCacheStore, *ResourceWatcher) {
	t.Helper()
	c3Setup(t)
	store := newResolvedCache(fu4Entries+100, 1<<30, time.Hour)
	Deps().SetStore(store)
	rw, _ := realWatcher(t, gvrFlexes())
	unwatched := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}
	for i := 0; i < fu4Entries; i++ {
		name := fmt.Sprintf("button-%d", i)
		store.Put("L1_"+name, &ResolvedEntry{RawJSON: []byte(`{"i":1}`), Inputs: widgetInputs(unwatched, "demo-system", name)})
	}
	return store, rw
}

// fu4AssertGapBoundedHold asserts, from inside a between-batch gap of the
// chunked full walk, that the store lock is NOT held across the batch — the
// batch-bounded-hold MECHANISM, checked deterministically with NO wall-clock
// ratio:
//
//   - TryLock (the deterministic core): during a genuine gap the walk holds no
//     lock, so store.mu.TryLock() succeeds. sync.Mutex is non-reentrant, so a
//     walk that holds c.mu across this batch — the whole-residency shape, or a
//     regression that spans several batches per acquisition — fails TryLock
//     here, race-free and with no timing. This closes the granularity blind
//     spot the batch COUNT alone leaves (a hold spanning 2 batches keeps the
//     count at ceil(N/512) but is caught here).
//   - concurrent Get (behavioural liveness): a real customer store.Get
//     completes within a GENEROUS deadlock backstop; a walk holding c.mu across
//     the residency blocks it. The backstop is a safety net for a stuck walk,
//     NOT the discriminator (the #328 pattern) — the TryLock above decides.
func fu4AssertGapBoundedHold(t *testing.T, store *ResolvedCacheStore) {
	t.Helper()
	if !store.mu.TryLock() {
		t.Fatalf("FU4: the store lock is held during a between-batch gap — the full walk holds c.mu across " +
			"batches; at 50K-100K entries /debug/reconcile would stall every customer /call for the whole walk (PM condition 3)")
	}
	store.mu.Unlock()
	const releaseBackstop = 30 * time.Second
	done := make(chan struct{})
	go func() { store.Get("L1_button-0"); close(done) }()
	select {
	case <-done:
	case <-time.After(releaseBackstop):
		t.Fatalf("FU4: a concurrent customer store.Get blocked >%s in a between-batch gap — the walk holds the store lock across the residency", releaseBackstop)
	}
}

func TestIssue1126_C3_FU4_FullWalkNeverBlocksAConcurrentGetBeyondOneBatch(t *testing.T) {
	store, rw := fu4Store(t)
	if _, alive := store.Get("L1_button-0"); !alive {
		t.Fatalf("premise: entry not resident")
	}

	// Load-independent: the chunked full walk visits and probes everything.
	rep := reconcileOnce(store, rw, 0)
	if rep.Sampled != fu4Entries || rep.Probed != fu4Entries || rep.Unknown != fu4Entries {
		t.Fatalf("FU4: sampled=%d probed=%d unknown=%d, want %d each (an unwatched GVR is UNKNOWN, never absent)",
			rep.Sampled, rep.Probed, rep.Unknown, fu4Entries)
	}

	// Mechanism (deterministic, no wall-clock ratio). The full walk holds the
	// store lock per BATCH, releasing it between batches, so a customer Get is
	// never blocked for the whole residency. Two axes, per arch's ruling:
	//   GRANULARITY — the walk runs in exactly wantBatches acquisitions, and the
	//                 store lock is free during EVERY gap (fu4AssertGapBoundedHold's
	//                 TryLock), so a hold spanning multiple batches is caught even
	//                 though it keeps the batch count intact.
	//   LIVENESS    — a concurrent customer Get completes in every gap.
	wantBatches := (fu4Entries + reconcileFullBatch - 1) / reconcileFullBatch
	batches, visited := 0, 0
	store.RangeMetadataBatched(reconcileFullBatch, func(metas []ResolvedEntryMeta, _ time.Duration) bool {
		batches++
		visited += len(metas)
		if len(metas) > reconcileFullBatch {
			t.Fatalf("FU4: a batch copied %d entries under one lock hold, want <= %d", len(metas), reconcileFullBatch)
		}
		fu4AssertGapBoundedHold(t, store)
		return true
	})
	if batches != wantBatches {
		t.Fatalf("FU4: full walk ran in %d acquisitions, want %d (%d entries / %d per batch) — the walk did not "+
			"chunk into batch-bounded holds", batches, wantBatches, fu4Entries, reconcileFullBatch)
	}
	if visited != fu4Entries {
		t.Fatalf("FU4: batched walk visited %d entries, want %d", visited, fu4Entries)
	}
}
