// issue1126_c3_followup_fields_test.go — the C3 follow-up arms built on the
// follow-up's NEW API/fields (RangeMetadataBatched, the new ReconcileReport
// fields). They do NOT compile on 2000cba by design — a full chunking revert is
// caught at compile time here; the behavioural-RED-on-2000cba arms (FU1, FU2)
// are in issue1126_c3_followup_test.go.
//
//	FU4  — the full batched walk holds the store lock per BATCH, not across the
//	       residency (PM condition 3), asserted DETERMINISTICALLY with no
//	       wall-clock ratio, in TWO arms (kept separate so a concurrent Get never
//	       races the lock probe): a GRANULARITY arm — exactly ⌈N/512⌉ acquisitions
//	       AND a race-free store.mu.TryLock() free in every between-batch gap
//	       (catches a hold spanning several batches even when the count stays
//	       intact), with NO concurrent Get; and a LIVENESS arm — a concurrent
//	       customer Get completes during the walk within a generous deadlock
//	       backstop. The removed fu4Ratio/fu4Rounds/fu4ExclusiveWalk/fu4Measure/
//	       fu4Hammer were the old ratio's machinery (#326).
//	FU4b — the chunked full walk reports its batches and measured holds:
//	       ⌈20000/512⌉ batches (SEPARATE acquisitions, not one hold across the
//	       residency), a measured per-batch hold, no truncation; and a batched
//	       walk visits every entry exactly once while entries evicted between
//	       batches are skipped, not re-visited. The batch-bounded-hold MECHANISM
//	       is asserted structurally (the batch count) — no wall-clock ratio.
//	FU2b — SkippedNoEdge is the report-level twin of the expvar key.

package cache

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1126_C3_FU4b_ChunkedWalkReportsBatchesAndBoundedHolds(t *testing.T) {
	store, rw := fu4Store(t)

	wantBatches := (fu4Entries + reconcileFullBatch - 1) / reconcileFullBatch

	// The chunked full walk reports its batches and measured holds. The
	// batch-bounded-hold MECHANISM is asserted deterministically — no
	// wall-clock ratio: the walk runs in wantBatches SEPARATE acquisitions
	// (not one hold across the residency), visits and probes everything without
	// hitting the wall cap, and records a per-batch hold. The behavioural twin
	// — a concurrent customer Get is never blocked for the whole walk — is
	// TestIssue1126_C3_FU4.
	rep := reconcileOnce(store, rw, 0)
	t.Logf("FU4b: chunked batches=%d snapshotHold=%dµs maxBatchHold=%dµs truncated=%v",
		rep.Batches, rep.SnapshotHoldMicros, rep.MaxBatchHoldMicros, rep.Truncated)
	if rep.Sampled != fu4Entries || rep.Probed != fu4Entries {
		t.Fatalf("FU4b: sampled=%d probed=%d, want %d each", rep.Sampled, rep.Probed, fu4Entries)
	}
	if rep.Batches != wantBatches {
		t.Fatalf("FU4b: batches=%d, want %d (%d entries / %d per batch) — the walk did not chunk into "+
			"batch-bounded holds; it held one acquisition across the residency", rep.Batches, wantBatches, fu4Entries, reconcileFullBatch)
	}
	if rep.Truncated {
		t.Fatalf("FU4b: a %d-entry walk hit the %s wall cap", fu4Entries, reconcileFullMaxWall)
	}
	if rep.MaxBatchHoldMicros <= 0 {
		t.Fatalf("FU4b: maxBatchHoldMicros=%d — the per-batch hold was not measured", rep.MaxBatchHoldMicros)
	}

	// Batched iteration is exact: every entry once, an entry evicted
	// between batches is skipped (not re-visited, not counted twice).
	//
	// #243: pick the evicted entry as one NOT collected in the first batch, so
	// the "evicted between batches ⇒ skipped" property holds regardless of Go
	// map iteration order. The old code deleted a hardcoded key
	// ("L1_button-19999") on batch 1, but whether that key had already been
	// collected into batch 1's metas depends on the store's map order — so ~5%
	// of runs the evicted key WAS in batch 1 (counted before the delete) and
	// len(seen) stayed fu4Entries, a false RED at :80. A still-pending victim
	// is deleted before its own (later) batch is collected, every run.
	seen := map[string]int{}
	batches := 0
	var victim string
	store.RangeMetadataBatched(512, func(metas []ResolvedEntryMeta, held time.Duration) bool {
		batches++
		for _, m := range metas {
			seen[m.KeyHash]++
		}
		if batches == 1 {
			// Any key absent from the first batch is still pending in a later
			// batch of THIS walk; deleting it now precedes its own batch.
			for i := 0; i < fu4Entries; i++ {
				k := fmt.Sprintf("L1_button-%d", i)
				if _, collected := seen[k]; !collected {
					victim = k
					break
				}
			}
			store.DeleteForTest(victim)
		}
		return true
	})
	if victim == "" {
		t.Fatal("FU4b: no uncollected victim after batch 1 — batch 1 held every entry?")
	}
	if len(seen) != fu4Entries-1 {
		t.Fatalf("FU4b: batched walk visited %d distinct entries, want %d (one evicted between batches)", len(seen), fu4Entries-1)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("FU4b: %s visited %d times", k, n)
		}
	}
	if _, twice := seen[victim]; twice {
		t.Fatalf("FU4b: the entry evicted between batches was still visited")
	}
}

func TestIssue1126_C3_FU2b_ReportCarriesSkippedNoEdge(t *testing.T) {
	c3Setup(t)
	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo-system", "flex-z", "z")
	// Put WITHOUT Record: no edge at all (the dropped_cap shape without the cap).
	store.Put("L1_flex-z", &ResolvedEntry{RawJSON: []byte(`{"k":"z"}`), Inputs: widgetInputs(gvr, "demo-system", "flex-z")})
	fu2Strand(t, rw, dyn, gvr, store, []string{"flex-z"}, []string{"L1_flex-z"})

	rep := reconcileOnce(store, rw, 512)
	if rep.SkippedNoEdge != 1 || rep.Divergent != 0 || len(rep.Entries) != 0 {
		t.Fatalf("FU2b: skippedNoEdge=%d divergent=%d entries=%d, want 1/0/0", rep.SkippedNoEdge, rep.Divergent, len(rep.Entries))
	}
	if _, alive := store.Get("L1_flex-z"); !alive {
		t.Fatalf("FU2b: the no-edge entry was evicted")
	}
}

// --- FU4 — the full walk holds the store lock per batch, not across residency -

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

func TestIssue1126_C3_FU4_FullWalkHoldsLockPerBatchNotAcrossResidency(t *testing.T) {
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

	// GRANULARITY (deterministic, no wall-clock ratio). The full walk runs in
	// exactly wantBatches acquisitions, and store.mu is FREE during every
	// between-batch gap. NO concurrent Get runs here (arch): the walk is then
	// the ONLY c.mu contender, so a correct per-batch release makes TryLock
	// ALWAYS succeed — a background Get holding c.mu at a TryLock instant would
	// false-fail it. sync.Mutex is non-reentrant, so a walk that holds c.mu
	// across a batch (the whole-residency shape, or a hold spanning several
	// batches that keeps the count at wantBatches) fails TryLock -> RED,
	// race-free. Closes the blind spot the batch count alone leaves. The
	// behavioural liveness twin is TestIssue1126_C3_FU4_ConcurrentGet...
	wantBatches := (fu4Entries + reconcileFullBatch - 1) / reconcileFullBatch
	batches, visited := 0, 0
	store.RangeMetadataBatched(reconcileFullBatch, func(metas []ResolvedEntryMeta, _ time.Duration) bool {
		batches++
		visited += len(metas)
		if len(metas) > reconcileFullBatch {
			t.Fatalf("FU4: a batch copied %d entries under one lock hold, want <= %d", len(metas), reconcileFullBatch)
		}
		if !store.mu.TryLock() {
			t.Fatalf("FU4: the store lock is held during a between-batch gap — the full walk holds c.mu across " +
				"batches; at 50K-100K entries /debug/reconcile would stall every customer /call for the whole walk (PM condition 3)")
		}
		store.mu.Unlock()
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

func TestIssue1126_C3_FU4_ConcurrentGetNeverBlockedForTheWholeWalk(t *testing.T) {
	store, rw := fu4Store(t)
	if _, alive := store.Get("L1_button-0"); !alive {
		t.Fatalf("premise: entry not resident")
	}

	// LIVENESS (behavioural symptom). A real customer store.Get fired DURING the
	// full walk must complete within a GENEROUS deadlock backstop; a walk that
	// holds c.mu across the whole residency blocks it for the entire walk -> the
	// backstop fires -> RED. The backstop is a safety net, NOT a latency gate
	// (the #328 pattern): correct code completes each Get in ~ms. Kept SEPARATE
	// from the TryLock arm (arch) so a concurrent Get never races that TryLock.
	const backstop = 30 * time.Second
	walkDone := make(chan struct{})
	go func() {
		reconcileOnce(store, rw, 0)
		close(walkDone)
	}()
	for {
		select {
		case <-walkDone:
			return // the walk finished; every concurrent Get completed within the backstop
		default:
		}
		done := make(chan struct{})
		go func() { store.Get("L1_button-0"); close(done) }()
		select {
		case <-done:
		case <-time.After(backstop):
			t.Fatalf("FU4: a concurrent customer store.Get blocked >%s during the full walk — the walk holds "+
				"c.mu across the residency (PM condition 3)", backstop)
		}
	}
}
