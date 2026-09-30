// issue1126_c3_followup_fields_test.go — the C3 follow-up arms that read the
// NEW ReconcileReport fields (they do not compile on 2000cba; the
// behavioural RED twins are in issue1126_c3_followup_test.go).
//
//	FU4b — the chunked full walk reports its batches and measured holds:
//	       ⌈20000/512⌉ batches (SEPARATE acquisitions, not one hold across the
//	       residency), a measured per-batch hold, no truncation; and a batched
//	       walk visits every entry exactly once while entries evicted between
//	       batches are skipped, not re-visited. The batch-bounded-hold MECHANISM
//	       is asserted structurally (the batch count) — no wall-clock ratio; the
//	       behavioural twin (a concurrent Get never blocked for the whole walk)
//	       is TestIssue1126_C3_FU4.
//	FU2b — SkippedNoEdge is the report-level twin of the expvar key.

package cache

import (
	"fmt"
	"testing"
	"time"
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
