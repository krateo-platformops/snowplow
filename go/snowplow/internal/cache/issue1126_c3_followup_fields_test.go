// issue1126_c3_followup_fields_test.go — the C3 follow-up arms that read the
// NEW ReconcileReport fields (they do not compile on 2000cba; the
// behavioural RED twins are in issue1126_c3_followup_test.go).
//
//	FU4b — the chunked full walk reports its batches and measured holds:
//	       ⌈20000/512⌉ batches, a per-batch hold under fu4GetBound, no
//	       truncation; and a batched walk visits every entry exactly once
//	       while entries evicted between batches are skipped, not
//	       re-visited.
//	FU2b — SkippedNoEdge is the report-level twin of the expvar key.

package cache

import (
	"testing"
	"time"
)

func TestIssue1126_C3_FU4b_ChunkedWalkReportsBatchesAndBoundedHolds(t *testing.T) {
	store, rw := fu4Store(t)

	rep := reconcileOnce(store, rw, 0)
	wantBatches := (fu4Entries + reconcileFullBatch - 1) / reconcileFullBatch
	t.Logf("FU4b: batches=%d snapshotHold=%dµs maxBatchHold=%dµs truncated=%v",
		rep.Batches, rep.SnapshotHoldMicros, rep.MaxBatchHoldMicros, rep.Truncated)
	if rep.Batches != wantBatches {
		t.Fatalf("FU4b: batches=%d, want %d (%d entries / %d per batch)", rep.Batches, wantBatches, fu4Entries, reconcileFullBatch)
	}
	if rep.Truncated {
		t.Fatalf("FU4b: a %d-entry walk hit the %s wall cap", fu4Entries, reconcileFullMaxWall)
	}
	if rep.MaxBatchHoldMicros <= 0 {
		t.Fatalf("FU4b: maxBatchHoldMicros=%d — the hold was not measured", rep.MaxBatchHoldMicros)
	}
	if hold := time.Duration(rep.MaxBatchHoldMicros) * time.Microsecond; hold > fu4GetBound {
		t.Fatalf("FU4b: longest batch hold %s exceeds the Get bound %s", hold, fu4GetBound)
	}

	// Batched iteration is exact: every entry once, an entry evicted
	// between batches is skipped (not re-visited, not counted twice).
	seen := map[string]int{}
	batches := 0
	store.RangeMetadataBatched(512, func(metas []ResolvedEntryMeta, held time.Duration) bool {
		batches++
		if batches == 1 {
			store.DeleteForTest("L1_button-19999") // gone before its batch is collected
		}
		for _, m := range metas {
			seen[m.KeyHash]++
		}
		return true
	})
	if len(seen) != fu4Entries-1 {
		t.Fatalf("FU4b: batched walk visited %d distinct entries, want %d (one evicted between batches)", len(seen), fu4Entries-1)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("FU4b: %s visited %d times", k, n)
		}
	}
	if _, twice := seen["L1_button-19999"]; twice {
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
