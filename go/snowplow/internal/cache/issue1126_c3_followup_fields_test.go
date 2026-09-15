// issue1126_c3_followup_fields_test.go — the C3 follow-up arms that read the
// NEW ReconcileReport fields (they do not compile on 2000cba; the
// behavioural RED twins are in issue1126_c3_followup_test.go).
//
//	FU4b — the chunked full walk reports its batches and measured holds:
//	       ⌈20000/512⌉ batches, a measured per-batch hold, no truncation,
//	       and — same-run ratio, arch N10 — the longest batch hold is at
//	       most 1/fu4Ratio of the exclusive walk's single hold measured in
//	       the same run; a batched walk visits every entry exactly once
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

	wantBatches := (fu4Entries + reconcileFullBatch - 1) / reconcileFullBatch
	// Same-run ratio (arch N10): the exclusive walk's ONE hold is the time
	// RangeMetadata takes with a trivial fn; the chunked walk's longest
	// batch hold is measured by the walk itself. Interleaved, min of rounds.
	var minEx, minCh time.Duration = -1, -1
	var rep ReconcileReport
	for i := 0; i < fu4Rounds; i++ {
		t0 := time.Now()
		n := 0
		store.RangeMetadata(func(ResolvedEntryMeta) bool { n++; return true })
		exHold := time.Since(t0)
		if n != fu4Entries {
			t.Fatalf("FU4b: exclusive walk visited %d, want %d", n, fu4Entries)
		}
		rep = reconcileOnce(store, rw, 0)
		chHold := time.Duration(rep.MaxBatchHoldMicros) * time.Microsecond
		t.Logf("FU4b round %d: exclusive hold %s | chunked batches=%d snapshotHold=%dµs maxBatchHold=%s truncated=%v",
			i+1, exHold, rep.Batches, rep.SnapshotHoldMicros, chHold, rep.Truncated)
		if rep.Batches != wantBatches {
			t.Fatalf("FU4b: batches=%d, want %d (%d entries / %d per batch)", rep.Batches, wantBatches, fu4Entries, reconcileFullBatch)
		}
		if rep.Truncated {
			t.Fatalf("FU4b: a %d-entry walk hit the %s wall cap", fu4Entries, reconcileFullMaxWall)
		}
		if rep.MaxBatchHoldMicros <= 0 {
			t.Fatalf("FU4b: maxBatchHoldMicros=%d — the hold was not measured", rep.MaxBatchHoldMicros)
		}
		if minEx < 0 || exHold < minEx {
			minEx = exHold
		}
		if minCh < 0 || chHold < minCh {
			minCh = chHold
		}
	}
	t.Logf("FU4b: min hold — exclusive %s, chunked batch %s, ratio %.1f (need ≥ %d)",
		minEx, minCh, float64(minEx)/float64(minCh), fu4Ratio)
	if minCh*fu4Ratio > minEx {
		t.Fatalf("FU4b: longest batch hold %s is not ≤ 1/%d of the exclusive walk's hold %s in the same run — "+
			"a batch of %d costs as much as the whole residency", minCh, fu4Ratio, minEx, reconcileFullBatch)
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
