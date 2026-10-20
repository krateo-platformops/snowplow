// apistage_fill_expansion_787_test.go — #787 step 1: the learned decode
// expansion's export.
//
//	X1  the summary and the per-GVR cells follow the calibration exactly, and
//	    every calibration sample lands on exactly one outcome
//	    (seeded / raised / decayed / ignored);
//	X2  past the cap, the GVRs with the most fill bytes keep their series and
//	    the tail folds into gvr="__other__" at its MINIMUM, counted as
//	    truncation;
//	X3  weighted is the mean weighted by fill bytes.
package api

import (
	"fmt"
	"math"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func reset787(t *testing.T) {
	t.Helper()
	resetContentFillGateForTest()
	ResetContentFillStatsForTest()
	t.Cleanup(func() {
		resetContentFillGateForTest()
		ResetContentFillStatsForTest()
	})
}

func calibrate787(gvr string, sample float64) {
	g := theContentFillGate()
	g.mu.Lock()
	g.calibrateLocked(gvr, sample)
	g.mu.Unlock()
}

func fillBytes787(gvr string, b uint64) {
	contentFillMu.Lock()
	contentFillGVRRow(gvr).Bytes += b
	contentFillMu.Unlock()
}

func samples787() map[string]uint64 { return contentFillCalibrationSamplesMap() }

func near787(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestContentFill787_X1_ExportFollowsCalibrationAndCountsEveryOutcome(t *testing.T) {
	reset787(t)
	if _, ok := ContentFillExpansionSummarySnapshot(); ok {
		t.Fatalf("X1: no measured GVR yet, the summary must be absent")
	}
	const g = "g787/v1, Resource=a"
	calibrate787(g, 14)   // seeded: 14
	calibrate787(g, 15)   // raised: 15
	calibrate787(g, 5)    // decayed: 15 + 0.05*(5-15) = 14.5
	calibrate787(g, -1)   // ignored
	calibrate787(g, 14.5) // raised (>= current): 14.5
	want := map[string]uint64{"seeded": 1, "raised": 2, "decayed": 1, "ignored": 1}
	if got := samples787(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("X1 RED: calibration samples by outcome = %v, want %v", got, want)
	}
	cells := ContentFillExpansionCells()
	if len(cells) != 1 || cells[0].GVR != g || !near787(cells[0].Expansion, 14.5) {
		t.Fatalf("X1 RED: the per-GVR cell must be the LEARNED value 14.5 (not the seed %v), got %+v",
			contentFillExpansionSeed, cells)
	}
	s, ok := ContentFillExpansionSummarySnapshot()
	if !ok || !near787(s.Min, 14.5) || !near787(s.Max, 14.5) || !near787(s.Weighted, 14.5) || s.GVRs != 1 {
		t.Fatalf("X1: summary %+v (ok=%v), want min=max=weighted=14.5 over 1 GVR", s, ok)
	}
}

func TestContentFill787_X2_TheCapKeepsTheBiggestAndFoldsTheTailAtItsMinimum(t *testing.T) {
	reset787(t)
	n := contentFillExpansionSeriesCap + 10
	for i := 0; i < n; i++ {
		gvr := fmt.Sprintf("g787/v1, Resource=r%04d", i)
		calibrate787(gvr, 20)
		fillBytes787(gvr, uint64(1_000_000-i)) // r0000 has the most bytes
	}
	// Two low expansions in the TAIL (the GVRs with the fewest bytes).
	calibrate787(fmt.Sprintf("g787/v1, Resource=r%04d", n-1), 3) // decays 20 → 19.15
	calibrate787(fmt.Sprintf("g787/v1, Resource=r%04d", n-2), 1) // decays 20 → 19.05
	lowTail := 20 + contentFillExpansionDecay*(1-20)
	truncated0 := cache.SeriesTruncatedSnapshot()[contentFillExpansionFamily]
	cells := ContentFillExpansionCells()
	if len(cells) != contentFillExpansionSeriesCap+1 {
		t.Fatalf("X2: want %d series (cap + __other__), got %d", contentFillExpansionSeriesCap+1, len(cells))
	}
	if cells[0].GVR != "g787/v1, Resource=r0000" {
		t.Fatalf("X2: the GVR with the most fill bytes must keep its series first, got %q", cells[0].GVR)
	}
	other := cells[len(cells)-1]
	if other.GVR != cache.OtherGVR || !near787(other.Expansion, lowTail) {
		t.Fatalf("X2 RED: the tail must fold into %q at its MINIMUM %v (the risk is a low value), got %+v",
			cache.OtherGVR, lowTail, other)
	}
	if got := cache.SeriesTruncatedSnapshot()[contentFillExpansionFamily] - truncated0; got != 10 {
		t.Fatalf("X2: the fold of 10 GVRs must be counted as truncation, got %d", got)
	}
}

func TestContentFill787_X3_WeightedIsByFillBytes(t *testing.T) {
	reset787(t)
	calibrate787("g787/v1, Resource=big", 10)
	calibrate787("g787/v1, Resource=small", 20)
	fillBytes787("g787/v1, Resource=big", 3_000)
	fillBytes787("g787/v1, Resource=small", 1_000)
	s, ok := ContentFillExpansionSummarySnapshot()
	if !ok || !near787(s.Min, 10) || !near787(s.Max, 20) {
		t.Fatalf("X3: summary %+v", s)
	}
	if want := (10*3000.0 + 20*1000.0) / 4000.0; !near787(s.Weighted, want) {
		t.Fatalf("X3 RED: weighted = %v, want the fill-bytes-weighted mean %v (a plain mean is 15)", s.Weighted, want)
	}
}
