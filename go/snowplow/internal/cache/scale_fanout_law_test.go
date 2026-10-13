// scale_fanout_law_test.go — MEASUREMENT HARNESS, not a gate.
//
// THE QUESTION. The production target is 50,000 compositions and 1,000 users.
// 057 carries 73 compositions and 6 users, and every scaling statement we have
// is a LINEAR extrapolation from that density (71.2 resolved-cache entries per
// composition, 28.6 dirty marks per object event). A linear extrapolation says
// the 100,000-entry cap binds at ~1,400 compositions. Nobody has checked
// whether the law IS linear — and it cannot be checked on 057, which is the
// demo cluster.
//
// This harness answers it in-process, driving the REAL DepTracker rather than a
// model of it: build a synthetic population of cells with realistic dependency
// edges, fire ONE object event, and read the fan-out the tracker actually
// reports. No cluster, no cost, no approvals.
//
// WHY FAN-OUT IS THE RIGHT THING TO MEASURE. A cell carrying a LIST dependency
// on a GVR is dirty-marked by ANY object event on that GVR — that is what
// RecordList means. A cell carrying an object dependency is marked only by its
// own coordinate. So if list-dep cells are a fixed FRACTION of the population,
// fan-out per event grows with the population, and since the event rate also
// grows with the population, total dirty-mark load grows with its SQUARE. That
// is a different wall from the entry cap, and a nearer one. This measures which
// regime we are actually in.
//
// CALIBRATION against 057, so the synthetic population is not invented: 689
// widgets across 73 compositions is ~9.4 widget cells per composition, and the
// measured fan-out is 28.6 cells per object event. The listDepShare below is
// swept rather than guessed, because it is the parameter the answer is most
// sensitive to and 057 does not expose it directly.
//
// This test LOGS and asserts only the structural claim it is designed to
// establish. It is deliberately not a performance gate: wall-clock on a shared
// runner would flake (the Task #328 anti-pattern), and the quantity of interest
// is a COUNT, which is machine-independent.

package cache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// scalePopulation builds compositions*cohorts*widgetsPer cells and returns the
// tracker plus the number of cells that carry a list dependency.
//
// Edge shape per cell, mirroring the three classes the attribution tests pin:
//   - listDepShare of them RecordList on the composition GVR  (fan-out carriers)
//   - the remainder Record an object dep on their OWN composition coordinate
func scalePopulation(t testing.TB, compositions, cohorts, widgetsPer int, listDepShare float64) (*DepTracker, int) {
	t.Helper()
	total := compositions * cohorts * widgetsPer
	d := newDepTracker(int64(total*2 + 1_000))
	// Caps set wide: this harness measures the fan-out LAW, and an eviction
	// mid-build would silently shrink the population being measured.
	store := newResolvedCache(total*2+1_000, 1<<30, time.Hour)
	d.SetStore(store)
	gvr := gvrCompositions()
	ctx := context.Background()

	listCells := 0
	for c := 0; c < compositions; c++ {
		comp := fmt.Sprintf("comp-%06d", c)
		for k := 0; k < cohorts; k++ {
			for w := 0; w < widgetsPer; w++ {
				l1 := fmt.Sprintf("L1|c%06d|k%03d|w%02d", c, k, w)
				store.Put(l1, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: inputsFor(gvr, "ns", comp)})
				// Deterministic interleave rather than rand: the share must be
				// exact and the run reproducible.
				if float64(w) < float64(widgetsPer)*listDepShare {
					d.RecordList(ctx, l1, gvr, "ns")
					listCells++
				} else {
					d.Record(ctx, l1, gvr, "ns", comp)
				}
			}
		}
	}
	return d, listCells
}

// TestScaleLaw_FanOutGrowsWithPopulation measures dirty-marks-per-event as the
// composition count grows with everything else held fixed.
func TestScaleLaw_FanOutGrowsWithPopulation(t *testing.T) {
	const (
		cohorts    = 2
		widgetsPer = 10  // ~9.4/composition on 057
		listShare  = 0.3 // swept separately below
	)
	gvr := gvrCompositions()

	type row struct{ comps, cells, listCells, marked int }
	var rows []row

	for _, comps := range []int{25, 50, 100, 200, 400} {
		d, listCells := scalePopulation(t, comps, cohorts, widgetsPer, listShare)
		// One object event on ONE composition's coordinate.
		_, marked := d.OnObjectEvent(gvr, "ns", "comp-000000", objExists)
		rows = append(rows, row{comps, comps * cohorts * widgetsPer, listCells, marked})

		// #603 PR-0 — the EXACT law, asserted rather than logged:
		//   marked = (every list-dep cell on the GVR) + (the changed object's own cells)
		// The own cells are the object-dep widgets of comp-000000 in every cohort.
		// This is today's behaviour; the #603 narrowing (PR-3/PR-4) is what
		// changes it, and must change this assertion with its own falsifier.
		own := cohorts * (widgetsPer - listCells/(comps*cohorts))
		if marked != listCells+own {
			t.Errorf("comps=%d: marked=%d, want listCells+own = %d+%d — the fan-out law moved",
				comps, marked, listCells, own)
		}
		// #479 1b — every mark is attributed to its entry class (the harness
		// cells are all "restactions"), and the by-class split sums to the total.
		byClass := d.dirtyMarkByEntryClassSnapshot()
		if byClass["restactions"] != uint64(marked) {
			t.Errorf("comps=%d: by_entry_class[restactions]=%d, want %d — %v", comps, byClass["restactions"], marked, byClass)
		}
	}

	t.Log("compositions | cells | list-dep cells | dirty-marked by ONE event | marks per cell")
	for _, r := range rows {
		t.Logf("  %5d      | %6d |   %6d       |   %6d                  | %.4f",
			r.comps, r.cells, r.listCells, r.marked, float64(r.marked)/float64(r.cells))
	}

	first, last := rows[0], rows[len(rows)-1]
	compGrowth := float64(last.comps) / float64(first.comps)
	markGrowth := float64(last.marked) / float64(first.marked)
	t.Logf("population grew %.0fx; fan-out per single event grew %.2fx", compGrowth, markGrowth)

	// THE STRUCTURAL CLAIM. If fan-out were bounded (only the changed
	// composition's own cells marked), markGrowth would be ~1 regardless of
	// population. If list-dep cells scale with the population, markGrowth
	// tracks compGrowth. Anything at or above half the population growth means
	// fan-out is population-dependent, which is the regime that squares.
	if markGrowth < compGrowth/2 {
		t.Logf("RESULT: fan-out is NOT strongly population-dependent (%.2fx vs %.0fx) — "+
			"the entry cap, not fan-out, is the binding constraint", markGrowth, compGrowth)
	} else {
		t.Logf("RESULT: fan-out IS population-dependent (%.2fx against %.0fx population growth). "+
			"Event rate also grows with population, so total dirty-mark load grows with its SQUARE.", markGrowth, compGrowth)
	}
}

// TestScaleLaw_ListDepShareIsTheLever sweeps the one parameter 057 does not
// expose, to show how much of the fan-out is attributable to list edges.
func TestScaleLaw_ListDepShareIsTheLever(t *testing.T) {
	const (
		comps      = 200
		cohorts    = 2
		widgetsPer = 10
	)
	gvr := gvrCompositions()
	t.Log("list-dep share | list-dep cells | dirty-marked by ONE event")
	for _, share := range []float64{0.0, 0.1, 0.3, 0.5, 1.0} {
		d, listCells := scalePopulation(t, comps, cohorts, widgetsPer, share)
		_, marked := d.OnObjectEvent(gvr, "ns", "comp-000000", objExists)
		t.Logf("     %.1f       |    %6d      |   %6d", share, listCells, marked)
	}
}
