// phase1_walk_coverage.go — 1.12.7 / #220. The prewarm COVERAGE regression
// alarm.
//
// (The #220 design named phase1_walk_metrics.go as a NEW file; it already
// exists and carries the Gate-1 children-count diagnostic, so this lands
// beside it rather than over it.)
//
// WHY IT EXISTS. #220: the walk's reachable set went from 177 widgets to 5.
// Every page in the portal became a cold first load, and NOTHING went red. The
// only WARN emitted in that window was phase1.roots.entry_point_empty about an
// obsolete config key — noise that actively masked the outage. #216 and #219
// were found the same way, by a human reading logs. This is the counter that
// would have caught the third one.
//
// SELF-ADAPTING, NO MAGIC NUMBER. The baseline is the process's own high-water
// mark of distinct harvested widgets, so there is nothing to tune and nothing
// to get wrong in a values file. A strict regression against what THIS process
// already achieved is the alarm. (feedback_self_adapt_no_magic_env_knobs.)
//
// NO GOROUTINE, NO TICKER, NO STATE WITHOUT A REMOVAL PATH. The high-water
// mark is an atomic field updated in-line by the walk that just finished. There
// is nothing to start and therefore nothing that can fail to stop — the failure
// mode behind #219 and #221.
//
// COMPLETED PASSES ONLY. A partial or aborted walk must never trip this. A walk
// cut short by a context deadline, or one where a root failed to resolve,
// legitimately harvests less, and alarming on it would produce false WARNs that
// train an operator to ignore the real one — which is precisely how #220 stayed
// invisible.
//
// NO BEHAVIOUR CHANGE TO THE WALK: nothing here influences what is harvested.
package dispatchers

import (
	"expvar"
	"log/slog"
	"sync"
	"sync/atomic"
)

var (
	// maxHarvestedWidgets is the process-lived high-water mark of DISTINCT
	// widgets a completed walk has harvested. The baseline, and the only
	// state this file keeps.
	maxHarvestedWidgets atomic.Int64
	// The last completed pass's figures, for the expvar.
	lastHarvestedWidgets     atomic.Int64
	lastHarvestedRestActions atomic.Int64
	lastWalkedRoots          atomic.Int64
	// coverageRegressedTotal counts completed passes that came back with
	// strictly fewer distinct widgets than the high-water mark.
	coverageRegressedTotal atomic.Uint64
)

// distinctHarvestedWidgets counts DISTINCT widgets (gvr+namespace+name), not
// harvested entries: one widget is held once per pagination tuple it was
// reached under, so entries move when pagination changes while the reachable
// SET does not. #220 was a change in the set — 177 widgets to 5 — so the set is
// what the alarm has to measure.
func distinctHarvestedWidgets(h *navWidgetHarvester) int {
	if h == nil {
		return 0
	}
	seen := map[string]struct{}{}
	for _, e := range h.snapshot() {
		if e.W == nil {
			continue
		}
		seen[e.GVR.String()+"|"+e.W.GetNamespace()+"|"+e.W.GetName()] = struct{}{}
	}
	return len(seen)
}

// recordWalkCoverage observes one walk pass and raises the regression WARN.
//
// Call it AFTER the walk loop, with the number of roots actually walked and
// whether the pass ran to completion. `completed` must be false for a pass cut
// short by ctx or one where any root failed — see the file doc.
//
// Reads the harvesters published at construction (harvest_inspect.go), so it
// needs no plumbing through the walk drivers and cannot disagree with what
// /debug/harvest reports.
func recordWalkCoverage(walkedRoots int, completed bool) {
	src := liveHarvesters.Load()
	if src == nil {
		return // prewarm off — there is no coverage to speak of
	}
	widgets := int64(distinctHarvestedWidgets(src.nav))
	restActions := int64(len(src.apiRef.snapshot()))

	lastHarvestedWidgets.Store(widgets)
	lastHarvestedRestActions.Store(restActions)
	lastWalkedRoots.Store(int64(walkedRoots))

	if !completed {
		// A partial pass updates the visible figures — an operator looking at
		// /debug/vars should see what the last pass actually found — but it
		// can neither raise the baseline nor trip the alarm.
		return
	}

	prev := maxHarvestedWidgets.Load()
	if widgets > prev {
		maxHarvestedWidgets.Store(widgets)
		return
	}
	if widgets == prev {
		return
	}

	coverageRegressedTotal.Add(1)
	slog.Warn("prewarm.coverage.regressed",
		slog.String("subsystem", "cache"),
		// BOTH numbers on the line. "Regressed" alone is useless; the operator
		// needs to see 177 -> 5 to know whether this is a rounding wobble or
		// the portal going cold.
		slog.Int64("harvested_widgets", widgets),
		slog.Int64("max_harvested_widgets", prev),
		slog.Int64("shortfall", prev-widgets),
		slog.Int64("walked_roots", int64(walkedRoots)),
		slog.Int64("harvested_restactions", restActions),
		slog.String("effect", "a COMPLETED prewarm walk reached fewer distinct widgets than this "+
			"process has reached before: the widgets in the shortfall are no longer prewarmed and "+
			"will be a COLD first load for every user until the walk reaches them again"),
		slog.String("hint", "the walk is lossy by design, so a small wobble can be benign; a large "+
			"drop means the navigation tree stopped being reachable — check that the nav roots in "+
			"the frontend ConfigMap still resolve and that their resourcesRefs still enumerate "+
			"children (#220)"),
	)
}

// ResetWalkCoverageForTest clears the baseline and the last-pass figures.
func ResetWalkCoverageForTest() {
	maxHarvestedWidgets.Store(0)
	lastHarvestedWidgets.Store(0)
	lastHarvestedRestActions.Store(0)
	lastWalkedRoots.Store(0)
	coverageRegressedTotal.Store(0)
}

// WalkCoverageForTest exposes the observed figures.
func WalkCoverageForTest() (widgets, maxWidgets, roots int64, regressed uint64) {
	return lastHarvestedWidgets.Load(), maxHarvestedWidgets.Load(),
		lastWalkedRoots.Load(), coverageRegressedTotal.Load()
}

var walkCoverageExpvarOnce sync.Once

func registerWalkCoverageExpvar() {
	walkCoverageExpvarOnce.Do(func() {
		expvar.Publish("prewarm.coverage", expvar.Func(func() any {
			return map[string]int64{
				"harvested_widgets":     lastHarvestedWidgets.Load(),
				"harvested_restactions": lastHarvestedRestActions.Load(),
				"walked_roots":          lastWalkedRoots.Load(),
				"max_harvested_widgets": maxHarvestedWidgets.Load(),
				"regressed_total":       int64(coverageRegressedTotal.Load()),
			}
		}))
	})
}
