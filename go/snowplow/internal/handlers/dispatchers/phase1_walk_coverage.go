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
// ── WHAT THE FIRST VERSION OF THIS FILE GOT WRONG, AND WHY IT IS WORTH
// SPELLING OUT. It compared each completed pass against a process-lived
// HIGH-WATER MARK of distinct widgets held by the harvester. Both halves of
// that were wrong in the same way:
//
//   - The harvester is a cumulative UNION, monotonic by deliberate design
//     (navWidgetHarvester.forgetCoordinate: remove on positive evidence only,
//     never on absence, because the walk is lossy). Against a monotonic series
//     a collapse is UNREACHABLE — 177 widgets stay counted after a pass reaches
//     5 — so the alarm was structurally silent on the one outage it was built
//     for, and the only thing that could ever move the number down was an
//     ordinary deletion. It was silent on #220 and loud on CRUD.
//   - A high-water baseline over a set that legitimately shrinks RATCHETS:
//     after the first delete it pins above the achievable reach and every later
//     pass warns forever. The fix deletes it rather than dampening it.
//
// ── WHAT IT MEASURES NOW. THE PASS, NOT THE UNION. The harvester records the
// coordinates each walking pass reached (phase1_pip_seed.go, above the dedupe
// return) and the coordinates an authoritative GONE verdict removed. The alarm
// is a SET DIFFERENCE between adjacent passes:
//
//	lost = reached(t-1) - reached(t) - forgotten_since
//
// and it fires iff that set is non-empty. forgotten_since is the union of the
// last TWO forget windows: a forget fires from the dep-event worker
// asynchronously to a walk that runs ~255s, so an explanation can arrive one
// pass after the disappearance it explains. That carry is bounded at exactly
// one pass and is the width of a real race, not a tolerance.
//
// ── THE BASELINE ROLLS ON THE EVALUATION, NOT ON THE PASS BOUNDARY. Both
// generations — the baseline reach and the forget window — are promoted inside
// recordWalkCoverage's completed-pass evaluation, in the same mutex hold that
// read them (navWidgetHarvester.coveragePassSnapshotAndPromote). BeginWalk only
// CLEARS the current pass's reach set. Rolling the baseline at the boundary
// instead puts the two on different clocks and a benign PARTIAL pass silently
// becomes the baseline:
//
//	A completes, reach=177
//	B PARTIAL (deadline cut), reach=5   -> not evaluated, but becomes baseline
//	C completes, real collapse to 5     -> lost = {} -> SILENT
//
// Partial passes are routine here (the F.4 resume path exists because deadline
// cuts happen) and a collapse caused by a root becoming unresolvable FIRST
// MANIFESTS as a partial, so that is the likely arrival path and not an exotic
// one. The forget window must move on the SAME clock or an ordinary delete
// turns into a false alarm instead — see the field block on navWidgetHarvester.
//
// ── IT IS A TRANSITION ALARM AND IT MUST NOT LATCH. It fires once, on the
// collapsing pass. While the collapse persists, reached(t-1) == reached(t) and
// it is silent. That is deliberate: the noise that masked #220 was a WARN
// firing 14 times in 50 minutes, and an alarm that re-fires every pass trains
// an operator to filter it. The standing state is carried by two other things:
// regressed_total LATCHES at >= 1 for the life of the process, and `reached` is
// an absolute gauge that reads 5, not 177. Event, state, level — no one of the
// three is sufficient alone, which docs/architecture/observability.md says in
// the operator's own reading surface.
//
// ── WHAT ITS ZERO DOES NOT EXCLUDE. A pod that BOOTS already collapsed never
// alarms: on its first completed pass there is no previous pass to differ
// against. That is structural — any in-process baseline is built from this
// process's own observations — and it is not solved here. It is why `reached`
// is published as an absolute, pod-uptime-independent gauge: the cross-boot
// comparison belongs to the SLI dashboard, against that gauge across versions.
//
// ── OUTCOMES, NOT A BOOLEAN. `completed bool` could not express "this pass did
// not walk at all", which is exactly what an F.4 resume pass does
// (prewarm_engine_boot.go skips the ~255s discovery re-walk and reuses the
// snapshot) and what a roots-absent boot does. Both used to evaluate
// `0 == 0` -> completed and republish the figures, which is why walked_roots
// read 0 on a healthy cluster. A non-walking pass now records NOTHING, and the
// two non-walking states are counted APART: reuse_passes_total for a deliberate
// F.4 snapshot reuse (healthy) and no_roots_passes_total for a driver that
// found nothing to walk (a pod covering nothing). One counter for both would
// hide the broken state inside the healthy one.
//
// NO GOROUTINE, NO TICKER, NO STATE WITHOUT A REMOVAL PATH. Everything here is
// an atomic updated in-line by the pass that just finished, plus four maps on
// the harvester that are replaced wholesale each pass. There is nothing to
// start and therefore nothing that can fail to stop — the failure mode behind
// #219 and #221.
//
// NO BEHAVIOUR CHANGE TO THE WALK: nothing here influences what is harvested,
// seeded or evicted.
package dispatchers

import (
	"expvar"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// walkPassOutcome is what a walk driver observed about the pass it just ran.
// The three cases are genuinely different and the middle one is the reason this
// is not a bool.
type walkPassOutcome int

const (
	// walkPassReusedSnapshot — an F.4 deadline-cut RESUME that deliberately
	// skipped the ~255s discovery re-walk and reused the process-lived
	// harvester snapshot. It is not a pass that reached zero widgets; it is not
	// a pass. Records nothing but reuse_passes_total. This is the HEALTHY
	// non-walking state.
	walkPassReusedSnapshot walkPassOutcome = iota
	// walkPassNoRoots — the driver meant to walk and found nothing to walk:
	// the config-vars ConfigMap absent at boot, or a roots list that came back
	// empty. Also records nothing, but it is counted SEPARATELY from a resume:
	// "reusing a snapshot" is healthy and "there was nothing to walk" is a pod
	// that is not covering anything, and a single counter merging them hides
	// the second behind the first — exactly the two states the observability
	// docs row exists to separate.
	walkPassNoRoots
	// walkPassPartial — the walk ran but did not finish every root (ctx
	// expired, or a root failed to resolve). It legitimately reached less, so
	// its figures are published and it never alarms.
	walkPassPartial
	// walkPassCompleted — every root walked to completion. The only outcome
	// that is evaluated, and the only one that moves the `reached` gauge or
	// rolls the baseline.
	walkPassCompleted
)

var (
	// The last WALKING pass's raw figures (partial or completed).
	lastHarvestedWidgets     atomic.Int64
	lastHarvestedRestActions atomic.Int64
	lastWalkedRoots          atomic.Int64
	// lastReachedWidgets is THE output of this file: how many distinct widgets
	// the last COMPLETED pass actually reached. An absolute gauge — unlike the
	// cumulative union it is independent of pod uptime, so two pods of
	// different ages serving the same cluster report the same number and a
	// dashboard can compare it across restarts and versions.
	lastReachedWidgets atomic.Int64
	// lastLostWidgets is |lost| from the last completed evaluation.
	lastLostWidgets atomic.Int64
	// completedPassesTotal — without it a pod that has not completed a pass yet
	// reads identically to one whose passes collapsed to zero reach.
	completedPassesTotal atomic.Uint64
	// reusePassesTotal counts F.4 RESUME passes: a pass that deliberately
	// reused the harvester snapshot instead of re-walking discovery. Healthy.
	reusePassesTotal atomic.Uint64
	// noRootsPassesTotal counts passes that meant to walk and found no roots.
	// Split out from reusePassesTotal because a roots-absent boot is "nothing
	// to walk" — a pod covering nothing — and NOT "reusing, healthy"; merging
	// them lets the broken state hide inside the healthy one's count.
	noRootsPassesTotal atomic.Uint64
	// lastCompletedPassUnix stamps the last completed pass so a dashboard can
	// align the gauge to a window instead of averaging a stale value across a
	// restart.
	lastCompletedPassUnix atomic.Int64
	// coverageRegressedTotal LATCHES: it is the standing "this process observed
	// a coverage collapse" flag, and the counterweight to a transition alarm
	// that deliberately goes quiet.
	coverageRegressedTotal atomic.Uint64
)

// distinctHarvestedWidgets counts DISTINCT widgets (gvr+namespace+name) the
// harvester currently HOLDS — the cumulative union across every pass this
// process has run. Published as harvested_widgets for continuity with
// /debug/harvest, and explicitly NOT what the alarm measures: it is
// path-dependent on pod uptime and it cannot fall when reachability does.
func distinctHarvestedWidgets(h *navWidgetHarvester) int {
	if h == nil {
		return 0
	}
	seen := map[string]struct{}{}
	for _, e := range h.snapshot() {
		if e.W == nil {
			continue
		}
		seen[navWidgetCoordKey(e.GVR, e.W.GetNamespace(), e.W.GetName())] = struct{}{}
	}
	return len(seen)
}

// beginWalkCoveragePass opens a real walk pass on the live nav harvester: it
// clears the per-pass reach set (navWidgetHarvester.BeginWalk) so what the pass
// about to run reaches is recorded from empty, and — unchanged #99b semantics —
// resets the config-root index to -1 so the per-root BeginRoot() calls stamp
// RootIndex 0..N-1 in config.json order every pass.
//
// It does NOT roll the baseline or the forget window; those are promoted at the
// completed evaluation in recordWalkCoverage, so the two can never end up on
// different clocks.
//
// BOTH drivers go through this one function, and it reads the harvesters
// published at construction exactly as recordWalkCoverage does. That is the
// point: the pass boundary and the measurement are then structurally taken on
// the same instance, rather than being the same instance by coincidence of two
// call sites happening to hold the same pointer. In production that pointer is
// necessarily set — phase1_walk.go publishes the pair BEFORE the boot walk and
// hands the very same navWidgetHarvester to the engine, and the engine is
// constructed only when that harvester exists — so the nil branch here is the
// prewarm-off pod, which has no pass to open.
//
// Call it at the top of a pass that is actually going to walk; a RESUME pass
// must not call it at all.
func beginWalkCoveragePass() {
	src := liveHarvesters.Load()
	if src == nil {
		return // prewarm off
	}
	src.nav.BeginWalk()
}

// recordWalkCoverage observes one walk pass and raises the coverage WARN.
//
// Call it AFTER the walk loop with the number of roots actually walked and the
// pass outcome. Reads the harvesters published at construction
// (harvest_inspect.go), so it needs no plumbing through the walk drivers and
// cannot disagree with what /debug/harvest reports.
func recordWalkCoverage(walkedRoots int, outcome walkPassOutcome) {
	src := liveHarvesters.Load()
	if src == nil {
		return // prewarm off — there is no coverage to speak of
	}

	switch outcome {
	case walkPassReusedSnapshot:
		// An F.4 resume reused the harvester snapshot. It observed NOTHING, so
		// it publishes nothing: republishing here is what used to stomp
		// walked_roots to 0 on a healthy cluster, and treating it as a completed
		// pass that reached zero would be a fabricated collapse. The counter is
		// the whole record of it — and it must NOT roll the baseline either, or
		// the next real pass would be compared against a pass that never ran.
		reusePassesTotal.Add(1)
		return
	case walkPassNoRoots:
		// The driver meant to walk and had nothing to walk. Same recording
		// discipline, different counter: this state is not healthy and must not
		// be counted as a reuse.
		noRootsPassesTotal.Add(1)
		return
	}

	widgets := int64(distinctHarvestedWidgets(src.nav))
	restActions := int64(len(src.apiRef.snapshot()))

	// The figures of the pass that actually walked — a partial pass updates
	// them too, because an operator looking at /debug/vars should see what the
	// last real pass found.
	lastHarvestedWidgets.Store(widgets)
	lastHarvestedRestActions.Store(restActions)
	lastWalkedRoots.Store(int64(walkedRoots))

	if outcome != walkPassCompleted {
		// A partial pass legitimately reached less: it can neither move the
		// `reached` gauge (which must stay comparable across pods) nor trip the
		// alarm. Alarming on it would produce false WARNs that train an operator
		// to ignore the real one — precisely how #220 stayed invisible.
		return
	}

	// ONE mu acquisition does the read, the input capture and the promotion of
	// BOTH generations. The promotion is anchored HERE, on the completed
	// evaluation, and nowhere else: anchoring it at the pass boundary instead
	// lets a benign PARTIAL pass install itself as the baseline, and a real
	// collapse arriving as completed(177) → partial(5) → completed(5) — the
	// LIKELY arrival order, because a root that stopped resolving manifests
	// first as a partial — then compares equal and is silent.
	reached, prev, forgotten := src.nav.coveragePassSnapshotAndPromote()
	lastReachedWidgets.Store(int64(len(reached)))
	completedPassesTotal.Add(1)
	lastCompletedPassUnix.Store(time.Now().Unix())

	// THE MEASURE: coordinates the previous pass reached, this pass did not,
	// and no GONE verdict in the last two windows explains.
	var lost []string
	for coord := range prev {
		if _, stillReached := reached[coord]; stillReached {
			continue
		}
		if _, explained := forgotten[coord]; explained {
			continue
		}
		lost = append(lost, coord)
	}
	lastLostWidgets.Store(int64(len(lost)))
	if len(lost) == 0 {
		// Includes the sustained-collapse case: reached(t) == reached(t-1) at
		// the collapsed level, so this is silent and regressed_total stays where
		// it is. `reached` is the gauge that shows the standing level.
		return
	}

	sort.Strings(lost)
	examples := lost
	if len(examples) > 5 {
		examples = examples[:5]
	}

	coverageRegressedTotal.Add(1)
	slog.Warn("prewarm.coverage.regressed",
		slog.String("subsystem", "cache"),
		// BOTH levels on the line. "Regressed" alone is useless; the operator
		// needs to see 177 -> 5 to know whether this is a wobble or the portal
		// going cold.
		slog.Int64("reached", int64(len(reached))),
		slog.Int64("previous_reached", int64(len(prev))),
		slog.Int64("lost", int64(len(lost))),
		slog.Int64("forgotten_since", int64(len(forgotten))),
		slog.Any("lost_examples", examples),
		slog.Int64("walked_roots", int64(walkedRoots)),
		slog.Int64("harvested_widgets", widgets),
		slog.Int64("harvested_restactions", restActions),
		slog.String("effect", "a COMPLETED prewarm walk stopped reaching widgets the PREVIOUS completed "+
			"walk reached, and no deletion explains them: those widgets are no longer prewarmed and "+
			"will be a COLD first load for every user until the walk reaches them again"),
		slog.String("hint", "this is a TRANSITION alarm — it fires once, on the pass that lost the "+
			"widgets, and goes quiet while the loss persists, so a later lost=0 means 'no change since "+
			"the previous pass', NOT 'healthy'. Read `reached` for the standing level and "+
			"regressed_total for whether this process ever saw a collapse. A large loss means the "+
			"navigation tree stopped being reachable — check that the nav roots in the frontend "+
			"ConfigMap still resolve and that their resourcesRefs still enumerate children (#220)"),
	)
}

// WalkCoverageFigures is the published coverage state, for tests.
type WalkCoverageFigures struct {
	// Reached is the last COMPLETED pass's distinct-widget reach.
	Reached int64
	// Lost is |lost| at the last completed evaluation.
	Lost int64
	// HarvestedWidgets is the cumulative union the harvester holds.
	HarvestedWidgets     int64
	HarvestedRestActions int64
	WalkedRoots          int64
	CompletedPasses      uint64
	ReusePasses          uint64
	NoRootsPasses        uint64
	RegressedTotal       uint64
	LastCompletedUnix    int64
}

// ResetWalkCoverageForTest clears the published figures. The per-pass sets live
// on the harvester and are reset by constructing one.
func ResetWalkCoverageForTest() {
	lastHarvestedWidgets.Store(0)
	lastHarvestedRestActions.Store(0)
	lastWalkedRoots.Store(0)
	lastReachedWidgets.Store(0)
	lastLostWidgets.Store(0)
	lastCompletedPassUnix.Store(0)
	completedPassesTotal.Store(0)
	reusePassesTotal.Store(0)
	noRootsPassesTotal.Store(0)
	coverageRegressedTotal.Store(0)
}

// WalkCoverageForTest exposes the observed figures.
func WalkCoverageForTest() WalkCoverageFigures {
	return WalkCoverageFigures{
		Reached:              lastReachedWidgets.Load(),
		Lost:                 lastLostWidgets.Load(),
		HarvestedWidgets:     lastHarvestedWidgets.Load(),
		HarvestedRestActions: lastHarvestedRestActions.Load(),
		WalkedRoots:          lastWalkedRoots.Load(),
		CompletedPasses:      completedPassesTotal.Load(),
		ReusePasses:          reusePassesTotal.Load(),
		NoRootsPasses:        noRootsPassesTotal.Load(),
		RegressedTotal:       coverageRegressedTotal.Load(),
		LastCompletedUnix:    lastCompletedPassUnix.Load(),
	}
}

var walkCoverageExpvarOnce sync.Once

func registerWalkCoverageExpvar() {
	walkCoverageExpvarOnce.Do(func() {
		expvar.Publish("prewarm.coverage", expvar.Func(func() any {
			return map[string]int64{
				// The SLI row: absolute, pod-uptime-independent, comparable
				// across pods, restarts and versions.
				"reached":                  lastReachedWidgets.Load(),
				"lost":                     lastLostWidgets.Load(),
				"completed_passes_total":   int64(completedPassesTotal.Load()),
				"last_completed_pass_unix": lastCompletedPassUnix.Load(),
				"reuse_passes_total":       int64(reusePassesTotal.Load()),
				"no_roots_passes_total":    int64(noRootsPassesTotal.Load()),
				"regressed_total":          int64(coverageRegressedTotal.Load()),
				// The last walking pass's raw figures. harvested_widgets is the
				// cumulative UNION the harvester holds — path-dependent on pod
				// uptime, and NOT the coverage measure.
				"harvested_widgets":     lastHarvestedWidgets.Load(),
				"harvested_restactions": lastHarvestedRestActions.Load(),
				"walked_roots":          lastWalkedRoots.Load(),
			}
		}))
	})
}
