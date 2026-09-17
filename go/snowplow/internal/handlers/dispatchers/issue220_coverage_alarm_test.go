// issue220_coverage_alarm_test.go — 1.12.7 / #220: the prewarm coverage
// alarm.
//
// #220 went 177 harvested widgets to 5 and nothing went red. The first version
// of this alarm did not fix that: it compared the harvester's cumulative UNION
// against a high-water mark, and a union is monotonic by deliberate design
// (phase1_pip_seed.go forgetCoordinate — remove on positive evidence, never on
// absence), so a collapse could not move it and only an ordinary deletion
// could. Silent on the outage, loud on CRUD. Arms A and B are that pair, and
// they were RED against the shipped code before this rebuild.
//
// EVERY ARM DRIVES A REAL DRIVER. No arm calls recordWalkCoverage, and no arm
// builds a second harvester and republishes it. That helper is exactly why the
// shipped arms were green against broken code: it fabricated a pass transition
// that production never makes, so it could not see that the recorder was
// measuring the wrong set or that a resume pass was being recorded as a
// completed one. Here:
//
//   - A, B, D, E drive phase1WarmupWith — the real boot walk driver — with an
//     injected lister + resolver, where the resolver feeds the REAL
//     harvestNavWidget primitive for the widgets that root reached (which is
//     what resolveNavigationRoot does in production).
//   - B removes its widget through the PRODUCTION removal path:
//     cache.Deps().OnDelete -> the registered gone-forget hook ->
//     forgetCoordinate. Nothing reaches into the harvester by hand.
//   - C drives the REAL F.4 resume branch: a boot walk establishes the figures,
//     then a scope is processed at attempt>0 (AddRateLimited) against a
//     non-empty snapshot, so bootShouldWalk returns false inside the real
//     rePrewarmBootScoped. Its lister FAILS the test if it is called.
//
// THERE IS DELIBERATELY NO ARM ASSERTING THE ALARM FIRES ON EVERY PASS WHILE
// COLLAPSED. It is a transition alarm: E pins that a sustained collapse stays
// silent. An alarm that re-fires is the noise that masked #220 in the first
// place (entry_point_empty, 14 times in 50 minutes).
package dispatchers

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const covNS = "krateo-system"

func covGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// covSyncBuf is a mutex-guarded slog sink (see the note in
// issue216_f6b_gone_forget_test.go: a process-lived ticker logs into whatever
// default is installed, so a bare bytes.Buffer sink races).
type covSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *covSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *covSyncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// covNames returns n widget names, stable across passes so a later pass
// reaching a prefix of them is a genuine REACH regression and not a rename.
func covNames(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, "flex-"+strconv.Itoa(i))
	}
	return out
}

// covRig is ONE harvester pair, published once, driven through real passes —
// the same instance the drivers write and the recorder reads, exactly as
// production wires it (phase1_walk.go publishes the pair it hands the engine).
type covRig struct {
	nav *navWidgetHarvester
	api *contentPrewarmHarvester
	rw  *cache.ResourceWatcher
	// midPass, if set, runs ONCE inside the resolver of the next pass, right
	// after that root's widgets have been harvested — i.e. DURING the pass, at
	// a point where the pass has already reached them. That is the ordering a
	// real delete takes against a ~255s walk, and it is what arm H needs; a
	// delete applied between passes is a different (and easier) case, already
	// covered by arm B. Cleared after it fires.
	midPass func()
}

func newCovRig(t *testing.T) *covRig {
	t.Helper()
	ResetWalkCoverageForTest()
	ResetHarvestInspectionForTest()
	t.Cleanup(ResetWalkCoverageForTest)
	t.Cleanup(ResetHarvestInspectionForTest)
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)

	r := &covRig{nav: newNavWidgetHarvester(), api: newContentPrewarmHarvester(), rw: phase1TestWatcher(t)}
	publishHarvestersForInspection(r.nav, r.api)
	return r
}

// harvest models the tail of a real root descent: resolveNavigationRoot feeds
// each widget it reached to harvestNavWidget. THE REAL PRIMITIVE, including its
// first-write-wins dedupe — which is the whole subtlety of where the per-pass
// reach is recorded.
func (r *covRig) harvest(names []string) {
	for _, n := range names {
		u := &unstructured.Unstructured{}
		u.SetNamespace(covNS)
		u.SetName(n)
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group: covGVR().Group, Version: covGVR().Version, Kind: "Flex",
		})
		r.nav.harvestNavWidget(u, covGVR(), -1, -1, -1, -1)
	}
}

// bootPass drives ONE REAL pass through phase1WarmupWith, the boot walk driver.
// One navigationRoot per perRoot entry; perRoot[i] == nil makes THAT root fail
// to resolve, which is the production partial-pass shape (a root that fails is
// logged and collected into walkErr, not fatal). Returns the captured logs.
func (r *covRig) bootPass(t *testing.T, perRoot ...[]string) string {
	t.Helper()
	lister := func(ctx context.Context) ([]navigationRoot, error) {
		out := make([]navigationRoot, 0, len(perRoot))
		for i := range perRoot {
			out = append(out, navigationRoot{
				Root: routesLoaderCR(covNS, "cov-root-"+strconv.Itoa(i)),
				GVR:  cache.RoutesLoadersGVR(),
			})
		}
		return out, nil
	}
	next := 0
	resolver := func(ctx context.Context, _ navigationRoot) error {
		i := next
		next++
		if perRoot[i] == nil {
			return fmt.Errorf("#220 arm: root %d failed to resolve", i)
		}
		r.harvest(perRoot[i])
		if r.midPass != nil {
			h := r.midPass
			r.midPass = nil
			h()
		}
		return nil
	}

	var buf covSyncBuf
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// A pass with a failing root returns that error by contract — not a test
	// failure, it is arm D's whole subject.
	_ = phase1WarmupWith(ctx, r.rw, lister, resolver, nil, nil, nil, nil)
	return buf.String()
}

func covAlarms(out string) int {
	return strings.Count(out, `"msg":"prewarm.coverage.regressed"`)
}

// ── ARM A — the #220 shape: 177 reachable become 5, nothing deleted ────────
//
// RED against the shipped alarm: it read the cumulative union, which still held
// 177, and said nothing.
func TestIssue220_ArmA_CollapseFiresOnce(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	r := newCovRig(t)

	// A healthy pass. The FIRST completed pass has no predecessor, so it must
	// establish the level without alarming.
	if out := r.bootPass(t, covNames(177)); covAlarms(out) != 0 {
		t.Fatalf("RED (#220): the first completed pass alarmed against an empty baseline:\n%s", out)
	}
	f := WalkCoverageForTest()
	if f.Reached != 177 || f.CompletedPasses != 1 || f.RegressedTotal != 0 || f.WalkedRoots != 1 {
		t.Fatalf("RED (#220): after one healthy pass reached=%d completed=%d regressed=%d roots=%d, "+
			"want 177/1/0/1", f.Reached, f.CompletedPasses, f.RegressedTotal, f.WalkedRoots)
	}

	// THE COLLAPSE: the next completed pass reaches 5 of the same 177. Nothing
	// was deleted, so the harvester still HOLDS all 177 — which is exactly why
	// measuring the union cannot see this.
	out := r.bootPass(t, covNames(5))
	f = WalkCoverageForTest()

	if n := covAlarms(out); n != 1 {
		t.Fatalf("RED (#220): a completed walk that reached 5 widgets where the previous completed "+
			"walk reached 177 emitted %d alarms, want exactly 1. This is the shape that took the "+
			"portal cold with nothing going red:\n%s", n, out)
	}
	if f.Reached != 5 || f.Lost != 172 || f.RegressedTotal != 1 {
		t.Fatalf("RED (#220): reached=%d lost=%d regressed=%d, want 5/172/1", f.Reached, f.Lost, f.RegressedTotal)
	}
	// The union is still 177 — the positive statement of why the shipped alarm
	// was structurally blind. If this ever reads 5, the removal policy at
	// phase1_pip_seed.go:401-403 has been broken and #216 is back.
	if f.HarvestedWidgets != 177 {
		t.Fatalf("RED (#216 guard): the cumulative union moved to %d during a reach collapse. The "+
			"harvester must only drop on positive GONE evidence — a lossy walk must never prune it",
			f.HarvestedWidgets)
	}

	// BOTH LEVELS ON THE LINE: "regressed" alone does not tell an operator
	// whether this is a wobble or the portal going cold.
	rec := findLogRecord(t, out, "prewarm.coverage.regressed")
	if rec == nil {
		t.Fatalf("RED (#220): no parseable prewarm.coverage.regressed record:\n%s", out)
	}
	if rec["reached"] != float64(5) || rec["previous_reached"] != float64(177) || rec["lost"] != float64(172) {
		t.Fatalf("RED (#220): the alarm line reads reached=%v previous_reached=%v lost=%v, want "+
			"5/177/172 — without both levels the operator cannot see 177 → 5",
			rec["reached"], rec["previous_reached"], rec["lost"])
	}
}

// ── ARM B — one widget is legitimately deleted ────────────────────────────
//
// The neighbouring event the alarm must NOT claim. RED against the shipped
// alarm, which fired here and only here.
func TestIssue220_ArmB_LegitimateDeleteIsSilent(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)

	r := newCovRig(t)
	all := covNames(177)
	if out := r.bootPass(t, all); covAlarms(out) != 0 {
		t.Fatalf("premise: the first pass alarmed:\n%s", out)
	}

	// THE PRODUCTION REMOVAL PATH — a real gone verdict through the registered
	// hook, the only thing allowed to remove from the harvester.
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: r.nav, harvester: r.api})
	cache.Deps().OnDelete(covGVR(), covNS, all[0])

	// The next pass reaches everything that still exists.
	out := r.bootPass(t, all[1:])
	f := WalkCoverageForTest()

	if covAlarms(out) != 0 || f.RegressedTotal != 0 {
		t.Fatalf("RED (#220): deleting ONE widget raised the coverage alarm (regressed=%d). A "+
			"deletion is ordinary CRUD; an alarm that fires on it is one an operator silences "+
			"within a week, which lands us back where #220 started:\n%s", f.RegressedTotal, out)
	}
	if f.Reached != 176 {
		t.Fatalf("RED (#220): reached=%d after one of 177 widgets was deleted, want 176", f.Reached)
	}
	if f.Lost != 0 {
		t.Fatalf("RED (#220): lost=%d after a legitimate delete, want 0 — the forget set is what "+
			"discounts it", f.Lost)
	}
}

// ── ARM C — an F.4 resume pass records nothing ────────────────────────────
//
// Drives the REAL resume branch (attempt>0 + non-empty snapshot →
// bootShouldWalk false inside rePrewarmBootScoped), not a hand-installed
// crossed-over state. The shipped code evaluated `rewalked == len(roots)` as
// 0 == 0 here, reported a COMPLETED pass that never walked, and republished the
// figures — which is why walked_roots read 0 on a healthy cluster.
func TestIssue220_ArmC_ResumePassRecordsNothing(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	r := newCovRig(t)

	// A real boot walk first, so there are figures a resume could stomp.
	r.bootPass(t, covNames(20))
	before := WalkCoverageForTest()
	if before.WalkedRoots != 1 || before.Reached != 20 || before.CompletedPasses != 1 {
		t.Fatalf("premise: after a one-root boot walk roots=%d reached=%d completed=%d, want 1/20/1",
			before.WalkedRoots, before.Reached, before.CompletedPasses)
	}

	deps := rePrewarmDeps{
		rw: r.rw,
		lister: func(context.Context) ([]navigationRoot, error) {
			// The discriminator: a resume must not even LIST roots. If this
			// fires, the pass walked and the arm is not testing the resume.
			t.Error("RED (#220 arm C): a resume pass listed roots — it did not take the reuse branch")
			return nil, nil
		},
		harvester: r.api,
		navHarv:   r.nav,
		saEP:      endpoints.Endpoint{},
		saRC:      nil, // never consumed: no root is walked on the reuse branch
		authnNS:   covNS,
	}
	buf := leverBSeedSeams(t, func(context.Context, navWidgetEntry) error { return nil })

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	e.scopeHandler = makeBootScopeHandler(deps)
	boot := prewarmScope{kind: scopeKindBoot}
	e.queue.AddRateLimited(boot) // F.4 posture: NumRequeues>0 → attempt>0
	if e.queue.NumRequeues(boot) == 0 {
		t.Fatalf("premise: AddRateLimited must set NumRequeues>0 for the resume posture")
	}
	leverBWaitReady(e)
	s, shut := e.queue.Get()
	if shut {
		t.Fatalf("premise: queue shut down before the resume pass")
	}
	e.processScope(context.Background(), s)

	logs := buf.String()
	if !strings.Contains(logs, "prewarm.engine.boot.rewalk_reused") {
		t.Fatalf("premise: the REAL resume branch did not run (no rewalk_reused emitted):\n%s", logs)
	}

	after := WalkCoverageForTest()
	if after.ReusePasses != 1 {
		t.Fatalf("RED (#220): a resume pass moved reuse_passes_total by %d, want 1 — without it a pod "+
			"that is reusing healthily reads identically to one that has stopped walking",
			after.ReusePasses)
	}
	if after.WalkedRoots != before.WalkedRoots {
		t.Fatalf("RED (#220 §2b): a resume pass — which walked nothing — republished the figures and "+
			"stomped walked_roots from %d to %d. On a healthy cluster the published metric then "+
			"reads zero roots walked", before.WalkedRoots, after.WalkedRoots)
	}
	if after.Reached != before.Reached || after.CompletedPasses != before.CompletedPasses {
		t.Fatalf("RED (#220): a resume pass was recorded as a pass (reached %d→%d, completed %d→%d). "+
			"It walked nothing; recording it as a completed pass that reached zero is a fabricated "+
			"collapse, and recording it at all makes the gauge meaningless",
			before.Reached, after.Reached, before.CompletedPasses, after.CompletedPasses)
	}
	if after.RegressedTotal != 0 || strings.Contains(logs, "prewarm.coverage.regressed") {
		t.Fatalf("RED (#220): a resume pass raised the coverage alarm:\n%s", logs)
	}
}

// ── ARM D — a partial pass: figures published, never an alarm ─────────────
//
// A walk cut short, or one that lost a root, legitimately reaches less.
// Alarming on it produces false WARNs, and a false WARN is how #220 stayed
// invisible in the first place.
func TestIssue220_ArmD_PartialPassNeverAlarms(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	r := newCovRig(t)

	if out := r.bootPass(t, covNames(177)); covAlarms(out) != 0 {
		t.Fatalf("premise: the first pass alarmed:\n%s", out)
	}
	before := WalkCoverageForTest()

	// Two roots; the first reaches 5 widgets, the second FAILS to resolve.
	out := r.bootPass(t, covNames(5), nil)
	after := WalkCoverageForTest()

	if covAlarms(out) != 0 || after.RegressedTotal != 0 {
		t.Fatalf("RED (#220): a PARTIAL pass raised the alarm. A walk that lost a root reaches less "+
			"by definition; alarming on it trains an operator to ignore the WARN, which is precisely "+
			"how the real 177 → 5 stayed invisible:\n%s", out)
	}
	// It must not move the completed-pass gauge — that is the figure the SLI
	// dashboard compares across pods, and a partial reach is not comparable.
	if after.Reached != before.Reached || after.CompletedPasses != before.CompletedPasses {
		t.Fatalf("RED (#220): a partial pass moved the completed-pass figures (reached %d→%d, "+
			"completed %d→%d)", before.Reached, after.Reached, before.CompletedPasses, after.CompletedPasses)
	}
	// ...but what it DID observe is still visible: an operator reading
	// /debug/vars must see the last real pass, and it walked one root of two.
	if after.WalkedRoots != 1 {
		t.Fatalf("RED (#220): a partial pass published walked_roots=%d, want 1 — the figures it "+
			"observed must still be visible", after.WalkedRoots)
	}
	if after.ReusePasses != 0 {
		t.Fatalf("RED (#220): a partial pass counted as a reuse pass (%d). It DID walk; conflating "+
			"the two hides a driver that has stopped walking altogether", after.ReusePasses)
	}
}

// ── ARM E — a SUSTAINED collapse does not re-fire ─────────────────────────
//
// The transition property. A latching alarm is the noise that masked #220
// (entry_point_empty fired 14 times in 50 minutes and taught everyone to ignore
// the log). The standing state lives in regressed_total and in `reached`.
func TestIssue220_ArmE_SustainedCollapseDoesNotLatch(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	r := newCovRig(t)

	r.bootPass(t, covNames(177))
	if out := r.bootPass(t, covNames(5)); covAlarms(out) != 1 {
		t.Fatalf("premise: the collapsing pass did not fire exactly once:\n%s", out)
	}

	// The collapse PERSISTS: a third completed pass reaches the same 5.
	out := r.bootPass(t, covNames(5))
	f := WalkCoverageForTest()

	if covAlarms(out) != 0 {
		t.Fatalf("RED (#220): the alarm re-fired on a pass that changed nothing. A sustained collapse "+
			"must warn ONCE; an alarm that repeats every pass is the noise that hid this outage:\n%s", out)
	}
	if f.RegressedTotal != 1 {
		t.Fatalf("RED (#220): regressed_total=%d after a sustained collapse, want 1 — it latches at "+
			"one observation, it does not count passes", f.RegressedTotal)
	}
	if f.Reached != 5 {
		t.Fatalf("RED (#220): reached=%d while still collapsed, want 5 — the gauge is the standing "+
			"level and the reason a quiet alarm is not read as health", f.Reached)
	}
	if f.Lost != 0 {
		t.Fatalf("RED (#220): lost=%d on a pass identical to its predecessor, want 0", f.Lost)
	}

	// RECOVERY — a pass that reaches MORE than its predecessor is silent and
	// the gauge follows it back up. (This is the guard the deleted
	// EqualOrBetterDoesNotAlarm arm carried: under a set difference "equal" is
	// the check above and "better" is this one. The high-water arm it also
	// carried — that the baseline may never fall — is deliberately NOT replaced:
	// a high-water baseline over a set that legitimately shrinks ratchets, and
	// after the first deletion it pins above the achievable reach and warns
	// forever.)
	out = r.bootPass(t, covNames(177))
	f = WalkCoverageForTest()
	if covAlarms(out) != 0 {
		t.Fatalf("RED (#220): a pass that RECOVERED to the old level alarmed:\n%s", out)
	}
	if f.Reached != 177 || f.Lost != 0 {
		t.Fatalf("RED (#220): after recovery reached=%d lost=%d, want 177/0", f.Reached, f.Lost)
	}
	if f.RegressedTotal != 1 {
		t.Fatalf("RED (#220): regressed_total=%d after recovery, want 1 — it is the standing record "+
			"that this process observed a collapse and must not be cleared by a later good pass",
			f.RegressedTotal)
	}
}

// ── ARM F — a PARTIAL pass must not become the baseline ───────────────────
//
// The defect both reviewers blocked the rebuild on. The baseline used to rotate
// at the pass boundary while the evaluation happened only on a COMPLETED pass —
// two clocks — so a benign partial silently installed itself as the thing the
// next real collapse was compared against:
//
//	A completes, reach=177
//	B PARTIAL (deadline cut), reach=5   -> not evaluated, but became baseline
//	C completes, real collapse to 5     -> lost = {} -> SILENT
//
// This is not an exotic ordering. Partial passes are routine (the F.4 resume
// path exists because deadline cuts happen) and a collapse caused by a root
// becoming unresolvable FIRST MANIFESTS as a partial, so partial-then-collapsed
// is the likely arrival path for the very outage this alarm exists for.
//
// The correct half is in the same arm deliberately: the fix must not be "never
// trust the partial's reach" applied so bluntly that a RECOVERED pass alarms.
func TestIssue220_ArmF_PartialPassIsNotTheBaseline(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	t.Run("collapse after a partial still fires", func(t *testing.T) {
		r := newCovRig(t)
		if out := r.bootPass(t, covNames(177)); covAlarms(out) != 0 {
			t.Fatalf("premise: the first completed pass alarmed:\n%s", out)
		}

		// A PARTIAL pass: two roots, the first reaches 5, the second fails. It
		// legitimately reached less and must be silent — and must leave no trace
		// on the baseline.
		if out := r.bootPass(t, covNames(5), nil); covAlarms(out) != 0 {
			t.Fatalf("premise: the partial pass alarmed:\n%s", out)
		}

		// THE REAL COLLAPSE, arriving the way it actually arrives.
		out := r.bootPass(t, covNames(5))
		f := WalkCoverageForTest()
		if n := covAlarms(out); n != 1 {
			t.Fatalf("RED (#220 amendment): completed(177) -> PARTIAL(5) -> completed(5) emitted %d "+
				"alarms, want exactly 1. The partial became the baseline, so the collapse compared "+
				"equal and went silent — the alarm is blind on the likeliest arrival path of the "+
				"outage it exists for:\n%s", n, out)
		}
		if f.Reached != 5 || f.Lost != 172 || f.RegressedTotal != 1 {
			t.Fatalf("RED (#220 amendment): reached=%d lost=%d regressed=%d, want 5/172/1 — the "+
				"comparison must be against the last COMPLETED pass (177), not the partial (5)",
				f.Reached, f.Lost, f.RegressedTotal)
		}
	})

	t.Run("recovery after a partial stays silent", func(t *testing.T) {
		r := newCovRig(t)
		if out := r.bootPass(t, covNames(177)); covAlarms(out) != 0 {
			t.Fatalf("premise: the first completed pass alarmed:\n%s", out)
		}
		if out := r.bootPass(t, covNames(5), nil); covAlarms(out) != 0 {
			t.Fatalf("premise: the partial pass alarmed:\n%s", out)
		}

		// The pass after the partial recovers to the previous completed level.
		// Nothing was ever lost, so nothing may fire.
		out := r.bootPass(t, covNames(177))
		f := WalkCoverageForTest()
		if covAlarms(out) != 0 || f.RegressedTotal != 0 {
			t.Fatalf("RED (#220 amendment): completed(177) -> PARTIAL(5) -> completed(177) alarmed "+
				"(regressed=%d). Ignoring the partial must not turn a recovery into a loss:\n%s",
				f.RegressedTotal, out)
		}
		if f.Reached != 177 || f.Lost != 0 {
			t.Fatalf("RED (#220 amendment): reached=%d lost=%d after recovery, want 177/0",
				f.Reached, f.Lost)
		}
	})
}

// ── ARM G — a walking pass that found NO ROOTS is not the baseline either ──
//
// The neighbour of arm F on the other non-evaluated outcome. A driver that
// meant to walk and listed an EMPTY root set covered nothing; it is counted
// (no_roots_passes_total) and otherwise records nothing, so the next real
// collapse must still be measured against the last COMPLETED pass. Note the
// lister hands back a non-nil empty slice — the shape a `roots == nil`
// classification would leave unclassified and let fall through to "completed".
func TestIssue220_ArmG_NoRootsPassIsNotTheBaseline(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	r := newCovRig(t)

	if out := r.bootPass(t, covNames(177)); covAlarms(out) != 0 {
		t.Fatalf("premise: the first completed pass alarmed:\n%s", out)
	}
	before := WalkCoverageForTest()

	// A pass with no roots at all.
	if out := r.bootPass(t); covAlarms(out) != 0 {
		t.Fatalf("premise: a no-roots pass alarmed:\n%s", out)
	}
	mid := WalkCoverageForTest()
	if mid.NoRootsPasses != 1 || mid.ReusePasses != 0 {
		t.Fatalf("RED (#220 amendment): a roots-absent pass counted no_roots=%d reuse=%d, want 1/0 — "+
			"'nothing to walk' is a pod covering nothing and must not be counted as a healthy "+
			"F.4 snapshot reuse", mid.NoRootsPasses, mid.ReusePasses)
	}
	if mid.Reached != before.Reached || mid.CompletedPasses != before.CompletedPasses ||
		mid.WalkedRoots != before.WalkedRoots {
		t.Fatalf("RED (#220): a no-roots pass was recorded as a pass (reached %d->%d, completed %d->%d, "+
			"roots %d->%d)", before.Reached, mid.Reached, before.CompletedPasses, mid.CompletedPasses,
			before.WalkedRoots, mid.WalkedRoots)
	}

	// THE COLLAPSE, one pass later.
	out := r.bootPass(t, covNames(5))
	f := WalkCoverageForTest()
	if n := covAlarms(out); n != 1 {
		t.Fatalf("RED (#220 amendment): completed(177) -> NO ROOTS -> completed(5) emitted %d alarms, "+
			"want exactly 1. A pass that walked nothing must not be able to install itself as the "+
			"baseline:\n%s", n, out)
	}
	if f.Reached != 5 || f.Lost != 172 {
		t.Fatalf("RED (#220 amendment): reached=%d lost=%d, want 5/172", f.Reached, f.Lost)
	}
}

// ── ARM H — a widget deleted DURING the pass that last reached it ─────────
//
// Why the forget window is TWO generations, and why that is not a tolerance.
// The walk runs ~255s and the gone verdict arrives on the dep-event worker, so
// a widget deleted mid-pass — AFTER that pass already reached it — has its
// forget consumed at that pass's evaluation without ever being needed: the
// widget was still in `reached`, so nothing was lost. The loss surfaces at the
// NEXT evaluation, after the promote drops it from `reached` but while it is
// still in the baseline. With a walk that long this is the COMMON ordering, not
// the corner, and a single-generation forget set turns every one of them into a
// false alarm on an ordinary delete.
func TestIssue220_ArmH_DeleteDuringTheReachingPassIsSilent(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetGoneForgetHooksForTest()
	t.Cleanup(cache.ResetGoneForgetHooksForTest)

	r := newCovRig(t)
	all := covNames(177)
	if out := r.bootPass(t, all); covAlarms(out) != 0 {
		t.Fatalf("premise: the first completed pass alarmed:\n%s", out)
	}
	registerHarvesterGoneForgetHook(rePrewarmDeps{navHarv: r.nav, harvester: r.api})

	// PASS 2 reaches all 177 — and THEN, still inside the pass, the real gone
	// verdict for one of them lands through the production removal path.
	r.midPass = func() { cache.Deps().OnDelete(covGVR(), covNS, all[0]) }
	if out := r.bootPass(t, all); covAlarms(out) != 0 {
		t.Fatalf("premise: the pass during which the delete landed alarmed:\n%s", out)
	}
	if r.midPass != nil {
		t.Fatalf("premise: the mid-pass delete never fired — the arm is not testing its own subject")
	}
	if held, _, ok := HarvestHoldsCoordinate(covGVR(), covNS, all[0]); !ok || held {
		t.Fatalf("premise: the gone verdict did not reach the harvester (held=%v ok=%v)", held, ok)
	}

	// PASS 3 reaches everything that still exists. The forget is now one
	// generation old and MUST still discount the disappearance.
	out := r.bootPass(t, all[1:])
	f := WalkCoverageForTest()
	if covAlarms(out) != 0 || f.RegressedTotal != 0 {
		t.Fatalf("RED (#220 amendment): a widget deleted DURING the pass that last reached it raised "+
			"the coverage alarm one pass later (regressed=%d). Its forget was consumed by an "+
			"evaluation that did not need it; with a ~255s walk that is the common ordering, so a "+
			"one-generation forget set alarms on ordinary deletes:\n%s", f.RegressedTotal, out)
	}
	if f.Reached != 176 || f.Lost != 0 {
		t.Fatalf("RED (#220 amendment): reached=%d lost=%d, want 176/0", f.Reached, f.Lost)
	}
}

// ── the coverage sets are SHARED MUTABLE STATE — race arm ─────────────────
//
// This change adds four maps to a type that is already written from two
// goroutines in production: the walk harvests while the dep-event worker
// delivers gone verdicts (forgetCoordinate), and the pass boundary + the
// measurement run from the walk driver. They are covered by the harvester's
// EXISTING mutex, which is the claim this arm exists to falsify — a missed
// lock, or a returned map that is the harvester's own rather than a copy,
// shows up here under -race and nowhere else in this file, because every other
// arm drives a single goroutine.
func TestIssue220_CoverageSets_ConcurrentAccessIsRaceFree(t *testing.T) {
	nav := newNavWidgetHarvester()
	gvr := covGVR()

	const iterations = 200
	var wg sync.WaitGroup
	start := make(chan struct{})

	widget := func(name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetNamespace(covNS)
		u.SetName(name)
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group: gvr.Group, Version: gvr.Version, Kind: "Flex",
		})
		return u
	}

	// Two harvesting goroutines (the walk), one forgetting (the dep-event
	// worker), one rotating the pass (the driver), one measuring (the
	// recorder).
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				nav.harvestNavWidget(widget("flex-"+strconv.Itoa(g)+"-"+strconv.Itoa(i)), gvr, -1, -1, -1, -1)
			}
		}(g)
	}
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			nav.forgetCoordinate(gvr, covNS, "flex-0-"+strconv.Itoa(i))
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations/10; i++ {
			nav.BeginWalk()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			reached, prev, forgotten := nav.coveragePassSnapshotAndPromote()
			// Writing into what was returned must not touch the harvester's own
			// maps — if the accessor handed out its internals, the harvesting
			// goroutines above are writing the same map and -race says so.
			reached["probe"] = struct{}{}
			prev["probe"] = struct{}{}
			forgotten["probe"] = struct{}{}
		}
	}()

	close(start)
	wg.Wait()

	// The probes must not have leaked into the harvester.
	reached, prev, forgotten := nav.coveragePassSnapshotAndPromote()
	for name, set := range map[string]map[string]struct{}{"reached": reached, "prev": prev, "forgotten": forgotten} {
		if _, leaked := set["probe"]; leaked {
			t.Fatalf("RED: coveragePassSnapshotAndPromote returned the harvester's own %s map — a caller's write "+
				"landed in it, so the measurement and the walk share a map", name)
		}
	}
}

// ── prewarm OFF is fully inert ────────────────────────────────────────────
func TestIssue220_NoHarvesterIsInert(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	ResetWalkCoverageForTest()
	ResetHarvestInspectionForTest()
	t.Cleanup(ResetWalkCoverageForTest)
	t.Cleanup(ResetHarvestInspectionForTest)
	cache.ResetPhase1DoneForTest()
	t.Cleanup(cache.ResetPhase1DoneForTest)

	// No publishHarvestersForInspection — the prewarm-off shape.
	r := &covRig{nav: newNavWidgetHarvester(), api: newContentPrewarmHarvester(), rw: phase1TestWatcher(t)}
	out := r.bootPass(t, covNames(3))

	f := WalkCoverageForTest()
	if covAlarms(out) != 0 {
		t.Fatalf("RED (#220): the alarm fired with no harvester published (prewarm off):\n%s", out)
	}
	if f != (WalkCoverageFigures{}) {
		t.Fatalf("RED (#220): prewarm-off published coverage figures %+v — including reuse_passes_total, "+
			"which on a pod with no prewarm at all would read as a driver that stopped walking", f)
	}
}
