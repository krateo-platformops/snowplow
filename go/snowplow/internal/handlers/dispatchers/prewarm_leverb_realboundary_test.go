// prewarm_leverb_realboundary_test.go — #135 F4b Lever B REAL-BOUNDARY arms.
//
// These upgrade the predicate-level LB-1/LB-2 (prewarm_leverb_rewalk_reuse_test.go,
// which hand-compute bootShouldWalk on a bare queue) to DRIVE THE ACTUAL
// processScope → rePrewarmBootScoped walk-vs-reuse decision ≥2× through a real
// local engine worker — per feedback_falsifier_must_drive_real_boundary_not_
// install_crossed_state (the exact lesson the shipped LB test under-delivered on:
// it never re-invoked the real resume boundary; it evaluated the pure predicate).
//
// THE REAL BOUNDARY. e.scopeHandler = makeBootScopeHandler(deps) with a real
// rePrewarmDeps whose discovery WALK is instrumented by a WALK-COUNTING seam:
// deps.lister (rootsLister) is invoked by rePrewarmBootScoped ONLY inside its
// `if doWalk` branch (prewarm_engine_boot.go:308-334). Each invocation
//   (a) increments a walk counter,
//   (b) records the roots it "walked" (the CURRENT topology), and
//   (c) harvests that topology into the SAME process-lived deps.navHarv via the
//       REAL harvestNavWidget primitive (first-write-wins, monotonic — the exact
//       accumulator prod reuses), then returns ZERO navigationRoots so the
//       cluster-bound w.walk/widgets.Resolve loop is a hermetic no-op.
// So walk-count == the number of times the real reuse decision chose WALK, and
// deps.navHarv.snapshot() is the real accumulated seed source rePrewarmBootScoped
// drains — no shadow copy, no hand-installed crossed-over state.
//
// The scope is driven ≥2× through the REAL e.processScope (NOT an inlined copy),
// so the REAL cache.WithBootResumeAttempt(scopeCtx, NumRequeues(s)) install +
// the REAL Forget-on-success / AddRateLimited-on-cut requeue plumbing are what
// feed bootShouldWalk. The ONLY stubbed surfaces are the seed OUTCOME seams
// (seedOneWidgetFn / enumeratePrewarmTargetsForGVRFn) + prewarmScopeTimeoutFn —
// the same named injection points the #105 and F.4 harnesses use.
//
// Hermetic, -race, seams + a real client-go-fake-backed ResourceWatcher only
// (buildP0Watcher). Serializes on engineLatchTestMu (shared counters + the
// process singleton is untouched here — every arm drives a LOCAL newTestEngine()).
// Never touches ./internal/rbac.

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// ── the WALK-COUNTING seam ────────────────────────────────────────────────
//
// walkCountingLister is a rootsLister test double closed over a live-topology
// pointer + a counter. rePrewarmBootScoped calls it ONLY when it decides to WALK
// (the `if doWalk` branch), so:
//   - calls == the count of real WALK decisions,
//   - it harvests the CURRENT topology into the shared deps.navHarv via the REAL
//     harvestNavWidget (so the seed source is the genuine accumulator), and
//   - returns nil roots → the per-root cluster walk loop is a hermetic no-op.
type walkCountingLister struct {
	mu       sync.Mutex
	calls    int        // times the real reuse decision chose WALK
	walked   [][]string // per-walk: the widget names harvested that pass
	topology []string   // the CURRENT topology (widget names), swappable between passes
	navHarv  *navWidgetHarvester
}

func newWalkCountingLister(navHarv *navWidgetHarvester, initial ...string) *walkCountingLister {
	return &walkCountingLister{navHarv: navHarv, topology: append([]string(nil), initial...)}
}

// setTopology swaps the live topology the NEXT walk will harvest (a genuine
// config-vars change: new nav set).
func (l *walkCountingLister) setTopology(names ...string) {
	l.mu.Lock()
	l.topology = append([]string(nil), names...)
	l.mu.Unlock()
}

// lister is the rootsLister rePrewarmBootScoped invokes. It harvests the current
// topology into the shared navHarv via the REAL harvestNavWidget, then returns
// zero roots (the cluster walk loop no-ops).
func (l *walkCountingLister) lister(_ context.Context) ([]navigationRoot, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	// The real walk pass resets the harvester's root index (BeginWalk) then
	// stamps per root — mirror the prod pass shape so first-write-wins dedupe
	// and RootIndex stamping behave exactly as production.
	l.navHarv.BeginWalk()
	l.navHarv.BeginRoot()
	names := append([]string(nil), l.topology...)
	for _, name := range names {
		w := &unstructured.Unstructured{}
		w.SetNamespace("krateo-system")
		w.SetName(name)
		w.SetGroupVersionKind(schema.GroupVersionKind{
			Group: engineSeedWidgetGVR.Group, Version: engineSeedWidgetGVR.Version, Kind: "Table",
		})
		// REAL harvest primitive (first-write-wins) into the shared accumulator.
		l.navHarv.harvestNavWidget(w, engineSeedWidgetGVR, unpaginatedPrewarm)
	}
	l.walked = append(l.walked, names)
	// Return NO roots → the cluster-bound per-root w.walk loop is a no-op.
	return nil, nil
}

func (l *walkCountingLister) walkCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func containsName(names []string, want string) bool {
	for _, n := range names {
		if n == want {
			return true
		}
	}
	return false
}

// leverBRealDeps builds a rePrewarmDeps whose discovery walk is the walk-counting
// seam and whose rw is a real client-go-fake-backed ResourceWatcher (so
// settleRegisteredSet + RegisteredGVRs run for real). Returns deps + the seam.
func leverBRealDeps(t *testing.T) (rePrewarmDeps, *walkCountingLister, *navWidgetHarvester) {
	t.Helper()
	rw, _ := buildP0Watcher(t) // CACHE_ENABLED=true; real informer-backed watcher
	navHarv := newNavWidgetHarvester()
	harv := newContentPrewarmHarvester()
	lister := newWalkCountingLister(navHarv, "widget-A")
	deps := rePrewarmDeps{
		rw:        rw,
		lister:    lister.lister,
		harvester: harv,
		navHarv:   navHarv,
		saEP:      endpoints.Endpoint{},
		saRC:      nil, // no root is ever walked (lister returns zero roots) → rc never consumed
		authnNS:   "krateo-system",
	}
	return deps, lister, navHarv
}

// leverBSeedSeams installs the seed OUTCOME seams for a real-boundary boot pass:
// enumeratePrewarmTargetsForGVRFn → one cohort; seedOneWidgetFn → the per-target
// behavior fn (Marks the BootSeededSet on success so #105's set-delta works,
// exactly like the #105 harness). prewarmScopeTimeoutFn → never-deadline unless
// the caller overrides. Also silences the first-nav latch (nil under these
// tests) and zeroes the customer-inflight gate. Returns a *syncBuffer (#392) of the
// captured JSON logs.
func leverBSeedSeams(t *testing.T, seedFn func(ctx context.Context, e navWidgetEntry) error) *syncBuffer {
	t.Helper()
	zeroCustomerInFlight()
	resetFirstNavLatchForTest()

	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	prevTO := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour }
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(schema.GroupVersionResource, string) []cache.PrewarmTarget {
		return makeTargets(1)
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevSeed := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		return seedFn(ctx, e)
	}
	t.Cleanup(func() { seedOneWidgetFn = prevSeed })

	return &buf
}

// leverBWaitReady pulls a rate-limited (AddRateLimited-deferred) item forward:
// the stock controller backoff base is 5ms, so poll briefly until the queue has
// a ready item (mirrors the F.4 boot-resume harness).
func leverBWaitReady(e *prewarmEngine) {
	deadline := time.Now().Add(3 * time.Second)
	for e.queue.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

// ── C1 (LOAD-BEARING) — real-boundary forget-race ─────────────────────────
//
// Drives the ACTUAL processScope → rePrewarmBootScoped reuse decision ≥2× via a
// real local worker. Sequence (design §5 "B-safety (LOAD-BEARING)"):
//  1. pass 0: attempt==0 → assert the walk RAN (walk-count==1); harvester holds
//     root {widget-A} → seeded {widget-A}.
//  2. force an F.4 deadline-cut requeue (AddRateLimited) → NumRequeues>0.
//  3. invoke the REAL redrive sequence for a NEW topology {widget-A, widget-B}
//     (genuine config-vars change): forgetScope THEN enqueueScope (exactly
//     enqueueBootReDrive's Lever B seam, parameterized on the local engine).
//  4. next real dequeue → assert it WALKED (walk-count==2) and seeded {widget-B}.
//
// RED arm: the pre-fix WIP dropped forgetScope from the redrive (plain
// enqueueScope only). NumRequeues persists >0 → attempt>0 → the resume REUSES
// (walk-count stays 1) → {widget-B} never harvested → missing. Proven RED then
// the real path GREEN. The RED mutation is expressed test-locally (call
// enqueueScope only) — NO prod seam.
func TestLeverB_C1_RealBoundary_ForgetRace_ReWalksNewTopology(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	// run drives the full 2-pass boundary; withForget selects GREEN (prod
	// redrive: forgetScope+enqueueScope) vs RED (enqueueScope only, the WIP).
	run := func(t *testing.T, withForget bool) (walkAfterPass0, walkAfterRedrive int, seededAfterPass0, seededAfterRedrive []string, logText string) {
		deps, lister, _ := leverBRealDeps(t)

		var seededMu sync.Mutex
		seeded := map[string]struct{}{}
		buf := leverBSeedSeams(t, func(ctx context.Context, e navWidgetEntry) error {
			// Model the real success site: Mark the BootSeededSet (so #105's
			// set-delta sees growth) + record the widget name we seeded.
			cache.BootSeededSetFromContext(ctx).Mark(stableSeededKey(ctx, e.W.GetName()))
			seededMu.Lock()
			seeded[e.W.GetName()] = struct{}{}
			seededMu.Unlock()
			return nil
		})
		seededSnapshot := func() []string {
			seededMu.Lock()
			defer seededMu.Unlock()
			out := make([]string, 0, len(seeded))
			for k := range seeded {
				out = append(out, k)
			}
			sort.Strings(out)
			return out
		}

		e := newTestEngine()
		e.yieldPoll = 2 * time.Millisecond
		e.scopeHandler = makeBootScopeHandler(deps)

		boot := prewarmScope{kind: scopeKindBoot}
		ctx := context.Background()

		// ── PASS 0 (attempt==0 → WALK) via REAL processScope.
		e.enqueueScope(boot)
		s0, _ := e.queue.Get()
		e.processScope(ctx, s0) // REAL: installs WithBootResumeAttempt(NumRequeues==0)
		walkAfterPass0 = lister.walkCount()
		seededAfterPass0 = seededSnapshot()

		// ── FORCE the F.4 deadline-cut requeue: AddRateLimited bumps NumRequeues
		// (this is exactly what processScope does on a deadline-cut, and what the
		// design §5 step 2 specifies: "force an F.4 deadline-cut requeue
		// (AddRateLimited) → NumRequeues>0").
		e.queue.AddRateLimited(boot)
		if n := e.queue.NumRequeues(boot); n == 0 {
			t.Fatalf("C1 setup: AddRateLimited must bump the boot requeue count; NumRequeues=%d", n)
		}

		// ── GENUINE config-vars change: new topology {widget-A, widget-B}.
		lister.setTopology("widget-A", "widget-B")

		// ── REDRIVE. GREEN = the prod Lever B seam (forgetScope THEN enqueueScope,
		// exactly enqueueBootReDrive:212-213 on the local engine). RED = the pre-fix
		// WIP (drop forgetScope; plain enqueueScope only).
		if withForget {
			e.forgetScope(boot) // <-- the #135 Lever B invalidation seam
		}
		e.enqueueScope(boot)

		// ── NEXT real dequeue → REAL processScope. GREEN: NumRequeues reset to 0
		// → attempt==0 → WALK the new topology. RED: NumRequeues persists >0 →
		// attempt>0 + non-empty harvester → REUSE (stale).
		leverBWaitReady(e)
		s1, _ := e.queue.Get()
		e.processScope(ctx, s1)
		walkAfterRedrive = lister.walkCount()
		seededAfterRedrive = seededSnapshot()
		return walkAfterPass0, walkAfterRedrive, seededAfterPass0, seededAfterRedrive, buf.String()
	}

	// ── GREEN — the real prod path.
	gW0, gW1, gS0, gS1, gLog := run(t, true /*withForget*/)
	if gW0 != 1 {
		t.Fatalf("C1 GREEN pass 0: attempt==0 MUST walk exactly once; walk-count=%d", gW0)
	}
	if !containsName(gS0, "widget-A") {
		t.Fatalf("C1 GREEN pass 0: must seed {widget-A}; seeded=%v", gS0)
	}
	// Liveness sanity only: the real rePrewarmBootScoped ran to its seed-source
	// summary. NOTE: rewalk_complete fires on EVERY pass (walk AND reuse — it is
	// post-branch at prewarm_engine_boot.go:425), so it is NOT the walk
	// discriminator; the walk discriminator is the walk-count seam (gW0==1 /
	// gW1==2 vs the RED rW1==1) asserted above and below.
	if !strings.Contains(gLog, "prewarm.engine.boot.rewalk_complete") {
		t.Fatalf("C1 GREEN: pass 0 must run to rewalk_complete (liveness); logs:\n%s", gLog)
	}
	if gW1 != 2 {
		t.Fatalf("C1 GREEN redrive: forgetScope reset attempt→0 → the new topology MUST re-WALK "+
			"(walk-count 1→2); got walk-count=%d. logs:\n%s", gW1, gLog)
	}
	if !containsName(gS1, "widget-B") {
		t.Fatalf("C1 GREEN redrive: the re-walk MUST discover+seed the NEW root {widget-B}; seeded=%v", gS1)
	}

	// ── RED — the pre-fix WIP (drop forgetScope).
	rW0, rW1, _, rS1, rLog := run(t, false /*withForget → RED*/)
	if rW0 != 1 {
		t.Fatalf("C1 RED pass 0: attempt==0 MUST still walk once; walk-count=%d", rW0)
	}
	if rW1 != 1 {
		t.Fatalf("C1 RED (LOAD-BEARING): without forgetScope the requeue count persists >0 → attempt>0 → "+
			"the redrive MUST REUSE the stale snapshot (walk-count STAYS 1); got walk-count=%d — if it walked, "+
			"the seam is not what discriminates. logs:\n%s", rW1, rLog)
	}
	if containsName(rS1, "widget-B") {
		t.Fatalf("C1 RED: the stale-reuse WIP must NOT seed the new root {widget-B} (it never re-walked); "+
			"seeded=%v", rS1)
	}

	// The discriminator: the forgetScope seam FLIPS the real re-walk decision.
	if gW1 == rW1 {
		t.Fatalf("C1: forgetScope must CHANGE the real re-walk outcome — GREEN re-walked (walk-count=%d) but "+
			"RED must reuse (walk-count=%d); they did not discriminate", gW1, rW1)
	}

	leverBArtifact(t, "c1_realboundary_forget_race.txt", fmt.Sprintf(
		"REAL processScope→rePrewarmBootScoped driven 2×.\n"+
			"GREEN (forgetScope+enqueueScope): pass0 walk-count=%d seeded=%v; after redrive walk-count=%d seeded=%v (new root widget-B re-walked+seeded).\n"+
			"RED (enqueueScope only, pre-fix WIP): pass0 walk-count=%d; after redrive walk-count=%d seeded=%v (STALE reuse — widget-B missing).\n"+
			"Discriminator: forgetScope flips the real re-walk (GREEN 2 vs RED 1).\n",
		gW0, gS0, gW1, gS1, rW0, rW1, rS1))
}

// ── C2 — compose #105 × #135 through the real gate ─────────────────────────
//
// e.scopeHandler = makeBootScopeHandler(deps) (real rePrewarmBootScoped, real
// bootShouldWalk), a DETERMINISTIC FAILER target present, driven ≥
// bootMaxNoProgressPasses+1 passes via real processScope. Asserts BOTH:
//
//	(a) pass 0 WALKED; the F.4 RESUME passes REUSED (rewalk_reused emitted;
//	    walk-count stays 1 across the resume passes);
//	(b) #105 still trips → converged_with_skips with the failer given up (no
//	    further re-enqueue).
//
// Proves the Lever B walk-skip does NOT perturb #105's set-delta bound and the
// bound still fires under reuse — the exact auto-merge semantic risk.
//
// THE RESUME MECHANISM (the load-bearing subtlety, established by driving the
// REAL boundary — see the C2 note appended to the ledger). Lever B reuse keys on
// NumRequeues>0, which the F.4 deadline-cut path (AddRateLimited, no Forget)
// sets. The #105 pass-tail re-enqueue is a NIL-return + plain Add, so its own
// resumes are Forget-reset to attempt==0 → they WALK (Lever B never reuses a
// #105 re-enqueue). The composition that genuinely exercises BOTH is therefore
// an F.4-deadline-cut-then-resume sequence that ALSO carries the #105 failer:
// each resume is driven at attempt>0 (AddRateLimited) so bootShouldWalk REUSES,
// yet runs its seed loop to a CLEAN nil tail so finalizeBootReEnqueue evaluates
// the set-delta over the REUSED snapshot. We drive that shape directly: before
// each resume Get we forget+AddRateLimited the boot key (the F.4 attempt>0
// posture) and drain the #105 plain-Add coalesce, so every resume is a REUSE
// pass whose failer still lands in the pass failedSet. The set-delta accumulates
// across the REUSE passes and trips at bootMaxNoProgressPasses.
func TestLeverB_C2_Compose105_ReuseDoesNotPerturbSetDeltaBound(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	deps, lister, _ := leverBRealDeps(t)
	// Topology: a healthy target + a deterministic failer (both harvested on the
	// pass-0 walk; reused on every resume).
	lister.setTopology("healthy-w", "det-failer-w")

	buf := leverBSeedSeams(t, func(ctx context.Context, e navWidgetEntry) error {
		switch e.W.GetName() {
		case "det-failer-w":
			// Deterministic non-apierror/non-ctx failure every pass (the #105
			// marketplace-detail class) → recorded into the pass failedSet.
			return detFailErr("det-failer-w")
		default:
			// Healthy: Mark the STABLE (widget,cohort) key every pass — grows the
			// seeded set on pass 0, no growth after (the set-delta crux).
			cache.BootSeededSetFromContext(ctx).Mark(stableSeededKey(ctx, e.W.GetName()))
			return nil
		}
	})

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	e.scopeHandler = makeBootScopeHandler(deps)

	boot := prewarmScope{kind: scopeKindBoot}
	ctx := context.Background()

	// drainBoot pops+Done+Forgets any pending boot scope (the #105 tail's plain
	// Add) so the next pass's attempt is set solely by our AddRateLimited below —
	// modeling "the F.4 deadline-cut resume path is the one running each resume."
	drainBoot := func() {
		for e.queue.Len() > 0 {
			s, shut := e.queue.Get()
			if shut {
				return
			}
			e.queue.Done(s)
			e.queue.Forget(s)
		}
	}

	// ── PASS 0 (attempt==0 → WALK) via REAL processScope.
	e.enqueueScope(boot)
	s0, _ := e.queue.Get()
	e.processScope(ctx, s0)
	passes := 1
	if lister.walkCount() != 1 {
		t.Fatalf("C2 pass 0: attempt==0 MUST walk exactly once; walk-count=%d", lister.walkCount())
	}

	// ── F.4 RESUME passes at attempt>0 (REUSE), each running its seed to a clean
	// nil tail so the #105 set-delta evaluates over the reused snapshot. Loop
	// until converged_with_skips fires (or a safety cap for a non-converging RED).
	const maxResumes = 10
	for r := 0; r < maxResumes; r++ {
		if strings.Contains(buf.String(), "prewarm.engine.boot.converged_with_skips") {
			break
		}
		drainBoot()                  // drop the #105 plain-Add (attempt-0 posture)
		e.queue.AddRateLimited(boot) // F.4 posture: NumRequeues>0 → Lever B REUSE
		if e.queue.NumRequeues(boot) == 0 {
			t.Fatalf("C2 resume setup: AddRateLimited must set NumRequeues>0 for the reuse posture")
		}
		leverBWaitReady(e)
		s, shut := e.queue.Get()
		if shut {
			break
		}
		e.processScope(ctx, s) // REAL: attempt>0 → bootShouldWalk REUSE; clean tail → #105 eval
		passes++
	}

	logText := buf.String()

	// (a) Lever B: pass 0 WALKED exactly once; every resume REUSED (walk-count
	// stays 1). The resume passes must have emitted rewalk_reused.
	if lister.walkCount() != 1 {
		t.Fatalf("C2 (a): only pass 0 must WALK; resume passes must REUSE the harvester snapshot — "+
			"walk-count MUST stay 1, got %d. passes=%d logs:\n%s", lister.walkCount(), passes, logText)
	}
	if passes < bootMaxNoProgressPasses+1 {
		t.Fatalf("C2: expected ≥%d passes (≥1 resume so the set-delta bound can trip under reuse); got %d",
			bootMaxNoProgressPasses+1, passes)
	}
	// rewalk_reused is the reuse marker — emitted ONLY on the walk-SKIP branch.
	// It must fire on every F.4 resume (== passes-1). (rewalk_complete, by
	// contrast, fires on EVERY pass regardless of walk-vs-reuse — it is the
	// boot.complete-adjacent seed-source summary, not the walk discriminator; the
	// discriminator is the walk-count seam + rewalk_reused.)
	reusedCount := strings.Count(logText, "prewarm.engine.boot.rewalk_reused")
	if reusedCount != passes-1 {
		t.Fatalf("C2 (a): every F.4 resume must emit prewarm.engine.boot.rewalk_reused (walk skipped); "+
			"got %d, want passes-1=%d. logs:\n%s", reusedCount, passes-1, logText)
	}

	// (b) #105 STILL trips under reuse: converged_with_skips with EXACTLY the
	// deterministic failer given up, and no further re-enqueue (queue drained).
	rec := findLogRecord(t, logText, "prewarm.engine.boot.converged_with_skips")
	if rec == nil {
		t.Fatalf("C2 (b): #105 set-delta bound must STILL trip under Lever B reuse — "+
			"converged_with_skips must fire. passes=%d walk-count=%d logs:\n%s", passes, lister.walkCount(), logText)
	}
	var givenUp []string
	if gu, ok := rec["given_up_targets"].([]any); ok {
		for _, v := range gu {
			if s, ok := v.(string); ok {
				givenUp = append(givenUp, s)
			}
		}
	}
	if len(givenUp) != 1 || givenUp[0] != "widget/krateo-system/det-failer-w" {
		t.Fatalf("C2 (b): given_up_targets must be EXACTLY [widget/krateo-system/det-failer-w] "+
			"(healthy seeded, only the deterministic failer given up); got %v", givenUp)
	}
	if p, ok := rec["passes"].(float64); ok && int(p) != bootMaxNoProgressPasses {
		t.Fatalf("C2 (b): converged_with_skips passes field = %v; want %d", p, bootMaxNoProgressPasses)
	}
	// No further re-enqueue after convergence (the terminal pass returned nil →
	// Forget; the queue is drained).
	if e.queue.Len() != 0 {
		t.Fatalf("C2 (b): after convergence the boot scope must NOT re-enqueue itself; queue depth=%d", e.queue.Len())
	}

	leverBArtifact(t, "c2_compose_105x135.txt", fmt.Sprintf(
		"REAL processScope over %d passes.\n"+
			"(a) Lever B: walk-count=%d (pass 0 only); rewalk_reused emitted %d× on the F.4 resumes (== passes-1).\n"+
			"(b) #105 STILL trips under reuse: converged_with_skips givenUp=%v passes=%v; no further re-enqueue.\n"+
			"→ the walk-skip does not perturb the set-delta bound; the bound still fires under reuse.\n",
		passes, lister.walkCount(), reusedCount, givenUp, rec["passes"]))
}
