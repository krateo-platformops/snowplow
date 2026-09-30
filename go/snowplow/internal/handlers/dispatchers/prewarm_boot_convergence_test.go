// prewarm_boot_convergence_test.go — #105 falsifiers for the per-boot-scope-
// lifetime re-walk re-enqueue bound (docs/105-marketplace-rewalk-boot-budget-
// trace-2026-07-24.md §7, PM conditions C-105-1/2/3/5).
//
// THE BOUNDARY DRIVEN FOR REAL (feedback_falsifier_must_drive_real_boundary_not_
// install_crossed_state): every arm drives the REAL seedScopeYielding
// re-invocation ≥N times through a REAL local engine worker's processScope →
// AddRateLimited requeue → re-Get → re-invoke loop. The engine-lived
// bootConvergenceState is installed by the REAL processScope, accumulates the
// cross-pass set-delta across the REAL resume passes, and the REAL
// finalizeBootReEnqueue tail decides re-enqueue. Nothing crossed-over is
// hand-installed: the state persists because the REAL scope requeues, exactly
// as in production. The ONLY things stubbed are the per-target seed OUTCOMES
// via the seedOneWidgetFn / seedOneRestactionFn seams (the design's named
// injection points §7) + prewarmScopeTimeoutFn (never a real deadline).
//
// SEEDED-SET MODELING (§5.0). A stub seed OUTCOME must model what the real
// primitive does at its success site: on a genuine Put OR a fresh-skip it
// cache.BootSeededSetFromContext(ctx).Mark(key)s the (target,cohort) key (the
// two success sites in phase1_pip_seed.go). A stub that returns nil WITHOUT
// marking would model a target that never warms — not the healthy-cohort shape.
// So the healthy stub Marks a STABLE per-(widget,cohort) key every pass (growth
// on first encounter, no growth after — the set-delta crux); the failer stub
// returns a deterministic non-apierror/non-ctx error every pass (recorded into
// the pass failedSet by the REAL classifyEngineSeedErr).
//
// DISCRIMINATION (feedback_falsifier_shape_must_discriminate): every arm carries
// ≥1 healthy always-succeeds target ALONGSIDE the failer, and Arm A's double-RED
// pins that a COUNT-based "seededThisPass>0 → progress" impl does NOT converge
// (the healthy target's per-pass Put/re-Put keeps a count > 0 forever) while the
// SET-DELTA impl does. A total-failure toy is inadmissible.
//
// Hermetic, -race, seams only. Serializes on engineLatchTestMu (shared counters
// + the process singleton is untouched here — every arm uses a LOCAL
// newTestEngine()). Never touches ./internal/rbac. Per-arm evidence transcripts
// go to each test's own t.TempDir() (arch Note A — no shared filesystem state
// across a concurrent package run); the path is t.Logf'd.

package dispatchers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// newServiceUnavailableErr is a 503 — operational (IsServiceUnavailable),
// NOT a ctx error, so it reaches the REAL classifyEngineSeedErr (and the pass
// failedSet) rather than aborting the pass at the ctx.Err() guard. This is the
// transient shape whose LATER success shrinks the failedSet + grows the
// seededSet (a set-delta progress → the no-progress counter resets, Arm B).
func newServiceUnavailableErr(name string) error {
	return fmt.Errorf("resolve widget krateo-system/%s: %w", name,
		&apierrors.StatusError{ErrStatus: metav1.Status{Code: 503, Reason: metav1.StatusReasonServiceUnavailable}})
}

// ── shared #105 harness ──────────────────────────────────────────────────

// stableSeededKey models the (widget, cohort) cell key a real primitive would
// Mark into the seeded-set — stable across passes for the same (widget,cohort)
// so a re-Put/fresh-skip on a later pass re-Marks an ALREADY-member key (no set
// growth), while a genuinely-new cohort/widget grows the set. Derived from the
// widget name + the cohort username (read off the cohort ctx, exactly as the
// real seed identity flows through withCohortSeedContext → WithUserInfo).
func stableSeededKey(ctx context.Context, widgetName string) string {
	user := "anon"
	if ui, err := xcontext.UserInfo(ctx); err == nil && ui.Username != "" {
		user = ui.Username
	}
	return "boot105|" + widgetName + "|" + user
}

// bootConvOutcome is the per-target seed behavior a #105 arm injects.
type bootConvOutcome int

const (
	// outcomeHealthy: SUCCEEDS every pass. Models the real primitive's success
	// site — Marks the stable (widget,cohort) key into the seeded-set (Put on
	// first encounter → set grows; re-Put/fresh-skip on later passes → same key,
	// no growth). Returns nil.
	outcomeHealthy bootConvOutcome = iota
	// outcomeDeterministicFail: FAILS every pass with a synthetic non-apierror,
	// non-ctx error (a "resolve filter: expected a string"-shaped errors.New —
	// jq-agnostic, NOT marketplace-detail). The REAL classifySeedErr fail-loud
	// default classifies it operational → recorded into the pass failedSet.
	outcomeDeterministicFail
	// outcomeTransientThenSucceed: FAILS on pass 1 with a ctx-deadline (the
	// canonical transient shape classifySeedErr maps to operational)… wait: a
	// ctx-error aborts the pass BEFORE the classifier (seedWidgetTarget checks
	// ctx.Err()). So the transient shape here uses an IsServiceUnavailable 503
	// (operational, NOT a ctx error) so it reaches the classifier + failedSet on
	// the early passes, then SUCCEEDS from transientClearsAtPass onward (failedSet
	// shrinks + seededSet grows → set-delta progress → counter resets).
	outcomeTransientThenSucceed
)

// bootConvTarget is one injected widget target: its name + outcome + (for the
// transient) the pass at which it starts succeeding.
type bootConvTarget struct {
	name                  string
	outcome               bootConvOutcome
	transientClearsAtPass int // 1-based; only for outcomeTransientThenSucceed
}

// detFailErr is a deterministic non-apierror, non-ctx seed error — the #105
// marketplace-detail class (a jq-eval error), jq-agnostic.
func detFailErr(name string) error {
	return fmt.Errorf("resolve widget krateo-system/%s: unable to resolve filter: expected a string", name)
}

// runBootConvergence drives the REAL engine worker over N boot passes with the
// given targets, returning per-pass observations. It:
//   - builds a LOCAL engine (real processScope installs the real
//     bootConvergenceState + BootSeededSet on the boot scope ctx),
//   - sets prewarmScopeTimeoutFn to never-deadline,
//   - swaps seedOneWidgetFn to the outcome-modeling stub (Marks the seeded-set
//     for healthy/cleared-transient; returns the deterministic/transient error
//     otherwise),
//   - sets scopeHandler = a closure that calls the REAL seedScopeYielding with
//     the injected widget entries + the seam enumerator returning ONE cohort,
//   - runs the worker loop: enqueue boot → process → (re-enqueue?) → re-process,
//     capped, recording rewalk_complete + converged_with_skips from the logs.
//
// The pass counter is shared with the stub so the transient can flip at a given
// pass and the healthy target can model TTL re-Put oscillation.
type bootConvObs struct {
	rewalkCompletes int
	converged       bool
	givenUp         []string
	passesField     float64
	logText         string
	handlerCalls    int
}

func runBootConvergence(t *testing.T, targets []bootConvTarget, maxIters int, healthyRePutEveryPass bool) bootConvObs {
	t.Helper()
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	// Isolation (arch Note A): reset the process-global first-nav latch before
	// every boot-driving arm — the seedScopeYielding flat-seed loop fires the
	// latch (currentFirstNavLatch()), so a latch left FIRED by a sibling test (or
	// a concurrent-package run) would let our passes no-op the fire and could
	// perturb an assertion via bleed. Every sibling first-nav-latch test does
	// this (prewarm_f4_boot_resume_test.go, prewarm_first_nav_latch_segment_test.go);
	// centralizing it here covers ALL #105 boot-driving arms uniformly. Pure test
	// isolation — no behavior change to the arms.
	resetFirstNavLatchForTest()

	// Cache ON — REQUIRED for the set-delta discriminator. WithBootSeededSet
	// (and every ctx-carried seed sink) no-ops under cache.Disabled(), so with
	// the cache off the seeded-set would never install and its GROWTH signal
	// would never be exercised — the whole point of the set-delta over a count.
	// t.Setenv restores the prior value on cleanup.
	t.Setenv("CACHE_ENABLED", "true")

	buf := captureDefaultSlogForTest(t, slog.LevelInfo)

	prevTO := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour }
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	// The seam enumerator returns ONE cohort (a single representative identity) —
	// enough for the set-delta shape (a stable cohort whose healthy cells stay
	// members). makeTargets(1) gives Username "user-0".
	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(schema.GroupVersionResource, string) []cache.PrewarmTarget {
		return makeTargets(1)
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	// Shared pass counter (incremented once per seedScopeYielding pass, at the
	// scopeHandler entry) so the stub knows which pass it is running in.
	pass := 0

	prevSeed := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		name := e.W.GetName()
		var tgt *bootConvTarget
		for i := range targets {
			if targets[i].name == name {
				tgt = &targets[i]
				break
			}
		}
		if tgt == nil {
			return nil
		}
		switch tgt.outcome {
		case outcomeHealthy:
			// Model the success site: Mark the stable (widget,cohort) key. On
			// pass 1 this is a genuine Put (set grows); on later passes it models
			// a fresh-skip (healthyRePutEveryPass=false) OR a TTL re-Put
			// (healthyRePutEveryPass=true) — either way the SAME key is Marked, so
			// the seeded SET does not grow after pass 1. (The count-based RED
			// companion keys on "did a Put happen this pass," which re-Put keeps
			// true forever.)
			cache.BootSeededSetFromContext(ctx).Mark(stableSeededKey(ctx, name))
			return nil
		case outcomeDeterministicFail:
			return detFailErr(name)
		case outcomeTransientThenSucceed:
			if pass >= tgt.transientClearsAtPass {
				cache.BootSeededSetFromContext(ctx).Mark(stableSeededKey(ctx, name))
				return nil
			}
			// 503 — operational (reaches the classifier + failedSet), NOT a ctx
			// error (which would abort the pass before classification).
			return newServiceUnavailableErr(name)
		}
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevSeed })

	// Build the widget entries (one per target).
	widgets := make([]navWidgetEntry, 0, len(targets))
	for _, tg := range targets {
		widgets = append(widgets, makeWidgetEntry("krateo-system", tg.name))
	}

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	handlerCalls := 0
	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		if s.kind != scopeKindBoot {
			return nil
		}
		pass++
		handlerCalls++
		// REAL seedScopeYielding — the boundary under test. No restactions; the
		// widget loop carries the same shared classifier + finalize tail.
		return seedScopeYielding(ctx,
			nil /* restactionRefs */, widgets,
			endpoints.Endpoint{}, nil /* saRC */, "test-authn-ns", seedModeBoot)
	}

	// DETERMINISTIC synchronous drive (no goroutine, no settle heuristic — the
	// #105 tail re-enqueue is an IMMEDIATE plain Add, so a converged pass leaves
	// the queue EMPTY and the loop ends; a still-looping RED re-Adds every pass
	// and hits the maxIters cap). This drives the REAL processScope (which
	// installs the engine-lived bootConvergenceState + BootSeededSet on the boot
	// scope ctx and REUSES it across the re-Adds — the cross-pass state under
	// test) exactly as runWorker would, minus the goroutine timing.
	ctx := context.Background()
	e.enqueueScope(prewarmScope{kind: scopeKindBoot})
	for iter := 0; iter < maxIters && e.queue.Len() > 0; iter++ {
		s, shutdown := e.queue.Get()
		if shutdown {
			break
		}
		e.processScope(ctx, s) // Done + Forget/re-Add handled inside
	}

	logText := buf.String()
	obs := bootConvObs{
		rewalkCompletes: strings.Count(logText, "prewarm.engine.boot.rewalk_complete"),
		handlerCalls:    handlerCalls,
		logText:         logText,
	}
	if rec := findLogRecord(t, logText, "prewarm.engine.boot.converged_with_skips"); rec != nil {
		obs.converged = true
		if p, ok := rec["passes"].(float64); ok {
			obs.passesField = p
		}
		if gu, ok := rec["given_up_targets"].([]any); ok {
			for _, v := range gu {
				if s, ok := v.(string); ok {
					obs.givenUp = append(obs.givenUp, s)
				}
			}
		}
	}
	return obs
}

// write105Artifact writes a per-arm evidence transcript under the test's OWN
// t.TempDir() (arch Note A hardening) — a per-test, auto-cleaned directory, so a
// concurrent package run never shares filesystem state across arms (the shared
// /tmp/105 could collide under -shuffle / parallel package runs). t.TempDir()
// creates the dir + registers cleanup; the path is logged so the transcript is
// locatable while the test binary runs.
func write105Artifact(t *testing.T, name, body string) {
	t.Helper()
	path := t.TempDir() + "/" + name
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Logf("write105Artifact %s: %v (non-fatal)", path, err)
		return
	}
	t.Logf("#105 falsifier artifact: %s", path)
}

// ── Arm A — the shape-of-the-real-bug arm (double-RED) ───────────────────
//
// A healthy always-succeeds target (fresh-skipped/re-Marked on passes ≥2)
// ALONGSIDE a deterministic non-apierror/non-ctx failer. With the SET-DELTA
// fix: the boot re-walk is BOUNDED — after bootMaxNoProgressPasses consecutive
// no-set-progress passes it STOPS and converged_with_skips fires with ONLY the
// failer given up. Double-RED:
//
//	(a) current unbounded: TestArmA_CountBasedModel_NeverConverges below models
//	    the pre-fix / count-based impl and shows it does NOT converge.
//	(b) the healthy target is the discriminator (feedback_falsifier_shape_must_
//	    discriminate): its seeded-set membership is stable from pass 2 (no
//	    growth), so ONLY a set-delta reads "no progress"; a count would see its
//	    per-pass activity as progress-forever.
func TestArmA_SetDelta_BoundsRewalk_ConvergesWithOnlyFailerGivenUp(t *testing.T) {
	targets := []bootConvTarget{
		{name: "healthy-w", outcome: outcomeHealthy},
		{name: "det-failer-w", outcome: outcomeDeterministicFail},
	}
	// healthyRePutEveryPass=true models the harsher §5.0 shape (TTL re-Put keeps
	// a COUNT > 0 forever) — the set-delta must STILL converge (same key re-Marked
	// = no growth). maxIters is a generous safety cap for a non-converging RED.
	obs := runBootConvergence(t, targets, 20, true /*healthyRePutEveryPass*/)

	if !obs.converged {
		t.Fatalf("Arm A: SET-DELTA fix must CONVERGE (fire converged_with_skips) — the boot re-walk is bounded; it did not. handlerCalls=%d logs:\n%s", obs.handlerCalls, obs.logText)
	}
	// Bound: passes ≤ bootMaxNoProgressPasses+1 (pass 1 grows the set → progress;
	// then bootMaxNoProgressPasses no-progress passes trip the bound).
	if obs.handlerCalls > bootMaxNoProgressPasses+1 {
		t.Fatalf("Arm A: re-walk not bounded — handlerCalls=%d, want ≤ %d (bootMaxNoProgressPasses+1)", obs.handlerCalls, bootMaxNoProgressPasses+1)
	}
	// ONLY the deterministic failer is given up (the healthy target is NOT).
	if len(obs.givenUp) != 1 || obs.givenUp[0] != "widget/krateo-system/det-failer-w" {
		t.Fatalf("Arm A: given_up_targets must be EXACTLY [widget/krateo-system/det-failer-w]; got %v", obs.givenUp)
	}
	if obs.passesField != float64(bootMaxNoProgressPasses) {
		t.Fatalf("Arm A: converged_with_skips passes field = %v; want %d", obs.passesField, bootMaxNoProgressPasses)
	}
	write105Artifact(t, "armA_setdelta_bounds_rewalk.txt",
		fmt.Sprintf("SET-DELTA: converged=%v handlerCalls=%d (≤%d) givenUp=%v passes=%v\nhealthy target NOT given up; only the deterministic failer.\n",
			obs.converged, obs.handlerCalls, bootMaxNoProgressPasses+1, obs.givenUp, obs.passesField))
}

// TestArmA_CountBasedModel_NeverConverges is the double-RED companion: it models
// a COUNT-based ("seededThisPass>0 → progress") impl over the SAME modeled pass
// sequence and proves it does NOT converge within the bound (the healthy
// target's per-pass Put/re-Put keeps seededThisPass>0 forever → the count-based
// reading resets the no-progress counter every pass → the bound never trips).
// This is why the count-based formula (my earlier WRONG formula, design §5.0)
// is inadmissible and the set-delta is required. Pure model — it does not touch
// prod code; it demonstrates the discriminator the prod set-delta passes and a
// count fails.
func TestArmA_CountBasedModel_NeverConverges(t *testing.T) {
	const passes = 10
	// SET-DELTA (prod shape): seeded SET is {healthy-key} from pass 1 (stable),
	// failed SET is {failer} every pass (stable). grew=false, shrank=false from
	// pass 2 → no progress → trips at bootMaxNoProgressPasses.
	setDeltaTrips := -1
	{
		seeded := map[string]struct{}{}
		priorSeeded := 0
		var priorFailed map[string]struct{} = map[string]struct{}{}
		noProg := 0
		for p := 1; p <= passes && setDeltaTrips < 0; p++ {
			seeded["healthy-key"] = struct{}{} // Put pass1 / re-Put+fresh-skip later — same key
			failed := map[string]struct{}{"failer": {}}
			grew := len(seeded) > priorSeeded
			shrank := len(failed) < len(priorFailed) && subsetOf(failed, priorFailed)
			if grew || shrank {
				noProg = 0
			} else {
				noProg++
			}
			priorSeeded = len(seeded)
			priorFailed = failed
			if noProg >= bootMaxNoProgressPasses {
				setDeltaTrips = p
			}
		}
	}
	// COUNT-based (the WRONG impl): "seededThisPass>0 → progress." The healthy
	// target Puts/re-Puts every pass → seededThisPass==1 every pass → progress
	// every pass → noProg never accumulates → NEVER trips.
	countTrips := -1
	{
		noProg := 0
		for p := 1; p <= passes && countTrips < 0; p++ {
			seededThisPass := 1 // healthy target Put/re-Put every pass
			if seededThisPass > 0 {
				noProg = 0
			} else {
				noProg++
			}
			if noProg >= bootMaxNoProgressPasses {
				countTrips = p
			}
		}
	}
	if setDeltaTrips < 0 {
		t.Fatalf("Arm A double-RED: the SET-DELTA model must converge within %d passes; it did not", passes)
	}
	if countTrips != -1 {
		t.Fatalf("Arm A double-RED: the COUNT-based model must NOT converge (RED) — but it tripped at pass %d. "+
			"A count-based impl was supposed to false-negative the fresh-skip/TTL-re-Put shape.", countTrips)
	}
	write105Artifact(t, "armA_double_red_count_vs_setdelta.txt",
		fmt.Sprintf("SET-DELTA converges at pass %d; COUNT-based never converges (%d = never within %d passes). "+
			"The healthy target's per-pass Put/re-Put keeps a count>0 forever → count resets no-progress → RED.\n",
			setDeltaTrips, countTrips, passes))
}

// ── Arm B — transient still retries (the fix doesn't kill legitimate retry) ─
//
// A healthy always-succeeds target PLUS a transient (503) target that FAILS on
// passes 1..2 then SUCCEEDS from pass 3. Its move failedSet→seededSet is a
// set-delta (failedSet shrinks AND seededSet grows) → the no-progress counter
// RESETS → converged_with_skips does NOT fire; the boot converges by genuinely
// seeding the transient. Discriminator: a degenerate "give up after N
// unconditionally" would kill this arm; only "give up after N *consecutive-no-
// set-progress*" passes it.
func TestArmB_TransientEventuallySeeds_NoConvergedWithSkips(t *testing.T) {
	targets := []bootConvTarget{
		{name: "healthy-w", outcome: outcomeHealthy},
		{name: "transient-w", outcome: outcomeTransientThenSucceed, transientClearsAtPass: 3},
	}
	obs := runBootConvergence(t, targets, 20, false /*fresh-skip healthy after pass1*/)

	if obs.converged {
		t.Fatalf("Arm B: converged_with_skips must NOT fire — the transient eventually seeds (set-delta progress resets the counter). givenUp=%v handlerCalls=%d logs:\n%s", obs.givenUp, obs.handlerCalls, obs.logText)
	}
	// The boot must have run enough passes for the transient to clear (≥3) then
	// complete cleanly (a clean pass with empty failedSet → no re-enqueue → stop).
	if obs.handlerCalls < 3 {
		t.Fatalf("Arm B: expected ≥3 passes (transient clears at pass 3); got %d", obs.handlerCalls)
	}
	write105Artifact(t, "armB_transient_retries.txt",
		fmt.Sprintf("transient cleared; converged_with_skips did NOT fire; handlerCalls=%d (≥3). The set-delta reset the no-progress counter when the transient moved failed→seeded.\n", obs.handlerCalls))
}

// ── Arm C — mixed (all three classes) ────────────────────────────────────
//
// always-succeeds + deterministic-fail + transient-then-succeed in ONE scope.
// Converge with EXACTLY the deterministic one given up (healthy seeded,
// transient eventually seeded, deterministic given up). Proves the bound is
// per-failing-target-set-progress, not whole-scope, AND that a still-making-
// progress scope (transient not yet resolved) is not prematurely given up while
// the deterministic one is pending.
func TestArmC_Mixed_ExactlyDeterministicGivenUp(t *testing.T) {
	targets := []bootConvTarget{
		{name: "healthy-w", outcome: outcomeHealthy},
		{name: "det-failer-w", outcome: outcomeDeterministicFail},
		{name: "transient-w", outcome: outcomeTransientThenSucceed, transientClearsAtPass: 2},
	}
	obs := runBootConvergence(t, targets, 20, false)

	if !obs.converged {
		t.Fatalf("Arm C: must converge_with_skips (the deterministic failer never clears). handlerCalls=%d logs:\n%s", obs.handlerCalls, obs.logText)
	}
	if len(obs.givenUp) != 1 || obs.givenUp[0] != "widget/krateo-system/det-failer-w" {
		t.Fatalf("Arm C: given_up_targets must be EXACTLY [widget/krateo-system/det-failer-w] (transient seeded, healthy seeded); got %v", obs.givenUp)
	}
	write105Artifact(t, "armC_mixed.txt",
		fmt.Sprintf("mixed 3-class converge; givenUp=%v (EXACTLY the deterministic one); handlerCalls=%d. "+
			"The transient's pass-2 clear kept the counter from tripping prematurely.\n", obs.givenUp, obs.handlerCalls))
}

// ── C-105-1 (RED) — slow-convergence safety: a new target reached on a later
// pass RESETS the counter, converged_with_skips does NOT fire. Models the 50K
// shape where a pass reaches a NEW cohort/target only every so often (the #123
// admin-cohort the loop was starving): as long as the seeded SET keeps GROWING,
// the boot keeps re-walking and never gives up. Here: a healthy target plus a
// "late-arriving" healthy target that first seeds on pass 4 (growing the set on
// pass 4 → progress → counter reset). No deterministic failer → the boot
// eventually completes cleanly with converged_with_skips NEVER firing. RED
// against a bound that trips on a raw pass count.
func TestC1051_NewTargetOnLaterPass_ResetsCounter_NoConverged(t *testing.T) {
	// Model "a new target reached on a later pass" via a target that first seeds
	// on pass bootMaxNoProgressPasses+1 — the EXACT pass the raw no-progress
	// counter would otherwise trip. Between pass 1 (healthy grows the set) and
	// that pass, no new target seeds, so the counter climbs toward the bound;
	// then the late arrival GROWS the seeded set on that pass → set-delta reads
	// progress → counter RESETS → NO converge. A raw-pass-count bound would have
	// given the late target up; the set-delta must not (50K slow-convergence
	// safety, the #123 admin-cohort the loop was starving). After the reset there
	// are no more failers → the next pass is a clean completion.
	clearAt := bootMaxNoProgressPasses + 1 // the grace-edge pass
	targets := []bootConvTarget{
		{name: "healthy-w", outcome: outcomeHealthy},
		{name: "late-arrival-w", outcome: outcomeTransientThenSucceed, transientClearsAtPass: clearAt},
	}
	obs := runBootConvergence(t, targets, 30, false)
	if obs.converged {
		t.Fatalf("C-105-1: a NEW target reached on a later pass must RESET the counter → converged_with_skips must NOT fire "+
			"(50K slow-convergence safety); it fired. givenUp=%v handlerCalls=%d logs:\n%s", obs.givenUp, obs.handlerCalls, obs.logText)
	}
	if obs.handlerCalls < clearAt {
		t.Fatalf("C-105-1: expected ≥%d passes (new target arrives at the grace-edge pass); got %d", clearAt, obs.handlerCalls)
	}
	write105Artifact(t, "c1051_new_target_resets.txt",
		fmt.Sprintf("late-arriving target (pass %d, the grace edge) grew the seeded set → reset the no-progress counter; "+
			"converged_with_skips did NOT fire; handlerCalls=%d. Proves the bound is set-PROGRESS, not a raw pass count.\n", clearAt, obs.handlerCalls))
}

// ── C-105-2 (RED) — config-vars redrive after give-up RE-ARMS with fresh empty
// sets. After a boot converges-with-skips (deterministic failer given up), a
// config-vars redrive (new topology) must clear the engine-lived convergence
// state so the next boot re-attempts the failer with an empty seeded/failed set
// and a zeroed no-progress counter. Drives the REAL clearBootConvergenceState
// teardown on the singleton path used by enqueueBootReDrive; here we assert the
// engine-level teardown wired to config-vars redrive drops the state.
func TestC1052_ConfigVarsRedrive_ReArmsFreshState(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	e := newTestEngine()
	key := prewarmScope{kind: scopeKindBoot}.key()

	// Simulate a converged boot: create the state, populate a prior* + a
	// no-progress run (as a resume would).
	st := e.bootConvergenceStateForKey(key)
	st.seeded.Mark("boot105|det-failer-w|user-0")
	st.consecutiveNoProgressPasses = bootMaxNoProgressPasses
	st.priorFailedSet = map[string]struct{}{"widget/krateo-system/det-failer-w": {}}
	st.priorSeededCount = 1

	// A config-vars redrive clears the state (the enqueueBootReDrive teardown
	// point — here exercised directly on this engine to keep the arm local).
	e.clearBootConvergenceState(key)

	// The NEXT access re-arms with a FRESH empty state (new nav topology gets a
	// genuine fresh attempt).
	st2 := e.bootConvergenceStateForKey(key)
	if st2 == st {
		t.Fatalf("C-105-2: config-vars redrive must TEAR DOWN the state (delete, not reuse) so a new instance is created; got the same pointer")
	}
	if st2.consecutiveNoProgressPasses != 0 || st2.priorSeededCount != 0 || len(st2.priorFailedSet) != 0 || st2.seeded.Len() != 0 {
		t.Fatalf("C-105-2: re-armed state must be EMPTY (fresh attempt): noProg=%d priorSeeded=%d priorFailed=%d seeded=%d",
			st2.consecutiveNoProgressPasses, st2.priorSeededCount, len(st2.priorFailedSet), st2.seeded.Len())
	}
	// And the given-up failer is NOT carried forward — a fresh evaluate with the
	// failer failing again on the fresh state does NOT immediately converge
	// (grew on the first fresh pass since the seeded set was empty).
	freshFailed := map[string]struct{}{"widget/krateo-system/det-failer-w": {}}
	st2.seeded.Mark("boot105|healthy-w|user-0") // a healthy target seeds → grows the fresh set
	reEnq, givenUp, _ := st2.evaluateBootProgress(freshFailed)
	if !reEnq || len(givenUp) != 0 {
		t.Fatalf("C-105-2: the FIRST pass after re-arm must re-enqueue (fresh attempt, seeded set grew) and NOT give up; reEnq=%v givenUp=%v", reEnq, givenUp)
	}
	write105Artifact(t, "c1052_configvars_rearm.txt",
		"config-vars redrive tore down the converged state; the re-armed state is empty and re-attempts the previously-given-up target (no carry-forward).\n")
}

// ── C-105-3 / C-105-5 — the terminal (converged) pass returns NIL through the
// clean completion path (NOT an error-return that would divert to the
// scope_incomplete/AddRateLimited requeue door and re-open the loop). We assert
// at the engine level: on the converged pass the scope is FORGOTTEN (not
// AddRateLimited-requeued) and no scope_incomplete/scope_requeued log fires —
// i.e. the terminal path is byte-identical to a clean boot (nil→Forget), which
// in production reaches boot.complete + the already-fired first-nav latch. (The
// latch/readyz-backstop firing itself is on the untouched rePrewarmBootScoped
// path, covered by the FIX-F/F5 latch tests; this arm pins the #105 terminal
// contribution: converged ⇒ nil return ⇒ Forget, never an error-requeue.)
func TestC1053_ConvergedPass_NilReturn_ForgetNotRequeue(t *testing.T) {
	targets := []bootConvTarget{
		{name: "healthy-w", outcome: outcomeHealthy},
		{name: "det-failer-w", outcome: outcomeDeterministicFail},
	}
	obs := runBootConvergence(t, targets, 20, true)
	if !obs.converged {
		t.Fatalf("C-105-3 setup: expected convergence; logs:\n%s", obs.logText)
	}
	// The clean terminal path must NOT emit the error-requeue lines. If the
	// terminal pass had error-returned, processScope would emit
	// prewarm.engine.scope_incomplete + scope_requeued and AddRateLimited it —
	// re-opening the loop by the other door (the C-105-5 hazard).
	if strings.Contains(obs.logText, "prewarm.engine.scope_incomplete") {
		t.Fatalf("C-105-5: the converged terminal pass must NOT error-return (no scope_incomplete). It must return nil → boot.complete→Forget. logs:\n%s", obs.logText)
	}
	if strings.Contains(obs.logText, "prewarm.engine.scope_requeued") {
		t.Fatalf("C-105-5: the converged terminal pass must NOT AddRateLimited-requeue (no scope_requeued). logs:\n%s", obs.logText)
	}
	write105Artifact(t, "c1053_terminal_nil_return.txt",
		"converged terminal pass returned nil → Forget, no scope_incomplete/scope_requeued (C-105-5: never diverts to the error-requeue door).\n")
}
