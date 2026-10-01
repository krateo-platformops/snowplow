// phase1_readiness_exit.go — #397: make the phase-1 readiness EXIT observable at
// LOG_LEVEL=warn.
//
// THE GAP. /readyz flips (cache.MarkPhase1Done) either through the first-nav
// latch (every cohort's nav widgets seeded — prewarm_first_nav_latch.go) or
// through the C2 backstop when the seed ctx (pipGlobalTimeout child) or the
// PHASE1_TIMEOUT_SECONDS parent expires first. The latch line
// (prewarm.first_nav.latch) and the phase-1 progress lines are slog.Info, and
// production runs LOG_LEVEL=warn, so on 057 an operator cannot tell WHICH arm
// released readiness, how long phase 1 took, or how many nav units were still
// cold at the flip.
//
// THE INSTRUMENT (no behaviour change). Three surfaces:
//
//  1. ONE structured line per process, `prewarm.phase1.readiness_exit`, emitted
//     at the readiness flip with outcome / elapsed / nav-unit counts / a bounded
//     failure-class summary. WARN when readiness was NOT released by the latch,
//     INFO otherwise. sync.Once-guarded: a later keepwarm / gvr-discovered pass,
//     a second boot chunk, or a second call site can never emit a second line.
//  2. /readyz reads Phase1WarmingReason / Phase1SinceProcessStart while warming
//     and Phase1ReadinessExitOutcome once Ready (internal/handlers/readyz.go).
//  3. The detector `snowplow_phase1_deadline_released_total` (expvar here + the
//     OTLP Int64ObservableCounter hand-wired in internal/metrics/metrics.go):
//     0 or 1 per process, 1 iff outcome == "deadline". Healthy = 0.
//
// EXIT POINTS (enumerated, #397). Every cache.MarkPhase1Done call in the tree:
//   - phase1_walk.go Step 7.6 seed-block defer (the C2 backstop) — outcomes
//     latch / deadline / boot_error / seed_panic / seed_returned. Recorded by
//     the defer that runs immediately BEFORE the MarkPhase1Done defer.
//   - phase1_walk.go Step 8 PIP-off else (pipSeed == nil) — none-configured.
//   - main.go "prewarm not scheduled" else (unreachable under cache-on, #57) —
//     none-configured via RecordPhase1ReadinessExitNoSeed.
//   - main.go readiness safety net (cache off / no watcher) — none-configured
//     via RecordPhase1ReadinessExitNoSeed.
//
// CARDINALITY / PRIVACY. Every attribute is a code-defined enum or a count.
// Never an identity, a widget name, or anything request-derived
// (feedback_debug_surface_never_dumps_per_identity_cache_bodies).
package dispatchers

import (
	"context"
	"errors"
	"expvar"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Readiness-exit outcomes (the `outcome` attribute + the /readyz `outcome`
// field). A closed, code-defined set.
const (
	// phase1ExitLatch — the first-nav latch had fired when readiness flipped:
	// every cohort's nav-widget units were processed (or provably none existed).
	phase1ExitLatch = "latch"
	// phase1ExitDeadline — the seed ctx (pipGlobalTimeout) or the PHASE1_TIMEOUT
	// parent ended BEFORE the latch fired: readiness was time-released with nav
	// units still unprocessed. The only outcome that bumps the detector.
	phase1ExitDeadline = "deadline"
	// phase1ExitBootError — the seed returned an error (the boot scope finished
	// with an error) before the latch fired and with the ctx still live.
	phase1ExitBootError = "boot_error"
	// phase1ExitSeedPanic — the seed panicked; the recover + C2 backstop flipped.
	phase1ExitSeedPanic = "seed_panic"
	// phase1ExitSeedReturned — the seed fn returned nil without the engine latch
	// having been armed/fired (a non-engine seed fn; not a production shape).
	phase1ExitSeedReturned = "seed_returned"
	// phase1ExitNoneConfigured — there was no seed to wait on (PIP/prewarm off,
	// cache off, or no watcher): readiness flipped with nothing to warm.
	phase1ExitNoneConfigured = "none-configured"
)

// Phase-1 stage reasons surfaced on the /readyz warming body (`reason`). A
// closed, code-defined set; set by phase1WarmupWith as it advances.
const (
	phase1StageNotStarted         = "phase1_not_started"
	phase1StageNoSAEndpoint       = "phase1_aborted_no_sa_endpoint"
	phase1StageRootsWalk          = "roots_walk"
	phase1StageInformerSync       = "informer_sync"
	phase1StageContentPrewarm     = "content_prewarm"
	phase1StageClusterListPrewarm = "cluster_list_prewarm"
	phase1StageBootSeed           = "boot_seed_in_progress"
	phase1StageReadinessReleased  = "readiness_released"
)

// phase1ProcessStart anchors `elapsed_s` / `since_process_start_ms`. Package
// init runs at process start, before main() — the closest in-package proxy for
// pod boot (the same anchor class as snowplow_prewarm_complete.elapsed_ms).
var phase1ProcessStart = time.Now()

var (
	phase1Stage atomic.Value // string, one of the phase1Stage* constants

	phase1ExitOnce    sync.Once
	phase1ExitOutcome atomic.Value // string, one of the phase1Exit* constants

	// phase1DeadlineReleased is the #397 detector: 1 iff this process's
	// readiness was released by the deadline (outcome == "deadline"). Published
	// as snowplow_phase1_deadline_released_total; mirrored to OTLP in metrics.go.
	phase1DeadlineReleased expvar.Int

	phase1ExitMetricsOnce sync.Once
)

func init() { registerPhase1ReadinessExitMetrics() }

// registerPhase1ReadinessExitMetrics publishes the single expvar key. Ungated on
// cache.Disabled(): readiness is meaningful with the cache off (the CFG-1
// nonCacheInitPublishers exception, same class as snowplow_readyz_backstop_fired).
// Idempotent (sync.Once) — expvar.Publish panics on a duplicate key.
func registerPhase1ReadinessExitMetrics() {
	phase1ExitMetricsOnce.Do(func() {
		expvar.Publish("snowplow_phase1_deadline_released_total", &phase1DeadlineReleased)
	})
}

// setPhase1Stage records the current phase-1 stage for the /readyz body. Never
// moves off readiness_released once the exit has been recorded.
func setPhase1Stage(stage string) {
	if s, _ := phase1Stage.Load().(string); s == phase1StageReadinessReleased {
		return
	}
	phase1Stage.Store(stage)
}

// Phase1WarmingReason is the /readyz `reason` while warming: the stage phase 1
// is currently in ("phase1_not_started" before Phase1Warmup reaches its first
// step).
func Phase1WarmingReason() string {
	if s, _ := phase1Stage.Load().(string); s != "" {
		return s
	}
	return phase1StageNotStarted
}

// Phase1SinceProcessStart is the /readyz `elapsed_s` basis.
func Phase1SinceProcessStart() time.Duration { return time.Since(phase1ProcessStart) }

// Phase1ReadinessExitOutcome returns the recorded readiness-exit outcome, or ""
// before the exit was recorded.
func Phase1ReadinessExitOutcome() string {
	s, _ := phase1ExitOutcome.Load().(string)
	return s
}

// Phase1DeadlineReleasedTotal is the OTLP accessor for the #397 detector.
func Phase1DeadlineReleasedTotal() int64 { return phase1DeadlineReleased.Value() }

// ── boot nav-unit progress ─────────────────────────────────────────────────

// bootNavProgress mirrors what the first-nav latch is waiting on, so the
// readiness-exit line can report it even when the latch never fires. Armed by
// seedScopeYielding under EXACTLY the condition under which that pass can fire
// the latch (latch built and not yet fired, non-keepwarm), so it reflects the
// pass readiness is waiting on and is frozen once the latch fires. Written by
// the (serial) seed loop, read by the exit recorder on another goroutine →
// mutex.
type bootNavProgress struct {
	mu                 sync.Mutex
	armed              bool
	passes             int // boot passes that armed (a cut boot chunk re-arms on resume)
	total              int // (widget × cohort) nav units in the arming pass
	cohorts            int // distinct cohorts with >=1 nav unit
	processed          int // units processed (success OR classified failure — the latch's own count)
	expectedDeny       int // processed units whose seed failed with an RBAC deny
	operationalFailure int // processed units whose seed failed operationally
	aborted            int // units left when the pass was cut by its ctx (0 if never cut)
}

var bootNavProgressState bootNavProgress

type navProgressSnapshot struct {
	armed                                                                 bool
	passes, total, cohorts, processed, expectedDeny, operational, aborted int
}

func (p *bootNavProgress) arm(total, cohorts int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = true
	p.passes++
	p.total = total
	p.cohorts = cohorts
	p.processed = 0
	p.expectedDeny = 0
	p.operationalFailure = 0
	p.aborted = 0
}

// unitProcessed counts one processed nav unit; err is the unit's seed error (nil
// on success). Classification reuses classifySeedErr (the same verdict the
// expected_deny / operational_failure log lines carry).
func (p *bootNavProgress) unitProcessed(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processed++
	switch classifySeedErr(err) {
	case seedFailNone:
	case seedFailRBACDeny:
		p.expectedDeny++
	default:
		p.operationalFailure++
	}
}

func (p *bootNavProgress) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed, p.passes, p.total, p.cohorts = false, 0, 0, 0
	p.processed, p.expectedDeny, p.operationalFailure, p.aborted = 0, 0, 0, 0
}

func (p *bootNavProgress) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aborted = p.total - p.processed
}

func (p *bootNavProgress) snapshot() navProgressSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return navProgressSnapshot{
		armed: p.armed, passes: p.passes, total: p.total, cohorts: p.cohorts,
		processed: p.processed, expectedDeny: p.expectedDeny,
		operational: p.operationalFailure, aborted: p.aborted,
	}
}

// ── the exit recorder ──────────────────────────────────────────────────────

// phase1StepTimings carries the per-step wall-clock of phase1WarmupWith so the
// exit line says WHERE the boot time went (walk / sync barrier / content /
// cluster_list / seed). -1 = the step did not run.
type phase1StepTimings struct {
	walk, syncWait, content, clusterList, seed time.Duration
}

func noPhase1StepTimings() phase1StepTimings {
	return phase1StepTimings{walk: -1, syncWait: -1, content: -1, clusterList: -1, seed: -1}
}

// classifyPhase1SeedExit derives the outcome + cause of the Step 7.6 seed
// block's exit from the parent (PHASE1_TIMEOUT) and seed (pipGlobalTimeout) ctx
// errors captured the instant the seed returned (nil/nil on a panic). Order
// matters: a fired latch wins (a tie with the deadline is still a latch
// release, the same predicate the F5 backstop alert uses); then a panic; then
// an ended seed ctx; then a seed error.
func classifyPhase1SeedExit(parentErr, seedCtxErr, seedErr error, panicked bool) (outcome, cause string) {
	if l := currentFirstNavLatch(); l != nil && l.fired() {
		return phase1ExitLatch, ""
	}
	if panicked {
		return phase1ExitSeedPanic, ""
	}
	if seedCtxErr != nil {
		switch {
		case errors.Is(parentErr, context.DeadlineExceeded):
			return phase1ExitDeadline, "phase1_timeout"
		case parentErr != nil:
			return phase1ExitDeadline, "canceled"
		case errors.Is(seedCtxErr, context.DeadlineExceeded):
			return phase1ExitDeadline, "pip_global_timeout"
		default:
			return phase1ExitDeadline, "canceled"
		}
	}
	if seedErr != nil {
		return phase1ExitBootError, ""
	}
	return phase1ExitSeedReturned, ""
}

// recordPhase1ReadinessExit emits the single `prewarm.phase1.readiness_exit`
// line, records the outcome for /readyz and bumps the detector on "deadline".
// Exactly once per process (sync.Once); every later call is a no-op. Never
// panics into the caller: it runs in the seed block's defer chain right before
// MarkPhase1Done, and instrumentation must not be able to change readiness.
func recordPhase1ReadinessExit(outcome, cause string, elapsed time.Duration, steps phase1StepTimings) {
	recordPhase1ReadinessExitWith(func() (string, string) { return outcome, cause }, elapsed, steps)
}

// recordPhase1SeedExit is the Step 7.6 seed-block recorder: classification
// (classifyPhase1SeedExit) runs INSIDE the once + recover guard, so a panic in
// classification, snapshotting or logging is swallowed here and can never
// escape the seed block's defer chain (C3).
func recordPhase1SeedExit(parentErr, seedCtxErr, seedErr error, panicked bool, elapsed time.Duration, steps phase1StepTimings) {
	recordPhase1ReadinessExitWith(func() (string, string) {
		return classifyPhase1SeedExit(parentErr, seedCtxErr, seedErr, panicked)
	}, elapsed, steps)
}

// phase1ExitLevel is the log level of the readiness-exit line for an outcome:
// WARN whenever readiness was NOT released by the latch or a no-seed path.
func phase1ExitLevel(outcome string) slog.Level {
	switch outcome {
	case phase1ExitLatch, phase1ExitNoneConfigured, phase1ExitSeedReturned:
		return slog.LevelInfo
	}
	return slog.LevelWarn
}

func recordPhase1ReadinessExitWith(classify func() (outcome, cause string), elapsed time.Duration, steps phase1StepTimings) {
	phase1ExitOnce.Do(func() {
		defer func() { _ = recover() }()
		outcome, cause := classify()
		phase1ExitOutcome.Store(outcome)
		phase1Stage.Store(phase1StageReadinessReleased)
		if outcome == phase1ExitDeadline {
			phase1DeadlineReleased.Set(1)
		}
		snap := bootNavProgressState.snapshot()
		remaining := snap.total - snap.processed
		latchFired := false
		if l := currentFirstNavLatch(); l != nil {
			latchFired = l.fired()
		}
		level := phase1ExitLevel(outcome)
		effect := "readiness released by the first-nav latch: every cohort's nav-widget units were processed"
		switch outcome {
		case phase1ExitDeadline:
			effect = "readiness released by the DEADLINE (seed ctx / PHASE1_TIMEOUT) before the first-nav latch fired: " +
				"nav_units_remaining nav units were still unprocessed at Ready; snowplow_phase1_deadline_released_total=1"
		case phase1ExitBootError, phase1ExitSeedPanic:
			effect = "readiness released by the C2 backstop after a seed error/panic, before the first-nav latch fired"
		case phase1ExitNoneConfigured:
			effect = "readiness released with no boot seed to wait on (prewarm/PIP off, cache off, or no watcher)"
		case phase1ExitSeedReturned:
			effect = "readiness released after a seed fn returned without the engine latch"
		}
		slog.Default().Log(context.Background(), level, "prewarm.phase1.readiness_exit",
			slog.String("subsystem", "cache"),
			slog.String("outcome", outcome),
			slog.String("deadline_cause", cause),
			slog.Bool("latch_fired", latchFired),
			slog.Int64("elapsed_ms", elapsed.Milliseconds()),
			slog.Int64("since_process_start_ms", Phase1SinceProcessStart().Milliseconds()),
			slog.Bool("nav_latch_armed", snap.armed),
			slog.Int("nav_boot_passes", snap.passes),
			slog.Int("nav_units_total", snap.total),
			slog.Int("nav_units_seeded", snap.processed),
			slog.Int("nav_units_remaining", remaining),
			slog.Int("cohorts", snap.cohorts),
			slog.Int("nav_units_expected_deny", snap.expectedDeny),
			slog.Int("nav_units_operational_failure", snap.operational),
			slog.Int("nav_units_aborted", snap.aborted),
			slog.Int64("walk_ms", steps.walk.Milliseconds()),
			slog.Int64("sync_wait_ms", steps.syncWait.Milliseconds()),
			slog.Int64("content_prewarm_ms", steps.content.Milliseconds()),
			slog.Int64("cluster_list_prewarm_ms", steps.clusterList.Milliseconds()),
			slog.Int64("seed_ms", steps.seed.Milliseconds()),
			slog.String("effect", effect),
		)
	})
}

// RecordPhase1ReadinessExitNoSeed records the "none-configured" exit for the
// main.go flip sites that bypass Phase1Warmup (cache off / no watcher / prewarm
// not scheduled). Call it immediately before cache.MarkPhase1Done there. A no-op
// if the exit was already recorded.
func RecordPhase1ReadinessExitNoSeed() {
	recordPhase1ReadinessExit(phase1ExitNoneConfigured, "", Phase1SinceProcessStart(), noPhase1StepTimings())
}

// resetPhase1ReadinessExitForTest re-arms the once, the stage, the nav progress
// and the detector so a test can observe a fresh exit. TEST-ONLY.
func resetPhase1ReadinessExitForTest() {
	phase1ExitOnce = sync.Once{}
	phase1ExitOutcome.Store("")
	phase1Stage.Store("")
	phase1DeadlineReleased.Set(0)
	bootNavProgressState.reset()
}

// ResetPhase1ReadinessExitForTest is the cross-package (handlers / metrics)
// TEST-ONLY reset. Pairs with cache.ResetPhase1DoneForTest.
func ResetPhase1ReadinessExitForTest() { resetPhase1ReadinessExitForTest() }

// SetPhase1StageForTest drives the /readyz `reason` from another package's
// test. TEST-ONLY.
func SetPhase1StageForTest(stage string) { setPhase1Stage(stage) }

// RecordPhase1DeadlineExitForTest drives the REAL recorder down the deadline
// outcome so a cross-package test (the OTLP callback) reads a genuine 1.
// TEST-ONLY.
func RecordPhase1DeadlineExitForTest() {
	recordPhase1ReadinessExit(phase1ExitDeadline, "phase1_timeout", Phase1SinceProcessStart(), noPhase1StepTimings())
}
