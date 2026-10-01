// phase1_readiness_exit_test.go — #397 falsifiers: the phase-1 readiness EXIT is
// observable at LOG_LEVEL=warn, and says which arm released readiness.
//
//	X1 (latch): the REAL phase1WarmupWith Step 7.6 block over a seed that runs the
//	   REAL seedScopeYielding boot loop (2 nav widgets × 2 cohorts, one unit
//	   RBAC-denied) → exactly ONE prewarm.phase1.readiness_exit at INFO with
//	   outcome=latch and the correct nav counts; the detector stays 0.
//	X2 (deadline): the same loop on an engine-shaped worker (process ctx, NOT the
//	   seed ctx) wedged after its first nav unit; the PHASE1 ctx expires first →
//	   outcome=deadline at WARN, nav_units_remaining=3, detector=1. The wedged
//	   worker is then released and the latch fires late: still ONE line.
//	X3 (once-only): after the latch exit, a keepwarm cycle, a post-boot re-walk
//	   and a direct second record emit NO second line and do not move the
//	   recorded outcome / counts.
//
// RED without #397: no prewarm.phase1.readiness_exit line exists (X1/X2 find 0
// lines), and the detector never moves (X2). Drives the real flip block and the
// real seed loop through the existing seams — no shadow of either.
//
// Serializes on engineLatchTestMu (shares the latch singleton, the seed seams and
// the default slog logger with the sibling latch tests).

package dispatchers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// exitRecord is one captured slog record (message, level, flattened attrs).
type exitRecord struct {
	msg   string
	level slog.Level
	attrs map[string]slog.Value
}

type exitCapture struct {
	mu   sync.Mutex
	recs []exitRecord
}

type exitCaptureHandler struct{ c *exitCapture }

func (h exitCaptureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h exitCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := exitRecord{msg: r.Message, level: r.Level, attrs: map[string]slog.Value{}}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value
		return true
	})
	h.c.mu.Lock()
	h.c.recs = append(h.c.recs, rec)
	h.c.mu.Unlock()
	return nil
}
func (h exitCaptureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h exitCaptureHandler) WithGroup(string) slog.Handler      { return h }

func (c *exitCapture) exits() []exitRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []exitRecord
	for _, r := range c.recs {
		if r.msg == "prewarm.phase1.readiness_exit" {
			out = append(out, r)
		}
	}
	return out
}

// captureExitLogs installs a capturing default logger for the test. Caller holds
// engineLatchTestMu.
func captureExitLogs(t *testing.T) *exitCapture {
	t.Helper()
	c := &exitCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(exitCaptureHandler{c: c}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return c
}

func exitInt(t *testing.T, r exitRecord, key string) int64 {
	t.Helper()
	v, ok := r.attrs[key]
	if !ok {
		t.Fatalf("readiness_exit line has no %q attribute; attrs=%v", key, r.attrs)
	}
	return v.Int64()
}

func exitStr(t *testing.T, r exitRecord, key string) string {
	t.Helper()
	v, ok := r.attrs[key]
	if !ok {
		t.Fatalf("readiness_exit line has no %q attribute; attrs=%v", key, r.attrs)
	}
	return v.String()
}

// exitFixture builds 2 nav widgets × 2 cohorts (4 nav units) through the real
// harvester and wires the seed seams. seedWidget is the widget seam body; it
// receives the cohort label ("group:devs" / "group:ops").
func exitFixture(t *testing.T, seedWidget func(ctx context.Context, widget, cohort string) error) []navWidgetEntry {
	t.Helper()
	devs := eID{name: "devs", group: true, collapsed: 10}
	ops := eID{name: "ops", group: true, collapsed: 5}
	h := newNavWidgetHarvester()
	gvrA := latchWidgetGVR("exitas")
	gvrB := latchWidgetGVR("exitbs")
	harvestWidgetAtRoot(h, "w-a", gvrA, 0)
	harvestWidgetAtRoot(h, "w-b", gvrB, 0)
	widgets := h.snapshot()

	installLatchSeams(t, &latchRecorder{},
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case gvrA, gvrB:
				return latchTargets(gvr, devs, ops)
			}
			return nil
		},
		func(templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})
	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		return seedWidget(ctx, e.W.GetName(), eIdentityLabel(ctx))
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	return widgets
}

// resetExitWorld resets every process-level piece of state the exit reads.
func resetExitWorld(t *testing.T) {
	t.Helper()
	zeroCustomerInFlight()
	cache.ResetPhase1DoneForTest()
	resetPhase1ReadinessExitForTest()
	resetFirstNavLatchForTest()
	t.Cleanup(func() {
		cache.ResetPhase1DoneForTest()
		resetPhase1ReadinessExitForTest()
		resetFirstNavLatchForTest()
		zeroCustomerInFlight()
	})
}

var exitNoRoots = func(context.Context) ([]navigationRoot, error) { return nil, nil }
var exitNoResolve = func(context.Context, navigationRoot) error { return nil }

func forbidden403() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Code: 403, Reason: metav1.StatusReasonForbidden}}
}

// TestIssue397_X1_LatchExit_OneInfoLine_CountsCorrect_DetectorZero — X1 + X3.
func TestIssue397_X1_LatchExit_OneInfoLine_CountsCorrect_DetectorZero(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	widgets := exitFixture(t, func(_ context.Context, widget, cohort string) error {
		if widget == "w-b" && cohort == "group:ops" {
			return forbidden403() // one EXPECTED deny — still a processed unit
		}
		return nil
	})
	ensureFirstNavLatch() // engineSeed builds the latch before the boot enqueue
	seed := pipSeedFn(func(ctx context.Context) error {
		return seedScopeYielding(ctx, nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil); err != nil {
		t.Fatalf("phase1WarmupWith: %v", err)
	}
	if !cache.IsPhase1Done() {
		t.Fatal("readiness did not flip (behaviour must be unchanged)")
	}

	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("X1 RED: want exactly ONE prewarm.phase1.readiness_exit line, got %d", len(ex))
	}
	r := ex[0]
	if r.level != slog.LevelInfo {
		t.Errorf("X1: a latch exit must log at INFO, got %v", r.level)
	}
	if got := exitStr(t, r, "outcome"); got != "latch" {
		t.Fatalf("X1: outcome = %q, want latch", got)
	}
	for k, want := range map[string]int64{
		"nav_units_total": 4, "nav_units_seeded": 4, "nav_units_remaining": 0, "cohorts": 2,
		"nav_units_expected_deny": 1, "nav_units_operational_failure": 0, "nav_units_aborted": 0,
	} {
		if got := exitInt(t, r, k); got != want {
			t.Errorf("X1: %s = %d, want %d", k, got, want)
		}
	}
	if exitInt(t, r, "elapsed_ms") < 0 || exitInt(t, r, "seed_ms") < 0 || exitInt(t, r, "sync_wait_ms") < 0 {
		t.Errorf("X1: elapsed/step timings must be measured on the seed path; attrs=%v", r.attrs)
	}
	if got := Phase1DeadlineReleasedTotal(); got != 0 {
		t.Errorf("X1: snowplow_phase1_deadline_released_total = %d on a latch exit, want 0", got)
	}
	if got := Phase1ReadinessExitOutcome(); got != "latch" {
		t.Errorf("X1: Phase1ReadinessExitOutcome() = %q, want latch", got)
	}

	// X3 — once-only. A later keepwarm cycle and a post-boot re-walk run the
	// same loop with the latch already fired; a stray second record (any
	// outcome) must be a no-op.
	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("keepwarm cycle: %v", err)
	}
	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("post-boot re-walk: %v", err)
	}
	recordPhase1ReadinessExit(phase1ExitDeadline, "phase1_timeout", time.Second, noPhase1StepTimings())
	if n := len(logs.exits()); n != 1 {
		t.Fatalf("X3 RED: the readiness exit must be emitted ONCE per process; got %d lines after keepwarm + re-walk + a second record", n)
	}
	if got := Phase1ReadinessExitOutcome(); got != "latch" {
		t.Errorf("X3: a second record moved the outcome to %q", got)
	}
	if got := Phase1DeadlineReleasedTotal(); got != 0 {
		t.Errorf("X3: a second record moved the detector to %d", got)
	}
	if s := bootNavProgressState.snapshot(); s.passes != 1 || s.total != 4 || s.processed != 4 {
		t.Errorf("X3: a post-latch pass re-armed the nav tracker: %+v", s)
	}
}

// TestIssue397_X2_DeadlineExit_WarnLine_RemainingPositive_DetectorOne — X2.
func TestIssue397_X2_DeadlineExit_WarnLine_RemainingPositive_DetectorOne(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	release := make(chan struct{})
	secondEntered := make(chan struct{})
	var calls int
	var callsMu sync.Mutex
	widgets := exitFixture(t, func(ctx context.Context, _, _ string) error {
		callsMu.Lock()
		calls++
		n := calls
		callsMu.Unlock()
		if n == 1 {
			return nil // the first nav unit seeds
		}
		if n == 2 {
			close(secondEntered)
		}
		<-release // the rest wedge (a slow/denied fetch burning the budget)
		return nil
	})
	latch := ensureFirstNavLatch()

	// Engine-shaped seed: the boot scope runs on a PROCESS-lifetime worker
	// (engineSeed's processCtx), and the seed fn returns on the FIRST of the
	// latch or the seed ctx — the engineSeed select shape.
	workerDone := make(chan struct{})
	seed := pipSeedFn(func(pctx context.Context) error {
		go func() {
			defer close(workerDone)
			_ = seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
		}()
		<-secondEntered // unit 1 processed and counted; unit 2 wedged
		select {
		case <-latch.wait():
			return nil
		case <-pctx.Done():
			return pctx.Err()
		}
	})

	// The PHASE1 budget (parent of the pipGlobalTimeout seed ctx) expires first.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	_ = phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil)
	if !cache.IsPhase1Done() {
		t.Fatal("readiness did not flip (C2 backstop behaviour must be unchanged)")
	}

	ex := logs.exits()
	if len(ex) != 1 {
		close(release)
		t.Fatalf("X2 RED: want exactly ONE prewarm.phase1.readiness_exit line, got %d", len(ex))
	}
	r := ex[0]
	if r.level != slog.LevelWarn {
		t.Errorf("X2: a deadline exit must log at WARN (visible at LOG_LEVEL=warn), got %v", r.level)
	}
	if got := exitStr(t, r, "outcome"); got != "deadline" {
		t.Errorf("X2: outcome = %q, want deadline", got)
	}
	if got := exitStr(t, r, "deadline_cause"); got != "phase1_timeout" {
		t.Errorf("X2: deadline_cause = %q, want phase1_timeout", got)
	}
	for k, want := range map[string]int64{
		"nav_units_total": 4, "nav_units_seeded": 1, "nav_units_remaining": 3, "cohorts": 2,
	} {
		if got := exitInt(t, r, k); got != want {
			t.Errorf("X2: %s = %d, want %d", k, got, want)
		}
	}
	if got := Phase1DeadlineReleasedTotal(); got != 1 {
		t.Errorf("X2 RED: snowplow_phase1_deadline_released_total = %d after a deadline release, want 1", got)
	}
	if got := Phase1ReadinessExitOutcome(); got != "deadline" {
		t.Errorf("X2: Phase1ReadinessExitOutcome() = %q, want deadline", got)
	}

	// Release the wedged worker: the boot loop finishes and the latch fires
	// LATE. The exit was already recorded — no second line, detector stays 1.
	close(release)
	select {
	case <-workerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not finish after release")
	}
	if !latch.fired() {
		t.Fatal("X2 setup: the late latch should have fired once the worker finished")
	}
	if n := len(logs.exits()); n != 1 {
		t.Fatalf("X2 once-only: a late latch fire emitted a second readiness_exit line (%d lines)", n)
	}
	if got := Phase1DeadlineReleasedTotal(); got != 1 {
		t.Errorf("X2: detector must stay 1 (0 or 1 per process), got %d", got)
	}
}

// TestIssue397_X4_NoSeed_NoneConfigured — the PIP-off flip site (pipSeed == nil)
// records outcome=none-configured at INFO, detector 0.
func TestIssue397_X4_NoSeed_NoneConfigured(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, nil, nil)
	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("want ONE readiness_exit line on the no-seed flip, got %d", len(ex))
	}
	if got := exitStr(t, ex[0], "outcome"); got != "none-configured" || ex[0].level != slog.LevelInfo {
		t.Errorf("no-seed exit: outcome=%q level=%v, want none-configured at INFO", got, ex[0].level)
	}
	if Phase1DeadlineReleasedTotal() != 0 {
		t.Error("no-seed exit must not bump the deadline detector")
	}
}

// TestIssue397_X5_ClassifyEveryBranch_OutcomeCauseLevel — C1. Every outcome the
// seed-block recorder can take, driven through the REAL recorder
// (recordPhase1SeedExit → classifyPhase1SeedExit inside the once/recover guard),
// asserting the emitted line's outcome, deadline_cause AND level, plus the
// detector (1 iff deadline — including a shutdown-mid-boot "canceled").
func TestIssue397_X5_ClassifyEveryBranch_OutcomeCauseLevel(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	boom := errors.New("boot scope failed")
	rows := []struct {
		name                           string
		fireLatch                      bool
		parentErr, seedCtxErr, seedErr error
		panicked                       bool
		noSeed                         bool
		wantOutcome, wantCause         string
		wantLevel                      slog.Level
		wantDetector                   int64
	}{
		{name: "latch", fireLatch: true, wantOutcome: "latch", wantLevel: slog.LevelInfo},
		{name: "latch_wins_tie_with_deadline", fireLatch: true, parentErr: context.DeadlineExceeded, seedCtxErr: context.DeadlineExceeded, seedErr: context.DeadlineExceeded, wantOutcome: "latch", wantLevel: slog.LevelInfo},
		{name: "deadline_phase1_timeout", parentErr: context.DeadlineExceeded, seedCtxErr: context.DeadlineExceeded, seedErr: context.DeadlineExceeded, wantOutcome: "deadline", wantCause: "phase1_timeout", wantLevel: slog.LevelWarn, wantDetector: 1},
		{name: "deadline_pip_global_timeout", seedCtxErr: context.DeadlineExceeded, seedErr: context.DeadlineExceeded, wantOutcome: "deadline", wantCause: "pip_global_timeout", wantLevel: slog.LevelWarn, wantDetector: 1},
		{name: "deadline_canceled_parent_shutdown", parentErr: context.Canceled, seedCtxErr: context.Canceled, seedErr: context.Canceled, wantOutcome: "deadline", wantCause: "canceled", wantLevel: slog.LevelWarn, wantDetector: 1},
		{name: "deadline_canceled_seed_only", seedCtxErr: context.Canceled, wantOutcome: "deadline", wantCause: "canceled", wantLevel: slog.LevelWarn, wantDetector: 1},
		{name: "boot_error", seedErr: boom, wantOutcome: "boot_error", wantLevel: slog.LevelWarn},
		{name: "seed_panic", panicked: true, wantOutcome: "seed_panic", wantLevel: slog.LevelWarn},
		{name: "seed_returned", wantOutcome: "seed_returned", wantLevel: slog.LevelInfo},
		{name: "none_configured", noSeed: true, wantOutcome: "none-configured", wantLevel: slog.LevelInfo},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			resetExitWorld(t)
			logs := captureExitLogs(t)
			if r.fireLatch {
				ensureFirstNavLatch().fire("segment-complete", 1, 1, "", -1, 0)
			}
			if r.noSeed {
				RecordPhase1ReadinessExitNoSeed()
			} else {
				recordPhase1SeedExit(r.parentErr, r.seedCtxErr, r.seedErr, r.panicked, time.Second, noPhase1StepTimings())
			}
			ex := logs.exits()
			if len(ex) != 1 {
				t.Fatalf("want ONE readiness_exit line, got %d", len(ex))
			}
			if got := exitStr(t, ex[0], "outcome"); got != r.wantOutcome {
				t.Errorf("outcome = %q, want %q", got, r.wantOutcome)
			}
			if got := exitStr(t, ex[0], "deadline_cause"); got != r.wantCause {
				t.Errorf("deadline_cause = %q, want %q", got, r.wantCause)
			}
			if ex[0].level != r.wantLevel {
				t.Errorf("level = %v, want %v", ex[0].level, r.wantLevel)
			}
			if got := Phase1DeadlineReleasedTotal(); got != r.wantDetector {
				t.Errorf("snowplow_phase1_deadline_released_total = %d, want %d", got, r.wantDetector)
			}
			if got := Phase1ReadinessExitOutcome(); got != r.wantOutcome {
				t.Errorf("Phase1ReadinessExitOutcome() = %q, want %q", got, r.wantOutcome)
			}
		})
	}
}

// TestIssue397_X6_RealSeedPanic_SeedPanicAtWarn_ReadinessFlips — C1 panic arm.
// A panicking seed drives the REAL phase1WarmupWith defer chain: recover →
// readiness-exit record (outcome=seed_panic at WARN) → MarkPhase1Done.
func TestIssue397_X6_RealSeedPanic_SeedPanicAtWarn_ReadinessFlips(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	panicSeed := pipSeedFn(func(context.Context) error { panic("#397 X6: seed panic") })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, panicSeed, nil); err != nil {
		t.Fatalf("phase1WarmupWith must survive a panicking seed; got %v", err)
	}
	if !cache.IsPhase1Done() {
		t.Fatal("readiness must still flip after a seed panic (C2 backstop unchanged)")
	}
	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("want ONE readiness_exit line, got %d", len(ex))
	}
	if got := exitStr(t, ex[0], "outcome"); got != "seed_panic" || ex[0].level != slog.LevelWarn {
		t.Fatalf("outcome=%q level=%v, want seed_panic at WARN", got, ex[0].level)
	}
	if Phase1DeadlineReleasedTotal() != 0 {
		t.Error("a seed panic is not a deadline release; the detector must stay 0")
	}
}

// TestIssue397_X7_ClassifierPanicIsContained — C3. A panic inside the
// classification step is swallowed by the recorder's own guard and cannot
// escape into the seed block's defer chain.
func TestIssue397_X7_ClassifierPanicIsContained(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	_ = captureExitLogs(t)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("C3 RED: a classifier panic escaped the recorder: %v", r)
		}
	}()
	recordPhase1ReadinessExitWith(func() (string, string) { panic("classifier boom") }, time.Second, noPhase1StepTimings())
	if Phase1DeadlineReleasedTotal() != 0 {
		t.Error("a contained classifier panic must not move the detector")
	}
}

// TestIssue397_X8_TerminalPutRefusedReseed_CountsOnceAsAttempted — composition
// with #394. A nav unit whose terminal Put is refused and then re-seeded once
// (the seedWidgetTarget one-shot) is ONE unit to the tracker, the same as the
// latch's navWidgetRemaining. It is classified by the re-seed's outcome: a
// re-seed that succeeds and one refused twice (reseedAfterTerminalPutRefusal →
// nil, "not a failure") both count as attempted with no failure class. The
// refusal never shows up as an operational failure.
func TestIssue397_X8_TerminalPutRefusedReseed_CountsOnceAsAttempted(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	rw := phase1TestWatcher(t)
	resetExitWorld(t)
	logs := captureExitLogs(t)

	var mu sync.Mutex
	calls := map[string]int{}
	widgets := exitFixture(t, func(_ context.Context, widget, cohort string) error {
		mu.Lock()
		defer mu.Unlock()
		k := widget + "|" + cohort
		calls[k]++
		switch {
		case widget == "w-a" && cohort == "group:devs" && calls[k] == 1:
			return errSeedTerminalPutRefused // refused once, the re-seed succeeds
		case widget == "w-b" && cohort == "group:ops":
			return errSeedTerminalPutRefused // refused twice
		}
		return nil
	})
	ensureFirstNavLatch()
	seed := pipSeedFn(func(ctx context.Context) error {
		return seedScopeYielding(ctx, nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil); err != nil {
		t.Fatalf("phase1WarmupWith: %v", err)
	}

	mu.Lock()
	reA, reB := calls["w-a|group:devs"], calls["w-b|group:ops"]
	mu.Unlock()
	if reA != 2 || reB != 2 {
		t.Fatalf("setup: the #394 one-shot re-seed must run exactly once per refused unit; calls w-a/devs=%d w-b/ops=%d, want 2/2", reA, reB)
	}
	ex := logs.exits()
	if len(ex) != 1 {
		t.Fatalf("want ONE readiness_exit line, got %d", len(ex))
	}
	if got := exitStr(t, ex[0], "outcome"); got != "latch" {
		t.Fatalf("outcome = %q, want latch", got)
	}
	for k, want := range map[string]int64{
		"nav_units_total": 4, "nav_units_seeded": 4, "nav_units_remaining": 0,
		"nav_units_expected_deny": 0, "nav_units_operational_failure": 0,
	} {
		if got := exitInt(t, ex[0], k); got != want {
			t.Errorf("%s = %d, want %d (a refused-then-reseeded unit counts ONCE, as attempted, never as a failure)", k, got, want)
		}
	}
}
