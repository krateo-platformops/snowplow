// phase1_backstop_once_402_test.go — #402: exactly ONE readyz backstop event
// per readiness release, with the most specific cause.
//
// Before #402 a deadline-released boot recorded the backstop twice: once as
// phase1_timeout inside engineSeed's release select (pctx.Done arm) and again as
// seed_incomplete in phase1WarmupWith's Step 7.6 block, because engineSeed
// returns pctx.Err() and the block treats any seed error as "incomplete". A
// boot_error was double-counted the same way (boot_error + seed_incomplete).
//
// The arms drive the REAL engineSeed release select (awaitEngineBootRelease)
// through the REAL phase1WarmupWith Step 7.6 block, and count both
// snowplow_readyz_backstop_fired and the readyz.backstop.fired ERROR lines.
//
// RED before #402: deadline → 2, boot_error → 2.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// backstopLogSink captures readyz.backstop.fired lines (JSON) from the default
// logger for the duration of a test.
type backstopLogSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *backstopLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *backstopLogSink) reasons(t *testing.T) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, ln := range strings.Split(s.buf.String(), "\n") {
		if !strings.Contains(ln, `"readyz.backstop.fired"`) {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("decode backstop line %q: %v", ln, err)
		}
		r, _ := m["reason"].(string)
		out = append(out, r)
	}
	return out
}

func captureBackstopLogs(t *testing.T) *backstopLogSink {
	t.Helper()
	sink := &backstopLogSink{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return sink
}

func TestIssue402_OneBackstopPerRelease(t *testing.T) {
	boom := errors.New("#402: boot scope failed")
	rows := []struct {
		name string
		// closeBoot closes bootDone with bootErr before the seed runs.
		closeBoot bool
		bootErr   error
		fireLatch bool
		ctxBudget time.Duration
		// cancelAfter > 0 cancels the parent ctx mid-boot (a SIGTERM shape).
		cancelAfter time.Duration
		wantDelta   int64
		wantReasons []string
		wantOutcome string
	}{
		{name: "deadline_phase1_timeout", ctxBudget: 300 * time.Millisecond, wantDelta: 1,
			wantReasons: []string{"phase1_timeout"}, wantOutcome: phase1ExitDeadline},
		{name: "deadline_canceled_shutdown_mid_boot", ctxBudget: 20 * time.Second, cancelAfter: 300 * time.Millisecond, wantDelta: 1,
			wantReasons: []string{"canceled"}, wantOutcome: phase1ExitDeadline},
		{name: "boot_error", closeBoot: true, bootErr: boom, ctxBudget: 20 * time.Second, wantDelta: 1,
			wantReasons: []string{"boot_error"}, wantOutcome: phase1ExitBootError},
		{name: "latch_control_no_backstop", fireLatch: true, ctxBudget: 20 * time.Second, wantDelta: 0,
			wantReasons: nil, wantOutcome: phase1ExitLatch},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			engineLatchTestMu.Lock()
			defer engineLatchTestMu.Unlock()
			rw := phase1TestWatcher(t)
			resetExitWorld(t)
			logs := captureBackstopLogs(t)

			latch := ensureFirstNavLatch()
			if r.fireLatch {
				latch.fire("segment-complete", 1, 1, "", -1, 0)
			}
			bootDone := make(chan struct{})
			bootErr := r.bootErr
			if r.closeBoot {
				close(bootDone)
			}
			seed := pipSeedFn(func(pctx context.Context) error {
				return awaitEngineBootRelease(pctx, latch, bootDone, func() error { return bootErr })
			})

			before := readinessBackstopFired.Value()
			ctx, cancel := context.WithTimeout(context.Background(), r.ctxBudget)
			defer cancel()
			if r.cancelAfter > 0 {
				timer := time.AfterFunc(r.cancelAfter, cancel)
				defer timer.Stop()
			}
			_ = phase1WarmupWith(ctx, rw, exitNoRoots, exitNoResolve, nil, nil, seed, nil)
			if !cache.IsPhase1Done() {
				t.Fatal("readiness did not flip (C2 backstop behaviour must be unchanged)")
			}
			if got := readinessBackstopFired.Value() - before; got != r.wantDelta {
				t.Errorf("#402 RED: snowplow_readyz_backstop_fired delta = %d for one release, want %d (reasons logged: %v)",
					got, r.wantDelta, logs.reasons(t))
			}
			got := logs.reasons(t)
			if strings.Join(got, ",") != strings.Join(r.wantReasons, ",") {
				t.Errorf("#402: readyz.backstop.fired reasons = %v, want %v (one line, most specific cause)", got, r.wantReasons)
			}
			if o := Phase1ReadinessExitOutcome(); o != r.wantOutcome {
				t.Errorf("readiness exit outcome = %q, want %q", o, r.wantOutcome)
			}
		})
	}
}

// TestIssue402_SeedExitBackstopReason_PipGlobalTimeout — the pipGlobalTimeout
// arm (the seed's own 8m budget ends while the PHASE1 parent is live) cannot be
// reached through phase1WarmupWith in a unit-test budget, so it drives the REAL
// single record site (recordPhase1SeedExit) with the ctx errors that release
// produces: exactly one backstop, reason pip_global_timeout.
func TestIssue402_SeedExitBackstopReason_PipGlobalTimeout(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	logs := captureBackstopLogs(t)

	before := readinessBackstopFired.Value()
	recordPhase1SeedExit(nil, context.DeadlineExceeded, context.DeadlineExceeded, false, time.Second, noPhase1StepTimings())
	if got := readinessBackstopFired.Value() - before; got != 1 {
		t.Errorf("#402: backstop delta = %d, want 1", got)
	}
	if got := logs.reasons(t); strings.Join(got, ",") != "pip_global_timeout" {
		t.Errorf("#402: reasons = %v, want [pip_global_timeout]", got)
	}
	if o := Phase1ReadinessExitOutcome(); o != phase1ExitDeadline {
		t.Errorf("outcome = %q, want deadline", o)
	}
}
