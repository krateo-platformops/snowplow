// phase1_readiness_exit_736_test.go — #736 falsifiers: EVERY readiness-exit
// outcome is observable on a counter, not only deadline.
//
// Each row drives the REAL exit path for one outcome (recordPhase1SeedExit for
// the seed-block outcomes, releasePhase1BootAborted, RecordPhase1ReadinessExit-
// NoSeed) and asserts:
//   - snowplow_phase1_readiness_exit_total is 1 at that outcome and 0 at the
//     six others (the closed set);
//   - an alert on "any outcome other than latch" fires for every row but latch;
//   - what the PRE-EXISTING instruments did for that outcome, pinned: the F5
//     backstop counter (no outcome label) and the #397 detector (deadline
//     only). seed_returned and none-configured moved NEITHER — the gap.
//
// Mutation → RED: drop notePhase1ExitOutcome (every row's new series stays 0).
package dispatchers

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIssue736_EveryReadinessExitOutcomeIsVisibleOnItsOwnSeries(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	boom := errors.New("boot scope failed")
	rows := []struct {
		name        string
		drive       func()
		want        string
		wantBackstp int64 // F5 backstop counter delta (pre-existing)
		wantDetect  int64 // #397 detector value (pre-existing)
	}{
		{"latch", func() {
			ensureFirstNavLatch().fire("segment-complete", 1, 1, "", -1, 0)
			recordPhase1SeedExit(nil, nil, nil, false, time.Second, noPhase1StepTimings())
		}, phase1ExitLatch, 0, 0},
		{"deadline", func() {
			recordPhase1SeedExit(context.DeadlineExceeded, context.DeadlineExceeded, context.DeadlineExceeded, false, time.Second, noPhase1StepTimings())
		}, phase1ExitDeadline, 1, 1},
		{"boot_error", func() {
			recordPhase1SeedExit(nil, nil, boom, false, time.Second, noPhase1StepTimings())
		}, phase1ExitBootError, 1, 0},
		{"seed_panic", func() {
			recordPhase1SeedExit(nil, nil, nil, true, time.Second, noPhase1StepTimings())
		}, phase1ExitSeedPanic, 1, 0},
		{"seed_returned", func() {
			recordPhase1SeedExit(nil, nil, nil, false, time.Second, noPhase1StepTimings())
		}, phase1ExitSeedReturned, 0, 0},
		{"boot_aborted", func() {
			releasePhase1BootAborted(phase1AbortNoSAEndpoint)
		}, phase1ExitBootAborted, 1, 0},
		{"none_configured", func() {
			RecordPhase1ReadinessExitNoSeed()
		}, phase1ExitNoneConfigured, 0, 0},
	}
	if len(rows) != len(Phase1ReadinessExitOutcomes) {
		t.Fatalf("PRECONDITION: %d rows for %d outcomes — every outcome needs its falsifier", len(rows), len(Phase1ReadinessExitOutcomes))
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			resetExitWorld(t)
			_ = captureExitLogs(t)
			for o, v := range Phase1ReadinessExitByOutcome() {
				if v != 0 {
					t.Fatalf("PRECONDITION: outcome %s = %d before the exit", o, v)
				}
			}
			b0 := readinessBackstopFired.Value()
			r.drive()
			if got := Phase1ReadinessExitOutcome(); got != r.want {
				t.Fatalf("PRECONDITION: the exit recorded outcome %q, want %q", got, r.want)
			}
			by := Phase1ReadinessExitByOutcome()
			for _, o := range Phase1ReadinessExitOutcomes {
				want := int64(0)
				if o == r.want {
					want = 1
				}
				if by[o] != want {
					t.Fatalf("#736 RED: snowplow_phase1_readiness_exit_total{outcome=%q} = %d after a %s release, want %d (%v)",
						o, by[o], r.want, want, by)
				}
			}
			alert := false
			for o, v := range by {
				if o != phase1ExitLatch && v > 0 {
					alert = true
				}
			}
			if alert != (r.want != phase1ExitLatch) {
				t.Fatalf("#736: an alert on outcome!=latch fired=%v for a %s release", alert, r.want)
			}
			if d := readinessBackstopFired.Value() - b0; d != r.wantBackstp {
				t.Fatalf("pinned: the F5 backstop counter moved %d for %s, want %d", d, r.want, r.wantBackstp)
			}
			if got := Phase1DeadlineReleasedTotal(); got != r.wantDetect {
				t.Fatalf("pinned: the #397 detector = %d for %s, want %d", got, r.want, r.wantDetect)
			}
		})
	}
}

// The one release where the two older instruments disagree: a seed that
// panics AFTER the latch fired is outcome latch (readiness was latch-released,
// every nav unit processed) but still bumps the F5 backstop (#402). The new
// series names it latch.
func TestIssue736_LatchThenSeedPanicIsLatchOnTheOutcomeSeries(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	_ = captureExitLogs(t)
	b0 := readinessBackstopFired.Value()
	ensureFirstNavLatch().fire("segment-complete", 1, 1, "", -1, 0)
	recordPhase1SeedExit(nil, nil, nil, true, time.Second, noPhase1StepTimings())
	by := Phase1ReadinessExitByOutcome()
	if by[phase1ExitLatch] != 1 || by[phase1ExitSeedPanic] != 0 {
		t.Fatalf("latch-then-panic: outcome series %v, want latch=1 seed_panic=0", by)
	}
	if d := readinessBackstopFired.Value() - b0; d != 1 {
		t.Fatalf("pinned (#402): latch-then-panic bumps the backstop once, moved %d", d)
	}
}

// The closed set is exactly the phase1Exit* outcome constants: a new outcome
// constant without a series (or a series for a removed outcome) fails here.
func TestIssue736_TheOutcomeSetIsExactlyThePhase1ExitConstants(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "phase1_readiness_exit.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var consts []string
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.CONST {
			continue
		}
		for _, sp := range g.Specs {
			vs := sp.(*ast.ValueSpec)
			for i, n := range vs.Names {
				if !strings.HasPrefix(n.Name, "phase1Exit") || i >= len(vs.Values) {
					continue
				}
				if bl, ok := vs.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
					v, _ := strconv.Unquote(bl.Value)
					consts = append(consts, v)
				}
			}
		}
	}
	got := append([]string(nil), Phase1ReadinessExitOutcomes...)
	sort.Strings(consts)
	sort.Strings(got)
	if strings.Join(consts, ",") != strings.Join(got, ",") {
		t.Fatalf("#736: Phase1ReadinessExitOutcomes = %v, the phase1Exit* constants are %v — every outcome needs its series", got, consts)
	}
}

// 0/1 PER PROCESS (architect, #748 gate): the record is set inside the exit's
// sync.Once, so later exit attempts — a latch firing and a seed exit, then a
// no-seed flip — change nothing: exactly ONE outcome reads 1 (the first,
// deadline), and the series sum to 1. Adapted from architect-ch's scratch
// TestGate736_SecondExitAttemptChangesNothing.
// Mutation → RED: classify + note BEFORE phase1ExitOnce.Do (the record moved
// outside the once) — the series then read deadline=1 latch=1
// none-configured=1.
func TestIssue736_LaterExitAttemptsChangeNothingOneOutcomePerProcess(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetExitWorld(t)
	_ = captureExitLogs(t)

	recordPhase1SeedExit(context.DeadlineExceeded, context.DeadlineExceeded, context.DeadlineExceeded, false, time.Second, noPhase1StepTimings())
	ensureFirstNavLatch().fire("segment-complete", 1, 1, "", -1, 0)
	recordPhase1SeedExit(nil, nil, nil, false, time.Second, noPhase1StepTimings())
	RecordPhase1ReadinessExitNoSeed()

	by := Phase1ReadinessExitByOutcome()
	var sum int64
	for _, v := range by {
		sum += v
	}
	if sum != 1 || by[phase1ExitDeadline] != 1 {
		t.Fatalf("#736 RED: after three exit attempts the series read %v (sum %d) — want exactly deadline=1: "+
			"the record is 0/1 per process, set once at the FIRST exit", by, sum)
	}
	if got := Phase1ReadinessExitOutcome(); got != phase1ExitDeadline {
		t.Fatalf("the recorded outcome moved to %q after the first exit", got)
	}
}

// The record and the closed set have the same length (one source of truth).
func TestIssue736_TheRecordIsSizedByTheOutcomeSet(t *testing.T) {
	if len(phase1ExitByOutcome) != len(Phase1ReadinessExitOutcomes) {
		t.Fatalf("phase1ExitByOutcome has %d slots for %d outcomes", len(phase1ExitByOutcome), len(Phase1ReadinessExitOutcomes))
	}
}
