package dispatchers

// issue1166_scope_budget_test.go — #1166: a scope's budget cuts its pass, on the real
// deadline (withPrewarmScopeBudget's production branch, context.WithTimeout) and on the
// injected one (h643VirtualBudget). The #643 arms whose cut is a held unit moved to the
// injected budget; these two keep each branch provably cutting a pass mid-way.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// h1166CutPass runs boot, then one gvr-discovered pass whose FIRST unit holds until its
// pass ctx ends (or a 30 s backstop: a budget that never fires), and returns that unit's
// ctx error and cause and whether the backstop fired.
func h1166CutPass(t *testing.T, env *h643Env, hold func(ctx context.Context) error) (err, cause error, backstop bool) {
	t.Helper()
	env.pass(prewarmScope{kind: scopeKindBoot})
	var units atomic.Int64
	var fired atomic.Bool
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered && units.Add(1) == 1 {
			herr := hold(ctx)
			if herr == nil {
				fired.Store(true)
				return errors.New("the budget never ended the pass")
			}
			err, cause = herr, context.Cause(ctx)
			return herr
		}
		return prevW(ctx, e, authnNS, mode)
	}
	env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR})
	seedOneWidgetFn = prevW
	return err, cause, fired.Load()
}

func h1166AssertCut(t *testing.T, env *h643Env, cut0, requeued0 uint64, budget time.Duration) PrewarmPassLast {
	t.Helper()
	if got := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut) - cut0; got != 1 {
		t.Fatalf("pass_total{gvr-discovered,cut} moved by %d, want 1 — the budget did not cut the pass", got)
	}
	l := h643Last(t, string(scopeKindGVRDiscovered))
	if l.Done != 0 || l.Planned <= l.Done {
		t.Fatalf("the cut pass recorded planned=%d done=%d, want done 0 < planned (cut at its first unit)", l.Planned, l.Done)
	}
	if l.BudgetMS != budget.Milliseconds() {
		t.Fatalf("the cut pass recorded budget_ms=%d, want %d", l.BudgetMS, budget.Milliseconds())
	}
	if n := env.engine.requeuedTotal.Load() - requeued0; n != 1 {
		t.Fatalf("the cut pass was requeued %d times, want 1 (a cut is resumed: F.4)", n)
	}
	return l
}

// The production branch: the pass ctx is context.WithTimeout(budget), so the held unit is
// released by the real deadline, with context.DeadlineExceeded, and the pass is a cut.
func TestIssue1166_ARealScopeBudgetCutsThePassMidway(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	const budget = 300 * time.Millisecond
	h643Budget(t, map[prewarmScopeKind]time.Duration{scopeKindGVRDiscovered: budget})
	cut0 := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut)
	requeued0 := env.engine.requeuedTotal.Load()
	err, _, backstop := h1166CutPass(t, env, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	})
	if backstop {
		t.Fatal("the 300 ms scope budget never ended the pass (the 30 s backstop did): processScope does not bound a scope")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the held unit ended with %v, want context.DeadlineExceeded (the real deadline)", err)
	}
	if l := h1166AssertCut(t, env, cut0, requeued0, budget); l.ElapsedMS < budget.Milliseconds() {
		t.Fatalf("the cut pass took %d ms, under its %v budget: it was not the deadline that cut it", l.ElapsedMS, budget)
	}
}

// The injected branch: h643VirtualBudget's deadline is fired by the hold itself, so the
// pass is cut at that instant, with the budget's own value recorded, long before it.
func TestIssue1166_AnInjectedScopeBudgetCutsThePassAtTheHold(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	const budget = time.Hour
	h643Budget(t, map[prewarmScopeKind]time.Duration{scopeKindGVRDiscovered: budget})
	h643VirtualBudget(t)
	cut0 := h643PassCount(string(scopeKindGVRDiscovered), prewarmPassOutcomeCut)
	requeued0 := env.engine.requeuedTotal.Load()
	err, cause, backstop := h1166CutPass(t, env, h643HoldUntilBudget)
	if backstop || err == nil {
		t.Fatal("the held unit was never released by its pass's budget")
	}
	if !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("the held unit's ctx ended with cause %v, want context.DeadlineExceeded (the budget)", cause)
	}
	if l := h1166AssertCut(t, env, cut0, requeued0, budget); l.ElapsedMS >= budget.Milliseconds() {
		t.Fatalf("the cut pass took %d ms: the injected budget did not cut it at the hold", l.ElapsedMS)
	}
}
