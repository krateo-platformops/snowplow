package dispatchers

// issue643_prc_f4b_backoff_test.go — #643 PR-C F4-b (architect-ch C4): the
// coalesced gvr-discovered item's backoff resets on PROGRESS. Three attempts
// each cut after re-Putting NEW units must each requeue at NumRequeues == 1 (the
// limiter's base delay); a 4th attempt cut with ZERO new units must grow it to 2.
// Driven through the real processScope and the real rate-limiting queue (no
// harness Forget between attempts). The mutation that drops the
// Forget-on-progress (3rd requeue reads 3) is recorded in the PR.
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"testing"
	"time"
)

func TestIssue643_F4b_BackoffResetsOnProgress(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: h643N})
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)
	env.pass(prewarmScope{kind: scopeKindBoot})
	budgets[scopeKindGVRDiscovered] = 2 * time.Second

	// Units run in a fixed order each attempt; done units are resume-skipped
	// (still a call here). Attempt k holds on call blockAt, i.e. after
	// 2·(k−1) skips and 2 NEW re-Puts — or, for the zero-progress attempt,
	// on the first not-yet-done unit.
	var calls, blockAt int
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if mode == seedModeGVRDiscovered {
			calls++
			if calls == blockAt {
				<-ctx.Done()
				return ctx.Err()
			}
		}
		return prevW(ctx, e, authnNS, mode)
	}

	done := env.engine.passDoneSetFor(prewarmScope{kind: scopeKindGVRDiscovered}.key())
	s := prewarmScope{kind: scopeKindGVRDiscovered}
	attempt := func(block int) (requeues, newDone int) {
		calls, blockAt = 0, block
		before := done.len()
		env.engine.enqueueScope(s)
		got, _ := env.engine.queue.Get()
		env.engine.processScope(context.Background(), got)
		return env.engine.queue.NumRequeues(s), done.len() - before
	}
	for k := 1; k <= 3; k++ {
		n, nd := attempt(2*(k-1) + 3)
		if nd != 2 {
			t.Fatalf("precondition: attempt %d re-Put %d new units, want 2", k, nd)
		}
		if n != 1 {
			t.Errorf("F4-b: attempt %d made progress (2 new units) yet NumRequeues = %d, want 1 (backoff reset on progress → base delay)", k, n)
		}
	}
	n, nd := attempt(2*3 + 1)
	if nd != 0 {
		t.Fatalf("precondition: the zero-progress attempt re-Put %d new units", nd)
	}
	if n != 2 {
		t.Fatalf("F4-b: an attempt with ZERO new units must grow the backoff: NumRequeues = %d, want 2", n)
	}
}
