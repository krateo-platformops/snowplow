package dispatchers

// issue643_f9_rbac_shift_cut_test.go — #643 F9, the outcome rule's second
// clause, RUN. rePrewarmRBACShift returns nil when its budget cuts the reseed
// (it re-merges the rotation and re-enqueues itself), so err alone would count
// that cut as complete. Through the real handler → rePrewarmRBACShift →
// reseedTargets, on the shared #643 harness: a budget that expires mid-reseed
// must read pass_total{rbac-shift,cut} += 1 with planned > done; a full run then
// reads complete with done == planned.
//
// The cut is made deterministic through the production seedOneWidgetFn seam:
// the wrapper runs the REAL seedOneWidget for the first two targets, then holds
// the third until the scope's budget expires (a stuck target).
//
// Not t.Parallel(): reassigns package vars.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue643_F9_RBACShiftSelfRequeuedCutIsCounted(t *testing.T) {
	const K, M, N = 3, 2, 4
	env := h643Setup(t, h643Opts{cohorts: K, external: M, internal: N})
	if got := h643Cohorts(); got != K {
		t.Fatalf("fixture precondition: %d cohorts, want %d", got, K)
	}
	budgets := map[prewarmScopeKind]time.Duration{}
	h643Budget(t, budgets)

	// The rbac-shift handler drains (and on a cut re-merges into) the PROCESS
	// engine's accumulator, and re-enqueues on the process queue: leave both clean.
	single := prewarmEngineSingleton()
	drainSingleton := func() {
		single.rbacShift.Drain()
		for single.queue.Len() > 0 {
			it, _ := single.queue.Get()
			single.queue.Forget(it)
			single.queue.Done(it)
		}
	}
	drainSingleton()
	t.Cleanup(drainSingleton)

	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	var rotated cache.RotatedSubjectSet
	cache.RegisterRBACShiftHook(func(r cache.RotatedSubjectSet) { rotated = r })
	subjects := make([]cache.RotatedSubject, 0, K)
	for i := 0; i < K; i++ {
		subjects = append(subjects, cache.RotatedSubject{Kind: "Group", Name: "h643-g" + itoaLine(i)})
	}
	rotate := func() {
		cache.NotifyRBACShiftForTest(subjects)
		if rotated.Len() != K {
			t.Fatalf("precondition: rotated set has %d subjects, want %d", rotated.Len(), K)
		}
		single.rbacShift.Merge(rotated)
	}

	env.pass(prewarmScope{kind: scopeKindBoot})

	// (1) A cut: the third target holds until the 1s budget expires.
	var calls atomic.Int64
	prevW := seedOneWidgetFn
	t.Cleanup(func() { seedOneWidgetFn = prevW })
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, authnNS string, mode seedScopeMode) error {
		if calls.Add(1) == 3 {
			<-ctx.Done()
			return ctx.Err()
		}
		return prevW(ctx, e, authnNS, mode)
	}
	budgets[scopeKindRBACShift] = time.Second
	rotate()
	cut0 := h643PassCount(string(scopeKindRBACShift), prewarmPassOutcomeCut)
	env.pass(prewarmScope{kind: scopeKindRBACShift})
	if got := h643PassCount(string(scopeKindRBACShift), prewarmPassOutcomeCut) - cut0; got != 1 {
		t.Fatalf("pass_total{rbac-shift,cut} moved by %d, want 1 — the handler returned nil with a tail, and the outcome rule must still read it as cut", got)
	}
	l := h643Last(t, string(scopeKindRBACShift))
	if l.Planned != int64(K*(M+N)) || l.Done != 2 {
		t.Fatalf("cut rbac-shift pass: planned=%d done=%d, want planned=%d done=2", l.Planned, l.Done, K*(M+N))
	}
	if single.rbacShift.Len() == 0 {
		t.Fatalf("precondition: the cut rbac-shift run did not re-merge its rotation — the handler did not see a cut")
	}

	// (2) A full run: no stuck target, an hour's budget → complete, done == planned.
	seedOneWidgetFn = prevW
	budgets[scopeKindRBACShift] = time.Hour
	drainSingleton()
	rotate()
	complete0 := h643PassCount(string(scopeKindRBACShift), prewarmPassOutcomeComplete)
	env.pass(prewarmScope{kind: scopeKindRBACShift})
	if got := h643PassCount(string(scopeKindRBACShift), prewarmPassOutcomeComplete) - complete0; got != 1 {
		t.Fatalf("pass_total{rbac-shift,complete} moved by %d, want 1", got)
	}
	if l := h643Last(t, string(scopeKindRBACShift)); l.Planned != int64(K*(M+N)) || l.Done != l.Planned {
		t.Fatalf("full rbac-shift pass: planned=%d done=%d, want both %d", l.Planned, l.Done, K*(M+N))
	}
}
