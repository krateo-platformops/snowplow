package dispatchers

// learned_identity_262_review_test.go — #262 review arms (architect-262 SHOULD
// 1/2, reviewer-415 C2/C3, #442).
//
//   TestS262_PerKindCostsWeightTheBound (SHOULD 1) — widget and RESTAction
//       costs differ 5×; the bound must charge each kind its own measured mean.
//       A mutant that charges RAs the widget mean admits every class → RED.
//   TestS262_ResidentPassYieldsToCustomers (SHOULD 2) — the resident pass
//       (every RBAC shift, every class-seed scope) must park while a customer
//       /call is in flight. Dropping the per-unit yield → RED.
//   TestS262_EngineSeedArmsTheLatchWithTheBackstop (C3) — the production
//       wiring: engineSeed arms the latch through armFirstNavLatchForSeed with
//       its OWN ctx, whose deadline is min(PHASE1_TIMEOUT parent,
//       pipGlobalTimeout child), and the latch reports the time left to it.
//   TestS262_SlowTailRecheckedBeforeEachPreLatchClass (#442) — the newest
//       pre-latch class runs slower than the mean; the time left is re-checked
//       before the next class, which moves to after the latch, so the latch
//       still fires before the backstop and every class ends up warm.

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

func TestS262_PerKindCostsWeightTheBound(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "1") // interval 750ms
	users, secrets := l262ManyUsers(t, 5)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets, widgets: 2, ras: 2})
	l262Stubs(t, l262StubOpts{widgetSleep: 15 * time.Millisecond, raSleep: 75 * time.Millisecond})
	l262WaitLearned(t, 5)

	l262Boot(t, env, seedModeBoot)

	capm := l262CapacityMap(t)
	us := func(k string) time.Duration { return time.Duration(capm[k].(int64)) * time.Microsecond }
	tW, tR, budget := us("t_widget_us"), us("t_ra_us"), us("budget_us")
	seeded, _ := cache.LearnedAdmissionStats()
	classCost := 2*tW + 2*tR
	wantSeeded := int(budget / classCost)
	if wantSeeded > 5 {
		wantSeeded = 5
	}
	t.Logf("SHOULD1: t_widget=%v t_ra=%v budget=%v class_cost=%v seeded=%d (want %d)", tW, tR, budget, classCost, seeded, wantSeeded)
	// The stubs cost 60ms more per RESTAction than per widget; a bound that
	// charges RAs the widget mean reports t_ra == t_widget.
	if tR-tW < 30*time.Millisecond {
		t.Fatalf("SHOULD1 RED: the bound must use each kind's OWN mean: t_ra=%v is not the measured RESTAction cost (t_widget=%v)", tR, tW)
	}
	if seeded != wantSeeded || seeded >= 5 {
		t.Fatalf("SHOULD1 RED: the per-kind budget admits %d classes, the bound admitted %d", wantSeeded, seeded)
	}
}

func TestS262_ResidentPassYieldsToCustomers(t *testing.T) {
	users, secrets := l262ManyUsers(t, 2)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets})
	l262Stubs(t, l262StubOpts{})
	l262WaitLearned(t, 2)

	release := markCustomerInFlight()
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	done := make(chan []residentUnit, 1)
	go func() { done <- enumerateResident(context.Background(), env.deps, true) }()
	select {
	case units := <-done:
		t.Fatalf("SHOULD2 RED: the resident pass enumerated %d units while a customer /call was in flight — it must yield", len(units))
	case <-time.After(150 * time.Millisecond):
	}
	release()
	released = true
	select {
	case units := <-done:
		if len(units) != len(env.widgets)+len(env.ras) {
			t.Fatalf("after the customer left the pass must complete: %d units", len(units))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the resident pass never resumed after the customer left")
	}
}

func TestS262_EngineSeedArmsTheLatchWithTheBackstop(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	resetFirstNavLatchForTest()
	t.Cleanup(resetFirstNavLatchForTest)

	// (a) the arming function, with the production ctx shape: the PHASE1_TIMEOUT
	// parent (here 2s) and the pipGlobalTimeout child (8m) — the backstop is the
	// EARLIER deadline.
	parent, cancelP := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelP()
	seedCtx, cancelS := context.WithTimeout(parent, pipGlobalTimeout)
	defer cancelS()
	latch := armFirstNavLatchForSeed(seedCtx)
	if latch != currentFirstNavLatch() {
		t.Fatal("armFirstNavLatchForSeed must arm THE process latch engineSeed awaits")
	}
	left, ok := latch.backstopRemaining()
	if !ok || left > 2*time.Second || left < time.Second {
		t.Fatalf("C3 RED: the latch must report the time left to min(PHASE1_TIMEOUT, pipGlobalTimeout) ≈2s, got %v ok=%v", left, ok)
	}

	// (b) engineSeed (a closure inside Phase1Warmup, unreachable from a test)
	// arms the latch through armFirstNavLatchForSeed with ITS OWN ctx parameter,
	// and never builds the latch without the backstop.
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "phase1_walk.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}
		if id, ok := as.Lhs[0].(*ast.Ident); !ok || id.Name != "engineSeed" {
			return true
		}
		lit, ok := as.Rhs[0].(*ast.FuncLit)
		if !ok || len(lit.Type.Params.List) != 1 || len(lit.Type.Params.List[0].Names) != 1 {
			return true
		}
		param := lit.Type.Params.List[0].Names[0].Name
		ast.Inspect(lit.Body, func(m ast.Node) bool {
			call, ok := m.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn, ok := call.Fun.(*ast.Ident)
			if !ok {
				return true
			}
			switch fn.Name {
			case "ensureFirstNavLatch":
				t.Errorf("C3 RED: engineSeed builds the latch with ensureFirstNavLatch — the backstop is never recorded")
			case "armFirstNavLatchForSeed":
				if len(call.Args) == 1 {
					if a, ok := call.Args[0].(*ast.Ident); ok && a.Name == param {
						found = true
					}
				}
			}
			return true
		})
		return false
	})
	if !found {
		t.Fatal("C3 RED: engineSeed must arm the latch via armFirstNavLatchForSeed(<its own ctx>)")
	}
}

func TestS262_SlowTailRecheckedBeforeEachPreLatchClass(t *testing.T) {
	const unit = 40 * time.Millisecond
	users, secrets := l262ManyUsers(t, 5)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets, widgets: 2, ras: 2})
	// The newest class's nav units run slower than the mean the admission used.
	l262Stubs(t, l262StubOpts{widgetSleep: unit, widgetSleepFor: map[string]time.Duration{users[4]: 70 * time.Millisecond}})
	l262WaitLearned(t, 5)

	var mu sync.Mutex
	resolves := map[string][]time.Time{}
	var fired time.Time
	inner := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil && ui.Username != "" {
			mu.Lock()
			resolves[ui.Username] = append(resolves[ui.Username], time.Now())
			mu.Unlock()
		}
		return inner(ctx, o)
	}
	t.Cleanup(func() { widgetsResolveFn = inner })
	prevObs := firstNavFireObserver
	firstNavFireObserver = func(string) { mu.Lock(); fired = time.Now(); mu.Unlock() }
	t.Cleanup(func() { firstNavFireObserver = prevObs })

	// Backstop: base nav (2 units) + room for exactly 2 classes' nav units at
	// the mean (the admission's pre-latch plan) + a small margin.
	latch := ensureFirstNavLatch()
	start := time.Now()
	deadline := start.Add(2*unit + 4*unit + 30*time.Millisecond)
	latch.setBackstopDeadline(deadline)

	l262Boot(t, env, seedModeBoot)

	mu.Lock()
	defer mu.Unlock()
	if fired.IsZero() {
		t.Fatal("NON-VACUITY: the latch never fired")
	}
	t.Logf("#442: latch at +%v, backstop at +%v", fired.Sub(start), deadline.Sub(start))
	if !fired.Before(deadline) {
		t.Errorf("#442 RED: a slow pre-latch class pushed the latch to +%v, past the backstop at +%v", fired.Sub(start), deadline.Sub(start))
	}
	before := func(u string) bool {
		ts := resolves[u]
		return len(ts) > 0 && ts[len(ts)-1].Before(fired)
	}
	if !before(users[4]) {
		t.Errorf("NON-VACUITY: the newest class must still seed pre-latch")
	}
	if before(users[3]) {
		t.Errorf("#442 RED: after the slow class, the next class did not fit the time left yet seeded pre-latch")
	}
	for _, u := range users {
		if w, n := l262Warm(t, env, l262CustomerCtx(u, []string{l262Group})); w != n {
			t.Errorf("every admitted class must end up warm: %s %d/%d", u, w, n)
		}
	}
}
