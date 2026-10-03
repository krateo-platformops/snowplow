package dispatchers

// learned_identity_262_backstop_test.go — #262 MUST 2 (architect-262).
//
// Learned classes' nav widgets seed BEFORE the first-nav latch (zero cold navs
// at Ready), but a latch released by the readiness BACKSTOP (PHASE1_TIMEOUT /
// pipGlobalTimeout) is a FAILED boot. So only the newest classes whose nav units
// fit the time left before the backstop, at the measured t_widget, seed
// pre-latch; the rest seed right after the latch, in the same order.
//
// TestS262_PreLatchLearnedWorkFitsTheBackstop: five distinct learned classes,
// every resolve costing exactly 40ms on a VIRTUAL seed clock (deterministic
// cost injection — no wall-clock margin), and a backstop deadline that leaves
// room for 2.5 classes' nav units after the base nav units. The latch must fire
// BEFORE the deadline, exactly the 2 newest classes seed pre-latch, the rest
// seed only after the latch AND after the base RA content tail, and every class
// ends up warm.

import (
	"context"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

func TestS262_PreLatchLearnedWorkFitsTheBackstop(t *testing.T) {
	const unit = 40 * time.Millisecond
	users, secrets := l262ManyUsers(t, 5)
	env := l262Setup(t, l262Opts{extraUsers: users, secrets: secrets, widgets: 2, ras: 2})
	clk := l262VirtualClock(t)
	l262Stubs(t, l262StubOpts{sleep: unit, clock: clk})
	l262WaitLearned(t, 5)

	var mu sync.Mutex
	resolves := map[string][]time.Time{} // learned user → widget resolve instants
	var fired time.Time
	inner := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, o widgets.ResolveOptions) (*widgets.Widget, error) {
		if ui, err := xcontext.UserInfo(ctx); err == nil && ui.Username != "" {
			mu.Lock()
			resolves[ui.Username] = append(resolves[ui.Username], clk.now())
			mu.Unlock()
		}
		return inner(ctx, o)
	}
	t.Cleanup(func() { widgetsResolveFn = inner })
	// PRIORITY (TL ruling): the base cohorts' RA content tail serves every user,
	// so it seeds BEFORE the learned overflow's nav widgets.
	var baseRA []time.Time
	innerGet := seedObjectsGetFn
	seedObjectsGetFn = func(ctx context.Context, ref templatesv1.ObjectReference) objects.Result {
		if ui, err := xcontext.UserInfo(ctx); err == nil && ui.Username == "" {
			mu.Lock()
			baseRA = append(baseRA, clk.now())
			mu.Unlock()
		}
		return innerGet(ctx, ref)
	}
	t.Cleanup(func() { seedObjectsGetFn = innerGet })
	prevObs := firstNavFireObserver
	firstNavFireObserver = func(string) {
		mu.Lock()
		fired = clk.now()
		mu.Unlock()
	}
	t.Cleanup(func() { firstNavFireObserver = prevObs })

	// The backstop: base nav (2 units) + 2.5 classes' nav units (2 each) on the
	// virtual clock.
	latch := ensureFirstNavLatch()
	start := clk.now()
	deadline := start.Add(2*unit + 5*unit)
	latch.setBackstopDeadline(deadline)

	l262Boot(t, env, seedModeBoot)

	mu.Lock()
	defer mu.Unlock()
	if fired.IsZero() {
		t.Fatal("NON-VACUITY: the first-nav latch never fired")
	}
	t.Logf("MUST2: latch fired at +%v, backstop at +%v", fired.Sub(start), deadline.Sub(start))
	if !fired.Before(deadline) {
		t.Errorf("MUST2 RED: the latch fired at +%v, AFTER the readiness backstop at +%v — learned-class nav "+
			"work pushed readiness onto the backstop (a failed boot)", fired.Sub(start), deadline.Sub(start))
	}
	// Classify each class by where its nav widgets ran relative to the latch.
	var pre, post []int
	for i, u := range users {
		ts := resolves[u]
		if len(ts) == 0 {
			t.Fatalf("class %s never seeded its nav widgets", u)
		}
		allBefore, allAfter := true, true
		for _, ti := range ts {
			if !ti.Before(fired) {
				allBefore = false
			} else {
				allAfter = false
			}
		}
		switch {
		case allBefore:
			pre = append(pre, i)
		case allAfter:
			post = append(post, i)
		default:
			t.Errorf("class %s straddles the latch (a class is pre- or post-latch whole)", u)
		}
	}
	t.Logf("MUST2: pre-latch classes %v, post-latch classes %v (users index; higher = newer)", pre, post)
	if len(pre) != 2 || len(post) != 3 {
		t.Errorf("MUST2 RED: the backstop fits exactly the 2 newest classes pre-latch, got pre=%v post=%v", pre, post)
	}
	for _, i := range pre {
		for _, j := range post {
			if i < j {
				t.Errorf("pre-latch classes must be the NEWEST: %s (older) pre-latch while %s (newer) post-latch", users[i], users[j])
			}
		}
	}
	if len(baseRA) == 0 {
		t.Fatal("NON-VACUITY: the base cohort's RA tail never seeded")
	}
	lastBaseRA := baseRA[len(baseRA)-1]
	for _, j := range post {
		for _, ti := range resolves[users[j]] {
			if ti.Before(lastBaseRA) {
				t.Errorf("ORDER RED: the overflow class %s seeded a nav widget before the base RA content tail finished", users[j])
				break
			}
		}
	}
	for _, u := range users {
		if w, n := l262Warm(t, env, l262CustomerCtx(u, []string{l262Group})); w != n {
			t.Errorf("every admitted class must end up warm (overflow seeds post-latch): %s %d/%d", u, w, n)
		}
	}
}
