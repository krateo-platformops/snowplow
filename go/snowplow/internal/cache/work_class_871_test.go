// work_class_871_test.go — #871 (measurement only): the cache package's work
// classes. Each arm installs a probe in the unit's body and reads the label the
// Go runtime recorded — on the goroutine (goroutine profile), on a goroutine
// the unit starts, and on CPU samples (CPU profile) — never the ctx alone.
//
//	R   a normal-tier refresher dequeue is snowplow_work=refresh
//	RV  a viewed-tier dequeue is snowplow_work=refresh_viewed
//	CMP a due comparator pass is snowplow_work=comparator, nested in the
//	    refresher's unit, which is restored when the pass returns
//	SR  a stale retry chain attempt is snowplow_work=fill_bg
package cache

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/testsupport/workclassprobe"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

func workClass871Refresh(t *testing.T, viewed bool, want string) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	p := &workclassprobe.Probe{Child: true, Spin: 150 * time.Millisecond}
	RegisterRefreshFunc("widgets", func(ctx context.Context, key string, _ ResolvedKeyInputs) error {
		if key == k {
			p.Observe(ctx)
		}
		return nil
	})
	r := refresherSingleton()
	if viewed {
		viewed765Arm(t, k)
	}
	terminalMark([]string{k})
	if viewed && r.viewedQueue.Len() != 1 {
		t.Fatalf("PRECONDITION: the key is not on the viewed tier (depth %d)", r.viewedQueue.Len())
	}
	if !viewed && r.queue.Len() != 1 {
		t.Fatalf("PRECONDITION: the key is not on the normal tier (depth %d)", r.queue.Len())
	}
	got := workclassprobe.CPUClasses(t, func() {
		for r.viewedQueue.Len() > 0 || r.queue.Len() > 0 {
			r.processNext(context.Background())
		}
	})
	workclassprobe.RequireAll(t, "refresher worker goroutine", p.Self(), want)
	workclassprobe.RequireAll(t, "goroutine started by the re-resolve", p.Children(), want)
	workclassprobe.RequireAll(t, "re-resolve ctx", p.Ctx(), want)
	workclassprobe.RequireOnly(t, "CPU samples of the re-resolve", got, want)
	if after := workclassprobe.GoroutineClass(); after != workclassprobe.None {
		t.Fatalf("the worker goroutine still carries %q after the dequeue", after)
	}
}

func TestWorkClass871_R_RefresherDequeueIsRefresh(t *testing.T) {
	workClass871Refresh(t, false, "refresh")
}

func TestWorkClass871_RV_ViewedDequeueIsRefreshViewed(t *testing.T) {
	workClass871Refresh(t, true, "refresh_viewed")
}

func TestWorkClass871_CMP_DuePassIsComparatorNestedInRefresh(t *testing.T) {
	p := &workclassprobe.Probe{Child: true, Spin: 150 * time.Millisecond}
	h := func(ctx context.Context) { p.Observe(ctx) }
	comparePassHookForTest.Store(&h)
	t.Cleanup(func() { comparePassHookForTest.Store(nil) })

	c := &ListCell{}
	c.passDue.Store(true)
	var after string
	got := workclassprobe.CPUClasses(t, func() {
		workclass.Do(context.Background(), workclass.Refresh, func(ctx context.Context) {
			c.runDuePass(ctx)
			after = workclassprobe.GoroutineClass()
		})
	})
	workclassprobe.RequireAll(t, "comparator pass goroutine", p.Self(), "comparator")
	workclassprobe.RequireAll(t, "goroutine started by the pass", p.Children(), "comparator")
	workclassprobe.RequireAll(t, "comparator pass ctx", p.Ctx(), "comparator")
	workclassprobe.RequireOnly(t, "CPU samples of the pass", got, "comparator")
	if after != "refresh" {
		t.Fatalf("after the pass the refresher unit carries %q, want refresh restored", after)
	}
	// Not due: no pass, no unit.
	n := len(p.Self())
	c.runDuePass(context.Background())
	if len(p.Self()) != n {
		t.Fatal("a pass ran although none was due")
	}
}

func TestWorkClass871_SR_StaleRetryAttemptIsFillBG(t *testing.T) {
	clock := retryEngineSetup(t)
	const key = "k-871-sr"
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()
	p := &workclassprobe.Probe{Child: true}
	spec := StaleRetrySpec{
		Source: StaleRetryBackground,
		Attempt: func(ctx context.Context) StaleRetryOutcome {
			p.Observe(ctx)
			return StaleRetryFresh
		},
		Dependents: func() []string { return []string{key} },
		StillStale: func() (bool, bool) { return true, true },
	}
	// The attempt runs on whichever goroutine fires the wake; label it as a
	// customer's to prove the attempt does not inherit it.
	workclass.Do(context.Background(), workclass.Customer, func(context.Context) {
		StaleRetryFailed(key, StaleRetryGenuine, 0, spec)
		fireOne(t, clock)
	})
	workclassprobe.RequireAll(t, "retry attempt goroutine", p.Self(), "fill_bg")
	workclassprobe.RequireAll(t, "goroutine started by the attempt", p.Children(), "fill_bg")
	workclassprobe.RequireAll(t, "retry attempt ctx", p.Ctx(), "fill_bg")
}
