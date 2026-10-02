// issue408_boot_remark_amplification_test.go — #408 REQUIRED amplification arm.
//
// #408 adds the #375 remark to the pre-readyz boot plain Put (PutThenRemark). At
// boot every dep bucket is COLD, so a bucket a boot resolve creates inherits the
// GVR's last-bump floor (#375 fix ii). On a busy cluster, an event on ANY object
// of a churning GVR inside a boot resolve's [startSeq, Record] window therefore
// remarks that cell. This arm measures the bound at production boot scale
// (N = 20,000 seed units), through the real boundary: real WithL1KeyContext sinks,
// real Record into cold buckets, real OnUpdate (bumpCoordinateGen + dirty-mark
// fan-out), real PutThenRemark on a real store, and remarks counted at the
// remark funnel (depGenRemarkObserver) and at the refresher hook.
//
// Claims (each sub-arm asserts them):
//   - a remark is per accepted Put, never per event or per dep: each cell is
//     remarked AT MOST once;
//   - only cells that DEPEND on the churning GVR are remarked: the cells that
//     depend on a quiet GVR are remarked 0 times;
//   - a quiet boot remarks 0;
//   - the boot shape costs exactly what the post-readyz PutIfGen seeds already
//     cost under #375 (same counts for the same schedule).
//
// The numbers are logged (t.Logf) for the PR.
package cache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	g408Churn = schema.GroupVersionResource{Group: "churn.example.io", Version: "v1", Resource: "hot"}
	g408Quiet = schema.GroupVersionResource{Group: "quiet.example.io", Version: "v1", Resource: "cold"}
)

const i408BootUnits = 20000

type i408Result struct {
	remarksTotal    int
	remarkedCells   int
	maxPerCell      int
	quietRemarked   int
	hookCalls       int64
	churnEvents     int64
	dependentsChurn int
	elapsed         time.Duration
}

// i408Boot runs N seed units through the real store/deps. Even-indexed units
// depend on g408Churn, odd-indexed ones on g408Quiet; each records its OWN
// object (a cold bucket). churnInWindow fires one event on ANOTHER object of
// g408Churn inside every churn-dependent unit's [startSeq, Record] window (the
// worst case: every dependent saw churn). churnBackground runs a continuous
// event stream on other g408Churn objects from a goroutine while 4 workers seed
// (a realistic busy-cluster boot). guarded selects the post-readyz PutIfGen
// carrier instead of the boot PutThenRemark (the #375 baseline).
func i408Boot(t *testing.T, n int, churnInWindow, churnBackground, guarded bool) i408Result {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	resetRefresherForTest()
	resetDepsForTest()
	c := newResolvedCache(n*2, 1<<30, time.Hour)
	Deps().SetStore(c)
	var mu sync.Mutex
	perKey := map[string]int{}
	restoreObs := SetDepGenRemarkObserverForTest(func(k, _ string) {
		mu.Lock()
		perKey[k]++
		mu.Unlock()
	})
	var hookCalls atomic.Int64
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) { hookCalls.Add(1) })
	t.Cleanup(func() {
		restoreObs()
		resetDepsForTest()
		resetRefresherForTest()
	})

	var events atomic.Int64
	stop := make(chan struct{})
	var bg sync.WaitGroup
	if churnBackground {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				Deps().OnUpdate(g408Churn, "ns", "other-"+strconv.Itoa(i%64))
				events.Add(1)
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}

	seedOne := func(i int) {
		key := "i408-cell-" + strconv.Itoa(i)
		gvr := g408Quiet
		if i%2 == 0 {
			gvr = g408Churn
		}
		ctx := WithL1KeyContext(context.Background(), key) // startSeq at resolve entry
		gen0 := c.CaptureGen(key)
		if churnInWindow && gvr == g408Churn {
			Deps().OnUpdate(g408Churn, "ns", "other-"+strconv.Itoa(i))
			events.Add(1)
		}
		Deps().Record(ctx, key, gvr, "ns", "own-"+strconv.Itoa(i)) // cold bucket → inherits the GVR floor
		e := &ResolvedEntry{RawJSON: []byte(`{}`)}
		if guarded {
			if !c.PutIfGen(ctx, key, e, gen0) {
				t.Errorf("setup: PutIfGen refused for %s", key)
			}
		} else {
			c.PutThenRemark(ctx, key, e)
		}
	}

	start := time.Now()
	if churnBackground {
		var wg sync.WaitGroup
		next := atomic.Int64{}
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= n {
						return
					}
					seedOne(i)
				}
			}()
		}
		wg.Wait()
	} else {
		for i := 0; i < n; i++ {
			seedOne(i)
		}
	}
	elapsed := time.Since(start)
	close(stop)
	bg.Wait()

	mu.Lock()
	defer mu.Unlock()
	r := i408Result{hookCalls: hookCalls.Load(), churnEvents: events.Load(), dependentsChurn: (n + 1) / 2, elapsed: elapsed}
	for k, v := range perKey {
		r.remarksTotal += v
		r.remarkedCells++
		if v > r.maxPerCell {
			r.maxPerCell = v
		}
		var idx int
		if _, err := fmt.Sscanf(k, "i408-cell-%d", &idx); err == nil && idx%2 == 1 {
			r.quietRemarked++
		}
	}
	return r
}

func (r i408Result) String() string {
	return fmt.Sprintf("units=%d churn-dependents=%d churn-events=%d remarks=%d remarked-cells=%d max-per-cell=%d "+
		"quiet-GVR-cells-remarked=%d refresher-hook-calls=%d elapsed=%s",
		i408BootUnits, r.dependentsChurn, r.churnEvents, r.remarksTotal, r.remarkedCells, r.maxPerCell,
		r.quietRemarked, r.hookCalls, r.elapsed.Round(time.Millisecond))
}

func (r i408Result) assertBound(t *testing.T, label string) {
	t.Helper()
	if r.maxPerCell > 1 {
		t.Errorf("#408 amplification RED (%s): a cell was remarked %d times — a remark must be once per accepted "+
			"Put, never per event / per dep. %s", label, r.maxPerCell, r)
	}
	if r.quietRemarked != 0 {
		t.Errorf("#408 amplification RED (%s): %d cells that depend ONLY on the quiet GVR were remarked — the "+
			"churn on another GVR must not reach them (the floor is per-GVR). %s", label, r.quietRemarked, r)
	}
	if r.remarksTotal > r.dependentsChurn {
		t.Errorf("#408 amplification RED (%s): remarks=%d exceed the cells depending on the churning GVR (%d). %s",
			label, r.remarksTotal, r.dependentsChurn, r)
	}
}

func TestIssue408_BootRemarkAmplification_ProductionScale(t *testing.T) {
	t.Run("quiet-boot_zero-remarks", func(t *testing.T) {
		r := i408Boot(t, i408BootUnits, false, false, false)
		t.Logf("#408 quiet boot: %s", r)
		if r.remarksTotal != 0 || r.hookCalls != 0 {
			t.Errorf("#408 amplification RED: a quiet boot must remark nothing; %s", r)
		}
	})
	t.Run("worst-case_churn-in-every-dependent-window", func(t *testing.T) {
		r := i408Boot(t, i408BootUnits, true, false, false)
		t.Logf("#408 worst case (boot PutThenRemark): %s", r)
		r.assertBound(t, "worst case")
		// The production counters (expvar snowplow_deps / OTLP) read the same boot cost.
		if total, boot := MovedRemarkTotals(); int(total) != r.remarksTotal || int(boot) != r.remarksTotal {
			t.Errorf("#408 (worst case): moved_remark_total=%d moved_remark_boot_total=%d, want both %d",
				total, boot, r.remarksTotal)
		}
		if r.remarksTotal != r.dependentsChurn {
			t.Errorf("#408 (worst case): every churn-dependent cell saw an event after its startSeq and must be "+
				"remarked exactly once (non-vacuity of the bound); %s", r)
		}
		base := i408Boot(t, i408BootUnits, true, false, true)
		t.Logf("#375 baseline (post-readyz PutIfGen, same schedule): %s", base)
		if base.remarksTotal != r.remarksTotal {
			t.Errorf("#408: the boot carrier must cost exactly what the #375 PutIfGen carrier costs on the same "+
				"schedule; boot=%d PutIfGen=%d", r.remarksTotal, base.remarksTotal)
		}
	})
	t.Run("continuous-background-churn_4-workers", func(t *testing.T) {
		r := i408Boot(t, i408BootUnits, false, true, false)
		t.Logf("#408 continuous churn (boot PutThenRemark): %s", r)
		r.assertBound(t, "continuous churn")
	})
}

// TestIssue408_MovedRemarkCounters_AttributeTheCarrier pins the production counters the
// boot remark cost is read from (#419). Every moved remark counts once in
// moved_remark_total. The boot carrier's remarks also count in moved_remark_boot_total,
// including a C3 re-check remark (moved_after_put) after a boot Put. A quiet Put counts
// nowhere.
func TestIssue408_MovedRemarkCounters_AttributeTheCarrier(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	resetRefresherForTest()
	resetDepsForTest()
	c := newResolvedCache(64, 1<<22, time.Hour)
	Deps().SetStore(c)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})
	t.Cleanup(func() {
		resetDepsForTest()
		resetRefresherForTest()
	})
	var mu sync.Mutex
	reasons := map[string]int{}
	t.Cleanup(SetDepGenRemarkObserverForTest(func(_, r string) {
		mu.Lock()
		reasons[r]++
		mu.Unlock()
	}))

	// boot, dep moved during the resolve → "moved", boot.
	ctx := WithL1KeyContext(context.Background(), "c408-boot")
	Deps().Record(ctx, "c408-boot", g408Churn, "ns", "b")
	Deps().OnUpdate(g408Churn, "ns", "b")
	c.PutThenRemark(ctx, "c408-boot", &ResolvedEntry{RawJSON: []byte(`{}`)})

	// boot, the dep is Recorded AFTER the Put and moved in [check, Record) → C3
	// "moved_after_put", still attributed to boot.
	ctx2 := WithL1KeyContext(context.Background(), "c408-boot-c3")
	c.PutThenRemark(ctx2, "c408-boot-c3", &ResolvedEntry{RawJSON: []byte(`{}`)})
	Deps().OnUpdate(g408Quiet, "ns", "late")
	Deps().Record(ctx2, "c408-boot-c3", g408Quiet, "ns", "late")

	// guarded, dep moved → "moved", not boot.
	ctx3 := WithL1KeyContext(context.Background(), "c408-guarded")
	gen := c.CaptureGen("c408-guarded")
	Deps().Record(ctx3, "c408-guarded", g408Churn, "ns", "g")
	Deps().OnUpdate(g408Churn, "ns", "g")
	if !c.PutIfGen(ctx3, "c408-guarded", &ResolvedEntry{RawJSON: []byte(`{}`)}, gen) {
		t.Fatal("setup: PutIfGen refused")
	}

	// quiet boot Put → nothing.
	ctx4 := WithL1KeyContext(context.Background(), "c408-quiet")
	Deps().Record(ctx4, "c408-quiet", g408Churn, "ns", "q")
	c.PutThenRemark(ctx4, "c408-quiet", &ResolvedEntry{RawJSON: []byte(`{}`)})

	mu.Lock()
	defer mu.Unlock()
	if reasons["moved"] != 2 || reasons["moved_after_put"] != 1 {
		t.Fatalf("PRECONDITION: want 2 moved + 1 moved_after_put remarks, got %v", reasons)
	}
	total, boot := MovedRemarkTotals()
	if total != 3 || boot != 2 {
		t.Fatalf("#408: moved_remark_total=%d moved_remark_boot_total=%d, want 3 and 2 (the C3 re-check after a "+
			"boot Put is a boot remark; the quiet Put counts nowhere)", total, boot)
	}
	if s := Deps().Stats(); s.MovedRemarkTotal != total || s.MovedRemarkBootTotal != boot {
		t.Fatalf("#408: Stats() drift: %+v", s)
	}
}
