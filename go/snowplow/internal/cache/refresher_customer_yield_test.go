// refresher_customer_yield_test.go — Ship #98 / 0.30.215 unit tests for
// the refresher's cooperative customer-priority yield.
//
// Coverage (each AC drilled directly):
//   - AC-98.3 / mechanism gate — yieldToCustomer parks the worker while
//     the injected customer-inflight predicate returns true; resumes
//     within ~yield-poll once the predicate returns false.
//   - AC-98.7 — race-safety of the customer-inflight hook + the
//     refresher's yield decision under 4 concurrent predicate writers
//     ("customer ServeHTTPs") + 4 concurrent refresher workers reading
//     it. MUST pass `-race`.
//   - AC-98.9 / R-yield-stall-deadlock — refresherYieldMaxParked cap
//     fires when a buggy never-decrementing customer counter holds
//     indefinitely: the refresher proceeds anyway after the cap and
//     ticks cappedTotal.
//   - AC-98.12 — yield-engaged but refresher STILL converges: under
//     intermittent customer pressure (50ms on / 50ms off) the refresher
//     processes its queue within a bounded settle window.
//   - Nil-hook default — SetCustomerInflightHook(nil) → no yield,
//     byte-identical pre-Ship-#98 behaviour.

package cache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withYieldKnobs lowers the yield cap to a test-friendly window. The
// production constants (25ms poll, 5s cap) are too slow for unit tests.
// We do NOT mutate the constants (they are const); instead the tests
// drive the predicate flips on a clock that matches the constants.
func customerHookInstall(t *testing.T, fn func() bool) {
	t.Helper()
	SetCustomerInflightHook(fn)
	t.Cleanup(func() { SetCustomerInflightHook(nil) })
}

// AC-98.3 / mechanism gate — yield engages when predicate=true, releases
// when predicate=false. Counters reflect both transitions.
func TestRefresher_YieldEngagesAndReleases(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	var inflight atomic.Bool
	inflight.Store(true)
	customerHookInstall(t, func() bool { return inflight.Load() })

	c := ResolvedCache()
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-feed"}
	key := ComputeKey(inputs)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"y":1}`), Inputs: &inputs})

	handlerRan := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		select {
		case handlerRan <- struct{}{}:
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(key)

	// Hook says inflight=true — yield must engage. Handler must NOT have
	// run within the first 200ms (gives ~8 yield-poll ticks for the
	// worker to register the yield).
	select {
	case <-handlerRan:
		t.Fatalf("handler ran while customerInFlight()=true; yield did not engage")
	case <-time.After(200 * time.Millisecond):
		// Expected — yielded.
	}
	if got := refresherSingleton().yieldedTotal.Load(); got == 0 {
		t.Fatalf("yieldedTotal=%d; expected ≥1 (yield should have engaged)", got)
	}
	if got := refresherSingleton().cappedTotal.Load(); got != 0 {
		t.Fatalf("cappedTotal=%d; expected 0 (max-parked cap should not have fired)", got)
	}

	// Release. Handler must run within ~one yield-poll + handler call.
	// Generous 2s budget covers slow CI.
	inflight.Store(false)
	select {
	case <-handlerRan:
		// Pass.
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not run within 2s after customerInFlight() went false; yield never released")
	}
}

// Nil-hook default — no yield, byte-identical pre-Ship-#98 behaviour.
func TestRefresher_NilHookNoYield(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	// No SetCustomerInflightHook call → hook is nil.
	c := ResolvedCache()
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-0042"}
	key := ComputeKey(inputs)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})

	handlerRan := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		handlerRan <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(key)

	select {
	case <-handlerRan:
		// Pass — handler ran without yielding (no hook installed).
	case <-time.After(1 * time.Second):
		t.Fatalf("handler did not run within 1s under nil-hook (no-yield) default")
	}

	if got := refresherSingleton().yieldedTotal.Load(); got != 0 {
		t.Fatalf("yieldedTotal=%d under nil-hook; expected 0 (no yield)", got)
	}
}

// AC-98.9 / R-yield-stall-deadlock — max-parked cap fires when a buggy
// never-decrementing predicate holds. Refresher proceeds anyway after
// the cap (with current production cap 5s the test is too slow; we
// validate the SAME mechanism via a stub clock by parking until cap.C
// fires after the configured cap.)
//
// Strategy: we cannot change refresherYieldMaxParked at runtime (const).
// Instead we assert the timing envelope: a permanently-inflight predicate
// must release after ≤ refresherYieldMaxParked + a small slack, AND
// cappedTotal must tick.
func TestRefresher_YieldCapBound(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 5s-cap test under -short")
	}
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()

	// Predicate always says inflight — simulates a buggy never-
	// decrementing counter.
	customerHookInstall(t, func() bool { return true })

	c := ResolvedCache()
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-dead"}
	key := ComputeKey(inputs)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})

	handlerRan := make(chan time.Time, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		select {
		case handlerRan <- time.Now():
		default:
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	t0 := time.Now()
	enqueueRefreshForTest(key)

	// Wait for handler to run — should happen ≈ refresherYieldMaxParked
	// after enqueue (the cap fires and the refresher proceeds).
	select {
	case ran := <-handlerRan:
		elapsed := ran.Sub(t0)
		// Expect ≈ refresherYieldMaxParked (5s). Allow a generous
		// envelope: [3s, 8s] tolerates yield-poll quanta + handler-call
		// latency. Below 3s would indicate the cap fired too early
		// (suggests a bug in yieldToCustomer); above 8s would indicate
		// the cap never fired and processing stalled.
		if elapsed < 3*time.Second || elapsed > 8*time.Second {
			t.Fatalf("handler ran %v after enqueue; expected ≈%v ± yield-poll envelope [3s,8s]", elapsed, refresherYieldMaxParked)
		}
	case <-time.After(refresherYieldMaxParked + 5*time.Second):
		t.Fatalf("handler did not run within %v; max-parked cap did not fire", refresherYieldMaxParked+5*time.Second)
	}

	if got := refresherSingleton().cappedTotal.Load(); got == 0 {
		t.Fatalf("cappedTotal=0; expected ≥1 (max-parked cap should have ticked)")
	}
	if got := refresherSingleton().yieldedTotal.Load(); got == 0 {
		t.Fatalf("yieldedTotal=0; expected ≥1 (yield should have engaged before cap)")
	}
}

// AC-98.12 — convergence under intermittent customer pressure. With the
// predicate flipping at a frequency faster than the production
// 10s-convergence SLA, the refresher MUST still process its queue.
func TestRefresher_ConvergesUnderIntermittentBurst(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	var inflight atomic.Bool
	customerHookInstall(t, func() bool { return inflight.Load() })

	// Predicate-flip loop: 50ms on, 50ms off, for 2s. Worst-case the
	// refresher parks for one 50ms window per yield call.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				inflight.Store(!inflight.Load())
			}
		}
	}()

	c := ResolvedCache()
	const N = 16
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: fmt.Sprintf("uid-0100-%d", i)}
		k := ComputeKey(in)
		c.put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
		keys[i] = k
	}

	var done atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		done.Add(1)
		return nil
	})

	StartRefresher(ctx)

	for _, k := range keys {
		enqueueRefreshForTest(k)
	}

	// Convergence SLA: ALL N keys must process within 10s under
	// intermittent burst (the AC-98.12 budget). Empirically this should
	// complete in <2s, but we allow the 10s production budget.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if done.Load() >= int32(N) {
			return // Pass.
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("convergence under intermittent burst: processed %d/%d in 10s (SLA)", done.Load(), N)
}

// AC-98.7 — race-safety. 4 concurrent "customer ServeHTTP" goroutines
// flip inflight up/down while 4 refresher workers read the hook each
// yield-poll tick + dispatch handlers. Must pass `-race`.
func TestRefresher_RaceYieldUnderConcurrentInflightFlips(t *testing.T) {
	// 4 workers (the production default), 4 concurrent inflight writers,
	// short test horizon. Run under `go test -race`.
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()

	// Use a counter-style predicate (not a bool) to mirror production:
	// production has multiple concurrent /calls each incrementing
	// /decrementing the same atomic.Int64. The hook reads
	// counter > 0.
	var counter atomic.Int64
	customerHookInstall(t, func() bool { return counter.Load() > 0 })

	c := ResolvedCache()
	const N = 64
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: fmt.Sprintf("uid-c000-%d", i)}
		k := ComputeKey(in)
		c.put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
		keys[i] = k
	}

	var handled atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		handled.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// 4 concurrent customer-ServeHTTP simulators each running
	// counter.Add(1); time.Sleep(small); counter.Add(-1) in a loop.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				counter.Add(1)
				time.Sleep(time.Duration(2+i) * time.Millisecond)
				counter.Add(-1)
				time.Sleep(time.Duration(1+i) * time.Millisecond)
			}
		}()
	}

	// Enqueue every key from a 5th goroutine to keep ALL workers busy.
	go func() {
		for _, k := range keys {
			enqueueRefreshForTest(k)
		}
	}()

	// Race window — 1.5s. Convergence assertion is opportunistic; the
	// real check is that go test -race reports no data races.
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Allow the refresher to drain whatever it already has in flight.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && handled.Load() < int32(N) {
		time.Sleep(20 * time.Millisecond)
	}
	if handled.Load() == 0 {
		t.Fatalf("race test: refresher processed 0 keys; expected ≥1 (workers stuck or deadlocked)")
	}
	// Counter must be 0 at quiescence — all customer goroutines exited.
	if got := counter.Load(); got != 0 {
		t.Fatalf("counter=%d after stop; expected 0 (customer goroutines should have decremented to 0)", got)
	}
}

// Hook injection contract — SetCustomerInflightHook(nil) clears prior
// hook; the next predicate-read returns false (no yield).
func TestRefresher_SetHookNilClears(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()

	var seen atomic.Int64
	SetCustomerInflightHook(func() bool {
		seen.Add(1)
		return true
	})

	if !customerInFlightLocked() {
		t.Fatalf("predicate-true hook did not return true")
	}
	if seen.Load() == 0 {
		t.Fatalf("hook function not invoked")
	}

	SetCustomerInflightHook(nil)
	if customerInFlightLocked() {
		t.Fatalf("nil hook returned true; expected false")
	}
}

// Concurrent SetCustomerInflightHook + customerInFlightLocked — `-race`
// guard for the RWMutex serialization.
func TestRefresher_HookSetterReaderRace(t *testing.T) {
	defer SetCustomerInflightHook(nil)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Setter loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			if i%2 == 0 {
				SetCustomerInflightHook(func() bool { return true })
			} else {
				SetCustomerInflightHook(nil)
			}
		}
	}()

	// 4 reader loops (refresher workers).
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = customerInFlightLocked()
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// AC-98.6 RBAC symmetry — the yield mechanism is uniform across handler
// kinds; this is the explicit cross-kind verification. The yield path is
// in processNext (BEFORE the handler dispatch), so handler kind cannot
// influence yield behaviour. Verify by enqueuing both kinds under an
// in-flight predicate and confirming neither runs early.
func TestRefresher_YieldUniformAcrossKinds(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	var inflight atomic.Bool
	inflight.Store(true)
	customerHookInstall(t, func() bool { return inflight.Load() })

	c := ResolvedCache()
	widgetInputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-0001"}
	rsInputs := ResolvedKeyInputs{CacheEntryClass: "restactions", BindingUID: "uid-0002"}
	wk := ComputeKey(widgetInputs)
	rk := ComputeKey(rsInputs)
	c.put(wk, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &widgetInputs})
	c.put(rk, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &rsInputs})

	wRan := make(chan struct{}, 1)
	rRan := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		wRan <- struct{}{}
		return nil
	})
	RegisterRefreshFunc("restactions", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		rRan <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(wk)
	enqueueRefreshForTest(rk)

	// Neither handler should run within 200ms while inflight=true.
	select {
	case <-wRan:
		t.Fatalf("widgets handler ran while inflight=true; yield did not engage for widgets kind")
	case <-rRan:
		t.Fatalf("restactions handler ran while inflight=true; yield did not engage for restactions kind")
	case <-time.After(200 * time.Millisecond):
		// Pass — both kinds yielded uniformly.
	}

	// Release.
	inflight.Store(false)
	// Both should complete within 2s.
	gotW, gotR := false, false
	for end := time.Now().Add(2 * time.Second); time.Now().Before(end) && (!gotW || !gotR); {
		select {
		case <-wRan:
			gotW = true
		case <-rRan:
			gotR = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !gotW || !gotR {
		t.Fatalf("yield uniformity: widgets=%v restactions=%v (both should be true)", gotW, gotR)
	}
}

// Compile-time sentinel: yield constants are sane (poll < cap, both > 0).
func TestRefresher_YieldConstantsSane(t *testing.T) {
	if refresherYieldPoll <= 0 {
		t.Fatalf("refresherYieldPoll=%v; expected >0", refresherYieldPoll)
	}
	if refresherYieldMaxParked <= refresherYieldPoll {
		t.Fatalf("refresherYieldMaxParked=%v ≤ refresherYieldPoll=%v; cap must be longer than poll cadence", refresherYieldMaxParked, refresherYieldPoll)
	}
}

var _ = strconv.Itoa // keep strconv used if anything moves
