// refresher_admission_1571_test.go — #1571: the refresher's customer yield
// admits a refresh by cores (customers in flight + refreshes running <
// GOMAXPROCS), event-woken, viewed work first, with the 5 s cap only as the
// leak guard.
//
// The arms drive the real refresher (StartRefresher → nextDequeue →
// processNext → admitRefresh → processOne) with the production signal shape:
// a COUNT of customer requests in flight (SetCustomerDemandHook) and the
// end-of-request wake (NoteCustomerDemandChanged), as the dispatchers' bracket
// installs them. GOMAXPROCS is pinned to 4 per arm so "every core" is a fixed
// count on any laptop.
//
//  1. CUSTOMERS BELOW THE CORES DO NOT PARK A REFRESH. One customer in flight
//     for the whole arm, 4 cores: a refresh runs at once and nothing yields.
//     Falsifier: the old "any customer in flight" predicate (a broken
//     classification) parks it.
//  2. CUSTOMERS HOLDING EVERY CORE PARK IT, AND ONE ENDING ADMITS IT, BY THE
//     EVENT. 4 customers: the refresh does not start; while nothing changes the
//     waiter does not re-read the signal (no poll); one customer ends and the
//     refresh starts within the wake. Falsifiers: the yield bypassed; the wake
//     removed; a poll in place of the wake.
//  3. VIEWED FIRST. One core free and a viewed refresh waiting: a normal
//     refresh dequeued now must wait behind it. Falsifier: the tier
//     classification broken (the viewed refresh counted as normal).
//  4. LEAK GUARD. A count that never drops releases at the 5 s cap, counted.
package cache

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// admission1571Setup pins 4 cores, a clean refresher of parallelism p, and a
// customer count the arm drives; it returns the count and a reader-call counter.
func admission1571Setup(t *testing.T, p int) (customers *atomic.Int64, reads *atomic.Int64) {
	t.Helper()
	prev := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	CompleteRBACSnapshotForTest(t)
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	customers, reads = new(atomic.Int64), new(atomic.Int64)
	SetCustomerDemandHook(func() int { reads.Add(1); return int(customers.Load()) })
	t.Cleanup(func() { SetCustomerDemandHook(nil) })
	return customers, reads
}

// admissionRecorder records when each key's refresh starts; a key in hold
// blocks inside its refresh until released.
type admissionRecorder struct {
	mu     sync.Mutex
	starts map[string]time.Time
	hold   map[string]chan struct{}
}

func newAdmissionRecorder() *admissionRecorder {
	return &admissionRecorder{starts: map[string]time.Time{}, hold: map[string]chan struct{}{}}
}

func (a *admissionRecorder) register(t *testing.T) {
	t.Helper()
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		a.mu.Lock()
		if _, seen := a.starts[k]; !seen {
			a.starts[k] = time.Now()
		}
		h := a.hold[k]
		a.mu.Unlock()
		if h != nil {
			<-h
		}
		return nil
	})
}

func (a *admissionRecorder) started(k string) (time.Time, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.starts[k]
	return s, ok
}

// TestAdmission1571_CustomersBelowCoresDoNotPark — arm 1.
func TestAdmission1571_CustomersBelowCoresDoNotPark(t *testing.T) {
	customers, _ := admission1571Setup(t, 1)
	customers.Store(1) // one customer request in flight for the whole arm
	_, keys := terminalFixtureNoReset(t, 1)
	rec := newAdmissionRecorder()
	rec.register(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	y0 := refresherSingleton().yieldedTotal.Load()
	marked := time.Now()
	terminalMark(keys)
	waitFor(t, 3*time.Second, "the refresh starts", func() bool { _, ok := rec.started(keys[0]); return ok })
	s, _ := rec.started(keys[0])
	if lat := s.Sub(marked); lat > 500*time.Millisecond {
		t.Fatalf("one customer in flight on 4 cores held a refresh %v: want at once (3 cores free)", lat)
	}
	if y := refresherSingleton().yieldedTotal.Load() - y0; y != 0 {
		t.Fatalf("yielded_total moved by %d with 3 of 4 cores free", y)
	}
}

// TestAdmission1571_EveryCoreBusyParksUntilACustomerEnds — arm 2.
func TestAdmission1571_EveryCoreBusyParksUntilACustomerEnds(t *testing.T) {
	customers, reads := admission1571Setup(t, 1)
	customers.Store(4) // customers hold every core
	_, keys := terminalFixtureNoReset(t, 1)
	rec := newAdmissionRecorder()
	rec.register(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	y0 := refresherSingleton().yieldedTotal.Load()
	terminalMark(keys)
	waitFor(t, 3*time.Second, "the refresh waits for a core", func() bool {
		_, w := RefreshAdmissionForTest()
		return w == 1
	})
	// While nothing changes the waiter re-reads nothing: it is woken by events,
	// never by a clock. Measured over 500 ms (a 25 ms poll reads ~20 times).
	r0 := reads.Load()
	time.Sleep(500 * time.Millisecond)
	if n := reads.Load() - r0; n > 1 {
		t.Fatalf("the waiting refresh re-read the customer signal %d times in 500 ms with nothing changing: it polls (want event-woken)", n)
	}
	if _, ok := rec.started(keys[0]); ok {
		t.Fatal("a refresh started while customers held every core")
	}
	// One customer request ends: the bracket's wake admits the refresh.
	ended := time.Now()
	customers.Store(3)
	NoteCustomerDemandChanged()
	waitFor(t, 6*time.Second, "the refresh starts", func() bool { _, ok := rec.started(keys[0]); return ok })
	s, _ := rec.started(keys[0])
	if lat := s.Sub(ended); lat > 250*time.Millisecond {
		t.Fatalf("a customer ended and the waiting refresh started %v later: want within the wake (event), not a timer", lat)
	}
	if y := refresherSingleton().yieldedTotal.Load() - y0; y != 1 {
		t.Fatalf("yielded_total moved by %d, want 1 (one wait)", y)
	}
	if c := refresherSingleton().cappedTotal.Load(); c != 0 {
		t.Fatalf("capped_total=%d: the admission came from the cap, not the customer's end", c)
	}
}

// TestAdmission1571_ViewedRefreshIsAdmittedFirst — arm 3.
func TestAdmission1571_ViewedRefreshIsAdmittedFirst(t *testing.T) {
	customers, _ := admission1571Setup(t, 4)
	customers.Store(4) // every core held: whatever is dequeued waits
	_, keys := terminalFixtureNoReset(t, 2)
	viewed, normal := keys[0], keys[1]
	viewed765Arm(t, viewed)
	rec := newAdmissionRecorder()
	rec.hold[viewed] = make(chan struct{})
	releaseViewed := sync.OnceFunc(func() { close(rec.hold[viewed]) })
	t.Cleanup(releaseViewed)
	rec.register(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	terminalMark([]string{viewed})
	waitFor(t, 3*time.Second, "the viewed refresh waits for a core", func() bool {
		_, w := RefreshAdmissionForTest()
		return w == 1
	})
	// One core frees WITHOUT a wake: the viewed refresh keeps waiting (nothing
	// told it), so the only one that can see the free core is the one dequeued
	// next. A normal refresh dequeued now must leave that core to the waiting
	// viewed one.
	customers.Store(3)
	terminalMark([]string{normal})
	waitFor(t, 3*time.Second, "both refreshes wait", func() bool {
		_, w := RefreshAdmissionForTest()
		return w == 2
	})
	time.Sleep(200 * time.Millisecond)
	if _, ok := rec.started(normal); ok {
		t.Fatal("a normal refresh took the one free core while a viewed refresh was waiting for it")
	}
	// The wake: the viewed refresh is admitted first and holds the core.
	NoteCustomerDemandChanged()
	waitFor(t, 3*time.Second, "the viewed refresh starts", func() bool { _, ok := rec.started(viewed); return ok })
	time.Sleep(200 * time.Millisecond)
	if _, ok := rec.started(normal); ok {
		t.Fatal("the normal refresh started while the viewed one held the only free core")
	}
	releaseViewed()
	waitFor(t, 3*time.Second, "the normal refresh starts once the core is released", func() bool { _, ok := rec.started(normal); return ok })
}

// TestAdmission1571_LeakingCounterReleasesAtTheCap — arm 4.
func TestAdmission1571_LeakingCounterReleasesAtTheCap(t *testing.T) {
	if testing.Short() {
		t.Skip("waits the 5 s cap")
	}
	customers, _ := admission1571Setup(t, 1)
	customers.Store(1 << 20) // a counter that never drops
	_, keys := terminalFixtureNoReset(t, 1)
	rec := newAdmissionRecorder()
	rec.register(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	c0 := refresherSingleton().cappedTotal.Load()
	marked := time.Now()
	terminalMark(keys)
	waitFor(t, refresherYieldMaxParked+3*time.Second, "the refresh proceeds at the cap", func() bool { _, ok := rec.started(keys[0]); return ok })
	s, _ := rec.started(keys[0])
	if lat := s.Sub(marked); lat < refresherYieldMaxParked-100*time.Millisecond {
		t.Fatalf("the refresh started after %v under a counter that never drops: want the %v cap", lat, refresherYieldMaxParked)
	}
	if c := refresherSingleton().cappedTotal.Load() - c0; c != 1 {
		t.Fatalf("capped_total moved by %d, want 1", c)
	}
	if run, w := RefreshAdmissionForTest(); w != 0 || run < 0 {
		t.Fatalf("admission state after the cap: running=%d waiting=%d", run, w)
	}
}
