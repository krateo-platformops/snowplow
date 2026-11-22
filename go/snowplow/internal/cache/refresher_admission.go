// refresher_admission.go — #1571: the refresher's customer yield admits a
// refresh by CORES, event-woken, viewed work first.
//
// THE GAP (bench run 30, N2 = 1.12.74). The Ship #98 yield parked a dequeued
// refresh while ANY customer request was in flight — the dispatchers' bracket
// covers the whole /call ServeHTTP, cache hits included — re-checking every
// 25 ms, for at most 5 s. A viewed cell's 15 ms refresh therefore waited for
// the slowest customer request in progress anywhere in the process. Run 30:
// with the pool parked ≤ 0.5 s per 15 s a viewed refresh waited 0–9 ms; with
// it parked > 5 s per 15 s it waited 1.5 s on average, and the probe samples
// at 5.03–5.14 s were exactly the 5 s cap (8 of 8).
//
// THE RULE (#854's guarantee kept: customers get the cores first):
//
//   - ADMISSION BY CORES. A dequeued refresh may start when the customer
//     requests in flight plus the refreshes already admitted leave a core free:
//     customers + running < GOMAXPROCS (runtime.GOMAXPROCS(0), read at each
//     admission; no constant). Customers that hold every core park all
//     refresher work, as before; customers that leave cores free no longer
//     freeze a refresh.
//   - EVENT-WOKEN. A waiting refresh re-checks only when something it depends
//     on changed: a customer request ended (NoteCustomerDemandChanged, called by
//     the dispatchers' bracket), an admitted refresh released its core, or a
//     waiting viewed refresh was admitted. There is no poll.
//   - VIEWED FIRST. A viewed refresh (a viewer is waiting on its push) is
//     admitted whenever one core is free. A normal or cluster_list refresh is
//     admitted only while a core stays free for every viewed refresh already
//     waiting.
//   - THE 5 s CAP (refresherYieldMaxParked) stays only as the guard against a
//     leaking counter: a wait that long proceeds anyway and counts capped_total.
//
// THE SIGNAL. SetCustomerDemandHook installs the COUNT of customer requests in
// flight (production: dispatchers.CustomerInFlightCount, wired by
// dispatchers.InstallRefresherCustomerSignal from main.go). The older
// SetCustomerInflightHook(bool) remains for tests: true means "customers hold
// every core", which keeps the old park-until-none semantics; a test that
// flips it calls NoteCustomerDemandChanged, as the production bracket does.
// With no signal at all (unit tests, cache-off) a refresh is never held and is
// not counted, exactly as before.
//
// COUNTERS keep their meaning: yielded_total (an admission that had to wait,
// once per wait), parked_ms_total (that wait's wall time), capped_total.
package cache

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

var (
	customerDemandHookMu sync.RWMutex
	customerDemandHook   func() int
)

// SetCustomerDemandHook installs the count of customer requests in flight (the
// dispatchers' bracket counter). It takes precedence over the boolean
// SetCustomerInflightHook. nil clears it.
func SetCustomerDemandHook(fn func() int) {
	customerDemandHookMu.Lock()
	customerDemandHook = fn
	customerDemandHookMu.Unlock()
	NoteCustomerDemandChanged()
}

// customerSignal reports how many cores the customers in flight hold, and
// whether any customer signal is wired at all. The count hook is read as is; the
// boolean hook's true is "every core".
func customerSignal(cores int) (demand int, wired bool) {
	customerDemandHookMu.RLock()
	count := customerDemandHook
	customerDemandHookMu.RUnlock()
	if count != nil {
		return max(count(), 0), true
	}
	customerInflightHookMu.RLock()
	busy := customerInflightHook
	customerInflightHookMu.RUnlock()
	if busy == nil {
		return 0, false
	}
	if busy() {
		return cores, true
	}
	return 0, true
}

// refreshAdmission is the process-wide admission state of refresher work.
type refreshAdmission struct {
	mu            sync.Mutex
	running       int           // admitted refreshes not yet released
	viewedWaiting int           // viewed refreshes waiting for a core
	wake          chan struct{} // closed (and replaced) on every change a waiter depends on
	// waiting mirrors the number of waiters so the customer bracket's
	// notification costs one atomic load when nobody waits.
	waiting atomic.Int64
}

var refreshAdmit = &refreshAdmission{wake: make(chan struct{})}

// broadcastLocked wakes every waiter to re-check. Caller holds a.mu.
func (a *refreshAdmission) broadcastLocked() {
	close(a.wake)
	a.wake = make(chan struct{})
}

// NoteCustomerDemandChanged tells waiting refreshes that the customer demand
// changed (a request ended). The dispatchers' customer bracket calls it on every
// request end; it is one atomic load when no refresh waits.
func NoteCustomerDemandChanged() {
	a := refreshAdmit
	if a.waiting.Load() == 0 {
		return
	}
	a.mu.Lock()
	a.broadcastLocked()
	a.mu.Unlock()
}

// release gives back one admitted refresh's core and wakes the waiters.
func (a *refreshAdmission) release() {
	a.mu.Lock()
	a.running--
	if a.waiting.Load() > 0 {
		a.broadcastLocked()
	}
	a.mu.Unlock()
}

// fits reports whether a refresh of the given side may start now: one core
// free for a viewed refresh; for any other, one core free beyond those the
// waiting viewed refreshes need. Caller holds a.mu.
func (a *refreshAdmission) fits(viewed bool, cores, demand int) bool {
	free := cores - demand - a.running
	need := 1
	if !viewed {
		need += a.viewedWaiting
	}
	return free >= need
}

// admitRefresh holds a dequeued refresh until it may start (the rule in the
// file comment) and returns the release the caller defers. viewed is the
// dequeue's tier.
func (r *refresher) admitRefresh(ctx context.Context, viewed bool) (release func()) {
	a := refreshAdmit
	cores := runtime.GOMAXPROCS(0)
	a.mu.Lock()
	demand, wired := customerSignal(cores)
	if !wired {
		// No customer signal (unit tests, cache-off): never held, not counted.
		a.mu.Unlock()
		return func() {}
	}
	if a.fits(viewed, cores, demand) {
		a.running++
		a.mu.Unlock()
		return a.release
	}
	// Wait. Counted once per wait, timed from here to the admission.
	r.yieldedTotal.Add(1)
	parkStart := time.Now()
	a.waiting.Add(1)
	if viewed {
		a.viewedWaiting++
	}
	a.mu.Unlock()
	// #1054 — parked: the process-wide count every slow-call snapshot reports.
	yieldMark := slowcall.BeginYield(ctx)
	capT := time.NewTimer(refresherYieldMaxParked)
	defer func() {
		capT.Stop()
		yieldMark.End()
		r.parkedNanos.Add(uint64(time.Since(parkStart)))
	}()
	leave := func(admitted bool) func() {
		// Caller holds a.mu.
		a.waiting.Add(-1)
		if viewed {
			a.viewedWaiting--
			// A non-viewed waiter may have been holding back for this one.
			a.broadcastLocked()
		}
		if !admitted {
			a.mu.Unlock()
			return func() {}
		}
		a.running++
		a.mu.Unlock()
		return a.release
	}
	for {
		a.mu.Lock()
		ch := a.wake
		demand, wired = customerSignal(cores)
		if !wired || a.fits(viewed, cores, demand) {
			return leave(true)
		}
		a.mu.Unlock()
		select {
		case <-ch:
		case <-capT.C:
			// The leak guard: a counter that never drops must not stall refresh
			// forever. Proceed regardless, counted.
			r.cappedTotal.Add(1)
			a.mu.Lock()
			return leave(true)
		case <-ctx.Done():
			a.mu.Lock()
			return leave(false)
		}
	}
}

// RefreshAdmissionForTest reports the admission state: refreshes admitted and
// not released, and refreshes waiting (all sides). TEST-ONLY.
func RefreshAdmissionForTest() (running, waiting int) {
	a := refreshAdmit
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.running, int(a.waiting.Load())
}
