// refresher_quiescence.go — #930: the refresher's outstanding-work count.
//
// WHAT IT COUNTS. One unit per key a tier will still process: queued, or taken
// by a worker and not yet Done. It is maintained by the workqueue's own metrics
// hooks, which client-go calls UNDER the queue's lock (queue.go Add / Get /
// Done): `adds` ticks once per dirty insert — exactly the inserts that will
// each be processed once more, including a re-add while the key is processing —
// and `work duration` ticks once per Done. So adds − dones is never zero while a
// key is queued or in a worker's hands, with no window between a Get's pop and
// any counter of ours: a Len() probe plus a worker-side busy counter cannot
// give that (the worker's increment lands after the pop releases the lock).
//
// Every tier counts into the same total: a key handed from one tier to another
// (#799 B2 releaseInFlight re-adds before its own Done) is never at zero
// between the two.
//
// WHAT IT DOES NOT COUNT. A deferred re-add waiting in the delaying queue — the
// #321 rate-floor AddAfter and the error-backoff AddRateLimited — is not yet in
// any tier; client-go gives no hook for its expiry. Those are counted where they
// are scheduled (floored, retried). An observer that needs "nothing will run"
// reads both: outstanding == 0 and no deferral since its baseline.
//
// The client-go metrics plumbing also runs its 500 ms unfinished-work updater
// per named queue (queue.go updateUnfinishedWorkLoop); it stops with the queue.
// No series is published from this provider: it exists for the count.
package cache

import (
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// refresherWorkCount is the workqueue MetricsProvider of every refresher tier.
// Only the adds counter and the work-duration histogram carry anything.
// idle receives (without blocking) each time the count reaches zero, so a
// waiter wakes on the event instead of polling.
type refresherWorkCount struct {
	outstanding atomic.Int64
	idle        chan struct{}
}

func newRefresherWorkCount() *refresherWorkCount {
	return &refresherWorkCount{idle: make(chan struct{}, 1)}
}

type workAdd struct{ n *atomic.Int64 }

func (a workAdd) Inc() { a.n.Add(1) }

type workDone struct{ w *refresherWorkCount }

func (d workDone) Observe(float64) {
	if d.w.outstanding.Add(-1) == 0 {
		select {
		case d.w.idle <- struct{}{}:
		default:
		}
	}
}

type workNoop struct{}

func (workNoop) Inc()            {}
func (workNoop) Dec()            {}
func (workNoop) Set(float64)     {}
func (workNoop) Observe(float64) {}

func (w *refresherWorkCount) NewDepthMetric(string) workqueue.GaugeMetric { return workNoop{} }
func (w *refresherWorkCount) NewAddsMetric(string) workqueue.CounterMetric {
	return workAdd{n: &w.outstanding}
}
func (w *refresherWorkCount) NewLatencyMetric(string) workqueue.HistogramMetric { return workNoop{} }
func (w *refresherWorkCount) NewWorkDurationMetric(string) workqueue.HistogramMetric {
	return workDone{w: w}
}
func (w *refresherWorkCount) NewUnfinishedWorkSecondsMetric(string) workqueue.SettableGaugeMetric {
	return workNoop{}
}
func (w *refresherWorkCount) NewLongestRunningProcessorSecondsMetric(string) workqueue.SettableGaugeMetric {
	return workNoop{}
}
func (w *refresherWorkCount) NewRetriesMetric(string) workqueue.CounterMetric { return workNoop{} }

// newRefresherTierQueue builds one tier's rate-limiting queue over base (nil:
// the default queue) counting into w. name only enables client-go's metrics
// hooks; it is never published.
func newRefresherTierQueue(name string, rl workqueue.TypedRateLimiter[string], w *refresherWorkCount, base func(workqueue.TypedInterface[string]) workqueue.TypedInterface[string]) workqueue.TypedRateLimitingInterface[string] {
	q := workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[string]{Name: name, MetricsProvider: w})
	var inner workqueue.TypedInterface[string] = q
	if base != nil {
		inner = base(q)
	}
	return workqueue.NewTypedRateLimitingQueueWithConfig(rl, workqueue.TypedRateLimitingQueueConfig[string]{
		DelayingQueue: workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Queue: inner}),
	})
}

// RefresherWork is one reading of the refresher's work: Outstanding keys queued
// or in a worker's hands across every tier, and the deferred re-adds scheduled
// so far (Floored rate-floor deferrals, Retried error backoffs).
type RefresherWork struct {
	Outstanding      int64
	Floored, Retried uint64
}

// RefresherWorkSnapshot reads the refresher's work. Zero before the pool is
// built (#203: a read never builds it).
func RefresherWorkSnapshot() RefresherWork {
	r := refresherPeek()
	if r == nil {
		return RefresherWork{}
	}
	return RefresherWork{Outstanding: r.work.outstanding.Load(), Floored: r.flooredTotal.Load(), Retried: r.retriedTotal.Load()}
}

// WaitRefresherQuiescentForTest blocks until the refresher has no key queued or
// in a worker's hands, and fails if a deferred re-add (rate floor or error
// backoff) was scheduled since base: that one is still to run, so the refresher
// is not quiescent however empty its tiers read. It wakes on the count reaching
// zero; timeout bounds a stuck worker only and is never the settle. TEST-ONLY.
func WaitRefresherQuiescentForTest(base RefresherWork, timeout time.Duration) (RefresherWork, error) {
	r := refresherPeek()
	if r == nil {
		return RefresherWork{}, nil
	}
	expire := time.NewTimer(timeout)
	defer expire.Stop()
	for {
		w := RefresherWorkSnapshot()
		if w.Floored != base.Floored || w.Retried != base.Retried {
			return w, errRefresherDeferred
		}
		if w.Outstanding == 0 {
			return w, nil
		}
		select {
		case <-r.work.idle:
		case <-expire.C:
			return RefresherWorkSnapshot(), errRefresherBusy
		}
	}
}

type refresherWaitError string

func (e refresherWaitError) Error() string { return string(e) }

const (
	errRefresherDeferred = refresherWaitError("cache: the refresher scheduled a deferred re-add (rate floor or error backoff) since the baseline; it is not quiescent")
	errRefresherBusy     = refresherWaitError("cache: the refresher still had work queued or in flight at the timeout")
)
