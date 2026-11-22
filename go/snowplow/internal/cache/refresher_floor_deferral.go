// refresher_floor_deferral.go — #1570: a floored key's wait follows the LIVE
// backlog of its side, not the backlog it saw when it was floored.
//
// THE LATCH THIS CLOSES. The floor is RefreshCost × max(1, K/S) (rateFloor). Read
// once at the floored dequeue and handed to the delaying queue as a fixed
// AddAfter, it froze K at that moment: a cell floored at a burst's peak kept the
// peak's wait after the burst drained. At the refresher's recorded burst scale
// (~1,669 keys, S = 3) a 275 ms cell dequeued at the peak would have held its
// last change for ~153 s, against main's fixed 2 s.
//
// THE RULE. The side owns its floored keys in one set. Each key's due time is
// recomputed from the side's CURRENT backlog:
//
//	due = CreatedAt + RefreshCost × max(1, K_now/S)
//
// and the set is re-evaluated whenever K can have fallen below the K it was last
// evaluated with — the end of a key's work on that side (endViewed /
// endUnviewed) — and at its soonest due time by ONE timer per side. A key whose
// due time has passed goes back to its tier queue (q.Add); its re-dequeue reads
// the live entry and the live K again, exactly as any dequeue does. No cap and
// no TTL: the wait is the cell's own cost times the backlog it actually waits
// behind, at every moment.
//
// WHY A SCAN ONLY WHEN K FELL. Every due time grows with K. The timer is armed
// at the soonest due time computed with kScan; while K ≥ kScan no key can be
// due before it, so only a K below kScan (or the timer itself, or a new key)
// needs a scan. A K that rose makes the timer fire early: the scan finds nothing
// due and re-arms it later. Work is O(floored keys) per scan, a multiply and a
// compare each.
//
// LOSSLESS, as the AddAfter was: a key leaves the set only through q.Add (the
// idempotent base Add, which a re-mark meanwhile may already have done), and the
// timer is armed whenever the set is not empty. The trigger GVRs and the dirty
// window stay with the key through the deferral (processNext consumes them only
// past the floor).
package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// floorDeferred is one floored key: the queue it returns to and the entry
// fields its floor is computed from, as the floored dequeue read them. Its
// re-dequeue reads the live entry; these only time the release.
type floorDeferred struct {
	q         workqueue.TypedRateLimitingInterface[string]
	createdAt time.Time     // the entry's CreatedAt, on the store clock
	cost      time.Duration // the entry's RefreshCost
}

// floorDeferral is one side's floored keys (viewed, or unviewed: normal plus
// cluster_list, the sides backlogShare reads).
type floorDeferral struct {
	r    *refresher
	tier refreshTier // the side backlogShare reads: tierViewed or tierNormal

	mu      sync.Mutex
	keys    map[string]floorDeferred
	kScan   int // the K the armed timer's due time was computed with
	timer   *time.Timer
	stopped bool

	n atomic.Int64 // len(keys): the completion hook's lock-free "anything floored?"
}

func newFloorDeferral(r *refresher, tier refreshTier) *floorDeferral {
	return &floorDeferral{r: r, tier: tier, keys: map[string]floorDeferred{}}
}

// floorFor is the floor of a cell whose last refresh cost cost, on a side
// serving k keys with s workers (rateFloor's rule; the test-only override wins).
func floorFor(cost time.Duration, k, s int) time.Duration {
	if d := refresherFloorOverride.Load(); d != nil {
		return *d
	}
	if cost <= 0 {
		return 0
	}
	if k <= s {
		return cost
	}
	return time.Duration(int64(cost) * int64(k) / int64(s))
}

// add takes a floored key into the set (a key already in it is re-timed from
// the entry this dequeue read) and re-evaluates.
func (d *floorDeferral) add(key string, q workqueue.TypedRateLimitingInterface[string], createdAt time.Time, cost time.Duration) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.keys[key] = floorDeferred{q: q, createdAt: createdAt, cost: cost}
	d.n.Store(int64(len(d.keys)))
	d.mu.Unlock()
	d.evaluate(true, false)
}

// backlogFell is the end of a key's work on this side: K may now be below the
// K the timer was armed with.
func (d *floorDeferral) backlogFell() {
	if d == nil || d.n.Load() == 0 {
		return
	}
	d.evaluate(false, false)
}

// evaluate releases every key whose due time, at the side's current K, has
// passed, and arms the timer at the soonest remaining one. force scans whatever
// K is (a new key, the timer); otherwise only a K below kScan can make a key
// due before the armed timer. fromTimer marks the timer's own evaluation: one
// that releases nothing while keys remain is counted (floorDeferralHeld).
func (d *floorDeferral) evaluate(force, fromTimer bool) {
	k, s := d.r.backlogShare(d.tier) // priorityGetMu, never taken under d.mu
	c := ResolvedCache()
	d.mu.Lock()
	if d.stopped || len(d.keys) == 0 || (!force && k >= d.kScan) {
		d.mu.Unlock()
		return
	}
	var now time.Time
	if c != nil {
		now = c.now() // the store clock CreatedAt was stamped on (#930)
	}
	var release []string
	var queues []workqueue.TypedRateLimitingInterface[string]
	var soonest time.Time
	for key, e := range d.keys {
		due := e.createdAt.Add(floorFor(e.cost, k, s))
		if c == nil || !due.After(now) {
			release = append(release, key)
			queues = append(queues, e.q)
			delete(d.keys, key)
			continue
		}
		if soonest.IsZero() || due.Before(soonest) {
			soonest = due
		}
	}
	d.kScan = k
	d.n.Store(int64(len(d.keys)))
	if fromTimer && len(release) == 0 && len(d.keys) > 0 {
		floorDeferralHeld.Add(1)
	}
	if len(d.keys) == 0 {
		if d.timer != nil {
			d.timer.Stop()
		}
	} else if wait := soonest.Sub(now); d.timer == nil {
		d.timer = time.AfterFunc(wait, func() { d.evaluate(true, true) })
	} else {
		d.timer.Reset(wait)
	}
	d.mu.Unlock()
	for i, key := range release {
		queues[i].Add(key)
	}
}

// stop ends the set with its queues: the timer is stopped and nothing is
// released any more (the queues are shut down; an Add would be dropped anyway).
func (d *floorDeferral) stop() {
	d.mu.Lock()
	d.stopped = true
	if d.timer != nil {
		d.timer.Stop()
	}
	d.keys = map[string]floorDeferred{}
	d.n.Store(0)
	d.mu.Unlock()
}

// floorDeferralHeld counts the timer's evaluations that released no key while
// floored keys remained: the timer fired at a due time the current K (or the
// store clock) had since moved later. TEST-ONLY readout
// (FloorDeferralHeldForTest); no series is published from it.
var floorDeferralHeld atomic.Uint64

// FloorDeferralHeldForTest reads floorDeferralHeld. TEST-ONLY: a cross-package
// arm uses it as the event "the floor's timer fired and the cell stayed
// floored" (dispatchers F6c). Production code MUST NOT call it.
func FloorDeferralHeldForTest() uint64 { return floorDeferralHeld.Load() }

// floorDeferralFor is the set of the side that serves tier.
func (r *refresher) floorDeferralFor(tier refreshTier) *floorDeferral {
	if tier == tierViewed {
		return r.viewedDeferral
	}
	return r.unviewedDeferral
}

// stopFloorDeferrals stops both sides' sets.
func (r *refresher) stopFloorDeferrals() {
	if r.viewedDeferral != nil {
		r.viewedDeferral.stop()
	}
	if r.unviewedDeferral != nil {
		r.unviewedDeferral.stop()
	}
}
