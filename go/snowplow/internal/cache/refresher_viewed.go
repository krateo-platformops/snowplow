// refresher_viewed.go — #765: the VIEWED tier of the refresher.
//
// THE GAP (architect-623, #765 issuecomment-6286642350). A cell open on a page
// (an armed /refreshes subscription) that a change made dirty waited in the
// normal FIFO behind every unviewed dirty cell, then queued at the content-fill
// cap as background. "Users never queue" (#710) held for reads and for #740's
// retries, not for a viewer passively waiting on a push. Diego's ruling: refresh
// "obviously with the higher priority if these widgets are being read from the
// user".
//
// THE RULE, for every key with an armed subscription (no resource or time
// special case, no knob):
//
//  1. ROUTE. onDirtyMark sends a key with HasRefreshSubscriber to viewedQueue
//     (same dedup and rate limiter as the other tiers, unbounded, never drops).
//     processNext drains it first. Its size is bounded by the cells on open pages.
//  2. PROMOTE ON ARM. When a tab arms a key whose dirty window is PENDING (a mark
//     waiting, not known-stale — that is #740's chain, which the arm already
//     resumes), the key is added to viewedQueue and marked `promoted`.
//  3. RE-CHECK AT DEQUEUE. A viewed-tier dequeue whose key is still subscribed
//     runs WithCustomerTriggeredRefresh (the admission-cap bypass, exactly like
//     K5 and the #740 retries); one unsubscribed meanwhile runs as background.
//
// EXACTLY ONCE. Promotion can leave one key in two tiers. A dequeue whose key
// has NO pending trigger set has nothing left to do: every key that reaches the
// viewed tier came from a mark (and every mark merges its triggers before the
// enqueue); the floored deferral keeps them and a failed attempt restores them;
// so they are absent only when another dequeue of the same key consumed them.
// The viewed tier skips such a dequeue; the other tiers skip it only for a key
// carrying the `promoted` marker (other normal-tier sources — proactive passes,
// self-enqueues — legitimately carry no triggers). A mark landing after the
// first refresh brings new triggers, so the second dequeue then runs.
//
// UNCHANGED: yieldToCustomer before every tier, the per-key rate floor (applied
// after the dequeue, so it covers the viewed tier and defers onto it), the
// suppression, known-stale and poison-pill paths.
//
// IDLE WAKE. Workers block in the normal tier's Get when it is empty; a key added
// to viewedQueue then would wake nobody. The viewed tier's underlying queue
// (wakingQueue) therefore adds viewedWakeKey to the normal queue on EVERY add —
// a direct enqueue, a rate-floor AddAfter or a retry's AddRateLimited reaching
// it, since the delaying queue adds through it; a worker that dequeues it goes straight
// back to the viewed probe (and re-adds it while more viewed keys wait, so the
// next idle worker wakes too). With a busy normal queue the wake is a no-op:
// every iteration probes the viewed tier first.
package cache

import (
	"sync/atomic"

	"k8s.io/client-go/util/workqueue"
)

// wakingQueue is the viewed tier's underlying queue: every Add (direct, or the
// delaying queue's when an AddAfter / AddRateLimited delay expires) also wakes
// an idle worker blocked on the normal tier.
type wakingQueue struct {
	workqueue.TypedInterface[string]
	wake func()
}

func (w *wakingQueue) Add(item string) {
	w.TypedInterface.Add(item)
	w.wake()
}

// newViewedQueue builds the viewed tier over rl, waking idle workers through the
// normal queue.
func newViewedQueue(rl workqueue.TypedRateLimiter[string], normal workqueue.TypedInterface[string]) workqueue.TypedRateLimitingInterface[string] {
	wq := &wakingQueue{TypedInterface: workqueue.NewTyped[string](), wake: func() { normal.Add(viewedWakeKey) }}
	return workqueue.NewTypedRateLimitingQueueWithConfig(rl, workqueue.TypedRateLimitingQueueConfig[string]{
		DelayingQueue: workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Queue: wq}),
	})
}

// viewedWakeKey is the reserved normal-queue item that wakes an idle worker for
// the viewed tier. A real L1 key is a hex hash; this cannot collide with one.
const viewedWakeKey = "\x00refresher:viewed-wake"

// refreshTier is the queue a dequeue came from.
type refreshTier uint8

const (
	tierNormal refreshTier = iota
	tierClusterList
	tierViewed
)

// viewedTierStats are the #765 viewed-tier counters (fields of *refresher).
type viewedTierStats struct {
	enqueued        atomic.Uint64
	promoted        atomic.Uint64
	dequeued        atomic.Uint64
	skippedConsumed atomic.Uint64
	customer        atomic.Uint64
	unsubscribed    atomic.Uint64
}

// enqueueViewed adds l1Key to the viewed tier and wakes an idle worker.
func (r *refresher) enqueueViewed(l1Key string) {
	if l1Key == "" || r.viewedQueue == nil {
		return
	}
	r.onEnqueueAudit(l1Key) // #603 PR-2 SHADOW audit, as every tier
	r.viewedQueue.Add(l1Key)
	r.enqueueTotal.Add(1)
	r.viewed.enqueued.Add(1)
}

// hasPendingTriggers reports whether a mark's trigger set is waiting for key.
func (r *refresher) hasPendingTriggers(key string) bool {
	_, ok := r.triggerGVRByKey.Load(key)
	return ok
}

// dirtyPending reports whether key's dirty window holds a mark still waiting for
// a dequeue (not known-stale: a failed refresh of a viewed cell is #740's chain).
func (r *refresher) dirtyPending(key string) bool {
	v, ok := r.dirty.Load(key)
	if !ok {
		return false
	}
	s := v.(*dirtyStamp)
	if s.stale != knownStaleNone {
		return false
	}
	return !s.inflight || !s.next.IsZero()
}

// promoteOnArm moves a dirty-queued key a tab just armed to the viewed tier
// (#765 rule 2). Idempotent per arm: viewedQueue dedups.
func (r *refresher) promoteOnArm(key string) {
	if key == "" || r.viewedQueue == nil || !r.dirtyPending(key) {
		return
	}
	r.promotedKeys.Store(key, struct{}{})
	r.viewed.promoted.Add(1)
	r.enqueueViewed(key)
}

// notePromoteOnArm is the /refreshes arm hook: each armed key with a pending
// mark is promoted to the viewed tier. A telemetry-free peek: an arm never
// constructs the refresher.
func notePromoteOnArm(armedKeys map[string]struct{}) {
	r := refresherPeek()
	if r == nil {
		return
	}
	for k := range armedKeys {
		r.promoteOnArm(k)
	}
}

// consumedElsewhere decides the exactly-once skip for a dequeue of key from tier
// (see the file comment). A skipped non-viewed copy retires the promoted marker.
func (r *refresher) consumedElsewhere(key string, tier refreshTier) bool {
	switch tier {
	case tierViewed:
		return !r.hasPendingTriggers(key)
	case tierNormal, tierClusterList:
		if _, promoted := r.promotedKeys.Load(key); promoted && !r.hasPendingTriggers(key) {
			r.promotedKeys.Delete(key)
			return true
		}
	}
	return false
}

// consumeTriggersOnTier is called where a dequeue takes key's trigger set: a
// non-viewed dequeue that runs the refresh itself retires the promoted marker
// (the viewed copy then finds no triggers and skips). A floored dequeue never
// gets here, so its marker survives to the copy that actually runs.
func (r *refresher) consumeTriggersOnTier(key string, tier refreshTier) {
	if tier != tierViewed {
		r.promotedKeys.Delete(key)
	}
}

// viewedQueueDepth is the live viewed-tier Len(), 0 before the pool exists.
func viewedQueueDepth(r *refresher) int64 {
	if r == nil || r.viewedQueue == nil {
		return 0
	}
	return int64(r.viewedQueue.Len())
}
