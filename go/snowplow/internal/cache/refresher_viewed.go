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
// #799 (dev-765h's pair at 6bd30396, RUN; architect-623's ruling):
//   - SHARE THE FILL. A viewed re-resolve also carries withViewedRefresh: a
//     passive viewer, no customer request waiting, so its content fill joins the
//     key's pending fill like a background reader (keeping the cap bypass)
//     instead of leading its own re-fill of the same 50K list (fills in flight
//     went 1 → 4). K5 / #547 demand / D2 never carry it and never wait.
//   - RESERVE A WORKER. At most parallelism−1 workers are in viewed work at
//     once (viewedSlotFree); with one worker the tiers alternate. Under constant
//     churn on viewed cells the viewed tier never empties, and strict priority
//     had the normal tier complete ZERO refreshes.
//   - STEP ASIDE. A promoted key's other copy does not run while its viewed copy
//     is still queued (the reservation gives the normal tier turns, so it could
//     otherwise run first, as background).
//
// #1069 (run 20: a viewer waited 2.4–3.2 s p95 for a change, against 1 s):
//   - RESERVE A WORKER FOR VIEWED WORK, the mirror of fix 2. All N workers
//     could be in ~0.9 s unviewed re-resolves at once, and the viewed tier was
//     probed only when one ended. Now normal + cluster_list work holds at most
//     parallelism−1 workers (unviewedSlotFree; a worker blocked in the normal
//     Get holds its slot), so one worker is always free or already in viewed
//     work. A worker that may take neither side parks on reserveCond and is
//     woken by wakeViewed — the same wake every viewed add already makes — never
//     by a poll. No new constant: both bounds are parallelism−1.
//   - PARALLELISM 1: nothing can be reserved; the tiers alternate, and the wake
//     key is not a turn (viewedSlotFree).
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
	"context"
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

// newViewedQueue builds the viewed tier over rl; wake (refresher.wakeViewed)
// wakes an idle worker through the normal queue.
// #930 — the tier counts its outstanding work into w with the other tiers.
func newViewedQueue(rl workqueue.TypedRateLimiter[string], wake func(), w *refresherWorkCount, stamps *enqueueStamps) workqueue.TypedRateLimitingInterface[string] {
	return newRefresherTierQueue("refresher_viewed", rl, w, stamps, func(q workqueue.TypedInterface[string]) workqueue.TypedInterface[string] {
		return &wakingQueue{TypedInterface: q, wake: wake}
	})
}

type ctxKeyViewedRefreshType struct{}

var ctxKeyViewedRefresh = ctxKeyViewedRefreshType{}

// withViewedRefresh marks a viewed-tier re-resolve whose tab is still armed
// (#799 fix 1). It rides beside WithCustomerTriggeredRefresh (the cap bypass)
// and tells the content fill this refresh has NO customer request waiting on
// it — a passive viewer waits on a push — so it may join the key's pending fill
// like a background reader instead of leading its own concurrent re-fill of the
// same list. K5 / #547 demand revalidations and the D2 refill (a customer read
// IS waiting) never carry it, and keep never waiting (#710).
func withViewedRefresh(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyViewedRefresh, true)
}

// ViewedRefreshFromContext reports withViewedRefresh.
func ViewedRefreshFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyViewedRefresh).(bool)
	return v
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
	// deferredInFlight (#799 B2): dequeues not run because another tier's
	// worker was re-resolving the same key; the key was re-added when it ended.
	deferredInFlight atomic.Uint64
	// #1069 — completed: successful viewed-tier dequeues (the viewed share of
	// the pool's completed); reserveParks: parks of a worker that could take
	// neither side's slot (the reserved worker waiting for viewed work). A park
	// is one Wait, so its rate is bounded by the work, never by elapsed time.
	completed    atomic.Uint64
	reserveParks atomic.Uint64
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
	r.promotedKeys.Store(key, promotedViewedPending)
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
		// The promoted key's viewed copy is being dequeued: from here its other
		// copy may only run for a mark that lands after it.
		r.promotedKeys.CompareAndSwap(key, promotedViewedPending, promotedViewedDone)
		return !r.hasPendingTriggers(key)
	case tierNormal, tierClusterList:
		v, promoted := r.promotedKeys.Load(key)
		if !promoted {
			return false
		}
		// #799 — the viewed copy is still queued: it carries the triggers and
		// runs at the viewer's priority, so this copy steps aside (else, with the
		// fix-2 reservation or p=1 alternation, it could run first as background).
		if v == promotedViewedPending || !r.hasPendingTriggers(key) {
			r.promotedKeys.Delete(key)
			return true
		}
	}
	return false
}

// The promoted marker's two states: the viewed copy is still queued, or it has
// been dequeued (the other copy then runs only for a mark that came after).
const (
	promotedViewedPending = 1
	promotedViewedDone    = 2
)

// consumeTriggersOnTier is called where a dequeue takes key's trigger set: a
// non-viewed dequeue that runs the refresh itself retires the promoted marker
// (the viewed copy then finds no triggers and skips). A floored dequeue never
// gets here, so its marker survives to the copy that actually runs.
func (r *refresher) consumeTriggersOnTier(key string, tier refreshTier) {
	if tier != tierViewed {
		r.promotedKeys.Delete(key)
	}
}

// routeDirty enqueues a dirty-marked (or deferred) key on its tier: viewed when
// a tab has it armed, else cluster_list when registered, else normal. Called by
// onDirtyMark AFTER markDirty: an arm racing the mark is either seen here, or
// sees the window markDirty opened (promoteOnArm).
func (r *refresher) routeDirty(l1Key string) {
	switch _, isClusterList := r.clusterListKeys.Load(l1Key); {
	case HasRefreshSubscriber(l1Key):
		r.enqueueViewed(l1Key)
	case isClusterList:
		r.enqueueClusterList(l1Key)
	default:
		r.enqueue(l1Key)
	}
}

// tierProbeHook is a test seam (nil in production) fired between a priority
// tier's Len()>0 probe and its Get, so an arm can hold a worker exactly there.
var tierProbeHook atomic.Pointer[func(refreshTier)]

// dequeuePick is what nextDequeue hands the worker.
type dequeuePick uint8

const (
	// pickPriority: a key from the viewed or the cluster_list tier.
	pickPriority dequeuePick = iota
	// pickNormal: the worker holds an unviewed slot and blocks in the normal
	// tier's Get (which may hand it the wake key).
	pickNormal
	// pickShutdown: the normal tier is shutting down and nothing is left for
	// this worker.
	pickShutdown
)

// nextDequeue picks the worker's next unit of work, under priorityGetMu.
//
// #799 B1 — a priority tier's Len()>0 probe and its Get are ONE step: processNext
// is the tiers' only consumer, so a Len()>0 seen under the lock is a key the Get
// returns at once. Before, two workers could both see Len()==1 and the loser
// parked in the blocking Get until the tier's next add.
//
// #1069 — TWO RESERVATIONS, one per side, both derived from the pool size:
//   - viewedSlotFree (#799 fix 2): viewed work holds at most parallelism−1
//     workers, so the normal and cluster_list tiers always keep one;
//   - unviewedSlotFree: normal + cluster_list work holds at most parallelism−1,
//     so the viewed tier always keeps one. A worker blocked in the normal tier's
//     Get HOLDS its unviewed slot while it blocks, so at most parallelism−1
//     workers can ever come back from that Get with a key.
//
// A worker that may take neither parks on reserveCond: it is the RESERVED worker.
// It wakes when wakeViewed announces a viewed key (every viewed add, delayed adds
// included, and a viewed slot release) and on shutdown. It never polls: a park
// is one Wait. A worker parks only while the unviewed side is full, which leaves
// no viewed worker beside it, so every viewed arrival finds it; if a worker
// leaving an unviewed job takes that key first, the woken one finds the slot
// that worker left and takes it. Before #1069 every worker could be in a 0.9 s
// unviewed job at once, and a viewed key waited for the first of them to end
// (run 20: 2.4–3.2 s p95 for a viewer).
func (r *refresher) nextDequeue() (key string, tier refreshTier, pick dequeuePick) {
	r.priorityGetMu.Lock()
	defer r.priorityGetMu.Unlock()
	for {
		if r.viewedQueue != nil && r.viewedQueue.Len() > 0 && r.viewedSlotFree() {
			if k, ok := r.priorityGetLocked(r.viewedQueue, tierViewed); ok {
				r.viewedActive++ // released by endViewed
				r.lastViewed.Store(true)
				return k, tierViewed, pickPriority
			}
		}
		if r.unviewedSlotFree() {
			r.unviewedActive++ // released by endUnviewed
			if r.clusterListQueue != nil && r.clusterListQueue.Len() > 0 {
				if k, ok := r.priorityGetLocked(r.clusterListQueue, tierClusterList); ok {
					r.lastViewed.Store(false) // #799 N4: a cluster_list turn counts too
					return k, tierClusterList, pickPriority
				}
			}
			r.normalGetBlocked++ // ended by leftNormalGet
			return "", tierNormal, pickNormal
		}
		if r.reserveStopped || r.queue.ShuttingDown() {
			return "", tierNormal, pickShutdown
		}
		r.viewed.reserveParks.Add(1)
		r.reserveWaiting++ // decremented by whoever signals this park
		r.reserveCond.Wait()
	}
}

// priorityGetLocked is the Get of a priority tier whose Len()>0 the caller saw
// under priorityGetMu (#799 B1), with the test seam between the two.
func (r *refresher) priorityGetLocked(q workqueue.TypedRateLimitingInterface[string], t refreshTier) (string, bool) {
	if fn := tierProbeHook.Load(); fn != nil {
		(*fn)(t)
	}
	key, shutdown := q.Get()
	return key, !shutdown
}

// viewedSlotFree is the #799 fix-2 reservation, read under priorityGetMu: the
// viewed tier may hold at most parallelism−1 workers at once, so the normal and
// cluster_list tiers always keep one — under constant churn on viewed cells the
// viewed tier never empties, and strict priority starved every other cell (RUN,
// dev-765h: zero normal-tier completions). Structural, not a knob.
//
// PARALLELISM 1 (#1069, defined): one worker cannot be reserved for either side
// without starving the other, so there is no reservation and the tiers ALTERNATE.
// After a viewed dequeue the next dequeue goes to the cluster_list or normal tier
// when either has work; after any other dequeue the viewed tier goes first. The
// wake key is not a turn (processNext leaves lastViewed as it was), so a viewed
// key waits for at most the job in hand plus one other-tier job.
func (r *refresher) viewedSlotFree() bool {
	if r.parallelism <= 1 {
		return !r.lastViewed.Load() || (r.queue.Len() == 0 && (r.clusterListQueue == nil || r.clusterListQueue.Len() == 0))
	}
	return r.viewedActive < r.parallelism-1
}

// unviewedSlotFree is the #1069 mirror of viewedSlotFree, read under
// priorityGetMu: normal + cluster_list work may hold at most parallelism−1
// workers, so one worker is always free to start a viewed key the moment it is
// queued. With one worker there is nothing to reserve (see viewedSlotFree).
func (r *refresher) unviewedSlotFree() bool {
	if r.parallelism <= 1 {
		return true
	}
	return r.unviewedActive < r.parallelism-1
}

// signalReserveLocked wakes one parked reserved worker. Under priorityGetMu.
// The signaller takes the park off reserveWaiting, so a second wake that comes
// before the woken worker runs counts it as gone and goes to the wake key.
func (r *refresher) signalReserveLocked() {
	r.reserveWaiting--
	r.reserveCond.Signal()
}

// wakeViewed announces viewed work. When the reservation has a slot a woken
// worker could take (#799 B3), it wakes the parked reserved worker (#1069), or,
// with none parked, adds the wake key to the normal queue when a worker is
// blocked in its Get. With every viewed slot full it does nothing: a wake would
// only bring a worker back to a refused viewed probe, the normal Get would hand
// it the next wake, and so on — a busy spin (architect-623, RUN: ~85% of a core
// idle). A full reservation is re-woken by the slot release instead.
//
// #1069 — no wake key when no worker is blocked in the normal Get: every other
// worker is in a job or about to probe, and re-probes the viewed tier itself. A
// wake key nobody waits for sits in the normal FIFO and reads as normal work to
// the p=1 alternation; under churn on a viewed cell that starved the viewed tier
// once the wake key stopped counting as a turn.
func (r *refresher) wakeViewed() {
	r.priorityGetMu.Lock()
	free := r.viewedSlotFree()
	reserved := free && r.reserveWaiting > 0
	if reserved {
		r.signalReserveLocked()
	}
	blocked := free && !reserved && r.normalGetBlocked > 0
	r.priorityGetMu.Unlock()
	if blocked {
		r.queue.Add(viewedWakeKey)
	}
}

// endViewed releases a viewed-tier worker slot (#799 fix 2).
func (r *refresher) endViewed() {
	r.priorityGetMu.Lock()
	r.viewedActive--
	r.priorityGetMu.Unlock()
}

// leftNormalGet ends a worker's normal-tier Get (#1069); release also gives back
// its unviewed slot (it came back with the wake key, or the tier shut down).
func (r *refresher) leftNormalGet(release bool) {
	r.priorityGetMu.Lock()
	r.normalGetBlocked--
	if release {
		r.unviewedActive--
	}
	r.priorityGetMu.Unlock()
}

// endUnviewed releases an unviewed worker slot (#1069). No wake: the releasing
// worker goes straight back to nextDequeue and takes the slot again, unless it
// takes a viewed key — one whose wake already woke the parked worker, which then
// takes this slot (see nextDequeue).
func (r *refresher) endUnviewed() {
	r.priorityGetMu.Lock()
	r.unviewedActive--
	r.priorityGetMu.Unlock()
}

// wakeReservedOnShutdown releases every parked reserved worker when the normal
// tier shuts down: a worker blocked in a queue's Get learns that from the Get, a
// parked one only from this.
func (r *refresher) wakeReservedOnShutdown() {
	r.priorityGetMu.Lock()
	r.reserveStopped = true
	r.reserveWaiting = 0
	r.reserveCond.Broadcast()
	r.priorityGetMu.Unlock()
}

// shutdownNotifyingQueue is the normal tier's underlying queue: its ShutDown —
// direct, or the delaying queue's, which shuts down through it — also releases
// the parked reserved workers (#1069), whoever shuts the queue down.
type shutdownNotifyingQueue struct {
	workqueue.TypedInterface[string]
	onShutDown func()
}

func (q *shutdownNotifyingQueue) ShutDown() {
	q.TypedInterface.ShutDown()
	q.onShutDown()
}

func (q *shutdownNotifyingQueue) ShutDownWithDrain() {
	q.onShutDown()
	q.TypedInterface.ShutDownWithDrain()
}

// claimInFlight marks key as being re-resolved by this worker (#799 B2). false:
// another tier's worker is re-resolving it now — this dequeue does not run, its
// trigger set stays for the re-add, and the finishing worker re-adds the key.
// Before #799 a key lived in one tier, whose per-item exclusivity gave one
// re-resolve per key at a time; the trigger set, the dirty window, the audit
// pair, the background-revalidation slot and the rate floor all assume it.
func (r *refresher) claimInFlight(key string) bool {
	r.inFlightMu.Lock()
	defer r.inFlightMu.Unlock()
	if _, busy := r.inFlightKeys[key]; busy {
		r.inFlightKeys[key] = true // deferred: re-add when it finishes
		r.viewed.deferredInFlight.Add(1)
		return false
	}
	r.inFlightKeys[key] = false
	return true
}

// releaseInFlight ends key's re-resolve and, when a dequeue from another tier
// was deferred meanwhile, re-adds the key on its tier now (viewed if a tab still
// has it armed), so the change that dequeue carried is not lost.
func (r *refresher) releaseInFlight(key string) {
	r.inFlightMu.Lock()
	deferred := r.inFlightKeys[key]
	delete(r.inFlightKeys, key)
	r.inFlightMu.Unlock()
	if deferred {
		r.routeDirty(key)
	}
}

// viewedQueueDepth is the live viewed-tier Len(), 0 before the pool exists.
func viewedQueueDepth(r *refresher) int64 {
	if r == nil || r.viewedQueue == nil {
		return 0
	}
	return int64(r.viewedQueue.Len())
}
