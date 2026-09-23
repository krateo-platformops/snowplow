// store_repair.go — snowplow#237 deliverable B: the bounded repair verb for
// divergences the forced pass finds.
//
// # WHY THE FORCED PASS NEEDS A REPAIR VERB AND THE SNAPSHOT PATH DOES NOT
//
// On the snapshot path there is a Replace, so repair is client-go's and it is
// already correct: processDeltas writes the store and THEN calls the handler.
// On the forced pass there is no Replace, so without a verb the pass would
// detect and do nothing — and routing it to submitDepEvent would be worse than
// nothing: that lands on probeObjectState -> GetByKey on the STILL-STALE
// indexer -> objExists -> dirty-mark -> the refresher re-resolves from the same
// stale store and reproduces the same stale bytes, turning the counter green
// over a still-wrong widget. #237 measured exactly that shape from the outside:
// eight consecutive /call requests over twelve minutes returning a
// byte-identical stale body.
//
// A detector that fires while the symptom persists is worse than no detector,
// because it converts an open investigation into a closed one.
//
// # THE SIX BOUNDS, AND WHY THE SECOND IS THE ONE THAT MATTERS
//
//  1. At most one repair per GVR in flight, never concurrent with itself.
//
//  2. GLOBAL SERIALISATION — at most one repair in flight ACROSS ALL GVRs.
//     A repair retracts the GVR's confirmation for the whole rebuild window, so
//     every resolve for it falls through to the apiserver. Unserialised, a
//     correlated divergence (an apiserver blip, a bad comparison) takes N GVRs
//     non-servable simultaneously and sends the whole portal to the apiserver:
//     the pre-cache regime PLUS a self-inflicted thundering herd, triggered by
//     our own repair, against the one rule customer traffic always wins.
//     Serialised, the blast radius is one widget class at a time. A long queue
//     is the better failure — slow, visible, bounded in radius.
//
//  3. CIRCUIT BREAKER. If the next verification of a GVR still reports
//     divergence after a repair, stop repairing it, latch, and raise. "I
//     relisted and it is still divergent" does not mean a lost event; it means
//     the comparison is wrong, or our view and the apiserver's differ for a
//     reason a relist cannot fix. Hammering a 50K GVR on that basis is
//     self-harm. Latched in noteVerified (store_verify_state.go), which is the
//     only place that sees both the pending verdict and the new result.
//
//  4. DETECTION CONTINUES UNDER SUPPRESSION. A suppressed GVR keeps reporting
//     divergence; it simply stops repairing. Silence must never be produced by
//     the breaker.
//
//  5. NO SIZE GATE. Large GVRs are not skipped — the 50K composition GVR is
//     exactly where staleness hurts most. What is bounded is CONCURRENCY
//     (bound 2), never eligibility.
//
//  6. THE GVR MUST BE REBUILDABLE. A GVR whose informer comes from the shared
//     dynamic factory cannot be torn down and re-registered — the factory
//     caches by GVR with no eviction API and hands the STOPPED informer back,
//     so the "repair" would kill the cache for that GVR instead. Refused, out
//     loud, with detection continuing. See enqueueStoreRepair.
//
// # THE QUEUE IS AN INSTRUMENT, NOT A HIDDEN DETAIL
//
// Serialisation buys blast-radius containment and pays for it in queue wait,
// and under a correlated divergence the queue wait DOMINATES: with ~5 minutes
// per repair, fifty correlated GVRs is about four hours of drain. So the
// staleness bound is maxVerificationAge + REPAIR-QUEUE WAIT + one relist, and
// "minutes" is wrong whenever the queue is deep. store_repairs_pending and
// store_repair_queue_age_seconds are where that term is observable; without
// them the bound would be quoted from a table that omits its largest term.
//
// FIFO by DETECTION TIME, not by GVR size, so a 50K GVR at the head cannot
// starve everything behind it by being re-detected first every round. Pending
// entries are deduped per GVR: bound 1 forbids concurrent relists of one GVR,
// which is not the same as forbidding a second queue entry for one already
// waiting, and a GVR re-detected at the next deadline while still pending would
// otherwise be enqueued twice.
package cache

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

type storeRepairRequest struct {
	gvr      schema.GroupVersionResource
	detected time.Time
	reason   string
}

var storeRepair = struct {
	mu       sync.Mutex
	queue    []storeRepairRequest
	wake     chan struct{}
	inFlight bool
}{
	wake: make(chan struct{}, 1),
}

// enqueueStoreRepair queues a repair for gvr, or returns false when it is
// already pending, already suppressed by the breaker, or unknown.
//
// The suppression check lives here rather than in the caller so every future
// caller inherits it: a breaker that only one call site honours is not a
// breaker.
func enqueueStoreRepair(gvr schema.GroupVersionResource, reason string) bool {
	storeVerify.mu.Lock()
	st, known := storeVerify.gvrs[gvr]
	if !known || st.suppressed || st.repairPending {
		storeVerify.mu.Unlock()
		return false
	}
	ownsInformer := st.ownsInformer
	storeVerify.mu.Unlock()

	// BOUND 6 — THE GVR MUST BE REBUILDABLE, and this one is not optional.
	//
	// relistGVRForRepair is removeResourceType + EnsureResourceType. That only
	// produces a FRESH informer when the GVR owns its informer outright: the
	// streaming constructor builds a new one every call, and so does the
	// standalone dynamic path for a navigation-discovered group. A GVR served
	// by the SHARED dynamic factory is different — the factory caches
	// informers by GVR with no eviction API, so the teardown stops the cached
	// informer and the re-register hands the STOPPED one straight back. The
	// symptoms are visible in the log as "informer has already started" and
	// "handler was not added to shared informer because it has stopped
	// already", and the GVR is then permanently dead rather than repaired.
	//
	// This is the same constraint R6 (0.30.115) already addressed for the CRD
	// schema-relist path by giving removable GVRs a standalone informer; the
	// repair path inherits it, and inheriting it silently would mean a repair
	// that DESTROYS the cache for that GVR while store_repairs_fired_total
	// counted it as a success. So it is refused, out loud, and detection
	// continues.
	//
	// THE TEST IS THE RECORDED PROPERTY, NEVER THE GROUP NAME. An earlier
	// draft asked `!decorated && !IsNavigationDiscoveredGroup(gvr.Group)`,
	// which answers the right question only by coincidence of today's routing.
	// H5 has already re-routed informers once; the next change would let a
	// factory-built GVR in a navigation-discovered group pass that check and
	// be killed by its own repair — and NO arm would have failed, because a
	// group-derived check and an ownership check agree on every GVR that
	// exists right now. ownsInformer is set by the branch that CONSTRUCTS the
	// informer (watcher.go), which is the only place that knows.
	//
	// The refused set is a CLASS, not a list: "a GVR whose informer is
	// factory-built rather than owned". Naming today's members would make this
	// comment wrong the next time routing changes.
	//
	// Leaving that class unrepairable is a DECISION, not an oversight — #244.
	// Its members' construction is load-bearing for RBAC correctness
	// (stripAndType needs *unstructured.Unstructured), and the risk is
	// inverted: RBAC staleness surfaces as an authorization decision, which is
	// loud and attributable, whereas #237 is severe precisely because widget
	// staleness is silent. If it ever does matter, the right verb is a
	// targeted rebuildRBACSnapshot, not an informer teardown.
	if !ownsInformer {
		storeRepairUnsupportedTotal.Add(1)
		slog.Warn("cache.store.repair_unsupported",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", "this GVR's informer comes from the shared dynamic factory, which has no "+
				"eviction API — tearing it down and re-registering would hand back the STOPPED "+
				"informer and kill the cache for this GVR rather than repair it. The divergence "+
				"is still reported; it is not repaired."))
		return false
	}

	// RE-CHECK UNDER THE SECOND LOCK. The mutex was dropped across the
	// ownership decision above, so the dedupe would otherwise be true only
	// because today's single caller — the strictly-serial deadline walker —
	// cannot race itself. This function's own comment says the suppression
	// check lives here "so every future caller inherits it", i.e. more callers
	// are expected; a guarantee that holds by caller discipline rather than by
	// construction is one a future caller will break without noticing.
	storeVerify.mu.Lock()
	st, ok := storeVerify.gvrs[gvr]
	if !ok || st.suppressed || st.repairPending {
		storeVerify.mu.Unlock()
		return false
	}
	st.repairPending = true
	storeVerify.mu.Unlock()

	storeRepair.mu.Lock()
	storeRepair.queue = append(storeRepair.queue, storeRepairRequest{
		gvr:      gvr,
		detected: time.Now(),
		reason:   reason,
	})
	storeRepair.mu.Unlock()

	select {
	case storeRepair.wake <- struct{}{}:
	default:
	}
	return true
}

// storeRepairQueueDepth is store_repairs_pending. Counts the queue plus the one
// in flight, because a repair that has started is still an outstanding repair
// from an operator's point of view and a gauge that drops to 0 for the five
// minutes the work actually takes would be actively misleading.
func storeRepairQueueDepth() int64 {
	storeRepair.mu.Lock()
	defer storeRepair.mu.Unlock()
	n := int64(len(storeRepair.queue))
	if storeRepair.inFlight {
		n++
	}
	return n
}

// storeRepairQueueAgeSeconds is store_repair_queue_age_seconds: the age of the
// OLDEST waiting entry, measured from when its divergence was detected. Rising
// without bound is the readable form of "the serialised queue is not draining",
// which is the regime in which the staleness bound stops holding.
func storeRepairQueueAgeSeconds() float64 {
	storeRepair.mu.Lock()
	defer storeRepair.mu.Unlock()
	if len(storeRepair.queue) == 0 {
		return 0
	}
	return time.Since(storeRepair.queue[0].detected).Seconds()
}

// runStoreRepairQueue is the single repair goroutine. Being single IS bound 2:
// there is no worker pool to size and no way for a future caller to widen the
// concurrency without deleting this comment and this loop.
func runStoreRepairQueue(ctx context.Context) {
	for {
		req, ok := dequeueStoreRepair()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-storeRepair.wake:
				continue
			case <-time.After(storeRepairIdlePoll):
				continue
			}
		}
		if ctx.Err() != nil {
			return
		}
		runOneStoreRepair(req)
	}
}

// storeRepairIdlePoll is the idle wake-up. The wake channel carries the signal;
// this only bounds how long a wake lost to a full channel can delay a repair,
// and it is deliberately far shorter than a repair takes so it never becomes
// the rate-limiting term.
const storeRepairIdlePoll = 5 * time.Second

func dequeueStoreRepair() (storeRepairRequest, bool) {
	storeRepair.mu.Lock()
	defer storeRepair.mu.Unlock()
	if len(storeRepair.queue) == 0 {
		return storeRepairRequest{}, false
	}
	req := storeRepair.queue[0]
	storeRepair.queue = storeRepair.queue[1:]
	storeRepair.inFlight = true
	return req, true
}

// runOneStoreRepair performs one repair and records that it happened.
func runOneStoreRepair(req storeRepairRequest) {
	defer func() {
		storeRepair.mu.Lock()
		storeRepair.inFlight = false
		storeRepair.mu.Unlock()
	}()
	defer func() {
		if rec := recover(); rec != nil {
			slog.Error("cache.store.repair.panic",
				slog.String("subsystem", "cache"),
				slog.String("gvr", req.gvr.String()),
				slog.Any("panic", rec),
			)
		}
		storeVerify.mu.Lock()
		if st, ok := storeVerify.gvrs[req.gvr]; ok {
			st.repairPending = false
		}
		storeVerify.mu.Unlock()
	}()

	rw := Global()
	waited := time.Since(req.detected)
	slog.Warn("cache.store.repair",
		slog.String("subsystem", "cache"),
		slog.String("gvr", req.gvr.String()),
		slog.String("reason", req.reason),
		slog.Float64("queue_wait_seconds", waited.Seconds()),
		slog.String("hint", "relisting this GVR to repair a confirmed store divergence. It is NOT "+
			"SERVABLE for the rebuild window and every resolve for it falls through to the "+
			"apiserver until its replacement informer syncs — the deliberate trade of "+
			"fast-but-wrong for slow-but-correct. Repairs are globally serialised, so "+
			"queue_wait_seconds is part of the staleness bound, not overhead beside it."),
	)
	syncCh, ok := crdDiscoverySingleton().relistGVRForRepair(rw, req.gvr, confirmRetractStoreRepair,
		func(d *DepTracker, g schema.GroupVersionResource) int { return d.OnResourceTypeStoreRepaired(g) })
	if !ok {
		// Nothing to relist — the GVR was torn down between detection and
		// here. Not a repair, so it is not counted as one: counting the
		// invocation rather than the effect is how repairs_fired_total would
		// come to read as health over work that never happened.
		recordVerifySkipped(verifySkipTornDown)
		return
	}
	storeRepairsFiredTotal.Add(1)

	// WAIT FOR THE REBUILD, and this wait is what makes bound 2 mean what it
	// says. Serialising only the teardown CALLS would leave the rebuild
	// WINDOWS free to overlap — and it is the window, not the call, during
	// which a GVR is non-servable and every resolve for it falls through to
	// the apiserver. Without this the queue would drain in milliseconds while
	// N GVRs went non-servable together: exactly the self-inflicted thundering
	// herd the serialisation exists to prevent, with a gauge reading 0 pending
	// throughout.
	//
	// Bounded, because a GVR that never syncs must not stop every other repair
	// for the life of the process. On expiry we move on and the next
	// verification of that GVR judges it like any other.
	//
	// The bound is relistPostSyncWait — the EXISTING budget for "how long may a
	// relisted GVR's replacement informer take to sync", already used by the
	// post-sync dirty-mark re-fire and already instrumented as
	// relist_postsync_timeout_total. Reused rather than duplicated: a second
	// constant for the same question is a second number to keep in step, and
	// the two could then disagree about how long a rebuild is allowed to take.
	if syncCh != nil {
		select {
		case <-syncCh:
		case <-time.After(relistPostSyncWait):
			slog.Warn("cache.store.repair_sync_timeout",
				slog.String("subsystem", "cache"),
				slog.String("gvr", req.gvr.String()),
				slog.Duration("waited", relistPostSyncWait),
				slog.String("hint", "the replacement informer did not sync within the bound, so the "+
					"repair queue moved on. This GVR stays non-servable until it does sync, and "+
					"the next verification decides whether the repair worked."))
		}
	}

	// ARM THE CIRCUIT BREAKER — AFTER the rebuild, and this ordering is the
	// whole of it. The relist goes through removeResourceType +
	// EnsureResourceType, and re-registration calls rememberStoreVerification,
	// which installs a FRESH bookkeeping entry: a flag set before the relist is
	// wiped by the relist itself, and the breaker could then never latch no
	// matter how many ineffective repairs ran. That is a detector that cannot
	// detect, so it is set here, once the entry the next verification will read
	// actually exists.
	//
	// Nothing consumes the flag in between: only the deadline pass calls
	// noteVerified, and a snapshot-path comparison never does.
	storeVerify.mu.Lock()
	if st, ok := storeVerify.gvrs[req.gvr]; ok {
		st.repairedAwaitingVerify = true
	}
	storeVerify.mu.Unlock()
}

// resetStoreRepairForTest drains the queue.
func resetStoreRepairForTest() {
	storeRepair.mu.Lock()
	storeRepair.queue = nil
	storeRepair.inFlight = false
	storeRepair.mu.Unlock()
}
