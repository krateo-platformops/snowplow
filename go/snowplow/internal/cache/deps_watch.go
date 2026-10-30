// deps_watch.go — the informer→DepTracker event bridge.
//
// 1.12.6 C1 (design-1.12.6-event-hardening §3): ACTIONS DERIVE FROM STATE,
// NOT FROM THE MESSAGE.
//
// Pre-1.12.6 the three informer handlers chose the ACTION at the moment the
// event arrived: AddFunc/UpdateFunc → dirty-mark inline, DeleteFunc → evict via
// a bounded channel + one worker. The action was the only carrier of the
// information, so a message that was lost (dead worker, relist teardown
// window), duplicated, reordered (a late DELETE after a same-name recreate
// evicted the CORRECT fresh entry) or coalesced produced a wrong action and
// nothing could notice.
//
// Now all three handlers do the same thing: ENQUEUE THE COORDINATE
// (gvr, namespace, name) on one typed, deduplicating, rate-limiting workqueue.
// One worker dequeues, PROBES the informer for the object's current state and
// derives the action from that state at the moment it runs:
//
//	objExists  → dirty-mark every dependent (ADD/UPDATE-equivalent)
//	objAbsent  → evict the self-representation, dirty-mark the rest (DELETE-equivalent)
//	objUnknown → the indexer is NOT authoritative for this GVR right now
//	             (informer torn down / not synced / watch broken / type
//	             unconfirmed, or passthrough mode): requeue with backoff on
//	             the SAME budget the refresher uses (maxRefreshRequeues), and
//	             on exhaustion DEGRADE to a dirty-mark — whose refresher
//	             re-fetch reaches the apiserver. Never "keep forever".
//
// This is the client-go sample-controller idiom (handlers queue.Add(key);
// the worker reads the lister; NotFound is the deletion branch) that snowplow
// had diverged from. Duplicates are idempotent, reordering is harmless,
// coalescing is safe (the typed workqueue dedups pending keys) and a lost
// event is recoverable by the next enqueue for the same coordinate — from the
// informer, the relist bridge (C2) or the sampled reconcile (C3).
//
// WHAT STAYS SPECIAL (design §3.4):
//   - The ADD pre-sync gate (addEventPostSync) stays AT THE HANDLER, ADD-only.
//     Every existing object arrives as an ADD during the initial LIST replay;
//     dirty-marking the world would storm the refresher. Applying the gate to
//     DELETEs would reintroduce loss, so it is not hoisted into the worker.
//   - LIST-scope deps (bucket 2, the June deleted-list-member class) and
//     dependent-GET deps (bucket 3) are state-independent: they dirty-mark
//     whatever the object's state. Only the SELF representation (bucket 1)
//     consults the state, and only objAbsent evicts it.
//
// WHY probeObjectState IS NOT THE INERT IsRegistered CONJUNCT (design §0/§3.3):
// different predicate (IsServable = registered AND synced AND watch not
// broken AND type confirmed — lazy registration satisfies only the first),
// different call site (the worker reads the watcher and never calls
// EnsureResourceType, so asking cannot change the answer), different failure
// direction (objUnknown DEFERS an indexer-derived eviction and hands the
// coordinate to a path that reaches the apiserver; it never suppresses one).
//
// SUPERVISION (1.12.5 #187 H1, kept): every event runs under its own recover;
// a panic costs exactly that one event (counted, WARN-logged with its
// coordinates) and the drain continues. The worker loop itself has a bounded
// re-spawn supervisor for a deterministic panic outside the per-event recover.
//
// QUEUE (design §3.5): the typed workqueue is unbounded and never drops, so
// the pre-1.12.6 overflow machinery (1024-slot channel, inline fallback,
// delete_queue_full) is RETIRED, not redefined. Memory bound: ~200 B per
// pending coordinate, deduplicated — a 50K-object churn storm is ≈10 MB.

package cache

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// depEventKey is the coordinate an informer event announces. It carries NO
// event type: the worker derives the action from the object's current state.
type depEventKey struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string
}

// depWatchCounters holds the bridge's falsifier counters. Process-scoped
// (the bridge wires one DepTracker singleton); atomic so they are readable
// without a lock.
type depWatchCounters struct {
	addDroppedPreSync atomic.Uint64 // ADD events dropped during initial replay (gate at the handler)
	addPropagated     atomic.Uint64 // ADD events that passed the gate and were enqueued
	addNilSyncCh      atomic.Uint64 // ADD events seen with a nil syncCh (O14)
	eventsSubmitted   atomic.Uint64 // coordinates enqueued (all three handlers)
	// #688: initial-LIST ADDs delivered after syncCh closed, dropped on
	// isInInitialList (a subset of addDroppedPreSync); and live ADDs
	// delivered while syncCh was still open, propagated (a subset of
	// addPropagated).
	addInitialListPastSync atomic.Uint64
	addLivePreSyncCh       atomic.Uint64

	// workerPanics — events whose processing panicked. Each one is a
	// SINGLE lost action (the worker survives and processes the next
	// event); pre-1.12.5 the first one killed DELETE handling process-wide
	// until restart. Published as delete_worker_panics_total for continuity
	// with the 1.12.5 operator procedure (the H1 signature).
	workerPanics atomic.Uint64

	// submitDroppedShutdown — submits that reached a shut-down queue: the
	// event is dropped (an Add after ShutDown is a no-op). #854: a lost
	// event, so the incremental list cells unanchor.
	submitDroppedShutdown atomic.Uint64

	// probe outcomes — what the worker derived the action from.
	probeExists          atomic.Uint64
	probeAbsent          atomic.Uint64
	probeUnknown         atomic.Uint64 // indexer not authoritative → requeued with backoff
	probeUnknownDegraded atomic.Uint64 // budget exhausted → degraded to a dirty-mark (apiserver decides)
}

// depWatch is the process-scoped informer→DepTracker bridge state: the
// counters plus the unified dep-event worker and its queue.
type depWatch struct {
	counters depWatchCounters

	// queue carries coordinates to the worker. Typed, deduplicating,
	// rate-limiting: fresh events use Add (immediate), objUnknown requeues
	// use AddRateLimited (exponential backoff, base/max from the refresher's
	// knobs so the subsystem has ONE backoff shape).
	//
	// Built LAZILY by startWorker, not by the singleton (1.12.6 C1 follow-up,
	// architect N1): a client-go workqueue spawns goroutines at construction
	// (the delaying queue's waitingLoop + a heartbeat ticker), so building it
	// in depWatchSingleton made a mere telemetry read (DepsStatsByStat under
	// CACHE_ENABLED=false, reached via the OTLP metrics mirror) spawn
	// goroutines — a violation of the byte-identical off-path. Nil until the
	// first submitDepEvent; readers use q() and nil-check.
	queue atomic.Pointer[depQueue]

	// watcher is the ResourceWatcher the worker probes. Bound at handler
	// construction (rw.depEventHandlers); production has exactly one.
	watcher atomic.Pointer[ResourceWatcher]

	startOnce sync.Once
	workerWG  sync.WaitGroup

	// nilSyncWarned ensures the O14 nil-syncCh WARN fires at most once.
	nilSyncWarned atomic.Bool

	// #603 PR-2 SHADOW — the per-coordinate transition masks (deps_list_interest.go),
	// OR-ed by the handlers BEFORE the coordinate is enqueued and swapped out by
	// the worker. The queue coalesces a coordinate's submits; the mask must not:
	// an A→B→A flip still sets the bit. Bounded like the queue (one entry per
	// pending coordinate).
	maskMu sync.Mutex
	masks  map[depEventKey]*eventMask

	// #854 — the coordinates whose event is still on its way to the hook:
	// queued (submitted or rate-limit-requeued, not yet picked up) and
	// processing (picked up, hook not yet returned). Guarded by maskMu. The
	// incremental list cell's comparator skips them (in flight, not diverged).
	queued     map[depEventKey]struct{}
	processing map[depEventKey]struct{}
}

// depWorkerTakenHookForTest runs in the dep worker right after takeMask, before
// the probe and the dep hook. TEST-ONLY (nil in production): #854 F5b stalls
// the worker there to prove the comparator treats such a key as in flight.
var depWorkerTakenHookForTest atomic.Pointer[func(gvr schema.GroupVersionResource, namespace, name string)]

// SetDepWorkerTakenHookForTest installs (nil clears) the hook. TEST-ONLY.
func SetDepWorkerTakenHookForTest(fn func(gvr schema.GroupVersionResource, namespace, name string)) (restore func()) {
	var p *func(gvr schema.GroupVersionResource, namespace, name string)
	if fn != nil {
		p = &fn
	}
	prev := depWorkerTakenHookForTest.Swap(p)
	return func() { depWorkerTakenHookForTest.Store(prev) }
}

// BumpDepWatchLostEventForTest moves one of the three lost-event counters
// ("panic", "nil_sync", "dropped_shutdown"). TEST-ONLY.
func BumpDepWatchLostEventForTest(kind string) {
	w := depWatchSingleton()
	switch kind {
	case "panic":
		w.counters.workerPanics.Add(1)
	case "nil_sync":
		w.counters.addNilSyncCh.Add(1)
	case "dropped_shutdown":
		w.counters.submitDroppedShutdown.Add(1)
	}
}

func (w *depWatch) markQueued(k depEventKey) {
	w.maskMu.Lock()
	if w.queued == nil {
		w.queued = map[depEventKey]struct{}{}
	}
	w.queued[k] = struct{}{}
	w.maskMu.Unlock()
}

func (w *depWatch) beginProcessing(k depEventKey) {
	w.maskMu.Lock()
	delete(w.queued, k)
	if w.processing == nil {
		w.processing = map[depEventKey]struct{}{}
	}
	w.processing[k] = struct{}{}
	w.maskMu.Unlock()
}

func (w *depWatch) endProcessing(k depEventKey) {
	w.maskMu.Lock()
	delete(w.processing, k)
	w.maskMu.Unlock()
}

// mergeMask ORs m into k's pending mask.
func (w *depWatch) mergeMask(k depEventKey, m *eventMask) {
	if m == nil {
		return
	}
	w.maskMu.Lock()
	if w.masks == nil {
		w.masks = map[depEventKey]*eventMask{}
	}
	if cur, ok := w.masks[k]; ok {
		cur.merge(m)
	} else {
		cp := *m
		cp.changed = nil
		cp.merge(&eventMask{gen: m.gen, changed: m.changed})
		w.masks[k] = &cp
	}
	w.maskMu.Unlock()
}

// takeMask removes and returns k's pending mask (nil when none).
func (w *depWatch) takeMask(k depEventKey) *eventMask {
	w.maskMu.Lock()
	m := w.masks[k]
	delete(w.masks, k)
	w.maskMu.Unlock()
	return m
}

// narrowUpdateMask / narrowMembershipMask / narrowUnknownMask are the
// handler-side mask builders; nil while the shadow is off.
func narrowUpdateMask(gvr schema.GroupVersionResource, oldObj, newObj interface{}) *eventMask {
	if !listNarrowShadowOn() {
		return nil
	}
	return Deps().updateMask(gvr, oldObj, newObj)
}

func narrowMembershipMask(gvr schema.GroupVersionResource) *eventMask {
	if !listNarrowShadowOn() {
		return nil
	}
	return Deps().membershipMask(gvr)
}

func narrowUnknownMask() *eventMask {
	if !listNarrowShadowOn() {
		return nil
	}
	return unknownMask()
}

var (
	depWatchInstance *depWatch
	depWatchOnce     sync.Once
)

// depWatchSingleton returns the process-scoped bridge, lazily building it.
// Always non-nil.
func depWatchSingleton() *depWatch {
	depWatchOnce.Do(func() {
		// Plain allocation only — no queue, no goroutine (architect N1).
		depWatchInstance = &depWatch{}
	})
	return depWatchInstance
}

// depQueue boxes the typed workqueue so it can sit behind an atomic pointer
// (written once by startWorker, read by the worker, the stats snapshot and
// the test shim's stopWorker without a lock).
type depQueue struct {
	workqueue.TypedRateLimitingInterface[depEventKey]
}

// newDepQueue builds the dep-event queue. The backoff knobs are the
// REFRESHER's (design §3.3: one backoff shape in the subsystem). This is the
// ONLY site that spawns the workqueue's goroutines, and it runs from
// startWorker only.
func newDepQueue() *depQueue {
	baseMS := positiveIntFromEnv(envRefresherBaseDelayMS, defaultRefresherBaseDelayMS)
	maxMS := positiveIntFromEnv(envRefresherMaxDelayMS, defaultRefresherMaxDelayMS)
	rl := workqueue.NewTypedItemExponentialFailureRateLimiter[depEventKey](
		time.Duration(baseMS)*time.Millisecond,
		time.Duration(maxMS)*time.Millisecond,
	)
	return &depQueue{workqueue.NewTypedRateLimitingQueue[depEventKey](rl)}
}

// q returns the dep-event queue, or a nil interface before startWorker has
// run. Every worker-path caller runs after startWorker; the two callers that
// can run before it (DepWatchStatsSnapshot, stopWorker) nil-check.
func (w *depWatch) q() workqueue.TypedRateLimitingInterface[depEventKey] {
	if p := w.queue.Load(); p != nil {
		return p.TypedRateLimitingInterface
	}
	return nil
}

// startWorker builds the queue and spawns the single dep-event worker
// goroutine exactly once (sync.Once-bounded). Production never stops it —
// its lifetime is the process lifetime. The queue is built HERE, on the
// first real event, so that reading the bridge (stats, expvar, the OTLP
// mirror) never creates the thing it measures.
//
// startOnce is ALSO the stop guard (#393): the test-only stopWorker consumes
// it before it Waits, so a startWorker that has not run by then never runs —
// no workerWG.Add can follow the Wait. In production stopWorker is never
// called, so the first event always wins the Once and behaviour is unchanged.
func (w *depWatch) startWorker() {
	w.startOnce.Do(func() {
		w.queue.Store(newDepQueue())
		w.workerWG.Add(1)
		go w.runDepEventWorker(0)
	})
}

// maxDepWorkerRespawns bounds the supervisor re-spawn chain (1.12.5
// architect Finding 4). The re-spawn path is unreachable today — every event
// runs under handleDepEvent's recover — but if a future edit put a
// DETERMINISTIC panic in the loop body outside that recover, an uncapped
// supervisor would turn one silent death into a hot spawn-and-log loop.
const maxDepWorkerRespawns = 5

// runDepEventWorker is the worker body. Every dequeued coordinate is handled
// under handleDepEvent's own recover, so nothing in this loop can panic out;
// the deferred supervisor exists for a future edit that adds work OUTSIDE
// handleDepEvent. Ordering is load-bearing: workerWG.Done is registered
// FIRST so it runs LAST, so the re-spawn's Add(1) lands while the counter is
// still ≥1 (no zero-transition race against stopWorker's Wait).
func (w *depWatch) runDepEventWorker(respawns int) {
	workclass.ClearGoroutine() // #871: process-lifetime; never charged to the unit that started it
	defer w.workerWG.Done()
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		w.counters.workerPanics.Add(1)
		if respawns >= maxDepWorkerRespawns {
			slog.Error("deps.delete_worker.panic",
				slog.String("subsystem", "cache"),
				slog.Any("panic", rec),
				slog.String("site", "worker_loop"),
				slog.Int("respawns", respawns),
				slog.String("effect", "dep-event worker loop unwound OUTSIDE the per-event recover "+
					"more than the re-spawn cap allows — STAYING DOWN. Event-driven L1 "+
					"invalidation is degraded to TTL until restart."),
			)
			return
		}
		slog.Error("deps.delete_worker.panic",
			slog.String("subsystem", "cache"),
			slog.Any("panic", rec),
			slog.String("site", "worker_loop"),
			slog.Int("respawns", respawns),
			slog.String("effect", "dep-event worker loop unwound OUTSIDE the per-event recover; re-spawning"),
		)
		w.workerWG.Add(1)
		go w.runDepEventWorker(respawns + 1)
	}()
	for {
		item, shutdown := w.q().Get()
		if shutdown {
			return
		}
		func() {
			defer w.q().Done(item)
			w.handleDepEvent(item)
		}()
	}
}

// handleDepEvent processes ONE coordinate under its OWN recover: probe the
// informer, derive the action, hand it to the tracker. A panic is confined
// to this event: counted, WARN-logged with the coordinates, and the caller's
// drain loop proceeds to the next event.
//
// WARN, not ERROR: the chart ships LOG_LEVEL=warn, and this line is the one
// an operator greps for after a missed invalidation.
func (w *depWatch) handleDepEvent(k depEventKey) {
	w.beginProcessing(k)
	defer w.endProcessing(k)
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		w.counters.workerPanics.Add(1)
		w.q().Forget(k)
		slog.Warn("deps.delete_worker.panic",
			slog.String("subsystem", "cache"),
			slog.String("gvr", k.gvr.String()),
			slog.String("ns", k.namespace),
			slog.String("name", k.name),
			slog.Any("panic", rec),
			slog.String("site", "on_object_event"),
			slog.String("effect", "this coordinate's L1 action was LOST (stale until TTL, or until the "+
				"next event / reconcile enqueues it again); the worker survived and continues"),
		)
	}()

	// #603 PR-2 — this processing consumes every transition merged so far; one
	// merged after this point re-adds the coordinate with its own mask.
	mask := w.takeMask(k)
	if h := depWorkerTakenHookForTest.Load(); h != nil {
		(*h)(k.gvr, k.namespace, k.name) // TEST-ONLY: after takeMask, before the hook (#854 F5b)
	}
	state := w.watcher.Load().probeObjectState(k.gvr, k.namespace, k.name)
	switch state {
	case objExists:
		w.counters.probeExists.Add(1)
	case objAbsent:
		w.counters.probeAbsent.Add(1)
	default:
		// objUnknown — the indexer is not authoritative. Requeue with backoff
		// on the shared budget; on exhaustion degrade to a dirty-mark so the
		// refresher's re-fetch takes the decision against the apiserver.
		w.counters.probeUnknown.Add(1)
		if w.q().NumRequeues(k) < maxRefreshRequeues {
			w.mergeMask(k, mask) // the requeued processing still owns these transitions
			w.markQueued(k)
			w.q().AddRateLimited(k)
			return
		}
		w.counters.probeUnknownDegraded.Add(1)
		w.q().Forget(k)
		slog.Warn("deps.probe_unknown_degraded",
			slog.String("subsystem", "cache"),
			slog.String("gvr", k.gvr.String()),
			slog.String("ns", k.namespace),
			slog.String("name", k.name),
			slog.Int("requeues", maxRefreshRequeues),
			slog.String("effect", "informer not authoritative for this GVR for the whole requeue budget — "+
				"dirty-marking dependents so the refresher decides against the apiserver "+
				"(404 ⇒ evict at the drop point). Non-zero here means an informer is not recovering."),
			slog.String("hint", "the 404 leg of that refresh evicts at the drop point; a 403/500/timeout on it "+
				"leaves the cell resident and KNOWN-STALE (#547): served, counted on known_stale_served_total "+
				"and revalidated on the next read"),
		)
		Deps().onObjectEventMasked(k.gvr, k.namespace, k.name, objUnknownDegraded, mask)
		return
	}
	w.q().Forget(k)
	Deps().onObjectEventMasked(k.gvr, k.namespace, k.name, state, mask)
}

// submitDepEvent binds the watcher, starts the worker on first use and
// enqueues the coordinate. Never blocks, never drops: the typed workqueue is
// unbounded and deduplicates pending keys.
func (w *depWatch) submitDepEvent(rw *ResourceWatcher, k depEventKey, source string) {
	// #603 PR-2 — a submit that carries no transition (reconcile, relist bridge)
	// decides nothing: its mask is unknown, so every narrowed mark is kept.
	w.submitDepEventMask(rw, k, source, narrowUnknownMask())
}

// submitDepEventMask is submitDepEvent carrying the transition mask the
// handler computed (#603 PR-2 SHADOW). The mask is merged BEFORE the Add.
func (w *depWatch) submitDepEventMask(rw *ResourceWatcher, k depEventKey, source string, mask *eventMask) {
	w.watcher.Store(rw)
	w.startWorker()
	w.counters.eventsSubmitted.Add(1)
	// #239 — count the submitting mechanism PRE-dedup: the workqueue coalesces
	// same-coordinate submits across sources, so this is the only place "which
	// mechanism submits the most events" is soundly answerable.
	Deps().recordSubmitSource(source)
	q := w.q()
	if q == nil {
		// Only reachable after the test-only stopWorker consumed startOnce
		// before any event built the queue (#393): the bridge is stopped, so
		// the event is dropped — what an Add onto the shut-down queue does.
		// In production startWorker above always builds the queue.
		return
	}
	if q.ShuttingDown() {
		// #854 — an Add onto a shut-down queue is dropped: a lost event.
		w.counters.submitDroppedShutdown.Add(1)
		return
	}
	w.mergeMask(k, mask)
	w.markQueued(k)
	q.Add(k)
}

// stopWorker shuts the queue down, lets the worker drain what is already
// queued, and blocks until the goroutine has exited. Coordinates still
// waiting in the rate limiter's delay are dropped. Used by the _test.go
// shim; production code MUST NOT call it.
//
// #393: it first consumes startOnce. sync.Once returns from every Do only
// after the winning call's function has finished, so either (a) a concurrent
// startWorker already won — its queue.Store and workerWG.Add(1) happen-before
// this Do returns, so the queue is visible below and the Add is ordered
// before the Wait — or (b) this call wins with a no-op and no later
// startWorker can ever Add. Without this, an informer event racing teardown
// either Added concurrently with the Wait (the -race report), started a
// worker AFTER the Wait returned (a leaked goroutine processing the event
// the caller meant to lose), or Added between the nil q() load and the Wait
// (Wait blocked forever on a queue nobody shut down).
func (w *depWatch) stopWorker() {
	w.startOnce.Do(func() {})
	if q := w.q(); q != nil {
		q.ShutDownWithDrain()
	}
	w.workerWG.Wait()
}

// depEventHandlers builds the shared informer event-handler set wired by
// BOTH addResourceTypeLocked (dynamic full-Unstructured) and
// addResourceTypeMetadataOnlyLocked (PartialObjectMetadata). The handler
// bodies read ONLY (namespace, name) via metaNSName, so they are
// *bytesObject-safe by construction (Ship H5): every streamed object shape
// embeds ObjectMeta. WARNING for a future editor: a content read added here
// would NOT be *bytesObject-safe — decode via decodeBytesObject first.
//
// All three handlers ENQUEUE THE COORDINATE and return. The only per-type
// difference left is the ADD pre-sync gate (design §3.4 point 1), which must
// stay ADD-only and at the handler.
//
// This is the plain-func view for callers that drive the handlers directly.
// It carries no isInInitialList, so its ADD keeps the syncCh gate. The
// informer registration uses depEventHandler, whose ADD is gated on
// client-go's isInInitialList (#688).
func (rw *ResourceWatcher) depEventHandlers(gvr schema.GroupVersionResource) clientcache.ResourceEventHandlerFuncs {
	return rw.depEventHandler(gvr).funcs
}

// depAddDeliveryHookForTest, when non-nil, runs at the TOP of every dep ADD
// delivery (before any gate), and the func it returns runs when that delivery
// has been fully handled. TEST-ONLY: production leaves it nil and pays a
// single atomic load. The #688 arm uses it to hold the handler's initial-LIST
// replay until the store-level syncCh has closed (the lag client-go's
// per-handler processorListener has under load), so the late-replay window
// is deterministic instead of contention-bound.
var depAddDeliveryHookForTest atomic.Pointer[func(gvr schema.GroupVersionResource) func()]

// depEventHandler is the handler set the informer registration installs. It
// implements clientcache.ResourceEventHandler itself (not through
// ResourceEventHandlerFuncs, whose OnAdd discards the flag) so the ADD gate
// sees isInInitialList.
//
// #688 — WHY THE FLAG AND NOT ONLY syncCh. syncCh[gvr] closes when the
// INFORMER's HasSynced flips: the initial LIST has been popped into the
// store. client-go delivers to each handler through that handler's own
// processorListener goroutine, so this handler can still be replaying the
// initial LIST after syncCh has closed; the syncCh gate then reads a replayed
// ADD as live, which stamps the freshness clock and dirty-marks dependents
// of an object that did not change, at boot and on every relist. The flag is
// set per notification by client-go (the FIFO's "not yet synced" at pop
// time, and true for the replay a handler added to a running informer
// receives), so it is exact whatever the delivery lag. A relist through
// RemoveResourceType + EnsureResourceType builds a fresh informer whose LIST
// is again initial; a reflector relist on a LIVE informer is not initial, so
// an object that appeared during a watch gap still propagates as an ADD.
// The same flag already gates #685's RBAC pre-build note
// (rbacSnapshotHandler.OnAdd, rbac_subgen_epoch.go).
//
// The converse window has the same root. syncCh closes on a 50ms poll of
// HasSynced, so a LIVE ADD the handler receives after the store synced but
// before the poll closed syncCh was dropped as "pre-sync": a lost
// dirty-mark, and a freshness clock that never heard the event. With the
// flag as the gate it propagates.
//
// So for informer deliveries the flag alone decides; syncCh is only read to
// attribute: add_initial_list_past_sync counts replayed ADDs that syncCh
// would have admitted, add_live_pre_syncch counts live ADDs it would have
// dropped. Both are delivery timing, not faults. Initial-list drops still
// count in add_dropped_pre_sync. A nil syncCh on this path is still counted
// in add_nil_syncch with the one-shot WARN (O14 detection), but no longer
// gates. A plain-func caller (depEventHandlers) carries no flag and keeps
// the syncCh gate, including O14's nil-syncCh fail-safe drop.
type depEventHandler struct {
	gvr    schema.GroupVersionResource
	funcs  clientcache.ResourceEventHandlerFuncs
	add    func(obj interface{}, isInInitialList, fromInformer bool, source string)
	update func(oldObj, newObj interface{}, source string)
	del    func(obj interface{}, source string)
}

// #854 / #870 — the incremental list cells' comparator reads each key's last
// delivery from the live instance's dep-handler `seen` record, which the
// adapter (store_instance.go) updates AFTER this handler has submitted the
// event. So the comparator sees each key either undelivered (its indexer write
// is still in the client-go listener buffer, or this handler is still
// running), queued / processing in the dep worker, or pending in the cell — in
// flight in all three — and only otherwise as settled.
func (h depEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	h.add(obj, isInInitialList, true, dmSourceWatch)
}
func (h depEventHandler) OnUpdate(oldObj, newObj interface{}) {
	h.update(oldObj, newObj, dmSourceWatch)
}
func (h depEventHandler) OnDelete(obj interface{}) {
	h.del(obj, dmSourceWatch)
}

// #870 — a swap delivery (design v5 §2.7) is an ordinary live event for the
// dep handler, attributed to submission source store_swap.
func (h depEventHandler) onSwapAdd(obj any) { h.add(obj, false, true, dmSourceStoreSwap) }
func (h depEventHandler) onSwapUpdate(oldObj, newObj any) {
	h.update(oldObj, newObj, dmSourceStoreSwap)
}
func (h depEventHandler) onSwapDelete(obj any) { h.del(obj, dmSourceStoreSwap) }

func (rw *ResourceWatcher) depEventHandler(gvr schema.GroupVersionResource) depEventHandler {
	w := depWatchSingleton()
	w.watcher.Store(rw)
	// Ship 0.30.233 — pre-compute the CRD-meta-GVR predicate once per
	// handler-set construction, NOT per-event.
	crdSideEffect := IsCRDGVR(gvr)
	add := func(obj interface{}, isInInitialList, fromInformer bool, source string) {
		if h := depAddDeliveryHookForTest.Load(); h != nil {
			defer (*h)(gvr)()
		}
		if fromInformer {
			// #688 — client-go's flag is the gate. syncCh is read only to
			// attribute the two windows it used to get wrong.
			present, closed := rw.syncChState(gvr)
			if !present {
				// O14 detection only: the flag gates, so the ADD is not
				// dropped for a missing syncCh, but the missing allocation
				// is still a registration defect worth surfacing.
				w.noteNilSyncCh(gvr, "informer ADD with a nil syncCh — a registration path bypassed the syncCh "+
					"allocation (or the event outlived its GVR's teardown). The ADD is gated on isInInitialList "+
					"and is not affected; the registration is the defect.")
			}
			if isInInitialList {
				w.counters.addDroppedPreSync.Add(1)
				if present && closed {
					w.counters.addInitialListPastSync.Add(1)
				}
				return
			}
			if present && !closed {
				w.counters.addLivePreSyncCh.Add(1)
			}
		} else if !rw.addEventPostSync(gvr, w) {
			w.counters.addDroppedPreSync.Add(1)
			return
		}
		ns, name := metaNSName(obj)
		w.counters.addPropagated.Add(1)
		rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
		// #603 PR-2 — an ADD is a membership change.
		w.submitDepEventMask(rw, depEventKey{gvr: gvr, namespace: ns, name: name}, source, narrowMembershipMask(gvr))
		// Ship 0.30.233 — CRD-ADD discovery side-effect, off the informer
		// processor goroutine. See crd_discovery_side_effect.go.
		if crdSideEffect {
			crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleAdd)
		}
	}
	update := func(oldObj, newObj interface{}, source string) {
		ns, name := metaNSName(newObj)
		rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
		// #603 PR-2 SHADOW — the ONLY place the pre-image exists: compare it
		// with the new object on every live interest of the GVR, per
		// transition, before the queue coalesces it (read-only on both).
		w.submitDepEventMask(rw, depEventKey{gvr: gvr, namespace: ns, name: name}, source,
			narrowUpdateMask(gvr, oldObj, newObj))
		// Ship L / 0.30.246 — CRD UPDATE lifecycle hook.
		if crdSideEffect {
			crdDiscoverySingleton().submitCRDLifecycleEvent(newObj, crdLifecycleUpdate)
		}
	}
	del := func(obj interface{}, source string) {
		// DeletedFinalStateUnknown wraps the last-known object when the
		// watcher missed the explicit DELETE. Unwrap so we still get the
		// (ns, name) tuple AND the underlying CRD spec for the CRD-DELETE
		// teardown branch below.
		if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
			obj = tomb.Obj
		}
		ns, name := metaNSName(obj)
		rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
		// #603 PR-2 — a DELETE (or a tombstone) is a membership change.
		w.submitDepEventMask(rw, depEventKey{gvr: gvr, namespace: ns, name: name}, source, narrowMembershipMask(gvr))
		// Ship L / 0.30.246 — CRD DELETE lifecycle hook (triggerCRDDelete).
		if crdSideEffect {
			crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleDelete)
		}
	}
	return depEventHandler{gvr: gvr, add: add, update: update, del: del, funcs: clientcache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { add(obj, false, false, dmSourceWatch) },
		UpdateFunc: func(oldObj, newObj interface{}) { update(oldObj, newObj, dmSourceWatch) },
		DeleteFunc: func(obj interface{}) { del(obj, dmSourceWatch) },
	}}
}

// probeObjectState is the tri-state probe the dep-event worker derives its
// action from (design §3.3). It reads the watcher directly and has NO side
// effect — in particular it never calls EnsureResourceType, so asking about
// a GVR cannot register it.
//
//	objExists  — the informer for gvr is servable (registered AND synced AND
//	             watch not broken AND type confirmed) and its indexer holds
//	             the object.
//	objAbsent  — the informer is servable and its indexer does NOT hold the
//	             object ⇒ deleted.
//	objUnknown — nil watcher, passthrough mode (no informers; GetObject would
//	             do a LIVE apiserver GET, which this probe must never add),
//	             or the GVR is not servable: torn down (relist window), not
//	             yet synced, watch broken, type unconfirmed. The indexer is
//	             not authoritative either way.
//
// One lock acquisition covers the servability check and the indexer read
// off the same informer handle (the GetObject check-then-act discipline).
func (rw *ResourceWatcher) probeObjectState(gvr schema.GroupVersionResource, namespace, name string) objectState {
	if rw == nil || rw.mode == modePassthrough {
		return objUnknown
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return objUnknown
	}
	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}
	_, exists, err := gi.Informer().GetIndexer().GetByKey(key)
	if err != nil {
		return objUnknown
	}
	if exists {
		return objExists
	}
	return objAbsent
}

// addEventPostSync implements the R1 initial-replay gate + the O14 nil-
// syncCh fail-safe. Returns true iff an ADD for gvr should propagate to the
// DepTracker.
//
// The syncCh read is the lock-free
//
//	select { case <-syncCh: <closed→post-sync> default: <open→pre-sync> }
//
// idiom — a closed channel always selects, an open one never does.
// noteNilSyncCh is O14's detector: count, and WARN once per process. A
// registered GVR always has a syncCh, so a nil one at ADD time means a
// registration path skipped the allocation (or the event outlived its
// GVR's teardown). Shared by both ADD paths so the detector is live on the
// path production registers, not only on the plain-func view (#694 review).
func (w *depWatch) noteNilSyncCh(gvr schema.GroupVersionResource, hint string) {
	w.counters.addNilSyncCh.Add(1)
	if w.nilSyncWarned.CompareAndSwap(false, true) {
		slog.Warn("deps.add_nil_syncch",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", hint),
		)
	}
}

func (rw *ResourceWatcher) addEventPostSync(gvr schema.GroupVersionResource, w *depWatch) bool {
	rw.mu.RLock()
	ch := rw.syncCh[gvr]
	rw.mu.RUnlock()

	if ch == nil {
		// O14: nil syncCh — a registration path skipped the allocation.
		// Fail safe: drop (never propagate, never block). One-shot WARN.
		w.noteNilSyncCh(gvr, "AddFunc fired with a nil syncCh — treating ADD as pre-sync (drop). "+
			"A registration path bypassed the syncCh allocation; dep dirty-marks for this GVR are degraded to TTL.")
		return false
	}

	select {
	case <-ch:
		return true // closed → informer post-sync → propagate
	default:
		return false // open → initial LIST replay → drop
	}
}

// syncChState reports whether gvr has a syncCh and whether it is closed. No
// side effect (unlike addEventPostSync, which counts and warns on nil).
func (rw *ResourceWatcher) syncChState(gvr schema.GroupVersionResource) (present, closed bool) {
	rw.mu.RLock()
	ch := rw.syncCh[gvr]
	rw.mu.RUnlock()
	if ch == nil {
		return false, false
	}
	select {
	case <-ch:
		return true, true
	default:
		return true, false
	}
}

// DepWatchStats is a read-only snapshot of the bridge counters. Consumed by
// the resolved_cache.summary log, snowplow_deps (expvar + OTLP) and the
// falsifier arms.
type DepWatchStats struct {
	AddDroppedPreSync uint64
	AddPropagated     uint64
	AddNilSyncCh      uint64
	EventsSubmitted   uint64
	// AddInitialListPastSync (#688) is the subset of AddDroppedPreSync that
	// syncCh alone would have admitted: initial-LIST replay the handler
	// received after the store had synced. AddLivePreSyncCh is the subset of
	// AddPropagated that syncCh alone would have dropped: live ADDs received
	// before the 50ms poll closed syncCh.
	AddInitialListPastSync uint64
	AddLivePreSyncCh       uint64

	// DeleteWorkerPanics is the 1.12.5 H1 signature (kept under its 1.12.5
	// name — the worker is now the unified dep-event worker): non-zero means
	// at least one event's action was LOST. DeleteQueueDepth is the live
	// number of coordinates pending on the dep-event queue.
	DeleteWorkerPanics uint64
	DeleteQueueDepth   int

	ProbeExists          uint64
	ProbeAbsent          uint64
	ProbeUnknown         uint64
	ProbeUnknownDegraded uint64
}

// DepWatchStatsSnapshot returns the current bridge counters. Reading it never
// starts the worker and never builds the queue (only submitDepEvent does):
// before the first event the depth reads 0, which is the truthful answer for
// a bridge that exists and is idle. Under CACHE_ENABLED=false nothing ever
// submits, so a scrape stays goroutine-free (architect N1; arm OFF).
func DepWatchStatsSnapshot() DepWatchStats {
	w := depWatchSingleton()
	depth := 0
	if q := w.q(); q != nil {
		depth = q.Len()
	}
	return DepWatchStats{
		AddDroppedPreSync:      w.counters.addDroppedPreSync.Load(),
		AddPropagated:          w.counters.addPropagated.Load(),
		AddNilSyncCh:           w.counters.addNilSyncCh.Load(),
		EventsSubmitted:        w.counters.eventsSubmitted.Load(),
		AddInitialListPastSync: w.counters.addInitialListPastSync.Load(),
		AddLivePreSyncCh:       w.counters.addLivePreSyncCh.Load(),
		DeleteWorkerPanics:     w.counters.workerPanics.Load(),
		DeleteQueueDepth:       depth,
		ProbeExists:            w.counters.probeExists.Load(),
		ProbeAbsent:            w.counters.probeAbsent.Load(),
		ProbeUnknown:           w.counters.probeUnknown.Load(),
		ProbeUnknownDegraded:   w.counters.probeUnknownDegraded.Load(),
	}
}

// resetDepWatchForTest tears the bridge singleton down so each test sees a
// clean bridge (counters zeroed, worker stopped). Exported via the _test.go
// shim; production code MUST NOT call it.
func resetDepWatchForTest() {
	if depWatchInstance != nil {
		depWatchInstance.stopWorker()
	}
	depWatchInstance = nil
	depWatchOnce = sync.Once{}
}
