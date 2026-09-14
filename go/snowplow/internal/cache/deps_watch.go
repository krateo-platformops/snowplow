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

	// workerPanics — events whose processing panicked. Each one is a
	// SINGLE lost action (the worker survives and processes the next
	// event); pre-1.12.5 the first one killed DELETE handling process-wide
	// until restart. Published as delete_worker_panics_total for continuity
	// with the 1.12.5 operator procedure (the H1 signature).
	workerPanics atomic.Uint64

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
			slog.String("hint", "the 404 leg of that refresh resolves in seconds; a 403/500/timeout on it stays "+
				"bounded by RESOLVED_CACHE_TTL_SECONDS (3600 s) until 1.12.6 C4 extends drop-point eviction "+
				"to non-404 deterministic failures"),
		)
		Deps().OnObjectEvent(k.gvr, k.namespace, k.name, objUnknownDegraded)
		return
	}
	w.q().Forget(k)
	Deps().OnObjectEvent(k.gvr, k.namespace, k.name, state)
}

// submitDepEvent binds the watcher, starts the worker on first use and
// enqueues the coordinate. Never blocks, never drops: the typed workqueue is
// unbounded and deduplicates pending keys.
func (w *depWatch) submitDepEvent(rw *ResourceWatcher, k depEventKey) {
	w.watcher.Store(rw)
	w.startWorker()
	w.counters.eventsSubmitted.Add(1)
	w.q().Add(k)
}

// stopWorker shuts the queue down, lets the worker drain what is already
// queued, and blocks until the goroutine has exited. Coordinates still
// waiting in the rate limiter's delay are dropped. Used by the _test.go
// shim; production code MUST NOT call it.
func (w *depWatch) stopWorker() {
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
func (rw *ResourceWatcher) depEventHandlers(gvr schema.GroupVersionResource) clientcache.ResourceEventHandlerFuncs {
	w := depWatchSingleton()
	w.watcher.Store(rw)
	// Ship 0.30.233 — pre-compute the CRD-meta-GVR predicate once per
	// handler-set construction, NOT per-event.
	crdSideEffect := IsCRDGVR(gvr)
	return clientcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if !rw.addEventPostSync(gvr, w) {
				w.counters.addDroppedPreSync.Add(1)
				return
			}
			ns, name := metaNSName(obj)
			w.counters.addPropagated.Add(1)
			rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
			w.submitDepEvent(rw, depEventKey{gvr: gvr, namespace: ns, name: name})
			// Ship 0.30.233 — CRD-ADD discovery side-effect, off the informer
			// processor goroutine. See crd_discovery_side_effect.go.
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleAdd)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			ns, name := metaNSName(newObj)
			rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
			w.submitDepEvent(rw, depEventKey{gvr: gvr, namespace: ns, name: name})
			// Ship L / 0.30.246 — CRD UPDATE lifecycle hook.
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(newObj, crdLifecycleUpdate)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// DeletedFinalStateUnknown wraps the last-known object when the
			// watcher missed the explicit DELETE. Unwrap so we still get the
			// (ns, name) tuple AND the underlying CRD spec for the CRD-DELETE
			// teardown branch below.
			if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			ns, name := metaNSName(obj)
			rw.noteInformerEvent(gvr) // 1.12.5 #187 — freshness clock
			w.submitDepEvent(rw, depEventKey{gvr: gvr, namespace: ns, name: name})
			// Ship L / 0.30.246 — CRD DELETE lifecycle hook (triggerCRDDelete).
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleDelete)
			}
		},
	}
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
func (rw *ResourceWatcher) addEventPostSync(gvr schema.GroupVersionResource, w *depWatch) bool {
	rw.mu.RLock()
	ch := rw.syncCh[gvr]
	rw.mu.RUnlock()

	if ch == nil {
		// O14: nil syncCh — a registration path skipped the allocation.
		// Fail safe: drop (never propagate, never block). One-shot WARN.
		w.counters.addNilSyncCh.Add(1)
		if w.nilSyncWarned.CompareAndSwap(false, true) {
			slog.Warn("deps.add_nil_syncch",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("hint", "AddFunc fired with a nil syncCh — treating ADD as pre-sync (drop). "+
					"A registration path bypassed the syncCh allocation; dep dirty-marks for this GVR are degraded to TTL."),
			)
		}
		return false
	}

	select {
	case <-ch:
		return true // closed → informer post-sync → propagate
	default:
		return false // open → initial LIST replay → drop
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
		AddDroppedPreSync:    w.counters.addDroppedPreSync.Load(),
		AddPropagated:        w.counters.addPropagated.Load(),
		AddNilSyncCh:         w.counters.addNilSyncCh.Load(),
		EventsSubmitted:      w.counters.eventsSubmitted.Load(),
		DeleteWorkerPanics:   w.counters.workerPanics.Load(),
		DeleteQueueDepth:     depth,
		ProbeExists:          w.counters.probeExists.Load(),
		ProbeAbsent:          w.counters.probeAbsent.Load(),
		ProbeUnknown:         w.counters.probeUnknown.Load(),
		ProbeUnknownDegraded: w.counters.probeUnknownDegraded.Load(),
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
