// deps_watch.go — Ship A (0.30.110): the informer→DepTracker event
// bridge.
//
// Pre-0.30.110 every registration path (addResourceTypeLocked and
// addResourceTypeMetadataOnlyLocked) inlined a near-identical
// ResourceEventHandlerFuncs literal that wired UpdateFunc + DeleteFunc
// and deliberately LEFT AddFunc unwired. Ship A:
//
//   R1 — wires AddFunc on BOTH paths via the single shared builder
//        depEventHandlers. ADD is gated by the initial-replay gate:
//        propagate only once the GVR's syncCh is closed (post-sync);
//        drop (counter addDroppedPreSync) during the informer's initial
//        LIST replay, where every existing object arrives as an ADD and
//        re-dirty-marking the whole world would be pointless churn.
//
//   R3 — the DELETE self-eviction burst must not run inline on the
//        informer processor goroutine. DeleteFunc hands DepTracker.
//        OnDelete to a single bounded worker goroutine draining
//        deleteEvictCh; the processor goroutine returns immediately.
//
//   O14 — a nil syncCh at AddFunc time is a fail-safe "drop": one-shot
//        WARN, treat as pre-sync (never propagate, never block).

package cache

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// deleteEvictQueueDepth bounds the R3 DELETE-event handoff channel. Each
// buffered slot is one DELETE event; 1024 is ample headroom for a
// DELETE storm without unbounded memory. A full channel falls back to
// inline OnDelete (correctness over the R3 off-processor guarantee).
const deleteEvictQueueDepth = 1024

// depWatchCounters holds the Ship A informer-bridge falsifier counters.
// Process-scoped (the bridge wires one DepTracker singleton); atomic so
// they are readable without a lock.
type depWatchCounters struct {
	addDroppedPreSync atomic.Uint64 // ADD events dropped during initial replay
	addPropagated     atomic.Uint64 // ADD events propagated post-sync
	addNilSyncCh      atomic.Uint64 // ADD events seen with a nil syncCh (O14)
	deleteQueueFull   atomic.Uint64 // DELETE events run inline on a full queue

	// 1.12.5 / #187 H1 — DELETE events whose OnDelete panicked. Each one
	// is a SINGLE lost eviction (the worker survives and processes the
	// next event); pre-1.12.5 the first one killed DELETE handling
	// process-wide until restart. Non-zero is always a defect worth a
	// stack-trace hunt: the WARN deps.delete_worker.panic names the
	// offending (gvr, ns, name) and the panic value.
	deleteWorkerPanics atomic.Uint64
}

// depWatch is the process-scoped informer→DepTracker bridge state: the
// counters plus the R3 DELETE-eviction worker.
type depWatch struct {
	counters depWatchCounters

	// deleteEvictCh carries DELETE events to the worker goroutine.
	deleteEvictCh chan depDeleteEvent

	startOnce sync.Once
	stopCh    chan struct{}
	workerWG  sync.WaitGroup

	// nilSyncWarned ensures the O14 nil-syncCh WARN fires at most once.
	nilSyncWarned atomic.Bool
}

// depDeleteEvent is one queued DELETE handed to the eviction worker.
type depDeleteEvent struct {
	gvr       schema.GroupVersionResource
	namespace string
	name      string
}

var (
	depWatchInstance *depWatch
	depWatchOnce     sync.Once
)

// depWatchSingleton returns the process-scoped bridge, lazily building
// it. Always non-nil.
func depWatchSingleton() *depWatch {
	depWatchOnce.Do(func() {
		depWatchInstance = &depWatch{
			deleteEvictCh: make(chan depDeleteEvent, deleteEvictQueueDepth),
			stopCh:        make(chan struct{}),
		}
	})
	return depWatchInstance
}

// startDeleteWorker spawns the single DELETE-eviction worker goroutine
// exactly once (sync.Once-bounded). The worker drains deleteEvictCh and
// runs Deps().OnDelete OFF the informer processor goroutine. It exits on
// stopCh close (test cleanup); production never stops it — its lifetime
// is the process lifetime.
func (w *depWatch) startDeleteWorker() {
	w.startOnce.Do(func() {
		w.workerWG.Add(1)
		go w.runDeleteWorker(0)
	})
}

// maxDeleteWorkerRespawns bounds the supervisor re-spawn chain
// (architect Finding 4). The re-spawn path is unreachable today — every
// event runs under handleDeleteEvent's recover — but if a future edit
// put a DETERMINISTIC panic in the loop body outside that recover, an
// uncapped supervisor would turn one silent death into a hot
// spawn-and-log loop that burns a core and floods the log. After this
// many consecutive re-spawns the worker stays down and says so once,
// loudly: degraded DELETE handling is bad, a spinning core is worse, and
// a log line naming the cap is diagnosable.
const maxDeleteWorkerRespawns = 5

// runDeleteWorker is the worker body. Split out of startDeleteWorker at
// 1.12.5 (#187 H1) so the supervisor defer can re-spawn it.
//
// #187 H1 — THE FIX. Pre-1.12.5 the recover sat OUTSIDE the drain loop,
// so ONE panic anywhere under Deps().OnDelete unwound the whole
// goroutine. startOnce had already fired, so nothing re-spawned it: from
// that instant every DELETE in the process was enqueued to deleteEvictCh
// and never processed — no counter, no repeat log — until the 1024-slot
// buffer filled and submitDeleteEvent started falling back to inline
// OnDelete. DELETE-driven L1 invalidation was dead process-wide while
// ADD/UPDATE (which run inline on the processor goroutine) kept working:
// exactly the discriminator #187 reports.
//
// The recover now sits INSIDE the per-event call (handleDeleteEvent), so
// a panicking event costs exactly one lost eviction — logged at WARN with
// its (gvr, ns, name), counted on deleteWorkerPanics — and the drain
// continues with the next event.
//
// The defer below is a supervisor, not the primary guarantee: with every
// event handled under handleDeleteEvent's recover, nothing in the loop
// body can panic out. It exists so that a future edit which adds work to
// the loop OUTSIDE handleDeleteEvent cannot silently re-introduce the
// process-wide death. Ordering is load-bearing: workerWG.Done is
// registered FIRST so it runs LAST, which means the re-spawn's Add(1)
// happens while the WaitGroup counter is still ≥1 (no zero-transition
// race against stopDeleteWorker's Wait).
func (w *depWatch) runDeleteWorker(respawns int) {
	defer w.workerWG.Done()
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		w.counters.deleteWorkerPanics.Add(1)
		if respawns >= maxDeleteWorkerRespawns {
			slog.Error("deps.delete_worker.panic",
				slog.String("subsystem", "cache"),
				slog.Any("panic", rec),
				slog.String("site", "worker_loop"),
				slog.Int("respawns", respawns),
				slog.String("effect", "worker loop unwound OUTSIDE the per-event recover "+
					"more than the re-spawn cap allows — STAYING DOWN. DELETE-driven L1 "+
					"invalidation is degraded to TTL until restart; a deterministic panic "+
					"in the loop body is the only way to reach this."),
			)
			return
		}
		slog.Error("deps.delete_worker.panic",
			slog.String("subsystem", "cache"),
			slog.Any("panic", rec),
			slog.String("site", "worker_loop"),
			slog.Int("respawns", respawns),
			slog.String("effect", "worker loop unwound OUTSIDE the per-event recover; re-spawning"),
		)
		w.workerWG.Add(1)
		go w.runDeleteWorker(respawns + 1)
	}()
	for {
		select {
		case <-w.stopCh:
			// Drain queued events before exit so test teardown
			// is deterministic.
			for {
				select {
				case ev := <-w.deleteEvictCh:
					w.handleDeleteEvent(ev)
				default:
					return
				}
			}
		case ev := <-w.deleteEvictCh:
			w.handleDeleteEvent(ev)
		}
	}
}

// handleDeleteEvent runs Deps().OnDelete for one queued DELETE under its
// OWN recover (#187 H1). A panic is confined to this event: it is
// counted, WARN-logged with the offending coordinates, and the caller's
// drain loop proceeds to the next event.
//
// WARN, not ERROR: the chart ships LOG_LEVEL=warn, and this line is the
// one an operator greps for after a missed invalidation. It names the
// object whose eviction was lost, which the pre-1.12.5 ERROR line did
// not.
func (w *depWatch) handleDeleteEvent(ev depDeleteEvent) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		w.counters.deleteWorkerPanics.Add(1)
		slog.Warn("deps.delete_worker.panic",
			slog.String("subsystem", "cache"),
			slog.String("gvr", ev.gvr.String()),
			slog.String("ns", ev.namespace),
			slog.String("name", ev.name),
			slog.Any("panic", rec),
			slog.String("site", "on_delete"),
			slog.String("effect", "this object's L1 eviction was LOST (stale until TTL); "+
				"the worker survived and continues draining subsequent DELETEs"),
		)
	}()
	Deps().OnDelete(ev.gvr, ev.namespace, ev.name)
}

// submitDeleteEvent hands a DELETE event to the worker goroutine (R3).
// On a full queue it falls back to running OnDelete inline — correctness
// over the off-processor guarantee — and counts deleteQueueFull.
func (w *depWatch) submitDeleteEvent(ev depDeleteEvent) {
	w.startDeleteWorker()
	select {
	case w.deleteEvictCh <- ev:
	default:
		w.counters.deleteQueueFull.Add(1)
		slog.Warn("deps.delete_queue_full",
			slog.String("subsystem", "cache"),
			slog.String("gvr", ev.gvr.String()),
			slog.String("hint", "DELETE storm outran the eviction worker — running OnDelete inline"),
		)
		// 1.12.5 / #187 H1 — the inline fallback runs on the INFORMER
		// PROCESSOR goroutine. Route it through the same per-event recover:
		// an unguarded panic here would kill event delivery for the whole
		// informer, a strictly worse failure than the worker death H1
		// describes. Same counter, same WARN line (site=on_delete).
		w.handleDeleteEvent(ev)
	}
}

// stopDeleteWorker closes the worker stop channel and blocks until the
// worker goroutine has exited (and drained pending events). Used by the
// _test.go shim; production code MUST NOT call it.
func (w *depWatch) stopDeleteWorker() {
	select {
	case <-w.stopCh:
		// already stopped
	default:
		close(w.stopCh)
	}
	w.workerWG.Wait()
}

// depEventHandlers builds the shared informer event-handler set wired by
// BOTH addResourceTypeLocked (dynamic full-Unstructured) and
// addResourceTypeMetadataOnlyLocked (PartialObjectMetadata). The handler
// bodies are byte-identical across the two paths — metaNSName extracts
// (ns, name) via the nsNameAccessor interface that both shapes satisfy.
//
// Ship H5 — *bytesObject-SAFE BY CONSTRUCTION. Post the H5 routing
// inversion the streaming informers deliver *bytesObject to these
// handlers. That is safe WITHOUT change here: every handler reads ONLY
// (namespace, name) via metaNSName, and *bytesObject embeds ObjectMeta
// so it satisfies metaNSName's nsNameAccessor interface exactly as
// *unstructured.Unstructured and *PartialObjectMetadata do. The
// dep-tracker never reads object CONTENT — so it needs no decode-on-
// access. WARNING for a future editor: a content-assert added here
// (obj.(*unstructured.Unstructured), reading spec/status, etc.) would
// NOT be *bytesObject-safe and would silently drop every streamed
// object — it must decode via decodeBytesObject first.
//
// R1 — AddFunc IS wired here (pre-0.30.110 it was deliberately omitted).
// The initial-replay gate consults rw.syncCh[gvr]:
//
//   - syncCh closed (informer post-sync) → propagate the ADD to
//     Deps().OnAdd (dirty-mark LIST-scope dependents of the new object).
//   - syncCh open (initial LIST replay in flight) → DROP. Every existing
//     object arrives as an ADD during the initial replay; dirty-marking
//     each one is pointless churn and would storm the refresher at
//     startup. addDroppedPreSync counts the drops.
//   - syncCh nil (O14) → fail-safe DROP + one-shot WARN. A nil channel
//     means a registration path bypassed the syncCh allocation; we never
//     propagate (could storm) and never block (could deadlock).
func (rw *ResourceWatcher) depEventHandlers(gvr schema.GroupVersionResource) clientcache.ResourceEventHandlerFuncs {
	w := depWatchSingleton()
	// Ship 0.30.233 — pre-compute the CRD-meta-GVR predicate once
	// per handler-set construction, NOT per-event. The predicate is
	// a structural equality check (IsCRDGVR — see crd_gvr.go);
	// hoisting it here keeps the AddFunc hot path free of any
	// GVR-comparison cost for the 99% case where gvr is NOT the
	// CRD meta-GVR.
	crdSideEffect := IsCRDGVR(gvr)
	return clientcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if !rw.addEventPostSync(gvr, w) {
				w.counters.addDroppedPreSync.Add(1)
				return
			}
			ns, name := metaNSName(obj)
			w.counters.addPropagated.Add(1)
			Deps().OnAdd(gvr, ns, name)
			// Ship 0.30.233 — CRD-ADD discovery side-effect.
			// Dispatches to the bounded worker channel so the
			// network-bound DiscoverGroupResources hop runs OFF
			// the informer processor goroutine (PM tightening
			// #1). triggerCRDDiscovery has its own defer recover
			// (PM tightening #2) so a malformed CRD object cannot
			// panic-kill the worker or this processor goroutine.
			// See crd_discovery_side_effect.go.
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleAdd)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			ns, name := metaNSName(newObj)
			Deps().OnUpdate(gvr, ns, name)
			// Ship L / 0.30.246 — CRD UPDATE lifecycle hook. A CRD
			// UPDATE may add a new served version, retire one, or
			// change spec.group. Re-firing discovery on the NEW spec
			// is cheap (DiscoverGroupResources is per-group
			// singleflighted at discovery_lookup.go:228+) and
			// idempotent (AddNavigationDiscoveredGroup at
			// discovery_lookup.go:87-102 is a no-op for an already-
			// known group). The worker queue serialises ADD/UPDATE/
			// DELETE so order is deterministic.
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(newObj, crdLifecycleUpdate)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// DeletedFinalStateUnknown wraps the last-known object when
			// the watcher missed the explicit DELETE. Unwrap so we still
			// get the (ns, name) tuple AND the underlying CRD spec for
			// Ship L's CRD-DELETE teardown branch below.
			if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			ns, name := metaNSName(obj)
			// R3: hand off to the worker — never run the eviction burst
			// inline on this informer processor goroutine.
			w.submitDeleteEvent(depDeleteEvent{gvr: gvr, namespace: ns, name: name})
			// Ship L / 0.30.246 — CRD DELETE lifecycle hook. Tears down
			// the per-resource informer for every served version of the
			// deleted CRD via RemoveResourceType + dirty-marks dependent
			// L1 via OnResourceTypeRemoved. See triggerCRDDelete in
			// crd_discovery_side_effect.go for the teardown contract +
			// failure modes (spec §3b + §10.4).
			if crdSideEffect {
				crdDiscoverySingleton().submitCRDLifecycleEvent(obj, crdLifecycleDelete)
			}
		},
	}
}

// addEventPostSync implements the R1 initial-replay gate + the O14 nil-
// syncCh fail-safe. Returns true iff an ADD for gvr should propagate to
// the DepTracker.
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

// DepWatchStats is a read-only snapshot of the Ship A informer-bridge
// counters. Consumed by the resolved_cache.summary log + the AC tests.
type DepWatchStats struct {
	AddDroppedPreSync uint64
	AddPropagated     uint64
	AddNilSyncCh      uint64
	DeleteQueueFull   uint64

	// 1.12.5 / #187. DeleteWorkerPanics is the H1 signature — non-zero
	// means at least one DELETE eviction was LOST. DeleteQueueDepth is
	// the live occupancy of deleteEvictCh, sampled at snapshot time: a
	// depth pinned near deleteEvictQueueDepth is the pre-1.12.5 dead-
	// worker shape, and on 1.12.5 it means the drain is genuinely behind.
	DeleteWorkerPanics uint64
	DeleteQueueDepth   int
	DeleteQueueCap     int
}

// DepWatchStatsSnapshot returns the current bridge counters.
func DepWatchStatsSnapshot() DepWatchStats {
	w := depWatchSingleton()
	return DepWatchStats{
		AddDroppedPreSync:  w.counters.addDroppedPreSync.Load(),
		AddPropagated:      w.counters.addPropagated.Load(),
		AddNilSyncCh:       w.counters.addNilSyncCh.Load(),
		DeleteQueueFull:    w.counters.deleteQueueFull.Load(),
		DeleteWorkerPanics: w.counters.deleteWorkerPanics.Load(),
		DeleteQueueDepth:   len(w.deleteEvictCh),
		DeleteQueueCap:     cap(w.deleteEvictCh),
	}
}

// resetDepWatchForTest tears the bridge singleton down so each test sees
// a clean bridge (counters zeroed, worker stopped). Exported via the
// _test.go shim; production code MUST NOT call it.
func resetDepWatchForTest() {
	if depWatchInstance != nil {
		depWatchInstance.stopDeleteWorker()
	}
	depWatchInstance = nil
	depWatchOnce = sync.Once{}
}
