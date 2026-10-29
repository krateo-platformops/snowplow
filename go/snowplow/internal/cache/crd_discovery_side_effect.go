// crd_discovery_side_effect.go — Ship 0.30.233. The CRD-ADD
// discovery side-effect bridge.
//
// PURPOSE — restore the pre-Ship-0.5 invariant "a CRD ADD event
// drives discovery for the new CRD's group", in a SIMPLER form
// than the deleted CRD-watch backplane: ONE side-effect hook on
// the EXISTING customresourcedefinitions informer's AddFunc, NOT a
// separate informer.
//
// PRE-Ship-0.5 — discovery was driven by an in-process CRD
// informer + an AddFunc that called OnResourceTypeAvailable +
// register-resource-type. Ship 0.5 / 0.30.223 (v6) deleted that
// informer and routed discovery through the walker
// (lazyRegisterInnerCallPaths → DiscoverGroupResources). The TRACE
// in docs/ship-0.30.233-s4-cache-invalidation-trace-2026-06-02.md
// proved the walker-only chain has a stuck-zero-state race when a
// CRD is created at runtime: stage 1 of compositions-list serves
// the cached `crds` LIST result (which doesn't yet include the new
// CRD), stage 2 iterator is empty, the discovery hop is never
// reached for the new group, and the composition informer is
// never registered.
//
// Ship 0.30.233 fixes this by handing every CRD-ADD event to a
// bounded worker channel that calls cache.AddNavigationDiscoveredGroup
// + cache.DiscoverGroupResources for the new CRD's spec.group on a
// dedicated goroutine — OFF the informer processor goroutine.
//
// PM TIGHTENING #1 — bounded worker channel (NOT inline).
// DiscoverGroupResources does network hops (disco.ServerGroups +
// disco.ServerResourcesForGroupVersion); running it on the
// informer processor goroutine would stall ADD delivery for every
// other informer sharing that processor during the discovery hop
// (~tens of ms × N versions). The pattern mirrors deps_watch.go's
// pre-1.12.6 deleteEvictCh — single bounded worker (1.12.5: park-and-drain, never drop)
// with WARN log.
//
// PM TIGHTENING #2 — defer recover() inside triggerCRDDiscovery.
// The informer processor goroutine (or worker goroutine here)
// must NEVER panic-kill the pod under a malformed CRD object.
// The recover wrapper logs at error level with debug.Stack() so a
// regression surfaces in pod logs without taking the pod down.

package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// crdDiscoveryQueueDepth bounds the worker channel. 256 buffered
// slots is ample headroom for a realistic CRD-CREATE burst (a
// blueprint install creates ~10-30 CRDs in a few hundred ms; 256
// covers the largest customer scenario observed). A full channel
// falls back to drop-with-WARN — DiscoverGroupResources is per-
// group singleflighted and idempotent, so a dropped event for a
// group ALREADY being discovered is harmless; a dropped event for
// a NEW group means a delayed discovery (the next CRD ADD for
// that group, or the next walker pass, will retry).
const crdDiscoveryQueueDepth = 256

// crdDiscoveryEvent is one queued CRD-lifecycle event handed to the
// discovery worker. The bridge captures the informer-delivered object
// at enqueue time so the worker reads spec.* from the snapshot the
// informer delivered — no late reads against a mutating store.
type crdDiscoveryEvent struct {
	// obj is the CRD event payload. Production delivery shape post-Ship-H5
	// is *bytesObject (streaming-listwatch is the default for every dynamic
	// informer per watcher.go:1035-1047). The stock-informer fallback path
	// delivers *unstructured.Unstructured. triggerCRDDiscovery + the §3b
	// DELETE handler route both through decodeBytesObject
	// (bytesobject.go:394) — see Ship L / 0.30.246. DELETE events may
	// arrive wrapped in clientcache.DeletedFinalStateUnknown; the AddFunc/
	// UpdateFunc/DeleteFunc literal bodies in deps_watch.go unwrap that
	// shape BEFORE handing obj to submitCRDLifecycleEvent.
	obj interface{}
	// kind discriminates ADD vs UPDATE vs DELETE so the worker dispatches
	// to the right side-effect (Ship L / 0.30.246). UPDATE re-runs
	// discovery against the new spec; DELETE tears down the per-resource
	// informer + dirty-marks dependent L1.
	kind crdLifecycleKind
	// rw and gvr carry a crdLifecycleListRefused event (#807): the watcher
	// whose informer for gvr had its LIST/WATCH refused with 404. obj is nil
	// for that kind.
	rw  *ResourceWatcher
	gvr schema.GroupVersionResource
}

// crdLifecycleKind discriminates the three CRD events the bridge handles
// (Ship L / 0.30.246).
type crdLifecycleKind uint8

const (
	crdLifecycleAdd         crdLifecycleKind = iota // CRD CREATE
	crdLifecycleUpdate                              // CRD UPDATE (spec.versions[] / served[] / group changes)
	crdLifecycleDelete                              // CRD DELETE (group no longer served by the apiserver)
	crdLifecycleListRefused                         // #807: a data informer's LIST/WATCH was refused with 404
)

// crdDiscovery is the process-scoped CRD-ADD-side-effect bridge:
// counters + the worker channel + the worker-goroutine lifecycle.
// Mirrors depWatch (deps_watch.go) — sibling pattern, distinct
// state to keep the falsifier surface auditable.
type crdDiscovery struct {
	// queue is the bounded ADD-event channel. Drained by exactly
	// one worker goroutine spawned via startOnce.
	queue chan crdDiscoveryEvent

	startOnce sync.Once
	stopCh    chan struct{}
	workerWG  sync.WaitGroup

	// Counters — observability for the falsifier + ops dashboards.
	// All atomic for lock-free reads.
	eventsEnqueued     atomic.Uint64 // lifecycle events accepted into the queue (ADD + UPDATE + DELETE)
	eventsDropped      atomic.Uint64 // lifecycle events dropped (park deadline exceeded, or shutdown)
	eventsParked       atomic.Uint64 // 1.12.5 #187: submits that had to park on a full queue
	eventsProcessed    atomic.Uint64 // lifecycle events drained by the worker
	discoveryInvoked   atomic.Uint64 // ADD+UPDATE calls that reached DiscoverGroupResources
	discoverySkippedNG atomic.Uint64 // ADD+UPDATE calls skipped (no group / decode-fail / no SA rc)
	deletesProcessed   atomic.Uint64 // Ship L — DELETE calls that completed teardown (>=1 GVR torn down)
	deleteSkippedNG    atomic.Uint64 // Ship L — DELETE calls skipped (decode-fail / no plural / no served versions)
	panicsRecovered    atomic.Uint64 // recover-wrapper panic catches across all lifecycle handlers

	// CRD schema-widen relist (followup-crd-schema-widen-informer-relist).
	// schemaFingerprints maps CRD object name → the last-seen fingerprint of
	// its structural schema subtree (spec.versions[].{name,schema}). A
	// running data informer that LIST/WATCHed under a NARROWER structural
	// schema caches apiserver-PRUNED objects; widening the CRD at runtime
	// (e.g. adding spec.apiRef.extras + x-kubernetes-preserve-unknown-fields)
	// does NOT relist that informer (EnsureResourceType is registration-
	// idempotent, watcher.go:612), so the cache keeps serving pruned objects
	// until a manual pod bounce. We detect a real schema delta here and force
	// a per-GVR relist. Keyed by CRD name; written ONLY by the single worker
	// goroutine (processEvent serialises ADD/UPDATE/DELETE), so a plain map
	// guarded by the worker's single-threadedness is sufficient — but we use
	// sync.Map for defensiveness against a future multi-worker change and so
	// the test reset can clear it without a lock.
	schemaFingerprints sync.Map      // map[string]string (CRD name → schema fingerprint)
	schemaRelistsFired atomic.Uint64 // schema-change passes that requested >=1 store build (#870: built beside the live copy, swapped in)
	schemaUnchanged    atomic.Uint64 // ADD/UPDATE where the schema fingerprint was unchanged (no relist — thrash guard hit)
	// #218 — discovery-identity fingerprint (spec.group / names / scope /
	// per-version {served,storage,deprecated,status/scale subresource}): the served-GVR SHAPE the global
	// discovery memcache (:519) and the SA CachedDiscoveryClient→RESTMapper
	// (:534) are built from — NOT the schema (that's schemaFingerprints). Gates
	// the two discovery invalidations on the fail-safe union (schema OR discovery
	// changed) so an idle ~9.5min CRD re-list (UpdateFunc Sync, ResyncPeriod:0)
	// re-pays neither. Same single-worker-serialised sync.Map discipline.
	discoveryFingerprints sync.Map      // map[string]string (CRD name → discovery-identity fingerprint)
	crdDiscoveryNoop      atomic.Uint64 // ADD/UPDATE where NEITHER fingerprint changed → full no-op (the amplifier removed)
	// 1.12.7 F2 / #219 — per-GVR state torn down because the CRD stopped
	// serving that version. Non-zero is NORMAL on a cluster that upgrades
	// components (every upgrade mints a new API version and retires the
	// previous one); it is the number that should have been moving while
	// watch_broken climbed 4 -> 40 and never fell.
	staleVersionPruned atomic.Uint64
	// #807 (unserved_version_block.go) — servedVersions maps crdServedKey →
	// map[string]struct{} (the CRD's served version names, from its latest
	// ADD/UPDATE). Written and read only on the worker; sync.Map for the same
	// reasons as schemaFingerprints.
	servedVersions               sync.Map
	listRefusedChecks            atomic.Uint64 // 404 refusals the worker checked
	listRefusedDropped           atomic.Uint64 // 404 refusals not queued (queue full; the reflector reports again)
	listRefusedCRDUnknown        atomic.Uint64 // checked refusals with no CRD record — declined
	listRefusedStillServed       atomic.Uint64 // checked refusals whose version the CRD still serves — declined
	refusedVersionPruned         atomic.Uint64 // informers torn down and blocked on a refusal
	unservedRegistrationsRefused atomic.Uint64 // registrations refused because the version is blocked
	// #870 — the 1.12.5 post-sync re-fire counters (relist_dirtymark_postsync,
	// relist_postsync_timeout) and the 1.12.6 C2 relist-bridge counters
	// (relist_bridge_{runs,enqueued,timeout,aborted}) are DELETED with the
	// mechanisms they counted: a schema widen no longer tears the informer
	// down, so there is no teardown window to bridge. The swap's own
	// instruments are snowplow_store_swap (store_swap_stats.go).
}

var (
	crdDiscoveryInstance *crdDiscovery
	crdDiscoveryOnce     sync.Once
)

// crdDiscoverySingleton returns the process-scoped bridge, lazily
// constructing it on first access. Always non-nil.
func crdDiscoverySingleton() *crdDiscovery {
	crdDiscoveryOnce.Do(func() {
		crdDiscoveryInstance = &crdDiscovery{
			queue:  make(chan crdDiscoveryEvent, crdDiscoveryQueueDepth),
			stopCh: make(chan struct{}),
		}
	})
	return crdDiscoveryInstance
}

// startCRDDiscoveryWorker spawns the single worker goroutine
// exactly once (sync.Once-bounded). The worker drains the queue
// and invokes triggerCRDDiscovery per event OFF the informer
// processor goroutine. It exits on stopCh close (test cleanup);
// production never stops it — its lifetime is the process
// lifetime. startOnce doubles as the stop guard (#393): see
// stopCRDDiscoveryWorker.
func (c *crdDiscovery) startCRDDiscoveryWorker() {
	c.startOnce.Do(func() {
		c.workerWG.Add(1)
		go func() {
			defer c.workerWG.Done()
			workclass.ClearGoroutine() // #871: process-lifetime; never charged to the unit that started it
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("cache.crd_discovery.worker.panic",
						slog.String("subsystem", "cache"),
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
					)
				}
			}()
			for {
				select {
				case <-c.stopCh:
					// Drain queued events before exit so test
					// teardown is deterministic.
					for {
						select {
						case ev := <-c.queue:
							c.processEvent(ev)
						default:
							return
						}
					}
				case ev := <-c.queue:
					c.processEvent(ev)
				}
			}
		}()
	})
}

// processEvent is the worker's per-event entry point. Bumps the
// processed counter then dispatches on the event kind. The recover
// wrappers (PM tightening #2) are INSIDE each trigger* function so a
// single bad CRD object cannot crash the worker.
//
// Ship L (0.30.246): dispatches on crdLifecycleKind. ADD + UPDATE
// share the discovery code path (triggerCRDDiscovery); DELETE branches
// to triggerCRDDelete (wired in commit-4).
func (c *crdDiscovery) processEvent(ev crdDiscoveryEvent) {
	switch ev.kind {
	case crdLifecycleAdd, crdLifecycleUpdate:
		triggerCRDDiscovery(ev.obj, ev.kind)
	case crdLifecycleDelete:
		triggerCRDDelete(ev.obj)
	case crdLifecycleListRefused:
		c.handleListRefused(ev.rw, ev.gvr)
	default:
		// Ship L review note (#200) — defensive default for the closed
		// crdLifecycleKind enum. submitCRDLifecycleEvent is the only
		// enqueue site and always passes a named constant, so an unknown
		// kind here is structurally unreachable today. The default makes
		// that contract explicit: an out-of-range kind from a future
		// mis-wiring is logged (not silently no-op'd) while the worker
		// stays alive and the event still counts as processed below. No
		// side-effect fires — there is no safe default action for an
		// unknown lifecycle event.
		slog.Error("cache.crd_discovery.unknown_kind",
			slog.String("subsystem", "cache"),
			slog.String("kind", crdLifecycleKindString(ev.kind)),
			slog.String("hint", "processEvent received an out-of-range crdLifecycleKind — "+
				"submitCRDLifecycleEvent must always enqueue a named constant. "+
				"This indicates a wiring regression; the worker continues."),
		)
	}
	// Task #85: bump eventsProcessed AFTER the side-effect (discovery /
	// delete) completes, not before. "Processed" must mean "the
	// side-effect has run" — so eventsProcessed is a valid happens-before
	// signal for the side-effect's observable state (e.g.
	// navDiscoveredGroups via AddNavigationDiscoveredGroup at
	// triggerCRDDiscovery -> discovery_lookup.go:352). Pre-0.30.252 the
	// bump preceded the dispatch, so WaitCRDDiscoveryProcessedForTest
	// (which polls eventsProcessed) could return BEFORE
	// AddNavigationDiscoveredGroup ran, racing the assertions in
	// TestCRDAdd_TriggersGroupDiscovery (and siblings) under -count load.
	// The final per-event count is unchanged (still exactly one Add per
	// event), so every EventsProcessed==N assertion still holds; the only
	// difference is a sub-microsecond window where a dequeued event is not
	// yet counted — benign for the /debug/vars `events_processed` gauge,
	// and the gauge now reports fully-processed events, which is the more
	// honest semantics.
	c.eventsProcessed.Add(1)
}

// submitCRDLifecycleEvent enqueues a CRD lifecycle event (ADD / UPDATE
// / DELETE) onto the worker queue. Non-blocking with bounded buffer.
// Called from the deps_watch.go AddFunc/UpdateFunc/DeleteFunc when
// IsCRDGVR(gvr) is true.
//
// On full queue: drop + WARN + counter bump. DiscoverGroupResources
// is per-group singleflighted; a dropped event for an in-flight
// group is harmless. A dropped event for a NEW group means the
// next event for that group (or the next walker pass) retries.
//
// Ship L (0.30.246) — renamed from submitCRDDiscoveryEvent. The kind
// parameter lets the worker dispatch to ADD/UPDATE/DELETE paths.
func (c *crdDiscovery) submitCRDLifecycleEvent(obj interface{}, kind crdLifecycleKind) {
	c.startCRDDiscoveryWorker()
	ev := crdDiscoveryEvent{obj: obj, kind: kind}

	// Fast path: room in the queue, no parking, byte-identical to before.
	select {
	case c.queue <- ev:
		c.eventsEnqueued.Add(1)
		return
	default:
	}

	// 1.12.5 / #187 — PARK, DO NOT DROP.
	//
	// The pre-1.12.5 behaviour dropped the event here. The rationale on the
	// books was about ADD/UPDATE: DiscoverGroupResources is singleflighted
	// per-group, so a duplicate for an in-flight group is harmless. That
	// reasoning does not extend to DELETE, and DELETE is the event class #187
	// is about. A dropped CRD DELETE means triggerCRDDelete never runs: the
	// per-resource informer is never torn down and dependent L1 entries are
	// never dirty-marked, so every entry of that type is stale until TTL with
	// no further trigger. That is a live carrier for the #187 class, and it is
	// silent apart from one WARN.
	//
	// So a full queue now PARKS the caller until the worker makes room,
	// bounded by crdLifecycleParkMax. Parking blocks the informer processor
	// goroutine, which is a real cost — but the alternative is losing an
	// invalidation, and the queue only fills when the worker is already behind
	// by 256 events.
	//
	// NOT INLINE. Running processEvent here instead would put the
	// network-bound DiscoverGroupResources hop on the informer processor
	// goroutine, which is the exact thing the bounded-worker design exists to
	// prevent (PM tightening #1). Park against the worker seam; never bypass
	// it.
	//
	// The drop remains only as a last resort after the park deadline, so a
	// wedged worker cannot stall event delivery for the whole process
	// indefinitely. Reaching it means something is badly wrong and the WARN
	// says so.
	c.eventsParked.Add(1)
	slog.Warn("cache.crd_discovery.queue_parked",
		slog.String("subsystem", "cache"),
		slog.String("kind", crdLifecycleKindString(kind)),
		slog.Duration("park_max", crdLifecycleParkMax),
		slog.String("hint", "CRD lifecycle burst outran the discovery worker — parking the "+
			"informer processor goroutine rather than dropping the event; a dropped DELETE "+
			"would leave an informer un-torn-down and its dependent L1 entries stale to TTL."),
	)

	timer := time.NewTimer(crdLifecycleParkMax)
	defer timer.Stop()
	select {
	case c.queue <- ev:
		c.eventsEnqueued.Add(1)
	case <-c.stopCh:
		// Shutting down: the worker is going away, nothing will process this.
		c.eventsDropped.Add(1)
	case <-timer.C:
		c.eventsDropped.Add(1)
		slog.Error("cache.crd_discovery.event_dropped",
			slog.String("subsystem", "cache"),
			slog.String("kind", crdLifecycleKindString(kind)),
			slog.Duration("parked", crdLifecycleParkMax),
			slog.String("effect", "the discovery worker did not drain a single slot within the "+
				"park deadline — event DROPPED. For a DELETE this means the per-resource "+
				"informer is not torn down and its dependent L1 entries stay resident until "+
				"TTL (#187). Investigate the worker."),
		)
	}
}

// crdLifecycleParkMax bounds how long submitCRDLifecycleEvent will park the
// informer processor goroutine on a full queue before giving up and dropping
// the event (1.12.5 / #187). Long enough that any realistic burst drains —
// the worker's per-event work is a singleflighted discovery hop — and short
// enough that a wedged worker cannot stall event delivery forever.
const crdLifecycleParkMax = 30 * time.Second

// crdLifecycleKindString renders the enum as a human-readable label for
// log lines + WARNs. Closed-set; default falls back to "unknown" rather
// than panicking on an invalid value.
func crdLifecycleKindString(k crdLifecycleKind) string {
	switch k {
	case crdLifecycleAdd:
		return "ADD"
	case crdLifecycleUpdate:
		return "UPDATE"
	case crdLifecycleDelete:
		return "DELETE"
	case crdLifecycleListRefused:
		return "LIST_REFUSED"
	default:
		return "unknown"
	}
}

// stopCRDDiscoveryWorker closes the worker stop channel and blocks
// until the worker goroutine has exited (and drained pending
// events). Used by the _test.go shim; production code MUST NOT
// call it.
//
// #393: it first consumes startOnce, the same guard as depWatch.stopWorker.
// sync.Once returns from every Do only after the winning function finished,
// so either a concurrent startCRDDiscoveryWorker already won and its
// workerWG.Add(1) happens-before the Wait below, or this call wins with a
// no-op and no later start can Add from zero against the Wait. Events
// submitted after the stop stay in the buffer, unprocessed, which is what
// the pre-#393 shim also did once its (late-started) worker had drained and
// exited. Production never calls this, so its first event always wins the
// Once and starts the worker as before.
func (c *crdDiscovery) stopCRDDiscoveryWorker() {
	c.startOnce.Do(func() {})
	select {
	case <-c.stopCh:
		// already stopped
	default:
		close(c.stopCh)
	}
	c.workerWG.Wait()
}

// triggerCRDDiscovery is the actual side-effect: extract spec.group
// from the CRD object, add it to the navigation-discovered set,
// and invoke DiscoverGroupResources. Soft-fails on every error
// path — the recover wrapper (PM tightening #2) catches panics so
// a malformed CRD cannot kill the worker / pod.
//
// Identity invariants:
//   - The SA *rest.Config comes from ProcessSARestConfig (set once
//     at main.go startup). nil → soft-skip + counter bump.
//   - The CRD object decodes via decodeBytesObject (bytesobject.go:394).
//     Production delivery shape post-Ship-H5 is *bytesObject (streaming-
//     listwatch is the default per watcher.go:1035-1047); the stock-
//     informer fallback delivers *unstructured.Unstructured. Anything
//     else (PartialObjectMetadata, nil, etc.) → soft-skip.
//   - spec.group is read via unstructured.NestedString — empty /
//     missing / non-string soft-skips.
//
// ASYNC — runs on the discovery worker goroutine, NOT the informer
// processor. DiscoverGroupResources blocks on the apiserver
// discovery hop (~tens of ms); the worker queues serialize CRD
// events so concurrent CRD ADDs do not parallelise discovery hops
// (singleflight inside DiscoverGroupResources serialises per-group
// anyway; the worker queue serialises across groups too, which is
// fine for the realistic CRD-CREATE burst rate).
//
// Ship L (0.30.246): handles both crdLifecycleAdd and crdLifecycleUpdate.
// AddNavigationDiscoveredGroup is idempotent (discovery_lookup.go:87-102);
// DiscoverGroupResources is per-group singleflighted, so UPDATE re-firing
// is cheap when nothing has changed.
func triggerCRDDiscovery(obj interface{}, kind crdLifecycleKind) {
	c := crdDiscoverySingleton()

	// PM TIGHTENING #2: panic-recover wrapper. The informer
	// processor goroutine (via the worker) must never panic-kill
	// the pod under a malformed CRD object. Logs at error level
	// with debug.Stack() so a regression is visible in pod logs.
	defer func() {
		if rec := recover(); rec != nil {
			c.panicsRecovered.Add(1)
			slog.Error("cache.crd_discovery.trigger.panic_recovered",
				slog.String("subsystem", "cache"),
				slog.String("kind", crdLifecycleKindString(kind)),
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())),
				slog.String("hint", "triggerCRDDiscovery panicked on a CRD object — "+
					"continuing (the worker stays alive). Inspect the stack trace "+
					"to identify the malformed CRD shape."),
			)
		}
	}()

	// Ship L / 0.30.246 — decode-on-access. Post Ship H5 the
	// streaming-listwatch is the default for every dynamic informer
	// (watcher.go:1035-1047), so the customresourcedefinitions
	// informer delivers *bytesObject here, NOT
	// *unstructured.Unstructured. decodeBytesObject is the established
	// H5-aware decode dance: *bytesObject → fresh Unstructured via
	// .Decode(); *unstructured.Unstructured → returned as-is. Anything
	// else (PartialObjectMetadata, nil, etc.) → (nil, false) and we
	// soft-skip + one-shot WARN per unique go-type.
	u, ok := decodeBytesObject(obj)
	if !ok || u == nil {
		c.discoverySkippedNG.Add(1)
		warnOnceCRDDecodeSkip(obj, kind)
		return
	}

	group, found, err := unstructured.NestedString(u.Object, "spec", "group")
	if err != nil || !found || group == "" {
		c.discoverySkippedNG.Add(1)
		return
	}

	// Add to navigation-discovered set FIRST so the watcher's
	// removable-discriminator (watcher.go:749/:1064) sees the group as
	// nav-discovered when EnsureResourceType spawns the GVR informer — both
	// inside DiscoverGroupResources AND inside the schema-relist (the relist's
	// re-add MUST build a re-creatable STANDALONE informer, not a frozen
	// shared-factory one; the standalone path is gated on this group being
	// nav-discovered — watcher.go:1056). Idempotent + order-independent w.r.t.
	// discovery, so it runs up front, ahead of the SA-rc gate.
	AddNavigationDiscoveredGroup(group)

	// #807 — record the served set and lift the block on every version the
	// CRD serves. On EVERY ADD/UPDATE, ahead of the fingerprint gates below:
	// a re-served version must never stay blocked because an event was judged
	// a no-op.
	c.recordServedVersions(u)

	saRC := ProcessSARestConfig()
	if saRC == nil {
		c.discoverySkippedNG.Add(1)
		slog.Warn("cache.crd_discovery.no_sa_rc",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.String("hint", "SetProcessSARestConfig was not called at startup — "+
				"CRD-ADD discovery is degraded to walker-only. Check main.go wiring."),
		)
		// followup-crd-schema-widen-informer-relist — the data-informer relist
		// is INDEPENDENT of the SA rest config (it operates on already-
		// registered local informers + the WATCH each owns; no discovery hop).
		// Run it even on the degraded no-SA-rc path so a runtime schema widen
		// is never silently dropped. The F-4 schema-memo ordering (relist after
		// the invalidators) is moot here: with no SA-rc there is no
		// DiscoverGroupResources hop and the invalidators below do not run.
		c.triggerCRDSchemaRelist(u)
		return
	}

	// #218 — TWO-FINGERPRINT GATE. Before #218 the three invalidations below
	// fired UNCONDITIONALLY on every ADD/UPDATE, including the ~9.5min idle
	// re-list (UpdateFunc Sync, ResyncPeriod:0) → a cluster-wide discovery wipe +
	// schema recompile every 9.5min per CRD. Gate them on what actually changed:
	// the compare makes an ADD (no prior fp) and a real change fire, while an
	// idle re-list trips NEITHER → full no-op. Fail-safe: an unreadable fp reads
	// as CHANGED (invalidate, never leave a cache stale).
	//
	// The discovery pair (:519 memcache + :534 SA discovery/RESTMapper) fires on
	// the UNION (schema OR discovery changed): both wipe SHARED caches holding
	// discovery AND OpenAPI, so a schema change may invalidate their OpenAPI too —
	// fire on either (over-invalidate on a rare real schema change, never toward
	// staleness). The schema memo (:542) fires on schema alone.
	// schemaFingerprintChanged is LOAD-ONLY so triggerCRDSchemaRelist below
	// decides its relist against the SAME prior schema fingerprint.
	name := u.GetName()
	schemaChanged := c.schemaFingerprintChanged(name, crdSchemaFingerprint(u))
	discoveryChanged := c.discoveryFingerprintChangedAndStore(name, crdDiscoveryFingerprint(u))

	if schemaChanged || discoveryChanged {
		c.discoveryInvoked.Add(1)
		// Fix A2 — force-fresh: GLOBAL discovery memcache wipe + apiserver
		// re-read before the registration walk, so a CREATE/UPDATE never
		// registers against a stale cached read (S4/F-4 stuck-zero class).
		ctx := context.Background()
		if _, derr := DiscoverGroupResourcesFresh(ctx, saRC, group); derr != nil {
			slog.Warn("cache.crd_discovery.discover_group_failed",
				slog.String("subsystem", "cache"),
				slog.String("group", group),
				slog.Any("err", derr),
			)
		}
		// #322/#318-R2 — invalidate the SA cached discovery client AFTER
		// discovery (F-4 ordering), so the next ValidateObjectStatus rebuilds the
		// RESTMapper against the new served-GVR shape. Soft no-op when unwired.
		invalidateSADiscovery()
	}
	if schemaChanged {
		// #323/#318-R2 — reset the compiled-CRD-schema validation memo so the
		// next ValidateObjectStatus recompiles from fresh CRD bytes. Soft no-op
		// when unwired.
		invalidateCRDSchemaMemo()
	}
	if !schemaChanged && !discoveryChanged {
		// The idle re-list this fix exists for: neither the schema nor the
		// served-GVR shape moved, so nothing is invalidated. Counted so the
		// amplifier's removal is measurable on /debug/vars.
		c.crdDiscoveryNoop.Add(1)
	}

	// followup-crd-schema-widen-informer-relist — the invalidators above
	// refresh the DISCOVERY client + the compiled-schema VALIDATION memo, but
	// neither relists the running DATA informer's indexer. Under
	// CACHE_ENABLED=true objects.Get serves widget/entry-CR reads from that
	// indexer (objects/get.go:73-142), and apiserver structural-schema pruning
	// happens at LIST/WATCH time — so an informer that listed under a NARROWER
	// schema keeps serving PRUNED objects after the CRD is widened, until a
	// manual bounce. Detect a real structural-schema delta and relist the
	// affected GVRs. Schema-delta-gated so benign CRD churn (status/printer-
	// column patches) does NOT thrash informers. Ordered AFTER the invalidators
	// so the relisted informer's first reads see the fresh discovery + schema
	// state. Soft no-op when the schema is unchanged.
	c.triggerCRDSchemaRelist(u)
}

// crdServedGVRs derives the GroupVersionResource set for every SERVED
// version of a decoded CRD object. Shared by the DELETE teardown and the
// schema-widen relist. Returns nil when group / plural is empty or no
// served version exists (caller soft-skips). Mirrors the derivation in
// triggerCRDDelete + cache_mode.go:312-321 exactly (served-only).
func crdServedGVRs(u *unstructured.Unstructured) []schema.GroupVersionResource {
	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
	if group == "" || plural == "" {
		return nil
	}
	versions, found, err := unstructured.NestedSlice(u.Object, "spec", "versions")
	if err != nil || !found || len(versions) == 0 {
		return nil
	}
	var out []schema.GroupVersionResource
	for _, v := range versions {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(vm, "name")
		served, _, _ := unstructured.NestedBool(vm, "served")
		if name == "" || !served {
			continue
		}
		out = append(out, schema.GroupVersionResource{Group: group, Version: name, Resource: plural})
	}
	return out
}

// crdSchemaFingerprint computes a stable fingerprint of the structural-schema
// subtree that governs apiserver pruning: per served version, its name plus
// its `schema.openAPIV3Schema`. A change here is exactly the class that flips
// which fields survive LIST/WATCH (adding a property, flipping
// x-kubernetes-preserve-unknown-fields, etc.). Status/printer-column/
// conversion churn lives OUTSIDE this subtree, so it does NOT change the
// fingerprint — that is the thrash guard. Returns "" when the schema subtree
// cannot be read (caller treats "" as "unknown" → no relist; a later event
// with a readable schema will reconcile).
func crdSchemaFingerprint(u *unstructured.Unstructured) string {
	versions, found, err := unstructured.NestedSlice(u.Object, "spec", "versions")
	if err != nil || !found {
		return ""
	}
	// Build a deterministic [name, schema] projection. NestedSlice already
	// returns deep-copied plain Go values; json.Marshal of a
	// map[string]any sorts keys, so the encoding is canonical.
	type vfp struct {
		Name   string      `json:"name"`
		Schema interface{} `json:"schema"`
	}
	proj := make([]vfp, 0, len(versions))
	for _, v := range versions {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(vm, "name")
		sch, _, _ := unstructured.NestedMap(vm, "schema", "openAPIV3Schema")
		proj = append(proj, vfp{Name: name, Schema: sch})
	}
	b, mErr := json.Marshal(proj)
	if mErr != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// triggerCRDSchemaRelist relists the running data informers for a CRD whose
// structural schema changed since the last ADD/UPDATE we observed. No-op when
// the fingerprint is unchanged (the common case — most CRD UPDATEs are
// status/printer churn). Runs on the single discovery-worker goroutine
// (serialised with every other CRD lifecycle side-effect), AFTER the
// discovery + schema-memo invalidators.
//
// Mechanism per served GVR (#870): a build with reason schema
// (requestStoreBuild, store_swap.go). The replacement informer LISTs under the
// now-current schema BESIDE the live one; the GVR stays servable and keeps
// serving the pre-widen objects, known-stale and counted
// (store_swap_known_stale_*{reason=schema}), until the swap; the swap delivers
// exactly the objects whose bytes the widen changed (M) to the consumers, so a
// cached widget resolve recomputes against the unpruned objects. There is no
// teardown, no whole-type dirty-mark and no live-read fall-through (N5: one
// repair mechanism for every reason).
func (c *crdDiscovery) triggerCRDSchemaRelist(u *unstructured.Unstructured) {
	name := u.GetName()
	if name == "" {
		return
	}
	fp := crdSchemaFingerprint(u)
	if fp == "" {
		// Unreadable schema subtree — cannot decide a delta. Do not relist;
		// do not poison the stored fingerprint (leave any prior value so a
		// later readable event still detects the real change).
		return
	}
	prev, had := c.schemaFingerprints.Load(name)
	c.schemaFingerprints.Store(name, fp)
	if !had {
		// FIRST observation of this CRD's schema. Any data informer already
		// registered for its GVR listed under the apiserver's CURRENT schema —
		// which is exactly the fingerprint we just recorded — so there is no
		// stale-prune to correct. Record and return; the relist fires only on a
		// subsequent CHANGE. (This also avoids a spurious relist at startup,
		// when the CRD informer's initial replay delivers an ADD for every
		// pre-existing CRD.)
		return
	}
	if prev.(string) == fp {
		// Schema unchanged since we last saw this CRD — benign churn. This is
		// the thrash guard: a status/printer-column UPDATE lands here and
		// does NOT relist.
		c.schemaUnchanged.Add(1)
		return
	}
	// A real structural-schema CHANGE since we last observed this CRD — the
	// load-bearing case (a runtime widen of an already-watched CRD). Relist its
	// registered+served GVRs so the data informer re-LISTs under the new schema.
	gvrs := crdServedGVRs(u)
	rw := Global()
	// 1.12.7 F2 (#219) — PRUNE the per-GVR state of versions this CRD no
	// longer serves, BEFORE re-registering the ones it does.
	//
	// WHY HERE. Both RemoveResourceType call sites (this loop and
	// triggerCRDDelete) iterate crdServedGVRs(u) — the CRD's CURRENT served
	// versions — so a version already dropped from spec.versions can never be
	// named by either of them. Its informers / syncCh / confirmed /
	// watchBroken / informerStop / lastSyncRV entries and its dep edges then
	// persist for the life of the process, and its reflector keeps retrying a
	// LIST/WATCH against an API version the apiserver no longer serves. That
	// is the measured #219 leak: on krateo-057 watch_broken rose 4 -> 40 over
	// 24 h with ZERO decreases, and every latched GVR was a composition
	// version that an upgrade had replaced.
	//
	// A version replacement always changes the fingerprint (crdSchemaFingerprint
	// projects spec.versions[].name), so the relist pass is exactly where the
	// retirement is observable.
	c.pruneUnservedGVRs(rw, u, gvrs)
	if len(gvrs) == 0 {
		return
	}
	relisted := 0
	for _, gvr := range gvrs {
		if rw == nil {
			break
		}
		// #870 — a BUILD with reason schema (store_swap.go): the replacement
		// is listed under the new schema beside the live copy and swapped in;
		// the GVR stays servable, and only the objects the widen changed reach
		// the consumers (M). A build already running for this GVR is
		// SUPERSEDED, carrying its reasons and keys (F7). The request returns
		// at once: this loop runs on the single CRD-lifecycle worker.
		if !rw.IsRegistered(gvr) || !rw.requestStoreBuild(gvr, reasonSchema, nil) {
			continue
		}
		relisted++
	}
	if relisted > 0 {
		c.schemaRelistsFired.Add(1)
		slog.Info("cache.crd_discovery.schema_relist",
			slog.String("subsystem", "cache"),
			slog.String("crd", name),
			slog.Int("gvrs_relisted", relisted),
			slog.String("hint", "CRD structural schema changed at runtime — relisted the data "+
				"informer(s) so newly-permitted spec fields stop being served pruned from the "+
				"pre-change indexer (followup-crd-schema-widen-informer-relist)."),
		)
	}
}

// pruneUnservedGVRs tears down the per-GVR state of every registered GVR that
// belongs to THIS CRD (same group + plural) and is no longer in its served
// version set. 1.12.7 F2 / #219.
//
// SCOPED, DELIBERATELY. The candidate set is filtered to the CRD's own
// group+plural before anything is removed, so a global prune can never tear
// down a kind this CRD knows nothing about — including kinds that were
// registered lazily by the resolver and that no CRD event will ever name.
//
// CONSERVATIVE ON AN EMPTY SERVED SET. When served is empty we prune NOTHING.
// crdServedGVRs returns nil both for "this CRD serves no version" (vanishingly
// rare, and the DELETE path's job) and for "the versions subtree could not be
// read" — and those two are indistinguishable here. Pruning on an unreadable
// spec would tear down every live informer for the kind, so the empty case
// declines. A version REPLACEMENT, which is the measured leak, always leaves
// at least one served version and is therefore always covered.
//
// STOPPED, NOT FORGOTTEN. RemoveResourceType closes the per-GVR stop channel
// (watcher.go) before deleting the map entries, so the reflector goroutine
// exits rather than continuing to retry a LIST/WATCH against an API version
// the apiserver no longer serves. Dropping the map entries alone would leave
// that goroutine running for the life of the process.
//
// Runs on the single CRD-lifecycle worker goroutine, like every other
// side-effect in this file. rw.RegisteredGVRs takes rw.mu.RLock and
// RemoveResourceType takes rw.mu.Lock — sequentially, never nested.
func (c *crdDiscovery) pruneUnservedGVRs(rw *ResourceWatcher, u *unstructured.Unstructured, served []schema.GroupVersionResource) {
	if rw == nil || len(served) == 0 {
		return
	}
	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
	if group == "" || plural == "" {
		// Cannot scope the prune to this CRD — decline rather than guess.
		return
	}
	keep := make(map[schema.GroupVersionResource]struct{}, len(served))
	for _, g := range served {
		keep[g] = struct{}{}
	}
	var pruned []schema.GroupVersionResource
	for _, reg := range rw.RegisteredGVRs() {
		if reg.Group != group || reg.Resource != plural {
			continue
		}
		if _, ok := keep[reg]; ok {
			continue
		}
		pruned = append(pruned, reg)
	}
	if len(pruned) == 0 {
		return
	}
	for _, gvr := range pruned {
		// #807 — retired AND blocked: a resolver touching the old path must
		// not register it again, which is how a pruned version came back and
		// 404'd forever.
		rw.retireUnservedVersion(gvr, confirmRetractStaleVersionPruned)
	}
	c.staleVersionPruned.Add(uint64(len(pruned)))
	names := make([]string, 0, len(pruned))
	for _, gvr := range pruned {
		names = append(names, gvr.Version)
	}
	slog.Info("cache.crd_discovery.stale_version_pruned",
		slog.String("subsystem", "cache"),
		slog.String("crd", u.GetName()),
		slog.String("group", group),
		slog.String("resource", plural),
		slog.Any("versions", names),
		slog.String("hint", "the CRD stopped serving these versions; their informers, sync channels, "+
			"confirmation, watch-broken and last-sync state and dep edges are torn down. Before 1.12.7 "+
			"they persisted for the life of the process with a reflector retrying a dead API version, and "+
			"every dep event for those kinds degraded to a dirty-mark forever (#219)."),
	)
}

// triggerCRDDelete handles a CRD DELETE event: derive the GVRs that
// were served, tear down each per-resource informer via
// RemoveResourceType, and dirty-mark dependent L1 entries via
// OnResourceTypeRemoved (Ship L / 0.30.246, spec §3b).
//
// IDENTITY INVARIANTS — same shape as triggerCRDDiscovery:
//   - obj is decoded via decodeBytesObject (H5-aware). Production
//     delivery shape is *bytesObject; stock fallback is
//     *unstructured.Unstructured. Anything else soft-skips + WARN.
//   - spec.group + spec.names.plural + each served spec.versions[].name
//     produce the GVRs that need teardown — same derivation as
//     cache_mode.go:310-321.
//   - RemoveResourceType is idempotent (watcher.go:1292): unknown GVR
//     is a no-op. Safe under double-fire (DELETE storm).
//   - OnResourceTypeRemoved is no-op-on-empty (deps.go:716-722).
//
// FAILURE MODES (see spec §10.4):
//   - decodeBytesObject fails -> soft-skip + WARN. The informer keeps
//     running until it WATCH-404s and the controller-health snapshot
//     re-establishes via OnResourceTypeRemoved on the next sync.
//   - spec.versions[] is empty -> no GVR to tear down. Counter + skip.
//   - cache.Global() returns nil (test path) -> RemoveResourceType is
//     itself nil-receiver-safe (watcher.go:1318).
//
// NOTE: navDiscoveredGroups stays APPEND-ONLY on DELETE (per OQ1
// ratified — spec §11.2). The set's removable-discriminator predicate
// at watcher.go:749/:1074 is dead-code under the H5 streaming default
// in production, but the append-only contract keeps the contract
// surface bounded. Ship L+1 will retire the dead predicate use under
// task #196.
func triggerCRDDelete(obj interface{}) {
	c := crdDiscoverySingleton()

	defer func() {
		if rec := recover(); rec != nil {
			c.panicsRecovered.Add(1)
			slog.Error("cache.crd_discovery.delete.panic_recovered",
				slog.String("subsystem", "cache"),
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())),
				slog.String("hint", "triggerCRDDelete panicked on a CRD object — "+
					"continuing (the worker stays alive). Inspect the stack trace "+
					"to identify the malformed CRD shape."),
			)
		}
	}()

	u, ok := decodeBytesObject(obj)
	if !ok || u == nil {
		c.deleteSkippedNG.Add(1)
		warnOnceCRDDecodeSkip(obj, crdLifecycleDelete)
		return
	}

	// #218 — drop this CRD's fingerprints so a delete+recreate re-fires the
	// discovery/schema invalidations. A recreate reusing the identical spec would
	// otherwise match the stale fingerprint and no-op, leaving the discovery cache
	// / RESTMapper without the recreated GVR.
	c.dropCRDFingerprints(u.GetName())
	c.forgetServedVersions(u) // #807

	group, _, _ := unstructured.NestedString(u.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(u.Object, "spec", "names", "plural")
	if group == "" || plural == "" {
		c.deleteSkippedNG.Add(1)
		slog.Warn("cache.crd_discovery.delete.no_group_or_plural",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.String("plural", plural),
			slog.String("hint", "CRD DELETE event has empty spec.group or "+
				"spec.names.plural — cannot derive GVRs to tear down."),
		)
		return
	}

	versions, found, vErr := unstructured.NestedSlice(u.Object, "spec", "versions")
	if vErr != nil || !found || len(versions) == 0 {
		c.deleteSkippedNG.Add(1)
		slog.Warn("cache.crd_discovery.delete.no_versions",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.String("plural", plural),
			slog.String("hint", "CRD DELETE event has empty spec.versions[] — "+
				"nothing to tear down."),
		)
		return
	}

	rw := Global()
	torn := 0
	for _, v := range versions {
		vm, vok := v.(map[string]any)
		if !vok {
			continue
		}
		name, _, _ := unstructured.NestedString(vm, "name")
		served, _, _ := unstructured.NestedBool(vm, "served")
		if name == "" || !served {
			// Not-served versions had no informer wired (per
			// cache_mode.go:312-316); nothing to tear down.
			continue
		}
		gvr := schema.GroupVersionResource{
			Group:    group,
			Version:  name,
			Resource: plural,
		}
		if rw != nil {
			rw.removeResourceTypeWithReason(gvr, confirmRetractCRDDeleted) // idempotent, nil-safe
		}
		Deps().OnResourceTypeRemoved(gvr) // dirty-mark dependent L1
		torn++
	}

	c.deletesProcessed.Add(1)
	slog.Info("cache.crd_discovery.delete.processed",
		slog.String("subsystem", "cache"),
		slog.String("group", group),
		slog.String("plural", plural),
		slog.Int("gvrs_torn_down", torn),
	)

	// Task #322 (#318-R2) Commit 1 — invalidate the SA-singleton cached
	// discovery client AFTER teardown (RemoveResourceType +
	// OnResourceTypeRemoved), so the next ValidateObjectStatus for a
	// just-deleted GVR rebuilds the mapper WITHOUT the removed GVR
	// (KindFor then misses -> error returned to the caller, not a stale
	// positive). STRICTLY ordered after teardown (F-4 safety). Soft
	// no-op when the dynamic singleton is unwired
	// (discovery_invalidation_hook.go).
	invalidateSADiscovery()

	// Task #323 (#318-R2 Commit 2-B) — reset the per-GVR compiled-CRD-schema
	// memo (crds/schema) in lockstep, AFTER teardown, so the next
	// ValidateObjectStatus for a just-deleted GVR recompiles (and then misses
	// at the CRD GET -> error to the caller, not a stale-positive schema).
	// Soft no-op when the schema-memo invalidator is unwired.
	invalidateCRDSchemaMemo()

	// NOTE: navDiscoveredGroups stays append-only. See doc-comment
	// above + spec §11.2 OQ1 worked-examples deep-dive — the
	// predicate's "remove on DELETE" hazard is documentation-preserved
	// but dead-code under H5 streaming default. Ship L+1 / task #196
	// addresses the dead predicate.
}

// warnOnceCRDDecodeSkip emits a single WARN per unique go-type observed
// at the decode-skip path. Bounded by a sync.Map keyed on the type name
// so log volume is bounded by the number of distinct delivery shapes
// (1-2 in practice). Ship L / 0.30.246.
//
// The silent-skip behaviour of the pre-Ship-L bridge is what hid the
// 0.30.233 bytesObject regression for 13 ships. This WARN is the
// observability surface so any future routing change surfaces in pod
// logs the moment the new shape is observed — not 5 months later in a
// bench failure.
var crdDecodeSkipWarnedTypes sync.Map // map[string]struct{}

func warnOnceCRDDecodeSkip(obj interface{}, kind crdLifecycleKind) {
	typeName := goTypeOf(obj)
	if _, loaded := crdDecodeSkipWarnedTypes.LoadOrStore(typeName, struct{}{}); loaded {
		return
	}
	slog.Warn("cache.crd_discovery.decode_skipped",
		slog.String("subsystem", "cache"),
		slog.String("kind", crdLifecycleKindString(kind)),
		slog.String("got_type", typeName),
		slog.String("hint", "CRD lifecycle event arrived in an undecodable shape — "+
			"decodeBytesObject returned (nil,false). If got_type is *bytesObject, "+
			"the raw bytes are malformed (rare); otherwise decodeBytesObject "+
			"(bytesobject.go:394) needs a new case for this shape."),
	)
}

// CRDDiscoveryStats is a read-only snapshot of the CRD-discovery
// bridge counters. Consumed by the Ship 0.30.233 falsifier and the
// /debug/vars surface (followup #143).
//
// Ship L (0.30.246) added DeletesProcessed + DeleteSkippedNG for the
// CRD DELETE lifecycle path. Both fields stay zero in test fixtures
// that exercise only the ADD/UPDATE paths.
type CRDDiscoveryStats struct {
	// 1.12.6 C7: every published field carries its /debug/vars key as a
	// `stat` tag; expvar, the OTLP mirror, the docs guard and the parity arm
	// all DERIVE the key set from these tags (stats_by_tag.go). A field
	// without a tag fails TestC7_StatsByTag_EveryNumericFieldIsTaggedOrExcluded.
	EventsEnqueued     uint64 `stat:"events_enqueued"`
	EventsDropped      uint64 `stat:"events_dropped"`
	EventsParked       uint64 `stat:"events_parked"` // 1.12.5 #187: submits that parked on a full queue
	EventsProcessed    uint64 `stat:"events_processed"`
	DiscoveryInvoked   uint64 `stat:"discovery_invoked"`    // ADD + UPDATE (DiscoverGroupResources calls)
	DiscoverySkippedNG uint64 `stat:"discovery_skipped_ng"` // ADD + UPDATE decode-skip / no-group / no-SA-rc
	DeletesProcessed   uint64 `stat:"deletes_processed"`    // Ship L — successful DELETE teardowns
	DeleteSkippedNG    uint64 `stat:"delete_skipped_ng"`    // Ship L — DELETE decode-skip / no-served-versions / no-plural
	PanicsRecovered    uint64 `stat:"panics_recovered"`
	// followup-crd-schema-widen-informer-relist
	SchemaRelistsFired uint64 `stat:"schema_relists_fired"` // ADD/UPDATE passes that relisted >=1 GVR on a detected structural-schema change
	SchemaUnchanged    uint64 `stat:"schema_unchanged"`     // ADD/UPDATE where the schema fingerprint was unchanged (thrash guard hit; no relist)
	CRDDiscoveryNoop   uint64 `stat:"crd_discovery_noop"`   // #218 ADD/UPDATE where NEITHER the schema nor the discovery-identity fingerprint changed → nothing invalidated (the ~9.5min re-list amplifier removed; climbs ~1/CRD/re-list at steady state)
	// 1.12.7 F2 / #219
	StaleVersionPruned uint64 `stat:"stale_version_pruned_total"` // per-GVR state torn down because the CRD stopped serving that version
	// #807
	ListRefusedChecks            uint64 `stat:"list_refused_checks_total"`                    // 404 LIST/WATCH refusals checked against the CRD's served set
	ListRefusedDropped           uint64 `stat:"list_refused_dropped_total"`                   // 404 refusals not queued (worker queue full)
	ListRefusedCRDUnknown        uint64 `stat:"list_refused_crd_unknown_total"`               // refusals declined: no CRD record for the group+plural
	ListRefusedStillServed       uint64 `stat:"list_refused_still_served_total"`              // refusals declined: the CRD still serves the version
	RefusedVersionPruned         uint64 `stat:"refused_version_pruned_total"`                 // informers torn down and blocked because the 404'd version is not served
	UnservedRegistrationsRefused uint64 `stat:"unserved_version_registrations_refused_total"` // registrations refused for a blocked version
}

// crdDiscoveryStatsOverride, when set, replaces the live snapshot. TEST-ONLY
// seam for the 1.12.6 C7 parity arms, which set every tagged field to a
// distinct value and read it back through expvar and OTLP; nil in production.
var crdDiscoveryStatsOverride atomic.Pointer[CRDDiscoveryStats]

// SetCRDDiscoveryStatsForTest installs (or, with nil, clears) a snapshot
// override. Production callers MUST NOT use it.
func SetCRDDiscoveryStatsForTest(s *CRDDiscoveryStats) { crdDiscoveryStatsOverride.Store(s) }

// CRDDiscoveryStatsSnapshot returns the current bridge counters.
func CRDDiscoveryStatsSnapshot() CRDDiscoveryStats {
	if o := crdDiscoveryStatsOverride.Load(); o != nil {
		return *o
	}
	c := crdDiscoverySingleton()
	return CRDDiscoveryStats{
		EventsEnqueued:     c.eventsEnqueued.Load(),
		EventsDropped:      c.eventsDropped.Load(),
		EventsParked:       c.eventsParked.Load(),
		EventsProcessed:    c.eventsProcessed.Load(),
		DiscoveryInvoked:   c.discoveryInvoked.Load(),
		DiscoverySkippedNG: c.discoverySkippedNG.Load(),
		DeletesProcessed:   c.deletesProcessed.Load(),
		DeleteSkippedNG:    c.deleteSkippedNG.Load(),
		PanicsRecovered:    c.panicsRecovered.Load(),
		SchemaRelistsFired: c.schemaRelistsFired.Load(),
		SchemaUnchanged:    c.schemaUnchanged.Load(),
		CRDDiscoveryNoop:   c.crdDiscoveryNoop.Load(),
		StaleVersionPruned: c.staleVersionPruned.Load(),

		ListRefusedChecks:            c.listRefusedChecks.Load(),
		ListRefusedDropped:           c.listRefusedDropped.Load(),
		ListRefusedCRDUnknown:        c.listRefusedCRDUnknown.Load(),
		ListRefusedStillServed:       c.listRefusedStillServed.Load(),
		RefusedVersionPruned:         c.refusedVersionPruned.Load(),
		UnservedRegistrationsRefused: c.unservedRegistrationsRefused.Load(),
	}
}

// resetCRDDiscoveryForTest tears the singleton down so each test
// starts clean (counters zeroed, worker stopped). TEST-ONLY.
func resetCRDDiscoveryForTest() {
	if crdDiscoveryInstance != nil {
		crdDiscoveryInstance.stopCRDDiscoveryWorker()
	}
	crdDiscoveryInstance = nil
	crdDiscoveryOnce = sync.Once{}
}

// WaitCRDDiscoveryProcessedForTest blocks until at least `n`
// events have been processed by the worker, or `pollTimeoutMs`
// elapses. TEST-ONLY helper for the falsifier — the worker is
// async so the test cannot assert post-AddFunc state synchronously.
//
// Returns true on success, false on timeout.
func WaitCRDDiscoveryProcessedForTest(n uint64, pollTimeoutMs int) bool {
	c := crdDiscoverySingleton()
	deadline := time.Now().Add(time.Duration(pollTimeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		if c.eventsProcessed.Load() >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return c.eventsProcessed.Load() >= n
}
