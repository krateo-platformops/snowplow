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
}

// crdLifecycleKind discriminates the three CRD events the bridge handles
// (Ship L / 0.30.246).
type crdLifecycleKind uint8

const (
	crdLifecycleAdd    crdLifecycleKind = iota // CRD CREATE
	crdLifecycleUpdate                         // CRD UPDATE (spec.versions[] / served[] / group changes)
	crdLifecycleDelete                         // CRD DELETE (group no longer served by the apiserver)
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
	schemaRelistsFired atomic.Uint64 // relist passes that tore down >=1 GVR on a detected schema change
	schemaUnchanged    atomic.Uint64 // ADD/UPDATE where the schema fingerprint was unchanged (no relist — thrash guard hit)
	// 1.12.5 / #187 — post-sync re-fires of the relist dirty-mark. One per
	// relisted GVR whose new informer synced inside the timeout. A count
	// lagging schema_relists_fired means informers are not syncing after a
	// relist, which strands entries deleted in the teardown window.
	relistDirtyMarkPostSync atomic.Uint64
	relistPostSyncTimeout   atomic.Uint64

	// 1.12.6 C2 — the relist delta bridge (relist_bridge.go). runs counts
	// bridges spawned (one per relisted GVR); enqueued counts coordinates
	// synthesized from (old indexer keys \ fresh LIST); timeout counts
	// bridges whose fresh informer never synced inside the bound (the
	// bridge then enqueued nothing — the SOAK SIGNAL that keeps the 1.12.5
	// re-fire in place); aborted counts bridges with nothing to diff (no
	// sync channel, or the GVR removed again while waiting).
	relistBridgeRuns     atomic.Uint64
	relistBridgeEnqueued atomic.Uint64
	relistBridgeTimeout  atomic.Uint64
	relistBridgeAborted  atomic.Uint64
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
// lifetime.
func (c *crdDiscovery) startCRDDiscoveryWorker() {
	c.startOnce.Do(func() {
		c.workerWG.Add(1)
		go func() {
			defer c.workerWG.Done()
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
	default:
		return "unknown"
	}
}

// stopCRDDiscoveryWorker closes the worker stop channel and blocks
// until the worker goroutine has exited (and drained pending
// events). Used by the _test.go shim; production code MUST NOT
// call it.
func (c *crdDiscovery) stopCRDDiscoveryWorker() {
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

	c.discoveryInvoked.Add(1)

	// Fire-and-forget discovery hop. DiscoverGroupResources is
	// per-group singleflighted (discovery_lookup.go:228-232) and
	// idempotent (EnsureResourceType is itself singleflighted via
	// rw.mu). Soft-fails on apiserver errors (warn-logged inside
	// DiscoverGroupResources at discovery_lookup.go:255-258 +
	// :270-275).
	ctx := context.Background()
	// Fix A2 — the CRD-event path MUST force-fresh: Invalidate the cached
	// discovery surface (a GLOBAL memcache wipe — all groups) and re-read
	// the apiserver BEFORE the registration walk, so a CREATE/UPDATE never
	// registers against a stale cached read (the S4/F-4 stuck-zero
	// regression class). The hot /call walker keeps the cached/short-
	// circuit DiscoverGroupResources.
	if _, derr := DiscoverGroupResourcesFresh(ctx, saRC, group); derr != nil {
		slog.Warn("cache.crd_discovery.discover_group_failed",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.Any("err", derr),
		)
	}

	// Task #322 (#318-R2) Commit 1 — invalidate the SA-singleton cached
	// discovery client AFTER DiscoverGroupResources, so the next
	// ValidateObjectStatus for the new/changed GVR rebuilds the mapper
	// and sees the new CRD's schema. STRICTLY ordered after discovery
	// (F-4 safety): a stale discovery cache cannot persist past this CRD
	// ADD/UPDATE. Soft no-op when the dynamic singleton is unwired
	// (discovery_invalidation_hook.go).
	invalidateSADiscovery()

	// Task #323 (#318-R2 Commit 2-B) — reset the per-GVR compiled-CRD-schema
	// memo (crds/schema) in lockstep with the discovery cache, AFTER
	// DiscoverGroupResources, so the next ValidateObjectStatus for the
	// new/changed GVR recompiles from fresh CRD bytes (a CRD UPDATE that
	// changes the schema MUST invalidate; this is that path). Soft no-op when
	// the schema-memo invalidator is unwired (discovery_invalidation_hook.go).
	invalidateCRDSchemaMemo()

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
// Mechanism per served GVR: RemoveResourceType (tears down the stale informer
// via the R6 per-GVR stop channel, watcher.go:1406) then EnsureResourceType
// (re-registers → fresh LIST under the now-current schema), and
// OnResourceTypeSchemaRelisted to dirty-mark L1 entries depending on that GVR
// so a cached widget resolve recomputes against the now-unpruned objects
// (same dirty-mark-only set as the DELETE path's OnResourceTypeRemoved, but
// it logs cache_event.consumed type=SCHEMA_RELIST, not CRD_DELETE). Between
// teardown and the new informer's HasSynced, objects.Get falls through to the
// apiserver under the 0.30.97 IsServable guard (correct, just slower).
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
	if len(gvrs) == 0 {
		return
	}
	rw := Global()
	relisted := 0
	for _, gvr := range gvrs {
		if rw == nil {
			break
		}
		// Only relist a GVR we are actually watching. EnsureResourceType is
		// registration-idempotent, so an unconditional Ensure would SPAWN an
		// informer for a never-watched GVR (wrong — lazy registration is the
		// resolver's job). Gate on current registration via IsRegistered.
		if !rw.IsRegistered(gvr) {
			continue
		}
		// 1.12.6 C2 — snapshot the OLD indexer's key set BEFORE the teardown
		// (relist_bridge.go). Read-only; the diff against the fresh LIST runs
		// on the bridge goroutine spawned below.
		before, had := rw.IndexerKeys(gvr)
		rw.RemoveResourceType(gvr)               // R6 per-GVR teardown; idempotent, nil-safe
		_, syncCh := rw.EnsureResourceType(gvr)  // re-register → fresh LIST under current schema
		Deps().OnResourceTypeSchemaRelisted(gvr) // dirty-mark dependent L1 (logs SCHEMA_RELIST, not CRD_DELETE)
		// 1.12.5 / #187 — RE-FIRE THE DIRTY-MARK AFTER THE NEW INFORMER SYNCS.
		//
		// The teardown above opens a window in which DELETEs are lost with no
		// trace at any log level. RemoveResourceType closes the old informer's
		// per-GVR stop channel and purges its state, so anything still in its
		// DeltaFIFO or in flight on its watch is dropped with no handler run.
		// The replacement informer is freshly constructed, so its knownObjects
		// indexer is EMPTY — DeltaFIFO.Replace() has nothing to diff against
		// and synthesises NO Deleted delta for an object that vanished before
		// the new LIST. The object simply never appears.
		//
		// The dirty-mark on the line above does cover entries whose objects
		// were ALREADY gone: OnResourceTypeSchemaRelisted walks the forward dep
		// index, not the indexer, so an absent object is still matched. But it
		// runs ONCE, at the START of the window. An entry dirty-marked at that
		// instant re-resolves SUCCESSFULLY (its object still exists), survives,
		// and is then stranded when the delete lands a moment later with no
		// future trigger of any kind. That is the #187 burst shape exactly:
		// relist at ~09:21, deletes 09:21-09:24.
		//
		// Re-firing the same dirty-mark once the new informer has synced closes
		// it. Every stranded entry is re-resolved, its own object 404s, and the
		// drop-point eviction removes it. Deliberately reuses the existing
		// dirty-mark rather than adding a store walk: no RangeMetadata, no
		// lock-order hazard between the store mutex and rw.mu, no new
		// invalidation semantics. The pre-sync fire is KEPT — both run, and the
		// dirty-mark is idempotent.
		//
		// Off the discovery worker goroutine: the relist runs on the single
		// CRD-lifecycle worker, so blocking it on a sync channel would stall
		// every other CRD event. Bounded by a timeout so a GVR that never syncs
		// cannot leak the goroutine for the process lifetime.
		//
		// WORTHLESS WITHOUT THE SELF-404 EVICTION. On its own this just re-runs
		// the five-requeue drop. It is the delivery half; the eviction is the
		// other half.
		// Accounted on workerWG so the bridge's stop path WAITS for it. The
		// goroutine touches the Deps() singleton after it wakes, and an
		// untracked goroutine outliving its bridge would read that singleton
		// while a teardown is replacing it.
		// The tracker handle is captured HERE, on the worker goroutine, not
		// read from the Deps() singleton after the wait. The goroutine
		// outlives this call by design, and a later read of the global would
		// race anything that replaces the singleton (test teardown does
		// exactly that). Capturing also makes the goroutine's dependency
		// explicit rather than ambient.
		c.workerWG.Add(1)
		go c.refireRelistDirtyMarkAfterSync(Deps(), gvr, syncCh)
		// 1.12.6 C2 — the delta bridge (relist_bridge.go). ADDITIVE to the
		// re-fire above (PM condition C10): the re-fire stays until the bridge
		// has soaked with relist_bridge_timeout_total at zero. Same goroutine
		// discipline: off the discovery worker, on workerWG, bounded wait.
		// The dep-event bridge handle is captured HERE, on the worker
		// goroutine, for the same reason Deps() is captured above: the
		// goroutine outlives this call and must not read a process singleton
		// after its wait (depEventHandlers captures it the same way).
		if had {
			c.workerWG.Add(1)
			go c.bridgeRelistDeletes(rw, depWatchSingleton(), gvr, before, syncCh)
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

// relistPostSyncTimeout bounds how long the post-sync re-fire goroutine
// waits for a relisted informer to sync before giving up (1.12.5 / #187).
// A relisted informer that has not synced in two minutes is not going to;
// waiting longer only holds the goroutine. The give-up is counted and
// logged, because it means entries deleted inside that GVR's teardown
// window stay stranded until their TTL.
const relistPostSyncWait = 2 * time.Minute

// refireRelistDirtyMarkAfterSync waits for a relisted GVR's replacement
// informer to finish its initial LIST, then re-fires the dependent
// dirty-mark. See the call site for why the single pre-sync fire is not
// enough.
//
// Runs on its own goroutine: the relist loop executes on the single
// CRD-lifecycle worker, and blocking there would stall every other CRD
// event. Panic-guarded for the same reason every other handler here is —
// a goroutine panic takes the process down, and this one is spawned from
// an informer-driven path.
func (c *crdDiscovery) refireRelistDirtyMarkAfterSync(d *DepTracker, gvr schema.GroupVersionResource, syncCh <-chan struct{}) {
	defer c.workerWG.Done()
	defer func() {
		if rec := recover(); rec != nil {
			c.panicsRecovered.Add(1)
			slog.Error("cache.crd_discovery.relist_postsync.panic",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())),
			)
		}
	}()

	if syncCh == nil {
		// EnsureResourceType returned no channel (passthrough / nil watcher).
		// Nothing to wait for and nothing useful to re-mark.
		return
	}

	timer := time.NewTimer(relistPostSyncWait)
	defer timer.Stop()

	select {
	case <-syncCh:
	case <-c.stopCh:
		return
	case <-timer.C:
		c.relistPostSyncTimeout.Add(1)
		slog.Warn("cache.crd_discovery.relist_postsync.timeout",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.Duration("waited", relistPostSyncWait),
			slog.String("effect", "the relisted informer never synced, so the post-sync "+
				"dirty-mark did not run — L1 entries whose objects were deleted inside the "+
				"relist teardown window stay resident until TTL (#187)"),
		)
		return
	}

	// Re-check the stop signal: the bridge may have been torn down while we
	// waited, and a dirty-mark issued after shutdown is pure noise.
	select {
	case <-c.stopCh:
		return
	default:
	}
	d.OnResourceTypeSchemaRelisted(gvr)
	c.relistDirtyMarkPostSync.Add(1)
	slog.Info("cache.crd_discovery.relist_postsync.dirty_marked",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("hint", "re-marked dependent L1 entries after the relisted informer synced; "+
			"an entry whose object was deleted inside the teardown window gets no informer "+
			"DELETE (a fresh informer synthesises none) and this is its only trigger (#187)"),
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
			rw.RemoveResourceType(gvr) // idempotent, nil-safe
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

	// 1.12.5 / #187
	RelistDirtyMarkPostSync uint64 `stat:"relist_dirtymark_postsync_total"` // post-sync re-fires of the relist dirty-mark
	RelistPostSyncTimeout   uint64 `stat:"relist_postsync_timeout_total"`   // relisted GVRs whose new informer did not sync in time

	// 1.12.6 C2 — relist delta bridge (relist_bridge.go)
	RelistBridgeRuns     uint64 `stat:"relist_bridge_runs_total"`     // bridges spawned (one per relisted GVR with a registered indexer)
	RelistBridgeEnqueued uint64 `stat:"relist_bridge_enqueued_total"` // coordinates synthesized from (old indexer keys \ fresh LIST)
	RelistBridgeTimeout  uint64 `stat:"relist_bridge_timeout_total"`  // bridges that gave up because the fresh informer never synced — the soak signal
	RelistBridgeAborted  uint64 `stat:"relist_bridge_aborted_total"`  // bridges with nothing to diff (no sync channel / GVR removed while waiting)
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

		RelistDirtyMarkPostSync: c.relistDirtyMarkPostSync.Load(),
		RelistPostSyncTimeout:   c.relistPostSyncTimeout.Load(),

		RelistBridgeRuns:     c.relistBridgeRuns.Load(),
		RelistBridgeEnqueued: c.relistBridgeEnqueued.Load(),
		RelistBridgeTimeout:  c.relistBridgeTimeout.Load(),
		RelistBridgeAborted:  c.relistBridgeAborted.Load(),
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
