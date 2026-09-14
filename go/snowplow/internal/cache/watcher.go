package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatainformer"
	"k8s.io/client-go/rest"
	clientcache "k8s.io/client-go/tools/cache"
)

// listPageLimit is the chunk size used by every informer LIST call.
// Bounded paging keeps the apiserver from streaming an unbounded
// response — matches the policy from earlier ResourceWatcher iterations
// (Q-OOM-WARMER, ship/0.25.320).
const listPageLimit int64 = 500

// listOptionsTweak is the single bounded-paging TweakListOptionsFunc
// applied to EVERY informer LIST — shared factories AND the R6
// (0.30.115) standalone per-GVR informers. Centralising it guarantees
// the standalone informers carry byte-identical paging policy to the
// factory-built ones (the architect's "same tweakListOptions" rule).
func listOptionsTweak(opts *metav1.ListOptions) {
	opts.Limit = listPageLimit
}

// watcherMode discriminates the two ResourceWatcher operational modes
// introduced at 0.30.71 ("extended CACHE_ENABLED"):
//
//   - modeInformer (CACHE_ENABLED=true, operational/production): the
//     factory is constructed, RBAC GVRs are eager-registered, the
//     SetTransform typed-RBAC pipeline runs, and every Get/List is
//     served from the informer indexer in O(1).
//
//   - modePassthrough (CACHE_ENABLED=false, diagnostic/measurement):
//     NO factory, NO informers, NO goroutines. Every Get/List call
//     reaches apiserver via the dynamic client. This is the "true
//     cache-off" baseline used to measure the L1+typed-RBAC+informer
//     stack's compound effect on warm-p50 latency. Operational
//     customers MUST NOT run in this mode (informer cache savings
//     vanish; apiserver pressure spikes).
type watcherMode int

const (
	modeInformer    watcherMode = 0 // cache=on, factory-backed
	modePassthrough watcherMode = 1 // cache=off + dyn provided, apiserver-routed
)

// RBACResourceTypes is the eager-registered Role-Based Access Control
// resource-type set (0.30.4 binding, plan §"Tag 0.30.4 What's implemented"
// bullet 1). The four GVRs are eagerly informer-registered by
// NewResourceWatcher when CACHE_ENABLED=true so EvaluateRBAC can serve
// in-process Role-Based Access Control decisions without ever calling
// SubjectAccessReview against apiserver (Revision 1 binding).
//
// Per feedback_no_special_cases.md: NO per-resource policy lives in this
// set — it is the bare minimum required by EvaluateRBAC.
var RBACResourceTypes = []schema.GroupVersionResource{
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"},
	{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"},
}

// ResourceWatcher is the cluster-wide informer cache. At 0.30.4 the
// factory is instantiated AND started by NewResourceWatcher when
// CACHE_ENABLED=true; the four Role-Based Access Control GVRs are
// eagerly registered. At 0.30.6 the RestAction-derived inventory is
// also eager-registered post-construction via EagerRegisterAll.
//
// All methods are safe for concurrent use. AddResourceType registers an
// informer for a GVR; Start launches them; Get/List read from the
// in-memory store; Stop cancels the underlying context and waits for
// graceful shutdown.
//
// Per feedback_no_special_cases.md the type does not embed any
// per-resource policy — every consumer treats Disabled() uniformly.
type ResourceWatcher struct {
	// mode is the operational discriminator (0.30.71). modeInformer
	// is the production path; modePassthrough routes every Get/List
	// straight to apiserver via dyn. mode is set once at construction
	// and never mutated.
	mode watcherMode

	dyn     dynamic.Interface
	factory dynamicinformer.DynamicSharedInformerFactory

	// metaClient + metaFactory back the §0.30.93 (Revision 18)
	// metadata-only routing. Both are populated by SetMetadataClient
	// AFTER NewResourceWatcher returns (the apiextensions LIST that
	// also runs at startup needs the same rest.Config, so main.go
	// builds the metadata client there and wires it in). nil-safe:
	// EnsureResourceType falls back to the dynamic full-informer path
	// when metaClient == nil — production code MUST set the metadata
	// client to keep the OOM-safety property for Composition GVRs.
	//
	// metaFactory is created lazily on the first metadata-only
	// EnsureResourceType so callers that never opt any GVR into the
	// metadata-only set pay zero allocation cost. Concurrency: writes
	// happen under rw.mu (same singleflight gate as the dynamic
	// factory creation).
	metaClient  metadata.Interface
	metaFactory metadatainformer.SharedInformerFactory

	mu        sync.RWMutex
	informers map[schema.GroupVersionResource]informers.GenericInformer
	started   bool

	// --- 0.30.98 Tag A: four-conjunct servability gate ---
	//
	// servable(gvr) := registered(gvr) AND HasSynced(gvr)
	//                  AND watchHealthy(gvr) AND resourceTypeConfirmed(gvr)
	//
	// The two maps below back conjuncts 3 and 4. Both are guarded by
	// rw.mu (same lock as rw.informers) — no separate mutex. Neither
	// holds per-Resource policy: they are populated uniformly for every
	// registered GVR (feedback_no_special_cases.md).

	// watchBroken records GVRs whose informer reflector has dropped its
	// WATCH connection (conjunct 3). Set by the SetWatchErrorHandler
	// closure installed before Informer().Run; cleared by the discovery-
	// refresh ticker once the informer's LastSyncResourceVersion advances
	// (a successful relist). A broken-WATCH informer has a potentially
	// stale store, so the pivot must fall through to apiserver until the
	// reflector reconnects.
	watchBroken map[schema.GroupVersionResource]struct{}

	// confirmed records GVRs whose resource *type* has been verified to
	// exist in the apiserver's currently-served API surface (conjunct 4 —
	// THE S4 FIX). A GVR is confirmed only after the discovery-refresh
	// ticker observes its group/version serving a non-empty
	// APIResourceList. A registered+synced informer whose type was NOT
	// served at initial-LIST time (a post-startup CRD) latches
	// HasSynced=true over an empty result; without this conjunct the
	// pivot would serve [] as servable=true (the S4 regression).
	confirmed map[schema.GroupVersionResource]struct{}

	// lastEventAt records, per GVR, the wall-clock nanosecond of the most
	// recent ADD/UPDATE/DELETE the informer bridge delivered for it
	// (1.12.5 / #187). It is the freshness half of the servability picture:
	// HasSynced says an informer finished its initial LIST once, and says
	// nothing about whether it has heard anything since. A GVR whose objects
	// churn but whose lastEventAt is hours old has a dead watch that
	// watchBroken did not catch.
	//
	// sync.Map of *atomic.Int64, NOT a plain map under rw.mu: this is written
	// on the informer processor goroutine for EVERY event, and taking the
	// watcher's write mutex there would serialise all event delivery behind
	// the same lock the registration paths hold.
	lastEventAt sync.Map // map[schema.GroupVersionResource]*atomic.Int64

	// lastSyncRV tracks the per-GVR LastSyncResourceVersion observed by
	// the most recent discovery refresh. Used to detect a successful
	// relist (RV advanced) so watchBroken can be cleared. Guarded by
	// rw.mu.
	lastSyncRV map[schema.GroupVersionResource]string

	// watchHandlerInstalled records GVRs whose informer has had the
	// conjunct-3 WATCH-error handler successfully installed (0.30.99
	// Tag B — watch-handler coverage guard). installWatchErrorHandler
	// records into this set on a successful SetWatchErrorHandler call.
	// The constructor's terminal block asserts every rw.informers entry
	// appears here — so a future pre-Start lazy-register path that
	// bypasses the constructor's install loop cannot silently drop
	// watch-handler coverage (conjunct 3 would then never fire for that
	// GVR and the pivot would serve a possibly-stale store). Guarded by
	// rw.mu (same lock as rw.informers).
	watchHandlerInstalled map[schema.GroupVersionResource]struct{}

	// disco is the discovery client used by resourceTypeConfirmed. nil
	// is a valid state: when no discovery client is wired,
	// resourceTypeConfirmed defaults to true so the pivot keeps its
	// pre-0.30.98 HasSynced-only behaviour rather than dying entirely.
	// This is a uniform degradation policy, not a per-GVR carve-out.
	// Set once at startup via SetDiscoveryClient. Guarded by rw.mu.
	disco ResourceTypeDiscovery

	// metadataOnly is the set of GVRs that were registered via the
	// metadata-only path. Kept so observability tools (logs, future
	// metrics, the §0.30.93 stress falsifier's
	// `cache.lazy_register.metadata_only` assertion) can distinguish
	// "informer registered as PartialObjectMetadata" from "informer
	// registered as full Unstructured". A GVR appears in
	// rw.informers AND rw.metadataOnly when it took the metadata path.
	metadataOnly map[schema.GroupVersionResource]struct{}

	// syncCh stores per-GVR sync channels. Closed when that GVR's
	// informer has completed its initial WaitForCacheSync. Used by
	// EnsureResourceType so concurrent first-readers can block until
	// the informer is live, or proceed via the apiserver-fallback
	// branch in the resolver hot path. Tag 0.30.9 Sub-scope B
	// (lazy-register-on-resolver-touch).
	syncCh map[schema.GroupVersionResource]chan struct{}

	// eagerSet is the set of GVRs the caller passed to MarkEagerSet —
	// the post-startup expectation is that NO AddResourceType call
	// fires for a GVR in this set (because eager already registered
	// it). When one does, addResourceTypeLocked emits the WARN
	// "lazy-AddResourceType-unexpected" so the regression is visible.
	// nil eagerSet = "eager registration not yet completed" — no
	// WARNs fire (the constructor's own RBAC registrations are not
	// "lazy").
	eagerSet  map[schema.GroupVersionResource]struct{}
	eagerDone bool

	// informerStop holds the per-GVR stop channel passed to each
	// informer's Run (and its sync-watcher) — the R6 (0.30.115)
	// informer-lifecycle change. Pre-0.30.115 every informer shared the
	// single process-wide rw.stopCh, so there was no way to cancel ONE
	// informer; RemoveResourceType needs exactly that.
	//
	// Each channel is closed exactly once — either by RemoveResourceType
	// (per-GVR teardown) or by Stop() (global shutdown, reaping every
	// channel still present). closePerGVRStopLocked is the single close
	// site; it guards the close with a closed-check under rw.mu so a
	// RemoveResourceType racing a Stop() cannot double-close (AC-R6.5).
	// Guarded by rw.mu (same lock as rw.informers).
	informerStop map[schema.GroupVersionResource]chan struct{}

	// restConfig is the in-cluster *rest.Config — Ship 0.30.122 R4
	// Lever 1. The streaming ListWatch (streaming_list.go) builds a
	// rest.RESTClient from it to issue the paged composition LIST as a
	// raw HTTP request whose response body it streams through a
	// json.Decoder, instead of materialising the whole 48,999-object
	// list. Wired by SetRESTConfig AFTER NewResourceWatcher returns —
	// main.go already holds rest.InClusterConfig() at watcher
	// construction (same wiring pattern as SetMetadataClient). nil-safe:
	// when restConfig is nil OR the streaming-list flag is off, the
	// composition GVR falls back to the standard NewFilteredDynamicInformer.
	restConfig *rest.Config

	stopCh chan struct{}

	// goroutineWG tracks the watcher-owned goroutines that are NOT
	// reaped by factory.Shutdown() — i.e. everything this type spawns
	// directly with `go`: the standalone (lazily-registered / navigation-
	// discovered) informer Run goroutines, every per-GVR waitInformerSync
	// sync-watcher, and the initial-RBAC-snapshot publisher. The factory-
	// driven bootstrap informers are tracked by the factory's OWN
	// WaitGroup and drained by factory.Shutdown() (client-go prior art:
	// dynamicinformer.dynamicSharedInformerFactory.Shutdown does
	// `defer wg.Wait()`); they are deliberately NOT double-counted here.
	//
	// Stop() closes the stop channels (signalling every goroutine to
	// exit) THEN blocks on factory.Shutdown() + goroutineWG.Wait(), so
	// after Stop() returns NO goroutine this watcher started is still
	// running. This is the deterministic-teardown seam the cache test
	// harness relies on (Task #85): pre-0.30.252 Stop() only SIGNALLED
	// the goroutines and returned, so a still-draining informer event
	// handler (depEventHandlers.func1 -> Deps().On{Add,Update,Delete})
	// could read the deps singleton while a neighbouring test's
	// ResetDepsForTest() wrote depsInstance=nil — a DATA RACE (and, when
	// the leaked Reflector LISTed against a neighbour's fake client, a
	// panic on the unknown GVR). It is also a production correctness
	// improvement: a graceful shutdown should drain its goroutines, not
	// merely signal them.
	//
	// Wrapped goroutines MUST observe a closed stop channel and return —
	// every site that does `goroutineWG.Add(1)` is paired with a
	// `defer goroutineWG.Done()` and an existing stopCh/per-GVR-stop
	// exit path. Never call goroutineWG.Wait() while holding rw.mu (the
	// goroutines take rw.mu.RLock on their exit paths — see
	// waitAndPublishInitialRBACSnapshot, rbac_snapshot.go); Stop()
	// releases rw.mu before waiting.
	goroutineWG sync.WaitGroup
}

// NewResourceWatcher constructs a cluster-wide ResourceWatcher.
//
// Mode selection (0.30.71 extended CACHE_ENABLED semantics):
//
//   - CACHE_ENABLED=true (production, modeInformer): the dynamic
//     informer factory is constructed, the four Role-Based Access
//     Control GVRs are eagerly registered, the SetTransform pipeline
//     runs, and factory.Start fires. Every Get/List is served from
//     the informer indexer in O(1).
//
//   - CACHE_ENABLED=false + dyn != nil (diagnostic, modePassthrough):
//     NO factory is constructed, NO informers run, NO goroutines
//     spawn. Every Get/List is routed to apiserver via dyn. This is
//     the "true cache-off" measurement mode introduced at 0.30.71.
//     A loud WARN log is emitted so operators see immediately that
//     ALL caching layers (L1, typed-RBAC, informer) are dead.
//
//   - CACHE_ENABLED=false + dyn == nil: returns (nil, nil) for
//     backward compatibility with PM-amendment-1 tests that asserted
//     "dormant when disabled" before 0.30.71 existed.
//
// Callers MUST nil-check the return value: when nil, every consumer
// takes the apiserver branch. When non-nil in passthrough mode,
// Get/List still route to apiserver — the difference is that the
// watcher API stays callable instead of forcing every consumer to
// nil-check + duplicate the apiserver branch.
//
// At 0.30.4 (Revision 1 binding) cache=on mode eagerly registers the
// Role-Based Access Control GVRs (Role, RoleBinding, ClusterRole,
// ClusterRoleBinding) and starts the factory so EvaluateRBAC can serve
// in-process Role-Based Access Control decisions without ever calling
// SubjectAccessReview against apiserver.
func NewResourceWatcher(ctx context.Context, dyn dynamic.Interface) (*ResourceWatcher, error) {
	if Disabled() {
		// 0.30.71 split: with dyn=nil we preserve the pre-0.30.71
		// (nil, nil) contract that watcher_test.go's dormancy tests
		// rely on. With dyn != nil we build a passthrough watcher
		// so consumers that hold the watcher pointer can still call
		// Get/List; every call routes to apiserver.
		if dyn == nil {
			slog.Info("cache.disabled=true",
				slog.String("subsystem", "cache"),
				slog.Bool("plumbing_present", true),
				slog.Bool("routed", false),
				slog.String("mode", "dormant-no-dyn"),
			)
			return nil, nil
		}

		slog.Warn(
			"CACHE_ENABLED=false — typed-RBAC + informer cache + L1 ALL disabled (diagnostic mode; do not run in production)",
			slog.String("subsystem", "cache"),
			slog.Bool("plumbing_present", true),
			slog.Bool("routed", true),
			slog.String("mode", "passthrough"),
			slog.String("rationale", "0.30.71: true cache-off baseline for warm-p50 measurement"),
			slog.String("rbac.evaluate_path", "SubjectAccessReview"),
			slog.String("informer.get_list_path", "apiserver"),
			slog.String("l1.resolved_cache_path", "disabled"),
		)
		_ = ctx
		return &ResourceWatcher{
			mode:         modePassthrough,
			dyn:          dyn,
			informers:    map[schema.GroupVersionResource]informers.GenericInformer{},
			syncCh:       map[schema.GroupVersionResource]chan struct{}{},
			metadataOnly: map[schema.GroupVersionResource]struct{}{},
			watchBroken:  map[schema.GroupVersionResource]struct{}{},
			confirmed:    map[schema.GroupVersionResource]struct{}{},
			lastSyncRV:   map[schema.GroupVersionResource]string{},
			stopCh:       make(chan struct{}),
		}, nil
	}

	if dyn == nil {
		return nil, fmt.Errorf("cache: NewResourceWatcher requires non-nil dynamic.Interface")
	}

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		dyn,
		0, // resync period 0 — pure event-driven; no periodic full re-list
		metav1.NamespaceAll,
		listOptionsTweak,
	)

	rw := &ResourceWatcher{
		mode:                  modeInformer,
		dyn:                   dyn,
		factory:               factory,
		informers:             map[schema.GroupVersionResource]informers.GenericInformer{},
		syncCh:                map[schema.GroupVersionResource]chan struct{}{},
		metadataOnly:          map[schema.GroupVersionResource]struct{}{},
		watchBroken:           map[schema.GroupVersionResource]struct{}{},
		confirmed:             map[schema.GroupVersionResource]struct{}{},
		lastSyncRV:            map[schema.GroupVersionResource]string{},
		watchHandlerInstalled: map[schema.GroupVersionResource]struct{}{},
		stopCh:                make(chan struct{}),
	}
	_ = ctx // reserved for future wiring (0.30.6 eager-registration caller may pass-through)

	// Revision 1 binding: register the four Role-Based Access Control
	// GVRs eagerly and start the factory. This is the single set of
	// types EvaluateRBAC reads from; without these we cannot meet the
	// "zero SubjectAccessReview in cache=on" rule.
	for _, gvr := range RBACResourceTypes {
		rw.addResourceTypeLocked(gvr)
	}

	// 0.30.99 Tag B — watch-handler coverage assertion. The SetTransform
	// strip (0.30.5, primer §4.7) and the conjunct-3 WATCH-error handler
	// (0.30.98 Tag A) are BOTH installed by addResourceTypeLocked at
	// registration time — for the RBAC GVRs that happened in the
	// RBACResourceTypes loop above, pre-Start. Pre-0.30.99 this block
	// re-installed both in a second loop; that duplication is removed
	// (addResourceTypeLocked is the single install site).
	//
	// What remains here is the architect's Tag B guard: assert that
	// every GVR in rw.informers had its WATCH-error handler installed
	// (recorded in rw.watchHandlerInstalled). Since addResourceTypeLocked
	// now installs unconditionally — not only in its post-Start branch —
	// a pre-Start lazy-register that bypasses the constructor cannot
	// silently drop conjunct-3 coverage. The assertion logs a loud WARN
	// if the invariant is ever violated, so the regression is visible at
	// boot rather than only under a dropped-WATCH incident.
	rw.assertWatchHandlerCoverageLocked()

	// 0.30.6 plan §Risks bullet 1 — startup assertion. The typed
	// RBAC overrides MUST be registered BEFORE factory.Start so
	// every Add/Update event fires through the typed transform.
	// Registration happens at package init() in strip.go; if it
	// regressed (someone deletes the init or renames the GVR), this
	// panics with the missing GVR so the regression cannot ship
	// silently.
	AssertRBACTypedOverridesRegistered()

	rw.factory.Start(rw.stopCh)
	rw.started = true

	// 0.30.9 Sub-scope B: now that the factory has started the
	// constructor-registered informers (the four RBAC GVRs),
	// spawn one sync-watcher per GVR so EnsureResourceType callers
	// for those GVRs (rare — only the test path; production callers
	// go through ListTypedObjects / GetTypedObject) see a closed
	// channel as soon as HasSynced flips.
	for gvr, gi := range rw.informers {
		ch := rw.syncCh[gvr]
		if ch == nil {
			continue
		}
		rw.goroutineWG.Add(1)
		go func(hasSynced func() bool, ch chan struct{}) {
			defer rw.goroutineWG.Done()
			waitInformerSync(hasSynced, ch, rw.stopCh)
		}(gi.Informer().HasSynced, ch)
	}

	// Ship B (0.30.138) — typed-RBAC snapshot wiring assertion. By this
	// point addResourceTypeLocked has been called for every GVR in
	// RBACResourceTypes (the eager-registration loop above) AND each
	// call has attached the snapshot event handler via the
	// isTypedRBACGVR branch in addResourceTypeLocked. If the assertion
	// fires, the snapshot writer wiring regressed and the boot panics —
	// analogous to AssertRBACTypedOverridesRegistered (strip.go:173).
	AssertRBACSnapshotWired()

	// Ship B (0.30.138) — initial snapshot publish. Spawn ONE goroutine
	// that waits for all 4 RBAC syncCh to close (the same Servable
	// signal the design AC-B.9 names) then synchronously runs the
	// initial rebuildRBACSnapshot — publishing the first snapshot
	// before the first user request that depends on it can succeed.
	//
	// Between cache=on activation and this publish, rbacSnap.Load()
	// returns nil → EvaluateRBAC AC-B.8 degrade-to-deny fires. No
	// silent-fall-through to UserCan (would violate Revision 1).
	rw.goroutineWG.Add(1)
	go func() {
		defer rw.goroutineWG.Done()
		waitAndPublishInitialRBACSnapshot(rw)
	}()

	slog.Info("cache.plumbing_present=true cache.routed=true rbac.informer_started=true",
		slog.String("subsystem", "cache"),
		slog.Int64("list_page_limit", listPageLimit),
		slog.Int("resource_types_registered", len(rw.informers)),
		slog.String("rbac.evaluate_path", "in-process"),
		slog.String("subject_access_review_calls_in_cache_on_path", "banned"),
	)

	return rw, nil
}

// SetMetadataClient wires the metadata.Interface used by the §0.30.93
// (Revision 18) metadata-only informer routing. Called once at startup
// from main.go AFTER NewResourceWatcher succeeds and BEFORE the first
// /call dispatch (so the first EnsureResourceType invocation for a
// metadata-only GVR has a live client).
//
// Passing nil clears the client (test-only path). In production, leaving
// it nil means metadata-only-eligible GVRs (Composition family, plus
// any annotation-discovered set) fall through to the dynamic
// full-informer path — re-introducing the 0.30.92 OOM risk. The
// constructor logs a one-shot WARN at EnsureResourceType time if it
// detects that situation, so the regression is visible.
//
// Concurrency: takes rw.mu. Idempotent on a same-pointer re-call. The
// metadata SharedInformerFactory is allocated lazily on the first
// metadata-only EnsureResourceType invocation so callers that never opt
// any GVR into metadata-only pay zero cost.
//
// Nil-receiver safe (test path: cache.Global() returns nil under
// CACHE_ENABLED=false).
func (rw *ResourceWatcher) SetMetadataClient(c metadata.Interface) {
	if rw == nil {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.metaClient = c
}

// SetRESTConfig wires the in-cluster *rest.Config — Ship 0.30.122 R4
// Lever 1. The streaming ListWatch (streaming_list.go) needs raw HTTP
// access to the apiserver to stream a paged LIST response body through a
// json.Decoder; the dynamic.Interface only returns a fully-materialised
// *UnstructuredList. main.go calls this right after NewResourceWatcher,
// passing the same rest.InClusterConfig() it already built — mirroring
// SetMetadataClient's post-construction wiring.
//
// Nil-receiver safe. When restConfig is never wired the streaming-list
// path is unavailable and addResourceTypeLocked falls back to the
// standard NewFilteredDynamicInformer for every GVR.
func (rw *ResourceWatcher) SetRESTConfig(rc *rest.Config) {
	if rw == nil {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()
	rw.restConfig = rc
}

// Mode returns the operational discriminator (modeInformer or
// modePassthrough). Exposed for falsifier-log diagnostics and unit
// tests; production callers should rely on Disabled() / Get-List
// behaviour instead.
func (rw *ResourceWatcher) Mode() watcherMode {
	if rw == nil {
		return modeInformer // unreachable: callers nil-check upstream
	}
	return rw.mode
}

// IsPassthrough reports whether the watcher routes Get/List to
// apiserver (0.30.71 diagnostic mode). A nil receiver returns false
// so call sites stay terse.
func (rw *ResourceWatcher) IsPassthrough() bool {
	return rw != nil && rw.mode == modePassthrough
}

// AddResourceType registers an informer for gvr. Idempotent: calling
// twice for the same GVR is a no-op. Safe for concurrent use.
//
// In modePassthrough (0.30.71) this is a no-op: every Get/List call
// is already apiserver-routed via the dynamic client, so there is
// nothing to register and no informer to start.
//
// At 0.30.4 the constructor eagerly registers the four Role-Based
// Access Control GVRs. At 0.30.6 the inventory walker (gated behind
// EAGER_REGISTER_ENABLED at 0.30.61) covered the RestAction-derived
// inventory. At 0.30.9 (Sub-scope B), the canonical lazy-registration
// API for resolver-hot-path callers is EnsureResourceType — it returns
// the per-GVR sync channel so callers can singleflight first-reads.
// AddResourceType is preserved for back-compat with EagerRegisterAll.
func (rw *ResourceWatcher) AddResourceType(gvr schema.GroupVersionResource) {
	if rw.mode == modePassthrough {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()

	rw.addResourceTypeLocked(gvr)
}

// EnsureResourceType is the idempotent, sync-channel-returning lazy
// registration API introduced at 0.30.9 (Sub-scope B). Behaviour:
//
//   - If gvr is already registered: returns (false, sync), where sync
//     is the channel that was created on first registration. The
//     channel is closed once that informer's initial WaitForCacheSync
//     completes. Callers may either block on it (to wait for the
//     informer cache to be live) or proceed via apiserver-fallback.
//   - If gvr is not yet registered: registers it under rw.mu (which
//     serves as the singleflight primitive — no separate sync.Once
//     needed), kicks off the informer goroutine (late-registration
//     branch), spawns a sync-watcher that closes sync on
//     WaitForCacheSync completion, and returns (true, sync).
//
// At §0.30.93 (Revision 18, Option D): the registration path is
// selected via `shouldUseMetadataOnly(gvr)` (defined in
// `internal/cache/cache_mode.go`). When the predicate returns true AND
// a metadata client is wired (rw.metaClient != nil), the informer is
// created against `metadatainformer.SharedInformerFactory`
// (PartialObjectMetadata events — ~2.5 KiB per object). When false (or
// the metadata client is nil), the registration takes the default
// dynamic full-informer path (~20 KiB per object post-strip).
//
// Both paths register the SAME DepTracker handlers (`UpdateFunc` →
// `Deps().OnUpdate`, `DeleteFunc` → `Deps().OnDelete`). The handlers
// use `metaNSName(obj)` which extracts (namespace, name) via the
// `nsNameAccessor` interface — `metav1.PartialObjectMetadata` embeds
// `ObjectMeta` so it satisfies that interface. The DELETE-evict /
// UPDATE-refresh semantics are byte-identical between the two paths.
// Per `feedback_l1_invalidation_delete_only.md`: DELETE evicts, UPDATE
// refreshes — preserved.
//
// In modePassthrough (0.30.71) this is a no-op: returns (false, nil).
// Callers in passthrough mode never hit the informer code path —
// every Get/List routes to apiserver via the dynamic client — so
// there is no informer to register and no sync channel to honour.
//
// Per plan §"Singleflight on EnsureResourceType" (binding): rw.mu IS
// the singleflight primitive. Concurrent first-reads for the same GVR
// see exactly one factory.ForResource + Informer().Run call; every
// subsequent caller receives the same sync channel and the same
// informer (via the existing AddResourceType idempotence).
//
// Per `feedback_no_special_cases.md`: the predicate
// `shouldUseMetadataOnly(gvr)` is the SINGLE source of per-GVR routing
// logic. EnsureResourceType is uniform plumbing — there is no
// per-Resource if-elif in the routing code path.
//
// Safe for concurrent use.
//
// Ship 0.30.217 Path 3.1 Bug 2 — RLock fast-path on the hit path. The
// pre-Path-3.1 implementation took `rw.mu.Lock()` UNCONDITIONALLY even
// when the GVR was already registered. At cluster-list-collapse
// activation EVERY /call drives the inner-call path through this lock
// (resolve.go:444 `lazyRegisterInnerCallPaths` + deps_extract.go:155
// `ensureWatcherInformerForGVR`), serialising all concurrent /call
// workers on a single writer mutex (2,988ms blocking observed in
// 0.30.216 canonical Chrome MCP). The fix mirrors the RLock fast-path
// pattern already used by `IsMetadataOnly` (line 893) and `IsSynced`/
// `IsServable` (servable.go:216). Correctness: the locked path's
// `addResourceTypeLocked` / `addResourceTypeMetadataOnlyLocked` already
// have idempotent re-check guards (`if _, exists := rw.informers[gvr];
// exists { return }`) so the classic check-then-act race window between
// RUnlock and Lock is benign.
func (rw *ResourceWatcher) EnsureResourceType(gvr schema.GroupVersionResource) (added bool, sync <-chan struct{}) {
	if rw == nil {
		return false, nil
	}
	if rw.mode == modePassthrough {
		return false, nil
	}

	// Path 3.1 Bug 2 — fast-path hit lookup under RLock. The hit path
	// is the dominant code path under cluster-list-collapse (every
	// /call worker checks the same handful of GVRs); doing it under
	// RLock allows concurrent readers and eliminates the writer-stack
	// contention that motivated this fix.
	rw.mu.RLock()
	if _, exists := rw.informers[gvr]; exists {
		ch, ok := rw.syncCh[gvr]
		rw.mu.RUnlock()
		if ok {
			return false, ch
		}
		// Defensive: invariant broken. Return a pre-closed channel
		// so callers don't deadlock.
		closed := make(chan struct{})
		close(closed)
		return false, closed
	}
	disco := rw.disco
	rw.mu.RUnlock()

	// #119 unserved-GROUP pre-check. A miss is confirmed — this call would
	// register + spawn an informer for gvr. Before it does, consult discovery:
	// if the apiserver AUTHORITATIVELY does not serve gvr's GROUP at all (a
	// dead/typo'd apiGroup, or a group from an uninstalled operator), DO NOT
	// register it. Such an informer's HasSynced never flips → conjunct 2 never
	// true → never servable → every dispatch falls through to a live apiserver
	// 404, and the reflector's ListAndWatch churns 404 forever (the #119
	// boot-latency + perpetual-refresher-404 residual). This is the ONLY
	// spawn-site funnel (all path-derived callers — resolve.go:1788,
	// informer_dispatch.go:359, apistage.go:585, objects/get.go:113,
	// deps_extract.go:156 — route here), so gating once here needs no
	// per-caller special-case (feedback_no_special_cases).
	//
	// GROUP granularity, NOT resource granularity — this is the architect-ruled
	// boundary that keeps the S4/stale-delete post-startup-CRD heal path
	// intact. A post-startup CRD lands under a group that is ALREADY served, so
	// groupAuthoritativelyAbsent is false for it → it registers and the
	// confirm-ticker heals it once its resource type appears. Only a group
	// entirely absent from ServerGroups() is skipped. See
	// groupAuthoritativelyAbsent (servable.go).
	//
	// FAIL-SAFE OPEN ON UNCERTAINTY (the crux): groupAuthoritativelyAbsent is
	// THREE-STATE — it returns true ONLY on a SUCCESSFUL ServerGroups() response
	// that definitively lacks the group. disco==nil / no ServerGroups surface /
	// a ServerGroups error / a nil list ALL register (never skip); the core
	// group ("") never skips. A false-skip of a genuinely-served group would
	// silently drop real data forever — strictly worse than the churn — so
	// uncertainty NEVER skips.
	//
	// NON-TERMINAL: the skip does not register anything, so a brand-new group's
	// first CRD (group briefly absent at first-touch) is recovered by the next
	// dispatch's EnsureResourceType re-touch against fresh discovery. The
	// dispatch still serves live via httpcall.Do apiserver fall-through
	// meanwhile, exactly as an unregistered GVR does today.
	//
	// Gated by SkipUnservedGroupInformers() (default ON, flips both ways);
	// flag-off restores register-unconditionally.
	if SkipUnservedGroupInformers() && groupAuthoritativelyAbsent(disco, gvr.Group) {
		closed := make(chan struct{})
		close(closed)
		slog.Info("cache.lazy_register.skipped_unserved_group",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("group", gvr.Group),
			slog.String("reason", "apiserver ServerGroups() authoritatively does not serve this group"),
			slog.String("effect", "informer NOT registered — no perpetual watch-404 churn; dispatch falls through to live apiserver; re-touch recovers if the group is later served"),
		)
		return false, closed
	}

	// Miss confirmed under RLock — upgrade to writer Lock. The
	// addResourceType*Locked helpers re-check the informers map (line
	// 694 + line 580-ish equivalents), so a peer that registered the
	// GVR while we were re-acquiring is handled idempotently.
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if _, exists := rw.informers[gvr]; exists {
		// Hit (peer registered between RUnlock + Lock): return the
		// existing sync channel. Defensive nil-check — the
		// constructor's RBAC registrations always allocate a channel,
		// but a future refactor could break this invariant.
		if ch, ok := rw.syncCh[gvr]; ok {
			return false, ch
		}
		// Defensive: invariant broken. Return a pre-closed channel
		// so callers don't deadlock.
		closed := make(chan struct{})
		close(closed)
		return false, closed
	}

	// §0.30.93 routing: consult the predicate. RBAC GVRs always take
	// the full-informer path (the predicate hardcodes that exclusion);
	// every other path is controlled by annotation + static seed.
	//
	// If the predicate selects metadata-only AND we have a metadata
	// client, take the metadata path. Otherwise fall through to the
	// dynamic full informer (the pre-§0.30.93 behaviour).
	if shouldUseMetadataOnly(gvr) && rw.metaClient != nil {
		rw.addResourceTypeMetadataOnlyLocked(gvr)
		ch := rw.syncCh[gvr]
		slog.Info("cache.lazy_register.metadata_only",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("path", "metadata-only"),
			slog.String("reason", metadataOnlyReason(gvr)),
			slog.String("hint", "PartialObjectMetadata informer — ~10x smaller than full Unstructured; DepTracker preserved"),
		)
		// Fix #130 F1b: prime conjunct-4 (typeConfirmed) for the GVR this call
		// just lazily registered so the NEXT dispatch serves from the informer
		// rather than falling through to a live LIST. Async + skip-guarded; runs
		// after this critical section releases rw.mu. See primeConfirmAsyncLocked.
		rw.primeConfirmAsyncLocked(gvr)
		return true, ch
	}
	// Soft-fail observability: if the predicate WOULD have routed
	// metadata-only but the metadata client is missing, log a one-shot
	// WARN so SRE sees the regression (Composition GVRs would land on
	// the full-Unstructured informer and re-introduce the 0.30.92 OOM).
	if shouldUseMetadataOnly(gvr) && rw.metaClient == nil {
		slog.Warn("cache.lazy_register.metadata_only_unwired",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", "predicate matched metadata-only routing but metaClient is nil — falling back to dynamic full informer; OOM risk at 50K scale"),
			slog.String("remediation", "main.go must call ResourceWatcher.SetMetadataClient(metadata.NewForConfig(rc)) at startup"),
		)
	}

	// Default path: dynamic full-Unstructured informer.
	//
	// Miss: register + allocate the sync channel + spawn the
	// sync-watcher goroutine. addResourceTypeLocked allocates the
	// channel and stores it in rw.syncCh; we read it here and
	// spawn the watcher so the channel closes on HasSynced.
	rw.addResourceTypeLocked(gvr)
	ch := rw.syncCh[gvr]

	// Falsifier per plan §"Pre-flight RUN protocol" step 2: emit a
	// log line `cache.lazy_register fired gvr=...` on first
	// registration. Distinct from `lazy-AddResourceType` (the
	// 0.30.6 falsifier triggered by post-eager-done lazy adds).
	slog.Info("cache.lazy_register",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("path", "full-unstructured"),
		slog.String("hint", "first resolver touch — informer registered + dep-tracker handlers wired"),
	)

	// Fix #130 F1b: prime conjunct-4 (typeConfirmed) for the GVR this call just
	// lazily registered so the NEXT dispatch serves from the informer rather
	// than falling through to a live LIST (the 23s benchapps 60K-item boot-walk
	// fall-through the 1.7.3/1.7.4 serve_miss readout showed). Async +
	// skip-guarded; runs after this critical section releases rw.mu. See
	// primeConfirmAsyncLocked.
	rw.primeConfirmAsyncLocked(gvr)

	return true, ch
}

// metadataOnlyReason renders a short label describing WHY the predicate
// chose the metadata-only path for gvr. Used in the log line so SRE can
// distinguish annotation-driven (preferred, long-term) routing from
// seed-driven (current operational reality) routing. The label is best-
// effort: a GVR matching both the annotation set AND the seed renders as
// "annotation" (the annotation takes precedence in logging because it
// is the customer-controlled lever).
func metadataOnlyReason(gvr schema.GroupVersionResource) string {
	if _, ok := annotatedGVRs.Load(gvr); ok {
		return "annotation"
	}
	for _, pat := range metadataOnlyGVRSeed {
		if matchesSeed(gvr, pat) {
			return "static_seed"
		}
	}
	return "unknown" // unreachable given the predicate fired
}

// addResourceTypeMetadataOnlyLocked is the §0.30.93 (Revision 18)
// metadata-only registration path. Callers MUST hold rw.mu.Lock().
//
// Differences vs addResourceTypeLocked:
//
//  1. Uses metadatainformer.SharedInformerFactory (lazily created from
//     rw.metaClient on first metadata-only registration) instead of the
//     dynamic factory. The factory's `ForResource(gvr)` returns an
//     `informers.GenericInformer` whose store holds
//     `*metav1.PartialObjectMetadata` instead of
//     `*unstructured.Unstructured`. ~10× memory reduction per object.
//
//  2. Skips SetTransform (the strip pipeline is designed for the full
//     Unstructured shape — managedFields, last-applied annotation;
//     PartialObjectMetadata already lacks both). Skipping is correct:
//     transform on PartialObjectMetadata would only add CPU cost.
//
//  3. SAME DepTracker handlers wired (UpdateFunc → OnUpdate, DeleteFunc
//     → OnDelete). The handlers call `metaNSName(obj)` which extracts
//     (namespace, name) via the `nsNameAccessor` interface;
//     `*metav1.PartialObjectMetadata` embeds `ObjectMeta` and therefore
//     satisfies it. This is the binding property that makes Option D
//     viable per plan §"Revision 18 redesign space" item 1.
//
//  4. Records the GVR in rw.metadataOnly so observability tools can
//     distinguish the two paths.
//
// Per `feedback_l1_invalidation_delete_only.md`: DELETE evicts, UPDATE
// refreshes — preserved byte-for-byte from the full-informer path.
func (rw *ResourceWatcher) addResourceTypeMetadataOnlyLocked(gvr schema.GroupVersionResource) {
	if rw.mode == modePassthrough {
		return
	}
	if _, exists := rw.informers[gvr]; exists {
		return
	}
	if rw.metaClient == nil {
		// Defensive: caller should have checked, but if metaClient
		// vanished between the EnsureResourceType predicate evaluation
		// and here (impossible under rw.mu), bail out.
		return
	}

	// R6 (0.30.115): removable GVRs get a STANDALONE metadata informer,
	// not a shared-metaFactory one — same rationale as the dynamic
	// full-informer path (the shared factory caches by GVR with no
	// eviction API, so a torn-down factory informer would be handed
	// back stopped + frozen on CRD recreate). NewFilteredMetadataInformer
	// is the exact constructor the shared metaFactory calls internally
	// (client-go metadatainformer/informer.go:113), with the SAME
	// listOptionsTweak. The composition family routes through the
	// metadata-only path when annotated/static-seeded, so this is the
	// common path for the GVRs RemoveResourceType actually tears down.
	var gi informers.GenericInformer
	standalone := IsNavigationDiscoveredGroup(gvr.Group)
	if standalone {
		gi = metadatainformer.NewFilteredMetadataInformer(
			rw.metaClient,
			gvr,
			metav1.NamespaceAll,
			0, // resync period 0 — pure event-driven, matches the factory
			clientcache.Indexers{clientcache.NamespaceIndex: clientcache.MetaNamespaceIndexFunc},
			listOptionsTweak,
		)
	} else {
		// Lazy shared-factory construction. resyncPeriod=0 ⇒ pure
		// event-driven, no periodic full re-list; listOptionsTweak
		// matches the dynamic factory's bounded-paging policy.
		if rw.metaFactory == nil {
			rw.metaFactory = metadatainformer.NewFilteredSharedInformerFactory(
				rw.metaClient,
				0, // resync period 0
				metav1.NamespaceAll,
				listOptionsTweak,
			)
		}
		gi = rw.metaFactory.ForResource(gvr)
	}
	rw.informers[gvr] = gi
	// Ship 1 / 0.30.225 — record GVR insert timestamp for the
	// /debug/vars snowplow_plurals_registered_gvrs envelope's
	// last_register_unix_ns field. The existence-check at the head
	// of this function early-returns for idempotent re-entries, so
	// reaching this line implies a genuine new insertion.
	NotifyGVRRegistered()
	if rw.metadataOnly == nil {
		rw.metadataOnly = map[schema.GroupVersionResource]struct{}{}
	}
	rw.metadataOnly[gvr] = struct{}{}

	if rw.syncCh == nil {
		rw.syncCh = map[schema.GroupVersionResource]chan struct{}{}
	}
	rw.syncCh[gvr] = make(chan struct{})

	// R6 (0.30.115): per-GVR stop channel — same rationale as the
	// dynamic full-informer path. RemoveResourceType cancels this
	// metadata informer alone.
	rw.perGVRStopLocked(gvr)

	resourceType := gvrResourceTypeString(gvr)

	// DepTracker event handlers (Ship A 0.30.110). Identical wiring to
	// addResourceTypeLocked — the SHARED depEventHandlers builder. The
	// obj coming through here is a *metav1.PartialObjectMetadata, which
	// embeds ObjectMeta and therefore satisfies the nsNameAccessor
	// interface used by metaNSName. ADD post-sync gate, UPDATE
	// dirty-mark, DELETE classify+evict-via-worker are byte-identical
	// to the full-informer path.
	if _, regErr := gi.Informer().AddEventHandler(rw.depEventHandlers(gvr)); regErr != nil {
		slog.Warn("cache.deps.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.String("resource_type", resourceType),
			slog.String("path", "metadata-only"),
			slog.String("error", regErr.Error()),
		)
	}

	// Ship 0 / 0.30.222 — declarative handler-extension registry; same
	// iteration as the full-informer path. Production never reaches here
	// for RBAC (the eager-registration loop in NewResourceWatcher uses
	// the full addResourceTypeLocked path); the registry still iterates
	// — non-matching predicates are O(1) skips — so a future caller that
	// adds RBAC metadata-only does not silently bypass snapshot wiring.
	attachMatchingHandlerExtensions(rw, gvr, gi.Informer())

	// 0.30.98 Tag A: install the WATCH-error handler BEFORE Run
	// (conjunct 3) — same uniform wiring as the dynamic full-informer
	// path. The metadata-only reflector drops its WATCH on the same
	// failure modes, so the servability gate must observe it.
	rw.installWatchErrorHandler(gvr, gi)

	// Start the metadata informer and spawn the sync-watcher. We
	// always reach this branch post-Start (the constructor never
	// metadata-registers; only lazy EnsureResourceType does), so the
	// "late registration" path is the only one. R6 (0.30.115): both
	// goroutines are bound by the per-GVR stop channel.
	//
	// Task #85 — track BOTH goroutines in rw.goroutineWG so Stop() drains
	// them deterministically. These standalone metadata-informer Run +
	// sync-watcher goroutines are NOT tracked by rw.metaFactory (the
	// metadata informer is constructed via NewFilteredMetadataInformer /
	// metaFactory.ForResource but Run by THIS `go`, not by
	// metaFactory.Start), so without this they outlive Stop() — the exact
	// leaked-Reflector source behind the deps-reset race (a leaked
	// composition-GVR metadata Reflector LISTing against a neighbour test's
	// fake client panicked, and its event handlers raced ResetDepsForTest).
	// The stopRequestedLocked() guard keeps Add ordered before Stop's Wait
	// under rw.mu (see the dynamic late-registration path for the full
	// rationale).
	if rw.stopRequestedLocked() {
		return
	}
	stop := rw.informerStop[gvr]
	ch := rw.syncCh[gvr]
	rw.goroutineWG.Add(2)
	go func(inf clientcache.SharedIndexInformer) {
		defer rw.goroutineWG.Done()
		inf.Run(stop)
	}(gi.Informer())
	go func(hasSynced func() bool) {
		defer rw.goroutineWG.Done()
		waitInformerSync(hasSynced, ch, stop)
	}(gi.Informer().HasSynced)
}

// primeConfirmAsyncLocked schedules a one-shot conjunct-4 (typeConfirmed)
// confirm-prime for a GVR that was JUST lazily registered by
// EnsureResourceType / EnsureResourceTypeMetadataOnly. Callers MUST hold
// rw.mu.Lock() (both EnsureResourceType success paths do, via their deferred
// unlock).
//
// # Why this exists (Fix #130 F1b — the REACHABILITY close)
//
// F1 (1.7.4) wired the approved confirm-prime (ConfirmResourceTypes) onto the
// discovery-walk register path (PrewarmRegisterFromNavigation) — but that walk
// is gated behind PREWARM_REGISTER_ENABLED, default OFF, so under the DEPLOYED
// default config it never runs and F1's on-deploy deltas were all zero. The
// registration path the default binary ACTUALLY runs is lazy registration:
// EnsureResourceType, reached from the dispatch seams (informer_dispatch.go,
// internal_dispatch.go via the walk-era GVRs). The 1.7.3/1.7.4 boot logs showed
// every lazily-registered GVR (incl. benchapps) latch
// {Registered:true HasSynced:true WatchHealthy:true TypeConfirmed:false} and
// fall through to the 23s live 60K LIST — conjunct 4 (typeConfirmed) was the
// sole universal blocker of informer-serve at boot. Priming confirm HERE, at
// the lazy-register success path, makes the F1 mechanism runtime-reachable with
// NO env flag on the call chain. Precedent: the Gate-6 dispatch path already
// primes ConfirmResourceType on ITS register (informer_dispatch.go, added &&
// name==""); this generalises that prime to the register site itself so it is
// independent of the dispatch's added/name gating.
//
// # Async, not sync (deadlock + no-stall — architect-settled)
//
// ConfirmResourceType issues a blocking discovery round-trip
// (ServerResourcesForGroupVersion) and then RE-ACQUIRES rw.mu to write
// rw.confirmed. Calling it inline here would (a) DEADLOCK — the caller holds
// rw.mu.Lock() — and (b) even if the lock were re-entrant, serialise every
// other dispatch behind a ~10ms network call held under the writer lock
// (F-no-stall). So the prime is dispatched on a watcher-owned goroutine that
// runs AFTER this critical section releases the lock. It uses a background
// context (independent of the registering caller's request ctx, which may be
// cancelled the instant the dispatch falls through) bounded by rw.stopCh so it
// cannot outlive Stop().
//
// # Skip-if-already-confirmed guard (cost bound)
//
// The prime is skipped when gvr is ALREADY confirmed (rw.confirmed has the key)
// — a re-registration of a confirmed GVR pays zero discovery calls. It is also
// a no-op when no discovery client is wired (rw.disco == nil): conjunct 4 is
// degraded-true in that mode (resourceTypeConfirmedLocked returns true), so
// there is nothing to fetch — the offline degrade is preserved byte-identically.
// Net cost bound: at most ONE discovery round-trip per GVR over the process
// lifetime of lazy registration (a GVR registers-lazily once; a re-register
// after CRD delete+recreate is the intended re-confirm, not waste).
//
// The FIRST dispatch after a lazy register is exactly the one we want to serve;
// ~10ms of async discovery beats a 23s live-LIST fall-through, and because the
// confirm runs off the register path's lock it never stalls unrelated
// dispatches.
func (rw *ResourceWatcher) primeConfirmAsyncLocked(gvr schema.GroupVersionResource) {
	// Skip-guard 1 — offline degrade preserved: with no discovery client,
	// conjunct 4 is degraded-true and there is nothing to confirm. Bail before
	// spawning a goroutine that would immediately no-op inside ConfirmResourceType.
	if rw.disco == nil {
		return
	}
	// Skip-guard 2 — already confirmed: a re-registration of a GVR that is
	// already conjunct-4 confirmed pays no discovery call. rw.confirmed may be
	// nil (never allocated) — a nil-map read is a safe miss.
	if _, ok := rw.confirmed[gvr]; ok {
		return
	}
	// Respect shutdown ordering (Task #85): the stopRequestedLocked() check
	// under rw.mu keeps goroutineWG.Add ordered before Stop's Wait, so we never
	// Add after Stop has begun draining.
	if rw.stopRequestedLocked() {
		return
	}
	rw.goroutineWG.Add(1)
	go func() {
		defer rw.goroutineWG.Done()
		// Background context: the registering caller's request ctx may be
		// cancelled the moment its dispatch falls through, but the prime must
		// still land so the NEXT dispatch of this GVR serves from the informer.
		// ConfirmResourceType is itself ctx-cancel and passthrough/nil safe; it
		// does the discovery call OFF rw.mu then re-locks to write rw.confirmed.
		bgCtx := context.Background()
		// Bound teardown: if Stop() has closed stopCh, abandon the prime rather
		// than issue a discovery call against a client that may be tearing down.
		select {
		case <-rw.stopCh:
			return
		default:
		}
		rw.ConfirmResourceType(bgCtx, gvr)
		slog.Info("cache.lazy_register.confirm_primed",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", "Fix #130 F1b — primed conjunct-4 typeConfirmed at the default-config lazy-register seam so the next dispatch serves from the informer instead of a live LIST fall-through"),
		)
	}()
}

// EnsureResourceTypeMetadataOnly is the explicit, signature-preserving
// metadata-only registration entry point introduced at §0.30.93
// (Revision 18). Mirrors `EnsureResourceType` but bypasses the
// predicate — the caller is asserting "I want PartialObjectMetadata
// routing for this GVR regardless of cluster annotation state".
//
// Production callers MUST go through `EnsureResourceType` (which
// consults the predicate). This explicit entry point exists for:
//
//   - Unit tests asserting metadata-only behaviour without touching
//     the predicate state.
//   - Future opt-in features where a caller knows a GVR is metadata-
//     only-safe (e.g. a manual operator-set route).
//
// Returns:
//
//   - (false, sync) on hit (same channel semantics as EnsureResourceType).
//     Note: a hit may have been the full-informer path; this method
//     does NOT promote a previously-full registration to metadata-only.
//     Promotion would require deleting the existing informer's store +
//     reallocating, which is the OOM-causing branch in Option F. We
//     deliberately do not implement promotion.
//   - (true, sync) on miss. Caller can block on sync to wait for the
//     initial LIST.
//   - (false, nil) when the watcher is nil, in passthrough mode, or
//     when no metadata client is wired. The (false, nil)-on-missing-
//     metaClient case is loud-logged so SRE sees the regression.
//
// Safe for concurrent use. Uses rw.mu as the singleflight primitive.
func (rw *ResourceWatcher) EnsureResourceTypeMetadataOnly(gvr schema.GroupVersionResource) (added bool, sync <-chan struct{}) {
	if rw == nil {
		return false, nil
	}
	if rw.mode == modePassthrough {
		return false, nil
	}

	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.metaClient == nil {
		slog.Warn("cache.metadata_only.no_client",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", "EnsureResourceTypeMetadataOnly called before SetMetadataClient — caller should use EnsureResourceType (predicate-routed) or wire the client at startup"),
		)
		return false, nil
	}

	if _, exists := rw.informers[gvr]; exists {
		if ch, ok := rw.syncCh[gvr]; ok {
			return false, ch
		}
		closed := make(chan struct{})
		close(closed)
		return false, closed
	}

	rw.addResourceTypeMetadataOnlyLocked(gvr)
	ch := rw.syncCh[gvr]
	slog.Info("cache.lazy_register.metadata_only",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("path", "metadata-only"),
		slog.String("reason", "explicit"),
		slog.String("hint", "PartialObjectMetadata informer — explicit caller, predicate bypassed"),
	)
	return true, ch
}

// IsMetadataOnly reports whether gvr was registered via the §0.30.93
// metadata-only path (PartialObjectMetadata informer). Useful for unit
// tests asserting routing decisions and for future observability metrics.
//
// Returns false for nil receivers, passthrough mode, or unknown GVRs.
// Safe for concurrent use; takes rw.mu in read mode.
func (rw *ResourceWatcher) IsMetadataOnly(gvr schema.GroupVersionResource) bool {
	if rw == nil {
		return false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	_, ok := rw.metadataOnly[gvr]
	return ok
}

// IsSynced reports whether the informer for gvr has completed its
// initial LIST. Returns false for nil receivers, passthrough mode,
// unknown GVRs, or in-flight initial sync. Cheap (RLock + map lookup
// + atomic HasSynced load).
//
// Used by the 0.30.95 resolver pivot (`dispatchViaInformer`) to gate
// cache-served reads behind first-LIST completion — pre-sync reads
// would return empty slices that look identical to "no objects exist",
// silently breaking widget JQ filters. The pivot falls through to
// apiserver until HasSynced flips true.
//
// Safe for concurrent use.
func (rw *ResourceWatcher) IsSynced(gvr schema.GroupVersionResource) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	rw.mu.RUnlock()
	if !ok {
		return false
	}
	return gi.Informer().HasSynced()
}

// addResourceTypeLocked is the lock-held implementation of
// AddResourceType. Callers MUST hold rw.mu.Lock().
//
// 0.30.5: every newly-added informer also has the SetTransform strip
// installed — including informers added lazily after Start(). For
// post-Start registration SetTransform returns an error (cannot mutate
// a running informer); we log it as a WARN so the regression is
// observable but do NOT fail the registration (the cache still works,
// just at higher memory cost for that GVR).
//
// 0.30.6: post-eager-registration calls into AddResourceType are
// expected to be rare — the inventory walker is supposed to cover the
// full RestAction-derived GVR set. When a lazy registration fires AND
// eager registration has completed AND the GVR was in the eager
// inventory set, we emit `lazy-AddResourceType-unexpected` so the
// regression is loud. Lazy registration for a GVR NOT in the eager
// inventory is normal (e.g. customer-added RestAction post-startup).
func (rw *ResourceWatcher) addResourceTypeLocked(gvr schema.GroupVersionResource) {
	// 0.30.71 + 0.30.8: defensive guard. AddResourceType already
	// early-returns in modePassthrough, and NewResourceWatcher's
	// passthrough branch never reaches addResourceTypeLocked (no
	// factory exists in passthrough mode). This re-asserts the
	// invariant in case a future caller routes around AddResourceType
	// — without it, rw.factory.ForResource(gvr) on the next line
	// would nil-panic.
	if rw.mode == modePassthrough {
		return
	}
	if _, exists := rw.informers[gvr]; exists {
		return
	}

	// R6 (0.30.115): removable GVRs get a STANDALONE informer, not a
	// shared-factory one. The shared dynamicSharedInformerFactory caches
	// informers by GVR with no eviction API, so a factory-built informer
	// torn down by RemoveResourceType would be handed back — stopped and
	// frozen — by a later EnsureResourceType (CRD delete→recreate). A
	// standalone informer is owned outright: RemoveResourceType drops it
	// and a re-register constructs a fresh one. NewFilteredDynamicInformer
	// is the exact constructor the shared factory calls internally
	// (client-go dynamicinformer/informer.go:84), with the SAME
	// listOptionsTweak — paging/strip policy is byte-identical.
	//
	// The removable discriminator is IsNavigationDiscoveredGroup — a GVR
	// is removable iff its group is one the walker has reached via a
	// templated apiserver path (composition GVRs). Renamed from the
	// pre-v6 accessor in Ship 0.5 / 0.30.223 (the walker-driven
	// discovery design that deletes the CRD informer). Semantics
	// identical — same navigation-derived predicate, same removable-iff-
	// walker-discovered contract.
	var gi informers.GenericInformer
	indexers := clientcache.Indexers{clientcache.NamespaceIndex: clientcache.MetaNamespaceIndexFunc}

	// Ship H5 — THE ROUTING INVERSION. Bytes-streaming is now the
	// DEFAULT for every dynamic informer; the stock NewFilteredDynamic-
	// Informer / factory path is reachable only as a principled
	// EXCEPTION (typed-RBAC) or as a failure fallback.
	//
	// WHY (the whack-a-mole H5 ends): H1..H4 grew a per-group allow-list
	// — bytesResourceOverrides — that had to be edited each time a new
	// group's stock informer surfaced as a NewFilteredDynamicInformer
	// .func3 heap offender (composition at H1-H2a, widgets at H4). A
	// future GVR re-created `func3` until someone noticed and edited the
	// list. H5 inverts the rule: streaming unless excepted. No allow-list
	// — so no future GVR can silently re-create `func3`.
	//
	// THE EXCEPTION — isStreamingException(gvr) (strip.go), true iff the
	// GVR has a typed-converting override (typedResourceOverrides — the
	// 4 typed-RBAC GVRs). typed-RBAC genuinely REQUIRES the stock
	// informer: stripAndType consumes a *unstructured.Unstructured, and
	// a *bytesObject from the streaming ListFunc would fail its cast.
	// The exception is a declarative discriminant (a GVR has a typed
	// override iff it has a purpose-built typed Go representation), not
	// a hardcoded literal — feedback_no_special_cases-clean.
	//
	// SINGLE SOURCE OF TRUTH: isStreamingException drives BOTH this
	// informer-routing choice AND the strip.go bytes-override re-gate —
	// one predicate, two call sites, cannot drift.
	//
	// Streaming is attempted for every non-excepted GVR. Gated by
	// RESOLVER_COMPOSITION_STREAMING_LIST (default ON — see the scope
	// note on envCompositionStreamingList: post-H5 it governs ALL
	// informers) AND a wired *rest.Config. When the GVR is excepted, OR
	// the toggle is off, OR newStreamingDynamicInformer cannot build its
	// REST client, gi stays nil and we fall through to the
	// stock-informer path below.
	if !isStreamingException(gvr) && compositionStreamingListEnabled() {
		if sgi, ok := newStreamingDynamicInformer(
			rw.restConfig, rw.dyn, gvr, indexers, listOptionsTweak,
		); ok {
			gi = sgi
			slog.Info("cache.streaming_list.informer_routed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("path", "streaming-listwatch"),
				slog.String("hint", "H5 — bytes-streaming is the default for every dynamic informer"),
			)
		}
	}

	// Stock-informer path — Ship H5: the EXCEPTION / FAILURE fallback,
	// no longer the default. Reached when gi is still nil: the GVR is a
	// typed-RBAC exception, OR the streaming toggle is off, OR
	// newStreamingDynamicInformer could not build its REST client
	// (the CACHE_ENABLED / toggle fallback — AC-6).
	//
	// R6 (0.30.115): removable GVRs get a STANDALONE informer, not a
	// shared-factory one. The shared dynamicSharedInformerFactory caches
	// informers by GVR with no eviction API, so a factory-built informer
	// torn down by RemoveResourceType would be handed back — stopped and
	// frozen — by a later EnsureResourceType (CRD delete→recreate). A
	// standalone informer is owned outright: RemoveResourceType drops it
	// and a re-register constructs a fresh one. NewFilteredDynamicInformer
	// is the exact constructor the shared factory calls internally
	// (client-go dynamicinformer/informer.go:84), with the SAME
	// listOptionsTweak — paging/strip policy is byte-identical.
	//
	// The removable discriminator is IsNavigationDiscoveredGroup — a GVR
	// is removable iff its group is one the walker has reached via a
	// templated apiserver path (composition GVRs). Renamed from the
	// pre-v6 accessor in Ship 0.5 / 0.30.223 (the walker-driven
	// discovery design that deletes the CRD informer). Semantics
	// identical — same navigation-derived predicate, same removable-iff-
	// walker-discovered contract.
	if gi == nil {
		standalone := IsNavigationDiscoveredGroup(gvr.Group)
		if standalone {
			gi = dynamicinformer.NewFilteredDynamicInformer(
				rw.dyn,
				gvr,
				metav1.NamespaceAll,
				0, // resync period 0 — pure event-driven, matches the factory
				indexers,
				listOptionsTweak,
			)
		} else {
			gi = rw.factory.ForResource(gvr)
		}
	}
	rw.informers[gvr] = gi
	// Ship 1 / 0.30.225 — record GVR insert timestamp for the
	// /debug/vars snowplow_plurals_registered_gvrs envelope's
	// last_register_unix_ns field. The existence-check at the head
	// of this function early-returns for idempotent re-entries, so
	// reaching this line implies a genuine new insertion.
	NotifyGVRRegistered()

	// 0.30.9 Sub-scope B: allocate the sync channel BEFORE we spawn
	// any goroutine that could close it. The channel is closed by
	// the late-registration sync-watcher (below, in the rw.started
	// branch) or by the constructor-driven post-Start sync watcher
	// (registered in NewResourceWatcher's terminal block). We
	// allocate the channel here unconditionally so EnsureResourceType
	// can return it for either path.
	if rw.syncCh == nil {
		rw.syncCh = map[schema.GroupVersionResource]chan struct{}{}
	}
	rw.syncCh[gvr] = make(chan struct{})

	// R6 (0.30.115): allocate the per-GVR stop channel. Both the
	// informer's Run goroutine and its sync-watcher are bound by THIS
	// channel — not the process-wide rw.stopCh — so RemoveResourceType
	// can cancel exactly this GVR. Stop() reaps every channel still
	// present, so global shutdown is unchanged.
	rw.perGVRStopLocked(gvr)

	resourceType := gvrResourceTypeString(gvr)
	tf := StripBulkyFieldsForResourceType(resourceType, gvr)
	if err := gi.Informer().SetTransform(tf); err != nil {
		slog.Warn("cache.strip.set_transform_failed",
			slog.String("subsystem", "cache"),
			slog.String("resource_type", resourceType),
			slog.String("error", err.Error()),
			slog.Bool("post_start", rw.started),
		)
	}

	// 0.30.8 / 0.30.110: dep-tracker event hooks for the L1
	// resolved-output cache. Installed at registration time so every
	// newly-added informer gains wiring on first use (covers both
	// eager + lazy AddResourceType paths).
	//
	// Ship A (0.30.110): the handler set is now built by the SHARED
	// depEventHandlers — ADD is wired (post-sync gated), UPDATE
	// dirty-marks, DELETE classifies + evicts self-representations via
	// a worker goroutine. Per feedback_l1_invalidation_delete_only.md
	// eviction stays DELETE-only; ADD/UPDATE only dirty-mark.
	//
	// Mode-gating (0.30.71 + 0.30.8): these handlers are wired ONLY
	// in modeInformer. In modePassthrough the early-return at the
	// top of this function fires; in modePassthrough L1 is also off
	// (ResolvedCache() returns nil) so a dep tracker without a store
	// would record forward edges that the watcher could never
	// invalidate — wiring them at all in passthrough is wasted work.
	//
	// The handlers run on the informer's processor goroutine. ADD and
	// UPDATE complete in O(deps-for-this-tuple); DELETE classification
	// is likewise cheap and the eviction burst is handed to the R3
	// worker so it never blocks the processor.
	if _, regErr := gi.Informer().AddEventHandler(rw.depEventHandlers(gvr)); regErr != nil {
		slog.Warn("cache.deps.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.String("resource_type", resourceType),
			slog.String("error", regErr.Error()),
		)
	}

	// Ship 0 / 0.30.222 — declarative handler-extension registry.
	// Pre-Ship-0 this site carried an inline `if isTypedRBACGVR(gvr)`
	// branch attaching `rbacSnapshotEventHandlers`, plus a separate
	// `StartCRDWatch` entry point installing the CRD composition-auto-
	// discovery handlers. Both wirings are now declared from their
	// owner packages' init() (rbac_snapshot.go; the CRD-watch entry
	// was deleted with the CRD informer in Ship 0.5) and attached blind
	// from here — addResourceTypeLocked carries zero
	// GVR literals (feedback_no_special_cases.md).
	//
	// The handler sets themselves are unchanged. attachMatching* logs
	// per-failure via the registry; this site stays silent on attach
	// failures (the contract is "log loud at the owner's branch", not
	// here).
	attachMatchingHandlerExtensions(rw, gvr, gi.Informer())

	// 0.30.99 Tag B — watch-handler coverage guard. Install the
	// conjunct-3 WATCH-error handler UNCONDITIONALLY here, at
	// registration time, NOT only in the post-Start branch below.
	//
	// Pre-0.30.99 the install lived inside `if rw.started`, so a
	// pre-Start lazy-register (an EnsureResourceType call landing before
	// NewResourceWatcher's factory.Start) would register an informer
	// with NO handler — and the constructor's install-loop had already
	// run, so nothing would ever install it. Moving the call out of the
	// branch closes that gap STRUCTURALLY: every registration funnels
	// through addResourceTypeLocked, so every informer gets a handler.
	//
	// SetWatchErrorHandler only errors if the informer has already
	// started — pre-Start it has not, so the call succeeds; post-Start
	// the informer's Run goroutine has not been spawned yet at THIS
	// point (it is spawned a few lines below), so it likewise succeeds.
	// The constructor-loop install for the RBAC GVRs already ran before
	// addResourceTypeLocked is reachable post-Start, so a constructor
	// GVR re-entering here would be a redundant (idempotent) install —
	// but the constructor registers RBAC GVRs pre-Start via this same
	// function, so this is in fact the single install site for them
	// too once 0.30.99 lands. assertWatchHandlerCoverageLocked verifies
	// the invariant at boot.
	rw.installWatchErrorHandler(gvr, gi)

	if rw.started && !rw.stopRequestedLocked() {
		// Late registration after Start(): kick the new informer.
		// R6 (0.30.115): bound by the per-GVR stop channel, NOT
		// rw.stopCh, so RemoveResourceType can cancel this informer
		// alone. Stop() closes every per-GVR channel still present, so
		// global shutdown reaps this goroutine just as before.
		//
		// Task #85 — the stopRequestedLocked() guard makes goroutineWG.Add
		// race-free against a concurrent Stop(): both this Add and Stop's
		// rw.stopCh close happen under rw.mu, so either we spawn BEFORE
		// Stop closes stopCh (Add happens-before Stop's Wait, counter > 0)
		// or we observe the closed stopCh and skip (no Add). A WaitGroup
		// Add starting from a zero counter never races the Wait. Skipping
		// is correct on its own terms too: a freshly-spawned informer would
		// see the already-closed stop channel and exit immediately anyway.
		stop := rw.informerStop[gvr]
		rw.goroutineWG.Add(1)
		go func(inf clientcache.SharedIndexInformer) {
			defer rw.goroutineWG.Done()
			inf.Run(stop)
		}(gi.Informer())

		// 0.30.9 Sub-scope B: spawn the sync-watcher for this GVR.
		// The watcher polls HasSynced (cheap atomic load in
		// client-go) and closes the sync channel as soon as the
		// informer's initial LIST is reconciled. We use a polling
		// loop bounded by the per-GVR stop channel so the goroutine
		// exits on RemoveResourceType OR Stop(). WaitForCacheSync
		// (client-go) uses the same polling primitive internally — we
		// re-implement it here so we don't need to allocate a context.
		//
		// Task #85: both goroutines are tracked by rw.goroutineWG so
		// Stop() drains them deterministically (the standalone informer
		// Run is NOT factory-tracked, so factory.Shutdown() alone would
		// not reap it).
		ch := rw.syncCh[gvr]
		rw.goroutineWG.Add(1)
		go func(hasSynced func() bool) {
			defer rw.goroutineWG.Done()
			waitInformerSync(hasSynced, ch, stop)
		}(gi.Informer().HasSynced)

		// 0.30.6 falsifier (plan §"Code-path falsifier"). If eager
		// registration has already completed AND this GVR was in
		// the eager set, the inventory walker missed it OR an
		// upstream caller is double-registering — either way the
		// SRE wants to see it.
		if rw.eagerDone {
			if _, wasInEager := rw.eagerSet[gvr]; wasInEager {
				slog.Warn("lazy-AddResourceType-unexpected",
					slog.String("subsystem", "cache"),
					slog.String("resource_type", resourceType),
					slog.String("hint", "was in eager inventory but registered lazily"),
				)
			} else {
				slog.Info("lazy-AddResourceType",
					slog.String("subsystem", "cache"),
					slog.String("resource_type", resourceType),
					slog.String("hint", "not in eager inventory — likely post-startup RestAction"),
				)
			}
		}
	}
}

// perGVRStopLocked returns the per-GVR stop channel for gvr, allocating
// it (and the backing map) on first use. Callers MUST hold rw.mu.Lock().
//
// R6 (0.30.115): the channel is the per-informer canceller — the
// informer's Run goroutine and its sync-watcher are both bound by it, so
// RemoveResourceType can stop exactly one informer. It is closed exactly
// once, by closePerGVRStopLocked, whether via RemoveResourceType
// (per-GVR teardown) or Stop() (global shutdown).
func (rw *ResourceWatcher) perGVRStopLocked(gvr schema.GroupVersionResource) chan struct{} {
	if rw.informerStop == nil {
		rw.informerStop = map[schema.GroupVersionResource]chan struct{}{}
	}
	if ch, ok := rw.informerStop[gvr]; ok {
		return ch
	}
	ch := make(chan struct{})
	rw.informerStop[gvr] = ch
	return ch
}

// closePerGVRStopLocked closes gvr's per-GVR stop channel exactly once.
// Callers MUST hold rw.mu.Lock(). The closed-check under the lock is the
// AC-R6.5 double-close guard: RemoveResourceType and Stop() both route
// every close through here, and rw.mu serialises them, so a
// RemoveResourceType racing a Stop() can never close the same channel
// twice (a double-close panics). A no-op when gvr has no channel.
func (rw *ResourceWatcher) closePerGVRStopLocked(gvr schema.GroupVersionResource) {
	ch, ok := rw.informerStop[gvr]
	if !ok {
		return
	}
	select {
	case <-ch:
		// Already closed — nothing to do.
	default:
		close(ch)
	}
}

// RemoveResourceType tears down exactly one GVR's informer — the R6
// (0.30.115) per-GVR informer-lifecycle teardown. It closes the GVR's
// per-GVR stop channel (which exits the informer's Run goroutine and its
// sync-watcher) and purges the GVR from every per-GVR map. The dep
// event handlers die with the informer goroutine — no separate
// unregistration needed.
//
// Pre-Ship-0.5 wired into the CRD-watch's DeleteFunc; Ship 0.5
// (0.30.223, v6) deleted that wiring along with the CRD informer.
// Re-wired at Ship L (0.30.246): the CRD-DELETE event bridge calls this
// from triggerCRDDelete (crd_discovery_side_effect.go) for each served
// GVR of the deleted CRD; unit-test fixtures (R6 self-heal coverage)
// also exercise it. The #117 periodic sweep was closed superseded
// (2026-06-12) — the event bridge + the 30s discovery refresher cover
// CRD removal.
//
// Idempotent (AC-R6.1): a second call for the same GVR, or a call for an
// unknown GVR, is a no-op — closePerGVRStopLocked tolerates a missing /
// already-closed channel, and the map deletes are no-ops.
//
// Nil-receiver / passthrough are no-ops (AC-R6.4): in modePassthrough no
// informer exists. GVR-keyed throughout — no per-resource switch
// (feedback_no_special_cases.md).
//
// Re-add correctness (R6 Option 1): removable GVRs run a STANDALONE
// informer (addResourceTypeLocked's IsNavigationDiscoveredGroup
// branch), NOT a shared-factory one. Deleting the GVR from rw.informers
// here drops the only reference to that standalone informer — nothing
// pins it. A later EnsureResourceType for the same GVR (a CRD
// delete→recreate) therefore constructs a FRESH standalone informer
// that lists + watches from scratch; it does not resurrect a stopped,
// frozen one. R6 is thus strictly more correct than pre-R6: no leaked
// goroutine AND no frozen
// store on recreate.
//
// Note: RemoveResourceType has two callers, both targeting lazily-
// registered (standalone, per-GVR-channel) GVRs whose group the CRD-watch
// reached after factory.Start:
//   - CRD DELETE teardown (triggerCRDDelete) — remove, no re-add; and
//   - the CRD schema-widen relist (triggerCRDSchemaRelist) — remove THEN
//     EnsureResourceType, which by the R6 contract above constructs a FRESH
//     standalone informer that re-LISTs under the now-current CRD schema
//     (the relist fires only AFTER AddNavigationDiscoveredGroup, so the
//     re-add takes the standalone path, never a frozen shared-factory one).
//
// Lazy registrations run their (standalone) informer on the per-GVR
// channel, so closing it genuinely stops the Run goroutine. The four RBAC
// bootstrap GVRs are factory-driven on rw.stopCh and are structurally never
// removed (and never schema-relisted — typed-RBAC GVRs are not widget GVRs).
func (rw *ResourceWatcher) RemoveResourceType(gvr schema.GroupVersionResource) {
	if rw == nil || rw.mode == modePassthrough {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if _, ok := rw.informers[gvr]; !ok {
		// Unknown GVR — nothing registered. Still attempt the channel
		// close + map deletes so a partial-state GVR is fully purged;
		// all are no-ops when the GVR is genuinely absent.
		rw.closePerGVRStopLocked(gvr)
		rw.deletePerGVRStateLocked(gvr)
		return
	}

	// Close the per-GVR stop channel — exits the informer's Run
	// goroutine and its sync-watcher.
	rw.closePerGVRStopLocked(gvr)
	// Purge every per-GVR map entry so no state outlives the informer.
	rw.deletePerGVRStateLocked(gvr)

	slog.Info("cache.resource_type.removed",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("note", "per-GVR informer torn down — Run goroutine + sync-watcher reaped, per-GVR state purged"),
	)
}

// deletePerGVRStateLocked removes gvr from every per-GVR map. Callers
// MUST hold rw.mu.Lock(). The single de-registration site so a future
// per-GVR map cannot be forgotten — every map keyed by GVR is purged
// here uniformly (feedback_no_special_cases.md: no map gets a carve-out).
func (rw *ResourceWatcher) deletePerGVRStateLocked(gvr schema.GroupVersionResource) {
	delete(rw.informers, gvr)
	delete(rw.syncCh, gvr)
	delete(rw.confirmed, gvr)
	delete(rw.watchBroken, gvr)
	delete(rw.informerStop, gvr)
	delete(rw.metadataOnly, gvr)
	delete(rw.lastSyncRV, gvr)
	delete(rw.watchHandlerInstalled, gvr)
}

// waitInformerSync polls the informer's HasSynced predicate and
// closes ch when it returns true OR stopCh is closed. Polled at
// 50ms — same cadence client-go uses internally (cache.WaitForCacheSync
// polls at 100ms; we use half that so callers see the live state a
// tick earlier on the first read). The goroutine is bounded by
// stopCh so Stop() reliably reaps it.
//
// Idempotent on ch close — we only close once. If stopCh fires before
// the informer syncs, ch is closed anyway so callers blocked on it
// unblock (they will see HasSynced()==false on a follow-up read and
// fall back to the apiserver path).
func waitInformerSync(hasSynced func() bool, ch chan struct{}, stopCh <-chan struct{}) {
	defer func() {
		select {
		case <-ch:
			// Already closed (e.g., constructor's bulk-sync path).
		default:
			close(ch)
		}
	}()
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	if hasSynced() {
		return
	}
	for {
		select {
		case <-stopCh:
			return
		case <-t.C:
			if hasSynced() {
				return
			}
		}
	}
}

// MarkEagerSet records the set of GVRs that were registered via the
// eager-registration pathway (Tag 0.30.6). After this call, lazy
// AddResourceType for any GVR in `eagerSet` emits the
// `lazy-AddResourceType-unexpected` WARN — the gap is a falsifier the
// PM gate verifies via `kubectl logs`.
//
// Calling MarkEagerSet with a nil slice is permitted (resets the
// eager-done flag back to false — used by tests).
//
// Safe for concurrent use.
func (rw *ResourceWatcher) MarkEagerSet(eagerSet []schema.GroupVersionResource) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if eagerSet == nil {
		rw.eagerSet = nil
		rw.eagerDone = false
		return
	}
	m := make(map[schema.GroupVersionResource]struct{}, len(eagerSet))
	for _, gvr := range eagerSet {
		m[gvr] = struct{}{}
	}
	rw.eagerSet = m
	rw.eagerDone = true
}

// metaNSName extracts (namespace, name) from an informer-event object.
// Returns ("", "") if obj is not a metav1-conforming runtime object.
//
// The function tolerates both *unstructured.Unstructured and any typed
// object that implements GetNamespace/GetName (the four RBAC types are
// typed; everything else is unstructured at 0.30.5+).
func metaNSName(obj interface{}) (string, string) {
	type nsNameAccessor interface {
		GetNamespace() string
		GetName() string
	}
	if a, ok := obj.(nsNameAccessor); ok {
		return a.GetNamespace(), a.GetName()
	}
	return "", ""
}

// gvrResourceTypeString renders gvr as "group/version/Resource" for the
// strip.applied falsifier log line. The core group renders as
// "core/v1/Resource" (rather than "/v1/Resource") so log readers don't
// need to special-case the empty-group case.
func gvrResourceTypeString(gvr schema.GroupVersionResource) string {
	group := gvr.Group
	if group == "" {
		group = "core"
	}
	return group + "/" + gvr.Version + "/" + gvr.Resource
}

// Start launches every registered informer and begins serving from the
// in-memory cache. Idempotent.
//
// At 0.30.4 NewResourceWatcher invokes Start() automatically after
// eager RBAC registration — callers normally do not need to call this
// directly. Future tags may use it for lazy GVR registration scenarios.
func (rw *ResourceWatcher) Start() {
	if rw.mode == modePassthrough {
		return
	}
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.started {
		return
	}
	rw.started = true
	rw.factory.Start(rw.stopCh)
}

// WaitForCacheSync blocks until every registered informer's local
// store is in sync with apiserver, or the timeout elapses. Returns nil
// on success, error on timeout or context cancellation.
//
// In modePassthrough (0.30.71) there is no informer to sync — every
// read goes to apiserver — so the function returns nil immediately.
func (rw *ResourceWatcher) WaitForCacheSync(ctx context.Context, timeout time.Duration) error {
	if rw.mode == modePassthrough {
		return nil
	}
	rw.mu.RLock()
	syncs := make([]clientcache.InformerSynced, 0, len(rw.informers))
	for _, gi := range rw.informers {
		syncs = append(syncs, gi.Informer().HasSynced)
	}
	rw.mu.RUnlock()

	if len(syncs) == 0 {
		return nil
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if !clientcache.WaitForCacheSync(cctx.Done(), syncs...) {
		return fmt.Errorf("cache: sync timeout after %s", timeout)
	}
	return nil
}

// WaitForGVRSync blocks until gvr's informer HasSynced, or the timeout /
// ctx elapses — a per-GVR bounded twin of WaitForCacheSync (#121 1a).
//
// Returns true iff the informer is registered AND reached HasSynced within
// the bound. Returns false for: an unregistered gvr (nothing to wait on),
// passthrough mode (no informer), a nil receiver, or a timeout. A false
// return is the signal for the caller to fall through to its live-apiserver
// path — the bound guarantees the wait can never wedge boot (C1 "never
// worse": a genuinely-unsyncable GVR costs at most `timeout`, then the live
// LIST runs exactly as today).
//
// This waits ONLY on conjunct 2 (HasSynced), not the full four-conjunct
// IsServable — the caller re-checks IsServable after this returns true, so a
// GVR that syncs but is watch-broken / unconfirmed still correctly falls
// through. Reuses client-go's WaitForCacheSync on the single informer's
// HasSynced (feedback_check_k8s_clientgo_prior_art — same primitive as the
// package's other sync gates).
func (rw *ResourceWatcher) WaitForGVRSync(ctx context.Context, gvr schema.GroupVersionResource, timeout time.Duration) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	rw.mu.RLock()
	gi, registered := rw.informers[gvr]
	rw.mu.RUnlock()
	if !registered {
		return false
	}
	// Already synced — no wait. (The common 1a case: benchapps synced long
	// before the walk's LIST dispatch, so this returns true immediately.)
	if gi.Informer().HasSynced() {
		return true
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return clientcache.WaitForCacheSync(cctx.Done(), gi.Informer().HasSynced)
}

// passthroughGetTimeout bounds the apiserver Get/List call in
// modePassthrough so a stalled apiserver cannot wedge a caller
// indefinitely. 30s mirrors the dynamic.Client default behaviour
// elsewhere in snowplow.
const passthroughGetTimeout = 30 * time.Second

// GetObject returns the unstructured object for (gvr, namespace,
// name) or (nil, false) when missing.
//
// In modeInformer (cache=on) the lookup is served from the informer
// indexer in O(1).
//
// In modePassthrough (cache=off + dyn provided, 0.30.71) the lookup
// is routed to apiserver via the dynamic client. Each call is a fresh
// apiserver Get; there is NO in-process caching. This is the "true
// cache-off" path the diagnostic mode promises.
func (rw *ResourceWatcher) GetObject(gvr schema.GroupVersionResource, namespace, name string) (*unstructured.Unstructured, bool) {
	if rw.mode == modePassthrough {
		ctx, cancel := context.WithTimeout(context.Background(), passthroughGetTimeout)
		defer cancel()
		var uns *unstructured.Unstructured
		var err error
		if namespace == "" {
			uns, err = rw.dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		} else {
			uns, err = rw.dyn.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		}
		if err != nil || uns == nil {
			return nil, false
		}
		return uns, true
	}

	// Hardening #1 (serve_requires_servable): hold rw.mu across the
	// servability gate AND the indexer read off the SAME gi handle —
	// closing the GET check-then-act gap (the caller's IsServable precheck
	// and this read were otherwise two separate lock acquisitions). A
	// not-servable GVR returns a silent miss (the CORRECT fallback — the
	// caller falls through to apiserver). The serve_requires_servable
	// assert guards the HIT path as defense-in-depth.
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false
	}

	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}

	obj, exists, err := gi.Informer().GetIndexer().GetByKey(key)
	if err != nil || !exists {
		return nil, false
	}

	// Defense-in-depth: assert the invariant at the serve (we are about to
	// return obj,true). Holds by construction (gi came from servableLocked)
	// — the assert trips if a future refactor reads the indexer without it.
	if !rw.assertServeRequiresServableLocked(gvr, "GetObject") {
		return nil, false
	}

	// Ship H1 — decode-on-access (FINDING 1, cast site 1/5). The
	// indexer value may be a *bytesObject (composition group, the
	// GC-lean representation) or a plain *unstructured.Unstructured
	// (every other GVR, and the CACHE_ENABLED=false path).
	// decodeBytesObject handles both; a bytesObject is decoded to a
	// fresh Unstructured, never silently dropped.
	uns, ok := decodeBytesObject(obj)
	if !ok {
		return nil, false
	}
	return uns, true
}

// ListObjects returns every object for gvr scoped to namespace.
// Pass empty string for cluster-wide listing.
//
// In modeInformer (cache=on) the list is served from the informer
// indexer in O(N) over the namespace partition.
//
// In modePassthrough (cache=off + dyn provided, 0.30.71) the list is
// routed to apiserver via the dynamic client with the same
// listPageLimit bounded paging policy used by the informer factory.
// Each call is a fresh apiserver LIST; there is NO in-process
// caching. Paging is iterated until Continue is empty so callers see
// the full set.
func (rw *ResourceWatcher) ListObjects(gvr schema.GroupVersionResource, namespace string) []*unstructured.Unstructured {
	if rw.mode == modePassthrough {
		return rw.listPassthrough(gvr, namespace)
	}
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	rw.mu.RUnlock()

	if !ok {
		return nil
	}
	return listFromIndexer(gi, namespace)
}

// listFromIndexer materializes the namespace-scoped slice for an
// already-resolved informer. Pass empty namespace for cluster-wide.
// Shared by ListObjects and ListObjectsServable so both render the
// indexer partition identically (the byte-equivalence the resolver
// pivot's JQ pipeline depends on — `feedback_cache_must_not_constrain_jq.md`).
func listFromIndexer(gi informers.GenericInformer, namespace string) []*unstructured.Unstructured {
	store := gi.Informer().GetIndexer()
	var items []interface{}
	if namespace == "" {
		items = store.List()
	} else {
		idx, err := store.ByIndex(clientcache.NamespaceIndex, namespace)
		if err != nil {
			items = filterByNamespace(store.List(), namespace)
		} else {
			items = idx
		}
	}

	out := make([]*unstructured.Unstructured, 0, len(items))
	for _, it := range items {
		// Ship H1 — decode-on-access (FINDING 1, cast site 2/5).
		// Shared by ListObjects + ListObjectsServable. A bytesObject
		// is decoded to a fresh Unstructured; a missed conversion
		// here would silently shrink every composition list.
		if uns, ok := decodeBytesObject(it); ok {
			out = append(out, uns)
		}
	}
	return out
}

// ListObjectsServable returns (items, true) only when the watcher can
// VOUCH for the answer: in modeInformer that means the GVR has a
// registered informer AND its initial LIST has completed (HasSynced).
// (nil, false) is returned for a nil receiver, an unregistered GVR, or
// a registered-but-not-yet-synced informer — in every such case the
// caller MUST fall through to the apiserver rather than emit an empty
// list it cannot distinguish from a genuine "no objects" answer.
//
// In modePassthrough the call routes to the apiserver via the dynamic
// client (listPassthrough) and the result is authoritative, so it
// returns (routed-list, true).
//
// 0.30.97: this method exists to close the check-then-act gap in the
// 0.30.95 resolver pivot. The pivot previously did IsSynced(gvr) then
// ListObjects(gvr,...) as two separate lock acquisitions — between them
// the registered/synced state could flip, or HasSynced() could report
// true while the indexer partition was still draining, yielding a
// transiently-empty slice served as `served=true`. The registered+synced
// check and the indexer read now live behind ONE method.
//
// 0.30.98 Tag A: the servability check is the four-conjunct
// servableLocked predicate — registered AND HasSynced AND watchHealthy
// AND resourceTypeConfirmed. The indexer is read off the SAME gi handle
// that servableLocked just vouched for, all under one rw.mu read hold.
// A genuinely-empty-but-synced-confirmed informer still returns
// ([], true) — that is a real answer the watcher can vouch for and MUST
// keep serving. An unconfirmed post-startup-CRD GVR returns (nil, false)
// — the S4 fix (regression journal 2026-05-15).
//
// Per `feedback_no_special_cases.md`: a uniform predicate over GVRs —
// no per-GVR carve-out.
//
// Safe for concurrent use; takes rw.mu in read mode.
func (rw *ResourceWatcher) ListObjectsServable(gvr schema.GroupVersionResource, namespace string) ([]*unstructured.Unstructured, bool) {
	if rw == nil {
		return nil, false
	}
	if rw.mode == modePassthrough {
		return rw.listPassthrough(gvr, namespace), true
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false
	}
	// Defense-in-depth: assert the invariant AT THE SERVE (the moment we
	// return data,true). gi was just resolved by servableLocked so this
	// holds by construction today; the assert is the guard that trips
	// (test-panic / prod-count="serve_requires_servable") if a future
	// refactor returns a HIT here without the predicate.
	if !rw.assertServeRequiresServableLocked(gvr, "ListObjectsServable") {
		return nil, false
	}
	return listFromIndexer(gi, namespace), true
}

// ListServableEnvelopeJSON serves gvr's namespace-scoped (or cluster-wide
// when namespace=="") item set from the informer indexer as a marshalled
// K8s LIST envelope — the informer-served form of the internal-dispatch
// paged LIST (#121 1a). Returns (bytes, true) only when the GVR is servable
// (the SAME four-conjunct IsServable gate ListObjectsServable enforces);
// (nil, false) otherwise, signalling the caller to fall through to the live
// apiserver LIST (never worse).
//
// ENVELOPE PARITY (the byte-parity contract): the returned bytes are
// json.Marshal of an unstructured.UnstructuredList's UnstructuredContent() —
// the EXACT marshal path the live internal-dispatch LIST uses
// (internal_dispatch.go), so the {apiVersion, kind, metadata, items} shape
// and the per-item bytes are identical. The List-level apiVersion/kind are
// synthesized from the items' own GroupVersionKind (`<Kind>List`), matching
// the apiserver's collection-kind convention. Two DELIBERATE differences
// from a fresh live LIST, both provably JQ-invariant for the composition-
// list consumers and both matching what the live path ALREADY does:
//   - metadata.resourceVersion is empty. A cached snapshot has no single
//     live collection RV, and the live paged path pins page-1's RV but the
//     downstream composition-list JQ reads only `.items[]` (never the
//     list-level RV). Leaving it empty is the honest, drift-free choice.
//   - metadata.continue is empty and remainingItemCount is nil — identical
//     to the live path, which clears both on the accumulated list.
//
// The dev's byte-parity golden asserts items-equality + envelope-shape-
// equality against a live LIST modulo these two fields (see the 1a
// falsifier). No per-GVR carve-out — uniform over every servable GVR
// (feedback_no_special_cases).
func (rw *ResourceWatcher) ListServableEnvelopeJSON(gvr schema.GroupVersionResource, namespace string) ([]byte, bool) {
	items, ok := rw.ListObjectsServable(gvr, namespace)
	if !ok {
		return nil, false
	}
	list := &unstructured.UnstructuredList{}
	// Synthesize the collection envelope from the items' own GVK. Every
	// informer item carries its singular apiVersion/kind; the LIST kind is
	// the apiserver convention `<Kind>List`. When the set is empty we cannot
	// read a per-item GVK — fall back to the GVR's group/version and a
	// resource-derived kind is not available, so leave the envelope kind
	// empty (the live empty-LIST path likewise carries only whatever the
	// apiserver returned; the composition-list JQ reads `.items[]`, which is
	// an empty array here — shape-correct).
	list.Items = make([]unstructured.Unstructured, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		list.Items = append(list.Items, *it)
	}
	if len(list.Items) > 0 {
		gvk := list.Items[0].GroupVersionKind()
		if gvk.Kind != "" {
			list.SetAPIVersion(gvk.GroupVersion().String())
			list.SetKind(gvk.Kind + "List")
		}
	}
	raw, err := json.Marshal(list.UnstructuredContent())
	if err != nil {
		return nil, false
	}
	return raw, true
}

// IsServable reports whether the watcher can vouch for a cache-served
// read of gvr. Returns false for a nil receiver or passthrough mode (no
// informers exist — callers route directly to the apiserver).
//
// 0.30.98 Tag A: IsServable is the GET-path entry to the SINGLE
// four-conjunct servability predicate (servableLocked):
//
//	servable(gvr) := registered(gvr) AND HasSynced(gvr)
//	                 AND watchHealthy(gvr) AND resourceTypeConfirmed(gvr)
//
// The fourth conjunct is the S4 fix: a registered+synced informer whose
// resource *type* did not exist at initial-LIST time (a post-startup
// CRD) latches HasSynced=true over an empty result. Without
// resourceTypeConfirmed the pivot served [] as servable=true, zeroing
// the Compositions feature (regression journal 2026-05-15).
//
// IsServable is intentionally NOT a superset of IsMetadataOnly: the
// metadata-only gate is a separate, orthogonal concern (it asks "does
// this informer carry full spec/status, or only ObjectMeta?"). Every
// current caller already checks IsMetadataOnly explicitly before the
// servability check, so folding it in here would duplicate the gate.
//
// Per feedback_no_special_cases.md: servableLocked is uniform over every
// GVR — no per-Resource carve-out, no hardcoded business-GVR list.
//
// Safe for concurrent use; takes rw.mu in read mode.
func (rw *ResourceWatcher) IsServable(gvr schema.GroupVersionResource) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	_, ok := rw.servableLocked(gvr)
	return ok
}

// ServabilitySnapshot is a diagnostic read-only view of the four
// servableLocked conjuncts for a single GVR, captured under ONE
// rw.mu.RLock() so the tuple is internally consistent (no check-then-act
// gap between four separate public accessors). It carries NO behaviour —
// it is consumed only by the internal_dispatch.list.serve_miss slog to
// discriminate WHY the informer-serve branch was not taken on a given
// boot. Task #130 F1.
type ServabilitySnapshot struct {
	Registered    bool
	HasSynced     bool
	WatchHealthy  bool
	TypeConfirmed bool
}

// ServabilitySnapshotFor returns the four servability conjuncts for gvr
// under a single read lock. Read-only: it inspects the same maps
// servableLocked reads and mutates nothing. On a nil / passthrough
// watcher every conjunct is false (the serve branch never engages there).
//
// Safe for concurrent use; takes rw.mu in read mode.
func (rw *ResourceWatcher) ServabilitySnapshotFor(gvr schema.GroupVersionResource) ServabilitySnapshot {
	if rw == nil || rw.mode == modePassthrough {
		return ServabilitySnapshot{}
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, registered := rw.informers[gvr]
	snap := ServabilitySnapshot{Registered: registered}
	if !registered {
		return snap
	}
	snap.HasSynced = gi.Informer().HasSynced()
	_, broken := rw.watchBroken[gvr]
	snap.WatchHealthy = !broken
	snap.TypeConfirmed = rw.resourceTypeConfirmedLocked(gvr)
	return snap
}

// servableLocked is the single four-conjunct servability predicate
// shared by IsServable and ListObjectsServable. Callers MUST hold rw.mu
// (read or write). Returns the resolved GenericInformer alongside the
// bool so ListObjectsServable can read the indexer off the SAME gi
// handle whose HasSynced was just observed true — no check-then-act gap.
//
// Conjuncts (all must hold):
//  1. registered    — gvr has an entry in rw.informers.
//  2. HasSynced      — the informer's initial LIST has reconciled.
//  3. watchHealthy   — the reflector's WATCH connection is not broken
//     (gvr absent from rw.watchBroken).
//  4. typeConfirmed  — the resource *type* is verified present in the
//     apiserver's served API surface, OR no discovery client is wired
//     (degraded mode falls back to pre-0.30.98 HasSynced-only gating).
func (rw *ResourceWatcher) servableLocked(gvr schema.GroupVersionResource) (informers.GenericInformer, bool) {
	gi, ok := rw.informers[gvr]
	if !ok { // conjunct 1
		return nil, false
	}
	if !gi.Informer().HasSynced() { // conjunct 2
		return nil, false
	}
	if _, broken := rw.watchBroken[gvr]; broken { // conjunct 3
		return nil, false
	}
	if !rw.resourceTypeConfirmedLocked(gvr) { // conjunct 4 — the S4 fix
		return nil, false
	}
	return gi, true
}

// resourceTypeConfirmedLocked reports conjunct 4. Callers MUST hold
// rw.mu. When no discovery client is wired (rw.disco == nil) it returns
// true — a uniform degradation policy that preserves the pre-0.30.98
// HasSynced-only behaviour rather than disabling the pivot entirely.
// When a discovery client IS wired, a GVR is confirmed only after the
// discovery-refresh ticker has observed its resource type being served.
func (rw *ResourceWatcher) resourceTypeConfirmedLocked(gvr schema.GroupVersionResource) bool {
	if rw.disco == nil {
		return true
	}
	_, ok := rw.confirmed[gvr]
	return ok
}

// listPassthrough is the modePassthrough implementation of ListObjects.
// Iterates apiserver LIST with bounded paging until Continue is empty.
// Errors are swallowed (logged at debug) and a possibly-partial slice
// is returned — same contract the informer indexer gives on a partial
// sync: callers MUST be tolerant of empty / partial results.
func (rw *ResourceWatcher) listPassthrough(gvr schema.GroupVersionResource, namespace string) []*unstructured.Unstructured {
	ctx, cancel := context.WithTimeout(context.Background(), passthroughGetTimeout)
	defer cancel()

	var out []*unstructured.Unstructured
	var continueToken string
	for {
		opts := metav1.ListOptions{Limit: listPageLimit, Continue: continueToken}
		var list *unstructured.UnstructuredList
		var err error
		if namespace == "" {
			list, err = rw.dyn.Resource(gvr).List(ctx, opts)
		} else {
			list, err = rw.dyn.Resource(gvr).Namespace(namespace).List(ctx, opts)
		}
		if err != nil || list == nil {
			slog.Debug("cache.passthrough.list_failed",
				slog.String("subsystem", "cache"),
				slog.String("gvr", gvr.String()),
				slog.String("namespace", namespace),
				slog.Any("err", err),
			)
			return out
		}
		for i := range list.Items {
			item := list.Items[i]
			out = append(out, &item)
		}
		continueToken = list.GetContinue()
		if continueToken == "" {
			return out
		}
	}
}

func filterByNamespace(items []interface{}, ns string) []interface{} {
	out := make([]interface{}, 0, len(items))
	for _, it := range items {
		// Ship H1 — decode-on-access (FINDING 1, cast site 3/5).
		// This is a namespace FILTER, not a content read: a
		// bytesObject embeds ObjectMeta and therefore satisfies
		// the GetNamespace() accessor directly — no decode of `raw`
		// is needed to filter. We keep the ORIGINAL item in the
		// output slice (bytesObject or Unstructured) so the
		// subsequent listFromIndexer pass decodes it. A plain type
		// assert to *unstructured.Unstructured would silently drop
		// every bytesObject here.
		type nsAccessor interface{ GetNamespace() string }
		if a, ok := it.(nsAccessor); ok && a.GetNamespace() == ns {
			out = append(out, it)
		}
	}
	return out
}

// GetTypedObject returns the cached object for (gvr, namespace, name)
// as a runtime.Object — without the *unstructured.Unstructured
// type-assert that GetObject performs. Used by callers that opt into
// the 0.30.6 typed-converting transform: the indexer entry is a typed
// pointer (e.g. *rbacv1.ClusterRoleBinding) and the caller does the
// final type-assert at the call-site.
//
// Returns (nil, false) when the GVR is not registered, the key is
// missing, or the underlying object is nil.
//
// In modeInformer (cache=on) this serves typed pointers in O(1) with
// zero per-call FromUnstructured cost. In modePassthrough (cache=off
// + dyn provided, 0.30.71) the call routes to apiserver and returns
// the resulting *unstructured.Unstructured (which IS a runtime.Object)
// — the caller's as{Kind} helper in internal/rbac/evaluate.go falls
// through to the Unstructured fallback path (FromUnstructured per
// call). That is exactly the "original FromUnstructured-based RBAC
// evaluation" the diagnostic mode promises.
//
// For non-RBAC callers that still want the Unstructured (e.g.
// resolver-side reads), GetObject is preserved unchanged.
func (rw *ResourceWatcher) GetTypedObject(gvr schema.GroupVersionResource, namespace, name string) (runtime.Object, bool) {
	if rw.mode == modePassthrough {
		uns, ok := rw.GetObject(gvr, namespace, name)
		if !ok || uns == nil {
			return nil, false
		}
		return uns, true
	}
	// Hardening #1 (serve_requires_servable): same servable-under-lock
	// discipline as GetObject — gate the serve on servableLocked (silent
	// miss-and-fallthrough when not servable), read off the SAME gi handle,
	// assert at the HIT path as defense-in-depth.
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false
	}

	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}

	obj, exists, err := gi.Informer().GetIndexer().GetByKey(key)
	if err != nil || !exists {
		return nil, false
	}

	if !rw.assertServeRequiresServableLocked(gvr, "GetTypedObject") {
		return nil, false
	}

	// Ship H1 — decode-on-access (FINDING 1, cast site 4/5).
	// asRuntimeObject decodes a *bytesObject to an Unstructured
	// (which IS a runtime.Object) and passes through anything
	// already a runtime.Object (the typed-RBAC pointers). In
	// production the composition group — the only bytes-routed GVR —
	// is never read via GetTypedObject (that path serves the four
	// RBAC GVRs); this conversion exists so AC-H1.2's all-five-sites
	// guarantee holds even off the production path.
	robj, ok := asRuntimeObject(obj)
	if !ok {
		return nil, false
	}
	return robj, true
}

// ListTypedObjects returns every object for gvr scoped to namespace
// as []runtime.Object — without the *unstructured.Unstructured
// type-assert that ListObjects performs. Caller does the final type
// assert at the call-site.
//
// Pass empty namespace for cluster-wide listing.
//
// In modeInformer (cache=on) the list is served from the informer
// indexer with zero per-call FromUnstructured cost.
//
// In modePassthrough (cache=off + dyn provided, 0.30.71) the list is
// routed to apiserver and the returned []runtime.Object holds
// *unstructured.Unstructured values — callers' as{Kind} helpers fall
// through to FromUnstructured (the "original" RBAC path).
func (rw *ResourceWatcher) ListTypedObjects(gvr schema.GroupVersionResource, namespace string) []runtime.Object {
	if rw.mode == modePassthrough {
		uns := rw.listPassthrough(gvr, namespace)
		out := make([]runtime.Object, 0, len(uns))
		for _, u := range uns {
			out = append(out, u)
		}
		return out
	}
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	rw.mu.RUnlock()

	if !ok {
		return nil
	}

	store := gi.Informer().GetIndexer()
	var items []interface{}
	if namespace == "" {
		items = store.List()
	} else {
		idx, err := store.ByIndex(clientcache.NamespaceIndex, namespace)
		if err != nil {
			items = filterRuntimeByNamespace(store.List(), namespace)
		} else {
			items = idx
		}
	}

	out := make([]runtime.Object, 0, len(items))
	for _, it := range items {
		// Ship H1 — decode-on-access (FINDING 1, cast site 5/5).
		// asRuntimeObject decodes a *bytesObject; a missed
		// conversion would silently shrink the list.
		if robj, ok := asRuntimeObject(it); ok {
			out = append(out, robj)
		}
	}
	return out
}

// filterRuntimeByNamespace is the runtime.Object analogue of
// filterByNamespace. Used by ListTypedObjects only when the indexer
// is missing the NamespaceIndex (rare; defensive).
func filterRuntimeByNamespace(items []interface{}, ns string) []interface{} {
	out := make([]interface{}, 0, len(items))
	for _, it := range items {
		// metav1.Object interface gives us GetNamespace without type
		// assertion against either Unstructured or typed kinds.
		type nsAccessor interface{ GetNamespace() string }
		if a, ok := it.(nsAccessor); ok && a.GetNamespace() == ns {
			out = append(out, it)
		}
	}
	return out
}

// MatchingObjects returns cached objects in namespace whose labels
// match selector. Use this for label-selected reads instead of
// post-filtering ListObjects, when the indexer can short-circuit.
func (rw *ResourceWatcher) MatchingObjects(gvr schema.GroupVersionResource, namespace string, selector labels.Selector) []*unstructured.Unstructured {
	all := rw.ListObjects(gvr, namespace)
	if selector == nil || selector.Empty() {
		return all
	}
	out := make([]*unstructured.Unstructured, 0, len(all))
	for _, uns := range all {
		if selector.Matches(labels.Set(uns.GetLabels())) {
			out = append(out, uns)
		}
	}
	return out
}

// everyPerGVRMapClearForTest reports whether gvr is absent from every
// per-GVR map. TEST-ONLY surface for the R6 falsifier's leak assertion.
func (rw *ResourceWatcher) everyPerGVRMapClearForTest(gvr schema.GroupVersionResource) bool {
	if rw == nil {
		return true
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	_, inInformers := rw.informers[gvr]
	_, inSync := rw.syncCh[gvr]
	_, inConfirmed := rw.confirmed[gvr]
	_, inBroken := rw.watchBroken[gvr]
	_, inStop := rw.informerStop[gvr]
	_, inMeta := rw.metadataOnly[gvr]
	_, inRV := rw.lastSyncRV[gvr]
	_, inWHI := rw.watchHandlerInstalled[gvr]
	return !inInformers && !inSync && !inConfirmed && !inBroken &&
		!inStop && !inMeta && !inRV && !inWHI
}

// Stop signals every informer goroutine to exit. Idempotent.
//
// R6 (0.30.115): closing rw.stopCh reaps the factory-driven bootstrap
// informers (started via factory.Start(rw.stopCh)); the per-GVR
// channels reap the lazily-registered informers (whose Run is bound by
// rw.informerStop[gvr]). Stop() therefore closes BOTH — rw.stopCh once,
// then every per-GVR channel still present (RemoveResourceType already
// closed + purged the ones it tore down). closePerGVRStopLocked's
// closed-check makes a remaining-channel close a no-op if it somehow
// raced shut, so global shutdown stays exactly-once.
// stopRequestedLocked reports whether Stop() has already closed rw.stopCh
// (i.e. the watcher is shutting down). Callers MUST hold rw.mu (read or
// write). Task #85 — used by the late-registration path to skip spawning
// new informer goroutines once shutdown has begun, which keeps
// goroutineWG.Add ordered before Stop()'s Wait under rw.mu.
func (rw *ResourceWatcher) stopRequestedLocked() bool {
	select {
	case <-rw.stopCh:
		return true
	default:
		return false
	}
}

func (rw *ResourceWatcher) Stop() {
	// Phase 1 — signal every goroutine to exit. Done under rw.mu because
	// it mutates / reads the per-GVR channel map.
	rw.mu.Lock()
	select {
	case <-rw.stopCh:
		// Already closed.
	default:
		close(rw.stopCh)
	}

	// Reap every per-GVR informer channel still present. A channel a
	// RemoveResourceType already closed was also deleted from the map,
	// so it is not revisited here; closePerGVRStopLocked's closed-check
	// is the belt-and-braces guard against any residual race.
	for gvr := range rw.informerStop {
		rw.closePerGVRStopLocked(gvr)
	}
	factory := rw.factory
	rw.mu.Unlock()

	// Phase 2 — Task #85: BLOCK until every goroutine this watcher
	// started has exited. MUST run with rw.mu released: the drained
	// goroutines take rw.mu.RLock on their exit paths (e.g.
	// waitAndPublishInitialRBACSnapshot snapshots the per-GVR sync
	// channels under RLock), so waiting under the write lock would
	// deadlock.
	//
	//   - factory.Shutdown() drains the factory-driven bootstrap
	//     informers via the factory's own WaitGroup (client-go prior art:
	//     dynamicSharedInformerFactory.Shutdown does `defer wg.Wait()`).
	//     nil in modePassthrough (no factory was constructed).
	//   - goroutineWG.Wait() drains the watcher-owned goroutines that the
	//     factory does NOT track: the standalone informer Run goroutines,
	//     the per-GVR sync-watchers, and the initial-RBAC-snapshot
	//     publisher.
	//
	// After both return, no goroutine this watcher spawned is running, so
	// a subsequent ResetDepsForTest() in a neighbouring test cannot race
	// an in-flight depEventHandlers.func1 -> Deps() access.
	if factory != nil {
		factory.Shutdown()
	}
	rw.goroutineWG.Wait()
}

// global holds the cluster-wide ResourceWatcher singleton wired in
// main.go. Cache=on consumers read it via Global(); a nil return is the
// canonical cache=off branch signal.
//
// We accept a package-level singleton here because:
//   - the watcher is genuinely process-scoped (one factory per pod);
//   - threading it through every resolver call site would touch ~30
//     unrelated files for no behavioural gain;
//   - the cache=off branch is encoded as nil — there is no other
//     "disabled" state to model.
//
// Per feedback_no_special_cases.md the singleton holds no per-resource
// or per-user policy: it is a pointer or it is nil.
var (
	globalMu      sync.RWMutex
	globalWatcher *ResourceWatcher
)

// noteInformerEvent stamps the per-GVR last-event clock (1.12.5 / #187).
// Called from the informer bridge's Add/Update/Delete handlers, i.e. on the
// informer processor goroutine, so it must stay allocation-free on the repeat
// path: after the first event for a GVR this is one sync.Map load plus one
// atomic store.
func (rw *ResourceWatcher) noteInformerEvent(gvr schema.GroupVersionResource) {
	if rw == nil {
		return
	}
	now := time.Now().UnixNano()
	if v, ok := rw.lastEventAt.Load(gvr); ok {
		v.(*atomic.Int64).Store(now)
		return
	}
	stamp := &atomic.Int64{}
	stamp.Store(now)
	actual, _ := rw.lastEventAt.LoadOrStore(gvr, stamp)
	actual.(*atomic.Int64).Store(now)
}

// lastEventUnixNano returns the last-event stamp for gvr, or 0 if the bridge
// has never delivered an event for it.
func (rw *ResourceWatcher) lastEventUnixNano(gvr schema.GroupVersionResource) int64 {
	if rw == nil {
		return 0
	}
	v, ok := rw.lastEventAt.Load(gvr)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

// SetGlobal wires rw as the process-scoped ResourceWatcher. Called once
// from main.go after NewResourceWatcher succeeds. Passing nil clears
// the singleton — used by tests and by the cache=off path.
func SetGlobal(rw *ResourceWatcher) {
	globalMu.Lock()
	globalWatcher = rw
	globalMu.Unlock()
}

// Global returns the process-scoped ResourceWatcher or nil when the
// cache subsystem is disabled / not yet wired. Cache=on consumers MUST
// nil-check the return value.
func Global() *ResourceWatcher {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalWatcher
}
