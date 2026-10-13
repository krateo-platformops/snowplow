// cached_client.go — Task #322 (#318-R2) Commit 1. SA-singleton cached
// discovery client for ValidateObjectStatus.
//
// WHY — dynamic.NewClient(rc) constructs a FRESH memCacheClient +
// DeferredDiscoveryRESTMapper on every call (client.go:18-39). When
// ValidateObjectStatus (schema.go:82) builds one per child GET, the very
// first GVR-only KindFor re-downloads the full API surface from the
// apiserver every call (TRACED 14.72s / 2.13% of the 0.30.258 drain
// profile — getDelegate -> GetAPIGroupResources -> memCacheClient
// refreshLocked -> downloadAPIs). This file lifts that construction to a
// process singleton built ONCE from the SA rest.Config and reused across
// every call, so the discovery download is paid ONCE per process lifetime
// (warmed at boot by Phase 1 / the drain) and every subsequent KindFor is
// an O(1) in-memory lookup.
//
// THIS IS THE CACHING CORRECTION, NOT THE WithSkipMapper REVIVAL.
// The 0.30.226->0.30.231 6-revert saga tried to make KindFor free by
// REMOVING the mapper (dynamic.WithSkipMapper -> uc.mapper == nil ->
// nil-deref panic at resourceInterfaceFor client.go:139/:146). This file
// does the opposite: it caches the BUILT, always-non-nil, populated
// mapper and reuses it. The mapper is present at every deref site; there
// is no nil-mapper path. The boot-smoke test (cached_client_test.go)
// exercises exactly the resourceInterfaceFor(Options{GVR:...}) deref
// site the 4 reverts crashed on — the permanent gate from
// project_regression_journal.md:1846.
//
// INVALIDATION — the discovery map is invalidated ONLY via
// InvalidateSADiscovery(), wired (in main.go) to the EXISTING 0.30.233
// CRD-lifecycle bridge (internal/cache/crd_discovery_side_effect.go).
// The bridge fires DiscoverGroupResources / teardown on CRD ADD / UPDATE
// / DELETE and THEN invalidates, so the next ValidateObjectStatus for a
// new / changed / removed GVR rebuilds the mapper and sees fresh
// cluster shape. The ONLY runtime mutation of the GVR set snowplow
// resolves THROUGH ValidateObjectStatus is the CRD lifecycle (CRD-object
// schema is K8s-immutable for the object lifetime — plurals_resolver.go
// :5-13; spec.versions[]/served[] changes fire crdLifecycleUpdate ->
// triggerCRDDiscovery, deps_watch.go:234). Snowplow serves NO aggregated
// APIs (no apiregistration / APIService handling anywhere in internal/),
// so an aggregated-API GVR-set mutation with no CRD event is not
// reachable on this path. (Q3-a narrowing.)
//
// RBAC-SAFE — the cached object is cluster-shape metadata (discovery map
// + restmapper) used EXCLUSIVELY for cluster-scoped CRD/discovery reads
// with the SA identity (ValidateObjectStatus reads
// apiextensions.k8s.io/v1/customresourcedefinitions, a non-per-user
// resource snowplow uses to validate the widget envelope shape). It sits
// BELOW the per-user RBAC layer; sharing one SA discovery client across
// users changes no user's visible data. This is NOT a per-user data
// cache (feedback_l1_per_user_keyed_never_cohort guards user-data L1;
// this is shape metadata). The per-user NewClient sites
// (objects/get.go:193, handlers/list.go:68) are deliberately UNTOUCHED —
// an SA-singleton there WOULD leak.
//
// CONCURRENCY — mirrors the process-singleton SHAPE of sa_client.go
// (lazy build, no-cache-on-error, mutex-guarded pointer) but NOT its
// lifetime: sa_client.go is build-once-FOREVER, while this singleton
// REBUILDS when the rc identity changes (see the rc check below). In
// production the SA rc is one stable process-wide pointer, so the
// rebuild path never fires and the state is de-facto immutable after
// the first build. Our sync.RWMutex guards the singleton pointer (read
// on the hot path, written on first build + on an rc-identity change). The cached mapper's own mutation surface
// (KindFor builds the delegate lazily under client-go's initMu;
// Reset()/Invalidate() are initMu/lock-guarded inside client-go —
// discovery.go:216, memcache.go:221/235) is race-safe by construction.
// The shared-vs-private conversion (the 0.30.128 hazard class) is gated
// by a concurrent -race test (cached_client_race_test.go) that runs
// InvalidateSADiscovery() on a SEPARATE goroutine from concurrent
// KindFor readers (feedback_shared_vs_copy_is_a_concurrency_change).
//
// REMOVABLE — pure acceleration. ValidateObjectStatus falls back to
// dynamic.NewClient(rc) when SharedSADiscoveryClient errors (nil rc /
// startup race), so the path is never worse than today and cache-off
// keeps working (project_cache_off_is_transparent_fallback).

package dynamic

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// SA-discovery singleton observability counters (Task #326). atomic for
// lock-free reads; exposed via the snowplow_sa_discovery_* expvar family
// (cached_client_metrics.go), Disabled()-gated like the rest.
//
//   - saDiscoveryBuilds:        bumped once per SUCCESSFUL buildSADiscoveryState
//     (= one discovery download paid). In production this is ~1 per process
//     lifetime; a climbing value means the rc identity is churning (rebuild
//     path) or InvalidateSADiscovery is being mis-wired to rebuild.
//   - saDiscoveryInvalidations: bumped once per InvalidateSADiscovery that
//     actually resets a live mapper (the CRD-lifecycle bridge fired).
//   - saDiscoveryFallbacks:     bumped every time SharedSADiscoveryClient
//     returns an error (nil rc / construction failure) — i.e. every time the
//     ValidateObjectStatus caller (schema.go:124) falls back to a per-call
//     dynamic.NewClient. The fallback Warn fires schema-side, but the counter
//     lives here with its two siblings so the SA-discovery subsystem is the
//     single source of truth for its own metrics (mirrors phase1_pip_metrics
//     keeping the seed counters co-located). A non-zero value is the
//     post-deploy signal that the caching win is silently evaporating back to
//     per-call discovery downloads.
//   - saDiscoveryImpersonatedRejected: #608 — bumped every time a caller hands
//     SharedSADiscoveryClient an IMPERSONATING config, which it refuses. Zero
//     in a healthy process; on krateo-057 1.12.43 it would have read ~50,403
//     (one per refresh that rebuilt the singleton around a cohort's config).
var (
	saDiscoveryBuilds               atomic.Uint64
	saDiscoveryInvalidations        atomic.Uint64
	saDiscoveryFallbacks            atomic.Uint64
	saDiscoveryImpersonatedRejected atomic.Uint64
)

// SADiscoveryStats is a read-only snapshot of the SA-discovery singleton
// counters. Consumed by the expvar publishers (cached_client_metrics.go) and
// the metrics test. Mirrors schema.CRDSchemaMemoStats / the refresher snapshot
// shape.
type SADiscoveryStats struct {
	Builds               uint64
	Invalidations        uint64
	Fallbacks            uint64
	ImpersonatedRejected uint64
}

// SADiscoveryStatsSnapshot returns the current SA-discovery counters. Exported
// because the expvar publishers register lazy Funcs that read it at scrape
// time and the metrics test asserts monotonic transitions through it.
func SADiscoveryStatsSnapshot() SADiscoveryStats {
	return SADiscoveryStats{
		Builds:               saDiscoveryBuilds.Load(),
		Invalidations:        saDiscoveryInvalidations.Load(),
		Fallbacks:            saDiscoveryFallbacks.Load(),
		ImpersonatedRejected: saDiscoveryImpersonatedRejected.Load(),
	}
}

// saDiscoveryState is the process-singleton cached discovery client +
// the typed mapper handle needed to call Reset(). The client field is
// the same *unstructuredClient dynamic.NewClient returns; we hold the
// mapper separately because the Client interface does not expose it and
// InvalidateSADiscovery needs the typed *DeferredDiscoveryRESTMapper to
// call Reset().
type saDiscoveryState struct {
	rc     *rest.Config // identity key — production sets this once
	client Client
	mapper *restmapper.DeferredDiscoveryRESTMapper
}

var (
	// saDiscoveryMu guards the singleton pointer. RWMutex because the
	// hot path (SharedSADiscoveryClient on a warm singleton) only reads;
	// the write happens on first build + on an rc-identity change
	// (production: never after boot).
	saDiscoveryMu       sync.RWMutex
	saDiscoveryInstance *saDiscoveryState
)

// SharedSADiscoveryClient returns a process-singleton dynamic.Client
// whose memCacheClient + DeferredDiscoveryRESTMapper are reused across
// calls, so the discovery download is paid ONCE per process lifetime
// instead of per call. The discovery map is invalidated ONLY via
// InvalidateSADiscovery() (wired to the CRD-lifecycle bridge).
//
// Caches on first success — subsequent calls with the same rc identity
// return the SAME Client pointer (the warm mapper). On failure (e.g.
// dynamic.NewForConfig / discovery client construction error), returns
// the error and does NOT cache, so a later call can retry — identical
// contract to ServiceAccountRESTConfig (sa_client.go:158-174).
//
// RBAC invariant: rc MUST be the SA rest.Config (the ONLY rc that
// reaches the swapped ValidateObjectStatus site — widgets.go:228
// RC: r.saRC on the customer /call path; phase1_walk_pagination_jobs.go
// :433/:579 saRC on the drain path). If the rc identity changes
// (production never does this; a future cache-off transition might), the
// singleton is rebuilt against the new rc — the old mapper is GC'd.
//
// nil rc returns an error WITHOUT caching, so ValidateObjectStatus falls
// back to dynamic.NewClient(rc) (transparent fallback, never worse than
// today).
//
// #608 — THE INVARIANT IS ENFORCED, NOT DOCUMENTED. An IMPERSONATING rc is
// refused with ErrImpersonatingConfig and counted, and the singleton is left
// exactly as it was. Before this guard the refresher's per-refresh cohort
// config (a fresh rest.CopyConfig every time) rebuilt the "SA" singleton on
// every memo miss — 50,403 rebuilds and 50,189 refused GET /api on 057.
//
// Goroutine-safe.
func SharedSADiscoveryClient(rc *rest.Config) (Client, error) {
	if rc == nil {
		saDiscoveryFallbacks.Add(1) // caller falls back to per-call dynamic.NewClient
		return nil, fmt.Errorf("dynamic.SharedSADiscoveryClient: nil *rest.Config")
	}
	if IsImpersonatingConfig(rc) {
		saDiscoveryImpersonatedRejected.Add(1)
		return nil, ErrImpersonatingConfig
	}

	// Fast path: warm singleton for this rc identity.
	saDiscoveryMu.RLock()
	if saDiscoveryInstance != nil && saDiscoveryInstance.rc == rc {
		cli := saDiscoveryInstance.client
		saDiscoveryMu.RUnlock()
		return cli, nil
	}
	saDiscoveryMu.RUnlock()

	// Slow path: build (or rebuild on rc change) under the write lock.
	saDiscoveryMu.Lock()
	defer saDiscoveryMu.Unlock()

	// Re-check under the write lock — another goroutine may have built
	// it while we waited for the lock.
	if saDiscoveryInstance != nil && saDiscoveryInstance.rc == rc {
		return saDiscoveryInstance.client, nil
	}

	st, err := buildSADiscoveryState(rc)
	if err != nil {
		// Do NOT cache on failure — a transient boot-time construction
		// error must not poison the process lifetime.
		saDiscoveryFallbacks.Add(1) // caller falls back to per-call dynamic.NewClient
		return nil, err
	}
	saDiscoveryInstance = st
	return st.client, nil
}

// ErrImpersonatingConfig is returned by SharedSADiscoveryClient for a config
// that impersonates another identity: the singleton serves identity-free
// cluster metadata and is only ever built from snowplow's own SA transport.
var ErrImpersonatingConfig = errors.New("dynamic.SharedSADiscoveryClient: refusing an impersonating *rest.Config " +
	"(the SA discovery singleton serves identity-free cluster metadata; #608)")

// IsImpersonatingConfig reports whether rc asks the apiserver to act as
// another identity.
func IsImpersonatingConfig(rc *rest.Config) bool {
	if rc == nil {
		return false
	}
	imp := rc.Impersonate
	return imp.UserName != "" || imp.UID != "" || len(imp.Groups) > 0 || len(imp.Extra) > 0
}

// discoveryClientForConfigFn is the package-private indirection that lets
// unit tests swap discovery.NewDiscoveryClientForConfig for a fake
// discovery client (the boot-smoke + race falsifiers need to count
// downloadAPIs-equivalent calls without a real apiserver). Production
// code path is unchanged; the variable is initialised once to the real
// constructor and is only reassigned by test code. Mirrors
// inClusterConfigFn (sa_client.go:184) and discoveryClientBuilder
// (cache/discovery_lookup.go:150).
var discoveryClientForConfigFn = func(rc *rest.Config) (discovery.DiscoveryInterface, error) {
	return discovery.NewDiscoveryClientForConfig(rc)
}

// buildSADiscoveryState constructs the cached client + mapper. Mirrors
// the dynamic.NewClient(rc) internals (client.go:18-39) but RETAINS the
// typed mapper handle so InvalidateSADiscovery can call Reset(). The
// constructed mapper is the same always-non-nil, lazily-populated
// DeferredDiscoveryRESTMapper NewClient builds — we cache the BUILT
// mapper, we do not skip it.
func buildSADiscoveryState(rc *rest.Config) (*saDiscoveryState, error) {
	dynamicClient, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("dynamic.SharedSADiscoveryClient: NewForConfig: %w", err)
	}

	discoveryClient, err := discoveryClientForConfigFn(rc)
	if err != nil {
		return nil, fmt.Errorf("dynamic.SharedSADiscoveryClient: NewDiscoveryClientForConfig: %w", err)
	}

	mapper := restmapper.NewDeferredDiscoveryRESTMapper(
		cacheddiscovery.NewMemCacheClient(discoveryClient),
	)

	cli := &unstructuredClient{
		dynamicClient:   dynamicClient,
		discoveryClient: discoveryClient,
		mapper:          mapper,
		converter:       runtime.DefaultUnstructuredConverter,
	}

	saDiscoveryBuilds.Add(1) // one discovery download paid (Task #326)
	return &saDiscoveryState{rc: rc, client: cli, mapper: mapper}, nil
}

// InvalidateSADiscovery resets the cached discovery map so the next
// KindFor / RESTMapping re-downloads the API surface. Wired (in main.go)
// to the END of the CRD-lifecycle bridge's triggerCRDDiscovery (ADD /
// UPDATE) and triggerCRDDelete (DELETE) — AFTER DiscoverGroupResources /
// teardown — so a mid-run CRD install/change/removal cannot leave a
// stale discovery cache.
//
// No-op when the singleton has not been built yet (the next
// SharedSADiscoveryClient call will build a fresh mapper anyway).
//
// mapper.Reset() (discovery.go:213) calls memCacheClient.Invalidate() +
// nils the delegate, both initMu/lock-guarded inside client-go, so a
// concurrent KindFor during a Reset() is safe — the next KindFor rebuilds
// the delegate. We hold the RLock only to read the singleton pointer.
//
// Goroutine-safe.
func InvalidateSADiscovery() {
	saDiscoveryMu.RLock()
	st := saDiscoveryInstance
	saDiscoveryMu.RUnlock()
	if st == nil || st.mapper == nil {
		return
	}
	saDiscoveryInvalidations.Add(1) // count only resets that hit a live mapper (Task #326)
	st.mapper.Reset()
}

// resetSADiscoveryForTest clears the singleton so each test sees a fresh
// state. Exported via the _test.go shim only; production code MUST NOT
// call this. Mirrors resetSARestConfigForTest (sa_client.go:198-202).
func resetSADiscoveryForTest() {
	saDiscoveryMu.Lock()
	saDiscoveryInstance = nil
	saDiscoveryMu.Unlock()
	// Task #326 — zero the observability counters too so each test sees a
	// fresh baseline (mirrors resetCRDSchemaMemoForTest clearing its counters).
	saDiscoveryBuilds.Store(0)
	saDiscoveryInvalidations.Store(0)
	saDiscoveryFallbacks.Store(0)
}
