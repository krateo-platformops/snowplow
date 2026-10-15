// discovery_lookup.go — Ship 0.5 / 0.30.223 (v6 design,
// docs/walker-driven-informer-design-2026-06-01.md). REPLACES the
// pre-v6 CRD-watch file (459 LOC, deleted): the in-process LIST/WATCH
// informer against apiextensions.k8s.io/v1/customresourcedefinitions
// is gone. Composition GVRs are discovered by one-shot apiserver
// discovery (Discovery.ServerResourcesForGroupVersion) invoked
// synchronously from the walker the first time a templated apiserver
// path is reached for each navigation-discovered group.
//
// WHY v6 IS SAFE — synchronous one-shot has no replay window. The
// pre-v6 pattern had a boot replay-vs-discover race closed by a
// post-walk reconcile re-scan; v6 deletes the informer (no replay
// window). DiscoverGroupResources is a single synchronous transaction
// — list resources, register each, dirty-mark stale-negative LIST
// entries, return.
//
// LAG TRADEOFF — accepted per Diego 2026-06-01. CRD CREATE detected
// on the NEXT walker pass under that group (bounded by Phase 1 +
// widget/RESTAction CRUD re-walks). CRD DELETE is event-driven since
// Ship L (0.30.246): the CRD-meta informer's DeleteFunc feeds
// triggerCRDDelete, which tears the informer down and dirty-marks
// dependents; the 30s discovery refresher retracts servability as a
// backstop. The #117 periodic sweep was closed superseded (2026-06-12).
//
// PRESERVED: the navigation-discovered group set (renamed accessors,
// load-bearing for watcher.go:749/:1064 removable-discriminator) +
// the FD1 dirty-mark chain (Ship D / 0.30.114).
//
// REMOVED: the CRD informer + handlers + handler-extension entry +
// post-walk reconcile + per-CRD-object derivation + register/
// unregister event handlers.

package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	cacheddiscovery "k8s.io/client-go/discovery/cached/memory"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// navDiscoveredGroups is the set of apiserver groups that the walker
// has reached via a templated apiserver path. NAVIGATION-DERIVED —
// starts empty, populated only by AddNavigationDiscoveredGroup.
//
// LOAD-BEARING for watcher.go's removable-discriminator at :749 +
// :1064: a GVR is removable (gets a standalone informer, not a
// shared-factory one) iff its group is in this set. Composition GVRs
// MUST be removable so RemoveResourceType can tear them down via the
// CRD-DELETE event bridge (Ship L/0.30.246; the #117 periodic sweep
// was closed superseded) without affecting unrelated informers.
//
// Guarded by navDiscoveredGroupsMu (its own mutex, not rw.mu —
// callers may be inside rw.mu when they consult IsNavigationDiscoveredGroup).
var (
	navDiscoveredGroupsMu sync.RWMutex
	navDiscoveredGroups   = map[string]struct{}{}
)

// AddNavigationDiscoveredGroup records group as a navigation-discovered
// group. Idempotent. Pure set-add — NO informer side-effects (v6).
// Called by the Phase 1 walk for every static group it extracts from
// a templated apiserver path via
// ExtractAPIServerGroupFromTemplatedPath.
//
// The empty string is rejected — the core group ("") is never a
// composition group and admitting it would corrupt the removable-
// discriminator predicate (every core resource would be treated as
// removable, which is wrong: built-in informers must never be torn
// down).
//
// v6 NOTE: pre-Ship-0.5 this call also fired a sync.Once spawning the
// CRD informer. v6 deletes that side-effect; composition GVR discovery
// is now done by DiscoverGroupResources (synchronous apiserver
// discovery). The walker calls AddNavigationDiscoveredGroup(group) +
// DiscoverGroupResources(ctx, rc, group) at the same site (resolve.go).
func AddNavigationDiscoveredGroup(group string) {
	if group == "" {
		return
	}
	navDiscoveredGroupsMu.Lock()
	_, existed := navDiscoveredGroups[group]
	navDiscoveredGroups[group] = struct{}{}
	navDiscoveredGroupsMu.Unlock()
	if !existed {
		slog.Info("cache.discovery.navigation_discovered_group_added",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.String("note", "navigation-derived — extracted from a resolved templated apiserver path"),
		)
	}
}

// IsNavigationDiscoveredGroup reports whether group is in the
// navigation-discovered set. The single membership predicate the
// watcher's removable-discriminator (:749, :1064) consults — no per-
// resource carve-out.
//
// Renamed from the pre-v6 accessor in Ship 0.5 / 0.30.223. Semantics
// identical — same navigation-derived predicate, same removable-iff-
// walker-discovered contract.
func IsNavigationDiscoveredGroup(group string) bool {
	navDiscoveredGroupsMu.RLock()
	defer navDiscoveredGroupsMu.RUnlock()
	_, ok := navDiscoveredGroups[group]
	return ok
}

// NavigationDiscoveredGroupsSnapshot returns a copy of the current
// navigation-discovered group set. Observability + test helper.
//
// Renamed from the pre-v6 accessor in Ship 0.5 / 0.30.223.
func NavigationDiscoveredGroupsSnapshot() []string {
	navDiscoveredGroupsMu.RLock()
	defer navDiscoveredGroupsMu.RUnlock()
	out := make([]string, 0, len(navDiscoveredGroups))
	for g := range navDiscoveredGroups {
		out = append(out, g)
	}
	return out
}

// ResetNavigationDiscoveredGroupsForTest clears the navigation-
// discovered set. TEST-ONLY — the production lifecycle is append-only.
//
// Renamed from the pre-v6 helper in Ship 0.5 / 0.30.223.
func ResetNavigationDiscoveredGroupsForTest() {
	navDiscoveredGroupsMu.Lock()
	navDiscoveredGroups = map[string]struct{}{}
	navDiscoveredGroupsMu.Unlock()
}

// --- Discovery hop (v6 core) ----------------------------------------------

// discoveryClientBuilder builds a Discovery client from a *rest.Config.
// Indirected via a package-level var so unit tests can swap in a fake
// without standing up a real REST endpoint. The signature uses
// interface{} for the input so the test fake does not need to import
// k8s.io/client-go/rest just to satisfy the parameter type.
var discoveryClientBuilder = func(rc *rest.Config) (discovery.DiscoveryInterface, error) {
	if rc == nil {
		return nil, fmt.Errorf("nil *rest.Config")
	}
	return credhttp.Discovery(rc)
}

// discoverGroupSingleflight ensures that concurrent
// DiscoverGroupResources calls for the same group share a single
// apiserver discovery hop. Per `feedback_seed_inherits_nested_call_
// identity`: the walker may invoke the discovery path from multiple
// goroutines (Phase 1 + widget CRUD re-walks); without singleflight
// each goroutine would issue its own discovery LIST.
var discoverGroupSingleflight sync.Map // group(string) -> *sync.Mutex

// discoverGroupLock returns the per-group serialize-only mutex. The
// mutex serializes concurrent calls for the SAME group only; calls for
// distinct groups proceed in parallel.
//
// 2026-06-22 Fix A2 (discovery storm) — the discovery hop is NO LONGER
// unconditionally repeated on every walker re-entry. The apiserver
// round-trips are now served from a process-singleton cache-local
// CachedDiscoveryInterface (cachedDiscoveryClient below), and the hot
// (forceFresh:false) path short-circuits to (0,nil) when the cache is
// Fresh() AND every registerable GVR of every currently-served version
// is already registered (versionCompleteForGroup). This kills the
// ~21-round-trip residual the v6 "repeat every walk" design paid on
// already-known state, WITHOUT a once-flag: the short-circuit predicate
// is RECOMPUTED every call, so a newly-served version (spec.versions[]
// widen) leaves a GVR un-registered → predicate false → full discovery
// walk. The CRD-event path (DiscoverGroupResourcesFresh, forceFresh:
// true) Invalidate()s the cached discovery surface (memcache.Invalidate
// is a GLOBAL wipe — all groups; client-go has no per-group shard) and
// re-reads the apiserver BEFORE the registration walk, so a CREATE/UPDATE
// never reads stale. The global wipe is a bounded, accepted cost: after a
// (rare) CRD event the next hot-path call for ANY nav-group sees
// Fresh()==false and pays ONE shared aggregated-discovery refresh
// (refreshLocked downloads the whole surface in a single hop,
// memcache.go:242-260), NOT N storms. Registration itself stays
// idempotent via EnsureResourceType.
func discoverGroupLock(group string) *sync.Mutex {
	v, _ := discoverGroupSingleflight.LoadOrStore(group, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// --- Fix A2 — cache-local cached discovery client -------------------------

// cachedDiscoveryClient is the process-singleton CachedDiscoveryInterface
// that serves ServerGroups + ServerResourcesForGroupVersion from an
// in-memory cache (k8s.io/client-go/discovery/cached/memory —
// NewMemCacheClient, the SAME prior art internal/dynamic/client.go:30
// uses). The first read per process downloads the API surface; every
// subsequent read on the hot (forceFresh:false) path is an O(1) in-memory
// lookup — eliminating the ~21 apiserver round-trips/call the raw v6
// client paid (discovery storm RC-1 residual).
//
// LAYERING — built cache-LOCAL in internal/cache; does NOT import the
// internal/dynamic SA-discovery singleton (internal/cache is BELOW
// internal/dynamic — importing up would invert the layering wall). It
// wraps whatever discoveryClientBuilder returns (the same test seam the
// fakes already swap), so unit tests need no new wiring.
//
// CACHE-OFF — built LAZILY and ONLY from inside discoverGroupResources,
// AFTER the modePassthrough guard. A CACHE_ENABLED=false process never
// reaches the build site, so the raw-apiserver path stays byte-identical
// (project_cache_off_is_transparent_fallback).
//
// CONCURRENCY — the singleton POINTER is guarded by cachedDiscoveryMu
// (built once under the write lock; read on the hot path). The memcache
// client's own mutation surface (Fresh()/Invalidate()/refreshLocked) is
// lock-guarded inside client-go (memcache.go:209/220/235), so a concurrent
// Invalidate() (forceFresh path) against a Fresh()/ServerGroups() read
// (hot path) is race-safe by construction. The shared-vs-private
// conversion (the 0.30.128 hazard class,
// feedback_shared_vs_copy_is_a_concurrency_change) is gated by the
// concurrent -race falsifier (discovery_storm_a2_race_test.go).
var (
	cachedDiscoveryMu     sync.RWMutex
	cachedDiscoveryClient discovery.CachedDiscoveryInterface
)

// getCachedDiscovery returns the process-singleton cached discovery
// client, building it lazily from rc on first use. MUST be called only
// AFTER the modePassthrough guard (so cache-off never builds it). The
// built client wraps discoveryClientBuilder(rc) in a memCacheClient.
//
// Goroutine-safe: build-once under the write lock, warm reads under the
// read lock. Mirrors the SharedSADiscoveryClient build-once shape
// (internal/dynamic/cached_client.go:181) but cache-local.
func getCachedDiscovery(rc *rest.Config) (discovery.CachedDiscoveryInterface, error) {
	cachedDiscoveryMu.RLock()
	if cachedDiscoveryClient != nil {
		c := cachedDiscoveryClient
		cachedDiscoveryMu.RUnlock()
		return c, nil
	}
	cachedDiscoveryMu.RUnlock()

	cachedDiscoveryMu.Lock()
	defer cachedDiscoveryMu.Unlock()
	if cachedDiscoveryClient != nil {
		return cachedDiscoveryClient, nil
	}
	raw, err := discoveryClientBuilder(rc)
	if err != nil {
		return nil, err
	}
	cachedDiscoveryClient = cacheddiscovery.NewMemCacheClient(raw)
	return cachedDiscoveryClient, nil
}

// resetCachedDiscoveryForTest clears the cached-discovery singleton so
// each test rebuilds it from the freshly-installed discoveryClientBuilder
// fake. TEST-ONLY.
func resetCachedDiscoveryForTest() {
	cachedDiscoveryMu.Lock()
	cachedDiscoveryClient = nil
	cachedDiscoveryMu.Unlock()
}

// versionCompleteForGroup reports whether every registerable GVR of every
// currently-served version of group is already registered in rw. The hot
// (forceFresh:false) short-circuit predicate — RECOMPUTED every call (NOT
// a once-flag): a newly-served version (spec.versions[] widen) lists a GVR
// that rw.IsRegistered reports false for → returns false → caller runs the
// full discovery walk. Reads served versions from the (possibly cached)
// disco client; a per-version discovery error is treated as "not complete"
// (conservative — forces the full walk, which re-attempts + soft-fails per
// version exactly as the non-short-circuit path does).
func versionCompleteForGroup(disco discovery.DiscoveryInterface, rw *ResourceWatcher, group string) bool {
	versions, err := serverVersionsForGroup(disco, group)
	if err != nil || len(versions) == 0 {
		return false
	}
	for _, version := range versions {
		gv := schema.GroupVersion{Group: group, Version: version}
		list, lerr := disco.ServerResourcesForGroupVersion(gv.String())
		if lerr != nil || list == nil {
			return false
		}
		for _, el := range list.APIResources {
			if !discoveryIsRegisterableResource(el) {
				continue
			}
			gvk := gv.WithKind(el.Kind)
			if isBuiltInKind(gvk) {
				continue
			}
			gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: el.Name}
			if !rw.IsRegistered(gvr) {
				return false
			}
		}
	}
	return true
}

// Counters — observability for the discovery hop. Mirror the
// existing handler_registry.go pattern: sync.Map keyed by group,
// values are *atomic.Uint64 (cheap atomic increment under contention).
var (
	// discoveryGroupResourcesFetched ticks once per successful
	// DiscoverGroupResources call (per group). Exposed at
	// /debug/vars via expvar (see discovery_lookup_expvar.go).
	discoveryGroupResourcesFetched sync.Map // group(string) -> *atomic.Uint64

	// discoveryGVRsSpawned ticks once per composition GVR registered
	// via EnsureResourceType inside DiscoverGroupResources (per
	// group). Exposed at /debug/vars via expvar.
	discoveryGVRsSpawned sync.Map // group(string) -> *atomic.Uint64
)

// DiscoverGroupResources lists `ServerResourcesForGroupVersion` for
// every version of `group`. For each APIResource whose Kind is NOT in
// scheme.Scheme.AllKnownTypes() (i.e. CRD-backed, not a built-in), it
// forms the composition GVR and:
//
//  1. Calls rw.EnsureResourceType(gvr) — idempotent; if added==true
//     the informer is spawned.
//  2. Calls Deps().OnResourceTypeAvailable(gvr) — fires the FD1
//     dirty-mark for stale-negative LIST entries, byte-identical to
//     the post-Ship-0 per-CRD-object registration body (preserved
//     invariant).
//
// Returns (count of GVRs newly registered this call, error). Soft-
// fails on per-version discovery errors (apiserver may not serve a
// stale version; subsequent walks retry). Returns nil error iff at
// least one version was queryable.
//
// SINGLEFLIGHTED PER GROUP — concurrent calls for the same group
// serialize so only one discovery hop is in flight at a time. The
// mutex is per-group so calls for different groups proceed in
// parallel.
//
// Nil-receiver / nil watcher / nil discovery client / passthrough
// mode are all soft no-ops returning (0, nil) so callers can wire
// this unconditionally.
func DiscoverGroupResources(ctx context.Context, rc *rest.Config, group string) (int, error) {
	return discoverGroupResources(ctx, rc, group, false)
}

// DiscoverGroupResourcesFresh is the CRD-event-path entry point
// (crd_discovery_side_effect.go). forceFresh:true → it Invalidate()s the
// cached discovery surface (memcache.Invalidate is a GLOBAL wipe — all
// groups) and re-reads the apiserver BEFORE the registration walk, so a
// CRD CREATE/UPDATE can NEVER register against a
// stale cached read (the S4/F-4 stuck-zero regression class,
// docs/ship-0.30.233-s4-cache-invalidation-trace). No version-complete
// short-circuit on this path: a CREATE/UPDATE is exactly the case where a
// newly-served version's GVR is not yet registered, so a fresh full walk
// is mandatory.
func DiscoverGroupResourcesFresh(ctx context.Context, rc *rest.Config, group string) (int, error) {
	return discoverGroupResources(ctx, rc, group, true)
}

// discoverGroupResources is the shared implementation behind
// DiscoverGroupResources (forceFresh:false, hot /call walker) and
// DiscoverGroupResourcesFresh (forceFresh:true, CRD-event path).
//
//   - forceFresh:false — the hot path. Short-circuits to (0,nil) when the
//     cached discovery client is Fresh() AND every served version's
//     registerable GVR is already registered (versionCompleteForGroup).
//     Otherwise runs the full walk against the (cached) client — already-
//     known state is served from memory, so a non-short-circuited walk
//     still pays ZERO apiserver round-trips while the cache stays warm.
//   - forceFresh:true — the CRD-event path. Invalidate()s the cached
//     discovery surface (memcache.Invalidate is a GLOBAL wipe — all
//     groups; a superset invalidation is always safe), forcing the next
//     read to re-download, then ALWAYS runs the full walk against the
//     freshly re-read apiserver state.
func discoverGroupResources(ctx context.Context, rc *rest.Config, group string, forceFresh bool) (int, error) {
	if group == "" {
		return 0, nil
	}
	rw := Global()
	if rw == nil || rw.mode == modePassthrough {
		// Cache off / passthrough — no informer to spawn, and the
		// cached-discovery client is NEVER built (byte-identical raw
		// path). Record nothing; a future cache-on transition rebuilds.
		return 0, nil
	}

	// Per-group singleflight — serialize concurrent discovery hops
	// for the same group across goroutines.
	lock := discoverGroupLock(group)
	lock.Lock()
	defer lock.Unlock()

	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	// Build (lazily, post-passthrough-guard) the process-singleton cached
	// discovery client. Replaces the per-call raw client — the apiserver
	// round-trips are now served from the memcache after the first read.
	disco, err := getCachedDiscovery(rc)
	if err != nil {
		slog.Warn("cache.discovery.client_build_failed",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.Any("err", err),
		)
		return 0, err
	}

	if forceFresh {
		// CRD CREATE/UPDATE — drop the cached discovery surface (a GLOBAL
		// memcache wipe, all groups) so the walk below re-reads the
		// apiserver and sees the newly-served version's GVRs.
		disco.Invalidate()
	} else if disco.Fresh() && versionCompleteForGroup(disco, rw, group) {
		// Hot path, cache warm + every served version fully registered —
		// nothing to do. Recomputed every call (NOT a once-flag): a newly-
		// served version leaves a GVR un-registered → predicate false →
		// fall through to the full walk below.
		return 0, nil
	}

	// Enumerate every version of `group` via ServerGroups().
	versions, err := serverVersionsForGroup(disco, group)
	if err != nil {
		// Group may not exist yet — common during cluster boot. Soft
		// fail; subsequent walks retry.
		slog.Warn("cache.discovery.server_groups_failed",
			slog.String("subsystem", "cache"),
			slog.String("group", group),
			slog.Any("err", err),
		)
		return 0, err
	}

	spawned := 0
	anyOK := false
	for _, version := range versions {
		if ctx.Err() != nil {
			return spawned, ctx.Err()
		}
		gv := schema.GroupVersion{Group: group, Version: version}
		list, lerr := disco.ServerResourcesForGroupVersion(gv.String())
		if lerr != nil || list == nil {
			slog.Warn("cache.discovery.server_resources_failed",
				slog.String("subsystem", "cache"),
				slog.String("group_version", gv.String()),
				slog.Any("err", lerr),
			)
			continue
		}
		anyOK = true
		for _, el := range list.APIResources {
			if !discoveryIsRegisterableResource(el) {
				continue
			}
			gvr := schema.GroupVersionResource{
				Group:    group,
				Version:  version,
				Resource: el.Name,
			}
			gvk := gvr.GroupVersion().WithKind(el.Kind)

			// Skip built-in kinds — only CRD-backed kinds need the
			// composition-discovery path. (Built-ins are pre-known
			// at boot from scheme.Scheme.AllKnownTypes().)
			if isBuiltInKind(gvk) {
				continue
			}

			added, _ := rw.EnsureResourceType(gvr)
			if added {
				// FD1 (Ship D / 0.30.114) — preserved invariant:
				// dirty-mark stale-negative LIST entries when a
				// genuinely-new GVR appears at runtime. Identical
				// callback chain to the pre-Ship-0.5
				// per-CRD-object registration body, just invoked from the
				// synchronous discovery path instead of a CRD-
				// informer AddFunc.
				marked := Deps().OnResourceTypeAvailable(gvr)
				slog.Info("cache.discovery.gvr_registered",
					slog.String("subsystem", "cache"),
					slog.String("gvr", gvr.String()),
					slog.Int("l1_keys_dirty_marked", marked),
					slog.String("note", "composition informer spawned via one-shot discovery; stale-negative LIST deps dirty-marked"),
				)

				// Ship 2 Stage 2 / 0.30.247 — re-prewarm wiring.
				//
				// ORDER IS LOAD-BEARING: AddNavigatedGVR MUST run BEFORE
				// notifyGVRDiscoveredForReprewarm.
				//
				// (1) AddNavigatedGVR widens the BindingsByGVR navigated set
				//     under idx.mu.Lock — SYNCHRONOUSLY enrols every already-
				//     known non-wildcard binding that grants get/list on the
				//     new GVR. Without this call, EnumeratePrewarmTargetsForGVR
				//     at prewarm_enumeration.go:100-109 returns ONLY the
				//     wildcard bucket — narrow-RBAC cohorts (production
				//     customer team-members at 1000-user scale per
				//     project_production_scale.md) are silently skipped and
				//     their cells stay stale forever. The function is defined
				//     at bindings_by_gvr.go:455 and had ZERO production callers
				//     prior to this ship (PM RC-1, P0).
				//
				// (2) notifyGVRDiscoveredForReprewarm fires the cache→dispatchers
				//     hook. The dispatchers-side handler ASYNCHRONOUSLY enqueues
				//     a scopeKindGVRDiscovered scope into the prewarm engine.
				//     The engine's rePrewarm then reads the freshly-widened
				//     BindingsByGVR index → narrow cohorts are now enumerated
				//     → the re-walk's seedOneRestaction records the dep edge
				//     missed by the empty-iterator short-circuit
				//     (resolve.go:377-381 — H4 root cause).
				//
				// Gated on PrewarmEnabled() — #57 implicit-on-cache, so when
				// the cache subsystem is off the engine is inert and the hook
				// has no consumer; the call is still safe but pointless.
				// Prewarm is now implicit under the single CACHE_ENABLED gate
				// (project_single_cache_flag_direction).
				if PrewarmEnabled() {
					AddNavigatedGVR(gvr)
					notifyGVRDiscoveredForReprewarm(gvr)
				}

				spawned++
				incDiscoveryGVRsSpawned(group)
			}
		}
	}

	if !anyOK {
		return spawned, fmt.Errorf("no version of group %q is currently served", group)
	}

	incDiscoveryGroupResourcesFetched(group)
	slog.Info("cache.discovery.group_resources_fetched",
		slog.String("subsystem", "cache"),
		slog.String("group", group),
		slog.Int("gvrs_spawned", spawned),
		slog.Int("versions_queried", len(versions)),
	)
	return spawned, nil
}

// serverVersionsForGroup returns every version of `group` the
// apiserver currently serves. Empty + nil error for an unknown
// group (apiserver responded but did not list it).
func serverVersionsForGroup(disco discovery.DiscoveryInterface, group string) ([]string, error) {
	groups, err := disco.ServerGroups()
	if err != nil || groups == nil {
		return nil, err
	}
	for _, g := range groups.Groups {
		if g.Name != group {
			continue
		}
		out := make([]string, 0, len(g.Versions))
		for _, v := range g.Versions {
			out = append(out, v.Version)
		}
		return out, nil
	}
	return nil, nil
}

// discoveryIsRegisterableResource reports whether an APIResource is
// suitable for EnsureResourceType. Filters subresources (status,
// scale, etc.) and resources lacking either a Name or Kind.
func discoveryIsRegisterableResource(el metav1.APIResource) bool {
	if el.Name == "" || el.Kind == "" {
		return false
	}
	// Subresources have a slash in their Name (e.g. "pods/status").
	// Their informer is not separately registered — the parent
	// resource's informer covers them.
	if strings.Contains(el.Name, "/") {
		return false
	}
	return true
}

// isBuiltInKind reports whether gvk is registered in
// scheme.Scheme.AllKnownTypes(). Built-in kinds (core/v1, apps/v1,
// rbac/v1, apiextensions/v1, etc.) are NOT CRD-backed and do NOT need
// the composition-discovery path.
//
// We use a package-level lazy-initialized map so the lookup is O(1)
// per call and we do not pay scheme.Scheme.AllKnownTypes()'s
// allocation cost on every discovery hop.
var (
	builtInKindsOnce sync.Once
	builtInKinds     map[schema.GroupVersionKind]struct{}
)

func isBuiltInKind(gvk schema.GroupVersionKind) bool {
	builtInKindsOnce.Do(initBuiltInKinds)
	_, ok := builtInKinds[gvk]
	return ok
}

// initBuiltInKinds populates the built-in-kinds map from
// kscheme.Scheme.AllKnownTypes() PLUS apiextensions/v1 (NOT in the
// client-go scheme by default). Called exactly once via sync.Once.
//
// kscheme.Scheme is k8s.io/client-go/kubernetes/scheme.Scheme — wires
// the standard Kubernetes API surface (core/v1, apps/v1, batch/v1,
// rbac/v1, networking/v1, etc.). We layer apiextensions/v1 on top so
// the CRD GVR is recognised as built-in (the v6 invariant: the
// discovery hop must never incidentally re-spawn the CRD informer).
// Anything NOT in this combined scheme is a CRD-backed kind and must
// take the composition-discovery path.
func initBuiltInKinds() {
	builtInKinds = make(map[schema.GroupVersionKind]struct{})
	// Standard Kubernetes API surface.
	for gvk := range kscheme.Scheme.AllKnownTypes() {
		builtInKinds[gvk] = struct{}{}
	}
	// apiextensions/v1 — wired explicitly because kscheme.Scheme does
	// not include it. Use the package's AddToScheme against a private
	// scheme so we extract its known types without polluting the
	// global client-go scheme.
	extScheme := runtime.NewScheme()
	_ = apiextensionsv1.AddToScheme(extScheme)
	for gvk := range extScheme.AllKnownTypes() {
		builtInKinds[gvk] = struct{}{}
	}
}

// --- Counters -------------------------------------------------------------

func incDiscoveryGroupResourcesFetched(group string) {
	v, _ := discoveryGroupResourcesFetched.LoadOrStore(group, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}

func incDiscoveryGVRsSpawned(group string) {
	v, _ := discoveryGVRsSpawned.LoadOrStore(group, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}

// DiscoveryGroupResourcesFetched returns the running count of
// successful DiscoverGroupResources calls for `group`. Used by the
// Ship 0.5 falsifier (B4 — re-walk on widget CRUD increments the
// counter) and operations (G1 — 24h cadence tracking).
func DiscoveryGroupResourcesFetched(group string) uint64 {
	v, ok := discoveryGroupResourcesFetched.Load(group)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// DiscoveryGVRsSpawned returns the running count of composition GVRs
// registered via DiscoverGroupResources for `group`. Used by the
// Ship 0.5 falsifier (M8 / B2 — composition informers PRESENT
// post-walk).
func DiscoveryGVRsSpawned(group string) uint64 {
	v, ok := discoveryGVRsSpawned.Load(group)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// DiscoveryGroupResourcesFetchedTotal returns the sum of every per-
// group fetch counter. Used by the expvar/observability surface.
func DiscoveryGroupResourcesFetchedTotal() uint64 {
	var total uint64
	discoveryGroupResourcesFetched.Range(func(_, v any) bool {
		total += v.(*atomic.Uint64).Load()
		return true
	})
	return total
}

// DiscoveryGVRsSpawnedTotal returns the sum of every per-group
// spawned-GVR counter. Used by the expvar/observability surface.
func DiscoveryGVRsSpawnedTotal() uint64 {
	var total uint64
	discoveryGVRsSpawned.Range(func(_, v any) bool {
		total += v.(*atomic.Uint64).Load()
		return true
	})
	return total
}

// ResetDiscoveryCountersForTest zeros every per-group counter.
// TEST-ONLY.
func ResetDiscoveryCountersForTest() {
	discoveryGroupResourcesFetched.Range(func(k, _ any) bool {
		discoveryGroupResourcesFetched.Delete(k)
		return true
	})
	discoveryGVRsSpawned.Range(func(k, _ any) bool {
		discoveryGVRsSpawned.Delete(k)
		return true
	})
}

// ResetDiscoverySingleflightForTest clears the per-group singleflight
// mutex map. TEST-ONLY. Production callers should never reset this;
// the map grows monotonically with the nav-discovered group set
// (bounded by ~tens of groups).
func ResetDiscoverySingleflightForTest() {
	discoverGroupSingleflight.Range(func(k, _ any) bool {
		discoverGroupSingleflight.Delete(k)
		return true
	})
}
