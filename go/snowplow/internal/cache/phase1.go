// phase1.go — 0.30.102 Tag B: startup informer-warmup state + the
// hardcoded meta-query seed budget + the all-informer sync-wait.
//
// Tag B premise (resting on Tag A 0.30.100): the resolver pivot (now
// implicit-on-cache, #57; historically gated by RESOLVER_USE_INFORMER=true)
// can only serve a GVR whose informer is registered AND synced. 0.30.99's
// bench failed because the navigated informers were registered lazily-late
// and never synced inside the navigation window — the pivot served nothing
// cold.
//
// Tag B closes that cold window with a startup PHASE 1: at boot, before
// traffic, the navigation roots named by the frontend ConfigMap are
// fetched and every one is RECURSIVELY
// resolved with the snowplow SERVICE-ACCOUNT identity through the
// standard widget/RESTAction resolver (0.30.105: the walk recurses
// Root -> Route -> Page -> Row/Column -> DataGrid/Table leaf via each
// resolved widget's status.resourcesRefs.items[]). As the resolver walks
// inner-call paths it auto-registers an informer for every GVR it
// touches via the flag-independent `lazyRegisterInnerCallPaths` hook —
// including the heavy `composition.krateo.io` informer behind the
// Compositions Page DataGrid. After the walk, Phase 1 BLOCKS until every
// registered informer reaches HasSynced, then signals Phase1Done. The
// /readyz probe gates pod readiness on Phase1Done so traffic only
// arrives once the navigated informers are warm.
//
// CRITICAL — feedback_no_special_cases.md: Phase 1 does NOT consult any
// configured GVR / RESTAction list. The ONLY hardcoded budget is the 5
// meta-query seeds below — bare anchors needed to bootstrap discovery,
// not per-resource policy. Every BUSINESS GVR (widgets, panels,
// compositions) is discovered by recursively resolving the
// ConfigMap-named navigation roots.
//
// Ship 0 / 0.30.222: the customresourcedefinitions GVR was REMOVED from
// the seed set. Ship 0.5 / 0.30.223 (v6): the CRD informer is DELETED
// entirely. Composition GVRs are discovered by one-shot apiserver
// discovery (cache.DiscoverGroupResources, invoked synchronously from
// the walker) instead of via an in-process CRD-informer event stream.
//
// #483: the 4 RBAC GVRs + restactions remain primordial because they
// have justified chicken-and-egg semantics — the walk cannot evaluate
// an RBAC decision or follow an apiRef edge without them, so they must
// exist before it starts. `routesloaders` and `navmenus` were REMOVED.
// This header used to list them as primordial "because the walker
// queries them to start the walk", and that had been FALSE since
// 0.30.107: root selection moved to the frontend ConfigMap
// (listNavigationRootsFromConfigMap, dispatchers/phase1_roots.go),
// which decodes the `.api.INIT` / `.api.ROUTES_LOADER` `/call` URLs into
// ObjectReferences and fetches each one by name. A root is reachable
// whether or not its type was seeded, so neither GVR was ever a
// precondition for the walk; they were left behind as informer-anchor
// seeds only. Both kinds were then removed from the platform by the
// routing refactor (no CRD under any group, zero objects), so the seeds
// anchored informers for types that do not exist — a permanent
// reconnect loop, ~4 failed discovery requests/min, 6% of all log
// volume and a ~45s readiness tax per roll. The CRD GVR's removal had
// a different reason: it has no chicken-and-egg claim at all, since by
// the time the walker meets a templated path it is already running.
//
// IMPLICIT-ON-CACHE (#57) — PrewarmEnabled() is now implicit under the
// single CACHE_ENABLED master gate: prewarm runs whenever the cache
// subsystem is on, and never when it is off. The standalone
// PREWARM_ENABLED env flag was folded away in Task #57
// (project_single_cache_flag_direction: "prewarm implicit when cache
// on"). When cache is OFF: Phase 1 never runs and Phase1Done is pre-set
// true at startup so /readyz is an immediate-200 no-op. This is the
// transparent cache-off fallback (project_cache_off_is_transparent_fallback).

package cache

import (
	"context"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/krateo-platformops/plumbing/env"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// PrewarmEnabled reports whether the Tag B startup warmup runs. Folded
// in Task #57 to be implicit-on-cache: prewarm is on iff the cache
// subsystem is on (CACHE_ENABLED truthy). The standalone PREWARM_ENABLED
// env flag was retired — see the package-doc IMPLICIT-ON-CACHE note. A
// stale PREWARM_ENABLED in the environment is ignored (main.go's
// retired-flag audit warns once); the only prewarm toggle is now
// CACHE_ENABLED. Cheap enough to read per call (delegates to Disabled()).
func PrewarmEnabled() bool {
	return !Disabled()
}

// phase1Done is the process-wide atomic that flips true exactly once,
// when the Phase 1 SA-credentialed resolution walk has finished AND
// every registered informer (including composition informers spawned
// via cache.DiscoverGroupResources during the walk) has reached
// HasSynced.
//
// When PrewarmEnabled()==false (cache off) the startup sequence calls
// MarkPhase1Done immediately (nothing to wait for) so /readyz is a
// no-op. When true (cache on), MarkPhase1Done is called only at the END
// of Phase1Warmup. /readyz returns 200 iff phase1Done.Load()==true.
var phase1Done atomic.Bool

// MarkPhase1Done flips the process-wide Phase1Done signal to true. Safe
// to call multiple times — atomic store is idempotent. Called once by
// the startup sequence (immediately when the cache subsystem is OFF —
// prewarm is implicit-on-cache, #57 — or at the tail of Phase1Warmup
// when ON).
//
// Task #221: also records the flip instant for the
// snowplow_prewarm_complete expvar (markPhase1DoneObserved is
// CompareAndSwap-once, so the boundary timestamp tracks the FIRST flip
// even under repeated idempotent calls). This is THE canonical wiring
// site — every flip path (dispatcher Step 8, cache-off else, readiness
// safety-net) funnels through here, so the expvar boundary is observed
// on all of them. See prewarm_complete_metric.go.
func MarkPhase1Done() {
	phase1Done.Store(true)
	markPhase1DoneObserved()
}

// IsPhase1Done reports whether the Tag B startup warmup has completed.
// The /readyz probe handler returns 200 iff this is true. Liveness
// (/health) does NOT consult this — a not-yet-warm pod is alive.
func IsPhase1Done() bool {
	return phase1Done.Load()
}

// ShouldFlipPhase1DoneOnStartup reports whether the startup safety-net
// at main.go's readiness-gate block should flip Phase1Done immediately
// (because there is nothing to warm), rather than wait for the
// Phase1Warmup goroutine to signal completion.
//
// The invariant: /readyz must return 200 once the pod is ready to serve
// traffic. When the cache subsystem is OFF, prewarm is OFF, or the
// watcher failed to construct, NO informer-warming work exists — the
// gate must flip at boot or the pod is stuck "warming" forever, the
// Service drops it from Endpoints, and the LB has 0 backends. The
// healthy cache-on path (with prewarm implicit-on-cache, #57) returns
// false here so Phase1Warmup retains ownership of the flip.
//
// 0.30.153 — Ship: introduced as a named helper to make the three-disjunct
// invariant testable and to retire the inline conditional at main.go that
// missed the cache-off + watcher-non-nil case (incident: pod stuck
// `{"status":"warming","phase1Done":false}`, Service endpoints empty,
// snowplow LB unroutable).
//
// Three reasons to flip (any one suffices):
//   - cacheEnabled == false — cache subsystem off, no informers exist
//   - prewarmEnabled == false — prewarm disabled, no warmup goroutine runs
//   - watcherIsNil == true — watcher construction failed, no informers exist
//
// #57 — the SIGNATURE is preserved verbatim (3 disjuncts) even though
// prewarm is now implicit-on-cache, so callers pass cache.PrewarmEnabled()
// which folds to !Disabled(); the middle disjunct then equals the first.
// The 0.30.153 incident was a MISSING disjunct readyz-hang, so the named
// 3-arg helper is the regression's encoded falsifier — collapsing it to 2
// disjuncts would erase that encoding for zero benefit.
//
// MarkPhase1Done is idempotent (atomic store) so a caller may invoke it
// unconditionally when this returns true.
func ShouldFlipPhase1DoneOnStartup(cacheEnabled, prewarmEnabled, watcherIsNil bool) bool {
	return !cacheEnabled || !prewarmEnabled || watcherIsNil
}

// ResetPhase1DoneForTest clears the Phase1Done signal. TEST-ONLY — the
// production lifecycle is set-once. Exported so the readyz handler test
// in another package can drive the gate deterministically.
func ResetPhase1DoneForTest() {
	phase1Done.Store(false)
}

// Ship 0.5 / 0.30.223 (v6): the apiextensions CRD GVR constant AND
// its accessor were DELETED. The CRD informer is no longer spawned
// anywhere in the cache codebase; the composition-GVR-discovery
// semantics it used to back are now satisfied by cache.
// DiscoverGroupResources (one-shot apiserver discovery, synchronous,
// invoked from the walker — see discovery_lookup.go).

// #483: routesLoadersGVR / navMenusGVR and their RoutesLoadersGVR() /
// NavMenusGVR() accessors were DELETED here, along with their entries in
// MetaQuerySeeds. They were informer-anchor seeds for the `routesloaders`
// and `navmenus` widget kinds, which the portal's routing refactor
// removed from the platform — `helm/portal/templates/menu.sidebar-nav.yaml`
// records the sidebar as "the SINGLE route source (no RoutesLoader)" and
// that "BOTH kinds were removed in the routing refactor". No CRD for
// either kind exists under any group and no objects exist, so the seeds
// registered informers for types the apiserver does not serve.
//
// They were NOT root-selection drivers and had not been since 0.30.107 —
// see the package header for that trace. Nothing in the walk depended on
// them: the only production consumer was MetaQuerySeeds, i.e. informer
// registration and nothing else.
//
// The seed set is now derived from its two sanctioned sources
// (restActionGVR + RBACResourceTypes), so re-adding a navigation kind
// cannot slip back in under a local alias —
// TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources fails on any
// seed that is not one of those.

// MetaQuerySeeds returns the COMPLETE hardcoded seed budget for Tag B —
// EXACTLY these 5 GVRs, nothing else (feedback_no_special_cases.md is a
// hard requirement here). Every entry is a meta-query INFORMER-ANCHOR
// seed: the watcher pre-registers an informer for the resource type so a
// `/call` to one of these can be served from cache. None of them is a
// root-SELECTION driver — the navigation roots come from the frontend
// ConfigMap (config.json .api.INIT / .api.ROUTES_LOADER; see
// dispatchers/phase1_roots.go).
//
// DERIVED, NOT ENUMERATED (#483). The set is the union of its two
// sanctioned sources and is written that way on purpose: a seed has to
// come from one of them, so there is no list for a new literal to be
// appended to. The two sources are the only ones with a chicken-and-egg
// claim on the walk.
//
//  1. restactions              — the restActionGVR anchor (already cited
//     by inventory.go; the resolver's apiRef edges target it).
//     2-5. the 4 RBACResourceTypes — roles / rolebindings / clusterroles /
//     clusterrolebindings (already bootstrap-registered in
//     NewResourceWatcher; included here so the seed set is the single
//     auditable source of truth).
//
// #483: `routesloaders` and `navmenus` were entries 1 and 2 until the
// routing refactor removed both kinds from the platform; see the note
// above their deleted declarations.
//
// Ship 0 / 0.30.222: customresourcedefinitions is NO LONGER a seed
// (Diego invariant: "no CRD informer if the CRD object itself is not
// walked"). Ship 0.5 / 0.30.223 (v6): the CRD informer was deleted
// entirely; composition GVRs are discovered by one-shot apiserver
// discovery (cache.DiscoverGroupResources) invoked synchronously from
// the walker.
//
// Every BUSINESS GVR — widgets, panels, compositions — is ABSENT from
// this set by construction. Those are discovered by RESOLVING the
// ConfigMap-derived navigation roots, never named in code.
// TestMetaQuerySeeds_IsDerivedFromItsTwoSanctionedSources recomputes this
// union from restActionGVR + RBACResourceTypes and requires set
// equality, so any added seed fails whatever it is named.
func MetaQuerySeeds() []schema.GroupVersionResource {
	seeds := []schema.GroupVersionResource{
		restActionGVR,
	}
	seeds = append(seeds, RBACResourceTypes...)
	return seeds
}

// RegisterMetaQuerySeeds registers an informer for the single non-RBAC
// meta-query seed (restactions) plus
// re-confirms the 4 RBAC GVRs (already registered by NewResourceWatcher
// — EnsureResourceType observes added=false for those) — 5 seeds total.
// Idempotent + singleflighted under rw.mu.
//
// #483: was 3 non-RBAC / 7 total, until the removed `routesloaders` and
// `navmenus` anchors stopped registering informers for kinds the
// apiserver no longer serves.
//
// Ship 0 / 0.30.222: the CRD GVR is no longer in this list. Ship 0.5
// / 0.30.223 (v6): the CRD informer is deleted; composition GVRs are
// discovered via cache.DiscoverGroupResources (one-shot apiserver
// discovery) invoked synchronously from the walker the first time a
// templated apiserver path is reached.
//
// This is the ONLY code that hands a hardcoded GVR to EnsureResourceType
// at startup. The Phase 1 walk registers everything else by resolution.
//
// Returns the count newly registered. Nil-receiver / passthrough are
// no-ops.
func (rw *ResourceWatcher) RegisterMetaQuerySeeds() int {
	if rw == nil || rw.mode == modePassthrough {
		return 0
	}
	registered := 0
	for _, gvr := range MetaQuerySeeds() {
		added, _ := rw.EnsureResourceType(gvr)
		if added {
			registered++
		}
	}
	slog.Info("cache.phase1.meta_query_seeds_registered",
		slog.String("subsystem", "cache"),
		slog.Int("seed_count", len(MetaQuerySeeds())),
		slog.Int("newly_registered", registered),
		slog.String("note", "bare meta-query anchors only — every business GVR is discovered by resolution"),
	)
	return registered
}

// WaitAllInformersSynced blocks until EVERY registered informer reaches
// HasSynced AND no new informer was registered DURING the wait, or ctx
// is cancelled. This is the Phase 1 sync barrier: after the SA-credentialed
// resolution walk has fanned out (registering an informer per touched
// GVR via lazyRegisterInnerCallPaths) AND every templated apiserver
// path has invoked cache.DiscoverGroupResources to spawn composition
// informers (v6, Ship 0.5), this call guarantees the navigated set is
// warm before Phase1Done flips.
//
// RE-SNAPSHOT LOOP — the load-bearing concurrency property. A single
// snapshot+wait has a race: a late EnsureResourceType (a composition
// informer spawned by a still-running DiscoverGroupResources, or by a
// late resolver inner-call touch) that lands AFTER the snapshot is
// taken but while WaitForCacheSync is blocked would NOT be in the sync
// set — Phase1Done could then flip while that composition informer is
// still cold, the exact premature-Ready failure /readyz exists to
// prevent. So this loop re-snapshots after every WaitForCacheSync pass
// and only returns when a full pass completed with the registered-
// informer count UNCHANGED across it (no registration occurred during
// the wait). client-go's HasSynced is monotonic — once true it stays
// true — so a stable count across a pass means every informer observed
// at the start of the pass is synced AND nothing new appeared, hence
// every informer is synced.
//
// BOUNDED EXIT POLICY (Fix 1b, task #43) — the outer ctx
// (PHASE1_TIMEOUT_SECONDS) remains the absolute backstop, but the loop no
// longer relies on it as the ONLY bound. Two UNIFORM env knobs (no
// per-GVR/per-name branch — feedback_no_special_cases) bound the loop:
//
//   - PHASE1_SYNC_PASS_GRACE_SECONDS (default 45): each WaitForCacheSync
//     pass runs under a per-pass CHILD ctx, not the outer 900s ctx. A
//     single never-HasSynced informer can therefore only stall ONE pass
//     for `passGrace`, not the whole budget.
//   - PHASE1_SYNC_QUIESCENCE_SECONDS (default 10): once the registered set
//     has been STABLE (count unchanged) for this window AND every informer
//     that CAN sync has, the barrier returns "good enough" (nil error,
//     readiness flips) even if a stuck informer never reached HasSynced.
//     The stuck GVR set is named in the cache.phase1.sync_wait_good_enough
//     structured log.
//
// "Good enough" is SAFE because a late- or never-syncing informer is
// covered downstream: lazy register-on-navigation serves its first /call
// via the apiserver fallthrough (informer-fallthrough-not-synced), and the
// refresher converges its data into L1 as events land — readiness gates on
// "substrate mostly warm + set stopped growing", not "every version-pinned
// composition CRD finished its initial LIST". This extends the same
// readiness-independent-of-cluster-shape invariant Ship 2 / 0.30.196 used
// for the background cohort seed.
//
// INVARIANT the count-equality test depends on: the registered-informer
// set is append-only during Phase 1 in practice — RemoveResourceType is
// wired to the CRD-DELETE event bridge (live since Ship L/0.30.246; the
// #117 periodic sweep was closed superseded 2026-06-12), which fires
// during Phase 1 only if a CRD is deleted mid-walk (rare; a delete in
// that window could make COUNT-equality mask a set change for one pass).
// So an unchanged COUNT across a pass implies an unchanged SET. If a future change adds an in-Phase-1
// de-registration path, this proxy breaks and the loop must compare
// the GVR set, not the count.
//
// Returns nil on success, ctx.Err()/DeadlineExceeded on cancellation. In
// modePassthrough there are no informers — returns nil immediately.
func (rw *ResourceWatcher) WaitAllInformersSynced(ctx context.Context) error {
	if rw == nil || rw.mode == modePassthrough {
		return nil
	}

	start := time.Now()

	// Fix 1b (task #43): uniform bounds, read once. Per-pass grace caps how
	// long a single WaitForCacheSync pass can block on a slow/stuck informer
	// (a child ctx, NOT the outer 900s budget). Quiescence is the
	// set-stability window after which we accept "good enough".
	passGrace := time.Duration(env.Int("PHASE1_SYNC_PASS_GRACE_SECONDS", 45)) * time.Second
	quiescence := time.Duration(env.Int("PHASE1_SYNC_QUIESCENCE_SECONDS", 10)) * time.Second

	// Set-stability tracking across passes (touched only in this goroutine).
	lastCount := -1
	lastChange := start

	for pass := 1; ; pass++ {
		if ctx.Err() != nil {
			slog.Warn("cache.phase1.sync_wait_incomplete",
				slog.String("subsystem", "cache"),
				slog.Int("pass", pass),
				slog.Int64("waited_ms", time.Since(start).Milliseconds()),
				slog.Any("err", ctx.Err()),
			)
			return ctx.Err()
		}

		// Snapshot the informer set + count + GVRs under the lock so a
		// post-pass !HasSynced check can name the stuck GVRs.
		rw.mu.RLock()
		countBefore := len(rw.informers)
		syncs := make([]clientcache.InformerSynced, 0, countBefore)
		gvrs := make([]schema.GroupVersionResource, 0, countBefore)
		for gvr, gi := range rw.informers {
			syncs = append(syncs, gi.Informer().HasSynced)
			gvrs = append(gvrs, gvr)
		}
		rw.mu.RUnlock()

		if len(syncs) == 0 {
			// Nothing registered — vacuously synced.
			return nil
		}

		// Track set-stability: a change resets the quiescence clock.
		if countBefore != lastCount {
			lastCount = countBefore
			lastChange = time.Now()
		}

		// Wait for this snapshot's informers to sync under a PER-PASS child
		// ctx (outside rw.mu, so concurrent registrations are not blocked).
		// A single never-HasSynced informer can stall at most this one pass
		// for `passGrace` — never the whole outer budget.
		passCtx, cancel := context.WithTimeout(ctx, passGrace)
		synced := clientcache.WaitForCacheSync(passCtx.Done(), syncs...)
		cancel()

		// The outer budget being exhausted is the hard backstop — surface it
		// as the existing incomplete warning regardless of pass outcome.
		if ctx.Err() != nil {
			slog.Warn("cache.phase1.sync_wait_incomplete",
				slog.String("subsystem", "cache"),
				slog.Int("pass", pass),
				slog.Int("informer_count", countBefore),
				slog.Int64("waited_ms", time.Since(start).Milliseconds()),
				slog.Any("err", ctx.Err()),
			)
			return ctx.Err()
		}

		countAfter := rw.RegisteredCount()
		setStable := countAfter == countBefore

		if synced && setStable {
			// Happy path: every informer in a stable set synced.
			slog.Info("cache.phase1.sync_wait_complete",
				slog.String("subsystem", "cache"),
				slog.Int("passes", pass),
				slog.Int("informer_count", countAfter),
				slog.Int64("waited_ms", time.Since(start).Milliseconds()),
			)
			return nil
		}

		// Pass did NOT fully sync (or set still growing). If the registered
		// set has been STABLE for the quiescence window, accept "good
		// enough": the informers that can sync have, and the stuck ones are
		// covered by lazy-register fallthrough + the refresher. Name the
		// stuck GVRs so the operator can see exactly what was deferred.
		if setStable && time.Since(lastChange) >= quiescence {
			unsynced := make([]string, 0)
			rw.mu.RLock()
			for _, gvr := range gvrs {
				if gi, ok := rw.informers[gvr]; ok && !gi.Informer().HasSynced() {
					unsynced = append(unsynced, gvr.String())
				}
			}
			rw.mu.RUnlock()
			slog.Info("cache.phase1.sync_wait_good_enough",
				slog.String("subsystem", "cache"),
				slog.Int("passes", pass),
				slog.Int("informer_count", countAfter),
				slog.Int64("waited_ms", time.Since(start).Milliseconds()),
				slog.Int64("set_stable_ms", time.Since(lastChange).Milliseconds()),
				slog.Int("unsynced_count", len(unsynced)),
				slog.Any("unsynced_gvrs", unsynced),
				slog.String("reason", "registered set stable for the quiescence window; deferring still-unsynced informers to lazy-register fallthrough + refresher"),
			)
			return nil
		}

		// Set still changing OR not yet quiescent — re-snapshot and re-wait.
		slog.Info("cache.phase1.sync_wait_repass",
			slog.String("subsystem", "cache"),
			slog.Int("pass", pass),
			slog.Int("count_before", countBefore),
			slog.Int("count_after", countAfter),
			slog.Bool("pass_synced", synced),
			slog.Int64("set_stable_ms", time.Since(lastChange).Milliseconds()),
			slog.String("reason", "set still growing or not yet quiescent — re-snapshotting"),
		)
	}
}

// RegisteredGVRs returns a snapshot of every GVR with a registered
// informer. The no-hardcode falsifier asserts over this full set that
// the orphan GVR is absent — a stronger check than a single-GVR probe.
func (rw *ResourceWatcher) RegisteredGVRs() []schema.GroupVersionResource {
	if rw == nil || rw.mode == modePassthrough {
		return nil
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	out := make([]schema.GroupVersionResource, 0, len(rw.informers))
	for gvr := range rw.informers {
		out = append(out, gvr)
	}
	return out
}

// IsRegistered reports whether an informer exists for gvr. Convenience
// over RegisteredGVRs for single-GVR assertions (falsifier tests).
func (rw *ResourceWatcher) IsRegistered(gvr schema.GroupVersionResource) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	_, ok := rw.informers[gvr]
	return ok
}

// RegisteredCount returns the number of registered informers without
// allocating a slice. The Phase 1 walk driver polls this to detect when
// the CRD-watch + resolution fan-out has settled.
func (rw *ResourceWatcher) RegisteredCount() int {
	if rw == nil || rw.mode == modePassthrough {
		return 0
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return len(rw.informers)
}

// Ship 0.30.127: WithPhase1Resolution / IsPhase1Resolution + their context
// key were removed (sole consumer — the deleted phase1IteratorCap).

// ctxKeyInternalEndpointType is the typed empty-struct context key for
// WithInternalEndpoint / InternalEndpointFromContext.
type ctxKeyInternalEndpointType struct{}

var ctxKeyInternalEndpoint = ctxKeyInternalEndpointType{}

// WithInternalEndpoint attaches an internal-dispatch apiserver endpoint
// to ctx. The RESTAction resolver's endpoint-resolution step consults
// this when a non-UAF api[] stage has NO EndpointRef AND the request is
// driven by an internal/startup path that has no per-user `-clientconfig`
// Secret to read.
//
// This is a GENERAL mechanism, not a per-resource carve-out
// (feedback_no_special_cases.md): any internal driver — Phase 1's
// SA-credentialed resolution walk today, a future background refresher
// tomorrow — can tell the standard resolver which endpoint to dispatch
// against instead of the per-user clientconfig lookup. The resolver
// stays one code path; only the endpoint SOURCE is parameterised.
//
// ep is carried as `any` so the cache package does not couple to the
// plumbing endpoints type; the resolver type-asserts to its endpoint
// type. nil ep returns the parent context unchanged.
func WithInternalEndpoint(ctx context.Context, ep any) context.Context {
	if ctx == nil || ep == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyInternalEndpoint, ep)
}

// InternalEndpointFromContext returns the internal-dispatch endpoint
// attached by WithInternalEndpoint, or (nil, false) when none was set
// (the ordinary per-user request path — the resolver then takes its
// standard per-user clientconfig lookup).
func InternalEndpointFromContext(ctx context.Context) (any, bool) {
	if ctx == nil {
		return nil, false
	}
	v := ctx.Value(ctxKeyInternalEndpoint)
	if v == nil {
		return nil, false
	}
	return v, true
}

// ctxKeyInternalRESTConfigType is the typed empty-struct context key for
// WithInternalRESTConfig / InternalRESTConfigFromContext.
type ctxKeyInternalRESTConfigType struct{}

var ctxKeyInternalRESTConfig = ctxKeyInternalRESTConfigType{}

// WithInternalRESTConfig attaches a ready-built apiserver *rest.Config to
// ctx. The snowplow object/resourceRef client-construction sites consult
// this when an internal/startup driver (Phase 1's SA-credentialed
// resolution walk) is in flight, and use it directly instead of rebuilding
// a client from the context endpoint via kubeconfig.NewClientConfig.
//
// 0.30.103 bug fix — WHY a *rest.Config and not just the endpoint:
// kubeconfig.NewClientConfig(ctx, ep) marshals the endpoint into a
// kubeconfig document and hands it to client-go's clientcmd loader. That
// path is CERT-AUTH-ONLY and base64-aware:
//   - it has NO token field — a token-bearing endpoint loses its only
//     credential (the SA client would then be unauthenticated);
//   - clientcmd base64-DECODES certificate-authority-data, so it requires
//     the CA to be base64-encoded. The per-user `<user>-clientconfig`
//     Secret stores credentials base64-encoded (the authn signup flow
//     base64-encodes them), so the per-user path works. But the snowplow
//     service account's in-cluster credentials — the projected
//     /var/run/secrets/.../token (a raw JWT) and ca.crt (raw PEM) — are
//     NOT base64-encoded. Feeding the raw-PEM SA CA through that path
//     fails with "illegal base64 data at input byte 0".
//
// So the SA cannot be expressed as a kubeconfig-loadable endpoint at all.
// The SA *rest.Config must be built directly from the raw in-cluster
// credentials (rest.InClusterConfig), then carried here so the resolver's
// client-construction sites use it verbatim — bypassing the
// base64/cert-only kubeconfig path entirely.
//
// This is a GENERAL mechanism, not a per-resource carve-out
// (feedback_no_special_cases.md): any internal driver can hand the
// resolver a pre-built *rest.Config; only the client SOURCE is
// parameterised, the resolver stays one code path. Ordinary per-user
// requests never set it and fall through to the unchanged
// kubeconfig.NewClientConfig path.
//
// rc is carried as `any` so the cache package does not couple to the
// k8s.io/client-go/rest type; the consuming site type-asserts to
// *rest.Config. nil rc returns the parent context unchanged.
func WithInternalRESTConfig(ctx context.Context, rc any) context.Context {
	if ctx == nil || rc == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyInternalRESTConfig, rc)
}

// InternalRESTConfigFromContext returns the internal-dispatch *rest.Config
// attached by WithInternalRESTConfig, or (nil, false) when none was set
// (the ordinary per-user request path — the caller then builds a client
// from the context endpoint via kubeconfig.NewClientConfig).
func InternalRESTConfigFromContext(ctx context.Context) (any, bool) {
	if ctx == nil {
		return nil, false
	}
	v := ctx.Value(ctxKeyInternalRESTConfig)
	if v == nil {
		return nil, false
	}
	return v, true
}

// ctxKeyServeWatcherType is the typed empty-struct context key for
// WithServeWatcher / ServeWatcherFromContext.
type ctxKeyServeWatcherType struct{}

var ctxKeyServeWatcher = ctxKeyServeWatcherType{}

// WithServeWatcher attaches the shared *ResourceWatcher to ctx so an
// internal-dispatch LIST can serve from the informer indexer instead of a
// live apiserver LIST when the GVR is servable — #121 1a.
//
// WHY (the #121 root cause): the internal-REST-config LIST path
// (dispatchViaInternalRESTConfig) ALWAYS issues a live paged apiserver LIST;
// it never consults the informer, even for a GVR whose informer is fully
// synced. At 50K compositions the boot re-walk's benchapps LIST is therefore
// a 27.5s / 60K-item live LIST that runs ~24s AFTER the informer already
// synced (measured: informer HasSynced ~3.9s, LIST at ~27.5s) — pure waste
// that eats the boot-scope budget and deadline-cuts the seed
// (docs/boot-walk-deadline-rootcause-2026-07-09.md).
//
// This mechanism is PREWARM-SCOPED by construction: only the Phase 1 walk /
// seed context attaches it (withPhase1SAContext, alongside the SA
// WithInternalRESTConfig it already sets). Ordinary per-user /call requests
// NEVER attach it, so their dispatch is byte-identical to pre-1a — the
// informer-serve branch is skipped entirely (ServeWatcherFromContext returns
// false) and the live LIST runs exactly as today.
//
// GENERAL, not a per-resource carve-out (feedback_no_special_cases): any
// internal driver can attach the watcher; the serve/fall-through decision is
// the uniform IsServable predicate over every GVR. nil rw returns the parent
// context unchanged (the fall-through-to-live-LIST path is preserved).
func WithServeWatcher(ctx context.Context, rw *ResourceWatcher) context.Context {
	if ctx == nil || rw == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyServeWatcher, rw)
}

// ServeWatcherFromContext returns the *ResourceWatcher attached by
// WithServeWatcher, or (nil, false) when none was set (the ordinary per-user
// request path — the caller then takes its unchanged live-LIST dispatch).
func ServeWatcherFromContext(ctx context.Context) (*ResourceWatcher, bool) {
	if ctx == nil {
		return nil, false
	}
	rw, ok := ctx.Value(ctxKeyServeWatcher).(*ResourceWatcher)
	if !ok || rw == nil {
		return nil, false
	}
	return rw, true
}
