// fallthrough_middleware.go — Ship D (0.30.141). The HTTP middleware
// that stamps a FallthroughScope onto the request context — the
// pairing of Layer A (fallthrough_ctx.go) with the snowplow HTTP
// surface. Composed into every `/call`-class route in main.go BEFORE
// the dispatcher.
//
// PATTERN. The constructor returns a
// `func(http.Handler) http.Handler` so it slots into the existing
// `chain.Append(...)` shape `plumbing/server/use` already uses for
// `use.UserConfig` (main.go:530-537). Same call-site shape — no new
// middleware infrastructure.
//
// CLOSED-ENUM SCOPE NAMES. The `name` argument is one of the bounded
// scope constants (Scope* below). The constructor validates against
// the enum at boot — an unknown name panics. This is the PM's
// cardinality discipline as code: a future caller can't mint an
// ad-hoc scope name.
//
// WRITE VERBS — PM EXPLICIT. POST/PUT/PATCH/DELETE `/call` routes
// ALSO go through the middleware with `call-write-<verb>` scope
// names. Write paths are out of the architectural-consistency
// invariant (they're legitimately apiserver-bound — F-11 in the
// design), but the middleware MUST cover them so:
//
//   - classification is centralized (one boot-assert validates ALL
//     `/call`-class routes, GET and write);
//   - silent escapes are impossible — a future GET-class route
//     accidentally registered as `call-write-*` would still fire
//     the counter (the counter sees the scope label and produces
//     metrics on the wrong cell, immediately visible).
//
// BOOT-ASSERT INTEGRATION. The middleware also records the route's
// scope name in a package-level registry (`routeScopeRegistry`) so
// `AssertReadPathsScoped` (fallthrough_assert.go) can verify every
// registered route is scoped at startup. Without this, a new
// `mux.Handle` line that forgets the middleware would silently bypass
// the invariant.
package cache

import (
	"context"
	"fmt"
	"net/http"
	"sync"
)

// Closed-enum scope names — bounded by the dispatcher's route list.
// Adding a new `/call`-class route REQUIRES adding a constant here
// (and to validScopeNames below) — defence-in-depth on the
// `path` label cardinality.
const (
	ScopeCallRestactions   = "call-restactions"
	ScopeCallWidgets       = "call-widgets"
	ScopeCallGeneric       = "call-generic"
	ScopeCallWritePost     = "call-write-post"
	ScopeCallWritePut      = "call-write-put"
	ScopeCallWritePatch    = "call-write-patch"
	ScopeCallWriteDelete   = "call-write-delete"
	ScopeList              = "list"
	ScopePlurals           = "plurals"
	ScopeNestedCall        = "nested-call"
	ScopeResolverInnerCall = "resolver-inner-call"

	// ScopeBootPrewarmWalk — Gate 1 (0.30.201-diag) diagnostic scope, now ALSO
	// LOAD-BEARING for a behavioral gate (#42 Option-2). NOT a /call-class
	// route: it is stamped on the Phase 1 prewarm discovery-walk context
	// (phase1_walk.go withPhase1SAContext) so the existing
	// RecordApiserverFallthrough calls on that path (KindFor at
	// resourcesrefs/resolve.go, informer-not-synced/not-servable at
	// informer_dispatch.go) become LIVE during boot and land in
	// snowplow_apiserver_fallthrough_cells keyed
	// "boot-prewarm-walk|<gvr>|<reason>". This discriminates the Gate-1
	// sub-cause (discovery-not-ready vs informer-not-synced vs
	// apiRef-resolved-but-empty) for the navmenu's boot children=0
	// observation. It is NEVER registered as a route (RegisterScopedRoute
	// is not called for it), so AssertReadPathsScoped's route-coverage
	// check is unaffected.
	//
	// #42 Option-2 — apiref.shouldServeRAFullList EXCLUDES this scope from the
	// Ship-4a unpaginated first-sight (the discovery walk harvests only nav
	// STRUCTURE, not composition DATA, so the 60K unpaginated materialization
	// is pure waste that blows the PHASE1_TIMEOUT budget). Do NOT delete this
	// stamp believing it diagnostic-only — removing it re-introduces the ~411s
	// re-walk.
	ScopeBootPrewarmWalk = "boot-prewarm-walk"

	// ScopeBootPrewarmSeed — the per-cohort SEED context scope (#42 Option-2).
	// The seed (seedScopeYielding) runs under the SAME rctx the discovery walk
	// stamps ScopeBootPrewarmWalk on (rePrewarmBootScoped passes the walk-scoped
	// rctx to seedScopeYielding), and withCohortSeedContext layers values via
	// xcontext.BuildContext WITHOUT stripping the inherited scope. So the seed
	// cohort ctx would otherwise INHERIT ScopeBootPrewarmWalk and be wrongly
	// excluded from Ship-4a — but the seed's seedRAFullListForWidget resolve
	// EXISTS precisely to engage raFullListServe and pin the per-cohort
	// full-list cell (phase1_pip_seed.go:1237 "Resolve at a paginated tuple so
	// apiref.Resolve engages raFullListServe"). withCohortSeedContext OVERRIDES
	// the inherited walk scope with THIS seed scope (last WithValue on the chain
	// wins) so shouldServeRAFullList — which excludes ONLY ScopeBootPrewarmWalk
	// — lets the seed's paginated 4a serve through. NOT a route; diagnostic +
	// gate-discriminator only.
	ScopeBootPrewarmSeed = "boot-prewarm-seed"

	// ScopeRefresher — #988. The refresher's scope, stamped on the context of
	// every refresh it runs (runRefreshFunc: a dequeued key, and a
	// RunRefreshNow replace). A refresh is a resolve origin like a /call, so
	// its context carries the resolve marker for
	// snowplow_resolve_collection_lists, and its genuine fall-throughs land in
	// snowplow_apiserver_fallthrough_cells under path "refresher" instead of
	// going uncounted. NOT a route; never registered with RegisterScopedRoute.
	ScopeRefresher = "refresher"

	// ScopeTelemetry — #1015, the browser RUM ingest POST /telemetry/v1/traces.
	// The route issues ZERO apiserver reads (JWT validation and a forward to the
	// collector), so a fall-through landing under this scope is a defect by
	// definition. The issue makes it a registered scoped route so that such a
	// read is attributed and visible rather than unscoped.
	ScopeTelemetry = "telemetry"
)

// validScopeNames is the boot-time validation set used by
// FallthroughScopeMiddleware. Reads are lock-free (the map is
// constructed at package init and never written after).
var validScopeNames = map[string]struct{}{
	ScopeCallRestactions:   {},
	ScopeCallWidgets:       {},
	ScopeCallGeneric:       {},
	ScopeCallWritePost:     {},
	ScopeCallWritePut:      {},
	ScopeCallWritePatch:    {},
	ScopeCallWriteDelete:   {},
	ScopeList:              {},
	ScopePlurals:           {},
	ScopeNestedCall:        {},
	ScopeResolverInnerCall: {},
	ScopeBootPrewarmWalk:   {},
	ScopeBootPrewarmSeed:   {},
	ScopeRefresher:         {},
	ScopeTelemetry:         {},
}

// routeScopeRegistry records the routes whose middleware chain has
// been constructed via FallthroughScopeMiddleware. Boot-assert reads
// the registry and verifies the static expected route set is covered;
// a missing entry fires AssertReadPathsScoped (panic in test, log +
// counter in prod).
//
// Keyed by the canonical mux route pattern (e.g. "GET /call"); the
// value is the scope name. Constructors append to the registry
// idempotently — re-registering the same (route, scope) is a no-op
// (mux.Handle would itself panic on the dup).
var (
	routeScopeRegistryMu sync.Mutex
	routeScopeRegistry   = map[string]string{}
)

// FallthroughScopeMiddleware returns the http.Handler middleware that
// stamps a FallthroughScopeData{Active:true, Path:scopeName} onto the
// request ctx. Composes via the existing `chain.Append(...)` shape.
//
// VALIDATION. Panics at constructor time (i.e. at main.go-load) if
// scopeName is not in the closed enum. This is a regression gate:
// adding a new `/call`-class route without adding its constant
// here fails loud at process startup — the operator sees the panic
// in the deploy log immediately.
//
// CACHE-TOGGLE. When `cache.Disabled() == true` the WithFallthroughScope
// call below is a no-op (see fallthrough_ctx.go) — the ctx flows
// through unchanged, the Layer B wrappers observe no active scope,
// the counter stays silent. Cache-off mode keeps the invariant inert
// without disabling the middleware.
func FallthroughScopeMiddleware(scopeName string) func(http.Handler) http.Handler {
	if _, ok := validScopeNames[scopeName]; !ok {
		panic(fmt.Errorf("cache.FallthroughScopeMiddleware: scope name %q not in the closed enum "+
			"(see ScopeCall* / ScopeList / ScopePlurals / ScopeNestedCall / ScopeResolverInnerCall) "+
			"— add a constant in fallthrough_middleware.go before registering a new route", scopeName))
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := WithFallthroughScope(r.Context(), scopeName)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RegisterScopedRoute records a (mux route pattern -> scope name)
// pair in the boot-assert registry. Called from main.go alongside the
// `mux.Handle(...)` call for each `/call`-class route. The
// AssertReadPathsScoped boot check walks the registry and verifies
// every required route is covered.
//
// Idempotent: re-registering the same pattern with the same scope is
// a no-op. Re-registering with a DIFFERENT scope panics — that would
// indicate a mis-wired duplicate route registration.
func RegisterScopedRoute(pattern, scopeName string) {
	if _, ok := validScopeNames[scopeName]; !ok {
		panic(fmt.Errorf("cache.RegisterScopedRoute: scope name %q not in the closed enum", scopeName))
	}
	routeScopeRegistryMu.Lock()
	defer routeScopeRegistryMu.Unlock()
	if existing, dup := routeScopeRegistry[pattern]; dup && existing != scopeName {
		panic(fmt.Errorf("cache.RegisterScopedRoute: pattern %q already registered with scope %q; "+
			"refusing to overwrite with %q (duplicate registration is a wiring bug)",
			pattern, existing, scopeName))
	}
	routeScopeRegistry[pattern] = scopeName
}

// scopedRouteRegistrySnapshot returns a copy of the registry — used
// by AssertReadPathsScoped and its tests.
func scopedRouteRegistrySnapshot() map[string]string {
	routeScopeRegistryMu.Lock()
	defer routeScopeRegistryMu.Unlock()
	out := make(map[string]string, len(routeScopeRegistry))
	for k, v := range routeScopeRegistry {
		out[k] = v
	}
	return out
}

// ResetRouteScopeRegistryForTest clears the route-scope registry.
// TEST-ONLY — production code MUST NOT call it.
func ResetRouteScopeRegistryForTest() {
	routeScopeRegistryMu.Lock()
	defer routeScopeRegistryMu.Unlock()
	routeScopeRegistry = map[string]string{}
}

// Compile-time check: scope names are valid Go identifiers and
// suitable for a Prometheus label value (lowercase, hyphens). If a
// future constant violates this, the linter notices. (No runtime
// validation — the enum is closed at compile time.)
var _ context.Context // import-keeper if a future helper needs it
