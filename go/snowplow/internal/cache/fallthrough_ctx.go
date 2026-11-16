// fallthrough_ctx.go — Ship D (0.30.141, architectural-consistency
// invariant). Layer A request-scope marker — paired with the Layer B
// counter in fallthrough_meter.go and the route-level middleware in
// internal/handlers/middleware/fallthrough_scope.go.
//
// PURPOSE — the scope marker discriminates "this apiserver request
// originates from a `/call`-class user-request handler" (counter
// fires) vs "this apiserver request originates from a startup /
// background / non-`/call` path" (counter silent). Without the
// marker, every apiserver construction at process scope would record
// — Phase 1 walker, watcher bootstrap, refresher, etc. — and the
// counter would be useless for diagnosing user-request fall-through.
//
// MARKER LIFECYCLE. The middleware in internal/handlers/middleware/
// fallthrough_scope.go stamps the scope onto the request context
// BEFORE the dispatcher. The handler chain then propagates the ctx
// through the resolver into Layer B wrappers; each
// RecordApiserverFallthrough call reads the scope via FallthroughScope.
// On the cache-off path the middleware is still installed but
// `Disabled()==true` makes both the stamper (here) and the recorder
// (fallthrough_meter.go) early-return — the invariant inert.
//
// CACHE-TOGGLE COMPLIANCE. WithFallthroughScope returns the parent
// ctx unchanged when `cache.Disabled() == true` — no stamping. This
// preserves the project_caching_is_provisional contract: cache=off
// makes the apiserver hops legitimate, and the counter machinery has
// no business observing them.
package cache

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// fallthroughScopeCtxKey is the context-value key for the request-
// scope marker. A package-level zero-size struct so its address is
// stable, comparable, and bounded — the standard Go idiom for
// context-value keys (matches plumbing/context's key shape).
type fallthroughScopeCtxKey struct{}

// FallthroughScopeData is the marker payload — a small immutable
// struct embedded as a context value by FallthroughScopeMiddleware.
// Fields:
//
//   - Active: true ⇔ this ctx is INSIDE a `/call`-class read path.
//     Set to true by the middleware unconditionally (the path of the
//     middleware is itself the gate); the `cache.Disabled()` short-
//     circuit happens upstream in `WithFallthroughScope` so a stamped
//     scope ALWAYS has Active==true at the read site.
//   - Path:   the closed-enum scope name (e.g. "call-restactions",
//     "call-widgets", "call-write-post"). Used as the `path` label
//     on the `snowplow_apiserver_fallthrough_total` counter.
//
// Immutable post-stamp: the middleware constructs ONE *FallthroughScopeData
// per request and stamps it via WithValue; no goroutine mutates the
// pointed-to struct. Readers are lock-free (context.Value is a
// goroutine-safe tree walk).
type FallthroughScopeData struct {
	Active bool
	Path   string
}

// WithFallthroughScope stamps a scope marker onto ctx. For `/call`-class
// ROUTE scopes the FallthroughScopeMiddleware (internal/handlers/middleware/)
// is the single entry point so boot-time AssertReadPathsScoped can verify the
// route wiring. Two NON-route boot scopes are stamped directly in production —
// ScopeBootPrewarmWalk (phase1_walk.go withPhase1SAContext) and
// ScopeBootPrewarmSeed (phase1_pip_seed.go withCohortSeedContext); both are
// diagnostic + gate-discriminator scopes that are never registered as routes,
// so AssertReadPathsScoped (which iterates only the required ROUTE set) is
// unaffected by them.
//
// Cache-toggle gate: when `cache.Disabled() == true`, returns the
// parent ctx UNCHANGED. Cache=off mode makes every apiserver hop
// legitimate by contract; the counter machinery must stay silent.
//
// `path` should be one of the closed scope names (call-restactions,
// call-widgets, call-generic, call-write-post, call-write-put,
// call-write-patch, call-write-delete, list, plurals, nested-call,
// resolver-inner-call). The middleware constructor enforces the
// bounded set.
//
// #988 — THE SCOPE IS THE RESOLVE MARKER. Every resolve origin scopes its
// context here (the /call middleware, the boot prewarm walk and seed, the
// refresher), so this is also where the context is marked for the wire-level
// resolve collection-LIST counter (credhttp.MarkResolve,
// snowplow_resolve_collection_lists). A new scope starts with no fall-through
// reason; RecordApiserverFallthrough stamps one.
func WithFallthroughScope(ctx context.Context, path string) context.Context {
	if Disabled() {
		return ctx
	}
	ctx = credhttp.MarkResolve(ctx)
	return context.WithValue(ctx, fallthroughScopeCtxKey{}, &FallthroughScopeData{
		Active: true,
		Path:   path,
	})
}

// FallthroughScope returns the request's scope marker (or nil if the
// ctx is not inside a `/call`-class read path — e.g. Phase 1 walker,
// watcher bootstrap, refresher). The Layer B wrappers in
// fallthrough_meter.go use this to short-circuit the counter on
// non-`/call` callers.
//
// Lock-free: context.Value walks the parent chain — goroutine-safe by
// the Go runtime contract.
func FallthroughScope(ctx context.Context) *FallthroughScopeData {
	if ctx == nil {
		return nil
	}
	v := ctx.Value(fallthroughScopeCtxKey{})
	if v == nil {
		return nil
	}
	s, ok := v.(*FallthroughScopeData)
	if !ok {
		return nil
	}
	return s
}
