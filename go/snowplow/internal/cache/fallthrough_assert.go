// fallthrough_assert.go — Ship D (0.30.141). The boot-time invariant
// assertion that catches a future `/call`-class route registered
// without the FallthroughScopeMiddleware. Analogue of
// `cache/strip.go:173` `AssertRBACTypedOverridesRegistered`, with the
// test/prod asymmetry the PM explicitly ratified:
//
//   - Test mode (env.TestMode() == true) → panic on missing
//     middleware. Tests must fail loud — a unit/integration test that
//     adds a new route without the middleware breaks immediately.
//   - Production (env.TestMode() == false) → log ERROR and increment
//     `snowplow_assertion_violations_total{check="read_paths_scoped"}`
//     once per missing route. The production posture is "diagnostic,
//     not lethal" — a missing-middleware regression should be loud in
//     logs + alertable from metrics, but it MUST NOT crash the pod
//     mid-deploy (the cost of an HTTP-500 server-down spike is far
//     larger than the cost of running with degraded invariant
//     visibility for one rollback cycle).
//
// CALL SITE. main.go invokes AssertReadPathsScoped AFTER all
// mux.Handle / cache.RegisterScopedRoute calls and BEFORE
// server.ListenAndServe. The same boot-path that runs
// AssertRBACTypedOverridesRegistered.
package cache

import (
	"log/slog"
	"sort"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/env"
)

// requiredScopedRoutes is the static expected set of `/call`-class
// route patterns. The boot-assert walks the registry built by
// RegisterScopedRoute (in fallthrough_middleware.go) and verifies
// every required entry is present. A future ship that adds a new
// `/call`-class route MUST add the pattern here AND call
// RegisterScopedRoute in main.go — the assert is the gate that catches
// either half going missing.
//
// Bounded enum — same discipline as validScopeNames. Adding a route
// is a deliberate code-level act.
var requiredScopedRoutes = []string{
	"GET /api-info/names",
	"GET /list",
	"GET /call",
	"POST /call",
	"PUT /call",
	"PATCH /call",
	"DELETE /call",
	// #186 — body-carrying READ path. Per-user apiserver reads (via the
	// resolve handlers or the read-only CallRead fallthrough) put it IN the
	// read-path invariant, exactly like GET /call — unlike /rbac & /refreshes,
	// which issue zero per-user reads and are deliberately unregistered.
	"POST /call/read",
	// #443 — the dry-run write twins. Same per-user apiserver writes as the
	// /call write verbs they mirror, so the same scoping rule applies.
	"POST /call/dry-run",
	"PUT /call/dry-run",
	"PATCH /call/dry-run",
}

// assertionViolationsTotal is the production-mode counter bumped by
// AssertReadPathsScoped when a required route is missing in
// non-test mode. Exposed via expvar in the same registration block as
// the fallthrough counter (see fallthrough_meter_expvar.go); the
// alert rule key is `snowplow_assertion_violations_total{check=
// "read_paths_scoped"}`.
var assertionViolationsTotal atomic.Uint64

// AssertionViolationsTotal returns the cumulative count of assertion
// violations observed in production mode. Exported for the AC-D.5 test
// gate.
func AssertionViolationsTotal() uint64 {
	return assertionViolationsTotal.Load()
}

// AssertReadPathsScoped verifies every required `/call`-class route
// has been registered with FallthroughScopeMiddleware (i.e. appears in
// the routeScopeRegistry).
//
// Test/prod asymmetry — PM ratified:
//
//   - env.TestMode()==true  → panic on any missing route. The test
//     binary must fail loud.
//   - env.TestMode()==false → log ERROR + Add(1) to
//     assertionViolationsTotal per missing route. The pod stays up.
//
// Returns the number of missing routes (0 means the invariant holds).
// Callers that need stronger semantics can act on the non-zero count
// directly (e.g. force a degraded-readiness flag); main.go's caller
// is fire-and-forget.
//
// Idempotent: re-runnable (e.g. a hot-reload that rebuilds the route
// set could call again). The assertion-violations counter accumulates
// — production operators decide on the per-window threshold.
func AssertReadPathsScoped() int {
	snap := scopedRouteRegistrySnapshot()
	missing := []string{}
	for _, want := range requiredScopedRoutes {
		if _, ok := snap[want]; !ok {
			missing = append(missing, want)
		}
	}
	if len(missing) == 0 {
		slog.Info("cache.read_paths_scoped.ok",
			slog.String("subsystem", "cache"),
			slog.Int("routes", len(snap)),
			slog.String("hint", "every /call-class route is FallthroughScopeMiddleware-wrapped — "+
				"Ship D invariant holds at boot"),
		)
		return 0
	}

	// Stable order for the panic / log message — deterministic
	// across runs irrespective of map iteration order.
	sort.Strings(missing)

	if env.TestMode() {
		// Test mode — fail loud. A unit/integration test that adds a
		// new route without RegisterScopedRoute / the middleware
		// surfaces here.
		panic("cache.AssertReadPathsScoped: required scoped routes missing in registry: " +
			joinStrings(missing, ", ") +
			" — every /call-class route must be registered via cache.RegisterScopedRoute " +
			"AND wrapped with cache.FallthroughScopeMiddleware in main.go")
	}

	// Production mode — log ERROR + bump the assertion-violations
	// counter once per missing route. The pod stays up.
	for _, m := range missing {
		assertionViolationsTotal.Add(1)
		slog.Error("cache.read_paths_scoped.violation",
			slog.String("subsystem", "cache"),
			slog.String("check", "read_paths_scoped"),
			slog.String("missing_route", m),
			slog.String("hint", "a /call-class route is registered without the "+
				"FallthroughScopeMiddleware — the architectural-consistency invariant "+
				"is unobservable on this route until the regression is fixed"),
		)
	}
	return len(missing)
}

// joinStrings is a tiny alloc-cheap concat — saves importing strings
// for one call site. Keeps the boot path import-light.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	total += len(sep) * (len(parts) - 1)
	out := make([]byte, 0, total)
	for i, p := range parts {
		if i > 0 {
			out = append(out, sep...)
		}
		out = append(out, p...)
	}
	return string(out)
}
