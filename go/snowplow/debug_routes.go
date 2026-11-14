// debug_routes.go — O-D3 (1.12.3): the whole /debug/* surface behind the same
// JWT gate /debug/refreshes already uses; #1362: and behind an RBAC
// authorization of the caller for the route's path.
//
// WHY THIS FILE EXISTS
//
// The debug surface (`/debug/pprof/*`, `/debug/vars`, `/debug/servable`,
// `/debug/apistage`, `/debug/refreshes`) is mounted on the SAME mux and the
// SAME port the chart publishes through a `service.type: LoadBalancer`
// (helm/snowplow/values.yaml). Before 1.12.3 only `/debug/refreshes` was gated
// (#69); the other four were world-readable on that LB:
//
//   - `/debug/pprof/*` returns heap/CPU/goroutine profiles — goroutine dumps
//     carry stack frames and `cmdline` carries the process argv;
//   - `/debug/vars` returns the whole expvar registry, which includes
//     per-cell maps keyed by `path|gvr|reason` (fall-through cells,
//     `snowplow_dispatch_l1_lookups`) — a free map of the tenant's GVRs;
//   - `/debug/servable` and `/debug/apistage` return per-GVR / per-entry
//     cache METADATA (never resolved bodies — that structural guard stays),
//     which is still an unauthenticated inventory of the cluster's shape.
//
// `/debug/profile` and `/debug/trace` are additionally a trivial availability
// lever: an anonymous caller can pin a CPU for the profile duration.
//
// THE GATE IS THE EXISTING ONE. registerDebugRoutes wraps every debug route in
// `middleware.RefreshAuth(jwtKeys)` — the cookie-or-header stateless RS256
// validation `/debug/refreshes` has used since #69, chosen deliberately over
// `middleware.UserConfig` because it performs ZERO apiserver reads (no
// `<user>-clientconfig` Secret GET), so a diagnostic pull never perturbs the
// cache-respecting invariant it is being used to diagnose. Missing, expired or
// invalid → 401; unfetchable JWKS → 503.
//
// #1362: AUTHENTICATION IS NOT ENOUGH. A valid JWT alone no longer passes.
// Several routes report metadata about EVERY identity's cache entries and about
// objects the caller's own RBAC may forbid. /debug/apistage, for example, lists
// each resident entry's coordinates and item count, whoever created it. So
// every route is ALSO wrapped in middleware.DebugAuthz, which authorizes the
// request through Kubernetes RBAC as a non-resource request: verb = lowercase
// HTTP method, path = req.URL.Path, for the JWT's username and groups. The
// decision is rbac.EvaluateRBAC, the one authorizer snowplow has: in-process
// with cache on, so still zero apiserver reads, and a SelfSubjectAccessReview
// with cache off. Denied → 403. Undecidable → 503 with a named reason, never
// 200. The grant is an ordinary ClusterRole rule, `nonResourceURLs:
// ["/debug/*"]` with verbs get and post. The chart ships it as
// `<fullname>-debug-<namespace>` (helm/snowplow/templates/
// clusterrole-debug.yaml) and binds it to no one. Any identity bound to
// cluster-admin already holds it (`nonResourceURLs: ["*"]`). The guard is
// mechanical, at three levels:
//   - every route below goes through one `mount`, which wraps it in the
//     `gated` chain;
//   - at runtime, routeTable refuses (panics at boot) a /debug pattern
//     mounted any other way;
//   - in the tests, TestIssue1362_EveryRoute_DeniesWithoutTheGrantAdmitsWithIt
//     (B1) drives every registered route without and with the grant, and
//     TestIssue1362_NoDebugRouteMountedOutsideTheGate (B5) checks the lanes
//     structurally, by type.
//
// KNOWN LIMITATION (#1362). With CACHE_ENABLED=false the authorizer is the
// requester's own SelfSubjectAccessReview, which needs the requester's
// clientconfig endpoint. RefreshAuth deliberately does not read that Secret, so
// with the cache off every /debug route answers 503 `review_unavailable`. With
// the cache on, the same 503 (`rbac_snapshot_not_built`) covers the boot window
// before the RBAC snapshot is published, pprof included (#1368, see #174).
//
// NOT GATED (deliberate): `/health` and `/readyz` stay anonymous — they are the
// kubelet probe targets (howto/operating.md, "the chart ⇄ binary probe
// contract") and the kubelet presents no JWT. `/swagger/` is unchanged.
//
// KNOWN LIMITATION (accepted here, follow-up ticketed). RefreshAuth maps
// jwtutil.ErrKeyUnavailable to 503 (refreshauth.go:124-127), so while authn or
// its JWKS endpoint is unreachable NO token can be verified and this whole
// surface answers 503 — including /debug/pprof/goroutine, which is exactly the
// dump an operator wants during that incident. The gate is on the mux, not on
// the network path, so a loopback curl from inside the container is 503 too:
// there is no break-glass today. The candidate follow-ups are binding the debug
// mux to localhost (so `kubectl port-forward` always works regardless of authn)
// or a static operator token. Documented for operators under "Known limitation"
// in howto/operating.md; deliberately NOT solved in this change.
//
// TESTABILITY: the registration is a function rather than a straight-line block
// in main() so main_debug_auth_test.go can build the real mux and drive real
// requests through it — the same "extracted, named constructor" shape
// snowplowCORSOptions uses for the CORS contract (main_cors_test.go).

package main

import (
	"expvar"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// debugRoutePatterns is the exact set of mux patterns registerDebugRoutes
// mounts, in registration order. Exported to the test only (same package) so
// the falsifier enumerates the production set rather than a hand-copied list:
// a future debug route added to registerDebugRoutes but NOT to this slice
// fails the "every registered pattern is gated" arm.
var debugRoutePatterns = []string{
	"GET /debug/pprof/",
	"GET /debug/pprof/cmdline",
	"GET /debug/pprof/profile",
	"GET /debug/pprof/symbol",
	"GET /debug/pprof/trace",
	"GET /debug/vars",
	"GET /debug/servable",
	"GET /debug/apistage",
	"GET /debug/refreshes",
	"GET /debug/reconcile",
	"GET /debug/harvest",
	"GET /debug/store",
	"GET /debug/shadow-parity",
	"POST /debug/shadow-parity",
	"GET /debug/deps",
}

// debugMux is the registration surface registerDebugRoutes needs: the ONE
// lane onto /debug (routeTable.handleDebug). The method is unexported, so only
// a package-main type can offer it. The falsifier passes a recording wrapper,
// so it can assert the EXACT set of patterns production registers: drift
// between registerDebugRoutes and debugRoutePatterns fails the test.
type debugMux interface {
	handleDebug(pattern string, handler http.Handler)
	// declareUnaudited declares, before it is mounted, that a debug route
	// that can change something is not audited, and why (see routeTable).
	declareUnaudited(pattern, reason string)
	// declareAudited declares, before it is mounted, a debug route's audit
	// descriptor: handleDebug then wraps it outermost in audit.OpenRecord.
	declareAudited(pattern string, route audit.Route)
}

// routeRegistrar is what a non-debug route registrar (mountCallWriteRoutes,
// mountExportRoutes, mountTelemetryRoute) needs: routeTable, which refuses a
// /debug pattern and an undeclared route that can change something (tests pass
// newRouteTable over their own mux).
type routeRegistrar interface {
	Handle(pattern string, handler http.Handler)
	HandleAudited(pattern string, route audit.Route, handler http.Handler)
	HandleUnaudited(pattern, reason string, handler http.Handler)
}

// routeTable is the ONLY registrar of snowplow's server mux (#1362 review). It
// keeps /debug behind the RBAC gate at RUNTIME, whatever form a pattern takes:
// a literal, a constant, a computed string, or a table entry.
//
//   - Handle mounts every non-debug route. A pattern under /debug PANICS, at
//     boot, naming the route. A /debug route cannot reach the mux by the
//     general lane, so a new one mounted without the gate fails the first time
//     the binary starts (and in every test that builds the route).
//   - handleDebug is the /debug lane. Its only caller is registerDebugRoutes'
//     mount, which wraps every handler in the gated chain. It refuses a
//     non-debug pattern, so the two lanes stay disjoint.
//
// TestIssue1362_NoDebugRouteMountedOutsideTheGate checks the rest
// structurally, by TYPE rather than by pattern text. No code in package main
// calls Handle/HandleFunc on an *http.ServeMux except routeTable itself. The
// only handleDebug call is mount's, and mount's handler is gated.Then(...).
//
// AUDIT (AUDIT-4 installer#410; #1379 review F2). Which routes are audited,
// and as what, is declared here, once, at registration:
//
//   - HandleAudited mounts a route with its audit descriptor (audit.Route),
//     wrapped OUTERMOST in audit.OpenRecord, before any middleware decodes a
//     body or verifies a token: every exit of the route then emits exactly
//     one record, the handler's or, for an exit that emitted none, the
//     descriptor's.
//   - HandleUnaudited mounts a route that is not audited, with the reason in
//     words; a debug route declares it (declareUnaudited) before mount.
//   - Handle and handleDebug PANIC, at boot, for a pattern whose method can
//     change something (POST, PUT, PATCH, DELETE, or no method) and that is
//     not declared: such a route must say which of the two it is, so a new
//     one is never silently rowless. TestAudit_RouteTable_* pins it.
type routeTable struct {
	mux *http.ServeMux
	// audited and unaudited are the declarations: pattern -> descriptor,
	// pattern -> reason.
	audited   map[string]audit.Route
	unaudited map[string]string
}

func newRouteTable(mux *http.ServeMux) *routeTable {
	return &routeTable{mux: mux, audited: map[string]audit.Route{}, unaudited: map[string]string{}}
}

// Handle mounts a non-debug route whose method changes nothing (GET, HEAD,
// OPTIONS); a /debug pattern, or a method that can change something, panics
// (see routeTable).
func (t *routeTable) Handle(pattern string, handler http.Handler) {
	t.notDebug(pattern)
	mustBeSafe(pattern)
	t.mux.Handle(pattern, handler)
}

// HandleAudited mounts a non-debug route with its audit descriptor (see
// routeTable).
func (t *routeTable) HandleAudited(pattern string, route audit.Route, handler http.Handler) {
	t.notDebug(pattern)
	if route.Asked == nil {
		panic(fmt.Sprintf("route %q is declared audited with no descriptor", pattern))
	}
	t.audited[pattern] = route
	t.mux.Handle(pattern, audit.OpenRecord(route)(handler))
}

// HandleUnaudited mounts a non-debug route that is not audited, with the
// reason; an empty reason panics.
func (t *routeTable) HandleUnaudited(pattern, reason string, handler http.Handler) {
	t.notDebug(pattern)
	t.unaudit(pattern, reason)
	t.mux.Handle(pattern, handler)
}

func (t *routeTable) notDebug(pattern string) {
	if isDebugPattern(pattern) {
		panic(fmt.Sprintf("route %q is under /debug: it must be mounted by registerDebugRoutes, "+
			"behind the RefreshAuth + DebugAuthz gate (#1362)", pattern))
	}
}

// handleDebug is the /debug lane (see routeTable). A route whose method can
// change something must have been declared (declareUnaudited) first.
func (t *routeTable) handleDebug(pattern string, handler http.Handler) {
	if !isDebugPattern(pattern) {
		panic(fmt.Sprintf("route %q is not under /debug: mount it with Handle", pattern))
	}
	if route, audited := t.audited[pattern]; audited {
		t.mux.Handle(pattern, audit.OpenRecord(route)(handler))
		return
	}
	if _, declared := t.unaudited[pattern]; !declared {
		mustBeSafe(pattern)
	}
	t.mux.Handle(pattern, handler)
}

// declareAudited declares a debug route's audit descriptor before mount (see
// handleDebug); a descriptor with no Asked panics.
func (t *routeTable) declareAudited(pattern string, route audit.Route) {
	if route.Asked == nil {
		panic(fmt.Sprintf("route %q is declared audited with no descriptor", pattern))
	}
	t.audited[pattern] = route
}

// declareUnaudited declares that pattern, a route that can change something,
// is not audited, and why; an empty reason panics.
func (t *routeTable) declareUnaudited(pattern, reason string) { t.unaudit(pattern, reason) }

func (t *routeTable) unaudit(pattern, reason string) {
	if strings.TrimSpace(reason) == "" {
		panic(fmt.Sprintf("route %q is declared unaudited with no reason", pattern))
	}
	t.unaudited[pattern] = reason
}

// mustBeSafe panics unless pattern's method changes nothing: GET (which the
// mux also serves for HEAD), HEAD or OPTIONS. A pattern with no method serves
// every method, so it is not safe.
func mustBeSafe(pattern string) {
	method, _, ok := strings.Cut(pattern, " ")
	if ok && (method == http.MethodGet || method == http.MethodHead || method == http.MethodOptions) {
		return
	}
	panic(fmt.Sprintf("route %q can change something: mount it with HandleAudited (its audit descriptor) "+
		"or HandleUnaudited (and the reason)", pattern))
}

// isDebugPattern reports whether a ServeMux pattern ("[METHOD ][HOST]/PATH")
// serves a path that is /debug or below it. The path is unescaped first: the
// mux unescapes pattern segments, so "/%64ebug/x" routes /debug/x and must
// count. An unescapable path counts as debug, so the check fails closed.
func isDebugPattern(pattern string) bool {
	i := strings.IndexByte(pattern, '/')
	if i < 0 {
		return false
	}
	path, err := url.PathUnescape(pattern[i:])
	if err != nil {
		return true
	}
	return path == "/debug" || strings.HasPrefix(path, "/debug/")
}

// registerDebugRoutes mounts the whole /debug/* surface on mux, each route
// wrapped in chain + middleware.RefreshAuth(jwtKeys) + middleware.DebugAuthz().
//
// Behaviour past the gate is byte-identical to the pre-1.12.3 handlers: the
// same pprof.Index/Cmdline/Profile/Symbol/Trace funcs, the same
// expvar.Handler(), the same handlers.DebugServable/DebugApistage/DebugRefreshes.
// The ONLY change is that an unauthenticated caller now gets 401 instead of the
// body.
//
// Callers MUST have registered every expvar publisher (cache.RegisterRBACSnapshotExpvar,
// rbac.RegisterAuthzMemoExpvar, …) BEFORE calling this — the mount accepts
// scrapes as soon as the server starts.
func registerDebugRoutes(mux debugMux, chain use.Chain, jwtKeys jwtutil.KeySource) {
	// #1362: authenticate (RefreshAuth), THEN authorize (DebugAuthz) every
	// route. Both are in this one chain, so no route can take one without the
	// other.
	gated := chain.Append(middleware.RefreshAuth(jwtKeys), middleware.DebugAuthz())
	// mount is the only call of handleDebug: every route below goes through
	// it, so every route takes the gated chain.
	mount := func(pattern string, handler http.Handler) {
		mux.handleDebug(pattern, gated.Then(handler))
	}

	// /debug/pprof/* — registered on the custom mux (server does NOT use
	// http.DefaultServeMux). Exposes goroutine, heap, profile, allocs, mutex,
	// block, cmdline, symbol, threadcreate, trace. pprof's handlers are plain
	// http.HandlerFunc values, so they wrap exactly like any other handler.
	mount("GET /debug/pprof/", http.HandlerFunc(pprof.Index))
	mount("GET /debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mount("GET /debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mount("GET /debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mount("GET /debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	// Ship D.1 (0.30.142) — expvar.Handler() carries Ship D's counters
	// (snowplow_apiserver_fallthrough_total / _cells,
	// snowplow_assertion_violations_total), the RBAC publish-seq
	// (cache.RegisterRBACSnapshotExpvar) and the Ship L2 snapshot authz-memo
	// counters (rbac.RegisterAuthzMemoExpvar). All of those are registered by
	// main() BEFORE this mount.
	mount("GET /debug/vars", expvar.Handler())

	// Fix #1 / stale-delete diagnostic — read-only per-GVR servability
	// snapshot {HasSynced, watchBroken, confirmed, servable}
	// (docs/rca-stale-delete-compositiondefinitions-informer-2026-06-25.md).
	// Mutates no state; available in both cache-on and cache-off so the
	// stale-delete latch (registered-but-unconfirmed / watch-broken GVR) is
	// diagnosable without a kubectl exec.
	mount("GET /debug/servable", handlers.DebugServable())

	// R1 diagnostic — read-only METADATA-ONLY snapshot of resolved-output
	// cache entries (class/path/gvr/age/ttl/items_count), for diagnosing a
	// degraded apistage entry (stale getComposition / cluster-scoped
	// allCompositionResources) without a kubectl exec. NEVER returns resolved
	// bodies (per-identity RBAC-sensitive) — the structural leak guard is
	// cache.ResolvedEntryMeta's type shape
	// (docs/design-r1-allcompositionresources-invalidation-2026-06-26.md §6).
	mount("GET /debug/apistage", handlers.DebugApistage())

	// #61 diagnostic — read-only AGGREGATE-ONLY refresh-broadcaster counters
	// (published/delivered/dropped/coalesced + subscriber count), the
	// on-cluster instrument for verifying live-refresh delivery
	// (refreshDeliveredTotal>0 for an armed key under churn) without a kubectl
	// exec. NO per-subscription-key/identity enumeration — totals only (a
	// per-key dump would be a cross-user signal)
	// (docs/rca-refreshes-zero-delivery-2026-06-26.md §5).
	//
	// #69 gated this route first; 1.12.3 (O-D3) extends the SAME gate to its
	// four unauthenticated siblings above, so this line is now the general
	// case rather than the exception.
	mount("GET /debug/refreshes", handlers.DebugRefreshes())

	// 1.12.6 C3 — opt-in FULL reconcile audit: walks every resident L1
	// entry, probes each self coordinate against the informer indexer and
	// hands every ABSENT one to the dep-event worker (the periodic ticker
	// does the same on a 512-entry sample every 30 s). Returns the
	// divergent set as METADATA ONLY (key hash / class / gvr / ns / name —
	// the same projection /debug/apistage exposes; never a body). Behind
	// the same gate as its siblings because it is a reconcile, not a dry
	// run, and because the full walk holds the store mutex for its duration
	// (docs/architecture/observability.md).
	mount("GET /debug/reconcile", handlers.DebugReconcile())

	// 1.12.7 observability — the Phase-1 harvester diagnostic. Same JWT gate,
	// same metadata-only contract as /debug/apistage: counts and booleans, no
	// harvested object ever leaves the pod. This is what lets the acceptance
	// step prove a confirmed-gone coordinate was actually FORGOTTEN, which
	// until now was observable only in tests.
	mount("GET /debug/harvest", handlers.DebugHarvest())

	// #237 — the per-object informer-store read. The incident it exists for
	// could not be diagnosed because every surface above is DOWNSTREAM of the
	// store: /debug/apistage reports L1 entry metadata and /debug/reconcile
	// probes L1 against the informer's own indexer, so "is the store stale or
	// is the cell stale?" renders identically either way. This route reports
	// the store's own resourceVersion/uid for one coordinate.
	//
	// Same gate, same metadata-and-hashes-only contract as its siblings, and
	// it reads NOTHING from the apiserver: a comparison served as the snowplow
	// ServiceAccount would tell any holder of the /debug grant the existence
	// and resourceVersion of objects their own RBAC forbids. The operator runs
	// that half with kubectl, under their own identity.
	mount("GET /debug/store", handlers.DebugStore())

	// #272 (1.12.17) — the runtime flip surface for the v7 Step-2 shadow-parity
	// toggle. GET reports the process-local, default-off toggle; POST
	// ?enabled=true|false flips it and reports the committed live state (absent
	// or unparseable → 400). Same gate as every sibling; since #1362 the POST
	// needs the `post` verb on its path (any authenticated caller could flip
	// it before). It is a zero-blast-radius control because the
	// shadow-parity hook is read-only and changes no verdict/byte/key — it only
	// turns the dark parity MEASUREMENT on or off. The toggle is per-pod, so a
	// measurement that uses it must pin to one pod (see the handler doc).
	mount("GET /debug/shadow-parity", handlers.DebugShadowParityGet())
	// #909 — the flip is a state change: audited (handlers.ShadowParityRoute,
	// target url.path), on the same gated chain.
	mux.declareAudited("POST /debug/shadow-parity", handlers.ShadowParityRoute())
	mount("POST /debug/shadow-parity", handlers.DebugShadowParitySet())

	// #277 (edge-3) — the read-only dep-graph diagnostic. Same JWT gate, same
	// METADATA-ONLY contract as /debug/apistage: it returns dependency edges as
	// cluster coordinates + per-cell freshness metadata + a body sha256, NEVER a
	// resolved body or a pre-hash key input. ?key inspects one cell; ?filter=
	// missing-backing-edges / stale-risk surface the serve-seam staleness class
	// (the C3 residual detector). The ?filter scans are lock-free (reverse-index
	// Range) and capped, so a diagnostic pull never holds a store/serve mutex
	// for a scan-length span.
	mount("GET /debug/deps", handlers.DebugDeps())
}
