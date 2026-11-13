package main

import (
	"log/slog"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/cache"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// envPublicIdentityEnabled is the #1334 opt-in. The chart renders it from
// publicIdentity.enabled, the same value that ships the impersonate grant, and
// refuses it in values.env so the two cannot disagree.
const envPublicIdentityEnabled = "PUBLIC_IDENTITY_ENABLED"

// newPublicIdentity returns the public identity backed by snowplow's own
// ServiceAccount transport when on, and nil (token-less requests are 401, as
// before #1334) when off.
func newPublicIdentity(on bool) *middleware.PublicIdentity {
	if !on {
		return nil
	}
	slog.Info("public identity enabled: a request with no Authorization header is served as the public identity, by impersonation",
		slog.String("identity_class", middleware.PublicIdentityClass),
		slog.String("needs", "the impersonate grant the chart value publicIdentity.enabled renders"))
	return middleware.NewPublicIdentity(idynamic.ServiceAccountEndpoint, idynamic.ServiceAccountRESTConfig)
}

// mountCallerIdentityRoutes mounts every route whose work is authorized by the
// caller's identity (#1334). Each takes callerIdentity: UserConfig, plus the
// public identity for a request with no Authorization header when public is
// non-nil (the install opted in). GET /rbac takes the plain UserConfig and
// NEVER the public identity: its enumeration runs under snowplow's own
// ServiceAccount, never under the caller's credential, so serving it token-less
// would act as the ServiceAccount for an anonymous caller
// (TestIssue1334_RouteTable_RBACNeverServesThePublicIdentity).
func mountCallerIdentityRoutes(routes *routeTable, chain use.Chain, keys jwtutil.KeySource, authnNS string, public *middleware.PublicIdentity) {
	callerIdentity := middleware.UserConfigOrPublic(keys, authnNS, public)

	routes.Handle("GET /list", chain.Append(
		callerIdentity,
		cache.FallthroughScopeMiddleware(cache.ScopeList)).
		Then(handlers.List()))
	cache.RegisterScopedRoute("GET /list", cache.ScopeList)

	// Ship D — GET /call is the canonical `/call` read path. Dispatcher
	// routes into the per-group restactions / widgets handlers; for the
	// scope label we use call-generic (the dispatcher's fallthrough lane
	// is the F-1 site). The restactions / widgets dispatcher entries
	// inherit the scope via the same ctx.
	routes.Handle("GET /call", chain.Append(
		callerIdentity,
		cache.FallthroughScopeMiddleware(cache.ScopeCallGeneric),
		handlers.Dispatcher(dispatchers.All())).
		Then(handlers.Call()))
	cache.RegisterScopedRoute("GET /call", cache.ScopeCallGeneric)

	// #186 — POST /call/read is the body-carrying READ path. It exists so a
	// large `extras` input rides the request BODY instead of the URL query,
	// where over HTTP/2 it is charged against the gateway's ~16 KiB header
	// budget and 431s before Snowplow is invoked. It shares the GET /call
	// chain EXACTLY — UserConfig, the call-generic read scope, and crucially
	// the dispatcher into the restactions / widgets resolve handlers — with
	// two additions: BodyExtrasDecode (decodes {"extras":{...}} → context, so
	// the context-first util.ParseExtras returns it) runs BEFORE the
	// dispatcher, and the dispatcher is the POST variant (ReadDispatcher —
	// the plain Dispatcher is GET-only by design). The terminal fallthrough
	// is handlers.CallRead(), the READ-ONLY /call (verb forced GET, no
	// Payload): a POST /call/read to an unhandled GVR is a read passthrough,
	// never a create. Same read-path invariant as GET /call, so it is
	// RegisterScopedRoute'd + listed in requiredScopedRoutes (a boot-assert
	// fails if either half is missing).
	routes.HandleUnaudited("POST /call/read", "a read: the body carries extras and the verb sent is GET (#186); a RESTAction write stage it reaches is the stage's record", chain.Append(
		callerIdentity,
		cache.FallthroughScopeMiddleware(cache.ScopeCallGeneric),
		middleware.BodyExtrasDecode,
		handlers.ReadDispatcher(dispatchers.All())).
		Then(handlers.CallRead()))
	cache.RegisterScopedRoute("POST /call/read", cache.ScopeCallGeneric)

	// GET /export — GENERIC export: any /call-resolvable list
	// (RESTAction or list/table Widget) serialized as a CSV/JSON/PDF
	// attachment. The handler re-dispatches IN-PROCESS through the same
	// dispatcher lane the GET /call route uses (identical auth, RBAC gate
	// and serve-time user-aware filtering — export can never see more
	// than the caller's own /call), then serializes the extracted rows.
	// The route shares the /call middleware chain, so the scope
	// classification and the read-path invariant hold; a scheduled
	// export is just a CronJob curling this route (see howto/export.md).
	//
	// POST /export (#970) is the same export with the extras in the request
	// BODY, for a filter too large for the URL (the gateway 431s a long
	// ?extras=); it mirrors POST /call/read exactly. Both export routes are
	// mounted by mountExportRoutes (so the route table is testable) and are
	// required scoped routes (requiredScopedRoutes).
	mountExportRoutes(routes, chain, callerIdentity)

	// GET /rbac — RESTAction read-set enumeration for core-provider RBAC
	// pre-generation (design docs/restaction-rbac-endpoint-design.md). It
	// resolves a referenced RESTAction's api[] stages to the (group, version,
	// resource, verb) tuples a /call WOULD read, WITHOUT dispatching.
	// Authenticated with the SAME JWT+clientconfig as /call (middleware.UserConfig):
	// the JWT authenticates the caller, but the enumeration uses NONE of the
	// caller's RBAC perms (it runs under the SA), so the read-set is computable
	// before any binding exists. DELIBERATELY NOT cache.RegisterScopedRoute'd:
	// like /refreshes above, it issues ZERO per-user apiserver reads, so it sits
	// outside the read-path-scoped invariant (fallthrough_assert.go).
	routes.Handle("GET /rbac", chain.Append(
		middleware.UserConfig(keys, authnNS)).
		Then(handlers.RBAC()))

	mountCallWriteRoutes(routes, chain, callerIdentity)

	routes.HandleUnaudited("POST /jq", "evaluates jq over the request body: no I/O, no state changes", chain.Append(callerIdentity).Then(handlers.JQ()))
}
