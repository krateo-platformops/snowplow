package rbac

import (
	"context"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// sa_regate.go — Part 2 (#268/#269, design §5): the ONE uniform guard for the
// remaining SA-transport path (the background refresher). It lives in `rbac`
// because it is the single package BOTH carriers can import — `objects`
// (getFromAPIServer, site 3) and `resolvers/restactions/api` (branch E, site 1)
// both import `rbac`, `rbac` imports `cache`, and IsServiceAccountUsername is here.
// (`objects` must not import `api`; `cache` must not import `rbac`.)

// ServesUnnarrowed reports whether a dispatch under ctx is a GENUINE
// ServiceAccount / identity-free operation that may serve un-narrowed (the SA
// maximal shell, re-narrowed at read). It is the single source of truth for the
// predicate api.internalDispatchServesUnnarrowed also uses:
//
//	(a) a Phase-1 walk / cohort seed / content-prewarm (ServeWatcher on ctx);
//	(b) an api-stage content-cell populate (WithApistageContentResolve);
//	(c) a truly identity-free populate (no UserInfo — cluster_list async);
//	(d) a canonical ServiceAccount username (the refresher's identity-free class).
//
// A REAL end-user — including the refresher's per-user-cohort REPRESENTATIVE
// identity (non-SA Username) and a group-only user — returns FALSE (must narrow).
func ServesUnnarrowed(ctx context.Context) bool {
	if _, ok := cache.ServeWatcherFromContext(ctx); ok {
		return true
	}
	if cache.ApistageContentResolveFromContext(ctx) {
		return true
	}
	user, err := xcontext.UserInfo(ctx)
	if err != nil {
		return true
	}
	if IsServiceAccountUsername(user.Username) {
		return true
	}
	return false
}

// saCredentialOnContext reports whether the ctx carries a snowplow SA credential
// (the internal endpoint or *rest.Config). Live per-user requests do NOT carry it
// post-Part-1 (the dispatcher attach was removed); the background refresher DOES
// (resolve_populate.go attaches its own SA transport). External endpoints and the
// per-user clientconfig path carry neither.
func saCredentialOnContext(ctx context.Context) bool {
	if _, ok := cache.InternalRESTConfigFromContext(ctx); ok {
		return true
	}
	if _, ok := cache.InternalEndpointFromContext(ctx); ok {
		return true
	}
	return false
}

// MustRegateSADial reports whether an SA-credentialed dispatch under ctx must be
// re-gated against the ctx identity before serving. It is applied at the two dial
// sites that would otherwise serve un-gated under a real (representative) identity:
// objects.getFromAPIServer (site 3, credential A) and branch E (site 1, credential B).
//
// THREE conjuncts:
//   - saCredentialOnContext — the SA endpoint/rest.Config is on the ctx.
//   - !ServesUnnarrowed — NOT a genuine SA / identity-free operation (i.e. a REAL
//     narrowing subject, e.g. the refresher's per-user-cohort representative).
//   - BackgroundResolveFromContext — the dispatch is a BACKGROUND re-resolve.
//
// The BackgroundResolve conjunct SCOPES the guard to the design §5 target — the
// background refresher/prewarm SA-transport re-resolve (both set WithBackgroundResolve:
// resolve_populate.go, prewarm_engine_boot.go). Post-Part-1 that is the COMPLETE set
// of production contexts carrying an SA credential under a real identity: a LIVE
// request has no SA cred on the ctx (the attach was removed), and every internal
// SA-credentialed driver either sets WithBackgroundResolve OR uses a serveUnnarrowed
// (Phase-1 ServeWatcher / canonical-SA) identity. So the conjunct is production-
// equivalent to (saCred && !serveUnnarrowed) while EXCLUDING a non-background
// real-user + internal-endpoint dispatch, which is not a production-reachable shape
// (only tests construct it). Verdict/outcomes:
//   - LIVE request (no SA cred on ctx)                           → false → unchanged.
//   - background refresher/prewarm, per-user representative      → true  → re-gate.
//   - background, genuine SA / identity-free (serveUnnarrowed)   → false → legitimate
//     un-gated SA serve unchanged.
//
// One predicate, no per-resource carve-outs (feedback_no_special_cases).
// NOTE (arch-268 to bless): the design stated `saCredentialOnCtx && !serveUnnarrowed`;
// the BackgroundResolve conjunct is a scoping refinement (see above) — it does not
// change which PRODUCTION paths are re-gated.
func MustRegateSADial(ctx context.Context) bool {
	return cache.BackgroundResolveFromContext(ctx) &&
		saCredentialOnContext(ctx) &&
		!ServesUnnarrowed(ctx)
}
