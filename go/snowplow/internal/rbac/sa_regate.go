package rbac

import (
	"context"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
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
//	(b) an api-stage content-cell populate (WithApistageContentResolve);
//	(c) a truly identity-free populate (no UserInfo — cluster_list async, or a
//	    Phase-1 walk whose SA token carries no canonical subject);
//	(a+d) the identity IS snowplow's own ServiceAccount: the username EQUALS the
//	    subject of the SA credential on the ctx (isSnowplowSAIdentity). This covers
//	    the Phase-1 walk and content prewarm (ServeWatcher, identity from
//	    phase1SAUsername) and the refresher's identity-free class (the same
//	    phase1SAUsername identity + WithInternalEndpoint(saEP)).
//
// Neither a ServeWatcher (#425) nor the canonical ServiceAccount FORM of a
// username (#427) exempts anything on its own. The cohort seed carries a
// ServeWatcher but resolves as the cohort REPRESENTATIVE; a representative can
// itself be a tenant ServiceAccount (cache.pickRepresentativeFromSubjects), and
// the refresher re-resolves its cell under that identity with snowplow's SA
// transport. An un-narrowed serve in either case caches SA-fetched data under
// that cohort's key. A ctx whose SA credential carries no decodable subject, or
// a different one, narrows (fail closed).
//
// A REAL end-user — including the refresher's per-user-cohort REPRESENTATIVE
// identity, the cohort seed's representative, a tenant ServiceAccount, and a
// group-only user — returns FALSE (must narrow).
func ServesUnnarrowed(ctx context.Context) bool {
	if cache.ApistageContentResolveFromContext(ctx) {
		return true
	}
	user, err := xcontext.UserInfo(ctx)
	if err != nil {
		return true
	}
	return isSnowplowSAIdentity(ctx, user.Username)
}

// isSnowplowSAIdentity reports whether username is the identity of the snowplow
// ServiceAccount credential carried on ctx. The canonical source is the one the
// Phase-1 builders install the identity from (dispatchers.phase1SAUsername): the
// `sub` claim of the projected SA token in the ctx's internal endpoint, or of the
// internal *rest.Config's bearer token when no endpoint token is present. No
// literal names the SA. A ctx with no decodable SA credential has no snowplow-SA
// identity to match, so the answer is false (narrow).
func isSnowplowSAIdentity(ctx context.Context, username string) bool {
	if !IsServiceAccountUsername(username) {
		return false
	}
	sa, ok := snowplowSAUsernameFromContext(ctx)
	return ok && sa == username
}

func snowplowSAUsernameFromContext(ctx context.Context) (string, bool) {
	var token string
	if v, ok := cache.InternalEndpointFromContext(ctx); ok {
		switch ep := v.(type) {
		case *endpoints.Endpoint:
			if ep != nil {
				token = ep.Token
			}
		case endpoints.Endpoint:
			token = ep.Token
		}
	}
	if token == "" {
		if v, ok := cache.InternalRESTConfigFromContext(ctx); ok {
			if rc, rcOK := v.(*rest.Config); rcOK && rc != nil {
				token = rc.BearerToken
			}
		}
	}
	if token == "" {
		return "", false
	}
	ui, err := jwtutil.ExtractUserInfo(token)
	if err != nil || !IsServiceAccountUsername(ui.Username) {
		return "", false
	}
	return ui.Username, true
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
// (snowplow-SA / identity-free) identity. So the conjunct is production-
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
