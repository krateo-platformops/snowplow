package cache

import "context"

// sa_dial_marker.go — #267/#268/#269 dial-site provenance marker. Mirrors the
// WithApistageContentResolve pair (deps.go): a context.Value flag, no parameter
// threaded through the resolver call chain.
//
// PROVENANCE, NOT SHAPE (arch-268 safety ruling). The snowplow SA endpoint and a
// TOKEN-auth per-user <user>-clientconfig are SHAPE-IDENTICAL at the dial site —
// same apiserver ServerURL (the nil-ref internal path overrides it to
// https://kubernetes.default.svc), same cluster CAData, same token-auth,
// !HasCertAuth — so no field predicate can tell them apart. The ONLY reliable
// signal is WHERE the endpoint came from: resolveOne returns the SA endpoint ONLY
// on its InternalEndpointFromContext branch (the WithInternalEndpoint attach always
// carries the SA endpoint), and the UAF stage takes it via serviceAccountEndpointFn.
// Those two sites mark the per-stage dispatch ctx here; a clientconfig resolved via
// FromSecret is never marked. The dial site then threads the SA token FILE
// (self-reloading) only for a marked dispatch, and a per-user clientconfig keeps
// its own static token — closing #267 without a shape predicate that would
// misclassify a token-auth clientconfig and reintroduce #268/#269.

// ctxKeyServiceAccountDialType is the typed context key for the SA-dial provenance
// marker. Distinct unexported type — same collision-safety as the other ctx keys.
type ctxKeyServiceAccountDialType struct{}

var ctxKeyServiceAccountDial = ctxKeyServiceAccountDialType{}

// WithServiceAccountDial returns a child context marked as dispatching under
// snowplow's own ServiceAccount endpoint (provenance: resolved from the
// context-carried internal endpoint, or the UAF SA endpoint). The dial site
// (api.httpClientForEndpoint) consults it via ServiceAccountDialFromContext and,
// when set, builds the client from the SA token FILE (transport.Config.TokenFile
// → client-go NewCachedFileTokenSource, self-reloading) instead of a read-once
// static token — the #267 fix. A nil ctx is returned unchanged.
func WithServiceAccountDial(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyServiceAccountDial, true)
}

// ServiceAccountDialFromContext reports whether ctx was marked by
// WithServiceAccountDial — i.e. whether this dispatch dials snowplow's own SA
// endpoint (by provenance) and must therefore present the file-backed SA token.
// Unmarked dispatches (per-user clientconfig via FromSecret, external endpoints)
// keep their static token, byte-identical to before.
func ServiceAccountDialFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyServiceAccountDial).(bool)
	return v
}
