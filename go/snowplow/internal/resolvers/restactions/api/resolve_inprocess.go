// resolve_inprocess.go — the direct-apiserver-path + `resolve: true`
// in-process resolve-and-substitute step (internal proposal 2026-06-22,
// Diego-ratified).
//
// THE MECHANISM. When an api-step's `path` points DIRECTLY at the apiserver
// path of a snowplow RESTAction or Widget CR (e.g.
// /apis/templates.krateo.io/v1/namespaces/{ns}/restactions/{name}), the
// step is internal + cacheable: it takes the informer pivot / internal-rest-
// config branch and yields the RAW CR envelope bytes. With `resolve: true`
// (the default) snowplow then runs that fetched CR through the resolver
// IN-PROCESS — restactions.Resolve / widgets.Resolve via the shared
// ResolveNestedCall seam — and substitutes the resolved envelope for the
// stage output, "as if /call'd", with NO outbound /call HTTP round-trip.
// `resolve: false` returns the raw CR unchanged (pre-proposal behaviour).
//
// WHY THE SEAM (import-cycle constraint, unchanged from the loopback era):
// the api package cannot import restactions/widgets/dispatchers (cycle), so
// the actual resolve lives behind the api.nestedCallResolver seam
// (nested_call_seam.go), filled at startup by
// api.RegisterNestedCallResolver(dispatchers.ResolveNestedCall). The seam
// branches on the fetched GVR (RESTAction / widget / raw no-op) and runs the
// checkDispatchRBAC gate + depth cap. This file is the resolver-side TRIGGER:
// it decides whether to call the seam for a given fetched apiserver-path
// stage, then feeds the substituted bytes.
//
// DEP PROPAGATION (proposal §5, the WIN): the seam runs under gctx, which
// carries the OUTER WithL1KeyContext (context.WithValue preserves parent
// values through WithNestedCallDepth). So the nested RA/widget's OWN inner
// apiserver-call dep edges land on the OUTER L1 key — transitive data
// invalidation works. The referenced-CR dep edge itself is recorded FOR FREE
// by the normal apiserver-path dep-record site (resolve.go, parseOK=true) —
// no extra dep code here.

package api

import (
	"context"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// inProcessResolveRestActionsResource / inProcessResolveWidgetsResource are
// the CRD plural resource names that select the in-process resolve. Matched
// on the GVR.Resource parsed from the stage's apiserver path — data-driven
// off the CRD plural, NOT a path/name special-case (feedback_no_special_cases).
// Kept in sync with the dispatchers seam's nestedResolve*Resource constants
// (the seam re-checks the fetched GVR; this is the resolver-side fast gate
// that avoids a wasteful re-fetch+resolve for non-RA/widget paths).
const (
	inProcessResolveRestActionsResource = "restactions"
	inProcessResolveWidgetsResource     = "widgets"
)

// maybeResolveInProcess decides whether the just-fetched apiserver-path stage
// result (`raw`) should be REPLACED by the in-process resolve of the
// referenced RESTAction/Widget, and returns the bytes to feed downstream.
//
// Returns (substituted, true, nil) when it ran the in-process resolve and
// `substituted` is the resolved envelope to feed instead of raw.
// Returns (nil, false, nil) when no substitution applies — the caller must
// feed the ORIGINAL `raw` unchanged (byte-identical to pre-proposal):
//   - resolve:false on this stage;
//   - the path is not a single-CR apiserver GET of a RESTAction/Widget
//     (a LIST, an external path, a non-RA/widget kind, or a templated path);
//   - the seam is not wired (nil resolver — structural fallback).
//
// NOTE — cache-off is NOT a no-op (Diego Option (ii)): resolve:true
// substitutes IN-PROCESS under cache-off too (the seam fetches over the user's
// own token), so the resolved data is identical cache-on/off — a transparent
// fallback. See the Gate-1 comment below.
// Returns (nil, false, err) on a hard resolve error (RBAC-deny, depth-cap,
// inner resolve failure) — the caller routes it through recordItemError
// exactly as an HTTP dispatch error, so a denied resolve surfaces a
// 403-class error, NOT empty content.
//
// The decision is uniform: resolve:true (default) + cache on + a single-CR
// apiserver GET whose GVR.Resource is restactions|widgets. Everything else is
// the no-substitution no-op.
func (r *resolveRun) maybeResolveInProcess(
	gctx context.Context,
	call httpcall.RequestOptions,
	resolve bool,
) (substituted []byte, did bool, err error) {
	// Gate 1 — the step opted out (resolve:false). resolve:false is the
	// explicit raw-CR opt-out.
	//
	// NOTE — cache-OFF is INTENTIONALLY NOT a no-op here (Diego ruling
	// 2026-06-22, Option (ii) / project_cache_off_is_transparent_fallback):
	// resolve:true MUST return the SAME resolved data cache-on and cache-off,
	// else cache-off would degrade resolve:true to a raw CR — a behaviour
	// divergence, not a transparent fallback. Under cache-off the seam's
	// objects.Get falls to getFromAPIServer (the USER's own token →
	// authoritative apiserver RBAC), and ResolveNestedCall's in-process
	// checkDispatchRBAC is itself gated on !cache.Disabled() so it is SKIPPED
	// under cache-off — the user-token apiserver GET IS the RBAC gate. So the
	// in-process resolve honours the cache-off (user-token) RBAC model, not
	// the cache-on SA + in-process-evaluator model. Same RBAC, different
	// mechanism — exactly the transparent-fallback contract.
	if !resolve {
		return nil, false, nil
	}

	// Gate 2 — the seam must be wired (structural fallback, mirrors the
	// loopback-era nil check). Production wires it once at startup; a test
	// that does not wire it gets the raw-CR no-op.
	if nestedCallResolver == nil {
		return nil, false, nil
	}

	// Gate 3 — only a GET can be resolve-substituted (a write verb is not a
	// cacheable single-CR fetch).
	if !isPlainGet(call.Verb) { // #1011: the one read-optimisation verb predicate
		return nil, false, nil
	}

	// Gate 4 — the path must be a SINGLE-CR apiserver GET (name != "") of a
	// RESTAction or Widget. ParseAPIServerPathToDep rejects templated /
	// external / malformed paths (parseOK=false) and yields name=="" for a
	// LIST (nothing single-object to resolve). The GVR.Resource check is the
	// resolver-side fast gate; the seam re-confirms via objects.Get's GVR.
	gvr, ns, name, parseOK := cache.ParseAPIServerPathToDep(call.Path)
	if !parseOK || name == "" {
		return nil, false, nil
	}
	if gvr.Resource != inProcessResolveRestActionsResource &&
		gvr.Resource != inProcessResolveWidgetsResource {
		return nil, false, nil
	}

	// Build the ObjectReference the seam's objects.Get consumes. APIVersion
	// is "group/version" (or bare "version" for the core group). The seam
	// fetches under gctx's identity, runs checkDispatchRBAC, resolves the
	// fetched object in-process under the OUTER L1 key (dep propagation), and
	// returns the FULL resolved envelope — byte-identical to the HTTP /call
	// body.
	apiVersion := gvr.Version
	if gvr.Group != "" {
		apiVersion = gvr.Group + "/" + gvr.Version
	}
	ref := templates.ObjectReference{
		Reference: templates.Reference{
			Name:      name,
			Namespace: ns,
		},
		Resource:   gvr.Resource,
		APIVersion: apiVersion,
	}

	// (c) cold-fan-out AGGREGATE-footprint bound — applied at the OUTERMOST
	// nested resolve ONLY (depth 0). Gates 1-4 above guarantee this is a real
	// cold single-CR RA/widget resolve that WILL recursively fan out
	// (resolve:true); a warm apistage/cache hit never reaches here. The
	// outermost entry holds ONE process-wide admission for the whole subtree —
	// inner recursive resolves (depth>0) inherit it and do NOT re-enter, so
	// there is no per-node explosion and no self-deadlock. Admission is against
	// LIVE headroom (GOMEMLIMIT − live heap), sampled at entry, aggregated
	// across ALL concurrent outermost trees process-wide (the per-tree #78 bound
	// missed the concurrent-tree page fan-out → 8Gi OOM). Transparent when
	// GOMEMLIMIT is unset. See nested_resolve_bound.go.
	//
	// #720 (owner ruling, "No one waits in queue"): only BACKGROUND trees wait
	// on the bound. A customer tree is admitted at once and counts toward the
	// in-flight weight; it never gets berr.
	if isOutermostNestedResolve(gctx) {
		release, berr := enterNestedResolveUnit(gctx)
		if berr != nil {
			// A BACKGROUND tree's ctx ended while it waited on the bound —
			// surface as a resolve error (routed like an HTTP dispatch error by
			// the caller); the background re-resolve fails and Puts nothing.
			return nil, false, berr
		}
		defer release()
	}

	resolved, rerr := nestedCallResolver(gctx, ref, r.opts.PerPage, r.opts.Page, r.opts.Extras)
	if rerr != nil {
		return nil, false, rerr
	}
	return resolved, true, nil
}
