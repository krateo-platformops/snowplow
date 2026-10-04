// nested_resolve_ingest.go — R (composition-resources loopback guard), the
// INGEST half. The emit half lives in the api resolver
// (resolve.go, the self-loopback bearer arm): it attaches
// X-Snowplow-Nested-Depth + X-Snowplow-Resolve-Ancestors to the outbound
// self-host /call. This file re-seeds those guards onto the inbound request ctx
// AT THE HTTP EDGE — but ONLY for a TRUSTED self-loopback — and then applies an
// HTTP-edge twin of the in-process #79 cycle-stop / depth-8 backstop
// (nested_call.go:114,169).
//
// WHY (RCA §0): the depth-8 counter (nested_call_depth.go) and the #79
// ancestor-set (nested_resolve_ancestor.go) are context.Value seams consumed
// only INSIDE the in-process ResolveNestedCall. They do NOT cross an HTTP hop.
// A composition-resources RA whose allCompositionResources managed set includes
// ITSELF dispatches a `/call?resource=restactions&name=<self>` loopback to
// snowplow's own self-host; that re-enters restActionHandler.ServeHTTP as a
// FRESH request carrying no depth / no ancestor set, so neither guard fires →
// unbounded HTTP self-recursion (the 3121-hop storm → context-canceled →
// decline-to-cache → never warms). R propagates the guards across the boundary.
//
// SECURITY (C1, the load-bearing line): the two headers are ADVISORY and
// SELF-TRUSTED ONLY. isTrustedSelfLoopback gates whether they are read into ctx
// at all. The trust check requires a VALID snowplow-issued identity —
// xcontext.UserInfo(ctx) present + non-error — which the auth middleware sets
// ONLY after jwtutil.Validate passes (an external / no-JWT / spoofed-JWT caller
// is rejected 401 upstream, never reaching the handler with UserInfo set). So
// `UserInfo present` ⟺ a snowplow-VALIDATED JWT holder. A req.Host==self check
// is belt-and-suspenders; the validated-identity check is the primary gate. Any
// request failing it has BOTH headers IGNORED (never read into ctx) → it gets a
// NORMAL full resolve (C2 — a forged X-Snowplow-Nested-Depth:8 + spoofed ancestor
// set from an UNAUTHENTICATED caller cannot force a premature raw-CR stop or
// suppress a legitimate resolve).
//
// 1.5.26 broadening (docs/user-path-loopback-storm-trace-fix-2026-07-02.md): the
// predicate was seed-token-only in 1.5.25, which closed the SEED loopback but not
// the USER loopback (a real user's JWT ≠ the seed token → guard never fired →
// user-path storm). The security property was never "only the seed" but "only a
// snowplow-VALIDATED JWT holder" — which holds for ANY authenticated user, seed
// included. See isTrustedSelfLoopback for the full trust anchor.
//
// BLAST-RADIUS BOUND: even a theoretical forge by an AUTHENTICATED user could
// only make snowplow return THIS ONE call's raw CR (resolve:false) or a bounded
// depth-stop — it cannot read cross-user data (the resolve still runs
// checkDispatchRBAC under the CALLER's identity, restactions.go:96, UNCHANGED by
// R) and cannot poison another user's L1 entry (per-binding key). So the forge
// blast radius is self-DoS of the forger's own call.
package dispatchers

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// selfLoopbackHost is the URL_SELF host (Hostname, no port/scheme) parsed once at
// startup by SetSelfLoopbackHost (main.go). Empty when URL_SELF is unset — then
// the belt-and-suspenders host check is skipped and the token match alone gates
// (the token match is the primary and sufficient guard; the host check only ever
// tightens, never loosens). atomic so the startup Store and per-request Load are
// race-free without a mutex, mirroring api.selfHost.
var selfLoopbackHost atomic.Pointer[string]

// SetSelfLoopbackHost parses rawURL (URL_SELF) and installs its host for the
// ingest belt-and-suspenders check. An empty/unparseable rawURL clears it (host
// check disabled). Wired once at startup from main.go alongside
// api.SetSelfHost(URL_SELF), so both the emit-side and ingest-side self-host
// derive from the SAME URL_SELF value.
func SetSelfLoopbackHost(rawURL string) {
	if rawURL == "" {
		selfLoopbackHost.Store(nil)
		return
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		selfLoopbackHost.Store(nil)
		return
	}
	h := u.Hostname()
	selfLoopbackHost.Store(&h)
}

// requestArrivedOnSelfHost reports whether req arrived on the configured
// self-host. Belt-and-suspenders ONLY (C1: the token match is primary). Returns
// true when no self-host is configured — it must never TIGHTEN beyond the token
// gate when unconfigured (the token match still stands as the sole gate). req.Host
// may carry a port; we compare the hostname component only.
func requestArrivedOnSelfHost(req *http.Request) bool {
	sh := selfLoopbackHost.Load()
	if sh == nil {
		return true // unconfigured — defer entirely to the token match
	}
	host := req.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h // req.Host carried a port — compare the hostname component
	}
	return host == *sh
}

// isTrustedSelfLoopback is the C1 forge-guard. TRUE iff the request carries a
// VALID snowplow-issued identity (xcontext.UserInfo present + non-error) AND
// (belt-and-suspenders) it arrived on the self-host. For ANY request that fails
// EITHER condition the depth/ancestor headers are ignored (the caller re-seeds
// NOTHING → a normal full resolve).
//
// 1.5.26 BROADENING (was seed-token-only, docs/user-path-loopback-storm-trace-fix-
// 2026-07-02.md): 1.5.25 trusted a self-loopback ONLY when the inbound bearer
// EXACTLY equalled snowplow's OWN seed JWT (token-provenance). That closed the
// SEED loopback but NOT the USER loopback: a real user's composition-resources
// /call runs under THEIR JWT ≠ the seed token → the guard returned false → the
// depth/ancestor headers were ignored → the HTTP-edge cycle-stop never fired →
// the user-path self-loopback recursed unbounded (the 1.5.24 storm, now
// user-driven; concause: the per-BINDING L1 entry means the seed's warm entries
// miss for a user → cold re-resolve → each self-loops unguarded). The security
// property was never "only the seed" — it was "only a snowplow-VALIDATED JWT
// holder", which holds for ANY authenticated user.
//
// TRUST ANCHOR (TRACED): the auth middleware runs jwtutil.Validate(signingKey,
// token) at userconfig.go:151 (mirrored in refreshauth.go) and, ONLY on success,
// sets xcontext.WithUserInfo(userInfo) at userconfig.go:231; an invalid /
// missing / spoofed JWT returns Unauthorized at userconfig.go:151 BEFORE the
// handler runs. So `UserInfo present` ⟺ the caller presented a JWT signed by
// snowplow's OWN signing key (validated at :151) — an external / no-JWT /
// spoofed-JWT caller can NEVER reach the handler with UserInfo set. The C2 forge
// hole stays closed by the middleware, not by a token compare here.
//
// FORGE-SAFE FOR ANY AUTHENTICATED USER (blast-radius, unchanged from R): the
// WORST a malicious AUTHENTICATED user can do by forging depth/ancestor headers
// on their OWN self-loopback is SELF-DoS of their own call — a raw CR
// (resolve:false) or a bounded 508 for THAT call. They CANNOT read cross-user
// data (the resolve still runs checkDispatchRBAC under THEIR identity,
// restactions.go:96) and CANNOT poison another user's L1 entry (per-binding
// key). No cross-user impact.
//
// SEED STAYS TRUSTED: the seed's loopback is a real HTTP /call carrying the
// authn-issued seed JWT (#57), which the SAME middleware validates → UserInfo is
// set → trusted under this broadened predicate too (no seed-path regression;
// gated by the seed-still-trusted falsifier arm).
func isTrustedSelfLoopback(req *http.Request) bool {
	// Primary gate: a snowplow-VALIDATED JWT holder. UserInfo present + non-error
	// ⟺ the auth middleware ran jwtutil.Validate successfully before the handler
	// (an unauth / invalid / spoofed JWT is rejected 401 upstream, never reaching
	// here with UserInfo set) — the forge hole stays closed.
	if _, err := xcontext.UserInfo(req.Context()); err != nil {
		return false
	}
	// Belt-and-suspenders secondary (tightens only; returns true when URL_SELF
	// is unconfigured). The validated-identity check above is the primary anchor.
	return requestArrivedOnSelfHost(req)
}

// provenanceFor maps "the RESTAction came from the request body" to its
// resolve Provenance (#443 part 2). Used at the single place Provenance is set
// (restactions.go, the restactionsResolveFn call).
func provenanceFor(inline bool) api.Provenance {
	if inline {
		return api.ProvenanceCallerSupplied
	}
	return api.ProvenanceStored
}

// reseedInertFromHeaders is the ingest half of #443 choke point (j): a nested
// HTTP self-loopback /call made by an inert resolve carries
// cache.InertHeader: 1, and the next hop must be inert too. The header is
// honoured ONLY on a trusted self-loopback (isTrustedSelfLoopback: a
// snowplow-validated JWT arriving on the URL_SELF host), the same anchor as
// the depth and ancestor headers. A user forging it affects only their own
// request (it skips L1 and makes no fill). Returns req unchanged otherwise.
// It must run first in ServeHTTP, before observeLiveCaller and before the L1
// key is minted.
func reseedInertFromHeaders(req *http.Request) *http.Request {
	if req == nil || req.Header.Get(cache.InertHeader) != "1" || cache.Inert(req.Context()) {
		return req
	}
	if !isTrustedSelfLoopback(req) {
		return req
	}
	return req.WithContext(cache.WithInert(req.Context()))
}

// reseedNestedGuardsFromHeaders re-installs the depth counter + ancestor set from
// the inbound X-Snowplow-* headers onto ctx — but ONLY when isTrustedSelfLoopback.
// Returns ctx unchanged for an untrusted request (headers ignored). The parse
// uses the SAME cache-package codec the emit site serialized with (anti-drift).
//
// Placement contract: the caller invokes this AFTER xcontext.BuildContext and
// AFTER the #83 Option-A WithNestedResolveAncestor self-seed, so the seeded
// ancestor set is {this node} ∪ {the header's ancestors} — exactly the current
// descent path the emit side serialized ({...ancestors, this-node's-parent}).
func reseedNestedGuardsFromHeaders(ctx context.Context, req *http.Request) context.Context {
	if !isTrustedSelfLoopback(req) {
		return ctx
	}
	if v := req.Header.Get(cache.NestedDepthHeader); v != "" {
		if d, err := strconv.Atoi(v); err == nil && d >= 0 {
			ctx = cache.WithNestedCallDepth(ctx, d)
		}
	}
	for _, node := range cache.ParseAncestorsHeader(req.Header.Get(cache.ResolveAncestorsHeader)) {
		ctx = cache.WithNestedResolveAncestor(ctx, node)
	}
	return ctx
}

// httpEdgeGuardStop is the HTTP-edge twin of the in-process cycle-stop /
// depth-8 backstop (nested_call.go:114,169). It runs AFTER fetchObject +
// checkDispatchRBAC (C3 — the stop is NEVER an RBAC bypass; it fires only on an
// authorized, real CR) and BEFORE the L1 lookup. It reads the ctx guards that
// reseedNestedGuardsFromHeaders installed (so an UNTRUSTED request has no guards
// → this always returns stop=false for it → normal resolve, C2).
//
//   - If node (this CR's resource/namespace/name — the SAME shared derivation
//     nestedResolveNodeKey builds) is already on the current descent path
//     (NestedResolveAncestorPresent, seeded from the header + the #83 self-seed),
//     this is a SELF-REFERENCE over the HTTP boundary: return stop=true, raw=true
//     → the handler serves the RAW CR (resolve:false semantics), NOT a recurse.
//     This is the primary, cost-optimal stop (fires at the first self-reentry).
//   - Else if the inbound depth has reached NestedCallMaxDepth, the depth-8
//     BACKSTOP trips: stop=true, raw=false → the handler returns a bounded
//     508-class error (a non-cyclic pathologically deep chain).
//   - Else stop=false → resolve normally.
func httpEdgeGuardStop(ctx context.Context, log *slog.Logger, resource, namespace, name string) (stop, raw bool) {
	node := nestedResolveNodeKey(resource, namespace, name)
	if cache.NestedResolveAncestorPresent(ctx, node) {
		cache.BumpHTTPEdgeCycleStop()
		log.Debug("http-edge nested resolve cycle-stop: node already an ancestor — returning raw CR",
			slog.String("node", node),
			slog.Int("depth", cache.NestedCallDepthFromContext(ctx)),
		)
		return true, true
	}
	if cache.NestedCallDepthFromContext(ctx) >= cache.NestedCallMaxDepth() {
		cache.BumpHTTPEdgeDepthStop()
		log.Warn("http-edge nested resolve depth-limit backstop: refusing to recurse further",
			slog.String("node", node),
			slog.Int("depth", cache.NestedCallDepthFromContext(ctx)),
			slog.Int("max", cache.NestedCallMaxDepth()),
		)
		return true, false
	}
	return false, false
}
