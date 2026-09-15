// nested_resolve_ingest_falsifier_test.go — R hermetic falsifiers for the
// composition-resources HTTP-loopback guard (design
// docs/composition-resources-loopback-fix-design-2026-07-01.md §6).
//
// Arms:
//
//	C2 (forge-guard, HARD RED): a NON-seed / external client sending
//	   X-Snowplow-Nested-Depth:8 + a spoofed ancestor set is NOT trusted →
//	   isTrustedSelfLoopback=false → reseedNestedGuardsFromHeaders is a no-op →
//	   the HTTP-edge stop never fires → a normal full resolve. Both header types
//	   ignored. This is the cache-poison / work-avoidance / DoS boundary.
//	GREEN (HTTP-edge stop): a TRUSTED seed self-loopback whose own node is in the
//	   inbound ancestor set → the stop returns raw=true (raw CR), the cycle-stop
//	   counter increments (proof the guard fires). RBAC ordering (C3) is pinned by
//	   construction: httpEdgeGuardStop takes NO identity and calls NO RBAC — so it
//	   structurally cannot precede/bypass the handler's checkDispatchRBAC (which
//	   the diff places BEFORE the stop-block).
//	DEPTH (backstop): a trusted loopback at depth==NestedCallMaxDepth trips the
//	   depth-8 backstop (raw=false → bounded error), bumping the depth counter.
//
// The D arms (PARTIAL_RESULT_TTL_SECONDS bounded-partial Put) that used to live
// here were retired with the layer in 1.12.6 C9.
package dispatchers

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// testLogger is a discard slog logger for the guard arms (they read no logs).
func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const testSelfHost = "snowplow.krateo-system.svc.cluster.local:8081"

// buildLoopbackReq builds a self-loopback request with explicit control over
// whether a VALID UserInfo is present (the 1.5.26 trust anchor — set ONLY after
// the auth middleware's jwtutil.Validate passes). username=="" ⇒ NO UserInfo on
// ctx (models an unauthenticated / rejected-JWT caller that reached the ingest
// helper hypothetically — the C4 forge arm). A non-empty username ⇒ a valid
// snowplow-validated identity (any authenticated user, seed included). The
// depth/ancestor headers + Host are set as the emit side would.
func buildLoopbackReq(username, host, depth, ancestors string) *http.Request {
	r := httptest.NewRequest("GET", "http://"+host+"/call?resource=restactions", nil)
	r.Host = host
	if depth != "" {
		r.Header.Set(cache.NestedDepthHeader, depth)
	}
	if ancestors != "" {
		r.Header.Set(cache.ResolveAncestorsHeader, ancestors)
	}
	ctx := r.Context()
	if username != "" {
		// The auth middleware sets UserInfo (and AccessToken) ONLY after a
		// successful jwtutil.Validate — model that with a present UserInfo.
		ctx = xcontext.BuildContext(ctx,
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: username}),
			xcontext.WithAccessToken("VALID-JWT-for-"+username),
		)
	}
	return r.WithContext(ctx)
}

// TestR_C2_ForgeGuard_NonSeedUserFires — 1.5.26 C2 (the arm that would have
// caught 1.5.25): a self-loopback under a VALID NON-SEED user identity + an
// ancestor header containing this request's own node → isTrustedSelfLoopback
// TRUE, reseed installs the ancestors, and the HTTP-edge cycle-stop FIRES. The
// user-path storm is closed. RED arm (documented): reverting the predicate to the
// seed-token compare makes a non-seed identity NOT trusted → guard does NOT fire
// → the exact 1.5.25 bug (the storm) returns. This is the discriminating arm the
// 1.5.25 seed-only test lacked.
func TestR_C2_ForgeGuard_NonSeedUserFires(t *testing.T) {
	SetSelfLoopbackHost("http://" + testSelfHost)
	t.Cleanup(func() { SetSelfLoopbackHost("") })

	const resource, ns, name = "restactions", "demo-system", "fsa-y7-composition-resources"
	selfNode := nestedResolveNodeKey(resource, ns, name)

	// A NON-seed authenticated user (e.g. admin) — NOT the seed token.
	req := buildLoopbackReq("admin", testSelfHost, "1", selfNode)

	if !isTrustedSelfLoopback(req) {
		t.Fatal("C2: a VALID non-seed user self-loopback MUST be trusted (the 1.5.25 user-path gap) — " +
			"broadened predicate must fire for any snowplow-validated JWT holder")
	}
	guardCtx := reseedNestedGuardsFromHeaders(req.Context(), req)
	if !cache.NestedResolveAncestorPresent(guardCtx, selfNode) {
		t.Fatal("C2: reseed must install the ancestor header for a trusted user request")
	}
	before := cache.HTTPEdgeCycleStop()
	stop, raw := httpEdgeGuardStop(guardCtx, testLogger(), resource, ns, name)
	if !stop || !raw {
		t.Fatalf("C2: a trusted non-seed self-reentry must cycle-STOP with raw CR; got stop=%v raw=%v", stop, raw)
	}
	if cache.HTTPEdgeCycleStop() != before+1 {
		t.Fatalf("C2: cycle-stop counter must increment (guard fired for the USER path); before=%d after=%d",
			before, cache.HTTPEdgeCycleStop())
	}
}

// TestR_C4_ForgeGuard_NoUserInfoIgnored — 1.5.26 C4 (forge arm preserved): an
// UNAUTHENTICATED caller (NO UserInfo — an invalid/missing JWT would be 401'd
// upstream, never reaching the handler with UserInfo set) presents a forged
// depth:8 + a spoofed ancestor set INCLUDING this request's own node (the worst
// case). It must NOT be trusted → reseed a no-op → the HTTP-edge stop does NOT
// fire (normal full resolve). Asserts the DOWNSTREAM no-op, not just the boolean.
func TestR_C4_ForgeGuard_NoUserInfoIgnored(t *testing.T) {
	SetSelfLoopbackHost("http://" + testSelfHost)
	t.Cleanup(func() { SetSelfLoopbackHost("") })

	const resource, ns, name = "restactions", "demo-system", "fsa-y7-composition-resources"
	selfNode := nestedResolveNodeKey(resource, ns, name)

	// username=="" ⇒ NO UserInfo on ctx (unauthenticated). Forged headers.
	req := buildLoopbackReq("", testSelfHost, "8", selfNode+",restactions/other/decoy")

	if isTrustedSelfLoopback(req) {
		t.Fatal("C4: a request with NO valid UserInfo (unauthenticated) must NOT be trusted — forge hole")
	}
	// Reseed must be a no-op: guardCtx carries NO depth, NO ancestors.
	guardCtx := reseedNestedGuardsFromHeaders(req.Context(), req)
	if cache.NestedCallDepthFromContext(guardCtx) != 0 {
		t.Fatalf("C4: forged depth header was read into ctx (got %d) — headers not ignored",
			cache.NestedCallDepthFromContext(guardCtx))
	}
	if cache.NestedResolveAncestorPresent(guardCtx, selfNode) {
		t.Fatal("C4: forged ancestor header was read into ctx — headers not ignored")
	}
	beforeCycle := cache.HTTPEdgeCycleStop()
	beforeDepth := cache.HTTPEdgeDepthStop()
	stop, _ := httpEdgeGuardStop(guardCtx, testLogger(), resource, ns, name)
	if stop {
		t.Fatal("C4: an unauthenticated caller's forged headers must NOT stop the resolve")
	}
	if cache.HTTPEdgeCycleStop() != beforeCycle || cache.HTTPEdgeDepthStop() != beforeDepth {
		t.Fatal("C4: forged headers bumped a guard counter — headers were trusted")
	}
}

// TestR_C3_SeedStillTrusted — 1.5.26 C3: the seed's own loopback (a valid
// authn-issued identity, DISTINCT from C2's admin) is STILL trusted under the
// broadened predicate → the guard fires. Proves no seed-path regression from the
// broadening. The seed is modeled as a valid UserInfo holder (its authn JWT is
// validated by the same middleware).
func TestR_C3_SeedStillTrusted(t *testing.T) {
	SetSelfLoopbackHost("http://" + testSelfHost)
	t.Cleanup(func() { SetSelfLoopbackHost("") })

	const resource, ns, name = "restactions", "demo-system", "fsa-y7-composition-resources"
	selfNode := nestedResolveNodeKey(resource, ns, name)

	// The seed identity (distinct from C2's "admin") — a valid snowplow-issued JWT
	// holder. Same trust path as any authenticated user.
	req := buildLoopbackReq("system:serviceaccount:krateo-system:snowplow", testSelfHost, "1", selfNode)

	if !isTrustedSelfLoopback(req) {
		t.Fatal("C3: the seed's own loopback (valid UserInfo) MUST stay trusted — no seed-path regression")
	}
	guardCtx := reseedNestedGuardsFromHeaders(req.Context(), req)
	before := cache.HTTPEdgeCycleStop()
	stop, raw := httpEdgeGuardStop(guardCtx, testLogger(), resource, ns, name)
	if !stop || !raw {
		t.Fatalf("C3: seed self-reentry must cycle-STOP with raw CR; got stop=%v raw=%v", stop, raw)
	}
	if cache.HTTPEdgeCycleStop() != before+1 {
		t.Fatalf("C3: cycle-stop counter must increment for the seed path; before=%d after=%d",
			before, cache.HTTPEdgeCycleStop())
	}
}

// TestR_DEPTH_Backstop_TrustedDepth8 — a trusted loopback that has NOT re-entered
// its own node but has reached depth==NestedCallMaxDepth trips the depth-8
// backstop (raw=false → bounded error), bumping the depth counter.
func TestR_DEPTH_Backstop_TrustedDepth8(t *testing.T) {
	SetSelfLoopbackHost("http://" + testSelfHost)
	t.Cleanup(func() { SetSelfLoopbackHost("") })

	const resource, ns, name = "restactions", "demo-system", "fsa-deep-chain"
	// A valid authenticated user; depth == max, and an ancestor set that does NOT
	// contain THIS node (so the cycle-stop does not fire first — depth backstop).
	req := buildLoopbackReq("admin", testSelfHost,
		strconv.Itoa(cache.NestedCallMaxDepth()), "restactions/other/ancestor-a")

	// Handler wiring: header ancestors only. THIS node is NOT in the header set
	// (a non-cyclic deep chain), so the cycle-stop does NOT fire — the depth
	// backstop is what trips.
	guardCtx := reseedNestedGuardsFromHeaders(req.Context(), req)

	before := cache.HTTPEdgeDepthStop()
	stop, raw := httpEdgeGuardStop(guardCtx, testLogger(), resource, ns, name)
	if !stop || raw {
		t.Fatalf("DEPTH: depth==max must trip the backstop (stop=true, raw=false); got stop=%v raw=%v", stop, raw)
	}
	if cache.HTTPEdgeDepthStop() != before+1 {
		t.Fatalf("DEPTH: depth-stop counter must increment; before=%d after=%d", before, cache.HTTPEdgeDepthStop())
	}
}

// TestR_HostMismatch_NotTrusted — even a valid authenticated user is not trusted
// if the request did not arrive on the self-host (belt-and-suspenders secondary).
func TestR_HostMismatch_NotTrusted(t *testing.T) {
	SetSelfLoopbackHost("http://" + testSelfHost)
	t.Cleanup(func() { SetSelfLoopbackHost("") })
	req := buildLoopbackReq("admin", "evil.example.com:8081", "1", "restactions/ns/x")
	if isTrustedSelfLoopback(req) {
		t.Fatal("host-mismatch: a valid user on a NON-self host must not be trusted (belt-and-suspenders)")
	}
}
