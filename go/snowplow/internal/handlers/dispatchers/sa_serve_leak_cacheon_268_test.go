//go:build falsifier_268_267

// sa_serve_leak_cacheon_268_test.go — the cache-ON #268 branch-E arms (RC1
// fall-through, the branch-C-gate-insufficient worst case, and the #267-ordering
// worst case). Shares the harness in sa_serve_leak_falsifier_268_267_test.go.
package dispatchers

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// ── ARM A (#268 RC1 fall-through — deny under an unpublished RBAC snapshot) ────

// leakCacheOnUnpublished puts the process in the cache-on / snapshot-UNPUBLISHED
// state (cache.Global()==nil) the RC1 guard keys on
// (internalDispatchRBACSnapshotUnpublished). Under it, branch C's #256 re-gate
// DENIES a per-user GET (EvaluateRBAC cannot vouch) and then FALLS THROUGH
// (served=false,err=nil) to the external fetch — the RC1 residual (#268 case 1).
func leakCacheOnUnpublished(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.SetGlobal(nil) // unpublished snapshot: Global()==nil
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})
}

// TestLeak268_ArmA_RC1FallThrough_ServedUnderSA — a normal-GVR GET-by-name that
// branch C's #256 re-gate DENIES under an unpublished snapshot. Pre-fix the RC1
// guard falls through to branch E, which dials the SA endpoint and serves the
// object ungated. DISTINCT carrier from the subresource arm: branch C IS engaged
// here (it dials the object via the SA rc, then falls through), so the wire shows
// TWO SA dials for the same denied object.
//
// RED (a6b9d348): the configmap is served under the SA identity. GREEN (Part 1):
// no attach → branch C Gate 1 fails → no SA endpoint → per-user path.
func TestLeak268_ArmA_RC1FallThrough_ServedUnderSA(t *testing.T) {
	leakCacheOnUnpublished(t)

	const path = "/api/v1/namespaces/ns1/configmaps/cm1"
	saBody := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": "ns1", "name": "cm1"},
		"data":     map[string]any{"secretKey": "SA-ONLY-CONFIGMAP-268"},
	}
	srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: saBody}})

	cr := raWithStage("ns1", "leak-ra-rc1",
		map[string]any{"name": "cm", "path": path, "verb": "GET", "filter": ".cm"},
		".cm")
	installLeakFetch(t, raLeakGVR, cr)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
	newRALeakHandler(srv.URL()).ServeHTTP(rec, req)

	served := responseStatus(t, rec.Body.Bytes())
	servedSHA := leakSHA(t, served)
	saSHA := leakSHA(t, saBody)
	wire := srv.sawBearer("GET", "/configmaps/cm1", saLeakToken)

	t.Logf("ARM-A code=%d served_sha256=%s sa_body_sha256=%s wire_sa_bearer=%v wire:%s", rec.Code, servedSHA, saSHA, wire, srv.wireDump())

	if wire && bodyContains(rec.Body.Bytes(), "SA-ONLY-CONFIGMAP-268") {
		t.Fatalf("LEAK (#268 RC1 fall-through) — RED: a per-user GET denied under an unpublished snapshot was served under the SA identity via branch E.\n"+
			"  served sha256 = %s\n  SA-read sha256 = %s\n  wire:%s", servedSHA, saSHA, srv.wireDump())
	}
	t.Logf("ARM-A GREEN: the RC1-denied read was not served under the SA identity.")
}

// ── W1 (branch-C gate alone is INSUFFICIENT — the "both must ship" proof) ──────

// TestLeak268_W1_BranchCGateAlone_Insufficient — the #256 branch-C re-gate is
// present at a6b9d348, yet the leak persists. Drives the RC1 GET where branch C's
// re-gate ENGAGES (it dials the object via the SA rc and DENIES) but the read
// STILL returns via branch E. The two-dial wire (a branch-C SA dial AND a branch-E
// SA dial for the SAME denied object) is the discriminator: "gating branch C alone
// (=#256) fixes the leak" is FALSE.
func TestLeak268_W1_BranchCGateAlone_Insufficient(t *testing.T) {
	leakCacheOnUnpublished(t)

	const path = "/api/v1/namespaces/ns1/configmaps/cm1"
	saBody := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": "ns1", "name": "cm1"},
		"data":     map[string]any{"secretKey": "SA-ONLY-W1-268"},
	}
	srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: saBody}})

	cr := raWithStage("ns1", "leak-ra-w1",
		map[string]any{"name": "cm", "path": path, "verb": "GET", "filter": ".cm"},
		".cm")
	installLeakFetch(t, raLeakGVR, cr)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
	newRALeakHandler(srv.URL()).ServeHTTP(rec, req)

	srv.mu.Lock()
	saDials := 0
	for _, r := range srv.reqs {
		if strings.HasSuffix(r.path, "/configmaps/cm1") && r.auth == "Bearer "+saLeakToken {
			saDials++
		}
	}
	srv.mu.Unlock()
	served := bodyContains(rec.Body.Bytes(), "SA-ONLY-W1-268")

	t.Logf("W1 code=%d sa_dials_for_object=%d served_cross_user=%v wire:%s", rec.Code, saDials, served, srv.wireDump())

	if served {
		t.Fatalf("W1 — RED: the #256 branch-C re-gate is present (it engaged: %d SA dial(s) for the object), yet the denied object STILL leaked via branch E. "+
			"Gating branch C alone does NOT fix the leak — Part 1 (remove the attach) is required.\n  wire:%s", saDials, srv.wireDump())
	}
	t.Logf("W1 GREEN: the object did not leak (branch-C gate + Part 1).")
}

// ── W2 (fixing #267 ALONE un-masks the leak — the ordering proof) ─────────────

// TestLeak268_W2_FreshTokenUnmasks — the branch-E leak is masked in production
// ONLY by #267's stale SA token (a 401). With a STALE token (the apiserver 401s
// the SA bearer) branch E fails → nothing leaks (the accidental "control"); with a
// FRESH token (accepted) branch E authenticates → the subresource leaks. Fixing
// #267 alone (fresh token) UN-MASKS #268 — Part 1 must land WITH the #267 fix.
func TestLeak268_W2_FreshTokenUnmasks(t *testing.T) {
	const path = "/api/v1/namespaces/ns1/pods/p1/status"
	saBody := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"namespace": "ns1", "name": "p1"},
		"status":   map[string]any{"phase": "Running", "secretField": "SA-ONLY-W2-268"},
	}

	// STALE-token sub-run: the apiserver rejects the SA bearer (401) — the #267
	// production mask. Branch E fails; nothing leaks.
	t.Run("stale_token_masks", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		var mu sync.Mutex
		var dialed bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			dialed = true
			mu.Unlock()
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":401,"reason":"Unauthorized"}`)
		}))
		t.Cleanup(srv.Close)
		cr := raWithStage("ns1", "leak-ra-w2-stale",
			map[string]any{"name": "podstatus", "path": path, "verb": "GET", "filter": ".podstatus"}, ".podstatus")
		installLeakFetch(t, raLeakGVR, cr)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
		newRALeakHandler(srv.URL).ServeHTTP(rec, req)
		mu.Lock()
		d := dialed
		mu.Unlock()
		leakedStale := bodyContains(rec.Body.Bytes(), "SA-ONLY-W2-268")
		t.Logf("W2/stale dialed_sa=%v leaked=%v (a 401 masks the leak — 'an accident, not a control')", d, leakedStale)
		if leakedStale {
			t.Fatalf("W2/stale: a 401'd SA token must NOT leak (secret_marker_in_body=true).")
		}
	})

	// FRESH-token sub-run: the token is accepted (what #267's fix produces). The
	// subresource now leaks — proving #267-alone un-masks #268.
	t.Run("fresh_token_unmasks", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		srv := newLeakServer(t, map[string]servedObj{path: {json: true, value: saBody}})
		cr := raWithStage("ns1", "leak-ra-w2-fresh",
			map[string]any{"name": "podstatus", "path": path, "verb": "GET", "filter": ".podstatus"}, ".podstatus")
		installLeakFetch(t, raLeakGVR, cr)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", false))
		newRALeakHandler(srv.URL()).ServeHTTP(rec, req)
		wire := srv.sawBearer("GET", "/pods/p1/status", saLeakToken)
		leakedFresh := bodyContains(rec.Body.Bytes(), "SA-ONLY-W2-268")
		t.Logf("W2/fresh dialed_sa=%v leaked=%v", wire, leakedFresh)
		if wire && leakedFresh {
			t.Fatalf("W2 — RED: a FRESH SA token un-masks the branch-E leak (the denied subresource is served; secret_marker_in_body=true, dialed under the SA bearer). "+
				"Fixing #267 alone (fresh token) opens #268 — Part 1 MUST land with the #267 fix.")
		}
		t.Logf("W2/fresh GREEN: a fresh token is safe — the SA endpoint is not on the live path.")
	})
}
