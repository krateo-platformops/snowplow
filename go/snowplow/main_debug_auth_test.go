// main_debug_auth_test.go — O-D3 [SEC] (1.12.3): the /debug/* surface is not
// world-readable on the chart's LoadBalancer.
//
// THE DEFECT THIS PINS. Before 1.12.3, main.go mounted `/debug/pprof/{,cmdline,
// profile,symbol,trace}`, `/debug/vars`, `/debug/servable` and `/debug/apistage`
// with NO middleware on the SAME mux and port the chart publishes through a
// `service.type: LoadBalancer`. Only `/debug/refreshes` was gated (#69). An
// anonymous caller could pull heap/goroutine profiles, the process argv, the
// whole expvar registry (which carries per-cell maps keyed by `path|gvr|reason`)
// and the per-GVR / per-entry cache metadata — and could pin a CPU for the
// duration of `/debug/pprof/profile`.
//
// THE ARMS (all hermetic — httptest + a test RS256 keypair, no apiserver):
//
//	A1  every pattern registerDebugRoutes mounts 401s WITHOUT credentials.
//	    This is the arm that is RED on origin/main (200 today).
//	A2  /health and /readyz answer 200 WITHOUT credentials — the kubelet probe
//	    contract (howto/operating.md) must not regress behind the gate.
//	A3  the SAME debug paths answer 200 WITH a valid Bearer JWT — the gate does
//	    not brick the diagnostic surface.
//	A4  an EXPIRED JWT 401s, and a valid JWT presented ONLY in the query string
//	    401s (RefreshAuth's no-token-in-URL contract, inherited).
//	A5  the set of patterns registerDebugRoutes registers is EXACTLY
//	    debugRoutePatterns — so a future debug route added to the registrar
//	    without being added to the enumerated (and therefore A1-driven) list
//	    fails here rather than shipping ungated.
//
// RED arm (TestDebugSurface_RED_UngatedRegistrationIsCaught): the PRE-1.12.3
// registration — the verbatim ungated `mux.HandleFunc(...)` / `mux.Handle(...)`
// block from main.go before this fix — is driven through the IDENTICAL A1
// assertion helper, and every path comes back 200. That proves A1 discriminates
// between the gated and the ungated wiring rather than passing vacuously.

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"expvar"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

const debugAuthTestKeyID = "test-kid-for-debug-auth-od3-1.12.3"

var (
	debugAuthTestPrivateKey *rsa.PrivateKey
	debugAuthTestKeys       jwtutil.KeySource
	// debugAuthWrongPrivateKey signs the C1 "well-formed but wrong-key" token:
	// the mux verifies against debugAuthTestKeys (the PUBLIC half of
	// debugAuthTestPrivateKey), so a token signed with THIS key is well-formed,
	// reaches the KeySource, and fails signature verification → 401. It is NOT
	// malformed garbage, which would 401 at the parse stage before the
	// KeySource is ever consulted and so could not distinguish a real
	// signature-rejection (401) from an unavailable-JWKS (503).
	debugAuthWrongPrivateKey *rsa.PrivateKey
)

func init() {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	debugAuthTestPrivateKey = key
	// In production the source is a JWKS fetcher against authn
	// (jwtutil.NewJWKSKeySource, main.go); a static source is the same
	// jwtutil.KeySource contract without the network.
	debugAuthTestKeys = jwtutil.NewStaticKeySource(&key.PublicKey)

	wrong, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	debugAuthWrongPrivateKey = wrong
}

// mintWrongKeyDebugToken issues a WELL-FORMED, unexpired JWT signed with a key
// the mux does NOT trust (C1). It reuses the trusted KeyID so the KeySource is
// consulted for the same kid and the rejection is a signature failure (401),
// not a missing-key 503 — exercising real signature/key verification rather
// than the parser's malformed-token path.
func mintWrongKeyDebugToken(t *testing.T) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   "od3-operator",
		Groups:     []string{"devs"},
		Duration:   time.Hour,
		KeyID:      debugAuthTestKeyID,
		PrivateKey: debugAuthWrongPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken (wrong key): %v", err)
	}
	return tok
}

// mintDebugToken issues a JWT signed with the test private key. A negative
// duration yields an already-expired token.
func mintDebugToken(t *testing.T, dur time.Duration) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   "od3-operator",
		Groups:     []string{"devs"},
		Duration:   dur,
		KeyID:      debugAuthTestKeyID,
		PrivateKey: debugAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

// debugProbePaths maps each registered pattern to a concrete request path that
// routes to it. `GET /debug/pprof/` is a prefix pattern, so it is probed with a
// real named profile (`heap`) — the exact shape an operator or `go tool pprof`
// uses.
var debugProbePaths = map[string]string{
	"GET /debug/pprof/":        "/debug/pprof/heap",
	"GET /debug/pprof/cmdline": "/debug/pprof/cmdline",
	"GET /debug/pprof/profile": "/debug/pprof/profile",
	"GET /debug/pprof/symbol":  "/debug/pprof/symbol",
	"GET /debug/pprof/trace":   "/debug/pprof/trace",
	"GET /debug/vars":          "/debug/vars",
	"GET /debug/servable":      "/debug/servable",
	"GET /debug/apistage":      "/debug/apistage",
	"GET /debug/refreshes":     "/debug/refreshes",
	"GET /debug/reconcile":     "/debug/reconcile",
	"GET /debug/harvest":       "/debug/harvest",
	// #237 — /debug/store takes required query parameters. The probe carries
	// valid ones so the authenticated arm below sees the route's real 200 and
	// not its 400: a gating arm that only ever drove a rejected request would
	// pass whether or not the handler works past the gate.
	"GET /debug/store": "/debug/store?gvr=v1/configmaps&namespace=krateo-system&name=probe",
	// #272 — the shadow-parity toggle. Two patterns on ONE path, split by
	// method. The GET probe reads state; the POST probe carries a valid
	// `enabled` so the authenticated arm below sees the route's real 200
	// (and mutates the process-local toggle — the method-aware test that
	// drives this resets it in cleanup).
	"GET /debug/shadow-parity":  "/debug/shadow-parity",
	"POST /debug/shadow-parity": "/debug/shadow-parity?enabled=true",
	// #277 — /debug/deps. A bare GET is a valid 200 (usage summary); the
	// authenticated arm drives it to prove the route works past the gate.
	"GET /debug/deps": "/debug/deps",
}

// debugPathsSafeToDriveAuthenticated is debugProbePaths minus the two
// long-running collectors: `/debug/pprof/profile` (30s CPU profile by default)
// and `/debug/pprof/trace` (1s execution trace). Their GATING is covered by A1
// like every other path; only the authenticated 200 arm skips them, because
// past the gate they intentionally block for their sample window.
var debugPathsSafeToDriveAuthenticated = []string{
	"/debug/pprof/heap",
	"/debug/pprof/cmdline",
	"/debug/pprof/symbol",
	"/debug/vars",
	"/debug/servable",
	"/debug/apistage",
	"/debug/refreshes",
	"/debug/reconcile",
	"/debug/store?gvr=v1/configmaps&namespace=krateo-system&name=probe",
	// #272 — GET /debug/shadow-parity is a pure read (no mutation), safe to
	// drive authenticated. The POST route's authenticated 200 is covered by
	// the method-aware arm below, not here (A3 drives GET only).
	"/debug/shadow-parity",
	// #277 — /debug/deps bare GET is a read-only usage summary.
	"/debug/deps",
}

// recordingMux records the patterns registered on it and delegates to a real
// http.ServeMux, so a test can assert BOTH the pattern set and the served
// behaviour off the SAME registration call.
type recordingMux struct {
	inner    *http.ServeMux
	patterns []string
}

func newRecordingMux() *recordingMux {
	return &recordingMux{inner: http.NewServeMux()}
}

func (m *recordingMux) Handle(pattern string, handler http.Handler) {
	m.patterns = append(m.patterns, pattern)
	m.inner.Handle(pattern, handler)
}

// buildProdDebugMux builds the mux exactly the way main() does for the routes
// under test: the anonymous probe endpoints, then the production
// registerDebugRoutes call with the production chain shape.
func buildProdDebugMux(t *testing.T) (*recordingMux, http.Handler) {
	t.Helper()
	m := newRecordingMux()
	// Anonymous, exactly as main.go mounts them (kubelet probe contract).
	m.inner.Handle("GET /health", handlers.HealthCheck(serviceName, build, func() (string, error) {
		return "krateo-system", nil
	}))
	m.inner.Handle("GET /readyz", handlers.ReadyCheck())
	registerDebugRoutes(m, use.NewChain(), debugAuthTestKeys)
	return m, m.inner
}

// getStatus drives one GET through h and returns the status code. `bearer`, when
// non-empty, is sent as an `Authorization: Bearer` header.
func getStatus(t *testing.T, h http.Handler, path, bearer string) int {
	t.Helper()
	return statusForMethod(t, h, http.MethodGet, path, bearer)
}

// statusForMethod drives one request through h with the given HTTP method and
// returns the status code. It is what lets the drift-guard probe each route
// with its DECLARED verb: a GET probe against a POST-only pattern would hit a
// coincidentally-registered sibling (or 405) rather than the POST handler, an
// arm that cannot fail. `bearer`, when non-empty, is sent as `Authorization:
// Bearer`.
func statusForMethod(t *testing.T, h http.Handler, method, path, bearer string) int {
	t.Helper()
	req := httptest.NewRequest(method, "http://snowplow.example"+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result().StatusCode
}

// methodOf returns the HTTP verb declared at the head of a "VERB /path" mux
// pattern (Go 1.22 method-aware patterns). Every debugRoutePatterns entry
// carries one.
func methodOf(t *testing.T, pattern string) string {
	t.Helper()
	method, _, ok := strings.Cut(pattern, " ")
	if !ok || method == "" {
		t.Fatalf("pattern %q has no leading HTTP method", pattern)
	}
	return method
}

// assertAllDebugPathsUnauthorized is THE A1 assertion, factored out so the RED
// arm can drive the pre-fix wiring through the identical check. It returns the
// paths that did NOT 401 (empty = the surface is gated).
func assertAllDebugPathsUnauthorized(t *testing.T, h http.Handler) []string {
	t.Helper()
	var leaked []string
	for _, path := range debugProbePaths {
		if code := getStatus(t, h, path, ""); code != http.StatusUnauthorized {
			leaked = append(leaked, fmt.Sprintf("%s → %d", path, code))
		}
	}
	return leaked
}

// ─── A1 — the O-D3 falsifier ────────────────────────────────────────────────

func TestDebugSurface_UnauthenticatedIs401(t *testing.T) {
	_, h := buildProdDebugMux(t)
	if leaked := assertAllDebugPathsUnauthorized(t, h); len(leaked) > 0 {
		t.Fatalf("O-D3: %d debug path(s) served WITHOUT credentials (want 401 on all): %v",
			len(leaked), leaked)
	}
}

// ─── A2 — the kubelet probe contract must NOT regress ───────────────────────

func TestProbeEndpointsStayAnonymous(t *testing.T) {
	cache.MarkPhase1Done() // /readyz is 503 "warming" until this flips
	defer cache.ResetPhase1DoneForTest()

	_, h := buildProdDebugMux(t)
	for _, path := range []string{"/health", "/readyz"} {
		if code := getStatus(t, h, path, ""); code != http.StatusOK {
			t.Errorf("%s unauthenticated = %d, want 200 (kubelet presents no JWT)", path, code)
		}
	}
}

// ─── A3 — a valid JWT still gets the diagnostic ─────────────────────────────

func TestDebugSurface_ValidJWTIs200(t *testing.T) {
	_, h := buildProdDebugMux(t)
	tok := mintDebugToken(t, time.Hour)
	for _, path := range debugPathsSafeToDriveAuthenticated {
		if code := getStatus(t, h, path, tok); code != http.StatusOK {
			t.Errorf("%s with a valid JWT = %d, want 200 (the gate must not brick the surface)", path, code)
		}
	}
}

// ─── A4 — expired token, and no-token-in-URL ────────────────────────────────

func TestDebugSurface_ExpiredAndQueryStringTokenAre401(t *testing.T) {
	_, h := buildProdDebugMux(t)

	if code := getStatus(t, h, "/debug/vars", mintDebugToken(t, -time.Hour)); code != http.StatusUnauthorized {
		t.Errorf("/debug/vars with an EXPIRED JWT = %d, want 401", code)
	}

	// A valid token in the query string only must NOT authenticate (it would
	// leak in logs/referrer) — RefreshAuth's contract, inherited by every
	// debug route now that they share the gate.
	tok := mintDebugToken(t, time.Hour)
	path := "/debug/vars?token=" + url.QueryEscape(tok)
	if code := getStatus(t, h, path, ""); code != http.StatusUnauthorized {
		t.Errorf("/debug/vars with the JWT ONLY in the query string = %d, want 401", code)
	}
}

// ─── A5 — the enumerated pattern set is the registered pattern set ──────────

func TestDebugRoutePatternsMatchRegistration(t *testing.T) {
	m, _ := buildProdDebugMux(t)

	got := map[string]bool{}
	for _, p := range m.patterns {
		got[p] = true
	}
	want := map[string]bool{}
	for _, p := range debugRoutePatterns {
		want[p] = true
	}
	for p := range want {
		if !got[p] {
			t.Errorf("debugRoutePatterns lists %q but registerDebugRoutes does not register it", p)
		}
		if _, ok := debugProbePaths[p]; !ok {
			t.Errorf("pattern %q has no entry in debugProbePaths — A1 would not drive it", p)
		}
	}
	for p := range got {
		if !want[p] {
			t.Errorf("registerDebugRoutes registers %q but debugRoutePatterns does not list it "+
				"— add it there (and to debugProbePaths) so the 401 arm drives it", p)
		}
	}
}

// ─── A6 — every pattern is gated UNDER ITS DECLARED METHOD (#272 C2) ────────
//
// A1 drives every probe with GET. For a GET-only route that is the route's own
// verb, but for a POST route a GET probe lands on the coincidentally-registered
// GET sibling (or a 405) and 401s for the wrong reason — an arm that cannot
// fail. This arm drives each debugRoutePatterns entry with the verb the pattern
// actually declares, so the POST route is exercised as a POST. Two rejections
// per route: no credential, and a WELL-FORMED wrong-key token (C1) that forces
// real signature verification (401), never the parser's malformed-garbage path.
func TestDebugSurface_EachPatternGatedUnderDeclaredMethod(t *testing.T) {
	// Defensive: the POST probes here never reach the handler (the gate 401s
	// first), so this arm does not mutate the toggle — but reset in cleanup
	// anyway so the arm is order-independent regardless of the build it runs in
	// (architect Nit 1).
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	_, h := buildProdDebugMux(t)
	wrong := mintWrongKeyDebugToken(t)

	for _, pattern := range debugRoutePatterns {
		method := methodOf(t, pattern)
		path, ok := debugProbePaths[pattern]
		if !ok {
			t.Fatalf("pattern %q has no debugProbePaths entry — cannot drive it", pattern)
		}
		// No credential → 401 at the gate (the handler never runs, so a POST
		// probe here does not mutate the toggle).
		if code := statusForMethod(t, h, method, path, ""); code != http.StatusUnauthorized {
			t.Errorf("%s (no credential) = %d, want 401", pattern, code)
		}
		// Well-formed wrong-key token → 401 (signature failure, KeySource
		// consulted), NOT 503 (which is what an unavailable JWKS returns).
		if code := statusForMethod(t, h, method, path, wrong); code != http.StatusUnauthorized {
			t.Errorf("%s (well-formed wrong-key token) = %d, want 401 (real signature verification)", pattern, code)
		}
	}
}

// ─── A7 — the #272 POST route: authenticated 200, and wrong-method 405 ──────
//
// The POST route's gating is covered by A6; this pins its POST-specific
// behaviour through the real gated mux: a valid JWT POST flips and reports the
// toggle (200), and a verb that is neither GET nor POST on the same path is a
// mux-level 405 (Method Not Allowed) before any handler or gate runs. The
// toggle is process-local; reset it in cleanup so no state leaks.
func TestDebugShadowParityRoute_MethodGatingAndAuth(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	_, h := buildProdDebugMux(t)
	tok := mintDebugToken(t, time.Hour)

	// Valid JWT POST ?enabled=true → 200 (flips the toggle past the gate).
	if code := statusForMethod(t, h, http.MethodPost, "/debug/shadow-parity?enabled=true", tok); code != http.StatusOK {
		t.Errorf("POST /debug/shadow-parity?enabled=true with a valid JWT = %d, want 200", code)
	}

	// A verb neither GET nor POST on the same path → 405 from the mux, even
	// authenticated: the path matches registered patterns but the method does
	// not, so the request never reaches a handler or the gate.
	if code := statusForMethod(t, h, http.MethodPut, "/debug/shadow-parity", tok); code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /debug/shadow-parity = %d, want 405 (path registered, method not)", code)
	}
}

// ─── RED arm — the pre-1.12.3 wiring fails the SAME assertion ───────────────

// registerDebugRoutesUngated is the VERBATIM pre-1.12.3 registration block from
// main.go (comments stripped): pprof + expvar + servable + apistage with no
// middleware, and only /debug/refreshes behind RefreshAuth. It exists solely so
// the RED arm can prove A1 discriminates.
func registerDebugRoutesUngated(mux *http.ServeMux, chain use.Chain, jwtKeys jwtutil.KeySource) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.Handle("GET /debug/vars", expvar.Handler())
	mux.HandleFunc("GET /debug/servable", handlers.DebugServable())
	mux.HandleFunc("GET /debug/apistage", handlers.DebugApistage())
	mux.Handle("GET /debug/refreshes", chain.Append(
		middleware.RefreshAuth(jwtKeys)).
		Then(handlers.DebugRefreshes()))
}

func TestDebugSurface_RED_UngatedRegistrationIsCaught(t *testing.T) {
	mux := http.NewServeMux()
	registerDebugRoutesUngated(mux, use.NewChain(), debugAuthTestKeys)

	// The pre-fix mux does not register profile/trace here (they are the
	// long-running collectors; their gating is A1's job on the real
	// registrar). Drive the four that ARE registered ungated.
	var leaked []string
	for _, path := range []string{
		"/debug/pprof/heap", "/debug/pprof/cmdline",
		"/debug/vars", "/debug/servable", "/debug/apistage",
	} {
		if code := getStatus(t, mux, path, ""); code != http.StatusUnauthorized {
			leaked = append(leaked, fmt.Sprintf("%s → %d", path, code))
		}
	}
	if len(leaked) == 0 {
		t.Fatal("RED arm did not fire: the PRE-1.12.3 ungated wiring returned 401 everywhere, " +
			"so the A1 assertion cannot distinguish gated from ungated")
	}
	t.Logf("RED arm fired as designed — pre-1.12.3 wiring served %d path(s) anonymously: %v",
		len(leaked), leaked)

	// And the gate that DID exist pre-fix still holds, so the RED arm is not
	// simply "nothing is ever gated".
	if code := getStatus(t, mux, "/debug/refreshes", ""); code != http.StatusUnauthorized {
		t.Errorf("/debug/refreshes was gated by #69 even pre-1.12.3; got %d, want 401", code)
	}
}
