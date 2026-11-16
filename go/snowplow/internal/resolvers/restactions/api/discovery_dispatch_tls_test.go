// discovery_dispatch_tls_test.go — Fix A1 falsifiers.
//
// Reproduces the bare-group-discovery x509 defect and proves the Fix A1
// discovery dispatch branch. The bug: a composition-resources RA issues
// an api-step GET against a bare group-discovery URL /apis/<g>/<v> (no
// resource segment); that 2-segment path parse-fails every CA-bearing
// dispatch branch and falls through to the external fetch, which builds a
// plumbing client from the per-user TOKEN-auth Endpoint. plumbing's
// tlsConfigFor drops the cluster caData for a token-auth endpoint
// (HasCertAuth()-only CA install) → x509: certificate signed by unknown
// authority. TRACED:
// docs/troubleshoot-discovery-url-apistep-x509-2026-06-23.md.
//
// Fix A1: a dispatch branch keyed on the discovery SHAPE
// (cache.ParseAPIServerDiscoveryPath) ahead of the external fetch serves
// group discovery via client-go's discovery client on a CA-bearing SA
// *rest.Config — client-go's transport installs the cluster CA correctly.
//
// CRITICAL — like internal_dispatch_tls_test.go, these run against an
// httptest TLS server with a synthetic CA; they structurally cannot
// exercise the real cluster CA or a real apiserver discovery handshake.
// Necessary but not sufficient: the on-cluster falsifier is a /call of the
// triggering RA returning 200 with no x509 in pod logs.
//
// Falsifier set (per PM gate):
//   F1 NEGATIVE — bare /apis/<g>/<v> GET via plumbing httpcall.Do over a
//      token-auth endpoint carrying a non-system CA → StatusFailure with
//      x509/certificate (reproduces today's bug, RED before the fix).
//   F2 POSITIVE — same bare path via dispatchViaDiscovery over a CA-bearing
//      SA rc → served=true, body = APIResourceList (kind + non-empty
//      resources), NO x509.
//   F3 behaviour-neutral — a non-apiserver / non-discovery path is NOT
//      served by the discovery branch.
//   F4 LEAK GUARD — a RESOURCE path (/apis/<g>/<v>/<resource> and
//      /apis/<g>/<v>/<resource>/<name>) and /api/v1/<resource> are NEVER
//      classified as discovery / routed to the discovery branch.
//   F5 verb gate — a non-GET (POST) bare-discovery-shaped path is NOT
//      served by the discovery branch.

package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

const fixtureDiscoveryGV = "templates.krateo.io/v1"
const fixtureDiscoveryPath = "/apis/templates.krateo.io/v1"
const fixtureDiscoveryResource1 = "compositiondefinitions"
const fixtureDiscoveryResource2 = "collections"

// newTLSDiscoveryServer starts an httptest TLS server that answers a bare
// group-discovery GET (/apis/<g>/<v>) with a minimal APIResourceList
// envelope carrying two resources. Its auto-generated certificate is
// signed by a CA that is NOT in the system root store — the same trust
// posture as a real cluster's self-signed apiserver CA. Returns the
// server and its CA PEM bytes.
//
// The APIResourceList carries TWO real resources so the fix-path test can
// assert a non-empty `resources` array (AC3 — body = APIResourceList with
// resources), not merely a 200.
func newTLSDiscoveryServer(t *testing.T) (*httptest.Server, []byte) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"`+fixtureDiscoveryGV+`","resources":[`+
			`{"name":"`+fixtureDiscoveryResource1+`","singularName":"compositiondefinition","namespaced":true,"kind":"CompositionDefinition","verbs":["get","list"]},`+
			`{"name":"`+fixtureDiscoveryResource2+`","singularName":"collection","namespaced":true,"kind":"Collection","verbs":["get","list"]}]}`)
	}))
	t.Cleanup(srv.Close)
	caPEM := pemEncodeCert(srv.Certificate())
	return srv, caPEM
}

// withDiscoverySARESTConfig swaps the package-private SA *rest.Config seam
// to point at rc (an httptest TLS server with a synthetic CA), restoring
// it on cleanup. It also resets the discovery-client memo so the test
// builds a fresh client against the swapped rc.
func withDiscoverySARESTConfig(t *testing.T, rc *rest.Config) {
	t.Helper()
	resetDiscoveryClientCacheForTest()
	prev := saRESTConfigForDiscoveryFn
	saRESTConfigForDiscoveryFn = func() (*rest.Config, error) { return rc, nil }
	t.Cleanup(func() {
		saRESTConfigForDiscoveryFn = prev
		resetDiscoveryClientCacheForTest()
	})
}

// TestPlumbingHttpcall_BareDiscoveryPath_DropsCA is F1, the NEGATIVE
// CONTROL. It reproduces today's bug directly: a bare group-discovery GET
// dispatched through plumbing's httpcall.Do over a token-auth Endpoint
// carrying the cluster CA gets an HTTP client that does NOT trust that CA
// — the TLS handshake fails with "certificate signed by unknown
// authority". This is the path the bare-discovery api-step takes TODAY
// (external fall-through). If a future plumbing bump fixes tlsConfigFor to
// honour HasCA() for token-auth endpoints, this test FAILS — flagging that
// the Fix A1 snowplow-side branch can be removed.
func TestPlumbingHttpcall_BareDiscoveryPath_DropsCA(t *testing.T) {
	// The x509 failure asserted here is deterministic; plumbing's
	// RetryClient would still retry it 5 times with jittered exponential
	// backoff (8-16s of sleep per run — the flaky wall-clock cost). The
	// retry budget is read from env per httpcall.Do call, so zeroing it
	// test-scoped keeps the negative control instant and deterministic.
	// See the sibling comment in internal_dispatch_tls_test.go.
	t.Setenv("CLIENT_MAX_RETRIES", "0")

	srv, caPEM := newTLSDiscoveryServer(t)

	// A token-auth endpoint carrying the cluster CA — the exact shape the
	// per-user <user>-clientconfig endpoint produces (raw-PEM CA, bearer
	// token, no client cert).
	ep := &endpoints.Endpoint{
		ServerURL:                srv.URL,
		Token:                    "fake-user-jwt",
		CertificateAuthorityData: string(caPEM),
	}
	if ep.HasCertAuth() {
		t.Fatal("precondition: token-auth endpoint must NOT be cert-auth")
	}
	if !ep.HasCA() {
		t.Fatal("precondition: endpoint must carry a CA")
	}

	res := httpcall.Do(context.Background(), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: fixtureDiscoveryPath},
		Endpoint:    ep,
	})

	if res.Status != response.StatusFailure {
		t.Fatalf("NEGATIVE CONTROL BROKEN: expected plumbing httpcall.Do to fail "+
			"TLS verification for a token-auth endpoint on the bare-discovery "+
			"path (today's bug), got status=%v message=%q. If plumbing fixed "+
			"tlsConfigFor to honour HasCA() for token-auth endpoints, the Fix A1 "+
			"snowplow-side discovery branch can be removed.", res.Status, res.Message)
	}
	if !strings.Contains(res.Message, "x509") &&
		!strings.Contains(res.Message, "certificate") {
		t.Fatalf("NEGATIVE CONTROL: expected a TLS/x509 verification error, got %q", res.Message)
	}
	t.Logf("negative control confirmed — plumbing httpcall.Do drops the CA for a "+
		"token-auth endpoint on the bare-discovery path: %s", res.Message)
}

// TestDiscoveryDispatch_TrustsClusterCA is F2, the POSITIVE proof. The same
// bare group-discovery path dispatched via dispatchViaDiscovery over a
// CA-bearing SA *rest.Config SUCCEEDS — client-go's transport installs the
// CA into RootCAs correctly — and the served body is the APIResourceList
// with a non-empty resources array (AC3), no x509.
func TestDiscoveryDispatch_TrustsClusterCA(t *testing.T) {
	srv, caPEM := newTLSDiscoveryServer(t)
	rc := &rest.Config{
		Host:        srv.URL,
		BearerToken: "fake-sa-jwt",
		TLSClientConfig: rest.TLSClientConfig{
			CAData: caPEM,
		},
	}
	withDiscoverySARESTConfig(t, rc)

	raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: fixtureDiscoveryPath},
	})
	if err != nil {
		t.Fatalf("FIX BROKEN: discovery dispatch via CA-bearing SA *rest.Config "+
			"failed the TLS handshake against a CA it carries verbatim: %v", err)
	}
	if !served {
		t.Fatal("FIX BROKEN: expected the bare group-discovery GET to be served " +
			"via the discovery dispatch branch")
	}

	var envelope struct {
		Kind         string `json:"kind"`
		GroupVersion string `json:"groupVersion"`
		Resources    []struct {
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"resources"`
	}
	if uErr := json.Unmarshal(raw, &envelope); uErr != nil {
		t.Fatalf("FIX BROKEN: served bytes are not valid JSON: %v", uErr)
	}
	if envelope.Kind != "APIResourceList" {
		t.Fatalf("AC3: served body kind must be APIResourceList, got %q. body: %q",
			envelope.Kind, string(raw))
	}
	if len(envelope.Resources) == 0 {
		t.Fatalf("AC3: served APIResourceList must carry a non-empty resources "+
			"array, got 0. body: %q", string(raw))
	}
	got := map[string]bool{}
	for _, r := range envelope.Resources {
		got[r.Name] = true
	}
	if !got[fixtureDiscoveryResource1] || !got[fixtureDiscoveryResource2] {
		t.Fatalf("AC3: served resources missing an entry — want %q + %q, got %v",
			fixtureDiscoveryResource1, fixtureDiscoveryResource2, got)
	}
	t.Logf("fix confirmed — client-go discovery client from rest.Config{CAData} "+
		"trusts the cluster CA AND the served body is an APIResourceList with %d "+
		"resources: %d bytes served", len(envelope.Resources), len(raw))
}

// TestDiscoveryDispatch_CoreGroupShape is the /api/<v> companion to F2 —
// the core-group discovery shape is also served by the branch.
func TestDiscoveryDispatch_CoreGroupShape(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// client-go's discovery client GETs LegacyPrefix("/api")+"/v1" for
		// the core group.
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[`+
			`{"name":"namespaces","singularName":"namespace","namespaced":false,"kind":"Namespace","verbs":["get","list"]}]}`)
	}))
	t.Cleanup(srv.Close)
	caPEM := pemEncodeCert(srv.Certificate())
	rc := &rest.Config{Host: srv.URL, BearerToken: "fake-sa-jwt", TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}}
	withDiscoverySARESTConfig(t, rc)

	raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/api/v1"},
	})
	if err != nil {
		t.Fatalf("core-group discovery dispatch failed: %v", err)
	}
	if !served {
		t.Fatal("expected /api/v1 core-group discovery to be served by the branch")
	}
	if !strings.Contains(string(raw), "APIResourceList") {
		t.Fatalf("served body is not an APIResourceList: %q", string(raw))
	}
}

// TestDiscoveryDispatch_BareRootServed is the #74 Class 1 POSITIVE arm: the
// bare discovery ROOTS /api and /apis are served by the discovery branch over
// the CA-bearing SA transport (via RESTClient AbsPath), returning the
// apiserver's raw root index — NOT falling through to the external plumbing
// branch (whose token-auth TLS drops the cluster CA → x509). This is the fix
// for the seed bare-/api x509 noise. The synthetic TLS server answers any path
// with a discovery-index body; the assertion is served=true + no x509 + the
// body round-trips.
func TestDiscoveryDispatch_BareRootServed(t *testing.T) {
	for _, root := range []string{"/api", "/apis"} {
		t.Run(root, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				// The apiserver root index shape (APIVersions for /api,
				// APIGroupList for /apis). Body content is incidental to the
				// fix — what matters is it's SA-served over the CA-bearing
				// transport, not external-fall-through. Return a minimal valid
				// JSON index.
				_, _ = io.WriteString(w, `{"kind":"APIVersions","versions":["v1"]}`)
			}))
			t.Cleanup(srv.Close)
			caPEM := pemEncodeCert(srv.Certificate())
			rc := &rest.Config{Host: srv.URL, BearerToken: "fake-sa-jwt", TLSClientConfig: rest.TLSClientConfig{CAData: caPEM}}
			withDiscoverySARESTConfig(t, rc)

			raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
				RequestInfo: httpcall.RequestInfo{Path: root},
			})
			if err != nil {
				// A bare-root that fell to the external branch over the
				// system-root-store TLS would x509 here; the fix routes it to
				// the CA-bearing SA transport instead.
				t.Fatalf("Class 1: bare root %q dispatch failed (x509 if it fell external): %v", root, err)
			}
			if !served {
				t.Fatalf("Class 1: bare root %q must be SERVED by the discovery branch (the seed /api x509 fix), "+
					"not fall through to the external token-auth TLS path", root)
			}
			if len(raw) == 0 {
				t.Fatalf("Class 1: served root %q returned empty bytes", root)
			}
		})
	}
}

// TestDiscoveryDispatch_BareRootLeakGuard is the #74 Class 1 RBAC-boundary RED
// arm: widening the dispatch to the bare roots must NOT widen it to any
// resource path. A resource path under /api or /apis is STILL not served (the
// root predicate matches ONLY the two exact roots). Pairs with F4.
func TestDiscoveryDispatch_BareRootLeakGuard(t *testing.T) {
	withDiscoverySARESTConfig(t, &rest.Config{Host: "https://kubernetes.default.svc"})
	for _, p := range []string{
		"/api/v1/namespaces",                // core LIST
		"/api/v1/namespaces/krateo",         // core GET
		"/apis/apps/v1/deployments",         // group LIST
		"/apis/apps/v1/namespaces/x/pods/p", // group namespaced GET
	} {
		raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: p},
		})
		if err != nil {
			t.Fatalf("Class 1 leak-guard: %q errored (should just not-serve): %v", p, err)
		}
		if served {
			t.Fatalf("Class 1 LEAK: resource path %q was SA-served by the discovery branch — the bare-root "+
				"widening must NOT route resource paths (RBAC boundary)", p)
		}
		if raw != nil {
			t.Fatalf("Class 1 leak-guard: %q returned %d bytes, want nil (not served)", p, len(raw))
		}
	}
}

// TestDiscoveryDispatch_NonAPIServerPathFallsThrough is F3, behaviour-
// neutral: a non-discovery (external / non-apiserver) path is NOT served
// by the discovery branch.
func TestDiscoveryDispatch_NonAPIServerPathFallsThrough(t *testing.T) {
	// Seam swap guards against accidental SA-rc use — if the shape gate
	// leaked, the dispatch would try to build a client and we'd notice.
	withDiscoverySARESTConfig(t, &rest.Config{Host: "https://kubernetes.default.svc"})

	raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "https://example.com/external"},
	})
	if err != nil {
		t.Fatalf("expected no error for a non-apiserver path, got %v", err)
	}
	if served {
		t.Fatal("F3: a non-apiserver path must NOT be served by the discovery branch")
	}
	if raw != nil {
		t.Fatalf("expected nil bytes on fall-through, got %d bytes", len(raw))
	}
}

// TestDiscoveryDispatch_ResourcePathNotRouted is F4, the LOAD-BEARING LEAK
// GUARD. A RESOURCE path (≥3 segments after /apis/, or /api/v1/<resource>),
// namespaced or cluster-scoped, GET-by-name or LIST, must NEVER be
// classified as discovery / routed to the SA-serving discovery branch —
// SA-serving a resource path would be a cross-user leak. This is what
// code-enforces the §1 RBAC boundary.
func TestDiscoveryDispatch_ResourcePathNotRouted(t *testing.T) {
	// If the gate leaked, the dispatch would try to build a client against
	// THIS rc; swapping it lets us assert served=false purely on the shape.
	withDiscoverySARESTConfig(t, &rest.Config{Host: "https://kubernetes.default.svc"})

	// NOTE (#74 Class 1): /apis and /api (the bare ROOTS) were here as
	// "not served"; they are now SERVED via the dispatch's bare-root branch
	// (TestDiscoveryDispatch_BareRootServed). They remain NOT-a-single-GV at
	// the ParseAPIServerDiscoveryPath predicate level (still false there —
	// asserted in discovery_path_test.go + TestParseAPIServerDiscoveryRoot), so
	// the GroupVersion predicate's RBAC boundary is unchanged. This list keeps
	// only the RESOURCE paths + the group-version-list, which must STILL never
	// be served by EITHER predicate.
	resourcePaths := []string{
		"/apis/templates.krateo.io/v1/compositiondefinitions",                              // group LIST
		"/apis/templates.krateo.io/v1/compositiondefinitions/foo",                          // group GET-by-name
		"/apis/templates.krateo.io/v1/namespaces/krateo-system/compositiondefinitions",     // namespaced LIST
		"/apis/templates.krateo.io/v1/namespaces/krateo-system/compositiondefinitions/foo", // namespaced GET-by-name
		"/api/v1/namespaces",        // core LIST
		"/api/v1/namespaces/krateo", // core GET-by-name
		"/api/v1/pods",              // core cluster LIST
		"/apis/templates.krateo.io", // group version list (not a single GV, not a bare root)
	}
	for _, p := range resourcePaths {
		// Predicate-level: ParseAPIServerDiscoveryPath must return false.
		if gv, ok := cache.ParseAPIServerDiscoveryPath(p); ok {
			t.Fatalf("F4 LEAK GUARD: ParseAPIServerDiscoveryPath(%q) returned ok=true "+
				"(gv=%q) — a resource/non-single-GV path must NEVER be classified as "+
				"discovery, else the SA-serve branch would leak it cross-user", p, gv)
		}
		// Dispatch-level: the branch must not serve it.
		raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: p},
		})
		if err != nil {
			t.Fatalf("F4: unexpected error for resource path %q: %v", p, err)
		}
		if served {
			t.Fatalf("F4 LEAK GUARD: resource/non-single-GV path %q was SERVED by the "+
				"discovery branch — cross-user leak", p)
		}
		if raw != nil {
			t.Fatalf("F4: expected nil bytes for non-routed path %q, got %d bytes", p, len(raw))
		}
	}

	// And the discovery shapes themselves MUST be classified true (positive
	// side of the boundary), so the guard isn't trivially passing.
	for _, p := range []string{fixtureDiscoveryPath, "/api/v1", "/apis/apps/v1"} {
		if _, ok := cache.ParseAPIServerDiscoveryPath(p); !ok {
			t.Fatalf("F4 boundary: ParseAPIServerDiscoveryPath(%q) must classify the "+
				"bare-discovery shape as ok=true", p)
		}
	}
}

// TestDiscoveryDispatch_NonGETNotServed is F5, the verb gate: a non-GET
// (POST) bare-discovery-shaped path is NOT served by the discovery branch.
func TestDiscoveryDispatch_NonGETNotServed(t *testing.T) {
	withDiscoverySARESTConfig(t, &rest.Config{Host: "https://kubernetes.default.svc"})

	raw, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: fixtureDiscoveryPath,
			Verb: ptr.To(http.MethodPost),
		},
	})
	if err != nil {
		t.Fatalf("expected no error on the non-GET fall-through, got %v", err)
	}
	if served {
		t.Fatal("F5: a non-GET bare-discovery-shaped path must NOT be served by the " +
			"discovery branch")
	}
	if raw != nil {
		t.Fatalf("expected nil bytes on the non-GET fall-through, got %d bytes", len(raw))
	}
}
