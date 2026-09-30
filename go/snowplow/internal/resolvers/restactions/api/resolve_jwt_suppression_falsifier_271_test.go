//go:build unit
// +build unit

// resolve_jwt_suppression_falsifier_271_test.go — #271 RED-first falsifier
// (credential SUPPRESSION + JWT leak on the SA-endpoint dial).
//
// THE BUG (TRACED, project_271_sar401_third_cause / arch-1217):
// access-preview's SubjectAccessReview POSTs run as a NON-UAF api-step with
// EndpointRef==nil under a prewarm ctx carrying the cohort's Krateo authn
// RS256 JWT via xcontext.WithAccessToken. Because EndpointRef==nil,
// bearerAppendForStage returns true (self_host.go) -> runStage writes
// Authorization: Bearer <cohort JWT> onto the stage-local Headers. The
// nil-ref + internal driver resolves to snowplow's OWN SA endpoint
// (resolveOne's WithInternalEndpoint branch -> isSA=true), and the non-GET
// call falls through to the external/owned-fetch dial. httpClientForEndpoint
// takes the owned-CA branch and, seeing the WithServiceAccountDial marker,
// sets transport.Config.BearerTokenFile to the projected SA token. But
// client-go's bearer round-tripper does NOT overwrite an EXISTING
// Authorization header (transport/round_trippers.go) -> the SA token is never
// attached; the request reaches the apiserver bearing the (non-k8s) Krateo
// JWT -> 401, AND the JWT leaks to the apiserver (unintended audience).
//
// THE FIX (self_host.go + resolve.go): thread the provenance bool isSA into
// bearerAppendForStage and refuse the append for an SA dial — the user JWT is
// never written onto an SA-endpoint call, so client-go's BearerTokenFile
// wrapper attaches the SA token.
//
// THIS FALSIFIER drives the REAL api.Resolve -> runStage (where the append
// lives) -> external/owned-fetch -> httpClientForEndpoint -> transport.New ->
// real dial against an httptest TLS apiserver that RECORDS the inbound
// Authorization. Distinct sentinels (JWT vs SA-token file) make a pass
// non-vacuous and exercise client-go's skip-if-Authorization-present.
//
//   RED (today's code): recorded auth == "Bearer <JWT sentinel>"
//       — suppression (SA token absent) AND leak (JWT on the wire).
//   GREEN (post-fix):   recorded auth == "Bearer <SA-token sentinel>"
//       — the append is gated off by isSA; the SA token file reaches the wire.
//
// No kubeconfig, no kind cluster (the integration TestMain in resolve_test.go
// is //go:build integration; this file is //go:build unit).

package api

import (
	"context"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"

	"k8s.io/client-go/rest"
)

const (
	// The cohort's Krateo authn RS256 JWT (NOT a k8s token). If this reaches
	// the wire on the SA dial, it both suppressed the SA token and leaked.
	jwt271Sentinel = "JWT-SENTINEL-271-krateo-portal-authn-rs256"
	// The projected SA token file contents — what a correct SA dial presents.
	saToken271Sentinel = "SA-TOKEN-SENTINEL-271-projected-file"
	// The SAR endpoint path (the real access-preview shape).
	sarPath271 = "/apis/authorization.k8s.io/v1/subjectaccessreviews"
)

// saDialFixture is an httptest TLS server that records the inbound
// Authorization header (and path) of every request. Its own leaf cert is
// exposed as caPEM so the owned-CA dial branch (endpointNeedsOwnedCAClient ->
// transport.Config.TLS.CAData) trusts it and completes a REAL TLS handshake.
type saDialFixture struct {
	server    *httptest.Server
	caPEM     string
	mu        sync.Mutex
	gotAuth   string
	gotPath   string
	gotMethod string
	hits      int
}

func newSADialFixture(t *testing.T) *saDialFixture {
	t.Helper()
	f := &saDialFixture{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.gotAuth = r.Header.Get("Authorization")
		f.gotPath = r.URL.Path
		f.gotMethod = r.Method
		f.hits++
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// A valid, minimal SAR response (POST) / empty List (GET). Both are
		// well-formed JSON so feedBytes / the dispatch never errors before the
		// dial we care about has already recorded its Authorization.
		fmt.Fprint(w, `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true},"items":[]}`)
	}))
	t.Cleanup(srv.Close)
	f.server = srv
	// The server's own leaf cert, PEM-encoded, is the CA the owned-CA dial
	// trusts (self-signed leaf in the root pool verifies itself).
	cert := srv.Certificate()
	f.caPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
	return f
}

func (f *saDialFixture) auth() string   { f.mu.Lock(); defer f.mu.Unlock(); return f.gotAuth }
func (f *saDialFixture) path() string   { f.mu.Lock(); defer f.mu.Unlock(); return f.gotPath }
func (f *saDialFixture) method() string { f.mu.Lock(); defer f.mu.Unlock(); return f.gotMethod }
func (f *saDialFixture) count() int     { f.mu.Lock(); defer f.mu.Unlock(); return f.hits }

// sa271Ctx builds the prewarm-shaped resolve ctx: UserInfo (Resolve bails
// without it) + the cohort Krateo JWT (WithAccessToken) + the SA endpoint
// attached via WithInternalEndpoint (nil-ref -> resolveOne returns isSA=true).
// The SA endpoint carries token-auth + the fixture CA and NO client cert, so
// endpointNeedsOwnedCAClient is true and the dial takes the owned-CA branch.
func sa271Ctx(f *saDialFixture) context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "access-preview-cohort"}),
		xcontext.WithAccessToken(jwt271Sentinel),
	)
	saEP := &endpoints.Endpoint{
		ServerURL:                f.server.URL,
		Token:                    "STALE-BOOT-TOKEN-must-not-appear", // superseded by BearerTokenFile on the SA dial
		CertificateAuthorityData: f.caPEM,
	}
	return cache.WithInternalEndpoint(ctx, saEP)
}

// installSATokenFile points ServiceAccountTokenFile() at a temp file holding
// the SA-token sentinel, so a correct SA dial presents saToken271Sentinel.
func installSATokenFile(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(saToken271Sentinel), 0o600); err != nil {
		t.Fatalf("write SA token file: %v", err)
	}
	restore := dynamic.SetServiceAccountTokenPathForTest(tokenFile)
	t.Cleanup(restore)
}

// sar271Stage is the vulnerable step: NON-UAF (no UserAccessFilter),
// EndpointRef==nil, Verb=POST, with a SAR payload.
func sar271Stage() *templates.API {
	return &templates.API{
		Name:    "sar",
		Path:    sarPath271,
		Verb:    ptr.To(http.MethodPost),
		Payload: ptr.To(`{"kind":"SubjectAccessReview","apiVersion":"authorization.k8s.io/v1","spec":{"resourceAttributes":{"verb":"list","resource":"compositions"}}}`),
	}
}

func sa271FailFast(t *testing.T) {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
}

// TestFalsifier271_SADial_NonGET_SuppressesAndLeaksUserJWT — the load-bearing
// RED/GREEN pair. A non-UAF nil-ref POST to the SA endpoint must present the
// SA token file on the wire, NEVER the cohort Krateo JWT.
//
//	RED (pre-fix):  recorded auth == "Bearer JWT-SENTINEL..." -> the first
//	                Fatalf fires (suppression + leak).
//	GREEN (post-fix): recorded auth == "Bearer SA-TOKEN-SENTINEL..." -> passes.
func TestFalsifier271_SADial_NonGET_SuppressesAndLeaksUserJWT(t *testing.T) {
	sa271FailFast(t)
	installSATokenFile(t)

	f := newSADialFixture(t)

	dict := Resolve(sa271Ctx(f), ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{sar271Stage()},
		RESTActionNamespace: "krateo-system",
		RESTActionName:      "access-preview",
	})
	_ = dict

	// Premise: the SA dial actually reached the apiserver (else the wire
	// assertion is vacuous — negative_evidence_needs_its_scope).
	if f.count() == 0 || f.method() != http.MethodPost || f.path() != sarPath271 {
		t.Fatalf("premise: the non-GET SA dial did not reach the fake apiserver as expected (hits=%d, method=%q, path=%q, want POST %q) — "+
			"the wire-Authorization assertion would be vacuous. Did the POST fall through to external/owned-fetch?",
			f.count(), f.method(), f.path(), sarPath271)
	}

	got := f.auth()

	// RED dimension — SUPPRESSION + LEAK: the cohort Krateo JWT rode the wire.
	if got == "Bearer "+jwt271Sentinel {
		t.Fatalf("#271 SUPPRESSION+LEAK (RED): the SA-endpoint dial carried the cohort Krateo authn JWT on the wire "+
			"(%q). The user JWT was appended to an SA dial (bearerAppendForStage true on EndpointRef==nil); client-go "+
			"then refused to overwrite it with the SA BearerTokenFile token, so the SAR reaches the apiserver as a "+
			"non-k8s JWT (401) and the JWT leaks. bearerAppendForStage must return false when isSA.", got)
	}

	// GREEN dimension: the SA token file reached the wire.
	if got != "Bearer "+saToken271Sentinel {
		t.Fatalf("#271 (RED): the SA dial did not present the projected SA token file on the wire "+
			"(got %q, want %q). With the user-JWT append gated off by isSA, client-go's BearerTokenFile wrapper "+
			"must attach the SA token.", got, "Bearer "+saToken271Sentinel)
	}

	// Belt-and-suspenders: the stale boot ep.Token must never appear either.
	if got == "Bearer STALE-BOOT-TOKEN-must-not-appear" {
		t.Fatalf("#271: the SA dial presented the stale static ep.Token (%q) instead of the token file.", got)
	}
}

// NOTE on a GET control: arch-1217's skeleton suggested a GET arm to show the
// live GET path already authenticates as the SA (the bug being non-GET only).
// That live behaviour comes from the GET-verb interception onto the internal SA
// rest config, which is an in-cluster/informer property NOT reproducible in
// this hermetic harness — here a nil-ref GET takes the SAME external/owned-fetch
// append path and leaks identically (empirically verified during RED-first).
// A hermetic GET arm would therefore assert a premise that is false in-process
// (feedback_no_fake_production_scenarios), so it is omitted; the live GET-safety
// is corroborated by the 403/404 (post-authentication) answers in
// project_271_sar401_third_cause, not by a hermetic test.
