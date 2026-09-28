//go:build falsifier_268_267

// sa_dial_tokenfile_falsifier_268_267_test.go — Arm F (#267) dial-site half +
// the SA-dial recognition guard matrix (arch-268 Option 1: PROVENANCE, not shape).
//
// The robust #267 fix routes the SA endpoint to transport.Config.BearerTokenFile
// (client-go's NewBearerAuthWithRefreshRoundTripper / NewCachedFileTokenSource,
// self-reloading) — but ONLY when the dispatch ctx is marked
// cache.WithServiceAccountDial (stamped by runStage from resolveStageEndpoint's SA
// provenance). Recognition is NEVER by endpoint shape: a token-auth per-user
// <user>-clientconfig is shape-identical to the SA endpoint.
//
// GUARD MATRIX (sha256/wire differs between GUARD and POSITIVE):
//   - POSITIVE: SA endpoint, ctx MARKED → given the token FILE → after the file
//     rotates, the wire bearer tracks the CURRENT file token (the #267 GREEN
//     criterion; relocated here from the removed ServiceAccountEndpoint-re-read
//     assertion).
//   - GUARD (the real one): a TOKEN-auth clientconfig (same apiserver ServerURL +
//     cluster CAData as the SA endpoint, no cert → reaches the owned-CA branch),
//     ctx NOT marked → wire bearer == its OWN static token, never the SA token
//     file. RED against a shape predicate (which would misclassify it as the SA);
//     GREEN against Option 1.
//   - NON-REG (cannot fail): a cert-auth clientconfig never reaches the owned-CA
//     branch (endpointNeedsOwnedCAClient excludes HasCertAuth) → dials with its
//     client cert via plumbing.
package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
)

// saDialSelfSignedPEM returns a valid self-signed certificate PEM + its key PEM —
// enough for caPEMBytes to accept the CA. The connection is plain HTTP (httptest),
// so the material is only exercised as valid PEM, not as a live TLS handshake.
func saDialSelfSignedPEM(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "sa-dial-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	return certPEM, keyPEM
}

// saDialServer records the Authorization header of every request.
func saDialServer(t *testing.T) (url string, lastAuth func() string) {
	t.Helper()
	var mu sync.Mutex
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auth = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() string { mu.Lock(); defer mu.Unlock(); return auth }
}

// TestLeak267_ArmF_POSITIVE_MarkedCtx_DialPresentsRotatedToken — the #267 GREEN
// criterion. The SA endpoint is minted with a STALE boot token (ep.Token); the
// projected token file then rotates; on a ctx MARKED WithServiceAccountDial the
// dial must present the ROTATED (file) token on the wire, not the stale static one.
func TestLeak267_ArmF_POSITIVE_MarkedCtx_DialPresentsRotatedToken(t *testing.T) {
	const staleBoot = "TOK-STALE-BOOT"
	const rotated = "TOK-FRESH-ROTATED"

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(rotated), 0o600); err != nil {
		t.Fatalf("write rotated token: %v", err)
	}
	restore := dynamic.SetServiceAccountTokenPathForTest(tokenFile)
	defer restore()

	certPEM, _ := saDialSelfSignedPEM(t)
	url, lastAuth := saDialServer(t)

	saEP := &endpoints.Endpoint{ServerURL: url, Token: staleBoot, CertificateAuthorityData: certPEM}
	// PROVENANCE: mark the dispatch ctx (as runStage does for an SA-endpoint stage).
	ctx := cache.WithServiceAccountDial(context.Background())

	cli, err := httpClientForEndpoint(ctx, saEP, &httpcall.RequestInfo{})
	if err != nil {
		t.Fatalf("httpClientForEndpoint: %v", err)
	}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = resp.Body.Close()

	auth := lastAuth()
	t.Logf("Arm F/POSITIVE wire Authorization=%q (want rotated FILE token %q, not stale boot %q)", auth, rotated, staleBoot)
	if strings.Contains(auth, staleBoot) {
		t.Fatalf("#267 — the marked SA dial presented the STALE boot token on the wire (%q); it must read the token FILE at dial time.", auth)
	}
	if !strings.Contains(auth, rotated) {
		t.Fatalf("#267 GREEN criterion NOT met: the marked SA dial did not present the rotated FILE token %q; got %q.", rotated, auth)
	}
	t.Logf("Arm F/POSITIVE GREEN: a WithServiceAccountDial-marked dial presents the rotated projected token from the file (self-adapting).")
}

// TestLeak267_ArmF_AccessPreviewSARPost_FreshTokenOnWire — #267's visible casualty:
// access-preview's SubjectAccessReview POSTs (non-GET → branch E) were 100%
// Unauthorized on the static token. The dial is verb-agnostic, so a marked POST
// presents the fresh file token → authenticates (200).
func TestLeak267_ArmF_AccessPreviewSARPost_FreshTokenOnWire(t *testing.T) {
	const rotated = "TOK-SAR-ROTATED"
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(rotated), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	restore := dynamic.SetServiceAccountTokenPathForTest(tokenFile)
	defer restore()

	certPEM, _ := saDialSelfSignedPEM(t)
	url, lastAuth := saDialServer(t)

	saEP := &endpoints.Endpoint{ServerURL: url, Token: "TOK-STALE-BOOT", CertificateAuthorityData: certPEM}
	ctx := cache.WithServiceAccountDial(context.Background())
	cli, err := httpClientForEndpoint(ctx, saEP, &httpcall.RequestInfo{})
	if err != nil {
		t.Fatalf("httpClientForEndpoint: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, url+"/apis/authorization.k8s.io/v1/subjectaccessreviews",
		strings.NewReader(`{"kind":"SubjectAccessReview"}`))
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("SAR POST dial: %v", err)
	}
	code := resp.StatusCode
	_ = resp.Body.Close()

	auth := lastAuth()
	t.Logf("Arm F/SAR-POST code=%d wire Authorization=%q (want fresh file token %q)", code, auth, rotated)
	if code != http.StatusOK {
		t.Fatalf("Arm F/SAR-POST: the SAR POST must authenticate (200) with the fresh token; got %d", code)
	}
	if !strings.Contains(auth, rotated) {
		t.Fatalf("Arm F/SAR-POST: the SAR POST must present the fresh projected token %q; got %q (the #267 access-preview casualty is not fixed).", rotated, auth)
	}
	t.Logf("Arm F/SAR-POST GREEN: access-preview's SAR POST presents the fresh projected token (verb-agnostic marked dial) → authenticates.")
}

// TestLeak267_ArmF_GUARD_TokenAuthClientconfig_UnmarkedCtx_KeepsOwnToken — the REAL
// guard (arch-268). A TOKEN-auth per-user clientconfig — same apiserver ServerURL +
// cluster CAData as the SA endpoint, no client cert so it reaches the owned-CA
// branch — dispatched on an UNMARKED ctx must present its OWN static token on the
// wire, NEVER the SA token file. RED against a shape predicate (ServerURL+CAData+
// token-auth would misclassify it as the SA and hand it the SA token file); GREEN
// against Option 1 (provenance: FromSecret never marks the ctx).
// The two guard sub-cases are both token-auth-with-CA (→ they reach the owned-CA
// branch where the SA-token-file wiring lives) but the ctx is NOT
// WithServiceAccountDial-marked, so provenance must keep each on its OWN token —
// regardless of ServerURL (per-user apiserver URL OR a customer webhook URL). RED
// against a shape predicate; GREEN by provenance.
func TestLeak267_ArmF_GUARD_TokenAuthClientconfig_UnmarkedCtx_KeepsOwnToken(t *testing.T) {
	const saFileToken = "SA-FILE-TOKEN-SHOULD-NOT-APPEAR"

	// per-user TOKEN-auth clientconfig: same apiserver ServerURL + cluster CAData as
	// the SA endpoint, token-auth, no cert. This is the hole a shape predicate misses.
	t.Run("per_user_token_auth_clientconfig", func(t *testing.T) {
		const userToken = "USER-TOKEN-XYZ"
		dir := t.TempDir()
		tokenFile := filepath.Join(dir, "token")
		if err := os.WriteFile(tokenFile, []byte(saFileToken), 0o600); err != nil {
			t.Fatalf("write SA token: %v", err)
		}
		restore := dynamic.SetServiceAccountTokenPathForTest(tokenFile)
		defer restore()
		certPEM, _ := saDialSelfSignedPEM(t) // stands in for the cluster CA
		url, lastAuth := saDialServer(t)
		ep := &endpoints.Endpoint{ServerURL: url, Token: userToken, CertificateAuthorityData: certPEM}
		if !endpointNeedsOwnedCAClient(ep) {
			t.Fatalf("precondition: token-auth + CA must reach the owned-CA branch")
		}
		cli, err := httpClientForEndpoint(context.Background() /*UNMARKED*/, ep, &httpcall.RequestInfo{})
		if err != nil {
			t.Fatalf("httpClientForEndpoint: %v", err)
		}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_ = resp.Body.Close()
		auth := lastAuth()
		t.Logf("GUARD/per-user-token wire Authorization=%q (want %q, NOT SA file token %q)", auth, userToken, saFileToken)
		if strings.Contains(auth, saFileToken) {
			t.Fatalf("GUARD — RED against a shape predicate: a per-user TOKEN-auth clientconfig (apiserver URL + cluster CA + token, shape-identical to the SA endpoint) was given the SA token file (%q). Unmarked ctx MUST keep its OWN token — provenance, not shape.", auth)
		}
		if !strings.Contains(auth, userToken) {
			t.Fatalf("GUARD: per-user token-auth clientconfig must present its own token %q; got %q", userToken, auth)
		}
		t.Logf("GUARD/per-user-token GREEN: keeps its own token (provenance).")
	})

	// customer token-auth webhook behind a PRIVATE CA (different ServerURL). Also
	// token-auth-with-CA → reaches the owned-CA branch; unmarked → keeps its token.
	t.Run("customer_token_auth_webhook", func(t *testing.T) {
		const webhookToken = "CUSTOMER-WEBHOOK-TOKEN-ABC"
		dir := t.TempDir()
		tokenFile := filepath.Join(dir, "token")
		if err := os.WriteFile(tokenFile, []byte(saFileToken), 0o600); err != nil {
			t.Fatalf("write SA token: %v", err)
		}
		restore := dynamic.SetServiceAccountTokenPathForTest(tokenFile)
		defer restore()
		privateCA, _ := saDialSelfSignedPEM(t) // a customer's private CA
		url, lastAuth := saDialServer(t)       // stands in for the customer webhook host
		ep := &endpoints.Endpoint{ServerURL: url, Token: webhookToken, CertificateAuthorityData: privateCA}
		if !endpointNeedsOwnedCAClient(ep) {
			t.Fatalf("precondition: token-auth + CA must reach the owned-CA branch")
		}
		cli, err := httpClientForEndpoint(context.Background() /*UNMARKED*/, ep, &httpcall.RequestInfo{})
		if err != nil {
			t.Fatalf("httpClientForEndpoint: %v", err)
		}
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_ = resp.Body.Close()
		auth := lastAuth()
		t.Logf("GUARD/customer-webhook wire Authorization=%q (want %q, NOT SA file token %q)", auth, webhookToken, saFileToken)
		if strings.Contains(auth, saFileToken) {
			t.Fatalf("GUARD — a customer token-auth webhook behind a private CA was given the SA token file (%q). Unmarked ctx MUST keep its OWN token — provenance, not shape.", auth)
		}
		if !strings.Contains(auth, webhookToken) {
			t.Fatalf("GUARD: customer webhook must present its own token %q; got %q", webhookToken, auth)
		}
		t.Logf("GUARD/customer-webhook GREEN: keeps its own token (provenance).")
	})
}

// TestLeak267_ArmF_NONREG_CertAuthClientconfig_DelegatesToPlumbing — non-reg
// (cannot fail): a cert-auth clientconfig never reaches the owned-CA branch (where
// the SA-token-file wiring lives), so it can never be given the SA token file — it
// dials with its client cert via plumbing. Structural assertion (no live dial: the
// cert material is base64-encoded for plumbing, orthogonal here).
func TestLeak267_ArmF_NONREG_CertAuthClientconfig_DelegatesToPlumbing(t *testing.T) {
	certPEM, keyPEM := saDialSelfSignedPEM(t)
	certAuthEP := &endpoints.Endpoint{
		ServerURL:                "https://kubernetes.default.svc",
		CertificateAuthorityData: certPEM,
		ClientCertificateData:    certPEM,
		ClientKeyData:            keyPEM,
	}
	if endpointNeedsOwnedCAClient(certAuthEP) {
		t.Fatalf("NON-REG: a cert-auth clientconfig must NOT enter the owned-CA branch (endpointNeedsOwnedCAClient must exclude HasCertAuth); it dials with its client cert via plumbing.")
	}
	t.Logf("NON-REG GREEN: a cert-auth clientconfig delegates to plumbing (client-cert dial) — never reaches the SA-token-file branch.")
}
