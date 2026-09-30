//go:build unit
// +build unit

// resolve_jwt_clientconfig_leak_falsifier_292_test.go — #292 RED-first falsifier
// (the #271 completion: the isSA=false nil-ref CLIENTCONFIG carrier).
//
// THE BUG (arch-1217 ruling): #271 gated the user-Krateo-JWT append off the SA
// dial (isSA). A nil-endpointRef stage that resolves to the user's
// <user>-clientconfig has isSA=FALSE, so bearerAppendForStage's bare
// `EndpointRef==nil` disjunct still appends the Krateo authn JWT. A clientconfig
// is CERT-AUTH (no client-go bearer round-tripper), so — unlike #271's
// token-auth SUPPRESSION carrier — the appended `Authorization: Bearer <JWT>`
// is actively PRESENTED on the wire to the apiserver (the leak). This is a
// DIFFERENT carrier than #271, so per feedback_leak_needs_acceptance_arm_per_carrier
// it gets its own end-to-end arm.
//
// THE FIX (arch-1217): DROP the bare `EndpointRef==nil` disjunct — a nil-ref
// always resolves to an apiserver-credential dial (SA, or the user's
// clientconfig) that carries its own k8s credential and never wants the Krateo
// JWT. The two legitimate JWT targets (ExportJWT opt-in, self-loopback) remain.
//
// This drives the REAL api.Resolve → runStage (append) → the cert-auth
// clientconfig dial (plumbing HTTPClientForEndpoint, NOT the owned-CA branch) →
// real TLS handshake against an httptest server recording the wire
// Authorization. The clientconfig is resolved for real via the secrets-cache
// snapshot (isSA=false), NOT installed as crossed state.
//
//   RED (today): recorded Authorization == "Bearer <JWT sentinel>"  (the leak)
//   GREEN (post-fix): NO Authorization header (cert-auth presents only the
//                     client cert; the append is gated off).
//
// Hermetic (//go:build unit, no kind cluster). Reuses tlsServerWithCA +
// selfSignedClientPair (endpoints_tls_229_falsifier_test.go) and mkSecret
// (endpoints_cache_test.go).

package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/client-go/rest"
)

const (
	jwt292Sentinel = "JWT-SENTINEL-292-krateo-portal-authn-rs256"
	authnNS292     = "krateo-system"
	user292        = "victim"
)

// seedCertAuthClientconfig292 makes the secrets cache servable in authnNS with a
// CERT-AUTH <user>-clientconfig Secret pointing at srvURL (server-url), trusting
// caPEM, and presenting a throwaway client cert/key — the exact shape of the 7
// live 057 clientconfigs. TestMode is set so resolveOne keeps the seeded
// server-url (the isInternal→kubernetes.default.svc override is skipped under
// TestMode), letting the dial reach the httptest server so the wire is
// observable.
func seedCertAuthClientconfig292(t *testing.T, srvURL string, caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })

	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	// plumbing base64-decodes ca/cert/key (transport.go); live clientconfig
	// Secrets store single-base64 PEM.
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{
		ByName: map[string]*corev1.Secret{
			user292 + "-clientconfig": mkSecret(authnNS292, user292+"-clientconfig", map[string]string{
				"server-url":                 srvURL,
				"certificate-authority-data": base64.StdEncoding.EncodeToString(caPEM),
				"client-certificate-data":    base64.StdEncoding.EncodeToString(certPEM),
				"client-key-data":            base64.StdEncoding.EncodeToString(keyPEM),
			}),
		},
	})
	cache.ForceSecretsCacheReadyForTest(authnNS292)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
}

// TestFalsifier292_ClientconfigDial_DoesNotLeakUserJWT — the end-to-end
// cert-auth-carrier arm. A non-UAF nil-ref POST resolves to the user's cert-auth
// clientconfig (isSA=false); the wire must NOT carry the cohort Krateo JWT.
func TestFalsifier292_ClientconfigDial_DoesNotLeakUserJWT(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	var mu sync.Mutex
	var gotAuth string
	var hits int
	srv, caPEM := tlsServerWithCA(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		hits++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":{"allowed":true}}`))
	})
	certPEM, keyPEM := selfSignedClientPair(t)

	seedCertAuthClientconfig292(t, srv.URL, caPEM, certPEM, keyPEM)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user292}),
		xcontext.WithAccessToken(jwt292Sentinel),
	)
	// NOTE: deliberately NO cache.WithInternalEndpoint — that would force
	// isSA=true (the SA path). The nil ref must resolve via the clientconfig
	// snapshot (isSA=false) to exercise the #292 carrier.

	stage := &templates.API{
		Name:    "sar",
		Path:    "/apis/authorization.k8s.io/v1/subjectaccessreviews",
		Verb:    ptr.To(http.MethodPost),
		Payload: ptr.To(`{"kind":"SubjectAccessReview","apiVersion":"authorization.k8s.io/v1","spec":{"resourceAttributes":{"verb":"list","resource":"compositions"}}}`),
	}

	_ = Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		AuthnNS:             authnNS292,
		Items:               []*templates.API{stage},
		RESTActionNamespace: authnNS292,
		RESTActionName:      "access-preview",
	})

	mu.Lock()
	auth, n := gotAuth, hits
	mu.Unlock()

	// Premise: the cert-auth clientconfig dial reached the server (else the
	// wire assertion is vacuous — negative_evidence_needs_its_scope). If this
	// fails the clientconfig resolve/dial did not happen — the arm is void.
	if n == 0 {
		t.Fatalf("premise: the nil-ref cert-auth clientconfig dial did not reach the fake apiserver (hits=0) — "+
			"the wire-Authorization assertion would be vacuous. Did the nil ref resolve to %s-clientconfig via the snapshot (isSA=false)?", user292)
	}

	// RED — LEAK: the cohort Krateo JWT rode the wire on the cert-auth dial.
	if auth == "Bearer "+jwt292Sentinel {
		t.Fatalf("#292 LEAK (RED): the user's Krateo authn JWT was PRESENTED on the wire to the cert-auth "+
			"clientconfig dial (%q). bearerAppendForStage appended it on the bare EndpointRef==nil disjunct "+
			"(isSA=false); cert-auth has no bearer round-tripper to skip it, so the JWT leaks to the apiserver. "+
			"Drop the nil-ref disjunct — a clientconfig carries its own k8s credential and never wants the Krateo JWT.", auth)
	}

	// GREEN — exact leak-absence (C1 discipline): no Authorization header at all.
	// The cert-auth clientconfig authenticates with its client cert; with the
	// append gated off there is NO bearer on the wire. Any non-empty Authorization
	// is a residual leak and MUST fail.
	if auth != "" {
		t.Fatalf("#292 (RED/leak-absence): the cert-auth clientconfig dial carried an Authorization header (%q); "+
			"post-fix it must present ONLY the client certificate — no bearer of any kind.", auth)
	}
}
