//go:build unit
// +build unit

package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
)

// endpoints_tls_229_falsifier_test.go — the falsifier arms for
// snowplow#229 / audit A1: plumbing DROPS the CA bundle of a TOKEN-AUTH
// endpoint (transport.go:37 returns before the CA install when
// !HasCertAuth()), so every non-GET api-step with no `endpointRef`
// failed TLS against the cluster's private CA while GETs escaped by
// being intercepted upstream.
//
// These drive the REAL boundary: httpFetchAllowingNonJSON against a
// genuine httptest TLS server with a private CA, through the real
// call site (external_fetch.go:136 → httpClientForEndpoint). No seam,
// no injected client — a green arm here means a real TLS handshake
// against a real private CA completed.
//
// The endpoint in every token-auth arm is built in the EXACT
// sa_client.go:126-131 shape: Token set, CertificateAuthorityData set,
// ClientCertificateData/ClientKeyData EMPTY, Insecure false.
//
// No kubeconfig, no kind cluster — pure in-process
// (feedback_no_go_test_against_remote_kubeconfig).

// sarPayload is a SubjectAccessReview body of the shape the ten live
// access-preview steps POST — an explicit-subject SAR, i.e. the request
// that has never once succeeded on 057.
const sarPayload = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview",` +
	`"spec":{"user":"alice","groups":["system:authenticated"],` +
	`"resourceAttributes":{"group":"composition.krateo.io","resource":"compositions","verb":"create"}}}`

const sarAllowedResponse = `{"status":{"allowed":true,"evaluated":true}}`

const saTestToken = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.sa-projected-token.sig"

// tlsServerWithCA starts an httptest TLS server with a self-signed
// certificate and returns it plus that certificate as RAW PEM — the
// exact shape sa_client.go reads out of the projected
// /var/run/secrets/.../ca.crt.
func tlsServerWithCA(t *testing.T, h http.HandlerFunc) (*httptest.Server, []byte) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: srv.Certificate().Raw,
	})
	if len(caPEM) == 0 {
		t.Fatalf("could not encode test server certificate to PEM")
	}
	return srv, caPEM
}

// saShapedEndpoint builds the endpoint EXACTLY as sa_client.go:126-131
// does: token-auth, CA present, no client cert/key.
func saShapedEndpoint(serverURL string, caData []byte) endpoints.Endpoint {
	return endpoints.Endpoint{
		ServerURL:                serverURL,
		Token:                    saTestToken,
		CertificateAuthorityData: string(caData),
		Insecure:                 false,
	}
}

// drive runs the REAL fetch through the REAL client builder.
func drive(t *testing.T, ep endpoints.Endpoint, verb, payload string) (*response.Status, []byte, error) {
	t.Helper()
	opts := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Verb:    ptr.To(verb),
			Payload: ptr.To(payload),
		},
		Endpoint: &ep,
	}
	st, jsonBytes, _, err := httpFetchAllowingNonJSON(context.Background(), opts)
	return st, jsonBytes, err
}

// sarHandler answers a SAR POST, recording the Authorization header and
// the verb it actually saw.
func sarHandler(gotAuth *string, gotVerb *string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		*gotAuth = r.Header.Get("Authorization")
		*gotVerb = r.Method
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(sarAllowedResponse))
	}
}

// --- ARM 1: the RED arm. POST + token-auth + raw-PEM CA. -------------
//
// This is the live failure reproduced exactly: POST to
// /apis/authorization.k8s.io/v1/subjectaccessreviews on a token-auth
// endpoint whose CA is RAW PEM. Red before the fix with
// "x509: certificate signed by unknown authority".
func TestIssue229_Arm1_POSTTokenAuthRawPEMCA(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, sarHandler(&gotAuth, &gotVerb))

	ep := saShapedEndpoint(srv.URL, caPEM)
	st, jsonBytes, err := drive(t, ep, http.MethodPost, sarPayload)

	if err != nil {
		t.Fatalf("ARM1 FAIL: POST through the token-auth/raw-PEM-CA endpoint errored: %v", err)
	}
	if st == nil || st.Status == response.StatusFailure {
		t.Fatalf("ARM1 FAIL: expected a non-failure envelope, got %+v", st)
	}
	if gotVerb != http.MethodPost {
		t.Fatalf("ARM1 FAIL: server saw verb %q, want POST", gotVerb)
	}
	// Content, not just status (feedback_validate_content_not_just_status):
	// the server's actual bytes must come back.
	if !strings.Contains(string(jsonBytes), `"allowed":true`) {
		t.Fatalf("ARM1 FAIL: body did not carry the server's bytes, got %q", string(jsonBytes))
	}
}

// --- ARM 2: the ENCODING arm. Same POST, base64 CA. ------------------
//
// sa_client.go hands us RAW PEM; a `<user>-clientconfig` Secret hands us
// single-base64 PEM. A fix that handles one and not the other works on
// one cluster and not another, so both must pass.
func TestIssue229_Arm2_POSTTokenAuthBase64CA(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, sarHandler(&gotAuth, &gotVerb))

	b64 := base64.StdEncoding.EncodeToString(caPEM)
	ep := saShapedEndpoint(srv.URL, []byte(b64))
	st, jsonBytes, err := drive(t, ep, http.MethodPost, sarPayload)

	if err != nil {
		t.Fatalf("ARM2 FAIL: POST with a single-base64 CA errored: %v", err)
	}
	if st == nil || st.Status == response.StatusFailure {
		t.Fatalf("ARM2 FAIL: expected a non-failure envelope, got %+v", st)
	}
	if !strings.Contains(string(jsonBytes), `"allowed":true`) {
		t.Fatalf("ARM2 FAIL: body did not carry the server's bytes, got %q", string(jsonBytes))
	}
}

// --- ARM 3: AUTH-SURVIVAL. ------------------------------------------
//
// Without this, a "fix" that drops the bearer roundtripper still passes
// arms 1-2 (the test server does not require auth) while silently
// downgrading every SA call to anonymous. The identity presented must be
// UNCHANGED by this fix — that property is what makes it safe.
func TestIssue229_Arm3_BearerTokenSurvives(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, sarHandler(&gotAuth, &gotVerb))

	ep := saShapedEndpoint(srv.URL, caPEM)
	if _, _, err := drive(t, ep, http.MethodPost, sarPayload); err != nil {
		t.Fatalf("ARM3 FAIL: POST errored: %v", err)
	}

	want := "Bearer " + saTestToken
	if gotAuth != want {
		t.Fatalf("ARM3 FAIL: the SA bearer token did not reach the wire.\n got: %q\nwant: %q", gotAuth, want)
	}
}

// --- ARM 4: ANTI-TRIVIAL. The one that stops the vacuous fix. --------
//
// Same token-auth POST, but CertificateAuthorityData is a DIFFERENT,
// unrelated CA. This MUST still fail x509. Without it, a fix that simply
// sets InsecureSkipVerify:true satisfies arms 1-3 vacuously while
// disabling certificate verification for every SA call in the process.
//
// THE UNRELATED CA MUST BE FRESHLY GENERATED. A second
// httptest.NewTLSServer does NOT provide one: net/http/httptest serves a
// single baked-in certificate for every instance, so two servers are
// byte-identical (verified: both certs sha256
// 468174fd18ae990a0a1e10568e30f9819a8acd23224c319f4ec3eb4f6f2980d9).
// Sourcing the "unrelated" CA that way makes this arm assert that a
// CORRECT CA fails — it goes red against a working fix and is the arm
// blaming the code for its own defect.
func TestIssue229_Arm4_UnrelatedCAStillFailsX509(t *testing.T) {
	var gotAuth, gotVerb string
	srv, srvCAPEM := tlsServerWithCA(t, sarHandler(&gotAuth, &gotVerb))

	otherCAPEM, _ := selfSignedClientPair(t)
	if string(otherCAPEM) == string(srvCAPEM) {
		t.Fatalf("ARM4 PREMISE BROKEN: the 'unrelated' CA is identical to the server's own; " +
			"this arm would be asserting that a correct CA fails")
	}

	ep := saShapedEndpoint(srv.URL, otherCAPEM)
	st, _, err := drive(t, ep, http.MethodPost, sarPayload)

	if err == nil && st != nil && st.Status != response.StatusFailure {
		t.Fatalf("ARM4 FAIL: a POST verified against an UNRELATED CA succeeded — " +
			"certificate verification is not actually happening (InsecureSkipVerify, or an empty root pool " +
			"falling back to system roots)")
	}
	// Pin the reason, not merely the failure: a failure for some other
	// cause would make this arm pass without testing anything.
	combined := ""
	if err != nil {
		combined = err.Error()
	}
	if st != nil {
		combined += " " + st.Message
	}
	if !strings.Contains(combined, "x509") && !strings.Contains(combined, "certificate") {
		t.Fatalf("ARM4 FAIL: expected a certificate-verification failure, got %q", combined)
	}
	if gotAuth != "" {
		t.Fatalf("ARM4 FAIL: the request reached the server despite an unrelated CA (auth=%q)", gotAuth)
	}
}

// --- ARM 5: the GET NEIGHBOUR. The other side of the asymmetry. ------
//
// The bug IS the GET/POST asymmetry, so the arm must pin both sides. In
// production a GET is intercepted by one of the four CA-bearing dispatch
// branches and never reaches this builder; here we drive one through the
// SAME builder to prove the fix does not regress a GET that does reach
// it.
func TestIssue229_Arm5_GETNeighbourStillWorks(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotVerb = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"APIResourceList"}`))
	})

	ep := saShapedEndpoint(srv.URL, caPEM)
	st, jsonBytes, err := drive(t, ep, http.MethodGet, "")

	if err != nil {
		t.Fatalf("ARM5 FAIL: the GET neighbour errored: %v", err)
	}
	if st == nil || st.Status == response.StatusFailure {
		t.Fatalf("ARM5 FAIL: expected a non-failure envelope for GET, got %+v", st)
	}
	if gotVerb != http.MethodGet {
		t.Fatalf("ARM5 FAIL: server saw verb %q, want GET", gotVerb)
	}
	if !strings.Contains(string(jsonBytes), "APIResourceList") {
		t.Fatalf("ARM5 FAIL: GET body mismatch: %q", string(jsonBytes))
	}
	if gotAuth != "Bearer "+saTestToken {
		t.Fatalf("ARM5 FAIL: GET lost its bearer token: %q", gotAuth)
	}
}

// --- ARM 6: NO-REGRESSION. The live cert-auth population. ------------
//
// All 7 live `<user>-clientconfig` Secrets on 057 are CERT-AUTH, and for
// those plumbing already takes its CA branch and works. That path must
// stay byte-identical — the fix must DELEGATE, not reroute. This arm
// drives a real cert-auth endpoint end to end through the unchanged
// plumbing builder.
func TestIssue229_Arm6_CertAuthEndpointUnchanged(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotVerb = r.Method
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})

	certPEM, keyPEM := selfSignedClientPair(t)

	// plumbing base64-decodes all three fields (transport.go:41,46,59),
	// so a cert-auth endpoint carries them base64-encoded.
	ep := endpoints.Endpoint{
		ServerURL:                srv.URL,
		CertificateAuthorityData: base64.StdEncoding.EncodeToString(caPEM),
		ClientCertificateData:    base64.StdEncoding.EncodeToString(certPEM),
		ClientKeyData:            base64.StdEncoding.EncodeToString(keyPEM),
		Insecure:                 false,
	}

	// Guard the routing decision itself: this endpoint must NOT take the
	// owned branch.
	if endpointNeedsOwnedCAClient(&ep) {
		t.Fatalf("ARM6 FAIL: a cert-auth endpoint was routed to the owned client; " +
			"the live <user>-clientconfig population must keep using plumbing verbatim")
	}

	st, jsonBytes, err := drive(t, ep, http.MethodGet, "")
	if err != nil {
		t.Fatalf("ARM6 FAIL: the cert-auth endpoint regressed: %v", err)
	}
	if st == nil || st.Status == response.StatusFailure {
		t.Fatalf("ARM6 FAIL: expected a non-failure envelope, got %+v", st)
	}
	if gotVerb != http.MethodGet || !strings.Contains(string(jsonBytes), `"ok":true`) {
		t.Fatalf("ARM6 FAIL: verb=%q body=%q", gotVerb, string(jsonBytes))
	}
	if gotAuth != "" {
		t.Fatalf("ARM6 FAIL: a cert-auth endpoint must not send an Authorization header, got %q", gotAuth)
	}
}

// --- ARM 7: the ROUTING PREDICATE, exhaustively. ---------------------
//
// endpointNeedsOwnedCAClient decides which endpoints change behaviour at
// all. Pinning it directly is what bounds the blast radius of this fix:
// exactly one shape is rerouted and every other shape still reaches
// plumbing untouched.
func TestIssue229_Arm7_RoutingPredicate(t *testing.T) {
	const ca = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
	cases := []struct {
		name string
		ep   *endpoints.Endpoint
		want bool
	}{
		{"nil endpoint", nil, false},
		{"no CA at all", &endpoints.Endpoint{ServerURL: "https://x", Token: "t"}, false},
		{"token-auth WITH a CA (the defect)", &endpoints.Endpoint{ServerURL: "https://x", Token: "t", CertificateAuthorityData: ca}, true},
		{"cert-auth with a CA (live population)", &endpoints.Endpoint{ServerURL: "https://x", CertificateAuthorityData: ca, ClientCertificateData: "c", ClientKeyData: "k"}, false},
		{"insecure explicitly requested", &endpoints.Endpoint{ServerURL: "https://x", Token: "t", CertificateAuthorityData: ca, Insecure: true}, false},
		{"basic-auth with a CA", &endpoints.Endpoint{ServerURL: "https://x", Username: "u", Password: "p", CertificateAuthorityData: ca}, true},
		// The EMPTY-USERNAME exclusion. plumbing calls this basic auth (it keys
		// off Password) and sends Basic base64(":password"); client-go keys off
		// Username and would install NO wrapper at all, so the owned branch
		// cannot reproduce it and must not claim it. Listed here and not only in
		// ARM 11 because this table is where the next reader checks what the
		// predicate does — omitting the shape would make the table lie by
		// omission. See TestIssue229_Arm11_PasswordOnlyBasicAuthIsNotSilentlyDropped
		// for the wire-level proof.
		{"basic-auth with an EMPTY username (the silent-drop shape)", &endpoints.Endpoint{ServerURL: "https://x", Password: "p", CertificateAuthorityData: ca}, false},
		// AWS auth — SigV4 is unexported upstream, so this shape is delegated
		// too. Same principle as the row above: the owned branch owns only what
		// it can reproduce exactly.
		{"AWS auth with a CA", &endpoints.Endpoint{ServerURL: "https://x", CertificateAuthorityData: ca, AwsAccessKey: "AKIA", AwsSecretKey: "s", AwsRegion: "eu-west-1", AwsService: "execute-api"}, false},
		// THE LIVE POPULATION GUARD. Field-for-field the shape sa_client.go:126
		// builds, which is what every non-GET no-endpointRef api-step uses. The
		// owned branch now carries TWO exclusions beyond its original predicate,
		// and the specific regression that creates is narrowing twice and
		// silently dropping part of the population the fix exists to serve.
		// Every other arm would stay green, because none of them asserts the
		// POPULATION. This row fails the moment a future exclusion captures it.
		//
		// WHY ONE SHAPE AND NOT FIVE — read this before concluding otherwise
		// from the five WithInternalEndpoint call sites. A nil endpointRef does
		// NOT resolve by a single lookup: endpoints.go consults a
		// CONTEXT-CARRIED internal endpoint first and only falls through to the
		// per-user clientconfig path if none is set. So the real question is
		// what can be PLACED INTO that context, and the answer is checked by
		// enumeration, not assumption:
		//
		//   - sa_client.go:126 is the ONLY populated endpoints.Endpoint{}
		//     construction in production code; every other one is a zero-value
		//     error return, and ServiceAccountEndpoint() memoises a singleton,
		//     so there is exactly one set of field values in the process.
		//   - all five setters (phase1_walk.go:1158, phase1_pip_seed.go:662,
		//     phase1_content_prewarm.go:247, restactions.go:261,
		//     widgets.go:272) pass that same SA endpoint.
		//   - cluster_list.go:557 LOOKS like a sixth but is a FORWARDER: it
		//     re-attaches what it read from the customer context onto a fresh
		//     background context for the async populate. It propagates; it
		//     never builds.
		//
		// extractEndpointFromSecret (endpoints_cache.go:168) is a genuine second
		// builder, but it serves `<user>-clientconfig` Secrets reached via
		// endpointRef — never this path.
		{"THE LIVE SA FALL-THROUGH SHAPE (sa_client.go:126-131)", &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-token", CertificateAuthorityData: ca, Insecure: false}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := endpointNeedsOwnedCAClient(tc.ep); got != tc.want {
				t.Fatalf("ARM7 FAIL: endpointNeedsOwnedCAClient = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- ARM 8: CA SHAPE DETECTION. -------------------------------------
//
// caPEMBytes is what makes arms 1 and 2 both pass. The ok=false cases
// are load-bearing: an unrecognised CA must NOT yield an empty root pool
// (which silently falls back to the system roots — the exact failure
// this fix removes).
func TestIssue229_Arm8_CAShapeDetection(t *testing.T) {
	_, caPEM := tlsServerWithCA(t, func(w http.ResponseWriter, r *http.Request) {})
	b64 := base64.StdEncoding.EncodeToString(caPEM)
	dbl := base64.StdEncoding.EncodeToString([]byte(b64))

	t.Run("raw PEM (the sa_client.go shape)", func(t *testing.T) {
		got, ok := caPEMBytes(caPEM)
		if !ok || string(got) != string(caPEM) {
			t.Fatalf("ARM8 FAIL: raw PEM not recognised (ok=%v)", ok)
		}
	})
	t.Run("single-base64 PEM (the clientconfig shape)", func(t *testing.T) {
		got, ok := caPEMBytes([]byte(b64))
		if !ok || string(got) != string(caPEM) {
			t.Fatalf("ARM8 FAIL: single-base64 PEM not decoded to raw PEM (ok=%v)", ok)
		}
	})
	t.Run("double-base64 PEM (the operator-written shape)", func(t *testing.T) {
		got, ok := caPEMBytes([]byte(dbl))
		if !ok || string(got) != string(caPEM) {
			t.Fatalf("ARM8 FAIL: double-base64 PEM not decoded to raw PEM (ok=%v)", ok)
		}
	})
	t.Run("empty is not a CA", func(t *testing.T) {
		if _, ok := caPEMBytes(nil); ok {
			t.Fatalf("ARM8 FAIL: empty input must not be reported as a usable CA")
		}
	})
	t.Run("garbage is rejected, not silently emptied", func(t *testing.T) {
		if _, ok := caPEMBytes([]byte("not a certificate at all")); ok {
			t.Fatalf("ARM8 FAIL: unrecognised CA shape must report ok=false so the caller delegates " +
				"rather than building a client with an empty root pool")
		}
	})
}

// selfSignedClientPair generates a throwaway client certificate + key as
// PEM, so ARM6 can exercise the real cert-auth branch end to end rather
// than asserting on the routing predicate alone.
func selfSignedClientPair(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "snowplow-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
