package objects

// token_only_endpoint_dial_test.go: a read under a TOKEN-ONLY clientconfig endpoint (a server URL,
// a CA and a bearer token, no client certificate: the Agentiko sandbox visitor's credential, since a
// browser cannot present a client certificate) presents that token on every request it sends.
//
// The route is objects.Get's own-credential fall-through, end to end: Get (cache off) →
// getFromAPIServer → cache.ClientConfigFor → credhttp.NewClientConfig → plumbing's
// kubeconfig.NewClientConfig → dynamic.NewClient, whose RESTMapper discovers the resource before the
// object GET. Up to plumbing v1.67.0, kubeconfig.Marshal wrote no token into the kubeconfig user, so
// the config it built carried no credential at all: discovery went out anonymous, the sandbox's
// fake apiserver logged `identity=none status=403`, and the read failed. plumbing v1.68.0 (#159)
// carries the token. This arm fails on v1.61.0 and passes on v1.68.0.
//
// The DEBUG arm pins what carrying the token must not cost: with DEBUG=true plumbing's kubeconfig
// round tripper logs each request, and the token is never in the log (v1.68.0 logs metadata only).
//
// The certificate-only arm pins the other side: a certificate endpoint still authenticates by its
// client certificate and sends no Authorization header.
//
// The token is generated per run and never printed: a failure names what the header held (absent,
// another credential, a bearer token that is not this one), never its value.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// seenRequest is what the test apiserver recorded of one request: never the header's value.
type seenRequest struct {
	path     string
	auth     string // "absent", "bearer-match", "bearer-other" or "other-scheme"
	clientCN string // the verified client certificate's CN, "" when none was presented
}

func (s seenRequest) discovery() bool {
	return s.path == "/api" || s.path == "/apis" || (strings.HasPrefix(s.path, "/api/v1") && !strings.Contains(s.path, "/namespaces/"))
}

func TestTokenOnlyEndpoint_ObjectsGetPresentsTheBearerTokenOnDiscoveryAndRead(t *testing.T) {
	// The requester's server URL is kept as given (no in-cluster rewrite), and the cache is off so
	// Get takes the apiserver path.
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
	t.Setenv("CACHE_ENABLED", "")

	token := randomToken(t)
	clientCA, clientCAKey := newTestCA(t, "token-only-arm-client-ca")
	certPEM, keyPEM := newClientCert(t, clientCA, clientCAKey, "cert-user")

	var mu sync.Mutex
	var seen []seenRequest
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := seenRequest{path: r.URL.Path, auth: classifyAuth(r.Header.Get("Authorization"), token)}
		if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 {
			s.clientCN = r.TLS.VerifiedChains[0][0].Subject.CommonName
		}
		mu.Lock()
		seen = append(seen, s)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		// As the apiserver: a request with neither credential is anonymous, and anonymous may not
		// discover or read.
		if s.auth != "bearer-match" && s.clientCN == "" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
			return
		}
		switch r.URL.Path {
		case "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`))
		case "/api/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
				`{"name":"configmaps","singularName":"configmap","namespaced":true,"kind":"ConfigMap","verbs":["get","list"]}]}`))
		case "/api/v1/namespaces/ns/configmaps/x":
			_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	srv := httptest.NewUnstartedServer(handler)
	pool := x509.NewCertPool()
	pool.AddCert(clientCA)
	srv.TLS = &tls.Config{ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// The endpoint carries its CA as a clientconfig Secret does: base64 of the PEM.
	serverCA := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	ref := templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: "x", Namespace: "ns"}, APIVersion: "v1", Resource: "configmaps",
	}

	for _, c := range []struct {
		name     string
		ep       endpoints.Endpoint
		wantAuth string
		wantCN   string
		debug    bool // DEBUG=true: plumbing's kubeconfig round tripper logs every request
	}{
		{
			name: "token_only_as_the_sandbox_visitor",
			ep: endpoints.Endpoint{
				ServerURL: srv.URL, CertificateAuthorityData: serverCA, Username: "visitor", Token: token,
			},
			wantAuth: "bearer-match",
		},
		{
			// With the token now on the wire, DEBUG must not put it in the log: plumbing v1.68.0's
			// kubeconfig round tripper logs metadata only (before it, a header dump would have).
			name: "token_only_under_DEBUG_logs_no_token",
			ep: endpoints.Endpoint{
				ServerURL: srv.URL, CertificateAuthorityData: serverCA, Username: "visitor", Token: token,
			},
			wantAuth: "bearer-match",
			debug:    true,
		},
		{
			name: "certificate_only",
			ep: endpoints.Endpoint{
				ServerURL: srv.URL, CertificateAuthorityData: serverCA, Username: "cert-user",
				ClientCertificateData: base64.StdEncoding.EncodeToString(certPEM),
				ClientKeyData:         base64.StdEncoding.EncodeToString(keyPEM),
			},
			wantAuth: "absent",
			wantCN:   "cert-user",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()

			var logs bytes.Buffer
			if c.debug {
				t.Setenv("DEBUG", "true")
			} else {
				t.Setenv("DEBUG", "false")
			}
			logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(logger), xcontext.WithUserConfig(c.ep))
			res := Get(ctx, ref)

			if c.debug {
				// The instrument must be able to say yes: the DEBUG round tripper ran and logged.
				if !strings.Contains(logs.String(), `"msg":"kubeconfig: request"`) {
					t.Errorf("PRECONDITION: DEBUG=true must install plumbing's kubeconfig request log; no such line was logged")
				}
			}
			if strings.Contains(logs.String(), token) {
				t.Errorf("the endpoint's bearer token appears in the log (%d bytes logged); it must never be logged", logs.Len())
			}

			mu.Lock()
			got := append([]seenRequest(nil), seen...)
			mu.Unlock()

			var discovery, reads int
			for _, s := range got {
				if s.auth != c.wantAuth || s.clientCN != c.wantCN {
					t.Errorf("%s: Authorization=%s client-cert-CN=%q, want Authorization=%s client-cert-CN=%q",
						s.path, s.auth, s.clientCN, c.wantAuth, c.wantCN)
				}
				switch {
				case s.path == "/api/v1/namespaces/ns/configmaps/x":
					reads++
				case s.discovery():
					discovery++
				}
			}
			if discovery == 0 {
				t.Errorf("no discovery request reached the apiserver (saw %d requests): the arm must cover the RESTMapper's discovery", len(got))
			}
			if reads != 1 {
				t.Errorf("object GETs = %d, want 1", reads)
			}
			if res.Err != nil || res.Unstructured == nil {
				t.Fatalf("the read must succeed under the endpoint's own credential, got err=%v", res.Err)
			}
			if res.Unstructured.GetName() != "x" {
				t.Fatalf("read the wrong object: %q", res.Unstructured.GetName())
			}
		})
	}
}

// classifyAuth names what an Authorization header holds without returning any part of it.
func classifyAuth(h, token string) string {
	switch {
	case h == "":
		return "absent"
	case h == "Bearer "+token:
		return "bearer-match"
	case strings.HasPrefix(h, "Bearer "):
		return "bearer-other"
	default:
		return "other-scheme"
	}
}

func randomToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func newTestCA(t *testing.T, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func newClientCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
}
