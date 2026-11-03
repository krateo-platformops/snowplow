package api

// endpoint_reuse_1088_helpers_test.go — #1088 test fixtures shared by the unit
// arms (endpoint_reuse_1088_test.go) and the #267 falsifier arm
// (sa_dial_tokenfile_falsifier_268_267_test.go), so it carries no build tag.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// pki1088 is a CA, a server cert for 127.0.0.1 and client certs it signs.
type pki1088 struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
}

func newPKI1088(t *testing.T) *pki1088 {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &pki1088{caCert: cert, caKey: key, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (p *pki1088) issue(t *testing.T, cn string, server bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
}

// connServer1088 is an HTTP/2 TLS server that records, per connection, every
// client identity (client-cert CN, else the Authorization header) it carried.
type connServer1088 struct {
	srv   *httptest.Server
	mu    sync.Mutex
	conns int
	ids   map[string]map[string]bool // remote addr → identities seen on it
	h2    atomic.Int32
	reqs  atomic.Int32
}

func newConnServer1088(t *testing.T, p *pki1088, requireClientCert bool) *connServer1088 {
	t.Helper()
	cs := &connServer1088{ids: map[string]map[string]bool{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.reqs.Add(1)
		if r.ProtoMajor == 2 {
			cs.h2.Add(1)
		}
		id := r.Header.Get("Authorization")
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			id = "cn:" + r.TLS.PeerCertificates[0].Subject.CommonName
		}
		cs.mu.Lock()
		if cs.ids[r.RemoteAddr] == nil {
			cs.ids[r.RemoteAddr] = map[string]bool{}
		}
		cs.ids[r.RemoteAddr][id] = true
		cs.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","data":{"k":"v"}}`))
	}))
	srv.EnableHTTP2 = true
	certPEM, keyPEM := p.issue(t, "server", true)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(p.caCert)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	if requireClientCert {
		srv.TLS.ClientAuth = tls.RequireAndVerifyClientCert
		srv.TLS.ClientCAs = pool
	}
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			cs.mu.Lock()
			cs.conns++
			cs.mu.Unlock()
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cs.srv = srv
	return cs
}

func (cs *connServer1088) connections() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.conns
}

// certEndpoint1088 is the live `<user>-clientconfig` shape: cert-auth, every
// datum single-base64, as endpoints.FromSecret + normalizeCAData hand it over.
func certEndpoint1088(t *testing.T, p *pki1088, url, cn string) *endpoints.Endpoint {
	certPEM, keyPEM := p.issue(t, cn, false)
	return &endpoints.Endpoint{
		ServerURL:                url,
		CertificateAuthorityData: base64.StdEncoding.EncodeToString(p.caPEM),
		ClientCertificateData:    base64.StdEncoding.EncodeToString(certPEM),
		ClientKeyData:            base64.StdEncoding.EncodeToString(keyPEM),
	}
}

// dialCounting1088 returns a ctx whose httptrace counts every connect the
// client starts.
func dialCounting1088(ctx context.Context) (context.Context, *atomic.Int32) {
	n := new(atomic.Int32)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{ConnectStart: func(string, string) { n.Add(1) }}), n
}

func fetch1088(t *testing.T, ctx context.Context, ep *endpoints.Endpoint, verb string) error {
	t.Helper()
	cp := *ep
	st, _, _, err := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/tenant-trial", Verb: ptr.To(verb)},
		Endpoint:    &cp,
	})
	if err != nil {
		return err
	}
	if st.Code != http.StatusOK {
		t.Fatalf("fetch answered %d: %s", st.Code, st.Message)
	}
	return nil
}
