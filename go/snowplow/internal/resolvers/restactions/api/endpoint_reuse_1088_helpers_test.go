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
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
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
// client identity it carried: the Authorization header AND the client-cert CN,
// recorded independently ("auth=<header>;cn=<cn>"), so a cert dial that also
// carries a bearer is seen as both (#1101 review B1). It also records, per
// connection, whether it came up and whether the client closed it.
type connServer1088 struct {
	srv   *httptest.Server
	mu    sync.Mutex
	conns map[string]*serverConn1088 // remote addr → the connection
	ids   map[string]map[string]bool // remote addr → identities seen on it
	h2    atomic.Int32
	reqs  atomic.Int32
}

// serverConn1088 is one connection the server accepted.
type serverConn1088 struct {
	// up: the server's side of the TLS handshake completed (it read the
	// client's Finished). That is when an https connection is up for the
	// client too (credhttp connections.go counts opened at TLSHandshakeDone).
	// #1509: a raced setup the client gives up mid-handshake never comes up.
	// The server's VerifyConnection does not mark this: without a client
	// certificate, Go's TLS 1.3 server calls it right after sending its own
	// Finished, before the client has answered (crypto/tls
	// handshake_server_tls13.go: readClientCertificate runs before
	// readClientFinished), so it also counts setups the client abandoned.
	up bool
	// peerClosed: the client closed it (a read saw EOF or a reset before the
	// server closed it).
	peerClosed bool
}

// rawConn1088 is an accepted TCP connection, below TLS, that records the
// client closing it.
type rawConn1088 struct {
	net.Conn
	cs           *connServer1088
	serverClosed atomic.Bool
}

func (c *rawConn1088) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) && !c.serverClosed.Load() {
		c.cs.mu.Lock()
		c.cs.connLocked(c.RemoteAddr().String()).peerClosed = true
		c.cs.mu.Unlock()
	}
	return n, err
}

func (c *rawConn1088) Close() error {
	c.serverClosed.Store(true)
	return c.Conn.Close()
}

// rawListener1088 records every connection the server accepts.
type rawListener1088 struct {
	net.Listener
	cs *connServer1088
}

func (l rawListener1088) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.cs.mu.Lock()
	l.cs.connLocked(c.RemoteAddr().String())
	l.cs.mu.Unlock()
	return &rawConn1088{Conn: c, cs: l.cs}, nil
}

func (cs *connServer1088) connLocked(addr string) *serverConn1088 {
	c := cs.conns[addr]
	if c == nil {
		c = &serverConn1088{}
		cs.conns[addr] = c
	}
	return c
}

func newConnServer1088(t *testing.T, p *pki1088, requireClientCert bool) *connServer1088 {
	t.Helper()
	cs := &connServer1088{conns: map[string]*serverConn1088{}, ids: map[string]map[string]bool{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.reqs.Add(1)
		if r.ProtoMajor == 2 {
			cs.h2.Add(1)
		}
		cn := ""
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		id := "auth=" + r.Header.Get("Authorization") + ";cn=" + cn
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
	// Each connection is marked up when the server's side of its handshake
	// completes (serverConn1088.up). The server runs the handshake when it
	// serves the connection; Handshake here waits for that same handshake (a
	// tls.Conn runs it once, under its handshake lock) and reports its outcome.
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		tc, ok := c.(*tls.Conn)
		if s != http.StateNew || !ok {
			return
		}
		go func() {
			if tc.Handshake() == nil {
				cs.mu.Lock()
				cs.connLocked(tc.RemoteAddr().String()).up = true
				cs.mu.Unlock()
			}
		}()
	}
	srv.Listener = rawListener1088{Listener: srv.Listener, cs: cs}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	cs.srv = srv
	return cs
}

// connections is how many connections came up on the server.
func (cs *connServer1088) connections() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	n := 0
	for _, c := range cs.conns {
		if c.up {
			n++
		}
	}
	return n
}

// stageOpened1088 is the client's count of stage connections opened (#847),
// every protocol: the winner of each setup and every raced loser that came up
// (credhttp noteRacedConnectionUp).
func stageOpened1088() uint64 {
	var n uint64
	for _, c := range credhttp.EgressConnectionsSnapshot() {
		if c.Egress == "stage" {
			n += c.Opened
		}
	}
	return n
}

// assertConnections1088 checks what #1088 is about on the server's record of
// every connection, after the fetches (openedBefore: stageOpened1088 before
// them):
//
//  1. no connection carried requests for two identities (exact);
//  2. each identity's requests were all served on one connection, and
//     wantIdentities identities were served (reuse);
//  3. every other connection carried no request and the client closed it: a
//     raced setup (#1509) that lost. Those that came up are counted by the
//     client as opened, so the client's opened count equals the connections
//     that came up on the server.
//
// A raced loser is closed by the client as soon as another attempt wins, so
// 3 is waited for (up to 5 s; the HTTP/2 server's own preface timeout, 10 s,
// is later, so a close in time is the client's).
func assertConnections1088(t *testing.T, cs *connServer1088, wantIdentities int, openedBefore uint64) {
	t.Helper()
	settled := func() bool {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		up := 0
		for addr, c := range cs.conns {
			if c.up {
				up++
			}
			if cs.ids[addr] == nil && !c.peerClosed {
				return false
			}
		}
		return stageOpened1088()-openedBefore == uint64(up)
	}
	for deadline := time.Now().Add(5 * time.Second); !settled() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	opened := stageOpened1088() - openedBefore
	cs.mu.Lock()
	defer cs.mu.Unlock()
	connsOf := map[string]int{} // identity → connections that carried it
	for addr, ids := range cs.ids {
		if len(ids) != 1 {
			t.Errorf("connection %s carried %d identities, want exactly 1: %v", addr, len(ids), ids)
		}
		for id := range ids {
			connsOf[id]++
		}
	}
	if len(connsOf) != wantIdentities {
		t.Errorf("%d identities were served, want %d", len(connsOf), wantIdentities)
	}
	for id, n := range connsOf {
		if n != 1 {
			t.Errorf("identity %q was served on %d connections, want 1 (its requests share one connection)", id, n)
		}
	}
	up, losers := 0, 0
	for addr, c := range cs.conns {
		if c.up {
			up++
		}
		if cs.ids[addr] != nil {
			continue
		}
		if !c.peerClosed {
			t.Errorf("connection %s carried no request and is still open: a raced setup's loser must be closed", addr)
		}
		if c.up {
			losers++
		}
	}
	if opened != uint64(up) {
		t.Errorf("the client counted %d connections opened; %d came up on the server (%d carried requests, %d were raced losers that came up)",
			opened, up, up-losers, losers)
	}
	if losers > 0 {
		t.Logf("%d raced losers came up and were closed without carrying a request", losers)
	}
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
