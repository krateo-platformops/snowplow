//go:build unit
// +build unit

package api

// endpoint_reuse_1088_test.go — #1088 stage-level arms: the real branch-E fetch
// (httpFetchAllowingNonJSON → httpClientForEndpoint → credhttp.Send) reuses one
// client-go client per endpoint identity, never shares a connection or a
// credential across identities, and never re-sends a write.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// ARM 1 + ARM 3: 100 fetches by one identity — the per-user cert-auth
// clientconfig dialling the apiserver host, held-credential marked as a live
// stage is — open ONE connection, over HTTP/2, sequentially and concurrently.
func TestReuse1088_SameIdentity100Fetches_OneConnection(t *testing.T) {
	p := newPKI1088(t)
	cs := newConnServer1088(t, p, true)
	ep := certEndpoint1088(t, p, cs.srv.URL, "user-a")
	ctx, dials := dialCounting1088(credhttp.WithHeldCredential(context.Background()))
	for i := 0; i < 50; i++ {
		if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := cs.reqs.Load(); got != 100 {
		t.Fatalf("server saw %d requests, want 100", got)
	}
	if c, d := cs.connections(), dials.Load(); c != 1 || d != 1 {
		t.Fatalf("100 fetches by one identity opened %d connections (%d dials), want 1", c, d)
	}
	if h2 := cs.h2.Load(); h2 != 100 {
		t.Fatalf("%d of 100 requests were HTTP/2, want all", h2)
	}
}

// ARM 3: the apiserver-host stage's client is built on client-go's transport,
// never plumbing's per-fetch one. A kept client must have what client-go's
// transport gives and plumbing's does not, and each property is read from what
// the client DOES on the wire, never from transport fields: on Go 1.27 x/net's
// HTTP/2 wraps net/http, so the old TLSNextProto["h2"] read no longer says
// whether HTTP/2 and its health check are on (#847).
//
//   - the client certificate is presented and HTTP/2 is negotiated (the server
//     requires the cert and counts HTTP/2 requests);
//   - the HTTP/2 health check closes a connection whose peer went silent, and
//     the next fetch opens a new one. The control arm turns the check off and
//     the same silent connection stays open for the whole window, so a close
//     is the check's doing and not something else's;
//   - client-go's per-host idle bound (25; net/http's default, which plumbing
//     keeps, is 2): 25 concurrent HTTP/1 fetches leave 25 connections parked,
//     and 25 more reuse all of them.
func TestReuse1088_ApiserverStage_BuiltOnClientGoTransport(t *testing.T) {
	p := newPKI1088(t)
	ep := certEndpoint1088(t, p, "https://kubernetes.default.svc", "user-a")
	if !endpointOwnedShape(ep) {
		t.Fatal("the cert-auth clientconfig is not owned: it would stay on plumbing's per-fetch transport")
	}

	// window bounds both health-check arms alike: the check closes within it
	// (expected ~2 s: 1 s read-idle, then 1 s for the unanswered ping), and with
	// the check off the connection must outlive it.
	const window = 4 * time.Second
	silentPeer := func(t *testing.T, readIdle string) (closedAfter time.Duration, closed bool) {
		t.Helper()
		t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", readIdle)
		t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "1")
		cs := newConnServer1088(t, p, true)
		px := newSilencer1088(t, cs.srv.Listener.Addr().String())
		// The server cert names 127.0.0.1, which the proxy also listens on.
		ep := certEndpoint1088(t, p, "https://"+px.addr(), "user-silent-"+readIdle)
		ctx, dials := dialCounting1088(credhttp.WithHeldCredential(context.Background()))
		if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
		if cs.h2.Load() != 1 {
			t.Fatal("the fetch was not HTTP/2")
		}
		px.silenceAll()
		start := time.Now()
		select {
		case <-px.clientClosed:
			closedAfter, closed = time.Since(start), true
		case <-time.After(window):
		}
		if closed {
			// The dead connection is not reused: the next fetch dials anew.
			if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
				t.Fatalf("fetch after the health-check close: %v", err)
			}
			if d := dials.Load(); d != 2 {
				t.Fatalf("the fetch after the close dialled %d times in all, want 2", d)
			}
		}
		return closedAfter, closed
	}
	t.Run("HTTP/2 health check closes a silent connection", func(t *testing.T) {
		after, closed := silentPeer(t, "1")
		if !closed {
			t.Fatalf("a silent peer's connection was still open after %v: client-go's HTTP/2 health check is not running", window)
		}
		t.Logf("closed %v after the peer went silent", after)
	})
	t.Run("control: with the health check off the silent connection stays open", func(t *testing.T) {
		if after, closed := silentPeer(t, "0"); closed {
			t.Fatalf("the connection closed %v after the peer went silent with the health check OFF: the arm above does not measure the check", after)
		}
	})

	t.Run("client-go's per-host idle bound keeps 25 connections", func(t *testing.T) {
		const n = 25
		srv, conns, round := newBarrierServer1088(t, p, n)
		ep := certEndpoint1088(t, p, srv.URL, "user-idle")
		ctx := credhttp.WithHeldCredential(context.Background())
		concurrent := func() {
			t.Helper()
			round()
			var wg sync.WaitGroup
			errs := make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
						errs <- err
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
		}
		// Every fetch reads its body to EOF, and net/http returns that EOF only
		// once the connection is back in the idle pool (or closed, past the
		// bound), so the second round starts on a settled pool.
		concurrent()
		if c := conns.Load(); c != n {
			t.Fatalf("%d concurrent fetches held open %d connections, want %d", n, c, n)
		}
		concurrent()
		if c := conns.Load(); c != n {
			t.Fatalf("a second round of %d concurrent fetches opened %d new connections, want 0: the idle bound is below client-go's %d", n, c-n, n)
		}
	})
}

// silencer1088 is a TCP proxy in front of a server. silenceAll makes every
// connection it carries go silent in both directions while staying open, like
// a peer that died without a FIN; connections it accepts later forward
// normally. clientClosed receives once per connection the client closed.
type silencer1088 struct {
	ln           net.Listener
	mu           sync.Mutex
	silent       []*atomic.Bool
	clientClosed chan struct{}
}

func newSilencer1088(t *testing.T, target string) *silencer1088 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &silencer1088{ln: ln, clientClosed: make(chan struct{}, 16)}
	var open sync.WaitGroup
	t.Cleanup(func() { _ = ln.Close(); open.Wait() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			silent := new(atomic.Bool)
			s.mu.Lock()
			s.silent = append(s.silent, silent)
			s.mu.Unlock()
			pump := func(dst, src net.Conn) error {
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if n > 0 && !silent.Load() {
						if _, werr := dst.Write(buf[:n]); werr != nil {
							return werr
						}
					}
					if err != nil {
						return err
					}
				}
			}
			open.Add(2)
			go func() { defer open.Done(); _ = pump(c, up); _ = c.Close() }()
			go func() {
				defer open.Done()
				_ = pump(up, c)
				s.clientClosed <- struct{}{}
				_ = up.Close()
			}()
			// The test's cleanup closes the listener; close the pair too.
			go func() { <-t.Context().Done(); _ = c.Close(); _ = up.Close() }()
		}
	}()
	return s
}

func (s *silencer1088) addr() string { return s.ln.Addr().String() }

func (s *silencer1088) silenceAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.silent {
		b.Store(true)
	}
}

// newBarrierServer1088 is an HTTP/1-only TLS server requiring a client cert.
// Each round (started by calling round) holds every request until n are in
// flight at once, so a round of n concurrent fetches needs n connections.
// conns counts the connections the server accepted.
func newBarrierServer1088(t *testing.T, p *pki1088, n int) (*httptest.Server, *atomic.Int32, func()) {
	t.Helper()
	var (
		mu      sync.Mutex
		arrived int
		release chan struct{}
	)
	conns := new(atomic.Int32)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		rel := release
		arrived++
		if arrived == n {
			close(rel)
		}
		mu.Unlock()
		select {
		case <-rel:
		case <-time.After(10 * time.Second): // the fetch fails the round's count, it does not hang
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","data":{"k":"v"}}`))
	}))
	certPEM, keyPEM := p.issue(t, "server", true)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(p.caCert)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		NextProtos:   []string{"http/1.1"},
	}
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	round := func() {
		mu.Lock()
		arrived, release = 0, make(chan struct{})
		mu.Unlock()
	}
	return srv, conns, round
}

// ARM 2: two identities never share a connection or a credential — two client
// certs on one CA, and two bearer tokens on one CA.
func TestReuse1088_TwoIdentities_NeverShareConnectionOrCredential(t *testing.T) {
	p := newPKI1088(t)
	t.Run("client certs", func(t *testing.T) {
		cs := newConnServer1088(t, p, true)
		a := certEndpoint1088(t, p, cs.srv.URL, "user-a")
		b := certEndpoint1088(t, p, cs.srv.URL, "user-b")
		ctx := credhttp.WithHeldCredential(context.Background())
		for i := 0; i < 20; i++ {
			for _, ep := range []*endpoints.Endpoint{a, b} {
				if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
					t.Fatal(err)
				}
			}
		}
		assertOneIdentityPerConn(t, cs, 2)
	})
	t.Run("bearer tokens", func(t *testing.T) {
		cs := newConnServer1088(t, p, false)
		ca := base64.StdEncoding.EncodeToString(p.caPEM)
		a := &endpoints.Endpoint{ServerURL: cs.srv.URL, CertificateAuthorityData: ca, Token: "token-a"}
		b := &endpoints.Endpoint{ServerURL: cs.srv.URL, CertificateAuthorityData: ca, Token: "token-b"}
		ctx := credhttp.WithHeldCredential(context.Background())
		for i := 0; i < 20; i++ {
			for _, ep := range []*endpoints.Endpoint{a, b} {
				if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
					t.Fatal(err)
				}
			}
		}
		assertOneIdentityPerConn(t, cs, 2)
	})
}

func assertOneIdentityPerConn(t *testing.T, cs *connServer1088, wantConns int) {
	t.Helper()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.conns != wantConns {
		t.Fatalf("%d connections for %d identities, want one each", cs.conns, wantConns)
	}
	for addr, ids := range cs.ids {
		if len(ids) != 1 {
			t.Fatalf("connection %s carried %d identities, want exactly 1", addr, len(ids))
		}
	}
}

// ARM 4: a genuinely external endpoint (a named endpointRef: token-auth, no
// CA, no held mark) reuses its transport across fetches.
func TestReuse1088_ExternalEndpoint_ReusesTransport(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	ep := &endpoints.Endpoint{ServerURL: srv.URL, Token: "third-party-token"}
	ctx, dials := dialCounting1088(context.Background())
	for i := 0; i < 50; i++ {
		if err := fetch1088(t, ctx, ep, http.MethodGet); err != nil {
			t.Fatal(err)
		}
	}
	if c, d := conns.Load(), dials.Load(); c != 1 || d != 1 {
		t.Fatalf("50 fetches to one external endpoint opened %d connections (%d dials), want 1", c, d)
	}
}

// ARM 6 (stage level): a POST, PUT or DELETE stage is never re-sent after a
// network error once its request may have been written. The server reads the
// whole request, then drops the connection.
func TestWrite1088_StageNotResentAfterRequestWritten(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "3")
	t.Setenv("CLIENT_BASE_BACKOFF", "2ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "4ms")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var received atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				_, _ = io.ReadAll(r.Body)
				received.Add(1)
			}(c)
		}
	}()
	ep := endpoints.Endpoint{ServerURL: "http://" + ln.Addr().String(), Token: "t"}
	for _, tc := range []struct{ verb, payload string }{
		{http.MethodPost, `{"a":1}`}, {http.MethodPut, `{"a":1}`}, {http.MethodDelete, ""},
	} {
		before := received.Load()
		cp := ep
		_, _, _, ferr := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
			RequestInfo: httpcall.RequestInfo{Path: "/x", Verb: ptr.To(tc.verb), Payload: ptr.To(tc.payload)},
			Endpoint:    &cp,
		})
		if ferr == nil || !strings.Contains(ferr.Error(), "EOF") {
			t.Fatalf("%s: want the dropped connection's EOF, got %v", tc.verb, ferr)
		}
		time.Sleep(50 * time.Millisecond)
		if got := received.Load() - before; got != 1 {
			t.Fatalf("%s stage reached the server %d times, want exactly 1", tc.verb, got)
		}
	}
}
