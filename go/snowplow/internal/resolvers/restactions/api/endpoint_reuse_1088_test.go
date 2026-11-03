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
	utilnet "k8s.io/apimachinery/pkg/util/net"
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

// ARM 3: the apiserver-host stage's client is built on client-go's transport
// (its wrapper chain reaches client-go's *http.Transport, with client-go's
// per-host idle bound), never plumbing's per-fetch one.
func TestReuse1088_ApiserverStage_BuiltOnClientGoTransport(t *testing.T) {
	p := newPKI1088(t)
	ep := certEndpoint1088(t, p, "https://kubernetes.default.svc", "user-a")
	if !endpointOwnedShape(ep) {
		t.Fatal("the cert-auth clientconfig is not owned: it would stay on plumbing's per-fetch transport")
	}
	cli, err := httpClientForEndpoint(credhttp.WithHeldCredential(context.Background()), ep, &httpcall.RequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	rt := cli.Transport
	for {
		if _, ok := rt.(*http.Transport); ok {
			break
		}
		w, ok := rt.(utilnet.RoundTripperWrapper)
		if !ok {
			t.Fatalf("the chain stops at %T, not client-go's *http.Transport (plumbing's wrappers do not unwrap)", rt)
		}
		rt = w.WrappedRoundTripper()
	}
	tr := rt.(*http.Transport)
	// client-go's tlsCache transport: MaxIdleConnsPerHost 25 (plumbing leaves
	// net/http's 2), client cert installed, HTTP/2 configured.
	if tr.MaxIdleConnsPerHost != 25 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want client-go's 25", tr.MaxIdleConnsPerHost)
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.GetClientCertificate == nil && len(tr.TLSClientConfig.Certificates) == 0 {
		t.Fatal("the client certificate is not installed on the transport")
	}
	if _, ok := tr.TLSNextProto["h2"]; !ok {
		t.Fatal("HTTP/2 (with client-go's health check) is not configured")
	}
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
