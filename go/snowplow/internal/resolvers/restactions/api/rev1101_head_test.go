//go:build unit

package api

// Reviewer arms for PR #1101, head only.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
)

func tlsPair(c, k []byte) (*tls.Config, error) {
	pair, err := tls.X509KeyPair(c, k)
	return &tls.Config{Certificates: []tls.Certificate{pair}}, err
}

func poolOf(p *revPKI) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(p.caCert)
	return pool
}

func poolFromPEM(b []byte) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(b)
	return pool
}

func pemDecode(c []byte) (string, error) {
	blk, _ := pem.Decode(c)
	crt, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return "", err
	}
	return crt.SerialNumber.String(), nil
}

// ---------- 1. key derivation fuzz ----------

func TestRev1101_KeyFuzz(t *testing.T) {
	base := endpoints.Endpoint{ServerURL: "https://k", ProxyURL: "http://p", CertificateAuthorityData: "CA",
		ClientCertificateData: "CERT", ClientKeyData: "KEY", Token: "T", Username: "U", Password: "P",
		AwsAccessKey: "a", AwsSecretKey: "s", AwsRegion: "r", AwsService: "sv", AwsTime: "tm"}
	k0 := credhttp.EndpointKeyOf(&base, "", "")
	v := reflect.ValueOf(&base).Elem()
	for i := 0; i < v.NumField(); i++ {
		cp := base
		f := reflect.ValueOf(&cp).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "x")
		case reflect.Bool:
			f.SetBool(!f.Bool())
		default:
			t.Fatalf("unhandled kind %s", f.Kind())
		}
		if credhttp.EndpointKeyOf(&cp, "", "") == k0 {
			t.Errorf("field %s change did not change the key", v.Type().Field(i).Name)
		}
	}
	// identical CA, different tokens
	a := endpoints.Endpoint{ServerURL: "https://k", CertificateAuthorityData: "CA", Token: "t1"}
	b := a
	b.Token = "t2"
	if credhttp.EndpointKeyOf(&a, "", "") == credhttp.EndpointKeyOf(&b, "", "") {
		t.Error("same CA, different tokens: same key")
	}
	// identical cert, different key
	c := endpoints.Endpoint{ServerURL: "https://k", CertificateAuthorityData: "CA", ClientCertificateData: "C", ClientKeyData: "K1"}
	d := c
	d.ClientKeyData = "K2"
	if credhttp.EndpointKeyOf(&c, "", "") == credhttp.EndpointKeyOf(&d, "", "") {
		t.Error("same cert, different key: same key")
	}
	// field-boundary shift
	e1 := endpoints.Endpoint{Token: "ab", Username: ""}
	e2 := endpoints.Endpoint{Token: "a", Username: "b"}
	if credhttp.EndpointKeyOf(&e1) == credhttp.EndpointKeyOf(&e2) {
		t.Error("boundary shift Token/Username collides")
	}
	// provenance position
	if credhttp.EndpointKeyOf(&a, "held", "") == credhttp.EndpointKeyOf(&a, "", "held") {
		t.Error("provenance positions collide")
	}
	// Token vs token-file (SA dial) on the same endpoint
	if credhttp.EndpointKeyOf(&a, "", "") == credhttp.EndpointKeyOf(&a, "", "sa-token-file:/var/run/x") {
		t.Error("SA dial and user dial of identical endpoint share a key")
	}
	if credhttp.EndpointKeyOf(&a, "held", "") == credhttp.EndpointKeyOf(&a, "", "") {
		t.Error("held vs not held share a key")
	}
	// nil vs zero
	var z endpoints.Endpoint
	if credhttp.EndpointKeyOf(nil) != credhttp.EndpointKeyOf(&z) {
		t.Error("nil and zero endpoint differ (documented equal)")
	}
	// random fuzz: 20000 random endpoints over a small alphabet, collisions only for equal structs
	rng := rand.New(rand.NewSource(1))
	seen := map[credhttp.EndpointKey]endpoints.Endpoint{}
	alpha := []string{"", "a", "b", "ab", "ba", "\x00", "a\x00"}
	for i := 0; i < 20000; i++ {
		var e endpoints.Endpoint
		ev := reflect.ValueOf(&e).Elem()
		for j := 0; j < ev.NumField(); j++ {
			f := ev.Field(j)
			if f.Kind() == reflect.String {
				f.SetString(alpha[rng.Intn(len(alpha))])
			} else {
				f.SetBool(rng.Intn(2) == 1)
			}
		}
		prov := []string{alpha[rng.Intn(len(alpha))], alpha[rng.Intn(len(alpha))]}
		k := credhttp.EndpointKeyOf(&e, prov...)
		if prev, ok := seen[k]; ok && prev != e {
			t.Fatalf("collision between distinct endpoints")
		}
		seen[k] = e
	}
}

// ---------- shared server: echoes the identity it saw in the body ----------

type revEcho struct {
	srv   *httptest.Server
	mu    sync.Mutex
	conns map[string]http.ConnState
	opens atomic.Int32
}

func newRevEcho(t *testing.T, p *revPKI) *revEcho {
	e := &revEcho{conns: map[string]http.ConnState{}}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "auth=" + r.Header.Get("Authorization")
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			id += ";cn=" + r.TLS.PeerCertificates[0].Subject.CommonName + ";serial=" + r.TLS.PeerCertificates[0].SerialNumber.String()
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"kind":"X","id":%q}`, id)
	}))
	srv.EnableHTTP2 = true
	c, k := p.issue(t, "server", true)
	pair, _ := tlsPair(c, k)
	srv.TLS = pair
	srv.TLS.ClientAuth = 3 // VerifyClientCertIfGiven
	srv.TLS.ClientCAs = poolOf(p)
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		e.mu.Lock()
		e.conns[c.RemoteAddr().String()] = s
		e.mu.Unlock()
		if s == http.StateNew {
			e.opens.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	e.srv = srv
	return e
}

func (e *revEcho) closed() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, s := range e.conns {
		if s == http.StateClosed {
			n++
		}
	}
	return n
}

func revGetID(t *testing.T, ctx context.Context, ep endpoints.Endpoint, verb string) string {
	t.Helper()
	st, body, _, err := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/x", Verb: ptr.To(verb)},
		Endpoint:    &ep,
	})
	if err != nil {
		return "ERR:" + err.Error()
	}
	if st != nil && st.Code >= 300 {
		return fmt.Sprintf("STATUS:%d %s", st.Code, st.Message)
	}
	s := string(body)
	i := strings.Index(s, `"id":"`)
	if i < 0 {
		return "BODY:" + s
	}
	return strings.TrimSuffix(strings.TrimSuffix(s[i+6:], "}"), `"`)
}

func certEP(t *testing.T, p *revPKI, url, cn string) (endpoints.Endpoint, string) {
	c, k := p.issue(t, cn, false)
	b64 := base64.StdEncoding.EncodeToString
	blk, _ := pemDecode(c)
	return endpoints.Endpoint{ServerURL: url, CertificateAuthorityData: b64(p.caPEM), ClientCertificateData: b64(c), ClientKeyData: b64(k)}, blk
}

// ---------- 2. request-level crossing under concurrency ----------

func TestRev1101_NoCrossingUnderConcurrency(t *testing.T) {
	p := newRevPKI(t)
	e := newRevEcho(t, p)
	held := credhttp.WithHeldCredential(context.Background())
	type id struct {
		ep   endpoints.Endpoint
		ctx  context.Context
		want string
	}
	var ids []id
	for i := 0; i < 8; i++ {
		ep, serial := certEP(t, p, e.srv.URL, fmt.Sprintf("user-%d", i))
		ids = append(ids, id{ep, held, fmt.Sprintf("auth=;cn=user-%d;serial=%s", i, serial)})
	}
	for i := 0; i < 8; i++ { // bearer tokens on one CA
		ep := endpoints.Endpoint{ServerURL: e.srv.URL, CertificateAuthorityData: string(p.caPEM), Token: fmt.Sprintf("tok-%d", i)}
		ids = append(ids, id{ep, held, fmt.Sprintf("auth=Bearer tok-%d", i)})
	}
	var wg sync.WaitGroup
	var bad atomic.Int32
	for r := 0; r < 25; r++ {
		for _, x := range ids {
			wg.Add(1)
			go func(x id) {
				defer wg.Done()
				if got := revGetID(t, x.ctx, x.ep, "GET"); got != x.want {
					if bad.Add(1) < 5 {
						t.Errorf("crossing: want %q got %q", x.want, got)
					}
				}
			}(x)
		}
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	t.Logf("COLD BURST: %d identities x 25 concurrent cold fetches opened %d connections, %d already closed; crossings=%d", len(ids), e.opens.Load(), e.closed(), bad.Load())
	// warm burst: same again, nothing new should dial
	o0 := e.opens.Load()
	for r := 0; r < 25; r++ {
		for _, x := range ids {
			wg.Add(1)
			go func(x id) {
				defer wg.Done()
				if got := revGetID(t, x.ctx, x.ep, "GET"); got != x.want {
					bad.Add(1)
				}
			}(x)
		}
	}
	wg.Wait()
	t.Logf("WARM BURST: new connections=%d crossings=%d", e.opens.Load()-o0, bad.Load())
}

// ---------- 3. SA token file never on a user / cert dial ----------

func TestRev1101_SATokenFileNeverOnUserDial(t *testing.T) {
	p := newRevPKI(t)
	e := newRevEcho(t, p)
	tokFile := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(tokFile, []byte("sa-file-token"), 0o600)
	defer dynamic.SetServiceAccountTokenPathForTest(tokFile)()
	// a user token-auth clientconfig field-identical to the SA endpoint
	ep := endpoints.Endpoint{ServerURL: e.srv.URL, CertificateAuthorityData: string(p.caPEM), Token: "boot-token"}
	sa := cache.WithServiceAccountDial(context.Background())
	held := credhttp.WithHeldCredential(context.Background())
	if got := revGetID(t, sa, ep, "GET"); got != "auth=Bearer sa-file-token" {
		t.Fatalf("SA dial presented %q", got)
	}
	for i := 0; i < 20; i++ {
		if got := revGetID(t, held, ep, "GET"); got != "auth=Bearer boot-token" {
			t.Fatalf("user dial of an SA-identical endpoint presented %q (token file leaked)", got)
		}
		if got := revGetID(t, context.Background(), ep, "GET"); got != "auth=Bearer boot-token" {
			t.Fatalf("unmarked dial presented %q", got)
		}
	}
	// cert-auth user with a token too, after an SA dial
	cep, serial := certEP(t, p, e.srv.URL, "cert-user")
	cep.Token = "cert-user-token"
	_ = revGetID(t, sa, cep, "GET")
	if got := revGetID(t, held, cep, "GET"); got != "auth=Bearer cert-user-token;cn=cert-user;serial="+serial {
		t.Fatalf("cert-auth user dial presented %q", got)
	}
	// cert-auth SA-marked, no token: no bearer at all
	cep2, serial2 := certEP(t, p, e.srv.URL, "cert-sa")
	if got := revGetID(t, sa, cep2, "GET"); got != "auth=;cn=cert-sa;serial="+serial2 {
		t.Fatalf("cert-auth SA-marked tokenless dial presented %q", got)
	}
	// rotate the file: SA dial follows, user dial never
	_ = os.WriteFile(tokFile, []byte("sa-file-token-2"), 0o600)
	if got := revGetID(t, held, ep, "GET"); got != "auth=Bearer boot-token" {
		t.Fatalf("after file rotation user dial presented %q", got)
	}
}

// ---------- 4. rotation + eviction ----------

func TestRev1101_RotationAndEviction(t *testing.T) {
	p := newRevPKI(t)
	e := newRevEcho(t, p)
	var mu sync.Mutex
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	advance := func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }
	orig := stageEndpointClients
	stageEndpointClients = credhttp.NewEndpointClientsForTest(90*time.Second, clock)
	defer func() { stageEndpointClients = orig }()
	held := credhttp.WithHeldCredential(context.Background())

	ep1, s1 := certEP(t, p, e.srv.URL, "user-a")
	for i := 0; i < 5; i++ {
		if got := revGetID(t, held, ep1, "GET"); !strings.HasSuffix(got, "serial="+s1) {
			t.Fatalf("pre-rotation got %q", got)
		}
	}
	// rotate: same user, new cert/key
	ep2, s2 := certEP(t, p, e.srv.URL, "user-a")
	for i := 0; i < 20; i++ {
		if got := revGetID(t, held, ep2, "GET"); !strings.HasSuffix(got, "serial="+s2) {
			t.Fatalf("post-rotation fetch %d served by old credential: %q", i, got)
		}
	}
	// token rotation
	tk1 := endpoints.Endpoint{ServerURL: e.srv.URL, CertificateAuthorityData: string(p.caPEM), Token: "old"}
	tk2 := tk1
	tk2.Token = "new"
	_ = revGetID(t, held, tk1, "GET")
	if got := revGetID(t, held, tk2, "GET"); got != "auth=Bearer new" {
		t.Fatalf("token rotation: %q", got)
	}
	if n := stageEndpointClients.Len(); n != 4 {
		t.Fatalf("entries=%d want 4", n)
	}
	// keep ep2 and tk2 hot, let ep1 and tk1 age out
	advance(60 * time.Second)
	_ = revGetID(t, held, ep2, "GET")
	_ = revGetID(t, held, tk2, "GET")
	advance(31 * time.Second) // ep1/tk1 unused 91 s; sweep allowed (91 s since last sweep)
	_ = revGetID(t, held, ep2, "GET")
	if n, ev := stageEndpointClients.Len(), stageEndpointClients.Evictions(); n != 2 || ev != 2 {
		t.Fatalf("after idle: entries=%d evictions=%d, want 2/2", n, ev)
	}
	deadline := time.Now().Add(3 * time.Second)
	for e.closed() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if c := e.closed(); c < 2 {
		t.Fatalf("B2: the sweep evicted 2 entries but the server saw %d of their 2 idle connections close", c)
	}
	t.Logf("rotation: new credential on every post-rotation fetch; evicted=%d closed conns=%d opens=%d", stageEndpointClients.Evictions(), e.closed(), e.opens.Load())
}

// ---------- 5. memory / goroutines per cached client ----------

// TestRev1101_ServerHelper is the out-of-process server for the memory arm.
func TestRev1101_ServerHelper(t *testing.T) {
	if os.Getenv("REV_SERVER_DIR") == "" {
		t.Skip("helper")
	}
	dir := os.Getenv("REV_SERVER_DIR")
	caPEM, _ := os.ReadFile(filepath.Join(dir, "ca.pem"))
	crt, _ := os.ReadFile(filepath.Join(dir, "srv.pem"))
	key, _ := os.ReadFile(filepath.Join(dir, "srv.key"))
	cfg, _ := tlsPair(crt, key)
	cfg.ClientAuth = 3
	cfg.ClientCAs = poolFromPEM(caPEM)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"X","id":"ok"}`))
	}))
	srv.EnableHTTP2 = true
	srv.TLS = cfg
	srv.StartTLS()
	fmt.Println("REVURL " + srv.URL)
	select {}
}

func TestRev1101_MemoryPerClient(t *testing.T) {
	p := newRevPKI(t)
	dir := t.TempDir()
	c, k := p.issue(t, "server", true)
	_ = os.WriteFile(filepath.Join(dir, "ca.pem"), p.caPEM, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "srv.pem"), c, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "srv.key"), k, 0o600)
	cmd := exec.Command(os.Args[0], "-test.run=^TestRev1101_ServerHelper$", "-test.timeout=10m")
	cmd.Env = append(os.Environ(), "REV_SERVER_DIR="+dir)
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	sc := bufio.NewScanner(out)
	url := ""
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "REVURL ") {
			url = strings.TrimPrefix(sc.Text(), "REVURL ")
			break
		}
	}
	go func() { _, _ = io.Copy(io.Discard, out) }()
	if url == "" {
		t.Fatal("no server")
	}
	orig := stageEndpointClients
	stageEndpointClients = credhttp.NewEndpointClientsForTest(time.Hour, time.Now)
	defer func() { stageEndpointClients = orig }()
	held := credhttp.WithHeldCredential(context.Background())
	const N = 400
	eps := make([]endpoints.Endpoint, N)
	for i := range eps {
		eps[i], _ = certEP(t, p, url, fmt.Sprintf("u%d", i))
	}
	measure := func() (uint64, int) {
		for i := 0; i < 3; i++ {
			runtime.GC()
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapInuse, runtime.NumGoroutine()
	}
	// warm one identity so lazy package-level state is paid outside the window
	w, _ := certEP(t, p, url, "warm")
	_ = revGetID(t, held, w, "GET")
	h0, g0 := measure()
	for i := range eps {
		if got := revGetID(t, held, eps[i], "GET"); got != "ok" {
			t.Fatalf("fetch: %s", got)
		}
	}
	h1, g1 := measure()
	t.Logf("CONNECTED: N=%d heap_inuse_delta=%d B (%.1f KiB/client) goroutines_delta=%d (%.2f/client) entries=%d",
		N, h1-h0, float64(h1-h0)/N/1024, g1-g0, float64(g1-g0)/N, stageEndpointClients.Len())
	// idle: close every idle connection, keep clients
	stageEndpointClients = credhttp.NewEndpointClientsForTest(time.Hour, time.Now)
	runtime.KeepAlive(eps)
	h2, g2 := measure()
	t.Logf("AFTER DROP: heap_inuse=%d (delta vs base %d) goroutines=%d (delta %d)", h2, int64(h2)-int64(h0), g2, g2-g0)
}

// ---------- 6. HTTP/2 health check on a silently dead kept connection ----------

type revBlackhole struct {
	ln     net.Listener
	n      atomic.Int32
	killed atomic.Bool // the FIRST connection goes silent when set
}

func newRevBlackhole(t *testing.T, upstream string) *revBlackhole {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &revBlackhole{ln: ln}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			idx := b.n.Add(1)
			u, err := net.Dial("tcp", upstream)
			if err != nil {
				c.Close()
				continue
			}
			pipe := func(dst, src net.Conn) {
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if err != nil {
						dst.Close()
						return
					}
					if idx == 1 && b.killed.Load() {
						continue // silently dropped: the peer is dead, TCP is not
					}
					if _, err := dst.Write(buf[:n]); err != nil {
						return
					}
				}
			}
			go pipe(u, c)
			go pipe(c, u)
		}
	}()
	return b
}

func TestRev1101_H2HealthCheckOnDeadKeptConn(t *testing.T) {
	for _, mode := range []struct {
		name     string
		readIdle string
		wantOK   bool
	}{{"healthcheck-1s", "1", true}, {"healthcheck-disabled-control", "0", false}} {
		t.Run(mode.name, func(t *testing.T) {
			t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", mode.readIdle)
			t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "1")
			t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
			t.Setenv("CLIENT_MAX_BACKOFF", "2ms")
			p := newRevPKI(t)
			e := newRevEcho(t, p)
			bh := newRevBlackhole(t, strings.TrimPrefix(e.srv.URL, "https://"))
			ep, serial := certEP(t, p, "https://"+bh.ln.Addr().String(), "user-h2")
			held := credhttp.WithHeldCredential(context.Background())
			if got := revGetID(t, held, ep, "GET"); !strings.HasSuffix(got, serial) {
				t.Fatalf("first fetch: %q", got)
			}
			bh.killed.Store(true)
			ctx, cancel := context.WithTimeout(held, 8*time.Second)
			defer cancel()
			start := time.Now()
			got := revGetID(t, ctx, ep, "GET")
			el := time.Since(start)
			t.Logf("%s: second fetch on dead kept conn took %v, result %.80q, proxy conns=%d", mode.name, el, got, bh.n.Load())
			if mode.wantOK && (!strings.HasSuffix(got, serial) || el > 6*time.Second) {
				t.Fatalf("health check did not rescue the dead connection: %v %q", el, got)
			}
			if !mode.wantOK && strings.HasSuffix(got, serial) && el < 7*time.Second {
				t.Fatalf("control: expected a hang without the health check, got %v %q", el, got)
			}
		})
	}
}

// ---------- 7. the eviction's closeIdle reaches the transport ----------
//
// B2: http.Client.CloseIdleConnections reaches a transport only when the
// outermost RoundTripper implements it, and no layer of a kept client does (it
// closed 0 of 2). The sweep therefore uses utilnet.CloseIdleConnectionsFor,
// which walks the wrapper chain; this arm pins that it closes the kept client's
// idle connection, held and unheld. TestRev1101_RotationAndEviction pins the
// sweep itself.
func TestRev1101_EvictionCloseIdleReachesTransport(t *testing.T) {
	for _, mode := range []string{"utilnet.CloseIdleConnectionsFor"} {
		t.Run(mode, func(t *testing.T) {
			p := newRevPKI(t)
			e := newRevEcho(t, p)
			held := credhttp.WithHeldCredential(context.Background())
			for _, ctx := range []context.Context{held, context.Background()} {
				ep, _ := certEP(t, p, e.srv.URL, "u")
				cli, err := httpClientForEndpoint(ctx, &ep, &httpcall.RequestInfo{})
				if err != nil {
					t.Fatal(err)
				}
				req, _ := http.NewRequest("GET", e.srv.URL+"/x", nil)
				resp, err := cli.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				before := e.closed()
				utilnetCloseIdle(cli.Transport) // what EndpointClients.closeIdle calls
				deadline := time.Now().Add(2 * time.Second)
				for e.closed() == before && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
				t.Logf("%s held=%v: idle conn closed=%v", mode, credhttp.HeldCredential(ctx), e.closed() > before)
				if e.closed() == before {
					t.Errorf("%s did not close the kept client's idle connection (held=%v)", mode, credhttp.HeldCredential(ctx))
				}
			}
		})
	}
}
