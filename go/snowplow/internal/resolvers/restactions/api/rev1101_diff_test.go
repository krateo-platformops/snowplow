//go:build unit

package api

// Reviewer differential for PR #1101 (compiles on base 22fe0579 and on head).
// Writes one JSON observation per scenario to $REV1101_OUT.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type revPKI struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	caPEM  []byte
}

func newRevPKI(t *testing.T) *revPKI {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rev-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &revPKI{c, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (p *revPKI) issue(t *testing.T, cn string, server bool) ([]byte, []byte) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		tmpl.DNSNames = []string{"localhost"}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})
}

func revDigest(s string) string {
	if s == "" {
		return ""
	}
	h := sha256.Sum256([]byte(s))
	return "sha:" + hex.EncodeToString(h[:4])
}

// wire is what the server saw for one request.
type revWire struct {
	Method, Path, Proto, ALPN, TLSVer, ClientCN, Auth string
	BodyLen                                           int
	Headers                                           []string
}

type revRecorder struct {
	mu    sync.Mutex
	wires []revWire
	n     atomic.Int32
}

func (r *revRecorder) record(req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	w := revWire{Method: req.Method, Path: req.URL.RequestURI(), Proto: req.Proto, BodyLen: len(b), Auth: revDigest(req.Header.Get("Authorization"))}
	if req.TLS != nil {
		w.ALPN = req.TLS.NegotiatedProtocol
		w.TLSVer = tls.VersionName(req.TLS.Version)
		if len(req.TLS.PeerCertificates) > 0 {
			w.ClientCN = req.TLS.PeerCertificates[0].Subject.CommonName
		}
	}
	for k, v := range req.Header {
		val := strings.Join(v, ",")
		switch strings.ToLower(k) {
		case "authorization":
			val = revDigest(val)
		case "x-krateo-traceid":
			val = "<set>"
		}
		w.Headers = append(w.Headers, k+"="+val)
	}
	sort.Strings(w.Headers)
	r.mu.Lock()
	r.wires = append(r.wires, w)
	r.mu.Unlock()
	r.n.Add(1)
}

func (r *revRecorder) take() []revWire {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.wires
	r.wires = nil
	return out
}

func revTLSServer(t *testing.T, p *revPKI, mtls bool, h http.HandlerFunc) *httptest.Server {
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	c, k := p.issue(t, "server", true)
	pair, _ := tls.X509KeyPair(c, k)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair}}
	if mtls {
		pool := x509.NewCertPool()
		pool.AddCert(p.caCert)
		srv.TLS.ClientAuth = tls.VerifyClientCertIfGiven
		srv.TLS.ClientCAs = pool
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

type revObs struct {
	Scenario string
	Code     int
	Status   string
	Reason   string
	Message  string
	Err      string
	Wires    []revWire
}

var revOut struct {
	mu  sync.Mutex
	obs []revObs
}

func revEmit(o revObs) {
	if len(o.Message) > 160 {
		o.Message = o.Message[:160]
	}
	revOut.mu.Lock()
	revOut.obs = append(revOut.obs, o)
	revOut.mu.Unlock()
}

func revFlush(t *testing.T) {
	path := os.Getenv("REV1101_OUT")
	if path == "" {
		return
	}
	revOut.mu.Lock()
	defer revOut.mu.Unlock()
	b, _ := json.MarshalIndent(revOut.obs, "", " ")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func revFetch(ctx context.Context, ep endpoints.Endpoint, verb, path string, payload *string) (int, string, string, string, string) {
	st, _, _, err := httpFetchAllowingNonJSON(ctx, httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(verb), Payload: payload},
		Endpoint:    &ep,
	})
	es := ""
	if err != nil {
		es = err.Error()
	}
	if st == nil {
		return 0, "", "", "", es
	}
	return st.Code, st.Status, string(st.Reason), st.Message, es
}

func TestRev1101_Differential(t *testing.T) {
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "2ms")
	defer revFlush(t)
	p := newRevPKI(t)
	rec := &revRecorder{}
	ok := func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"x"}}`))
	}
	srv := revTLSServer(t, p, true, ok)
	plain := httptest.NewServer(http.HandlerFunc(ok))
	defer plain.Close()

	certPEM, keyPEM := p.issue(t, "user-a", false)
	b64 := base64.StdEncoding.EncodeToString
	certEP := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: b64(p.caPEM), ClientCertificateData: b64(certPEM), ClientKeyData: b64(keyPEM)}
	tokCA := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: string(p.caPEM), Token: "user-token-1"}
	tokNoCA := endpoints.Endpoint{ServerURL: plain.URL, Token: "ext-token-1"}
	insecureTok := endpoints.Endpoint{ServerURL: srv.URL, Insecure: true, Token: "ins-token-1"}
	basicUser := endpoints.Endpoint{ServerURL: srv.URL, Insecure: true, Username: "u", Password: "pw"}
	tokUserNoPw := endpoints.Endpoint{ServerURL: plain.URL, Token: "tk-2", Username: "u"}
	certInsecure := certEP
	certInsecure.Insecure = true
	noAuth := endpoints.Endpoint{ServerURL: plain.URL}

	// proxy: an http proxy that answers as the origin.
	prox := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Rev-Proxied-Absolute", r.URL.String())
		ok(w, r)
	}))
	defer prox.Close()
	proxied := endpoints.Endpoint{ServerURL: "http://origin.invalid", ProxyURL: prox.URL, Token: "px-token"}

	tokDir := t.TempDir()
	tokFile := filepath.Join(tokDir, "token")
	_ = os.WriteFile(tokFile, []byte("sa-file-token"), 0o600)
	restore := dynamic.SetServiceAccountTokenPathForTest(tokFile)
	defer restore()
	saEP := endpoints.Endpoint{ServerURL: srv.URL, CertificateAuthorityData: string(p.caPEM), Token: "sa-boot-token"}

	held := credhttp.WithHeldCredential(context.Background())
	sa := cache.WithServiceAccountDial(context.Background())
	pl := ptr.To(`{"a":1}`)
	cases := []struct {
		name string
		ctx  context.Context
		ep   endpoints.Endpoint
		verb string
		body *string
	}{
		{"cert-held-GET", held, certEP, "GET", nil},
		{"cert-held-POST", held, certEP, "POST", pl},
		{"cert-held-PUT", held, certEP, "PUT", pl},
		{"cert-held-DELETE", held, certEP, "DELETE", nil},
		{"tokCA-held-GET", held, tokCA, "GET", nil},
		{"tokNoCA-ext-GET", context.Background(), tokNoCA, "GET", nil},
		{"tokNoCA-ext-PATCH", context.Background(), tokNoCA, "PATCH", pl},
		{"insecureTok-GET", context.Background(), insecureTok, "GET", nil},
		{"basicUser-GET", context.Background(), basicUser, "GET", nil},
		{"tokUserNoPw-GET", context.Background(), tokUserNoPw, "GET", nil},
		{"certInsecure-held-GET", held, certInsecure, "GET", nil},
		{"noAuth-GET", context.Background(), noAuth, "GET", nil},
		{"proxied-GET", context.Background(), proxied, "GET", nil},
		{"sa-GET", sa, saEP, "GET", nil},
		{"sa-POST", sa, saEP, "POST", pl},
	}
	for _, c := range cases {
		code, st, reason, msg, es := revFetch(c.ctx, c.ep, c.verb, "/api/v1/namespaces/ns/configmaps/x", c.body)
		revEmit(revObs{Scenario: c.name, Code: code, Status: st, Reason: reason, Message: msg, Err: es, Wires: rec.take()})
	}

	// #1112 D1 — write-once on an external endpointRef dial (token, no CA,
	// and Insecure token over h2), server answers 502 first then 200.
	for _, shape := range []struct {
		name string
		ep   func(url string) endpoints.Endpoint
		tls  bool
	}{
		{"ext-http1-tokNoCA", func(u string) endpoints.Endpoint { return endpoints.Endpoint{ServerURL: u, Token: "ext-w"} }, false},
		{"ext-h2-insecureTok", func(u string) endpoints.Endpoint {
			return endpoints.Endpoint{ServerURL: u, Token: "ext-w", Insecure: true}
		}, true},
	} {
		for _, verb := range []string{"GET", "PUT", "DELETE", "POST", "PATCH"} {
			var n atomic.Int32
			wr := &revRecorder{}
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wr.record(r)
				if n.Add(1) == 1 {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"kind":"X"}`))
			})
			var url string
			if shape.tls {
				url = revTLSServer(t, p, false, h).URL
			} else {
				s := httptest.NewServer(h)
				defer s.Close()
				url = s.URL
			}
			var body *string
			if verb != "GET" && verb != "DELETE" {
				body = pl
			}
			if verb == "DELETE" {
				body = nil
			}
			code, st, reason, msg, es := revFetch(context.Background(), shape.ep(url), verb, "/x", body)
			revEmit(revObs{Scenario: "writeonce-" + shape.name + "-" + verb, Code: code, Status: st, Reason: reason, Message: msg, Err: es, Wires: wr.take()})
		}
	}
	// PUT with body over h2, 502 then 200 (the #1112 D1 shape exactly).
	{
		var n atomic.Int32
		wr := &revRecorder{}
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			wr.record(r)
			if n.Add(1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"X"}`))
		})
		u := revTLSServer(t, p, false, h).URL
		code, st, reason, msg, es := revFetch(context.Background(), endpoints.Endpoint{ServerURL: u, Token: "ext-w", Insecure: true}, "PUT", "/x", pl)
		revEmit(revObs{Scenario: "writeonce-h2-PUT-body-502-then-200", Code: code, Status: st, Reason: reason, Message: msg, Err: es, Wires: wr.take()})
	}

	// #1106 — a named endpointRef's bearer on a cross-host redirect.
	{
		b := &revRecorder{}
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b.record(r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"Evil"}`))
		}))
		defer target.Close()
		evil := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)
		origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, evil+"/stolen", http.StatusFound)
		}))
		defer origin.Close()
		for _, c := range []struct {
			name string
			ctx  context.Context
		}{{"redirect-named-ref", context.Background()}, {"redirect-held", held}} {
			code, st, reason, msg, es := revFetch(c.ctx, endpoints.Endpoint{ServerURL: origin.URL, Token: "named-ref-secret"}, "GET", "/x", nil)
			revEmit(revObs{Scenario: c.name, Code: code, Status: st, Reason: reason, Message: msg, Err: es, Wires: b.take()})
		}
	}

	// /call corpus through credhttp.Do: status x body x verb.
	type resp struct {
		code int
		ct   string
		body string
	}
	corpus := []resp{
		{200, "application/json", `{"kind":"ConfigMap"}`},
		{201, "application/json", `{"kind":"ConfigMap"}`},
		{204, "", ``},
		{200, "text/plain", `hi`},
		{400, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"bad","reason":"BadRequest","code":400}`},
		{403, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden","reason":"Forbidden","code":403}`},
		{404, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"nf","reason":"NotFound","code":404}`},
		{409, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"conflict","reason":"AlreadyExists","code":409}`},
		{422, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"invalid","reason":"Invalid","code":422}`},
		{409, "application/json", `{"kind":"Thing","spec":{}}`},
		{404, "application/json", `{"status":"Success","code":200}`},
		{500, "application/json", `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"boom","reason":"InternalError","code":500}`},
		{500, "application/json", `{"error":"x"}`},
		{502, "text/plain", ``},
		{503, "text/plain", `unavailable`},
		{504, "application/json", `{"kind":"Status","status":"Failure","message":"timeout","code":504}`},
		{429, "text/plain", `slow down`},
		{401, "text/plain", `nope`},
	}
	t.Setenv("CLIENT_MAX_RETRIES", "1")
	for _, rc := range corpus {
		for _, verb := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
			wr := &revRecorder{}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wr.record(r)
				if rc.ct != "" {
					w.Header().Set("Content-Type", rc.ct)
				}
				w.WriteHeader(rc.code)
				_, _ = w.Write([]byte(rc.body))
			}))
			var body *string
			if verb != "GET" && verb != "DELETE" {
				body = pl
			}
			ep := endpoints.Endpoint{ServerURL: s.URL}
			st := credhttp.Do(credhttp.WithHeldCredential(context.Background()), httpcall.RequestOptions{
				RequestInfo:     httpcall.RequestInfo{Path: "/x", Verb: ptr.To(verb), Payload: body},
				Endpoint:        &ep,
				ResponseHandler: func(rc io.ReadCloser) error { _, err := io.ReadAll(rc); return err },
			})
			s.Close()
			o := revObs{Scenario: "call-" + verb + "-" + http.StatusText(rc.code) + "-" + revDigest(rc.body), Wires: nil}
			if st != nil {
				o.Code, o.Status, o.Reason, o.Message = st.Code, st.Status, string(st.Reason), st.Message
			}
			w := wr.take()
			o.Err = "server_requests=" + strconv.Itoa(len(w))
			revEmit(o)
		}
	}
}
