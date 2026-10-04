// call_443_test.go — #443 parts 1 and 3, the hermetic falsifiers (design §6:
// TestS443_DryRun a/c/d/e/h/i, TestS443_Raw a/b/c/d/e, TestS443_Capabilities
// a/b, RBAC r1). The kind-backed arms (DryRun b and f, Raw c as a real
// apiserver 403/404) live in call_443_kind_test.go; the route-table and CORS
// arms (Capabilities c and d) live in main_443_test.go.
//
// Every arm drives the real handler (Call / CallRead / CallDryRun, behind the
// real Dispatcher where the arm is about dispatch) against a fake apiserver
// that records EVERY request it receives, so "zero outbound requests" is
// observed, not assumed.
package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// rec443 is one request the fake apiserver received.
type rec443 struct {
	method   string
	path     string
	query    url.Values
	body     string
	clientCN string // CN of the presented TLS client certificate ("" when none)
}

// fakeAPI443 is a fake apiserver that records every request and answers with
// a configurable status and body.
type fakeAPI443 struct {
	mu     sync.Mutex
	reqs   []rec443
	status int
	body   string
	srv    *httptest.Server
}

func (f *fakeAPI443) handler(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	cn := ""
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		cn = r.TLS.PeerCertificates[0].Subject.CommonName
	}
	f.mu.Lock()
	f.reqs = append(f.reqs, rec443{
		method: r.Method, path: r.URL.Path, query: r.URL.Query(), body: string(b), clientCN: cn,
	})
	status, body := f.status, f.body
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func (f *fakeAPI443) requests() []rec443 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rec443(nil), f.reqs...)
}

func (f *fakeAPI443) reset() {
	f.mu.Lock()
	f.reqs = nil
	f.mu.Unlock()
}

func (f *fakeAPI443) respond(status int, body string) {
	f.mu.Lock()
	f.status, f.body = status, body
	f.mu.Unlock()
}

// newFakeAPI443 starts a plain-HTTP fake apiserver. Test mode is switched on
// so plumbing's UserConfig keeps the injected ServerURL.
func newFakeAPI443(t *testing.T) (*fakeAPI443, endpoints.Endpoint) {
	t.Helper()
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	f := &fakeAPI443{status: http.StatusOK, body: `{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"cm-x"}}`}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(func() {
		f.srv.Close()
		xenv.SetTestMode(prev)
	})
	return f, endpoints.Endpoint{ServerURL: f.srv.URL}
}

// withCaller443 attaches the caller identity and endpoint the UserConfig
// middleware would attach in production.
func withCaller443(req *http.Request, ep endpoints.Endpoint) *http.Request {
	ctx := xcontext.BuildContext(req.Context(),
		xcontext.WithAccessToken("test-token"),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
		xcontext.WithUserConfig(ep),
	)
	return req.WithContext(ctx)
}

const (
	cmCreate443 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default"},"data":{"k":"v"}}`
	cmQuery443  = "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"
	raQuery443  = "apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name=kube-get"
	wQuery443   = "apiVersion=widgets.templates.krateo.io/v1beta1&resource=tables&namespace=demo-system&name=t1"
)

// serve443 drives h with method + "/call?"+query (+ body) as alice.
func serve443(h http.Handler, ep endpoints.Endpoint, method, target, body string) *httptest.ResponseRecorder {
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := withCaller443(httptest.NewRequest(method, target, rdr), ep)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var echoHeaders443 = []string{HeaderDryRun, HeaderFieldValidation, HeaderRaw, HeaderResolveSource}

func assertNoEcho443(t *testing.T, rec *httptest.ResponseRecorder, ctxMsg string) {
	t.Helper()
	for _, h := range echoHeaders443 {
		if v := rec.Header().Get(h); v != "" {
			t.Errorf("%s: echo header %s=%q must be absent", ctxMsg, h, v)
		}
	}
}

// TestS443_DryRun — design §6 arms a, c, d, e, h, i (+ the dry-run part of r1).
func TestS443_DryRun(t *testing.T) {
	// a — ForwardsAll. Each dry-run verb sends the apiserver dryRun=All, with
	// the same verb and the object body; the dry-run is a write the apiserver
	// validates, not a read.
	t.Run("a_ForwardsAll", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
			f.reset()
			rec := serve443(CallDryRun(), ep, m, "/call/dry-run?"+cmQuery443, cmCreate443)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: code=%d body=%s", m, rec.Code, rec.Body.String())
			}
			got := f.requests()
			if len(got) != 1 {
				t.Fatalf("%s: outbound requests=%d, want exactly 1", m, len(got))
			}
			if got[0].method != m {
				t.Errorf("%s: outbound verb=%s", m, got[0].method)
			}
			if dr := got[0].query["dryRun"]; !reflect.DeepEqual(dr, []string{"All"}) {
				t.Errorf("%s: outbound dryRun=%v, want exactly [All] — a dry-run that does not reach the apiserver as dryRun=All is a REAL write", m, dr)
			}
			if got[0].body == "" {
				t.Errorf("%s: outbound body empty; a dry-run must send the object for the apiserver to validate", m)
			}
		}
	})

	// c — InvalidValues400. Each is a 400 with ZERO outbound requests and no
	// echo header.
	t.Run("c_InvalidValues400", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		cases := []struct {
			name, method, target string
			h                    http.Handler
		}{
			{"dryRun=all", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&dryRun=all", CallDryRun()},
			{"dryRun=true", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&dryRun=true", CallDryRun()},
			{"dryRun=empty", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&dryRun=", CallDryRun()},
			{"dryRun=All,All", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&dryRun=All,All", CallDryRun()},
			{"dryRun twice", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&dryRun=All&dryRun=All", CallDryRun()},
			{"fieldValidation=strict", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&fieldValidation=strict", CallDryRun()},
			{"fieldValidation=Foo", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&fieldValidation=Foo", CallDryRun()},
			{"fieldValidation=Warn (dry-run)", http.MethodPost, "/call/dry-run?" + cmQuery443 + "&fieldValidation=Warn", CallDryRun()},
			{"fieldValidation=Warn (call)", http.MethodPost, "/call?" + cmQuery443 + "&fieldValidation=Warn", Call()},
			{"fieldValidation on GET", http.MethodGet, "/call?" + cmQuery443 + "&fieldValidation=Strict", Call()},
			{"fieldValidation on DELETE", http.MethodDelete, "/call?" + cmQuery443 + "&fieldValidation=Strict", Call()},
			{"fieldValidation on /call/read", http.MethodPost, "/call/read?" + cmQuery443 + "&fieldValidation=Strict", CallRead()},
			{"dry-run DELETE", http.MethodDelete, "/call/dry-run?" + cmQuery443, CallDryRun()},
			{"dry-run GET", http.MethodGet, "/call/dry-run?" + cmQuery443, CallDryRun()},
		}
		for _, tc := range cases {
			f.reset()
			rec := serve443(tc.h, ep, tc.method, tc.target, cmCreate443)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: code=%d, want 400 (body=%s)", tc.name, rec.Code, rec.Body.String())
			}
			if n := len(f.requests()); n != 0 {
				t.Errorf("%s: %d outbound requests, want 0 — a rejected value must never reach the apiserver", tc.name, n)
			}
			assertNoEcho443(t, rec, tc.name)
		}
	})

	// d — PlainCallDryRun400. On plain /call, dryRun is a 400 with zero
	// outbound requests, on every verb. RED on main: POST /call?dryRun=All
	// forwards only page/perPage, so the apiserver receives a REAL create.
	t.Run("d_PlainCallDryRun400", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodGet} {
			f.reset()
			rec := serve443(Call(), ep, m, "/call?"+cmQuery443+"&dryRun=All", cmCreate443)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s /call?dryRun=All: code=%d, want 400", m, rec.Code)
			}
			if got := f.requests(); len(got) != 0 {
				t.Errorf("%s /call?dryRun=All: %d outbound requests (first: %s %s?%s), want 0 — on plain /call a dryRun must never be forwarded or dropped",
					m, len(got), got[0].method, got[0].path, got[0].query.Encode())
			}
			if !strings.Contains(rec.Body.String(), "/call/dry-run") {
				t.Errorf("%s /call?dryRun=All: body %q does not point the client at /call/dry-run", m, rec.Body.String())
			}
			assertNoEcho443(t, rec, m+" /call?dryRun=All")
		}
	})

	// e — Echo. X-Snowplow-Dry-Run: All on a 2xx and on an apiserver 422,
	// absent on a validation 400. fieldValidation is echoed when forwarded.
	t.Run("e_Echo", func(t *testing.T) {
		f, ep := newFakeAPI443(t)

		rec := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443+"&fieldValidation=Strict", cmCreate443)
		if rec.Code != http.StatusOK || rec.Header().Get(HeaderDryRun) != "All" {
			t.Errorf("2xx: code=%d %s=%q, want 200 + All", rec.Code, HeaderDryRun, rec.Header().Get(HeaderDryRun))
		}
		if v := rec.Header().Get(HeaderFieldValidation); v != "Strict" {
			t.Errorf("2xx: %s=%q, want Strict", HeaderFieldValidation, v)
		}
		if q := f.requests()[0].query; q.Get("fieldValidation") != "Strict" {
			t.Errorf("fieldValidation not forwarded: outbound query %v", q)
		}

		f.respond(http.StatusUnprocessableEntity,
			`{"kind":"Status","apiVersion":"v1","status":"Failure","message":"ConfigMap \"cm-x\" is invalid: bogus","reason":"Invalid","code":422}`)
		rec = serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmCreate443)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Errorf("apiserver 422: code=%d, want 422 passed through", rec.Code)
		}
		if v := rec.Header().Get(HeaderDryRun); v != "All" {
			t.Errorf("apiserver 422: %s=%q, want All — the request DID reach the apiserver as a dry run", HeaderDryRun, v)
		}
		if v := rec.Header().Get(HeaderFieldValidation); v != "" {
			t.Errorf("apiserver 422 without fieldValidation: %s=%q, want absent", HeaderFieldValidation, v)
		}

		rec = serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443+"&dryRun=nope", cmCreate443)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("validation 400: code=%d", rec.Code)
		}
		assertNoEcho443(t, rec, "validation 400")
	})

	// h — NoStateTouched. A dry-run create of a GVR that was never informed
	// leaves L1, dep edges, the watcher's GVR set, the refresher counters and
	// the SSE counters unchanged. This cannot fail on main (the route does not
	// exist there), so it was proven live by mutation: injecting
	// cache.Global().EnsureResourceType(opts.gvr) into the CallDryRun path
	// turns it RED on the watcher-GVR-set assertion (see the PR body).
	t.Run("h_NoStateTouched", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		t.Setenv("RESOLVED_CACHE_ENABLED", "true")
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
		t.Cleanup(func() {
			cache.ResetDepsForTest()
			cache.ResetResolvedCacheForTest()
		})

		listKinds := listKinds443()
		wctx, wcancel := context.WithCancel(context.Background())
		rw, err := cache.NewResourceWatcher(wctx, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds))
		if err != nil || rw == nil {
			wcancel()
			t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
		}
		prev := cache.Global()
		cache.SetGlobal(rw)
		t.Cleanup(func() {
			rw.Stop()
			wcancel()
			cache.SetGlobal(prev)
		})
		store := cache.ResolvedCache()
		if store == nil {
			t.Fatal("ResolvedCache() nil with RESOLVED_CACHE_ENABLED=true")
		}
		cmGVR := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

		type snap struct {
			gvrs     []string
			l1       []string
			deps     map[string]int64
			refresh  [12]uint64
			sse      [4]uint64
			informed bool
		}
		take := func() snap {
			var s snap
			for _, g := range rw.RegisteredGVRs() {
				s.gvrs = append(s.gvrs, g.String())
				if g == cmGVR {
					s.informed = true
				}
			}
			sort.Strings(s.gvrs)
			s.l1 = store.KeysForTest()
			sort.Strings(s.l1)
			s.deps = cache.DepsStatsByStat()
			e, c, fl, rt, d, sne, snh, sse, y, cp, fo, qd := cache.RefresherSnapshot()
			s.refresh = [12]uint64{e, c, fl, rt, d, sne, snh, sse, y, cp, fo, uint64(qd)}
			p, dl, dr, co := cache.RefreshBroadcasterCounters()
			s.sse = [4]uint64{p, dl, dr, co}
			return s
		}

		before := take()
		if before.informed {
			t.Fatalf("precondition: %s must not be informed before the dry run", cmGVR)
		}

		f, ep := newFakeAPI443(t)
		h := cache.FallthroughScopeMiddleware(cache.ScopeCallWritePost)(CallDryRun())
		rec := serve443(h, ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmCreate443)
		if rec.Code != http.StatusOK || len(f.requests()) != 1 {
			t.Fatalf("dry run did not run: code=%d outbound=%d", rec.Code, len(f.requests()))
		}

		after := take()
		if after.informed || !reflect.DeepEqual(before.gvrs, after.gvrs) {
			t.Errorf("watcher GVR set changed by a dry run: before=%v after=%v", before.gvrs, after.gvrs)
		}
		if !reflect.DeepEqual(before.l1, after.l1) {
			t.Errorf("L1 keys changed by a dry run: before=%v after=%v", before.l1, after.l1)
		}
		if !reflect.DeepEqual(before.deps, after.deps) {
			t.Errorf("dep stats changed by a dry run: before=%v after=%v", before.deps, after.deps)
		}
		if before.refresh != after.refresh {
			t.Errorf("refresher counters changed by a dry run: before=%v after=%v", before.refresh, after.refresh)
		}
		if before.sse != after.sse {
			t.Errorf("SSE counters changed by a dry run: before=%v after=%v", before.sse, after.sse)
		}
	})

	// i — Audit. A dry run emits Action "call.dryRun"; a plain write keeps
	// "call" (the control that proves the recorder sees the plain event too).
	t.Run("i_Audit", func(t *testing.T) {
		lrec := logtest.NewRecorder()
		audit.SetDefault(audit.New(lrec.Logger("test-443")))
		t.Cleanup(func() { audit.SetDefault(nil) })

		_, ep := newFakeAPI443(t)
		serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmCreate443)
		serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmCreate443)

		var actions []string
		for _, recs := range lrec.Result() {
			for _, r := range recs {
				for _, kv := range r.Attributes {
					if kv.Key == "krateo.action" {
						actions = append(actions, kv.Value.AsString())
					}
				}
			}
		}
		if !reflect.DeepEqual(actions, []string{"call.dryRun", "call"}) {
			t.Fatalf("audit actions=%v, want [call.dryRun call]", actions)
		}
	})
}

// TestS443_RBAC_CallerCert — r1. Every write the fake apiserver receives (a
// dry run, a fieldValidation write) is made with the CALLER's client
// certificate. Snowplow holds no other identity on this path: the endpoint
// comes from the caller's clientconfig, so a request that reached the
// apiserver under any other identity would show a different CN here.
func TestS443_RBAC_CallerCert(t *testing.T) {
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca-443"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, _ := x509.ParseCertificate(caDER)
	cliKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "alice", Organization: []string{"devs"}},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	cliDER, _ := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)

	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	f := &fakeAPI443{status: http.StatusOK, body: `{"kind":"ConfigMap"}`}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(f.handler))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAnyClientCert}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	srvCAPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	// plumbing reads clientconfig data base64-encoded, as the Secret stores it.
	b64 := base64.StdEncoding.EncodeToString
	ep := endpoints.Endpoint{
		ServerURL:                srv.URL,
		CertificateAuthorityData: b64(srvCAPEM),
		ClientCertificateData:    b64(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cliDER})),
		ClientKeyData:            b64(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(cliKey)})),
	}

	runs := []struct {
		name   string
		h      http.Handler
		target string
	}{
		{"dry-run", CallDryRun(), "/call/dry-run?" + cmQuery443},
		{"dry-run+Strict", CallDryRun(), "/call/dry-run?" + cmQuery443 + "&fieldValidation=Strict"},
		{"call+Strict", Call(), "/call?" + cmQuery443 + "&fieldValidation=Strict"},
	}
	for _, r := range runs {
		f.reset()
		rec := serve443(r.h, ep, http.MethodPost, r.target, cmCreate443)
		got := f.requests()
		if rec.Code != http.StatusOK || len(got) != 1 {
			t.Fatalf("%s: code=%d outbound=%d body=%s", r.name, rec.Code, len(got), rec.Body.String())
		}
		if got[0].clientCN != "alice" {
			t.Errorf("%s: outbound client cert CN=%q, want the caller (alice)", r.name, got[0].clientCN)
		}
	}
}

// raCR443 is a stored RESTAction as the apiserver returns it, with a status
// the resolver would never produce, so a resolved reply cannot pass for it.
const raCR443 = `{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction","metadata":{"name":"kube-get","namespace":"demo-system"},"spec":{"api":[{"name":"x","path":"/api/v1/namespaces"}]},"status":{"stored":"as-is-443"}}`

// TestS443_Raw — design §6 arms a, b, c, d, e.
func TestS443_Raw(t *testing.T) {
	// a/b — raw=true on restactions and on the widgets group returns the
	// STORED object read as the caller: the resolve handler is never called
	// and .status is exactly what the apiserver stored. RED on main: the
	// dispatcher sends raw=true to the resolver.
	for _, tc := range []struct {
		arm, query, wantPath string
	}{
		{"a_RESTAction", raQuery443, "/apis/templates.krateo.io/v1/namespaces/demo-system/restactions/kube-get"},
		{"b_Widget", wQuery443, "/apis/widgets.templates.krateo.io/v1beta1/namespaces/demo-system/tables/t1"},
	} {
		t.Run(tc.arm, func(t *testing.T) {
			f, ep := newFakeAPI443(t)
			f.respond(http.StatusOK, raCR443)
			ra, widget, m := dispatchHandlers()
			h := Dispatcher(m)(Call())
			rec := serve443(h, ep, http.MethodGet, "/call?"+tc.query+"&raw=true", "")
			if ra.called || widget.called {
				t.Fatal("raw=true reached a resolve handler; a raw read must skip the resolver")
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
			}
			got := f.requests()
			if len(got) != 1 || got[0].method != http.MethodGet || got[0].path != tc.wantPath {
				t.Fatalf("outbound=%+v, want one GET %s", got, tc.wantPath)
			}
			if _, leaked := got[0].query["raw"]; leaked {
				t.Errorf("raw leaked to the apiserver query: %v", got[0].query)
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			_ = json.Unmarshal([]byte(raCR443), &want)
			if !reflect.DeepEqual(body["status"], want["status"]) {
				t.Errorf(".status=%v, want the stored %v", body["status"], want["status"])
			}
			if v := rec.Header().Get(HeaderRaw); v != "true" {
				t.Errorf("%s=%q, want true", HeaderRaw, v)
			}
		})
	}

	// c — the apiserver's 403 and 404 pass through, and the raw echo is still
	// set (the request did reach the apiserver as a raw read).
	t.Run("c_CallerErrorsPassThrough", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		_, _, m := dispatchHandlers()
		h := Dispatcher(m)(Call())
		for _, code := range []int{http.StatusForbidden, http.StatusNotFound} {
			f.respond(code, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"nope","code":`+itoa443(code)+`}`)
			rec := serve443(h, ep, http.MethodGet, "/call?"+raQuery443+"&raw=true", "")
			if rec.Code != code {
				t.Errorf("apiserver %d: code=%d", code, rec.Code)
			}
			if rec.Header().Get(HeaderRaw) != "true" {
				t.Errorf("apiserver %d: raw echo missing", code)
			}
		}
	})

	// d — raw must be exactly "true" and only on reads. Each bad case is a 400
	// with zero outbound requests, even for a GVR that has a resolve handler
	// (the dispatcher falls through on the parameter's presence). POST
	// /call/read with raw=true falls through to the read-only CallRead.
	t.Run("d_Validation", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		ra, widget, m := dispatchHandlers()
		get := Dispatcher(m)(Call())
		bad := []struct{ name, method, target string }{
			{"raw=false", http.MethodGet, "/call?" + raQuery443 + "&raw=false"},
			{"raw=1", http.MethodGet, "/call?" + raQuery443 + "&raw=1"},
			{"raw=True", http.MethodGet, "/call?" + raQuery443 + "&raw=True"},
			{"raw=empty", http.MethodGet, "/call?" + raQuery443 + "&raw="},
			{"raw on POST", http.MethodPost, "/call?" + raQuery443 + "&raw=true"},
			{"raw on PUT", http.MethodPut, "/call?" + raQuery443 + "&raw=true"},
			{"raw on PATCH", http.MethodPatch, "/call?" + raQuery443 + "&raw=true"},
			{"raw on DELETE", http.MethodDelete, "/call?" + raQuery443 + "&raw=true"},
		}
		for _, tc := range bad {
			f.reset()
			rec := serve443(get, ep, tc.method, tc.target, raCR443)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: code=%d, want 400", tc.name, rec.Code)
			}
			if n := len(f.requests()); n != 0 {
				t.Errorf("%s: %d outbound requests, want 0", tc.name, n)
			}
			assertNoEcho443(t, rec, tc.name)
		}
		if ra.called || widget.called {
			t.Error("a raw request reached a resolve handler")
		}
		// raw=true on the dry-run route is a write verb too.
		f.reset()
		if rec := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443+"&raw=true", cmCreate443); rec.Code != http.StatusBadRequest || len(f.requests()) != 0 {
			t.Errorf("raw on /call/dry-run: code=%d outbound=%d, want 400 and 0", rec.Code, len(f.requests()))
		}

		// POST /call/read raw=true → CallRead → one GET, no body.
		f.reset()
		f.respond(http.StatusOK, raCR443)
		ra2, w2, m2 := dispatchHandlers()
		rec := serve443(ReadDispatcher(m2)(CallRead()), ep, http.MethodPost, "/call/read?"+raQuery443+"&raw=true", `{"extras":{}}`)
		if ra2.called || w2.called {
			t.Fatal("POST /call/read raw=true reached a resolve handler")
		}
		got := f.requests()
		if rec.Code != http.StatusOK || len(got) != 1 || got[0].method != http.MethodGet || got[0].body != "" {
			t.Fatalf("POST /call/read raw=true: code=%d outbound=%+v, want 200 and one bodiless GET", rec.Code, got)
		}
		if rec.Header().Get(HeaderRaw) != "true" {
			t.Error("POST /call/read raw=true: raw echo missing")
		}
	})

	// e — no L1 or informer change after a raw read, and the passthrough is
	// counted under ReasonRawRead, not ReasonClientBuild.
	t.Run("e_NoStateAndReason", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		t.Setenv("RESOLVED_CACHE_ENABLED", "true")
		cache.ResetResolvedCacheForTest()
		cache.ResetFallthroughCountersForTest()
		t.Cleanup(func() {
			cache.ResetResolvedCacheForTest()
			cache.ResetFallthroughCountersForTest()
		})
		wctx, wcancel := context.WithCancel(context.Background())
		rw, err := cache.NewResourceWatcher(wctx, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds443()))
		if err != nil || rw == nil {
			wcancel()
			t.Fatalf("NewResourceWatcher: %v", err)
		}
		prev := cache.Global()
		cache.SetGlobal(rw)
		t.Cleanup(func() {
			rw.Stop()
			wcancel()
			cache.SetGlobal(prev)
		})
		store := cache.ResolvedCache()
		gvrsBefore := len(rw.RegisteredGVRs())
		l1Before := store.Len()

		f, ep := newFakeAPI443(t)
		f.respond(http.StatusOK, raCR443)
		_, _, m := dispatchHandlers()
		h := cache.FallthroughScopeMiddleware(cache.ScopeCallGeneric)(Dispatcher(m)(Call()))
		if rec := serve443(h, ep, http.MethodGet, "/call?"+raQuery443+"&raw=true", ""); rec.Code != http.StatusOK {
			t.Fatalf("raw read: code=%d", rec.Code)
		}
		if got := len(rw.RegisteredGVRs()); got != gvrsBefore {
			t.Errorf("watcher GVR count %d → %d after a raw read", gvrsBefore, got)
		}
		if got := store.Len(); got != l1Before {
			t.Errorf("L1 entries %d → %d after a raw read", l1Before, got)
		}
		if n := cache.FallthroughCount(cache.ScopeCallGeneric, "", cache.ReasonRawRead); n != 1 {
			t.Errorf("FallthroughCount(raw-read)=%d, want 1", n)
		}
		if n := cache.FallthroughCount(cache.ScopeCallGeneric, "", cache.ReasonClientBuild); n != 0 {
			t.Errorf("FallthroughCount(client-build)=%d, want 0 — a raw read must not be counted as an unhandled-GVR passthrough", n)
		}
	})
}

// listKinds443 is every GVR the fake dynamic client may be asked to LIST: the
// RBAC set NewResourceWatcher informs at boot, plus the GVRs the arms address
// (so a mutation that informs one of them lists cleanly instead of panicking).
func listKinds443() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Version: "v1", Resource: "configmaps"}:                                              "ConfigMapList",
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}:               "RESTActionList",
	}
}

func itoa443(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// capProbes443 maps every advertised capability token to a probe that drives
// the implementation and reports whether the behaviour the token promises is
// there. Removing an implementation turns its probe false.
var capProbes443 = map[string]func(t *testing.T) bool{
	// call.read.inline: POST /call/read with an inline RESTAction resolves
	// the BODY (its filter output appears) with the two echoes, and reads no
	// stored object from the apiserver (the caller's own access check on
	// restactions may still run). The draft has no stages, so the probe needs
	// no ServiceAccount config.
	CapCallReadInline: func(t *testing.T) bool {
		f, ep := newFakeAPI443(t)
		obj := map[string]any{
			"apiVersion": "templates.krateo.io/v1", "kind": "RESTAction",
			"metadata": map[string]any{"name": "draft-cap", "namespace": "demo-system"},
			"spec":     map[string]any{"filter": `{"probe":"inline-cap-443"}`},
		}
		body, _ := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
		h := middleware.BodyExtrasDecode(ReadDispatcher(map[string]http.Handler{
			"restactions.templates.krateo.io": dispatchers.RESTAction(),
		})(CallRead()))
		rec := serve443(h, ep, http.MethodPost,
			"/call/read?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=demo-system&name=draft-cap", string(body))
		for _, r := range f.requests() {
			if strings.Contains(r.path, "/restactions/") {
				return false // the stored object was read: not an inline resolve
			}
		}
		return rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "inline-cap-443") &&
			rec.Header().Get(HeaderDryRun) == "All" && rec.Header().Get(HeaderResolveSource) == "request-body"
	},
	CapCallDryRun: func(t *testing.T) bool {
		f, ep := newFakeAPI443(t)
		rec := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmCreate443)
		got := f.requests()
		return rec.Code == http.StatusOK && rec.Header().Get(HeaderDryRun) == "All" &&
			len(got) == 1 && got[0].query.Get("dryRun") == "All"
	},
	CapCallFieldValidation: func(t *testing.T) bool {
		f, ep := newFakeAPI443(t)
		ok := true
		for _, v := range []string{"Ignore", "Strict"} {
			f.reset()
			rec := serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443+"&fieldValidation="+v, cmCreate443)
			got := f.requests()
			ok = ok && rec.Code == http.StatusOK && rec.Header().Get(HeaderFieldValidation) == v &&
				len(got) == 1 && got[0].query.Get("fieldValidation") == v
		}
		return ok
	},
	CapCallRaw: func(t *testing.T) bool {
		f, ep := newFakeAPI443(t)
		f.respond(http.StatusOK, raCR443)
		ra, widget, m := dispatchHandlers()
		rec := serve443(Dispatcher(m)(Call()), ep, http.MethodGet, "/call?"+raQuery443+"&raw=true", "")
		return !ra.called && !widget.called && rec.Code == http.StatusOK &&
			rec.Header().Get(HeaderRaw) == "true" && len(f.requests()) == 1
	},
}

// TestS443_Capabilities — design §6 arms a and b (c and d are in
// main_443_test.go, next to the CORS options and the route table).
func TestS443_Capabilities(t *testing.T) {
	// a — Registry. The advertised set is exactly the probed set (two-way),
	// every probe passes, the list is sorted, and call.read.inline is NOT
	// advertised until part 2 ships.
	t.Run("a_Registry", func(t *testing.T) {
		rec := httptest.NewRecorder()
		CapabilitiesHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/capabilities", nil))
		if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			t.Fatalf("code=%d content-type=%q", rec.Code, rec.Header().Get("Content-Type"))
		}
		var body struct {
			Capabilities []string `json:"capabilities"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		want := []string{"call.dryRun", "call.fieldValidation", "call.raw", "call.read.inline"}
		if !reflect.DeepEqual(body.Capabilities, want) {
			t.Fatalf("capabilities=%v, want %v (sorted)", body.Capabilities, want)
		}
		for _, tok := range body.Capabilities {
			probe, ok := capProbes443[tok]
			if !ok {
				t.Errorf("advertised token %q has no probe — an advertised capability must be exercised", tok)
				continue
			}
			if !probe(t) {
				t.Errorf("advertised token %q: its probe failed — the implementation is missing or broken", tok)
			}
		}
		for tok := range capProbes443 {
			found := false
			for _, a := range body.Capabilities {
				found = found || a == tok
			}
			if !found {
				t.Errorf("probed token %q is not advertised", tok)
			}
		}
	})

	// b — Echo matrix. Echoes on dry-run and raw replies, success and
	// failure; none on a plain /call read or write.
	t.Run("b_EchoMatrix", func(t *testing.T) {
		f, ep := newFakeAPI443(t)
		_, _, m := dispatchHandlers()
		type row struct {
			name           string
			status         int
			h              http.Handler
			method, target string
			want           map[string]string
		}
		rows := []row{
			{"plain GET", 200, Call(), http.MethodGet, "/call?" + cmQuery443, nil},
			{"plain POST", 200, Call(), http.MethodPost, "/call?" + cmQuery443, nil},
			{"plain POST 422", 422, Call(), http.MethodPost, "/call?" + cmQuery443, nil},
			{"plain POST Strict", 200, Call(), http.MethodPost, "/call?" + cmQuery443 + "&fieldValidation=Strict", map[string]string{HeaderFieldValidation: "Strict"}},
			{"dry-run 200", 200, CallDryRun(), http.MethodPost, "/call/dry-run?" + cmQuery443, map[string]string{HeaderDryRun: "All"}},
			{"dry-run 403", 403, CallDryRun(), http.MethodPost, "/call/dry-run?" + cmQuery443, map[string]string{HeaderDryRun: "All"}},
			{"dry-run PATCH Ignore 422", 422, CallDryRun(), http.MethodPatch, "/call/dry-run?" + cmQuery443 + "&fieldValidation=Ignore", map[string]string{HeaderDryRun: "All", HeaderFieldValidation: "Ignore"}},
			{"raw 200", 200, Dispatcher(m)(Call()), http.MethodGet, "/call?" + raQuery443 + "&raw=true", map[string]string{HeaderRaw: "true"}},
			{"raw 404", 404, Dispatcher(m)(Call()), http.MethodGet, "/call?" + raQuery443 + "&raw=true", map[string]string{HeaderRaw: "true"}},
		}
		for _, r := range rows {
			body := `{"kind":"ConfigMap"}`
			if r.status >= 300 {
				body = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"x","code":` + itoa443(r.status) + `}`
			}
			f.respond(r.status, body)
			rec := serve443(r.h, ep, r.method, r.target, cmCreate443)
			if rec.Code != r.status {
				t.Errorf("%s: code=%d, want %d", r.name, rec.Code, r.status)
			}
			for _, h := range echoHeaders443 {
				if got, want := rec.Header().Get(h), r.want[h]; got != want {
					t.Errorf("%s: %s=%q, want %q", r.name, h, got, want)
				}
			}
		}
	})
}
