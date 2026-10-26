package dispatchers

// issue903_background_never_writes_test.go — #903: background work only reads.
// A RESTAction write stage is sent only on a customer's own request, whatever
// credential the stage uses.
//
// Each background driver's REAL production ctx (driverCtxArms720 — the #720
// driver arms, which the resolve-entry census keeps complete — plus the #547
// demand revalidation, captured from the real demandRevalidateOnce, and the
// HTTP self-loopback hop, built by the real ingest) resolves a RESTAction with
// a read stage and a write stage through each endpoint kind:
//
//	own        no endpointRef: snowplow's ServiceAccount endpoint for a
//	           background driver (the ctx internal endpoint), the caller's own
//	           <user>-clientconfig for a customer;
//	ref        an endpointRef Secret with no credential of its own;
//	external   an endpointRef Secret pointing at ANOTHER host, with its own
//	           bearer token.
//
// The upstreams are httptest servers that count every request by method; the
// test owns the count.
//
// ARMS
//   D1  every driver × kind: 0 write requests reach the upstream, and the
//       refused stage is recorded as a stage error (the Put declines). The read
//       stage still runs (non-vacuity: GET count ≥ 1 where no other guard
//       refuses it).
//   D2  a customer request (no marker) sends the write EXACTLY once per kind.
//   H1  a forged X-Snowplow-Background only stops the forger's own request: a
//       sibling request without it still writes; any value but "1", or a
//       request without a validated identity, is ignored; the hop marker
//       changes nothing else (RefDenialOrigin stays serve, not inert).
//   H2  egress: a background resolve's call to snowplow ITSELF carries the
//       header; a call to any other host never does — even when the
//       RESTAction sets it itself; a customer's self-loopback does not.
//
// Mutation (logic-only, owned): make refuseBackgroundWrite return false → D1
// RED for every driver (on the ref and external kinds; the own kind is also
// guarded by #268's SA re-gate for the drivers that carry it).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	b903AuthnNS = "krateo-system"
	b903User    = "cyberjoker"
	b903RefEP   = "provider-ep"
	b903ExtEP   = "external-ep"
)

// b903Counter counts requests by method and remembers the background header.
type b903Counter struct {
	srv *httptest.Server
	mu  sync.Mutex
	n   map[string]int
	bg  map[string]string // "METHOD path" -> X-Snowplow-Background value seen
}

func newB903Counter(t *testing.T) *b903Counter {
	t.Helper()
	c := &b903Counter{n: map[string]int{}, bg: map[string]string{}}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.n[r.Method]++
		c.bg[r.Method+" "+r.URL.Path] = r.Header.Get(cache.BackgroundHeader)
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *b903Counter) count(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[method]
}

func (c *b903Counter) header(key string) string {
	v, _ := c.headerSeen(key)
	return v
}

// headerSeen returns the X-Snowplow-Background value of the request key
// ("METHOD path") and whether that request was received at all.
func (c *b903Counter) headerSeen(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.bg[key]
	return v, ok
}

func (c *b903Counter) writes() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[http.MethodPost] + c.n[http.MethodPut] + c.n[http.MethodPatch] + c.n[http.MethodDelete]
}

// b903Env: the provider upstream, the external upstream, and a fake apiserver
// serving the endpointRef Secrets and the customer's clientconfig.
type b903Env struct {
	up, ext   *b903Counter
	apiserver *httptest.Server
}

func newB903Env(t *testing.T) *b903Env {
	t.Helper()
	prevTM := env.TestMode()
	env.SetTestMode(true) // keep a clientconfig's server-url (no kubernetes.default.svc rewrite)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	// Every Secret is read from the fake apiserver (no snapshot).
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	e := &b903Env{up: newB903Counter(t), ext: newB903Counter(t)}
	secrets := map[string]map[string][]byte{
		b903RefEP: {"server-url": []byte(e.up.srv.URL)},
		b903ExtEP: {"server-url": []byte(e.ext.srv.URL), "token": []byte("ext-token-903")},
		kubeutil.MakeDNS1123Compatible(b903User) + "-clientconfig": {"server-url": []byte(e.up.srv.URL)},
	}
	e.apiserver = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := "/api/v1/namespaces/" + b903AuthnNS + "/secrets/"
		name := strings.TrimPrefix(r.URL.Path, prefix)
		data, ok := secrets[name]
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, prefix) || !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b903AuthnNS}, Data: data})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}))
	t.Cleanup(e.apiserver.Close)
	return e
}

// stages builds a read + a write through one endpoint kind.
func (e *b903Env) stages(kind string) []*templatesv1.API {
	var ref *templatesv1.Reference
	switch kind {
	case "ref":
		ref = &templatesv1.Reference{Name: b903RefEP, Namespace: b903AuthnNS}
	case "external":
		ref = &templatesv1.Reference{Name: b903ExtEP, Namespace: b903AuthnNS}
	}
	return []*templatesv1.API{
		{Name: "read", Path: "/things/1", Verb: ptr.To(http.MethodGet), EndpointRef: ref,
			ContinueOnError: ptr.To(true), ErrorKey: ptr.To("readErr")},
		{Name: "write", Path: "/things", Verb: ptr.To(http.MethodPost), Payload: ptr.To(`{"n":1}`), EndpointRef: ref,
			ContinueOnError: ptr.To(true), ErrorKey: ptr.To("writeErr")},
	}
}

func (e *b903Env) upstreamFor(kind string) *b903Counter {
	if kind == "external" {
		return e.ext
	}
	return e.up
}

// resolve runs the RESTAction under ctx. A background driver's own-credential
// stage dials the ctx internal endpoint (snowplow's SA endpoint), pointed at
// the provider upstream.
func (e *b903Env) resolve(ctx context.Context, kind string, background bool) (map[string]any, *cache.StageErrorSink) {
	if _, err := xcontext.UserInfo(ctx); err != nil {
		ctx = xcontext.BuildContext(ctx, xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User, Groups: []string{"devs"}}))
	}
	if background {
		ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: e.up.srv.URL, Token: "sa-token-903"})
	}
	ctx, sink := cache.WithStageErrorSink(ctx)
	dict := restactionsapi.Resolve(context.WithoutCancel(ctx), restactionsapi.ResolveOptions{
		RC: &rest.Config{Host: e.apiserver.URL}, AuthnNS: b903AuthnNS, Items: e.stages(kind),
		RESTActionNamespace: "ns-903", RESTActionName: "ra-903",
	})
	return dict, sink
}

var b903Kinds = []string{"own", "ref", "external"}

// b903DemandRevalidationCtx captures the ctx the REAL #547 demand revalidation
// hands to the re-entered handler.
func b903DemandRevalidationCtx(t *testing.T) context.Context {
	t.Helper()
	var got context.Context
	h := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Context() })
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	demandRevalidateOnce(h, req, "key-903", time.Now(), "test")
	if got == nil {
		t.Fatal("PRE: the demand revalidation never re-entered the handler")
	}
	return got
}

// b903HopCtx is the ctx of a request a background resolve made to snowplow
// itself, through the REAL ingest.
func b903HopCtx(t *testing.T) context.Context {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Header.Set(cache.BackgroundHeader, "1")
	req = req.WithContext(xcontext.BuildContext(req.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User, Groups: []string{"devs"}})))
	return reseedBackgroundFromHeaders(req).Context()
}

func b903Drivers(t *testing.T) map[string][]context.Context {
	out := map[string][]context.Context{}
	for name, arm := range driverCtxArms720 {
		out[name] = arm(t)
	}
	out["demand_revalidation"] = []context.Context{b903DemandRevalidationCtx(t)}
	out["background_hop"] = []context.Context{b903HopCtx(t)}
	return out
}

func TestIssue903_D1_EveryBackgroundDriverSendsNoWrite(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	drivers := b903Drivers(t)
	if len(drivers) < 8 {
		t.Fatalf("PRE: expected every #720 driver arm plus the demand revalidation and the hop, got %d", len(drivers))
	}
	for name, ctxs := range drivers {
		for i, dctx := range ctxs {
			if cache.NonCustomerWriteOrigin(dctx) == "" {
				t.Fatalf("PRE: driver %s ctx #%d classifies as a customer", name, i)
			}
			for _, kind := range b903Kinds {
				t.Run(name+"/"+kind, func(t *testing.T) {
					e := newB903Env(t)
					up := e.upstreamFor(kind)
					dict, sink := e.resolve(dctx, kind, true)
					if n := up.writes(); n != 0 {
						t.Fatalf("#903 RED: driver %s ctx #%d sent %d write request(s) through the %s endpoint", name, i, n, kind)
					}
					if kind != "own" && up.count(http.MethodGet) < 1 {
						t.Fatalf("PRE (non-vacuity): driver %s ctx #%d never dialed the %s read stage", name, i, kind)
					}
					if werr, _ := json.Marshal(dict["writeErr"]); !strings.Contains(string(werr), "background work never writes") {
						t.Fatalf("the refused write must be recorded under its errorKey: %s", werr)
					}
					if sink.Count() == 0 {
						t.Fatal("the refusal must bump the stage-error sink (every Put declines)")
					}
				})
			}
		}
	}
}

func TestIssue903_D2_CustomerRequestWritesExactlyOnce(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	for _, kind := range b903Kinds {
		t.Run(kind, func(t *testing.T) {
			e := newB903Env(t)
			up := e.upstreamFor(kind)
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User, Groups: []string{"devs"}}))
			if cache.NonCustomerWriteOrigin(ctx) != "" {
				t.Fatal("PRE: a plain request must classify as a customer")
			}
			dict, _ := e.resolve(ctx, kind, false)
			if n := up.count(http.MethodPost); n != 1 {
				t.Fatalf("#903: a customer request must send its write exactly once through the %s endpoint, got %d (writeErr=%v)",
					kind, n, dict["writeErr"])
			}
		})
	}
}

func TestIssue903_H1_ForgedHeaderOnlyRestrictsTheForgersOwnRequest(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	user := xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User, Groups: []string{"devs"}})
	reqWith := func(v string, withIdentity bool) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/call", nil)
		if v != "" {
			req.Header.Set(cache.BackgroundHeader, v)
		}
		if withIdentity {
			req = req.WithContext(xcontext.BuildContext(req.Context(), user))
		}
		return reseedBackgroundFromHeaders(req)
	}

	forged := reqWith("1", true)
	sibling := reqWith("", true)
	e := newB903Env(t)
	e.resolve(forged.Context(), "ref", false)
	if n := e.up.writes(); n != 0 {
		t.Fatalf("a request carrying X-Snowplow-Background: 1 must not write, sent %d", n)
	}
	e.resolve(sibling.Context(), "ref", false)
	if n := e.up.count(http.MethodPost); n != 1 {
		t.Fatalf("a sibling request without the header must still write exactly once, sent %d", n)
	}
	// Only "1" counts, and only behind a validated identity.
	for _, r := range []*http.Request{reqWith("0", true), reqWith("true", true), reqWith("1", false)} {
		if cache.BackgroundHop(r.Context()) {
			t.Fatalf("the hop marker was installed for %q (identity=%v)", r.Header.Get(cache.BackgroundHeader), r.Context() != nil)
		}
	}
	// It never relaxes and never changes anything else.
	hop := forged.Context()
	if cache.RefDenialOrigin(hop) != cache.RefDenialOriginServe || cache.Inert(hop) ||
		cache.BackgroundResolveFromContext(hop) || cache.PrewarmPathFromContext(hop) {
		t.Fatal("the hop marker must change nothing but the write refusal")
	}
	bg := refresherResolveCtx(context.Background())
	bgReq := httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(xcontext.BuildContext(bg, user))
	bgReq.Header.Set(cache.BackgroundHeader, "0")
	if cache.NonCustomerWriteOrigin(reseedBackgroundFromHeaders(bgReq).Context()) == "" {
		t.Fatal("a header value can never make a background resolve a customer")
	}
}

func TestIssue903_H2_HeaderOnlyToSnowplowItself(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	e := newB903Env(t)
	// The provider upstream plays snowplow itself; the external one another host.
	restactionsapi.SetSelfHost(e.up.srv.URL)
	t.Cleanup(func() { restactionsapi.SetSelfHost("") })

	bg := refresherResolveCtx(context.Background())
	e.resolve(bg, "ref", true)
	if got := e.up.header("GET /things/1"); got != "1" {
		t.Fatalf("a background resolve's call to snowplow itself must carry %s: 1, got %q", cache.BackgroundHeader, got)
	}

	// To another host: never, even when the RESTAction sets it itself.
	stages := e.stages("external")
	stages[0].Headers = []string{cache.BackgroundHeader + ": 1", "Accept: application/json"}
	ctx := xcontext.BuildContext(bg, xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User}))
	restactionsapi.Resolve(context.WithoutCancel(ctx), restactionsapi.ResolveOptions{
		RC: &rest.Config{Host: e.apiserver.URL}, AuthnNS: b903AuthnNS, Items: stages,
		RESTActionNamespace: "ns-903", RESTActionName: "ra-903"})
	if got, seen := e.ext.headerSeen("GET /things/1"); !seen || got != "" {
		t.Fatalf("#903 leak: an external endpoint received %s=%q (dialed=%v)", cache.BackgroundHeader, got, seen)
	}

	// A customer's self-loopback carries no header.
	e2 := newB903Env(t)
	restactionsapi.SetSelfHost(e2.up.srv.URL)
	cust := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: b903User}))
	e2.resolve(cust, "ref", false)
	if got := e2.up.header("GET /things/1"); got != "" {
		t.Fatalf("a customer's call to snowplow itself must not carry %s, got %q", cache.BackgroundHeader, got)
	}
}
