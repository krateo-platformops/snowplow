//go:build unit
// +build unit

package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/rest"
)

// external_fetch_cache_test.go — the external fetch with its conversion
// memoised by content (external_fetch_cache.go), driven over the real wire:
// every read goes through httpFetchExternal against an httptest server.
//
// The invariants under test: every read reaches the upstream; identical bytes
// convert once; a change upstream is in the next read; a 304 maps to the bytes
// the SAME request identity last saw; the memo is byte-bounded.

// extOrigin is a configurable upstream. body and etag are read per request;
// when etag is set, a matching If-None-Match gets a 304.
type extOrigin struct {
	srv         *httptest.Server
	requests    atomic.Int64
	full        atomic.Int64 // 200s written
	notModified atomic.Int64 // 304s written
	conditional atomic.Int64 // requests that carried If-None-Match / If-Modified-Since
	mu          sync.Mutex
	headers     map[string]string
	etag        func(r *http.Request) string
	body        func(r *http.Request) string
}

func newExtOrigin(t *testing.T) *extOrigin {
	t.Helper()
	o := &extOrigin{
		headers: map[string]string{},
		etag:    func(*http.Request) string { return "" },
		body:    func(*http.Request) string { return helmIndexYAML },
	}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.requests.Add(1)
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			o.conditional.Add(1)
		}
		o.mu.Lock()
		etag, body := o.etag(r), o.body(r)
		for k, v := range o.headers {
			w.Header().Set(k, v)
		}
		o.mu.Unlock()
		if etag != "" {
			w.Header().Set("ETag", etag)
			if r.Header.Get("If-None-Match") == etag {
				o.notModified.Add(1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.Header().Set("Content-Type", "text/yaml")
		o.full.Add(1)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(o.srv.Close)
	return o
}

func (o *extOrigin) setBody(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.body = func(*http.Request) string { return s }
}

func (o *extOrigin) setETag(s string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.etag = func(*http.Request) string { return s }
}

func (o *extOrigin) opts(ep endpoints.Endpoint, headers ...string) httpcall.RequestOptions {
	ep.ServerURL = o.srv.URL
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/index.yaml", Verb: ptr.To(http.MethodGet), Headers: headers},
		Endpoint:    &ep,
	}
}

func testExternalFetchCache(t *testing.T, budget int64) *externalFetchCache {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	c := newExternalFetchCache()
	c.budget = func(int64) int64 { return budget }
	return c
}

func mustFetch(t *testing.T, c *externalFetchCache, ctx context.Context, opts httpcall.RequestOptions) []byte {
	t.Helper()
	res, body, _, err := c.do(ctx, opts)
	if err != nil || res == nil || res.Status == response.StatusFailure {
		t.Fatalf("fetch failed: err=%v res=%+v", err, res)
	}
	return body
}

func mustConvert(t *testing.T, ct, doc string) []byte {
	t.Helper()
	out, err := toJSONBytes(ct, []byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Every read reaches the upstream, whatever caching headers it sends, and a
// change upstream is in the very next read.
func TestExternalFetchCache_ChangedUpstreamReflectedOnNextRead(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.headers["Cache-Control"] = "public, max-age=86400"
	o.headers["Expires"] = time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)

	v1 := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	o.setBody("apiVersion: v1\nentries:\n  changed: []\n")
	v2 := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if bytes.Equal(v1, v2) {
		t.Fatalf("the next read after an upstream change served the old bytes (time-based reuse)")
	}
	if !bytes.Equal(v2, mustConvert(t, "text/yaml", "apiVersion: v1\nentries:\n  changed: []\n")) {
		t.Fatalf("v2 is not the fresh conversion of the new document: %s", v2)
	}
	if n := o.requests.Load(); n != 2 {
		t.Fatalf("upstream saw %d requests for 2 reads; want 2 (every read fetches)", n)
	}
	// And with an ETag: a changed ETag is in the next read too.
	o.setETag(`"v3"`)
	o.setBody("apiVersion: v1\nentries:\n  v3: []\n")
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	o.setETag(`"v4"`)
	o.setBody("apiVersion: v1\nentries:\n  v4: []\n")
	v4 := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if !strings.Contains(string(v4), `"v4"`) {
		t.Fatalf("a changed ETag served stale bytes: %s", v4)
	}
}

// Identical bytes convert once, and the memoised bytes are byte-identical to a
// fresh conversion. Every read still fetches.
func TestExternalFetchCache_IdenticalBytesConvertOnce(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	want := mustConvert(t, "text/yaml", helmIndexYAML)
	for i := 0; i < 10; i++ {
		got := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
		if !bytes.Equal(got, want) {
			t.Fatalf("read %d: memoised bytes differ from a fresh conversion", i)
		}
	}
	if m, h := c.convertMisses.Load(), c.convertHits.Load(); m != 1 || h != 9 {
		t.Fatalf("conversions: miss=%d hit=%d; want 1/9", m, h)
	}
	if n := o.full.Load(); n != 10 {
		t.Fatalf("upstream served %d full bodies for 10 reads; want 10 (no fetch is skipped)", n)
	}
}

// Content-addressed sharing: two users with different credentials receiving
// the SAME bytes share one conversion; different bytes never do.
func TestExternalFetchCache_ConversionSharedOnlyForIdenticalBytes(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.body = func(r *http.Request) string {
		if r.Header.Get("Authorization") == "Bearer token-bob" {
			return "owner: bob\n"
		}
		return "owner: alice\n"
	}
	a := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{Token: "token-alice"}))
	b := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{Token: "token-bob"}))
	if !strings.Contains(string(a), "alice") || !strings.Contains(string(b), "bob") {
		t.Fatalf("LEAK: alice=%s bob=%s", a, b)
	}
	if m := c.convertMisses.Load(); m != 2 {
		t.Fatalf("misses=%d; want 2 (different bytes convert separately)", m)
	}
	o.setBody("shared: true\n")
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{Token: "token-alice"}))
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{Token: "token-bob"}))
	if m, h := c.convertMisses.Load(), c.convertHits.Load(); m != 3 || h != 1 {
		t.Fatalf("misses=%d hits=%d; want 3/1 (identical bytes shared across users)", m, h)
	}
}

// Conditional requests: every read is a live conditional GET; a 304 serves
// exactly the bytes that came with that validator; a new version is served at
// once.
func TestExternalFetchCache_ETagConditionalEveryRead(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.setETag(`"v1"`)
	first := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	for i := 0; i < 4; i++ {
		got := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
		if !bytes.Equal(got, first) {
			t.Fatalf("304 read %d served different bytes", i)
		}
	}
	if r, nm := o.requests.Load(), o.notModified.Load(); r != 5 || nm != 4 {
		t.Fatalf("requests=%d 304s=%d; want 5/4 (a live conditional check on every read)", r, nm)
	}
	if nm := c.notModified.Load(); nm != 4 {
		t.Fatalf("not_modified counter=%d; want 4", nm)
	}
	if m := c.convertMisses.Load(); m != 1 {
		t.Fatalf("converted %d times; want 1", m)
	}
}

// A JSON passthrough body with an ETag: a 304 maps to those exact bytes.
func TestExternalFetchCache_ETagOnJSONPassthrough(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("ETag", `"j1"`)
		if r.Header.Get("If-None-Match") == `"j1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"a":1}`)
	}))
	t.Cleanup(srv.Close)
	ep := endpoints.Endpoint{ServerURL: srv.URL}
	op := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodGet)}, Endpoint: &ep}
	for i := 0; i < 3; i++ {
		if got := mustFetch(t, c, context.Background(), op); string(got) != `{"a":1}` {
			t.Fatalf("read %d: %s", i, got)
		}
	}
	if hits.Load() != 3 || c.notModified.Load() != 2 {
		t.Fatalf("requests=%d 304s=%d; want 3/2", hits.Load(), c.notModified.Load())
	}
}

// THE VALIDATOR LEAK TEST. The upstream answers 304 to ANY request carrying the
// right ETag, whoever sends it (a naive server). Alice reads first; Bob — other
// credentials, same URL — must not send Alice's validator, or he is served
// Alice's bytes. Red if the request identity drops the credential.
func TestExternalFetchCache_ValidatorNeverCrossesIdentities(t *testing.T) {
	for _, tc := range []struct {
		name       string
		alice, bob func(o *extOrigin) httpcall.RequestOptions
		ctxAlice   context.Context
		ctxBob     context.Context
		who        func(r *http.Request) string
	}{
		{
			name:  "endpoint-token",
			alice: func(o *extOrigin) httpcall.RequestOptions { return o.opts(endpoints.Endpoint{Token: "token-alice"}) },
			bob:   func(o *extOrigin) httpcall.RequestOptions { return o.opts(endpoints.Endpoint{Token: "token-bob"}) },
			who:   func(r *http.Request) string { return r.Header.Get("Authorization") },
		},
		{
			name: "basic-password",
			alice: func(o *extOrigin) httpcall.RequestOptions {
				return o.opts(endpoints.Endpoint{Username: "u", Password: "pw-alice"})
			},
			bob: func(o *extOrigin) httpcall.RequestOptions {
				return o.opts(endpoints.Endpoint{Username: "u", Password: "pw-bob"})
			},
			who: func(r *http.Request) string { _, p, _ := r.BasicAuth(); return p },
		},
		{
			name: "exportJwt",
			alice: func(o *extOrigin) httpcall.RequestOptions {
				return o.opts(endpoints.Endpoint{}, "Authorization: Bearer jwt-alice")
			},
			bob: func(o *extOrigin) httpcall.RequestOptions {
				return o.opts(endpoints.Endpoint{}, "Authorization: Bearer jwt-bob")
			},
			ctxAlice: xcontext.BuildContext(context.Background(), xcontext.WithAccessToken("jwt-alice")),
			ctxBob:   xcontext.BuildContext(context.Background(), xcontext.WithAccessToken("jwt-bob")),
			who:      func(r *http.Request) string { return r.Header.Get("Authorization") },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testExternalFetchCache(t, 1<<20)
			o := newExtOrigin(t)
			o.setETag(`"same-etag"`)
			o.body = func(r *http.Request) string { return fmt.Sprintf("owner: %q\n", tc.who(r)) }
			ca, cb := tc.ctxAlice, tc.ctxBob
			if ca == nil {
				ca, cb = context.Background(), context.Background()
			}
			a := string(mustFetch(t, c, ca, tc.alice(o)))
			b := string(mustFetch(t, c, cb, tc.bob(o)))
			if !strings.Contains(a, "alice") || !strings.Contains(b, "bob") {
				t.Fatalf("LEAK: a validator crossed identities: alice=%s bob=%s", a, b)
			}
			// Each identity does reuse its OWN validator.
			if a2 := string(mustFetch(t, c, ca, tc.alice(o))); a2 != a {
				t.Fatalf("alice's own 304 served %s; want %s", a2, a)
			}
			if o.notModified.Load() != 1 {
				t.Fatalf("304s=%d; want 1 (alice's second read only)", o.notModified.Load())
			}
		})
	}
}

// Every Endpoint field is in the request identity: changing any one changes
// it. Red if the fold skips a field — including one added to the struct later.
func TestExternalFetchCache_RequestKeyFoldsEveryEndpointField(t *testing.T) {
	base := endpoints.Endpoint{ServerURL: "https://example.test"}
	opts := func(ep endpoints.Endpoint) httpcall.RequestOptions {
		return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/x"}, Endpoint: &ep}
	}
	k0 := externalRequestKeyOf(opts(base))
	rt := reflect.TypeOf(base)
	for i := 0; i < rt.NumField(); i++ {
		ep := base
		f := reflect.ValueOf(&ep).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString(f.String() + "-changed")
		case reflect.Bool:
			f.SetBool(!f.Bool())
		default:
			t.Fatalf("Endpoint.%s has kind %s the test cannot vary", rt.Field(i).Name, f.Kind())
		}
		if externalRequestKeyOf(opts(ep)) == k0 {
			t.Errorf("Endpoint.%s is not folded into the request identity", rt.Field(i).Name)
		}
	}
	for name, mut := range map[string]func(*httpcall.RequestOptions){
		"path":    func(o *httpcall.RequestOptions) { o.Path = "/y" },
		"verb":    func(o *httpcall.RequestOptions) { o.Verb = ptr.To(http.MethodPost) },
		"payload": func(o *httpcall.RequestOptions) { o.Payload = ptr.To("{}") },
		"header":  func(o *httpcall.RequestOptions) { o.Headers = []string{"X-Tenant: a"} },
	} {
		o := opts(base)
		mut(&o)
		if externalRequestKeyOf(o) == k0 {
			t.Errorf("request %s is not folded into the request identity", name)
		}
	}
	a, b := opts(base), opts(base)
	a.Headers = []string{"X-A: 1", "X-A: 2"}
	b.Headers = []string{"X-A: 2", "X-A: 1"}
	if externalRequestKeyOf(a) == externalRequestKeyOf(b) {
		t.Errorf("reordered repeated headers share an identity; they send different values")
	}
}

// Validators are never sent where the identity lives in the ctx, on a write, or
// when the stage sets its own conditional/range headers.
func TestExternalFetchCache_ConditionalEligibility(t *testing.T) {
	ep := endpoints.Endpoint{ServerURL: "https://example.test"}
	get := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/x", Verb: ptr.To(http.MethodGet)}, Endpoint: &ep}
	bg := context.Background()
	if !externalConditionalEligible(bg, get) {
		t.Fatal("a plain GET must be eligible")
	}
	post := get
	post.Verb = ptr.To(http.MethodPost)
	hdr := func(h string) httpcall.RequestOptions { o := get; o.Headers = []string{h}; return o }
	for name, tc := range map[string]struct {
		ctx  context.Context
		opts httpcall.RequestOptions
	}{
		"post":            {bg, post},
		"sa-dial":         {cache.WithServiceAccountDial(bg), get},
		"held-credential": {credhttp.WithHeldCredential(bg), get},
		"impersonated":    {withEndpointImpersonation(bg, rest.ImpersonationConfig{UserName: "u"}), get},
		"if-none-match":   {bg, hdr("If-None-Match: x")},
		"range":           {bg, hdr("Range: bytes=0-1")},
	} {
		if externalConditionalEligible(tc.ctx, tc.opts) {
			t.Errorf("%s: eligible; must not send validators", name)
		}
	}
}

// A validator whose bytes were evicted: the read is re-made unconditionally and
// serves the current bytes.
func TestExternalFetchCache_EvictedBytesRefetchUnconditionally(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.setETag(`"v1"`)
	first := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	c.mu.Lock()
	for _, conv := range c.conversions {
		c.removeLocked(conv)
	}
	c.mu.Unlock()
	got := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if !bytes.Equal(got, first) {
		t.Fatalf("refetch after eviction served different bytes")
	}
	if r, nm := o.requests.Load(), o.notModified.Load(); r != 3 || nm != 1 {
		t.Fatalf("requests=%d 304s=%d; want 3/1 (conditional 304, then an unconditional re-read)", r, nm)
	}
}

// The memo is byte-bounded: a conversion over the budget is not stored, and
// storing past the budget evicts the least recently used.
func TestExternalFetchCache_Bounded(t *testing.T) {
	doc := func(i int) string { return fmt.Sprintf("apiVersion: v1\nentries:\n  chart-%d: [a, b, c]\n", i) }
	one := int64(len(mustConvert(t, "text/yaml", doc(0)))) + int64(len(externalContentKey{}))
	c := testExternalFetchCache(t, 2*one+one/2)
	o := newExtOrigin(t)
	for i := 0; i < 3; i++ {
		o.setBody(doc(i))
		mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	}
	conv, _, b := c.held()
	if conv != 2 || b > 2*one+one/2 {
		t.Fatalf("held %d conversions / %d bytes; want 2 within %d", conv, b, 2*one+one/2)
	}
	if e := c.evictions.Load(); e != 1 {
		t.Fatalf("evictions=%d; want 1", e)
	}
	o.setBody(doc(0))
	before := c.convertMisses.Load()
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if c.convertMisses.Load() != before+1 {
		t.Fatalf("doc 0 was the least recently used and must have been evicted")
	}

	small := testExternalFetchCache(t, one-1)
	mustFetch(t, small, context.Background(), o.opts(endpoints.Endpoint{}))
	if conv, val, b := small.held(); conv != 0 || val != 0 || b != 0 {
		t.Fatalf("an over-budget conversion was stored: %d/%d/%d", conv, val, b)
	}
}

// Concurrent conversions of identical bytes single-flight.
func TestExternalFetchCache_ConversionSingleFlight(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	gate := make(chan struct{})
	var calls atomic.Int64
	c.toJSON = func(ct string, dat []byte) ([]byte, error) {
		calls.Add(1)
		<-gate
		return toJSONBytes(ct, dat)
	}
	const n = 16
	var wg sync.WaitGroup
	outs := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out, _, _, err := c.convert("text/yaml", []byte(helmIndexYAML))
			if err != nil {
				t.Error(err)
			}
			outs[i] = out
		}(i)
	}
	for calls.Load() < 1 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let every caller join the flight
	close(gate)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("converted %d times for %d concurrent identical bodies; want 1", calls.Load(), n)
	}
	for i := range outs {
		if !bytes.Equal(outs[i], outs[0]) {
			t.Fatalf("caller %d got different bytes", i)
		}
	}
}

// Conversion errors are never stored: malformed YAML is converted (and fails)
// on every read.
func TestExternalFetchCache_ConversionErrorsNotStored(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.setBody("key: [unclosed\n")
	for i := 0; i < 3; i++ {
		res, _, _, _ := c.do(context.Background(), o.opts(endpoints.Endpoint{}))
		if res.Status != response.StatusFailure {
			t.Fatalf("malformed YAML came back as %s", res.Status)
		}
	}
	if m := c.convertMisses.Load(); m != 3 {
		t.Fatalf("misses=%d; want 3 (a failed conversion is not stored)", m)
	}
	if conv, _, _ := c.held(); conv != 0 {
		t.Fatalf("a failed conversion was stored")
	}
}

// With the cache off, the layer steps aside entirely.
func TestExternalFetchCache_CacheOffStepsAside(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	t.Setenv("CACHE_ENABLED", "false")
	o := newExtOrigin(t)
	o.setETag(`"v1"`)
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if o.conditional.Load() != 0 || c.convertMisses.Load() != 0 {
		t.Fatalf("cache off: conditional=%d misses=%d; want 0/0", o.conditional.Load(), c.convertMisses.Load())
	}
}

// END TO END through api.Resolve: a stage that dials snowplow's own SA endpoint
// (the ctx internal endpoint) reaches the layer; it is fetched every time,
// never sends a validator (its identity lives in the ctx), and its conversion
// is memoised by content.
func TestExternalFetchCache_E2E_SADial(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	var hits, conditional atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
		}
		w.Header().Set("ETag", `"e2e"`)
		w.Header().Set("Content-Type", "text/yaml")
		fmt.Fprint(w, "ok: true\nname: e2e-sa\n")
	}))
	t.Cleanup(srv.Close)
	missBefore, hitBefore := externalFetches.convertMisses.Load(), externalFetches.convertHits.Load()
	stage := &templates.API{Name: "j", Path: "/e2e-sa.yaml", Verb: ptr.To(http.MethodGet), Filter: ptr.To(".j")}
	for i := 0; i < 2; i++ {
		dict := runAC6Resolve(&ac6Fixture{server: srv}, []*templates.API{stage})
		if got, ok := dict["j"].(map[string]any); !ok || got["ok"] != true || got["name"] != "e2e-sa" {
			t.Fatalf("resolve %d: dict[j]=%#v", i, dict["j"])
		}
	}
	if hits.Load() != 2 || conditional.Load() != 0 {
		t.Fatalf("upstream requests=%d conditional=%d; want 2/0", hits.Load(), conditional.Load())
	}
	if m, h := externalFetches.convertMisses.Load()-missBefore, externalFetches.convertHits.Load()-hitBefore; m != 1 || h != 1 {
		t.Fatalf("conversion miss/hit delta = %d/%d; want 1/1 (the resolve reaches sharedExternalFetch)", m, h)
	}
}
