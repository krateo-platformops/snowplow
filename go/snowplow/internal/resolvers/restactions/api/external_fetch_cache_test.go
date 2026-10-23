//go:build unit
// +build unit

package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// external_fetch_cache_test.go — the external fetch with its conversion
// memoised by content (external_fetch_cache.go), driven over the real wire:
// every read goes through httpFetchConverting against an httptest server.
//
// The invariants: every read is a full, unconditional fetch; a change upstream
// is in the next read; identical bytes convert once and the memoised bytes are
// byte-identical to a fresh conversion; the memo is byte-bounded and shrinks
// with its budget.

// extOrigin is a configurable upstream that also sends an ETag and a
// Last-Modified on every answer — validators the fetch must never send back.
type extOrigin struct {
	srv         *httptest.Server
	requests    atomic.Int64
	conditional atomic.Int64 // requests carrying If-None-Match / If-Modified-Since
	mu          sync.Mutex
	headers     map[string]string
	body        func(r *http.Request) string
}

func newExtOrigin(t *testing.T) *extOrigin {
	t.Helper()
	o := &extOrigin{headers: map[string]string{}, body: func(*http.Request) string { return helmIndexYAML }}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.requests.Add(1)
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			o.conditional.Add(1)
		}
		o.mu.Lock()
		body := o.body(r)
		for k, v := range o.headers {
			w.Header().Set(k, v)
		}
		o.mu.Unlock()
		w.Header().Set("ETag", `"constant"`)
		w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
		w.Header().Set("Content-Type", "text/yaml")
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

// Every read is a full, unconditional fetch — whatever caching headers and
// validators the upstream sends — and a change upstream is in the next read.
func TestExternalFetchCache_ChangedUpstreamReflectedOnNextRead(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	o := newExtOrigin(t)
	o.headers["Cache-Control"] = "public, max-age=86400"
	o.headers["Expires"] = time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)

	v1 := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	o.setBody("apiVersion: v1\nentries:\n  changed: []\n")
	v2 := mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if bytes.Equal(v1, v2) {
		t.Fatalf("the next read after an upstream change served the old bytes")
	}
	if !bytes.Equal(v2, mustConvert(t, "text/yaml", "apiVersion: v1\nentries:\n  changed: []\n")) {
		t.Fatalf("v2 is not the fresh conversion of the new document: %s", v2)
	}
	if r, cond := o.requests.Load(), o.conditional.Load(); r != 2 || cond != 0 {
		t.Fatalf("upstream saw %d requests (%d conditional) for 2 reads; want 2 full fetches, 0 conditional", r, cond)
	}
}

// Identical bytes convert once; the memoised bytes are byte-identical to a
// fresh conversion; every read still fetches the full body.
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
	if r, cond := o.requests.Load(), o.conditional.Load(); r != 10 || cond != 0 {
		t.Fatalf("upstream saw %d requests (%d conditional) for 10 reads; want 10/0", r, cond)
	}
}

// THE LEAK TEST. Two users with different credentials, receiving DIFFERENT
// bytes, each get their own conversion; receiving the SAME bytes, they share
// one. Red if the key stops being the content.
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

// The #845 review's freshness probe, adopted: an upstream served by Go's own
// http.ServeContent (what http.FileServer does for a static file), which sends
// Last-Modified and would answer If-Modified-Since with 304 at one-second
// resolution — including for a file rolled back to an OLDER mtime.
type reviewer845File struct {
	mu    sync.Mutex
	body  string
	mtime time.Time
}

func (f *reviewer845File) set(body string, mtime time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body, f.mtime = body, mtime
}

func reviewer845Server(t *testing.T, f *reviewer845File) (*httptest.Server, *atomic.Int64) {
	var notModified atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		body, mtime := f.body, f.mtime
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/yaml")
		rec := httptest.NewRecorder()
		http.ServeContent(rec, r, "index.yaml", mtime, strings.NewReader(body))
		if rec.Code == http.StatusNotModified {
			notModified.Add(1)
		}
		for k, vs := range rec.Header() {
			w.Header()[k] = vs
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	t.Cleanup(srv.Close)
	return srv, &notModified
}

func reviewer845Opts(url string) httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{Path: "/index.yaml", Verb: ptr.To(http.MethodGet)},
		Endpoint:    &endpoints.Endpoint{ServerURL: url},
	}
}

func TestReviewer845_ChangedBodySameSecond(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	f := &reviewer845File{}
	srv, notModified := reviewer845Server(t, f)
	t0 := time.Date(2026, 10, 22, 12, 0, 0, 100e6, time.UTC)
	f.set("version: one\n", t0)
	mustFetch(t, c, context.Background(), reviewer845Opts(srv.URL))
	f.set("version: two\n", t0.Add(500*time.Millisecond)) // edited again within the same second
	v2 := mustFetch(t, c, context.Background(), reviewer845Opts(srv.URL))
	if !bytes.Contains(v2, []byte("two")) {
		t.Errorf("STALE: upstream body changed, the read served %s", v2)
	}
	if n := notModified.Load(); n != 0 {
		t.Errorf("upstream answered %d 304s; a read must never be conditional", n)
	}
}

func TestReviewer845_RolledBackFileOlderMtime(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	f := &reviewer845File{}
	srv, notModified := reviewer845Server(t, f)
	t0 := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	f.set("version: new\n", t0)
	mustFetch(t, c, context.Background(), reviewer845Opts(srv.URL))
	f.set("version: old\n", t0.Add(-time.Hour)) // a rollback restores the older file and mtime
	for i := 0; i < 3; i++ {
		if got := mustFetch(t, c, context.Background(), reviewer845Opts(srv.URL)); !bytes.Contains(got, []byte("old")) {
			t.Fatalf("STALE: read %d after the rollback served %s", i, got)
		}
	}
	if n := notModified.Load(); n != 0 {
		t.Errorf("upstream answered %d 304s; a read must never be conditional", n)
	}
}

// The mode is part of the content key: the same bytes under the two modes never
// share an entry.
func TestExternalFetchCache_ContentKeyFoldsMode(t *testing.T) {
	b := []byte("a: 1\n")
	if externalContentKeyOf(true, b) == externalContentKeyOf(false, b) {
		t.Fatal("the content key ignores the conversion mode")
	}
	if externalContentKeyOf(true, b) == externalContentKeyOf(true, []byte("a: 2\n")) {
		t.Fatal("the content key ignores the bytes")
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
	n, b := c.held()
	if n != 2 || b > 2*one+one/2 {
		t.Fatalf("held %d conversions / %d bytes; want 2 within %d", n, b, 2*one+one/2)
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
	if n, b := small.held(); n != 0 || b != 0 {
		t.Fatalf("an over-budget conversion was stored: %d/%d", n, b)
	}
}

// The memo shrinks when its budget drops, on a HIT — not only on its next
// store. Every read here is a hit, and nothing new is ever stored.
func TestExternalFetchCache_ShrinksWhenBudgetDropsOnHits(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	var budget atomic.Int64
	budget.Store(1 << 20)
	c.budget = func(int64) int64 { return budget.Load() }
	o := newExtOrigin(t)
	docs := []string{"a: [1]\n", "b: [2]\n", "c: [3]\n"}
	for _, d := range docs {
		o.setBody(d)
		mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	}
	if n, _ := c.held(); n != 3 {
		t.Fatalf("held %d; want 3", n)
	}
	missBefore := c.convertMisses.Load()
	budget.Store(0) // the heap filled: no room at all
	o.setBody(docs[2])
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if n, b := c.held(); n != 0 || b != 0 {
		t.Fatalf("after the budget dropped to 0, a read left %d entries / %d bytes held; want 0", n, b)
	}
	if c.convertMisses.Load() != missBefore+1 {
		t.Fatalf("the read must convert afresh once its entry is gone")
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
			out, err := c.convert("text/yaml", []byte(helmIndexYAML))
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
	if n, _ := c.held(); n != 0 {
		t.Fatalf("a failed conversion was stored")
	}
}

// With the cache off, the memo steps aside entirely.
func TestExternalFetchCache_CacheOffStepsAside(t *testing.T) {
	c := testExternalFetchCache(t, 1<<20)
	t.Setenv("CACHE_ENABLED", "false")
	o := newExtOrigin(t)
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	mustFetch(t, c, context.Background(), o.opts(endpoints.Endpoint{}))
	if m, h := c.convertMisses.Load(), c.convertHits.Load(); m != 0 || h != 0 {
		t.Fatalf("cache off: misses=%d hits=%d; want 0/0", m, h)
	}
}

// END TO END through api.Resolve: an external stage reaches the memo; it is
// fetched in full every time and its conversion is memoised by content.
func TestExternalFetchCache_E2E_Resolve(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ac6FailFast(t)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	var hits, conditional atomic.Int64
	// A body unique to this run: the process-wide memo outlives a -count=N run.
	nonce := time.Now().UnixNano()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			conditional.Add(1)
		}
		w.Header().Set("ETag", `"e2e"`)
		w.Header().Set("Content-Type", "text/yaml")
		fmt.Fprintf(w, "ok: true\nname: e2e-resolve\nnonce: %d\n", nonce)
	}))
	t.Cleanup(srv.Close)
	missBefore, hitBefore := externalFetches.convertMisses.Load(), externalFetches.convertHits.Load()
	stage := &templates.API{Name: "j", Path: "/e2e.yaml", Verb: ptr.To(http.MethodGet), Filter: ptr.To(".j")}
	for i := 0; i < 2; i++ {
		dict := runAC6Resolve(&ac6Fixture{server: srv}, []*templates.API{stage})
		if got, ok := dict["j"].(map[string]any); !ok || got["ok"] != true || got["name"] != "e2e-resolve" {
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
