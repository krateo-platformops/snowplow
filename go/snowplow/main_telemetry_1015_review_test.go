package main

// main_telemetry_1015_review_test.go — #1020 review (FAIL at 4d05e813): the
// arms for F1 (memory bounded by caps while decoding, global decode bound),
// F2 (rules no arm covered: M2, M3, M10, M11, M16, M18; M5 is in
// internal/telemetry/rum/limiter_test.go), F4 (refusals attributable) and F6
// (global spans/s cap, dropped_auth counted once). Same harness as
// main_telemetry_1015_test.go: the real router, RefreshAuth, Ingest and
// Forwarder; only the collector and the signing key are doubles.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"
)

// Documented limits (howto/rum-ingest.md), as literals so an arm can never
// loosen together with the constant it checks.
const (
	docSpanCap       = 512
	docAttrCap       = 64
	docGlobalBurst   = 2048
	docSpansPerBatch = 512
)

// countingBody counts the bytes the server pulls from the client.
type countingBody struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}
func (c *countingBody) Close() error { return nil }

func postRUMRequest(t *testing.T, h http.Handler, req *http.Request, user string) *httptest.ResponseRecorder {
	t.Helper()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mintRUMToken(t, user))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// spansBody: n valid krateo.widget.fetch spans under the krateo-frontend resource.
func spansBody(t *testing.T, n int) []byte {
	t.Helper()
	spans := make([]otlpSpan, n)
	for i := range spans {
		spans[i] = widgetFetch()
	}
	return otlpBody(t, rum.ServiceName, nil, spans...)
}

// emptySpansBody is the review's F1 shape: ~size bytes of `{}` spans.
func emptySpansBody(size int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"resourceSpans":[{"scopeSpans":[{"spans":[{}`)
	for b.Len() < size-16 {
		b.WriteString(`,{}`)
	}
	b.WriteString(`]}]}]}`)
	return b.Bytes()
}

// manyAttributesBody is the review's second F1 shape: one allowlisted span
// carrying ~size bytes of empty attributes.
func manyAttributesBody(size int) []byte {
	var b bytes.Buffer
	b.WriteString(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"krateo-frontend"}}]},"scopeSpans":[{"spans":[{"name":"krateo.widget.fetch","attributes":[{}`)
	for b.Len() < size-16 {
		b.WriteString(`,{}`)
	}
	b.WriteString(`]}]}]}]}`)
	return b.Bytes()
}

// F1 — what one request may allocate is bounded by the caps, not by the shape
// of a body under the byte cap. The review measured 109 MiB allocated for 512
// KiB of empty spans (decoded in full before the span count was checked) and
// 279 MiB for ten requests of one span with 174K attributes, all 200.
//
// BOUND: 4 MiB allocated per request, averaged over 5 requests, for each
// shape (both are refused with 413 at cap+1). That is 8x the body; the
// measured cost is the body-sized decoder buffer, JWT validation and the
// recorder, and it does not grow with the element count.
//
// MUTATION (RED, shown in the PR): the span cap in decodeCapped raised to
// 1<<30 (the old decode-then-count behaviour): the empty-spans shape allocates
// tens of MiB and is not refused.
func TestS1015_DecodeAllocationIsBoundedByCaps(t *testing.T) {
	const bound = 4 << 20
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	for _, shape := range []struct {
		name string
		body []byte
	}{
		{"512KiB of empty spans", emptySpansBody(documentedBodyCap)},
		{"one span with ~174K empty attributes", manyAttributesBody(documentedBodyCap)},
	} {
		t.Run(shape.name, func(t *testing.T) {
			if len(shape.body) > documentedBodyCap {
				t.Fatalf("SETUP: body %d bytes is over the cap; it would be refused before decoding", len(shape.body))
			}
			const runs = 5
			tokens := make([]string, runs)
			for i := range tokens {
				tokens[i] = mintRUMToken(t, fmt.Sprintf("alloc-%s-%d", shape.name[:3], i))
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for i := 0; i < runs; i++ {
				req := httptest.NewRequest(http.MethodPost, rum.Path, bytes.NewReader(shape.body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+tokens[i])
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusRequestEntityTooLarge {
					t.Fatalf("run %d: status %d, want 413 (a structural cap is exceeded)", i, rec.Code)
				}
			}
			runtime.ReadMemStats(&after)
			per := (after.TotalAlloc - before.TotalAlloc) / runs
			t.Logf("%s: %d bytes body, %.2f MiB allocated per request (bound %d MiB)",
				shape.name, len(shape.body), float64(per)/(1<<20), bound>>20)
			if per > bound {
				t.Fatalf("%s allocated %.1f MiB per request, bound %d MiB: memory follows the input shape, not the caps",
					shape.name, float64(per)/(1<<20), bound>>20)
			}
		})
	}
}

// F1 — decodes in flight are bounded across ALL users: with every slot held
// by a request still reading its body, the next request (another user, a
// normal body) is refused 429 with Retry-After at once, and counted busy.
//
// MUTATION (RED, shown in the PR): the decode-slot acquire removed → the extra
// request is decoded and answered 200.
func TestS1015_DecodesInFlightAreBoundedGlobally(t *testing.T) {
	prev := runtime.GOMAXPROCS(2) // the ingest sizes its slots from GOMAXPROCS at construction
	defer runtime.GOMAXPROCS(prev)
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	const slots = 2
	var wg sync.WaitGroup
	writers := make([]*io.PipeWriter, slots)
	for i := 0; i < slots; i++ {
		pr, pw := io.Pipe()
		writers[i] = pw
		req := httptest.NewRequest(http.MethodPost, rum.Path, pr)
		req.ContentLength = -1
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+mintRUMToken(t, fmt.Sprintf("slow-%d", i)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeHTTP(httptest.NewRecorder(), req)
		}()
		// The write returns once the decoder has read it: the request then
		// holds a slot and waits for more body.
		if _, err := pw.Write([]byte(`{"resourceSpans":[`)); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for _, pw := range writers {
			_ = pw.CloseWithError(io.ErrUnexpectedEOF)
		}
		wg.Wait()
	}()

	before := rum.Read()
	start := time.Now()
	rec := postRUM(t, h, mintRUMToken(t, "fast-user"), otlpBody(t, rum.ServiceName, nil, widgetFetch()))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("with every decode slot held, a new request got %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("busy 429 without Retry-After")
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("the busy refusal took %v; it must not wait for a slot", el)
	}
	if d := rum.Read().Requests["dropped_busy"] - before.Requests["dropped_busy"]; d != 1 {
		t.Fatalf("dropped_busy delta %d, want 1", d)
	}
}

// F2 / M2 — a body with no Content-Length is still bounded: it is read through
// MaxBytesReader, refused 413, and no more than cap+1 bytes are pulled from
// the client. The fixture is ONE valid span whose 4 MiB attribute value is
// within every structural cap, so only the byte cap can stop it.
//
// MUTATION (RED, shown in the PR): http.MaxBytesReader removed → the whole
// 4 MiB is read and the request answered 200.
func TestS1015_ChunkedBodyIsBounded(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	s := widgetFetch()
	s.attrs = append(s.attrs, strAttr("padding", strings.Repeat("x", 4<<20)))
	body := &countingBody{r: bytes.NewReader(otlpBody(t, rum.ServiceName, nil, s))}
	req := httptest.NewRequest(http.MethodPost, rum.Path, nil)
	req.Body = body
	req.ContentLength = -1 // chunked: no declared length for the precheck
	if rec := postRUMRequest(t, h, req, "chunked-user"); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked 4 MiB body: status %d, want 413", rec.Code)
	}
	if n := body.n.Load(); n > documentedBodyCap+1 {
		t.Fatalf("read %d bytes of a chunked body, cap %d: the body is not bounded", n, documentedBodyCap)
	}
}

// F2 / M3 — the structural caps, at the documented values: 512 spans and 64
// attributes per span pass, one more is 413.
//
// MUTATION (RED, shown in the PR): MaxSpansPerRequest raised to 1<<30
// (513 spans answered 200).
func TestS1015_SpanAndAttributeCaps(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	if rec := postRUM(t, h, mintRUMToken(t, "cap-a"), spansBody(t, docSpanCap)); rec.Code != http.StatusOK {
		t.Fatalf("%d spans: status %d, want 200", docSpanCap, rec.Code)
	}
	if rec := postRUM(t, h, mintRUMToken(t, "cap-b"), spansBody(t, docSpanCap+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("%d spans: status %d, want 413", docSpanCap+1, rec.Code)
	}
	withAttrs := func(n int) []byte {
		s := widgetFetch()
		for len(s.attrs) < n {
			s.attrs = append(s.attrs, strAttr(fmt.Sprintf("extra.%d", len(s.attrs)), "v"))
		}
		return otlpBody(t, rum.ServiceName, nil, s)
	}
	if rec := postRUM(t, h, mintRUMToken(t, "cap-c"), withAttrs(docAttrCap)); rec.Code != http.StatusOK {
		t.Fatalf("%d attributes: status %d, want 200", docAttrCap, rec.Code)
	}
	if rec := postRUM(t, h, mintRUMToken(t, "cap-d"), withAttrs(docAttrCap+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("%d attributes: status %d, want 413", docAttrCap+1, rec.Code)
	}
}

// routeKept posts one widget-fetch span with http.route=route and reports
// whether the forwarded span carries it.
func routeKept(t *testing.T, h http.Handler, c *collector, user, route string) bool {
	t.Helper()
	s := otlpSpan{name: "krateo.widget.fetch", attrs: []map[string]any{strAttr("http.route", route)}}
	if rec := postRUM(t, h, mintRUMToken(t, user), otlpBody(t, rum.ServiceName, nil, s)); rec.Code != http.StatusOK {
		t.Fatalf("route %q: status %d, want 200", route, rec.Code)
	}
	got := awaitExport(t, c)
	for _, a := range got.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0].GetAttributes() {
		if a.GetKey() == rum.AttrRoute {
			return true
		}
	}
	return false
}

// F2 / M10 — the character rule in isolation: short routes whose ONLY defect
// is a query string, '=' or '&' (no segment near the token length) are
// dropped; the plain route is kept.
//
// MUTATION (RED, shown in the PR): '?', '=' and '&' admitted to the route
// character set.
func TestS1015_RouteQueryCharactersRuleAlone(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	if !routeKept(t, h, c, "q-ctrl", "/call") {
		t.Fatal("control: /call was dropped")
	}
	for i, route := range []string{"/call?a=1", "/a=b", "/a&b"} {
		if routeKept(t, h, c, fmt.Sprintf("q-%d", i), route) {
			t.Errorf("route %q was forwarded; a query string, '=' or '&' is never a template", route)
		}
	}
}

// F2 / M11 — the token-length rule alone: a 24-character lowercase segment
// (no dot, so the JWT-shape rule cannot apply; only allowed characters) is
// dropped, and the 23-character one is kept, at the documented boundary.
//
// MUTATION (RED, shown in the PR): the 24-character segment rule turned off.
func TestS1015_RouteTokenLengthRuleAlone(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	if !routeKept(t, h, c, "len-23", "/s/"+strings.Repeat("a", 23)) {
		t.Fatal("control: a 23-character segment was dropped")
	}
	if routeKept(t, h, c, "len-24", "/s/"+strings.Repeat("a", 24)) {
		t.Fatal("a 24-character segment was forwarded; it is token-shaped by length")
	}
}

// unsetenv clears k for the test (t.Setenv restores the original afterwards).
func unsetenv(t *testing.T, k string) {
	t.Helper()
	t.Setenv(k, "")
	_ = os.Unsetenv(k)
}

// F2 / M16 — the default-off contract (Diego, 2026-10-30: on whenever tracing
// is on, no switch of its own), through newRUMIngest, the function main runs:
// on ONLY when tracing resolves to enabled AND a collector endpoint is set;
// off, the route is a 404 to an authenticated caller.
//
// MUTATION (RED, shown in the PR): rum.CollectorURL ignoring tracing.Enabled().
func TestS1015_DefaultOffContract(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	cases := []struct {
		name                       string
		enabled, tracing, endpoint string // "" = unset
		wantOn                     bool
	}{
		{"nothing set", "", "", "", false},
		{"endpoint only", "", "", c.srv.URL, false},
		{"tracing on, no endpoint", "true", "", "", false},
		{"master on, tracing off, endpoint", "true", "false", c.srv.URL, false},
		{"master off, endpoint", "false", "", c.srv.URL, false},
		{"tracing on + endpoint", "true", "", c.srv.URL, true},
		{"per-signal on + endpoint", "", "true", c.srv.URL, true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range map[string]string{"OTEL_ENABLED": tc.enabled, "OTEL_TRACING_ENABLED": tc.tracing, "OTEL_EXPORTER_OTLP_ENDPOINT": tc.endpoint} {
				if v == "" {
					unsetenv(t, k)
				} else {
					t.Setenv(k, v)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ingest := newRUMIngest(ctx)
			if (ingest != nil) != tc.wantOn {
				t.Fatalf("newRUMIngest on=%v, want %v", ingest != nil, tc.wantOn)
			}
			mux := http.NewServeMux()
			mountTelemetryRoute(mux, use.NewChain(), debugAuthTestKeys, ingest)
			t.Cleanup(cache.ResetRouteScopeRegistryForTest)
			want := http.StatusNotFound
			if tc.wantOn {
				want = http.StatusOK
			}
			rec := postRUM(t, mux, mintRUMToken(t, fmt.Sprintf("off-%d", i)), otlpBody(t, rum.ServiceName, nil, widgetFetch()))
			if rec.Code != want {
				t.Fatalf("authenticated POST answered %d, want %d", rec.Code, want)
			}
		})
	}
}

// F2 / M18 — mountTelemetryRoute registers the route as scoped, so the boot
// assert (AssertReadPathsScoped, which requires POST /telemetry/v1/traces)
// counts it present.
//
// MUTATION (RED, shown in the PR): cache.RegisterScopedRoute dropped from
// mountTelemetryRoute.
func TestS1015_RouteIsRegisteredScoped(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(false) // production mode: AssertReadPathsScoped counts instead of panicking
	t.Cleanup(func() { xenv.SetTestMode(prev) })
	cache.ResetRouteScopeRegistryForTest()
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)

	missingBefore := cache.AssertReadPathsScoped()
	mountTelemetryRoute(http.NewServeMux(), use.NewChain(), debugAuthTestKeys, http.NotFoundHandler())
	missingAfter := cache.AssertReadPathsScoped()
	if missingBefore-missingAfter != 1 {
		t.Fatalf("required scoped routes missing: %d before mounting, %d after; mounting the ingest must register exactly %s",
			missingBefore, missingAfter, rum.Pattern)
	}
}

// F4 — a refusal is attributable: it writes a WARN line carrying the caller's
// redact.User label (internal/redact, #453: never the username in clear), the
// status and the reason, and nothing from the body.
//
// MUTATIONS (RED, shown in the PR): the line logs the raw username; the line
// removed.
func TestS1015_RefusalIsAttributable(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{w: &buf, mu: &mu}, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	const user = "zq1015-attributable-user"
	req := httptest.NewRequest(http.MethodPost, rum.Path, bytes.NewReader([]byte{0x0a}))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+mintRUMToken(t, user))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status %d, want 415", rec.Code)
	}

	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if strings.Contains(out, user) {
		t.Fatalf("the refusal log carries the username in clear: %s", out)
	}
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) != nil || rec["msg"] != "rum.ingest.refused" {
			continue
		}
		found = true
		if rec["user"] != redact.User(user) || rec["reason"] != "content_type" || rec["status"] != float64(415) {
			t.Fatalf("refusal line %v, want user=%s reason=content_type status=415", rec, redact.User(user))
		}
	}
	if !found {
		t.Fatalf("no rum.ingest.refused line for a refused request; log: %s", out)
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// F6 — the global spans/s cap into the collector: across distinct users (so no
// per-user limit applies), the first docGlobalBurst/512 = 4 full batches are
// forwarded, and the burst is then exhausted: a later batch is refused 429
// with Retry-After and counted, not forwarded.
//
// MUTATIONS (RED, shown in the PR): the global cap removed (never 429); the
// burst lowered (one of the first 4 refused).
func TestS1015_GlobalSpanRateCap(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	body := spansBody(t, docSpansPerBatch)
	before := rum.Read()
	var limited *httptest.ResponseRecorder
	for i := 0; i < 12 && limited == nil; i++ {
		rec := postRUM(t, h, mintRUMToken(t, fmt.Sprintf("global-%d", i)), body)
		switch {
		case rec.Code == http.StatusTooManyRequests:
			if i < docGlobalBurst/docSpansPerBatch {
				t.Fatalf("batch %d refused within the documented burst of %d spans", i, docGlobalBurst)
			}
			limited = rec
		case rec.Code != http.StatusOK:
			t.Fatalf("batch %d: status %d", i, rec.Code)
		}
	}
	if limited == nil {
		t.Fatalf("12 batches of %d spans back to back were never limited by the global cap", docSpansPerBatch)
	}
	if limited.Header().Get("Retry-After") == "" {
		t.Fatal("global-cap 429 without Retry-After")
	}
	if d := rum.Read().Spans["forward_rate_limited"] - before.Spans["forward_rate_limited"]; d != docSpansPerBatch {
		t.Fatalf("forward_rate_limited delta %d, want %d (one refused batch)", d, docSpansPerBatch)
	}
}

// F6(d) — dropped_auth is counted once per 401, whichever layer answers it.
// Here the Ingest itself refuses a request that reached it with no identity
// (a mis-wired chain); CountAuthFailures around it is the only counter.
//
// MUTATION (RED, shown in the PR): the Ingest counting its own 401 again.
func TestS1015_AuthDropCountedOnce(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwd := rum.NewForwarder(c.srv.URL + "/v1/traces")
	fwd.Start(ctx)
	h := rum.CountAuthFailures(rum.NewIngest(fwd))
	before := rum.Read()
	if rec := postRUM(t, h, "", otlpBody(t, rum.ServiceName, nil, widgetFetch())); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no identity: status %d, want 401", rec.Code)
	}
	if d := rum.Read().Requests["dropped_auth"] - before.Requests["dropped_auth"]; d != 1 {
		t.Fatalf("dropped_auth delta %d for one 401, want 1", d)
	}
}
