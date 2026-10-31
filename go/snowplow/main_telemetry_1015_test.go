package main

// main_telemetry_1015_test.go — #1015: the authenticated browser RUM ingest,
// POST /telemetry/v1/traces, driven through the REAL router: an http.ServeMux
// with the route mounted by mountTelemetryRoute (the function main calls), the
// real middleware.RefreshAuth over a JWT key source, the real rum.Ingest and a
// real rum.Forwarder posting to an in-process collector. Only the collector
// and the signing key are test doubles.
//
// The rum counters are process-global, so every arm reads deltas.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// ---- harness -----------------------------------------------------------

// collector is an in-process OTLP/HTTP traces receiver.
type collector struct {
	srv  *httptest.Server
	got  chan *collectortracepb.ExportTraceServiceRequest
	hits atomic.Int32
}

func newCollector(t *testing.T, block <-chan struct{}) *collector {
	t.Helper()
	c := &collector{got: make(chan *collectortracepb.ExportTraceServiceRequest, 256)}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.hits.Add(1)
		if block != nil {
			<-block
		}
		body, _ := io.ReadAll(r.Body)
		var req collectortracepb.ExportTraceServiceRequest
		if r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/x-protobuf" ||
			proto.Unmarshal(body, &req) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.got <- &req
		w.WriteHeader(http.StatusOK)
	}))
	return c
}

// telemetryRouter mounts the route exactly as main does, forwarding to url,
// on a mux that also carries snowplow's /call routes: GET /call with main's
// pattern, and the write verbs and /call/dry-run from the production
// registrar mountCallWriteRoutes. The ingest's RouteSet is derived from this
// mux (rum.RegisteredRoutes), as newRUMIngest derives it from main's.
func telemetryRouter(t *testing.T, url string) http.Handler {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fwd := rum.NewForwarder(url)
	fwd.Start(ctx)
	mux := http.NewServeMux()
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /call", stub)
	mux.Handle("GET /swagger/", stub) // main registers this subtree pattern too
	mountCallWriteRoutes(mux, use.NewChain(), func(http.Handler) http.Handler { return stub })
	mountTelemetryRoute(mux, use.NewChain(), debugAuthTestKeys, rum.NewIngest(fwd, rum.RegisteredRoutes(mux)))
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	return mux
}

func mintRUMToken(t *testing.T, user string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: user, Groups: []string{"devs"}, Duration: time.Hour,
		KeyID: debugAuthTestKeyID, PrivateKey: debugAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

// otlpSpan is one span in an OTLP/JSON request body.
type otlpSpan struct {
	name  string
	attrs []map[string]any
}

func strAttr(k, v string) map[string]any {
	return map[string]any{"key": k, "value": map[string]any{"stringValue": v}}
}

func intAttr(k string, v int) map[string]any {
	return map[string]any{"key": k, "value": map[string]any{"intValue": fmt.Sprint(v)}}
}

// otlpBody builds an OTLP/JSON ExportTraceServiceRequest the way the
// browser's exporter-trace-otlp-http does: hex ids, string nanos.
func otlpBody(t *testing.T, service string, extraResource []map[string]any, spans ...otlpSpan) []byte {
	t.Helper()
	var js []map[string]any
	for i, s := range spans {
		js = append(js, map[string]any{
			"traceId":           "4bf92f3577b34da6a3ce929d0e0e4736",
			"spanId":            fmt.Sprintf("00f067aa0ba9%04x", i+1),
			"parentSpanId":      "00f067aa0ba90200",
			"name":              s.name,
			"kind":              3,
			"startTimeUnixNano": "1761800000000000000",
			"endTimeUnixNano":   "1761800000250000000",
			"attributes":        s.attrs,
			"status":            map[string]any{"code": 0, "message": "free text that must not be forwarded"},
			"events":            []map[string]any{{"name": "click", "timeUnixNano": "1761800000100000000"}},
		})
	}
	res := append([]map[string]any{strAttr("service.name", service)}, extraResource...)
	b, err := json.Marshal(map[string]any{"resourceSpans": []map[string]any{{
		"resource":   map[string]any{"attributes": res},
		"scopeSpans": []map[string]any{{"scope": map[string]any{"name": "krateo-portal"}, "spans": js}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func widgetFetch() otlpSpan {
	return otlpSpan{name: "krateo.widget.fetch", attrs: []map[string]any{
		strAttr("http.route", "/call"),
		strAttr("http.request.method", "GET"),
		intAttr("http.response.status_code", 200),
	}}
}

func postRUM(t *testing.T, h http.Handler, token string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, rum.Path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func awaitExport(t *testing.T, c *collector) *collectortracepb.ExportTraceServiceRequest {
	t.Helper()
	select {
	case got := <-c.got:
		return got
	case <-time.After(10 * time.Second):
		t.Fatal("the collector received nothing within 10s")
		return nil
	}
}

// ---- arms --------------------------------------------------------------

// Unauthenticated → 401, counted as an auth drop, nothing forwarded.
//
// MUTATIONS (RED, shown in the PR): mountTelemetryRoute mounts a stand-in that
// attaches an identity instead of middleware.RefreshAuth (→ 200); the
// rum.CountAuthFailures wrapper removed (→ the auth counter does not move).
func TestS1015_UnauthenticatedIs401(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	body := otlpBody(t, rum.ServiceName, nil, widgetFetch())

	before := rum.Read()
	for _, tok := range []string{"", "not-a-jwt", mintWrongKeyDebugToken(t)} {
		if rec := postRUM(t, h, tok, body); rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %.12q: status %d, want 401", tok, rec.Code)
		}
	}
	after := rum.Read()
	if d := after.Requests["dropped_auth"] - before.Requests["dropped_auth"]; d != 3 {
		t.Fatalf("dropped_auth delta %d, want 3", d)
	}
	if d := after.Requests["accepted"] - before.Requests["accepted"]; d != 0 {
		t.Fatalf("an unauthenticated request was accepted (delta %d)", d)
	}
	if n := c.hits.Load(); n != 0 {
		t.Fatalf("collector received %d posts from unauthenticated requests", n)
	}
}

// documentedBodyCap is the cap the frontend doc promises (howto/rum-ingest.md).
// It is a literal on purpose: an arm sized from rum.MaxBodyBytes would loosen
// along with any change to the constant and could never catch one.
const documentedBodyCap = 512 << 10

// paddedBody returns a valid OTLP/JSON body of exactly n bytes.
func paddedBody(t *testing.T, n int) []byte {
	t.Helper()
	base := len(otlpBody(t, rum.ServiceName, nil, widgetFetch())) + len(`,{"key":"padding","value":{"stringValue":""}}`)
	s := widgetFetch()
	s.attrs = append(s.attrs, strAttr("padding", strings.Repeat("x", n-base)))
	b := otlpBody(t, rum.ServiceName, nil, s)
	if len(b) != n {
		t.Fatalf("SETUP: padded body is %d bytes, want %d", len(b), n)
	}
	return b
}

// Over the size cap → 413, counted; at the cap → accepted.
//
// MUTATIONS (RED): MaxBodyBytes raised to 512 MiB (the over-cap body is
// accepted); MaxBodyBytes lowered to 256 KiB (the at-cap body is refused).
func TestS1015_OverSizeCapIs413(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")

	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, "size-user"), paddedBody(t, documentedBodyCap+1)); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d for a %d-byte body, want 413", rec.Code, documentedBodyCap+1)
	}
	if d := rum.Read().Requests["dropped_size"] - before.Requests["dropped_size"]; d != 1 {
		t.Fatalf("dropped_size delta %d, want 1", d)
	}
	if rec := postRUM(t, h, mintRUMToken(t, "size-user-2"), paddedBody(t, documentedBodyCap)); rec.Code != http.StatusOK {
		t.Fatalf("status %d for a body exactly at the %d-byte cap, want 200", rec.Code, documentedBodyCap)
	}
}

// Per-user rate → 429 with Retry-After; another user is unaffected.
//
// The burst is the documented literal (howto/rum-ingest.md), not
// rum.PerUserBurst, so a change to the constant cannot loosen the arm with it.
// The first documentedBurst requests must pass: at 1/s refill, the tokens
// back-to-back requests consume cannot be confused with a smaller burst.
//
// MUTATIONS (RED): the limiter keyed on one global key instead of the user
// (user B is refused); the limiter always allowing (no 429); PerUserBurst
// raised to 1000 (no 429 within documentedBurst+5).
func TestS1015_PerUserRateLimit(t *testing.T) {
	const documentedBurst = 10
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	body := otlpBody(t, rum.ServiceName, nil, widgetFetch())
	a, b := mintRUMToken(t, "rate-user-a"), mintRUMToken(t, "rate-user-b")

	before := rum.Read()
	var limited *httptest.ResponseRecorder
	for i := 0; i < documentedBurst+5 && limited == nil; i++ {
		rec := postRUM(t, h, a, body)
		switch {
		case rec.Code == http.StatusTooManyRequests:
			limited = rec
		case rec.Code != http.StatusOK:
			t.Fatalf("request %d: status %d", i, rec.Code)
		}
	}
	if limited == nil {
		t.Fatalf("user A sent %d requests back to back and was never limited", documentedBurst+5)
	}
	if ra := limited.Header().Get("Retry-After"); ra == "" || ra == "0" {
		t.Fatalf("429 without a usable Retry-After (%q)", ra)
	}
	if rec := postRUM(t, h, b, body); rec.Code != http.StatusOK {
		t.Fatalf("user B got %d right after user A was limited; the limit must be per user", rec.Code)
	}
	if d := rum.Read().Requests["dropped_rate"] - before.Requests["dropped_rate"]; d < 1 {
		t.Fatalf("dropped_rate delta %d, want >= 1", d)
	}
}

// A wrong service.name, or an unknown span name, is dropped and counted, and
// only the allowlisted span is forwarded.
//
// MUTATIONS (RED): the service.name check skipped; the span-name check skipped.
func TestS1015_WrongServiceAndUnknownNameDropped(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	tok := mintRUMToken(t, "filter-user")

	before := rum.Read()
	if rec := postRUM(t, h, tok, otlpBody(t, "evil-service", nil, widgetFetch())); rec.Code != http.StatusOK {
		t.Fatalf("wrong service: status %d, want 200 with a partial success", rec.Code)
	}
	mixed := otlpBody(t, rum.ServiceName, nil, otlpSpan{name: "document.load"}, widgetFetch())
	rec := postRUM(t, h, tok, mixed)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"rejectedSpans":"1"`) {
		t.Fatalf("mixed batch: status %d body %s, want 200 with rejectedSpans 1", rec.Code, rec.Body)
	}
	after := rum.Read()
	if d := after.Spans["dropped_service"] - before.Spans["dropped_service"]; d != 1 {
		t.Fatalf("dropped_service delta %d, want 1", d)
	}
	if d := after.Spans["dropped_name"] - before.Spans["dropped_name"]; d != 1 {
		t.Fatalf("dropped_name delta %d, want 1", d)
	}
	got := awaitExport(t, c)
	spans := got.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()
	if len(spans) != 1 || spans[0].GetName() != "krateo.widget.fetch" {
		t.Fatalf("forwarded %d spans (%v), want only the krateo.widget.fetch one", len(spans), spans)
	}
	select {
	case extra := <-c.got:
		t.Fatalf("the wrong-service request was forwarded: %v", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// Non-allowlisted attributes, a query string and a token-shaped value are
// dropped, and the forwarded payload holds ONLY allowlisted fields: one
// resource attribute (service.name), snowplow's scope, and per span the ids,
// name, kind, times, status code and the three allowlisted attributes.
//
// MUTATIONS (RED): allowedValue passing any key through; validRoute accepting
// any value that starts with "/"; tokenShaped always false.
func TestS1015_ForwardedPayloadIsAllowlistOnly(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	// SYNTHETIC JWT, not a credential (#1015): header {"alg":"RS256","typ":"JWT"},
	// payload {"sub":"synthetic-test"}, signature the bytes "synthetic-signature";
	// no key ever issued it. It exists to prove a token-shaped value never leaves
	// the ingest. The gitleaks `jwt` finding is allowed on THIS line only, by the
	// inline marker, which survives a squash merge (a .gitleaksignore fingerprint,
	// pinned to a commit SHA, would not). No path or rule allowlist.
	const secret = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJzeW50aGV0aWMtdGVzdCJ9.c3ludGhldGljLXNpZ25hdHVyZQ" // gitleaks:allow
	const name = "zq1015-person-name"
	spans := []otlpSpan{
		{name: "krateo.widget.fetch", attrs: []map[string]any{
			strAttr("http.route", "/call"),
			strAttr("http.request.method", "GET"),
			intAttr("http.response.status_code", 200),
			strAttr("enduser.id", name),
			strAttr("url.full", "https://portal/call?name="+name),
			strAttr("http.request.header.authorization", "Bearer "+secret),
		}},
		{name: "krateo.page.load", attrs: []map[string]any{
			strAttr("http.route", "/call?name="+name), // query string
			strAttr("http.request.method", "GET"),
		}},
		{name: "krateo.form.submit", attrs: []map[string]any{
			strAttr("http.route", "/compositions/"+secret), // token-shaped segment
			strAttr("http.request.method", "post"),         // not a method token
			intAttr("http.response.status_code", 42),       // not a status
		}},
	}
	extraRes := []map[string]any{strAttr("enduser.id", name), strAttr("user_agent.original", "Mozilla/5.0")}

	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, name), otlpBody(t, rum.ServiceName, extraRes, spans...)); rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if d := rum.Read().Attributes - before.Attributes; d != 7 {
		t.Errorf("attributes_dropped delta %d, want 7 (3 non-allowlisted keys, the query-string route, the token route, the lower-case method, status 42)", d)
	}

	got := awaitExport(t, c)
	raw, _ := proto.Marshal(got)
	for _, leak := range []string{name, secret, "?name=", "free text", "Mozilla", "krateo-portal", "click"} {
		if bytes.Contains(raw, []byte(leak)) {
			t.Errorf("forwarded payload contains %q", leak)
		}
	}
	rs := got.GetResourceSpans()
	if len(rs) != 1 || len(rs[0].GetScopeSpans()) != 1 {
		t.Fatalf("forwarded %d resource spans, want exactly 1 with 1 scope", len(rs))
	}
	if ra := rs[0].GetResource().GetAttributes(); len(ra) != 1 || ra[0].GetKey() != "service.name" ||
		ra[0].GetValue().GetStringValue() != rum.ServiceName {
		t.Errorf("forwarded resource attributes %v, want only service.name=%s", ra, rum.ServiceName)
	}
	if sc := rs[0].GetScopeSpans()[0].GetScope(); sc.GetName() != rum.ScopeName || len(sc.GetAttributes()) != 0 || sc.GetVersion() != "" {
		t.Errorf("forwarded scope %v, want only name=%s", sc, rum.ScopeName)
	}
	allowed := map[string]bool{rum.AttrRoute: true, rum.AttrMethod: true, rum.AttrStatus: true}
	out := rs[0].GetScopeSpans()[0].GetSpans()
	if len(out) != 3 {
		t.Fatalf("forwarded %d spans, want 3", len(out))
	}
	for _, s := range out {
		for _, a := range s.GetAttributes() {
			if !allowed[a.GetKey()] {
				t.Errorf("span %s forwards attribute %s", s.GetName(), a.GetKey())
			}
		}
		if len(s.GetEvents()) != 0 || len(s.GetLinks()) != 0 || s.GetTraceState() != "" ||
			s.GetStatus().GetMessage() != "" || len(s.GetTraceId()) != 16 || len(s.GetSpanId()) != 8 ||
			len(s.GetParentSpanId()) != 8 || s.GetEndTimeUnixNano()-s.GetStartTimeUnixNano() != 250_000_000 {
			t.Errorf("span %s carries a non-allowlisted field or lost an allowlisted one: %v", s.GetName(), s)
		}
	}
	if n := len(out[0].GetAttributes()); n != 3 {
		t.Errorf("krateo.widget.fetch forwarded %d attributes, want its 3 allowlisted ones", n)
	}
	if n := len(out[1].GetAttributes()); n != 1 {
		t.Errorf("krateo.page.load forwarded %d attributes, want 1 (the query-string route dropped)", n)
	}
	if n := len(out[2].GetAttributes()); n != 0 {
		t.Errorf("krateo.form.submit forwarded %d attributes, want 0", n)
	}
}

// Collector down: requests never block on it, and what cannot be delivered is
// counted (forward_failed when it errors, forward_overflow when it hangs and
// the queue is full).
//
// MUTATION (RED): Forwarder.enqueue made a blocking send.
func TestS1015_CollectorDownNeverBlocksAndIsCounted(t *testing.T) {
	// (1) Nothing listens: the post fails fast → forward_failed.
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL + "/v1/traces"
	dead.Close()
	h := telemetryRouter(t, deadURL)
	before := rum.Read()
	if rec := postRUM(t, h, mintRUMToken(t, "down-user"), otlpBody(t, rum.ServiceName, nil, widgetFetch())); rec.Code != http.StatusOK {
		t.Fatalf("status %d with the collector down, want 200", rec.Code)
	}
	deadline := time.Now().Add(10 * time.Second)
	for rum.Read().Spans["forward_failed"] == before.Spans["forward_failed"] {
		if time.Now().After(deadline) {
			t.Fatal("forward_failed did not move with the collector down")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// (2) The collector accepts the connection and never answers: the worker
	// is stuck on one batch, the queue fills, and every further batch is
	// dropped and counted, while each request still returns at once.
	block := make(chan struct{})
	hung := newCollector(t, block)
	t.Cleanup(hung.srv.Close)
	t.Cleanup(func() { close(block) })
	h = telemetryRouter(t, hung.srv.URL+"/v1/traces")
	before = rum.Read()
	body := otlpBody(t, rum.ServiceName, nil, widgetFetch())
	sent := 0
	for u := 0; sent < rum.ForwardQueue+rum.PerUserBurst; u++ {
		tok := mintRUMToken(t, fmt.Sprintf("hung-user-%d", u))
		for i := 0; i < rum.PerUserBurst; i++ {
			start := time.Now()
			rec := postRUM(t, h, tok, body)
			if el := time.Since(start); el > time.Second {
				t.Fatalf("request %d took %v with the collector hung; it must not block", sent, el)
			}
			if rec.Code != http.StatusOK {
				t.Fatalf("request %d: status %d, want 200", sent, rec.Code)
			}
			sent++
		}
	}
	if d := rum.Read().Spans["forward_overflow"] - before.Spans["forward_overflow"]; d == 0 {
		t.Fatalf("%d batches against a hung collector and forward_overflow did not move", sent)
	}
}

// Off (no collector, or tracing disabled): the route answers 404 to everyone,
// authenticated or not.
func TestS1015_DisabledRouteIs404(t *testing.T) {
	mux := http.NewServeMux()
	mountTelemetryRoute(mux, use.NewChain(), debugAuthTestKeys, nil)
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	for _, tok := range []string{"", mintRUMToken(t, "off-user")} {
		if rec := postRUM(t, mux, tok, otlpBody(t, rum.ServiceName, nil, widgetFetch())); rec.Code != http.StatusNotFound {
			t.Fatalf("disabled ingest answered %d, want 404", rec.Code)
		}
	}
}

// The browser exporter sends traceparent and tracestate with each export, so
// its POST is preflighted. The preflight for THIS route, through the
// production CORS wrapper (use.CORS(snowplowCORSOptions()), outermost in the
// pod), must allow them along with Authorization and Content-Type, or the
// browser never sends the request.
//
// MUTATION (RED, shown in the PR): "traceparent" and "tracestate" removed from
// snowplowCORSOptions().AllowedHeaders.
func TestS1015_CORSPreflightAllowsTraceContext(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := use.CORS(snowplowCORSOptions())(telemetryRouter(t, c.srv.URL+"/v1/traces"))

	req := httptest.NewRequest(http.MethodOptions, rum.Path, nil)
	req.Header.Set("Origin", "https://portal.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "authorization,content-type,traceparent,tracestate")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code/100 != 2 {
		t.Fatalf("preflight status %d, want 2xx", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got == "" {
		t.Fatal("preflight answered without Access-Control-Allow-Origin: the browser would block the export")
	}
	allowed := map[string]bool{}
	for _, v := range rec.Header().Values("Access-Control-Allow-Headers") {
		for _, h := range strings.Split(v, ",") {
			allowed[strings.ToLower(strings.TrimSpace(h))] = true
		}
	}
	for _, want := range []string{"traceparent", "tracestate", "authorization", "content-type"} {
		if !allowed[want] {
			t.Errorf("preflight on %s does not allow %q (Access-Control-Allow-Headers=%q)",
				rum.Path, want, rec.Header().Values("Access-Control-Allow-Headers"))
		}
	}
	if m := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(strings.ToUpper(m), http.MethodPost) {
		t.Errorf("preflight does not allow POST (Access-Control-Allow-Methods=%q)", m)
	}
	if n := c.hits.Load(); n != 0 {
		t.Fatalf("a preflight reached the collector (%d posts)", n)
	}
}

// Protobuf, the exporter's other encoding, is refused with 415: the contract
// is OTLP/JSON only (settled with frontend 2026-10-30).
//
// MUTATION (RED, shown in the PR): the Content-Type check accepting
// application/x-protobuf.
func TestS1015_ProtobufIs415(t *testing.T) {
	c := newCollector(t, nil)
	defer c.srv.Close()
	h := telemetryRouter(t, c.srv.URL+"/v1/traces")
	req := httptest.NewRequest(http.MethodPost, rum.Path, bytes.NewReader([]byte{0x0a, 0x00}))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer "+mintRUMToken(t, "proto-user"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("protobuf body answered %d, want 415", rec.Code)
	}
}
