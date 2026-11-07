package main

// main_request_log_trace_1184_test.go — #1184: a request's WARN and ERROR lines
// carry the trace_id of its Server span, through the pod's own composition:
// newServerHandler (span) → the route → the site → the default or request logger
// → buildLogHandler at LOG_LEVEL=warn.
//
// The trace id comes from the SPAN (the in-memory exporter's copy) or from the
// inbound traceparent, never from a logger or a context the test built. The
// request is served synchronously by ServeHTTP, so the span has ended and the
// record is written when it returns: nothing here waits.
//
// A site's arm goes RED when that site is reverted to the plain form (shown in
// the PR). The deeper classes have their arms next to their harness:
// internal/handlers/dispatchers/issue1184_stage_log_trace_test.go (resolver, cache
// on, a real stage failure), internal/cache/issue1184_log_trace_test.go (read and
// registration paths), internal/rbac/evaltest/issue1184_log_trace_test.go and
// internal/credhttp/issue1184_log_trace_test.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// lines1184 is the log stream: every goroutine in the process may write it, and
// the test reads it, so it locks (#392: a test cannot quiesce every emitter).
type lines1184 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lines1184) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

// records returns the decoded lines whose msg is msg.
func (l *lines1184) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	raw := append([]byte(nil), l.b.Bytes()...)
	l.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("a log line is not one JSON object: %v", err)
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// served1184 is the pod's composition over the routes the arms drive, with the
// production log chain as the default and the request logger.
func served1184(t *testing.T) (http.Handler, *lines1184) {
	t.Helper()
	sink := &lines1184{}
	log := slog.New(buildLogHandler(false, slog.LevelWarn, sink))
	prev := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fwd := rum.NewForwarder("http://127.0.0.1:1")
	fwd.Start(ctx)

	chain := newRequestChain(log)
	mux := http.NewServeMux()
	stub := http.HandlerFunc(ok200)
	mux.Handle("GET /call", chain.Append(handlers.Dispatcher(dispatchers.All())).Then(stub))
	mux.Handle("POST /jq", chain.Then(handlers.JQ()))
	mountTelemetryRoute(mux, chain, debugAuthTestKeys, rum.NewIngest(fwd, rum.RegisteredRoutes(mux)))
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	return newServerHandler(mux), sink
}

func tracer1184(t *testing.T, opts ...sdktrace.TracerProviderOption) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(append(opts, sdktrace.WithSyncer(exp))...)
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	return exp
}

// serverSpan is the single Server span the request produced.
func serverSpan(t *testing.T, exp *tracetest.InMemoryExporter, name string) tracetest.SpanStub {
	t.Helper()
	var found []tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.SpanKind == trace.SpanKindServer && s.Name == name {
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("PRECONDITION: want one %q Server span, got %d", name, len(found))
	}
	return found[0]
}

func TestS1184_ARequestsLinesCarryItsServerSpan(t *testing.T) {
	cases := []struct {
		name      string
		span      string
		request   func(t *testing.T) *http.Request
		msg       string
		wantLevel string
	}{
		{
			// A handler frame: the plumbing request logger, the request's context.
			name: "dispatcher handler frame, request logger",
			span: "GET /call",
			request: func(*testing.T) *http.Request {
				return httptest.NewRequest(http.MethodGet, "/call?apiVersion=none.example.io/v1&resource=things&name=x", nil)
			},
			msg:       "handler not found",
			wantLevel: "WARN",
		},
		{
			name: "handler frame at ERROR",
			span: "POST /jq",
			request: func(*testing.T) *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/jq", strings.NewReader(`not json`))
				r.Header.Set("Content-Type", "application/json")
				return r
			},
			msg:       "unable to get user endpoint",
			wantLevel: "ERROR",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exp := tracer1184(t, sdktrace.WithSampler(sdktrace.AlwaysSample()))
			h, sink := served1184(t)

			h.ServeHTTP(httptest.NewRecorder(), c.request(t))

			span := serverSpan(t, exp, c.span)
			recs := sink.records(t, c.msg)
			if len(recs) != 1 {
				t.Fatalf("want one %q line for one request, got %d", c.msg, len(recs))
			}
			rec := recs[0]
			if rec["level"] != c.wantLevel {
				t.Fatalf("level %v, want %s", rec["level"], c.wantLevel)
			}
			if got, want := rec["trace_id"], span.SpanContext.TraceID().String(); got != want {
				t.Fatalf("#1184 RED: %q carries trace_id %v; its Server span's is %s", c.msg, got, want)
			}
			if got, want := rec["span_id"], span.SpanContext.SpanID().String(); got != want {
				t.Fatalf("#1184 RED: %q carries span_id %v; its Server span's is %s", c.msg, got, want)
			}
		})
	}
}

// A sampler that drops the span keeps the id: the request's lines still group
// together, which is the handler comment's claim. The id's independent source
// here is the inbound traceparent, since a dropped span is never exported.
func TestS1184_ASampledOutRequestKeepsItsTraceID(t *testing.T) {
	exp := tracer1184(t, sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())))
	h, sink := served1184(t)

	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const parent = "00f067aa0ba902b7"
	r := httptest.NewRequest(http.MethodGet, "/call?apiVersion=none.example.io/v1&resource=things&name=x", nil)
	r.Header.Set("traceparent", "00-"+traceID+"-"+parent+"-00")
	h.ServeHTTP(httptest.NewRecorder(), r)

	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("PRECONDITION: the span must be dropped, %d exported", n)
	}
	recs := sink.records(t, "handler not found")
	if len(recs) != 1 {
		t.Fatalf("want one line, got %d", len(recs))
	}
	if recs[0]["trace_id"] != traceID {
		t.Fatalf("#1184 RED: a dropped span's request logged trace_id %v, want the inbound %s", recs[0]["trace_id"], traceID)
	}
	if id, _ := recs[0]["span_id"].(string); len(id) != 16 || id == parent {
		t.Fatalf("span_id %v: want this request's own span id, not the caller's", recs[0]["span_id"])
	}
}

// A route with no Server span has no id to stamp: RUM uploads are excluded from
// spans by ruling (#1020 D6). Its refusal line passes the request's context like
// every other, and stays honest about having none.
func TestS1184_AnUnspannedRouteStampsNothing(t *testing.T) {
	exp := tracer1184(t, sdktrace.WithSampler(sdktrace.AlwaysSample()))
	h, sink := served1184(t)

	r := httptest.NewRequest(http.MethodPost, rum.Path, strings.NewReader(`x`))
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("Authorization", "Bearer "+mintRUMToken(t, "s1184-user"))
	h.ServeHTTP(httptest.NewRecorder(), r)

	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("PRECONDITION: an upload gets no span, %d exported", n)
	}
	recs := sink.records(t, "rum.ingest.refused")
	if len(recs) != 1 {
		t.Fatalf("want one line, got %d", len(recs))
	}
	if _, ok := recs[0]["trace_id"]; ok {
		t.Fatalf("#1184: an unspanned route invented a trace_id: %v", recs[0]["trace_id"])
	}
}

// With tracing off no id appears: the context is passed and carries no span, so
// nothing is invented and the line is what it always was.
func TestS1184_TracingOffStampsNothing(t *testing.T) {
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(trace.NewNoopTracerProvider()) //nolint:staticcheck // the off state under test
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	h, sink := served1184(t)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet,
		"/call?apiVersion=none.example.io/v1&resource=things&name=x", nil))

	recs := sink.records(t, "handler not found")
	if len(recs) != 1 {
		t.Fatalf("want one line, got %d", len(recs))
	}
	for _, k := range []string{"trace_id", "span_id"} {
		if _, ok := recs[0][k]; ok {
			t.Fatalf("#1184: tracing is off and the line carries %s: %v", k, recs[0])
		}
	}
}
