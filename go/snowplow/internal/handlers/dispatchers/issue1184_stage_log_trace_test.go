package dispatchers

// issue1184_stage_log_trace_test.go — #1184's acceptance arm: a real request at
// LOG_LEVEL=warn that fails in a stage emits a line with the same trace_id as
// its Server span.
//
// Driven through the REAL chain on the #443 ps harness (cache ON, a real watcher,
// the requester's clientconfig endpoint on a fake apiserver): tracing's Server
// span → middleware.RequestLogger → the RESTAction /call handler → restactions.Resolve →
// api.Resolve → the fetch. The log chain is the real decorator over the JSON
// handler at WARN, as the default logger and as the request logger. The trace id
// is read from the exported span, never from a logger or a context this test
// built.
//
// One arm per way the context reaches a site:
//
//	worker   a stage worker's local gctx (dispatchOneCall)
//	run      resolveRun's r.ctx, for a method with no context parameter
//	param    a helper that took no context until #1184 (paginationFromQuery)
//
// Each goes RED when its site is reverted to the plain form (shown in the PR).
// ServeHTTP returns after the span ended and the record was written: no wait.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
)

const i1184Path = "/api/v1/namespaces/" + psTargetNS + "/configmaps/fails1184"

// i1184Records decodes the captured lines whose msg is msg.
func i1184Records(t *testing.T, buf *syncBuffer, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("a log line is not one JSON object: %v", err)
		}
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// i1184Serve answers GET target for the stored RESTAction ra through the span
// and log chain, and returns the single Server span it produced with the log.
func i1184Serve(t *testing.T, f *in443API, ra map[string]any, target string) (tracetest.SpanStub, *syncBuffer) {
	t.Helper()
	return i1184ServeWith(t, f, ra, target, func(w io.Writer) slog.Handler {
		return tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}, func(h http.Handler) http.Handler { return h })
}

// i1184ServeWith is i1184Serve with the caller's log handler and an outermost
// wrapper around the served chain (#1260: the request-root middleware).
func i1184ServeWith(t *testing.T, f *in443API, ra map[string]any, target string,
	newHandler func(io.Writer) slog.Handler, wrap func(http.Handler) http.Handler,
) (tracetest.SpanStub, *syncBuffer) {
	t.Helper()
	quiesceDefaultSummaryForTest(t)
	buf := &syncBuffer{}
	log := slog.New(newHandler(buf))
	prevLog := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: (&unstructured.Unstructured{Object: ra}).DeepCopy()}
	})
	defer restore()

	handler := middleware.RequestLogger(log)(&restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}})
	mux := http.NewServeMux()
	mux.Handle("GET /call", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// What the UserConfig middleware leaves on the request.
		handler.ServeHTTP(w, r.WithContext(xcontext.BuildContext(r.Context(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: psAlice, Groups: psGroups(in443Arm)}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: f.srv.URL, Token: "tok-" + psAlice}),
		)))
	}))
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	wrap(tracing.NewServerHandler(mux, mux)).ServeHTTP(httptest.NewRecorder(), req)

	var servers []tracetest.SpanStub
	for _, s := range exp.GetSpans() {
		if s.SpanKind == trace.SpanKindServer {
			servers = append(servers, s)
		}
	}
	if len(servers) != 1 {
		t.Fatalf("PRECONDITION: want one Server span, got %d", len(servers))
	}
	return servers[0], buf
}

func assertI1184(t *testing.T, span tracetest.SpanStub, buf *syncBuffer, msg, level string) {
	t.Helper()
	recs := i1184Records(t, buf, msg)
	if len(recs) == 0 {
		t.Fatalf("PRECONDITION: the request logged no %q line:\n%s", msg, buf.String())
	}
	want := span.SpanContext.TraceID().String()
	wantSpan := span.SpanContext.SpanID().String()
	for _, rec := range recs {
		if rec["level"] != level {
			t.Fatalf("%q at %v, want %s", msg, rec["level"], level)
		}
		if rec["trace_id"] != want {
			t.Fatalf("#1184 RED: %q carries trace_id %v; its Server span's is %s", msg, rec["trace_id"], want)
		}
		if rec["span_id"] != wantSpan {
			t.Fatalf("#1184 RED: %q carries span_id %v; its Server span's is %s", msg, rec["span_id"], wantSpan)
		}
	}
}

// The issue's arm: the stage's GET fails, on a stage worker.
func TestIssue1184_StageFailure_LogsTheServerSpansTraceID(t *testing.T) {
	ra := in443RA("trace-1184", i729Filter, map[string]any{"name": "crd", "verb": "GET", "path": i1184Path})
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})
	f.mu.Lock()
	f.extra["GET "+i1184Path] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","code":500}`))
	}
	f.mu.Unlock()

	span, buf := i1184Serve(t, f, ra, "/call")
	assertI1184(t, span, buf, "api call response failure", "ERROR")
}

// A resolveRun method with no context parameter: its context is the run's.
func TestIssue1184_EndpointResolveFailure_UsesTheRunsContext(t *testing.T) {
	ra := in443RA("ref-1184", i729Filter, map[string]any{"name": "crd", "verb": "GET", "path": i1184Path,
		"endpointRef": map[string]any{"name": "no-such-endpoint-1184", "namespace": psAuthnNS}})
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})

	span, buf := i1184Serve(t, f, ra, "/call")
	assertI1184(t, span, buf, "unable to resolve api endpoint reference", "ERROR")
}

// A helper that took no context until #1184.
func TestIssue1184_BadPagination_ThreadsTheRequestContext(t *testing.T) {
	ra := in443RA("page-1184", "", targetStage())
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})

	span, buf := i1184Serve(t, f, ra, "/call?perPage=not-a-number")
	assertI1184(t, span, buf, "unable convert perPage parameter to int", "ERROR")
}

// A demand revalidation outlives its request and keeps its context, values
// included: its lines carry the trace_id of the request that caused the work,
// after that request's span ended and its context was cancelled. No span is
// created for them. The line is the attempt's real panic report, on the context
// the production template and detach build.
func TestIssue1184_ADetachedRevalidationKeepsTheTraceAfterItsSpanEnded(t *testing.T) {
	quiesceDefaultSummaryForTest(t)
	buf := &syncBuffer{}
	log := slog.New(tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	parent, cancel := context.WithCancel(xcontext.BuildContext(context.Background(), xcontext.WithLogger(log)))
	ctx, span := tp.Tracer("s1184").Start(parent, "GET /call", trace.WithSpanKind(trace.SpanKindServer))
	const key = "L1_issue1184_detached"
	req := httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx)

	tpl := setDemandTemplate(key, req)
	span.End()
	cancel()
	if req.Context().Err() == nil {
		t.Fatal("PRECONDITION: the request's context must be cancelled")
	}

	demandRevalidateOnce(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("s1184") }), tpl.req, key, time.Now(), "s1184")

	const msg = "demand revalidation panicked; cell stays known-stale"
	recs := i1184Records(t, buf, msg)
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: want one %q line, got %d:\n%s", msg, len(recs), buf.String())
	}
	if got, want := recs[0]["trace_id"], span.SpanContext().TraceID().String(); got != want {
		t.Fatalf("#1184 RED: the detached line carries trace_id %v, the request's span was %s", got, want)
	}
	if got, want := recs[0]["span_id"], span.SpanContext().SpanID().String(); got != want {
		t.Fatalf("#1184 RED: the detached line carries span_id %v, the request's span was %s", got, want)
	}
}
