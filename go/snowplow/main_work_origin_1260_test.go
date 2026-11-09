package main

// main_work_origin_1260_test.go — #1260: through the pod's own composition
// (newServerHandler → the route → the site → buildLogHandler at LOG_LEVEL=warn),
// a request's WARN says work_origin=request next to its trace_id, and a line on
// a background driver's context says that driver and carries no trace_id.
//
// The harness is #1184's (served1184): the same routes and the same log chain.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const s1260Call = "/call?apiVersion=none.example.io/v1&resource=things&name=x"

func TestS1260_ARequestsWarnSaysRequestNextToItsTraceID(t *testing.T) {
	exp := tracer1184(t, sdktrace.WithSampler(sdktrace.AlwaysSample()))
	h, sink := served1184(t)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, s1260Call, nil))

	span := serverSpan(t, exp, "GET /call")
	recs := sink.records(t, "handler not found")
	if len(recs) != 1 || recs[0]["level"] != "WARN" {
		t.Fatalf("PRECONDITION: want one WARN \"handler not found\" line, got %v", recs)
	}
	if got := recs[0]["work_origin"]; got != cache.WorkOriginRequest {
		t.Fatalf("#1260 RED: a request's WARN carries work_origin=%v, want %q: %v", got, cache.WorkOriginRequest, recs[0])
	}
	if got, want := recs[0]["trace_id"], span.SpanContext.TraceID().String(); got != want {
		t.Fatalf("a request's WARN carries trace_id %v, want its Server span's %s", got, want)
	}
}

// The origin does not depend on tracing: with tracing off a request's line still
// says request, and invents no trace_id.
func TestS1260_TracingOff_ARequestStillSaysRequest(t *testing.T) {
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(trace.NewNoopTracerProvider()) //nolint:staticcheck // the off state under test
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	h, sink := served1184(t)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, s1260Call, nil))

	recs := sink.records(t, "handler not found")
	if len(recs) != 1 {
		t.Fatalf("want one line, got %d", len(recs))
	}
	if recs[0]["work_origin"] != cache.WorkOriginRequest {
		t.Fatalf("#1260 RED: tracing off, a request's line carries work_origin=%v, want request", recs[0]["work_origin"])
	}
	if _, ok := recs[0]["trace_id"]; ok {
		t.Fatalf("tracing off, yet a trace_id: %v", recs[0])
	}
}

// buildLogHandler's classifier is cache.WorkOrigin: a background driver's
// context is named, a context with no driver gets no work_origin (absent, not
// "unknown"). The contexts are the drivers' own markers; the drivers' real roots
// are proven in dispatchers (work_origin_census_1260_test.go).
func TestS1260_BuildLogHandler_NamesTheDriverAndNothingElse(t *testing.T) {
	for _, c := range []struct {
		name string
		ctx  context.Context
		want string
	}{
		{"refresher", cache.WithBackgroundResolveOrigin(context.Background(), cache.BackgroundOriginRefresher), cache.BackgroundOriginRefresher},
		{"cohort seed", cache.WithBackgroundResolveOrigin(context.Background(), cache.BackgroundOriginCohortSeed), cache.BackgroundOriginCohortSeed},
		{"stale replace", cache.WithStaleReplaceWork(context.Background()), cache.WorkOriginStaleReplace},
		{"no driver", context.Background(), ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			slog.New(buildLogHandler(false, slog.LevelWarn, &buf)).WarnContext(c.ctx, "s1260")
			var rec map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
				t.Fatalf("not one JSON record: %v", err)
			}
			got, has := rec["work_origin"]
			if c.want == "" {
				if has {
					t.Fatalf("a context naming no driver got work_origin=%v", got)
				}
				return
			}
			if got != c.want {
				t.Fatalf("#1260 RED: work_origin=%v, want %q: %v", got, c.want, rec)
			}
			if _, ok := rec["trace_id"]; ok {
				t.Fatalf("background work carries a trace_id: %v", rec)
			}
		})
	}
}
