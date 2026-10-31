package main

// main_telemetry_1020_d6_test.go — #1020 D6, Diego's ruling 2026-10-30: no
// Server span for RUM uploads. POST /telemetry/v1/traces is in the same
// tracing.unspannedRoutes set as the kubelet probes (#1018), with no branch of
// its own. Driven through newServerHandler, the composition the pod serves,
// on a mux carrying the real telemetry route and a /call route.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// An accepted upload exports no Server span; a /call through the same handler
// still exports exactly one. The request uses rum.Path itself, so the arm also
// pins tracing's literal entry to the ingest's real path.
//
// MUTATION (RED, shown in the PR): "/telemetry/v1/traces" removed from
// tracing.unspannedRoutes (the upload exports a "POST /telemetry/v1/traces"
// Server span).
func TestS1020_RUMUploadGetsNoServerSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	c := newCollector(t, nil)
	defer c.srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fwd := rum.NewForwarder(c.srv.URL + "/v1/traces")
	fwd.Start(ctx)
	mux := http.NewServeMux()
	mux.Handle("GET /call", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	mountTelemetryRoute(mux, use.NewChain(), debugAuthTestKeys, rum.NewIngest(fwd, rum.RegisteredRoutes(mux)))
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	h := newServerHandler(mux)

	if rec := postRUM(t, h, mintRUMToken(t, "d6-user"), otlpBody(t, rum.ServiceName, nil, widgetFetch())); rec.Code != http.StatusOK {
		t.Fatalf("SETUP: the upload was answered %d, want 200: the arm needs an accepted upload", rec.Code)
	}
	if spans := exp.GetSpans(); len(spans) != 0 {
		t.Fatalf("a RUM upload exported %d Server span(s) (first %q); uploads get none (D6)", len(spans), spans[0].Name)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call", nil))
	spans := exp.GetSpans()
	if len(spans) != 1 || spans[0].Name != "GET /call" {
		names := make([]string, len(spans))
		for i, s := range spans {
			names[i] = s.Name
		}
		t.Fatalf("a /call exported spans %v, want exactly [GET /call]: the exclusion must not reach real API routes", names)
	}
}
