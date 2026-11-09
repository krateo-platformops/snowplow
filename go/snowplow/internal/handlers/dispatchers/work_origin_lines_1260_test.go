package dispatchers

// work_origin_lines_1260_test.go — #1260's line arms, on real drivers through
// the production decorator (tracing.NewCorrelationHandler with cache.WorkOrigin,
// what buildLogHandler builds; main_work_origin_1260_test.go pins that
// buildLogHandler passes exactly that classifier).
//
//	refresher  the real refresher entry (resolveAndPopulateL1 → resolveOnceProd →
//	           objects.getFromAPIServer) emits "SA-credentialed fetch re-gated
//	           and DENIED" at WARN: work_origin=refresher, and no trace_id even
//	           with tracing on and sampling everything.
//	request    a real /call whose stage fails emits "api call response failure":
//	           work_origin=request and its Server span's trace_id.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// production1260Handler is buildLogHandler's decorator over a JSON sink at WARN.
func production1260Handler(w io.Writer) slog.Handler {
	return tracing.NewCorrelationHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelWarn}), cache.WorkOrigin)
}

func TestIssue1260_RefresherWarn_SaysRefresher_NoTraceID(t *testing.T) {
	// Tracing ON, sampling everything: a background line still has no span to
	// stamp, so the absent trace_id is the code's, not the sampler's.
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	const tplPath = "/apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/s1260-platform"
	served := sar574RA("krateo-system", "s1260-platform", sar574ReviewStage(sar574SARPath)).Object
	sar574Watcher(t, nil, false, []rbacv1.PolicyRule{sar574CreateSAR})
	srv := newSAR574Server(t, map[string]any{tplPath: served})
	logs := &sar574LogBuf{}
	logs.logger = slog.New(production1260Handler(logs))
	prev := slog.Default()
	slog.SetDefault(logs.logger)
	t.Cleanup(func() { slog.SetDefault(prev) })

	sar574Refresh(t, logs.ctx(context.Background()), srv, "krateo-system", "s1260-platform", http.MethodPost, sar574SARPath)

	if ok, _ := srv.saw(http.MethodGet, tplPath); !ok {
		t.Fatalf("PRECONDITION: the refresher never fetched the template.\n  wire:%s", srv.dump())
	}
	recs := logs.lines(t, sar574Site3Msg)
	if len(recs) == 0 {
		t.Fatal("PRECONDITION: the refresher emitted no re-gate DENIED WARN")
	}
	for _, rec := range recs {
		if rec["level"] != "WARN" {
			t.Fatalf("PRECONDITION: level %v, want WARN", rec["level"])
		}
		if got := rec["work_origin"]; got != cache.BackgroundOriginRefresher {
			t.Errorf("#1260 RED: the refresher's WARN carries work_origin=%v, want %q: %v", got, cache.BackgroundOriginRefresher, rec)
		}
		if id, ok := rec["trace_id"]; ok {
			t.Errorf("the refresher's WARN carries a trace_id %v; background work has no span", id)
		}
	}
}

func TestIssue1260_RequestStageFailure_SaysRequest_WithTraceID(t *testing.T) {
	ra := in443RA("trace-1260", i729Filter, map[string]any{"name": "crd", "verb": "GET", "path": i1184Path})
	f := in443Setup(t, &unstructured.Unstructured{Object: ra})
	f.mu.Lock()
	f.extra["GET "+i1184Path] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","code":500}`))
	}
	f.mu.Unlock()

	// The request root main.go installs (newServerHandler) is the outermost
	// wrapper; here it wraps the same span + handler chain i1184Serve drives.
	span, buf := i1184ServeWith(t, f, ra, "/call", production1260Handler, cache.RequestWorkMiddleware)
	assertI1184(t, span, buf, "api call response failure", "ERROR")
	for _, rec := range i1184Records(t, buf, "api call response failure") {
		if got := rec["work_origin"]; got != cache.WorkOriginRequest {
			t.Errorf("#1260 RED: a request's stage-failure line carries work_origin=%v, want request: %v", got, rec)
		}
	}
}
