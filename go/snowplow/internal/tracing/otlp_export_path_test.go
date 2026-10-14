package tracing

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// #596 — snowplow emitted ZERO spans, ever, with tracing ON.
//
// ROOT CAUSE (reproduced against the shipped binary and a path-faithful fake
// collector): the chart's OTEL_EXPORTER_OTLP_ENDPOINT is a bare base URL,
// http://$(HOST_IP):4318, passed through otlptracehttp.WithEndpointURL. Since
// otlptracehttp v1.45.0 a URL with no path is taken literally, so every span
// batch was POSTed to "/" and the collector — whose otlp receiver mux serves
// only /v1/{traces,metrics,logs} — answered 404.
//
// The receiver below is PATH-FAITHFUL for that reason: it 404s every path but
// /v1/traces, as the real collector does. A receiver that accepts any path
// (the shape of the metrics F1 receiver before #596) is green on the defect.
//
// RED on main (57e48a5e): the export goes to "/", the receiver records a 404
// and no span; this arm fails with "no span reached /v1/traces".
type pathFaithfulReceiver struct {
	srv *httptest.Server

	mu       sync.Mutex
	paths    []string
	spanName []string
}

func newPathFaithfulReceiver(t *testing.T) *pathFaithfulReceiver {
	t.Helper()
	r := &pathFaithfulReceiver{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.paths = append(r.paths, req.URL.Path)
		r.mu.Unlock()
		if req.URL.Path != "/v1/traces" {
			http.NotFound(w, req)
			return
		}
		var body io.Reader = req.Body
		if req.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gz.Close()
			body = gz
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		msg := &collectortracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(raw, msg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		for _, rs := range msg.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					r.spanName = append(r.spanName, s.GetName())
				}
			}
		}
		r.mu.Unlock()
		resp, _ := proto.Marshal(&collectortracepb.ExportTraceServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(resp)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// TestF596_SpansReachV1TracesFromABareBaseEndpoint drives the REAL Setup and
// the REAL otelhttp server handler (the same wrap main.go installs) with the
// endpoint in the chart's exact shape, then asserts the span landed at
// /v1/traces. Both the bare form and a trailing-slash form are covered.
func TestF596_SpansReachV1TracesFromABareBaseEndpoint(t *testing.T) {
	for _, suffix := range []string{"", "/"} {
		t.Run("endpoint-suffix="+suffix, func(t *testing.T) {
			rcv := newPathFaithfulReceiver(t)

			t.Setenv("OTEL_ENABLED", "true")
			t.Setenv(EnvOTLPEndpoint, rcv.srv.URL+suffix) // e.g. http://127.0.0.1:NNNN — no signal path
			t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

			shutdown, err := Setup(context.Background(), "deadbeef")
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}

			h := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}), serviceName, otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				return r.Method + " " + r.URL.Path
			}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/call?resource=x", nil))

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := shutdown(ctx); err != nil {
				t.Fatalf("shutdown (flush): %v", err)
			}

			rcv.mu.Lock()
			defer rcv.mu.Unlock()
			if len(rcv.spanName) == 0 {
				t.Fatalf("no span reached /v1/traces; exporter POSTed to %v (the #596 defect posts to \"/\")", rcv.paths)
			}
			for _, p := range rcv.paths {
				if p != "/v1/traces" {
					t.Errorf("exporter POSTed to %q; every trace export must go to /v1/traces", p)
				}
			}
			if rcv.spanName[0] != "GET /call" {
				t.Errorf("span name = %q; want \"GET /call\"", rcv.spanName[0])
			}
		})
	}
}
