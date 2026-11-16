package credhttp

// issue1184_log_trace_test.go — #1184 at the transport: an endpoint retry's WARN
// is written from the outbound request's context, which is the inbound request's.
// A real record through the real decorator under a real span, on a real dropped
// connection; RED when the site goes back to the plain form (shown in the PR).
// Send returns after the retries were logged: nothing waits.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type log1184 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *log1184) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func TestIssue1184_ARetryLogsTheRequestsTraceID(t *testing.T) {
	fastRetries(t, "1")
	addr, _ := acceptThenDrop(t)

	sink := &log1184{}
	log := slog.New(tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelWarn})))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("s1184").Start(xcontext.BuildContext(context.Background(), xcontext.WithLogger(log)), "GET /call")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Send(SendSiteStage, &http.Client{}, req); err == nil {
		t.Fatal("want the dropped connection's error")
	}

	sink.mu.Lock()
	raw := append([]byte(nil), sink.b.Bytes()...)
	sink.mu.Unlock()
	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(raw), &rec); err != nil {
		t.Fatalf("want one WARN line for one retry: %v\n%s", err, raw)
	}
	if rec["msg"] != "endpoint attempt failed; retrying" {
		t.Fatalf("PRECONDITION: unexpected line %v", rec["msg"])
	}
	if got, want := rec["trace_id"], span.SpanContext().TraceID().String(); got != want {
		t.Fatalf("#1184 RED: the retry line carries trace_id %v, the request's span is %s", got, want)
	}
}
