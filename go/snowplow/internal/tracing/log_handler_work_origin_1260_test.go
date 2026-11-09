package tracing

// log_handler_work_origin_1260_test.go — #1260: the decorator stamps
// work_origin at the TOP of every record whose context names a driver, with or
// without a span, and on derived loggers too; a context naming none gets
// nothing and stays byte-identical. The classifier here is a stand-in: the
// production one (cache.WorkOrigin, passed by buildLogHandler) is pinned in
// package main and in dispatchers.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

type workOriginKey1260 struct{}

func origin1260(ctx context.Context) (string, bool) {
	s, _ := ctx.Value(workOriginKey1260{}).(string)
	return s, s != ""
}

func decode1260(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(b), &rec); err != nil {
		t.Fatalf("not one JSON record: %v: %s", err, b)
	}
	return rec
}

func TestIssue1260_Decorator_StampsWorkOriginTopLevel(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
	})
	spanned := trace.ContextWithSpanContext(context.Background(), sc)
	cases := []struct {
		name       string
		ctx        context.Context
		wantOrigin string
		wantTrace  bool
	}{
		{"origin, no span (background work)", context.WithValue(context.Background(), workOriginKey1260{}, "refresher"), "refresher", false},
		{"origin and span (a request)", context.WithValue(spanned, workOriginKey1260{}, "request"), "request", true},
		{"span, no origin", spanned, "", true},
		{"neither", context.Background(), "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			base := slog.New(NewCorrelationHandler(slog.NewJSONHandler(&buf, nil), origin1260))
			// A derived logger, grouped: the stamp must still land at the top.
			base.With("k", "v").WithGroup("g").WarnContext(c.ctx, "m1260", slog.String("a", "b"))
			rec := decode1260(t, buf.Bytes())
			got, has := rec["work_origin"]
			if c.wantOrigin == "" && has {
				t.Fatalf("no driver on ctx, yet work_origin=%v: %v", got, rec)
			}
			if c.wantOrigin != "" && got != c.wantOrigin {
				t.Fatalf("#1260 RED: work_origin=%v at the top of the record, want %q: %v", got, c.wantOrigin, rec)
			}
			if _, has := rec["trace_id"]; has != c.wantTrace {
				t.Fatalf("trace_id present=%v, want %v: %v", has, c.wantTrace, rec)
			}
			if g, _ := rec["g"].(map[string]any); g == nil || g["a"] != "b" {
				t.Fatalf("the derivation chain was not replayed: %v", rec)
			}
		})
	}
}

// A context with no driver writes exactly what the undecorated base writes.
func TestIssue1260_Decorator_NoOriginIsByteIdentical(t *testing.T) {
	var got, want bytes.Buffer
	opts := &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}}
	slog.New(NewCorrelationHandler(slog.NewJSONHandler(&got, opts), origin1260)).WarnContext(context.Background(), "m", "k", "v")
	slog.New(slog.NewJSONHandler(&want, opts)).WarnContext(context.Background(), "m", "k", "v")
	if got.String() != want.String() {
		t.Fatalf("an unmarked record changed:\n got %s\nwant %s", got.String(), want.String())
	}
}

// NewTraceCorrelationHandler (no classifier) never stamps work_origin.
func TestIssue1260_Decorator_NilClassifierNeverStamps(t *testing.T) {
	var buf bytes.Buffer
	slog.New(NewTraceCorrelationHandler(slog.NewJSONHandler(&buf, nil))).
		WarnContext(context.WithValue(context.Background(), workOriginKey1260{}, "refresher"), "m")
	if rec := decode1260(t, buf.Bytes()); rec["work_origin"] != nil {
		t.Fatalf("a decorator built without a classifier stamped work_origin: %v", rec)
	}
}
