package tracing

// log_handler_bench_test.go — #1184: what passing the request's context costs a
// WARN. The request logger is the shape use.Logger derives (traceId plus a user
// group), so the correlated path replays a real derivation chain.
//
//	go test ./internal/tracing -run '^$' -bench BenchmarkWarn -benchmem

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func benchLoggers() (bare, derived *slog.Logger, spanCtx context.Context) {
	bare = slog.New(NewTraceCorrelationHandler(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})))
	derived = bare.With("traceId", "short-id",
		slog.Group("user", slog.String("name", "n"), slog.String("groups", "g")))
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
	})
	return bare, derived, trace.ContextWithSpanContext(context.Background(), sc)
}

func BenchmarkWarn(b *testing.B) {
	bare, derived, spanCtx := benchLoggers()
	attrs := []slog.Attr{slog.String("subsystem", "cache"), slog.String("gvr", "v1/configmaps"), slog.Int("n", 3)}
	args := []any{attrs[0], attrs[1], attrs[2]}
	for _, c := range []struct {
		name string
		emit func()
	}{
		{"bare/plain", func() { bare.Warn("m", args...) }},
		{"bare/context-no-span", func() { bare.WarnContext(context.Background(), "m", args...) }},
		{"bare/context-span", func() { bare.WarnContext(spanCtx, "m", args...) }},
		{"derived/plain", func() { derived.Warn("m", args...) }},
		{"derived/context-span", func() { derived.WarnContext(spanCtx, "m", args...) }},
		{"disabled/plain", func() { bare.Info("m", args...) }},
		{"disabled/context-span", func() { bare.InfoContext(spanCtx, "m", args...) }},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.emit()
			}
		})
	}
}
