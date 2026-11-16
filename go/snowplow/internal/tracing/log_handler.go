package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// traceCorrelationHandler decorates a slog.Handler so that every record
// emitted with a context carrying a VALID OTel span context gains two
// TOP-LEVEL attributes:
//
//	trace_id  the W3C trace id (hex), joinable to otel_traces.TraceId
//	span_id   the W3C span id (hex)
//
// NAMING — read this before touching either field. Every snowplow record
// ALREADY carries "traceId" (camelCase): the Krateo plumbing shortid
// correlation id (xcontext.TraceId, surfaced as status.traceId), which is
// unrelated to W3C. The decorator adds "trace_id" (snake_case), the OTel
// id. Two near-identical keys on one record is an operator trap the docs
// and the dashboard name explicitly; the HyperDX source expression for
// correlation is JSONExtractString(Body, 'trace_id').
//
// TOP-LEVEL, EVEN ON DERIVED LOGGERS. A logger derived via
// log.With(...) / log.WithGroup(...) derives its handler; if the
// decorator simply wrapped the derived base and appended the two attrs at
// Handle time, a WithGroup("g") logger would nest them as g.trace_id and
// the ClickHouse expression above would miss them. So the decorator
// keeps the ROOT base plus the ordered derivation chain, and on a
// correlated record applies the trace attrs to the root FIRST and then
// replays the chain — the ids always land at the top of the record. The
// cached derived handler serves the uncorrelated (no span) path with no
// per-record allocation, so a context-less or tracing-off record costs
// exactly what it cost before 1.12.4.
//
// NO-OP WHEN TRACING IS OFF. With the OTel pipeline disabled (or for any
// slog call made without a request context — log.Info rather than
// log.InfoContext — which reaches the handler with context.Background())
// SpanContextFromContext returns an invalid context, nothing is appended,
// and the record is byte-identical to the pre-1.12.4 emission. The F6
// byte-identity arm pins that. Note that a span context is VALID (and
// the ids are stamped) even when the sampler dropped the span: under
// any sampler, ratio or parent-based, a record that reaches this handler with a
// served request's context carries that request's trace_id — which is what
// lets an operator group one request's log lines together whether or not its
// trace was exported.
//
// WHICH RECORDS THOSE ARE IS DECIDED AT THE CALL, not here (#1184). A WARN or
// ERROR passes the context in scope, a site with none says why in a classified
// waiver, and scripts/check_request_log_context.sh keeps it so. At the
// production LOG_LEVEL=warn a served request's own lines therefore carry its
// trace_id. Not covered, and correct: an Info or Debug line in the plain form;
// a route with no span (probes, /debug, the RUM upload); a waived process-wide
// init that happens to run on a request's goroutine; plumbing's own lines; and
// background work, whose context has no span. Work a request detaches keeps the
// id only if it keeps the context: a demand revalidation or a shared fill does,
// so its lines carry the trace_id of the request that started it, after that
// span ended; a stale retry starts from a fresh context and has none. No span
// is invented for any of them.
//
// WHAT THIS DOES NOT DO. The trace_id is only PRESENT on the record; the
// platform's filelog pipeline has no json_parser, so otel_logs.TraceId
// stays empty until the krateo-observability collector fix lands. That
// half is out of this release (design §4.2, C9).
type traceCorrelationHandler struct {
	root    slog.Handler                      // the undecorated base, before any With*/WithGroup
	ops     []func(slog.Handler) slog.Handler // derivation chain, in order
	derived slog.Handler                      // ops applied to root, cached for the no-span path
	level   level                             // the keys the current level holds, each once (#916)
}

// NewTraceCorrelationHandler wraps base in the decorator. buildLogHandler in
// package main is the production caller; it lives here so an arm anywhere in
// the module can send a record through the real decorator.
func NewTraceCorrelationHandler(base slog.Handler) slog.Handler {
	return &traceCorrelationHandler{root: base, derived: base}
}

// Enabled defers to the derived handler so level filtering is unchanged.
func (h *traceCorrelationHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.derived.Enabled(ctx, l)
}

// Handle stamps trace_id/span_id at the top level when a valid span is on
// ctx, then delegates; without a span it delegates to the cached derived
// handler untouched.
func (h *traceCorrelationHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return h.derived.Handle(ctx, r)
	}
	target := h.root.WithAttrs([]slog.Attr{
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	})
	for _, op := range h.ops {
		target = op(target)
	}
	return target.Handle(ctx, r)
}

// WithAttrs / WithGroup extend the derivation chain (and the cached
// derived handler) so a logger created via log.With(...) or
// log.WithGroup(...) keeps the decorator. Without these two, every
// derived logger would silently lose correlation — the exact failure the
// F6 derived-logger arm exists to catch.
func (h *traceCorrelationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	cp := lastOfEachKey(attrs)
	if h.level.holdsAny(cp) {
		return h.replacing(cp)
	}
	d := h.extend(func(b slog.Handler) slog.Handler { return b.WithAttrs(cp) })
	d.level = h.level.plus(cp)
	return d
}

func (h *traceCorrelationHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	d := h.extend(func(b slog.Handler) slog.Handler { return b.WithGroup(name) })
	d.level = level{start: len(d.ops)}
	return d
}

func (h *traceCorrelationHandler) extend(op func(slog.Handler) slog.Handler) *traceCorrelationHandler {
	ops := make([]func(slog.Handler) slog.Handler, len(h.ops), len(h.ops)+1)
	copy(ops, h.ops)
	ops = append(ops, op)
	return &traceCorrelationHandler{root: h.root, ops: ops, derived: op(h.derived)}
}
