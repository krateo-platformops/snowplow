package tracing

import (
	"context"
	"log/slog"
	"slices"

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
//
// EACH KEY ONCE (#916). A key a derivation sets is on the record once, even
// when a later derivation sets it again at the same level: the later value
// replaces the earlier, as a map assignment does. The repeat that made this
// necessary is plumbing's: xcontext.WithLogger stamps "traceId" on whatever
// logger it is given (plumbing context/context.go:91), one that already
// carries it included. A context's logger reaches it again whenever a site
// carries that logger into a new context (cluster_list.go's async populate,
// resolve_populate.go's learned representative), as plumbing's use.Logger did
// before #1199 (server/use/logger.go:39, the 057 line's "traceId,user,traceId").
// The base handlers write every copy they are given, and readers disagree on
// which copy wins: ClickHouse's JSONExtractString takes the first,
// encoding/json the last. The decorator keeps the chain, so it drops the
// earlier copy and rebuilds; that happens only when a key repeats, and the
// usual derivation costs what it cost before. The same holds for the stamp: a
// top-level derivation that sets trace_id or span_id is left out of a
// correlated record, whose ids are the live span's. Out of scope: an attribute
// with an empty key (slog drops it, or inlines a group), and a key a call site
// passes with the record itself, which is that site's to name once.
type traceCorrelationHandler struct {
	root    slog.Handler // the undecorated base, before any With*/WithGroup
	ops     []derivation // derivation chain, in order; no key repeats within a level
	derived slog.Handler // ops applied to root, cached for the no-span path
	// stampShadowed: a top-level derivation sets trace_id or span_id, so a
	// correlated record replays the chain without them.
	stampShadowed bool
}

// derivation is one step of the chain: WithGroup(group) when group is set,
// otherwise WithAttrs(attrs).
type derivation struct {
	group string
	attrs []slog.Attr
}

func (d derivation) apply(h slog.Handler) slog.Handler {
	if d.group != "" {
		return h.WithGroup(d.group)
	}
	return h.WithAttrs(d.attrs)
}

const (
	traceIDKey = "trace_id"
	spanIDKey  = "span_id"
)

// stampKeys are the keys Handle stamps on a correlated record.
var stampKeys = []string{traceIDKey, spanIDKey}

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
		slog.String(traceIDKey, sc.TraceID().String()),
		slog.String(spanIDKey, sc.SpanID().String()),
	})
	ops := h.ops
	if h.stampShadowed {
		ops = without(ops, 0, firstGroup(ops), stampKeys)
	}
	for _, op := range ops {
		target = op.apply(target)
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
	attrs = lastOfEachKey(attrs)
	keys := make([]string, 0, len(attrs))
	for _, a := range attrs {
		if a.Key != "" {
			keys = append(keys, a.Key)
		}
	}
	start := levelStart(h.ops)
	if !sets(h.ops[start:], keys) {
		return h.extend(derivation{attrs: attrs})
	}
	// A key this level already holds: drop the earlier copy and rebuild.
	ops := append(without(h.ops, start, len(h.ops), keys), derivation{attrs: attrs})
	derived := h.root
	for _, op := range ops {
		derived = op.apply(derived)
	}
	return &traceCorrelationHandler{root: h.root, ops: ops, derived: derived, stampShadowed: shadowsStamp(ops)}
}

func (h *traceCorrelationHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h.extend(derivation{group: name})
}

func (h *traceCorrelationHandler) extend(d derivation) slog.Handler {
	ops := make([]derivation, len(h.ops), len(h.ops)+1)
	copy(ops, h.ops)
	ops = append(ops, d)
	return &traceCorrelationHandler{root: h.root, ops: ops, derived: d.apply(h.derived), stampShadowed: shadowsStamp(ops)}
}

// levelStart is the index of the first step after the last group: the steps
// from there on set keys at the level a new attribute lands on.
func levelStart(ops []derivation) int {
	for i := len(ops) - 1; i >= 0; i-- {
		if ops[i].group != "" {
			return i + 1
		}
	}
	return 0
}

// firstGroup is the index of the first group step, len(ops) when there is
// none: the steps before it set top-level keys.
func firstGroup(ops []derivation) int {
	for i, op := range ops {
		if op.group != "" {
			return i
		}
	}
	return len(ops)
}

// shadowsStamp reports whether a top-level step sets a key Handle stamps.
func shadowsStamp(ops []derivation) bool {
	return sets(ops[:firstGroup(ops)], stampKeys)
}

// sets reports whether an attribute step in ops sets one of keys.
func sets(ops []derivation, keys []string) bool {
	for _, op := range ops {
		for _, a := range op.attrs {
			if slices.Contains(keys, a.Key) {
				return true
			}
		}
	}
	return false
}

// without returns a copy of ops whose attribute steps in [from, to) no longer
// set any of keys; a step left with nothing is dropped. ops is not modified:
// the handlers derived from it share it.
func without(ops []derivation, from, to int, keys []string) []derivation {
	out := make([]derivation, 0, len(ops)+1)
	for i, op := range ops {
		if i < from || i >= to || op.group != "" {
			out = append(out, op)
			continue
		}
		kept := make([]slog.Attr, 0, len(op.attrs))
		for _, a := range op.attrs {
			if !slices.Contains(keys, a.Key) {
				kept = append(kept, a)
			}
		}
		if len(kept) > 0 {
			out = append(out, derivation{attrs: kept})
		}
	}
	return out
}

// lastOfEachKey returns a copy of attrs that keeps only the last copy of a key
// given more than once, in its place. Attributes with an empty key are kept as
// given.
// A derivation names a handful of attributes, so the scan is linear.
func lastOfEachKey(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for i, a := range attrs {
		if a.Key == "" || !slices.ContainsFunc(attrs[i+1:], func(b slog.Attr) bool { return b.Key == a.Key }) {
			out = append(out, a)
		}
	}
	return out
}
