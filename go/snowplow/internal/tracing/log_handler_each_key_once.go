package tracing

import (
	"log/slog"
	"slices"
)

// EACH KEY ONCE (#916). A key a derivation (log.With) sets is on the record
// once, even when a later derivation sets it again at the same level: the
// later value replaces the earlier, as a map assignment does.
//
// The repeat that made this necessary is plumbing's: xcontext.WithLogger stamps
// "traceId" on whatever logger it is given (plumbing context/context.go:91),
// one that already carries it included. A context's logger reaches it again
// whenever a site carries that logger into a new context (cluster_list.go's
// async populate, resolve_populate.go's learned representative), as plumbing's
// use.Logger did on every authenticated request before #1199
// (server/use/logger.go:39, the 1.12.56 line's "traceId,user,traceId"). A
// logger does not say which keys it carries, so a site cannot tell that its
// logger already holds one; this decorator, which every snowplow record passes
// through, keeps the derivation chain and can. The base handlers write every
// copy they are given, and readers disagree on which copy wins: ClickHouse's
// JSONExtractString takes the first, encoding/json the last.
//
// HOW. The decorator remembers the attributes of the current level (those set
// since the last WithGroup). A derivation that sets none of them is appended to
// the chain as before, so the usual derivation costs what it cost before. One
// that sets a key the level holds replaces the level's steps with one step
// that holds each key once, the later value in the later place, and rebuilds
// the cached derived handler from the root. The parent's chain is not
// modified: siblings derived from it share it.
//
// Keys at different levels are different keys (g.traceId is not traceId).
// Out of scope: an attribute with an empty key (slog drops it, or inlines a
// group's members), a key a call site passes with the record itself (that
// site's to name once), and the trace_id/span_id this decorator stamps (no
// snowplow derivation sets either key).

// level is what the current level of a derivation chain holds.
type level struct {
	start int         // index in ops of the level's first step (the step after the last WithGroup)
	attrs []slog.Attr // the attributes the level's steps set, in order, each key once
}

// holdsAny reports whether the level already holds a key of attrs.
func (l level) holdsAny(attrs []slog.Attr) bool {
	return slices.ContainsFunc(attrs, func(a slog.Attr) bool { return holds(l.attrs, a.Key) })
}

// plus is the level after a derivation that sets attrs, none of which it holds.
func (l level) plus(attrs []slog.Attr) level {
	return level{start: l.start, attrs: slices.Concat(l.attrs, attrs)}
}

// replacing derives h with attrs when its level already holds one of their
// keys: the level's steps become one, without the earlier copies.
func (h *traceCorrelationHandler) replacing(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.level.attrs)+len(attrs))
	for _, a := range h.level.attrs {
		if !holds(attrs, a.Key) {
			merged = append(merged, a)
		}
	}
	merged = append(merged, attrs...)
	ops := make([]func(slog.Handler) slog.Handler, h.level.start, h.level.start+1)
	copy(ops, h.ops)
	ops = append(ops, func(b slog.Handler) slog.Handler { return b.WithAttrs(merged) })
	derived := h.root
	for _, op := range ops {
		derived = op(derived)
	}
	d := *h
	d.ops, d.derived, d.level = ops, derived, level{start: h.level.start, attrs: merged}
	return &d
}

// holds reports whether attrs sets key. An empty key is never held.
func holds(attrs []slog.Attr, key string) bool {
	return key != "" && slices.ContainsFunc(attrs, func(a slog.Attr) bool { return a.Key == key })
}

// lastOfEachKey returns a copy of attrs that keeps only the last copy of a key
// given more than once, in its place. Attributes with an empty key are kept as
// given. A derivation names a handful of attributes, so the scan is linear.
func lastOfEachKey(attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs))
	for i, a := range attrs {
		if !holds(attrs[i+1:], a.Key) {
			out = append(out, a)
		}
	}
	return out
}
