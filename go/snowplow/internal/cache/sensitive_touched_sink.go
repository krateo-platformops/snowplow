// sensitive_touched_sink.go — #398 enforcement point 3: no Secret body is
// resident in any resolved-output L1 cell.
//
// The resolver bumps this sink at the per-call dispatch site whenever a call
// targets a sensitive resource (IsSensitiveResource — core v1/secrets). Every
// resolved-output Put site (customer restactions / widgets, the widgetContent
// shell, the raFullList cell, the seed terminal write, the refresher re-Put)
// reads Count()>0 and declines its write; the body is still SERVED to its own
// requester. A Secret read is therefore always a live apiserver read.
//
// WHY THIS STAYS AFTER #423 (keys by the requester's full binding set). #423
// made a resolved-output cell RBAC-class-exact, so sharing a Secret-bearing
// body among identical-binding-set users is not, by itself, a leak. It is kept
// because:
//   - #398's thesis is that "one bug away from a cross-user leak is too thin a
//     margin for Secrets" — and #423/#424 were exactly such a bug, found and
//     fixed in this hotfix cycle. For credential material the cache adds no
//     value worth that margin: after portal#285 Secret reads are rare, so
//     declining the Put costs a live GET on an infrequent path.
//   - revocation is then immediate by construction (nothing to rotate away
//     from), instead of depending on the key-rotation machinery reaching every
//     path that can change a verdict;
//   - credential bytes do not sit in the long-lived process heap (L1 lives up to
//     the max-age bound), which a heap dump / debug surface regression would
//     otherwise expose.
//
// CHAINING. Nested resolves (an apiRef'd RESTAction resolved in-process inside
// a widget, the raFullList unpaginated resolve) may install their own sink. A
// child sink forwards every bump to its parent, so an outer Put site always
// observes a sensitive read made anywhere beneath it.

package cache

import (
	"context"
	"sync/atomic"
)

type ctxKeySensitiveTouchedSinkType struct{}

var ctxKeySensitiveTouchedSink = ctxKeySensitiveTouchedSinkType{}

// SensitiveTouchedSink counts sensitive-resource dispatches made under a
// resolve. Atomic: bumped from the resolver's errgroup workers.
type SensitiveTouchedSink struct {
	count  atomic.Int64
	parent *SensitiveTouchedSink
}

// Bump records one sensitive dispatch here and in every ancestor sink.
// nil-receiver-safe.
func (s *SensitiveTouchedSink) Bump() {
	for cur := s; cur != nil; cur = cur.parent {
		cur.count.Add(1)
	}
}

// Count returns the sensitive dispatches recorded (0 for a nil receiver).
func (s *SensitiveTouchedSink) Count() int64 {
	if s == nil {
		return 0
	}
	return s.count.Load()
}

// WithSensitiveTouchedSink returns a child context carrying a fresh sink that
// forwards to any sink already on ctx.
func WithSensitiveTouchedSink(ctx context.Context) (context.Context, *SensitiveTouchedSink) {
	sink := &SensitiveTouchedSink{parent: SensitiveTouchedSinkFromContext(ctx)}
	if ctx == nil {
		return ctx, sink
	}
	return context.WithValue(ctx, ctxKeySensitiveTouchedSink, sink), sink
}

// SensitiveTouchedSinkFromContext returns the sink on ctx, or nil.
func SensitiveTouchedSinkFromContext(ctx context.Context) *SensitiveTouchedSink {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeySensitiveTouchedSink).(*SensitiveTouchedSink)
	return v
}

// sensitiveSkippedPut counts resolved-output Puts declined because the resolve
// touched a sensitive resource.
var sensitiveSkippedPut atomic.Uint64

// DeclineSensitivePut reports whether the resolve under ctx touched a sensitive
// resource, counting the declined Put when it did. Call exactly once per
// candidate Put, at the decision point.
//
// #443: under the inert (dry-run) flag the decline STILL fires (it returns
// true): the #398 decline and the inert Put refusal are independent checks, so
// neither hides the other. Only the counter is skipped, because dry-run traffic
// must not count as a decline.
func DeclineSensitivePut(ctx context.Context) bool {
	if SensitiveTouchedSinkFromContext(ctx).Count() == 0 {
		return false
	}
	if !Inert(ctx) {
		sensitiveSkippedPut.Add(1)
	}
	return true
}

// SensitiveSkippedPutForTest reads the declined-Put counter.
func SensitiveSkippedPutForTest() uint64 { return sensitiveSkippedPut.Load() }
