// stage_error_sink.go — Ship 0.30.120: the error-aware Put-gate seam.
//
// THE DEFECT (pre-existing since 0.30.113): the SA-transport L1
// refresher re-resolves a whole RESTAction with no per-user JWT. A
// stage with exportJwt:true makes a nested snowplow /call loopback that
// needs the user's bearer token; without it the loopback 401s. The
// stage's continueOnError swallows the 401 as a STRUCTURALLY-VALID empty
// result — the resolver writes call.ErrorKey into its dict, the
// top-level RESTAction filter strips that key before encode, and the
// refresher Puts a ~1.9 KB empty envelope over the user's correct
// ~26 MB entry. Uniform under-serve, not a leak.
//
// WHY A CONTEXT SINK (architect's load-bearing finding): the error
// SIGNAL is gone by the time the bytes are encoded — the RESTAction's
// top-level filter strips the error key, and a generic JSON walk for a
// field named "error" would false-positive on legitimate fields. The
// signal MUST be read PRE-filter, at the moment the resolver writes
// dict[call.ErrorKey]. WithStageErrorSink threads an *atomic.Int64 down
// the resolve context; the resolver's three ErrorKey-write sites bump it
// when a sink is present; resolveAndPopulateL1 reads Load()>0 before its
// Put and declines to overwrite the prior good entry (TTL is the outer
// net). On the normal request path no sink is installed — Load is never
// called, Add is never called, the diff is byte-identical and zero-cost.
//
// Mirrors WithL1KeyContext / L1KeyFromContext (deps.go): a distinct
// unexported empty-struct key so external packages cannot collide via a
// raw string key.

package cache

import (
	"context"
	"sync/atomic"
)

// ctxKeyStageErrorSinkType is the typed empty-struct context key used by
// WithStageErrorSink / StageErrorSinkFromContext. Distinct unexported
// type — no cross-package raw-string-key collision.
type ctxKeyStageErrorSinkType struct{}

var ctxKeyStageErrorSink = ctxKeyStageErrorSinkType{}

// StageErrorSink counts stage errors observed during a refresh re-resolve
// AND captures the FIRST stage error's name + message as a diagnostic
// sample. Ship 0.30.254 / Task #301 (observability): the decline log
// previously surfaced only the count, so attributing a decline to its
// failing stage required a cross-grep against the resolver's error lines
// (two traces). The sample is captured once (first bump wins) so the
// decline log can name the stage + error inline.
//
// Count is the load-bearing gate signal (Count()>0 declines the Put);
// the sample is observability-only.
type StageErrorSink struct {
	count  atomic.Int64
	sample atomic.Pointer[stageErrSample]
	// parent is the enclosing resolve's sink when this is a CHILD sink
	// (WithChildPutGateSinks, #684); every bump is forwarded to it. nil for a
	// sink installed by WithStageErrorSink.
	parent *StageErrorSink
}

type stageErrSample struct {
	stage string
	err   string
}

// Bump records one stage error: increments the count and, on the FIRST
// bump only, stores the (stage, err) sample. nil-receiver-safe so the
// request-path no-op (nil sink) callers can call it unconditionally.
func (s *StageErrorSink) Bump(stage, err string) {
	if s == nil {
		return
	}
	smp := &stageErrSample{stage: stage, err: err}
	for cur := s; cur != nil; cur = cur.parent {
		cur.count.Add(1)
		// First-sample-wins via CAS from nil. Subsequent bumps leave the
		// first sample intact (it's the most causally-relevant for the
		// decline — later stages may fail as a consequence).
		cur.sample.CompareAndSwap(nil, smp)
	}
}

// Count returns the number of stage errors recorded. nil-receiver-safe
// (returns 0) so the request path's nil sink reads as "no error".
func (s *StageErrorSink) Count() int64 {
	if s == nil {
		return 0
	}
	return s.count.Load()
}

// Sample returns the first recorded (stage, err) sample, or ("","") when
// none was recorded. nil-receiver-safe.
func (s *StageErrorSink) Sample() (stage, err string) {
	if s == nil {
		return "", ""
	}
	if smp := s.sample.Load(); smp != nil {
		return smp.stage, smp.err
	}
	return "", ""
}

// WithStageErrorSink returns a child context carrying a fresh
// *StageErrorSink, plus the sink itself. The resolver reads the sink via
// StageErrorSinkFromContext at each dict[ErrorKey] write site and bumps
// it; the refresh-and-store path reads Count()>0 before its Put and
// declines to overwrite a good entry with a result produced under a
// stage error (and logs the sink's Sample() for attribution).
//
// Installed ONLY by the background refresher (resolveAndPopulateL1) — a
// normal request path never calls this, so its resolve carries no sink
// and the resolver's bump sites are no-ops (the diff is byte-identical
// to 0.30.119 on the request path).
func WithStageErrorSink(ctx context.Context) (context.Context, *StageErrorSink) {
	sink := &StageErrorSink{}
	if ctx == nil {
		return ctx, sink
	}
	return context.WithValue(ctx, ctxKeyStageErrorSink, sink), sink
}

// StageErrorSinkFromContext returns the *StageErrorSink attached to ctx
// by WithStageErrorSink, or nil when none is attached (the normal request
// path). A nil return MUST be treated by callers as "no sink — do not
// record"; it is not an error. The sink's methods are nil-receiver-safe.
func StageErrorSinkFromContext(ctx context.Context) *StageErrorSink {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeyStageErrorSink).(*StageErrorSink)
	return v
}

// RefresherSkippedStageError returns the process-wide count of L1 Puts
// the error-aware gate declined because a stage error was observed
// during the refresh re-resolve (Ship 0.30.120 layer (b)). The counter
// itself lives on the refresher singleton (refresher.go); this is the
// accessor the dispatchers package calls when the gate fires.
func RefresherSkippedStageError() uint64 {
	return refresherSingleton().refresherSkippedStageError.Load()
}

// BumpRefresherSkippedStageError increments the error-aware-gate
// declined-Put counter. Called by resolveAndPopulateL1's Put-gate
// (dispatchers package) when a stage error was observed during the
// background refresh re-resolve.
func BumpRefresherSkippedStageError() {
	refresherSingleton().refresherSkippedStageError.Add(1)
}

// NOTE — the Ship 0.30.120 layer-(a) RefresherSkippedExportJwt /
// BumpRefresherSkippedExportJwt accessors were REMOVED at Ship 0.30.123
// (#155). Layer (a) (the exportJwt skip-to-TTL net) is obsolete:
// in-process nested /call now resolves an exportJwt loopback stage
// correctly, so the refresher no longer declines to refresh those
// RESTActions. Layer (b) — the error-aware Put-gate above — STAYS as the
// general backstop for any genuinely-failing stage.
