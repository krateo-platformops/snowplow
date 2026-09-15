// external_touched_sink.go — the external-fetch-aware Put-gate seam.
//
// THE DEFECT: a RESTAction whose resolve touches a genuine EXTERNAL endpoint
// (an endpointRef to a non-apiserver URL — the live httpFetchAllowingNonJSON
// path, ADR 0006) has NO informer/dep edge to invalidate it. RecordList /
// Record fire only for apiserver GVRs, so an external-RA L1 entry serves
// up-to-TTL-stale data and is NEVER dirty-marked when the source changes.
// Skipping the L1 Put for any resolve that touched the external fetch makes
// every /call re-resolve and re-fetch the external API LIVE (fresh).
//
// WHY A CONTEXT SINK (mirrors StageErrorSink, the load-bearing finding for
// 0.30.120): the "did this resolve touch an external endpoint" SIGNAL is at
// the dispatch SITE — the moment control reaches httpFetchAllowingNonJSON
// (resolve.go) — not derivable from the Endpoint shape downstream (a
// ${...}-templated path that resolves to an apiserver path is internal; only
// control reaching the external fetch is authoritative). By construction the
// external fetch is the FALL-THROUGH branch: the loopback / informer-pivot /
// internal-rest-config paths all return before it, so a bump at the dispatch
// site cannot mis-classify an internal branch as external (proposal §"Detection").
//
// WithExternalTouchedSink threads an *atomic.Int64-backed sink down the
// resolve context; the external-fetch site bumps it; each L1 Put site reads
// Count()>0 and DECLINES the Put (body still served — identical to the #313
// partial-result posture). Declining the Put also declines the self-dep
// Record, so an external RA never enters the DepTracker (no dirty-marks, no
// refresher churn). On the request path a sink IS installed (this is the
// first request-path consumer, unlike StageErrorSink which is refresher-only);
// the bump site is a nil-receiver-safe no-op when no sink is present.
//
// Mirrors WithL1KeyContext / StageErrorSink (deps.go / stage_error_sink.go):
// a distinct unexported empty-struct key so external packages cannot collide
// via a raw string key.

package cache

import (
	"context"
	"sync/atomic"
)

// ctxKeyExternalTouchedSinkType is the typed empty-struct context key used by
// WithExternalTouchedSink / ExternalTouchedSinkFromContext. Distinct
// unexported type — no cross-package raw-string-key collision.
type ctxKeyExternalTouchedSinkType struct{}

var ctxKeyExternalTouchedSink = ctxKeyExternalTouchedSinkType{}

// ExternalTouchedSink counts how many times a resolve under this context
// reached the live external fetch (httpFetchAllowingNonJSON). Count()>0 means
// "this resolve touched a genuine external endpoint" → the result MUST NOT be
// Put into L1 (no dep edge can invalidate it). The sink is bumped from the
// errgroup workers of a multi-stage resolve, so all access is atomic.
type ExternalTouchedSink struct {
	count atomic.Int64
}

// Bump records one external-fetch touch. nil-receiver-safe so the bump site
// can call it unconditionally even when no sink is installed on ctx.
func (s *ExternalTouchedSink) Bump() {
	if s == nil {
		return
	}
	s.count.Add(1)
}

// Count returns the number of external-fetch touches recorded.
// nil-receiver-safe (returns 0) so a Put site that finds no sink reads as
// "no external touch — Put as normal".
func (s *ExternalTouchedSink) Count() int64 {
	if s == nil {
		return 0
	}
	return s.count.Load()
}

// WithExternalTouchedSink returns a child context carrying a fresh
// *ExternalTouchedSink, plus the sink itself. The resolver bumps the sink at
// the external-fetch dispatch site (resolve.go httpFetchAllowingNonJSON); the
// L1 Put sites read Count()>0 before their Put and decline to cache a result
// produced by touching an external endpoint (and decline the self-dep Record).
//
// Installed by EVERY request/refresh/walker resolve entry that may Put an L1
// entry (the 5 Put surfaces' callers): restactions.go, widgets.go,
// phase1_walk.go (the F2 walker resolveCtx), resolve_populate.go, and the
// apiref chokepoint inherits it via the widget→apiref ctx. A resolve with no
// sink installed bumps a nil receiver (no-op) and Puts as before.
func WithExternalTouchedSink(ctx context.Context) (context.Context, *ExternalTouchedSink) {
	sink := &ExternalTouchedSink{}
	if ctx == nil {
		return ctx, sink
	}
	return context.WithValue(ctx, ctxKeyExternalTouchedSink, sink), sink
}

// ExternalTouchedSinkFromContext returns the *ExternalTouchedSink attached to
// ctx by WithExternalTouchedSink, or nil when none is attached. A nil return
// MUST be treated by callers as "no sink — Put as normal"; it is not an error.
// The sink's methods are nil-receiver-safe.
func ExternalTouchedSinkFromContext(ctx context.Context) *ExternalTouchedSink {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeyExternalTouchedSink).(*ExternalTouchedSink)
	return v
}

// ExternalSkippedPut returns the process-wide count of L1 Puts the
// external-touched gate declined because the resolve touched a genuine
// external endpoint. Production falsifier for "did the gate fire?".
func ExternalSkippedPut() uint64 {
	r := refresherPeek() // #203: a read never constructs the refresher
	if r == nil {
		return 0
	}
	return r.externalSkippedPut.Load()
}

// BumpExternalSkippedPut increments the external-touched-gate declined-Put
// counter. Called by each L1 Put site when the gate fires.
func BumpExternalSkippedPut() {
	refresherSingleton().externalSkippedPut.Add(1)
}
