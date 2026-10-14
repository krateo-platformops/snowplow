// per_identity_forbidden.go — #681: cache a resolved RESTAction / widget whose
// ONLY stage errors are per-identity forbidden (owner ruling, 2026-10-14).
//
// THE DEFECT. Every Put site declined a resolve that recorded ANY stage error
// (the 0.30.120 / #313 stage-error gate). A step correctly denied to the
// requester — Kubernetes says 403, or snowplow's per-identity re-gate denies —
// is a stage error under continueOnError, so for a user without access to one
// step (the 7 portal RAs reading blueprints-catalog-index on krateo-057) the
// whole RESTAction was NEVER cached: every visit was a cold navigation, for a
// result that is correct (smaller) and deterministic for that identity.
//
// WHAT COUNTS (typed only — never message text). A stage error is a
// per-identity forbidden iff it is one of:
//
//	F1  branch C's re-gate deny (api regateDeniedError): snowplow's in-process
//	    `get` evaluation for the ctx identity said no (refresher, seed).
//	F2  the apiserver's 403 Forbidden on the requester's OWN dial (/call): the
//	    response Status Code==403 && Reason==Forbidden, for a stage with no
//	    endpointRef (the requester's own clientconfig), not an SA dial, on a
//	    parseable apiserver resource path. An endpointRef host's 403 (a rate
//	    limit, say) never counts.
//
// Everything else (timeouts, 5xx, 404, decode / jq, iterator, the branch-E SA
// refusal, a genuine external touch) keeps declining exactly as before.
//
// HOW IT IS COUNTED. The resolver bumps this sink at the SAME site, for the SAME
// item, as the StageErrorSink bump (stage). An F2 dial also bumped the
// ExternalTouchedSink before the fetch — it reached the external fall-through,
// where the per-user apiserver dial lives — so it is counted again here
// (external), and GenuineExternal subtracts it: an own-credential apiserver 403
// is not an external touch.
//
// WHERE IT IS READ. DenialOnly is the ONE Put predicate for the /call RA and
// widget gates, the refresher and the cohort seed; GenuineExternal is the ONE
// external count for those gates AND for every declined-external (DES) mark site
// (#656 seed / engine, #680 walk).
//
// NO NEW KEY DIMENSION. A denial-only cell is keyed exactly like every other
// per-identity cell (BindingUID + SubjectBindingSet + RBACSubGen): requesters
// that share the binding SET get identical verdicts for every step, so they get
// an identical denial-only body (with the identity-free canonical error text the
// resolver writes for F1 and F2); any other set derives another key. This relies
// on the invariant every L1 cell already makes — RBAC is the authorizer that
// decides per-identity reads — and a denial-only body only ever carries LESS
// data than the fuller one, so it introduces no new disclosure.

package cache

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"
)

type ctxKeyPerIdentityForbiddenSinkType struct{}

var ctxKeyPerIdentityForbiddenSink = ctxKeyPerIdentityForbiddenSinkType{}

// PerIdentityForbiddenSink counts a resolve's per-identity forbidden stage
// errors (stage) and, of those, the ones whose dial also bumped the
// ExternalTouchedSink (external — F2). nil-receiver-safe throughout.
type PerIdentityForbiddenSink struct {
	stage    atomic.Int64
	external atomic.Int64
}

// BumpStage records one per-identity forbidden stage error (F1 or F2).
func (s *PerIdentityForbiddenSink) BumpStage() {
	if s != nil {
		s.stage.Add(1)
	}
}

// BumpExternal records that a per-identity forbidden (F2) dial was the one that
// bumped the ExternalTouchedSink.
func (s *PerIdentityForbiddenSink) BumpExternal() {
	if s != nil {
		s.external.Add(1)
	}
}

// Stage returns the per-identity forbidden stage-error count.
func (s *PerIdentityForbiddenSink) Stage() int64 {
	if s == nil {
		return 0
	}
	return s.stage.Load()
}

// External returns the count of external-touch bumps that were F2 dials.
func (s *PerIdentityForbiddenSink) External() int64 {
	if s == nil {
		return 0
	}
	return s.external.Load()
}

// WithPerIdentityForbiddenSink installs a fresh sink on ctx. Installed wherever
// a StageErrorSink is (the /call RA and widget handlers, the refresher, the
// cohort seed); nested resolves share it through the ctx.
func WithPerIdentityForbiddenSink(ctx context.Context) (context.Context, *PerIdentityForbiddenSink) {
	s := &PerIdentityForbiddenSink{}
	if ctx == nil {
		return ctx, s
	}
	return context.WithValue(ctx, ctxKeyPerIdentityForbiddenSink, s), s
}

// PerIdentityForbiddenSinkFromContext returns the sink on ctx, or nil.
func PerIdentityForbiddenSinkFromContext(ctx context.Context) *PerIdentityForbiddenSink {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKeyPerIdentityForbiddenSink).(*PerIdentityForbiddenSink)
	return s
}

// GenuineExternal is the external-touch count with the own-credential apiserver
// 403s (F2) removed — the ONE definition every Put gate and every
// declined-external mark site uses (#681 C2). Never negative.
func GenuineExternal(ext *ExternalTouchedSink, f *PerIdentityForbiddenSink) int64 {
	n := ext.Count() - f.External()
	if n < 0 {
		return 0
	}
	return n
}

// DenialOnly reports whether a resolve whose stage-error sink is non-empty may
// still be Put: every stage error was a per-identity forbidden, no genuine
// external endpoint was touched, and the cell's class is identity-bound (a
// shared identity-free shell must never hold one requester's denial).
func DenialOnly(stage *StageErrorSink, f *PerIdentityForbiddenSink, ext *ExternalTouchedSink, class string) bool {
	n := stage.Count()
	return n > 0 && n == f.Stage() && GenuineExternal(ext, f) == 0 && !IsIdentityFreeClass(class)
}

// --- snowplow_resolved_cache_denial_only_put_total{site} --------------------

// The Put sites (a CLOSED set, every cell pre-populated).
const (
	DenialOnlyPutSiteCallRA     = "call_ra"
	DenialOnlyPutSiteCallWidget = "call_widget"
	DenialOnlyPutSiteRefresher  = "refresher"
	DenialOnlyPutSiteSeed       = "seed"
)

// DenialOnlyPutSites is the closed site set, in emit order.
var DenialOnlyPutSites = []string{
	DenialOnlyPutSiteCallRA, DenialOnlyPutSiteCallWidget, DenialOnlyPutSiteRefresher, DenialOnlyPutSiteSeed,
}

var (
	denialOnlyPutMu sync.Mutex
	denialOnlyPut   = newDenialOnlyPut()
)

func newDenialOnlyPut() map[string]uint64 {
	m := make(map[string]uint64, len(DenialOnlyPutSites))
	for _, s := range DenialOnlyPutSites {
		m[s] = 0
	}
	return m
}

// RecordDenialOnlyPut counts one denial-only result admitted to a Put at site.
// A site outside the closed set is dropped.
func RecordDenialOnlyPut(site string) {
	denialOnlyPutMu.Lock()
	if _, ok := denialOnlyPut[site]; ok {
		denialOnlyPut[site]++
	}
	denialOnlyPutMu.Unlock()
}

// DenialOnlyPutCount is one {site} cell.
type DenialOnlyPutCount struct {
	Site  string
	Count uint64
}

// DenialOnlyPutSnapshot returns every site cell, zeros included.
func DenialOnlyPutSnapshot() []DenialOnlyPutCount {
	denialOnlyPutMu.Lock()
	defer denialOnlyPutMu.Unlock()
	out := make([]DenialOnlyPutCount, 0, len(DenialOnlyPutSites))
	for _, s := range DenialOnlyPutSites {
		out = append(out, DenialOnlyPutCount{Site: s, Count: denialOnlyPut[s]})
	}
	return out
}

// ResetDenialOnlyPutForTest zeroes the breakdown. Tests only.
func ResetDenialOnlyPutForTest() {
	denialOnlyPutMu.Lock()
	denialOnlyPut = newDenialOnlyPut()
	denialOnlyPutMu.Unlock()
}

// CFG-1: published only with the cache on.
func init() {
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_resolved_cache_denial_only_put_total", expvar.Func(func() any {
		m := map[string]uint64{}
		for _, c := range DenialOnlyPutSnapshot() {
			m[c.Site] = c.Count
		}
		return m
	}))
}
