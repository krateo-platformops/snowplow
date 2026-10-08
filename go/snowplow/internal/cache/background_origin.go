// background_origin.go — #563: WHICH background driver is this resolve, and the
// resourceRef-denial attribution keyed on it.
//
// WHY AN ORIGIN AT ALL. WithBackgroundResolve (deps.go) answers "is this a
// customer?" — one bit, which is all the C5 admission gate and
// rbac.MustRegateSADial need. It has THREE production producers (the refresher
// resolve_populate.go, the per-cohort seed phase1_pip_seed.go, the prewarm
// engine boot re-drive prewarm_engine_boot.go), and the question #563 asks is
// not "customer?" but "WHICH of them is generating this": 83% of the 057 log
// was one background denial line and the single bit could not say whether it
// came from the refresher or the seed. A {background, serve} counter cannot
// close that gap, so the origin rides the same stamp.
//
// It REFINES the marker, never replaces it: WithBackgroundResolveOrigin calls
// WithBackgroundResolve, so every existing reader (the aggregate admission
// gate, MustRegateSADial's #268/#269 conjunct) sees exactly what it saw before.
//
// WHY THE CLASSIFIER IS ONE FUNCTION. The log LEVEL of a denial and the counter
// CELL it lands in are the same question asked twice; #214 answered it in the
// resolver with an inline predicate, and the refresher then drifted out of that
// predicate for 12 releases. RefDenialOrigin is the single answer: the call site
// counts it and derives the level from it, so a new background driver cannot be
// Debug-logged but mis-counted, or counted but WARN-logged.

package cache

import (
	"context"
	"expvar"
	"sync"
)

// The background origins — one per production WithBackgroundResolve producer.
// Each is stamped at the ROOT that owns the work, so a nested resolve inherits
// its driver's name however deep the tree goes.
const (
	// BackgroundOriginRefresher — the refresher's re-resolve
	// (dispatchers/resolve_populate.go, resolveOnceProd).
	BackgroundOriginRefresher = "refresher"
	// BackgroundOriginCohortSeed — the per-identity cohort seed
	// (dispatchers/phase1_pip_seed.go, withCohortSeedContext).
	BackgroundOriginCohortSeed = "cohort-seed"
	// BackgroundOriginPrewarmEngineBoot — the prewarm engine's boot-scope
	// re-drive (dispatchers/prewarm_engine_boot.go, rePrewarmBootScoped).
	BackgroundOriginPrewarmEngineBoot = "prewarm-engine-boot"
)

// The two non-background cells of the denial counter.
const (
	// RefDenialOriginPrewarmPath — a ctx marked WithPrewarmPath and NOT
	// WithBackgroundResolve: the SA content prewarm
	// (dispatchers/phase1_content_prewarm.go:251). This is #214's population,
	// and it is a SEPARATE cell from the three background origins because that
	// driver carries a different marker, not because it is a special case.
	RefDenialOriginPrewarmPath = "prewarm-path"
	// RefDenialOriginBackgroundUnattributed — the DETECTOR cell: a ctx that
	// carries the background marker with no origin on it, i.e. a producer that
	// landed without declaring itself. Non-zero means the attribution has a
	// hole. It is backed by a STATIC census (dispatchers,
	// background_origin_census_563_test.go) because a cell that reads zero when
	// healthy cannot be the only guard.
	RefDenialOriginBackgroundUnattributed = "background-unattributed"
	// RefDenialOriginServe — no marker at all: a real customer /call. This is
	// the cell whose denials still WARN.
	RefDenialOriginServe = "serve"
)

type ctxKeyBackgroundOriginType struct{}

var ctxKeyBackgroundOrigin = ctxKeyBackgroundOriginType{}

// WithBackgroundResolveOrigin marks ctx as a background (non-customer) resolve
// AND names the driver. It is the form every production producer must use; the
// bare WithBackgroundResolve remains the marker's definition (and the form
// tests use when the driver is irrelevant).
func WithBackgroundResolveOrigin(ctx context.Context, origin string) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(WithBackgroundResolve(ctx), ctxKeyBackgroundOrigin, origin)
}

// BackgroundResolveOriginFromContext returns the background driver's name, and
// whether one was stamped. A background ctx with no origin returns ("", false)
// — the unattributed case.
func BackgroundResolveOriginFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	s, _ := ctx.Value(ctxKeyBackgroundOrigin).(string)
	if s == "" {
		return "", false
	}
	return s, true
}

// RefDenialOrigin classifies a resourceRef RBAC denial by WHO is resolving.
//
// ORDER IS LOAD-BEARING. The cohort seed carries BOTH markers (phase1_pip_seed.go
// stamps WithBackgroundResolve at :710 and WithPrewarmPath at :1606), so the
// origin must win over the prewarm-path fallback or every seed denial would hide
// inside the prewarm cell and the attribution gap would survive the fix.
//
// Everything that is not a customer returns a non-serve origin, which is the
// `PrewarmPathFromContext || BackgroundResolveFromContext` predicate #563 needs,
// expressed once: the prewarm arm is NOT dropped when the background arm is
// added (the content prewarm sets ONLY WithPrewarmPath, so dropping it would
// re-WARN exactly the population #214 silenced).
func RefDenialOrigin(ctx context.Context) string {
	if o, ok := BackgroundResolveOriginFromContext(ctx); ok {
		return o
	}
	if PrewarmPathFromContext(ctx) {
		return RefDenialOriginPrewarmPath
	}
	if BackgroundResolveFromContext(ctx) {
		return RefDenialOriginBackgroundUnattributed
	}
	return RefDenialOriginServe
}

// refDeniedByOrigin is the {origin} breakdown of resourceRef RBAC denials. The
// label set is CLOSED and tiny, every member is a compile-time constant, and a
// denial is not a hot path, so a plain map under a mutex is the right shape
// (same as confirmRetractedByReason, informer_watch_stats.go).
//
// Every cell is PRE-POPULATED at 0: an operator reading `serve: 0` must be able
// to tell "no serve denials" from "this instrument never ran", and a missing key
// cannot say that.
var (
	refDeniedMu       sync.Mutex
	refDeniedByOrigin = map[string]uint64{
		BackgroundOriginRefresher:             0,
		BackgroundOriginCohortSeed:            0,
		BackgroundOriginPrewarmEngineBoot:     0,
		RefDenialOriginPrewarmPath:            0,
		RefDenialOriginBackgroundUnattributed: 0,
		RefDenialOriginServe:                  0,
	}
)

// RecordRefDenied counts one resourceRef RBAC denial under its origin (take it
// from RefDenialOrigin — do not re-derive it, or the counter and the log level
// can disagree).
//
// An origin outside the closed set is folded into the unattributed cell rather
// than growing the map: the label set must stay bounded, and the census is what
// keeps an unknown origin from existing in the first place.
//
// It also keeps snowplow_prewarm_ref_denied_total moving for the prewarm cell:
// that key is #214's and predates this breakdown, so it keeps its exact meaning
// and any dashboard built on it.
func RecordRefDenied(origin string) {
	refDeniedMu.Lock()
	if _, known := refDeniedByOrigin[origin]; !known {
		origin = RefDenialOriginBackgroundUnattributed
	}
	refDeniedByOrigin[origin]++
	refDeniedMu.Unlock()
	if origin == RefDenialOriginPrewarmPath {
		prewarmRefDenied.Add(1)
	}
}

// RefDeniedByOriginSnapshot returns a copy of the {origin} breakdown.
func RefDeniedByOriginSnapshot() map[string]uint64 {
	refDeniedMu.Lock()
	defer refDeniedMu.Unlock()
	out := make(map[string]uint64, len(refDeniedByOrigin))
	for k, v := range refDeniedByOrigin {
		out[k] = v
	}
	return out
}

// expvar-only exposure, like #214's own counter and like
// snowplow_informer_confirm_retracted_by_reason: a labelled breakdown cannot
// ride the C7 tag system (a stat tag yields one scalar, and the tag system has
// no label facility), and a denial count whose non-zero is EXPECTED on three of
// its six cells is a diagnostic, not an alerting surface.
func init() {
	// CFG-1 mirror (same gate as every other cache-side expvar publisher): under
	// CACHE_ENABLED=false there is no prewarm, no seed and no refresher, so this
	// key MUST NOT appear at /debug/vars — otherwise the cfg1_structural_test.go
	// complement arm fails it as an ungated leak.
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_ref_denied_by_origin", expvar.Func(func() any {
		return RefDeniedByOriginSnapshot()
	}))
}
