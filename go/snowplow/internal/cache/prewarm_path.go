// prewarm_path.go — #214: a dedicated "this resolve is a prewarm walk" context
// marker, plus the diagnostic counter that replaces the WARN spam it gates.
//
// WHY A DEDICATED MARKER (not WithPrewarmIterSerial). WithPrewarmIterSerial
// means "run inner-call fan-out serially (OOM mitigation)" and is prewarm-only
// only BY COINCIDENCE today; gating a correctness-relevant log decision on it
// is a latent trap (the day someone sets it on a serve path for memory reasons,
// the WARN-suppression would silently fire on serve and hide a real denial).
// WithPrewarmPath's SOLE meaning is "this resolve is a prewarm walk, not a
// served request." It is set at each prewarm ROOT and read by
// resourcesrefs.resolveOne to Debug-and-count (not WARN) an RBAC denial the
// walk is expected to hit.

package cache

import (
	"context"
	"expvar"
	"sync/atomic"
)

type ctxKeyPrewarmPathType struct{}

var ctxKeyPrewarmPath = ctxKeyPrewarmPathType{}

// WithPrewarmPath returns a child context marked as executing on a prewarm
// walk (#214). Set it at each prewarm ROOT that transitively resolves widget
// resourcesRefs (the per-user login-cohort seed is the confirmed denial
// source). nil ctx is returned unchanged (matches WithApistagePrewarm).
func WithPrewarmPath(ctx context.Context) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyPrewarmPath, true)
}

// PrewarmPathFromContext reports whether ctx is on a prewarm walk (#214).
func PrewarmPathFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v, _ := ctx.Value(ctxKeyPrewarmPath).(bool)
	return v
}

// prewarmRefDenied counts resourceRef RBAC denials observed on the PREWARM
// path (#214). DIAGNOSTIC, not a detector: a non-zero value is EXPECTED and
// normal — the per-user prewarm walk resolves a widget's resourcesRefs incl.
// write verbs (create/publish) the user may not hold, so every such row is a
// denial. Before #214 each one WARN-logged (546 lines/12h on 057); now the
// prewarm-path denial is Debug-logged and counted here, keeping the signal
// without the noise. Serve-path denials are NOT counted here — they stay WARN.
var prewarmRefDenied atomic.Uint64

// RecordPrewarmRefDenied bumps the prewarm resourceRef-denied counter. Called
// by resourcesrefs.resolveOne when a denial is observed on the prewarm path.
func RecordPrewarmRefDenied() { prewarmRefDenied.Add(1) }

// PrewarmRefDeniedTotal reads the counter. Pure read; creates nothing.
func PrewarmRefDeniedTotal() uint64 { return prewarmRefDenied.Load() }

// expvar-only exposure (/debug/vars). No OTLP bridge (#311): a counter whose
// non-zero is HEALTH does not need alerting — if anomaly alerting on prewarm
// denials is ever wanted, hand-wire OTLP then.
func init() {
	// CFG-1 mirror (same gate as every other cache-side expvar publisher,
	// e.g. lazy_register_skip_metrics.go): under CACHE_ENABLED=false there is
	// no prewarm walk, so this key MUST NOT appear at /debug/vars — otherwise
	// the cfg1_structural_test.go complement arm fails it as an ungated leak.
	// The derivation arm folds this file into the gated set from the
	// `if Disabled() { return }` shape below.
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_prewarm_ref_denied_total", expvar.Func(func() any {
		return prewarmRefDenied.Load()
	}))
}
