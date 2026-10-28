// put_gate_effects.go — #912 re-review N1: a resolve served WITHOUT running
// (a SeedResolveMemo hit) is indistinguishable from the resolve it replaces.
//
// THE DEFECT. Every L1 Put site decides from the sinks the resolve under it
// bumped: stage errors and per-identity forbiddens (#681 DenialOnly), external
// touches and the declared-write mark (#903 F1, #915), userAccessFilter
// refilters (#592). A memo hit returns the producer's body with NO dispatch, so
// none of those bumps reach the hitter's sinks: a sibling widget sharing a
// RESTAction that declares a write (or reads an external endpoint, or failed a
// stage) was Put although its producer was declined — and the customer's /call
// of that sibling was then an L1 hit that never sent the write.
//
// THE INVARIANT. The producer runs under CHILD sinks (WithPutGateEffectsCapture):
// every bump still reaches the producer's own sinks, and the child counts are
// exactly what the producing resolve recorded. The memo stores them with the
// body (PutGateEffects); a hit replays them onto the hitter's sinks (Replay), so
// every Put rule — F1, #915's external-TTL branch, the external decline, the
// stage-error and denial-only gates, the UAF decline — judges a memo-served body
// exactly as it judged the producer's. No rule needs to know a memo exists.
//
// NOT CARRIED: the sensitive-read sink. A body produced from a sensitive read is
// never stored in the memo at all (#398: it would keep the Secret in the memo),
// so there is nothing to replay.

package cache

import "context"

// PutGateEffects is what one resolve recorded on the Put-gate sinks. Immutable
// value; the zero value is "no effect".
type PutGateEffects struct {
	StageErrors       int64
	StageSampleStage  string
	StageSampleErr    string
	ForbiddenStage    int64
	ForbiddenExternal int64
	External          int64
	DeclaredWrite     bool
	UAF               int64
}

// PutGateCapture holds the child sinks a producing resolve runs under.
type PutGateCapture struct {
	stage     *StageErrorSink
	forbidden *PerIdentityForbiddenSink
	external  *ExternalTouchedSink
	uaf       *UAFTouchedSink
}

// WithPutGateEffectsCapture installs CHILD stage-error, per-identity forbidden,
// external-touched and UAF-touched sinks on ctx. Each forwards every bump to the
// sink already on ctx (if any), so the enclosing Put site observes exactly what
// it did before; the capture's own counts are only what resolves under the
// returned ctx recorded.
func WithPutGateEffectsCapture(ctx context.Context) (context.Context, *PutGateCapture) {
	c := &PutGateCapture{
		stage:     &StageErrorSink{parent: StageErrorSinkFromContext(ctx)},
		forbidden: &PerIdentityForbiddenSink{parent: PerIdentityForbiddenSinkFromContext(ctx)},
		external:  &ExternalTouchedSink{parent: ExternalTouchedSinkFromContext(ctx)},
		uaf:       &UAFTouchedSink{parent: UAFTouchedSinkFromContext(ctx)},
	}
	if ctx == nil {
		return ctx, c
	}
	ctx = context.WithValue(ctx, ctxKeyStageErrorSink, c.stage)
	ctx = context.WithValue(ctx, ctxKeyPerIdentityForbiddenSink, c.forbidden)
	ctx = context.WithValue(ctx, ctxKeyExternalTouchedSink, c.external)
	ctx = context.WithValue(ctx, ctxKeyUAFTouchedSink, c.uaf)
	return ctx, c
}

// Effects snapshots what the capture's resolves recorded. nil-receiver-safe
// (the zero value).
func (c *PutGateCapture) Effects() PutGateEffects {
	if c == nil {
		return PutGateEffects{}
	}
	stage, err := c.stage.Sample()
	return PutGateEffects{
		StageErrors:       c.stage.Count(),
		StageSampleStage:  stage,
		StageSampleErr:    err,
		ForbiddenStage:    c.forbidden.Stage(),
		ForbiddenExternal: c.forbidden.External(),
		External:          c.external.Count(),
		DeclaredWrite:     c.external.DeclaredWrite(),
		UAF:               c.uaf.Count(),
	}
}

// Replay records e on the sinks ctx carries (and their ancestors), as if the
// resolve that produced e had run under ctx. Sinks ctx does not carry are
// skipped, exactly as the producing resolve's bumps would have been.
func (e PutGateEffects) Replay(ctx context.Context) {
	if e == (PutGateEffects{}) {
		return
	}
	if e.StageErrors > 0 {
		smp := &stageErrSample{stage: e.StageSampleStage, err: e.StageSampleErr}
		for cur := StageErrorSinkFromContext(ctx); cur != nil; cur = cur.parent {
			cur.count.Add(e.StageErrors)
			cur.sample.CompareAndSwap(nil, smp)
		}
	}
	for cur := PerIdentityForbiddenSinkFromContext(ctx); cur != nil; cur = cur.parent {
		cur.stage.Add(e.ForbiddenStage)
		cur.external.Add(e.ForbiddenExternal)
	}
	for cur := ExternalTouchedSinkFromContext(ctx); cur != nil; cur = cur.parent {
		if e.DeclaredWrite {
			cur.declaredWrite.Store(true)
		}
		cur.count.Add(e.External)
	}
	for cur := UAFTouchedSinkFromContext(ctx); cur != nil; cur = cur.parent {
		cur.count.Add(e.UAF)
	}
}
