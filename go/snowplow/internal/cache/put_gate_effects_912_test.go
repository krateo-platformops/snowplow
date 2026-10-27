package cache

// put_gate_effects_912_test.go — #912 re-review N1: a memo hit is
// indistinguishable from the resolve it replaces, on EVERY Put-gate sink.
//
//   E1  a producer's bumps under WithPutGateEffectsCapture still reach the
//       enclosing sinks unchanged (the producer's own Put site is unaffected).
//   E2  replaying the captured effects onto a fresh set of sinks gives exactly
//       the counts, sample, declared-write mark and derived verdicts
//       (GenuineExternal, DenialOnly) that the same bumps give when made there
//       directly — stage errors, per-identity forbiddens, external touches, the
//       declared-write mark and UAF refilters.
//   E3  through a real SeedResolveMemo: Store carries the effects, a hit returns
//       them, and the zero value replays nothing.
//
// Mutations (logic-only): drop any field from Effects() or Replay() → E2 RED on
// that field; store a zero PutGateEffects in the memo → E3 RED.

import (
	"context"
	"testing"
)

type e912Sinks struct {
	stage *StageErrorSink
	forb  *PerIdentityForbiddenSink
	ext   *ExternalTouchedSink
	uaf   *UAFTouchedSink
}

func e912Fresh() (context.Context, e912Sinks) {
	ctx, st := WithStageErrorSink(context.Background())
	ctx, f := WithPerIdentityForbiddenSink(ctx)
	ctx, ex := WithExternalTouchedSink(ctx)
	ctx, u := WithUAFTouchedSink(ctx)
	return ctx, e912Sinks{st, f, ex, u}
}

// e912Produce is one producing resolve's bumps, made against ctx's sinks.
func e912Produce(ctx context.Context) {
	StageErrorSinkFromContext(ctx).Bump("render", "first failure")
	StageErrorSinkFromContext(ctx).Bump("other", "second failure")
	PerIdentityForbiddenSinkFromContext(ctx).BumpStage()
	PerIdentityForbiddenSinkFromContext(ctx).BumpExternal()
	ExternalTouchedSinkFromContext(ctx).Bump()
	ExternalTouchedSinkFromContext(ctx).BumpDeclaredWrite()
	BumpUAFTouched(ctx)
}

func e912Same(t *testing.T, label string, got, want e912Sinks) {
	t.Helper()
	gs, ge := got.stage.Sample()
	ws, we := want.stage.Sample()
	switch {
	case got.stage.Count() != want.stage.Count(), gs != ws, ge != we:
		t.Errorf("%s: stage errors %d (%q,%q), want %d (%q,%q)", label, got.stage.Count(), gs, ge, want.stage.Count(), ws, we)
	case got.forb.Stage() != want.forb.Stage(), got.forb.External() != want.forb.External():
		t.Errorf("%s: forbidden %d/%d, want %d/%d", label, got.forb.Stage(), got.forb.External(), want.forb.Stage(), want.forb.External())
	case got.ext.Count() != want.ext.Count(), got.ext.DeclaredWrite() != want.ext.DeclaredWrite():
		t.Errorf("%s: external %d declaredWrite=%v, want %d/%v", label, got.ext.Count(), got.ext.DeclaredWrite(), want.ext.Count(), want.ext.DeclaredWrite())
	case got.uaf.Count() != want.uaf.Count():
		t.Errorf("%s: uaf %d, want %d", label, got.uaf.Count(), want.uaf.Count())
	case GenuineExternal(got.ext, got.forb) != GenuineExternal(want.ext, want.forb):
		t.Errorf("%s: GenuineExternal differs", label)
	case DenialOnly(got.stage, got.forb, got.ext, "widgets") != DenialOnly(want.stage, want.forb, want.ext, "widgets"):
		t.Errorf("%s: DenialOnly differs", label)
	}
}

func TestIssue912_E1_E2_ReplayEqualsTheProducersBumps(t *testing.T) {
	// The reference: the producer's bumps made directly on a set of sinks.
	refCtx, ref := e912Fresh()
	e912Produce(refCtx)

	// E1: the same bumps under a capture still reach the enclosing sinks.
	prodCtx, prod := e912Fresh()
	capCtx, capture := WithPutGateEffectsCapture(prodCtx)
	e912Produce(capCtx)
	e912Same(t, "E1 producer's own sinks", prod, ref)

	// E2: the captured effects, replayed on a hitter's fresh sinks.
	hitCtx, hit := e912Fresh()
	capture.Effects().Replay(hitCtx)
	e912Same(t, "E2 replay", hit, ref)
	if !hit.ext.DeclaredWrite() || hit.uaf.Count() == 0 || hit.stage.Count() == 0 {
		t.Fatalf("E2: the replay lost an effect: %+v", capture.Effects())
	}

	// A replay reaches ancestor sinks too, like a direct bump from a child.
	parentCtx, parent := e912Fresh()
	childCtx, _ := WithPutGateEffectsCapture(parentCtx)
	capture.Effects().Replay(childCtx)
	e912Same(t, "E2 replay through a child", parent, ref)
}

func TestIssue912_E3_MemoCarriesTheEffects(t *testing.T) {
	m := NewSeedResolveMemo()
	prodCtx, _ := e912Fresh()
	capCtx, capture := WithPutGateEffectsCapture(prodCtx)
	e912Produce(capCtx)
	m.StoreLayeredWithStates("k", "", []byte(`{"x":1}`), nil, nil, DepGenEpochNow(), nil, capture.Effects())
	_, _, _, _, _, effects, ok := m.LoadLayeredWithStates("k", "")
	if !ok {
		t.Fatal("PRE: the memo entry was not served")
	}
	if effects != capture.Effects() {
		t.Fatalf("E3 RED: the memo served effects %+v, the producer recorded %+v", effects, capture.Effects())
	}
	hitCtx, hit := e912Fresh()
	PutGateEffects{}.Replay(hitCtx)
	if hit.stage.Count()+hit.forb.Stage()+hit.ext.Count()+hit.uaf.Count() != 0 || hit.ext.DeclaredWrite() {
		t.Fatal("E3: the zero PutGateEffects replayed an effect")
	}
}
