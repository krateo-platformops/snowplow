package cache

// put_gate_child_sinks_684_test.go — #684: WithChildPutGateSinks gives a nested
// resolve its OWN stage-error / per-identity forbidden / external-touched view,
// and forwards every bump to the enclosing sinks.

import (
	"context"
	"testing"
)

func TestWithChildPutGateSinks_OwnCountsAndForwarding(t *testing.T) {
	ctx, outerStage := WithStageErrorSink(context.Background())
	ctx, outerForb := WithPerIdentityForbiddenSink(ctx)
	ctx, outerExt := WithExternalTouchedSink(ctx)
	// Counts the enclosing resolve recorded BEFORE the nested one.
	outerStage.Bump("outer", "boom")
	outerExt.Bump()

	cctx, g := WithChildPutGateSinks(ctx)
	if StageErrorSinkFromContext(cctx) != g.Stage || PerIdentityForbiddenSinkFromContext(cctx) != g.Forbidden ||
		ExternalTouchedSinkFromContext(cctx) != g.External {
		t.Fatal("the child sinks are not the ones on the returned ctx")
	}
	if g.Stage.Count() != 0 || g.External.Count() != 0 {
		t.Fatal("a child sink must start at zero — it must not read the enclosing counts")
	}
	// A denial-only nested resolve: one F2 (stage + forbidden stage/external + external).
	StageErrorSinkFromContext(cctx).Bump("den", "forbidden")
	PerIdentityForbiddenSinkFromContext(cctx).BumpStage()
	PerIdentityForbiddenSinkFromContext(cctx).BumpExternal()
	ExternalTouchedSinkFromContext(cctx).Bump()

	if !DenialOnly(g.Stage, g.Forbidden, g.External, CacheEntryClassRAFullList) || GenuineExternal(g.External, g.Forbidden) != 0 {
		t.Fatal("the nested resolve's own view must read denial-only with no genuine external touch")
	}
	if outerStage.Count() != 2 || outerForb.Stage() != 1 || outerForb.External() != 1 || outerExt.Count() != 2 {
		t.Fatalf("bumps did not reach the enclosing sinks: stage=%d forb=%d/%d ext=%d",
			outerStage.Count(), outerForb.Stage(), outerForb.External(), outerExt.Count())
	}
	if st, _ := outerStage.Sample(); st != "outer" {
		t.Fatalf("the enclosing sink's first sample was overwritten: %q", st)
	}
	if st, _ := g.Stage.Sample(); st != "den" {
		t.Fatalf("the child sink's sample = %q, want den", st)
	}
	// The enclosing view still declines (its own unrelated error + external).
	if DenialOnly(outerStage, outerForb, outerExt, "widgets") {
		t.Fatal("the enclosing view must not read denial-only")
	}
}

func TestWithChildPutGateSinks_NoEnclosingSinks(t *testing.T) {
	cctx, g := WithChildPutGateSinks(context.Background())
	StageErrorSinkFromContext(cctx).Bump("s", "e")
	ExternalTouchedSinkFromContext(cctx).Bump()
	PerIdentityForbiddenSinkFromContext(cctx).BumpStage()
	if g.Stage.Count() != 1 || g.External.Count() != 1 || g.Forbidden.Stage() != 1 {
		t.Fatal("a child with no enclosing sink must still count")
	}
	// The plain constructors stay parent-less (byte-identical to pre-#684).
	_, s := WithStageErrorSink(cctx)
	_, e := WithExternalTouchedSink(cctx)
	_, f := WithPerIdentityForbiddenSink(cctx)
	if s.parent != nil || e.parent != nil || f.parent != nil {
		t.Fatal("WithStageErrorSink / WithExternalTouchedSink / WithPerIdentityForbiddenSink must not chain")
	}
}
