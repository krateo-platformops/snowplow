package cache

// put_verdict_912_test.go — V1: THE Put-gate verdict's closed set, per input
// (#912 re-review, shared-verdict form). A declared write is its own input and
// reason; it is never counted as an external touch.

import (
	"context"
	"testing"
	"time"
)

func TestIssue912_V1_PutGateVerdictPerInput(t *testing.T) {
	type in struct {
		stage, forbStage, forbExt, ext int64
		write                          bool
		class                          string
	}
	sinks := func(c in) PutGateSinks {
		g := PutGateSinks{Stage: &StageErrorSink{}, Forbidden: &PerIdentityForbiddenSink{}, External: &ExternalTouchedSink{}}
		g.Stage.count.Store(c.stage)
		g.Forbidden.stage.Store(c.forbStage)
		g.Forbidden.external.Store(c.forbExt)
		g.External.count.Store(c.ext)
		if c.write {
			g.External.MarkDeclaredWrite()
		}
		return g
	}
	cases := []struct {
		name string
		in   in
		want PutVerdict
	}{
		{"nothing recorded", in{class: "restactions"}, PutVerdict{decision: PutStore}},
		{"declared write", in{write: true, class: "restactions"}, PutVerdict{decision: PutDeclinePermanent, reason: PutReasonDeclaredWrite}},
		{"declared write beats everything", in{write: true, stage: 2, ext: 3, class: "restactions"}, PutVerdict{decision: PutDeclinePermanent, reason: PutReasonDeclaredWrite}},
		{"stage error", in{stage: 1, class: "restactions"}, PutVerdict{decision: PutDeclineTransient, reason: PutReasonStageError}},
		{"stage error beside an external touch", in{stage: 1, ext: 1, class: "restactions"}, PutVerdict{decision: PutDeclinePermanent, reason: PutReasonStageError}},
		{"external touch", in{ext: 1, class: "restactions"}, PutVerdict{decision: PutDeclinePermanent, reason: PutReasonExternal}},
		{"an own-credential 403 is not external", in{stage: 1, forbStage: 1, forbExt: 1, ext: 1, class: "restactions"}, PutVerdict{decision: PutStore, denialOnly: true}},
		{"denial-only, identity-bound", in{stage: 2, forbStage: 2, class: "widgets"}, PutVerdict{decision: PutStore, denialOnly: true}},
		{"denial-only refused for an identity-free class", in{stage: 1, forbStage: 1, class: CacheEntryClassWidgetContent}, PutVerdict{decision: PutDeclineTransient, reason: PutReasonStageError}},
	}
	for _, c := range cases {
		if got := PutGateVerdict(sinks(c.in), c.in.class); got != c.want {
			t.Errorf("%s: verdict %+v, want %+v", c.name, got, c.want)
		}
	}
	// A declared write is not an external touch.
	g := sinks(in{write: true})
	if g.External.Count() != 0 || GenuineExternal(g.External, g.Forbidden) != 0 {
		t.Fatalf("#912 RED: MarkDeclaredWrite counted an external touch (%d)", g.External.Count())
	}
	// nil sinks read as empty.
	if v := PutGateVerdict(PutGateSinks{}, "restactions"); v != (PutVerdict{decision: PutStore}) {
		t.Fatalf("nil sinks: %+v", v)
	}
}

// V2 — the TYPE enforces the verdict: the zero value admits nothing, the
// external-TTL opt-in only turns an external-ONLY decline into a store, and
// every verdict-gated write refuses a declining verdict (and the zero value).
func TestIssue912_V2_GatedWritesRefuseADecliningVerdict(t *testing.T) {
	var zero PutVerdict
	if zero.Store() || zero.Permanent() {
		t.Fatalf("the zero PutVerdict must admit nothing and be no decline: %+v", zero)
	}
	ext := &ExternalTouchedSink{}
	ext.Bump()
	external := PutGateVerdict(PutGateSinks{External: ext}, "widgets")
	if v := external.WithExternalTTL(true); !v.Store() || !v.ExternalTTL() {
		t.Fatalf("an external-only decline with the opt-in must store with ExternalTTL: %+v", v)
	}
	if v := external.WithExternalTTL(false); v.Store() {
		t.Fatalf("without the opt-in an external touch declines: %+v", v)
	}
	w := &ExternalTouchedSink{}
	w.Bump()
	w.MarkDeclaredWrite()
	declared := PutGateVerdict(PutGateSinks{External: w}, "widgets")
	if v := declared.WithExternalTTL(true); v.Store() {
		t.Fatalf("#915 RED: the external-TTL opt-in stored a resolve that declares a write: %+v", v)
	}
	stage := &StageErrorSink{}
	stage.Bump("s", "boom")
	stageExt := PutGateVerdict(PutGateSinks{Stage: stage, External: ext}, "widgets")
	if v := stageExt.WithExternalTTL(true); v.Store() {
		t.Fatalf("the external-TTL opt-in stored a resolve with a stage error: %+v", v)
	}

	ctx := context.Background()
	full := map[string]any{"items": []any{}}
	for name, v := range map[string]PutVerdict{"zero": zero, "declared write": declared, "stage error": stageExt, "external": external} {
		c := newResolvedCache(10, 1<<20, time.Hour)
		entry := func() *ResolvedEntry { return &ResolvedEntry{RawJSON: []byte(`{}`)} }
		wrote := []bool{
			c.PutResolvedIfGen(ctx, v, "k1", entry(), c.CaptureGen("k1")),
			c.PutResolvedThenRemark(ctx, v, "k2", entry()),
			c.PutResolvedEndingKnownStale(ctx, v, "k3", entry()),
			c.PutRAFullListIfGen(ctx, v, "k4", ResolvedKeyInputs{}, full, c.CaptureGen("k4")),
		}
		_, pinned := c.PutRAFullList(v, "k5", ResolvedKeyInputs{}, full)
		for i, ok := range wrote {
			if ok {
				t.Errorf("#912 RED: gated write %d stored under a %s verdict", i, name)
			}
		}
		for _, k := range []string{"k1", "k2", "k3", "k4", "k5"} {
			if _, ok := c.GetNoTouch(k); ok {
				t.Errorf("#912 RED: %s is resident after a %s verdict (pinned=%v)", k, name, pinned)
			}
		}
	}
	// Control: a storing verdict writes.
	c := newResolvedCache(10, 1<<20, time.Hour)
	if !c.PutResolvedIfGen(ctx, PutGateVerdict(PutGateSinks{}, "widgets"), "ok", &ResolvedEntry{RawJSON: []byte(`{}`)}, c.CaptureGen("ok")) {
		t.Fatal("control: a storing verdict must write")
	}
}
