package cache

// put_verdict_912_test.go — V1: THE Put-gate verdict's closed set, per input
// (#912 re-review, shared-verdict form). A declared write is its own input and
// reason; it is never counted as an external touch.

import "testing"

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
		{"nothing recorded", in{class: "restactions"}, PutVerdict{Decision: PutStore}},
		{"declared write", in{write: true, class: "restactions"}, PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonDeclaredWrite}},
		{"declared write beats everything", in{write: true, stage: 2, ext: 3, class: "restactions"}, PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonDeclaredWrite}},
		{"stage error", in{stage: 1, class: "restactions"}, PutVerdict{Decision: PutDeclineTransient, Reason: PutReasonStageError}},
		{"stage error beside an external touch", in{stage: 1, ext: 1, class: "restactions"}, PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonStageError}},
		{"external touch", in{ext: 1, class: "restactions"}, PutVerdict{Decision: PutDeclinePermanent, Reason: PutReasonExternal}},
		{"an own-credential 403 is not external", in{stage: 1, forbStage: 1, forbExt: 1, ext: 1, class: "restactions"}, PutVerdict{Decision: PutStore, DenialOnly: true}},
		{"denial-only, identity-bound", in{stage: 2, forbStage: 2, class: "widgets"}, PutVerdict{Decision: PutStore, DenialOnly: true}},
		{"denial-only refused for an identity-free class", in{stage: 1, forbStage: 1, class: CacheEntryClassWidgetContent}, PutVerdict{Decision: PutDeclineTransient, Reason: PutReasonStageError}},
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
	if v := PutGateVerdict(PutGateSinks{}, "restactions"); v != (PutVerdict{Decision: PutStore}) {
		t.Fatalf("nil sinks: %+v", v)
	}
}
