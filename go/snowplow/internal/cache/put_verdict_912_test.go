package cache

// put_verdict_912_test.go — V1: THE Put-gate verdict's closed set, per input
// (#912 re-review, shared-verdict form). A declared write is its own input and
// reason; it is never counted as an external touch. A missing sink is
// "unobserved" and admits nothing (#912 delta-3 item 3).

import (
	"context"
	"testing"
	"time"
)

// freshSinks is a complete set of Put-gate sinks; a non-nil argument replaces
// the fresh one.
func freshSinks(st *StageErrorSink, f *PerIdentityForbiddenSink, ext *ExternalTouchedSink) PutGateSinks {
	g := PutGateSinks{Stage: &StageErrorSink{}, Forbidden: &PerIdentityForbiddenSink{}, External: &ExternalTouchedSink{}}
	if st != nil {
		g.Stage = st
	}
	if f != nil {
		g.Forbidden = f
	}
	if ext != nil {
		g.External = ext
	}
	return g
}

func TestIssue912_V1_PutGateVerdictPerInput(t *testing.T) {
	type in struct {
		stage, forbStage, forbExt, ext int64
		write                          bool
		class                          string
	}
	sinks := func(c in) PutGateSinks {
		g := freshSinks(nil, nil, nil)
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
}

// V1b (#912 delta-3 item 3) — a store asserts that the resolve recorded nothing
// forbidding it, so it needs every sink. Empty sinks, or any one left out, are
// "unobserved": no store, no permanent decline — even when the sinks that are
// present recorded nothing.
//
// Mutation (logic-only, RUN): drop PutGateVerdict's missing-sink row → RED (the
// empty set stores).
func TestIssue912_V1b_MissingSinkIsUnobservedNeverStore(t *testing.T) {
	unobserved := PutVerdict{reason: PutReasonUnobserved}
	for name, g := range map[string]PutGateSinks{
		"PutGateSinks{}":    {},
		"no stage sink":     {Forbidden: &PerIdentityForbiddenSink{}, External: &ExternalTouchedSink{}},
		"no forbidden sink": {Stage: &StageErrorSink{}, External: &ExternalTouchedSink{}},
		"no external sink":  {Stage: &StageErrorSink{}, Forbidden: &PerIdentityForbiddenSink{}},
	} {
		v := PutGateVerdict(g, "restactions")
		if v != unobserved || v.Store() || v.Permanent() {
			t.Errorf("#912 delta-3 RED: %s gave %+v (store=%v permanent=%v), want the unobserved decline", name, v, v.Store(), v.Permanent())
		}
		if v.WithExternalTTL(true).Store() {
			t.Errorf("#912 delta-3 RED: the external-TTL opt-in turned an unobserved verdict into a store (%s)", name)
		}
	}
	if !PutGateVerdict(freshSinks(nil, nil, nil), "restactions").Store() {
		t.Fatal("control: three installed sinks that recorded nothing must store")
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
	external := PutGateVerdict(freshSinks(nil, nil, ext), "widgets")
	if v := external.WithExternalTTL(true); !v.Store() || !v.ExternalTTL() {
		t.Fatalf("an external-only decline with the opt-in must store with ExternalTTL: %+v", v)
	}
	if v := external.WithExternalTTL(false); v.Store() {
		t.Fatalf("without the opt-in an external touch declines: %+v", v)
	}
	w := &ExternalTouchedSink{}
	w.Bump()
	w.MarkDeclaredWrite()
	declared := PutGateVerdict(freshSinks(nil, nil, w), "widgets")
	if v := declared.WithExternalTTL(true); v.Store() {
		t.Fatalf("#915 RED: the external-TTL opt-in stored a resolve that declares a write: %+v", v)
	}
	stage := &StageErrorSink{}
	stage.Bump("s", "boom")
	stageExt := PutGateVerdict(freshSinks(stage, nil, ext), "widgets")
	if v := stageExt.WithExternalTTL(true); v.Store() {
		t.Fatalf("the external-TTL opt-in stored a resolve with a stage error: %+v", v)
	}
	unobserved := PutGateVerdict(PutGateSinks{}, "widgets")

	ctx := context.Background()
	full := map[string]any{"items": []any{}}
	for name, v := range map[string]PutVerdict{"zero": zero, "declared write": declared, "stage error": stageExt, "external": external, "unobserved": unobserved} {
		c := newResolvedCache(10, 1<<20, time.Hour)
		entry := func() *ResolvedEntry { return &ResolvedEntry{RawJSON: []byte(`{}`)} }
		// k6/k7 are live entries so a REPLACE has something it could replace.
		c.put("k6", entry())
		c.put("k7", entry())
		wrote := []bool{
			c.PutResolvedIfGen(ctx, v, "k1", entry(), c.CaptureGen("k1")),
			c.PutResolvedThenRemark(ctx, v, "k2", entry()),
			c.PutResolvedEndingKnownStale(ctx, v, "k3", entry()),
			c.PutRAFullListIfGen(ctx, v, "k4", ResolvedKeyInputs{}, full, c.CaptureGen("k4")),
			c.PutResolved(v, "k8", entry()),
			c.ReplaceResolvedIfGen(ctx, v, "k6", &ResolvedEntry{RawJSON: []byte(`{"new":1}`)}, c.CaptureGen("k6")),
			c.ReplaceResolvedIfGenRefresh(ctx, v, "k7", &ResolvedEntry{RawJSON: []byte(`{"new":1}`)}, c.CaptureGen("k7")),
		}
		_, pinned := c.PutRAFullList(v, "k5", ResolvedKeyInputs{}, full)
		for i, ok := range wrote {
			if ok {
				t.Errorf("#912 RED: gated write %d stored under a %s verdict", i, name)
			}
		}
		for _, k := range []string{"k1", "k2", "k3", "k4", "k5", "k8"} {
			if _, ok := c.GetNoTouch(k); ok {
				t.Errorf("#912 RED: %s is resident after a %s verdict (pinned=%v)", k, name, pinned)
			}
		}
		for _, k := range []string{"k6", "k7"} {
			if e, ok := c.GetNoTouch(k); !ok || string(e.RawJSON) != `{}` {
				t.Errorf("#912 RED: live %s was replaced under a %s verdict", k, name)
			}
		}
	}
	// Control: a storing verdict writes.
	c := newResolvedCache(10, 1<<20, time.Hour)
	if !c.PutResolvedIfGen(ctx, fixtureStoreVerdict, "ok", &ResolvedEntry{RawJSON: []byte(`{}`)}, c.CaptureGen("ok")) {
		t.Fatal("control: a storing verdict must write")
	}
}

// V3 (#912 delta-3 item 3) — PutContentIfGen, the one exported write without a
// verdict, writes a GET-only content cell and nothing else: an apistage-class
// entry under the key its own Inputs compute. A resolve output (another class),
// an entry without Inputs, or a key that is not its Inputs' key is refused.
//
// Mutations (logic-only, RUN): drop the class check → RED on the widgets row;
// drop the key check → RED on the foreign-key row.
func TestIssue912_V3_PutContentIfGenWritesOnlyAContentCell(t *testing.T) {
	ctx := context.Background()
	content := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassApistage, Version: "v1", Resource: "configmaps", Namespace: "ns"}
	contentKey := ComputeKey(content)
	widget := ResolvedKeyInputs{CacheEntryClass: "widgets", Version: "v1", Resource: "configmaps", Namespace: "ns"}
	widgetKey := ComputeKey(widget)
	ptr := func(in ResolvedKeyInputs) *ResolvedKeyInputs { return &in }

	refused := []struct {
		name  string
		key   string
		entry *ResolvedEntry
	}{
		{"a widgets-class resolve output", widgetKey, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: ptr(widget)}},
		{"no Inputs", contentKey, &ResolvedEntry{RawJSON: []byte(`{}`)}},
		{"a content entry under a foreign key", widgetKey, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: ptr(content)}},
		{"nil entry", contentKey, nil},
	}
	for _, r := range refused {
		c := newResolvedCache(10, 1<<20, time.Hour)
		if c.PutContentIfGen(ctx, r.key, r.entry, c.CaptureGen(r.key)) {
			t.Errorf("#912 delta-3 RED: PutContentIfGen wrote %s", r.name)
		}
		if _, ok := c.GetNoTouch(r.key); ok {
			t.Errorf("#912 delta-3 RED: %s is resident after PutContentIfGen", r.name)
		}
	}
	c := newResolvedCache(10, 1<<20, time.Hour)
	if !c.PutContentIfGen(ctx, contentKey, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: ptr(content)}, c.CaptureGen(contentKey)) {
		t.Fatal("control: an apistage entry under its own key must be written")
	}
}
