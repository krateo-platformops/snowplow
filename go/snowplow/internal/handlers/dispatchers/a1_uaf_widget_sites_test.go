// a1_uaf_widget_sites_test.go — R-1 per-site arms for the two remaining widget
// carriers: the BOOT SEED (seedOneWidget) and the identity-free WIDGETCONTENT
// populate (populateWidgetContentL1).
//
// The R-2 acceptance arm covers the customer dispatch. These two cover the paths
// that fill the same cells WITHOUT a customer request — and they matter more than
// the dispatch on a cold pod, because they warm a per-BINDING cell under a cohort
// REPRESENTATIVE identity (the seed) or an identity-FREE shared cell under the
// SA identity (the walker). Both are read by every co-bound user's first paint.
//
// Each arm pairs the refiltered case with a non-refiltered CONTROL that must
// still be cached, so a gate that simply disabled the widget seed or the content
// layer wholesale cannot pass.

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestR1_WidgetContentPopulate_DeclinesOnRefilter_ControlPopulates drives the
// REAL populateWidgetContentL1 with and without a refilter marked on ctx.
//
// This cell is the worst destination for a narrowed body in the whole system: it
// carries NO identity fold at all (widgetContentL1Key leaves Username/Groups
// zero), so it is shared by every user, and its serve-time gate re-derives only
// status.resourcesRefs.items[].allowed — it NEVER narrows status.widgetData.
//
// RED on origin/main: the envelope is Put and every user reads it.
func TestR1_WidgetContentPopulate_DeclinesOnRefilter_ControlPopulates(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	in := h1WidgetUnstructured(map[string]any{
		"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS},
	})
	res := h1WidgetUnstructured(map[string]any{})
	if err := unstructured.SetNestedField(res.Object, "narrowed-for-whoever-resolved", "status", "widgetData", "rows"); err != nil {
		t.Fatalf("setup: %v", err)
	}

	key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
	if key == "" {
		t.Fatalf("PRECONDITION: the widgetContent layer must be live (key derivation returned \"\") — otherwise this " +
			"arm passes vacuously because nothing would be Put either way")
	}
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatal("PRECONDITION: expected a live resolved cache")
	}

	// --- REFILTERED: a sink with a touch on ctx → decline. ------------------
	ctx, sink := cache.WithUAFTouchedSink(context.Background())
	sink.Bump()
	populateWidgetContentL1(ctx, h1WidgetGVR, in, -1, -1, res, 0)
	if entry, ok := c.Get(key); ok {
		t.Fatalf("R-1 RED (widgetContent carrier): a userAccessFilter-narrowed envelope was seeded into the "+
			"IDENTITY-FREE shared content cell at %q (%q). That cell has no identity fold whatsoever and its serve-time "+
			"gate never narrows status.widgetData, so EVERY user would be served the resolving identity's rows.",
			key, entry.RawJSON)
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 1 {
		t.Fatalf("the widgetContent decline must tick the widgets-class counter once; got %d", got)
	}

	// --- CONTROL: no refilter observed → the layer still works. -------------
	ctlCtx, _ := cache.WithUAFTouchedSink(context.Background())
	populateWidgetContentL1(ctlCtx, h1WidgetGVR, in, -1, -1, res, 0)
	if _, ok := c.Get(key); !ok {
		t.Fatalf("R-1 CONTROL BROKE: a widget whose resolve ran NO refilter was not seeded into the content cell at "+
			"%q — the gate is over-broad and has disabled the identity-free content layer wholesale, which would make "+
			"the assertion above vacuous", key)
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 1 {
		t.Fatalf("control: a non-refiltered populate must not tick the decline counter; total went to %d", got)
	}
}

// TestR1_WidgetSeedGate_ClassRuleHoldsAtTheSeedCarrier covers the seedOneWidget
// leg at the level the gate actually operates: the shared widget-family decision
// over (inputs, sink).
//
// WHY NOT AN END-TO-END seedOneWidget DRIVE for the predicate: the end-to-end
// behaviour of that leg is pinned by
// TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink, which drives the real
// function through the widgetsResolveFn seam. What is left to pin here is the
// RULE the seed's own inputs shape resolves to, which is a different question
// and the one #561 changed.
//
// #561 TURNED THIS FROM A ONE-SIDED ARM INTO A TWO-SIDED ONE, and the second
// side is why it was not simply inverted. The widget family spans BOTH sides of
// the class rule, and the seed carrier's inputs are the shape that makes the
// distinction concrete:
//
//	widgets        — identity-BOUND. The pre-#561 arm asserted the seed MUST
//	                 decline, on the grounds that a cohort REPRESENTATIVE's
//	                 narrowing would be read by every co-bound member. The
//	                 post-#423 key folds the requester's full binding set, and
//	                 the representative's binding set IS a group-only member's
//	                 (TestKey423_SeedRepresentativeParity), so the members who
//	                 share that cell are exactly the members who deserve its
//	                 rows. A member with any extra binding derives a different
//	                 key. The seed must NOT decline.
//
//	widgetContent  — identity-FREE. One cell, every requester, no identity in
//	                 the key at all. The seed must STILL decline, and that half
//	                 of the gate is what keeps the pre-#561 protection where it
//	                 is still needed.
//
// Driving both halves through ONE gate call with only the class differing is the
// point: it fails if the rule ever stops being per-class — in either direction.
func TestR1_WidgetSeedGate_ClassRuleHoldsAtTheSeedCarrier(t *testing.T) {
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	// The seed's inputs never carry HasUAF for a widget — the declaration lives
	// on the apiRef'd RESTAction, frames below. So the OBSERVED limb is the only
	// one that can fire here, which is exactly the R-1 shape. Everything else
	// about these inputs is held constant across the two arms so the CLASS is
	// the only thing that differs.
	seedInputs := func(class string) *cache.ResolvedKeyInputs {
		return &cache.ResolvedKeyInputs{
			CacheEntryClass:        class,
			Namespace:              h1NS,
			Name:                   h1WName,
			BindingUID:             "uid-shared-portal-binding",
			SubjectBindingSet:      "digest-of-the-representatives-bindings",
			RepresentativeUsername: a1Alice,
		}
	}
	marked := func(t *testing.T) *cache.UAFTouchedSink {
		t.Helper()
		_, sink := cache.WithUAFTouchedSink(context.Background())
		sink.Bump() // the apiRef chokepoint marked the widget's resolve
		return sink
	}

	t.Run("identity_free_widgetContent_still_declines", func(t *testing.T) {
		cache.ResetUAFPutDeclineCountersForTest()
		if !cache.IsIdentityFreeClass(cache.CacheEntryClassWidgetContent) {
			t.Fatal("PRECONDITION: widgetContent must be identity-free, or this arm is testing the wrong half " +
				"of the rule and would pass for the wrong reason")
		}
		if !declineWidgetUAFPut(seedInputs(cache.CacheEntryClassWidgetContent), marked(t)) {
			t.Fatal("R-1 RED (identity-free carrier): a refilter-narrowed envelope would be seeded into the " +
				"IDENTITY-FREE shared content cell, which folds NO identity whatsoever and whose serve-time gate " +
				"never narrows status.widgetData — EVERY user would be served the resolving identity's rows. " +
				"#561 scopes the decline by class; it does not remove it.")
		}
		if got := cache.WidgetsUAFPutDeclined(); got != 1 {
			t.Fatalf("the decline must tick the widget-family counter exactly once; got %d", got)
		}
	})

	t.Run("identity_bound_widgets_no_longer_declines", func(t *testing.T) {
		cache.ResetUAFPutDeclineCountersForTest()
		if cache.IsIdentityFreeClass("widgets") {
			t.Fatal("PRECONDITION: widgets must be identity-BOUND — if it has been moved into identityFreeClasses " +
				"then #561's whole argument (the key separates requesters) no longer holds for it and the decline " +
				"must come back, not this assertion")
		}
		if declineWidgetUAFPut(seedInputs("widgets"), marked(t)) {
			t.Fatal("#561 RED: the boot seed declined a refilter-narrowed widget cell. The widgets key folds the " +
				"requester's full binding set (#423), two members served different narrowings necessarily hold " +
				"different binding sets, and the cohort representative's binding set is a real group-only " +
				"member's — so this cell is the cohort's own. Declining it is what left every composition data " +
				"grid uncached, unsubscribable and cold (p95 1,814ms vs 166ms).")
		}
		if got := cache.WidgetsUAFPutDeclined(); got != 0 {
			t.Fatalf("a seed that was not declined must not tick the counter; got %d", got)
		}
	})

	t.Run("control_no_refilter_never_declines_in_either_class", func(t *testing.T) {
		// Without this, a gate hard-wired to "decline iff identity-free" would
		// pass both arms above while having stopped reading the sink at all.
		for _, class := range []string{"widgets", cache.CacheEntryClassWidgetContent} {
			cache.ResetUAFPutDeclineCountersForTest()
			_, cleanSink := cache.WithUAFTouchedSink(context.Background())
			if declineWidgetUAFPut(seedInputs(class), cleanSink) {
				t.Fatalf("CONTROL BROKE (%s): a widget seed whose resolve ran NO refilter must be Put — the gate "+
					"has stopped reading the sink and now declines on class alone, which would disable the seed "+
					"for the whole identity-free layer regardless of content", class)
			}
			if got := cache.WidgetsUAFPutDeclined(); got != 0 {
				t.Fatalf("control (%s): a non-refiltered seed must not tick the decline counter; got %d", class, got)
			}
		}
	})
}
