// a1_uaf_seed_ctx_propagation_test.go — the widget seed's CTX-PROPAGATION arm:
// the sink the RESOLVE ran under must be the sink the PUT PATH reads.
//
// THE FAILURE MODE IT EXISTS FOR, unchanged since adv-cache-isolation: the
// resolve running under a DIFFERENT ctx than the one carrying the sink the Put
// path consults. Under that bug the sink is present, the helper that reads it is
// correct, the structural AST guard is green — and the UAF signal the refilter
// raised never reaches the code that acts on it, because the sink it was bumped
// on is not the sink that was read. Neither a predicate test over (inputs, sink)
// nor an AST walk can see it; only driving the real seedOneWidget can.
//
// #561 RE-POINTED WHAT "ACTS ON IT" MEANS, and the arm had to move with it.
//
// Before #561 the action was a DECLINE: bump the sink the resolve saw, watch the
// cell NOT be Put. widgets is identity-bound, so there is no decline to watch
// any more — and inverting this arm to "the cell is Put" would have DELETED the
// coverage rather than moved it, because a cell is Put whether or not the signal
// propagated.
//
// The action is now a STAMP. A widget whose resolve ran a userAccessFilter
// refilter holds per-requester narrowed rows, so its cell carries the short
// #118 (d) cap — and the ONLY way the Put path learns that is by reading the
// sink the resolve bumped (a widget CR declares no userAccessFilter of its own;
// the declaration sits several resolver frames below on the apiRef'd RA). So the
// discriminator is preserved exactly: bump the sink the RESOLVE saw, then look
// at the stored cell. Two distinct sinks leave a cell that is Put, carries
// HasUAF=false and inherits the FULL store TTL — the silent-uncapped-cell defect
// #561 found written into the widgets waiver — and the assertions below catch
// it. One sink leaves a cell stamped and capped.
//
// SAMENESS IS STILL PROVEN BEHAVIOURALLY, NOT BY COMPARING POINTERS. The Put
// path's sink is a local inside seedOneWidget and is not reachable from here. It
// does not need to be: bumping the sink the RESOLVE saw and observing the CAP
// land on the stored entry proves the two are the same object AND that the Put
// path reads it AND that reading it changes the entry it writes.
//
// The identity-FREE half of this property — where the signal still drives a
// DECLINE — is covered end-to-end by
// TestIssue450_DrainWidgetContentPut_DeclinesOnUAFAndExternal/uaf_refilter,
// which drives the pagination drain's resolve seam and asserts the shared
// widgetContent cell is not Put. Both halves of the class rule therefore keep a
// live ctx-propagation arm.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink drives the REAL
// seedOneWidget end-to-end with only the resolver seamed.
func TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	// The cap must be ENABLED, or uafTTLOverrideForEntry returns 0 whether or
	// not HasUAF was stamped and the arm cannot tell the two apart.
	const capSeconds = 300
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "300")
	if got := cache.UAFResolvedTTL(); got != capSeconds*time.Second {
		t.Fatalf("PRECONDITION: the UAF cap must be live for this arm to observe it; UAFResolvedTTL()=%v", got)
	}
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	entry := navWidgetEntry{
		W:          h1WidgetUnstructured(map[string]any{"apiRef": map[string]any{"name": a1RAName2, "namespace": h1NS}}),
		GVR:        h1WidgetGVR,
		Pagination: unpaginatedPrewarm,
	}

	// The cohort ctx must present the GROUP the shared CRB grants (a1Group),
	// exactly as a real cohort representative does — a username-only cohort
	// derives an empty first-match BindingUID and seedOneWidget short-circuits
	// at the #95 guard before ever reaching the resolve.
	seedCtx := withCohortSeedContext(context.Background(),
		seedTarget{Username: a1Alice, Groups: []string{a1Group}}, endpoints.Endpoint{}, nil)
	key, handle, inputs := dispatchCacheLookupKey(seedCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(seedCtx, entry.W.Object, nil))
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: the seed cohort must derive a live, non-empty-BindingUID widgets key — otherwise "+
			"seedOneWidget short-circuits for the #95 reason and this arm proves nothing; key=%q handle=%v inputs=%+v",
			key, handle != nil, inputs)
	}

	orig := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = orig })

	// sinkSeenByResolver is the sink the RESOLVE actually ran under. The Put
	// path below reads whatever seedOneWidget installed; if those are different
	// objects, the bump lands on one and the stamp is derived from the other.
	var sinkSeenByResolver *cache.UAFTouchedSink
	var resolverRan bool
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		resolverRan = true
		sinkSeenByResolver = cache.UAFTouchedSinkFromContext(ctx)
		// Stand in for the apiRef'd UAF RESTAction's refilter: mark THIS ctx.
		cache.BumpUAFTouched(ctx)
		out := h1WidgetUnstructured(map[string]any{})
		if err := unstructured.SetNestedField(out.Object, "representative-narrowing", "status", "widgetData", "rows"); err != nil {
			t.Fatalf("seam: %v", err)
		}
		return out, nil
	}

	if err := seedOneWidget(seedCtx, entry, h1NS, seedModeBoot); err != nil {
		t.Fatalf("seedOneWidget returned %v; want nil", err)
	}

	if !resolverRan {
		t.Fatal("PRECONDITION: seedOneWidget never reached the resolve — it short-circuited earlier, so nothing " +
			"about the sink or the stamp was exercised and this arm would pass vacuously")
	}
	if sinkSeenByResolver == nil {
		t.Fatal("R-1 CTX-PROPAGATION RED: seedOneWidget ran the widget resolve under a ctx carrying NO UAFTouchedSink. " +
			"The refilter's bump would land on a nil receiver, the Put path would read Count()==0, and every " +
			"refilter-narrowed widget would be seeded UNCAPPED under the full store TTL.")
	}
	if got := sinkSeenByResolver.Count(); got != 1 {
		t.Fatalf("the sink on the resolve ctx did not record the bump the resolver made (count=%d, want 1) — it is "+
			"not a live sink, so nothing downstream can read the mark", got)
	}

	// THE #561 BEHAVIOUR: the cell exists. Before #561 this seed wrote nothing,
	// which is why the composition data grids were cold on first paint.
	stored, ok := handle.Get(key)
	if !ok {
		t.Fatalf("#561: the widget seed must now Put a refilter-narrowed envelope under the per-binding key %q — "+
			"widgets is identity-bound and the key folds the requester's binding set, so the cell is the cohort's "+
			"own. An absent cell means the decline is still firing for an identity-bound class", key)
	}

	// THE ASSERTION. The resolve marked the sink; if the Put path read the SAME
	// object, the cell carries the UAF fact and the short cap. A second,
	// distinct sink leaves it stamped false and capped at nothing.
	if stored.Inputs == nil || !stored.Inputs.HasUAF {
		t.Fatalf("R-1 CTX-PROPAGATION RED: the seeded widget cell under %q does NOT carry HasUAF although its "+
			"resolve ran a refilter. The bump the resolve made did not reach the sink the Put path consults — they "+
			"are different objects, so the stamp is wired but INERT on this path and this cell will hold one "+
			"requester's narrowed rows for the FULL store TTL. Inputs=%+v", key, stored.Inputs)
	}
	if stored.TTLOverride != capSeconds*time.Second {
		t.Fatalf("the seeded UAF cell must carry the short cap: TTLOverride=%v want %v. HasUAF was stamped, so a "+
			"zero here means the Put site never passed the inputs through uafTTLOverrideForEntry — the stamp "+
			"reached nothing", stored.TTLOverride, capSeconds*time.Second)
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 0 {
		t.Fatalf("#561: the widgets class is identity-BOUND and must not be declined; counter=%d want 0", got)
	}

	// --- CONTROL: same drive, resolve marks NOTHING → seeded, and NOT capped.
	// Without this, an implementation that stamped HasUAF unconditionally would
	// satisfy every assertion above while telling us nothing about the sink. ---
	cache.ResetUAFPutDeclineCountersForTest()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	ctlKey, ctlHandle, ctlInputs := dispatchCacheLookupKey(seedCtx, "widgets",
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(seedCtx, entry.W.Object, nil))
	if ctlHandle == nil || ctlKey == "" || ctlInputs == nil {
		t.Fatal("control: the store reset left no live handle to re-derive the key through")
	}
	widgetsResolveFn = func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		if cache.UAFTouchedSinkFromContext(ctx) == nil {
			t.Fatal("control: the sink must still be installed on the resolve ctx even when nothing bumps it")
		}
		out := h1WidgetUnstructured(map[string]any{})
		if err := unstructured.SetNestedField(out.Object, "same-for-everyone", "status", "widgetData", "rows"); err != nil {
			t.Fatalf("control seam: %v", err)
		}
		return out, nil
	}
	if err := seedOneWidget(seedCtx, entry, h1NS, seedModeBoot); err != nil {
		t.Fatalf("control: seedOneWidget returned %v", err)
	}
	ctlStored, ok := ctlHandle.Get(ctlKey)
	if !ok {
		t.Fatalf("R-1 CONTROL BROKE: a widget seed whose resolve marked NO refilter was not Put under %q — the "+
			"widget boot seed is disabled wholesale, which would make the assertions above vacuous", ctlKey)
	}
	if ctlStored.Inputs != nil && ctlStored.Inputs.HasUAF {
		t.Fatal("R-1 CONTROL BROKE: a widget seed whose resolve ran NO refilter was stamped HasUAF — the stamp is " +
			"unconditional, so the main arm proves nothing about the sink")
	}
	if ctlStored.TTLOverride != 0 {
		t.Fatalf("control: a non-refiltered widget cell must take the standard TTL, not the UAF cap; TTLOverride=%v",
			ctlStored.TTLOverride)
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 0 {
		t.Fatalf("control: a non-refiltered widget seed must not tick the decline counter; got %d", got)
	}
}
