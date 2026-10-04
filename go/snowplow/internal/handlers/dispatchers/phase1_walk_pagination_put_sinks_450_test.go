// phase1_walk_pagination_put_sinks_450_test.go — #450: the pagination-drain
// widgetContent Put must decline on a userAccessFilter refilter and on an
// external-endpoint touch, exactly like the walker's page-1 Put.
//
// THE GAP. iterateApiRefPages (phase1_walk_pagination.go) resolves each deep
// page and Puts it into the IDENTITY-FREE widgetContent cell via
// populateWidgetContentL1. That function gates the Put on three sinks read
// from ctx: StageErrorSink, ExternalTouchedSink, UAFTouchedSink. The page-1
// walk (phase1_walk.go) installs all three on its resolve ctx; the drain
// installed only the StageErrorSink. With no UAF / external sink on ctx the
// resolver's bumps land on a nil receiver, the gates read Count()==0, and the
// Put proceeds.
//
// WHY THIS IS NOT A LIVE LEAK TODAY, AND HOW THE ARM UNMASKS IT. Every widget
// the drain reaches is isApiRefTemplateDriven (apiRef + resourcesRefsTemplate),
// and isRBACSensitiveApiRefWidget (the routing heuristic consulted FIRST inside
// populateWidgetContentL1) returns true for that same shape, so the Put is
// declined before any sink gate is read. The 1.12.3 A-1/R-1 sink gates exist
// precisely so the shared cell does NOT rest on that heuristic: it is
// declaration-shaped and it DE-CLASSIFIES ON ACCESSOR ERROR
// (`rrt, _ := widgets.GetResourcesRefsTemplate(obj)` discards the error and
// reads len()==0). The arm drives that documented de-classification: the
// resolved page envelope carries a spec.apiRef and a spec.resourcesRefsTemplate
// whose item is not an object, so GetResourcesRefsTemplate errors, the
// heuristic says "not sensitive", and the sink gates become the ONLY thing
// between the drained page and the shared cell. The CONTROL sub-arm (same
// envelope, nothing bumped) proves the bypass is effective: the Put lands, so
// the decline in the main arms is the sink gate's doing and not the heuristic's.
//
// THE RESOLVE SEAM. paginationResolvePageFn stands in for widgets.Resolve and
// marks the sink found on the ctx it was handed — the same thing the apiref
// chokepoint (cache.BumpUAFTouched) and the api resolver's external-endpoint
// branch (ExternalTouchedSinkFromContext(gctx).Bump()) do in production; those
// bump sites are pinned by their own arms (apiref/a1_uaf_sink_bump_test.go,
// restactions/api/resolve_inprocess_falsifier_test.go). What this arm pins is
// the WIRING at the drain: the sink the resolve sees is the sink the Put gate
// reads. Precedent: TestR1_SeedOneWidget_ResolveCtxCarriesTheGatesSink.
//
// RED on origin/main (c7533929): both refiltered and external sub-arms find the
// page cell Put. GREEN once the drain installs the shared widgetContent sink set.

package dispatchers

import (
	"context"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// heuristicDeclassifiedPageEnvelope is a drained page envelope that IS the
// apiRef+template shape in intent, but whose resourcesRefsTemplate item fails
// to decode — so isRBACSensitiveApiRefWidget de-classifies it (accessor error →
// len 0) and populateWidgetContentL1 falls through to its sink gates.
func heuristicDeclassifiedPageEnvelope(ns, name string, page int) *unstructured.Unstructured {
	e := nonRBACSensitivePageEnvelope(ns, name, page, false)
	spec := e.Object["spec"].(map[string]any)
	spec["resourcesRefsTemplate"] = []any{"not-an-object"}
	return e
}

func TestIssue450_DrainWidgetContentPut_DeclinesOnUAFAndExternal(t *testing.T) {
	const ns, name = "krateo-system", "compositions-page-datagrid"

	// Precondition for the whole arm: the envelope really does slip past the
	// heuristic. If a future change makes the heuristic fail closed on accessor
	// error, this arm would go vacuous — say so loudly instead.
	if isRBACSensitiveApiRefWidget(heuristicDeclassifiedPageEnvelope(ns, name, 2).Object) {
		t.Fatal("PRECONDITION: isRBACSensitiveApiRefWidget now classifies the accessor-error envelope as sensitive — " +
			"the heuristic no longer masks the drain's sink gates, so this arm cannot observe them; rework the bypass")
	}

	cases := []struct {
		name string
		// mark stands in for the resolver's production bump on the resolve
		// ctx. Returns whether a live sink was present to receive it.
		mark      func(ctx context.Context) bool
		wantPut   bool
		counterFn func() uint64
	}{
		{
			name: "uaf_refilter",
			mark: func(ctx context.Context) bool {
				present := cache.UAFTouchedSinkFromContext(ctx) != nil
				cache.BumpUAFTouched(ctx)
				return present
			},
			counterFn: func() uint64 { return uint64(cache.WidgetsUAFPutDeclined()) },
		},
		{
			name: "external_endpoint",
			mark: func(ctx context.Context) bool {
				s := cache.ExternalTouchedSinkFromContext(ctx)
				s.Bump() // nil-receiver-safe, as in production
				return s != nil
			},
			counterFn: cache.ExternalSkippedPut,
		},
		{
			name:    "control_nothing_marked",
			mark:    func(context.Context) bool { return true },
			wantPut: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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

			paginationTestMu.Lock()
			defer paginationTestMu.Unlock()
			drainAllCustomerInFlight()

			oldCap := phase1MaxApiRefPagesForTest
			phase1MaxApiRefPagesForTest = 2 // drain page 2 only
			t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

			prevFetch, prevResolve := paginationFetchPageFn, paginationResolvePageFn
			t.Cleanup(func() {
				paginationFetchPageFn = prevFetch
				paginationResolvePageFn = prevResolve
			})
			paginationFetchPageFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
				return fetchOKResult(ns, name)
			}
			var resolved, sinkPresent bool
			paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
				resolved = true
				sinkPresent = tc.mark(ctx)
				return heuristicDeclassifiedPageEnvelope(ns, name, 2), nil
			}

			var before uint64
			if tc.counterFn != nil {
				before = tc.counterFn()
			}

			gvr := drainPageCellGVR()
			iterateApiRefPages(
				context.Background(),
				newPhase1Walker(nil, "krateo-system"),
				newUnstructuredWidget(ns, name),
				gvr,
				fakePage1Driven(), // apiRef+template, wants continue → loop entry
				1,                 // depth
				5,                 // perPage (resolution)
				-1,                // keyPerPage (root tuple)
				-1,                // keyPage (root tuple)
				"krateo-system",
			)

			if !resolved {
				t.Fatal("PRECONDITION: the drain never reached the page resolve — nothing was exercised")
			}
			key, _ := widgetContentL1Key(gvr, ns, name, -1, drainKeyPageFor(-1, 2))
			if key == "" {
				t.Fatal("PRECONDITION: widgetContent layer must be live (empty key)")
			}
			c := cache.ResolvedCache()
			if c == nil {
				t.Fatal("PRECONDITION: expected a live resolved cache")
			}
			entry, hit := c.Get(key)

			if tc.wantPut {
				if !hit {
					t.Fatalf("CONTROL BROKE: an unmarked drained page was not Put at %q — either the heuristic "+
						"bypass stopped working or the drain's Put is disabled wholesale; the decline arms would "+
						"then pass vacuously", key)
				}
				return
			}
			if hit {
				t.Fatalf("#450 RED (%s): the pagination drain Put a page envelope into the IDENTITY-FREE shared "+
					"widgetContent cell %q (%d bytes) although its resolve was marked. sinkOnResolveCtx=%v — the "+
					"drain's resolve ctx lacks the sink populateWidgetContentL1 gates on, so the gate is inert on "+
					"this path and the cell's isolation rests on isRBACSensitiveApiRefWidget alone",
					tc.name, key, len(entry.RawJSON), sinkPresent)
			}
			if !sinkPresent {
				t.Fatalf("%s: the Put was declined but the resolve ctx carried no sink — declined for the wrong reason", tc.name)
			}
			if got := tc.counterFn() - before; got != 1 {
				t.Fatalf("%s: the decline must route through its gate exactly once; counter delta=%d want 1", tc.name, got)
			}
		})
	}
}

// TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink pins the helper's
// set against populateWidgetContentL1's gates. Both widgetContent Put paths
// (phase1Walker.walk, iterateApiRefPages) get their sinks ONLY from this helper,
// and the per-gate populate arms (e.g. TestR1_WidgetContentPopulate_*) install
// their own sink and call populateWidgetContentL1 directly, so they cannot see
// a sink dropped from the helper. Adding a gate to populateWidgetContentL1 means
// adding its sink to the helper AND a line here.
func TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink(t *testing.T) {
	ctx := withWidgetContentPutSinks(context.Background())
	if cache.StageErrorSinkFromContext(ctx) == nil {
		t.Error("withWidgetContentPutSinks must install a StageErrorSink (#313 Cache-A gate)")
	}
	if cache.ExternalTouchedSinkFromContext(ctx) == nil {
		t.Error("withWidgetContentPutSinks must install an ExternalTouchedSink (external-no-cache gate)")
	}
	if cache.UAFTouchedSinkFromContext(ctx) == nil {
		t.Error("withWidgetContentPutSinks must install a UAFTouchedSink (1.12.3 A-1/R-1 gate)")
	}
}
