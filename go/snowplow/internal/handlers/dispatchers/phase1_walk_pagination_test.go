// phase1_walk_pagination_test.go — Path 3.2.2 unit tests for the
// walker apiRef pagination predicates and bounds.
//
// SCOPE: This test file exercises the SHAPE PREDICATES and the
// SAFETY BOUND constant — the parts of phase1_walk_pagination.go that
// are testable without a live apiserver / mocked widget resolver.
// End-to-end pagination (resolver→populate→recurse) is exercised by
// the integration falsifier (Phase 3 bench probe on the live cluster).

package dispatchers

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIsApiRefTemplateDriven_PositiveShape: widget with non-empty
// spec.apiRef.name AND non-empty spec.resourcesRefsTemplate triggers
// pagination eligibility. Mirrors the compositions-page-datagrid shape.
func TestIsApiRefTemplateDriven_PositiveShape(t *testing.T) {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Datagrid",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "compositions-page-datagrid"},
		"spec": map[string]any{
			"apiRef": map[string]any{
				"name":      "compositions-list",
				"namespace": "krateo-system",
			},
			"resourcesRefsTemplate": []any{
				map[string]any{
					"id":   "${.name}",
					"path": "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1&namespace=${.namespace}&name=${.name}-composition-panel",
					"verb": "GET",
				},
			},
		},
	}}
	if !isApiRefTemplateDriven(w.Object) {
		t.Fatalf("isApiRefTemplateDriven should fire on apiRef+template widget; obj=%v", w.Object)
	}
}

// TestIsApiRefTemplateDriven_NegativeNoApiRef: widget WITHOUT apiRef
// MUST NOT trigger pagination (no external data source to page over).
func TestIsApiRefTemplateDriven_NegativeNoApiRef(t *testing.T) {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Markdown",
		"metadata":   map[string]any{"namespace": "ns", "name": "static-md"},
		"spec": map[string]any{
			// Static markdown — no apiRef
			"content": "hello",
		},
	}}
	if isApiRefTemplateDriven(w.Object) {
		t.Fatalf("isApiRefTemplateDriven must be false when apiRef absent; obj=%v", w.Object)
	}
}

// TestIsApiRefTemplateDriven_NegativeNoTemplate: widget with apiRef but
// NO resourcesRefsTemplate — the items list comes from a static
// spec.resourcesRefs (not the apiRef). Paginating apiRef pages would
// not produce new items; pagination is correctly disabled.
func TestIsApiRefTemplateDriven_NegativeNoTemplate(t *testing.T) {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Piechart",
		"metadata":   map[string]any{"namespace": "ns", "name": "agg-pie"},
		"spec": map[string]any{
			"apiRef": map[string]any{
				"name":      "compositions-list",
				"namespace": "krateo-system",
			},
			// No resourcesRefsTemplate — piechart renders entirely
			// from status.widgetData. Pagination would not help.
		},
	}}
	if isApiRefTemplateDriven(w.Object) {
		t.Fatalf("isApiRefTemplateDriven must be false when resourcesRefsTemplate absent; obj=%v", w.Object)
	}
}

// TestIsApiRefTemplateDriven_NegativeEmptyApiRefName: a spec.apiRef
// block whose `.name` is empty is treated as absent (same conservative
// direction widget_content.go uses).
func TestIsApiRefTemplateDriven_NegativeEmptyApiRefName(t *testing.T) {
	w := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"apiRef":                map[string]any{"name": "", "namespace": "ns"},
			"resourcesRefsTemplate": []any{map[string]any{"id": "x"}},
		},
	}}
	if isApiRefTemplateDriven(w.Object) {
		t.Fatalf("isApiRefTemplateDriven must be false for empty apiRef.name")
	}
}

// TestIsApiRefTemplateDriven_NilObject: defensive — nil map returns
// false (the walker may hand a nil-Object widget when ParseCallPath fails).
func TestIsApiRefTemplateDriven_NilObject(t *testing.T) {
	if isApiRefTemplateDriven(nil) {
		t.Fatalf("isApiRefTemplateDriven must be false for nil object")
	}
}

// TestResolverWantsContinue_True: resolved envelope with
// status.resourcesRefs.slice.continue == true signals more pages
// available — the resolver's promise that another page would yield
// new items.
func TestResolverWantsContinue_True(t *testing.T) {
	res := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{map[string]any{"id": "a"}},
				"slice": map[string]any{
					"perPage":  5,
					"page":     2,
					"continue": true,
				},
			},
		},
	}}
	if !resolverWantsContinue(res) {
		t.Fatalf("resolverWantsContinue should return true on .slice.continue==true")
	}
}

// TestResolverWantsContinue_False: slice.continue==false signals end
// of pagination.
func TestResolverWantsContinue_False(t *testing.T) {
	res := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{},
				"slice": map[string]any{
					"perPage":  5,
					"page":     3,
					"continue": false,
				},
			},
		},
	}}
	if resolverWantsContinue(res) {
		t.Fatalf("resolverWantsContinue should return false on .slice.continue==false")
	}
}

// TestResolverWantsContinue_NoSlice: resolved envelope without a
// status.resourcesRefs.slice block (the resolver did NOT mark
// continuation — e.g. perPage was unbounded). Conservative direction:
// no continuation.
func TestResolverWantsContinue_NoSlice(t *testing.T) {
	res := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{map[string]any{"id": "a"}},
			},
		},
	}}
	if resolverWantsContinue(res) {
		t.Fatalf("resolverWantsContinue must be false when .slice absent")
	}
}

// TestResolverWantsContinue_NilRes: defensive nil-input case.
func TestResolverWantsContinue_NilRes(t *testing.T) {
	if resolverWantsContinue(nil) {
		t.Fatalf("resolverWantsContinue must be false for nil envelope")
	}
}

// TestMaxApiRefPages_DefaultUsedAbsentOverride: the production default
// constant must be returned when no test override is set. Pins the
// safety cap so a code edit to lower it surfaces immediately.
func TestMaxApiRefPages_DefaultUsedAbsentOverride(t *testing.T) {
	old := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 0
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = old })

	if got := maxApiRefPages(); got != phase1MaxApiRefPages {
		t.Fatalf("maxApiRefPages() = %d, want default %d", got, phase1MaxApiRefPages)
	}
}

// TestMaxApiRefPages_TestOverrideHonoured: a non-zero test override
// supersedes the default constant. Used by future end-to-end tests to
// bound the apiRef iteration to a small number.
func TestMaxApiRefPages_TestOverrideHonoured(t *testing.T) {
	old := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 3
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = old })

	if got := maxApiRefPages(); got != 3 {
		t.Fatalf("maxApiRefPages() with override=3 = %d, want 3", got)
	}
}

// TestPhase1MaxApiRefPages_BoundedSane: the backstop must be a
// reasonable bound — not 0 (would disable pagination), not absurdly
// large (would never cap a runaway apiRef). #156 / 0.30.256: the constant
// is now a LIVENESS BACKSTOP (raised to 20,000 = 100K items at perPage=5,
// ~2x the 50K production population) rather than a 5%-sample cap; the
// operative terminator is the blueprint's .slice.continue. The bounds here
// keep it an honest anti-runaway ceiling.
func TestPhase1MaxApiRefPages_BoundedSane(t *testing.T) {
	if phase1MaxApiRefPages <= 0 {
		t.Fatalf("phase1MaxApiRefPages must be > 0; got %d", phase1MaxApiRefPages)
	}
	if phase1MaxApiRefPages > 100_000 {
		t.Fatalf("phase1MaxApiRefPages absurdly large (%d) — unbounded apiRef would loop forever",
			phase1MaxApiRefPages)
	}
	// #156: the backstop must clear the 50K production worst case
	// (ceil(50,000/5)=10,000 pages) with margin so a well-behaved RA exits on
	// .slice.continue, never on the backstop. Pin the derived floor so a
	// future lowering of the constant below the production population trips
	// here (it would re-introduce the 0.30.219 truncation defect).
	const prod50kWorstCasePages = 10_000 // ceil(50,000 compositions / perPage=5)
	if phase1MaxApiRefPages < prod50kWorstCasePages {
		t.Fatalf("phase1MaxApiRefPages=%d is below the 50K production worst case (%d pages) — "+
			"the backstop would TRUNCATE the real population (#156 regression)",
			phase1MaxApiRefPages, prod50kWorstCasePages)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// #156 (P9-A) / 0.30.256 — iterateApiRefPages continuation / backstop / yield
// / expvar falsifiers. Drive the page loop through the paginationFetchPageFn
// + paginationResolvePageFn seams (phase1_walk_pagination.go) so the loop's
// behaviour is unit-falsifiable with NO live apiserver and NO 20,000-page
// mock. Pure, deterministic, -race clean. Does NOT touch ./internal/rbac/...
// (destructive TestMain).
// ─────────────────────────────────────────────────────────────────────────

// paginationTestMu serializes the #156 page-loop tests: they swap the
// package-level paginationFetchPageFn / paginationResolvePageFn seams, the
// process customerInFlightCount atomic, and read the package-level prewarm
// metric atomics + prewarmEngineSingleton().yieldTotal. Serializing keeps the
// counter-DELTA assertions deterministic (mirrors engineLatchTestMu).
var paginationTestMu sync.Mutex

// fakePage1Driven returns a widget CR + page-1 resolved envelope that
// iterateApiRefPages accepts (isApiRefTemplateDriven true via spec.apiRef +
// spec.resourcesRefsTemplate) and that wants continuation (.slice.continue
// true). status.resourcesRefs.items is EMPTY so no child recursion fires —
// the test isolates the PAGE loop (page cell + continuation + backstop +
// yield), not the child-walk subtree.
func fakePage1Driven() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Datagrid",
		"metadata":   map[string]any{"namespace": "krateo-system", "name": "compositions-page-datagrid"},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "compositions-list", "namespace": "krateo-system"},
			"resourcesRefsTemplate": []any{
				map[string]any{"id": "${.name}", "path": "/call?x", "verb": "GET"},
			},
		},
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{}, // empty → no child recursion
				"slice": map[string]any{"perPage": 5, "page": 1, "continue": true},
			},
		},
	}}
}

// fakePageEnvelope clones the page-1 shape and sets .slice.continue + .page.
// Used by the fake resolver to drive per-page continuation. Mutates the map
// directly (NOT via unstructured.NestedMap, which deep-copies and panics on
// plain int values) — purely a test fixture; production envelopes are real.
func fakePageEnvelope(page int, wantContinue bool) *unstructured.Unstructured {
	e := fakePage1Driven()
	status := e.Object["status"].(map[string]any)
	rr := status["resourcesRefs"].(map[string]any)
	rr["slice"] = map[string]any{"perPage": 5, "page": page, "continue": wantContinue}
	return e
}

// installFakePaginationSeams swaps the fetch + resolve seams. fetch returns a
// fresh fake CR (non-nil Unstructured, nil Err) so the loop proceeds to
// resolve; resolve delegates to resolveFn (which the test controls per page).
// The returned pageResolves pointer counts resolve invocations (page-progress
// probe for the yield test). t.Cleanup restores the production seams.
func installFakePaginationSeams(t *testing.T,
	resolveFn func(page int) (*unstructured.Unstructured, error),
) *atomic.Int64 {
	t.Helper()
	prevFetch := paginationFetchPageFn
	prevResolve := paginationResolvePageFn
	t.Cleanup(func() {
		paginationFetchPageFn = prevFetch
		paginationResolvePageFn = prevResolve
	})

	var pageResolves atomic.Int64
	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return objects.Result{Unstructured: fakePage1Driven()}
	}
	paginationResolvePageFn = func(_ context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		pageResolves.Add(1)
		return resolveFn(opts.Page)
	}
	return &pageResolves
}

// drainAllCustomerInFlight zeroes the process customer-inflight counter so
// engineYieldCheckpoint is a fast no-op (mirrors zeroCustomerInFlight in the
// latch test). The page-loop tests that are NOT the yield test call this so a
// stray counter from a failed sibling does not park them.
func drainAllCustomerInFlight() {
	for customerInFlight() {
		customerInFlightCount.Add(-1)
	}
}

// snapPrewarmMetrics returns a snapshot of the three #156 metric atomics for
// delta assertions.
type prewarmMetricsSnap struct{ planned, seeded, pages uint64 }

func snapPrewarmMetrics() prewarmMetricsSnap {
	return prewarmMetricsSnap{
		planned: prewarmUnitsPlanned.Load(),
		seeded:  prewarmUnitsSeeded.Load(),
		pages:   prewarmApiRefPagesTotal.Load(),
	}
}

// runIterateApiRefPagesForTest invokes iterateApiRefPages with a minimal
// walker shell + the fake page-1 envelope. Captures slog at Info level so the
// backstop Warn / completed Info can be asserted. Returns the captured log.
func runIterateApiRefPagesForTest(t *testing.T, ctx context.Context) string {
	t.Helper()
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logCtx := xcontext.BuildContext(ctx, xcontext.WithLogger(slog.New(h)))

	in := fakePage1Driven()
	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"}
	iterateApiRefPages(
		logCtx,
		newPhase1Walker(nil, "test-authn-ns"),
		in,
		gvr,
		fakePage1Driven(),          // page1Res — wants continue
		1,                          // depth
		newPrewarmPagination(5, 1), // #966: the first page's one pagination
		"test-authn-ns",
	)
	return buf.String()
}

// TestIterateApiRefPages_ContinuesBeyondPageOne_DeclaredSlice (§4.2 #1):
// a declared-slice widget continues page 2..N and HALTS on
// .slice.continue==false at page N — BEFORE the backstop. Proves the
// continuation fires beyond page 1 and the blueprint terminator is operative,
// not the cap.
func TestIterateApiRefPages_ContinuesBeyondPageOne_DeclaredSlice(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	// Backstop set WELL ABOVE the declared list so the cap is NOT the binding
	// constraint — the loop must stop on .slice.continue==false.
	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 500
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	// Resolver: continue==true for pages 2..7, false at page 8 (the last page).
	const lastPage = 8
	resolves := installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, page < lastPage), nil
	})

	before := snapPrewarmMetrics()
	log := runIterateApiRefPagesForTest(t, context.Background())

	// Pages 2..8 resolved = 7 extra pages, then break on continue==false@8.
	const wantPages = lastPage - 1 // pages 2..8
	if got := resolves.Load(); got != wantPages {
		t.Fatalf("resolve invocations = %d; want %d (pages 2..%d, halt on .slice.continue==false@%d)",
			got, wantPages, lastPage, lastPage)
	}
	delta := snapPrewarmMetrics()
	if got := delta.pages - before.pages; got != wantPages {
		t.Errorf("apiref_pages_total delta = %d; want %d", got, wantPages)
	}
	// Empty items → one planned/seeded unit per page (the page cell only).
	if got := delta.planned - before.planned; got != wantPages {
		t.Errorf("units_planned delta = %d; want %d (page cell per page, no children)", got, wantPages)
	}
	if got := delta.seeded - before.seeded; got != wantPages {
		t.Errorf("units_seeded delta = %d; want %d", got, wantPages)
	}
	// The HALT was the declared terminator, NOT the backstop — no backstop Warn.
	if rec := findLogRecord(t, log, "phase1.walk.apiref_pagination.backstop_hit"); rec != nil {
		t.Errorf("backstop_hit Warn emitted on a declared-slice halt; the terminator must be "+
			".slice.continue, not the cap. logs:\n%s", log)
	}
	if rec := findLogRecord(t, log, "phase1.walk.apiref_pagination.completed"); rec != nil {
		if bh, _ := rec["backstop_hit"].(bool); bh {
			t.Errorf("completed event backstop_hit=true on a declared-slice halt; want false")
		}
	}
}

// TestIterateApiRefPages_HaltsOnSliceContinueFalse (§4.2 #2): the loop stops
// at the page where the resolver flips continue=false, with a high cap.
// Proves the blueprint-declared terminator is operative.
func TestIterateApiRefPages_HaltsOnSliceContinueFalse(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 500
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	// continue flips false at page 3 → loop resolves pages 2,3 then breaks.
	resolves := installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, page < 3), nil
	})

	log := runIterateApiRefPagesForTest(t, context.Background())

	if got := resolves.Load(); got != 2 {
		t.Fatalf("resolve invocations = %d; want 2 (pages 2,3 then halt on continue==false@3)", got)
	}
	if rec := findLogRecord(t, log, "phase1.walk.apiref_pagination.backstop_hit"); rec != nil {
		t.Errorf("backstop_hit Warn must NOT fire when the resolver declares the end; logs:\n%s", log)
	}
}

// TestIterateApiRefPages_BackstopHaltsRunawayAndWarns (§4.2 #2 backstop half):
// a never-ending continue=true fake is HALTED by the liveness backstop, and a
// loud Warn (phase1.walk.apiref_pagination.backstop_hit) is emitted — hitting
// the backstop is now an ANOMALY. Pins the liveness guarantee.
func TestIterateApiRefPages_BackstopHaltsRunawayAndWarns(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	// Small backstop so the runaway is capped quickly.
	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 10
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	// Resolver ALWAYS wants continue → only the backstop can stop the loop.
	resolves := installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, true), nil
	})

	log := runIterateApiRefPagesForTest(t, context.Background())

	// pages 2..10 = 9 resolves, then loop bound (page>10) exits.
	const wantPages = 10 - 1
	if got := resolves.Load(); got != wantPages {
		t.Fatalf("resolve invocations = %d; want %d (backstop=10 → pages 2..10)", got, wantPages)
	}
	// The loud anomaly Warn MUST be present.
	rec := findLogRecord(t, log, "phase1.walk.apiref_pagination.backstop_hit")
	if rec == nil {
		t.Fatalf("missing backstop_hit Warn on a runaway continue==true fake; logs:\n%s", log)
	}
	if lvl, _ := rec["level"].(string); lvl != "WARN" {
		t.Errorf("backstop_hit level = %q; want WARN (hitting the backstop is an anomaly)", lvl)
	}
	// completed event must carry backstop_hit=true.
	if c := findLogRecord(t, log, "phase1.walk.apiref_pagination.completed"); c != nil {
		if bh, _ := c["backstop_hit"].(bool); !bh {
			t.Errorf("completed event backstop_hit=false on a runaway; want true")
		}
	}
}

// TestIterateApiRefPages_CapDiscriminates_RedGreen (§4.2 discrimination):
// the RED/GREEN proof that the cap-raise is load-bearing. A fake population
// of >500 pages is TRUNCATED at the OLD cap (500) but FULLY drained when the
// backstop is raised above the population. Both halves in one test so the
// discrimination is explicit.
func TestIterateApiRefPages_CapDiscriminates_RedGreen(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	// A fake declared population of 600 pages (continue flips false at 601 →
	// items 1..600 worth of pages). This is the > 500 population the OLD cap
	// could not cover. (We keep it modest so the test runs fast; the real 50K
	// population is 10,000 pages.)
	const populationLastPage = 601 // continue==false at 601 → pages 2..601 if uncapped

	resolveFn := func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, page < populationLastPage), nil
	}

	// ── RED: OLD cap = 500. The loop hits the backstop at page 500 and is
	// TRUNCATED before the resolver's declared end → coverage lost.
	func() {
		oldCap := phase1MaxApiRefPagesForTest
		phase1MaxApiRefPagesForTest = 500
		t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })
		resolves := installFakePaginationSeams(t, resolveFn)
		log := runIterateApiRefPagesForTest(t, context.Background())

		// pages 2..500 = 499 resolves, then backstop (page>500) exits.
		if got := resolves.Load(); got != 499 {
			t.Fatalf("RED (cap=500): resolve invocations = %d; want 499 (TRUNCATED at backstop)", got)
		}
		if findLogRecord(t, log, "phase1.walk.apiref_pagination.backstop_hit") == nil {
			t.Fatalf("RED (cap=500): expected backstop_hit Warn (population 600 > cap 500 → truncation); logs:\n%s", log)
		}
	}()

	// ── GREEN: raised backstop (1,000 > population 600). The loop runs to the
	// resolver's declared end (.slice.continue==false@601) → FULL drain, no
	// backstop Warn.
	func() {
		oldCap := phase1MaxApiRefPagesForTest
		phase1MaxApiRefPagesForTest = 1_000
		t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })
		resolves := installFakePaginationSeams(t, resolveFn)
		log := runIterateApiRefPagesForTest(t, context.Background())

		// pages 2..601 = 600 resolves, halt on declared continue==false@601.
		if got := resolves.Load(); got != 600 {
			t.Fatalf("GREEN (cap=1000): resolve invocations = %d; want 600 (FULL drain to declared end)", got)
		}
		if rec := findLogRecord(t, log, "phase1.walk.apiref_pagination.backstop_hit"); rec != nil {
			t.Fatalf("GREEN (cap=1000): backstop_hit must NOT fire — the declared terminator stopped the loop; logs:\n%s", log)
		}
	}()
}

// TestIterateApiRefPages_YieldsToCustomer (§4.2 #3): with a customer marked
// in-flight, the page loop PARKS (no resolve progress) until the customer
// completes; the prewarm engine's yieldTotal advances while parked. Pins the
// new P8 checkpoint (engineYieldCheckpoint at the top of each page iteration).
func TestIterateApiRefPages_YieldsToCustomer(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 50
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	// Resolver always continues (so the only thing that can stop progress is
	// the yield, not an early halt).
	resolves := installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, true), nil
	})

	// Hold a customer in-flight via the production bracket BEFORE the loop
	// starts so the FIRST page-iteration yield parks.
	done := markCustomerInFlight()
	yieldBefore := prewarmEngineSingleton().yieldTotal.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		runIterateApiRefPagesForTest(t, ctx)
		close(finished)
	}()

	// Window: the loop MUST NOT have resolved any page while the customer is
	// in flight (the yield parks at the top of iteration before the fetch).
	time.Sleep(200 * time.Millisecond)
	if got := resolves.Load(); got != 0 {
		done()
		t.Fatalf("page loop resolved %d pages while a customer was in flight; the P8 yield did not engage", got)
	}
	// The engine must have parked at least once (yieldTotal advanced).
	if got := prewarmEngineSingleton().yieldTotal.Load() - yieldBefore; got == 0 {
		done()
		t.Fatalf("prewarm engine yieldTotal did not advance while parked; engineYieldCheckpoint not exercised")
	}

	// Release the customer — the loop must resume and make progress.
	done()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if resolves.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := resolves.Load(); got == 0 {
		cancel()
		t.Fatalf("page loop made no progress within 2s after the customer completed; yield never released")
	}

	// Stop the (otherwise unbounded continue==true) loop and wait for the
	// goroutine to exit cleanly so -race sees no leaked writer.
	cancel()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("iterateApiRefPages goroutine did not exit after ctx cancel")
	}
}

// TestIterateApiRefPages_ExpvarsIncrementAcrossFakeWalk (§4.2 #4): the three
// #156 expvars increment correctly across a fake multi-page walk. Asserts the
// published expvar values move (not just the backing atomics) so the
// /debug/vars acceptance surface is wired. Uses RegisterPhase1PaginationMetricsForTest
// to publish under the test binary (CACHE_ENABLED may be unset at init).
func TestIterateApiRefPages_ExpvarsIncrementAcrossFakeWalk(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	RegisterPhase1PaginationMetricsForTest()

	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 500
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	// 4 extra pages: continue==true for 2..5, false at 6 → pages 2..5? No:
	// continue==false at page 5 means the loop resolves 2,3,4,5 then breaks =
	// 4 pages. Keep it explicit.
	const lastPage = 5
	installFakePaginationSeams(t, func(page int) (*unstructured.Unstructured, error) {
		return fakePageEnvelope(page, page < lastPage), nil
	})

	readExpvar := func(name string) uint64 {
		v := expvar.Get(name)
		if v == nil {
			t.Fatalf("expvar %s not published; RegisterPhase1PaginationMetricsForTest did not run or name mismatch", name)
		}
		// expvar.Func returns the value via String() as a JSON number.
		var n uint64
		if _, err := fmt.Sscanf(v.String(), "%d", &n); err != nil {
			t.Fatalf("expvar %s value %q not a uint64: %v", name, v.String(), err)
		}
		return n
	}

	plannedBefore := readExpvar("snowplow_phase1_units_planned")
	seededBefore := readExpvar("snowplow_phase1_units_seeded")
	pagesBefore := readExpvar("snowplow_phase1_apiref_pages_total")

	_ = runIterateApiRefPagesForTest(t, context.Background())

	const wantPages = lastPage - 1 // pages 2..5
	if got := readExpvar("snowplow_phase1_apiref_pages_total") - pagesBefore; got != wantPages {
		t.Errorf("snowplow_phase1_apiref_pages_total delta = %d; want %d", got, wantPages)
	}
	if got := readExpvar("snowplow_phase1_units_planned") - plannedBefore; got != wantPages {
		t.Errorf("snowplow_phase1_units_planned delta = %d; want %d", got, wantPages)
	}
	if got := readExpvar("snowplow_phase1_units_seeded") - seededBefore; got != wantPages {
		t.Errorf("snowplow_phase1_units_seeded delta = %d; want %d", got, wantPages)
	}
}
