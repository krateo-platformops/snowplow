// phase1_walk_pagination_keymodel_test.go — Task #318 Step 1 falsifiers.
//
// THREE defects in the deferred-pagination drain, proven RED on HEAD
// (68ebfd3) and GREEN after the Step-1 fix (design
// docs/task-318-deep-page-coverage-design-2026-06-11.md §1, §6):
//
//  1. KEY-MODEL (#317-superseding). The drain's two key sites disagree:
//     the WithL1KeyContext install (phase1_walk_pagination.go ~:403) hashes
//     (keyPerPage, page) while the populate call (~:446) hashes
//     (perPage, page). When keyPerPage != perPage (the datagrid: keyPerPage
//     = -1 from the (-1,-1) root tuple, perPage = 5 the resolution default)
//     the dep-edges land in one cell and the envelope in another — the
//     AC-G.5 detached-entry defect re-introduced. AND the drain's child Put
//     key must equal paginationInfo's serve-time key for ?page=2&perPage=M.
//
//  2. CACHE-A SINK PARITY. The drain's :446 populate is handed the bare ctx,
//     which carries no StageErrorSink — so a per-item stage error during the
//     page resolve does NOT decline the Put (no parity with the page-1 site
//     at phase1_walk.go:1162/1226). The fix installs WithStageErrorSink on
//     the drain resolveCtx and passes resolveCtx into populateWidgetContentL1.
//
//  3. COLLECTION ROBUSTNESS (post-storm zero). When the walk reaches an
//     eligible (isApiRefTemplateDriven) widget whose page-1 resolve produced
//     NO continuation (the post-storm short/empty page), collect() silently
//     drops it — no counter, no candidate, no re-collection. The fix records
//     the eligible-but-no-continuation condition (counter + log) and the drain
//     re-resolves page-1 for those candidates; a retry that now sees
//     continuation produces a collected job.
//
// All falsifiers are UNIT (seam-swappable via paginationFetchPageFn /
// paginationResolvePageFn). Pure, deterministic, -race clean. Does NOT touch
// ./internal/rbac/... (destructive TestMain).

package dispatchers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fetchOKResult is a fake objects.Get result that returns a fresh widget CR
// (non-nil Unstructured, nil Err) so the page loop / re-collection proceeds to
// resolve. Mirrors installFakePaginationSeams' fetch shape.
func fetchOKResult(ns, name string) objects.Result {
	return objects.Result{Unstructured: newUnstructuredWidget(ns, name)}
}

// ─────────────────────────────────────────────────────────────────────────
// Falsifier 1 — drain page-cell KEY MODEL: install key == Put key, and the
// drain key tuple matches the serve-time paginationInfo key model.
// ─────────────────────────────────────────────────────────────────────────

// drainPageCellGVR — a stable GVR for the key falsifiers.
func drainPageCellGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "datagrids"}
}

// TestDrainKeyModel_InstallKeyEqualsPutKey (design §1a, §6 row 1).
//
// OBSERVES THE REAL CODE (not a static reproduction): the drain installs the
// inner-call dep-edge key on the resolve ctx at the :403 WithL1KeyContext site
// and Puts the page envelope at the :446 populate site. Both MUST land in the
// SAME L1 cell or the dep edges detach from the entry (AC-G.5).
//
// We drive iterateApiRefPages with seams that:
//   - capture the L1 key the :403 install site put on the resolve ctx
//     (cache.L1KeyFromContext), and
//   - return a NON-RBAC-sensitive per-page envelope so the :446 populate Put
//     actually LANDS (a real datagrid page cell is RBAC-declined; here we
//     isolate the KEY-MODEL — does the Put land where the install pointed?).
//
// The page-1 envelope is apiRef+template-driven (loop entry). Pre-#318 the
// install keyed (keyPerPage=-1, page=2) while the Put keyed (perPage=5, page=2)
// → the Put landed in a DIFFERENT cell than the install pointed at → RED.
// Since #966 the first page's ONE pagination is (perPage=5, page=1) and drain
// page 2's is pg.pageAfter(1) = (5, 2) for the install, the resolve AND the Put.
func TestDrainKeyModel_InstallKeyEqualsPutKey(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 2 // page 2 only, then halt
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	prevFetch := paginationFetchPageFn
	prevResolve := paginationResolvePageFn
	t.Cleanup(func() {
		paginationFetchPageFn = prevFetch
		paginationResolvePageFn = prevResolve
	})

	gvr := drainPageCellGVR()
	const ns, name = "krateo-system", "compositions-page-datagrid"

	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult(ns, name)
	}
	var installKey atomic.Value // string — the L1 key the :403 install put on ctx
	installKey.Store("")
	paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		// The :403 install decorates the resolve ctx with the page-cell L1 key.
		installKey.Store(cache.L1KeyFromContext(ctx))
		// NON-RBAC-sensitive page envelope so the :446 Put lands; continue=false
		// halts the loop after this page.
		return nonRBACSensitivePageEnvelope(ns, name, 2, false), nil
	}

	iterateApiRefPages(
		context.Background(),
		newPhase1Walker(nil, "krateo-system"),
		newUnstructuredWidget(ns, name),
		gvr,
		fakePage1Driven(),          // page1Res — apiRef+template + wants continue
		1,                          // depth
		newPrewarmPagination(5, 1), // #966: the first page's one pagination
		"krateo-system",
	)

	ik, _ := installKey.Load().(string)
	if ik == "" {
		t.Fatalf("the :403 install site did not decorate the resolve ctx with an L1 key")
	}
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache nil under cache=on")
	}
	if _, hit := c.Get(ik); !hit {
		// The Put landed somewhere OTHER than the install key. Reconstruct the
		// HEAD Put cell (perPage=5, page=2) for the diagnostic.
		headPutKey, _ := widgetContentL1Key(gvr, ns, name, 5, 2)
		_, headHit := c.Get(headPutKey)
		t.Fatalf("DRAIN KEY MISMATCH (#317/AC-G.5): the :446 Put did NOT land in the :403 install "+
			"cell — dep edges detach from the entry.\n"+
			"  install key (on resolve ctx) = %s (cache hit: false)\n"+
			"  HEAD Put cell (perPage=5,page=2) = %s (cache hit: %v)",
			ik, headPutKey, headHit)
	}
}

// TestDrainKeyModel_ChildPutKeyMatchesServeTime (design §1b, §6 row 1).
//
// A leaf child reached via a ?page=2&perPage=M /call Path is the drain's only
// serveable value (design §1d). Its Put key (derived via prewarmPaginationFor,
// the page-1-identical path) MUST equal the serve-time content lookup key the
// dispatcher composes from paginationInfo for the SAME URL. This pins the
// prewarmPaginationFor ↔ paginationInfo contract (prewarm_pagination.go).
//
// This GREENs on HEAD for the CHILD path (children go through
// prewarmPaginationFor); it is included as the positive control that the key
// model the fix adopts for the PAGE cell is the SAME one serve-time uses.
func TestDrainKeyModel_ChildPutKeyMatchesServeTime(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
	const ns, name = "fireworks-app", "bench-app-01-panel"
	const perPageURL, pageURL = 7, 2
	childPath := "/call?resource=panels&apiVersion=widgets.templates.krateo.io/v1beta1" +
		"&namespace=" + ns + "&name=" + name +
		"&page=2&perPage=7"

	// Prewarm side: the child's one pagination comes from prewarmPaginationFor.
	childPg := prewarmPaginationFor(childPath)
	childKeyPerPage, childKeyPage := childPg.PerPage, childPg.Page
	childPutKey, _ := widgetContentL1Key(gvr, ns, name, childKeyPerPage, childKeyPage)

	// Serve side: the dispatcher composes the content key from paginationInfo's
	// URL-derived tuple. Drive paginationInfo via dispatchWidgetContentKey with
	// the SAME (perPage, page) the URL declares.
	serveKey, _, _ := dispatchWidgetContentKey(context.Background(),
		gvr.Group, gvr.Version, gvr.Resource, ns, name, perPageURL, pageURL, nil)

	if childPutKey == "" || serveKey == "" {
		t.Fatalf("keys must be non-empty; childPut=%q serve=%q", childPutKey, serveKey)
	}
	if childPutKey != serveKey {
		t.Fatalf("CHILD Put key != serve-time lookup key for %s\n  childPut (perPage=%d,page=%d) = %s\n  serve    (perPage=%d,page=%d) = %s",
			childPath, childKeyPerPage, childKeyPage, childPutKey, perPageURL, pageURL, serveKey)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Falsifier 2 — CACHE-A SINK PARITY at the drain's :446 populate site.
//
// The drain must pass a ctx carrying a StageErrorSink into
// populateWidgetContentL1 so a per-item stage error during the page resolve
// declines the Put — parity with the page-1 site (phase1_walk.go:1162/1226).
// On HEAD :446 passes the bare ctx (no sink): a non-RBAC-sensitive page cell
// resolved under an error is still Put. RED on HEAD; GREEN after the fix.
// ─────────────────────────────────────────────────────────────────────────

// TestDrainSinkParity_PopulateDeclinesOnStageError (design §1c-note, §6 row 2).
//
// We drive iterateApiRefPages through the resolve seam. The seam:
//   - bumps the StageErrorSink it finds on the resolve ctx (a no-op on HEAD,
//     where no sink is installed — proving the gap), and
//   - returns a NON-RBAC-sensitive envelope (apiRef present, NO render
//     template) so populateWidgetContentL1 does NOT decline at the upstream
//     RBAC-sensitive guard (:213) and the stage-error gate (:268) is the only
//     thing that can decline the Put.
//
// The PAGE-1 envelope handed to iterateApiRefPages is apiRef+template-driven
// (to satisfy the loop's entry predicate); the per-PAGE seam result is the
// non-RBAC-sensitive shape — this isolates the :446 call-site wiring.
//
// HEAD: no sink on ctx → seam's Bump is a no-op → sink gate sees count 0 →
// Put lands → RED. Fix: resolveCtx carries the sink → Bump registers →
// sink gate declines → no Put → GREEN.
func TestDrainSinkParity_PopulateDeclinesOnStageError(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()
	drainAllCustomerInFlight()

	// One drain page, then halt (continue=false on the returned envelope).
	oldCap := phase1MaxApiRefPagesForTest
	phase1MaxApiRefPagesForTest = 2 // allow page 2 only
	t.Cleanup(func() { phase1MaxApiRefPagesForTest = oldCap })

	prevFetch := paginationFetchPageFn
	prevResolve := paginationResolvePageFn
	t.Cleanup(func() {
		paginationFetchPageFn = prevFetch
		paginationResolvePageFn = prevResolve
	})

	gvr := drainPageCellGVR()
	const ns, name = "krateo-system", "compositions-page-datagrid"

	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult(ns, name)
	}
	var sinkSeen atomic.Bool
	paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		// Mirror what the api resolver does on a per-item hard error: bump
		// the stage-error sink the resolve ctx carries. nil-safe.
		if sink := cache.StageErrorSinkFromContext(ctx); sink != nil {
			sinkSeen.Store(true)
			sink.Bump("compositions-list", "boom: per-item iterator error")
		}
		// Return a NON-RBAC-sensitive envelope (apiRef present, NO render
		// template) so the populate is not declined upstream by the
		// RBAC-sensitive guard — the stage-error gate is the deciding gate.
		// continue=false so the loop halts after this page.
		return nonRBACSensitivePageEnvelope(ns, name, 2, false), nil
	}

	// Page-1 envelope is apiRef+template-driven + wants continue (loop entry).
	page1 := fakePage1Driven()

	iterateApiRefPages(
		context.Background(),
		newPhase1Walker(nil, "krateo-system"),
		newUnstructuredWidget(ns, name),
		gvr,
		page1,
		1,                          // depth
		newPrewarmPagination(5, 1), // #966: the first page's one pagination
		"krateo-system",
	)

	// The cell that the :446 populate would write is keyed by the FIXED key
	// model — compute it via the same helper. With the fix, this cell is
	// DECLINED (sink count>0). On HEAD it is Put.
	putKey, _ := widgetContentL1Key(gvr, ns, name, -1, 2) // post-fix key tuple
	headPutKey, _ := widgetContentL1Key(gvr, ns, name, 5, 2)
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache nil under cache=on")
	}
	_, hitFixKey := c.Get(putKey)
	_, hitHeadKey := c.Get(headPutKey)
	if hitFixKey || hitHeadKey {
		t.Fatalf("SINK PARITY GAP: the :446 populate Put landed despite a per-item stage error "+
			"during the page resolve. The drain must pass a sink-bearing resolveCtx so the Put "+
			"declines (parity with phase1_walk.go:1162/1226). sinkSeenByResolve=%v hitFixKey=%v hitHeadKey=%v",
			sinkSeen.Load(), hitFixKey, hitHeadKey)
	}
	// After the fix the resolve MUST have observed a non-nil sink (the wiring
	// is the load-bearing change). On HEAD this is false → the assertion above
	// already fired; this guards the GREEN state is for the RIGHT reason.
	if !sinkSeen.Load() {
		t.Fatalf("the resolve ctx carried NO StageErrorSink — the :446 site is still passing the " +
			"bare ctx; sink parity wiring missing")
	}
}

// nonRBACSensitivePageEnvelope returns a resolved page envelope that is NOT
// RBAC-sensitive (apiRef present but NO widgetDataTemplate / NO
// resourcesRefsTemplate) so populateWidgetContentL1 does not decline at the
// RBAC-sensitive guard. Carries a non-empty resourcesRefs.items so the
// empty-shell guard does not fire either. The slice.continue controls the loop.
func nonRBACSensitivePageEnvelope(ns, name string, page int, wantContinue bool) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Datagrid",
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec": map[string]any{
			// apiRef present, but NO render template → not RBAC-sensitive.
			"apiRef": map[string]any{"name": "compositions-list", "namespace": ns},
		},
		"status": map[string]any{
			"resourcesRefs": map[string]any{
				"items": []any{map[string]any{"id": "row", "path": "/x", "verb": "GET", "allowed": true}},
				"slice": map[string]any{"perPage": 5, "page": page, "continue": wantContinue},
			},
		},
	}}
}

// ─────────────────────────────────────────────────────────────────────────
// Falsifier 3 — COLLECTION ROBUSTNESS (post-storm zero-collection).
//
// When the walk reaches an eligible (isApiRefTemplateDriven) widget whose
// page-1 resolve produced NO continuation, collect() must record an
// "eligible-but-no-continuation" candidate (counter + log) so a re-collection
// pass can retry it. HEAD silently drops it → RED.
// ─────────────────────────────────────────────────────────────────────────

// TestCollectionRobustness_EligibleNoContinueRecorded (design §3, §6 row 3).
//
// HEAD: collect() returns silently at the resolverWantsContinue==false branch
// for an eligible widget — no counter, no candidate. RED. Fix: the counter
// advances and the candidate is tracked for re-collection.
func TestCollectionRobustness_EligibleNoContinueRecorded(t *testing.T) {
	c := newApiRefPaginationCollector()

	// Eligible widget (apiRef+template) whose page-1 resolve did NOT signal
	// continue (the post-storm short/empty page).
	job := fakeApiRefPaginationJob("krateo-system", "compositions-page-datagrid")
	status := job.Page1Res.Object["status"].(map[string]any)
	slice := status["resourcesRefs"].(map[string]any)["slice"].(map[string]any)
	slice["continue"] = false

	before := eligibleNoContinueCount()
	c.collect(job)

	// No job is collected (correct — page-1 said no continuation).
	if got := c.count(); got != 0 {
		t.Fatalf("a no-continuation page-1 must NOT yield a collected job; count=%d want=0", got)
	}
	// But the eligible-but-no-continuation condition MUST be observable.
	if got := eligibleNoContinueCount(); got <= before {
		t.Fatalf("ELIGIBLE-BUT-NO-CONTINUATION not recorded: counter did not advance (before=%d after=%d). "+
			"The post-storm zero-collection condition is silent on HEAD; the fix must surface it for re-collection.",
			before, eligibleNoContinueCount())
	}
	// AND the candidate MUST be tracked so the drain can re-resolve it.
	if n := c.pendingRecollectCount(); n != 1 {
		t.Fatalf("eligible-but-no-continuation candidate not tracked for re-collection; pending=%d want=1", n)
	}
}

// TestCollectionRobustness_RecollectProducesJobWhenContinueReturns (design §3, §6 row 3).
//
// A re-collection pass re-resolves page-1 for the pending candidates via the
// existing fetch/resolve seams. When the retry NOW sees continuation, a job is
// collected (idempotent — dedup by jobKey). HEAD has no re-collection path at
// all → the function does not exist → RED at compile until the fix lands.
func TestCollectionRobustness_RecollectProducesJobWhenContinueReturns(t *testing.T) {
	paginationTestMu.Lock()
	defer paginationTestMu.Unlock()

	// Skip the production informer-settle wait — this is a unit falsifier.
	t.Cleanup(recollectDelayForTest(0))

	c := newApiRefPaginationCollector()

	// Stage one eligible-but-no-continuation candidate.
	job := fakeApiRefPaginationJob("krateo-system", "compositions-page-datagrid")
	status := job.Page1Res.Object["status"].(map[string]any)
	slice := status["resourcesRefs"].(map[string]any)["slice"].(map[string]any)
	slice["continue"] = false
	c.collect(job)
	if c.pendingRecollectCount() != 1 {
		t.Fatalf("setup: expected 1 pending recollect candidate; got %d", c.pendingRecollectCount())
	}

	// Re-collection seams: the retry re-fetches + re-resolves page-1; this time
	// the resolver signals continuation (the informer has settled).
	prevFetch := paginationFetchPageFn
	prevResolve := paginationResolvePageFn
	t.Cleanup(func() {
		paginationFetchPageFn = prevFetch
		paginationResolvePageFn = prevResolve
	})
	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult("krateo-system", "compositions-page-datagrid")
	}
	paginationResolvePageFn = func(_ context.Context, opts widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		// Re-collection MUST re-resolve at PAGE 1 (it is re-trying the page-1
		// continuation decision, not advancing to page 2).
		if opts.Page != 1 {
			t.Errorf("re-collection must re-resolve page 1; got page=%d", opts.Page)
		}
		return fakeContinuingPage1(), nil
	}

	// saRC=nil is the unit-test posture (the fake resolve seam ignores RC);
	// production passes the SA *rest.Config (nil-rc 500 class).
	recollectPendingApiRefPaginationJobs(context.Background(), c, nil)

	if got := c.count(); got != 1 {
		t.Fatalf("re-collection did not produce a job after the retry saw continuation; count=%d want=1", got)
	}
	// The candidate is consumed (no infinite re-collection).
	if n := c.pendingRecollectCount(); n != 0 {
		t.Fatalf("pending recollect candidate must be consumed after a successful retry; pending=%d want=0", n)
	}
}

// fakeContinuingPage1 is the page-1 envelope a settled informer would produce:
// eligible (apiRef+template) AND wants continuation.
func fakeContinuingPage1() *unstructured.Unstructured {
	return fakePage1Driven() // already apiRef+template + slice.continue=true
}

// TestCollectionRobustness_CollectorConcurrentRace (-race): the collector's NEW
// pendingRecollect map (Task #318 Step 1) is written by collect() and
// read/cleared by pendingRecollectCount/drainPendingRecollect. The collector's
// contract is "safe regardless of how the walk is scheduled" — adding a second
// mutex-guarded map MUST preserve that. Hammer collect (mixed continue /
// no-continue) + the readers + drainPendingRecollect concurrently; -race must
// see no data race on jobs OR pendingRecollect.
//
// Per feedback_shared_vs_copy_is_a_concurrency_change: the new shared map is a
// concurrency surface and needs a concurrent -race test, not a content check.
func TestCollectionRobustness_CollectorConcurrentRace(t *testing.T) {
	c := newApiRefPaginationCollector()

	const writers = 8
	const perWriter = 200

	var writersWG sync.WaitGroup
	// Writers: even workers collect CONTINUING jobs (→ c.jobs), odd workers
	// collect NO-CONTINUATION eligible jobs (→ c.pendingRecollect). Distinct
	// (ns,name) per writer so both maps grow concurrently across keys.
	for wkr := 0; wkr < writers; wkr++ {
		writersWG.Add(1)
		go func(wkr int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				job := fakeApiRefPaginationJob(fmt.Sprintf("ns-%d", wkr), fmt.Sprintf("w-%d-%d", wkr, i))
				if wkr%2 == 1 {
					st := job.Page1Res.Object["status"].(map[string]any)
					sl := st["resourcesRefs"].(map[string]any)["slice"].(map[string]any)
					sl["continue"] = false // eligible-but-no-continuation → pendingRecollect
				}
				c.collect(job)
			}
		}(wkr)
	}

	// Concurrent readers + a periodic pending drainer (the drain goroutine
	// shape). They race against the writers on BOTH maps until stopped.
	stop := make(chan struct{})
	var readersWG sync.WaitGroup
	for r := 0; r < 3; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.count()
					_ = c.pendingRecollectCount()
					_ = c.drainPendingRecollect()
				}
			}
		}()
	}

	writersWG.Wait() // all collects done
	close(stop)      // stop readers
	readersWG.Wait() // join readers — -race verdict is the assertion

	// Sanity floor only (the drainer races the writers so exact counts are
	// non-deterministic): the test's purpose is the -race clean verdict.
	if c.count() < 0 || c.pendingRecollectCount() < 0 {
		t.Fatalf("impossible negative counts")
	}
}

// TestCollectionRobustness_HEADSilentlyDropsEligibleNoContinue is the
// HEAD-COMPATIBLE RED probe for piece 3 (uses ONLY symbols that exist on HEAD).
// It documents the defect the fix closes: an eligible widget whose page-1
// resolve did not signal continuation is silently dropped by collect() with NO
// trace — count()==0 and nothing else observable. After the fix the counter +
// pendingRecollectCount surface the condition (proven by the two tests above).
//
// This test PASSES on HEAD (it asserts the defective silent-drop behaviour) and
// is the captured evidence that the drop is currently silent; it stays GREEN
// after the fix too (collect still does not produce a job — the NEW signal is
// the counter/candidate, asserted separately). It is a documentation pin, not
// a behaviour gate.
func TestCollectionRobustness_HEADSilentlyDropsEligibleNoContinue(t *testing.T) {
	c := newApiRefPaginationCollector()
	job := fakeApiRefPaginationJob("krateo-system", "compositions-page-datagrid")
	status := job.Page1Res.Object["status"].(map[string]any)
	slice := status["resourcesRefs"].(map[string]any)["slice"].(map[string]any)
	slice["continue"] = false

	c.collect(job)
	if c.count() != 0 {
		t.Fatalf("eligible-but-no-continuation must not yield a collected job; count=%d", c.count())
	}
}
