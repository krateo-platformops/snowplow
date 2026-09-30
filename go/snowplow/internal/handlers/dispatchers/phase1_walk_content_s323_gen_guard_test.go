// phase1_walk_content_s323_gen_guard_test.go — #323 per-carrier real-race REFUSAL
// arm for the widget_content keep-warm carrier (populateWidgetContentL1).
//
// This drives the REAL pagination walker (iterateApiRefPages) so the production
// capture point runs: the walker computes the content key and CaptureGen's its
// generation at phase1_walk_pagination.go:460, BEFORE the page resolve
// (paginationResolvePageFn). The resolve seam DELETE-evicts that content key
// mid-resolve — bumping its generation past the captured value — so the tail
// populateWidgetContentL1 (post-readyz, cache.IsPhase1Done()==true) REFUSES its
// PutIfGen: the pre-delete body is not resurrected (cell absent) and the refusal
// counter bumps. Body-independent discriminator; catches a wrong capture key (M1)
// and — because it drives the REAL walker capture, not an installed gen — a
// capture-after-resolve (M2). RED-captured by neutering the guard.

package dispatchers

import (
	"context"
	"sync/atomic"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPhase1WalkContent_S323_PostReadyz_ResolveRacingDelete_RefusedNotResurrected(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	// POST-readyz: engage populateWidgetContentL1's generation guard (the boot
	// seed's pre-readyz plain-Put exemption is off).
	cache.MarkPhase1Done()
	t.Cleanup(cache.ResetPhase1DoneForTest)

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
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache nil under cache=on")
	}

	paginationFetchPageFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return fetchOKResult(ns, name)
	}
	var wcKeyV atomic.Value // string — the content key the :460 capture used
	wcKeyV.Store("")
	paginationResolvePageFn = func(ctx context.Context, _ widgets.ResolveOptions) (*unstructured.Unstructured, error) {
		// The :460 install decorated the resolve ctx with the page-cell content key
		// AFTER the walker CaptureGen'd it. DELETE-evict it HERE (during the resolve)
		// so the tail populateWidgetContentL1 PutIfGen refuses.
		wcKey := cache.L1KeyFromContext(ctx)
		wcKeyV.Store(wcKey)
		store.Put(wcKey, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
		store.DeleteForTest(wcKey)
		// NON-RBAC-sensitive page envelope so the tail Put site is reached;
		// continue=false halts the loop after this page.
		return nonRBACSensitivePageEnvelope(ns, name, 2, false), nil
	}

	refusedBefore := store.Stats().PutRefusedGenerationMovedTotal
	iterateApiRefPages(
		context.Background(),
		newPhase1Walker(nil, "krateo-system"),
		newUnstructuredWidget(ns, name),
		gvr,
		fakePage1Driven(), // page1Res — apiRef+template + wants continue
		1,                 // depth
		5,                 // perPage (resolution default)
		-1,                // keyPerPage (root tuple)
		-1,                // keyPage (root tuple)
		"krateo-system",
	)

	wcKey, _ := wcKeyV.Load().(string)
	if wcKey == "" {
		t.Fatalf("resolve seam did not observe the content key on ctx (walker never reached the resolve)")
	}
	if got := store.Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
		t.Fatalf("#323 walker A5 RED: populateWidgetContentL1's post-readyz PutIfGen was NOT refused (refused_total %d->%d) — the walker capture (:460 wcGen0) must run BEFORE the page resolve; a wrong key (M1) or a capture-after-resolve (M2) resurrects here.", refusedBefore, got)
	}
	if _, hit := store.Get(wcKey); hit {
		t.Fatalf("#323 walker A5 RED: the DELETE-evicted widget-content cell was RESURRECTED under %q", wcKey)
	}
}
