// viewed_coalesce_799_test.go — #799 fix 1, arm F1 (architect-623's ruling,
// #799 issuecomment-6289159553).
//
// dev-765h's pair at 6bd30396 (RUN): 50K-list fills in flight went from max 1 to
// 4. A viewed-tier refresh runs customer-triggered (the cap bypass), so its fill
// was admitted at the debt class, and #772's pending-flight coalescing was for
// the background class only: under churn every viewed refresh, refused by the
// in-flight fill, led its OWN concurrent re-fill of the same list.
//
// F1: a fill of the list is in flight; K changes land one by one, each followed
// by a viewed-tier refresh reader. At most ONE more dispatch (the shared
// pending fill), every reader served a body at least as new as the change
// before it entered. Mutation: drop the viewed term from the coalesce rule →
// K more dispatches.
//
// F2 is the existing B5 arm (apistage_content_stamp_765_test.go): a
// customer-triggered refresh WITHOUT the viewed marker (K5, #547 demand) still
// never waits on a pending fill.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func viewed799Ctx() context.Context {
	return cache.WithViewedRefreshForTest(cache.WithCustomerTriggeredRefresh(stamp765RefreshCtx()))
}

func TestViewed799_F1_ViewedRefreshesShareOnePendingFillUnderChurn(t *testing.T) {
	const k = 6
	dyn := newFill710Watcher(t, 5)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	stamp765Prime(t, store)
	if got := ContentFillClassForTest(viewed799Ctx()); got != "debt" {
		t.Fatalf("PRECONDITION: a viewed refresh is admitted at %q, want debt (the cap bypass is kept)", got)
	}

	calls, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(viewed799Ctx(), store)
	stamp765WaitClosed(t, "the first viewed refresh never dispatched", read[0])

	readers := make([]chan fill710Served, k)
	for i := 0; i < k; i++ {
		stamp765Change(t, dyn, 3+i)
		readers[i] = stamp765Serve(viewed799Ctx(), store)
		if !fill710WaitFor(10*time.Second, func() bool { return len(readers[i]) == 1 || stamp765Refused() == uint64(i+1) }) {
			t.Fatalf("PRECONDITION: viewed reader %d neither returned nor was refused by the in-flight fill", i)
		}
	}
	release[0]()
	stamp765Recv(t, "first viewed refresh", lead)
	for i, ch := range readers {
		if got := fill710ManagedOf(t, stamp765Recv(t, "viewed reader", ch).v, stamp765Item); got < 3+i {
			t.Fatalf("LOSSY: viewed reader %d entered after managed=%d and was served managed=%d", i, 3+i, got)
		}
	}
	if got := calls.Load(); got > 2 {
		t.Fatalf("F1 RED (#799): %d viewed refreshes, each after one more change, dispatched %d fills of the shared list — "+
			"want at most 2 (the in-flight fill plus ONE pending fill they share)", k, got)
	}
}
