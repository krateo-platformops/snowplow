// inline443_cluster_list_test.go — #443 part 2, TestS443_Inline/c2: the
// cluster-list collapse's async populate under an inert (dry-run) resolve.
//
// populateClusterListCellAsync detaches its goroutine to context.Background(),
// which would drop the inert flag, so (d) gates it at the head, BEFORE the
// inflight LoadOrStore. The arm drives the REAL async function and observes
// the populate seam: inert → the populate never runs and no inflight marker
// is left; the control (same call without the flag) runs the populate once.
package api

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesapi "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestS443_Inline_c2_ClusterListAsyncPopulate(t *testing.T) {
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		time.Sleep(50 * time.Millisecond)
		ResetClusterListAsyncStateForTest()
	})
	var runs atomic.Int64
	ran := make(chan struct{}, 4)
	prev := populateClusterListCellSyncFn
	populateClusterListCellSyncFn = func(context.Context, *slog.Logger, *templatesapi.API, schema.GroupVersionResource, string, httpcall.RequestOptions, *cache.ResolvedCacheStore) bool {
		runs.Add(1)
		ran <- struct{}{}
		return true
	}
	t.Cleanup(func() { populateClusterListCellSyncFn = prev })

	customer := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "inline443", Groups: []string{"g"}}))
	apiCall := &templatesapi.API{Name: "inline443-cluster-list"}
	const cell = "inline443-cluster-list-cell"

	populateClusterListCellAsync(cache.WithInert(customer), clusterListLogger(t), apiCall, endpoints.Endpoint{}, f1WidgetsGVR, cell,
		httpcall.RequestOptions{}, cache.ResolvedCache())
	select {
	case <-ran:
		t.Fatal("an inert (dry-run) resolve spawned the cluster-list async populate")
	case <-time.After(200 * time.Millisecond):
	}
	if _, inflight := clusterListInflightCells.Load(cell); inflight {
		t.Fatal("an inert (dry-run) resolve left an inflight marker for the cell")
	}

	// Control: the same call without the flag runs the populate exactly once.
	populateClusterListCellAsync(customer, clusterListLogger(t), apiCall, endpoints.Endpoint{}, f1WidgetsGVR, cell,
		httpcall.RequestOptions{}, cache.ResolvedCache())
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("CONTROL: a non-inert call did not run the populate — the arm cannot fail")
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("populate runs=%d, want exactly 1 (the control)", n)
	}
}
