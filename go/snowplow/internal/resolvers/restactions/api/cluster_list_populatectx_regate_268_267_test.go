//go:build falsifier_268_267

// cluster_list_populatectx_regate_268_267_test.go — #268/#269 Part 2 drift-guard,
// the 5th producer row (TL Option A). cluster_list's populateCtx is the ONE
// SA-transport producer that is not a directly-callable builder: it is constructed
// inline in populateClusterListCellAsync's goroutine (existing tests bypass it by
// calling populateClusterListCellSync directly). This arm drives the REAL async path
// and captures the REAL populateCtx via the populateClusterListCellSyncFn seam (no
// hand-rolled mirror), then asserts the MustRegateSADial invariant for it:
// ServesUnnarrowed || BackgroundResolve.
//
// populateCtx carries an SA credential only when the CUSTOMER ctx already does — the
// internal-driver-nested case (a refresher/prewarm re-resolve that triggers a
// cluster-list collapse). cluster_list re-attaches the SA endpoint + rest.Config from
// customerCtx but builds populateCtx from a FRESH context.Background(), so the
// customer identity is DETACHED — populateCtx has no UserInfo and is ServesUnnarrowed
// via that (the identity-free Class-3 cluster-scope by-name collapse). This arm puts a
// REAL non-SA identity on customerCtx and asserts populateCtx is STILL ServesUnnarrowed
// — so if a future change carried the customer identity into populateCtx, the
// classification would flip !ServesUnnarrowed and this arm trips (that flip is exactly
// what the census cannot see, because the WithInternal* call count is unchanged).

package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	templatesapi "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// TestClusterListPopulateCtx_SatisfiesRegateInvariant drives the real async
// populate and asserts populateCtx satisfies ServesUnnarrowed || BackgroundResolve
// (and is genuinely SA-credentialed, so the assertion is non-vacuous).
func TestClusterListPopulateCtx_SatisfiesRegateInvariant(t *testing.T) {
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		time.Sleep(50 * time.Millisecond)
		ResetClusterListAsyncStateForTest()
	})

	// Capture the real populateCtx by seaming populateClusterListCellSync. The stub
	// returns true (skip the real populate — this arm is about the ctx, not the LIST).
	captured := make(chan context.Context, 1)
	prev := populateClusterListCellSyncFn
	populateClusterListCellSyncFn = func(ctx context.Context, _ *slog.Logger, _ *templatesapi.API, _ schema.GroupVersionResource, _ string, _ httpcall.RequestOptions, _ *cache.ResolvedCacheStore) bool {
		select {
		case captured <- ctx:
		default:
		}
		return true
	}
	t.Cleanup(func() { populateClusterListCellSyncFn = prev })

	// customerCtx models the internal-driver-nested case: a REAL non-SA identity
	// (as the refresher's representative would carry) PLUS the SA transport
	// (WithInternalEndpoint/WithInternalRESTConfig) that cluster_list re-attaches.
	ep := endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-tok-driftguard"}
	rc := &rest.Config{Host: ep.ServerURL}
	customerCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cluster-list-customer", Groups: []string{"tenant-a"}}),
	)
	customerCtx = cache.WithInternalEndpoint(customerCtx, &ep)
	customerCtx = cache.WithInternalRESTConfig(customerCtx, rc)

	apiCall := &templatesapi.API{Name: "drift-guard-cluster-list"}
	gvr := f1WidgetsGVR
	contentKey := "drift-guard-cluster-list-cell"

	populateClusterListCellAsync(customerCtx, clusterListLogger(t), apiCall, ep, gvr, contentKey, httpcall.RequestOptions{}, cache.ResolvedCache())

	var populateCtx context.Context
	select {
	case populateCtx = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatalf("populate goroutine did not run within 5s (inflight dedup / semaphore not reset?)")
	}

	// Non-vacuity: the SA credential MUST have been re-attached, else populateCtx is
	// not an SA producer and the invariant assertion proves nothing.
	_, hasEP := cache.InternalEndpointFromContext(populateCtx)
	_, hasRC := cache.InternalRESTConfigFromContext(populateCtx)
	if !hasEP && !hasRC {
		t.Fatalf("populateCtx carries no SA credential (re-attach did not fire) — the invariant would be vacuous")
	}

	serves := rbac.ServesUnnarrowed(populateCtx)
	bg := cache.BackgroundResolveFromContext(populateCtx)
	t.Logf("cluster_list populateCtx: ServesUnnarrowed=%v BackgroundResolve=%v", serves, bg)

	if !serves && !bg {
		t.Fatalf("DRIFT (#268/#269 Part 2): cluster_list populateCtx attaches an SA credential but is NEITHER ServesUnnarrowed NOR BackgroundResolve.\n"+
			"  MustRegateSADial would go false for it, so an SA-served cluster-scope read would NOT be re-gated. Restore the identity-free construction (build populateCtx from context.Background(), do not carry the customer UserInfo).")
	}
	if !serves {
		t.Errorf("cluster_list populateCtx classification is ServesUnnarrowed (via no-UserInfo), but ServesUnnarrowed(ctx)=false (invariant held only via BackgroundResolve=%v). Re-verify the classification and the census.", bg)
	}

	// Sensitivity: the customer identity MUST have been detached. If a change carried
	// the customer's (non-SA) UserInfo into populateCtx, ServesUnnarrowed would flip
	// false above; this explicit check names the cause.
	if _, err := xcontext.UserInfo(populateCtx); err == nil {
		t.Errorf("populateCtx carries a UserInfo — the customer identity was NOT detached; a non-SA identity here flips ServesUnnarrowed false and would leave the cluster-scope serve un-re-gated")
	}
}
