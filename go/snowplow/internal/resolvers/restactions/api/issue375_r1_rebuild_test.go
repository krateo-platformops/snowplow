// issue375_r1_rebuild_test.go — #375 R1 for the TYPE-LEVEL bump sources (TL ask).
//
// The type-level bump (schema relist / store repair / CRD add) fires BEFORE the
// replacement informer has synced. R1 still holds because of one invariant: while a GVR
// is mid-rebuild (registered but not yet synced), a resolve FALLS THROUGH to the
// apiserver and never serves from the informer. So a resolve with startSeq ≥ bump reads
// live data, never the torn-down or partial store. This arm pins that invariant on the
// real watcher. It performs a real teardown + re-register (RemoveResourceType +
// EnsureResourceType, the relistGVRForRepair sequence) and holds the replacement
// informer's LIST so it cannot sync. While held, the informer dispatch and the content
// serve must both decline. NEUTER: let Gate 6 (`!rw.IsSynced(gvr)` in
// dispatchViaInformer) serve an unsynced GVR → RED. If the invariant ever regresses,
// R1 for these sources silently breaks.

package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"

	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestIssue375_R1_MidRebuildGVR_FallsThroughNeverServesInformer(t *testing.T) {
	dyn := newWatcher375(t)
	rw := cache.Global()
	if rw == nil || !rw.IsServable(f1WidgetsGVR) {
		t.Fatalf("setup: widgets must be servable before the rebuild")
	}
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/" + f1WidgetsGVR.Group + "/" + f1WidgetsGVR.Version + "/" + f1WidgetsGVR.Resource,
		Verb: ptr.To(http.MethodGet),
	}}
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: u375}))
	if _, ok := dispatchViaInformer(cache.WithApistageContentResolve(ctx), call); !ok {
		t.Fatalf("setup: a synced GVR must be informer-served")
	}

	// Hold the replacement informer's LIST so the rebuild cannot sync.
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	dyn.(*dynamicfake.FakeDynamicClient).PrependReactor("list", f1WidgetsGVR.Resource,
		func(k8stesting.Action) (bool, runtime.Object, error) {
			<-release
			return false, nil, nil
		})
	// Production relists REMOVABLE (navigation-discovered, composition) GVRs, whose
	// informers are owned outright and rebuilt fresh on re-register; mark the group so,
	// else the shared factory hands back the old synced informer and nothing rebuilds.
	cache.AddNavigationDiscoveredGroup(f1WidgetsGVR.Group)
	t.Cleanup(cache.ResetNavigationDiscoveredGroupsForTest)
	rw.RemoveResourceType(f1WidgetsGVR)              // teardown (relistGVRForRepair :812)
	_, syncCh := rw.EnsureResourceType(f1WidgetsGVR) // re-register, unsynced (:813)
	// <- the type-level bump fires HERE in production (:814), before sync.

	for i := 0; i < 20; i++ {
		if rw.IsServable(f1WidgetsGVR) {
			t.Fatalf("setup: the GVR became servable while its LIST is held")
		}
		if raw, ok := dispatchViaInformer(cache.WithApistageContentResolve(ctx), call); ok {
			t.Fatalf("#375 R1 RED: a mid-rebuild (registered, NOT synced) GVR was SERVED from the informer "+
				"(%d bytes) — the type-level bump fires before sync, so R1 needs every such read to fall "+
				"through to the apiserver", len(raw))
		}
		if _, served, ok := apistageContentServe(ctx, cache.ResolvedCache(), call, false); ok || served {
			t.Fatalf("#375 R1 RED: the content serve answered (ok=%v served=%v) for a mid-rebuild GVR", ok, served)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("setup: the replacement informer did not sync after release")
	}
}
