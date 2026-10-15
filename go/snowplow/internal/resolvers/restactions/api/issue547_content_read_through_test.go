// issue547_content_read_through_test.go — #547 F3, the identity-free content
// substrate arm (owner ruling 3, #547 comment 6140460857).
//
// An apistage CONTENT cell is shared by every requester and feeds resolves
// whose result may itself be cached. The customer's identity cannot rescue it
// (its refresh runs one identity-free K8s call; ErrContentNotServable is not an
// identity problem), so a KNOWN-STALE content cell is READ THROUGH — the reader
// takes the existing MISS fall-through (one K8s call) — and is never served
// stale, to a customer OR to a background resolve. That is what keeps a stale
// substrate from being baked into a new widget cell.
//
// THE DRIVE. The content cell is filled and holds a dep edge on its object; a
// REAL informer UPDATE of that object dirty-marks it; the real refresher
// re-dispatches it and the refresh is NOT SERVABLE at that moment (the typed
// ErrContentNotServable the production content refresh returns pre-sync /
// metadata-only), across the whole budget, until the drop. The informer then
// serves the new object — the shape of an informer that recovered — and the
// arm reads the cell through the production reader, apistageContentServe.
//
// RED on 9c233de9: the drop erases the stamp and the reader serves the
// pre-UPDATE envelope from the cell.
package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func k547FSA(managed int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": r1Name},
		"status":     map[string]any{"managed": r1ManagedList(managed)},
	}}
}

// newK547ContentWatcher is newR1RefreshBypassWatcher whose live object starts
// EQUAL to the cell (stale count) and whose dynamic client is returned, so the
// arm can UPDATE it for real.
func newK547ContentWatcher(t *testing.T) dynamic.Interface {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "k547-fsa-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fullstackapps"}, Verbs: []string{"get", "list"}},
		},
	}
	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: r1Ns, Name: "k547-fsa-binding"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: r1User}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "k547-fsa-reader"},
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		r1FsaGVR: "FullStackAppList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, role, binding, k547FSA(r1StaleManaged))
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(r1FsaGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("fsa informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

func TestIssue547_F3_KnownStaleContentCellIsReadThroughNeverServedStale(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	dyn := newK547ContentWatcher(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache nil")
	}
	cache.ResetRefresherForTest()

	// The content cell, filled consistently with the live object, plus the dep
	// edge its own fill records (apistage.go's content edge).
	key := putR1StaleContentEntry(store)
	cache.Deps().Record(context.Background(), key, r1FsaGVR, r1Ns, r1Name)

	// The refresh is not servable while the informer is (in the live shape)
	// pre-sync / metadata-only: the typed error the production content refresh
	// returns, across the whole budget.
	var refreshes atomic.Int64
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(context.Context, string, cache.ResolvedKeyInputs) error {
		refreshes.Add(1)
		return cache.ErrContentNotServable
	})
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest() })

	// The portal write.
	d0 := cache.RefresherStatsByStat()["dirty_ended_unfresh_dropped"]
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), k547FSA(r1FreshManaged), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	for end := time.Now().Add(30 * time.Second); cache.RefresherStatsByStat()["dirty_ended_unfresh_dropped"] == d0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("PRECONDITION: the content refresh never reached the drop point (refreshes=%d)", refreshes.Load())
		}
	}
	// The informer delivered the UPDATE before the dirty-mark it fanned out
	// (the store is written before the handlers run), so it now holds the new
	// object — what a read-through returns.

	reads := []struct {
		name string
		ctx  context.Context
	}{
		{"customer", r1Ctx()},
		// A background resolve reads the substrate too (the refresher's own
		// whole-RA re-resolve, the seed): it must not bake the stale cell into a
		// fresh-looking widget cell either.
		{"background", cache.WithBackgroundResolve(r1Ctx())},
	}
	for _, r := range reads {
		v, served, ok, _ := apistageContentServe(r.ctx, store, r1GetCall(), false)
		if !ok || !served {
			t.Fatalf("%s: the read was not served (ok=%v served=%v)", r.name, ok, served)
		}
		if got := managedCountOf(t, v); got != r1FreshManaged {
			t.Fatalf("F3 RED (%s): the reader served the pre-UPDATE content cell (%d managed, want %d read through). "+
				"The content refresh ended without a Put, the stamp was erased, and the stale substrate is served "+
				"as input to whatever resolve reads it (#547)", r.name, got, r1FreshManaged)
		}
	}
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatalf("F3: the content cell was evicted — read-through never deletes")
	}
}
