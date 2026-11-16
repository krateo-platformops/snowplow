package dispatchers

// issue1483_seed_get_before_fetch_test.go — #1483 (b): the RESTAction cohort seed
// decides its unit's `get` from the ref's coordinates BEFORE it fetches the CR, as
// the widget leg does.
//
//   D  the 057 shape: an identity that may `list` configmaps in team-a (so the
//      enumeration can target it) and has no `get` on the RESTAction. The seed
//      makes NO fetch, reaches no Put, and returns nil (the empty_binding skip).
//   A  an identity that may `get` it: one fetch, the Put tail reached, under the
//      key the seed derived before #1483 from the FETCHED object's coordinates,
//      so the seeded cell set does not change for an allowed class.
//
// Hermetic: the fetch and the resolve+Put tail are the seams the #113 arms use.

import (
	"context"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	s1483Denied  = "s1483-lists-configmaps-in-team-a"
	s1483Allowed = "s1483-gets-restactions"
)

// watcher1483Seed publishes an RBAC snapshot in which s1483Denied may list
// configmaps in team-a only and s1483Allowed may get restactions everywhere.
func watcher1483Seed(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		restActionGVR:                  "RESTActionList",
		rbacGVR("clusterrolebindings"): "ClusterRoleBindingList",
		rbacGVR("clusterroles"):        "ClusterRoleList",
		rbacGVR("rolebindings"):        "RoleBindingList",
		rbacGVR("roles"):               "RoleList",
	}
	ref := rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole"}
	raGetter, cmLister := ref, ref
	raGetter.Name, cmLister.Name = "ra-getter-1483", "cm-lister-1483"
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: raGetter.Name}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get"}, APIGroups: []string{restActionGVR.Group}, Resources: []string{restActionGVR.Resource}}}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: cmLister.Name}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}}},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "gets-restactions-1483", UID: types.UID("uid-1483-allowed")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: s1483Allowed}},
			RoleRef:    raGetter,
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "lists-configmaps-1483", UID: types.UID("uid-1483-denied")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: s1483Denied}},
			RoleRef:    cmLister,
		},
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(restActionGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

// seed1483 runs the REAL seedOneRestaction for user, with the fetch and the
// resolve+Put tail seamed, and reports the fetches made and the key the tail got
// ("" when it was not reached).
func seed1483(t *testing.T, user string) (fetches int, tailKey string) {
	t.Helper()
	origGet, origPut := seedObjectsGetFn, seedRestactionResolveAndPutFn
	t.Cleanup(func() { seedObjectsGetFn, seedRestactionResolveAndPutFn = origGet, origPut })
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		fetches++
		return fetchedRestaction(false)
	}
	seedRestactionResolveAndPutFn = func(
		_, _ context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, key string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		tailKey = key
		return nil
	}
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "hub-spoke-ra", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	for _, mode := range []seedScopeMode{seedModeKeepwarm} {
		if err := seedOneRestaction(seedCohortCtx(user), ref, "krateo-system", mode); err != nil {
			t.Fatalf("seedOneRestaction(%v): %v", mode, err)
		}
	}
	return fetches, tailKey
}

func TestIssue1483_D_ASeedThatMayNotGetTheRESTActionNeverFetchesIt(t *testing.T) {
	watcher1483Seed(t)
	fetches, tailKey := seed1483(t, s1483Denied)
	if fetches != 0 {
		t.Errorf("RED: the seed fetched the RESTAction %d time(s) for an identity that may not get it, want 0", fetches)
	}
	if tailKey != "" {
		t.Errorf("RED: the seed reached the Put tail for an identity that may not get the RESTAction (key %q)", tailKey)
	}
}

func TestIssue1483_A_ASeedThatMayGetTheRESTActionSeedsTheSameCell(t *testing.T) {
	watcher1483Seed(t)
	fetches, tailKey := seed1483(t, s1483Allowed)
	if fetches != 1 || tailKey == "" {
		t.Fatalf("the allowed identity must be fetched once and reach the Put tail: fetches=%d key=%q", fetches, tailKey)
	}
	// The key the seed derived before #1483, from the fetched object's
	// coordinates: the cell is the same one.
	got := fetchedRestaction(false)
	want, _, _ := dispatchCacheLookupKey(seedCohortCtx(s1483Allowed), "restactions",
		got.GVR.Group, got.GVR.Version, got.GVR.Resource,
		got.Unstructured.GetNamespace(), got.Unstructured.GetName(), -1, -1, nil)
	if want == "" || tailKey != want {
		t.Errorf("RED: the seeded cell's key changed: %q, want %q (the key from the fetched object)", tailKey, want)
	}
}
