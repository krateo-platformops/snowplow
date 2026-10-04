// identity_key_parity_golden_test.go — #449 key-parity golden at the MINT SITES.
//
// The cache-package golden pins ComputeKey over hand-built inputs. This one pins
// the keys the production builders mint from a real published RBAC snapshot, so
// moving the identity derivation behind rbac.IdentityClassOf / SetIdentity is
// proven not to move any key a live pod holds:
//
//   - restactions / widgets: dispatchCacheLookupKey (customer, seed, subscription)
//   - widgetContent:         dispatchWidgetContentKey and widgetContentL1Key
//   - raFullList:            apiref.RAFullListKey (seedFullListRAKey)
//   - seed memo class:       the "<binding-set digest>/<sub-gen>" string the
//     SeedResolveMemo key folds (apiref rbacClassForMemo)
//
// apistage is minted only by the api package's contentKeyInputs (zero identity);
// the cache-package golden pins its three mint shapes.
//
// Values captured on origin/main c7533929, before the #449 refactor.
package dispatchers

import (
	"context"
	"strconv"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var goldenRAGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// goldenPublish starts a real ResourceWatcher over objs, publishes its RBAC
// snapshot as the global, and zeroes the sub-generations.
func goldenPublish(t *testing.T, objs []runtime.Object) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	g := "rbac.authorization.k8s.io"
	listKinds := map[schema.GroupVersionResource]string{
		goldenRAGVR: "RESTActionList",
		{Group: g, Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: g, Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: g, Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: g, Version: "v1", Resource: "roles"}:               "RoleList",
	}
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...))
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		cache.ResetRBACSubGenForTest()
	})
}

func goldenParityWorld() []runtime.Object {
	g := "rbac.authorization.k8s.io"
	return []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "golden-ra-reader"},
			Rules: []rbacv1.PolicyRule{
				{Verbs: []string{"get", "list"}, APIGroups: []string{goldenRAGVR.Group}, Resources: []string{goldenRAGVR.Resource}},
				{Verbs: []string{"get", "list"}, APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"panels"}},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "golden-ra-bind", UID: types.UID("uid-golden-crb")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: "golden-devs"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "golden-ra-reader"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "golden-cm"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "golden-cm", UID: types.UID("uid-golden-rb")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: g, Name: "golden-alice"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "golden-cm"},
		},
	}
}

func TestIdentityKeyParityGolden_MintSites(t *testing.T) {
	goldenPublish(t, goldenParityWorld())

	type id struct {
		name   string
		user   string
		groups []string
	}
	ids := []id{
		{"alice", "golden-alice", []string{"golden-devs"}},
		{"bob", "golden-bob", []string{"golden-devs", "golden-other"}},
	}
	want := map[string]string{
		"alice/restactions":   "2ec3e09b07db113c59a2d9c7f69238dbcab43e1b49eb7c8df8f345a4637f4345",
		"alice/widgets":       "46da150101fbb9160d978af84e834520a7fa4c3290dabc3133721ee11ccfdadb",
		"alice/widgetContent": "50b3a463dc3d989865661becc82c568c3c24e915e2dfb7975fd146dd20a2a7d4",
		"alice/wcL1":          "59bf31c0efbcd5ab18e479676f4897cea7c102f6fc53ac3bc39b731f72b0495c",
		"alice/raFullList":    "fb561f455f7152c5eaf0a5bc1d5ebbf57adb78ad587a55dcfe9dc6f8bfe0f008",
		"alice/memoClass":     "6bcd1fccfa19fd654748c456b20268d012ae380da6d10e0f2d792a53f299ad8d/0",
		"bob/restactions":     "2318dea6ce3a421435872a5a7219805059c0f2d7f1c75756d62b65f6c5bd3ebf",
		"bob/widgets":         "db985f52b166ad28c5828f47a9bc33d0722e2b4b5f1c75c3d5a7facb01d70b5a",
		"bob/widgetContent":   "50b3a463dc3d989865661becc82c568c3c24e915e2dfb7975fd146dd20a2a7d4",
		"bob/wcL1":            "59bf31c0efbcd5ab18e479676f4897cea7c102f6fc53ac3bc39b731f72b0495c",
		"bob/raFullList":      "27de087fd9289003a7a4584995d9c7ed8ab6aa93e022996246017666a1d69d83",
		"bob/memoClass":       "9684aa26127bb24d89059c8b995f69913583ec8922b7dde8ead65da6e6a69ede/0",
	}
	extras := map[string]any{"tenant": "acme"}
	got := map[string]string{}
	for _, u := range ids {
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: u.user, Groups: u.groups}))
		k, _, _ := dispatchCacheLookupKey(ctx, "restactions",
			goldenRAGVR.Group, goldenRAGVR.Version, goldenRAGVR.Resource, "krateo-system", "golden-ra", -1, -1, extras)
		got[u.name+"/restactions"] = k
		k, _, _ = dispatchCacheLookupKey(ctx, "widgets",
			"widgets.templates.krateo.io", "v1beta1", "panels", "krateo-system", "golden-panel", 10, 1, extras)
		got[u.name+"/widgets"] = k
		k, _, _ = dispatchWidgetContentKey(ctx,
			"widgets.templates.krateo.io", "v1beta1", "panels", "krateo-system", "golden-panel", 10, 1, extras)
		got[u.name+"/widgetContent"] = k
		k, _ = widgetContentL1Key(h1WidgetGVR, "krateo-system", "golden-panel", 10, 1)
		got[u.name+"/wcL1"] = k
		_, k, ok := apiref.RAFullListKey(ctx, goldenRAGVR, "krateo-system", "golden-ra", extras)
		if !ok {
			t.Fatalf("%s: RAFullListKey not minted", u.name)
		}
		got[u.name+"/raFullList"] = k
		got[u.name+"/memoClass"] = rbac.SubjectBindingSetDigest(u.user, u.groups) + "/" +
			strconv.FormatUint(cache.RBACSubGenForSubject(u.user, rbac.WithAuthenticatedGroup(u.groups)), 10)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s: key moved: got %q want %q", name, got[name], w)
		}
	}
}
