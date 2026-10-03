package dispatchers

// rotated_effective_groups_436_test.go — #436 (folded into #262).
//
// Seed and customer keys fold system:authenticated into RBACSubGen
// (dispatchCacheLookupKey + #424 WithAuthenticatedGroup). The #258 reseed
// decided which cohort representatives a flush rotated with
// RotatedSubjectSet.Rotated over the representative's RAW groups, which never
// name system:authenticated. So a change to a system:authenticated binding
// rotated EVERY cohort's key but reseeded only the {"", [system:authenticated]}
// representative — every other cohort's first navigation went cold.
//
//   TestS436_RotatedFoldsEffectiveGroups — the predicate itself: a flush that
//       rotated only Group/system:authenticated rotates a group representative
//       and a user identity.
//   TestS436_SysAuthBindingChange_ReseedsEveryCohort — end to end through the
//       real informer → flush → hook: a ClusterRoleBinding to
//       system:authenticated is added; every cohort representative is in the
//       reseed set, and each cohort's first navigation afterwards is an L1 HIT.
//
// Self-contained on purpose (only pre-#262 seams), so the same file runs RED on
// the pre-fix tree.

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	s436Devs = "s436-devs"
	s436Ops  = "s436-ops"
)

func TestS436_RotatedFoldsEffectiveGroups(t *testing.T) {
	registerS258WideningHook(t)
	s258ResetSink()
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "Group", Name: "system:authenticated"}})
	s258WideningSink.mu.Lock()
	if len(s258WideningSink.sets) != 1 {
		s258WideningSink.mu.Unlock()
		t.Fatal("precondition: one captured set")
	}
	rs := s258WideningSink.sets[0]
	s258WideningSink.mu.Unlock()
	if !rs.Rotated("", []string{s436Devs}) {
		t.Fatal("#436 RED: a system:authenticated rotation must rotate the group representative {\"\", [devs]} — " +
			"its key folds system:authenticated")
	}
	if !rs.Rotated("someone", nil) {
		t.Fatal("#436 RED: a system:authenticated rotation must rotate every authenticated identity")
	}
}

func s436BuildWatcher(t *testing.T) (*dynamicfake.FakeDynamicClient, *cache.ResourceWatcher) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:     "RESTActionList",
		h1WidgetGVR: "PanelList",
		s258CRBGVR:  "ClusterRoleBindingList",
		s258CRGVR:   "ClusterRoleList",
		s258RBGVR:   "RoleBindingList",
		s258RGVR:    "RoleList",
	}
	crb := func(name, group string) *rbacv1.ClusterRoleBinding {
		return &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s436-reader"},
		}
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "s436-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs: []string{"get", "list"}, APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group},
				Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource},
			}},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "s436-cm"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		crb("s436-devs", s436Devs),
		crb("s436-ops", s436Ops),
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1WidgetGVR, h1RAGVR})
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
	return dyn, rw
}

func TestS436_SysAuthBindingChange_ReseedsEveryCohort(t *testing.T) {
	registerS258WideningHook(t)
	dyn, rw := s436BuildWatcher(t)
	stubWidgetResolve(t)
	s258ResetSink()

	navHarv := newNavWidgetHarvester()
	e := reseedWidgetEntry()
	navHarv.harvestNavWidget(e.W, e.GVR, e.PerPage, e.Page, e.KeyPerPage, e.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	// THE CHANGE: a binding whose subject is system:authenticated.
	sys := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "s436-sysauth", UID: types.UID("uid-s436-sysauth")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:authenticated"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s436-cm"},
	}
	devsCust := s436CustomerKey(t, s436Devs, e)
	opsCust := s436CustomerKey(t, s436Ops, e)

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(sys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dyn.Resource(s258CRBGVR).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var rotated cache.RotatedSubjectSet
	deadline := time.Now().Add(10 * time.Second)
	for rotated.Len() == 0 {
		s258WideningSink.mu.Lock()
		for _, rs := range s258WideningSink.sets {
			if rs.Rotated("x", []string{"system:authenticated"}) {
				rotated = rs
			}
		}
		s258WideningSink.mu.Unlock()
		if rotated.Len() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the system:authenticated binding ADD never produced an RBAC-shift flush")
		}
		cache.RebuildRBACSnapshotForTest(rw)
		time.Sleep(20 * time.Millisecond)
	}
	if s436CustomerKey(t, s436Devs, e) == devsCust || s436CustomerKey(t, s436Ops, e) == opsCust {
		t.Fatal("PRECONDITION: a system:authenticated binding change must rotate every cohort's key")
	}

	reqs := enumerateRotatedResidentTargets(context.Background(), deps, rotated)
	in := map[string]bool{}
	for _, r := range reqs {
		if r.identity.Username == "" && len(r.identity.Groups) == 1 {
			in[r.identity.Groups[0]] = true
		}
	}
	if !in[s436Devs] || !in[s436Ops] {
		t.Fatalf("#436 RED: the reseed set after a system:authenticated binding change holds devs=%v ops=%v; "+
			"EVERY cohort representative's key rotated and must be reseeded", in[s436Devs], in[s436Ops])
	}
	if re := reseedTargets(context.Background(), deps, reqs); len(re) != 0 {
		t.Fatalf("reseed left %d to re-enqueue", len(re))
	}
	for _, g := range []string{s436Devs, s436Ops} {
		k := s436CustomerKey(t, g, e)
		if _, ok := cache.ResolvedCache().GetNoTouch(k); !ok {
			t.Fatalf("#436 RED: a %s member's first navigation after the change must HIT", g)
		}
	}
}

func s436CustomerKey(t *testing.T, group string, e navWidgetEntry) string {
	t.Helper()
	k, _, _ := reseedWidgetKey(t, l436Ctx(group), e)
	return k
}

// l436Ctx is a /call identity: a group-only member (JWT username + groups).
func l436Ctx(group string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "member-of-" + group, Groups: []string{group}}))
}
