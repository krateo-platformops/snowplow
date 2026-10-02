package dispatchers

// reseed_widening_test.go — #258 WIDENING arm. A subject that had NO access gains
// it mid-test through a REAL binding ADD on the watcher's dynamic client. The
// real path then runs: informer ADD → bindings-by-GVR delta (widening source) →
// pending sub-gen bump → rebuildRBACSnapshot publish → flushPendingSubGenBumps →
// notifyRBACShift (the cache→engine hook). The rotated set the REAL flush hands
// the hook must make the newly-authorized subject appear in the reseed set
// (enumerateRotatedResidentTargets: resident harvester units × the LIVE binding
// index), and reseeding it must warm that subject's serve key.
//
// This is the evidence for dropping the separate per-subject "widening walk":
// for a RESIDENT unit, a widening subject is reached by the snapshot re-key
// because the identity side of the enumeration is read from the live binding
// index at reseed time, not from a boot-time cohort list. (Units the harvester
// has never seen — a runtime-new GVR — stay scopeKindGVRDiscovered's domain.)
//
// RED controls:
//   - pre-grant: the same enumeration over a rotated set that names the subject
//     yields NO target for it (no access yet). So the post-grant target comes
//     from the live index, not from rotated-set membership alone.
//   - the subject's serve key is cold before the reseed and warm after it.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const s258WideningUser = "carol" // starts with NO binding; gains one mid-test

// s258WideningSink collects every RotatedSubjectSet the cache hands the RBAC-shift
// hook. The hook registry dedups by function pointer, so ONE package-level
// closure is registered once (registerS258WideningHook) and writes here.
var s258WideningSink struct {
	mu   sync.Mutex
	sets []cache.RotatedSubjectSet
}

var s258WideningHookOnce sync.Once

func registerS258WideningHook() {
	s258WideningHookOnce.Do(func() {
		cache.RegisterRBACShiftHook(func(rs cache.RotatedSubjectSet) {
			s258WideningSink.mu.Lock()
			s258WideningSink.sets = append(s258WideningSink.sets, rs)
			s258WideningSink.mu.Unlock()
		})
	})
}

func s258ResetSink() {
	s258WideningSink.mu.Lock()
	s258WideningSink.sets = nil
	s258WideningSink.mu.Unlock()
}

// s258RotatedFor returns the first captured set in which user rotated.
func s258RotatedFor(user string) (cache.RotatedSubjectSet, bool) {
	s258WideningSink.mu.Lock()
	defer s258WideningSink.mu.Unlock()
	for _, rs := range s258WideningSink.sets {
		if rs.Rotated(user, nil) {
			return rs, true
		}
	}
	return cache.RotatedSubjectSet{}, false
}

var (
	s258CRBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	s258CRGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	s258RBGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	s258RGVR   = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

// s258BuildWatcher publishes a ClusterRole granting get/list on the RA + widget
// GVRs, bound ONLY to alice. carol has no binding. Returns the dynamic client so
// the test can ADD carol's binding through the informer path.
func s258BuildWatcher(t *testing.T) (dynamic.Interface, *cache.ResourceWatcher) {
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
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "s258-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "list"},
				APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group},
				Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "s258-alice", UID: types.UID("uid-s258-alice")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: a1Alice}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s258-reader"},
		},
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
	// The live binding index (what EnumeratePrewarmTargetsForGVR reads, and what
	// gates the informer delta → pending sub-gen bump). Phase 1 builds it in prod.
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

func s258IdentityCtx(user string) context.Context {
	return withCohortSeedContext(context.Background(), seedTarget{Username: user}, endpoints.Endpoint{}, nil)
}

func s258TargetsFor(reqs []reseedRequest, user string) int {
	n := 0
	for _, r := range reqs {
		if r.identity.Username == user {
			n++
		}
	}
	return n
}

func TestS258_WideningSubjectAppearsInReseedSet(t *testing.T) {
	registerS258WideningHook()
	dyn, rw := s258BuildWatcher(t)
	stubWidgetResolve(t)
	s258ResetSink()

	// A RESIDENT unit: the widget is in the nav harvester snapshot (the boot walk
	// reached it). No restaction units.
	navHarv := newNavWidgetHarvester()
	e := reseedWidgetEntry()
	navHarv.harvestNavWidget(e.W, e.GVR, e.PerPage, e.Page, e.KeyPerPage, e.KeyPage)
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	// RED control (pre-grant): a rotated set naming carol (driven through the
	// same hook chain) yields NO target for her — she has no access yet.
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: s258WideningUser, Widening: true}})
	forged, ok := s258RotatedFor(s258WideningUser)
	if !ok {
		t.Fatal("precondition: the hook sink did not capture the forged rotated set (hook not registered?)")
	}
	if n := s258TargetsFor(enumerateRotatedResidentTargets(context.Background(), deps, forged), s258WideningUser); n != 0 {
		t.Fatalf("pre-grant: carol has no binding, yet the reseed set holds %d target(s) for her — the "+
			"enumeration is not reading the live binding index", n)
	}
	if n := s258TargetsFor(enumerateRotatedResidentTargets(context.Background(), deps, forged), a1Alice); n != 0 {
		t.Fatalf("precision: alice did not rotate, yet the reseed set holds %d target(s) for her", n)
	}
	// POSITIVE CONTROL (non-vacuity): alice HAS access, so a set naming her
	// yields exactly her one resident target — the enumeration is live.
	s258ResetSink()
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})
	aliceSet, ok := s258RotatedFor(a1Alice)
	if !ok {
		t.Fatal("precondition: the hook sink did not capture alice's forged set")
	}
	if n := s258TargetsFor(enumerateRotatedResidentTargets(context.Background(), deps, aliceSet), a1Alice); n != 1 {
		t.Fatalf("VACUOUS GUARD: a rotated set naming the authorized alice yields %d target(s), want 1 — the "+
			"binding index or the harvester fixture is not live, so the pre-grant zero proves nothing", n)
	}
	s258ResetSink()

	// THE WIDENING EVENT: a real binding ADD for carol on the watcher's client.
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "s258-carol", UID: types.UID("uid-s258-carol")},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: s258WideningUser}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s258-reader"},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(crb)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, err := dyn.Resource(s258CRBGVR).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create carol's binding: %v", err)
	}

	// Wait for the REAL flush to hand the hook a rotated set containing carol.
	// The delta schedules its own rebuild; RebuildRBACSnapshotForTest is a
	// belt-and-braces publish (the flush is idempotent — an empty pending set is
	// a no-op).
	var rotated cache.RotatedSubjectSet
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs, ok := s258RotatedFor(s258WideningUser); ok {
			rotated = rs
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the real binding ADD never produced an RBAC-shift flush naming carol (informer → delta → " +
				"pending bump → publish → notifyRBACShift)")
		}
		cache.RebuildRBACSnapshotForTest(rw)
		time.Sleep(20 * time.Millisecond)
	}
	if !rotated.Widening(s258WideningUser, nil) {
		t.Fatal("a binding ADD must be classified WIDENING in the rotated set")
	}

	// THE ARM: carol now appears in the reseed set for the resident widget.
	reqs := enumerateRotatedResidentTargets(context.Background(), deps, rotated)
	if n := s258TargetsFor(reqs, s258WideningUser); n != 1 {
		t.Fatalf("post-grant: the reseed set holds %d target(s) for carol, want exactly 1 (the resident "+
			"widget); reqs=%d", n, len(reqs))
	}
	if n := s258TargetsFor(reqs, a1Alice); n != 0 {
		t.Fatalf("precision: alice did not rotate, yet the reseed set holds %d target(s) for her", n)
	}

	// COVERAGE: reseeding warms carol's serve key (cold before, warm after).
	key, handle, _ := reseedWidgetKey(t, s258IdentityCtx(s258WideningUser), e)
	if _, ok := handle.Get(key); ok {
		t.Fatal("precondition: carol's widgets serve key must be cold before the reseed")
	}
	if re := reseedTargets(context.Background(), deps, reqs); len(re) != 0 {
		t.Fatalf("reseed left %d target(s) to re-enqueue", len(re))
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("the widening reseed must warm carol's serve key %q", key)
	}
}
