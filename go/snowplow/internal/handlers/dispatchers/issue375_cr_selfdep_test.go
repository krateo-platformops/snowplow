// issue375_cr_selfdep_test.go — #375 (TL ruling): the dispatched CR's OWN self-dep.
//
// The customer handler reads the RESTAction/widget CR (fetchObjectFn) BEFORE the L1 key
// is known, and Records its self-dep only AFTER the accepted PutIfGen. Pre-fix the sink's
// startSeq came from WithL1KeyContext, too late to see an edit of the CR, and the
// self-dep was not in the sink at Put time. A user editing the CR during a COLD first-fill
// therefore had their self dirty-mark consumed as skipped_no_entry, and the old-spec body
// was Put with no remark. The fix: DepGenEpochNow() at handler entry (before the fetch)
// and WithL1KeyContextFromEpoch(…, self dk) at the key site.
//
// The arms drive the REAL restActionHandler.ServeHTTP with a REAL CR UPDATE through the
// dynamic client. The informer delivers it to OnObjectEvent while the resolve is in
// flight, i.e. inside [fetch, Put]. NEUTER: revert to WithL1KeyContext (no entry epoch,
// no self pre-declare) → the cold arm goes RED.

package dispatchers

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// h1BuildWatcherWithRA is h1BuildWatcher plus the RESTAction CR itself, served by a
// synced informer, and it returns the dynamic client so the arm can edit the CR for real.
func h1BuildWatcherWithRA(t *testing.T) dynamic.Interface {
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
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"},
		APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group}, Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "h1-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "h1-bind", UID: types.UID("uid-h1")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: h1User}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "h1-reader"},
		},
		h1RAUnstructured(),
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	_, syncCh := rw.EnsureResourceType(h1RAGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("restactions informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
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
	return dyn
}

// editRA performs a REAL UPDATE of the RESTAction CR and blocks until its dep event
// reached OnObjectEvent (the bump completed).
func editRA(t *testing.T, dyn dynamic.Interface, rev string) {
	t.Helper()
	seq0 := cache.DepEventSeqForTest()
	u, err := dyn.Resource(h1RAGVR).Namespace(h1NS).Get(context.Background(), h1RAName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get RA: %v", err)
	}
	u.SetLabels(map[string]string{"rev": rev})
	if _, err := dyn.Resource(h1RAGVR).Namespace(h1NS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update RA: %v", err)
	}
	for end := time.Now().Add(5 * time.Second); cache.DepEventSeqForTest() == seq0; time.Sleep(2 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("the RA UPDATE never reached OnObjectEvent")
		}
	}
}

func runSelfDepArm(t *testing.T, edit, warm bool) (remarks int, key string) {
	dyn := h1BuildWatcherWithRA(t)
	var mu sync.Mutex
	byKey := map[string]int{}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, _ string) { mu.Lock(); byKey[k]++; mu.Unlock() }))
	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: a live cacheable key is required; key=%q", key)
	}
	if warm {
		handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"old":true}`), Inputs: inputs})
		cache.Deps().Record(context.Background(), key, h1RAGVR, h1NS, h1RAName) // its self edge
	}
	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true }, // the fetch: CR@old
		func(ctx context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			if edit {
				editRA(t, dyn, "new") // the user edits the CR while the resolve is in flight
			}
			return resolved, nil // built from the OLD spec
		})
	defer restore()
	rec := httptest.NewRecorder()
	RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
	if rec.Code != 200 {
		t.Fatalf("dispatch: want 200, got %d %s", rec.Code, rec.Body.String())
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("setup: the dispatch did not Put its cell")
	}
	mu.Lock()
	defer mu.Unlock()
	return byKey[key], key
}

// COLD key, the CR is edited in [fetch, Put] → the Put remarks exactly once. RED pre-fix.
func TestIssue375_CRSelfDep_ColdKey_EditInWindow_Remarks(t *testing.T) {
	if got, _ := runSelfDepArm(t, true, false); got != 1 {
		t.Fatalf("#375 CR self-dep RED: the dispatched RESTAction was edited during a COLD first-fill resolve, but "+
			"its Put remarked %d times (want 1). The old-spec body stays with its self dirty-mark consumed while "+
			"non-resident; the entry epoch must precede the CR fetch and the self coordinate must be pre-declared", got)
	}
}

// Anti-amp control: no CR edit → 0 remarks.
func TestIssue375_CRSelfDep_NoEdit_ZeroRemarks(t *testing.T) {
	if got, _ := runSelfDepArm(t, false, false); got != 0 {
		t.Fatalf("#375 CR self-dep anti-amp RED: no edit, yet %d remarks", got)
	}
}

// Warm-key control: a WARM key is an L1 hit (no resolve, no Put), so the guard is not
// involved. An edit of its CR must still reach it through the real dirty-mark, landing
// while the key is RESIDENT so the refresher re-resolves it instead of skipping it.
func TestIssue375_CRSelfDep_WarmKey_EditCaughtByMark(t *testing.T) {
	dyn := h1BuildWatcherWithRA(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil {
		t.Fatalf("PRECONDITION: a live cacheable key is required")
	}
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"old":true}`), Inputs: inputs})
	cache.Deps().Record(context.Background(), key, h1RAGVR, h1NS, h1RAName) // its self edge
	var mu sync.Mutex
	residentMarks := 0
	cache.Deps().SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		if _, ok := handle.Get(k); ok && k == key {
			mu.Lock()
			residentMarks++
			mu.Unlock()
		}
	})
	editRA(t, dyn, "new")
	mu.Lock()
	defer mu.Unlock()
	if residentMarks == 0 {
		t.Fatalf("#375 CR self-dep warm control: the CR edit did not dirty-mark the RESIDENT warm key")
	}
}
