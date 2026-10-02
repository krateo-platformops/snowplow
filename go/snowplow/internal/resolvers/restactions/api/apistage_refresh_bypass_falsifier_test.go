// apistage_refresh_bypass_falsifier_test.go — R1 Layer 1 hermetic falsifier
// for the refresh-context content-bypass (the content-shield cure).
//
// THE DEFECT (R1 §3, the content-shield): during a refresher whole-RA
// re-resolve triggered by GVR X, the RA's getComposition-style content stage
// is served from its OWN apistage-content HIT — the STORED (possibly stale)
// bytes — instead of being re-dispatched fresh. So the re-resolve consumes a
// stale sibling snapshot and re-stores a degraded result.
//
// THE FIX (R1 Layer 1, Option A): apistageContentServe force-MISSES a content
// HIT whose own dep GVR == the refresh TRIGGER GVR (WithRefreshTriggerGVR on
// the refresh ctx), so the whole-RA re-resolve reads the FRESH input. UNIFORM
// dep-edge equality — no per-resource/path special-case.
//
// DISCRIMINATING ARMS:
//   - RED (the defect, structurally still present on the REQUEST path): a
//     content HIT served WITHOUT the refresh marker returns the STALE stored
//     shape — asserted POSITIVELY (the collapsed 1-element snapshot), per
//     pm-warmup's F-3 condition (fails for the content-shield reason, not
//     merely "≠ fresh"). This is ALSO the negative control proving the
//     request path keeps stale-while-revalidate (no force-miss off-refresh).
//   - GREEN (the cure): the SAME HIT served WITH WithRefreshTriggerGVR(==gvr)
//     force-misses → re-dispatches fresh → returns the FRESH 2-element shape.
//   - NEGATIVE (F-4 dep-edge equality, not "any refresh"): WITH a refresh
//     marker for a DIFFERENT GVR, the HIT is still served stale (the bypass
//     fires ONLY on dep-edge equality, never blanket-on-refresh — the
//     0.30.185 amplification guard).

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// r1FsaGVR is a generic composition-style GET-by-name content GVR (the
// getComposition shape). Customer-style — no carve-out (feedback_no_special_cases).
var r1FsaGVR = schema.GroupVersionResource{
	Group:    "composition.krateo.io",
	Version:  "v1",
	Resource: "fullstackapps",
}

const (
	r1Ns   = "demo-system"
	r1Name = "fsa-y2"
	r1User = "r1-broad-user"
)

// r1StaleManagedCount / r1FreshManagedCount model the 1→26 transition: the
// stored content entry is the STALE 1-managed snapshot; the live informer
// holds the FRESH object with 2 managed (a smaller-but-distinct count keeps
// the test fast; the shape is what matters).
const (
	r1StaleManaged = 1
	r1FreshManaged = 2
)

// newR1RefreshBypassWatcher builds a cache-on watcher whose fake client holds
// the FRESH fsa-y2 object (so a forced-miss re-dispatch GET-by-name returns
// fresh) and grants r1User get on fullstackapps in demo-system.
func newR1RefreshBypassWatcher(t *testing.T) *cache.ResourceWatcher {
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
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "r1-fsa-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fullstackapps"}, Verbs: []string{"get", "list"}},
		},
	}
	binding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: r1Ns, Name: "r1-fsa-binding"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: r1User}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "r1-fsa-reader"},
	}

	// The FRESH live object (2 managed) — what a forced-miss re-dispatch reads.
	freshFSA := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": r1Name},
		"status":     map[string]any{"managed": r1ManagedList(r1FreshManaged)},
	}}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		r1FsaGVR: "FullStackAppList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, role, binding, freshFSA)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	// Register + sync the fsa informer so a forced-miss GET-by-name is servable
	// and returns the fresh object.
	rw.EnsureResourceType(r1FsaGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

func r1ManagedList(n int) []any {
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, map[string]any{"name": "child", "kind": "ConfigMap"})
	}
	return out
}

// putR1StaleContentEntry seeds the apistage GET-by-name content entry with the
// STALE 1-managed snapshot — the content-shield's stored bytes.
func putR1StaleContentEntry(store *cache.ResolvedCacheStore) string {
	key := cache.ComputeKey(contentKeyInputs(r1FsaGVR, r1Ns, r1Name))
	stale := map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": r1Name},
		"status":     map[string]any{"managed": r1ManagedList(r1StaleManaged)},
	}
	raw, _ := json.Marshal(stale)
	store.Put(key, &cache.ResolvedEntry{
		RawJSON: raw,
		Inputs:  ptrTo(contentKeyInputs(r1FsaGVR, r1Ns, r1Name)),
	})
	return key
}

// managedCountOf extracts .status.managed length from an apistageContentServe
// value. For a GET-by-name the gate returns the DECODED envelope (a
// map[string]any via json.Unmarshal, apistage.go gateContentEnvelope:290) —
// NOT raw bytes. Handle both shapes defensively.
func managedCountOf(t *testing.T, v any) int {
	t.Helper()
	var obj map[string]any
	switch typed := v.(type) {
	case map[string]any:
		obj = typed
	case []byte:
		if err := json.Unmarshal(typed, &obj); err != nil {
			t.Fatalf("unmarshal served bytes: %v (raw=%s)", err, typed)
		}
	default:
		t.Fatalf("served value is %T, want map[string]any or []byte", v)
	}
	status, _ := obj["status"].(map[string]any)
	managed, _ := status["managed"].([]any)
	return len(managed)
}

func r1GetCall() httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps/" + r1Name,
			Verb: ptr.To(http.MethodGet),
		},
	}
}

func r1Ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: r1User}),
	)
}

// TestFalsifierR1_RefreshTriggerForcesMissOfStaleContent is the discriminating
// 3-arm falsifier.
func TestFalsifierR1_RefreshTriggerForcesMissOfStaleContent(t *testing.T) {
	_ = newR1RefreshBypassWatcher(t) // installs the cache.Global watcher + fresh fsa object
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}

	// --- RED arm + negative control: REQUEST path (no refresh marker) serves
	// the STALE content HIT. Asserted POSITIVELY: the served value is the
	// 1-managed collapsed snapshot (the content-shield, F-3 condition).
	putR1StaleContentEntry(store)
	v, served, ok := apistageContentServe(r1Ctx(), store, r1GetCall(), false)
	if !ok || !served {
		t.Fatalf("request-path content HIT must serve (ok=%v served=%v)", ok, served)
	}
	if got := managedCountOf(t, v); got != r1StaleManaged {
		t.Fatalf("RED arm: request-path HIT must serve the STALE %d-managed snapshot "+
			"(stale-while-revalidate on the request path); got %d managed", r1StaleManaged, got)
	}

	// --- NEGATIVE control: refresh marker for a DIFFERENT GVR must NOT
	// force-miss (dep-edge inequality) — still serves stale.
	otherGVR := schema.GroupVersionResource{Group: "x.io", Version: "v1", Resource: "others"}
	vNeg, servedNeg, okNeg := apistageContentServe(
		cache.WithRefreshTriggerGVR(r1Ctx(), otherGVR), store, r1GetCall(), false)
	if !okNeg || !servedNeg {
		t.Fatalf("different-GVR refresh HIT must still serve (ok=%v served=%v)", okNeg, servedNeg)
	}
	if got := managedCountOf(t, vNeg); got != r1StaleManaged {
		t.Fatalf("NEGATIVE control: a refresh triggered by a DIFFERENT GVR must NOT force-miss "+
			"(F-4 dep-edge equality, not blanket-on-refresh); want stale %d, got %d",
			r1StaleManaged, got)
	}

	// --- GREEN arm: refresh marker for THE SAME GVR force-misses → fresh.
	vFresh, servedFresh, okFresh := apistageContentServe(
		cache.WithRefreshTriggerGVR(r1Ctx(), r1FsaGVR), store, r1GetCall(), false)
	if !okFresh || !servedFresh {
		t.Fatalf("same-GVR refresh re-dispatch must serve fresh (ok=%v served=%v)", okFresh, servedFresh)
	}
	if got := managedCountOf(t, vFresh); got != r1FreshManaged {
		t.Fatalf("GREEN arm: a refresh triggered by the entry's OWN GVR must FORCE-MISS and "+
			"re-dispatch FRESH (%d managed from the live informer), NOT serve the stale "+
			"content HIT; got %d managed — the content-shield bypass did not fire",
			r1FreshManaged, got)
	}

	// And the forced miss RE-STORED the fresh content: a subsequent
	// request-path HIT now serves fresh (the re-resolve corrected the cell).
	vAfter, _, okAfter := apistageContentServe(r1Ctx(), store, r1GetCall(), false)
	if !okAfter {
		t.Fatalf("post-bypass request-path HIT must serve")
	}
	if got := managedCountOf(t, vAfter); got != r1FreshManaged {
		t.Fatalf("after the forced miss re-Put the fresh content, the request-path HIT must "+
			"serve fresh %d; got %d", r1FreshManaged, got)
	}
}
