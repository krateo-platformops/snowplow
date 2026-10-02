// key424_ra_full_list_test.go — #424 raFullList Put TOCTOU arm.
//
// The raKey is minted (seedFullListRAKey) BEFORE the unpaginated resolve and the
// cell is written AFTER it. A grant to the requester that lands DURING the
// resolve makes the body the requester's NEW class's while raKey still names the
// old class — shared with a co-member who lacks the grant. The Put must be
// declined; the requester is still served their own rows.
package apiref

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func k424Watcher(t *testing.T, seed ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, f6RBACListKinds(), seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

func TestKey424_RAFullListPut_TOCTOU_GrantMidResolve(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	// Only the RA grant (Group:portal-423) + bob's unrelated RB: alice and bob
	// start in ONE class; alice's cm-reader RB is granted mid-resolve.
	fx := k423RAFixture()
	var seed []runtime.Object
	for _, o := range fx {
		if rb, ok := o.(*rbacv1.RoleBinding); ok && rb.Name == "alice-cm-423" {
			continue
		}
		seed = append(seed, o)
	}
	dyn := k424Watcher(t, seed...)

	const ns, name = "krateo-system", "k424-ra-full-list"
	aliceCtx := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	bobCtx := f6CtxWithUser(t, "bob-423", []string{"portal-423"})
	_, aKey, okA := seedFullListRAKey(aliceCtx, gvr(), ns, name, nil)
	_, bKey, okB := seedFullListRAKey(bobCtx, gvr(), ns, name, nil)
	// The arm asserts on the cell under alice's PRE-grant raKey: every identity in
	// her pre-grant class (any portal-423 member with no extra binding) derives it,
	// so a post-grant body there is the leak whether or not such a member has
	// called yet. bob (who holds an unrelated RB) is logged for context only.
	if !okA || !okB {
		t.Fatalf("PRE: both users must derive a raKey")
	}

	var granted atomic.Bool
	var calls atomic.Int64
	rows := k423PerUserRows(t, &calls)
	resolve := func(ctx context.Context, perPage, page int) (map[string]any, error) {
		if granted.CompareAndSwap(false, true) {
			rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
			for _, o := range fx {
				if rb, ok := o.(*rbacv1.RoleBinding); ok && rb.Name == "alice-cm-423" {
					rb.TypeMeta = metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"}
					m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
					if _, err := dyn.Resource(rbGVR).Namespace("x").Create(context.Background(),
						&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
						t.Fatalf("grant: %v", err)
					}
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if ok, _, _ := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{Username: "alice-423", Groups: []string{"portal-423"},
					Verb: "get", Resource: "configmaps", Namespace: "x", Name: "y"}); ok {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		return rows(ctx, perPage, page)
	}

	got, served, err := raFullListServe(aliceCtx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil || !served {
		t.Fatalf("SETUP: alice must be served; served=%v err=%v", served, err)
	}
	if b, _ := json.Marshal(got); !strings.Contains(string(b), k423Sentinel) {
		t.Fatalf("SETUP: alice's own (post-grant) page must carry the sentinel row; got %s", b)
	}
	if _, aNow, _ := seedFullListRAKey(aliceCtx, gvr(), ns, name, nil); aNow == aKey {
		t.Fatalf("PRE: the mid-resolve grant must move alice's raKey (else the arm cannot fail)")
	}
	t.Logf("bob shares alice's pre-grant raKey: %v", bKey == aKey)

	// The defect: alice's post-grant full list stored under her PRE-grant raKey.
	if e, hit := cache.ResolvedCache().Get(aKey); hit && strings.Contains(string(e.RawJSON), k423Sentinel) {
		t.Fatalf("TOCTOU LEAK (raFullList): alice's post-grant full list was written under her pre-grant raKey " +
			"(the class every pre-grant co-member derives)")
	}
}
