package apiref

// #435 arms, adopted from reviewer-424's REVIEW-435 probes (RED on main
// 22bcbf0d): the raFullList customer Put path vs RBAC moves that leave the
// binding set where it was. Pre-#435 the raKey folded no RBACSubGen; the fix
// folds it (seedFullListRAKey), so the key rotates on such a move and
// raKeyClassCurrent sees it at both Put sites.
//
//   A  grant-then-revoke INSIDE alice's customer first-sight resolve. alice and
//      carol are one class (group-only portal-423). The body read under alice's
//      transient grant: is it written under the class key, and served to carol?
//   B  role-rules REVOKE between two customer requests (no mid-resolve race):
//      the Role bound to Group:portal-423 loses `get configmaps`. carol, a
//      member, then calls: is she served the pre-revoke cell? (restactions /
//      widgets keys fold RBACSubGen and rotate here; raFullList keys do not.)

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
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	r435RB   = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	r435Role = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

func r435Create(t *testing.T, dyn *dynamicfake.FakeDynamicClient, g schema.GroupVersionResource, ns string, obj runtime.Object) {
	t.Helper()
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if _, err := dyn.Resource(g).Namespace(ns).Create(context.Background(), &unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", g.Resource, err)
	}
}

func r435Can(user string) bool {
	ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{Username: user, Groups: []string{"portal-423"},
		Verb: "get", Resource: "configmaps", Namespace: "x", Name: "y"})
	return ok
}

func r435Wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// base fixture: the RA grant to Group:portal-423 only (no cm grant at all).
func r435Base(t *testing.T) *dynamicfake.FakeDynamicClient {
	cache.ResetResolvedCacheForTest()
	var seed []runtime.Object
	for _, o := range k423RAFixture() {
		switch v := o.(type) {
		case *rbacv1.RoleBinding:
			continue // no seeded RoleBindings
		case *rbacv1.Role:
			if v.Name == "cm-reader-423" {
				continue // created at runtime when needed
			}
		}
		seed = append(seed, o)
	}
	dyn := k424Watcher(t, seed...)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{gvr(), {Version: "v1", Resource: "configmaps"}})
	return dyn
}

func r435Sentinel(v any) bool {
	b, _ := json.Marshal(v)
	return strings.Contains(string(b), k423Sentinel)
}

func TestIssue435_A_GrantThenRevokeInsideCustomerResolve(t *testing.T) {
	dyn := r435Base(t)
	r435Create(t, dyn, r435Role, "x", &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "cm-reader-423"},
		Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}}})
	const ns, name = "krateo-system", "r435a-ra"
	alice := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	carol := f6CtxWithUser(t, "carol-423", []string{"portal-423"})
	aIn, aKey, _ := seedFullListRAKey(alice, gvr(), ns, name, nil)
	_, cKey, _ := seedFullListRAKey(carol, gvr(), ns, name, nil)
	if aKey != cKey || r435Can("alice-423") || r435Can("carol-423") {
		t.Fatalf("PRE: alice and carol must share one raKey and both be denied")
	}
	var armed atomic.Bool
	var calls atomic.Int64
	rows := k423PerUserRows(t, &calls)
	resolve := func(ctx context.Context, perPage, page int) (map[string]any, error) {
		if !armed.CompareAndSwap(false, true) {
			return rows(ctx, perPage, page)
		}
		r435Create(t, dyn, r435RB, "x", &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "alice-transient", UID: types.UID("uid-alice-transient")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "alice-423"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-reader-423"}})
		r435Wait(t, "alice granted", func() bool { return r435Can("alice-423") })
		out, err := rows(ctx, perPage, page) // read under the transient grant
		if derr := dyn.Resource(r435RB).Namespace("x").Delete(context.Background(), "alice-transient", metav1.DeleteOptions{}); derr != nil {
			t.Fatalf("revoke: %v", derr)
		}
		r435Wait(t, "alice revoked", func() bool { return !r435Can("alice-423") })
		// The discriminating shape: alice's BINDING SET is back to the minted one,
		// so a binding-set-only check cannot see the move.
		r435Wait(t, "alice's binding set back to the minted one", func() bool {
			return rbac.SubjectBindingSetDigest("alice-423", []string{"portal-423"}) == aIn.SubjectBindingSet
		})
		return out, err
	}
	// Prime: an ordinary first sight records the (raKey x shape) sliceable
	// verdict and stores the cell; then the cell leaves the store, so alice's
	// next call takes the REPOPULATE branch (one unpaginated resolve, no
	// re-verify) — the production shape after an eviction or a TTL expiry.
	armed.Store(true)
	if _, ok, perr := raFullListServe(alice, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, resolve); perr != nil || !ok {
		t.Fatalf("PRIME: alice first sight failed ok=%v err=%v", ok, perr)
	}
	if _, ok := cache.ResolvedCache().Get(aKey); !ok {
		t.Fatalf("PRIME: the first sight must store the class cell")
	}
	cache.ResolvedCache().DeleteForTest(aKey)
	armed.Store(false)
	declined0 := RAKeyClassDriftDeclinedForTest()
	got, served, err := raFullListServe(alice, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, resolve)
	t.Logf("alice served=%v err=%v ownPageHasSentinel=%v", served, err, r435Sentinel(got))
	e, stored := cache.ResolvedCache().Get(aKey)
	t.Logf("class cell stored=%v sentinel=%v", stored, stored && strings.Contains(string(e.RawJSON), k423Sentinel))
	if stored && strings.Contains(string(e.RawJSON), k423Sentinel) {
		t.Fatalf("#435 LEAK (A): the row alice read under a transient grant was written under the class cell carol derives")
	}
	if _, ck, _ := seedFullListRAKey(carol, gvr(), ns, name, nil); ck != cKey {
		t.Fatalf("PRE: carol (never granted) must still derive the class key")
	}
	cgot, _, cerr := raFullListServe(carol, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows)
	if cerr != nil {
		t.Fatalf("carol serve: %v", cerr)
	}
	if RAKeyClassDriftDeclinedForTest() <= declined0 {
		t.Fatalf("the write must have been declined by raKeyClassCurrent (the #424 Put-site guard)")
	}
	if r435Sentinel(cgot) {
		t.Fatalf("#435 LEAK (A): carol — never granted — was served the row alice read under a transient grant, from the class cell")
	}
}

func TestIssue435_B_RoleRulesRevokeBetweenRequests(t *testing.T) {
	dyn := r435Base(t)
	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "cm-reader-423"},
		Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}}}
	r435Create(t, dyn, r435Role, "x", role)
	r435Create(t, dyn, r435RB, "x", &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "portal-cm", UID: types.UID("uid-portal-cm")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "portal-423"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-reader-423"}})
	r435Wait(t, "group granted", func() bool { return r435Can("alice-423") && r435Can("carol-423") })

	const ns, name = "krateo-system", "r435b-ra"
	alice := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	carol := f6CtxWithUser(t, "carol-423", []string{"portal-423"})
	var calls atomic.Int64
	rows := k423PerUserRows(t, &calls)
	got, _, err := raFullListServe(alice, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows)
	if err != nil || !r435Sentinel(got) {
		t.Fatalf("SETUP: alice (granted) must see the row; err=%v", err)
	}
	_, keyBefore, _ := seedFullListRAKey(carol, gvr(), ns, name, nil)
	sgBefore := cache.RBACSubGenForSubject("carol-423", rbac.WithAuthenticatedGroup([]string{"portal-423"}))

	// Role-rules revoke: the Role keeps its name (binding set unchanged), loses the rule.
	role.Rules = []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"secrets"}}}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(role)
	if _, err := dyn.Resource(r435Role).Namespace("x").Update(context.Background(), &unstructured.Unstructured{Object: m}, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update role: %v", err)
	}
	r435Wait(t, "group revoked", func() bool { return !r435Can("carol-423") })
	// The deferred sub-gen bump lands on the snapshot publish; give it the same
	// bound as the revoke (no fatal here: on a key that never rotates the leak
	// assertion below is the finding).
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if _, k, _ := seedFullListRAKey(carol, gvr(), ns, name, nil); k != keyBefore {
			break
		}
	}
	_, keyAfter, _ := seedFullListRAKey(carol, gvr(), ns, name, nil)
	sgAfter := cache.RBACSubGenForSubject("carol-423", rbac.WithAuthenticatedGroup([]string{"portal-423"}))
	t.Logf("raKey moved=%v (restactions/widgets key would move: subgen %d -> %d)", keyAfter != keyBefore, sgBefore, sgAfter)

	before := calls.Load()
	cgot, _, cerr := raFullListServe(carol, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, rows)
	if cerr != nil {
		t.Fatalf("carol serve: %v", cerr)
	}
	t.Logf("carol resolves=%d (0 = served from the pre-revoke cell)", calls.Load()-before)
	if r435Sentinel(cgot) {
		t.Fatalf("#435 LEAK (B): after a role-rules REVOKE, carol was served the pre-revoke row from the raFullList " +
			"cell — the raKey did not rotate (no sub-gen) and no guard ran on the hit path")
	}
}

// TestIssue435_RoleEditRotatesRAFullListKey — a rules edit on a Role referenced
// by the class's (group) binding rotates the raFullList key for every member,
// through the single key builder; an edit to a Role nobody in the class is bound
// to does not (the class keeps sharing one warm cell).
func TestIssue435_RoleEditRotatesRAFullListKey(t *testing.T) {
	dyn := r435Base(t)
	newRole := func(name string) *rbacv1.Role {
		return &rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: name},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}}}
	}
	bound, unbound := newRole("cm-reader-423"), newRole("nobody-435")
	r435Create(t, dyn, r435Role, "x", bound)
	r435Create(t, dyn, r435Role, "x", unbound)
	r435Create(t, dyn, r435RB, "x", &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "portal-cm", UID: types.UID("uid-portal-cm-435r")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "portal-423"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-reader-423"}})
	r435Wait(t, "group granted", func() bool { return r435Can("carol-423") })

	const ns, name = "krateo-system", "r435r-ra"
	alice := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	carol := f6CtxWithUser(t, "carol-423", []string{"portal-423"})
	key := func(ctx context.Context) string {
		_, k, ok := seedFullListRAKey(ctx, gvr(), ns, name, nil)
		if !ok {
			t.Fatalf("no raKey")
		}
		return k
	}
	// Let the setup's deferred sub-gen bumps settle before the baseline.
	r435Wait(t, "setup settled", func() bool {
		k := key(carol)
		time.Sleep(100 * time.Millisecond)
		return key(carol) == k
	})
	k0 := key(carol)
	if key(alice) != k0 {
		t.Fatalf("PRE: alice and carol (one binding set, no personal RBAC history) must share one raKey")
	}
	update := func(r *rbacv1.Role, res string) {
		r.Rules = []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{res}}}
		m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(r)
		if _, err := dyn.Resource(r435Role).Namespace("x").Update(context.Background(),
			&unstructured.Unstructured{Object: m}, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update role: %v", err)
		}
	}

	// Negative control: an unreferenced Role's edit must NOT rotate the key.
	update(unbound, "secrets")
	time.Sleep(500 * time.Millisecond)
	if key(carol) != k0 {
		t.Fatalf("OVER-ROTATION: editing a Role no class member is bound to rotated the raFullList key")
	}

	// The referenced Role loses its rule: the key rotates, for both members alike.
	update(bound, "secrets")
	r435Wait(t, "group revoked", func() bool { return !r435Can("carol-423") })
	deadline := time.Now().Add(5 * time.Second)
	for key(carol) == k0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if key(carol) == k0 {
		t.Fatalf("NO ROTATION: a rules edit on the Role the class is bound to left the raFullList key unchanged " +
			"(the key folds no RBAC sub-gen) — members keep hitting the pre-edit cell until the TTL")
	}
	if key(alice) != key(carol) {
		t.Fatalf("after the rotation alice and carol must still share one raKey")
	}
}
