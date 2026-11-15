// issue1449_dangling_binding_role_index_test.go — #1449: a binding whose role is
// absent when the bindings-by-role index is built must still follow that role.
//
// A rules change reaches an identity's cache keys only through the
// sub-generation of the subjects of the bindings that name the role
// (cache/bindings_by_gvr_delta.go onRoleRulesChanged, over the index's byRole
// map). The index BUILD skips a binding whose role is absent from the snapshot
// (cache/bindings_by_gvr.go BuildBindingsByGVRIndex), so the role's later
// create, and every later edit of it, find no binding and bump no subject. The
// delta path (a binding ADDED after the build) does record such a binding under
// the role it names.
//
// THE ARMS (TestIssue1449_BindingToAbsentRoleFollowsThatRole). One ordinary user
// is the only subject of one binding. The role that binding names does not
// exist. Then the role is created, granting `list` on the target; the user
// reads and the answer is stored; the role's rule is removed; the user reads
// again.
//
//	at-build/*                     the binding is in the snapshot the index is
//	                               built over, in each binding/role kind pair.
//	at-build/…/read-before-grant   the same, the user having read (and stored
//	                               the denied view) before the role exists.
//	after-build/*                  CONTROL: the binding is created after the
//	                               build (the delta path).
//	after-build-then-rebuild/*     the binding is created after the first build
//	                               and the index is then built again, as the
//	                               prewarm boot pass does after the readiness
//	                               settle (prewarm_engine_boot.go step 3).
//	aggregate-present-at-build     CONTROL: the role exists at build with no
//	                               rules and an aggregationRule; its rules are
//	                               then written (the fake has no aggregation
//	                               controller: the test performs its write).
//
// REAL BOUNDARY (the #423 ps harness, as #1439): a real ResourceWatcher over
// the fake apiserver, the real RBAC snapshot rebuild and its sub-generation
// flush, the real bindings index build and delta hooks, the real
// dispatchCacheLookupKey, the real restActionHandler.ServeHTTP (Put and hit
// path) and the real evaluator. The only seam is the read of the RESTAction CR.
//
// ASSERTED at each step on the user's class and key, and ON THE BYTES SERVED.
package dispatchers

import (
	"context"
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

const (
	i1449RoleName    = "i1449-late-reader"
	i1449BindingName = "i1449-late-binding"
	i1449AggLabel    = "i1449-aggregate"
)

var (
	i1449CRGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	i1449CRBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
)

type i1449When int

const (
	i1449AtBuild i1449When = iota
	i1449AfterBuild
	i1449AfterBuildThenRebuild
)

type i1449Arm struct {
	name        string
	bindingKind string // RoleBinding | ClusterRoleBinding
	roleKind    string // Role | ClusterRole
	when        i1449When
	// aggregate: the role exists at build, with an aggregationRule and no rules.
	aggregate bool
	// readBeforeGrant: the user reads before the role exists.
	readBeforeGrant bool
}

type i1449Env struct {
	t        *testing.T
	a        psArm
	arm      i1449Arm
	dyn      *dynamicfake.FakeDynamicClient
	srvURL   string
	counters map[string]*atomic.Int64
	cr       *unstructured.Unstructured
	ctx      context.Context
}

func i1449Unstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("to unstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: m}
}

func (arm i1449Arm) binding() runtime.Object {
	subj := []rbacv1.Subject{i1439UserSubject()}
	ref := rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: arm.roleKind, Name: i1449RoleName}
	if arm.bindingKind == "ClusterRoleBinding" {
		return &rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: i1449BindingName, UID: types.UID("uid-crb-1449-late")},
			Subjects:   subj, RoleRef: ref,
		}
	}
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: i1449BindingName, UID: types.UID("uid-rb-1449-late")},
		Subjects:   subj, RoleRef: ref,
	}
}

func (arm i1449Arm) role(rules []rbacv1.PolicyRule) runtime.Object {
	if arm.roleKind == "ClusterRole" {
		cr := &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: i1449RoleName},
			Rules:      rules,
		}
		if arm.aggregate {
			cr.AggregationRule = &rbacv1.AggregationRule{ClusterRoleSelectors: []metav1.LabelSelector{
				{MatchLabels: map[string]string{i1449AggLabel: "true"}}}}
		}
		return cr
	}
	return &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: i1449RoleName},
		Rules:      rules,
	}
}

func i1449GrantRules(a psArm) []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{a.target.Resource}}}
}

// i1449RevokedRules grants nothing on the target.
var i1449RevokedRules = []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"pods"}}}

func i1449Setup(t *testing.T, arm i1449Arm) *i1449Env {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	var world []runtime.Object
	if arm.when == i1449AtBuild {
		world = append(world, arm.binding())
	}
	if arm.aggregate {
		world = append(world, arm.role(nil))
	}
	dyn := psBuildWatcher(t, a, world...)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetPendingSubGenBumpsForTest)

	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	return &i1449Env{t: t, a: a, arm: arm, dyn: dyn, srvURL: srv.URL, counters: counters, cr: k423ListCR(a),
		ctx: i1439Ctx(psCarol, []string{psGroup})}
}

func (e *i1449Env) class() cache.IdentityClass { return i1439Class(psCarol, []string{psGroup}) }

func (e *i1449Env) canList() bool {
	e.t.Helper()
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: psCarol, Groups: []string{psGroup}, Verb: "list", Resource: e.a.target.Resource, Namespace: psTargetNS,
	})
	if err != nil {
		e.t.Fatalf("EvaluateRBAC: %v", err)
	}
	return ok
}

func (e *i1449Env) settle(what string, cond func() bool) {
	e.t.Helper()
	if !i444Wait(10*time.Second, cond) {
		e.t.Fatalf("SETUP: timed out waiting for %s", what)
	}
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		e.t.Fatalf("SETUP: RBAC rebuild did not quiesce after %s: %v", what, err)
	}
}

func (e *i1449Env) roleClient() interface {
	Create(context.Context, *unstructured.Unstructured, metav1.CreateOptions, ...string) (*unstructured.Unstructured, error)
	Update(context.Context, *unstructured.Unstructured, metav1.UpdateOptions, ...string) (*unstructured.Unstructured, error)
	Get(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error)
} {
	if e.arm.roleKind == "ClusterRole" {
		return e.dyn.Resource(i1449CRGVR)
	}
	return e.dyn.Resource(icsRoleGVR).Namespace(psTargetNS)
}

// createBinding creates the binding through the fake apiserver (the delta path).
func (e *i1449Env) createBinding() {
	e.t.Helper()
	before := e.class().SubjectBindingSet
	u := i1449Unstructured(e.t, e.arm.binding())
	var err error
	if e.arm.bindingKind == "ClusterRoleBinding" {
		_, err = e.dyn.Resource(i1449CRBGVR).Create(context.Background(), u, metav1.CreateOptions{})
	} else {
		_, err = e.dyn.Resource(k424RBGVR).Namespace(psTargetNS).Create(context.Background(), u, metav1.CreateOptions{})
	}
	if err != nil {
		e.t.Fatalf("create binding: %v", err)
	}
	e.settle("the binding", func() bool { return e.class().SubjectBindingSet != before })
}

// grant makes the role grant `list` on the target: a CREATE of the role, or,
// for the aggregate arm, the write of its rules.
func (e *i1449Env) grant() {
	e.t.Helper()
	if e.arm.aggregate {
		// The contributing role appears; the aggregation controller then writes
		// the union into the aggregate's rules. The fake has no such
		// controller, so the test performs that write.
		contrib := &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: i1449RoleName + "-contributor", Labels: map[string]string{i1449AggLabel: "true"}},
			Rules:      i1449GrantRules(e.a),
		}
		if _, err := e.dyn.Resource(i1449CRGVR).Create(context.Background(), i1449Unstructured(e.t, contrib), metav1.CreateOptions{}); err != nil {
			e.t.Fatalf("create contributing role: %v", err)
		}
		e.setRules(i1449GrantRules(e.a))
	} else if _, err := e.roleClient().Create(context.Background(), i1449Unstructured(e.t, e.arm.role(i1449GrantRules(e.a))), metav1.CreateOptions{}); err != nil {
		e.t.Fatalf("create role: %v", err)
	}
	e.settle("the grant", e.canList)
}

func (e *i1449Env) setRules(rules []rbacv1.PolicyRule) {
	e.t.Helper()
	if _, err := e.roleClient().Update(context.Background(), i1449Unstructured(e.t, e.arm.role(rules)), metav1.UpdateOptions{}); err != nil {
		e.t.Fatalf("update role: %v", err)
	}
}

func (e *i1449Env) revoke() {
	e.t.Helper()
	e.setRules(i1449RevokedRules)
	e.settle("the rule removal", func() bool { return !e.canList() })
}

type i1449Read struct {
	row         bool
	dials, hits int64
	body        string
}

func (e *i1449Env) read() i1449Read {
	e.t.Helper()
	dials := e.counters[psCarol].Load()
	hits := cache.ResolvedCache().Stats().HitTotal
	rec := psServeCR(e.t, e.cr, e.srvURL, e.ctx)
	return i1449Read{
		row:   psHasSentinel(rec.Body.Bytes()),
		dials: e.counters[psCarol].Load() - dials,
		hits:  int64(cache.ResolvedCache().Stats().HitTotal - hits),
		body:  psTrunc(rec.Body.String(), 200),
	}
}

func TestIssue1449_BindingToAbsentRoleFollowsThatRole(t *testing.T) {
	for _, arm := range []i1449Arm{
		{name: "at-build/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", when: i1449AtBuild},
		{name: "at-build/rolebinding-to-role/read-before-grant", bindingKind: "RoleBinding", roleKind: "Role", when: i1449AtBuild, readBeforeGrant: true},
		{name: "at-build/rolebinding-to-clusterrole", bindingKind: "RoleBinding", roleKind: "ClusterRole", when: i1449AtBuild},
		{name: "at-build/clusterrolebinding-to-clusterrole", bindingKind: "ClusterRoleBinding", roleKind: "ClusterRole", when: i1449AtBuild},
		{name: "after-build/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", when: i1449AfterBuild},
		{name: "after-build/clusterrolebinding-to-clusterrole", bindingKind: "ClusterRoleBinding", roleKind: "ClusterRole", when: i1449AfterBuild},
		{name: "after-build-then-rebuild/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", when: i1449AfterBuildThenRebuild},
		{name: "aggregate-present-at-build/clusterrolebinding", bindingKind: "ClusterRoleBinding", roleKind: "ClusterRole", when: i1449AtBuild, aggregate: true},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			e := i1449Setup(t, arm)
			a := e.a
			bystander := i1439Class(psDave, []string{psGroup}) // no binding names this identity's subjects but the group's
			detector := cache.ResolvedCache().Stats().ServeMissedRotationAtriskRolerefUnresolved

			if arm.when != i1449AtBuild {
				e.createBinding()
			}
			if arm.when == i1449AfterBuildThenRebuild {
				cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})
			}
			if e.canList() {
				t.Fatalf("PRE: with the role absent the user must be DENIED `list %s` in %s", a.target.Resource, psTargetNS)
			}

			// --- step 0: the binding exists, its role grants nothing yet.
			c0 := e.class()
			k0, _ := k423RAKey(t, e.ctx)
			t.Logf("step 0 (binding, no grant): sub-gen=%d key=%.12s… may list=%v", c0.RBACSubGen, k0, e.canList())
			var before i1449Read
			if arm.readBeforeGrant {
				before = e.read()
				t.Logf("step 0 read: carries row=%v apiserver dials=%d L1 hits=%d body=%s", before.row, before.dials, before.hits, before.body)
				if before.row {
					t.Fatalf("PRE: the denied user's read must not carry the row")
				}
			}

			// --- step 1: the role is created (or its rules written), granting list.
			e.grant()
			c1 := e.class()
			k1, _ := k423RAKey(t, e.ctx)
			t.Logf("step 1 (role grants list): sub-gen=%d key=%.12s… may list=%v; class moved=%v key moved=%v",
				c1.RBACSubGen, k1, e.canList(), c1 != c0, k1 != k0)
			if c1.SubjectBindingSet != c0.SubjectBindingSet {
				t.Fatalf("PRE: a role event must not change the binding set")
			}
			if c1.RBACSubGen <= c0.RBACSubGen || k1 == k0 {
				t.Errorf("GRANT DID NOT ROTATE: the role the user's binding names started granting `list`, and the user's "+
					"sub-generation went %d → %d (key moved=%v)", c0.RBACSubGen, c1.RBACSubGen, k1 != k0)
			}

			// --- the user reads; the answer is stored.
			granted := e.read()
			t.Logf("step 1 read: carries row=%v apiserver dials=%d L1 hits=%d body=%s", granted.row, granted.dials, granted.hits, granted.body)
			if !granted.row {
				if !arm.readBeforeGrant {
					t.Fatalf("SETUP: the granted user's read must carry row %s/%s", psTargetNS, psTargetObj)
				}
				t.Errorf("STALE DENIAL SERVED: the user — allowed `list %s` in %s by the real evaluator — was served the view "+
					"stored before the grant, without row %s/%s (apiserver dials=%d, L1 hits=%d)",
					a.target.Resource, psTargetNS, psTargetNS, psTargetObj, granted.dials, granted.hits)
				// The revoke half needs the granted view stored; the sibling arm
				// without the early read covers it.
				return
			}
			stored, ok := cache.ResolvedCache().GetNoTouch(k1)
			if !ok || !psHasSentinel(stored.RawJSON) {
				t.Fatalf("SETUP: the granted view must be stored under the user's key")
			}

			// --- step 2: the rule is removed.
			e.revoke()
			c2 := e.class()
			k2, _ := k423RAKey(t, e.ctx)
			t.Logf("step 2 (rule removed): sub-gen=%d key=%.12s… may list=%v; class moved=%v key moved=%v",
				c2.RBACSubGen, k2, e.canList(), c2 != c1, k2 != k1)
			if c2.RBACSubGen <= c1.RBACSubGen || k2 == k1 {
				t.Errorf("REVOKE DID NOT ROTATE: the role the user's binding names stopped granting `list`, and the user's "+
					"sub-generation went %d → %d (key moved=%v)", c1.RBACSubGen, c2.RBACSubGen, k2 != k1)
			}

			// --- the user's next read.
			preRead, resident := cache.ResolvedCache().GetNoTouch(k2)
			t.Logf("step 2, before the read: the cell under the user's current key is the entry stored at step 1: %v", resident && preRead == stored)
			after := e.read()
			t.Logf("step 2 read: carries row=%v apiserver dials=%d L1 hits=%d body=%s", after.row, after.dials, after.hits, after.body)
			if after.row {
				t.Errorf("STALE GRANT SERVED: the user — denied `list %s` in %s by the real evaluator — was served the stored "+
					"body carrying row %s/%s (apiserver dials=%d, L1 hits=%d)",
					a.target.Resource, psTargetNS, psTargetNS, psTargetObj, after.dials, after.hits)
			}
			t.Logf("#261 case-3 detector (%s) moved by %d over the arm", c3RolerefUnresolvedStat, cache.ResolvedCache().Stats().ServeMissedRotationAtriskRolerefUnresolved-detector)

			if got := i1439Class(psDave, []string{psGroup}); got != bystander {
				t.Errorf("BLAST RADIUS: the class of an identity that is no subject of the binding moved")
			}

			// --- control: the same request with nothing cached is the denied view.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			if ctl := e.read(); ctl.row {
				t.Fatalf("CONTROL FAILED: the user's own post-removal resolve carries the row — the revoke is not real; body=%s", ctl.body)
			}
		})
	}
}
