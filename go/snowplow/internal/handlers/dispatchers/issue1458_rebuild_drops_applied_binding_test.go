// issue1458_rebuild_drops_applied_binding_test.go — #1458: nothing a prewarm
// pass does to the bindings index may leave it holding less (or more) than the
// RBAC event path has already applied.
//
// A rules change reaches an identity's cache keys only through the
// sub-generation of the subjects of the bindings that name the role
// (cache/bindings_by_gvr_delta.go onRoleRulesChanged, over the index's byRole
// map). Until #1458 a binding entered that map two ways: the event path (as
// soon as the informer delivered it) and a BUILD, which emptied the index and
// refilled it from the PUBLISHED RBAC snapshot. The snapshot is published later
// than the event is handled (the handler only schedules the rebuild), so a
// build that ran in between refilled from a snapshot that did not hold the
// change yet: a binding the event path had recorded was dropped, a subject it
// had added was forgotten, a binding it had removed came back. Nothing repaired
// it until the next build, and every rules edit of the role in between bumped
// the wrong subjects, or none. Production built at the readiness settle and
// then again in every prewarm pass: the boot pass, every keep-warm sweep and
// every GVR discovery.
//
// Since #1458 the event hooks alone write the index's membership, from the
// informers' first event on, and a pass only sets the navigated set
// (prewarmPassIndexStep).
//
// THE ARMS (TestIssue1458_PassBetweenAnRBACEventAndItsPublish). One ordinary
// user; a role that grants `list` on the target. An RBAC event that changes
// what binds the user to that role is delivered while the snapshot writer is
// held (the test barrier at the top of the snapshot rebuild): the event path
// has applied it, the snapshot holding it is not published. Then the PASS STEP
// runs — prewarmPassIndexStep, the function the three prewarm passes call, so
// an arm cannot go green by the test no longer doing what a pass does — the
// writer is released, the user reads and the answer is stored; the role's rule
// is removed; the user reads again.
//
//	binding-created/*          the binding is created in the window (three
//	                           binding/role kind pairs).
//	subject-added/*            the binding exists for another user; an update
//	                           adds this user in the window.
//	roleref-changed/*          the user's binding names a role that grants
//	                           nothing; an update re-points it at the granting
//	                           role in the window. (A real apiserver refuses a
//	                           roleRef update; a delete and re-create behind a
//	                           relist reaches the handler as this update.)
//	binding-deleted/*          the user's binding is deleted in the window: the
//	                           pass must not bring it back.
//	before-ready/*             the binding is created before the index is ready
//	                           (the initial publish is held before it marks the
//	                           index ready), its snapshot unpublished; then the
//	                           index becomes ready and the pass step runs.
//	control/no-pass            CONTROL: the binding-created choreography with
//	                           no pass step.
//	control/pass-after-publish CONTROL: the pass step once the snapshot holding
//	                           the binding is published.
//
// REAL BOUNDARY (the #423 ps harness, as #1449): a real ResourceWatcher over
// the fake apiserver, the real RBAC snapshot rebuild and its sub-generation
// flush, the real bindings index fed by the real informer events from the
// watcher's start (no test seed), the real pass step, the real
// dispatchCacheLookupKey, the real restActionHandler.ServeHTTP (Put and hit
// path) and the real evaluator. The seams: the read of the RESTAction CR; the
// snapshot writer's test barrier, which only widens a window production has at
// every event (it is one snapshot rebuild wide); and, for before-ready, the
// hook between the initial publish and the ready mark.
//
// "The event path has applied the event" is OBSERVED, not waited for by time:
// the index is read (cache.IdentityAuthorisedForGVR, or the membership itself
// before ready).
//
// ASSERTED on the user's class and key, on the index, and ON THE BYTES SERVED.
//
// RED before the fix (PR #1459's first commit, on #1455's head 82605786): the
// binding-created arms, the stored row served after the rule was removed.
// Mutation, RED here: the pass step refilling membership from the published
// snapshot.
package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type i1458Event int

const (
	i1458BindingCreated i1458Event = iota
	i1458SubjectAdded
	i1458RoleRefChanged
	i1458BindingDeleted
	i1458BeforeReady
)

type i1458Pass int

const (
	i1458PassInWindow i1458Pass = iota
	i1458NoPass
	i1458PassAfterPublish
)

type i1458Arm struct {
	name        string
	bindingKind string // RoleBinding | ClusterRoleBinding
	roleKind    string // Role | ClusterRole
	event       i1458Event
	pass        i1458Pass
}

const i1458IdleRole = "i1458-grants-nothing"

// i1458Binding is the arm's binding, naming role, with the given users as its
// subjects.
func i1458Binding(arm i1449Arm, role string, users ...string) runtime.Object {
	var subj []rbacv1.Subject
	for _, u := range users {
		subj = append(subj, rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: u})
	}
	switch b := arm.binding().(type) {
	case *rbacv1.ClusterRoleBinding:
		b.Subjects, b.RoleRef.Name = subj, role
		return b
	case *rbacv1.RoleBinding:
		b.Subjects, b.RoleRef.Name = subj, role
		return b
	}
	return nil
}

// i1458IdleRoleObj is a role of the arm's kind that grants nothing on the target.
func i1458IdleRoleObj(arm i1449Arm) runtime.Object {
	switch r := arm.role(i1449RevokedRules).(type) {
	case *rbacv1.ClusterRole:
		r.Name = i1458IdleRole
		return r
	case *rbacv1.Role:
		r.Name = i1458IdleRole
		return r
	}
	return nil
}

func (e *i1449Env) bindingClient() interface {
	Create(context.Context, *unstructured.Unstructured, metav1.CreateOptions, ...string) (*unstructured.Unstructured, error)
	Update(context.Context, *unstructured.Unstructured, metav1.UpdateOptions, ...string) (*unstructured.Unstructured, error)
	Delete(context.Context, string, metav1.DeleteOptions, ...string) error
} {
	if e.arm.bindingKind == "ClusterRoleBinding" {
		return e.dyn.Resource(i1449CRBGVR)
	}
	return e.dyn.Resource(k424RBGVR).Namespace(psTargetNS)
}

// i1458Setup builds the world and the watcher. The bindings index is the one
// the watcher's own RBAC events fed, from its start: no test seed. With
// holdReady the watcher's initial publish is held before it marks the index
// ready; the returned func releases it.
func i1458Setup(t *testing.T, arm i1449Arm, holdReady bool, world ...runtime.Object) (*i1449Env, func()) {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	releaseReady := func() {}
	if holdReady {
		gate := make(chan struct{})
		restore := cache.SetInitialPublishBeforeIndexHookForTest(func() { <-gate })
		released := false
		releaseReady = func() {
			if !released {
				released = true
				close(gate)
				restore()
			}
		}
	}
	dyn := psBuildWatcher(t, a, world...)
	// Registered after the watcher's own cleanup, so it runs before it: a held
	// initial publish must not outlive the arm.
	t.Cleanup(releaseReady)
	rw := cache.Global()
	if !holdReady {
		if err := rw.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
			t.Fatalf("SETUP: the watcher's initial RBAC publish: %v", err)
		}
		if !cache.BindingsByGVRIndexBuilt() {
			t.Fatalf("SETUP: the initial publish returned and the bindings index is not ready")
		}
	}
	// The navigated set, as the readiness settle and the boot pass give it.
	prewarmPassIndexStep(rw)
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("SETUP: the snapshot writer is not idle: %v", err)
	}
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetPendingSubGenBumpsForTest)

	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	return &i1449Env{t: t, a: a, arm: arm, dyn: dyn, srvURL: srv.URL, counters: counters, cr: k423ListCR(a),
		ctx: i1439Ctx(psCarol, []string{psGroup})}, releaseReady
}

func TestIssue1458_PassBetweenAnRBACEventAndItsPublish(t *testing.T) {
	rb, crb, role, cr := "RoleBinding", "ClusterRoleBinding", "Role", "ClusterRole"
	for _, arm := range []i1458Arm{
		{"binding-created/rolebinding-to-role", rb, role, i1458BindingCreated, i1458PassInWindow},
		{"binding-created/rolebinding-to-clusterrole", rb, cr, i1458BindingCreated, i1458PassInWindow},
		{"binding-created/clusterrolebinding-to-clusterrole", crb, cr, i1458BindingCreated, i1458PassInWindow},
		{"subject-added/rolebinding-to-role", rb, role, i1458SubjectAdded, i1458PassInWindow},
		{"subject-added/clusterrolebinding-to-clusterrole", crb, cr, i1458SubjectAdded, i1458PassInWindow},
		{"roleref-changed/rolebinding-to-role", rb, role, i1458RoleRefChanged, i1458PassInWindow},
		{"roleref-changed/clusterrolebinding-to-clusterrole", crb, cr, i1458RoleRefChanged, i1458PassInWindow},
		{"binding-deleted/rolebinding-to-role", rb, role, i1458BindingDeleted, i1458PassInWindow},
		{"binding-deleted/clusterrolebinding-to-clusterrole", crb, cr, i1458BindingDeleted, i1458PassInWindow},
		{"before-ready/rolebinding-to-role", rb, role, i1458BeforeReady, i1458PassInWindow},
		{"before-ready/clusterrolebinding-to-clusterrole", crb, cr, i1458BeforeReady, i1458PassInWindow},
		{"control/no-pass/rolebinding-to-role", rb, role, i1458BindingCreated, i1458NoPass},
		{"control/pass-after-publish/rolebinding-to-role", rb, role, i1458BindingCreated, i1458PassAfterPublish},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			spec := i1449Arm{name: arm.name, bindingKind: arm.bindingKind, roleKind: arm.roleKind, when: i1449AfterBuild}
			world := []runtime.Object{spec.role(i1449GrantRules(psArm{target: psConfigmapsGVR}))}
			switch arm.event {
			case i1458SubjectAdded:
				world = append(world, i1458Binding(spec, i1449RoleName, psDave))
			case i1458RoleRefChanged:
				world = append(world, i1458IdleRoleObj(spec), i1458Binding(spec, i1458IdleRole, psCarol))
			}
			e, releaseReady := i1458Setup(t, spec, arm.event == i1458BeforeReady, world...)
			a := e.a
			rw := cache.Global()
			indexed := func() bool { return cache.IdentityAuthorisedForGVR(a.target, psCarol, []string{psGroup}) }
			named := func() bool { return cache.BindingsIndexNamesSubjectForTest("User", "", psCarol) }
			startsGranted := arm.event == i1458BindingDeleted
			if startsGranted {
				// The binding to delete is created through the fake apiserver's
				// own client (the fake's watch cannot deliver the delete of an
				// object seeded typed), published, and the sub-generations zeroed.
				if _, err := e.bindingClient().Create(context.Background(),
					i1449Unstructured(t, i1458Binding(spec, i1449RoleName, psCarol)), metav1.CreateOptions{}); err != nil {
					t.Fatalf("SETUP: create the binding: %v", err)
				}
				e.settle("the binding", e.canList)
				cache.ResetPendingSubGenBumpsForTest()
				cache.ResetRBACSubGenForTest()
			}

			if e.canList() != startsGranted {
				t.Fatalf("PRE: the user's `list %s` verdict at start must be %v", a.target.Resource, startsGranted)
			}
			if arm.event != i1458BeforeReady && indexed() != startsGranted {
				t.Fatalf("PRE: the index must answer %v for the user at start, answers %v", startsGranted, indexed())
			}
			c0 := e.class()

			// binding-deleted: the user reads while granted; the answer is stored.
			if startsGranted {
				if r := e.read(); !r.row {
					t.Fatalf("SETUP: the granted user's read must carry row %s/%s; body=%s", psTargetNS, psTargetObj, r.body)
				}
			}

			// --- hold the snapshot writer: the next snapshot rebuild stops
			// before it reads the informers, so nothing is published.
			release := make(chan struct{})
			entered := make(chan struct{}, 64)
			cache.SetRebuildBarrierForTest(func() {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			})
			held := true
			free := func() {
				if held {
					held = false
					close(release)
					cache.SetRebuildBarrierForTest(nil)
				}
			}
			t.Cleanup(free)
			seq := cache.RBACGen()

			// --- the RBAC event; the informer delivers it; the event path applies it.
			ctx := context.Background()
			var err error
			applied := indexed
			switch arm.event {
			case i1458BindingCreated:
				_, err = e.bindingClient().Create(ctx, i1449Unstructured(t, i1458Binding(spec, i1449RoleName, psCarol)), metav1.CreateOptions{})
			case i1458BeforeReady:
				_, err = e.bindingClient().Create(ctx, i1449Unstructured(t, i1458Binding(spec, i1449RoleName, psCarol)), metav1.CreateOptions{})
				applied = named
			case i1458SubjectAdded:
				_, err = e.bindingClient().Update(ctx, i1449Unstructured(t, i1458Binding(spec, i1449RoleName, psDave, psCarol)), metav1.UpdateOptions{})
			case i1458RoleRefChanged:
				_, err = e.bindingClient().Update(ctx, i1449Unstructured(t, i1458Binding(spec, i1449RoleName, psCarol)), metav1.UpdateOptions{})
			case i1458BindingDeleted:
				err = e.bindingClient().Delete(ctx, i1449BindingName, metav1.DeleteOptions{})
				applied = func() bool { return !indexed() }
			}
			if err != nil {
				t.Fatalf("the RBAC event: %v", err)
			}
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatalf("SETUP: the event never reached the snapshot writer")
			}
			if !i444Wait(10*time.Second, applied) {
				t.Fatalf("SETUP: the event path never applied the event to the index")
			}
			if cache.RBACGen() != seq || e.canList() != startsGranted {
				t.Fatalf("SETUP: a snapshot was published while the writer was held (publish seq %d → %d, may list=%v)",
					seq, cache.RBACGen(), e.canList())
			}
			t.Logf("in the window: the event path has applied the event (the index names the user=%v); the published snapshot does not hold it (may list=%v)",
				named(), e.canList())

			if arm.event == i1458BeforeReady {
				if cache.BindingsByGVRIndexBuilt() {
					t.Fatalf("SETUP: the index is ready while its initial publish is held before the ready mark")
				}
				releaseReady()
				if err := rw.WaitInitialRBACPublishForTest(30 * time.Second); err != nil {
					t.Fatalf("the watcher's initial RBAC publish: %v", err)
				}
				if !cache.BindingsByGVRIndexBuilt() {
					t.Fatalf("the initial publish returned and the bindings index is not ready")
				}
				if cache.RBACGen() != seq {
					t.Fatalf("SETUP: a snapshot was published while the writer was held")
				}
				t.Logf("the index became ready with the binding's snapshot unpublished: the index names the user=%v", named())
			}

			// --- the pass step, as the boot pass, a keep-warm sweep and a GVR
			// discovery pass run it.
			if arm.pass == i1458PassInWindow {
				_, bindings := prewarmPassIndexStep(rw)
				t.Logf("pass step in the window (the index holds %d bindings): the index names the user=%v", bindings, named())
			}
			wantNamed := arm.event != i1458BindingDeleted
			if named() != wantNamed {
				t.Errorf("THE PASS CHANGED WHAT THE EVENT PATH APPLIED: after the pass step the index names the user=%v, want %v",
					named(), wantNamed)
			}

			// --- the writer is released: the snapshot holding the event is published.
			free()
			e.settle("the publish of the event", func() bool { return e.canList() != startsGranted })
			if arm.pass == i1458PassAfterPublish {
				prewarmPassIndexStep(rw)
			}
			c1 := e.class()
			k1, _ := k423RAKey(t, e.ctx)
			t.Logf("step 1 (event published): sub-gen=%d key=%.12s… may list=%v; the index names the user=%v, as a prewarm target=%v",
				c1.RBACSubGen, k1, e.canList(), named(), indexed())
			if c1 == c0 {
				t.Errorf("THE EVENT DID NOT MOVE THE USER'S CLASS: sub-gen %d → %d, binding set moved=%v",
					c0.RBACSubGen, c1.RBACSubGen, c1.SubjectBindingSet != c0.SubjectBindingSet)
			}

			if arm.event == i1458BindingDeleted {
				// No phantom: the deleted binding is not in the index, and the
				// user's read no longer carries the row.
				if named() || indexed() {
					t.Errorf("PHANTOM BINDING: the binding deleted in the window is in the index after the pass (names the user=%v, prewarm target=%v)",
						named(), indexed())
				}
				after := e.read()
				t.Logf("read after the delete: carries row=%v apiserver dials=%d L1 hits=%d body=%s", after.row, after.dials, after.hits, after.body)
				if after.row {
					t.Errorf("STALE GRANT SERVED: the user, whose binding was deleted, was served the stored body carrying row %s/%s",
						psTargetNS, psTargetObj)
				}
				return
			}

			// --- the user reads; the answer is stored.
			granted := e.read()
			t.Logf("step 1 read: carries row=%v apiserver dials=%d L1 hits=%d body=%s", granted.row, granted.dials, granted.hits, granted.body)
			if !granted.row {
				t.Fatalf("SETUP: the granted user's read must carry row %s/%s", psTargetNS, psTargetObj)
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
					"sub-generation went %d → %d (key moved=%v): the index lost what the event path had applied",
					c1.RBACSubGen, c2.RBACSubGen, k2 != k1)
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

			// --- control: the same request with nothing cached is the denied view.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			if ctl := e.read(); ctl.row {
				t.Fatalf("CONTROL FAILED: the user's own post-removal resolve carries the row — the revoke is not real; body=%s", ctl.body)
			}
		})
	}
}
