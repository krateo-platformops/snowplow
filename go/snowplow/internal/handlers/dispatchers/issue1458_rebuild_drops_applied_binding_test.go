// issue1458_rebuild_drops_applied_binding_test.go — #1458: a rebuild of the
// bindings index must never hold less than the event path has already applied.
//
// A rules change reaches an identity's cache keys only through the
// sub-generation of the subjects of the bindings that name the role
// (cache/bindings_by_gvr_delta.go onRoleRulesChanged, over the index's byRole
// map). A binding enters that map two ways: the event path (onBindingAdd, as
// soon as the informer delivers the binding) and a build
// (cache/bindings_by_gvr.go BuildBindingsByGVRIndex), which EMPTIES the index
// and refills it from the PUBLISHED RBAC snapshot. The snapshot is published
// later than the event is handled (the handler only schedules the rebuild), so
// a build that runs in between refills from a snapshot that does not hold the
// binding yet: the binding the event path recorded is dropped. Nothing puts it
// back until the next build, and every rules edit of its role in between finds
// no binding and bumps no subject.
//
// Production builds the index at the readiness settle and then again in every
// prewarm pass: the boot pass, every keep-warm sweep and every GVR discovery
// (prewarm_engine_boot.go rePrewarmBootScoped step 3).
//
// THE ARMS (TestIssue1458_RebuildBetweenABindingEventAndItsPublish). The role
// exists and grants `list` on the target. One ordinary user is the only subject
// of a binding to it, created while the snapshot writer is held (the test
// barrier at the top of the snapshot rebuild): the event path has recorded the
// binding, the snapshot holding it is not published. Then the writer is
// released, the user reads and the answer is stored; the role's rule is
// removed; the user reads again.
//
//	rebuild-in-window/*        the index is rebuilt while the writer is held, in
//	                           each binding/role kind pair.
//	no-rebuild-in-window       CONTROL: the same choreography, no rebuild.
//	rebuild-after-publish      CONTROL: the index is rebuilt once the snapshot
//	                           holding the binding is published.
//
// REAL BOUNDARY (the #423 ps harness, as #1449): a real ResourceWatcher over
// the fake apiserver, the real RBAC snapshot rebuild and its sub-generation
// flush, the real bindings index build and delta hooks, the real
// dispatchCacheLookupKey, the real restActionHandler.ServeHTTP (Put and hit
// path) and the real evaluator. The seams: the read of the RESTAction CR, and
// the snapshot writer's test barrier, which only widens a window production
// has at every event (it is one snapshot rebuild wide). The build is called
// directly, as the #1449 arms do; the prewarm pass calls the same function.
//
// "The event path has recorded the binding" is OBSERVED, not waited for by
// time: cache.IdentityAuthorisedForGVR reads the index and turns true for the
// user when onBindingAdd has enrolled the binding.
//
// ASSERTED on the user's class and key, and ON THE BYTES SERVED.
//
// RED on #1455's head 82605786 (the #1449 fix): the rebuild-in-window arms,
// the stored row served after the rule was removed.
package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type i1458Rebuild int

const (
	i1458NoRebuild i1458Rebuild = iota
	i1458RebuildInWindow
	i1458RebuildAfterPublish
)

type i1458Arm struct {
	name        string
	bindingKind string // RoleBinding | ClusterRoleBinding
	roleKind    string // Role | ClusterRole
	rebuild     i1458Rebuild
}

// i1458Setup: the role exists with the grant, no binding names the user, the
// index is built and the snapshot writer is idle.
func i1458Setup(t *testing.T, arm i1449Arm) *i1449Env {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, arm.role(i1449GrantRules(a)))
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{h1RAGVR, a.target})
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetPendingSubGenBumpsForTest)
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("SETUP: the snapshot writer is not idle: %v", err)
	}

	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	return &i1449Env{t: t, a: a, arm: arm, dyn: dyn, srvURL: srv.URL, counters: counters, cr: k423ListCR(a),
		ctx: i1439Ctx(psCarol, []string{psGroup})}
}

func TestIssue1458_RebuildBetweenABindingEventAndItsPublish(t *testing.T) {
	for _, arm := range []i1458Arm{
		{name: "rebuild-in-window/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", rebuild: i1458RebuildInWindow},
		{name: "rebuild-in-window/rolebinding-to-clusterrole", bindingKind: "RoleBinding", roleKind: "ClusterRole", rebuild: i1458RebuildInWindow},
		{name: "rebuild-in-window/clusterrolebinding-to-clusterrole", bindingKind: "ClusterRoleBinding", roleKind: "ClusterRole", rebuild: i1458RebuildInWindow},
		{name: "no-rebuild-in-window/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", rebuild: i1458NoRebuild},
		{name: "rebuild-after-publish/rolebinding-to-role", bindingKind: "RoleBinding", roleKind: "Role", rebuild: i1458RebuildAfterPublish},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			e := i1458Setup(t, i1449Arm{name: arm.name, bindingKind: arm.bindingKind, roleKind: arm.roleKind, when: i1449AfterBuild})
			a := e.a
			navigated := []schema.GroupVersionResource{h1RAGVR, a.target}
			indexed := func() bool { return cache.IdentityAuthorisedForGVR(a.target, psCarol, []string{psGroup}) }

			if e.canList() || indexed() {
				t.Fatalf("PRE: with no binding the user must be denied `list %s` and absent from the index", a.target.Resource)
			}
			c0 := e.class()

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

			// --- the binding is created; the informer delivers it; the event
			// path records it in the index.
			u := i1449Unstructured(t, e.arm.binding())
			var err error
			if arm.bindingKind == "ClusterRoleBinding" {
				_, err = e.dyn.Resource(i1449CRBGVR).Create(context.Background(), u, metav1.CreateOptions{})
			} else {
				_, err = e.dyn.Resource(k424RBGVR).Namespace(psTargetNS).Create(context.Background(), u, metav1.CreateOptions{})
			}
			if err != nil {
				t.Fatalf("create binding: %v", err)
			}
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatalf("SETUP: the binding's event never reached the snapshot writer")
			}
			if !i444Wait(10*time.Second, indexed) {
				t.Fatalf("SETUP: the event path never recorded the binding in the index")
			}
			if cache.RBACGen() != seq || e.canList() {
				t.Fatalf("SETUP: a snapshot was published while the writer was held (publish seq %d → %d, may list=%v)",
					seq, cache.RBACGen(), e.canList())
			}
			t.Logf("in the window: the event path has recorded the binding (in the index=%v); the published snapshot does not hold it (may list=%v)",
				indexed(), e.canList())

			// --- the rebuild, as a prewarm pass does.
			if arm.rebuild == i1458RebuildInWindow {
				cache.SeedBindingsIndexFromSnapshotForTest(navigated)
				t.Logf("index rebuilt in the window: the binding is in the index=%v", indexed())
			}

			// --- the writer is released: the snapshot holding the binding is published.
			free()
			e.settle("the publish of the binding", e.canList)
			if arm.rebuild == i1458RebuildAfterPublish {
				cache.SeedBindingsIndexFromSnapshotForTest(navigated)
			}
			c1 := e.class()
			k1, _ := k423RAKey(t, e.ctx)
			t.Logf("step 1 (binding published, role grants list): sub-gen=%d key=%.12s… may list=%v; the binding is in the index=%v",
				c1.RBACSubGen, k1, e.canList(), indexed())
			if c1 == c0 {
				t.Fatalf("PRE: the binding's create must move the user's class")
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
					"sub-generation went %d → %d (key moved=%v): the rebuild dropped a binding the event path had recorded",
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
