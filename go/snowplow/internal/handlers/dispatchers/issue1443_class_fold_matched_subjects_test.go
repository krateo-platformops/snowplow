// issue1443_class_fold_matched_subjects_test.go — #1443: the identity class must
// fold every subject through which the evaluator can match a binding.
//
// The class is (binding-set digest, sub-generation). A rules edit leaves the
// digest alone and is carried only by the sub-generation: the edit bumps the
// SUBJECTS of every binding that references the role, and a requester's
// sub-generation moves only if it folds one of those subjects. Before the fix the evaluator
// matched a ServiceAccount requester through subjects the fold did not read:
//
//   - Group system:serviceaccounts and Group system:serviceaccounts:<ns>, which
//     the evaluator adds to every ServiceAccount requester whether or not the
//     request presents them; the fold read
//     only the presented groups plus the apiserver's implicit group;
//   - a User-kind subject naming the ServiceAccount's username
//     (system:serviceaccount:<ns>:<name>), which the evaluator matches like any
//     other user name (anySubjectMatches); the fold read the ServiceAccount
//     counter for such a username and never the User one.
//
// So a rules edit behind such a binding changed the verdict and rotated nothing.
// Since the fix both read one enumeration (cache.SubjectMatcher).
//
// THE ARMS (TestIssue1443_RulesEditBehindEveryMatchedSubjectRotatesTheKey). Two
// identities hold one binding set, matched through the arm's subject(s). The
// first lists configmaps and its answer is stored. The rules of the role behind
// the binding are edited, the `list` grant being removed. Then each identity
// makes the same request. Asserted at each step: the class, the key, and the
// bytes served.
//
// REAL BOUNDARY: the #1439 / #423 harness (issue1439_rbac_state_naming_test.go):
// a real ResourceWatcher over a fake apiserver, the real RBAC snapshot rebuild
// and its sub-generation flush, the real bindings index, the real
// dispatchCacheLookupKey, the real restActionHandler.ServeHTTP (Put and hit
// path) and the real resolver over the informer pivot. The only seam is the read
// of the RESTAction CR itself (fetchObjectFn).
package dispatchers

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

const (
	i1443SANamespace = "i1443-apps"
	i1443SAFirst     = "system:serviceaccount:" + i1443SANamespace + ":reader-a"
	i1443SASecond    = "system:serviceaccount:" + i1443SANamespace + ":reader-b"
	i1443AllSAs      = "system:serviceaccounts"
	i1443NamespaceSA = "system:serviceaccounts:" + i1443SANamespace
	i1443Human       = "i1443-human"
)

// i1443TokenGroups is what a real ServiceAccount token presents: the groups the
// apiserver's TokenReview reports for it.
var i1443TokenGroups = []string{i1443AllSAs, i1443NamespaceSA, "system:authenticated"}

type i1443Identity struct {
	user   string
	groups []string
}

func (id i1443Identity) ctx() context.Context       { return i1439Ctx(id.user, id.groups) }
func (id i1443Identity) class() cache.IdentityClass { return i1439Class(id.user, id.groups) }
func (id i1443Identity) presents() string {
	if len(id.groups) == 0 {
		return "no groups"
	}
	return "its token's groups"
}

type i1443Arm struct {
	name string
	// The subjects of the two bindings (the RESTAction grant and the target
	// grant) both identities are matched through.
	subjects      []rbacv1.Subject
	first, second i1443Identity
}

func i1443Group(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: name}
}

func i1443User(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: name}
}

func i1443SA(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "ServiceAccount", Namespace: i1443SANamespace, Name: name}
}

// i1443Bindings grants the arm's subjects the RESTAction (cluster-wide) and
// `get`/`list` on the target in the target namespace, through target-reader:
// the role the arm then edits.
func i1443Bindings(subjects []rbacv1.Subject) []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "i1443-ra-bind", UID: types.UID("uid-crb-1443-ra")},
			Subjects:   subjects,
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "i1443-target", UID: types.UID("uid-rb-1443-target")},
			Subjects:   subjects,
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
		},
	}
}

// i1443SeedClientconfigs publishes one owned clientconfig Secret per identity,
// under the name the resolver reads for it.
func i1443SeedClientconfigs(t *testing.T, srvURL string, ids ...i1443Identity) {
	t.Helper()
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	byName := map[string]*corev1.Secret{}
	for _, id := range ids {
		for _, name := range credsecret.For(id.user, id.groups) {
			byName[name] = &corev1.Secret{
				ObjectMeta: authnMeta(psAuthnNS, name, id.user),
				Data:       map[string][]byte{"server-url": []byte(srvURL), "token": []byte("tok-" + id.user)},
			}
		}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: byName})
	cache.ForceSecretsCacheReadyForTest(psAuthnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
}

func i1443CanList(t *testing.T, a psArm, id i1443Identity) bool {
	t.Helper()
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: id.user, Groups: id.groups, Verb: "list", Resource: a.target.Resource, Namespace: psTargetNS,
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC: %v", err)
	}
	return ok
}

func TestIssue1443_RulesEditBehindEveryMatchedSubjectRotatesTheKey(t *testing.T) {
	saA := func(groups []string) i1443Identity { return i1443Identity{i1443SAFirst, groups} }
	saB := func(groups []string) i1443Identity { return i1443Identity{i1443SASecond, groups} }
	for _, arm := range []i1443Arm{
		{
			name:     "sa-presenting-no-groups/bound-via-group-system:serviceaccounts",
			subjects: []rbacv1.Subject{i1443Group(i1443AllSAs)},
			first:    saA(nil), second: saB(nil),
		},
		{
			name:     "sa-presenting-no-groups/bound-via-group-system:serviceaccounts:ns",
			subjects: []rbacv1.Subject{i1443Group(i1443NamespaceSA)},
			first:    saA(nil), second: saB(nil),
		},
		{
			name:     "sa-presenting-no-groups/bound-via-user-kind-subject-naming-its-username",
			subjects: []rbacv1.Subject{i1443User(i1443SAFirst), i1443User(i1443SASecond)},
			first:    saA(nil), second: saB(nil),
		},
		{
			name:     "sa-presenting-its-groups/bound-via-user-kind-subject-naming-its-username",
			subjects: []rbacv1.Subject{i1443User(i1443SAFirst), i1443User(i1443SASecond)},
			first:    saA(i1443TokenGroups), second: saB(i1443TokenGroups),
		},
		{
			// One binding names a human user AND the ServiceAccount namespace
			// group. The human's class rotates on the edit; the ServiceAccount's
			// does not, and it is the class the human cached under.
			name:     "human-user-then-sa-presenting-no-groups/one-binding-naming-both",
			subjects: []rbacv1.Subject{i1443User(i1443Human), i1443Group(i1443NamespaceSA)},
			first:    i1443Identity{i1443Human, nil}, second: saA(nil),
		},
		// Controls: subjects the fold already reads. Green before the fix.
		{
			name:     "control/sa-presenting-its-groups/bound-via-group-system:serviceaccounts:ns",
			subjects: []rbacv1.Subject{i1443Group(i1443NamespaceSA)},
			first:    saA(i1443TokenGroups), second: saB(i1443TokenGroups),
		},
		{
			name:     "control/sa-presenting-no-groups/bound-via-serviceaccount-kind-subject",
			subjects: []rbacv1.Subject{i1443SA("reader-a"), i1443SA("reader-b")},
			first:    saA(nil), second: saB(nil),
		},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			k423Env(t)
			a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
			dyn := psBuildWatcher(t, a, i1443Bindings(arm.subjects)...)
			cache.ResetBindingsByGVRIndexForTest()
			t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
			cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{h1RAGVR, a.target})
			cache.ResetPendingSubGenBumpsForTest()
			cache.ResetRBACSubGenForTest()
			t.Cleanup(cache.ResetPendingSubGenBumpsForTest)
			srv := psFakeAPIServer(t, a, k423Counters())
			i1443SeedClientconfigs(t, srv.URL, arm.first, arm.second)
			cr := k423ListCR(a)
			ids := []struct {
				role string
				id   i1443Identity
			}{{"first", arm.first}, {"second", arm.second}}

			// --- before the edit: one binding set, one class, one key; both allowed.
			classBefore := arm.first.class()
			if arm.second.class() != classBefore {
				t.Fatalf("PRE: the two identities must be in ONE class before the edit")
			}
			keyBefore, _ := k423RAKey(t, arm.first.ctx())
			if k, _ := k423RAKey(t, arm.second.ctx()); k != keyBefore {
				t.Fatalf("PRE: one class must give one key before the edit")
			}
			for _, x := range ids {
				if !i1443CanList(t, a, x.id) {
					t.Fatalf("PRE: the %s identity must be allowed `list %s` in %s before the edit", x.role, a.target.Resource, psTargetNS)
				}
			}

			// --- the first identity caches its answer under the pre-edit state.
			if rec := psServeCR(t, cr, srv.URL, arm.first.ctx()); !psHasSentinel(rec.Body.Bytes()) {
				t.Fatalf("SETUP: the first identity's pre-edit body must carry row %s/%s; code=%d body=%s",
					psTargetNS, psTargetObj, rec.Code, psTrunc(rec.Body.String(), 300))
			}
			preEdit, ok := cache.ResolvedCache().GetNoTouch(keyBefore)
			if !ok || !psHasSentinel(preEdit.RawJSON) {
				t.Fatalf("SETUP: the first identity's pre-edit body must be stored under its key")
			}
			t.Logf("before the edit: both identities in one class (sub-gen=%d), one key; the first identity's answer carrying row %s/%s is stored under it",
				classBefore.RBACSubGen, psTargetNS, psTargetObj)

			// --- the rules edit: target-reader loses every grant on the target.
			bumpsBefore := cache.RBACSubGenBumpsTotal()
			u, err := dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Get(context.Background(), "target-reader", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get role: %v", err)
			}
			if err := unstructured.SetNestedSlice(u.Object, i1439RulesRevoke, "rules"); err != nil {
				t.Fatalf("edit role: %v", err)
			}
			if _, err := dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("update role: %v", err)
			}
			// Settled on the VERDICT and the flush, never on the class: the class
			// not moving is what the arm is here to see.
			if !i444Wait(10*time.Second, func() bool {
				return !i1443CanList(t, a, arm.first) && !i1443CanList(t, a, arm.second) && cache.RBACSubGenBumpsTotal() != bumpsBefore
			}) {
				t.Fatalf("SETUP: the rules edit must deny both identities and bump at least one subject; first allowed=%v second allowed=%v bumps=%d",
					i1443CanList(t, a, arm.first), i1443CanList(t, a, arm.second), cache.RBACSubGenBumpsTotal()-bumpsBefore)
			}
			if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
				t.Fatalf("SETUP: RBAC rebuild did not quiesce after the rules edit: %v", err)
			}
			t.Logf("rules edit published: both identities are now DENIED `list %s` in %s by the real evaluator; the flush bumped %d subject(s)",
				a.target.Resource, psTargetNS, cache.RBACSubGenBumpsTotal()-bumpsBefore)

			// --- each identity's class, key and next read after the edit.
			for _, x := range ids {
				classNow := x.id.class()
				keyNow, _ := k423RAKey(t, x.id.ctx())
				entry, resident := cache.ResolvedCache().GetNoTouch(keyNow)
				hitsBefore := cache.ResolvedCache().Stats().HitTotal
				rec := psServeCR(t, cr, srv.URL, x.id.ctx())
				hits := cache.ResolvedCache().Stats().HitTotal - hitsBefore
				served := psHasSentinel(rec.Body.Bytes())
				t.Logf("%s identity (a %s presenting %s): class moved=%v (sub-gen %d → %d, binding set equal=%v); key rotated=%v; "+
					"cell under its key before its read: resident=%v, the pre-edit entry=%v; its /call: code=%d L1 hit delta=%d carries row %s/%s=%v body=%s",
					x.role, i1443Kind(x.id), x.id.presents(), classNow != classBefore, classBefore.RBACSubGen, classNow.RBACSubGen,
					classNow.SubjectBindingSet == classBefore.SubjectBindingSet, keyNow != keyBefore,
					resident, resident && entry == preEdit, rec.Code, hits, psTargetNS, psTargetObj, served, psTrunc(rec.Body.String(), 200))

				if classNow == classBefore {
					t.Errorf("CLASS DID NOT MOVE (%s identity): its verdict went allowed → denied and its class is unchanged", x.role)
				}
				if keyNow == keyBefore {
					t.Errorf("KEY DID NOT ROTATE (%s identity): its post-edit key is the pre-edit key", x.role)
				}
				if served {
					whose := "its OWN pre-edit body (stale)"
					if x.id.user != arm.first.user {
						whose = "the FIRST identity's pre-edit body (another identity's cell)"
					}
					t.Errorf("REVOKED ROW SERVED (%s identity): denied `list %s` in %s by the real evaluator, it was served %s "+
						"carrying row %s/%s (L1 hits=%d)", x.role, a.target.Resource, psTargetNS, whose, psTargetNS, psTargetObj, hits)
				}
			}

			// --- control: the same requests with nothing cached are the denied view.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			for _, x := range ids {
				ctl := psServeCR(t, cr, srv.URL, x.id.ctx())
				if psHasSentinel(ctl.Body.Bytes()) {
					t.Fatalf("CONTROL FAILED: the %s identity's own post-edit resolve carries the row — the revoke is not real; body=%s",
						x.role, psTrunc(ctl.Body.String(), 300))
				}
				t.Logf("CONTROL %s identity, cold cache: code=%d body=%s", x.role, ctl.Code, psTrunc(ctl.Body.String(), 200))
			}
		})
	}
}

func i1443Kind(id i1443Identity) string {
	if rbac.IsServiceAccountUsername(id.user) {
		return "ServiceAccount"
	}
	return "human user"
}
