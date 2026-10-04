// identity_class_property_test.go — #449: the semantic half of the identity-key
// gate (scripts/checkidentitykeys is the structural half).
//
// The AST gate proves every identity-bound key folds the identity class through
// the one derivation (rbac.IdentityClassOf → SetIdentity). It cannot prove the
// class itself is RIGHT — #423's key folded every dimension the class had, the
// class just lacked one. These two properties pin the class against the RBAC
// evaluator, over generated worlds:
//
//   - SUFFICIENCY: two identities that derive the SAME identity-bound key get the
//     SAME verdict for every (verb, resource, namespace) the world can ask about.
//     Otherwise the cell the first writes is served verbatim to the second while
//     its resolve may have read rows the second is denied (#423). RED on
//     c20cc763 (pre-#423): 39 of 40 worlds.
//   - ROTATION: after any RBAC mutation, an identity whose verdicts changed
//     derives a NEW key for every identity-bound key — the restactions/widgets
//     cell (dispatchCacheLookupKey), the raFullList cell (apiref.RAFullListKey)
//     and the SeedResolveMemo class. Otherwise a cell resolved under the old
//     rights keeps serving under the new ones (#435: a Role-rules revoke left the
//     raFullList key in place; #432: a grant mid-pass left the memo key in
//     place). RED on c8dbbe69 (pre-#435) on the raFullList key.
//
// Worlds: random Roles + RoleBindings in two namespaces granting get/list on
// random resources to random users and groups, plus one ClusterRoleBinding that
// lets group g0 get the dispatched RESTAction. Mutations go through the fake
// apiserver into the REAL ResourceWatcher, so the snapshot republish and the
// per-subject sub-generation bumps are the production event path. Keys come from
// the production builders; verdicts from the production rbac.EvaluateRBAC.
package dispatchers

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	icsRAGVR      = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	icsRoleGVR    = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	icsRBGVR      = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	icsNamespaces = []string{"ns-a", "ns-b"}
	icsResources  = []string{"secrets", "configmaps", "pods"}
	icsUsers      = []string{"ics-u0", "ics-u1", "ics-u2"}
	icsGroups     = []string{"ics-g0", "ics-g1", "ics-g2"}
)

const icsRBACGroup = "rbac.authorization.k8s.io"

type icsIdentity struct {
	user   string
	groups []string
}

func (id icsIdentity) String() string { return id.user + "[" + strings.Join(id.groups, ",") + "]" }

func (id icsIdentity) ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: id.user, Groups: id.groups}))
}

func icsRandomSubject(r *rand.Rand) rbacv1.Subject {
	if r.Intn(2) == 0 {
		return rbacv1.Subject{Kind: "User", APIGroup: icsRBACGroup, Name: icsUsers[r.Intn(len(icsUsers))]}
	}
	return rbacv1.Subject{Kind: "Group", APIGroup: icsRBACGroup, Name: icsGroups[r.Intn(len(icsGroups))]}
}

func icsRandomRule(r *rand.Rand) []rbacv1.PolicyRule {
	verbs := [][]string{{"get"}, {"list"}, {"get", "list"}}[r.Intn(3)]
	return []rbacv1.PolicyRule{{Verbs: verbs, APIGroups: []string{""}, Resources: []string{icsResources[r.Intn(len(icsResources))]}}}
}

func icsWorld(seed int64) []runtime.Object {
	r := rand.New(rand.NewSource(seed))
	objs := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "ics-ra-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{icsRAGVR.Group}, Resources: []string{icsRAGVR.Resource}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "ics-ra-bind", UID: types.UID("uid-ics-ra")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: icsRBACGroup, Name: icsGroups[0]}},
			RoleRef:    rbacv1.RoleRef{APIGroup: icsRBACGroup, Kind: "ClusterRole", Name: "ics-ra-reader"},
		},
	}
	for i := 0; i < 2+r.Intn(4); i++ {
		ns := icsNamespaces[r.Intn(len(icsNamespaces))]
		name := fmt.Sprintf("ics-r%d", i)
		objs = append(objs,
			&rbacv1.Role{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
				Rules:      icsRandomRule(r),
			},
			&rbacv1.RoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(fmt.Sprintf("uid-ics-%d-%d", seed, i))},
				Subjects:   []rbacv1.Subject{icsRandomSubject(r)},
				RoleRef:    rbacv1.RoleRef{APIGroup: icsRBACGroup, Kind: "Role", Name: name},
			})
	}
	return objs
}

// icsPublish starts a real ResourceWatcher over objs, publishes its snapshot,
// installs it as the global, and zeroes the sub-generations (steady state: no
// RBAC change since boot). Later mutations through the returned fake client
// reach the watcher's real event handlers.
func icsPublish(t *testing.T, objs []runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		icsRAGVR: "RESTActionList",
		{Group: icsRBACGroup, Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: icsRBACGroup, Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		icsRBGVR:   "RoleBindingList",
		icsRoleGVR: "RoleList",
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	// The steady state after boot: the prewarm engine has built the
	// BindingsByGVR index (prewarm_engine_boot.go), whose role → bindings map is
	// how a Role-rules edit reaches the sub-generation of every bound subject.
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	var navigated []schema.GroupVersionResource
	for _, res := range icsResources {
		navigated = append(navigated, schema.GroupVersionResource{Version: "v1", Resource: res})
	}
	cache.BuildBindingsByGVRIndex(append(navigated, icsRAGVR))
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		cache.ResetPendingSubGenBumpsForTest()
		cache.ResetRBACSubGenForTest()
	})
	return dyn
}

func icsIdentities() []icsIdentity {
	var out []icsIdentity
	for _, u := range icsUsers {
		for mask := 0; mask < 1<<len(icsGroups); mask++ {
			var gs []string
			for i, g := range icsGroups {
				if mask&(1<<i) != 0 {
					gs = append(gs, g)
				}
			}
			out = append(out, icsIdentity{u, gs})
		}
	}
	return out
}

// icsVerdicts is the identity's full verdict vector over the world's probes.
func icsVerdicts(t *testing.T, id icsIdentity) string {
	t.Helper()
	var b strings.Builder
	for _, ns := range icsNamespaces {
		for _, res := range icsResources {
			for _, verb := range []string{"get", "list"} {
				ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: id.user, Groups: id.groups, Verb: verb, Resource: res, Namespace: ns, SkipBindingUID: true,
				})
				if err != nil {
					t.Fatalf("EvaluateRBAC(%s): %v", id, err)
				}
				if ok {
					fmt.Fprintf(&b, "%s/%s/%s;", verb, ns, res)
				}
			}
		}
	}
	return b.String()
}

// icsKeys are every identity-bound key the identity derives, by key kind.
func icsKeys(t *testing.T, id icsIdentity) map[string]string {
	t.Helper()
	ctx := id.ctx()
	out := map[string]string{}
	key, handle, in := dispatchCacheLookupKey(ctx, "restactions",
		icsRAGVR.Group, icsRAGVR.Version, icsRAGVR.Resource, "krateo-system", "ics-ra", -1, -1, nil)
	if handle == nil || in == nil {
		t.Fatalf("no cache handle")
	}
	if in.BindingUID == "" {
		return nil // never served nor populated (serveFromCacheEligible)
	}
	out["restactions"] = key
	if _, raKey, ok := apiref.RAFullListKey(ctx, icsRAGVR, "krateo-system", "ics-ra", nil); ok {
		out["raFullList"] = raKey
	}
	out["seedMemoClass"] = rbac.IdentityClassOf(jwtutil.UserInfo{Username: id.user, Groups: id.groups}).String()
	return out
}

func TestIdentityClass_SufficiencyOverGeneratedWorlds(t *testing.T) {
	const worlds = 40
	violations, sharedMembers := 0, 0
	for seed := int64(1); seed <= worlds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("world-%02d", seed), func(t *testing.T) {
			icsPublish(t, icsWorld(seed))
			byKey := map[string][]icsIdentity{}
			for _, id := range icsIdentities() {
				if keys := icsKeys(t, id); keys != nil {
					byKey[keys["restactions"]] = append(byKey[keys["restactions"]], id)
				}
			}
			keys := make([]string, 0, len(byKey))
			for k := range byKey {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				members := byKey[k]
				sharedMembers += len(members) - 1
				want := icsVerdicts(t, members[0])
				for _, m := range members[1:] {
					if got := icsVerdicts(t, m); got != want {
						violations++
						t.Errorf("SHARED CELL, DIVERGENT RBAC: %s and %s derive one key %s… but their verdicts differ:\n  %s: %s\n  %s: %s",
							members[0], m, k[:12], members[0], want, m, got)
						break
					}
				}
			}
		})
	}
	t.Logf("worlds=%d identities sharing another's cell=%d divergent shared cells=%d", worlds, sharedMembers, violations)
	// The arm must be able to fail: if no two identities ever shared a cell,
	// a GREEN would say nothing about sufficiency.
	if sharedMembers == 0 {
		t.Fatalf("VACUOUS: no generated identity shared a cell with another; the property was never exercised")
	}
}

// icsMutate applies one random RBAC mutation through the fake apiserver: a Role
// rules edit (keeps every binding — the #435 shape), a RoleBinding delete, or a
// new RoleBinding to an existing Role. It returns a description for the log.
func icsMutate(t *testing.T, dyn *dynamicfake.FakeDynamicClient, objs []runtime.Object, r *rand.Rand, step int) string {
	t.Helper()
	var roles []*rbacv1.Role
	for _, o := range objs {
		if ro, ok := o.(*rbacv1.Role); ok {
			roles = append(roles, ro)
		}
	}
	role := roles[r.Intn(len(roles))]
	ctx := context.Background()
	if role.Annotations == nil {
		role.Annotations = map[string]string{}
	}
	switch r.Intn(3) {
	case 0:
		// A rules edit that changes nothing would publish nothing to wait for.
		rules := icsRandomRule(r)
		for fmt.Sprint(rules) == fmt.Sprint(role.Rules) {
			rules = icsRandomRule(r)
		}
		role.Rules = rules
		m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(role)
		if _, err := dyn.Resource(icsRoleGVR).Namespace(role.Namespace).Update(ctx, &unstructured.Unstructured{Object: m}, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update role: %v", err)
		}
		return fmt.Sprintf("edit rules of Role %s/%s", role.Namespace, role.Name)
	case 1:
		// Revoke: delete a binding this test granted (created through the fake
		// apiserver, so its DELETE event has the apiserver's shape), else move the
		// seeded binding to another subject — a revoke for the old one.
		if extra := role.Annotations["ics-extra"]; extra != "" {
			if err := dyn.Resource(icsRBGVR).Namespace(role.Namespace).Delete(ctx, extra, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("delete binding: %v", err)
			}
			delete(role.Annotations, "ics-extra")
			return fmt.Sprintf("delete RoleBinding %s/%s", role.Namespace, extra)
		}
		var rb *rbacv1.RoleBinding
		for _, o := range objs {
			if b, ok := o.(*rbacv1.RoleBinding); ok && b.Namespace == role.Namespace && b.Name == role.Name {
				rb = b
			}
		}
		subj := icsRandomSubject(r)
		for subj == rb.Subjects[0] {
			subj = icsRandomSubject(r)
		}
		rb.Subjects = []rbacv1.Subject{subj}
		m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
		if _, err := dyn.Resource(icsRBGVR).Namespace(role.Namespace).Update(ctx, &unstructured.Unstructured{Object: m}, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update binding: %v", err)
		}
		return fmt.Sprintf("move RoleBinding %s/%s to %s %s", role.Namespace, role.Name, subj.Kind, subj.Name)
	default:
		if role.Annotations["ics-extra"] != "" {
			// One test grant per Role at a time keeps the revoke above simple.
			return icsMutate(t, dyn, objs, r, step)
		}
		name := fmt.Sprintf("%s-extra-%d", role.Name, step)
		rb := &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: role.Namespace, Name: name, UID: types.UID("uid-" + name)},
			Subjects:   []rbacv1.Subject{icsRandomSubject(r)},
			RoleRef:    rbacv1.RoleRef{APIGroup: icsRBACGroup, Kind: "Role", Name: role.Name},
		}
		m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
		if _, err := dyn.Resource(icsRBGVR).Namespace(role.Namespace).Create(ctx, &unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create binding: %v", err)
		}
		role.Annotations["ics-extra"] = name
		return fmt.Sprintf("grant Role %s/%s to %s %s", role.Namespace, role.Name, rb.Subjects[0].Kind, rb.Subjects[0].Name)
	}
}

// icsAwaitRepublish waits until the watcher has published a snapshot newer than
// seq (the mutation's event reached the real rebuild, which flushes the
// sub-generation bumps right after the store) and the publish has settled.
func icsAwaitRepublish(t *testing.T, seq uint64, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s := cache.RBACSnapshotForTest(); s != nil && s.PublishSeq > seq {
			// One more beat so a burst of events lands in the same window.
			last := s.PublishSeq
			time.Sleep(50 * time.Millisecond)
			if s2 := cache.RBACSnapshotForTest(); s2 != nil && s2.PublishSeq == last {
				return
			}
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for the RBAC snapshot to republish past seq %d after %q (now %d)", seq, what, cache.RBACSnapshotForTest().PublishSeq)
}

func TestIdentityClass_RotationOverGeneratedWorlds(t *testing.T) {
	const worlds, steps = 20, 3
	moved, stale := 0, 0
	for seed := int64(1); seed <= worlds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("world-%02d", seed), func(t *testing.T) {
			objs := icsWorld(seed)
			dyn := icsPublish(t, objs)
			r := rand.New(rand.NewSource(1000 + seed))
			for step := 0; step < steps; step++ {
				type before struct {
					verdicts string
					keys     map[string]string
				}
				prior := map[string]before{}
				for _, id := range icsIdentities() {
					prior[id.String()] = before{icsVerdicts(t, id), icsKeys(t, id)}
				}
				seq := cache.RBACSnapshotForTest().PublishSeq
				what := icsMutate(t, dyn, objs, r, step)
				icsAwaitRepublish(t, seq, what)
				for _, id := range icsIdentities() {
					p := prior[id.String()]
					if p.keys == nil || icsVerdicts(t, id) == p.verdicts {
						continue
					}
					moved++
					now := icsKeys(t, id)
					for kind, k := range p.keys {
						if now[kind] == k {
							stale++
							t.Errorf("STALE KEY after %q: %s's verdicts changed but its %s key did not rotate (%s…) — a cell resolved under the old rights keeps serving",
								what, id, kind, k[:12])
						}
					}
				}
			}
		})
	}
	t.Logf("worlds=%d identities whose verdicts moved=%d stale keys=%d", worlds, moved, stale)
	if moved == 0 {
		t.Fatalf("VACUOUS: no mutation moved any identity's verdicts; the property was never exercised")
	}
}
