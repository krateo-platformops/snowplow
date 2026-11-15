// bindings_by_gvr_build_delta_parity_1449_test.go — #1449: the index BUILD and
// the binding-ADD delta hook are one function (recordBindingLocked), so for one
// RBAC world they produce one index. Before #1449 the build skipped a binding
// whose role was absent while the delta hook recorded it under the role it
// names, and every build dropped what the hook had recorded.
//
// The world holds a binding to a present role, a wildcard binding, and a
// ClusterRoleBinding, a RoleBinding-to-Role and a RoleBinding-to-ClusterRole
// whose roles are absent.
//
//	arrived by the BUILD        every binding is in the snapshot the build reads
//	arrived by the DELTA HOOK   the build runs over the roles alone, then each
//	                            binding arrives as an ADD event
//
// The two indexes must be equal, every binding must be recorded under the role
// it names, and a binding to an absent role must be in no GVR or wildcard
// bucket. Then the absent role is created: the role-ADD event must reach the
// binding's subject in both, and leave the two indexes equal again.
//
// RED under each mutation: the build skipping a binding whose role is absent;
// the delta hook dropping the role key of such a binding.
package cache

import (
	"reflect"
	"sort"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// i1449IndexView is a comparable copy of the index's four maps.
type i1449IndexView struct {
	ByGVR    map[groupResource][]string
	Wildcard []string
	ByRole   map[string][]string
	Entries  map[string][]subjectKey
}

func i1449SortedIDs(set map[bindingID]struct{}) []string {
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, string(id))
	}
	sort.Strings(out)
	return out
}

func i1449View() i1449IndexView {
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	v := i1449IndexView{
		ByGVR:    map[groupResource][]string{},
		Wildcard: i1449SortedIDs(idx.wildcard),
		ByRole:   map[string][]string{},
		Entries:  map[string][]subjectKey{},
	}
	for gr, set := range idx.byGVR {
		if len(set) > 0 {
			v.ByGVR[gr] = i1449SortedIDs(set)
		}
	}
	for rk, set := range idx.byRole {
		if len(set) > 0 {
			v.ByRole[rk] = i1449SortedIDs(set)
		}
	}
	for id, e := range idx.entries {
		v.Entries[string(id)] = append([]subjectKey(nil), e.subjects...)
	}
	return v
}

func i1449PendingSubjects() map[subjectKey]struct{} {
	pendingSubGenBumps.mu.Lock()
	defer pendingSubGenBumps.mu.Unlock()
	out := map[subjectKey]struct{}{}
	for s := range pendingSubGenBumps.set {
		out[s] = struct{}{}
	}
	return out
}

func TestIssue1449_BuildAndDeltaHookProduceOneIndex(t *testing.T) {
	const ns = "i1449-ns"
	target := gr("composition.krateo.io", "compositions")
	navigated := []schema.GroupVersionResource{target, gr("widgets.templates.krateo.io", "panels")}
	user := func(n string) rbacv1.Subject { return rbacv1.Subject{Kind: "User", Name: n} }
	ref := func(kind, name string) rbacv1.RoleRef {
		return rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: kind, Name: name}
	}

	presentCRB, presentCR := crbRuleUID("i1449-present", "uid-present", user("i1449-present-user"),
		getListRules(target.Group, target.Resource))
	adminCRB, adminCR := crbRuleUID("i1449-admin", "uid-admin", user("i1449-admin-user"), wildcardRules())
	lateCRB := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "i1449-late-crb", UID: types.UID("uid-late-crb")},
		Subjects:   []rbacv1.Subject{user("i1449-late-crb-user")},
		RoleRef:    ref("ClusterRole", "i1449-late-cr"),
	}
	lateRB := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "i1449-late-rb", UID: types.UID("uid-late-rb")},
		Subjects:   []rbacv1.Subject{user("i1449-late-rb-user")},
		RoleRef:    ref("Role", "i1449-late-role"),
	}
	lateRBToCR := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "i1449-late-rb-cr", UID: types.UID("uid-late-rb-cr")},
		Subjects:   []rbacv1.Subject{user("i1449-late-rb-cr-user")},
		RoleRef:    ref("ClusterRole", "i1449-late-cr"),
	}
	crbs := []*rbacv1.ClusterRoleBinding{presentCRB, adminCRB, lateCRB}
	rbs := []*rbacv1.RoleBinding{lateRB, lateRBToCR}
	roles := []*rbacv1.ClusterRole{presentCR, adminCR}
	lateCR := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "i1449-late-cr"},
		Rules:      getListRules(target.Group, target.Resource),
	}

	world := func(withBindings bool, crs []*rbacv1.ClusterRole) *RBACSnapshot {
		var c []*rbacv1.ClusterRoleBinding
		if withBindings {
			c = crbs
		}
		s := buildSnap(c, crs)
		if withBindings {
			s.RoleBindingsByNS[ns] = rbs
			rebuildSubjectIndexes(s)
		}
		return s
	}
	reset := func() {
		ResetBindingsByGVRIndexForTest()
		ResetPendingSubGenBumpsForTest()
	}
	t.Cleanup(func() {
		reset()
		PublishRBACSnapshotForTest(nil)
	})

	// The late role is created: the event, then what it left behind.
	roleCreated := func() (i1449IndexView, map[subjectKey]struct{}) {
		ResetPendingSubGenBumpsForTest()
		PublishRBACSnapshotForTest(world(true, append(append([]*rbacv1.ClusterRole(nil), roles...), lateCR)))
		onRoleObjectChanged(bumpSrcRoleAdd, lateCR)
		return i1449View(), i1449PendingSubjects()
	}

	// --- every binding arrives by the BUILD.
	reset()
	PublishRBACSnapshotForTest(world(true, roles))
	if n := BuildBindingsByGVRIndex(navigated); n != len(crbs)+len(rbs) {
		t.Errorf("the build must record every binding in the snapshot: recorded %d of %d", n, len(crbs)+len(rbs))
	}
	built := i1449View()
	builtAfter, builtBumped := roleCreated()

	// --- every binding arrives by the DELTA HOOK.
	reset()
	PublishRBACSnapshotForTest(world(false, roles))
	BuildBindingsByGVRIndex(navigated)
	PublishRBACSnapshotForTest(world(true, roles))
	for _, b := range crbs {
		onBindingAdd(b)
	}
	for _, b := range rbs {
		onBindingAdd(b)
	}
	hooked := i1449View()
	hookedAfter, hookedBumped := roleCreated()

	if !reflect.DeepEqual(built, hooked) {
		t.Errorf("ONE WORLD, TWO INDEXES (role absent):\n  by the build:      %+v\n  by the delta hook: %+v", built, hooked)
	}
	if !reflect.DeepEqual(builtAfter, hookedAfter) {
		t.Errorf("ONE WORLD, TWO INDEXES (after the role is created):\n  by the build:      %+v\n  by the delta hook: %+v", builtAfter, hookedAfter)
	}

	// --- the properties themselves, on each path.
	lateCRBID, lateRBID, lateRBToCRID := string(crbBindingID(lateCRB)), string(rbBindingID(lateRB)), string(rbBindingID(lateRBToCR))
	wantLateCR := []string{lateCRBID, lateRBToCRID}
	sort.Strings(wantLateCR)
	for _, p := range []struct {
		path          string
		before, after i1449IndexView
		bumped        map[subjectKey]struct{}
	}{
		{"build", built, builtAfter, builtBumped},
		{"delta hook", hooked, hookedAfter, hookedBumped},
	} {
		if got := p.before.ByRole["C/i1449-late-cr"]; !reflect.DeepEqual(got, wantLateCR) {
			t.Errorf("%s: the bindings naming the absent ClusterRole must be recorded under it; got %v want %v", p.path, got, wantLateCR)
		}
		if got := p.before.ByRole["R/"+ns+"/i1449-late-role"]; !reflect.DeepEqual(got, []string{lateRBID}) {
			t.Errorf("%s: the binding naming the absent Role must be recorded under it; got %v", p.path, got)
		}
		for _, id := range []string{lateCRBID, lateRBID, lateRBToCRID} {
			if _, ok := p.before.Entries[id]; !ok {
				t.Errorf("%s: binding %s (role absent) has no entry", p.path, id)
			}
			for g, ids := range p.before.ByGVR {
				if sort.SearchStrings(ids, id) < len(ids) && ids[sort.SearchStrings(ids, id)] == id {
					t.Errorf("%s: binding %s (role absent, grants nothing) is in the %v bucket", p.path, id, g)
				}
			}
			if i := sort.SearchStrings(p.before.Wildcard, id); i < len(p.before.Wildcard) && p.before.Wildcard[i] == id {
				t.Errorf("%s: binding %s (role absent, grants nothing) is in the wildcard bucket", p.path, id)
			}
		}
		// The role's create reaches the subjects of the bindings naming it,
		// and nobody else.
		wantBumped := map[subjectKey]struct{}{
			{Kind: "User", Name: "i1449-late-crb-user"}:   {},
			{Kind: "User", Name: "i1449-late-rb-cr-user"}: {},
		}
		if !reflect.DeepEqual(p.bumped, wantBumped) {
			t.Errorf("%s: the role's create must record a bump for exactly the subjects of the bindings naming it; got %v want %v",
				p.path, p.bumped, wantBumped)
		}
		if got := p.after.ByGVR[grFromGVR(target)]; !reflect.DeepEqual(got, func() []string {
			w := []string{string(crbBindingID(presentCRB)), lateCRBID, lateRBToCRID}
			sort.Strings(w)
			return w
		}()) {
			t.Errorf("%s: after the role is created its bindings must be in the target bucket; got %v", p.path, got)
		}
	}
}
