// issue1443_matched_subjects_test.go — #1443: the matched-subject set
// (SubjectMatcher) is the one list the evaluator matches with and the identity
// class folds. These arms pin, inside the cache package:
//
//   - the set itself, written out for each requester shape (the anchor the
//     cross-package parity test does not give: there the evaluator and the fold
//     are compared with each other, so a subject dropped from the SHARED
//     enumeration moves both together);
//   - that each of the three consumers in this package reads exactly it: the
//     sub-generation fold (RBACSubGenForSubject), the reseed predicate
//     (RotatedSubjectSet.Rotated) and Match;
//   - that a binding event bumps a subject in the form Match compares
//     (subjectsFromRBAC against bindingSubjectKey);
//   - that the fold and the matcher allocate nothing on the request path.
package cache

import (
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

const (
	m1443SA   = "system:serviceaccount:apps:reader"
	m1443User = "User"
	m1443Grp  = "Group"
	m1443SAK  = "ServiceAccount"
)

type m1443Shape struct {
	name     string
	username string
	groups   []string
	want     []subjectKey // in Visit order
}

func m1443Shapes() []m1443Shape {
	saGroups := []subjectKey{{Kind: m1443Grp, Name: "system:serviceaccounts"}, {Kind: m1443Grp, Name: "system:serviceaccounts:apps"}}
	saSubject := subjectKey{Kind: m1443SAK, Name: "reader", Namespace: "apps"}
	authn := subjectKey{Kind: m1443Grp, Name: "system:authenticated"}
	unauthn := subjectKey{Kind: m1443Grp, Name: "system:unauthenticated"}
	return []m1443Shape{
		{"human, no groups", "alice", nil,
			[]subjectKey{{Kind: m1443User, Name: "alice"}, authn}},
		{"human, one group", "alice", []string{"devs"},
			[]subjectKey{{Kind: m1443User, Name: "alice"}, {Kind: m1443Grp, Name: "devs"}, authn}},
		{"human presenting system:authenticated", "alice", []string{"system:authenticated"},
			[]subjectKey{{Kind: m1443User, Name: "alice"}, authn}},
		{"serviceaccount, no groups", m1443SA, nil,
			[]subjectKey{{Kind: m1443User, Name: m1443SA}, saGroups[0], saGroups[1], authn, saSubject}},
		{"serviceaccount, its token's groups", m1443SA, []string{"system:serviceaccounts", "system:serviceaccounts:apps", "system:authenticated"},
			[]subjectKey{{Kind: m1443User, Name: m1443SA}, saGroups[0], saGroups[1], authn, saGroups[0], saGroups[1], saSubject}},
		{"serviceaccount, an unrelated group", m1443SA, []string{"devs"},
			[]subjectKey{{Kind: m1443User, Name: m1443SA}, {Kind: m1443Grp, Name: "devs"}, saGroups[0], saGroups[1], authn, saSubject}},
		{"anonymous", "system:anonymous", nil,
			[]subjectKey{{Kind: m1443User, Name: "system:anonymous"}, unauthn}},
		{"public identity", "krateo:public", []string{"system:unauthenticated"},
			[]subjectKey{{Kind: m1443User, Name: "krateo:public"}, unauthn}},
		{"no requester, one group", "", []string{"devs"},
			[]subjectKey{{Kind: m1443Grp, Name: "devs"}}},
		{"no requester, no groups", "", nil, nil},
		{"not a serviceaccount username: missing name", "system:serviceaccount:apps", nil,
			[]subjectKey{{Kind: m1443User, Name: "system:serviceaccount:apps"}, authn}},
	}
}

func m1443Subject(k subjectKey) rbacv1.Subject {
	return rbacv1.Subject{Kind: k.Kind, Name: k.Name, Namespace: k.Namespace}
}

func TestIssue1443_MatchedSubjectSet_PerRequesterShape(t *testing.T) {
	for _, sh := range m1443Shapes() {
		t.Run(sh.name, func(t *testing.T) {
			got := foldSubjects(sh.username, sh.groups)
			if len(got) == 0 {
				got = nil
			}
			if !reflect.DeepEqual(got, sh.want) {
				t.Fatalf("matched-subject set = %v, want %v", got, sh.want)
			}
			m := NewSubjectMatcher(sh.username, sh.groups)
			for _, k := range sh.want {
				if ok, _ := m.Match(m1443Subject(k)); !ok {
					t.Errorf("Match does not match %v, which Visit enumerates", k)
				}
			}
		})
	}
}

// Every consumer in this package reads the set and nothing else: bumping ONE
// subject moves the requester's sub-generation, and marks it rotated, exactly
// when Match matches that subject.
func TestIssue1443_FoldRotatedAndMatchAgree(t *testing.T) {
	var universe []subjectKey
	seen := map[subjectKey]bool{}
	add := func(k subjectKey) {
		if !seen[k] {
			seen[k] = true
			universe = append(universe, k)
		}
	}
	for _, sh := range m1443Shapes() {
		for _, k := range sh.want {
			add(k)
		}
	}
	for _, k := range []subjectKey{
		{Kind: m1443User, Name: "bob"}, {Kind: m1443Grp, Name: "ops"}, {Kind: m1443Grp, Name: "alice"},
		{Kind: m1443Grp, Name: "system:serviceaccounts:other"}, {Kind: m1443Grp, Name: m1443SA},
		{Kind: m1443SAK, Name: "reader", Namespace: "other"}, {Kind: m1443SAK, Name: "reader"},
		{Kind: m1443SAK, Name: "writer", Namespace: "apps"}, {Kind: m1443User, Name: "reader"},
		{Kind: "Robot", Name: "alice"},
	} {
		add(k)
	}
	matchedPairs := 0
	for _, sh := range m1443Shapes() {
		for _, k := range universe {
			ResetRBACSubGenForTest()
			before := RBACSubGenForSubject(sh.username, sh.groups)
			BumpSubjectSubGens([]subjectKey{k})
			folded := RBACSubGenForSubject(sh.username, sh.groups) != before
			rotated := RotatedSubjectSet{set: map[subjectKey]struct{}{k: {}}}.Rotated(sh.username, sh.groups)
			matched, _ := NewSubjectMatcher(sh.username, WithAuthenticatedGroup(sh.username, sh.groups)).Match(m1443Subject(k))
			// The fold's callers pass the effective group set; the bare fold
			// here does not, and for a requester with a user name the matcher
			// adds the same implicit group itself.
			matchedRaw, _ := NewSubjectMatcher(sh.username, sh.groups).Match(m1443Subject(k))
			if matchedRaw {
				matchedPairs++
			}
			if folded != matchedRaw {
				t.Errorf("%s / %v: sub-generation moved=%v, Match=%v", sh.name, k, folded, matchedRaw)
			}
			if rotated != matched {
				t.Errorf("%s / %v: Rotated=%v, Match over the effective groups=%v", sh.name, k, rotated, matched)
			}
		}
	}
	ResetRBACSubGenForTest()
	if matchedPairs < 25 {
		t.Fatalf("VACUOUS: only %d (requester, subject) pairs matched", matchedPairs)
	}
}

// A binding event bumps each subject in the form Match compares.
func TestIssue1443_BumpedFormIsTheMatchedForm(t *testing.T) {
	subjects := []rbacv1.Subject{
		{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "alice"},
		{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: m1443SA},
		{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:serviceaccounts:apps"},
		{Kind: "ServiceAccount", Name: "reader", Namespace: "apps"},
		{Kind: "ServiceAccount", Name: "reader"},
		{Kind: "Robot", Name: "alice"},
	}
	var want []subjectKey
	for _, s := range subjects {
		if k, ok := bindingSubjectKey(s); ok {
			want = append(want, k)
		}
	}
	if got := subjectsFromRBAC(subjects); !reflect.DeepEqual(got, want) {
		t.Fatalf("a binding event bumps %v; Match compares %v", got, want)
	}
	if len(want) != len(subjects)-1 {
		t.Fatalf("the unknown kind must be the only subject without a key; got %d keys for %d subjects", len(want), len(subjects))
	}
}

// The fold and the matcher run on every request: nothing is allocated for a
// user, and one string (the namespace group's name) per matcher for a
// ServiceAccount, however many subjects are then matched with it.
func TestIssue1443_FoldAndMatchAllocations(t *testing.T) {
	groups := []string{"devs", "ops", "system:authenticated"}
	subjects := []rbacv1.Subject{
		{Kind: "Group", Name: "nobody"}, {Kind: "User", Name: "nobody"},
		{Kind: "ServiceAccount", Name: "nobody", Namespace: "apps"}, {Kind: "Group", Name: "system:serviceaccounts:apps"},
	}
	for _, c := range []struct {
		name, username string
		want           float64
		wantHit        bool
	}{{"user", "alice", 0, false}, {"serviceaccount", m1443SA, 1, true}} {
		var sink uint64
		var hit bool
		if n := testing.AllocsPerRun(200, func() { sink += RBACSubGenForSubject(c.username, groups) }); n != c.want {
			t.Errorf("%s: RBACSubGenForSubject allocates %v times per call, want %v", c.name, n, c.want)
		}
		if n := testing.AllocsPerRun(200, func() {
			m := NewSubjectMatcher(c.username, groups)
			for _, s := range subjects {
				ok, _ := m.Match(s)
				hit = hit || ok
			}
		}); n != c.want {
			t.Errorf("%s: one matcher over %d subjects allocates %v times, want %v", c.name, len(subjects), n, c.want)
		}
		if hit != c.wantHit {
			t.Errorf("%s: matched its namespace group = %v, want %v", c.name, hit, c.wantHit)
		}
		_ = sink
	}
}
