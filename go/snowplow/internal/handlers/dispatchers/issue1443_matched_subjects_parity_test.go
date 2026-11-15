// issue1443_matched_subjects_parity_test.go — #1443: for every requester shape,
// the subjects the identity class folds are exactly the subjects through which
// the evaluator matches a binding.
//
// Neither side is read as a list. For each requester and each candidate binding
// subject, drawn from a superset of everything any shape could match:
//
//   - MATCHED: the real evaluator (rbac.EvaluateRBAC over the published
//     snapshot) allows a verb granted by ONE binding whose only subject is the
//     candidate — as a RoleBinding and as a ClusterRoleBinding, which take the
//     evaluator's two candidate routes;
//   - FOLDED: deleting that binding through the fake apiserver — the real
//     binding event, the real pending-bump record and the real flush at the
//     snapshot publish — moves the requester's sub-generation
//     (rbac.IdentityClassOf).
//
// The two must be equal for every pair. MATCHED and not FOLDED is #1443: a
// binding that decides the requester's verdict and never rotates its key.
// FOLDED and not MATCHED is the blast radius: a key rotated by a binding that
// cannot touch the requester.
//
// The requester is evaluated as the class sees it, with the apiserver's
// effective group set (rbac.WithAuthenticatedGroup). For every requester with a
// user name that is the evaluator's own rule; for the empty-username seed
// representative it is what the seed context resolves under
// (withCohortSeedContext).
package dispatchers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

const (
	p1443NS      = "p1443"
	p1443SAUser  = "system:serviceaccount:ns-a:sa0"
	p1443Human   = "p1443-human"
	p1443Team    = "p1443-team"
	p1443AllSAs  = "system:serviceaccounts"
	p1443SAsOfA  = "system:serviceaccounts:ns-a"
	p1443SAsOfB  = "system:serviceaccounts:ns-b"
	p1443Authn   = "system:authenticated"
	p1443Unauthn = "system:unauthenticated"
)

var p1443CRBGVR = schema.GroupVersionResource{Group: icsRBACGroup, Version: "v1", Resource: "clusterrolebindings"}

type p1443Requester struct {
	name   string
	user   string
	groups []string
	// The floor on distinct matched candidates, so a shape whose evaluator
	// matches nothing cannot pass by folding nothing.
	minMatched int
}

func p1443Requesters() []p1443Requester {
	return []p1443Requester{
		{"human/no-groups", p1443Human, nil, 2},                                                             // User, system:authenticated
		{"human/with-a-group", p1443Human, []string{p1443Team}, 3},                                          // + the group
		{"serviceaccount/no-groups", p1443SAUser, nil, 5},                                                   // User form, 2 SA groups, system:authenticated, SA subject
		{"serviceaccount/its-token-groups", p1443SAUser, []string{p1443AllSAs, p1443SAsOfA, p1443Authn}, 5}, // the same five
		{"serviceaccount/an-unrelated-group", p1443SAUser, []string{p1443Team}, 6},                          // + the group
		{"serviceaccount/another-namespace-group", p1443SAUser, []string{p1443SAsOfB}, 6},                   // + the presented one
		{"anonymous", "system:anonymous", []string{p1443Unauthn}, 2},                                        // User, system:unauthenticated
		{"anonymous/no-groups", "system:anonymous", nil, 2},                                                 // the same two
		{"public-identity", "krateo:public", []string{p1443Unauthn}, 2},                                     // User, system:unauthenticated
		{"seed-representative/group", "", []string{p1443Team}, 2},                                           // the group, system:authenticated
		{"seed-representative/authenticated-only", "", nil, 1},                                              // system:authenticated
	}
}

// p1443Candidates is the superset: every kind × every name any requester above
// could be matched by, plus near misses.
func p1443Candidates() []rbacv1.Subject {
	var out []rbacv1.Subject
	for _, n := range []string{
		p1443Human, p1443SAUser, "system:serviceaccount:ns-b:sa0", "system:serviceaccount:ns-a:sa1",
		"system:anonymous", "krateo:public", "p1443-someone-else", p1443Team, p1443Authn,
	} {
		out = append(out, rbacv1.Subject{Kind: "User", APIGroup: icsRBACGroup, Name: n})
	}
	for _, n := range []string{
		p1443Team, "p1443-other-team", p1443Authn, p1443Unauthn, p1443AllSAs, p1443SAsOfA, p1443SAsOfB,
		"system:masters", p1443Human, p1443SAUser, "sa0",
	} {
		out = append(out, rbacv1.Subject{Kind: "Group", APIGroup: icsRBACGroup, Name: n})
	}
	for _, s := range [][2]string{{"ns-a", "sa0"}, {"ns-b", "sa0"}, {"ns-a", "sa1"}, {p1443NS, "sa0"}, {"", "sa0"}} {
		out = append(out, rbacv1.Subject{Kind: "ServiceAccount", Namespace: s[0], Name: s[1]})
	}
	return out
}

func p1443SubjectString(s rbacv1.Subject) string {
	if s.Kind == "ServiceAccount" {
		return fmt.Sprintf("ServiceAccount %q/%q", s.Namespace, s.Name)
	}
	return fmt.Sprintf("%s %q", s.Kind, s.Name)
}

func TestIssue1443_FoldedSubjectsEqualMatchedSubjects(t *testing.T) {
	cands := p1443Candidates()
	type binding struct {
		cand      int
		clusterBD bool
		name      string
		resource  string
	}
	// One world for all requesters' evaluations: per candidate, a RoleBinding
	// and a ClusterRoleBinding, each to its own ClusterRole granting `get` on a
	// resource no other binding grants. The last candidate is a sentinel group
	// nobody presents but the readiness check below.
	const sentinelGroup = "p1443-sentinel"
	cands = append(cands, rbacv1.Subject{Kind: "Group", APIGroup: icsRBACGroup, Name: sentinelGroup})
	var roles []runtime.Object
	var bindings []binding
	var bindingObjs []runtime.Object
	for i, c := range cands {
		for _, cluster := range []bool{false, true} {
			b := binding{cand: i, clusterBD: cluster}
			if cluster {
				b.name, b.resource = fmt.Sprintf("p1443-crb-%02d", i), fmt.Sprintf("viacrb%02d", i)
			} else {
				b.name, b.resource = fmt.Sprintf("p1443-rb-%02d", i), fmt.Sprintf("viarb%02d", i)
			}
			bindings = append(bindings, b)
			roles = append(roles, &rbacv1.ClusterRole{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
				ObjectMeta: metav1.ObjectMeta{Name: b.name},
				Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{b.resource}}},
			})
			ref := rbacv1.RoleRef{APIGroup: icsRBACGroup, Kind: "ClusterRole", Name: b.name}
			if cluster {
				bindingObjs = append(bindingObjs, &rbacv1.ClusterRoleBinding{
					TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
					ObjectMeta: metav1.ObjectMeta{Name: b.name, UID: types.UID("uid-" + b.name)},
					Subjects:   []rbacv1.Subject{c}, RoleRef: ref,
				})
			} else {
				bindingObjs = append(bindingObjs, &rbacv1.RoleBinding{
					TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
					ObjectMeta: metav1.ObjectMeta{Namespace: p1443NS, Name: b.name, UID: types.UID("uid-" + b.name)},
					Subjects:   []rbacv1.Subject{c}, RoleRef: ref,
				})
			}
		}
	}
	// The roles are published at boot; the bindings are created through the
	// fake apiserver, so their later DELETE events carry the shape a watch
	// delivers (the informer drops a typed tombstone).
	dyn := icsPublish(t, roles)
	for i, o := range bindingObjs {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
		if err != nil {
			t.Fatalf("to unstructured: %v", err)
		}
		u := &unstructured.Unstructured{Object: m}
		if bindings[i].clusterBD {
			_, err = dyn.Resource(p1443CRBGVR).Create(context.Background(), u, metav1.CreateOptions{})
		} else {
			_, err = dyn.Resource(icsRBGVR).Namespace(p1443NS).Create(context.Background(), u, metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatalf("create %s: %v", bindings[i].name, err)
		}
	}
	sentinelSees := func(resource string) bool {
		ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "p1443-sentinel-user", Groups: []string{sentinelGroup}, Verb: "get", Resource: resource, Namespace: p1443NS, SkipBindingUID: true,
		})
		return ok
	}
	last := bindings[len(bindings)-2:]
	if !i444Wait(20*time.Second, func() bool { return sentinelSees(last[0].resource) && sentinelSees(last[1].resource) }) {
		t.Fatalf("SETUP: the created bindings did not reach the published snapshot")
	}
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("SETUP: RBAC rebuild did not quiesce: %v", err)
	}

	reqs := p1443Requesters()
	effective := func(r p1443Requester) []string { return rbac.WithAuthenticatedGroup(r.user, r.groups) }
	subGen := func(r p1443Requester) uint64 {
		return rbac.IdentityClassOf(jwtutil.UserInfo{Username: r.user, Groups: r.groups}).RBACSubGen
	}

	// MATCHED, for every (requester, binding), before anything is deleted.
	matched := make([][]bool, len(reqs))
	for ri, r := range reqs {
		matched[ri] = make([]bool, len(bindings))
		for bi, b := range bindings {
			ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
				Username: r.user, Groups: effective(r), Verb: "get", Resource: b.resource, Namespace: p1443NS, SkipBindingUID: true,
			})
			if err != nil {
				t.Fatalf("EvaluateRBAC(%s, %s): %v", r.name, b.name, err)
			}
			matched[ri][bi] = ok
		}
	}

	// FOLDED: delete each binding in turn; who moved?
	pairs, nMatched, nDrift := 0, 0, 0
	distinct := make([]map[int]bool, len(reqs))
	for ri := range reqs {
		distinct[ri] = map[int]bool{}
	}
	for bi, b := range bindings {
		before := make([]uint64, len(reqs))
		for ri, r := range reqs {
			before[ri] = subGen(r)
		}
		seq := cache.RBACSnapshotForTest().PublishSeq
		var err error
		if b.clusterBD {
			err = dyn.Resource(p1443CRBGVR).Delete(context.Background(), b.name, metav1.DeleteOptions{})
		} else {
			err = dyn.Resource(icsRBGVR).Namespace(p1443NS).Delete(context.Background(), b.name, metav1.DeleteOptions{})
		}
		if err != nil {
			t.Fatalf("delete %s: %v", b.name, err)
		}
		icsAwaitRepublish(t, seq, "delete of "+b.name)
		kind := "RoleBinding"
		if b.clusterBD {
			kind = "ClusterRoleBinding"
		}
		for ri, r := range reqs {
			folded := subGen(r) != before[ri]
			pairs++
			if matched[ri][bi] {
				nMatched++
				distinct[ri][b.cand] = true
			}
			switch {
			case matched[ri][bi] && !folded:
				nDrift++
				t.Errorf("MATCHED, NOT FOLDED: requester %s is matched by the evaluator through a %s to %s, and that binding's "+
					"event does not move its sub-generation — a change behind it moves the verdict and rotates no key",
					r.name, kind, p1443SubjectString(cands[b.cand]))
			case !matched[ri][bi] && folded:
				nDrift++
				t.Errorf("FOLDED, NOT MATCHED: requester %s is not matched by the evaluator through a %s to %s, and that "+
					"binding's event moves its sub-generation — its key rotates for a binding that cannot touch it",
					r.name, kind, p1443SubjectString(cands[b.cand]))
			}
		}
	}
	for ri, r := range reqs {
		if len(distinct[ri]) < r.minMatched {
			t.Errorf("VACUOUS for %s: the evaluator matched it through %d distinct candidate subjects, want at least %d",
				r.name, len(distinct[ri]), r.minMatched)
		}
	}
	t.Logf("requesters=%d candidate subjects=%d bindings=%d (requester, binding) pairs=%d of which matched=%d; "+
		"pairs where matched != folded=%d", len(reqs), len(cands), len(bindings), pairs, nMatched, nDrift)
}
