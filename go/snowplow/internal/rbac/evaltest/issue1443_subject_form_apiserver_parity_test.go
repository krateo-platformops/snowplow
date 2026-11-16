// issue1443_subject_form_apiserver_parity_test.go — #1443: every FORM of binding
// subject grants to the same requesters in snowplow as at the apiserver.
//
// The #1443 parity test compares snowplow's evaluator with snowplow's key fold,
// and both read one enumeration (cache.SubjectMatcher). A subject form dropped
// from that enumeration moves both together, so it needs a reference outside
// snowplow. The #1334 parity test gives one for GROUP subjects. This one covers
// the other forms, the User form of a ServiceAccount username in particular (a
// User-kind subject naming system:serviceaccount:<ns>:<name>), through both
// routes the evaluator takes: a ClusterRoleBinding and a RoleBinding.
//
// THE REFERENCE is the suite's apiserver authorizer (testsupport/k8sauthz): the
// requester's groups come from the apiserver's real impersonation filter and
// the subject match is the authorizer's rule.
package evaltest

import (
	"context"
	"fmt"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"
)

const (
	f1443APIGroup = "f1443.test"
	f1443NS       = "ns-f1443"
	f1443SA       = "system:serviceaccount:ns1:sa1"
)

func f1443Subjects() []rbacv1.Subject {
	return []rbacv1.Subject{
		userSubject("alice"),
		userSubject(f1443SA), // the User form of a ServiceAccount username
		userSubject("system:serviceaccount:ns2:sa1"),
		userSubject("system:serviceaccount:ns1:sa2"),
		userSubject("system:anonymous"),
		userSubject("krateo:public"),
		userSubject("sa1"),
		groupSubject("devs"),
		groupSubject("system:authenticated"),
		groupSubject("system:unauthenticated"),
		groupSubject("system:serviceaccounts"),
		groupSubject("system:serviceaccounts:ns1"),
		groupSubject("system:serviceaccounts:ns2"),
		groupSubject(f1443SA),
		groupSubject("alice"),
		saSubject("ns1", "sa1"),
		saSubject("ns2", "sa1"),
		saSubject("ns1", "sa2"),
	}
}

func f1443ClusterRes(i int) string { return fmt.Sprintf("cres%02d", i) }
func f1443NSRes(i int) string      { return fmt.Sprintf("nsres%02d", i) }

func f1443World() []runtime.Object {
	var objs []runtime.Object
	for i, s := range f1443Subjects() {
		cr := clusterRole(fmt.Sprintf("cr-f1443-%02d", i), rule([]string{f1443APIGroup}, []string{f1443ClusterRes(i)}, []string{"get"}))
		crb := clusterRoleBinding(fmt.Sprintf("crb-f1443-%02d", i), cr.Name, s)
		crb.UID = types.UID(fmt.Sprintf("uid-crb-f1443-%02d", i))
		r := role(f1443NS, fmt.Sprintf("r-f1443-%02d", i), rule([]string{f1443APIGroup}, []string{f1443NSRes(i)}, []string{"get"}))
		rb := roleBinding(f1443NS, fmt.Sprintf("rb-f1443-%02d", i), "Role", r.Name, s)
		rb.UID = types.UID(fmt.Sprintf("uid-rb-f1443-%02d", i))
		objs = append(objs, cr, crb, r, rb)
	}
	return objs
}

func TestIssue1443_SubjectForms_ParityWithAPIServer(t *testing.T) {
	world := f1443World()
	newTestWatcher(t, world...)
	oracle := k8sauthz.New(world...)
	subjects := f1443Subjects()

	requesters := []struct {
		name   string
		user   string
		groups []string
		// The subjects the requester must be granted through: the floor that
		// keeps an agreement of two denials from passing.
		wantVia int
	}{
		{"user with a group", "alice", []string{"devs"}, 3}, // User, devs, system:authenticated
		{"user, no groups", "alice", nil, 2},                // User, system:authenticated
		{"serviceaccount, no groups", f1443SA, nil, 5},      // User form, 2 SA groups, system:authenticated, SA subject
		{"serviceaccount, its token's groups", f1443SA, []string{"system:serviceaccounts", "system:serviceaccounts:ns1", "system:authenticated"}, 5},
		{"public identity", "krateo:public", []string{"system:unauthenticated"}, 2},
		{"system:anonymous", "system:anonymous", []string{"system:unauthenticated"}, 2},
	}
	pairs, granted := 0, 0
	for _, r := range requesters {
		t.Run(r.name, func(t *testing.T) {
			via := 0
			for i, s := range subjects {
				for _, route := range []struct{ kind, resource, ns string }{
					{"ClusterRoleBinding", f1443ClusterRes(i), "elsewhere-f1443"},
					{"RoleBinding", f1443NSRes(i), f1443NS},
				} {
					want := oracle.Allowed(k8sauthz.Attributes{User: r.user, Groups: r.groups, Verb: "get",
						Group: f1443APIGroup, Resource: route.resource, Namespace: route.ns})
					got, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
						Username: r.user, Groups: r.groups, Verb: "get",
						Group: f1443APIGroup, Resource: route.resource, Namespace: route.ns,
					})
					if err != nil {
						t.Fatalf("EvaluateRBAC: %v", err)
					}
					pairs++
					if got != want {
						t.Errorf("through a %s to %s %q (namespace %q): snowplow allows=%v, the apiserver allows=%v",
							route.kind, s.Kind, s.Name, s.Namespace, got, want)
					}
					if want {
						granted++
						if route.kind == "RoleBinding" {
							via++
						}
					}
				}
			}
			if via != r.wantVia {
				t.Errorf("the apiserver grants this requester through %d subject forms, the arm expects %d: "+
					"the reference or the world moved", via, r.wantVia)
			}
		})
	}
	if granted == 0 || pairs == 0 {
		t.Fatalf("VACUOUS: %d pairs, %d granted", pairs, granted)
	}
}
