// issue1046_adversarial_shapes_test.go — #1046, the PR #1048 security review's
// adversarial arm, adopted. Grant shapes the parity matrix
// (issue1046_object_check_parity_test.go) does not hold, each checked in the
// same four modes (cache on: EvaluateRBAC(ObjectCheck) and UserCan; cache off:
// the SelfSubjectAccessReview and the EvaluateRBAC fallback to it), twice so
// the snapshot authz memo is cold and then warm:
//
//   - a name-scoped get merged with a namespace-wide list, across two Roles and
//     inside one Role;
//   - the verb wildcard with resourceNames (all item verbs, no collection verb,
//     and never an empty name);
//   - resourceNames: ["*"], which upstream matches LITERALLY (not a wildcard);
//   - a name-scoped grant to a Group;
//   - a name-scoped ClusterRole bound by a RoleBinding (its namespace only,
//     never cluster scope);
//   - a grant in another namespace only;
//   - one Role holding the SAME verb twice, name-scoped first and unscoped
//     second (a name mismatch on the first rule must not end the walk);
//   - (#1334) grants to the implicit groups system:authenticated and
//     system:unauthenticated, requested by an identity presenting
//     system:unauthenticated and by an impersonated system:anonymous with NO
//     groups. The apiserver (impersonation.go:126-157, k8s.io/apiserver
//     v0.36.5) gives the first no system:authenticated, and the second exactly
//     [system:unauthenticated]; an authenticated control holds only
//     system:authenticated. These rows are the only ones whose verdict
//     depends on the implicit group, so they are what judges the model's
//     requestGroups: a model that always adds system:authenticated, or one
//     that adds nothing for system:anonymous, disagrees with the hand table.
//
// TWO ORACLES. Every case carries a hand-written expectation derived from the
// upstream authorizer (RuleAllows + the RequestInfo name of a plain item or
// collection URL), and the k8sauthz model must agree with it — so the model is
// not the only ground truth here.

package evaltest

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"

	authv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const i1046AdvNS, i1046AdvOther = "team-a", "team-b"

var i1046AdvGroups = map[string][]string{
	"r-merge-2roles": nil,
	"r-merge-1role":  nil,
	"r-wildverb":     nil,
	"r-starname":     nil,
	"r-group":        {"g-scoped"},
	"r-cr-via-rb":    nil,
	"r-other-ns":     nil,
	"r-dup":          nil,
	// #1334
	i1046AdvPublic: {"system:unauthenticated"},
	i1046AdvAnon:   nil,
}

// #1334 identities: a user presenting system:unauthenticated, and
// system:anonymous with no groups.
const i1046AdvPublic, i1046AdvAnon = "r-public", "system:anonymous"

func i1046AdvSeed() []runtime.Object {
	g, r := []string{i1046GVR.Group}, []string{i1046GVR.Resource}
	return []runtime.Object{
		// merge across two Roles: name-scoped get on x + namespace-wide list
		role(i1046AdvNS, "rv-get-x", ruleNamed(g, r, []string{"get"}, []string{"x"})),
		role(i1046AdvNS, "rv-list", rule(g, r, []string{"list"})),
		roleBinding(i1046AdvNS, "rv-m2a", "Role", "rv-get-x", userSubject("r-merge-2roles")),
		roleBinding(i1046AdvNS, "rv-m2b", "Role", "rv-list", userSubject("r-merge-2roles")),
		// merge inside ONE Role: name-scoped rule first, then an unscoped one
		role(i1046AdvNS, "rv-mixed", ruleNamed(g, r, []string{"get", "update"}, []string{"x"}), rule(g, r, []string{"list", "watch"})),
		roleBinding(i1046AdvNS, "rv-m1", "Role", "rv-mixed", userSubject("r-merge-1role")),
		// verb wildcard with resourceNames
		role(i1046AdvNS, "rv-wild", ruleNamed(g, r, []string{"*"}, []string{"x"})),
		roleBinding(i1046AdvNS, "rv-w", "Role", "rv-wild", userSubject("r-wildverb")),
		// resourceNames "*" is NOT a wildcard upstream
		role(i1046AdvNS, "rv-star", ruleNamed(g, r, []string{"get"}, []string{"*"})),
		roleBinding(i1046AdvNS, "rv-s", "Role", "rv-star", userSubject("r-starname")),
		// group subject, name-scoped
		roleBinding(i1046AdvNS, "rv-g", "Role", "rv-get-x", groupSubject("g-scoped")),
		// name-scoped ClusterRole bound by a RoleBinding: only in its namespace
		clusterRole("rv-cr-x", ruleNamed(g, r, []string{"get", "delete"}, []string{"x"})),
		roleBinding(i1046AdvNS, "rv-crrb", "ClusterRole", "rv-cr-x", userSubject("r-cr-via-rb")),
		// grant in another namespace only
		role(i1046AdvOther, "rv-get-x-other", ruleNamed(g, r, []string{"get"}, []string{"x"})),
		roleBinding(i1046AdvOther, "rv-o", "Role", "rv-get-x-other", userSubject("r-other-ns")),
		// SAME verb: name-scoped rule first, unscoped rule second, one Role
		role(i1046AdvNS, "rv-dup", ruleNamed(g, r, []string{"get"}, []string{"x"}), rule(g, r, []string{"get"})),
		roleBinding(i1046AdvNS, "rv-d", "Role", "rv-dup", userSubject("r-dup")),
		// #1334: one object per implicit group
		role(i1046AdvNS, "rv-get-authonly", ruleNamed(g, r, []string{"get"}, []string{"auth-only"})),
		roleBinding(i1046AdvNS, "rv-authn", "Role", "rv-get-authonly", groupSubject("system:authenticated")),
		role(i1046AdvNS, "rv-get-unauthonly", ruleNamed(g, r, []string{"get"}, []string{"unauth-only"})),
		roleBinding(i1046AdvNS, "rv-unauthn", "Role", "rv-get-unauthonly", groupSubject("system:unauthenticated")),
	}
}

type i1046AdvCase struct {
	user, verb, ns, name string
	want                 bool
}

// i1046AdvCases are hand-written from upstream RuleAllows + RequestInfo, NOT
// from the model: name is the object of the request; the request NAME the
// apiserver sees is it for an item URL (get/update/patch/delete) and "" for a
// collection URL (list/watch/create/deletecollection).
var i1046AdvCases = []i1046AdvCase{
	{"r-merge-2roles", "get", i1046AdvNS, "x", true},
	{"r-merge-2roles", "get", i1046AdvNS, "y", false},
	{"r-merge-2roles", "list", i1046AdvNS, "x", true},
	{"r-merge-2roles", "watch", i1046AdvNS, "x", false},
	{"r-merge-2roles", "create", i1046AdvNS, "x", false},
	{"r-merge-1role", "get", i1046AdvNS, "x", true},
	{"r-merge-1role", "update", i1046AdvNS, "x", true},
	{"r-merge-1role", "get", i1046AdvNS, "y", false},
	{"r-merge-1role", "update", i1046AdvNS, "y", false},
	{"r-merge-1role", "list", i1046AdvNS, "y", true},
	{"r-merge-1role", "watch", i1046AdvNS, "y", true},
	{"r-merge-1role", "delete", i1046AdvNS, "x", false},
	{"r-wildverb", "get", i1046AdvNS, "x", true},
	{"r-wildverb", "delete", i1046AdvNS, "x", true},
	{"r-wildverb", "patch", i1046AdvNS, "x", true},
	{"r-wildverb", "get", i1046AdvNS, "y", false},
	{"r-wildverb", "list", i1046AdvNS, "x", false},
	{"r-wildverb", "watch", i1046AdvNS, "x", false},
	{"r-wildverb", "create", i1046AdvNS, "x", false},
	{"r-wildverb", "deletecollection", i1046AdvNS, "x", false},
	{"r-wildverb", "get", i1046AdvNS, "", false}, // empty name never matches [x]
	{"r-starname", "get", i1046AdvNS, "x", false},
	{"r-starname", "get", i1046AdvNS, "*", true}, // literal match only
	{"r-group", "get", i1046AdvNS, "x", true},
	{"r-group", "get", i1046AdvNS, "y", false},
	{"r-group", "list", i1046AdvNS, "x", false},
	{"r-cr-via-rb", "get", i1046AdvNS, "x", true},
	{"r-cr-via-rb", "delete", i1046AdvNS, "x", true},
	{"r-cr-via-rb", "get", i1046AdvOther, "x", false},
	{"r-cr-via-rb", "get", "", "x", false}, // cluster scope: RB grants nothing
	{"r-other-ns", "get", i1046AdvNS, "x", false},
	{"r-other-ns", "get", i1046AdvOther, "x", true},
	{"r-other-ns", "get", i1046AdvOther, "y", false},
	{"r-dup", "get", i1046AdvNS, "x", true},
	{"r-dup", "get", i1046AdvNS, "y", true},
	{"r-dup", "list", i1046AdvNS, "y", false},
	// #1334: system:unauthenticated presented — no system:authenticated
	{i1046AdvPublic, "get", i1046AdvNS, "auth-only", false},
	{i1046AdvPublic, "get", i1046AdvNS, "unauth-only", true},
	// #1334: impersonated system:anonymous, no groups — gets
	// system:unauthenticated (impersonation.go:145-157), never
	// system:authenticated (126)
	{i1046AdvAnon, "get", i1046AdvNS, "auth-only", false},
	{i1046AdvAnon, "get", i1046AdvNS, "unauth-only", true},
	// control: an authenticated user holds system:authenticated only
	{"r-starname", "get", i1046AdvNS, "auth-only", true},
	{"r-starname", "get", i1046AdvNS, "unauth-only", false},
}

func TestIssue1046_ObjectCheck_AdversarialShapes(t *testing.T) {
	seed := i1046AdvSeed()
	oracle := k8sauthz.New(seed...)
	newTestWatcher(t, seed...)
	restore := rbac.SetSARClientsetForTest(func(_ context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error) {
		user := ep.Token
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
			ra := action.(ktesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview).Spec.ResourceAttributes
			ok := ra != nil && oracle.Allowed(k8sauthz.Attributes{User: user, Groups: i1046AdvGroups[user], Verb: ra.Verb,
				Group: ra.Group, Resource: ra.Resource, Namespace: ra.Namespace, Name: ra.Name})
			return true, &authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: ok}}, nil
		})
		return cs, nil
	})
	t.Cleanup(restore)
	t.Setenv("TEST_MODE", "true")

	ctx := func(u string) context.Context {
		return xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: u, Groups: i1046AdvGroups[u]}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test", Token: u}))
	}
	type mode struct {
		name    string
		cacheOn bool
		f       func(c i1046AdvCase) bool
	}
	viaOC := func(c i1046AdvCase) bool {
		ok, _, err := rbac.EvaluateRBAC(ctx(c.user), rbac.ObjectCheck(c.user, i1046AdvGroups[c.user], c.verb,
			rbac.ObjectTarget{Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: c.ns, Name: c.name}))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		return ok
	}
	viaUC := func(c i1046AdvCase) bool {
		return rbac.UserCan(ctx(c.user), rbac.UserCanOptions{Verb: c.verb, GroupResource: i1046GVR.GroupResource(),
			Namespace: c.ns, Name: c.name})
	}
	modes := []mode{{"on/EvaluateRBAC", true, viaOC}, {"on/UserCan", true, viaUC},
		{"off/UserCan(SAR)", false, viaUC}, {"off/EvaluateRBAC→SAR", false, viaOC}}
	n := 0
	for _, m := range modes {
		if m.cacheOn {
			t.Setenv("CACHE_ENABLED", "true")
		} else {
			t.Setenv("CACHE_ENABLED", "false")
		}
		// run twice so the snapshot authz memo is warm on pass 2
		for pass := 0; pass < 2; pass++ {
			for _, c := range i1046AdvCases {
				model := oracle.Allowed(k8sauthz.Attributes{User: c.user, Groups: i1046AdvGroups[c.user], Verb: c.verb,
					Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: c.ns, Name: k8sauthz.RequestName(c.verb, c.name)})
				if model != c.want {
					t.Errorf("MODEL disagrees with hand table: %+v model=%v", c, model)
				}
				if got := m.f(c); got != c.want {
					t.Errorf("%s pass%d: %s %s %s/%q = %v, want %v", m.name, pass, c.user, c.verb, c.ns, c.name, got, c.want)
				}
				n++
			}
		}
	}
	t.Logf("%d checks", n)
}
