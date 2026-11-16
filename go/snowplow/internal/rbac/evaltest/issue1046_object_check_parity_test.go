// issue1046_object_check_parity_test.go — #1046: a decision about ONE object is
// the decision Kubernetes makes, with the cache on and with it off.
//
// THE MATRIX. Grants {namespace-wide, name-scoped on x (Role), name-scoped on x
// (ClusterRole via a ClusterRoleBinding), ONE Role holding a name-scoped rule on
// x followed by an unscoped `get` (a name mismatch on the first rule must not
// end the rule walk: `get y` is allowed by the second), none} × verbs {get,
// update, patch, delete, list, create} × objects {x, y}. For every cell four answers must
// equal the ground truth:
//
//   - cache on,  EvaluateRBAC(rbac.ObjectCheck(...))  (the dispatch gate, the
//     refilter, the informer GET filter, the key derivation);
//   - cache on,  rbac.UserCan(... Name)               (resourcesRefs `allowed`);
//   - cache off, rbac.UserCan(... Name)               (SelfSubjectAccessReview);
//   - cache off, EvaluateRBAC(rbac.ObjectCheck(...))  (the refilter's cache=off
//     fallback, the reverse divergence the issue reports).
//
// THE GROUND TRUTH is internal/testsupport/k8sauthz, an independent model of
// the upstream authorizer, fed the attributes the apiserver's own request
// carries (k8sauthz.RequestName: an item URL names the object, a collection URL
// does not). Under cache off the fake SelfSubjectAccessReview endpoint answers
// with the SAME model on the attributes snowplow actually SENT — so a review
// that drops the name (the pre-#1046 cache-off path) or adds one to a
// collection verb (a `create resourceNames: [x]` rule would then match, which
// the real POST never does) is caught.
//
// RED on 05eace28: every name-scoped `get/update/patch/delete x` cell, in the
// cache-off rows (the review carried no name) — the cache-on UserCan rows too.

package evaltest

import (
	"context"
	"fmt"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"

	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const (
	i1046NS = "team-a"
	i1046X  = "x"
	i1046Y  = "y"
)

var i1046GVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// i1046Users: one requester per grant shape.
var i1046Users = []string{"u-nswide", "u-name-x", "u-cluster-name-x", "u-mixed", "u-none"}

var i1046Verbs = []string{"get", "update", "patch", "delete", "list", "create"}

func i1046Seed() []runtime.Object {
	all := []string{"get", "update", "patch", "delete", "list", "create"}
	scoped := rbacv1.PolicyRule{APIGroups: []string{i1046GVR.Group}, Resources: []string{i1046GVR.Resource},
		Verbs: all, ResourceNames: []string{i1046X}}
	return []runtime.Object{
		role(i1046NS, "ra-all", rule([]string{i1046GVR.Group}, []string{i1046GVR.Resource}, all)),
		roleBinding(i1046NS, "rb-nswide", "Role", "ra-all", userSubject("u-nswide")),
		role(i1046NS, "ra-x", scoped),
		roleBinding(i1046NS, "rb-name-x", "Role", "ra-x", userSubject("u-name-x")),
		clusterRole("cr-ra-x", scoped),
		clusterRoleBinding("crb-name-x", "cr-ra-x", userSubject("u-cluster-name-x")),
		// u-mixed: ONE Role, the name-scoped rule FIRST, then an unscoped get.
		role(i1046NS, "ra-mixed", scoped, rule([]string{i1046GVR.Group}, []string{i1046GVR.Resource}, []string{"get"})),
		roleBinding(i1046NS, "rb-mixed", "Role", "ra-mixed", userSubject("u-mixed")),
		// u-none holds an unrelated grant, so "none" is not "no binding at all".
		role(i1046NS, "pods", rule([]string{""}, []string{"pods"}, []string{"get"})),
		roleBinding(i1046NS, "rb-none", "Role", "pods", userSubject("u-none")),
	}
}

// i1046SARFake answers every SelfSubjectAccessReview with the oracle, for the
// user the endpoint's token names, on the attributes snowplow sent.
func i1046SARFake(t *testing.T, oracle *k8sauthz.Authorizer) {
	t.Helper()
	restore := rbac.SetSARClientsetForTest(func(_ context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error) {
		user := ep.Token
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews",
			func(action ktesting.Action) (bool, runtime.Object, error) {
				ssar := action.(ktesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview)
				ra := ssar.Spec.ResourceAttributes
				allowed := ra != nil && oracle.Allowed(k8sauthz.Attributes{
					User: user, Verb: ra.Verb, Group: ra.Group, Resource: ra.Resource,
					Namespace: ra.Namespace, Name: ra.Name,
				})
				return true, &authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: allowed}}, nil
			})
		return cs, nil
	})
	t.Cleanup(restore)
}

func i1046Ctx(user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test", Token: user}),
	)
}

type i1046Mode struct {
	name     string
	cacheOn  bool
	evaluate func(ctx context.Context, user, verb, name string) (bool, error)
}

func i1046Modes() []i1046Mode {
	viaObjectCheck := func(ctx context.Context, user, verb, name string) (bool, error) {
		ok, _, err := rbac.EvaluateRBAC(ctx, rbac.ObjectCheck(user, nil, verb, rbac.TargetOf(i1046GVR, i1046NS, name)))
		return ok, err
	}
	viaUserCan := func(ctx context.Context, _, verb, name string) (bool, error) {
		return rbac.UserCan(ctx, rbac.UserCanOptions{Verb: verb, GroupResource: i1046GVR.GroupResource(),
			Namespace: i1046NS, Name: name}), nil
	}
	return []i1046Mode{
		{"cache-on/EvaluateRBAC(ObjectCheck)", true, viaObjectCheck},
		{"cache-on/UserCan", true, viaUserCan},
		{"cache-off/UserCan(SAR)", false, viaUserCan},
		{"cache-off/EvaluateRBAC(ObjectCheck)→SAR", false, viaObjectCheck},
	}
}

func TestIssue1046_ObjectCheck_ParityWithKubernetes(t *testing.T) {
	seed := i1046Seed()
	oracle := k8sauthz.New(seed...)
	newTestWatcher(t, seed...)
	i1046SARFake(t, oracle)
	t.Setenv("TEST_MODE", "true")

	// The matrix must discriminate: the ground truth itself has to say both
	// yes and no for a name-scoped requester, or a constant answer passes.
	if !oracle.Allowed(k8sauthz.Attributes{User: "u-name-x", Verb: "get", Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: i1046NS, Name: i1046X}) ||
		oracle.Allowed(k8sauthz.Attributes{User: "u-name-x", Verb: "get", Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: i1046NS, Name: i1046Y}) {
		t.Fatalf("PRE: the oracle must allow u-name-x `get x` and deny `get y`")
	}
	// The merged shape must discriminate too: `get y` allowed only by the
	// SECOND rule of the Role, `update y` by neither.
	if !oracle.Allowed(k8sauthz.Attributes{User: "u-mixed", Verb: "get", Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: i1046NS, Name: i1046Y}) ||
		oracle.Allowed(k8sauthz.Attributes{User: "u-mixed", Verb: "update", Group: i1046GVR.Group, Resource: i1046GVR.Resource, Namespace: i1046NS, Name: i1046Y}) {
		t.Fatalf("PRE: the oracle must allow u-mixed `get y` and deny `update y`")
	}

	mismatches := 0
	for _, mode := range i1046Modes() {
		if mode.cacheOn {
			t.Setenv("CACHE_ENABLED", "true")
		} else {
			t.Setenv("CACHE_ENABLED", "false")
		}
		for _, user := range i1046Users {
			for _, verb := range i1046Verbs {
				for _, name := range []string{i1046X, i1046Y} {
					want := oracle.Allowed(k8sauthz.Attributes{User: user, Verb: verb, Group: i1046GVR.Group,
						Resource: i1046GVR.Resource, Namespace: i1046NS, Name: k8sauthz.RequestName(verb, name)})
					got, err := mode.evaluate(i1046Ctx(user), user, verb, name)
					if err != nil {
						t.Fatalf("%s %s %s %s: %v", mode.name, user, verb, name, err)
					}
					if got != want {
						mismatches++
						t.Errorf("%s: %s `%s restactions/%s` = %v, Kubernetes says %v", mode.name, user, verb, name, got, want)
					}
				}
			}
		}
	}
	if mismatches == 0 {
		t.Logf("%d modes × %d users × %d verbs × 2 objects agree with the oracle",
			len(i1046Modes()), len(i1046Users), len(i1046Verbs))
	}
}

// TestIssue1046_ObjectCheck_CarriesNameOnlyForASingleObject pins the request
// shape ObjectCheck builds, independently of any RBAC store.
func TestIssue1046_ObjectCheck_CarriesNameOnlyForASingleObject(t *testing.T) {
	target := rbac.TargetOf(i1046GVR, i1046NS, i1046X)
	for _, verb := range append(i1046Verbs, "watch", "deletecollection") {
		opts := rbac.ObjectCheck("u", []string{"g"}, verb, target)
		want := k8sauthz.RequestName(verb, i1046X)
		if opts.Name != want {
			t.Errorf("ObjectCheck(%s).Name = %q, want %q (the name the apiserver's request carries)", verb, opts.Name, want)
		}
		got := fmt.Sprintf("%s|%v|%s|%s|%s|%s", opts.Username, opts.Groups, opts.Verb, opts.Group, opts.Resource, opts.Namespace)
		if wantRest := fmt.Sprintf("u|[g]|%s|%s|%s|%s", verb, i1046GVR.Group, i1046GVR.Resource, i1046NS); got != wantRest {
			t.Errorf("ObjectCheck(%s) = %s, want %s", verb, got, wantRest)
		}
	}
}
