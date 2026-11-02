// issue1049_collection_check_parity_test.go — #1049: a list or watch is
// authorized with the Name Kubernetes gives it, with the cache on and off.
//
// THE GAP. Kubernetes' RequestInfo takes a list's or watch's Name from a
// `fieldSelector=metadata.name=<x>` (k8s.io/apiserver pkg/endpoints/request,
// requestinfo.go), and ResourceNameMatches ignores the verb, so a
// `list resourceNames: [x]` grant allows `?fieldSelector=metadata.name=x`.
// Snowplow checked every list without a name and its evaluator refused a
// resourceNames rule for any collection verb: 403 with the cache on, 200 from
// the apiserver.
//
// THE GROUND TRUTH IS THE VENDORED RESOLVER. Every row's expected verb and
// Name come from request.RequestInfoFactory.NewRequestInfo on the same URL —
// the function the apiserver runs — never from snowplow's code. The verdict
// oracle is internal/testsupport/k8sauthz, fed those attributes.
//
//   - TestIssue1049_CollectionRead_ParityWithRequestInfo: rbac.CollectionRead
//     vs NewRequestInfo over a table of queries × {namespaced, cluster-wide}.
//   - TestIssue1049_CollectionCheck_ParityWithKubernetes: EvaluateRBAC of
//     rbac.CollectionCheck vs the oracle, grants × queries × scopes, with the
//     cache on (in-process evaluator) and off (the SelfSubjectAccessReview
//     fallback, whose fake answers with the oracle on what snowplow SENT).
//   - TestIssue1049_NameScopedListGrant_Arms: the issue's arms by name.

package evaltest

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
)

const (
	i1049NS = "team-a"
	i1049X  = "x"
	i1049Y  = "y"
)

// i1049Queries are raw query strings, as written on a collection URL.
var i1049Queries = []struct{ name, raw string }{
	{"no query", ""},
	{"name=x", "fieldSelector=metadata.name%3Dx"},
	{"name==x", "fieldSelector=metadata.name%3D%3Dx"},
	{"name=y", "fieldSelector=metadata.name%3Dy"},
	{"name!=y", "fieldSelector=metadata.name!%3Dy"},
	{"name!=x", "fieldSelector=metadata.name!%3Dx"},
	{"empty name value", "fieldSelector=metadata.name%3D"},
	{"namespace term only", "fieldSelector=metadata.namespace%3Dteam-a"},
	{"namespace then name=x", "fieldSelector=" + url.QueryEscape("metadata.namespace=team-a,metadata.name=x")},
	{"name=x then name=y (sorted: x)", "fieldSelector=" + url.QueryEscape("metadata.name=x,metadata.name=y")},
	{"name=y then name=x (sorted: x)", "fieldSelector=" + url.QueryEscape("metadata.name=y,metadata.name=x")},
	{"name!=y then name=x", "fieldSelector=" + url.QueryEscape("metadata.name!=y,metadata.name=x")},
	{"other field", "fieldSelector=status.phase%3DRunning"},
	{"other field named name", "fieldSelector=spec.name%3Dx"},
	{"label selector only", "labelSelector=name%3Dx"},
	{"label + name=x", "labelSelector=app%3Dweb&fieldSelector=metadata.name%3Dx"},
	{"spaces around name=x", "fieldSelector=" + url.QueryEscape(" metadata.name = x ")},
	{"escaped comma value", "fieldSelector=" + url.QueryEscape(`metadata.name=a\,b`)},
	{"name=. (not a path segment)", "fieldSelector=metadata.name%3D."},
	{"name=.. (not a path segment)", "fieldSelector=metadata.name%3D.."},
	{"name with / (not a path segment)", "fieldSelector=" + url.QueryEscape("metadata.name=a/b")},
	{"name with %% (not a path segment)", "fieldSelector=" + url.QueryEscape("metadata.name=a%b")},
	{"repeated fieldSelector", "fieldSelector=metadata.name%3Dx&fieldSelector=metadata.name%3Dy"},
	{"invalid field selector", "fieldSelector=metadata.name"},
	{"invalid label selector drops the name", "labelSelector=" + url.QueryEscape("env in (") + "&fieldSelector=metadata.name%3Dx"},
	{"undecodable limit drops the name", "limit=abc&fieldSelector=metadata.name%3Dx"},
	{"limit + name=x", "limit=5&fieldSelector=metadata.name%3Dx"},
	{"watch=true + name=x", "watch=true&fieldSelector=metadata.name%3Dx"},
	{"watch=1 + name=x", "watch=1&fieldSelector=metadata.name%3Dx"},
	{"watch=false + name=x", "watch=false&fieldSelector=metadata.name%3Dx"},
	{"watch=0", "watch=0"},
	{"watch=true, no selector", "watch=true"},
	{"undecodable watch with name=x", "watch=bogus&fieldSelector=metadata.name%3Dx"},
	{"watch=true + undecodable limit", "watch=true&limit=abc&fieldSelector=metadata.name%3Dx"},
	{"malformed pair skipped", "%zz&fieldSelector=metadata.name%3Dx"},
}

// i1049URL is the collection URL of restactions in ns ("" = cluster-wide).
func i1049URL(ns, raw string) string {
	p := "/apis/" + i1046GVR.Group + "/" + i1046GVR.Version + "/"
	if ns != "" {
		p += "namespaces/" + ns + "/"
	}
	p += i1046GVR.Resource
	if raw != "" {
		p += "?" + raw
	}
	return p
}

// i1049Upstream is what the apiserver authorizes for GET url: the vendored
// RequestInfo resolver, configured as the kube-apiserver configures it.
func i1049Upstream(t *testing.T, rawURL string) *request.RequestInfo {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequest(%q): %v", rawURL, err)
	}
	f := request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	info, err := f.NewRequestInfo(req)
	if err != nil {
		t.Fatalf("NewRequestInfo(%q): %v", rawURL, err)
	}
	if !info.IsResourceRequest || info.Resource != i1046GVR.Resource || info.Subresource != "" {
		t.Fatalf("NewRequestInfo(%q) is not a collection read of %s: %+v", rawURL, i1046GVR.Resource, info)
	}
	return info
}

func TestIssue1049_CollectionRead_ParityWithRequestInfo(t *testing.T) {
	named, unnamed, watches := 0, 0, 0
	for _, ns := range []string{i1049NS, ""} {
		for _, q := range i1049Queries {
			u := i1049URL(ns, q.raw)
			info := i1049Upstream(t, u)
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			verb, name := rbac.CollectionRead(req.URL.RawQuery)
			if verb != info.Verb || name != info.Name {
				t.Errorf("%s [ns=%q]: CollectionRead = (%s, %q), RequestInfo = (%s, %q)",
					q.name, ns, verb, name, info.Verb, info.Name)
			}
			if info.Namespace != ns {
				t.Fatalf("%s: RequestInfo namespace %q, want %q", q.name, info.Namespace, ns)
			}
			if info.Name != "" {
				named++
			} else {
				unnamed++
			}
			if info.Verb == "watch" {
				watches++
			}
		}
	}
	// The table must discriminate: upstream gives both named and unnamed
	// reads, and both verbs, or a constant answer could pass.
	if named == 0 || unnamed == 0 || watches == 0 {
		t.Fatalf("PRE: the table is not discriminating (named=%d unnamed=%d watches=%d)", named, unnamed, watches)
	}
	t.Logf("%d named, %d unnamed (%d watch) rows agree with the vendored RequestInfo", named, unnamed, watches)
}

// i1049Users: one requester per grant shape.
var i1049Users = []string{"u-list-x", "u-listwatch-x", "u-cluster-listwatch-x", "u-list-x-y", "u-nswide", "u-mixed", "u-none"}

func i1049Seed() []runtime.Object {
	g, r := []string{i1046GVR.Group}, []string{i1046GVR.Resource}
	scoped := func(verbs []string, names ...string) rbacv1.PolicyRule {
		return rbacv1.PolicyRule{APIGroups: g, Resources: r, Verbs: verbs, ResourceNames: names}
	}
	return []runtime.Object{
		role(i1049NS, "list-x", scoped([]string{"list"}, i1049X)),
		roleBinding(i1049NS, "rb-list-x", "Role", "list-x", userSubject("u-list-x")),
		role(i1049NS, "listwatch-x", scoped([]string{"list", "watch"}, i1049X)),
		roleBinding(i1049NS, "rb-listwatch-x", "Role", "listwatch-x", userSubject("u-listwatch-x")),
		clusterRole("cr-listwatch-x", scoped([]string{"list", "watch"}, i1049X)),
		clusterRoleBinding("crb-listwatch-x", "cr-listwatch-x", userSubject("u-cluster-listwatch-x")),
		role(i1049NS, "list-x-y", scoped([]string{"list"}, i1049X, i1049Y)),
		roleBinding(i1049NS, "rb-list-x-y", "Role", "list-x-y", userSubject("u-list-x-y")),
		role(i1049NS, "all", rule(g, r, []string{"list", "watch"})),
		roleBinding(i1049NS, "rb-nswide", "Role", "all", userSubject("u-nswide")),
		// u-mixed: a name-scoped `get x`, then an unscoped `watch` — a name
		// mismatch on the first rule must not end the walk.
		role(i1049NS, "mixed", scoped([]string{"get", "list"}, "other"), rule(g, r, []string{"watch"})),
		roleBinding(i1049NS, "rb-mixed", "Role", "mixed", userSubject("u-mixed")),
		role(i1049NS, "pods", rule([]string{""}, []string{"pods"}, []string{"list"})),
		roleBinding(i1049NS, "rb-none", "Role", "pods", userSubject("u-none")),
	}
}

// i1049Truth is Kubernetes' verdict for user's GET of the collection URL.
func i1049Truth(t *testing.T, oracle *k8sauthz.Authorizer, user, ns, raw string) bool {
	info := i1049Upstream(t, i1049URL(ns, raw))
	return oracle.Allowed(k8sauthz.Attributes{User: user, Verb: info.Verb, Group: i1046GVR.Group,
		Resource: i1046GVR.Resource, Namespace: info.Namespace, Name: info.Name})
}

func i1049Evaluate(t *testing.T, user, ns, raw string) bool {
	t.Helper()
	ok, _, err := rbac.EvaluateRBAC(i1046Ctx(user), rbac.CollectionCheck(user, nil, i1046GVR, ns, raw))
	if err != nil {
		t.Fatalf("EvaluateRBAC(%s, ns=%q, %q): %v", user, ns, raw, err)
	}
	return ok
}

func i1049Setup(t *testing.T) *k8sauthz.Authorizer {
	seed := i1049Seed()
	oracle := k8sauthz.New(seed...)
	newTestWatcher(t, seed...)
	i1046SARFake(t, oracle)
	t.Setenv("TEST_MODE", "true")
	return oracle
}

func TestIssue1049_CollectionCheck_ParityWithKubernetes(t *testing.T) {
	oracle := i1049Setup(t)
	allowedNamed := 0
	for _, cacheOn := range []bool{true, false} {
		mode := "cache-off/SAR"
		if cacheOn {
			mode = "cache-on/evaluator"
			t.Setenv("CACHE_ENABLED", "true")
		} else {
			t.Setenv("CACHE_ENABLED", "false")
		}
		for _, user := range i1049Users {
			for _, ns := range []string{i1049NS, ""} {
				for _, q := range i1049Queries {
					want := i1049Truth(t, oracle, user, ns, q.raw)
					got := i1049Evaluate(t, user, ns, q.raw)
					if got != want {
						t.Errorf("%s: %s GET %s = %v, Kubernetes says %v", mode, user, i1049URL(ns, q.raw), got, want)
					}
					if want && (strings.HasPrefix(user, "u-list") || user == "u-cluster-listwatch-x") {
						allowedNamed++
					}
				}
			}
		}
	}
	// Discrimination: name-scoped requesters must be allowed somewhere (the
	// #1049 cells), or the matrix would pass a constant deny — the old code.
	if allowedNamed == 0 {
		t.Fatalf("PRE: no name-scoped requester is ever allowed — the matrix cannot see #1049")
	}
}

// TestIssue1049_NameScopedListGrant_Arms states the issue's arms by name, in
// both cache modes, against hard-coded expectations (each also checked
// against the vendored resolver + oracle, so a wrong expectation fails too).
func TestIssue1049_NameScopedListGrant_Arms(t *testing.T) {
	oracle := i1049Setup(t)
	q := func(sel string) string { return "fieldSelector=" + url.QueryEscape(sel) }
	arms := []struct {
		arm, user, ns, raw string
		want               bool
	}{
		{"list name=x, list grant on x", "u-list-x", i1049NS, q("metadata.name=x"), true},
		{"list name==x, list grant on x", "u-list-x", i1049NS, q("metadata.name==x"), true},
		{"watch name=x, list+watch grant on x", "u-listwatch-x", i1049NS, "watch=true&" + q("metadata.name=x"), true},
		{"watch name=x, list-only grant: watch is not granted", "u-list-x", i1049NS, "watch=true&" + q("metadata.name=x"), false},
		{"cluster-wide list name=x, ClusterRole on x", "u-cluster-listwatch-x", "", q("metadata.name=x"), true},
		{"cluster-wide list name=x, Role on x: RoleBindings grant no cluster scope", "u-list-x", "", q("metadata.name=x"), false},
		{"list name=y, grant on x", "u-list-x", i1049NS, q("metadata.name=y"), false},
		{"list name=y, grant on x and y", "u-list-x-y", i1049NS, q("metadata.name=y"), true},
		{"list, no selector, grant on x", "u-list-x", i1049NS, "", false},
		{"list name!=y, grant on x", "u-list-x", i1049NS, q("metadata.name!=y"), false},
		{"list empty name, grant on x", "u-list-x", i1049NS, q("metadata.name="), false},
		{"list namespace selector, grant on x", "u-list-x", i1049NS, q("metadata.namespace=team-a"), false},
		{"list namespace,name=x, grant on x", "u-list-x", i1049NS, q("metadata.namespace=team-a,metadata.name=x"), true},
		{"list other field, grant on x", "u-list-x", i1049NS, q("status.phase=Running"), false},
		{"list label selector, grant on x", "u-list-x", i1049NS, "labelSelector=name%3Dx", false},
		{"list name=x,name=y, grant on x (terms sorted: x)", "u-list-x", i1049NS, q("metadata.name=x,metadata.name=y"), true},
		{"list name=y,name=x, grant on x (terms sorted: x)", "u-list-x", i1049NS, q("metadata.name=y,metadata.name=x"), true},
		{"list name=z,name=y, grant on x (terms sorted: y)", "u-list-x", i1049NS, q("metadata.name=z,metadata.name=y"), false},
		{"list name=x + undecodable limit, grant on x", "u-list-x", i1049NS, "limit=abc&" + q("metadata.name=x"), false},
		{"list, no selector, namespace-wide grant", "u-nswide", i1049NS, "", true},
		{"list name=x, unrelated grant", "u-none", i1049NS, q("metadata.name=x"), false},
	}
	for _, a := range arms {
		if truth := i1049Truth(t, oracle, a.user, a.ns, a.raw); truth != a.want {
			t.Fatalf("arm %q: expectation %v contradicts Kubernetes (%v)", a.arm, a.want, truth)
		}
	}
	for _, cacheOn := range []bool{true, false} {
		mode := "cache-off"
		if cacheOn {
			mode = "cache-on"
			t.Setenv("CACHE_ENABLED", "true")
		} else {
			t.Setenv("CACHE_ENABLED", "false")
		}
		for _, a := range arms {
			if got := i1049Evaluate(t, a.user, a.ns, a.raw); got != a.want {
				t.Errorf("%s: %s = %v, want %v", mode, a.arm, got, a.want)
			}
		}
	}
}

// TestIssue1049_ObjectCheck_StillCarriesNoNameForCollectionVerbs pins that the
// evaluator's new upstream rule (no verb gate) cannot be reached through
// ObjectCheck by a collection verb: a `list`/`create resourceNames: [x]` rule
// must not grant a per-object check, whatever object it is about.
func TestIssue1049_ObjectCheck_StillCarriesNoNameForCollectionVerbs(t *testing.T) {
	i1049Setup(t)
	t.Setenv("CACHE_ENABLED", "true")
	for _, verb := range []string{"list", "watch", "create", "deletecollection"} {
		opts := rbac.ObjectCheck("u-listwatch-x", nil, verb, rbac.TargetOf(i1046GVR, i1049NS, i1049X))
		if opts.Name != "" {
			t.Errorf("ObjectCheck(%s).Name = %q, want \"\"", verb, opts.Name)
		}
		ok, _, err := rbac.EvaluateRBAC(i1046Ctx("u-listwatch-x"), opts)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("ObjectCheck(%s, x) allowed by a resourceNames:[x] rule — the collection URL names no object", verb)
		}
	}
}
