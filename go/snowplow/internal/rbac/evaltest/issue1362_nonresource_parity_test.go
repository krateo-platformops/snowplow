// issue1362_nonresource_parity_test.go — #1362: snowplow's in-process RBAC
// evaluator (cache on) and its SelfSubjectAccessReview fallback (cache off)
// authorize a NON-RESOURCE request (a /debug/* path) exactly as the
// kube-apiserver authorizes the same request.
//
// GROUND TRUTH, NOT A SELF-COMPARISON.
//   - The ATTRIBUTES come from the vendored apiserver RequestInfo resolver
//     (k8s.io/apiserver pkg/endpoints/request NewRequestInfo) on the same HTTP
//     request: IsResourceRequest=false, Verb=lowercase method, Path=URL.Path.
//   - The VERDICT comes from internal/testsupport/k8sauthz, written from the
//     upstream authorizer (RuleAllows' non-resource branch, ClusterRoleBindings
//     only) and sharing no code with internal/rbac.
//   - The cache-off SSAR fake answers whichever attributes snowplow SENDS, as
//     the apiserver would. ResourceAttributes are evaluated as a resource
//     request and NonResourceAttributes as a non-resource one. A snowplow that
//     sent a non-resource check as ResourceAttributes{} would get the answer
//     the real apiserver gives that mistake: a `resources: ["*"]` reader would
//     be allowed. Echoing "deny" for any unexpected shape would hide that bug
//     (feedback_fakes_must_normalise_like_the_apiserver).
//
// evaltest only, never internal/rbac against a remote kubeconfig.

package evaltest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"

	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func nonResourceRule(urls, verbs []string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{NonResourceURLs: urls, Verbs: verbs}
}

// i1362Roles: one ClusterRole per rule shape the matrix exercises.
var i1362Roles = map[string][]rbacv1.PolicyRule{
	// The chart's grant (helm/snowplow/templates/clusterrole-debug.yaml).
	"debug-wild": {nonResourceRule([]string{"/debug/*"}, []string{"get", "post"})},
	"debug-get":  {nonResourceRule([]string{"/debug/*"}, []string{"get"})},
	"vars-exact": {nonResourceRule([]string{"/debug/vars"}, []string{"get"})},
	"pprof-pfx":  {nonResourceRule([]string{"/debug/pprof/*"}, []string{"get"})},
	// cluster-admin's non-resource rule.
	"all":       {nonResourceRule([]string{"*"}, []string{"*"})},
	"bare":      {nonResourceRule([]string{"/debug"}, []string{"get"})},
	"starstar":  {nonResourceRule([]string{"/debug/**"}, []string{"get"})},
	"verb-star": {nonResourceRule([]string{"/debug/*"}, []string{"*"})},
	// A wildcard RESOURCE reader with no nonResourceURLs (the snowplow SA's own
	// read rule). The apiserver never lets it reach a /debug path.
	"resource-wild": {rule([]string{"*"}, []string{"*"}, []string{"*"})},
	// Both shapes in one role, as two rules.
	"mixed": {
		rule([]string{""}, []string{"configmaps"}, []string{"get"}),
		nonResourceRule([]string{"/debug/vars"}, []string{"get"}),
	},
}

type i1362Identity struct {
	user   string
	groups []string
}

// i1362World binds one user per role through a ClusterRoleBinding, plus:
// a GROUP binding (ops → debug-wild); a RoleBinding of the all-powerful role,
// which the apiserver never applies to a non-resource request; and a user with
// no binding at all.
func i1362World() ([]runtime.Object, []i1362Identity) {
	var objs []runtime.Object
	var ids []i1362Identity
	for name, rules := range i1362Roles {
		objs = append(objs, clusterRole("i1362-"+name, rules...),
			clusterRoleBinding("i1362-crb-"+name, "i1362-"+name, userSubject("u-"+name)))
		ids = append(ids, i1362Identity{user: "u-" + name})
	}
	objs = append(objs,
		clusterRoleBinding("i1362-crb-ops", "i1362-debug-wild", groupSubject("ops")),
		roleBinding("default", "i1362-rb-all", "ClusterRole", "i1362-all", userSubject("u-rb-only")),
	)
	ids = append(ids,
		i1362Identity{user: "u-in-ops", groups: []string{"ops"}},
		i1362Identity{user: "u-rb-only"},
		i1362Identity{user: "u-nobody", groups: []string{"devs"}},
	)
	return objs, ids
}

// i1362Requests are (method, path) pairs: every shape the /debug mux serves,
// plus the edges of the wildcard rule.
var i1362Requests = []struct{ method, path string }{
	{http.MethodGet, "/debug/vars"},
	{http.MethodGet, "/debug/apistage"},
	{http.MethodGet, "/debug/pprof/heap"},
	{http.MethodGet, "/debug/pprof/"},
	{http.MethodPost, "/debug/shadow-parity"},
	{http.MethodGet, "/debug/shadow-parity"},
	{http.MethodHead, "/debug/vars"},
	{http.MethodPut, "/debug/vars"},
	{http.MethodGet, "/debug"},
	{http.MethodGet, "/debugger"},
	{http.MethodGet, "/debug/store"},
}

// i1362Upstream is the apiserver's RequestInfo for the request.
func i1362Upstream(t *testing.T, method, path string) *request.RequestInfo {
	t.Helper()
	req, err := http.NewRequest(method, "https://snowplow.test"+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	f := request.RequestInfoFactory{
		APIPrefixes:          sets.NewString("api", "apis"),
		GrouplessAPIPrefixes: sets.NewString("api"),
	}
	info, err := f.NewRequestInfo(req)
	if err != nil {
		t.Fatalf("NewRequestInfo(%s %s): %v", method, path, err)
	}
	return info
}

// TestIssue1362_NonResourceCheck_ParityWithRequestInfo: the check snowplow
// builds carries the attributes the apiserver derives for the same request.
func TestIssue1362_NonResourceCheck_ParityWithRequestInfo(t *testing.T) {
	for _, r := range i1362Requests {
		info := i1362Upstream(t, r.method, r.path)
		if info.IsResourceRequest {
			t.Fatalf("PRE: the apiserver treats %s %s as a resource request: %+v", r.method, r.path, info)
		}
		got := rbac.NonResourceCheck("u", []string{"g"}, r.method, r.path)
		if got.Verb != info.Verb || got.NonResourceURL != info.Path {
			t.Errorf("%s %s: snowplow (verb=%q path=%q), apiserver (verb=%q path=%q)",
				r.method, r.path, got.Verb, got.NonResourceURL, info.Verb, info.Path)
		}
		if got.Group != "" || got.Resource != "" || got.Namespace != "" || got.Name != "" {
			t.Errorf("%s %s: a non-resource check carries resource attributes: %+v", r.method, r.path, got)
		}
	}
}

// i1362SARFake answers every SSAR, as the apiserver does, for the user the
// endpoint's token names, on WHICHEVER attributes snowplow sent.
func i1362SARFake(t *testing.T, oracle *k8sauthz.Authorizer, groupsOf map[string][]string) {
	t.Helper()
	restore := rbac.SetSARClientsetForTest(func(_ context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error) {
		user := ep.Token
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews",
			func(action ktesting.Action) (bool, runtime.Object, error) {
				spec := action.(ktesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview).Spec
				attr := k8sauthz.Attributes{User: user, Groups: groupsOf[user]}
				switch {
				case spec.NonResourceAttributes != nil:
					attr.Verb, attr.Path = spec.NonResourceAttributes.Verb, spec.NonResourceAttributes.Path
				case spec.ResourceAttributes != nil:
					ra := spec.ResourceAttributes
					attr.Verb, attr.Group, attr.Resource, attr.Namespace, attr.Name =
						ra.Verb, ra.Group, ra.Resource, ra.Namespace, ra.Name
				default:
					return true, nil, fmt.Errorf("SelfSubjectAccessReview: one of resourceAttributes or nonResourceAttributes is required")
				}
				return true, &authv1.SelfSubjectAccessReview{
					Status: authv1.SubjectAccessReviewStatus{Allowed: oracle.Allowed(attr)}}, nil
			})
		return cs, nil
	})
	t.Cleanup(restore)
}

// TestIssue1362_NonResource_ParityWithAPIServer: for every identity × request,
// cache-on EvaluateRBAC and cache-off EvaluateRBAC both equal the apiserver.
func TestIssue1362_NonResource_ParityWithAPIServer(t *testing.T) {
	objs, ids := i1362World()
	oracle := k8sauthz.New(objs...)
	groupsOf := map[string][]string{}
	for _, id := range ids {
		groupsOf[id.user] = id.groups
	}

	type row struct {
		id     i1362Identity
		method string
		path   string
		want   bool
	}
	var rows []row
	allows, denies := 0, 0
	for _, id := range ids {
		for _, r := range i1362Requests {
			info := i1362Upstream(t, r.method, r.path)
			want := oracle.Allowed(k8sauthz.Attributes{User: id.user, Groups: id.groups, Verb: info.Verb, Path: info.Path})
			if want {
				allows++
			} else {
				denies++
			}
			rows = append(rows, row{id, r.method, r.path, want})
		}
	}
	// Non-vacuity: the matrix must contain both verdicts, and the oracle must
	// give the answers the upstream rule implies for the shapes this issue is
	// about. If the oracle itself drifted, these fail first.
	if allows == 0 || denies == 0 {
		t.Fatalf("vacuous matrix: %d allows, %d denies", allows, denies)
	}
	for _, c := range []struct {
		user, method, path string
		want               bool
	}{
		{"u-debug-wild", "GET", "/debug/apistage", true},
		{"u-debug-wild", "POST", "/debug/shadow-parity", true},
		{"u-debug-get", "POST", "/debug/shadow-parity", false},
		{"u-debug-wild", "GET", "/debug", false},
		{"u-debug-wild", "GET", "/debugger", false},
		{"u-vars-exact", "GET", "/debug/apistage", false},
		{"u-pprof-pfx", "GET", "/debug/pprof/heap", true},
		{"u-pprof-pfx", "GET", "/debug/vars", false},
		{"u-starstar", "GET", "/debug/vars", true},
		{"u-all", "PUT", "/debug/vars", true},
		{"u-resource-wild", "GET", "/debug/vars", false},
		{"u-rb-only", "GET", "/debug/vars", false},
		{"u-mixed", "GET", "/debug/vars", true},
	} {
		if got := oracle.Allowed(k8sauthz.Attributes{User: c.user, Groups: groupsOf[c.user],
			Verb: strings.ToLower(c.method), Path: c.path}); got != c.want {
			t.Fatalf("PRE (oracle): %s %s %s = %v, want %v", c.user, c.method, c.path, got, c.want)
		}
	}

	// cache on: the in-process evaluator over the informer-built snapshot.
	t.Run("cache-on", func(t *testing.T) {
		rbac.ResetAuthzMemoForTest()
		newTestWatcher(t, objs...)
		for _, r := range rows {
			// Twice: the second call may be served by the authz memo, which
			// must give the same answer as the cold walk.
			for pass := 0; pass < 2; pass++ {
				got, _, err := rbac.EvaluateRBAC(context.Background(),
					rbac.NonResourceCheck(r.id.user, r.id.groups, r.method, r.path))
				if err != nil {
					t.Fatalf("%s %s %s: err %v", r.id.user, r.method, r.path, err)
				}
				if got != r.want {
					t.Errorf("cache-on pass %d: %s%v %s %s = %v, apiserver says %v",
						pass, r.id.user, r.id.groups, r.method, r.path, got, r.want)
				}
			}
		}
	})

	// cache off: the requester's own SSAR, answered as the apiserver answers.
	t.Run("cache-off", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		i1362SARFake(t, oracle, groupsOf)
		for _, r := range rows {
			ctx := xcontext.BuildContext(context.Background(),
				xcontext.WithUserInfo(jwtutil.UserInfo{Username: r.id.user, Groups: r.id.groups}),
				xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test", Token: r.id.user}),
			)
			got, _, err := rbac.EvaluateRBAC(ctx, rbac.NonResourceCheck(r.id.user, r.id.groups, r.method, r.path))
			if err != nil {
				t.Fatalf("%s %s %s: err %v", r.id.user, r.method, r.path, err)
			}
			if got != r.want {
				t.Errorf("cache-off: %s%v %s %s = %v, apiserver says %v",
					r.id.user, r.id.groups, r.method, r.path, got, r.want)
			}
		}
	})
}

// TestIssue1362_MemoKeyFoldsThePath: a permit for one path, which the authz
// memo caches, must never answer a check for another path.
func TestIssue1362_MemoKeyFoldsThePath(t *testing.T) {
	rbac.ResetAuthzMemoForTest()
	newTestWatcher(t,
		clusterRole("vars-only", nonResourceRule([]string{"/debug/vars"}, []string{"get"})),
		clusterRoleBinding("vars-only", "vars-only", userSubject("scraper")),
	)
	for i := 0; i < 3; i++ {
		ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.NonResourceCheck("scraper", nil, "GET", "/debug/vars"))
		if err != nil || !ok {
			t.Fatalf("PRE: scraper GET /debug/vars = %v, %v; want permit", ok, err)
		}
	}
	if hits, _, _, _, _ := rbac.AuthzMemoStatsForTest(); hits == 0 {
		t.Fatalf("PRE: the permit was not memoised, so this arm would not exercise the memo")
	}
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.NonResourceCheck("scraper", nil, "GET", "/debug/apistage"))
	if err != nil {
		t.Fatalf("GET /debug/apistage: %v", err)
	}
	if ok {
		t.Fatal("LEAK: the memoised /debug/vars permit answered GET /debug/apistage — the memo key does not fold the path")
	}
	// And a non-resource permit never answers a RESOURCE check of the same
	// verb, whose resource fields are all empty.
	ok, _, _ = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{Username: "scraper", Verb: "get"})
	if ok {
		t.Fatal("LEAK: a non-resource permit answered a resource check")
	}
}

// TestIssue1362_UndecidableIsAnError: every way the evaluator cannot decide a
// non-resource check is a named error, never a silent deny and never a permit.
func TestIssue1362_UndecidableIsAnError(t *testing.T) {
	check := rbac.NonResourceCheck("op", nil, "GET", "/debug/vars")

	t.Run("cache-on/no-watcher", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		prev := cache.Global()
		cache.SetGlobal(nil)
		t.Cleanup(func() { cache.SetGlobal(prev) })
		ok, _, err := rbac.EvaluateRBAC(context.Background(), check)
		if ok || !errors.Is(err, rbac.ErrWatcherNotWired) {
			t.Fatalf("got (%v, %v), want (false, ErrWatcherNotWired)", ok, err)
		}
	})

	t.Run("cache-on/no-snapshot", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		prevW, prevS := cache.Global(), cache.RBACSnapshotForTest()
		cache.SetGlobal(&cache.ResourceWatcher{})
		cache.PublishRBACSnapshotForTest(nil)
		t.Cleanup(func() { cache.SetGlobal(prevW); cache.PublishRBACSnapshotForTest(prevS) })
		ok, _, err := rbac.EvaluateRBAC(context.Background(), check)
		if ok || !errors.Is(err, rbac.ErrSnapshotNotBuilt) {
			t.Fatalf("got (%v, %v), want (false, ErrSnapshotNotBuilt)", ok, err)
		}
	})

	t.Run("cache-off/no-endpoint", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "op"}))
		ok, _, err := rbac.EvaluateRBAC(ctx, check)
		if ok || !errors.Is(err, rbac.ErrReviewUnavailable) {
			t.Fatalf("got (%v, %v), want (false, ErrReviewUnavailable)", ok, err)
		}
	})

	t.Run("cache-off/create-fails", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		restore := rbac.SetSARClientsetForTest(func(context.Context, endpoints.Endpoint) (kubernetes.Interface, error) {
			cs := k8sfake.NewSimpleClientset()
			cs.PrependReactor("create", "selfsubjectaccessreviews",
				func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("connection refused")
				})
			return cs, nil
		})
		t.Cleanup(restore)
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "op"}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test", Token: "op"}))
		ok, _, err := rbac.EvaluateRBAC(ctx, check)
		if ok || !errors.Is(err, rbac.ErrReviewUnavailable) {
			t.Fatalf("got (%v, %v), want (false, ErrReviewUnavailable)", ok, err)
		}
	})
}
