package api

// issue1124_uaf_own_resource_test.go — #1124 (SECURITY) arms.
//
// THE DEFECT. A userAccessFilter with resourcesFrom kept an item if the caller
// could act on ANY plural of the stage's set in the item's namespace. For a
// stage whose items are OBJECTS of those plurals that is not Kubernetes'
// decision: a caller with list on p1s in ns-a, who can also see p2's
// CompositionDefinition, read every p2 object in ns-a. Measured on the real
// portal compositions-list and composition-detail.
//
// THE RULE (uafItemResources). An object of a checked resource (the stage's
// request names the UAF group and one of the set's plurals) is checked on that
// resource alone, in its own namespace, with the stage's verb. Any other item
// (a namespace, a name) asks a scope question, and the OR across the set stays.
//
// THE ORACLE. Every expectation comes from i24Allows, Kubernetes' RBAC decision
// written down over the grant table i24Grants: allowed iff some grant has the
// group, the resource (or "*"), the namespace and the verb. It never calls
// snowplow's evaluator. snowplow's evaluator reads RBAC objects built from the
// same table (i24RBAC); the cache=off arm's SelfSubjectAccessReviews are
// answered by i24Allows itself.
//
// ARMS. The two real RESTActions are resolved end to end, cache on (content
// cell) and cache off, from testdata/issue1124_portal_uaf_ras.yaml.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jqutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

const (
	i24Comp  = "composition.krateo.io"
	i24Core  = "core.krateo.io"
	i24Agent = "kagent.dev"
	i24Ver   = "v1-0-0"
	i24BP    = "krateo-blueprints"
	i24CDs   = "compositiondefinitions"

	i24UA  = "u-1124-a"  // list p1s in ns-a; sees no blueprint
	i24UA2 = "u-1124-a2" // list p1s in ns-a; sees the blueprints (the #1124 case)
	i24UB  = "u-1124-b"  // list p2s in ns-a and ns-b; sees the blueprints
	i24UM  = "u-1124-m"  // list p1s in ns-a, p2s in ns-b; creates p1s in ns-c, agents in ns-b
	i24UW  = "u-1124-w"  // list composition.krateo.io/* in ns-a; sees the blueprints
)

var i24Users = []string{i24UA, i24UA2, i24UB, i24UM, i24UW}

type i24Grant struct {
	group, resource, ns string
	verbs               []string
}

var i24Read = []string{"get", "list"}

var i24Grants = map[string][]i24Grant{
	i24UA:  {{i24Comp, "p1s", "ns-a", i24Read}},
	i24UA2: {{i24Comp, "p1s", "ns-a", i24Read}, {i24Core, i24CDs, i24BP, i24Read}},
	i24UB:  {{i24Comp, "p2s", "ns-a", i24Read}, {i24Comp, "p2s", "ns-b", i24Read}, {i24Core, i24CDs, i24BP, i24Read}},
	i24UM: {{i24Comp, "p1s", "ns-a", i24Read}, {i24Comp, "p2s", "ns-b", i24Read}, {i24Core, i24CDs, i24BP, i24Read},
		{i24Comp, "p1s", "ns-c", []string{"create"}}, {i24Agent, "agents", "ns-b", []string{"create"}}},
	i24UW: {{i24Comp, "*", "ns-a", i24Read}, {i24Core, i24CDs, i24BP, i24Read}},
}

// i24Allows is Kubernetes' RBAC decision over i24Grants (the oracle): a
// RoleBinding grants its rule in its own namespace only; "*" matches any
// resource. Written from Kubernetes semantics, never from snowplow's evaluator.
func i24Allows(user, verb, group, resource, ns string) bool {
	for _, g := range i24Grants[user] {
		if g.group != group || g.ns != ns || (g.resource != "*" && g.resource != resource) {
			continue
		}
		for _, v := range g.verbs {
			if v == verb {
				return true
			}
		}
	}
	return false
}

// i24RBAC is i24Grants as the RBAC objects snowplow's evaluator reads.
func i24RBAC() []runtime.Object {
	tm := func(k string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: k}
	}
	var out []runtime.Object
	users := append([]string(nil), i24Users...)
	sort.Strings(users)
	for _, u := range users {
		for i, g := range i24Grants[u] {
			name := fmt.Sprintf("cr-%s-%d", u, i)
			out = append(out,
				&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: name},
					Rules: []rbacv1.PolicyRule{{APIGroups: []string{g.group}, Resources: []string{g.resource}, Verbs: g.verbs}}},
				&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: g.ns, Name: "rb-" + name},
					Subjects: []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: u}},
					RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}})
		}
	}
	return out
}

var (
	i24GVRCD = schema.GroupVersionResource{Group: i24Core, Version: "v1alpha1", Resource: i24CDs}
	i24GVRP1 = schema.GroupVersionResource{Group: i24Comp, Version: i24Ver, Resource: "p1s"}
	i24GVRP2 = schema.GroupVersionResource{Group: i24Comp, Version: i24Ver, Resource: "p2s"}
	i24GVRNS = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
)

type i24Obj struct {
	gvr      schema.GroupVersionResource
	kind     string
	ns, name string
}

var i24Namespaces = []string{"ns-a", "ns-b", "ns-c", i24BP}

func i24Objects() []i24Obj {
	out := []i24Obj{
		{i24GVRCD, "CompositionDefinition", i24BP, "cd-p1"},
		{i24GVRCD, "CompositionDefinition", i24BP, "cd-p2"},
		{i24GVRP1, "P1", "ns-a", "a-p1-in-ns-a"},
		{i24GVRP1, "P1", "ns-b", "x-p1-in-ns-b"},
		{i24GVRP2, "P2", "ns-a", "b-p2-in-ns-a"},
		{i24GVRP2, "P2", "ns-b", "b-p2-in-ns-b"},
	}
	for _, ns := range i24Namespaces {
		out = append(out, i24Obj{i24GVRNS, "Namespace", "", ns})
	}
	return out
}

// i24Compositions are the composition objects the arms read rows of.
func i24Compositions() []i24Obj {
	var out []i24Obj
	for _, o := range i24Objects() {
		if o.gvr.Group == i24Comp {
			out = append(out, o)
		}
	}
	return out
}

func (o i24Obj) object() map[string]any {
	av := o.gvr.Version
	if o.gvr.Group != "" {
		av = o.gvr.Group + "/" + o.gvr.Version
	}
	md := map[string]any{"name": o.name, "uid": "uid-" + o.name, "creationTimestamp": "2026-11-01T00:00:00Z"}
	if o.ns != "" {
		md["namespace"] = o.ns
	}
	m := map[string]any{"apiVersion": av, "kind": o.kind, "metadata": md}
	switch o.kind {
	case "CompositionDefinition":
		res := strings.TrimPrefix(o.name, "cd-") + "s"
		m["status"] = map[string]any{"resource": res, "apiVersion": i24Comp + "/" + i24Ver, "kind": strings.ToUpper(res[:2])}
	case "Namespace":
	default:
		m["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}
	}
	return m
}

// i24Watcher is the cache=on informer layer: RBAC from i24RBAC plus every
// fixture object.
func i24Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	seed := i24RBAC()
	for _, o := range i24Objects() {
		kinds[o.gvr] = o.kind + "List"
		seed = append(seed, &unstructured.Unstructured{Object: o.object()})
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// i24SAServer is the snowplow ServiceAccount's apiserver: every fixture object,
// cluster-wide or in the path's namespace, LIST or GET by name.
func i24SAServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gvr, ns, name, ok := cache.ParseAPIServerPathToDep(r.URL.Path)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		items := []any{}
		for _, o := range i24Objects() {
			if o.gvr != gvr || (ns != "" && o.ns != ns) {
				continue
			}
			if name != "" {
				if o.name == name {
					_ = json.NewEncoder(w).Encode(o.object())
					return
				}
				continue
			}
			items = append(items, o.object())
		}
		if name != "" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": gvr.GroupVersion().String(), "kind": "List",
			"metadata": map[string]any{"resourceVersion": "1"}, "items": items})
	}))
	t.Cleanup(srv.Close)
	return srv
}

type i24Arm string

const (
	i24Content i24Arm = "cache-on/content"
	i24Off     i24Arm = "cache-off"
)

var i24Arms = []i24Arm{i24Content, i24Off}

type i24Env struct {
	t  *testing.T
	rw *cache.ResourceWatcher
	sa *httptest.Server
}

func newI24Env(t *testing.T) *i24Env {
	t.Helper()
	e := &i24Env{t: t, rw: i24Watcher(t), sa: i24SAServer(t)}
	t.Cleanup(SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: e.sa.URL, Token: "SA-EP-TOKEN"}, nil
	}))
	t.Cleanup(SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: e.sa.URL, BearerToken: "SA-IC-TOKEN"}, nil
	}))
	// cache=off: the caller's SelfSubjectAccessReviews, answered by the oracle
	// for the user the request's bearer token names.
	t.Cleanup(rbac.SetSARClientsetForTest(func(_ context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error) {
		user := strings.TrimPrefix(ep.Token, "tok:")
		cs := k8sfake.NewSimpleClientset()
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, runtime.Object, error) {
			ssar, ok := a.(k8stesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview)
			if !ok || ssar.Spec.ResourceAttributes == nil {
				return true, nil, errors.New("i24: not a resource SelfSubjectAccessReview")
			}
			ra := ssar.Spec.ResourceAttributes
			out := ssar.DeepCopy()
			out.Status.Allowed = i24Allows(user, ra.Verb, ra.Group, ra.Resource, ra.Namespace)
			return true, out, nil
		})
		return cs, nil
	}))
	return e
}

// resolve runs stages for user on arm, with a fresh L1, and returns the dict.
func (e *i24Env) resolve(arm i24Arm, user string, ra *templates.RESTAction, extras map[string]any, stages []*templates.API) map[string]any {
	t := e.t
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.SetGlobal(e.rw)
	if arm == i24Off {
		t.Setenv("CACHE_ENABLED", "false")
		cache.SetGlobal(nil)
	}
	defer cache.SetGlobal(e.rw)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)

	ctx := xcontext.BuildContext(ctxWithUser(user, "g-1124"),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: e.sa.URL, Token: "tok:" + user}))
	ctx, _ = withSinks681(ctx)
	ctx, _ = WithStageOutcomes(ctx)
	cp := make([]*templates.API, len(stages))
	for i, s := range stages {
		c := *s
		cp[i] = &c
	}
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC:      &rest.Config{Host: e.sa.URL, BearerToken: "SA-RC-TOKEN"},
		AuthnNS: "krateo-system", Items: cp, Extras: extras,
		RESTActionNamespace: ra.Namespace, RESTActionName: ra.Name,
	})
	return dict
}

// i24RA loads one vendored portal RESTAction.
func i24RA(t *testing.T, name string) *templates.RESTAction {
	t.Helper()
	b, err := os.ReadFile("testdata/issue1124_portal_uaf_ras.yaml")
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(b), 4096)
	for {
		var ra templates.RESTAction
		err := dec.Decode(&ra)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode corpus: %v", err)
		}
		if ra.Kind == "RESTAction" && ra.Name == name {
			return &ra
		}
	}
	t.Fatalf("RESTAction %s not in the corpus", name)
	return nil
}

// i24Stage returns the RESTAction's stage named name.
func i24Stage(t *testing.T, ra *templates.RESTAction, name string) *templates.API {
	t.Helper()
	for _, s := range ra.Spec.API {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("%s has no stage %s", ra.Name, name)
	return nil
}

// i24Output applies the RESTAction's top-level filter (restactions.Resolve's
// last step; that package imports this one, so it is replicated here).
func i24Output(t *testing.T, ra *templates.RESTAction, dict map[string]any) string {
	t.Helper()
	if ra.Spec.Filter == nil {
		b, _ := json.Marshal(dict)
		return string(b)
	}
	s, err := jqutil.Eval(context.TODO(), jqutil.EvalOptions{Query: *ra.Spec.Filter, Data: dict, ModuleLoader: jqsupport.ModuleLoader()})
	if err != nil {
		t.Fatalf("%s filter: %v", ra.Name, err)
	}
	return s
}

// i24Rows lists the composition objects whose uid the output carries.
func i24Rows(out string) []string {
	rows := []string{}
	for _, o := range i24Compositions() {
		if strings.Contains(out, `"uid-`+o.name+`"`) {
			rows = append(rows, o.name)
		}
	}
	sort.Strings(rows)
	return rows
}

// i24SeesBlueprints: the crds stage lists CompositionDefinitions in their own
// namespace, so the plural set a caller's resolve discovers is both or none.
func i24SeesBlueprints(user string) bool { return i24Allows(user, "list", i24Core, i24CDs, i24BP) }

// i24WantRows is Kubernetes' answer for an object list: the compositions whose
// own plural the caller may list in their own namespace (of the plurals the
// caller's resolve discovers).
func i24WantRows(user string, objs []i24Obj) []string {
	rows := []string{}
	if !i24SeesBlueprints(user) {
		return rows
	}
	for _, o := range objs {
		if i24Allows(user, "list", o.gvr.Group, o.gvr.Resource, o.ns) {
			rows = append(rows, o.name)
		}
	}
	sort.Strings(rows)
	return rows
}

func i24Has(rows []string, name string) bool {
	for _, r := range rows {
		if r == name {
			return true
		}
	}
	return false
}

// TestIssue1124_CompositionsList_EachObjectOnItsOwnPlural — the real
// compositions-list, end to end. A2 (p1s in ns-a + the blueprints) gets no p2
// row and keeps its own p1 row; the mixed caller M gets exactly the objects
// whose own plural it may list; every caller's rows equal Kubernetes' answer.
func TestIssue1124_CompositionsList_EachObjectOnItsOwnPlural(t *testing.T) {
	ra := i24RA(t, "compositions-list")
	e := newI24Env(t)
	for _, arm := range i24Arms {
		for _, u := range i24Users {
			rows := i24Rows(i24Output(t, ra, e.resolve(arm, u, ra, nil, ra.Spec.API)))
			want := i24WantRows(u, i24Compositions())
			if strings.Join(rows, ",") != strings.Join(want, ",") {
				t.Errorf("%s %s: rows %v, Kubernetes' per-object answer %v", arm, u, rows, want)
			}
			switch u {
			case i24UA2:
				if i24Has(rows, "b-p2-in-ns-a") {
					t.Errorf("%s: A2 read b-p2-in-ns-a with no grant on p2s (#1124)", arm)
				}
				if !i24Has(rows, "a-p1-in-ns-a") {
					t.Errorf("%s: A2 lost its own p1 row a-p1-in-ns-a", arm)
				}
			case i24UM:
				if strings.Join(rows, ",") != "a-p1-in-ns-a,b-p2-in-ns-b" {
					t.Errorf("%s: mixed caller rows %v, want exactly [a-p1-in-ns-a b-p2-in-ns-b]", arm, rows)
				}
			}
		}
	}
}

// TestIssue1124_CompositionDetail_EachObjectOnItsOwnPlural — the real
// composition-detail, end to end, asked for every composition by every caller:
// the page returns the object iff Kubernetes lets the caller list its plural in
// its namespace.
func TestIssue1124_CompositionDetail_EachObjectOnItsOwnPlural(t *testing.T) {
	ra := i24RA(t, "composition-detail")
	e := newI24Env(t)
	for _, arm := range i24Arms {
		for _, u := range i24Users {
			for _, o := range i24Compositions() {
				out := i24Output(t, ra, e.resolve(arm, u, ra, map[string]any{"namespace": o.ns, "name": o.name}, ra.Spec.API))
				got := i24Has(i24Rows(out), o.name)
				want := len(i24WantRows(u, []i24Obj{o})) == 1
				if got != want {
					t.Errorf("%s %s asks %s/%s: returned=%v, Kubernetes' per-object answer %v", arm, u, o.ns, o.name, got, want)
				}
			}
		}
	}
}

// TestIssue1124_GetByName_IsCheckedOnItsOwnResource — a stage that GETs one
// object by name under a two-plural set: the single-object branch checks the
// object's own resource. A2 holds get on p1s in ns-a; the p2 object there is
// not A2's to read.
func TestIssue1124_GetByName_IsCheckedOnItsOwnResource(t *testing.T) {
	ra := &templates.RESTAction{}
	ra.Name, ra.Namespace = "get-by-name-1124", "krateo-system"
	e := newI24Env(t)
	for _, arm := range i24Arms {
		for _, u := range i24Users {
			for _, o := range i24Compositions() {
				stage := &templates.API{
					Name: "obj",
					Path: "/apis/" + i24Comp + "/" + i24Ver + "/namespaces/" + o.ns + "/" + o.gvr.Resource + "/" + o.name,
					UserAccessFilter: &templates.UserAccessFilterSpec{
						Verb: "get", Group: i24Comp, ResourcesFrom: `["p1s","p2s"]`, NamespaceFrom: ".metadata.namespace",
					},
				}
				b, _ := json.Marshal(e.resolve(arm, u, ra, nil, []*templates.API{stage})["obj"])
				got := strings.Contains(string(b), `"uid-`+o.name+`"`)
				want := i24Allows(u, "get", i24Comp, o.gvr.Resource, o.ns)
				if got != want {
					t.Errorf("%s %s GETs %s/%s: returned=%v, Kubernetes %v", arm, u, o.ns, o.name, got, want)
				}
			}
		}
	}
}

// i24ScopeWant is the scope question a namespaces stage asks: the namespaces in
// which the caller may perform verb on ANY plural of the set.
func i24ScopeWant(user, verb, group string, plurals []string) []string {
	out := []string{}
	for _, ns := range i24Namespaces {
		for _, p := range plurals {
			if i24Allows(user, verb, group, p, ns) {
				out = append(out, ns)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// i24NamesIn returns the namespace names the value v lists, in any of the
// shapes the three stages leave (an options list, a name array, a list).
func i24NamesIn(v any) []string {
	b, _ := json.Marshal(v)
	s := string(b)
	out := []string{}
	for _, ns := range i24Namespaces {
		if strings.Contains(s, `"`+ns+`"`) {
			out = append(out, ns)
		}
	}
	sort.Strings(out)
	return out
}

// TestIssue1124_ScopePruningNamespaceStagesUnchanged — the three portal stages
// whose items are namespaces keep the OR across their plural set: projects
// (end to end, the multi-plural set the crds stage discovers),
// blueprint-formdef.namespaces and agent-formdef.namespaces (their stage
// verbatim, alone, with the upstream value its resourcesFrom reads).
func TestIssue1124_ScopePruningNamespaceStagesUnchanged(t *testing.T) {
	e := newI24Env(t)

	projects := i24RA(t, "projects")
	bp := i24RA(t, "blueprint-formdef")
	bpNS := *i24Stage(t, bp, "namespaces")
	bpNS.DependsOn = nil
	ag := i24RA(t, "agent-formdef")
	agNS := *i24Stage(t, ag, "namespaces")
	agNS.DependsOn = nil

	for _, arm := range i24Arms {
		for _, u := range i24Users {
			plurals := []string{}
			if i24SeesBlueprints(u) {
				plurals = []string{"p1s", "p2s"}
			}
			got := i24NamesIn(e.resolve(arm, u, projects, nil, projects.Spec.API)["namespaces"])
			if want := i24ScopeWant(u, "list", i24Comp, plurals); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s %s projects: namespaces %v, the scope answer %v", arm, u, got, want)
			}
			if u == i24UM && strings.Join(got, ",") != "ns-a,ns-b" {
				t.Errorf("%s: M's projects %v, want [ns-a ns-b] (p1s in ns-a OR p2s in ns-b)", arm, got)
			}

			got = i24NamesIn(e.resolve(arm, u, bp, map[string]any{"compdef": map[string]any{"status": map[string]any{"resource": "p1s"}}},
				[]*templates.API{&bpNS})["namespaces"])
			if want := i24ScopeWant(u, "create", i24Comp, []string{"p1s"}); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s %s blueprint-formdef.namespaces: %v, the scope answer %v", arm, u, got, want)
			}

			got = i24NamesIn(e.resolve(arm, u, ag, nil, []*templates.API{&agNS})["namespaces"])
			if want := i24ScopeWant(u, "create", i24Agent, []string{"agents"}); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s %s agent-formdef.namespaces: %v, the scope answer %v", arm, u, got, want)
			}
		}
	}
	// Non-vacuity: M's answers are not empty, so an all-deny mutation is RED.
	if got := i24ScopeWant(i24UM, "create", i24Comp, []string{"p1s"}); strings.Join(got, ",") != "ns-c" {
		t.Fatalf("fixture: M creates p1s in ns-c, oracle says %v", got)
	}
}

// TestIssue1124_RefilterParityWithKubernetes — the refilter against Kubernetes'
// per-object decision over the whole matrix: every caller, every verb of the
// table, an object list of each checked plural spread over every namespace, the
// stage's set holding both plurals. Kept == allowed(verb, own plural, own ns).
// Objects of a resource outside the set (a scope question) keep the OR.
func TestIssue1124_RefilterParityWithKubernetes(t *testing.T) {
	newRefilterTestWatcher(t, i24RBAC()...)
	dict := map[string]any{"crds": []any{map[string]any{"plural": "p1s"}, map[string]any{"plural": "p2s"}}}
	type served struct {
		gr     schema.GroupResource
		member bool
	}
	for _, s := range []served{
		{schema.GroupResource{Group: i24Comp, Resource: "p1s"}, true},
		{schema.GroupResource{Group: i24Comp, Resource: "p2s"}, true},
		{schema.GroupResource{Group: "apps", Resource: "deployments"}, false},
		// a set plural's name in another group is another resource: scope
		{schema.GroupResource{Group: "x.example.io", Resource: "p1s"}, false},
		// the UAF's group, a plural the set does not hold: the author's scope
		{schema.GroupResource{Group: i24Comp, Resource: "p3s"}, false},
	} {
		for _, verb := range []string{"get", "list", "create"} {
			for _, u := range i24Users {
				items := []any{}
				for _, ns := range i24Namespaces {
					items = append(items, map[string]any{"metadata": map[string]any{"namespace": ns, "name": "o-" + ns}})
				}
				uaf := &templates.UserAccessFilterSpec{Verb: verb, Group: i24Comp, ResourcesFrom: "[ (.crds // [])[] | .plural ]"}
				pig := map[string]any{"st": map[string]any{"items": items}}
				applyUserAccessFilterOnPig(ctxWithUser(u), pig, dict, "st", uaf, s.gr, true)
				got := i24NamesIn(pig["st"])
				want := []string{}
				for _, ns := range i24Namespaces {
					var ok bool
					if s.member {
						ok = i24Allows(u, verb, i24Comp, s.gr.Resource, ns)
					} else {
						ok = i24Allows(u, verb, i24Comp, "p1s", ns) || i24Allows(u, verb, i24Comp, "p2s", ns)
					}
					if ok {
						want = append(want, ns)
					}
				}
				if strings.Join(got, ",") != strings.Join(want, ",") {
					t.Errorf("served %s %s %s: kept %v, want %v", s.gr, verb, u, got, want)
				}
			}
		}
	}
}

// TestIssue1124_UAFItemResources is the rule's table: narrowed only for a
// served member of the set, in the UAF's group.
func TestIssue1124_UAFItemResources(t *testing.T) {
	uaf := &templates.UserAccessFilterSpec{Group: i24Comp}
	set := []string{"p1s", "p2s"}
	for _, c := range []struct {
		name   string
		served schema.GroupResource
		ok     bool
		want   string
	}{
		{"object of a checked plural", schema.GroupResource{Group: i24Comp, Resource: "p2s"}, true, "p2s"},
		{"namespaces (scope)", schema.GroupResource{Resource: "namespaces"}, true, "p1s,p2s"},
		{"same plural, another group (scope)", schema.GroupResource{Group: "x.example.io", Resource: "p1s"}, true, "p1s,p2s"},
		{"same group, a plural outside the set (scope)", schema.GroupResource{Group: i24Comp, Resource: "p3s"}, true, "p1s,p2s"},
		{"path names no resource", schema.GroupResource{}, false, "p1s,p2s"},
	} {
		if got := strings.Join(uafItemResources(uaf, set, c.served, c.ok), ","); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
	// A static resource is a set of one: unchanged either way.
	if got := uafItemResources(uaf, []string{"p1s"}, schema.GroupResource{Group: i24Comp, Resource: "p1s"}, true); len(got) != 1 || got[0] != "p1s" {
		t.Errorf("static resource: %v", got)
	}
}

// TestIssue1124_TwinEntryNarrowsLikeTheLiveOne — applyUserAccessFilter (the
// dead twin of the live entry) derives the served resource from the stage's
// path and narrows the same way, so it cannot become a bypass if re-wired.
func TestIssue1124_TwinEntryNarrowsLikeTheLiveOne(t *testing.T) {
	newRefilterTestWatcher(t, i24RBAC()...)
	apiCall := &templates.API{
		Name: "st", Path: "/apis/" + i24Comp + "/" + i24Ver + "/p2s",
		UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "list", Group: i24Comp, ResourcesFrom: `["p1s","p2s"]`},
	}
	dict := map[string]any{"st": map[string]any{"items": []any{
		map[string]any{"metadata": map[string]any{"namespace": "ns-a", "name": "b-p2-in-ns-a"}},
	}}}
	applyUserAccessFilter(ctxWithUser(i24UA2), dict, apiCall)
	if got := i24NamesIn(dict["st"]); len(got) != 0 {
		t.Fatalf("twin entry kept a p2 object for A2 (p1s only): %v", got)
	}
}
