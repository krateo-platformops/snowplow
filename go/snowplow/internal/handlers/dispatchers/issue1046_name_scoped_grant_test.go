// issue1046_name_scoped_grant_test.go — #1046 acceptance arms: a
// resourceNames-scoped grant is honoured by every per-object decision, with the
// cache on and with it off, exactly as Kubernetes decides it.
//
// THE DEFECT. checkDispatchRBAC evaluated `get` on the dispatched CR without its
// name, so `get restactions resourceNames: [x]` could never match: the cache=on
// /call of x was 403 while the cache=off /call (the per-user apiserver GET, which
// carries the name) was 200. The same name-less shape sat in the nested
// `resolve: true` gate (same function), in objects.Get's informer filter, and in
// the cache=off SelfSubjectAccessReview — which made userAccessFilter drop a
// name-scoped item with the cache OFF while keeping it with the cache on (the
// reverse divergence).
//
// THE REAL BOUNDARY. No RBAC seam and no fetch seam: the real handlers
// (restActionHandler / widgetsHandler.ServeHTTP), the real fetchObject →
// objects.Get (informer-served with the cache on, the apiserver with it off),
// the real checkDispatchRBAC over a real published RBAC snapshot, the real
// restactions.Resolve (the widget arm alone seams widgets.Resolve: its decision
// is made before it). The cache=off path talks to a fake apiserver that
// authorizes every request — object GETs and SelfSubjectAccessReviews — with
// internal/testsupport/k8sauthz, an independent model of the Kubernetes RBAC
// authorizer over the SAME RBAC objects. So both modes are compared with one
// ground truth, never with each other only.
//
// RED on 05eace28: Dispatch (RESTAction + widget) and Nested — every name-scoped
// `x` cell is 403 / an error with the cache on; UAF — the name-scoped item is
// dropped with the cache off; ResourcesRefs — the `allowed` flag of a
// name-scoped ref is false in both modes.

package dispatchers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/resourcesrefs"
	"github.com/krateo-platformops/snowplow/internal/testsupport/k8sauthz"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

const (
	n46NS     = "team-n46"
	n46AuthNS = "krateo-system"
	n46Group  = "n46-portal"
	n46RAX    = "ra-x"
	n46RAY    = "ra-y"
	n46WX     = "w-x"
	n46WY     = "w-y"
	n46Lister = "ra-lister" // UAF: lists the RESTActions, filtered by `get`
	n46Pick   = "ra-pick"   // reads RESTAction ra-x as the requester (key arm)
	n46NestX  = "ra-nest-x" // resolve:true stage on ra-x
	n46NestY  = "ra-nest-y" // resolve:true stage on ra-y
	n46SAUser = "system:serviceaccount:krateo-system:snowplow"

	n46NSWide = "n46-nswide" // get on every RESTAction / Panel in the namespace
	n46NameX  = "n46-name-x" // get only on ra-x / w-x (resourceNames)
	n46NameY  = "n46-name-y" // get only on ra-y / w-y (resourceNames)
	n46None   = "n46-none"   // no grant on either
)

var n46Users = []string{n46NSWide, n46NameX, n46NameY, n46None}

func n46Sentinel(name string) string { return "N46-SENTINEL-" + name }

func n46Groups() []string { return []string{n46Group} }

// n46RA is a RESTAction in n46NS. With no stages its filter projects a
// sentinel naming it, so a 200 body proves the CR was really resolved.
func n46RA(name string, filter string, stages ...map[string]any) *unstructured.Unstructured {
	api := make([]any, 0, len(stages))
	for _, s := range stages {
		api = append(api, s)
	}
	if filter == "" {
		filter = `{"sentinel":"` + n46Sentinel(name) + `"}`
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": n46NS, "uid": "uid-" + name},
		"spec":       map[string]any{"api": api, "filter": filter},
	}}
}

func n46Widget(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": name, "namespace": n46NS, "uid": "uid-" + name},
		"spec":       map[string]any{"title": n46Sentinel(name)},
	}}
}

func n46RAPath(name string) string {
	return "/apis/" + h1RAGVR.Group + "/" + h1RAGVR.Version + "/namespaces/" + n46NS + "/restactions/" + name
}

// n46Objects are the CRs the cluster holds.
func n46Objects() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		n46RA(n46RAX, ""), n46RA(n46RAY, ""),
		n46RA(n46Lister, `[.items.items[].metadata.name]`, map[string]any{
			"name": "items", "continueOnError": true,
			"path": "/apis/" + h1RAGVR.Group + "/" + h1RAGVR.Version + "/namespaces/" + n46NS + "/restactions",
			"userAccessFilter": map[string]any{
				"verb": "get", "group": h1RAGVR.Group, "resource": h1RAGVR.Resource,
			},
		}),
		n46RA(n46Pick, `.target`, map[string]any{"name": "target", "continueOnError": true, "path": n46RAPath(n46RAX)}),
		n46RA(n46NestX, `.child`, map[string]any{"name": "child", "continueOnError": true, "resolve": true, "path": n46RAPath(n46RAX)}),
		n46RA(n46NestY, `.child`, map[string]any{"name": "child", "continueOnError": true, "resolve": true, "path": n46RAPath(n46RAY)}),
		n46Widget(n46WX), n46Widget(n46WY),
	}
}

// n46RBAC: one requester per grant shape, plus a group grant on the parent
// RESTActions every requester dispatches (lister / pick / nest-*), so the
// per-object decision under test is the ONLY one that differs between them.
func n46RBAC() []runtime.Object {
	rbacGroup := "rbac.authorization.k8s.io"
	user := func(n string) []rbacv1.Subject {
		return []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacGroup, Name: n}}
	}
	scoped := func(ra, w string) []rbacv1.PolicyRule {
		return []rbacv1.PolicyRule{
			{Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}, ResourceNames: []string{ra}},
			{Verbs: []string{"get"}, APIGroups: []string{h1WidgetGVR.Group}, Resources: []string{h1WidgetGVR.Resource}, ResourceNames: []string{w}},
		}
	}
	return []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "n46-parents"}, Rules: []rbacv1.PolicyRule{{
			Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource},
			ResourceNames: []string{n46Lister, n46Pick, n46NestX, n46NestY},
		}}},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "n46-parents", UID: types.UID("uid-crb-n46-parents")},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacGroup, Name: n46Group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacGroup, Kind: "ClusterRole", Name: "n46-parents"},
		},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-all"}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get", "list"}, APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}},
			{Verbs: []string{"get", "list"}, APIGroups: []string{h1WidgetGVR.Group}, Resources: []string{h1WidgetGVR.Resource}},
		}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-nswide", UID: types.UID("uid-rb-nswide")},
			Subjects: user(n46NSWide), RoleRef: rbacv1.RoleRef{APIGroup: rbacGroup, Kind: "Role", Name: "n46-all"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-x"}, Rules: scoped(n46RAX, n46WX)},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-name-x", UID: types.UID("uid-rb-name-x")},
			Subjects: user(n46NameX), RoleRef: rbacv1.RoleRef{APIGroup: rbacGroup, Kind: "Role", Name: "n46-x"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-y"}, Rules: scoped(n46RAY, n46WY)},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-name-y", UID: types.UID("uid-rb-name-y")},
			Subjects: user(n46NameY), RoleRef: rbacv1.RoleRef{APIGroup: rbacGroup, Kind: "Role", Name: "n46-y"}},
		// n46-none holds an unrelated grant, so "none" is not "no binding".
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-pods"}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"pods"}}}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: n46NS, Name: "n46-none", UID: types.UID("uid-rb-none")},
			Subjects: user(n46None), RoleRef: rbacv1.RoleRef{APIGroup: rbacGroup, Kind: "Role", Name: "n46-pods"}},
	}
}

// ── the fake apiserver ──────────────────────────────────────────────────────

type n46Req struct{ method, path, user string }

// n46API is one fake apiserver reached through one listener PER IDENTITY: the
// requester is the listener a request arrives on. A per-user endpoint reaches
// snowplow's dial sites as a kubeconfig, which carries no bearer token (only
// client certificates), so the listener — not a header — is what tells the
// fake who is asking, exactly as a client certificate tells the real one.
type n46API struct {
	sa     *httptest.Server            // snowplow's ServiceAccount
	users  map[string]*httptest.Server // one per requester
	oracle *k8sauthz.Authorizer
	mu     sync.Mutex
	reqs   []n46Req
}

func (f *n46API) urlFor(user string) string { return f.users[user].URL }

func (f *n46API) record(r n46Req) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r)
	f.mu.Unlock()
}

func (f *n46API) reset() {
	f.mu.Lock()
	f.reqs = nil
	f.mu.Unlock()
}

// objectGETs counts GETs of one RESTAction / Panel CR (not reviews, lists or
// Secrets) the apiserver received as user.
func (f *n46API) objectGETs(user, resource, name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.reqs {
		if r.method == http.MethodGet && r.user == user && strings.HasSuffix(r.path, "/"+resource+"/"+name) {
			n++
		}
	}
	return n
}

func (f *n46API) reviews() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.reqs {
		if strings.HasSuffix(r.path, "/selfsubjectaccessreviews") {
			n++
		}
	}
	return n
}

// n46ParsePath splits a resource path into (group, ns, resource, name).
func n46ParsePath(p string) (group, ns, resource, name string, ok bool) {
	seg := strings.Split(strings.Trim(p, "/"), "/")
	switch {
	case len(seg) >= 5 && seg[0] == "apis" && seg[3] == "namespaces":
		group, seg = seg[1], seg[4:]
	case len(seg) >= 4 && seg[0] == "api" && seg[2] == "namespaces":
		group, seg = "", seg[3:]
	default:
		return "", "", "", "", false
	}
	ns = seg[0]
	if len(seg) < 2 {
		return "", "", "", "", false
	}
	resource = seg[1]
	if len(seg) > 2 {
		name = seg[2]
	}
	return group, ns, resource, name, true
}

// n46Discovery is the discovery document the dynamic client's REST mapper
// reads before its first request.
var n46Discovery = func() map[string]string {
	group := func(g, v string) string {
		return `{"name":"` + g + `","versions":[{"groupVersion":"` + g + `/` + v + `","version":"` + v + `"}],` +
			`"preferredVersion":{"groupVersion":"` + g + `/` + v + `","version":"` + v + `"}}`
	}
	resources := func(gv string, rs ...string) string {
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"` + gv + `","resources":[` + strings.Join(rs, ",") + `]}`
	}
	res := func(name, kind string, namespaced bool) string {
		ns := "false"
		if namespaced {
			ns = "true"
		}
		return `{"name":"` + name + `","singularName":"","namespaced":` + ns + `,"kind":"` + kind + `","verbs":["get","list","create"]}`
	}
	return map[string]string{
		"/api":    `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`,
		"/api/v1": resources("v1", res("secrets", "Secret", true), res("pods", "Pod", true)),
		"/apis": `{"kind":"APIGroupList","apiVersion":"v1","groups":[` + strings.Join([]string{
			group(h1RAGVR.Group, h1RAGVR.Version), group(h1WidgetGVR.Group, h1WidgetGVR.Version),
			group("authorization.k8s.io", "v1")}, ",") + `]}`,
		"/apis/" + h1RAGVR.Group + "/" + h1RAGVR.Version:         resources(h1RAGVR.Group+"/"+h1RAGVR.Version, res(h1RAGVR.Resource, "RESTAction", true)),
		"/apis/" + h1WidgetGVR.Group + "/" + h1WidgetGVR.Version: resources(h1WidgetGVR.Group+"/"+h1WidgetGVR.Version, res(h1WidgetGVR.Resource, "Panel", true)),
		"/apis/authorization.k8s.io/v1":                          resources("authorization.k8s.io/v1", res("selfsubjectaccessreviews", "SelfSubjectAccessReview", false)),
	}
}()

func n46Forbidden(w http.ResponseWriter) {
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
}

func newN46API(t *testing.T, objs []*unstructured.Unstructured, rbacObjs []runtime.Object) *n46API {
	t.Helper()
	f := &n46API{oracle: k8sauthz.New(rbacObjs...), users: map[string]*httptest.Server{}}
	byPath := map[string]*unstructured.Unstructured{}
	for _, o := range objs {
		gv := o.GetAPIVersion()
		res := h1RAGVR.Resource
		if o.GetKind() == "Panel" {
			res = h1WidgetGVR.Resource
		}
		byPath["/apis/"+gv+"/namespaces/"+o.GetNamespace()+"/"+res+"/"+o.GetName()] = o
	}
	handlerFor := func(user string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			f.record(n46Req{method: r.Method, path: r.URL.Path, user: user})
			groups := []string(nil)
			if user != n46SAUser {
				groups = n46Groups()
			}
			// SelfSubjectAccessReview — answered by the oracle on the attributes the
			// caller SENT.
			if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/selfsubjectaccessreviews") {
				// The clientset may send protobuf: decode with the scheme's
				// universal deserializer (JSON or protobuf), answer in JSON.
				body, _ := io.ReadAll(r.Body)
				ssar := &authv1.SelfSubjectAccessReview{}
				if _, _, err := k8sscheme.Codecs.UniversalDeserializer().Decode(body, nil, ssar); err != nil {
					t.Errorf("fake apiserver: undecodable SelfSubjectAccessReview (%s): %v", r.Header.Get("Content-Type"), err)
				}
				ra := ssar.Spec.ResourceAttributes
				ssar.Status.Allowed = ra != nil && f.oracle.Allowed(k8sauthz.Attributes{
					User: user, Groups: groups, Verb: ra.Verb, Group: ra.Group,
					Resource: ra.Resource, Namespace: ra.Namespace, Name: ra.Name,
				})
				ssar.APIVersion, ssar.Kind = "authorization.k8s.io/v1", "SelfSubjectAccessReview"
				out, _ := json.Marshal(ssar)
				_, _ = w.Write(out)
				return
			}
			if d, ok := n46Discovery[r.URL.Path]; ok {
				_, _ = w.Write([]byte(d))
				return
			}
			group, ns, resource, name, ok := n46ParsePath(r.URL.Path)
			if !ok || r.Method != http.MethodGet {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// The per-user clientconfig Secrets the cache=off stage dial reads
			// through snowplow's own config.
			if resource == "secrets" {
				for _, u := range n46Users {
					if ns == n46AuthNS && name == credsecret.For(u, n46Groups()) {
						out, _ := json.Marshal(n46ClientconfigSecret(u, f.urlFor(u)))
						_, _ = w.Write(out)
						return
					}
				}
				w.WriteHeader(http.StatusNotFound)
				return
			}
			verb := "get"
			if name == "" {
				verb = "list"
			}
			if user != n46SAUser && !f.oracle.Allowed(k8sauthz.Attributes{
				User: user, Groups: groups, Verb: verb, Group: group, Resource: resource, Namespace: ns,
				Name: k8sauthz.RequestName(verb, name),
			}) {
				n46Forbidden(w)
				return
			}
			if name == "" {
				var items []any
				keys := make([]string, 0, len(byPath))
				for k := range byPath {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					if strings.HasPrefix(k, r.URL.Path+"/") {
						items = append(items, byPath[k].Object)
					}
				}
				out, _ := json.Marshal(map[string]any{"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
					"kind": "RESTActionList", "metadata": map[string]any{}, "items": items})
				_, _ = w.Write(out)
				return
			}
			o := byPath[r.URL.Path]
			if o == nil {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
				return
			}
			out, _ := json.Marshal(o.Object)
			_, _ = w.Write(out)
		}
	}
	f.sa = httptest.NewServer(handlerFor(n46SAUser))
	t.Cleanup(f.sa.Close)
	for _, u := range n46Users {
		f.users[u] = httptest.NewServer(handlerFor(u))
		t.Cleanup(f.users[u].Close)
	}
	return f
}

func n46ClientconfigSecret(user, srvURL string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Namespace: n46AuthNS, Name: credsecret.For(user, n46Groups())},
		Data:       map[string][]byte{"server-url": []byte(srvURL), "token": []byte("tok-" + user)},
	}
}

// ── the two modes ──────────────────────────────────────────────────────────

type n46Env struct {
	api     *n46API
	cacheOn bool
}

// n46Setup builds the shared harness and switches to one mode. Cache on: a
// real ResourceWatcher over the same RBAC objects and CRs, its RBAC snapshot
// published, the RESTAction / Panel informers servable, the clientconfig
// Secrets in the Secrets snapshot. Cache off: no watcher at all — every read
// is the apiserver's.
func n46Setup(t *testing.T, cacheOn bool) *n46Env {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	prevTM := env.TestMode()
	env.SetTestMode(true) // keep the endpoint's server-url (no kubernetes.default.svc rewrite)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRBACSubGenForTest()
	cache.ResetPendingSubGenBumpsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
		cache.ResetRBACSubGenForTest()
	})

	objs, rbacObjs := n46Objects(), n46RBAC()
	e := &n46Env{api: newN46API(t, objs, rbacObjs), cacheOn: cacheOn}
	t.Cleanup(api.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: e.api.sa.URL, Token: "tok-sa"}, nil
	}))
	t.Cleanup(setNestedCallResolverForTest(ResolveNestedCall))

	if !cacheOn {
		t.Setenv("CACHE_ENABLED", "false")
		prev := cache.Global()
		cache.SetGlobal(nil)
		t.Cleanup(func() { cache.SetGlobal(prev) })
		return e
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", h1WidgetGVR: "PanelList",
		rbacGVR("clusterrolebindings"): "ClusterRoleBindingList", rbacGVR("clusterroles"): "ClusterRoleList",
		rbacGVR("rolebindings"): "RoleBindingList", rbacGVR("roles"): "RoleList",
		{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
	}
	seed := append([]runtime.Object(nil), rbacObjs...)
	for _, o := range objs {
		seed = append(seed, o.DeepCopy())
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 10*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, h1WidgetGVR} {
		if _, ch := rw.EnsureResourceType(gvr); ch != nil {
			select {
			case <-ch:
			case <-time.After(10 * time.Second):
				t.Fatalf("informer %s did not sync", gvr)
			}
		}
		if !rw.IsServable(gvr) {
			t.Fatalf("PRE: informer %s must be servable", gvr)
		}
	}
	if err := rw.WaitInitialRBACPublishForTest(10 * time.Second); err != nil {
		t.Fatalf("initial RBAC publish: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})

	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	byName := map[string]*corev1.Secret{}
	for _, u := range n46Users {
		s := n46ClientconfigSecret(u, e.api.urlFor(u))
		byName[s.Name] = s
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: byName})
	cache.ForceSecretsCacheReadyForTest(n46AuthNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	return e
}

func (e *n46Env) mode() string {
	if e.cacheOn {
		return "cache-on"
	}
	return "cache-off"
}

// ctx is the request ctx as the auth middleware leaves it: the identity and
// the caller's own clientconfig endpoint.
func (e *n46Env) ctx(user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: n46Groups()}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: e.api.urlFor(user), Token: "tok-" + user}),
	)
}

// call drives GET /call for one CR through the real handler, fetch included.
func (e *n46Env) call(t *testing.T, user string, gvr schema.GroupVersionResource, name string) *httptest.ResponseRecorder {
	t.Helper()
	q := url.Values{}
	q.Set("apiVersion", gvr.Group+"/"+gvr.Version)
	q.Set("resource", gvr.Resource)
	q.Set("namespace", n46NS)
	q.Set("name", name)
	req := httptest.NewRequest(http.MethodGet, "/call?"+q.Encode(), nil).WithContext(e.ctx(user))
	rec := httptest.NewRecorder()
	saRC := &rest.Config{Host: e.api.sa.URL, BearerToken: "tok-sa"}
	if gvr == h1WidgetGVR {
		(&widgetsHandler{authnNS: n46AuthNS, saRC: saRC}).ServeHTTP(rec, req)
	} else {
		(&restActionHandler{authnNS: n46AuthNS, saRC: saRC}).ServeHTTP(rec, req)
	}
	return rec
}

// k8sAllowsGet is the ground truth for `get <resource>/<name>`.
func (e *n46Env) k8sAllowsGet(user string, gvr schema.GroupVersionResource, name string) bool {
	return e.api.oracle.Allowed(k8sauthz.Attributes{User: user, Groups: n46Groups(), Verb: "get",
		Group: gvr.Group, Resource: gvr.Resource, Namespace: n46NS, Name: name})
}

// ── arms ──────────────────────────────────────────────────────────────────

// n46DispatchDecisions drives /call for every (user, target) in one mode and
// returns the observed decision (true = 200 with the resolved sentinel).
func n46DispatchDecisions(t *testing.T, e *n46Env, gvr schema.GroupVersionResource, targets []string) map[string]bool {
	t.Helper()
	got := map[string]bool{}
	for _, u := range n46Users {
		for _, target := range targets {
			e.api.reset()
			rec := e.call(t, u, gvr, target)
			allowed := rec.Code == http.StatusOK
			hasSentinel := strings.Contains(rec.Body.String(), n46Sentinel(target))
			want := e.k8sAllowsGet(u, gvr, target)
			switch {
			case allowed != hasSentinel:
				t.Errorf("%s: %s /call %s: code %d but sentinel present=%v — the body does not match the decision; body=%s",
					e.mode(), u, target, rec.Code, hasSentinel, psTrunc(rec.Body.String(), 300))
			case allowed != want:
				t.Errorf("%s: %s /call %s %s = %d, Kubernetes says allowed=%v; body=%s",
					e.mode(), u, gvr.Resource, target, rec.Code, want, psTrunc(rec.Body.String(), 300))
			case !allowed && rec.Code != http.StatusForbidden:
				t.Errorf("%s: %s /call %s: a denial must be 403, got %d", e.mode(), u, target, rec.Code)
			}
			// Cache on, an allowed object is served from the informer: the
			// apiserver never sees the requester GET it. (A name-less informer
			// filter would deny it there and fall through to the apiserver,
			// which allows it — a 200 that hides the defect.)
			if e.cacheOn && want {
				if n := e.api.objectGETs(u, gvr.Resource, target); n != 0 {
					t.Errorf("cache-on: %s /call %s reached the apiserver %d× as the requester — objects.Get's informer "+
						"filter did not honour the grant", u, target, n)
				}
			}
			got[u+"|"+target] = allowed
		}
	}
	return got
}

func n46AssertModesAgree(t *testing.T, on, off map[string]bool) {
	t.Helper()
	for k, v := range on {
		if off[k] != v {
			t.Errorf("cache on/off DISAGREE on %s: on=%v off=%v", k, v, off[k])
		}
	}
}

// TestIssue1046_Dispatch_RESTAction: `/call x` allowed and `/call y` denied for
// the user granted only x, the same with the cache on and off, and equal to
// Kubernetes for every grant shape.
func TestIssue1046_Dispatch_RESTAction(t *testing.T) {
	var on, off map[string]bool
	t.Run("cache-on", func(t *testing.T) {
		on = n46DispatchDecisions(t, n46Setup(t, true), h1RAGVR, []string{n46RAX, n46RAY})
	})
	t.Run("cache-off", func(t *testing.T) {
		off = n46DispatchDecisions(t, n46Setup(t, false), h1RAGVR, []string{n46RAX, n46RAY})
	})
	n46AssertModesAgree(t, on, off)
	if !on[n46NameX+"|"+n46RAX] || on[n46NameX+"|"+n46RAY] {
		t.Errorf("the name-scoped requester must be allowed x and denied y; got x=%v y=%v", on[n46NameX+"|"+n46RAX], on[n46NameX+"|"+n46RAY])
	}
}

// TestIssue1046_Dispatch_Widget: the same for a widget (Panel) dispatch. The
// widget resolve is seamed to the identity: the decision is the gate's.
func TestIssue1046_Dispatch_Widget(t *testing.T) {
	t.Cleanup(setWidgetsResolveForTest(func(_ context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		return opts.In, nil
	}))
	var on, off map[string]bool
	t.Run("cache-on", func(t *testing.T) {
		on = n46DispatchDecisions(t, n46Setup(t, true), h1WidgetGVR, []string{n46WX, n46WY})
	})
	t.Run("cache-off", func(t *testing.T) {
		off = n46DispatchDecisions(t, n46Setup(t, false), h1WidgetGVR, []string{n46WX, n46WY})
	})
	n46AssertModesAgree(t, on, off)
	if !on[n46NameX+"|"+n46WX] || on[n46NameX+"|"+n46WY] {
		t.Errorf("the name-scoped requester must be allowed w-x and denied w-y; got x=%v y=%v", on[n46NameX+"|"+n46WX], on[n46NameX+"|"+n46WY])
	}
}

// TestIssue1046_Nested_ResolveTrueStage: a `resolve: true` stage on a
// name-scoped RESTAction resolves it for the requester granted it and fails
// the stage for the one who is not — through a parent /call (the stage's
// in-process ResolveNestedCall, the "nested resolve dispatch denied by
// EvaluateRBAC" site) and through ResolveNestedCall directly.
func TestIssue1046_Nested_ResolveTrueStage(t *testing.T) {
	type nestKey struct{ user, child string }
	run := func(t *testing.T, e *n46Env) map[nestKey]bool {
		got := map[nestKey]bool{}
		for _, u := range n46Users {
			for parent, child := range map[string]string{n46NestX: n46RAX, n46NestY: n46RAY} {
				want := e.k8sAllowsGet(u, h1RAGVR, child)

				rec := e.call(t, u, h1RAGVR, parent)
				if rec.Code != http.StatusOK {
					t.Fatalf("%s: %s /call %s (the parent every requester may get) = %d; body=%s",
						e.mode(), u, parent, rec.Code, psTrunc(rec.Body.String(), 300))
				}
				viaParent := strings.Contains(rec.Body.String(), n46Sentinel(child))
				if viaParent != want {
					t.Errorf("%s: %s parent %s → resolve:true %s: child resolved=%v, Kubernetes says allowed=%v; body=%s",
						e.mode(), u, parent, child, viaParent, want, psTrunc(rec.Body.String(), 400))
				}

				raw, err := ResolveNestedCall(e.ctx(u), templates.ObjectReference{
					Reference:  templates.Reference{Name: child, Namespace: n46NS},
					Resource:   h1RAGVR.Resource,
					APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
				}, 0, 0, nil)
				direct := err == nil && strings.Contains(string(raw), n46Sentinel(child))
				if direct != want {
					t.Errorf("%s: %s ResolveNestedCall(%s) resolved=%v (err=%v), Kubernetes says allowed=%v",
						e.mode(), u, child, direct, err, want)
				}
				got[nestKey{u, child}] = direct && viaParent
			}
		}
		return got
	}
	var on, off map[nestKey]bool
	t.Run("cache-on", func(t *testing.T) { on = run(t, n46Setup(t, true)) })
	t.Run("cache-off", func(t *testing.T) { off = run(t, n46Setup(t, false)) })
	for k, v := range on {
		if off[k] != v {
			t.Errorf("cache on/off DISAGREE on nested %+v: on=%v off=%v", k, v, off[k])
		}
	}
}

// TestIssue1046_UserAccessFilter_NameScoped: a `get` userAccessFilter keeps
// exactly the items Kubernetes lets the requester get — the name-scoped
// requester keeps ra-x and not ra-y — with the cache on and with it off (where
// the refilter's decision is a SelfSubjectAccessReview, the reverse divergence
// the issue reports).
func TestIssue1046_UserAccessFilter_NameScoped(t *testing.T) {
	candidates := []string{n46Lister, n46NestX, n46NestY, n46Pick, n46RAX, n46RAY}
	run := func(t *testing.T, e *n46Env) map[string][]string {
		got := map[string][]string{}
		for _, u := range n46Users {
			e.api.reset()
			rec := e.call(t, u, h1RAGVR, n46Lister)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s: %s /call %s = %d; body=%s", e.mode(), u, n46Lister, rec.Code, psTrunc(rec.Body.String(), 300))
			}
			var env struct {
				Status []string `json:"status"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("%s: %s: decode the lister body: %v; body=%s", e.mode(), u, err, psTrunc(rec.Body.String(), 300))
			}
			sort.Strings(env.Status)
			var want []string
			for _, c := range candidates {
				if e.k8sAllowsGet(u, h1RAGVR, c) {
					want = append(want, c)
				}
			}
			sort.Strings(want)
			if strings.Join(env.Status, ",") != strings.Join(want, ",") {
				t.Errorf("%s: %s userAccessFilter kept %v, Kubernetes allows get on %v", e.mode(), u, env.Status, want)
			}
			if !e.cacheOn && e.api.reviews() == 0 {
				t.Errorf("cache-off: %s: no SelfSubjectAccessReview reached the apiserver — the refilter's cache-off path did not run", u)
			}
			got[u] = env.Status
		}
		return got
	}
	var on, off map[string][]string
	t.Run("cache-on", func(t *testing.T) { on = run(t, n46Setup(t, true)) })
	t.Run("cache-off", func(t *testing.T) { off = run(t, n46Setup(t, false)) })
	for u, v := range on {
		if strings.Join(off[u], ",") != strings.Join(v, ",") {
			t.Errorf("cache on/off DISAGREE on %s's userAccessFilter: on=%v off=%v", u, v, off[u])
		}
	}
	keeps := func(user, name string) bool {
		for _, n := range on[user] {
			if n == name {
				return true
			}
		}
		return false
	}
	if !keeps(n46NameX, n46RAX) || keeps(n46NameX, n46RAY) {
		t.Errorf("the name-scoped requester must keep %s and not %s; got %v", n46RAX, n46RAY, on[n46NameX])
	}
}

// TestIssue1046_ResourcesRefs_AllowedFlag: a widget resourcesRef names its
// object; its `allowed` flag is Kubernetes' answer for that name, cache on and
// off — both where it is computed (resourcesrefs.Resolve, the cold widget
// resolve) and where it is re-derived per requester on the identity-free widget
// cell (gateWidgetEnvelope → recomputeAllowedFromRefItem).
func TestIssue1046_ResourcesRefs_AllowedFlag(t *testing.T) {
	item := func(name string) map[string]any {
		q := url.Values{}
		q.Set("apiVersion", h1RAGVR.Group+"/"+h1RAGVR.Version)
		q.Set("resource", h1RAGVR.Resource)
		q.Set("namespace", n46NS)
		q.Set("name", name)
		return map[string]any{"path": "/call?" + q.Encode(), "verb": http.MethodGet}
	}
	run := func(t *testing.T, e *n46Env) map[string]bool {
		got := map[string]bool{}
		for _, u := range n46Users {
			for _, target := range []string{n46RAX, n46RAY} {
				want := e.k8sAllowsGet(u, h1RAGVR, target)
				regated := recomputeAllowedFromRefItem(e.ctx(u), item(target))
				if regated != want {
					t.Errorf("%s: %s serve-time resourcesRef GET %s: allowed=%v, Kubernetes says %v", e.mode(), u, target, regated, want)
				}
				res, err := resourcesrefs.Resolve(e.ctx(u), []templates.ResourceRef{{
					ID: "ref-" + target, Name: target, Namespace: n46NS, Resource: h1RAGVR.Resource,
					APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Verb: http.MethodGet,
				}})
				if err != nil || len(res) != 1 {
					t.Fatalf("%s: %s resourcesrefs.Resolve(%s): %d results, err=%v", e.mode(), u, target, len(res), err)
				}
				if res[0].Allowed != want {
					t.Errorf("%s: %s cold resourcesRef GET %s: allowed=%v, Kubernetes says %v", e.mode(), u, target, res[0].Allowed, want)
				}
				got[u+"|"+target] = regated && res[0].Allowed
			}
		}
		return got
	}
	var on, off map[string]bool
	t.Run("cache-on", func(t *testing.T) { on = run(t, n46Setup(t, true)) })
	t.Run("cache-off", func(t *testing.T) { off = run(t, n46Setup(t, false)) })
	n46AssertModesAgree(t, on, off)
}

// TestIssue1046_KeyNeverServesAnotherGrantsCell — the cache-key implication.
// A decision now depends on the object's name, so a cell must never reach a
// requester whose grant is on a DIFFERENT name:
//
//	(a) direct: name-y fills the ra-y cell; name-x's /call of ra-y is 403 and
//	    carries none of it (the gate runs before the L1 lookup, and name-x's
//	    key for ra-y derives no binding — it is never eligible to serve);
//	(b) a name-scoped requester's own dispatch is cacheable (its key derives a
//	    binding through the name), so the fix does not silently uncache it;
//	(c) the carrier: ra-pick, which name-x and name-y may both dispatch through
//	    the SAME group binding (equal first-match BindingUID — the pre-#423
//	    collision shape), reads ra-x as the requester. name-x's body (with
//	    ra-x's data) is cached; name-y's response must not contain it. Only the
//	    identity class (the full binding set, which differs exactly by the
//	    name-scoped RoleBinding) keeps them apart, and the arm asserts on
//	    name-y's BYTES, not on the keys.
func TestIssue1046_KeyNeverServesAnotherGrantsCell(t *testing.T) {
	e := n46Setup(t, true)
	key := func(user, name string) (string, *cache.ResolvedKeyInputs) {
		k, h, in := dispatchCacheLookupKey(e.ctx(user), "restactions", h1RAGVR.Group, h1RAGVR.Version,
			h1RAGVR.Resource, n46NS, name, -1, -1, map[string]any{})
		if h == nil || in == nil {
			t.Fatalf("PRE: need a live cache handle and key inputs")
		}
		return k, in
	}

	// (a)
	if rec := e.call(t, n46NameY, h1RAGVR, n46RAY); rec.Code != http.StatusOK {
		t.Fatalf("(a) SETUP: name-y /call ra-y = %d", rec.Code)
	}
	yKey, yIn := key(n46NameY, n46RAY)
	if _, ok := cache.ResolvedCache().Get(yKey); !ok || !serveFromCacheEligible(yIn) {
		t.Fatalf("(a) SETUP: name-y's ra-y body must be cached under an eligible key (BindingUID=%q)", yIn.BindingUID)
	}
	rec := e.call(t, n46NameX, h1RAGVR, n46RAY)
	if rec.Code != http.StatusForbidden || strings.Contains(rec.Body.String(), n46Sentinel(n46RAY)) {
		t.Errorf("(a) name-x /call ra-y must be 403 without ra-y's data; got %d body=%s", rec.Code, psTrunc(rec.Body.String(), 300))
	}
	if xyKey, xyIn := key(n46NameX, n46RAY); xyKey == yKey || serveFromCacheEligible(xyIn) {
		t.Errorf("(a) name-x's key for ra-y must neither equal name-y's nor be serve-eligible (BindingUID=%q)", xyIn.BindingUID)
	}

	// (b)
	if rec := e.call(t, n46NameX, h1RAGVR, n46RAX); rec.Code != http.StatusOK {
		t.Fatalf("(b) name-x /call ra-x = %d", rec.Code)
	}
	xKey, xIn := key(n46NameX, n46RAX)
	if _, ok := cache.ResolvedCache().Get(xKey); !ok || !serveFromCacheEligible(xIn) {
		t.Errorf("(b) name-x's own ra-x dispatch must be cached under an eligible key; BindingUID=%q", xIn.BindingUID)
	}

	// (c)
	_, pxIn := key(n46NameX, n46Pick)
	_, pyIn := key(n46NameY, n46Pick)
	if pxIn.BindingUID == "" || pxIn.BindingUID != pyIn.BindingUID {
		t.Fatalf("(c) PRE: name-x and name-y must dispatch ra-pick through ONE binding; got %q / %q", pxIn.BindingUID, pyIn.BindingUID)
	}
	xRec := e.call(t, n46NameX, h1RAGVR, n46Pick)
	if xRec.Code != http.StatusOK || !strings.Contains(xRec.Body.String(), n46Sentinel(n46RAX)) {
		t.Fatalf("(c) PRE: name-x's ra-pick must carry ra-x's data (the per-object read honours the grant); code=%d body=%s",
			xRec.Code, psTrunc(xRec.Body.String(), 300))
	}
	pxKey, _ := key(n46NameX, n46Pick)
	if _, ok := cache.ResolvedCache().Get(pxKey); !ok {
		t.Fatalf("(c) PRE: name-x's ra-pick body must be cached, or the arm cannot observe a cross-grant serve")
	}
	yRec := e.call(t, n46NameY, h1RAGVR, n46Pick)
	if strings.Contains(yRec.Body.String(), n46Sentinel(n46RAX)) {
		t.Errorf("(c) LEAK: name-y (granted only ra-y) was served ra-x's data from name-x's cell; body=%s",
			psTrunc(yRec.Body.String(), 400))
	}

	// (d) the apiRef RAFullList cell key derives through the same per-object
	// check: a name-scoped requester mints it for the RESTAction it may get
	// (so its widget's apiRef data is cacheable) and never for one it may not.
	if _, _, ok := apiref.RAFullListKey(e.ctx(n46NameX), h1RAGVR, n46NS, n46RAX, nil); !ok {
		t.Errorf("(d) name-x must mint the RAFullList key for ra-x (its grant names it)")
	}
	if _, _, ok := apiref.RAFullListKey(e.ctx(n46NameX), h1RAGVR, n46NS, n46RAY, nil); ok {
		t.Errorf("(d) name-x must NOT mint a RAFullList key for ra-y")
	}
}
