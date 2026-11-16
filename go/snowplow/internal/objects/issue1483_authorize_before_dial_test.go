package objects

// issue1483_authorize_before_dial_test.go — #1483 (a): a ServiceAccount read for a
// narrowing identity (a background resolve: the cohort seed, the refresher) decides
// the identity's `get` of the object BEFORE the dial, as #677 did for branch C.
//
//   F  the falsifier (the 057 shape): the identity may `list` configmaps in
//      namespace team-a and has no `get` on restactions in team-b. Reading the
//      RESTAction team-b/ra makes ZERO requests to the apiserver and returns the
//      same 403 the post-fetch re-gate returns (PerIdentityDenied).
//   A  control: an identity that may `get` it reads it, through one dial.
//   U  the RC1 fall-through: with no RBAC snapshot published the deny is not
//      trusted before the dial; the read dials and the post-fetch re-gate
//      decides, with the same 403 body as F.
//   S  the scope fall-through: a namespaced GVR referenced without a namespace
//      is never authorized cluster-wide here; the read dials.
//   L  not a background ServiceAccount read (a live request's own read): no
//      pre-dial decision; the read dials.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	u1483Denied  = "u1483-lists-configmaps-in-team-a"
	u1483Allowed = "u1483-gets-restactions-in-team-b"
	ns1483A      = "team-a"
	ns1483B      = "team-b"
	ra1483       = "ra"
)

// the1483Forbidden is the 403 body the post-fetch re-gate has always returned
// for team-b/ra (a literal, not read from the code under test).
const the1483Forbidden = `restactions.templates.krateo.io "ra" is forbidden: user not authorized to get team-b/ra`

// watcher1483 publishes an RBAC snapshot in which u1483Denied may list
// configmaps in team-a only, and u1483Allowed may get restactions in team-b.
func watcher1483(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	role := func(name, group, resource, verb string) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{group}, Resources: []string{resource}, Verbs: []string{verb}}},
		}
	}
	bind := func(ns, name, user, role string) *rbacv1.RoleBinding {
		return &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: user}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
		}
	}
	seed := []runtime.Object{
		role("cm-lister", "", "configmaps", "list"),
		role("ra-getter", serveTestGVR.Group, serveTestGVR.Resource, "get"),
		bind(ns1483A, "lists-configmaps", u1483Denied, "cm-lister"),
		bind(ns1483B, "gets-restactions", u1483Allowed, "ra-getter"),
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(serveTestScheme(), serveTestListKinds(), seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

// apiserver1483 serves team-b/ra (and its discovery) and counts every request.
func apiserver1483(t *testing.T) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var all, object atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		all.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api":
			_, _ = w.Write([]byte(`{"kind":"APIVersions","versions":["v1"]}`))
		case r.URL.Path == "/apis":
			_, _ = w.Write([]byte(`{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"templates.krateo.io",` +
				`"versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],` +
				`"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}]}`))
		case r.URL.Path == "/apis/templates.krateo.io/v1":
			_, _ = w.Write([]byte(`{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
				`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RESTAction","verbs":["get","list"]}]}`))
		case strings.HasSuffix(r.URL.Path, "/restactions/"+ra1483):
			object.Add(1)
			if r.URL.Path != "/apis/templates.krateo.io/v1/namespaces/"+ns1483B+"/restactions/"+ra1483 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction",` +
				`"metadata":{"name":"ra","namespace":"team-b"},"spec":{"marker":"m1483"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &all, &object
}

// bgSAReadCtx is a background resolve's ServiceAccount read as user: the cohort
// seed's and the refresher's shape (rbac.MustRegateSADial holds).
func bgSAReadCtx(user, host string) context.Context {
	ctx := cache.WithBackgroundResolve(cache.WithInternalRESTConfig(ctxWithUser(user), &rest.Config{Host: host}))
	return xcontext.BuildContext(ctx, xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: host, Token: "t"}))
}

func ref1483(namespace string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: ra1483, Namespace: namespace},
		APIVersion: serveTestGVR.Group + "/" + serveTestGVR.Version,
		Resource:   serveTestGVR.Resource,
	}
}

func testMode1483(t *testing.T) {
	t.Helper()
	prev := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prev) })
}

func require1483Forbidden(t *testing.T, res Result) {
	t.Helper()
	if res.Err == nil || res.Err.Code != http.StatusForbidden || string(res.Err.Reason) != "Forbidden" {
		t.Fatalf("want the re-gate's 403 Forbidden, got %+v", res.Err)
	}
	if res.Err.Message != the1483Forbidden {
		t.Errorf("RED: the 403 changed: %q, want %q", res.Err.Message, the1483Forbidden)
	}
	if !res.PerIdentityDenied || res.Unstructured != nil {
		t.Errorf("RED: the deny must be the identity's and serve nothing (PerIdentityDenied=%v, object=%v)", res.PerIdentityDenied, res.Unstructured != nil)
	}
}

func TestIssue1483_F_ADeniedGetIsDecidedBeforeTheDial(t *testing.T) {
	testMode1483(t)
	watcher1483(t)
	srv, all, object := apiserver1483(t)
	res := getFromAPIServer(bgSAReadCtx(u1483Denied, srv.URL), ref1483(ns1483B))
	require1483Forbidden(t, res)
	if n := all.Load(); n != 0 {
		t.Errorf("RED: the denied read made %d apiserver request(s) (%d of the object), want 0", n, object.Load())
	}
}

func TestIssue1483_A_AnAllowedGetDials(t *testing.T) {
	testMode1483(t)
	watcher1483(t)
	srv, _, object := apiserver1483(t)
	res := getFromAPIServer(bgSAReadCtx(u1483Allowed, srv.URL), ref1483(ns1483B))
	if res.Err != nil || res.Unstructured == nil {
		t.Fatalf("the allowed read must serve the object, got err=%+v", res.Err)
	}
	if got, _, _ := unstructuredString(res.Unstructured.Object, "spec", "marker"); got != "m1483" {
		t.Errorf("served %v, want the apiserver's object", res.Unstructured.Object)
	}
	if n := object.Load(); n != 1 {
		t.Errorf("the allowed read fetched the object %d time(s), want 1", n)
	}
}

func TestIssue1483_U_NoPublishedSnapshotFallsThroughToTheDial(t *testing.T) {
	testMode1483(t)
	t.Setenv("CACHE_ENABLED", "true")
	cache.SetGlobal(nil) // no RBAC snapshot published
	srv, _, object := apiserver1483(t)
	res := getFromAPIServer(bgSAReadCtx(u1483Denied, srv.URL), ref1483(ns1483B))
	if n := object.Load(); n != 1 {
		t.Fatalf("RED: with no published RBAC snapshot the read must dial (RC1 guard); object fetched %d time(s)", n)
	}
	// The post-fetch re-gate still denies, with the same body.
	require1483Forbidden(t, res)
}

func TestIssue1483_S_NoNamespaceForANamespacedGVRFallsThroughToTheDial(t *testing.T) {
	testMode1483(t)
	watcher1483(t)
	prev := preDialScopeFn
	preDialScopeFn = func(gvr schema.GroupVersionResource) (bool, error) { return true, nil }
	t.Cleanup(func() { preDialScopeFn = prev })
	srv, _, object := apiserver1483(t)
	_ = getFromAPIServer(bgSAReadCtx(u1483Denied, srv.URL), ref1483(""))
	if n := object.Load(); n != 1 {
		t.Fatalf("RED: a namespaced GVR read with no namespace must not be authorized cluster-wide before the dial; object fetched %d time(s)", n)
	}
}

func TestIssue1483_L_ALiveReadIsNotDecidedBeforeTheDial(t *testing.T) {
	testMode1483(t)
	watcher1483(t)
	srv, _, object := apiserver1483(t)
	// No background resolve: a live request's own read, which the apiserver
	// authorizes itself; nothing is re-gated or decided here.
	ctx := xcontext.BuildContext(cache.WithInternalRESTConfig(ctxWithUser(u1483Denied), &rest.Config{Host: srv.URL}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "t"}))
	_ = getFromAPIServer(ctx, ref1483(ns1483B))
	if n := object.Load(); n != 1 {
		t.Fatalf("RED: a read that is not a background ServiceAccount read must dial; object fetched %d time(s)", n)
	}
}

func unstructuredString(obj map[string]any, fields ...string) (string, bool, error) {
	cur := any(obj)
	for _, f := range fields {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false, nil
		}
		cur = m[f]
	}
	s, ok := cur.(string)
	return s, ok, nil
}
