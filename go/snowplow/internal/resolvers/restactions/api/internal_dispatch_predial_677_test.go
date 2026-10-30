package api

// internal_dispatch_predial_677_test.go — #677 (1): branch C authorizes a
// per-identity GET-by-name BEFORE the SA dial, through the SAME helper the
// post-fetch re-gate uses, and a deny makes NO dial.
//
// ARMS
//   P1  differential: for user, group-only, tenant SA and learned-class
//       identities, over namespaced/cluster × resourceNames hit/miss ×
//       allow/deny, the pre-dial verdict equals the post-fetch verdict in every
//       cell; and the real dispatch serves on allow (1 dial) and returns the
//       typed re-gate Forbidden on deny with ZERO dials.
//   P2  a namespaced GVR addressed with an EMPTY namespace is never authorized
//       cluster-wide: the pre-dial step falls back to the post-fetch path
//       (one dial, then the post-fetch verdict).
//   P3  the post-fetch re-gate stays load-bearing: the apiserver answers a
//       GET for an allowed namespace with an object whose metadata.namespace is
//       a DENIED one; the result is Forbidden. Mutation P3-m removes the
//       post-fetch re-gate and the same fixture serves the object (RED).
//   P4  a denied GET of a NONEXISTENT object is a 403, not a 404 (intended
//       change: authorize-before-lookup, the apiserver's own order).
//   P5  RC1 is kept: a pre-dial deny under an UNPUBLISHED snapshot falls
//       through (served=false, err=nil), and makes no dial.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var (
	cmGVR677 = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	ktGVR677 = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1alpha1", Resource: "kubernetestargets"}
)

// fakeAPI677 is an httptest apiserver that counts every request (the dial
// counter) and answers a GET-by-name for configmaps (namespaced) and
// kubernetestargets (cluster-scoped). respNS, when set, overrides the
// namespace the returned object claims (the P3 fixture). missing names 404.
type fakeAPI677 struct {
	dials   atomic.Int64
	respNS  atomic.Value // string
	missing map[string]bool
}

func newFakeAPI677(t *testing.T) (*fakeAPI677, *rest.Config) {
	t.Helper()
	f := &fakeAPI677{missing: map[string]bool{}}
	f.respNS.Store("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.dials.Add(1)
		w.Header().Set("Content-Type", "application/json")
		seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		notFound := func() {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		}
		switch {
		// /api/v1/namespaces/<ns>/configmaps/<name>
		case len(seg) == 6 && seg[0] == "api" && seg[4] == "configmaps":
			ns, name := seg[3], seg[5]
			if f.missing[name] {
				notFound()
				return
			}
			if o, _ := f.respNS.Load().(string); o != "" {
				ns = o
			}
			_, _ = fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":%q,"name":%q},"data":{"k":"v"}}`, ns, name)
		// /apis/core.krateo.io/v1alpha1/kubernetestargets/<name>
		case len(seg) == 5 && seg[0] == "apis" && seg[3] == "kubernetestargets":
			if f.missing[seg[4]] {
				notFound()
				return
			}
			_, _ = fmt.Fprintf(w, `{"apiVersion":"core.krateo.io/v1alpha1","kind":"KubernetesTarget","metadata":{"name":%q}}`, seg[4])
		default:
			notFound()
		}
	}))
	t.Cleanup(srv.Close)
	return f, &rest.Config{Host: srv.URL}
}

// The four identity shapes the background drivers resolve under.
type ident677 struct {
	label    string
	username string
	groups   []string
}

var idents677 = []ident677{
	{"user", "u677", nil},
	{"group-only", "", []string{"g677"}},
	{"tenant-sa", "system:serviceaccount:tenant:sa677", nil},
	{"learned-class", "l677", []string{"g677", "other677"}},
}

func cr677(name string, gvr schema.GroupVersionResource, names ...string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{gvr.Group}, Resources: []string{gvr.Resource},
			Verbs: []string{"get"}, ResourceNames: names,
		}},
	}
}

func rb677(ns, name, role string, s rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Subjects:   []rbacv1.Subject{s},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func crb677(name, role string, s rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Subjects:   []rbacv1.Subject{s},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

// rbacSeed677: every identity has a mix of allow/deny, by namespace and by
// resourceNames, on both a namespaced and a cluster-scoped GVR.
func rbacSeed677() []runtime.Object {
	u := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: "u677"}
	g := rbacv1.Subject{Kind: rbacv1.GroupKind, APIGroup: "rbac.authorization.k8s.io", Name: "g677"}
	sa := rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: "tenant", Name: "sa677"}
	l := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: "l677"}
	return []runtime.Object{
		cr677("cm-get-any", cmGVR677),
		cr677("cm-get-catalog", cmGVR677, "catalog"),
		cr677("kt-get-any", ktGVR677),
		cr677("kt-get-t1", ktGVR677, "t1"),
		// user: any configmap in team-a; only t1 cluster-wide.
		rb677("team-a", "u-cm", "cm-get-any", u),
		crb677("u-kt", "kt-get-t1", u),
		// group: only `catalog` in team-a; any target.
		rb677("team-a", "g-cm", "cm-get-catalog", g),
		crb677("g-kt", "kt-get-any", g),
		// tenant SA: only `catalog` in team-b; only t1.
		rb677("team-b", "sa-cm", "cm-get-catalog", sa),
		crb677("sa-kt", "kt-get-t1", sa),
		// learned class: its own grant in team-b + everything g677 has.
		rb677("team-b", "l-cm", "cm-get-any", l),
	}
}

func newWatcher677(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	kinds[cmGVR677] = "ConfigMapList"
	kinds[ktGVR677] = "KubernetesTargetList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, rbacSeed677()...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
}

// scope677 is the test scope resolver: configmaps namespaced, targets cluster.
func scope677(t *testing.T) {
	t.Helper()
	prev := preDialScopeFn
	preDialScopeFn = func(gvr schema.GroupVersionResource) (bool, error) {
		switch gvr {
		case cmGVR677:
			return true, nil
		case ktGVR677:
			return false, nil
		}
		return false, errors.New("unknown gvr")
	}
	t.Cleanup(func() { preDialScopeFn = prev })
}

type target677 struct {
	gvr       schema.GroupVersionResource
	ns, name  string
	path      string
	clusterSc bool
}

func targets677() []target677 {
	var out []target677
	for _, ns := range []string{"team-a", "team-b"} {
		for _, n := range []string{"catalog", "other"} {
			out = append(out, target677{cmGVR677, ns, n, "/api/v1/namespaces/" + ns + "/configmaps/" + n, false})
		}
	}
	for _, n := range []string{"t1", "t2"} {
		out = append(out, target677{ktGVR677, "", n, "/apis/core.krateo.io/v1alpha1/kubernetestargets/" + n, true})
	}
	return out
}

func TestIssue677_P1_PreDialEqualsPostFetch_AndDenyNeverDials(t *testing.T) {
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)

	for _, id := range idents677 {
		var allow, deny, rnHit, rnMiss int
		for _, tg := range targets677() {
			ctx := cache.WithInternalRESTConfig(ctxWithUser(id.username, id.groups...), rc)
			obj := &unstructured.Unstructured{}
			obj.SetNamespace(tg.ns)
			obj.SetName(tg.name)
			pre := authorizeGetByRBAC(ctx, tg.gvr, tg.ns, tg.name)
			post := filterGetByRBAC(ctx, tg.gvr, obj)
			if pre != post {
				t.Fatalf("%s %s: pre-dial=%v post-fetch=%v — the two checks disagree", id.label, tg.path, pre, post)
			}
			if tg.name == "catalog" || tg.name == "t1" {
				rnHit++
			} else {
				rnMiss++
			}

			d0 := f.dials.Load()
			raw, served, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, tg.path))
			dials := f.dials.Load() - d0
			if pre {
				allow++
				if err != nil || !served || len(raw) == 0 || dials != 1 {
					t.Fatalf("%s %s allowed: served=%v err=%v dials=%d, want served with 1 dial", id.label, tg.path, served, err, dials)
				}
			} else {
				deny++
				if served || raw != nil || !apierrors.IsForbidden(err) || !isRegateDenied(err) {
					t.Fatalf("%s %s denied: served=%v err=%v, want the typed re-gate Forbidden", id.label, tg.path, served, err)
				}
				if dials != 0 {
					t.Fatalf("RED: %s %s denied but the SA dial ran (%d) — the deny must be decided before the dial", id.label, tg.path, dials)
				}
			}
		}
		if allow == 0 || deny == 0 || rnHit == 0 || rnMiss == 0 {
			t.Fatalf("%s: fixture lacks a cell (allow=%d deny=%d rnHit=%d rnMiss=%d)", id.label, allow, deny, rnHit, rnMiss)
		}
		t.Logf("%s: allow=%d deny=%d", id.label, allow, deny)
	}
}

func TestIssue677_P2_NamespacedGVRWithEmptyNamespaceFallsBackToPostFetch(t *testing.T) {
	newWatcher677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	f.respNS.Store("team-a")
	// The GVR is namespaced: the pre-dial step must not evaluate it cluster-wide.
	prev := preDialScopeFn
	called := 0
	preDialScopeFn = func(schema.GroupVersionResource) (bool, error) { called++; return true, nil }
	t.Cleanup(func() { preDialScopeFn = prev })

	if ns, ok := preDialAuthzNamespace(ktGVR677, ""); ok {
		t.Fatalf("a namespaced GVR with an empty ref namespace was pre-authorized at ns=%q", ns)
	}
	// And a scope-resolution error falls back too.
	preDialScopeFn = func(schema.GroupVersionResource) (bool, error) { return false, errors.New("mapper cold") }
	if _, ok := preDialAuthzNamespace(ktGVR677, ""); ok {
		t.Fatal("a scope error was treated as cluster-scoped")
	}
	if called != 1 {
		t.Fatalf("scope resolver called %d times, want 1", called)
	}
	_ = rc
}

// P3 / P3-m — the post-fetch re-gate is kept and load-bearing.
func runP3(t *testing.T) (served bool, err error) {
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	// u677 may get any configmap in team-a, nothing in team-b. The apiserver
	// answers the team-a GET with an object that claims team-b.
	f.respNS.Store("team-b")
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	_, served, err = dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/other"))
	return served, err
}

func TestIssue677_P3_PostFetchRegateStillDenies(t *testing.T) {
	served, err := runP3(t)
	if served || !apierrors.IsForbidden(err) || !isRegateDenied(err) {
		t.Fatalf("an object whose own namespace is denied was served (served=%v err=%v)", served, err)
	}
}

func TestIssue677_P3m_Mutation_NoPostFetchRegate_Serves(t *testing.T) {
	prev := postFetchRegateFn
	postFetchRegateFn = func(context.Context, schema.GroupVersionResource, *unstructured.Unstructured) bool { return true }
	t.Cleanup(func() { postFetchRegateFn = prev })
	served, err := runP3(t)
	if !served || err != nil {
		t.Fatalf("mutation: without the post-fetch re-gate the fixture should serve the denied object; got served=%v err=%v — the P3 arm cannot see its defect", served, err)
	}
	t.Log("P3-m RED as required: the post-fetch re-gate is load-bearing")
}

func TestIssue677_P4_DeniedNonexistentIs403Not404(t *testing.T) {
	newWatcher677(t)
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	f.missing["ghost"] = true
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	_, _, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, "/api/v1/namespaces/team-b/configmaps/ghost"))
	if !apierrors.IsForbidden(err) || f.dials.Load() != 0 {
		t.Fatalf("denied nonexistent object: err=%v dials=%d, want a 403 with no dial", err, f.dials.Load())
	}
	// Allowed nonexistent stays a 404 from the apiserver.
	_, _, err = dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, "/api/v1/namespaces/team-a/configmaps/ghost"))
	if !apierrors.IsNotFound(err) {
		t.Fatalf("allowed nonexistent object: err=%v, want 404", err)
	}
}

func TestIssue677_P5_RC1_UnpublishedSnapshotFallsThroughWithoutDial(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.SetGlobal(nil) // UNPUBLISHED
	t.Cleanup(func() { cache.SetGlobal(nil) })
	scope677(t)
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	f, rc := newFakeAPI677(t)
	ctx := cache.WithInternalRESTConfig(ctxWithUser("u677"), rc)
	raw, served, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, "/api/v1/namespaces/team-b/configmaps/catalog"))
	if served || err != nil || raw != nil {
		t.Fatalf("RC1: want fall-through (nil,false,nil); got served=%v err=%v", served, err)
	}
	if f.dials.Load() != 0 {
		t.Fatalf("RC1 fall-through made %d SA dial(s)", f.dials.Load())
	}
}
