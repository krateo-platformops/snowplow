package api

// issue961_forbidden_list_test.go — #961: a LIST stage running as the caller
// gets ONE answer for a denied list, whichever path serves it.
//
// THE DEFECT (origin/main 4085c447). Served from snowplow's own copy (the
// informer, the api-stage content cell), filterListByRBAC dropped every item a
// caller with NO grant may not list and answered 200 `items: []`: nothing under
// errorKey, no Forbidden in X-Snowplow-Stage-Outcomes. Served directly (branch
// E: the GVR not servable — right after boot, a watch break, a lazily
// registered type), the apiserver answered 403, recorded under errorKey as the
// identity-free Forbidden Status (#681). Same request, same identity, two
// answers depending on cache state.
//
// ARMS — every paired arm drives the REAL api.ResolveWithFailure for the SAME
// caller and the SAME stage three ways, and compares what a RESTAction sees
// (the stage value, the errorKey value, the stage-outcome header, the stage
// failure, the per-identity forbidden sink):
//   - content: informer-served through the api-stage content cell (the default
//     with the cache on);
//   - inline:  informer-served through dispatchViaInformer's inline gate
//     (RESOLVED_CACHE_ENABLED=false);
//   - direct:  no informer (cache.Global nil) → branch E dials the caller's own
//     apiserver, a fake that answers exactly what Kubernetes answers that
//     caller for that request (403 naming the requester, or the list).
//
//   P1  no grant, cluster-wide list          → 403 on every path, one errorKey body
//   P2  no grant, namespaced list            → 403 on every path
//   P3  cluster grant, cluster-wide list     → 200, the same items
//   P4  namespace grant, its namespace       → 200, the same items
//   P5  namespace grant, another namespace   → 403 on every path
//   P6  namespace grant, its EMPTY namespace → 200 `items: []` (allowed-empty)
//   P7  cluster grant, empty cluster-scoped  → 200 `items: []`
//   P8  namespace grant, cluster-scoped list → 403 (a RoleBinding grants nothing
//       at the cluster scope) — on an EMPTY collection: decided from the
//       rules, never from the items
//   P9  continueOnError=false: the stage failure is the same on every path
//   P10 namespace grant, cluster-wide list   → 403 on every path (Diego's
//       ruling: match Kubernetes; a cross-namespace view is userAccessFilter's)
//   P11 kind=Role RoleBinding, its namespace → 200: the Role is looked up in the
//       binding's own namespace
//   P12 kind=Role RoleBinding, cluster-wide  → 403
//   P13 20 non-permitting RoleBindings ahead of the permitting one, in the
//       listed namespace → 200: no candidate's "no" ends the walk
//   C1  branch C (the SA read re-gated for the caller): a LIST without a grant
//       at its scope is the typed re-gate Forbidden with the canonical text, a
//       granted one serves
//
// Mutations (logic-only; each turns the named arms RED) are tabled in the PR.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// Synthetic identities (no real user names).
const (
	u961None    = "u961-none"    // no grant at all
	u961Cluster = "u961-cluster" // list restactions + kubernetestargets cluster-wide
	u961NS      = "u961-ns"      // list both, via RoleBindings in team-a and team-c only
	u961Role    = "u961-role"    // list restactions in team-r, via a RoleBinding to a namespaced Role
	u961Many    = "u961-many"    // 20 non-permitting RoleBindings in team-a, then a permitting one
	g961        = "g961-cohort"
	g961Many    = "g961-many" // the permitting binding's subject: routed after the user's own bindings
)

// groups961 is the group set each synthetic identity presents.
func groups961(user string) []string {
	if user == u961Many {
		return []string{g961, g961Many}
	}
	return []string{g961}
}

const (
	p961Cluster = "/apis/templates.krateo.io/v1/restactions"
	p961TeamA   = "/apis/templates.krateo.io/v1/namespaces/team-a/restactions"
	p961TeamB   = "/apis/templates.krateo.io/v1/namespaces/team-b/restactions"
	p961TeamC   = "/apis/templates.krateo.io/v1/namespaces/team-c/restactions" // granted, no objects
	p961TeamR   = "/apis/templates.krateo.io/v1/namespaces/team-r/restactions"
	p961KT      = "/apis/core.krateo.io/v1alpha1/kubernetestargets" // cluster-scoped, no objects
)

// seed961Items is the restactions collection: two in team-a, two in team-b,
// one in team-r.
var seed961Items = [][2]string{{"team-a", "a-1"}, {"team-a", "a-2"}, {"team-b", "b-1"}, {"team-b", "b-2"}, {"team-r", "r-1"}}

func rbac961Seed() []runtime.Object {
	tm := func(kind string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind}
	}
	lister := &rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "lister-961"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{dispatchTestGVR.Group, ktGVR677.Group},
			Resources: []string{dispatchTestGVR.Resource, ktGVR677.Resource},
			Verbs:     []string{"list"},
		}}}
	user := func(n string) []rbacv1.Subject {
		return []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: n}}
	}
	ref := rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "lister-961"}
	out := []runtime.Object{
		lister,
		&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: metav1.ObjectMeta{Name: "crb-961"},
			Subjects: user(u961Cluster), RoleRef: ref},
	}
	for _, ns := range []string{"team-a", "team-c"} {
		out = append(out, &rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"),
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rb-961"}, Subjects: user(u961NS), RoleRef: ref})
	}
	// kind=Role: a namespaced Role in team-r, bound in team-r (reviewer M11).
	out = append(out,
		&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-r", Name: "role-lister-961"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{dispatchTestGVR.Group}, Resources: []string{dispatchTestGVR.Resource}, Verbs: []string{"list"}}}},
		&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-r", Name: "rb-role-961"},
			Subjects: user(u961Role), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "role-lister-961"}})
	// Many non-permitting candidates first (reviewer M17): 20 RoleBindings in
	// team-a bind u961Many directly to a role on ANOTHER resource; the one that
	// permits binds its group. User-subject candidates are walked before group
	// ones, so a walk that stops at the first candidate's "no" denies.
	out = append(out, &rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "other-961"},
		Rules: []rbacv1.PolicyRule{{APIGroups: []string{"other.example"}, Resources: []string{"things"}, Verbs: []string{"list"}}}})
	for i := 0; i < 20; i++ {
		out = append(out, &rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"),
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: fmt.Sprintf("rb-other-961-%02d", i)},
			Subjects:   user(u961Many), RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "other-961"}})
	}
	out = append(out, &rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"),
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "rb-many-961"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: g961Many}}, RoleRef: ref})
	return out
}

func obj961(ns, name string) *unstructured.Unstructured {
	return newTestRestActionObject(ns, name, "m961")
}

// watcher961 is a synced cache=on watcher holding the restactions collection
// and an EMPTY kubernetestargets collection, with the RBAC seed published.
func watcher961(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	kinds[ktGVR677] = "KubernetesTargetList"
	seed := rbac961Seed()
	for _, it := range seed961Items {
		seed = append(seed, obj961(it[0], it[1]))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	for _, gvr := range []schema.GroupVersionResource{dispatchTestGVR, ktGVR677} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s informer did not sync", gvr)
		}
	}
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

// apiAnswer961 is what the Kubernetes apiserver answers one caller for one
// request — written down from Kubernetes semantics, never computed by
// snowplow's evaluator (the instrument under test).
type apiAnswer961 struct {
	forbidden bool
	items     [][2]string // the list, when allowed
	kind      string
	apiVer    string
}

func listBody961(a apiAnswer961) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"apiVersion":%q,"kind":%q,"metadata":{"resourceVersion":"7"},"items":[`, a.apiVer, a.kind)
	for i, it := range a.items {
		if i > 0 {
			b.WriteByte(',')
		}
		raw, _ := json.Marshal(obj961(it[0], it[1]).Object)
		b.Write(raw)
	}
	b.WriteString(`]}`)
	return b.String()
}

// fakeAPI961 is the caller's OWN apiserver (branch E). Its 403 names the
// requester exactly as a real one does; none of that may reach a result.
func fakeAPI961(t *testing.T, user string, answer apiAnswer961) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var dials atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if answer.forbidden {
			w.WriteHeader(http.StatusForbidden)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,`+
				`"message":"restactions.templates.krateo.io is forbidden: User \"%s\" cannot list resource \"restactions\" in API group \"templates.krateo.io\" at the cluster scope: groups [\"%s\"]"}`,
				user, g961)
			return
		}
		_, _ = io.WriteString(w, listBody961(answer))
	}))
	t.Cleanup(srv.Close)
	return srv, &dials
}

// seen961 is everything a RESTAction (and its caller) can observe of one stage.
type seen961 struct {
	value     string // canonical JSON of dict["ra"], "" when absent
	errKey    string // canonical JSON of the errorKey value, "" when absent
	header    string
	failure   string // the stage failure, "" when none
	forbidden int64  // per-identity forbidden sink (stage)
	dials     int64
}

type arm961 string

const (
	armContent arm961 = "content"
	armInline  arm961 = "inline"
	armDirect  arm961 = "direct"
)

// resolve961 runs ONE resolve of a single LIST stage as user, on arm.
func resolve961(t *testing.T, rw *cache.ResourceWatcher, arm arm961, user, path string, coe bool, answer apiAnswer961) seen961 {
	t.Helper()
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	if arm == armInline {
		t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	} else {
		t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	}
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	if arm == armDirect {
		cache.SetGlobal(nil)
		defer cache.SetGlobal(rw)
	}
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)

	srv, dials := fakeAPI961(t, user, answer)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secret := kubeutil.MakeDNS1123Compatible(user) + "-clientconfig"
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		secret: {ObjectMeta: metav1.ObjectMeta{Namespace: "krateo-system", Name: secret},
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok")}},
	}})
	cache.ForceSecretsCacheReadyForTest("krateo-system")
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()

	ctx, s := withSinks681(ctxWithUser(user, groups961(user)...))
	ctx, outcomes := WithStageOutcomes(ctx)
	stage := &templates.API{Name: "ra", Path: path, Verb: ptr.To(http.MethodGet),
		ContinueOnError: ptr.To(coe), ErrorKey: ptr.To("raErr")}
	dict, failure := ResolveWithFailure(ctx, ResolveOptions{RC: &rest.Config{Host: srv.URL}, AuthnNS: "krateo-system",
		Items: []*templates.API{stage}, RESTActionNamespace: "krateo-system", RESTActionName: "ra961"})

	out := seen961{header: outcomes.HeaderValue(), forbidden: s.forb.Stage(), dials: dials.Load()}
	if v, ok := dict["ra"]; ok {
		out.value = canon961(t, v)
	}
	if v, ok := dict["raErr"]; ok {
		out.errKey = canon961(t, accumulatedFirst(v))
	}
	if failure != nil {
		out.failure = fmt.Sprintf("%s/%d/%s/%s", failure.Stage, failure.Code, failure.Reason, failure.Message)
	}
	whole, _ := json.Marshal(dict)
	if strings.Contains(string(whole), user) || strings.Contains(string(whole), "g961") {
		t.Fatalf("%s: identity text reached the result: %s", arm, whole)
	}
	return out
}

// canon961 is a canonical JSON form with list items sorted by namespace/name,
// so informer order and apiserver order compare equal.
func canon961(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var g any
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m, ok := g.(map[string]any); ok {
		if items, ok := m["items"].([]any); ok {
			key := func(v any) string {
				it, _ := v.(map[string]any)
				md, _ := it["metadata"].(map[string]any)
				ns, _ := md["namespace"].(string)
				n, _ := md["name"].(string)
				return ns + "/" + n
			}
			sort.SliceStable(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
		}
	}
	b, _ = json.Marshal(g)
	return string(b)
}

// itemNames961 returns the sorted ns/name of a stage value's items.
func itemNames961(t *testing.T, value string) []string {
	t.Helper()
	var env struct {
		Items []struct {
			Metadata struct{ Namespace, Name string } `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(value), &env); err != nil {
		t.Fatalf("stage value is not a list: %v (%s)", err, value)
	}
	out := []string{}
	for _, it := range env.Items {
		out = append(out, it.Metadata.Namespace+"/"+it.Metadata.Name)
	}
	sort.Strings(out)
	return out
}

func forbiddenBody961(gvr schema.GroupVersionResource, ns string) string {
	b, _ := json.Marshal(canonicalForbiddenBody(canonicalForbiddenMessage(gvr, ns, "", "list")))
	return string(b)
}

const forbiddenHeader961 = `[{"name":"ra","ok":false,"reason":"Forbidden"}]`

type pair961 struct {
	name   string
	user   string
	path   string
	coe    bool
	answer apiAnswer961
	// expectations, asserted on EVERY arm
	forbidden bool
	gvr       schema.GroupVersionResource
	ns        string
	items     []string // allowed: the sorted ns/name the stage holds
}

func raList961(items ...[2]string) apiAnswer961 {
	return apiAnswer961{items: items, kind: "RestActionList", apiVer: "templates.krateo.io/v1"}
}

func TestIssue961_PairedInformerVersusDirect(t *testing.T) {
	rw := watcher961(t)
	ktEmpty := apiAnswer961{kind: "KubernetesTargetList", apiVer: "core.krateo.io/v1alpha1"}
	forbid := apiAnswer961{forbidden: true}
	cases := []pair961{
		{name: "P1/no-grant/cluster-wide", user: u961None, path: p961Cluster, coe: true, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: ""},
		{name: "P2/no-grant/namespaced", user: u961None, path: p961TeamA, coe: true, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: "team-a"},
		{name: "P3/cluster-grant/cluster-wide", user: u961Cluster, path: p961Cluster, coe: true,
			answer: raList961(seed961Items...), items: []string{"team-a/a-1", "team-a/a-2", "team-b/b-1", "team-b/b-2", "team-r/r-1"}},
		{name: "P4/ns-grant/allowed-namespace", user: u961NS, path: p961TeamA, coe: true,
			answer: raList961(seed961Items[0], seed961Items[1]), items: []string{"team-a/a-1", "team-a/a-2"}},
		{name: "P5/ns-grant/denied-namespace", user: u961NS, path: p961TeamB, coe: true, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: "team-b"},
		{name: "P6/ns-grant/allowed-empty-namespace", user: u961NS, path: p961TeamC, coe: true,
			answer: raList961(), items: []string{}},
		{name: "P7/cluster-grant/empty-cluster-scoped", user: u961Cluster, path: p961KT, coe: true,
			answer: ktEmpty, items: []string{}},
		{name: "P8/ns-grant/cluster-scoped-is-denied-on-empty", user: u961NS, path: p961KT, coe: true, answer: forbid,
			forbidden: true, gvr: ktGVR677, ns: ""},
		{name: "P9/no-grant/without-continueOnError", user: u961None, path: p961TeamB, coe: false, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: "team-b"},
		{name: "P10/ns-grant/cluster-wide-is-denied", user: u961NS, path: p961Cluster, coe: true, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: ""},
		{name: "P11/kind-Role-grant/its-namespace", user: u961Role, path: p961TeamR, coe: true,
			answer: raList961(seed961Items[4]), items: []string{"team-r/r-1"}},
		{name: "P12/kind-Role-grant/cluster-wide-is-denied", user: u961Role, path: p961Cluster, coe: true, answer: forbid,
			forbidden: true, gvr: dispatchTestGVR, ns: ""},
		{name: "P13/non-permitting-candidates-first/its-namespace", user: u961Many, path: p961TeamA, coe: true,
			answer: raList961(seed961Items[0], seed961Items[1]), items: []string{"team-a/a-1", "team-a/a-2"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			seen := map[arm961]seen961{}
			for _, arm := range []arm961{armContent, armInline, armDirect} {
				s := resolve961(t, rw, arm, c.user, c.path, c.coe, c.answer)
				seen[arm] = s
				t.Logf("%s: value=%s errKey=%s header=%s failure=%q forb=%d dials=%d",
					arm, s.value, s.errKey, s.header, s.failure, s.forbidden, s.dials)
				if arm == armDirect && s.dials == 0 {
					t.Fatalf("direct arm made no dial: the informer served it, the pairing is void")
				}
				if arm != armDirect && s.dials != 0 {
					t.Fatalf("%s arm dialed the apiserver %d times: not informer-served", arm, s.dials)
				}
				if c.forbidden {
					if want := forbiddenBody961(c.gvr, c.ns); s.errKey != want {
						t.Fatalf("%s: errorKey\n got %s\nwant %s", arm, s.errKey, want)
					}
					if s.header != forbiddenHeader961 {
						t.Fatalf("%s: stage-outcome header %s, want %s", arm, s.header, forbiddenHeader961)
					}
					if s.value != "" {
						t.Fatalf("%s: a denied list produced a stage value %s", arm, s.value)
					}
					if s.forbidden != 1 {
						t.Fatalf("%s: per-identity forbidden sink = %d, want 1", arm, s.forbidden)
					}
					continue
				}
				if s.errKey != "" || s.header != "[]" || s.failure != "" {
					t.Fatalf("%s: an allowed list failed: errKey=%s header=%s failure=%q", arm, s.errKey, s.header, s.failure)
				}
				if got := itemNames961(t, s.value); strings.Join(got, ",") != strings.Join(c.items, ",") {
					t.Fatalf("%s: items %v, want %v", arm, got, c.items)
				}
			}
			// One answer: everything but the dial count is identical on every arm.
			ref := seen[armDirect]
			ref.dials = 0
			for _, arm := range []arm961{armContent, armInline} {
				if got := seen[arm]; got != ref {
					t.Fatalf("%s differs from direct:\n%s: %+v\ndirect: %+v", arm, arm, got, ref)
				}
			}
		})
	}
}

// C1 — branch C (snowplow's SA read, re-gated for the caller) answers a
// no-grant LIST with the typed re-gate Forbidden whose text is the canonical
// message the direct path records; resolve.go records both with one body
// (recordRBACDeny, pinned for GET by #681 A1/A3).
func TestIssue961_BranchC_NoGrantList_IsTheCanonicalForbidden(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	t.Setenv("CACHE_ENABLED", "true")
	kinds := dispatchTestListKinds()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), kinds, rbac961Seed()...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	var items []fakeRAItem
	for _, it := range seed961Items {
		items = append(items, fakeRAItem{it[0], it[1]})
	}
	rc := newFakeRestActionAPIServer(t, items)
	for _, c := range []struct {
		user, path, ns string
		denied         bool
		want           []string
	}{
		{u961None, p961Cluster, "", true, nil},
		{u961NS, p961TeamB, "team-b", true, nil},
		{u961NS, p961Cluster, "", true, nil},
		{u961NS, p961TeamA, "team-a", false, []string{"team-a"}},
		{u961Cluster, p961Cluster, "", false, []string{"team-a", "team-b", "team-r"}},
	} {
		raw, served, err := dispatchViaInternalRESTConfig(cache.WithInternalRESTConfig(ctxWithUser(c.user), rc),
			buildCall(http.MethodGet, c.path))
		if c.denied {
			if served || !isRegateDenied(err) {
				t.Fatalf("%s %s: served=%v err=%v, want the typed re-gate Forbidden", c.user, c.path, served, err)
			}
			if want := canonicalForbiddenMessage(dispatchTestGVR, c.ns, "", "list"); err.Error() != want {
				t.Fatalf("%s %s: text %q, want the canonical %q", c.user, c.path, err.Error(), want)
			}
			continue
		}
		if err != nil || !served {
			t.Fatalf("%s %s: served=%v err=%v, want served", c.user, c.path, served, err)
		}
		if got := sortedSet(servedItemNamespaces(t, raw)); strings.Join(got, ",") != strings.Join(c.want, ",") {
			t.Fatalf("%s %s: namespaces %v, want %v", c.user, c.path, got, c.want)
		}
	}
}

// M1 — the deny names its scope the way the apiserver does: "at the cluster
// scope" or "in the namespace "<ns>"", never a dangling "/" (reviewer nit). A
// named object keeps the "<ns>/<name>" form objects.Get shares.
func TestIssue961_CanonicalMessage_NamesTheScope(t *testing.T) {
	for _, c := range []struct{ ns, name, verb, want string }{
		{"", "", "list", `restactions.templates.krateo.io is forbidden: user not authorized to list at the cluster scope`},
		{"team-b", "", "list", `restactions.templates.krateo.io is forbidden: user not authorized to list in the namespace "team-b"`},
		{"team-b", "catalog", "get", `restactions.templates.krateo.io "catalog" is forbidden: user not authorized to get team-b/catalog`},
	} {
		if got := canonicalForbiddenMessage(dispatchTestGVR, c.ns, c.name, c.verb); got != c.want {
			t.Fatalf("(%q,%q,%q):\n got %s\nwant %s", c.ns, c.name, c.verb, got, c.want)
		}
	}
}
