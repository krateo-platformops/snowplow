// apistage_list_overserve_falsifier_test.go — #58 SECURITY falsifier
// (UAF-AWARE, 3 arms). Architect-spec'd: the UAF arm is the real proof.
//
// THE VULN (pre-existing, 0.30.242): the hot pre-parsed-LIST apistage serve
// (`serveParsedListEnvelope`) rendered the cell's items with NO serve-time
// filter. The cell is populated SA-MAXIMAL / un-gated (dispatchViaInformer
// skips its inline filterListByRBAC under WithApistageContentResolve), so a
// NARROW tenant Get-hitting it received the FULL cluster-wide list —
// cross-tenant enumeration (HIGH, customer-reachable).
//
// THE FIX (UAF-AWARE — a blunt uniform revert would BREAK UAF):
//   - NON-UAF LIST → gateListItems (filterListByRBAC: raw `list` RBAC). Fixes
//     the over-serve.
//   - UAF LIST → serveParsedListEnvelope UNCHANGED (pass SA-maximal through);
//     the UAF refilter (applyUserAccessFilter) narrows DOWNSTREAM by the UAF
//     RULE (verb/resource/NamespaceFrom), which DIVERGES from raw `list` RBAC
//     — a UAF step is called with elevated privilege precisely because the
//     requester LACKS the raw RBAC. Routing it through gateListItems would
//     strip everything → the refilter gets nothing → UAF broken.
//
// ARMS:
//  1. UAF (architect-spec'd, the real proof — END-TO-END serve→refilter): a
//     SA-maximal LIST `namespaces` cell across 3 ns; a DIVERGENT UAF
//     (verb=get on compositions, NamespaceFrom=.metadata.name — the
//     blueprint/namespaces shape); a narrow identity with NO raw `list
//     namespaces` but compositions access in bench-ns-01 ONLY → serve
//     (uafActive=true, must pass the full set) THEN refilter → assert kept ==
//     EXACTLY {bench-ns-01}. ONE assertion catches BOTH failures: {} = raw
//     filter broke UAF (the blunt-revert bug), {all 3} = no narrowing
//     (over-serve). GREEN only for the UAF-aware serve.
//  2. no-UAF (over-serve fix): SA-maximal widgets cell across 4 ns; narrow
//     user with raw `list` ns-A/ns-B → serve (uafActive=false) → == granted ns.
//     RED pre-fix = all 4 leak.
//  3. GET-unchanged: a GET-by-name no-UAF serve stays gated (filterGetByRBAC).
//
// Hermetic: dynamicfake + the real in-process RBAC indexer.

package api

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// ── ARM 1: UAF end-to-end (serve → refilter), the architect's real proof ──

// nsGVR is the LISTed GVR for the UAF arm (core namespaces — a cluster-scope
// LIST a UAF step elevates to without the requester holding `list namespaces`).
var nsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

// seedNamespacesCell Puts a SA-maximal pre-parsed LIST `namespaces` cell
// across the given namespaces — the un-gated populate shape.
func seedNamespacesCell(t *testing.T, nss ...string) {
	t.Helper()
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}
	items := make([]*unstructured.Unstructured, 0, len(nss))
	for _, ns := range nss {
		items = append(items, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]any{"name": ns},
		}})
	}
	key := cache.ComputeKey(contentKeyInputs(nsGVR, "", ""))
	store.Put(key, &cache.ResolvedEntry{
		RawJSON:         []byte(`{"apiVersion":"v1","kind":"NamespaceList","items":[]}`),
		Inputs:          ptrTo(contentKeyInputs(nsGVR, "", "")),
		Items:           items,
		ItemsAPIVersion: "v1",
		ItemsKind:       "NamespaceList",
	})
}

// newUAFNamespacesWatcher mirrors newRefilterTestWatcher but ALSO registers a
// NamespaceList list-kind, because the UAF arm's apistage LIST content-HIT
// heal re-touch (EnsureResourceType(namespaces)) spawns a namespaces informer
// — the fake client panics on an unregistered list-kind. Seeds the RBAC
// objects + publishes the watcher as cache.Global.
func newUAFNamespacesWatcher(t *testing.T, seed ...runtime.Object) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := runtime.NewScheme()
	_ = rbacv1.AddToScheme(sch)

	lk := map[schema.GroupVersionResource]string{
		nsGVR: "NamespaceList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, lk, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

// TestFalsifier58_UAF_EndToEnd_NarrowsToUAFScope is the architect-spec'd UAF
// proof: serve (UAF, must pass full set) → refilter (UAF rule narrows) →
// EXACTLY {bench-ns-01}.
func TestFalsifier58_UAF_EndToEnd_NarrowsToUAFScope(t *testing.T) {
	// RBAC: "devs" group gets compositions in bench-ns-01 ONLY — and NO
	// `list namespaces` anywhere. So raw filterListByRBAC(namespaces) would
	// drop ALL items; only the UAF rule (compositions-get, ns from the
	// namespace's own name) keeps bench-ns-01.
	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "ns01-comp-reader", Namespace: "bench-ns-01"},
		Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{"composition.krateo.io"}, Resources: []string{"compositions"}},
		},
	}
	binding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "ns01-comp-reader-binding", Namespace: "bench-ns-01"},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "ns01-comp-reader"},
	}
	newUAFNamespacesWatcher(t, role, binding)
	seedNamespacesCell(t, "bench-ns-01", "bench-ns-02", "bench-ns-03")

	uafStage := &templates.API{
		Name: "namespaces",
		UserAccessFilter: &templates.UserAccessFilterSpec{
			Verb:          "get",
			Group:         "composition.krateo.io",
			Resource:      "compositions",
			NamespaceFrom: ".metadata.name", // DIVERGENT: ns == the namespace's own name
		},
	}
	ctx := ctxWithUser("cyberjoker", "devs")

	// STEP 1 — serve the SA-maximal cell with uafActive=true. It MUST pass the
	// full set (un-narrowed) so the refilter can apply the UAF rule. If the
	// fix wrongly raw-filtered here, the user (no `list namespaces`) gets {} →
	// the refilter then keeps nothing → UAF broken.
	store := cache.ResolvedCache()
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/api/v1/namespaces", Verb: ptr.To(http.MethodGet),
	}}
	v, served, ok := apistageContentServe(ctx, store, call, true /*uafActive*/)
	if !ok || !served {
		t.Fatalf("UAF serve: expected served=true (full set passes through); ok=%v served=%v", ok, served)
	}
	env, _ := v.(map[string]any)

	// STEP 2 — feed the served envelope to the UAF refilter (handler-chain
	// equivalent: applyUserAccessFilter narrows by the UAF rule).
	dict := map[string]any{"namespaces": env}
	res := applyUserAccessFilter(ctx, dict, uafStage)

	// ASSERT — exactly {bench-ns-01}. Catches {} (raw filter broke UAF) AND
	// {all 3} (no narrowing / over-serve).
	got := keptNamespaceNames(t, dict["namespaces"])
	want := []string{"bench-ns-01"}
	if !equalStrs(got, want) {
		t.Fatalf("#58 UAF END-TO-END: serve→refilter kept %v, want EXACTLY %v. "+
			"{} ⇒ the apistage serve raw-filtered a UAF step (blunt-revert bug, breaks UAF); "+
			"{all 3} ⇒ no narrowing (over-serve). GREEN only for the UAF-aware serve "+
			"(pass-through) + UAF refilter (narrow). (res.Kept=%d Dropped=%d)", got, want, res.Kept, res.Dropped)
	}
}

func keptNamespaceNames(t *testing.T, wrapper any) []string {
	t.Helper()
	w, _ := wrapper.(map[string]any)
	items, _ := w["items"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		obj, _ := it.(map[string]any)
		meta, _ := obj["metadata"].(map[string]any)
		if n, _ := meta["name"].(string); n != "" {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// ── ARM 2: no-UAF over-serve fix (reuses the F1 widget watcher) ──

func seedOverserveCell(t *testing.T) {
	t.Helper()
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}
	items := make([]*unstructured.Unstructured, 0, len(f1AllNamespaces))
	for _, ns := range f1AllNamespaces {
		items = append(items, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "widgets.krateo.io/v1", "kind": "Widget",
			"metadata": map[string]any{"namespace": ns, "name": "widget-" + ns},
		}})
	}
	key := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	store.Put(key, &cache.ResolvedEntry{
		RawJSON:         []byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","items":[]}`),
		Inputs:          ptrTo(contentKeyInputs(f1WidgetsGVR, "", "")),
		Items:           items,
		ItemsAPIVersion: "widgets.krateo.io/v1",
		ItemsKind:       "WidgetList",
	})
}

func servedWidgetNamespaces(t *testing.T, user jwtutil.UserInfo) (nss []string, served bool) {
	t.Helper()
	store := cache.ResolvedCache()
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(user))
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/widgets.krateo.io/v1/widgets", Verb: ptr.To(http.MethodGet),
	}}
	v, servedOK, ok := apistageContentServe(ctx, store, call, false /*no-UAF*/)
	if !ok {
		t.Fatalf("apistageContentServe ok=false")
	}
	if !servedOK {
		return nil, false
	}
	env, _ := v.(map[string]any)
	rawItems, _ := env["items"].([]any)
	seen := map[string]bool{}
	for _, it := range rawItems {
		obj, _ := it.(map[string]any)
		meta, _ := obj["metadata"].(map[string]any)
		if ns, _ := meta["namespace"].(string); ns != "" {
			seen[ns] = true
		}
	}
	for ns := range seen {
		nss = append(nss, ns)
	}
	sort.Strings(nss)
	return nss, true
}

func TestFalsifier58_NoUAF_NarrowTenantGetsOnlyOwnNamespaces(t *testing.T) {
	_ = newF1Watcher(t)
	seedOverserveCell(t)

	narrowNss, served := servedWidgetNamespaces(t, jwtutil.UserInfo{Username: f1NarrowUser})
	if !served {
		t.Fatalf("narrow tenant: expected a served (gated) LIST")
	}
	want := append([]string{}, f1NarrowNamespaces...)
	sort.Strings(want)
	if !equalStrs(narrowNss, want) {
		t.Fatalf("#58 SECURITY (no-UAF): narrow tenant received %v, want ONLY %v — over-serve "+
			"(serveParsedListEnvelope skipped serve-time filterListByRBAC).", narrowNss, want)
	}

	broadNss, servedB := servedWidgetNamespaces(t, jwtutil.UserInfo{Username: f1BroadUser})
	if !servedB {
		t.Fatalf("broad tenant: expected a served LIST")
	}
	wantAll := append([]string{}, f1AllNamespaces...)
	sort.Strings(wantAll)
	if !equalStrs(broadNss, wantAll) {
		t.Fatalf("broad tenant received %v, want all %v — over-narrowed a permitted tenant", broadNss, wantAll)
	}

	anonNss, servedAnon := servedWidgetNamespaces(t, jwtutil.UserInfo{Username: "stranger-no-bindings"})
	if servedAnon && len(anonNss) != 0 {
		t.Fatalf("#58 SECURITY: an ungranted identity received %v — must be zero", anonNss)
	}
}

// ── ARM 3: GET-by-name no-UAF stays gated (unchanged) ──

// TestFalsifier58_GetByName_StaysGated: a no-UAF GET-by-name content hit still
// runs the per-user gate (gateContentEnvelope → gateGetEnvelope → filterGetByRBAC).
// An ungranted user is fail-closed (served=false → apiserver fallthrough); the
// #58 LIST change does not loosen the GET path.
func TestFalsifier58_GetByName_StaysGated(t *testing.T) {
	_ = newF1Watcher(t)
	store := cache.ResolvedCache()
	// Seed a GET-by-name content cell for a widget in team-c (a namespace the
	// NARROW user is NOT granted).
	raw := []byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"namespace":"team-c","name":"w-team-c"}}`)
	key := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "team-c", "w-team-c"))
	store.Put(key, &cache.ResolvedEntry{RawJSON: raw, Inputs: ptrTo(contentKeyInputs(f1WidgetsGVR, "team-c", "w-team-c"))})

	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1NarrowUser}))
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/widgets.krateo.io/v1/namespaces/team-c/widgets/w-team-c", Verb: ptr.To(http.MethodGet),
	}}
	_, served, ok := apistageContentServe(ctx, store, call, false /*no-UAF*/)
	if !ok {
		t.Fatalf("GET serve: ok=false")
	}
	// The narrow user lacks get on team-c → fail-closed (served=false) so the
	// apiserver's per-user token answers. The #58 LIST change did not loosen this.
	if served {
		t.Fatalf("#58: a no-UAF GET-by-name for an UNGRANTED namespace must stay gated "+
			"(served=false → apiserver fallthrough); the GET path must not be loosened")
	}
}

// ── ARM 4: UAF GET-by-name passes through (GET-extension proof) ──

// TestFalsifier58_UAF_GetByName_PassesThroughForRefilter is the REQUIRED
// GET-extension arm (arch). It is an INVARIANT PROOF, not a corpus repro: all
// 13 enterprise UAF steps are LIST, ZERO are GET-by-name today, so this uses a
// SYNTHETIC divergent UAF GET-by-name. It proves the gateContentEnvelope GET
// extension is correct: a UAF GET-by-name serve passes the object through
// UN-gated (so the downstream UAF refilter narrows by the UAF rule), instead
// of being raw-RBAC-filtered to a fail-closed miss for a requester who lacks
// raw `get` on the GVR but has the UAF-resource access. WITHOUT the GET
// extension, gateGetEnvelope's filterGetByRBAC would deny → served=false →
// the (hypothetical) UAF GET breaks identically to the LIST break.
func TestFalsifier58_UAF_GetByName_PassesThroughForRefilter(t *testing.T) {
	// "devs" gets compositions in bench-ns-01 only; NO `get widgets` anywhere.
	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Name: "ns01-comp-reader", Namespace: "bench-ns-01"},
		Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{"composition.krateo.io"}, Resources: []string{"compositions"}},
		},
	}
	binding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "ns01-comp-reader-binding", Namespace: "bench-ns-01"},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "ns01-comp-reader"},
	}
	newUAFNamespacesWatcher(t, role, binding) // registers namespaces; widgets GET-by-name needs no informer (raw bytes)

	// Seed a GET-by-name content cell for a widget the user has NO raw get on.
	store := cache.ResolvedCache()
	raw := []byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"namespace":"bench-ns-01","name":"w-1"}}`)
	key := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "bench-ns-01", "w-1"))
	store.Put(key, &cache.ResolvedEntry{RawJSON: raw, Inputs: ptrTo(contentKeyInputs(f1WidgetsGVR, "bench-ns-01", "w-1"))})

	ctx := ctxWithUser("cyberjoker", "devs")
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/widgets.krateo.io/v1/namespaces/bench-ns-01/widgets/w-1", Verb: ptr.To(http.MethodGet),
	}}

	// uafActive=true → the GET extension must PASS THROUGH (served=true, object
	// returned), NOT fail-closed via filterGetByRBAC. The refilter narrows
	// downstream by the UAF rule (out of this serve's scope).
	v, served, ok := apistageContentServe(ctx, store, call, true /*uafActive*/)
	if !ok || !served {
		t.Fatalf("#58 UAF-GET extension: a UAF GET-by-name must PASS THROUGH (served=true) for the "+
			"downstream refilter; got ok=%v served=%v. Without the gateContentEnvelope UAF branch, "+
			"filterGetByRBAC denies (the user lacks raw `get widgets`) → fail-closed → UAF-GET broken.", ok, served)
	}
	obj, _ := v.(map[string]any)
	meta, _ := obj["metadata"].(map[string]any)
	if meta["name"] != "w-1" {
		t.Fatalf("UAF GET passthrough must return the object verbatim; got %v", obj)
	}

	// CONTRAST (no-UAF): the SAME GET, uafActive=false, must FAIL CLOSED (the
	// user lacks raw get) — the GET extension only loosens the UAF path.
	_, servedNoUAF, okNoUAF := apistageContentServe(ctx, store, call, false /*no-UAF*/)
	if !okNoUAF {
		t.Fatalf("no-UAF GET serve: ok=false")
	}
	if servedNoUAF {
		t.Fatalf("#58: the SAME GET-by-name under no-UAF must stay fail-closed (filterGetByRBAC denies); "+
			"the GET extension must NOT loosen the no-UAF path")
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
