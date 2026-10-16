// issue547_known_stale_test.go — #547: a cell whose latest dirty mark was not
// closed by an accepted re-Put is KNOWN-STALE, and that state outlives the
// failed refresh.
//
// THE DEFECT (architect-547 trace, #547 comment 6140421686). A portal write
// UPDATEs a composition; the informer dirty-marks every widget cell that read it;
// the refresher re-resolves the cell and FAILS under the background identity
// (a refresher-only 403, a stage error, #608's `resolve widget: unknown`). The
// refresh ends without a Put and dirty_window.go:182 DELETES the stamp: the old
// body stays resident, the cell looks exactly like a fresh one, every reload is
// a hit on the pre-change body, and stale_served_total stops counting at the
// very moment the user starts seeing stale data.
//
// THE SHAPE. Every arm drives the production boundary end to end:
//
//	customer GET-by-name /call (Widgets().ServeHTTP, ?extras={name,namespace})
//	→ cold resolve → per-cohort Put + inner-call dep edge
//	→ a REAL informer UPDATE through the dynamic fake (deps_watch UpdateFunc)
//	→ dep worker → onDirtyMark → refresher processNext → resolveAndPopulateL1
//	→ the next customer GET.
//
// K=2 cohorts (two users on two different bindings, so two per-cohort keys) ×
// M=3 compositions. Composition m=1 is the one that changes.
//
// THE INJECTION is the one seam BOTH callers resolve through (widgetsResolveFn;
// resolveWidgetForRefresh calls it since #547): it denies when
// cache.BackgroundResolveFromContext(ctx) is true and serves the customer. That
// is the measured live asymmetry — the refresher identity fails where the
// customer's own succeeds — without special-casing any resolver.
//
// ARMS
//
//	F1  drop: the background refresh fails across the whole budget and is
//	    dropped. RED on 9c233de9: the next 5 GETs serve the pre-UPDATE body.
//	F2  stage-error decline under the background identity, past K declines.
//	F4  the detector (folded into F1/F2): known_stale_served_total equals the
//	    GETs that received the pre-UPDATE body. RED: the stat does not exist.
//	F5  owner ruling 1 — the demand revalidation ALSO fails: the body keeps
//	    being served (200, never evicted), every serve is counted, and every
//	    read retries; once the cohort can resolve again, the next read heals.
//	F6  coordinator — the revalidation runs on a detached, deadline-bounded ctx
//	    carrying the requester's identity and never blocks the response.
//	F7  owner ruling (#547 comment 6141782784, question a) — a known-stale
//	    SHARED identity-free widget shell is SERVED instantly (counted) and
//	    revalidated in the BACKGROUND by the refresher under its own identity —
//	    never read through, never resolved under the requester, never written
//	    from a per-requester resolve. It heals once the refresher can resolve.
//	F10 reviewer-625 — the RESTAction handler's demand-revalidation trigger:
//	    GET-by-name /call of the composition RESTAction, K=2 × M=3. A known-stale
//	    RA cell is served, then revalidated under the requester; the next GET
//	    carries the new condition.
//	F11 reviewer-625 — the RAFullList read-through: the widget's page comes
//	    from the REAL apiref.Resolve of the composition-list RESTAction. A
//	    known-stale full list is read through, never Go-sliced into a new
//	    widget cell.
//	F12 reviewer-625 N3 — team ruling (b) on the shared shell's background path:
//	    a permanently-suppressed known-stale shell is served but enqueues
//	    nothing; the skip is counted.
//	F13 reviewer-625 addendum — an opt-in external-TTL widget whose resolve also
//	    reads K8s is dirty-marked like any other; its accepted re-Put (the plain
//	    external-TTL Put) must end the known-stale state, or every read starts
//	    another external refetch until the TTL.
//	F9  team ruling (b) — a known-stale cell whose key is suppressed for a
//	    structurally-permanent reason starts NO demand revalidation; the skip
//	    is counted on known_stale_revalidation_skipped_total.
//	F3c architect R1 — the content arm at the production boundary, in the
//	    composition-detail shape: GET /call → the widget's apiRef RA stage
//	    `found` (a cluster LIST of compositions) through the REAL api-stage
//	    resolver and its identity-free content cell. The content refresh is not
//	    servable across the budget; a cohort's next resolve must READ THROUGH
//	    (new condition), never build its widget cell from the stale substrate.
//	    F3b at the same level: that read-through refill ENDS the known-stale
//	    state, so the next cohort's resolve HITs the fresh content cell.
//	F8  architect B1 — an accepted Put of a fresh body by ANY carrier (here a
//	    seed-style PutThenRemark) whose resolve began after the mark ends the
//	    known-stale state: the next Get counts nothing and starts no demand. A
//	    Put whose resolve began BEFORE the mark does not.
//
// The apistage content arm at the reader (F3) lives with the reader:
// internal/resolvers/restactions/api/issue547_content_read_through_test.go.
package dispatchers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var (
	k547CompGVR   = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1-2-2", Resource: "fireworksapps"}
	k547WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "tables"}
	k547RAGVR     = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
)

const (
	k547CompNS  = "demo-system"
	k547WNS     = "krateo-system"
	k547WName   = "list-composition-detail-conditions"
	k547M       = 3
	k547Changed = 1 // the composition the portal write UPDATEs
	k547RAName  = "composition-list"
)

// k547Cohorts are the K=2 cohorts: different users on different bindings, so
// each derives its own per-cohort widgets key.
var k547Cohorts = []struct{ user, group, uid string }{
	{"alice", "devs", "uid-547-a"},
	{"bob", "ops", "uid-547-b"},
}

// k547Mode selects what the BACKGROUND (refresher) identity sees.
type k547Mode int

const (
	k547DenyBackground       k547Mode = iota // a refresher-only 403 → retried → dropped
	k547StageErrorBackground                 // a continueOnError stage error → declined
)

type k547Harness struct {
	t    *testing.T
	dyn  dynamic.Interface
	mode k547Mode

	bgCalls   atomic.Int64 // background (refresher) resolves of the changed composition
	custCalls atomic.Int64 // customer-shaped resolves of the changed composition

	// failCustomer makes the CUSTOMER-shaped resolve of the changed composition
	// fail too (F5: a real failure for the cohort, not an identity mismatch).
	failCustomer atomic.Bool
	// allowBackground lifts the background-identity injection (F7: the
	// refresher's own identity recovers, so its background revalidation heals).
	allowBackground atomic.Bool
	// onDoomed (#740), when set, runs inside a resolve of the changed composition
	// AFTER it has read an injected failure (the background deny, or
	// failCustomer) and BEFORE it returns it: an arm parks the attempt there, so
	// the failure it already read outlives a recovery that lands meanwhile.
	onDoomed atomic.Pointer[func(bg bool)]
	// onHealing (#740 K6), when set, runs inside a resolve of the changed
	// composition that has read NO injected failure (it will succeed), before
	// it reads the composition: an arm parks a healing attempt there.
	onHealing atomic.Pointer[func(bg bool)]

	mu sync.Mutex
	// block, when set, parks a customer-shaped resolve of the changed composition
	// that is NOT a top-level customer GET (F6): entered is closed when it parks,
	// release unparks it. seen records what that resolve's ctx carried.
	block   *k547Block
	widgetU *unstructured.Unstructured

	// contentMode (F3c): the widget's data comes from its apiRef RA stage
	// `found` — a cluster LIST of compositions resolved by the REAL api-stage
	// resolver through its identity-free content cell — instead of a direct read.
	contentMode bool

	// apiRefMode (F11): the widget's data comes from the REAL apiref.Resolve of
	// the composition-list RESTAction at a page (PerPage=k547M, Page=1), so the
	// request is served by the page-independent RAFullList cell (Go-sliced).
	apiRefMode bool

	// external (F13): every resolve also touches a genuine external endpoint
	// (the ExternalTouchedSink is bumped), so an opted-in widget takes the
	// external-TTL Put branch. The resolve still reads Kubernetes and records
	// the inner-call dep edge on the widget L1 key — the traced live shape: an
	// external widget whose RESTAction also reads K8s IS dirty-marked.
	external atomic.Bool
}

type k547Block struct {
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	user        string
	hasDeadline bool
}

func k547Composition(m int, reason string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k547CompGVR.Group + "/" + k547CompGVR.Version,
		"kind":       "FireworksApp",
		"metadata":   map[string]any{"name": fmt.Sprintf("comp-%d", m), "namespace": k547CompNS},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "reason": reason},
		}},
	}}
}

func k547Widget() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k547WidgetGVR.Group + "/" + k547WidgetGVR.Version,
		"kind":       "Table",
		"metadata":   map[string]any{"name": k547WName, "namespace": k547WNS},
		// The Conditions card is keyed by the composition it renders.
		"spec": map[string]any{"keyExtras": []any{"name", "namespace"}},
	}}
}

// newK547Harness wires a cache-on watcher whose informers serve the widget CR
// and M compositions, an RBAC snapshot granting both cohorts, the resolve
// seam, and the real refresher loop behind the production resolveAndPopulateL1.
func newK547Harness(t *testing.T, mode k547Mode) *k547Harness {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i187RefresherEnv(t)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRefresherForTest()

	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	listKinds := map[schema.GroupVersionResource]string{
		k547CompGVR:                    "FireworksAppList",
		k547WidgetGVR:                  "TableList",
		k547RAGVR:                      "RESTActionList",
		rbacGVR("clusterrolebindings"): "ClusterRoleBindingList",
		rbacGVR("clusterroles"):        "ClusterRoleList",
		rbacGVR("rolebindings"):        "RoleBindingList",
		rbacGVR("roles"):               "RoleList",
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "k547-reader"}, Rules: []rbacv1.PolicyRule{{
			Verbs:     []string{"get", "list"},
			APIGroups: []string{k547CompGVR.Group, k547WidgetGVR.Group, k547RAGVR.Group},
			Resources: []string{k547CompGVR.Resource, k547WidgetGVR.Resource, k547RAGVR.Resource},
		}}},
		k547Widget(),
		k547RA(),
	}
	for _, c := range k547Cohorts {
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "k547-" + c.user, UID: types.UID(c.uid)},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: c.user}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "k547-reader"},
		})
	}
	// The refresher's own identity for identity-free cells (the SA named by
	// phase1RealSAToken's `sub`) holds the same read grant.
	seed = append(seed, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "k547-sa", UID: types.UID("uid-547-sa")},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: "krateo-system"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "k547-reader"},
	})
	for m := 0; m < k547M; m++ {
		seed = append(seed, k547Composition(m, "v0"))
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	for _, gvr := range []schema.GroupVersionResource{k547CompGVR, k547WidgetGVR, k547RAGVR} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("informer %s did not sync", gvr)
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})

	h := &k547Harness{t: t, dyn: dyn, mode: mode, widgetU: k547Widget()}
	r1 := setFetchObjectForTest(func(req *http.Request) objects.Result {
		if req.URL.Query().Get("resource") == k547RAGVR.Resource {
			return objects.Result{GVR: k547RAGVR, Unstructured: k547RA()}
		}
		return objects.Result{GVR: k547WidgetGVR, Unstructured: h.widgetU.DeepCopy()}
	})
	r2 := setCheckDispatchRBACForTest(func(context.Context, schema.GroupVersionResource, string) bool { return true })
	r3 := setWidgetsResolveForTest(h.resolve)
	r4 := setRestactionsResolveForTest(h.resolveRESTAction)
	t.Cleanup(func() { r4(); r3(); r2(); r1() })
	// Registered AFTER the seam restore, so it runs BEFORE it (LIFO): a detached
	// demand revalidation re-enters ServeHTTP through these seams and holds its
	// slot until the handler returns, so restoring them under it is a data race
	// on the next arm (reviewer-625 C2: F10 flaked 1/41 under -race). Bounded.
	t.Cleanup(func() {
		for end := time.Now().Add(10 * time.Second); cache.DemandRevalidationsInFlight() > 0; time.Sleep(5 * time.Millisecond) {
			if time.Now().After(end) {
				t.Errorf("cleanup: %d demand revalidation(s) still in flight after 10s", cache.DemandRevalidationsInFlight())
				return
			}
		}
	})

	// The production RefreshFunc body, for the per-cohort class and the
	// identity-free widget content class.
	// The SA transport the production RefreshFunc closes over, against a fake
	// apiserver that answers discovery (the widget CR re-fetch itself is served
	// from the synced informer). Without it every background refresh fails at
	// the re-fetch ("user *Endpoint not found") BEFORE reaching the injected
	// seam, and an identity-free shell could never heal (F7).
	srv := k547APIServer(t)
	saEP := &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}
	saRC := &rest.Config{Host: srv.URL}
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	}
	cache.RegisterRefreshFunc("widgets", refresh)
	cache.RegisterRefreshFunc(cache.CacheEntryClassWidgetContent, refresh)
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest() })
	return h
}

// k547APIServer is a fake apiserver for the refresher's SA transport: discovery
// for the widget group only. Every object read is served from the informers; an
// object path 404s, which would surface as a self-gone eviction in the arms.
func k547APIServer(t *testing.T) *httptest.Server {
	t.Helper()
	gv := k547WidgetGVR.Group + "/" + k547WidgetGVR.Version
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api":
			writeJSON(w, &metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
		case "/api/v1":
			writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1"})
		case "/apis":
			gvd := metav1.GroupVersionForDiscovery{GroupVersion: gv, Version: k547WidgetGVR.Version}
			writeJSON(w, &metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
				Groups: []metav1.APIGroup{{Name: k547WidgetGVR.Group, Versions: []metav1.GroupVersionForDiscovery{gvd}, PreferredVersion: gvd}}})
		case "/apis/" + gv:
			writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv,
				APIResources: []metav1.APIResource{{Name: k547WidgetGVR.Resource, SingularName: "table", Namespaced: true, Kind: "Table",
					Verbs: metav1.Verbs{"get", "list", "watch"}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// k547RASliceJQ lists the compositions cluster-wide, sorted, as ONE top-level
// array (the Ship-4a sliceable shape the RAFullList cell is Go-sliced from).
const k547RASliceJQ = `
{
  comps: (
    ((.comps.items // []) | map({ns: .metadata.namespace, name: .metadata.name, conditions: .status.conditions})) as $items
    | ($items | sort_by(.ns + "/" + .name)) as $sorted
    | (.slice.offset  // 0)                 as $offset
    | (.slice.perPage // ($sorted | length)) as $perPage
    | [ $sorted | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $sorted[.] ]
  )
}
`

// k547RA is the composition-list RESTAction: one api stage LISTing the
// compositions cluster-wide, filtered into the sliceable shape.
func k547RA() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k547RAGVR.Group + "/" + k547RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": k547RAName, "namespace": k547WNS},
		"spec": map[string]any{
			"api": []any{map[string]any{
				"name": "comps", "verb": "GET",
				"path": "/apis/" + k547CompGVR.Group + "/" + k547CompGVR.Version + "/" + k547CompGVR.Resource,
			}},
			"filter": k547RASliceJQ,
		},
	}}
}

// resolveViaApiRef is F11's widget data source: the REAL apiref.Resolve of the
// composition-list RESTAction at a page, which serves the page from (or fills)
// the identity-bound RAFullList cell. Returns composition ns/name's conditions.
func (h *k547Harness) resolveViaApiRef(ctx context.Context, ns, name string) ([]any, bool) {
	rctx := cache.WithServeWatcher(ctx, cache.Global())
	rctx = cache.WithInternalEndpoint(rctx, &endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"})
	out, err := apiref.Resolve(rctx, apiref.ResolveOptions{
		RC: &rest.Config{Host: "http://127.0.0.1:1"},
		ApiRef: templates.ObjectReference{
			Reference:  templates.Reference{Name: k547RAName, Namespace: k547WNS},
			Resource:   k547RAGVR.Resource,
			APIVersion: k547RAGVR.Group + "/" + k547RAGVR.Version,
		},
		PerPage: k547M, Page: 1,
	})
	if err != nil {
		h.t.Logf("apiref.Resolve: %v", err)
		return nil, false
	}
	items, _ := out["comps"].([]any)
	for _, it := range items {
		m, _ := it.(map[string]any)
		if m["name"] == name && m["ns"] == ns {
			conds, _ := m["conditions"].([]any)
			return conds, true
		}
	}
	return nil, false
}

// resolveRESTAction is the restactionsResolveFn seam (F10): it reads the
// composition named by the extras LIVE, records the inner-call dep edge on the
// L1 key being filled, and renders its conditions into the RA status.
func (h *k547Harness) resolveRESTAction(ctx context.Context, opts restactions.ResolveOptions) (*templates.RESTAction, error) {
	name, _ := opts.Extras["name"].(string)
	ns, _ := opts.Extras["namespace"].(string)
	if name == fmt.Sprintf("comp-%d", k547Changed) && !cache.BackgroundResolveFromContext(ctx) {
		h.custCalls.Add(1)
	}
	u, err := h.dyn.Resource(k547CompGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if l1 := cache.L1KeyFromContext(ctx); l1 != "" {
		cache.Deps().Record(ctx, l1, k547CompGVR, ns, name)
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	raw, _ := json.Marshal(map[string]any{"conditions": conds})
	out := opts.In.DeepCopy()
	out.Status = &runtime.RawExtension{Raw: raw}
	return out, nil
}

// getRA issues the production GET-by-name /call for the RESTAction, cohort k,
// composition m, and returns the rendered condition reason and status code.
func (h *k547Harness) getRA(k, m int) (string, int) {
	h.t.Helper()
	ex, _ := json.Marshal(k547Extras(m))
	u := "/call?apiVersion=" + url.QueryEscape(k547RAGVR.Group+"/"+k547RAGVR.Version) +
		"&resource=" + k547RAGVR.Resource + "&name=" + k547RAName + "&namespace=" + k547WNS +
		"&extras=" + url.QueryEscape(string(ex))
	rec := httptest.NewRecorder()
	RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", u, nil).WithContext(k547ReqCtx(k)))
	var obj map[string]any
	if json.Unmarshal(rec.Body.Bytes(), &obj) != nil {
		return "", rec.Code
	}
	conds, _, _ := unstructured.NestedSlice(obj, "status", "conditions")
	if len(conds) == 0 {
		return "", rec.Code
	}
	c, _ := conds[0].(map[string]any)
	r, _ := c["reason"].(string)
	return r, rec.Code
}

// resolve is the shared widgetsResolveFn seam. It reads the composition named
// by the extras LIVE, records the inner-call dep edge on the L1 key being
// filled (resolve.go's innerCall edge, on whatever key the caller threaded),
// and renders its conditions into status.widgetData.
func (h *k547Harness) resolve(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
	name, _ := opts.Extras["name"].(string)
	ns, _ := opts.Extras["namespace"].(string)
	changed := name == fmt.Sprintf("comp-%d", k547Changed)
	bg := cache.BackgroundResolveFromContext(ctx)
	if changed {
		if bg {
			h.bgCalls.Add(1)
		} else {
			h.custCalls.Add(1)
		}
	}
	if bg && !h.allowBackground.Load() {
		if fn := h.onDoomed.Load(); fn != nil && changed {
			(*fn)(true)
		}
		switch h.mode {
		case k547DenyBackground:
			return nil, apierrors.NewForbidden(schema.GroupResource{Group: k547CompGVR.Group, Resource: k547CompGVR.Resource},
				name, errors.New("refresher identity denied (injected at the shared resolve seam)"))
		case k547StageErrorBackground:
			if s := cache.StageErrorSinkFromContext(ctx); s != nil {
				s.Bump("getComposition", "403 under the refresher identity (injected)")
			}
		}
	} else if changed {
		if h.failCustomer.Load() {
			if fn := h.onDoomed.Load(); fn != nil {
				(*fn)(false)
			}
			return nil, apierrors.NewForbidden(schema.GroupResource{Group: k547CompGVR.Group, Resource: k547CompGVR.Resource},
				name, errors.New("cohort genuinely denied (injected, F5)"))
		}
		if fn := h.onHealing.Load(); fn != nil {
			(*fn)(bg)
		}
		h.mu.Lock()
		b := h.block
		h.mu.Unlock()
		if b != nil {
			parked := false
			b.once.Do(func() {
				ui, _ := xcontext.UserInfo(ctx)
				_, b.hasDeadline = ctx.Deadline()
				b.user = ui.Username
				close(b.entered)
				parked = true
			})
			if parked {
				<-b.release
			}
		}
	}
	if h.external.Load() {
		cache.ExternalTouchedSinkFromContext(ctx).Bump()
	}
	var conds []any
	if h.apiRefMode {
		var found bool
		conds, found = h.resolveViaApiRef(ctx, ns, name)
		if !found {
			return nil, fmt.Errorf("composition %s/%s not in the apiRef full list", ns, name)
		}
	} else if h.contentMode {
		var found bool
		conds, found = h.resolveFoundStage(ctx, ns, name)
		if !found {
			return nil, fmt.Errorf("composition %s/%s not in the found stage", ns, name)
		}
	} else {
		u, err := h.dyn.Resource(k547CompGVR).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if l1 := cache.L1KeyFromContext(ctx); l1 != "" {
			cache.Deps().Record(ctx, l1, k547CompGVR, ns, name)
		}
		conds, _, _ = unstructured.NestedSlice(u.Object, "status", "conditions")
	}
	out := opts.In.DeepCopy()
	out.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": conds}}
	return out, nil
}

// resolveFoundStage runs the composition-detail RA's `found` stage — GET the
// cluster LIST of compositions — through the REAL api-stage resolver (content
// cell, per-requester gate, innerCall dep edge on the widget key the caller
// threaded) and returns the named composition's conditions.
func (h *k547Harness) resolveFoundStage(ctx context.Context, ns, name string) ([]any, bool) {
	rw := cache.Global()
	rctx := cache.WithServeWatcher(ctx, rw)
	// The K8s call is served from the synced informer; 127.0.0.1:1 fails fast if
	// anything ever dials it.
	rctx = cache.WithInternalEndpoint(rctx, &endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"})
	dict := restactionsapi.Resolve(rctx, restactionsapi.ResolveOptions{
		RC:      &rest.Config{Host: "http://127.0.0.1:1"},
		Watcher: rw,
		Items: []*templates.API{{
			Name: "found",
			Path: "/apis/" + k547CompGVR.Group + "/" + k547CompGVR.Version + "/" + k547CompGVR.Resource,
			Verb: ptr.To("GET"),
		}},
	})
	var items []any
	switch v := dict["found"].(type) {
	case map[string]any:
		items, _ = v["items"].([]any)
	case []any:
		items = v
	}
	for _, it := range items {
		m, _ := it.(map[string]any)
		md, _ := m["metadata"].(map[string]any)
		if md["name"] == name && md["namespace"] == ns {
			conds, _, _ := unstructured.NestedSlice(m, "status", "conditions")
			return conds, true
		}
	}
	return nil, false
}

func k547ReqCtx(k int) context.Context {
	return xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{
		Username: k547Cohorts[k].user, Groups: []string{k547Cohorts[k].group},
	}))
}

func k547Extras(m int) map[string]any {
	return map[string]any{"name": fmt.Sprintf("comp-%d", m), "namespace": k547CompNS}
}

// get issues the production GET-by-name /call for cohort k, composition m and
// returns the rendered condition reason ("" when absent) and the status code.
func (h *k547Harness) get(k, m int) (string, int) {
	return h.getCtx(k547ReqCtx(k), m)
}

func (h *k547Harness) getCtx(ctx context.Context, m int) (string, int) {
	h.t.Helper()
	ex, _ := json.Marshal(k547Extras(m))
	u := "/call?apiVersion=" + url.QueryEscape(k547WidgetGVR.Group+"/"+k547WidgetGVR.Version) +
		"&resource=" + k547WidgetGVR.Resource + "&name=" + k547WName + "&namespace=" + k547WNS +
		"&extras=" + url.QueryEscape(string(ex))
	rec := httptest.NewRecorder()
	Widgets().ServeHTTP(rec, httptest.NewRequest("GET", u, nil).WithContext(ctx))
	return k547Reason(rec.Body.Bytes()), rec.Code
}

func k547Reason(body []byte) string {
	var obj map[string]any
	if json.Unmarshal(body, &obj) != nil {
		return ""
	}
	conds, _, _ := unstructured.NestedSlice(obj, "status", "widgetData", "conditions")
	if len(conds) == 0 {
		return ""
	}
	c, _ := conds[0].(map[string]any)
	r, _ := c["reason"].(string)
	return r
}

// key is the per-cohort widgets key cohort k derives for composition m.
func (h *k547Harness) key(k, m int) string {
	h.t.Helper()
	ctx := k547ReqCtx(k)
	key, handle, _ := dispatchCacheLookupKey(ctx, "widgets",
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(m)))
	if handle == nil || key == "" {
		h.t.Fatalf("PRECONDITION: cohort %d derives no cacheable widgets key", k)
	}
	return key
}

func (h *k547Harness) residentReason(key string) (string, bool) {
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok {
		return "", false
	}
	return k547Reason(e.RawJSON), true
}

// warm fills all K×M per-cohort cells through cold customer GETs.
func (h *k547Harness) warm() {
	h.t.Helper()
	seen := map[string]bool{}
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				h.t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q, want 200 v0", k, m, code, r)
			}
			key := h.key(k, m)
			if r, ok := h.residentReason(key); !ok || r != "v0" {
				h.t.Fatalf("warm (k=%d,m=%d): cell not resident with v0 (ok=%v reason=%q)", k, m, ok, r)
			}
			seen[key] = true
		}
	}
	if len(seen) != len(k547Cohorts)*k547M {
		h.t.Fatalf("PRECONDITION: %d distinct cells, want K×M=%d", len(seen), len(k547Cohorts)*k547M)
	}
}

// update writes reason onto composition m through the dynamic client and waits
// until the informer UPDATE has reached OnObjectEvent.
func (h *k547Harness) update(m int, reason string) {
	h.t.Helper()
	seq0 := cache.DepEventSeqForTest()
	if _, err := h.dyn.Resource(k547CompGVR).Namespace(k547CompNS).Update(context.Background(),
		k547Composition(m, reason), metav1.UpdateOptions{}); err != nil {
		h.t.Fatalf("update comp-%d: %v", m, err)
	}
	for end := time.Now().Add(5 * time.Second); cache.DepEventSeqForTest() == seq0; time.Sleep(2 * time.Millisecond) {
		if time.Now().After(end) {
			h.t.Fatalf("the comp-%d UPDATE never reached OnObjectEvent", m)
		}
	}
}

func k547Stat(stat string) int64 { return cache.RefresherStatsByStat()[stat] }

func k547WaitFor(t *testing.T, d time.Duration, what string, cond func() bool) bool {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// knownStaleServed reads the #547 detector off the published snowplow_resolved_cache
// stat map. ok=false: the stat does not exist on this build.
func k547KnownStaleServed() (int64, bool) {
	v, ok := cache.ResolvedCacheStatsByStat()["known_stale_served_total"]
	return v, ok
}

func k547KnownStaleServedReason(reason string) int64 {
	return cache.ResolvedCacheStatsByStat()["known_stale_served_"+reason+"_total"]
}

// driveChangedToKnownStale UPDATEs composition m=1 and waits until both cohorts'
// background refreshes have ENDED unfresh (dropped / declined).
func (h *k547Harness) driveChangedToKnownStale(reason string) {
	h.t.Helper()
	switch h.mode {
	case k547DenyBackground:
		d0 := k547Stat("dirty_ended_unfresh_dropped")
		h.update(k547Changed, reason)
		if !k547WaitFor(h.t, 30*time.Second, "both cells dropped", func() bool {
			return k547Stat("dirty_ended_unfresh_dropped")-d0 >= int64(len(k547Cohorts))
		}) {
			h.t.Fatalf("PRECONDITION: the background refreshes never reached the drop point (dropped +%d, bg resolves %d)",
				k547Stat("dirty_ended_unfresh_dropped")-d0, h.bgCalls.Load())
		}
	case k547StageErrorBackground:
		d0 := k547Stat("dirty_ended_unfresh_declined")
		h.update(k547Changed, reason)
		if !k547WaitFor(h.t, 15*time.Second, "both cells declined", func() bool {
			return k547Stat("dirty_ended_unfresh_declined")-d0 >= int64(len(k547Cohorts))
		}) {
			h.t.Fatalf("PRECONDITION: the background refreshes never ended declined (declined +%d)",
				k547Stat("dirty_ended_unfresh_declined")-d0)
		}
	}
	// Quiesce: nothing still queued for the changed cells.
	k547WaitFor(h.t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
}

// assertConvergesOnRead is the GREEN contract shared by F1 and F2, per cohort:
// the FIRST GET serves the pre-UPDATE body (zero cold navigation) and starts a
// demand revalidation; the SECOND GET (and the SSE publish) carry the new
// condition. Returns how many GETs received the pre-UPDATE body.
func (h *k547Harness) assertConvergesOnRead(arm, oldReason, newReason string) int64 {
	h.t.Helper()
	var staleGets int64
	for k := range k547Cohorts {
		key := h.key(k, k547Changed)
		if r, ok := h.residentReason(key); !ok || r != oldReason {
			h.t.Fatalf("%s PRECONDITION (k=%d): after the failed refresh the cell must still be resident on the "+
				"pre-UPDATE body (ok=%v reason=%q, want %q) — never evicted", arm, k, ok, r, oldReason)
		}
		sub, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
		ss0 := cache.ResolvedCacheStatsByStat()["stale_served_total"]

		// GET #1 — served from the cell, never a cold navigation.
		r, code := h.get(k, k547Changed)
		if code != 200 || r != oldReason {
			unsub()
			h.t.Fatalf("%s (k=%d) GET#1: code=%d reason=%q, want 200 serving the resident body %q", arm, k, code, r, oldReason)
		}
		staleGets++

		// The demand revalidation is asynchronous: wait for the cell to turn
		// fresh, bounded. On a build without it nothing happens and GET#2..#5
		// below carry the RED.
		healed := k547WaitFor(h.t, 5*time.Second, "cell refreshed by the demand revalidation", func() bool {
			rr, _ := h.residentReason(key)
			return rr == newReason
		})
		var published bool
		if healed {
			select {
			case got := <-sub:
				published = got == key
			case <-time.After(5 * time.Second):
			}
		}
		unsub()

		for i := 2; i <= 5; i++ {
			r, code := h.get(k, k547Changed)
			if code != 200 {
				h.t.Fatalf("%s (k=%d) GET#%d: code %d", arm, k, i, code)
			}
			if r != newReason {
				staleGets++
				h.t.Fatalf("%s RED (k=%d): GET#%d still served the pre-UPDATE condition %q (want %q). The background "+
					"refresh ended without a Put, the dirty stamp was erased, and nothing re-validates the cell: "+
					"every reload is a hit on a body the system KNEW was stale (#547). stale_served_total moved %d "+
					"across these GETs; customer-identity resolves of the changed composition: %d.",
					arm, k, i, r, newReason, cache.ResolvedCacheStatsByStat()["stale_served_total"]-ss0, h.custCalls.Load())
			}
		}
		if !published {
			h.t.Fatalf("%s (k=%d): the accepted revalidation published nothing over SSE — a mounted page would "+
				"not update without a reload", arm, k)
		}
	}
	// The unchanged compositions are untouched for both cohorts.
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if m == k547Changed {
				continue
			}
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				h.t.Fatalf("%s: untouched cell (k=%d,m=%d) changed: code=%d reason=%q", arm, k, m, code, r)
			}
		}
	}
	return staleGets
}

func TestIssue547_F1_DropLeavesCellKnownStaleAndTheNextReadHeals(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	// The drop must come from the INJECTED refresher-only denial at the shared
	// resolve seam, not from an earlier failure (the pre-7b20dabc harness failed
	// at the CR re-fetch and never reached it).
	if h.bgCalls.Load() == 0 {
		t.Fatalf("PRECONDITION: the background refresh never reached the injected seam (bgCalls=0)")
	}
	ks0, _ := k547KnownStaleServed()
	dropped0 := k547KnownStaleServedReason("dropped")

	staleGets := h.assertConvergesOnRead("F1", "v0", "v1")

	// F4 — the detector equals the GETs that received the pre-UPDATE body.
	ks, ok := k547KnownStaleServed()
	if !ok {
		t.Fatalf("F4 RED: snowplow_resolved_cache{stat=known_stale_served_total} does not exist")
	}
	if got := ks - ks0; got != staleGets {
		t.Fatalf("F4: known_stale_served_total moved %d, want %d (the GETs that received the pre-UPDATE body)", got, staleGets)
	}
	if got := k547KnownStaleServedReason("dropped") - dropped0; got != staleGets {
		t.Fatalf("F4: known_stale_served_dropped_total moved %d, want %d", got, staleGets)
	}
}

func TestIssue547_F2_StageErrorDeclinesPastKLeaveCellKnownStaleAndTheNextReadHeals(t *testing.T) {
	h := newK547Harness(t, k547StageErrorBackground)
	h.warm()
	// Past K: four background declines in a row (on 9c233de9 K=3, so the 4th
	// dequeue is the suppressed skip).
	for i := 1; i <= 4; i++ {
		h.driveChangedToKnownStale(fmt.Sprintf("v%d", i))
	}
	ks0, _ := k547KnownStaleServed()

	staleGets := h.assertConvergesOnRead("F2", "v0", "v4")

	ks, ok := k547KnownStaleServed()
	if !ok {
		t.Fatalf("F4 RED: snowplow_resolved_cache{stat=known_stale_served_total} does not exist")
	}
	if got := ks - ks0; got != staleGets {
		t.Fatalf("F4: known_stale_served_total moved %d, want %d", got, staleGets)
	}
}

// F5 — owner ruling 1: when the demand revalidation ALSO fails under the
// requester, keep serving the cached body (counted), retry on the next read,
// never evict.
func TestIssue547_F5_RevalidationFailureKeepsServingCountedAndRetriesOnRead(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	h.failCustomer.Store(true)
	key := h.key(0, k547Changed)
	ks0, _ := k547KnownStaleServed()
	c0 := h.custCalls.Load()
	f0 := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_failed_total"]

	const reads = 3
	for i := 1; i <= reads; i++ {
		r, code := h.get(0, k547Changed)
		if code != 200 || r != "v0" {
			t.Fatalf("F5 GET#%d: code=%d reason=%q — a failed revalidation must keep SERVING the cached body (200 v0), "+
				"never a cold navigation and never an error", i, code, r)
		}
		// Each read retries once (sequentially: wait for this read's attempt).
		if !k547WaitFor(t, 5*time.Second, "revalidation attempt", func() bool { return h.custCalls.Load()-c0 >= int64(i) }) {
			t.Fatalf("F5 RED: read #%d started no revalidation under the requester (attempts=%d) — "+
				"nothing retries a known-stale cell on read", i, h.custCalls.Load()-c0)
		}
		// ...and wait for it to SETTLE (released, counted failed) before the next read.
		k547WaitFor(t, 2*time.Second, "attempt settled", func() bool {
			return cache.ResolvedCacheStatsByStat()["known_stale_revalidation_failed_total"]-f0 >= int64(i)
		})
		if r, ok := h.residentReason(key); !ok || r != "v0" {
			t.Fatalf("F5: after failed revalidation #%d the cell must stay resident on v0 (ok=%v reason=%q) — never evict", i, ok, r)
		}
	}
	ks, ok := k547KnownStaleServed()
	if !ok || ks-ks0 != reads {
		t.Fatalf("F5: known_stale_served_total moved %d (exists=%v), want %d — every stale serve must be counted", ks-ks0, ok, reads)
	}

	// The cohort recovers: the next read revalidates and heals.
	h.failCustomer.Store(false)
	if r, _ := h.get(0, k547Changed); r != "v0" {
		t.Fatalf("F5: the healing read itself must still serve the cached body, got %q", r)
	}
	if !k547WaitFor(t, 5*time.Second, "healed", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("F5: the cell never healed once the cohort could resolve again")
	}
	if r, _ := h.get(0, k547Changed); r != "v1" {
		t.Fatalf("F5: after healing GET served %q, want v1", r)
	}
}

// F6 — coordinator: the revalidation runs on a DETACHED ctx carrying the
// requester's identity with a bounded deadline, and never blocks the response.
func TestIssue547_F6_RevalidationIsDetachedBoundedAndNeverBlocksTheResponse(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	b := &k547Block{entered: make(chan struct{}), release: make(chan struct{})}
	h.mu.Lock()
	h.block = b
	h.mu.Unlock()
	defer func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
	}()

	reqCtx, cancelReq := context.WithCancel(k547ReqCtx(1))
	done := make(chan string, 1)
	go func() { r, _ := h.getCtx(reqCtx, k547Changed); done <- r }()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("F6 RED: no revalidation under the requester started after a known-stale read")
	}
	select {
	case r := <-done:
		if r != "v0" {
			t.Fatalf("F6: the response must carry the cached body v0, got %q", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("F6: the customer response did not return while the revalidation was still running — it BLOCKED the customer")
	}
	// The request is over: its ctx is cancelled. A detached revalidation survives it.
	cancelReq()
	if b.user != k547Cohorts[1].user {
		t.Fatalf("F6: the revalidation ran as %q, want the requester %q", b.user, k547Cohorts[1].user)
	}
	if !b.hasDeadline {
		t.Fatalf("F6: the revalidation ctx carries no deadline — it must be bounded")
	}
	close(b.release)
	key := h.key(1, k547Changed)
	if !k547WaitFor(t, 5*time.Second, "healed after release", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("F6: the detached revalidation did not complete after the request ctx was cancelled")
	}
}

// F7 — owner ruling (question a): a known-stale SHARED identity-free widget
// shell is served instantly and revalidated in the background, never read
// through and never resolved under the requester.
func TestIssue547_F7_KnownStaleSharedShellIsServedAndRevalidatedInTheBackground(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatalf("PRECONDITION: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
	raw, _ := json.Marshal(shell.Object)
	handle.Put(contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
	cache.Deps().Record(context.Background(), contentKey, k547CompGVR, k547CompNS, fmt.Sprintf("comp-%d", k547Changed))
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("PRECONDITION: the shared shell must serve before the UPDATE (code=%d reason=%q)", code, r)
	}

	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "shell dropped", func() bool { return k547Stat("dirty_ended_unfresh_dropped") > d0 }) {
		t.Fatalf("PRECONDITION: the shell's background refresh never reached the drop point")
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("PRECONDITION: the shell is not known-stale after the drop")
	}

	// Each cohort is SERVED the shell instantly — no read-through, no resolve
	// under the requester — and the serve is counted.
	ks0, _ := k547KnownStaleServed()
	c0 := h.custCalls.Load()
	d1 := k547Stat("dirty_ended_unfresh_dropped")
	for k := range k547Cohorts {
		r, code := h.get(k, k547Changed)
		if code != 200 || r != "v0" {
			t.Fatalf("F7 RED (k=%d): GET served %q (code %d), want the shared shell v0 served instantly — a known-stale "+
				"shared shell must not be read through (owner ruling, question a)", k, r, code)
		}
	}
	if got := h.custCalls.Load() - c0; got != 0 {
		t.Fatalf("F7 RED: %d resolve(s) ran under the requester's identity — the shared shell must be revalidated "+
			"by the refresher, never read through or resolved per requester", got)
	}
	if ks, _ := k547KnownStaleServed(); ks-ks0 != int64(len(k547Cohorts)) {
		t.Fatalf("F7: known_stale_served_total moved %d, want %d (every serve of the known-stale shell)", ks-ks0, len(k547Cohorts))
	}
	for k := range k547Cohorts {
		if _, ok := cache.ResolvedCache().GetNoTouch(h.key(k, k547Changed)); ok {
			t.Fatalf("F7 (k=%d): a per-cohort cell was filled — the request fell through instead of serving the shell", k)
		}
	}
	// ...and the serve handed the shell to the refresher: a background
	// re-resolve under the refresher's identity, which still fails here and so
	// reaches the drop point again (the shell stays served, known-stale).
	if !k547WaitFor(t, 30*time.Second, "background revalidation dropped", func() bool {
		return k547Stat("dirty_ended_unfresh_dropped") > d1
	}) {
		t.Fatalf("F7 RED: serving the known-stale shell started no background revalidation (dropped stays %d)", d1)
	}
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("F7: a FAILED background revalidation ended the known-stale state")
	}
	// The refresher's identity recovers: the next serve's background
	// revalidation heals the shell, still identity-free. Every serve above may
	// have started its own revalidation (the slot is released per dequeue), and
	// one still running read the deny before the flip: wait until every
	// revalidation started before the flip has settled (#734), so the serve
	// below claims a slot of its own and its resolve runs after the flip.
	h.allowBackground.Store(true)
	if !k547WaitFor(t, 30*time.Second, "background revalidations settled", func() bool {
		return cache.BackgroundRevalidationsInFlight() == 0
	}) {
		t.Fatalf("F7: a background revalidation started before the refresher identity recovered never settled")
	}
	if r, _ := h.get(1, k547Changed); r != "v0" && r != "v1" {
		t.Fatalf("F7: GET served %q", r)
	}
	if !k547WaitFor(t, 10*time.Second, "shell healed", func() bool {
		e, ok := handle.GetNoTouch(contentKey)
		return ok && k547Reason(e.RawJSON) == "v1"
	}) {
		t.Fatalf("F7: the shared shell never healed once the refresher identity could resolve")
	}
	if _, _, ks := cache.KnownStale(contentKey); ks {
		t.Fatalf("F7: the healed shell is still known-stale")
	}
	e, _ := handle.GetNoTouch(contentKey)
	if e.Inputs == nil || e.Inputs.RepresentativeUsername != "" || len(e.Inputs.RepresentativeGroups) != 0 ||
		e.Inputs.CacheEntryClass != cache.CacheEntryClassWidgetContent {
		t.Fatalf("F7: the shared shell's inputs changed identity: %+v", e.Inputs)
	}
	if got := h.custCalls.Load() - c0; got != 0 {
		t.Fatalf("F7: %d resolve(s) ran under a requester's identity", got)
	}
	if r, code := h.get(0, k547Changed); code != 200 || r != "v1" {
		t.Fatalf("F7: after healing GET served %q (code %d), want v1", r, code)
	}
}

// F9 — team ruling (b): a structurally-permanent suppression starts no demand
// revalidation; the skip is counted.
func TestIssue547_F9_StructurallyPermanentSuppressionSkipsDemandRevalidation(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	if !cache.NoteRefreshDecline(key, "uaf", true) {
		t.Fatalf("PRECONDITION: the key did not become suppressed")
	}
	sk0 := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_skipped_total"]
	c0 := h.custCalls.Load()
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("F9: GET served %q (code %d), want the cached v0", r, code)
	}
	time.Sleep(200 * time.Millisecond) // a demand, if any, would have started by now
	if got := h.custCalls.Load() - c0; got != 0 {
		t.Fatalf("F9 RED: a suppressed (uaf) key started %d demand revalidation(s) — its cold path declines the "+
			"Put by the same gate, so it can never succeed", got)
	}
	if d := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_skipped_total"] - sk0; d != 1 {
		t.Fatalf("F9 RED: known_stale_revalidation_skipped_total moved %d, want 1", d)
	}
	// An unsuppressed cohort still revalidates on demand.
	if r, _ := h.get(1, k547Changed); r != "v0" {
		t.Fatalf("F9: cohort 1 GET#1 served %q, want v0", r)
	}
	if !k547WaitFor(t, 5*time.Second, "cohort 1 healed", func() bool { r, _ := h.residentReason(h.key(1, k547Changed)); return r == "v1" }) {
		t.Fatalf("F9: the unsuppressed cohort did not revalidate on demand")
	}
}

// F3c + F3b (architect R1) — the content arm at the production boundary.
func TestIssue547_F3c_KnownStaleFoundListIsReadThroughAtTheCallBoundary(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.contentMode = true
	// The identity-free content cell's refresh is NOT SERVABLE across the whole
	// budget — the typed error the production content refresh returns while the
	// informer is pre-sync / metadata-only (the "flip to not-servable").
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(context.Context, string, cache.ResolvedKeyInputs) error {
		return cache.ErrContentNotServable
	})

	// Warm K=2 cohorts on compositions m=0 and m=2: their resolves fill the shared
	// `found` content cell. The m=1 cells stay cold, so the next read of m=1 is
	// a resolve that READS the content cell — where a stale substrate would be
	// baked into a new widget cell.
	for k := range k547Cohorts {
		for _, m := range []int{0, 2} {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q", k, m, code, r)
			}
		}
	}
	var contentKey string
	n := 0
	cache.ResolvedCache().RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassApistage {
			contentKey = m.KeyHash
			n++
		}
		return true
	})
	if n != 1 {
		t.Fatalf("PRECONDITION: %d apistage content cells after warm, want exactly the one `found` cluster LIST", n)
	}

	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "content cell dropped not-servable", func() bool {
		return k547Stat("dirty_ended_unfresh_dropped") > 0 && cache.ResolvedCacheStatsByStat()["known_stale_resident"] > 0
	}) {
		t.Fatalf("PRECONDITION: the content refresh never reached the drop point")
	}
	k547WaitFor(t, 30*time.Second, "content cell known-stale", func() bool {
		_, r, ok := cache.KnownStale(contentKey)
		return ok && r == "not_servable"
	})

	rt0 := cache.ResolvedCacheStatsByStat()["known_stale_read_through_total"]
	r, code := h.get(0, k547Changed)
	if code != 200 || r != "v1" {
		t.Fatalf("F3c RED: cohort 0's resolve of comp-1 served %q (code %d), want v1 READ THROUGH — the widget "+
			"cell was built from the stale `found` content cell (#547)", r, code)
	}
	if rr, ok := h.residentReason(h.key(0, k547Changed)); !ok || rr != "v1" {
		t.Fatalf("F3c: cohort 0's widget cell holds %q (ok=%v) — a widget cell was Put from the stale substrate", rr, ok)
	}
	rt1 := cache.ResolvedCacheStatsByStat()["known_stale_read_through_total"]
	if rt1 <= rt0 {
		t.Fatalf("F3c: the read did not go through the known-stale content cell (read_through %d -> %d)", rt0, rt1)
	}

	// F3b — the read-through refill began after the mark: the content cell is
	// fresh, so the NEXT cohort's resolve HITs it (no further read-through).
	if _, _, ks := cache.KnownStale(contentKey); ks {
		t.Fatalf("F3b RED: the `found` content cell is still known-stale after a post-mark read-through refill — " +
			"every later read would read through (B1)")
	}
	if r, code := h.get(1, k547Changed); code != 200 || r != "v1" {
		t.Fatalf("F3b: cohort 1 served %q (code %d), want v1", r, code)
	}
	if rt2 := cache.ResolvedCacheStatsByStat()["known_stale_read_through_total"]; rt2 != rt1 {
		t.Fatalf("F3b: cohort 1's resolve read through again (read_through %d -> %d) — the fresh cell must HIT", rt1, rt2)
	}
}

// F8 (architect B1) — an accepted Put of a fresh body whose resolve began after
// the mark ends the known-stale state, whatever carrier wrote it.
func TestIssue547_F8_AFreshAcceptedPutByAnyCarrierEndsKnownStale(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	epochBefore := cache.DepGenEpochNow()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	store := cache.ResolvedCache()
	e, ok := store.GetNoTouch(key)
	if !ok {
		t.Fatalf("PRECONDITION: cell not resident")
	}
	if _, _, ks := cache.KnownStale(key); !ks {
		t.Fatalf("PRECONDITION: cell not known-stale")
	}

	// A Put whose resolve began BEFORE the mark proves nothing: still known-stale.
	pre := cache.WithL1KeyContextFromEpoch(context.Background(), key, epochBefore)
	store.PutThenRemark(pre, key, &cache.ResolvedEntry{RawJSON: e.RawJSON, Inputs: e.Inputs})
	if _, _, ks := cache.KnownStale(key); !ks {
		t.Fatalf("F8: a Put whose resolve began BEFORE the mark ended the known-stale state — it carries the pre-change body")
	}

	// A seed-style Put of a fresh body whose resolve began after the mark.
	var obj map[string]any
	if err := json.Unmarshal(e.RawJSON, &obj); err != nil {
		t.Fatal(err)
	}
	obj["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v1"}}}}
	fresh, _ := json.Marshal(obj)
	post := cache.WithL1KeyContext(context.Background(), key)
	store.PutThenRemark(post, key, &cache.ResolvedEntry{RawJSON: fresh, Inputs: e.Inputs})

	ks0, _ := k547KnownStaleServed()
	c0 := h.custCalls.Load()
	if r, code := h.get(0, k547Changed); code != 200 || r != "v1" {
		t.Fatalf("F8: GET served %q (code %d), want the fresh v1", r, code)
	}
	time.Sleep(100 * time.Millisecond) // a demand, if any, would have started by now
	if ks, _ := k547KnownStaleServed(); ks != ks0 {
		t.Fatalf("F8 RED: known_stale_served_total moved %d -> %d on a FRESH body — the detector counts a false "+
			"positive because no carrier but the refresher/demand ends the state (B1)", ks0, ks)
	}
	if got := h.custCalls.Load() - c0; got != 0 {
		t.Fatalf("F8 RED: the GET of a fresh cell started %d redundant demand revalidation(s)", got)
	}
}

// k547RefresherDenied is the refresher-only failure injected at the RefreshFunc
// seam for a class whose top-level resolve the harness does not fake (F10, F11).
func k547RefresherDenied(context.Context, string, cache.ResolvedKeyInputs) error {
	return apierrors.NewForbidden(schema.GroupResource{Group: k547CompGVR.Group, Resource: k547CompGVR.Resource},
		"", errors.New("refresher identity denied (injected at the RefreshFunc seam)"))
}

// F10 (reviewer-625) — the RESTAction class's demand-revalidation trigger at the
// production boundary: GET-by-name /call of the composition RESTAction, K=2 × M=3.
// The background refresh of the changed composition's RA cells fails across the
// budget; the next GET serves the cached body and revalidates it on demand
// under the requester, so the GET after it carries the new condition.
func TestIssue547_F10_KnownStaleRESTActionIsRevalidatedOnDemandAtTheCallBoundary(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	cache.RegisterRefreshFunc("restactions", k547RefresherDenied)
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if r, code := h.getRA(k, m); code != 200 || r != "v0" {
				t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q, want 200 v0", k, m, code, r)
			}
		}
	}
	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "RA cells dropped", func() bool {
		return k547Stat("dirty_ended_unfresh_dropped")-d0 >= int64(len(k547Cohorts))
	}) {
		t.Fatalf("PRECONDITION: the RA cells' background refreshes never reached the drop point (dropped +%d)",
			k547Stat("dirty_ended_unfresh_dropped")-d0)
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })

	ks0, _ := k547KnownStaleServed()
	f0 := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_fresh_total"]
	for k := range k547Cohorts {
		if r, code := h.getRA(k, k547Changed); code != 200 || r != "v0" {
			t.Fatalf("F10 (k=%d) GET#1: code=%d reason=%q, want 200 serving the cached RA body v0 — never a cold "+
				"navigation", k, code, r)
		}
		if !k547WaitFor(t, 5*time.Second, "RA demand revalidation", func() bool {
			return cache.ResolvedCacheStatsByStat()["known_stale_revalidation_fresh_total"]-f0 >= int64(k+1)
		}) {
			t.Fatalf("F10 RED (k=%d): serving a KNOWN-STALE RESTAction cell started no demand revalidation under the "+
				"requester (revalidation_fresh +%d) — the restactions handler's trigger is missing (#547)",
				k, cache.ResolvedCacheStatsByStat()["known_stale_revalidation_fresh_total"]-f0)
		}
		if r, code := h.getRA(k, k547Changed); code != 200 || r != "v1" {
			t.Fatalf("F10 RED (k=%d) GET#2: code=%d reason=%q, want v1 after the demand revalidation", k, code, r)
		}
	}
	if ks, _ := k547KnownStaleServed(); ks-ks0 != int64(len(k547Cohorts)) {
		t.Fatalf("F10: known_stale_served_total moved %d, want %d (one stale serve per cohort)", ks-ks0, len(k547Cohorts))
	}
	for k := range k547Cohorts {
		for m := 0; m < k547M; m++ {
			if m == k547Changed {
				continue
			}
			if r, code := h.getRA(k, m); code != 200 || r != "v0" {
				t.Fatalf("F10: untouched RA cell (k=%d,m=%d) changed: code=%d reason=%q", k, m, code, r)
			}
		}
	}
}

// F11 (reviewer-625) — the RAFullList read-through at the production boundary:
// GET-by-name /call of the Conditions widget whose data is a PAGE of the
// composition-list RESTAction, served by the REAL apiref.Resolve from the
// identity-bound RAFullList cell. K=2 cohorts warm M=0 and M=2, which fills each
// cohort's full-list cell. The cell's background refresh fails across the
// budget, so it is KNOWN-STALE. Each cohort's first read of M=1 (a cold widget
// cell) must READ THROUGH the stale full list, never Go-slice it into a new,
// fresh-looking widget cell.
func TestIssue547_F11_KnownStaleRAFullListIsReadThroughAtTheCallBoundary(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.apiRefMode = true
	cache.RegisterRefreshFunc(cache.CacheEntryClassRAFullList, k547RefresherDenied)
	for k := range k547Cohorts {
		for _, m := range []int{0, 2} {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q", k, m, code, r)
			}
		}
	}
	var raKeys []string
	cache.ResolvedCache().RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassRAFullList {
			raKeys = append(raKeys, m.KeyHash)
		}
		return true
	})
	if len(raKeys) != len(k547Cohorts) {
		t.Fatalf("PRECONDITION: %d RAFullList cells after warm, want one per cohort (%d)", len(raKeys), len(k547Cohorts))
	}

	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "full-list cells known-stale", func() bool {
		for _, k := range raKeys {
			if _, _, ks := cache.KnownStale(k); !ks {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("PRECONDITION: the RAFullList cells never became known-stale")
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })

	for k := range k547Cohorts {
		rt0 := cache.ResolvedCacheStatsByStat()["known_stale_read_through_total"]
		if r, code := h.get(k, k547Changed); code != 200 || r != "v1" {
			t.Fatalf("F11 RED (k=%d): GET served %q (code %d), want v1 READ THROUGH — the widget page was Go-sliced "+
				"from a KNOWN-STALE RAFullList cell (#547)", k, r, code)
		}
		if rr, ok := h.residentReason(h.key(k, k547Changed)); !ok || rr != "v1" {
			t.Fatalf("F11 (k=%d): the widget cell holds %q (ok=%v) — a widget cell was Put from the stale full list", k, rr, ok)
		}
		if rt := cache.ResolvedCacheStatsByStat()["known_stale_read_through_total"]; rt <= rt0 {
			t.Fatalf("F11 RED (k=%d): the read did not go through the known-stale full list (read_through %d -> %d)", k, rt0, rt)
		}
	}
}

// F12 (reviewer-625 N3) — team ruling (b) on the SHARED SHELL's background path:
// a known-stale shell whose key is suppressed for a structurally-permanent
// reason is still served instantly (counted), but its serve hands NOTHING to the
// refresher; the skip is counted.
func TestIssue547_F12_PermanentSuppressionSkipsTheShellBackgroundRevalidation(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatalf("PRECONDITION: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
	raw, _ := json.Marshal(shell.Object)
	handle.Put(contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
	cache.Deps().Record(context.Background(), contentKey, k547CompGVR, k547CompNS, fmt.Sprintf("comp-%d", k547Changed))

	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "shell dropped", func() bool { return k547Stat("dirty_ended_unfresh_dropped") > d0 }) {
		t.Fatalf("PRECONDITION: the shell's background refresh never reached the drop point")
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("PRECONDITION: the shell is not known-stale after the drop")
	}
	if !cache.NoteRefreshDecline(contentKey, "external", true) {
		t.Fatalf("PRECONDITION: the shell key did not become suppressed")
	}

	sk0 := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_skipped_total"]
	enq0 := k547Stat("enqueue")
	ks0, _ := k547KnownStaleServed()
	for k := range k547Cohorts {
		if r, code := h.get(k, k547Changed); code != 200 || r != "v0" {
			t.Fatalf("F12 (k=%d): GET served %q (code %d), want the suppressed shell served instantly (v0)", k, r, code)
		}
	}
	time.Sleep(100 * time.Millisecond) // an enqueue, if any, would have landed by now
	if d := k547Stat("enqueue") - enq0; d != 0 {
		t.Fatalf("F12 RED: serving a permanently-suppressed known-stale shell enqueued %d background revalidation(s) — "+
			"it can never succeed (team ruling b)", d)
	}
	if d := cache.ResolvedCacheStatsByStat()["known_stale_revalidation_skipped_total"] - sk0; d != int64(len(k547Cohorts)) {
		t.Fatalf("F12 RED: known_stale_revalidation_skipped_total moved %d, want %d (one per serve)", d, len(k547Cohorts))
	}
	if ks, _ := k547KnownStaleServed(); ks-ks0 != int64(len(k547Cohorts)) {
		t.Fatalf("F12: known_stale_served_total moved %d, want %d", ks-ks0, len(k547Cohorts))
	}
}

// F13 (reviewer-625 addendum) — the external-TTL widget Put closes KNOWN-STALE.
func TestIssue547_F13_ExternalTTLWidgetRePutEndsKnownStale(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.external.Store(true)
	h.widgetU.SetAnnotations(map[string]string{externalCacheTTLAnnotation: "300"})
	key := h.key(0, k547Changed)
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("warm: code=%d reason=%q", code, r)
	}
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok || !e.ExternalTTL {
		t.Fatalf("PRECONDITION: the warm GET must have taken the external-TTL Put (resident=%v)", ok)
	}

	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "external cell dropped", func() bool {
		return k547Stat("dirty_ended_unfresh_dropped") > d0
	}) {
		t.Fatalf("PRECONDITION: the external widget cell was never dirty-marked and dropped — the K8s dep edge the " +
			"resolve recorded on its L1 key did not mark it")
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
	if _, _, ks := cache.KnownStale(key); !ks {
		t.Fatalf("PRECONDITION: the external widget cell is not known-stale after the drop")
	}

	// GET#1 serves v0 and revalidates on demand; that re-run takes the external
	// branch again and re-Puts v1 through the plain external-TTL Put.
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("F13 GET#1: code=%d reason=%q, want the cached v0", code, r)
	}
	if !k547WaitFor(t, 5*time.Second, "external re-Put", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("PRECONDITION: the demand revalidation never re-Put the external cell")
	}
	k547WaitFor(t, 5*time.Second, "demand settled", func() bool { return cache.DemandRevalidationsInFlight() == 0 })
	if _, _, ks := cache.KnownStale(key); ks {
		t.Fatalf("F13 RED: the external-TTL re-Put of a fresh body left the cell KNOWN-STALE — the ctx-less plain Put " +
			"never closes the stamp, so every read starts another external refetch until the TTL (#547 B1)")
	}
	ks0, _ := k547KnownStaleServed()
	c1 := h.custCalls.Load()
	if r, code := h.get(0, k547Changed); code != 200 || r != "v1" {
		t.Fatalf("F13 GET#2: code=%d reason=%q, want v1", code, r)
	}
	time.Sleep(100 * time.Millisecond)
	if d := h.custCalls.Load() - c1; d != 0 {
		t.Fatalf("F13 RED: GET#2 of the fresh external cell started %d more external refetch(es)", d)
	}
	if ks, _ := k547KnownStaleServed(); ks != ks0 {
		t.Fatalf("F13 RED: GET#2 counted a known-stale serve on a fresh body")
	}

}
