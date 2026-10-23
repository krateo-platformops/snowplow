package dispatchers

// issue643_harness_test.go — the shared #643 harness: background prewarm
// passes driven through the REAL engine against an external endpoint that
// counts every dial.
//
// REAL, end to end:
//   - the engine: a prewarmEngine with the production client-go rate-limiting
//     queue, driven item by item through the real processScope (Get → yield →
//     per-scope timeout → handler → Done → Forget / AddRateLimited requeue);
//   - the handler: makeBootScopeHandler → rePrewarmBoot / rePrewarmGVRDiscovered /
//     rePrewarmKeepwarm → rePrewarmBootScoped → seedScopeYielding → seedOneWidget
//     with its key derivation, skip decision, admission gate, decline gate and
//     terminal Put into the real L1 store;
//   - the cohorts: the RBAC informer over a fake dynamic client → snapshot →
//     BindingsByGVR index → the production target enumeration (K Group cohorts);
//   - the RESTActions: informer-served through objects.Get (the RA GVR is made
//     servable), resolved by the real apiref.Resolve → restactions.Resolve;
//   - the external endpoint: an httptest.Server set as the seed's SA endpoint,
//     so a stage with no endpointRef and a non-apiserver path falls through to
//     the real httpFetchAllowingNonJSON (the ExternalTouchedSink bump site).
//
// ONE SEAM: widgetsResolveFn (dispatch_seams.go, a production seam). widgets.Resolve
// ends with a CRD-status schema validation that needs apiserver discovery, which
// a hermetic test cannot provide. The harness's widgetsResolveFn calls the REAL
// apiref.Resolve for the widget's spec.apiRef — the frame that dials — and sets
// status.widgetData from it, so every external fetch the seed makes still goes
// through the production resolver.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakewatch"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
)

const (
	h643NS      = "h643-ns"
	h643AuthnNS = "h643-authn"
	// h643ExtSecret is the external endpoint's Secret (server-url = the
	// httptest endpoint), in the authn namespace the Secrets informer serves.
	h643ExtSecret = "h643-ext-endpoint"
)

var (
	h643RAGVR     = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	h643WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
)

// h643Endpoint is the external endpoint. It counts dials per path and can be
// told to stall a path prefix (a slow external query) past the caller's bound.
type h643Endpoint struct {
	srv   *httptest.Server
	mu    sync.Mutex
	dials map[string]int
	stall time.Duration // applied to every path containing "/slow/"
	// discovery (#643 PR-B2): also answer as the apiserver for the widget
	// GVR's discovery and its CRD, so the REAL widgets.Resolve the discovery
	// walk calls can run its CRD-status validation hermetically. Off for the
	// PR-A..C arms, which never walk.
	discovery atomic.Bool
	// stringRows (#643 PR-B2 ruling (c) arms): answer /ext/ rows with a STRING
	// value instead of the default number — the external DATA that decides
	// whether a widget's widgetDataTemplate output validates (the 057
	// float-into-string shape), so a walk resolve errors or succeeds with no
	// change to the widget.
	stringRows atomic.Bool
}

// total counts the EXTERNAL dials (paths under /ext/). The same server also
// stands in as the SA rest.Config host, so the seed's discovery probes (/api,
// /apis) land here too; they are not external fetches and are not counted.
func (h *h643Endpoint) total() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for p, v := range h.dials {
		if strings.HasPrefix(p, "/ext/") {
			n += v
		}
	}
	return n
}

func (h *h643Endpoint) byPath() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.dials))
	for k, v := range h.dials {
		out[k] = v
	}
	return out
}

func newH643Endpoint(t *testing.T) *h643Endpoint {
	t.Helper()
	h := &h643Endpoint{dials: map[string]int{}}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		h.dials[r.URL.Path]++
		stall := h.stall
		h.mu.Unlock()
		if h.discovery.Load() && h643ServeDiscovery(w, r.URL.Path) {
			return
		}
		if h.stringRows.Load() && strings.HasPrefix(r.URL.Path, "/ext/") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"rows":[{"v":"ok"}],"path":"` + r.URL.Path + `"}`))
			return
		}
		if stall > 0 && strings.Contains(r.URL.Path, "/slow/") {
			select {
			case <-time.After(stall):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"rows":[{"v":1}],"path":"` + r.URL.Path + `"}`))
	}))
	t.Cleanup(h.srv.Close)
	return h
}

// h643Opts is the fixture shape.
type h643Opts struct {
	cohorts  int // K Group cohorts, each granted get/list on widgets + RAs
	external int // M external widgets, 2 external stages each
	slow     int // of the M external widgets, how many hit a stalled path
	internal int // N internal widgets (their RA has no api stage)
	// extra are hand-built units (PR-B arms): a widget, its RESTAction's api[]
	// stages verbatim, and whether the arm counts it as external.
	extra []h643Extra
	// things seeds n objects of h643ThingsGVR (an internal list an iterator
	// stage can range over), readable by every cohort.
	things int
	// objs are extra raw objects (#643 PR-B2: the walk's widget tree and its
	// RESTActions). Unlike extra they are NOT pre-harvested: the discovery
	// walk is what finds them.
	objs []*unstructured.Unstructured
	// typed are extra typed objects (an RBAC binding for the walk's SA).
	typed []runtime.Object
	// walk turns on the endpoint's discovery + CRD answers (h643ServeDiscovery)
	// and resets the process-wide plural store / CRD-schema memo around the arm.
	walk bool
}

// h643Extra is one hand-built (widget, RESTAction) unit.
type h643Extra struct {
	widget, ra string
	external   bool
	api        []any
}

// h643ThingsGVR is an internal (apiserver) list the iterator arms range over.
var h643ThingsGVR = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1", Resource: "h643things"}

// h643Thing builds one h643things object.
func h643Thing(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h643ThingsGVR.Group + "/" + h643ThingsGVR.Version,
		"kind":       "H643Thing",
		"metadata":   map[string]any{"name": name, "namespace": h643NS},
	}}
}

// h643ExtStage is an api[] stage dispatched through the external endpointRef.
func h643ExtStage(name, path string) map[string]any {
	return map[string]any{
		"name": name, "path": path, "verb": "GET",
		"endpointRef": map[string]any{"name": h643ExtSecret, "namespace": h643AuthnNS},
	}
}

type h643Env struct {
	t    *testing.T
	ep   *h643Endpoint
	rw   *cache.ResourceWatcher
	kube *kubefake.Clientset
	dyn  *dynamicfake.FakeDynamicClient
	// watches (#749) is the fakewatch barrier: setup returns only once the
	// informers the arms mutate through dyn have an ESTABLISHED watch.
	watches  *fakewatch.Barrier
	deps     rePrewarmDeps
	engine   *prewarmEngine
	widgets  []navWidgetEntry
	external map[string]bool // widget ns/name → external
}

// h643RA builds a RESTAction. Every stage dispatches through endpointRef epName
// (a Secret in the authn namespace, served by the Secrets informer) when epName
// is non-empty — the shape of the 057 observability RESTActions.
func h643RA(name, epName string, stagePaths ...string) *unstructured.Unstructured {
	api := make([]any, 0, len(stagePaths))
	for i, p := range stagePaths {
		st := map[string]any{
			"name": "s" + itoaLine(i),
			"path": p,
			"verb": "GET",
		}
		if epName != "" {
			st["endpointRef"] = map[string]any{"name": epName, "namespace": h643AuthnNS}
		}
		api = append(api, st)
	}
	spec := map[string]any{}
	if len(api) > 0 {
		spec["api"] = api
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h643RAGVR.Group + "/" + h643RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h643NS, "uid": "uid-" + name, "resourceVersion": "1"},
		"spec":       spec,
	}}
}

func h643Widget(name, raName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h643WidgetGVR.Group + "/" + h643WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": name, "namespace": h643NS, "uid": "uid-" + name, "resourceVersion": "1"},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": raName, "namespace": h643NS},
			// The CRD-status validation the walk's REAL widgets.Resolve runs
			// (h643ServeDiscovery) needs a widgetData; the seed's seam ignores it.
			"widgetData": map[string]any{"title": name},
		},
	}}
}

// h643ServeDiscovery answers the discovery + CRD requests the real
// widgets.Resolve makes (KindForGVR for a child ref, GVRFor + the CRD GET for
// the status validation) for the panels GVR. Reports whether it answered.
func h643ServeDiscovery(w http.ResponseWriter, path string) bool {
	g, v := h643WidgetGVR.Group, h643WidgetGVR.Version
	writeJSON := func(obj any) bool {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(obj)
		return true
	}
	gvd := func(g, v string) metav1.GroupVersionForDiscovery {
		return metav1.GroupVersionForDiscovery{GroupVersion: g + "/" + v, Version: v}
	}
	switch path {
	case "/api":
		return writeJSON(&metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
	case "/api/v1":
		return writeJSON(&metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1"})
	case "/apis":
		return writeJSON(&metav1.APIGroupList{
			TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
			Groups: []metav1.APIGroup{
				{Name: g, Versions: []metav1.GroupVersionForDiscovery{gvd(g, v)}, PreferredVersion: gvd(g, v)},
				{Name: "apiextensions.k8s.io", Versions: []metav1.GroupVersionForDiscovery{gvd("apiextensions.k8s.io", "v1")},
					PreferredVersion: gvd("apiextensions.k8s.io", "v1")},
			},
		})
	case "/apis/" + g + "/" + v:
		return writeJSON(&metav1.APIResourceList{
			TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: g + "/" + v,
			APIResources: []metav1.APIResource{{Name: h643WidgetGVR.Resource, SingularName: "panel", Namespaced: true,
				Kind: "Panel", Verbs: metav1.Verbs{"get", "list", "watch"}}},
		})
	case "/apis/apiextensions.k8s.io/v1":
		return writeJSON(&metav1.APIResourceList{
			TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "apiextensions.k8s.io/v1",
			APIResources: []metav1.APIResource{{Name: "customresourcedefinitions", SingularName: "customresourcedefinition",
				Kind: "CustomResourceDefinition", Verbs: metav1.Verbs{"get", "list", "watch"}}},
		})
	case "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/" + h643WidgetGVR.Resource + "." + g:
		return writeJSON(map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata":   map[string]any{"name": h643WidgetGVR.Resource + "." + g},
			"spec": map[string]any{
				"group": g,
				"names": map[string]any{"kind": "Panel", "plural": h643WidgetGVR.Resource, "singular": "panel"},
				"scope": "Namespaced",
				"versions": []any{map[string]any{
					"name": v, "served": true, "storage": true,
					"schema": map[string]any{"openAPIV3Schema": map[string]any{
						"type": "object",
						"properties": map[string]any{"spec": map[string]any{
							"type": "object",
							"properties": map[string]any{"widgetData": map[string]any{
								"type":       "object",
								"properties": map[string]any{"title": map[string]any{"type": "string"}},
							}},
						}},
					}},
				}},
			},
		})
	}
	return false
}

// h643Setup builds the fixture. Arms that use it reassign package vars, so they
// are not t.Parallel().
// h643WatchDelayForTest, when set, forces the list/watch gap: every watch
// registers with the fake tracker only after this delay (fakewatch.
// InstallDelayed). #749's arm sets it to prove the setup's barrier is what
// keeps a write from being lost.
var h643WatchDelayForTest time.Duration

func h643Setup(t *testing.T, o h643Opts) *h643Env {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	engineLatchTestMu.Lock()
	t.Cleanup(engineLatchTestMu.Unlock)
	zeroCustomerInFlight()
	resetFirstNavLatchForTest()
	t.Cleanup(resetFirstNavLatchForTest)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.ResetLearnedIdentitiesForTest()
	t.Cleanup(cache.ResetLearnedIdentitiesForTest)
	resetLearnedAdmissionForTest()
	t.Cleanup(resetLearnedAdmissionForTest)
	resetSeedUnitCostForTest()
	t.Cleanup(resetSeedUnitCostForTest)
	resetPrewarmPassStatsForTest()
	t.Cleanup(resetPrewarmPassStatsForTest)

	env := &h643Env{t: t, ep: newH643Endpoint(t), external: map[string]bool{}}
	if o.walk {
		env.ep.discovery.Store(true)
		cache.ResetPluralsStoreForTest()
		t.Cleanup(cache.ResetPluralsStoreForTest)
		crdschema.InvalidateCRDSchemaMemo()
		t.Cleanup(crdschema.InvalidateCRDSchemaMemo)
	}

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h643RAGVR:     "RESTActionList",
		h643WidgetGVR: "PanelList",
		h643ThingsGVR: "H643ThingList",
		crbGVR:        "ClusterRoleBindingList",
		crGVR:         "ClusterRoleList",
		rbGVR:         "RoleBindingList",
		rGVR:          "RoleList",
	}
	objs := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "h643-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "list"},
				APIGroups: []string{h643RAGVR.Group, h643WidgetGVR.Group, h643ThingsGVR.Group},
				Resources: []string{h643RAGVR.Resource, h643WidgetGVR.Resource, h643ThingsGVR.Resource},
			}},
		},
	}
	for i := 0; i < o.cohorts; i++ {
		b := l262Binding("h643-cohort-"+itoaLine(i), "Group", "h643-g"+itoaLine(i))
		b.RoleRef.Name = "h643-reader"
		objs = append(objs, b)
	}

	navHarv := newNavWidgetHarvester()
	add := func(wName, raName string, external bool, stagePaths ...string) {
		ep := ""
		if external {
			ep = h643ExtSecret
		}
		objs = append(objs, h643RA(raName, ep, stagePaths...))
		w := h643Widget(wName, raName)
		objs = append(objs, w)
		navHarv.harvestNavWidget(w, h643WidgetGVR, -1, -1, -1, -1)
		env.external[h643NS+"/"+wName] = external
	}
	for _, x := range o.extra {
		objs = append(objs, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": h643RAGVR.Group + "/" + h643RAGVR.Version,
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": x.ra, "namespace": h643NS, "uid": "uid-" + x.ra, "resourceVersion": "1"},
			"spec":       map[string]any{"api": x.api},
		}})
		w := h643Widget(x.widget, x.ra)
		objs = append(objs, w)
		navHarv.harvestNavWidget(w, h643WidgetGVR, -1, -1, -1, -1)
		env.external[h643NS+"/"+x.widget] = x.external
	}
	for _, obj := range o.objs {
		objs = append(objs, obj)
	}
	objs = append(objs, o.typed...)
	for i := 0; i < o.things; i++ {
		objs = append(objs, h643Thing("thing-"+itoaLine(i)))
	}
	for i := 0; i < o.internal; i++ {
		add("int-w"+itoaLine(i), "int-ra"+itoaLine(i), false)
	}
	for i := 0; i < o.external; i++ {
		seg := "fast"
		if i < o.slow {
			seg = "slow"
		}
		ra := "ext-ra" + itoaLine(i)
		add("ext-w"+itoaLine(i), ra, true, "/ext/"+seg+"/"+ra+"/s0", "/ext/"+seg+"/"+ra+"/s1")
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
	env.dyn = dyn
	// #749 — WaitForCacheSync turns true after the initial LIST, which can be
	// BEFORE the informer's WATCH reaches the fake tracker; the tracker does not
	// replay from a resourceVersion, so a write in that gap is lost for good
	// (#660/#702/#689). The barrier below closes it.
	if h643WatchDelayForTest > 0 {
		env.watches = fakewatch.InstallDelayed(dyn, h643WatchDelayForTest)
	} else {
		env.watches = fakewatch.Install(dyn)
	}
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		rw.Stop()
		wcancel()
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
	// Serve the RESTActions from the informer (objects.Get never dials).
	for _, g := range []schema.GroupVersionResource{h643RAGVR, h643WidgetGVR, h643ThingsGVR} {
		if added, syncCh := rw.EnsureResourceType(g); added {
			select {
			case <-syncCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s informer did not sync", g)
			}
		}
		deadline := time.Now().Add(5 * time.Second)
		for !rw.IsServable(g) {
			if time.Now().After(deadline) {
				t.Fatalf("%s never became servable", g)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	// #749 — every resource the arms mutate through dyn has an established
	// watch before any arm writes (event-driven, bounded by the watch delay a
	// test forces, never a fixed sleep).
	if err := env.watches.WaitFor(10*time.Second+h643WatchDelayForTest,
		h643RAGVR.Resource, h643WidgetGVR.Resource, h643ThingsGVR.Resource); err != nil {
		t.Fatalf("#749: %v", err)
	}
	// The endpointRef Secret, served by the production Secrets informer over a
	// fake clientset (FromInformerSecret — no apiserver Secret GET).
	env.kube = kubefake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: h643ExtSecret, Namespace: h643AuthnNS, UID: "uid-" + h643ExtSecret, ResourceVersion: "1"},
		Data:       map[string][]byte{"server-url": []byte(env.ep.srv.URL)},
	})
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	restoreCli := cache.SetSecretsClientForTest(env.kube)
	ictx, icancel := context.WithCancel(context.Background())
	if err := cache.StartSecretsInformer(ictx, nil, h643AuthnNS); err != nil {
		icancel()
		t.Fatalf("StartSecretsInformer: %v", err)
	}
	t.Cleanup(func() {
		icancel()
		restoreCli()
		cache.ResetSecretsInformerForTest()
		cache.ResetSecretsSnapshotForTest()
	})
	deadline := time.Now().Add(5 * time.Second)
	for !cache.SecretsCacheServable() {
		if time.Now().After(deadline) {
			t.Fatalf("the Secrets informer never became servable")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h643WidgetGVR, h643RAGVR, h643ThingsGVR})
	knowEveryRepresentative(t, h643WidgetGVR, h643RAGVR, h643ThingsGVR) // #841: a known requester per representative
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	env.rw = rw

	// The one seam: the widget's apiRef through the REAL apiref.Resolve.
	prevW := widgetsResolveFn
	widgetsResolveFn = func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		ref, err := widgets.GetApiRef(opts.In.Object)
		if err != nil {
			return opts.In, err
		}
		ds, err := apiref.Resolve(ctx, apiref.ResolveOptions{
			RC: opts.RC, ApiRef: ref, AuthnNS: opts.AuthnNS, PerPage: opts.PerPage, Page: opts.Page,
		})
		if err != nil {
			return opts.In, err
		}
		if err := unstructured.SetNestedField(opts.In.Object, ds, "status", "widgetData"); err != nil {
			return opts.In, err
		}
		return opts.In, nil
	}
	t.Cleanup(func() { widgetsResolveFn = prevW })

	env.widgets = navHarv.snapshot()
	env.deps = rePrewarmDeps{
		rw:        rw,
		lister:    func(context.Context) ([]navigationRoot, error) { return nil, nil },
		harvester: newContentPrewarmHarvester(),
		navHarv:   navHarv,
		saEP:      endpoints.Endpoint{ServerURL: env.ep.srv.URL},
		saRC:      &rest.Config{Host: env.ep.srv.URL},
		authnNS:   h643AuthnNS,
	}
	env.engine = &prewarmEngine{
		queue: workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedControllerRateLimiter[prewarmScope](),
		),
		rbacShift: cache.NewRBACShiftAccumulator(),
		yieldPoll: defaultEngineYieldPoll,
	}
	env.engine.scopeHandler = makeBootScopeHandler(env.deps)
	t.Cleanup(env.engine.queue.ShutDown)
	return env
}

// h643Budget installs a per-kind scope budget (the production prewarmScopeTimeoutFn
// seam). Kinds absent from m get an hour — never a real deadline.
func h643Budget(t *testing.T, m map[prewarmScopeKind]time.Duration) {
	t.Helper()
	prev := prewarmScopeTimeoutFn
	prewarmScopeTimeoutFn = func(s prewarmScope) time.Duration {
		if d, ok := m[s.kind]; ok {
			return d
		}
		return time.Hour
	}
	t.Cleanup(func() { prewarmScopeTimeoutFn = prev })
}

// pass enqueues s and runs exactly ONE processScope for it through the real
// queue. It returns the dials the pass made. A requeue the pass triggers (a cut)
// is left on the queue's rate limiter, not run.
func (env *h643Env) pass(s prewarmScope) int {
	env.t.Helper()
	before := env.ep.total()
	env.engine.enqueueScope(s)
	var got prewarmScope
	for {
		var shutdown bool
		got, shutdown = env.engine.queue.Get()
		if shutdown {
			env.t.Fatalf("queue shut down")
		}
		if got == s {
			break
		}
		// A cut pass's rate-limited requeue (or a #105 tail re-enqueue) that
		// landed since: not the pass the arm asked for — discard it.
		env.engine.queue.Forget(got)
		env.engine.queue.Done(got)
	}
	env.engine.processScope(context.Background(), got)
	// A cut pass AddRateLimited-requeues itself; drop it so the next pass the arm
	// drives is the one it asked for (the backoff never fires before Forget).
	env.engine.queue.Forget(got)
	return env.ep.total() - before
}

// putKeys returns the L1 keys holding a widget cell for any harvested widget,
// split by whether the widget is external.
func (env *h643Env) warmWidgetCells() (internal, external int) {
	env.t.Helper()
	store := cache.ResolvedCache()
	for _, k := range store.KeysForTest() {
		e, ok := store.GetNoTouch(k)
		if !ok || e == nil || e.Inputs == nil || e.Inputs.Resource != h643WidgetGVR.Resource {
			continue
		}
		if env.external[e.Inputs.Namespace+"/"+e.Inputs.Name] {
			external++
		} else {
			internal++
		}
	}
	return internal, external
}

// h643Cohorts returns the enumerated cohort count for the widget GVR — the arm's
// precondition that the fixture really has K cohorts.
func h643Cohorts() int {
	return len(enumeratePrewarmTargetsForGVRFn(h643WidgetGVR, "list"))
}
