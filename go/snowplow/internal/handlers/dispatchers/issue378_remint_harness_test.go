package dispatchers

// issue378_remint_harness_test.go — the shared #378 P1 harness (brief
// issuecomment-5990884686): K=4 cell classes × M cells, filled and read through
// the real serve paths, kept fresh by the real refresher pool and the real
// reaper tick (#316), against a fake apiserver (xenv test mode) and a real
// ResourceWatcher whose informers serve every CR and target object.
//
// REAL: the /call handlers (restActionHandler / widgetsHandler ServeHTTP with
// the production fetchObject → objects.Get → informer, the production RBAC
// gate and key derivation), the store and its lazy evicts, the summary/reaper
// tick, the refresher workqueue + resolveAndPopulateL1 (key recomputation from
// the stored inputs, the #424 guard, every decline gate, the terminal write),
// the restactions / raFullList / apistage re-resolves (resolveOnceProd), the
// apiref raFullList Go-slice serve.
//
// SEAMED (the #444 pattern, approved for #378): the widgets resolver body —
// widgetsResolveFn on the serve path and the widgets / widgetContent branch of
// resolveOnceFn on the refresh path — because widgets.Resolve needs in-cluster
// CRD-schema discovery. The seam returns the CR it was handed plus a sequence
// stamp; the key, the Put, the serve and the refresh terminal stay production.
// The raFullList and widgetContent cells are FILLED by their production
// producers (the boot-seed shape: PutRAFullList + the sliceability verdict +
// the RA self-dep, and populateWidgetContentL1) — no customer /call writes them
// in production either — and READ by customers through the real serve paths.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	r378RA     = "r378-ra"   // restactions class: GET-by-name, page=2&perPage=10 + extras
	r378RAFL   = "r378-rafl" // raFullList class: the apiRef RA of the widgets-class widget
	r378Widget = "r378-w"    // widgets class: keyExtras ["tab"], RBAC-sensitive apiRef widget
	r378WCPfx  = "r378-wc-"  // widgetContent class: plain widgets r378-wc-0..M-1
)

// r378RAFLFilter is a cleanly sliceable filter over the RA's list step.
const r378RAFLFilter = `
{
  items: (
    (.cms.items // []) as $all
    | (.slice.offset  // 0)              as $offset
    | (.slice.perPage // ($all | length)) as $perPage
    | [ $all | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $all[.] ]
  )
}
`

// r378Bounds sets the short REAL store bounds. Must run before the store is
// first built (psBuildWatcher resets it; it is rebuilt lazily from the env).
func r378Bounds(t *testing.T, ttlS, maxAgeS int) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", fmt.Sprint(ttlS))
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", fmt.Sprint(maxAgeS))
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "1") // the reaper tick
}

// r378Stat reads one snowplow_resolved_cache stat; ok=false when absent.
func r378Stat(name string) (int64, bool) {
	v, ok := cache.ResolvedCacheStatsByStat()[name]
	return v, ok
}

func r378MustStat(t *testing.T, name string) int64 {
	t.Helper()
	v, ok := r378Stat(name)
	if !ok {
		t.Fatalf("#378 RED: snowplow_resolved_cache has no stat %q", name)
	}
	return v
}

type r378Cell struct {
	class string
	i     int
	key   string
	born  time.Time
}

type r378Env struct {
	a        psArm
	dyn      *dynamicfake.FakeDynamicClient
	srvURL   string
	saEP     *endpoints.Endpoint
	saRC     *rest.Config
	alice    context.Context
	m        int
	cells    []r378Cell
	seq      atomic.Int64
	stopOnce sync.Once
	stopFns  []func()
}

func r378RAObj(name string, api []any, filter string) *unstructured.Unstructured {
	spec := map[string]any{"api": api}
	if filter != "" {
		spec["filter"] = filter
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec":       spec,
	}}
}

func r378WidgetObj(name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec":       spec,
	}}
}

// r378Objects are the CRs the informers serve.
func r378Objects(m int) []runtime.Object {
	objs := []runtime.Object{
		r378RAObj(r378RA, []any{map[string]any{"name": "target",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj, "continueOnError": true}}, ""),
		r378RAObj(r378RAFL, []any{map[string]any{"name": "cms",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "continueOnError": true}}, r378RAFLFilter),
		r378WidgetObj(r378Widget, map[string]any{
			"keyExtras":          []any{"tab"},
			"apiRef":             map[string]any{"name": r378RAFL, "namespace": h1NS},
			"widgetDataTemplate": []any{map[string]any{"forPath": "v", "expression": "${ .items }"}},
		}),
	}
	for i := 0; i < m; i++ {
		objs = append(objs, r378WidgetObj(fmt.Sprintf("%s%d", r378WCPfx, i), map[string]any{}))
	}
	return objs
}

// r378Widget is the widgets-resolver stand-in: the CR handed in plus a sequence
// stamp (so a refresh is distinguishable from the fill).
func (e *r378Env) widgetBody(in *unstructured.Unstructured) *unstructured.Unstructured {
	out := in.DeepCopy()
	_ = unstructured.SetNestedField(out.Object, fmt.Sprint(e.seq.Add(1)), "status", "widgetData", "seq")
	return out
}

func r378Setup(t *testing.T, ttlS, maxAgeS, m int, extra ...runtime.Object) *r378Env {
	t.Helper()
	k423Env(t)
	r378Bounds(t, ttlS, maxAgeS)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	objs := append(append(append(k423WidgetExtras(a), k423PortalReaders(a)...), r378Objects(m)...), extra...)
	dyn := psBuildWatcher(t, a, objs...)
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	rw := cache.Global()
	for _, gvr := range r378GVRs() {
		if _, ch := rw.EnsureResourceType(gvr); ch != nil {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("SETUP: informer %s did not sync", gvr)
			}
		}
		deadline := time.Now().Add(3 * time.Second)
		for !rw.IsServable(gvr) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if !rw.IsServable(gvr) {
			t.Fatalf("SETUP: %s never became servable", gvr)
		}
	}
	e := &r378Env{a: a, dyn: dyn, srvURL: srv.URL, m: m,
		saEP: &endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, saRC: &rest.Config{Host: srv.URL},
		alice: psUserCtx(a, psAlice)}
	restoreW := setWidgetsResolveForTest(func(_ context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		return e.widgetBody(opts.In), nil
	})
	t.Cleanup(restoreW)
	restoreR := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		switch in.CacheEntryClass {
		case "widgets", cache.CacheEntryClassWidgetContent:
			obj, err := e.dyn.Resource(h1WidgetGVR).Namespace(in.Namespace).Get(ctx, in.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return encodeResolvedJSON(e.widgetBody(obj))
		}
		return resolveOnceProd(ctx, in)
	})
	t.Cleanup(restoreR)
	t.Cleanup(e.stop)
	return e
}

// startRefresher starts the real refresher pool for every L1 class, wired the
// way RegisterRefreshHandlers wires production (one resolveAndPopulateL1
// closure), with this harness's SA transport.
func (e *r378Env) startRefresher(t *testing.T) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "4")
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, e.saEP, e.saRC)
	}
	for _, class := range []string{"restactions", "widgets", cache.CacheEntryClassApistage,
		cache.CacheEntryClassWidgetContent, cache.CacheEntryClassRAFullList} {
		cache.RegisterRefreshFunc(class, refresh)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	e.stopFns = append(e.stopFns, func() { cancel(); cache.ResetRefresherForTest() })
}

func (e *r378Env) stop() {
	e.stopOnce.Do(func() {
		for i := len(e.stopFns) - 1; i >= 0; i-- {
			e.stopFns[i]()
		}
	})
}

func r378GVRs() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{h1RAGVR, h1WidgetGVR}
}

func (e *r378Env) raQuery(i int) string {
	ex, _ := json.Marshal(map[string]any{"tab": fmt.Sprintf("t%d", i), "q": "x"})
	return fmt.Sprintf("/call?resource=restactions&apiVersion=%s/%s&namespace=%s&name=%s&page=2&perPage=10&extras=%s",
		h1RAGVR.Group, h1RAGVR.Version, h1NS, r378RA, url.QueryEscape(string(ex)))
}

func (e *r378Env) widgetQuery(name string, extras map[string]any) string {
	q := fmt.Sprintf("/call?resource=%s&apiVersion=%s/%s&namespace=%s&name=%s",
		h1WidgetGVR.Resource, h1WidgetGVR.Group, h1WidgetGVR.Version, h1NS, name)
	if extras != nil {
		ex, _ := json.Marshal(extras)
		q += "&extras=" + url.QueryEscape(string(ex))
	}
	return q
}

func (e *r378Env) raflExtras(i int) map[string]any { return map[string]any{"q": fmt.Sprintf("v%d", i)} }

// serve drives one customer read of cell (class, i) through its real serve path
// and returns an error if it was not a 200 / a real serve.
func (e *r378Env) serve(ctx context.Context, class string, i int) error {
	switch class {
	case "restactions":
		rec := httptest.NewRecorder()
		(&restActionHandler{authnNS: psAuthnNS, saRC: e.saRC}).ServeHTTP(rec,
			httptest.NewRequest(http.MethodGet, e.raQuery(i), nil).WithContext(ctx))
		if rec.Code != http.StatusOK {
			return fmt.Errorf("restactions[%d]: %d %s", i, rec.Code, psTrunc(rec.Body.String(), 200))
		}
	case "widgets":
		rec := httptest.NewRecorder()
		(&widgetsHandler{authnNS: psAuthnNS, saRC: e.saRC}).ServeHTTP(rec,
			httptest.NewRequest(http.MethodGet, e.widgetQuery(r378Widget, map[string]any{"tab": fmt.Sprintf("t%d", i)}), nil).WithContext(ctx))
		if rec.Code != http.StatusOK {
			return fmt.Errorf("widgets[%d]: %d %s", i, rec.Code, psTrunc(rec.Body.String(), 200))
		}
	case cache.CacheEntryClassWidgetContent:
		rec := httptest.NewRecorder()
		(&widgetsHandler{authnNS: psAuthnNS, saRC: e.saRC}).ServeHTTP(rec,
			httptest.NewRequest(http.MethodGet, e.widgetQuery(fmt.Sprintf("%s%d", r378WCPfx, i), nil), nil).WithContext(ctx))
		if rec.Code != http.StatusOK {
			return fmt.Errorf("widgetContent[%d]: %d %s", i, rec.Code, psTrunc(rec.Body.String(), 200))
		}
	case cache.CacheEntryClassRAFullList:
		page, err := apiref.Resolve(cache.WithL1KeyContext(ctx, fmt.Sprintf("L1_r378_consumer_%d", i)), apiref.ResolveOptions{
			ApiRef: templatesv1.ObjectReference{
				Reference:  templatesv1.Reference{Name: r378RAFL, Namespace: h1NS},
				Resource:   h1RAGVR.Resource,
				APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
			},
			PerPage: 5, Page: 1, Extras: e.raflExtras(i),
		})
		if err != nil {
			return fmt.Errorf("raFullList[%d]: %v", i, err)
		}
		if _, ok := page["items"]; !ok {
			return fmt.Errorf("raFullList[%d]: no items in %v", i, page)
		}
	default:
		return fmt.Errorf("unknown class %s", class)
	}
	return nil
}

// r378CellHits counts customer serves answered from the class's OWN L1 cell
// (the dispatcher's per-(handler, GVR) hit counter; the apiref Go-slice serve
// counter for raFullList) — not hit_total, which also counts the nested
// apistage hits a cold RA resolve makes.
func r378CellHits(class string) int64 {
	if class == cache.CacheEntryClassRAFullList {
		return int64(cache.RAFullListServeSnapshot().Hit)
	}
	gvr := h1WidgetGVR
	if class == "restactions" {
		gvr = h1RAGVR
	}
	v, ok := l1LookupCells.Load(class + "|" + gvr.String())
	if !ok {
		return 0
	}
	return int64(v.(*l1LookupCell).hit.Load())
}

var r378Classes = []string{"restactions", "widgets", cache.CacheEntryClassRAFullList, cache.CacheEntryClassWidgetContent}

// fill creates every cell: restactions + widgets by a cold customer /call;
// raFullList + widgetContent by their production producers. It then locates
// each cell's key by scanning L1 (independent of the key builders) and records
// its BornAt.
func (e *r378Env) fill(t *testing.T) {
	t.Helper()
	c := cache.ResolvedCache()
	for i := 0; i < e.m; i++ {
		for _, class := range []string{"restactions", "widgets"} {
			if err := e.serve(e.alice, class, i); err != nil {
				t.Fatalf("FILL: %v", err)
			}
		}
		// raFullList — the producer's boot-seed shape (issue426 pattern).
		ex := e.raflExtras(i)
		seedIn, ok := apiref.SeedFullListRAKeyInputsForTest(e.alice, h1RAGVR, h1NS, r378RAFL, ex)
		if !ok {
			t.Fatalf("FILL: no raKey for alice")
		}
		k := cache.ComputeKey(seedIn)
		c.PutRAFullList(k, seedIn, map[string]any{"items": []any{map[string]any{"n": i, "phase": "fill"}}})
		cache.RecordSliceability(k, cache.SliceShapeHash("apiref", h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource,
			h1NS, r378RAFL, r378RAFLFilter), true)
		cache.Deps().Record(context.Background(), k, h1RAGVR, h1NS, r378RAFL) // RA-CR self-dep (the producer records it)
		// widgetContent — the walker's production Put.
		name := fmt.Sprintf("%s%d", r378WCPfx, i)
		obj, err := e.dyn.Resource(h1WidgetGVR).Namespace(h1NS).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("FILL: %v", err)
		}
		populateWidgetContentL1(context.Background(), h1WidgetGVR, obj, -1, -1, e.widgetBody(obj), 0)
	}
	// Locate every cell by its stored inputs.
	byClass := map[string]map[string]bool{}
	for _, k := range c.KeysForTest() {
		ent, ok := c.GetNoTouch(k)
		if !ok || ent.Inputs == nil {
			continue
		}
		in := ent.Inputs
		match := false
		switch in.CacheEntryClass {
		case "restactions":
			match = in.Name == r378RA
		case "widgets":
			match = in.Name == r378Widget
		case cache.CacheEntryClassRAFullList:
			match = in.Name == r378RAFL
		case cache.CacheEntryClassWidgetContent:
			match = len(in.Name) > len(r378WCPfx) && in.Name[:len(r378WCPfx)] == r378WCPfx
		}
		if !match {
			continue
		}
		if byClass[in.CacheEntryClass] == nil {
			byClass[in.CacheEntryClass] = map[string]bool{}
		}
		byClass[in.CacheEntryClass][k] = true
		e.cells = append(e.cells, r378Cell{class: in.CacheEntryClass, key: k, born: ent.BornAt})
	}
	for _, class := range r378Classes {
		if got := len(byClass[class]); got != e.m {
			t.Fatalf("FILL: class %s has %d cells, want %d (the fill did not reach L1)", class, got, e.m)
		}
	}
	// Every cell is served from L1 now (no cold read remains in the harness).
	h0, _ := r378Stat("hit_total")
	e.readAll(t, e.alice)
	if h1, _ := r378Stat("hit_total"); h1-h0 != int64(len(r378Classes)*e.m) {
		t.Fatalf("FILL: the warm re-read must be %d L1 hits, got %d", len(r378Classes)*e.m, h1-h0)
	}
}

func (e *r378Env) readAll(t *testing.T, ctx context.Context) {
	t.Helper()
	for i := 0; i < e.m; i++ {
		for _, class := range r378Classes {
			if err := e.serve(ctx, class, i); err != nil {
				t.Errorf("READ: %v", err)
			}
		}
	}
}

func (e *r378Env) maxBorn() time.Time {
	var m time.Time
	for _, c := range e.cells {
		if c.born.After(m) {
			m = c.born
		}
	}
	return m
}

// churn issues fake informer UPDATEs on every cell's backing object (the
// ConfigMap the RAs read, both RA CRs, every widget CR) every period until
// stop is closed.
func (e *r378Env) churn(t *testing.T, period time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		tk := time.NewTicker(period)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
			}
			n++
			bump := func(gvr schema.GroupVersionResource, ns, name string) {
				obj, err := e.dyn.Resource(gvr).Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
				if err != nil {
					return
				}
				if gvr == psConfigmapsGVR {
					_ = unstructured.SetNestedField(obj.Object, fmt.Sprintf("%s-%d", psSentinel, n), "data", "password")
				} else {
					ann := obj.GetAnnotations()
					if ann == nil {
						ann = map[string]string{}
					}
					ann["r378/churn"] = fmt.Sprint(n)
					obj.SetAnnotations(ann)
				}
				_, _ = e.dyn.Resource(gvr).Namespace(ns).Update(context.Background(), obj, metav1.UpdateOptions{})
			}
			bump(psConfigmapsGVR, psTargetNS, psTargetObj)
			bump(h1RAGVR, h1NS, r378RA)
			bump(h1RAGVR, h1NS, r378RAFL)
			bump(h1WidgetGVR, h1NS, r378Widget)
			for i := 0; i < e.m; i++ {
				bump(h1WidgetGVR, h1NS, fmt.Sprintf("%s%d", r378WCPfx, i))
			}
		}
	}()
	return &wg
}

// keepWarm issues a customer read of every cell every period until stop.
func (e *r378Env) keepWarm(t *testing.T, period time.Duration, stop <-chan struct{}) *sync.WaitGroup {
	t.Helper()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(period)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
			}
			for i := 0; i < e.m; i++ {
				for _, class := range r378Classes {
					_ = e.serve(e.alice, class, i)
				}
			}
		}
	}()
	return &wg
}
