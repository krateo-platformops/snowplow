// issue375_dep_gen_guard_test.go — #375 dep-generation guard: the REAL-BOUNDARY arms
// at the apistage content site (the ms-class read → Record window) and the outer
// resolve's convergence through the real refresher.
//
// Everything here is driven through the real pieces: a real fake-apiserver-backed
// ResourceWatcher with a real informer, real object mutations through the dynamic
// client (so dep events reach Deps().OnObjectEvent through the real watch-handler →
// dep-event worker funnel, never via the OnUpdate shim), the real api.Resolve stage
// loop, real apistageContentServe, real gen-guarded Puts, and the real refresher
// (StartRefresher, real dequeue, the real trigger-set consume). The only stand-ins
// are the two RefreshFuncs, because resolveAndPopulateL1 lives in dispatchers. They
// mirror its entry shape: WithL1KeyContext, then re-resolve, then ReplaceIfGen.
// The deterministic window is the existing dispatchViaInformerFn seam, which sits
// exactly between the content read and the cell's Put → Record.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var g375Gadgets = schema.GroupVersionResource{Group: "gadgets.krateo.io", Version: "v1", Resource: "gadgets"}

const u375 = "broad-375"

func obj375(gvr schema.GroupVersionResource, kind, ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.Group + "/" + gvr.Version,
		"kind":       kind,
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// newWatcher375 is newF1Watcher plus a second content GVR (gadgets) and it RETURNS the
// dynamic client so arms can mutate objects for real.
func newWatcher375(t *testing.T) dynamic.Interface {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetRefresherForTest()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetRefresherForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})
	var seed []runtime.Object
	for _, ns := range []string{"team-a", "team-b"} {
		seed = append(seed, obj375(f1WidgetsGVR, "Widget", ns, "widget-"+ns))
		seed = append(seed, obj375(g375Gadgets, "Gadget", ns, "gadget-"+ns))
		seed = append(seed, &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "c375-" + u375},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: u375}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "c375-lister"},
		})
	}
	seed = append(seed, &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "c375-lister"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{f1WidgetsGVR.Group}, Resources: []string{f1WidgetsGVR.Resource}, Verbs: []string{"list", "get"}},
			{APIGroups: []string{g375Gadgets.Group}, Resources: []string{g375Gadgets.Resource}, Verbs: []string{"list", "get"}},
		},
	})
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		f1WidgetsGVR: "WidgetList",
		g375Gadgets:  "GadgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	for _, g := range []schema.GroupVersionResource{f1WidgetsGVR, g375Gadgets} {
		_, syncCh := rw.EnsureResourceType(g)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("%v informer did not sync", g)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

func stage375(id string, gvr schema.GroupVersionResource) *templates.API {
	return &templates.API{
		Name:   id,
		Path:   "/apis/" + gvr.Group + "/" + gvr.Version + "/" + gvr.Resource,
		Verb:   ptr.To(http.MethodGet),
		Filter: ptr.To("." + id + ".items"),
	}
}

// resolve375 runs the REAL api.Resolve stage loop as u375 under parent (which carries
// the resolve-entry L1 key / sink, and on a refresher re-resolve the trigger set).
func resolve375(parent context.Context, stages ...*templates.API) map[string]any {
	ctx := xcontext.BuildContext(parent, xcontext.WithUserInfo(jwtutil.UserInfo{Username: u375}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	return Resolve(ctx, ResolveOptions{
		RC: &rest.Config{}, Items: stages,
		RESTActionNamespace: "default", RESTActionName: "c375",
	})
}

func names375(dict map[string]any, id string) []string {
	var out []string
	items, _ := dict[id].([]any)
	for _, it := range items {
		m, _ := it.(map[string]any)
		meta, _ := m["metadata"].(map[string]any)
		if n, _ := meta["name"].(string); n != "" {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func outerBody375(dict map[string]any, ids ...string) []byte {
	m := map[string][]string{}
	for _, id := range ids {
		m[id] = names375(dict, id)
	}
	b, _ := json.Marshal(m)
	return b
}

// churn375 creates a REAL object through the dynamic client and blocks until the dep
// event reached OnObjectEvent (depEventSeq advanced) — the bump is real and complete.
func churn375(t *testing.T, dyn dynamic.Interface, gvr schema.GroupVersionResource, kind, ns, name string) {
	t.Helper()
	seq0 := cache.DepEventSeqForTest()
	if _, err := dyn.Resource(gvr).Namespace(ns).Create(context.Background(), obj375(gvr, kind, ns, name), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %v %s/%s: %v", gvr, ns, name, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for cache.DepEventSeqForTest() == seq0 {
		if time.Now().After(deadline) {
			t.Fatalf("the real dep event for %v %s/%s never reached OnObjectEvent", gvr, ns, name)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type remarks375 struct {
	mu sync.Mutex
	n  map[string]int
}

func (r *remarks375) get(k string) int { r.mu.Lock(); defer r.mu.Unlock(); return r.n[k] }

func observeRemarks375(t *testing.T) *remarks375 {
	r := &remarks375{n: map[string]int{}}
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(k, _ string) {
		r.mu.Lock()
		r.n[k]++
		r.mu.Unlock()
	}))
	return r
}

// armChurnSeam installs the dispatchViaInformerFn seam: the REAL dispatch reads the
// data (@N), then — once per GVR, while armed — a REAL object is created for that GVR
// (N+1) and the bump completes, then the STALE bytes are returned. That is exactly a
// churn inside [content read (:605) → Put (:658) → Record (:661)].
func armChurnSeam(t *testing.T, dyn dynamic.Interface, armed *atomic.Bool, churns map[schema.GroupVersionResource][2]string) {
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	var done sync.Map
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := dispatchViaInformer(ctx, call)
		if armed.Load() {
			for g, kn := range churns {
				if strings.HasSuffix(call.Path, "/"+g.Resource) {
					if _, dup := done.LoadOrStore(g, true); !dup {
						churn375(t, dyn, g, kn[0], "team-a", kn[1])
					}
				}
			}
		}
		return raw, ok
	}
}

// ---------------------------------------------------------------------------
// arm-1 (LOAD-BEARING) — WINDOW discriminator at the ms-class apistage site. The
// outer /call resolve reads the widgets LIST content (data@N at :605), a real widget
// is created (N+1) and its dep event completes, and only THEN is the dep Recorded
// (:661, via the content child sink, propagating to the outer sink). The outer
// resolve's accepted PutIfGen must PUT-THEN-REMARK. GREEN under (c): startSeq was
// captured at resolve ENTRY. NEUTER (a) "capture at Record" (recordInternal re-
// captures each sink's startSeq on append) → the outer baseline is post-churn → NO
// remark → the stale body is accepted silently → RED.
// ---------------------------------------------------------------------------
func TestIssue375_Arm1_ApistageReadToRecordWindow_OuterRemarks(t *testing.T) {
	dyn := newWatcher375(t)
	rem := observeRemarks375(t)
	store := cache.ResolvedCache()
	var armed atomic.Bool
	armChurnSeam(t, dyn, &armed, map[schema.GroupVersionResource][2]string{f1WidgetsGVR: {"Widget", "late-widget"}})
	// WARM: the coordinate already has a dependent (the re-resolve shape).
	cache.Deps().RecordList(context.Background(), "arm1-warm-other", f1WidgetsGVR, "")

	const outerKey = "arm1-outer"
	octx := cache.WithL1KeyContext(context.Background(), outerKey)
	gen0 := store.CaptureGen(outerKey)
	armed.Store(true)
	dict := resolve375(octx, stage375("w", f1WidgetsGVR))
	armed.Store(false)
	if got := names375(dict, "w"); strings.Contains(strings.Join(got, ","), "late-widget") {
		t.Fatalf("setup: the resolve must have read PRE-churn data (the seam returns the @N bytes), got %v", got)
	}
	if !store.PutIfGen(octx, outerKey, &cache.ResolvedEntry{RawJSON: outerBody375(dict, "w")}, gen0) {
		t.Fatalf("setup: outer PutIfGen refused")
	}
	if got := rem.get(outerKey); got != 1 {
		t.Fatalf("#375 arm-1 RED: a real dep event inside the apistage [read :605 → Record :661] window must "+
			"PUT-THEN-REMARK the outer resolve exactly once — got %d. Without it the stale body (no late-widget) "+
			"is accepted with no pending mark.", got)
	}
}

// ---------------------------------------------------------------------------
// GATING (TL) — the outer CONVERGES FRESH through a real two-worker refresher
// interleave. The initial /call resolve reads widgets@N while a real widget is created;
// the outer's own dirty-mark lands while it is NOT resident and the real refresher
// CONSUMES it (skipped_no_entry — the #375 race, driven for real); the content cell and
// the outer are Put stale and both PUT-THEN-REMARKed. Worker 1 dequeues the content
// refresh and is held IN FLIGHT; worker 2 dequeues the outer re-resolve meanwhile.
// With (B)+(C) the outer's remark carries the moved GVR, the re-resolve force-misses
// the stale content cell and converges FRESH. RED @38f18496 / on the (B)-neuter
// (remark with no trigger): the outer re-resolve HITs the stale content cell and
// converges STALE with nothing left to re-mark it.
// ---------------------------------------------------------------------------
func TestIssue375_OuterConvergesFresh_TwoWorkerInterleave(t *testing.T) {
	runConvergenceArm375(t, []*templates.API{stage375("w", f1WidgetsGVR)},
		map[schema.GroupVersionResource][2]string{f1WidgetsGVR: {"Widget", "late-widget"}})
}

// GATING (TL, multi-GVR) — TWO GVRs move inside the initial resolve; the outer must
// converge FRESH for BOTH. RED on the "first moved GVR only" neuter (the multi-GVR
// residual): the second GVR's stale content cell is HIT by the outer re-resolve.
func TestIssue375_TwoGVRsMoved_OuterConvergesFreshForBoth(t *testing.T) {
	runConvergenceArm375(t, []*templates.API{stage375("w", f1WidgetsGVR), stage375("g", g375Gadgets)},
		map[schema.GroupVersionResource][2]string{
			f1WidgetsGVR: {"Widget", "late-widget"},
			g375Gadgets:  {"Gadget", "late-gadget"},
		})
}

func runConvergenceArm375(t *testing.T, stages []*templates.API, churns map[schema.GroupVersionResource][2]string) {
	dyn := newWatcher375(t)
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "4")
	cache.ResetRefresherForTest()
	store := cache.ResolvedCache()
	ids := make([]string, len(stages))
	for i, s := range stages {
		ids[i] = s.Name
	}
	outerIn := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Namespace: "default", Name: "c375-outer"}
	outerKey := cache.ComputeKey(outerIn)

	var armed atomic.Bool
	armChurnSeam(t, dyn, &armed, churns)

	// Refresher stand-ins with the resolve_populate entry shape.
	outerDone := make(chan struct{})
	var outerOnce sync.Once
	var outerReResolves atomic.Int32
	cache.RegisterRefreshFunc("restactions", func(hctx context.Context, k string, used cache.ResolvedKeyInputs) error {
		outerReResolves.Add(1)
		gen0 := store.CaptureGen(k)
		rctx := cache.WithL1KeyContext(hctx, k) // hctx carries the consumed trigger set
		dict := resolve375(rctx, stages...)
		store.ReplaceIfGen(rctx, k, &cache.ResolvedEntry{RawJSON: outerBody375(dict, ids...), Inputs: &used}, gen0)
		outerOnce.Do(func() { close(outerDone) })
		return nil
	})
	var contentInFlight atomic.Int32
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(hctx context.Context, k string, used cache.ResolvedKeyInputs) error {
		contentInFlight.Add(1)
		select { // hold the content refresh IN FLIGHT while the outer re-resolves
		case <-outerDone:
		case <-time.After(5 * time.Second):
		}
		gen0 := store.CaptureGen(k)
		path := "/apis/" + used.Group + "/" + used.Version + "/" + used.Resource
		raw, ok := dispatchViaInformer(cache.WithApistageContentResolve(hctx),
			httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}})
		if ok {
			rctx := cache.WithL1KeyContext(hctx, k)
			store.ReplaceIfGen(rctx, k, &cache.ResolvedEntry{RawJSON: raw, Inputs: &used}, gen0)
		}
		return nil
	})
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(rctx)

	skipped0 := cache.RefresherStatsByStat()["skipped_no_entry"]
	dropped0 := cache.EnqueueDroppedNonResidentTotal()
	octx := cache.WithL1KeyContext(context.Background(), outerKey)
	gen0 := store.CaptureGen(outerKey)
	armed.Store(true)
	dict := resolve375(octx, stages...)
	armed.Store(false)
	// The #375 race, for real: the churn dirty-marked the (not-yet-resident) outer and the
	// mark was LOST before the stale Put below. Two ways to lose it: since #383 the hook
	// drops a non-resident key's mark at enqueue (enqueue_dropped_non_resident); before
	// #383, or for a mark that slipped past the drop, the refresher consumed it as
	// skipped_no_entry. Either way the outer is stale with no pending mark until the remark.
	deadline := time.Now().Add(5 * time.Second)
	for cache.RefresherStatsByStat()["skipped_no_entry"] == skipped0 && cache.EnqueueDroppedNonResidentTotal() == dropped0 {
		if time.Now().After(deadline) {
			t.Fatalf("setup: the outer's dirty-mark was never lost (neither dropped at enqueue nor consumed as " +
				"skipped_no_entry) — race not driven")
		}
		time.Sleep(2 * time.Millisecond)
	}
	stale := outerBody375(dict, ids...)
	for _, kn := range churns {
		if strings.Contains(string(stale), kn[1]) {
			t.Fatalf("setup: the initial resolve must have served PRE-churn data, got %s", stale)
		}
	}
	if !store.PutIfGen(octx, outerKey, &cache.ResolvedEntry{RawJSON: stale, Inputs: &outerIn}, gen0) {
		t.Fatalf("setup: outer PutIfGen refused")
	}

	// Converge: the outer must end FRESH (every churned object visible).
	var last string
	ok := false
	for end := time.Now().Add(6 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		e, hit := store.Get(outerKey)
		if !hit {
			continue
		}
		last = string(e.RawJSON)
		fresh := true
		for _, kn := range churns {
			fresh = fresh && strings.Contains(last, kn[1])
		}
		if fresh {
			ok = true
			break
		}
	}
	if contentInFlight.Load() == 0 || outerReResolves.Load() == 0 {
		t.Fatalf("setup: interleave not driven (content refreshes in flight=%d, outer re-resolves=%d)",
			contentInFlight.Load(), outerReResolves.Load())
	}
	if !ok {
		t.Fatalf("#375 convergence RED: the outer converged STALE %s after its PUT-THEN-REMARK — the re-resolve "+
			"(dequeued while the content refresh was in flight) HIT the stale content cell; the remark must "+
			"carry every moved GVR so the re-resolve force-misses them (want all of %v)", last, churns)
	}
	// And it STAYS fresh — nothing re-stales it after the content refresh lands.
	time.Sleep(300 * time.Millisecond)
	if e, hit := store.Get(outerKey); hit {
		for _, kn := range churns {
			if !strings.Contains(string(e.RawJSON), kn[1]) {
				t.Fatalf("#375 convergence RED: the outer regressed to stale %s", e.RawJSON)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// (A) at the real content site — the content cell's OWN dep churns in [read, Put]
// on a resolve whose sink does NOT already hold that dep (the content-prewarm shape:
// standalone WithDepGenSink, no L1 key, so api.Resolve records no outer edge-3). The
// content Put must remark the CONTENT key (and the content refresh then converges it
// FRESH). NEUTER (A) (WithContentDepGenSink returns ctx unchanged) → the content Put
// checks the empty prewarm sink → no remark → the cell stays stale → RED.
// ---------------------------------------------------------------------------
func TestIssue375_ContentOwnDepChurn_ContentKeyRemarked_ConvergesFresh(t *testing.T) {
	dyn := newWatcher375(t)
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	cache.ResetRefresherForTest()
	rem := observeRemarks375(t)
	store := cache.ResolvedCache()
	restoreHook := cache.SetUnguardedPutHookForTest(func(k string) { t.Errorf("#375: unguarded (nil-sink) Put of %s on the content-prewarm shape", k) })
	defer restoreHook()
	var armed atomic.Bool
	armChurnSeam(t, dyn, &armed, map[schema.GroupVersionResource][2]string{f1WidgetsGVR: {"Widget", "late-widget"}})
	contentKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))

	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(hctx context.Context, k string, used cache.ResolvedKeyInputs) error {
		gen0 := store.CaptureGen(k)
		path := "/apis/" + used.Group + "/" + used.Version + "/" + used.Resource
		raw, ok := dispatchViaInformer(cache.WithApistageContentResolve(hctx),
			httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: path, Verb: ptr.To(http.MethodGet)}})
		if ok {
			store.ReplaceIfGen(cache.WithL1KeyContext(hctx, k), k, &cache.ResolvedEntry{RawJSON: raw, Inputs: &used}, gen0)
		}
		return nil
	})
	rctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(rctx)

	pctx := cache.WithDepGenSink(context.Background()) // the content-prewarm resolve entry
	armed.Store(true)
	_ = resolve375(pctx, stage375("w", f1WidgetsGVR))
	armed.Store(false)
	if got := rem.get(contentKey); got != 1 {
		t.Fatalf("#375 (A) RED: the content cell's own coordinate churned in [read, Put] but the content Put "+
			"remarked %d times (want 1)", got)
	}
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if e, hit := store.Get(contentKey); hit && strings.Contains(string(e.RawJSON), "late-widget") {
			return
		}
	}
	e, _ := store.Get(contentKey)
	t.Fatalf("#375 (A) RED: the content cell did not converge FRESH: %s", e.RawJSON)
}
