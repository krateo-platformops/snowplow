//go:build paired854

// paired_865_test.go — #854 binding CUSTOMER arm on the REAL refresher (#865
// review). The same file compiles on main and on the branch (no #854 symbol; the
// list-cell stats come through pairedListStats865, a stub on main), so each run
// measures the code under test with an identical workload.
//
// Everything a request touches is production code:
//
//   - a cache-on ResourceWatcher over a fake apiserver: one 50K-object LIST
//     (objects ~1.4 KB) in PAIRED865_NS, 200 objects in PAIRED865_OTHER_NS that
//     no event touches, the RESTAction CRs (informer-served), RBAC;
//   - D=20 dependent RESTActions, each one api stage LISTing the 50K namespace
//     and a dashboard-style filter using `now`; 1 unrelated RESTAction LISTing
//     the 200-object namespace with the same filter;
//   - CUSTOMERS are GET /call requests through restActionHandler.ServeHTTP (the
//     markCustomerInFlight bracket, the real key derivation, L1, the real
//     restactions.Resolve on a miss), closed loop, PAIRED865_THINK_MS apart:
//     PAIRED865_CUST_DEP readers of the dependents, PAIRED865_CUST_UNREL readers
//     of the unrelated RESTAction;
//   - the REFRESHER is cache.StartRefresher with production settings (no env
//     override: 4 workers, the 2 s rate floor), main.go's customer hook
//     (cache.SetCustomerInflightHook(CustomerInFlight)), so its yield is
//     refresher.yieldToCustomer itself, and the production refresh closure
//     (resolveAndPopulateL1) registered for every class RegisterRefreshHandlers
//     registers. The one difference from RegisterRefreshHandlers: the SA
//     endpoint points at this harness's fake apiserver instead of the in-cluster
//     token file;
//   - object events at PAIRED865_RATE per second (real writes through the
//     informer; the dep tracker dirty-marks the dependents);
//   - logging at production's default level (info): main.go hands the
//     refresher a ctx carrying the configured logger, and requests carry it
//     from the middleware (without one, plumbing's xcontext.Logger logs DEBUG).
//
// Reported (JSON to PAIRED865_OUT): customer p50/p95/p99 and req/s per class,
// the STALENESS a customer read of a dependent sees (each dependent's filter
// reports the newest object generation it read; staleness is the time since the
// first write that read does not reflect, 0 when it is current), the
// refresher's own dirty→fresh samples (cache.SetDirtyToFreshRecorder: a window
// is sampled only when it closes fresh, so under a steady event stream it is
// censored — the sample count is reported with it),
// process CPU, refresher completed / yielded / capped / parked, content fills,
// and the list-cell stats on the branch. A CPU profile (PAIRED865_PROF).
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	p865NS       = "paired-ns"
	p865OtherNS  = "paired-other"
	p865User     = "paired-customer"
	p865Deps     = 20
	p865UnrelRA  = "unrelated"
	p865AuthnNS  = "krateo-system"
	p865SAUser   = "system:serviceaccount:krateo-system:snowplow"
	p865Template = `[.list.items[] | {uid: .metadata.uid, name: .metadata.name, namespace: .metadata.namespace,
  created: .metadata.creationTimestamp, kind: .kind, apiVersion: .apiVersion,
  ready: (((.status.conditions // []) | map(select(.type == "Ready")) | first) // {} | .status),
  age: (now - (.metadata.creationTimestamp | fromdateiso8601))}] as $rows
  | {n: ($rows | length), d: %d, gen: ([.list.items[].spec.gen] | max)}`
)

var p865FsaGVR = schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "fullstackapps"}

func p865EnvInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func p865Obj(ns string, i, gen int) *unstructured.Unstructured {
	pad := make([]byte, 700)
	for j := range pad {
		pad[j] = 'a' + byte((i+j)%26)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata": map[string]any{
			"namespace": ns, "name": fmt.Sprintf("fsa-%06d", i), "uid": fmt.Sprintf("uid-%s-%06d", ns, i),
			"creationTimestamp": "2026-10-23T00:00:00Z",
			"labels":            map[string]any{"krateo.io/composition-id": strconv.Itoa(i), "env": []string{"prod", "dev"}[i%2]},
			"annotations":       map[string]any{"krateo.io/pad": string(pad)},
		},
		"spec": map[string]any{"gen": int64(gen), "values": map[string]any{"replicas": int64(i % 3), "image": "ghcr.io/krateo/app:1.0"}},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": []string{"True", "False"}[gen%2], "reason": "Available", "lastTransitionTime": "2026-10-23T00:00:00Z"},
			map[string]any{"type": "Synced", "status": "True", "reason": "ReconcileSuccess"},
		}},
	}}
}

func p865RA(name, ns string, d int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": p865NS},
		"spec": map[string]any{
			"api": []any{map[string]any{"name": "list", "verb": "GET",
				"path": "/apis/" + p865FsaGVR.Group + "/" + p865FsaGVR.Version + "/namespaces/" + ns + "/" + p865FsaGVR.Resource}},
			"filter": fmt.Sprintf(p865Template, d),
		},
	}}
}

func p865DepName(d int) string { return fmt.Sprintf("dep-%02d", d) }

// p865Quantiles returns n and p50/p95/p99/max (ms) of xs.
func p865Quantiles(xs []float64) map[string]float64 {
	if len(xs) == 0 {
		return map[string]float64{"n": 0}
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	q := func(p float64) float64 { return s[max(int(math.Ceil(p*float64(len(s))))-1, 0)] }
	return map[string]float64{"n": float64(len(s)), "p50_ms": q(0.50), "p95_ms": q(0.95), "p99_ms": q(0.99), "max_ms": s[len(s)-1]}
}

type p865Samples struct {
	mu sync.Mutex
	xs []float64
}

func (s *p865Samples) add(ms float64) { s.mu.Lock(); s.xs = append(s.xs, ms); s.mu.Unlock() }
func (s *p865Samples) get() []float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]float64(nil), s.xs...)
}

func p865CPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// p865APIServer is the SA transport's apiserver: discovery for the two groups
// the resolves touch; every object read is served from the informers (an object
// path 404s).
func p865APIServer(t *testing.T) *httptest.Server {
	t.Helper()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	groups := []struct {
		gvr        schema.GroupVersionResource
		kind, sing string
	}{{h1RAGVR, "RESTAction", "restaction"}, {p865FsaGVR, "FullStackApp", "fullstackapp"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api":
			writeJSON(w, &metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
			return
		case "/api/v1":
			writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1"})
			return
		case "/apis":
			var gl []metav1.APIGroup
			for _, g := range groups {
				gvd := metav1.GroupVersionForDiscovery{GroupVersion: g.gvr.Group + "/" + g.gvr.Version, Version: g.gvr.Version}
				gl = append(gl, metav1.APIGroup{Name: g.gvr.Group, Versions: []metav1.GroupVersionForDiscovery{gvd}, PreferredVersion: gvd})
			}
			writeJSON(w, &metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}, Groups: gl})
			return
		}
		for _, g := range groups {
			gv := g.gvr.Group + "/" + g.gvr.Version
			if r.URL.Path == "/apis/"+gv {
				writeJSON(w, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv,
					APIResources: []metav1.APIResource{{Name: g.gvr.Resource, SingularName: g.sing, Namespaced: true, Kind: g.kind,
						Verbs: metav1.Verbs{"get", "list", "watch"}}}})
				return
			}
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestPaired865Customer(t *testing.T) {
	n := p865EnvInt("PAIRED865_N", 50000)
	rate := p865EnvInt("PAIRED865_RATE", 10)
	secs := p865EnvInt("PAIRED865_SECONDS", 30)
	depN := p865EnvInt("PAIRED865_CUST_DEP", 2)
	unrelN := p865EnvInt("PAIRED865_CUST_UNREL", 4)
	// 0 is a valid think time: the sustained arm keeps a customer request in
	// flight at all times, so the refresher's yield runs to its 5 s cap.
	think := 50 * time.Millisecond
	if v, err := strconv.Atoi(os.Getenv("PAIRED865_THINK_MS")); err == nil && v >= 0 {
		think = time.Duration(v) * time.Millisecond
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	level := slog.LevelInfo
	if os.Getenv("PAIRED865_DEBUG") != "" { // diagnosis only, never in a measured run
		level = slog.LevelDebug
	}
	infoLog := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	prevLog := slog.Default()
	slog.SetDefault(infoLog)
	defer slog.SetDefault(prevLog)

	// --- the cluster ---------------------------------------------------------
	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	scheme := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", p865FsaGVR: "FullStackAppList",
		crbGVR: "ClusterRoleBindingList", crGVR: "ClusterRoleList", rbGVR: "RoleBindingList", rGVR: "RoleList",
	}
	seed := []k8sruntime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "paired-reader"}, Rules: []rbacv1.PolicyRule{{
			Verbs: []string{"get", "list"}, APIGroups: []string{h1RAGVR.Group, p865FsaGVR.Group},
			Resources: []string{h1RAGVR.Resource, p865FsaGVR.Resource},
		}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "paired-customer", UID: types.UID("uid-paired-customer")},
			Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: p865User}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "paired-reader"}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "paired-sa", UID: types.UID("uid-paired-sa")},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: p865AuthnNS}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "paired-reader"}},
	}
	ras := map[string]*unstructured.Unstructured{}
	for d := 0; d < p865Deps; d++ {
		ras[p865DepName(d)] = p865RA(p865DepName(d), p865NS, d)
	}
	ras[p865UnrelRA] = p865RA(p865UnrelRA, p865OtherNS, 99)
	for _, ra := range ras {
		seed = append(seed, ra.DeepCopy())
	}
	for i := 0; i < n; i++ {
		seed = append(seed, p865Obj(p865NS, i, 0))
	}
	for i := 0; i < 200; i++ {
		seed = append(seed, p865Obj(p865OtherNS, i, 0))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	seed = nil
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Stop()
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, p865FsaGVR} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(5 * time.Minute):
			t.Fatalf("informer %s did not sync", gvr)
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 5*time.Minute)
	if err := rw.WaitForCacheSync(sctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	scancel()
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prevGlobal := cache.Global()
	cache.SetGlobal(rw)
	defer cache.SetGlobal(prevGlobal)
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, p865FsaGVR} {
		for end := time.Now().Add(time.Minute); !rw.IsServable(gvr); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("%s never servable", gvr)
			}
		}
	}

	// --- the customer's own clientconfig (the UserConfig middleware's output) -
	srv := p865APIServer(t)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		p865User + "-clientconfig": {ObjectMeta: metav1.ObjectMeta{Namespace: p865AuthnNS, Name: p865User + "-clientconfig"},
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-" + p865User)}},
	}})
	cache.ForceSecretsCacheReadyForTest(p865AuthnNS)
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()
	restoreFetch := setFetchObjectForTest(func(req *http.Request) objects.Result {
		ra := ras[req.URL.Query().Get("name")]
		if ra == nil {
			ra = ras[p865DepName(0)] // never reached: the harness only asks for its own names
		}
		return objects.Result{GVR: h1RAGVR, Unstructured: ra.DeepCopy()}
	})
	defer restoreFetch()
	reqCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithLogger(infoLog),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: p865User}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-" + p865User}),
	)
	handler := &restActionHandler{authnNS: p865AuthnNS, saRC: &rest.Config{Host: srv.URL}}
	call := func(name string) (int, time.Duration, []byte) {
		u := "/call?apiVersion=" + url.QueryEscape(h1RAGVR.Group+"/"+h1RAGVR.Version) +
			"&resource=" + h1RAGVR.Resource + "&name=" + name + "&namespace=" + p865NS
		req := httptest.NewRequest(http.MethodGet, u, nil).WithContext(reqCtx)
		req.Header.Set("Authorization", "Bearer "+in443JWT())
		rec := httptest.NewRecorder()
		t0 := time.Now()
		handler.ServeHTTP(rec, req)
		return rec.Code, time.Since(t0), rec.Body.Bytes()
	}
	// writeAt[g] is when the write carrying spec.gen == g was issued (g ≥ 1).
	var writeMu sync.Mutex
	writeAt := []time.Time{{}}
	servedGen := func(body []byte) (int, bool) {
		var ra struct {
			Status struct {
				Gen *float64 `json:"gen"`
			} `json:"status"`
		}
		if json.Unmarshal(body, &ra) != nil || ra.Status.Gen == nil {
			return 0, false
		}
		return int(*ra.Status.Gen), true
	}

	// --- the refresher: production settings and wiring -------------------------
	saEP := &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}
	saRC := &rest.Config{Host: srv.URL}
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	}
	for _, class := range []string{"restactions", "widgets", cache.CacheEntryClassApistage, cache.CacheEntryClassWidgetContent} {
		cache.RegisterRefreshFunc(class, refresh)
	}
	cache.SetCustomerInflightHook(CustomerInFlight)
	var d2f p865Samples
	var inWindow atomic.Bool
	cache.SetDirtyToFreshRecorder(func(ms int64) {
		if inWindow.Load() {
			d2f.add(float64(ms))
		}
	})
	defer cache.SetDirtyToFreshRecorder(nil)
	rctx, rcancel := context.WithCancel(xcontext.BuildContext(context.Background(), xcontext.WithLogger(infoLog)))
	cache.StartRefresher(rctx)
	defer func() { rcancel(); cache.ResetRefresherForTest() }()

	// Warm: one customer GET of every RESTAction fills its L1 cell, its content
	// cell and their dep edges.
	for name := range ras {
		if code, _, body := call(name); code != http.StatusOK {
			t.Fatalf("warm GET %s: %d", name, code)
		} else if _, ok := servedGen(body); !ok {
			t.Fatalf("PRECONDITION: GET %s carries no status.gen (body %d bytes)", name, len(body))
		}
	}
	runtime.GC()

	// --- customers ------------------------------------------------------------
	var depLat, unrelLat, staleness p865Samples
	var custErrs atomic.Int64
	custStop := make(chan struct{})
	var custWG sync.WaitGroup
	customer := func(class string, pick func(i int) string, lat *p865Samples) {
		defer custWG.Done()
		for i := 0; ; i++ {
			select {
			case <-custStop:
				return
			case <-time.After(think):
			}
			var code int
			var d time.Duration
			var body []byte
			writeMu.Lock()
			latest := len(writeAt) - 1 // every write issued before this read started
			writeMu.Unlock()
			start := time.Now()
			pprof.Do(context.Background(), pprof.Labels("cust", class), func(context.Context) { code, d, body = call(pick(i)) })
			if code != http.StatusOK {
				custErrs.Add(1)
			}
			if inWindow.Load() {
				lat.add(float64(d.Microseconds()) / 1000)
				if class == "dep" {
					if g, ok := servedGen(body); ok {
						stale := 0.0
						if g < latest {
							writeMu.Lock()
							stale = float64(start.Sub(writeAt[g+1]).Microseconds()) / 1000
							writeMu.Unlock()
						}
						staleness.add(stale)
					}
				}
			}
		}
	}
	for c := 0; c < depN; c++ {
		custWG.Add(1)
		c := c
		go customer("dep", func(i int) string { return p865DepName((c*7 + i) % p865Deps) }, &depLat)
	}
	for c := 0; c < unrelN; c++ {
		custWG.Add(1)
		go customer("unrel", func(int) string { return p865UnrelRA }, &unrelLat)
	}

	// --- measurement window -----------------------------------------------------
	var prof *os.File
	if p := os.Getenv("PAIRED865_PROF"); p != "" {
		prof, _ = os.Create(p)
		_ = pprof.StartCPUProfile(prof)
	}
	var peak atomic.Uint64
	sampStop := make(chan struct{})
	go func() {
		tk := time.NewTicker(50 * time.Millisecond)
		defer tk.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-sampStop:
				return
			case <-tk.C:
				runtime.ReadMemStats(&ms)
				for cur := peak.Load(); ms.HeapInuse > cur && !peak.CompareAndSwap(cur, ms.HeapInuse); cur = peak.Load() {
				}
			}
		}
	}()
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	fills0 := restactionsapi.ContentFillStatsSnapshot().Total
	st0 := cache.RefresherStatsByStat()
	cpu0, t0 := p865CPU(), time.Now()
	inWindow.Store(true)

	res := dyn.Resource(p865FsaGVR).Namespace(p865NS)
	events := 0
	tick := time.NewTicker(time.Second / time.Duration(rate))
	end := time.After(time.Duration(secs) * time.Second)
loop:
	for {
		select {
		case <-end:
			break loop
		case <-tick.C:
			i := (events * 7919) % n
			writeMu.Lock()
			writeAt = append(writeAt, time.Now())
			writeMu.Unlock()
			if _, err := res.Update(context.Background(), p865Obj(p865NS, i, events+1), metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			events++
		}
	}
	tick.Stop()
	inWindow.Store(false)
	wall := time.Since(t0)
	cpu1 := p865CPU()
	runtime.ReadMemStats(&ms1)
	st1 := cache.RefresherStatsByStat()
	fills1 := restactionsapi.ContentFillStatsSnapshot().Total
	close(sampStop)
	if prof != nil {
		pprof.StopCPUProfile()
		_ = prof.Close()
	}
	close(custStop)
	custWG.Wait()
	// Teardown, outside the window: stop the refresher's queues and let the
	// re-resolves in flight finish before the deferred cancel (a cancel under a
	// running re-resolve aborts its stages and logs a refresh failure).
	cache.StopRefresher()
	time.Sleep(2 * time.Second)

	var fills uint64
	for k, v := range fills1 {
		fills += v - fills0[k]
	}
	stat := func(k string) int64 { return st1[k] - st0[k] }
	dep, unrel := depLat.get(), unrelLat.get()
	out := map[string]any{
		"n": n, "rate": rate, "seconds": secs, "events": events,
		"gomaxprocs":         runtime.GOMAXPROCS(0),
		"wall_s":             wall.Seconds(),
		"cpu_cores":          (cpu1 - cpu0).Seconds() / wall.Seconds(),
		"alloc_bytes":        ms1.TotalAlloc - ms0.TotalAlloc,
		"peak_heap_inuse":    peak.Load(),
		"fills":              fills,
		"cust_dep":           p865Quantiles(dep),
		"cust_dep_staleness": p865Quantiles(staleness.get()),
		"cust_unrel":         p865Quantiles(unrel),
		"cust_dep_rps":       float64(len(dep)) / wall.Seconds(),
		"cust_unrel_rps":     float64(len(unrel)) / wall.Seconds(),
		"cust_errors":        custErrs.Load(),
		"cust_dep_n":         depN,
		"cust_unrel_n":       unrelN,
		"cust_think_ms":      think.Milliseconds(),
		"dirty_to_fresh":     p865Quantiles(d2f.get()),
		"refresh_completed":  stat("completed"),
		"refresh_yielded":    stat("yielded"),
		"refresh_capped":     stat("capped"),
		"refresh_floored":    stat("floored"),
		"refresh_parked_s":   float64(stat("parked_ms")) / 1000,
		"completions_s":      float64(stat("completed")) / wall.Seconds(),
		"num_gc":             ms1.NumGC - ms0.NumGC,
	}
	if st := pairedListStats865(); st != nil {
		out["list_incremental"] = st
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	if p := os.Getenv("PAIRED865_OUT"); p != "" {
		_ = os.WriteFile(p, b, 0o644)
	}
	t.Logf("%s", strings.TrimSpace(string(b)))
}
