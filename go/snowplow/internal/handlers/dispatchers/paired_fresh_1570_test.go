//go:build paired854 && pairedfresh

// paired_fresh_1570_test.go — bench run 30's freshness journey, in process, on
// the REAL refresher with the real customer yield (#1570 rate floor, #1571
// customer admission). The same file compiles on main and on both branches; the
// only tree-dependent line is the customer-signal wiring, which is main.go's own
// in each tree (pairedWireCustomerSignal, paired_wire_test.go).
//
// Everything a request touches is production code, as in paired_865_test.go:
// a cache-on ResourceWatcher over a fake apiserver, restActionHandler.ServeHTTP
// for every customer and probe read (the markCustomerInFlight bracket, the real
// key, L1, restactions.Resolve on a miss), cache.StartRefresher with production
// settings and the production refresh closure, and a real /refreshes
// subscription (cache.SubscribeRefreshStream) on the probe's cell.
//
// The shape is run 30's:
//   - CHURN: PFRESH_NS namespaces × 20 compositions, written round-robin at
//     1, 10 and 50 per second (one phase each). The probe's namespace holds
//     list positions 0..19 of every cycle, so it receives bursts of 20 writes,
//     as the bench's fresh606 churn did.
//   - THE PROBE: a viewed cell (an armed /refreshes subscription) whose
//     RESTAction lists the probe namespace (PFRESH_PROBE_ITEMS items) and
//     projects each item's marker annotation. Each sample writes a new marker on
//     the probe object; push_ms is that write's return to the first refresh
//     frame for the cell on the subscription (the floor-free number; the bench
//     adds its 100 ms poll on top). The tab then refetches the cell, as the SPA
//     does, and the refetch must carry the marker (correctness).
//   - CUSTOMERS: PFRESH_FAST closed-loop readers of small list cells, think
//     PFRESH_THINK_MS (L1 hits: 145–185 requests/s together), plus one COLD
//     reader issuing a never-cached read (unique extras) of a PFRESH_BIG-object
//     list with a dashboard filter at PFRESH_COLD_PER_S per second: the
//     multi-second tail that keeps a customer request in flight.
//
// Reported per phase (JSON to PFRESH_OUT): probe push p50/p95/max and samples,
// the viewed tier's own refresh-cycle wait histogram (share ≤ 100 ms), the
// refresher's yielded / capped / parked / floored / completed, customer
// requests/s and p50/p95 per class.
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
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
	pfRANS       = "pfresh-ras"
	pfProbeNS    = "pfresh-01"
	pfBigNS      = "pfresh-big"
	pfProbeObj   = "probe"
	pfMarkerAnno = "bench.krateo.io/fresh"
	pfPerNS      = 20
)

func pfNS(i int) string { return fmt.Sprintf("pfresh-%02d", i) }

// pfObj is one composition; marker goes in the probe annotation.
func pfObj(ns, name, marker string, gen int) *unstructured.Unstructured {
	o := p865Obj(ns, 0, gen)
	md := o.Object["metadata"].(map[string]any)
	md["name"] = name
	md["uid"] = "uid-" + ns + "-" + name
	md["annotations"].(map[string]any)[pfMarkerAnno] = marker
	return o
}

func pfRA(name, ns, filter string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": pfRANS},
		"spec": map[string]any{
			"api": []any{map[string]any{"name": "list", "verb": "GET",
				"path": "/apis/" + p865FsaGVR.Group + "/" + p865FsaGVR.Version + "/namespaces/" + ns + "/" + p865FsaGVR.Resource}},
			"filter": filter,
		},
	}}
}

// pfHist is a refresh-cycle histogram delta: count and the share at or below
// each bound.
func pfHist(a, b cache.RefreshCycleHist) map[string]any {
	n := b.Count - a.Count
	out := map[string]any{"n": n}
	if n == 0 || len(b.Buckets) != len(a.Buckets) {
		return out
	}
	for i, bound := range cache.RefreshCycleBoundsMS {
		if bound == 100 || bound == 1000 || bound == 2000 || bound == 5000 {
			out["share_le_"+strconv.FormatInt(bound, 10)] = float64(b.Buckets[i]-a.Buckets[i]) / float64(n)
		}
	}
	out["mean_ms"] = float64(b.SumMS-a.SumMS) / float64(n)
	return out
}

func TestPairedFresh1570(t *testing.T) {
	nsN := p865EnvInt("PFRESH_NS", 50)
	probeItems := p865EnvInt("PFRESH_PROBE_ITEMS", 1000)
	bigN := p865EnvInt("PFRESH_BIG", 50000)
	samples := p865EnvInt("PFRESH_SAMPLES", 20)
	gapMS := p865EnvInt("PFRESH_GAP_MS", 3000)
	fastN := p865EnvInt("PFRESH_FAST", 8)
	thinkMS := p865EnvInt("PFRESH_THINK_MS", 40)
	coldMilli := p865EnvInt("PFRESH_COLD_MILLI_PER_S", 1000) // cold reads per second, ×1000
	seed := int64(p865EnvInt("PFRESH_SEED", 1))
	rates := []int{1, 10, 50}
	if v := os.Getenv("PFRESH_RATES"); v != "" {
		rates = nil
		for _, f := range strings.Split(v, ",") {
			if n, err := strconv.Atoi(f); err == nil && n > 0 {
				rates = append(rates, n)
			}
		}
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	cache.ResetRefreshCycleForTest()
	infoLog := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	prevLog := slog.Default()
	slog.SetDefault(infoLog)
	defer slog.SetDefault(prevLog)

	// --- the cluster -----------------------------------------------------------
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
	seedObjs := []k8sruntime.Object{
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
	// Churn targets: namespace-major, the probe namespace first (positions 0..19).
	type target struct{ ns, name string }
	var targets []target
	for n := 1; n <= nsN; n++ {
		for k := 0; k < pfPerNS; k++ {
			name := fmt.Sprintf("app-%02d", k)
			targets = append(targets, target{pfNS(n), name})
			seedObjs = append(seedObjs, pfObj(pfNS(n), name, "", 0))
		}
	}
	for i := pfPerNS; i < probeItems-1; i++ { // the rest of the probe namespace
		seedObjs = append(seedObjs, pfObj(pfProbeNS, fmt.Sprintf("fill-%04d", i), "", 0))
	}
	seedObjs = append(seedObjs, pfObj(pfProbeNS, pfProbeObj, "m-0", 0))
	for i := 0; i < bigN; i++ {
		seedObjs = append(seedObjs, p865Obj(pfBigNS, i, 0))
	}
	probeFilter := `[.list.items[] | {name: .metadata.name, m: (.metadata.annotations["` + pfMarkerAnno + `"] // "")}]`
	smallFilter := `{n: (.list.items | length), names: [.list.items[].metadata.name]}`
	ras := map[string]*unstructured.Unstructured{
		"probe": pfRA("probe", pfProbeNS, probeFilter),
		"cold":  pfRA("cold", pfBigNS, fmt.Sprintf(p865Template, 0)),
	}
	hitNames := []string{}
	for i := 0; i < 10 && i+2 <= nsN; i++ {
		name := fmt.Sprintf("hit-%02d", i)
		ras[name] = pfRA(name, pfNS(i+2), smallFilter)
		hitNames = append(hitNames, name)
	}
	for _, ra := range ras {
		seedObjs = append(seedObjs, ra.DeepCopy())
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seedObjs...)
	seedObjs = nil
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

	// --- identity, handler -------------------------------------------------------
	srv := p865APIServer(t)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		p865User + "-clientconfig": {ObjectMeta: authnMeta(p865AuthnNS, p865User+"-clientconfig", p865User),
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-" + p865User)}},
	}})
	cache.ForceSecretsCacheReadyForTest(p865AuthnNS)
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()
	restoreFetch := setFetchObjectForTest(func(req *http.Request) objects.Result {
		ra := ras[req.URL.Query().Get("name")]
		if ra == nil {
			ra = ras["probe"]
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
	call := func(name, extras string) (int, time.Duration, []byte, http.Header) {
		u := "/call?apiVersion=" + url.QueryEscape(h1RAGVR.Group+"/"+h1RAGVR.Version) +
			"&resource=" + h1RAGVR.Resource + "&name=" + name + "&namespace=" + pfRANS
		if extras != "" {
			u += "&extras=" + url.QueryEscape(extras)
		}
		req := httptest.NewRequest(http.MethodGet, u, nil).WithContext(reqCtx)
		req.Header.Set("Authorization", "Bearer "+in443JWT())
		rec := httptest.NewRecorder()
		t0 := time.Now()
		handler.ServeHTTP(rec, req)
		return rec.Code, time.Since(t0), rec.Body.Bytes(), rec.Header()
	}

	// --- the refresher: production settings and wiring ---------------------------
	saEP := &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}
	saRC := &rest.Config{Host: srv.URL}
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	}
	for _, class := range []string{"restactions", "widgets", cache.CacheEntryClassApistage, cache.CacheEntryClassWidgetContent} {
		cache.RegisterRefreshFunc(class, refresh)
	}
	pairedWireCustomerSignal()
	rctx, rcancel := context.WithCancel(xcontext.BuildContext(context.Background(), xcontext.WithLogger(infoLog)))
	cache.StartRefresher(rctx)
	defer func() { rcancel(); cache.ResetRefresherForTest() }()

	// Warm every cell; the probe's response carries its refresh key.
	var probeKey string
	for name := range ras {
		code, _, body, hdr := call(name, "")
		if code != http.StatusOK {
			t.Fatalf("warm GET %s: %d %s", name, code, body)
		}
		if name == "probe" {
			probeKey = hdr.Get(refreshKeyHeader)
		}
	}
	if probeKey == "" {
		t.Fatal("PRECONDITION: the probe's /call carries no X-Snowplow-Refresh-Key")
	}
	st := cache.SubscribeRefreshStream(map[string]struct{}{probeKey: {}})
	defer st.Unsub()
	if !cache.HasRefreshSubscriber(probeKey) {
		t.Fatal("PRECONDITION: the probe cell is not armed")
	}
	var frameMu sync.Mutex
	var frames []time.Time
	go func() {
		for k := range st.Keys {
			now := time.Now()
			if k == probeKey {
				frameMu.Lock()
				frames = append(frames, now)
				frameMu.Unlock()
				st.Wrote(k)
			}
		}
	}()
	frameAfter := func(t0 time.Time) (time.Time, bool) {
		frameMu.Lock()
		defer frameMu.Unlock()
		for _, f := range frames {
			if f.After(t0) {
				return f, true
			}
		}
		return time.Time{}, false
	}
	runtime.GC()

	// --- customers -----------------------------------------------------------------
	var fastLat, coldLat p865Samples
	var fastCnt, coldCnt, custErrs atomic.Int64
	var measuring atomic.Bool
	custStop := make(chan struct{})
	var custWG sync.WaitGroup
	for c := 0; c < fastN; c++ {
		custWG.Add(1)
		go func(c int) {
			defer custWG.Done()
			for i := 0; ; i++ {
				select {
				case <-custStop:
					return
				case <-time.After(time.Duration(thinkMS) * time.Millisecond):
				}
				code, d, _, _ := call(hitNames[(c*3+i)%len(hitNames)], "")
				if code != http.StatusOK {
					custErrs.Add(1)
				}
				if measuring.Load() {
					fastLat.add(float64(d.Microseconds()) / 1000)
					fastCnt.Add(1)
				}
			}
		}(c)
	}
	rnd := rand.New(rand.NewSource(seed))
	var rndMu sync.Mutex
	custWG.Add(1)
	go func() { // the cold reader: Poisson arrivals, each a never-cached read
		defer custWG.Done()
		var seq atomic.Int64
		for {
			rndMu.Lock()
			wait := time.Duration(rnd.ExpFloat64() / (float64(coldMilli) / 1000) * float64(time.Second))
			rndMu.Unlock()
			select {
			case <-custStop:
				return
			case <-time.After(wait):
			}
			go func(i int64) {
				code, d, _, _ := call("cold", fmt.Sprintf(`{"q":%d}`, i))
				if code != http.StatusOK {
					custErrs.Add(1)
				}
				if measuring.Load() {
					coldLat.add(float64(d.Microseconds()) / 1000)
					coldCnt.Add(1)
				}
			}(seq.Add(1))
		}
	}()
	time.Sleep(3 * time.Second) // customers reach steady state

	// --- phases ----------------------------------------------------------------------
	res := dyn.Resource(p865FsaGVR)
	write := func(ns, name, marker string, gen int) error {
		_, err := res.Namespace(ns).Update(context.Background(), pfObj(ns, name, marker, gen), metav1.UpdateOptions{})
		return err
	}
	type phaseOut map[string]any
	var phases []phaseOut
	gen := 1
	for _, rate := range rates {
		// churn: round-robin over the targets at `rate` per second
		churnStop := make(chan struct{})
		var churnWG sync.WaitGroup
		var churnWrites atomic.Int64
		churnWG.Add(1)
		go func() {
			defer churnWG.Done()
			t0 := time.Now()
			for i := 0; ; i++ {
				due := t0.Add(time.Duration(i) * time.Second / time.Duration(rate))
				select {
				case <-churnStop:
					return
				case <-time.After(time.Until(due)):
				}
				tg := targets[i%len(targets)]
				if err := write(tg.ns, tg.name, fmt.Sprintf("churn-%d", i), i+1); err == nil {
					churnWrites.Add(1)
				}
			}
		}()
		cyc0 := cache.RefreshCycleStatsSnapshot()
		st0 := cache.RefresherStatsByStat()
		fast0, cold0 := fastCnt.Load(), coldCnt.Load()
		fastLat0, coldLat0 := len(fastLat.get()), len(coldLat.get())
		measuring.Store(true)
		p0 := time.Now()
		var push []float64
		var unconverged, stale int
		for s := 0; s < samples; s++ {
			gen++
			marker := fmt.Sprintf("m-%d-%d-%d", rate, s, gen)
			if err := write(pfProbeNS, pfProbeObj, marker, gen); err != nil {
				t.Fatalf("probe write: %v", err)
			}
			w := time.Now()
			var f time.Time
			ok := false
			for end := w.Add(15 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
				if f, ok = frameAfter(w); ok {
					break
				}
			}
			if !ok {
				unconverged++
			} else {
				push = append(push, float64(f.Sub(w).Microseconds())/1000)
				// the tab's refetch on the frame (a customer /call, as the SPA's)
				if code, _, body, _ := call("probe", ""); code != http.StatusOK || !strings.Contains(string(body), marker) {
					stale++
				}
			}
			time.Sleep(time.Duration(gapMS) * time.Millisecond)
		}
		wall := time.Since(p0)
		measuring.Store(false)
		close(churnStop)
		churnWG.Wait()
		cyc1 := cache.RefreshCycleStatsSnapshot()
		st1 := cache.RefresherStatsByStat()
		stat := func(k string) int64 { return st1[k] - st0[k] }
		fl, cl := fastLat.get()[fastLat0:], coldLat.get()[coldLat0:]
		phases = append(phases, phaseOut{
			"rate": rate, "samples": samples, "unconverged": unconverged, "refetch_without_marker": stale,
			"push": p865Quantiles(push), "push_samples_ms": push, "wall_s": wall.Seconds(),
			"churn_writes_per_s":  float64(churnWrites.Load()) / wall.Seconds(),
			"viewed_wait":         pfHist(cyc0.Wait["viewed"], cyc1.Wait["viewed"]),
			"normal_wait":         pfHist(cyc0.Wait["normal"], cyc1.Wait["normal"]),
			"viewed_duration":     pfHist(cyc0.Duration["viewed"], cyc1.Duration["viewed"]),
			"viewed_put_to_frame": pfHist(cyc0.PutToFrame["viewed"], cyc1.PutToFrame["viewed"]),
			"refresh_completed":   stat("completed"),
			"completions_s":       float64(stat("completed")) / wall.Seconds(),
			"refresh_yielded":     stat("yielded"),
			"refresh_capped":      stat("capped"),
			"refresh_parked_s":    float64(stat("parked_ms")) / 1000,
			"refresh_floored":     stat("floored"),
			"viewed_completed":    stat("viewed_completed"),
			"cust_fast":           p865Quantiles(fl),
			"cust_cold":           p865Quantiles(cl),
			"cust_rps":            float64(fastCnt.Load()-fast0+coldCnt.Load()-cold0) / wall.Seconds(),
			"cust_cold_rps":       float64(coldCnt.Load()-cold0) / wall.Seconds(),
		})
		t.Logf("phase @%d: push %v viewed_wait %v yielded=%d capped=%d floored=%d", rate, p865Quantiles(push),
			pfHist(cyc0.Wait["viewed"], cyc1.Wait["viewed"]), stat("yielded"), stat("capped"), stat("floored"))
	}
	close(custStop)
	custWG.Wait()
	cache.StopRefresher()
	time.Sleep(2 * time.Second)

	sort.Slice(phases, func(i, j int) bool { return phases[i]["rate"].(int) < phases[j]["rate"].(int) })
	out := map[string]any{
		"gomaxprocs": runtime.GOMAXPROCS(0), "ns": nsN, "probe_items": probeItems, "big": bigN,
		"fast_customers": fastN, "think_ms": thinkMS, "cold_per_s": float64(coldMilli) / 1000,
		"gap_ms": gapMS, "seed": seed, "cust_errors": custErrs.Load(), "phases": phases,
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	if p := os.Getenv("PFRESH_OUT"); p != "" {
		_ = os.WriteFile(p, b, 0o644)
	}
	t.Logf("%s", strings.TrimSpace(string(b)))
}
