//go:build paired854

// paired_884_test.go — #884 falsifier 2: the churn harness. Known-stale means
// "the last refresh FAILED" (owner ruling F5), so under steady churn a declined
// cell leaves the state at its first accepted Put and customer reads stop
// starting demand revalidations.
//
// THE DEFECT (bench run 15). At ≥ 10 edits/s a mark lands during every resolve
// of a hot cell. Before #884 a known-stale cell never left the state: each
// accepted Put's resolve was overtaken by a newer mark, so every customer read
// of the cell started a demand revalidation (a full re-resolve, 50K per-user
// refilter included) and its #740 rerun, for as long as the churn lasted.
//
// THE HARNESS is paired_865's production stack (same build tag, same helpers):
// a cache-on ResourceWatcher over a 50K-object namespace, D dependent
// RESTActions LISTing it, real informer writes at PAIRED884_RATE per second,
// the REAL refresher (production settings, customer yield hook) running the
// production refresh closure, and CUSTOMERS as GET /call through
// restActionHandler.ServeHTTP. Two differences:
//
//   - N IDENTITIES (PAIRED884_IDS): each customer goroutine is its own user on
//     its own ClusterRoleBinding, so every (identity, RESTAction) is its own
//     per-identity cell — the demand revalidation runs under that requester;
//   - EXACTLY K ONE-SHOT DECLINES (PAIRED884_K), injected at the RefreshFunc
//     seam: the first K refresher re-resolves of a RESTAction cell during the
//     window return without a Put, exactly as a stage-error decline does
//     ("declining to overwrite good entry"). Each makes one cell known-stale.
//
// THE BOUND comes from the injection, never from the instrument under test: a
// known-stale episode starts ONE demand revalidation (its first read claims the
// key's slot; later reads are absorbed, and an accepted Put earns no rerun). The
// only other attempts are reads that saw the cell known-stale BEFORE its first
// accepted Put and claimed after that attempt released the slot — at most one
// per customer goroutine per episode. So
//
//	demand revalidations ≤ K · (1 + C)        C = customer goroutines
//
// Before #884 the count grows with the run length (every read of a declined cell
// while its slot is free, plus reruns). Precondition: the refresher declined
// exactly K times in the window (no natural decline adds an episode).
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
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

func p884User(i int) string { return fmt.Sprintf("paired-884-u%d", i) }

func TestPaired884KnownStaleChurn(t *testing.T) {
	n := p865EnvInt("PAIRED884_N", 50000)
	rate := p865EnvInt("PAIRED884_RATE", 10)
	secs := p865EnvInt("PAIRED884_SECONDS", 30)
	ids := p865EnvInt("PAIRED884_IDS", 4)
	deps := p865EnvInt("PAIRED884_DEPS", 5)
	k := int64(p865EnvInt("PAIRED884_K", 6))
	think := 50 * time.Millisecond
	if v, err := strconv.Atoi(os.Getenv("PAIRED884_THINK_MS")); err == nil && v >= 0 {
		think = time.Duration(v) * time.Millisecond
	}
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	infoLog := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	prevLog := slog.Default()
	slog.SetDefault(infoLog)
	defer slog.SetDefault(prevLog)

	// --- the cluster: paired_865's, with one binding per identity ---------------
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
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "paired-sa", UID: types.UID("uid-paired-sa")},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: p865AuthnNS}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "paired-reader"}},
	}
	for i := 0; i < ids; i++ {
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: p884User(i), UID: types.UID("uid-" + p884User(i))},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: p884User(i)}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "paired-reader"}})
	}
	ras := map[string]*unstructured.Unstructured{}
	for d := 0; d < deps; d++ {
		ras[p865DepName(d)] = p865RA(p865DepName(d), p865NS, d)
	}
	for _, ra := range ras {
		seed = append(seed, ra.DeepCopy())
	}
	for i := 0; i < n; i++ {
		seed = append(seed, p865Obj(p865NS, i, 0))
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

	// --- each identity's own clientconfig (the UserConfig middleware's output) -
	srv := p865APIServer(t)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	secrets := map[string]*corev1.Secret{}
	for i := 0; i < ids; i++ {
		name := p884User(i) + "-clientconfig"
		secrets[name] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: p865AuthnNS, Name: name},
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-" + p884User(i))}}
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: secrets})
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
	reqCtx := make([]context.Context, ids)
	for i := 0; i < ids; i++ {
		reqCtx[i] = xcontext.BuildContext(context.Background(),
			xcontext.WithLogger(infoLog),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: p884User(i)}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-" + p884User(i)}),
		)
	}
	handler := &restActionHandler{authnNS: p865AuthnNS, saRC: &rest.Config{Host: srv.URL}}
	call := func(id int, name string) (int, time.Duration, []byte) {
		u := "/call?apiVersion=" + url.QueryEscape(h1RAGVR.Group+"/"+h1RAGVR.Version) +
			"&resource=" + h1RAGVR.Resource + "&name=" + name + "&namespace=" + p865NS
		req := httptest.NewRequest(http.MethodGet, u, nil).WithContext(reqCtx[id])
		req.Header.Set("Authorization", "Bearer "+in443JWT())
		rec := httptest.NewRecorder()
		t0 := time.Now()
		handler.ServeHTTP(rec, req)
		return rec.Code, time.Since(t0), rec.Body.Bytes()
	}

	// --- the refresher: production settings, K one-shot declines injected -------
	saEP := &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}
	saRC := &rest.Config{Host: srv.URL}
	var armed atomic.Bool
	var injected, raRefreshes atomic.Int64
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		if armed.Load() && in.CacheEntryClass == "restactions" {
			raRefreshes.Add(1)
			for {
				cur := injected.Load()
				if cur >= k {
					break
				}
				if injected.CompareAndSwap(cur, cur+1) {
					return nil // a one-shot decline: the refresh returns without a Put
				}
			}
		}
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	}
	for _, class := range []string{"restactions", "widgets", cache.CacheEntryClassApistage, cache.CacheEntryClassWidgetContent} {
		cache.RegisterRefreshFunc(class, refresh)
	}
	cache.SetCustomerInflightHook(CustomerInFlight)
	rctx, rcancel := context.WithCancel(xcontext.BuildContext(context.Background(), xcontext.WithLogger(infoLog)))
	cache.StartRefresher(rctx)
	defer func() { rcancel(); cache.ResetRefresherForTest() }()

	// Warm: every identity GETs every dependent once (its own cell, the shared
	// content cell, their dep edges).
	for i := 0; i < ids; i++ {
		for name := range ras {
			if code, _, body := call(i, name); code != http.StatusOK {
				t.Fatalf("warm GET %s as identity %d: %d", name, i, code)
			} else if !json.Valid(body) {
				t.Fatalf("PRECONDITION: warm GET %s returned no JSON", name)
			}
		}
	}
	runtime.GC()

	// --- customers: one closed loop per identity ------------------------------
	var lat p865Samples
	var custErrs atomic.Int64
	var inWindow atomic.Bool
	custStop := make(chan struct{})
	var custWG sync.WaitGroup
	for i := 0; i < ids; i++ {
		custWG.Add(1)
		go func(id int) {
			defer custWG.Done()
			for j := 0; ; j++ {
				select {
				case <-custStop:
					return
				case <-time.After(think):
				}
				code, d, _ := call(id, p865DepName((id*7+j)%deps))
				if code != http.StatusOK {
					custErrs.Add(1)
				}
				if inWindow.Load() {
					lat.add(float64(d.Microseconds()) / 1000)
				}
			}
		}(i)
	}

	// --- measurement window ---------------------------------------------------
	stat := func(name string) int64 { return cache.ResolvedCacheStatsByStat()[name] }
	rstat := func(name string) int64 { return cache.RefresherStatsByStat()[name] }
	demand := func() int64 {
		return stat("known_stale_revalidation_fresh_total") + stat("known_stale_revalidation_failed_total")
	}
	dem0, rerun0 := demand(), stat("known_stale_revalidation_rerun_demand_total")
	ks0, ss0 := stat("known_stale_served_total"), stat("stale_served_total")
	dec0, drop0 := rstat("dirty_ended_unfresh_declined"), rstat("dirty_ended_unfresh_dropped")
	comp0 := rstat("completed")
	cpu0, t0 := p865CPU(), time.Now()
	inWindow.Store(true)
	armed.Store(true)

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
	residentEnd := stat("known_stale_resident")
	dem := demand() - dem0
	reruns := stat("known_stale_revalidation_rerun_demand_total") - rerun0
	declined := rstat("dirty_ended_unfresh_declined") - dec0
	dropped := rstat("dirty_ended_unfresh_dropped") - drop0
	close(custStop)
	custWG.Wait()
	cache.StopRefresher()
	time.Sleep(2 * time.Second) // let re-resolves in flight finish before the deferred cancel

	bound := k * int64(1+ids)
	out := map[string]any{
		"n": n, "rate": rate, "seconds": secs, "events": events, "identities": ids, "deps": deps,
		"k_injected":                     injected.Load(),
		"k_asked":                        k,
		"bound":                          bound,
		"demand_revalidations":           dem,
		"demand_reruns":                  reruns,
		"refresher_declined":             declined,
		"refresher_dropped":              dropped,
		"ra_refreshes_in_window":         raRefreshes.Load(),
		"known_stale_served":             stat("known_stale_served_total") - ks0,
		"stale_served":                   stat("stale_served_total") - ss0,
		"known_stale_resident_at_end":    residentEnd,
		"refresh_completed":              rstat("completed") - comp0,
		"cust":                           p865Quantiles(lat.get()),
		"cust_rps":                       float64(len(lat.get())) / wall.Seconds(),
		"cust_errors":                    custErrs.Load(),
		"cpu_cores":                      (cpu1 - cpu0).Seconds() / wall.Seconds(),
		"wall_s":                         wall.Seconds(),
		"cust_think_ms":                  think.Milliseconds(),
		"gomaxprocs":                     runtime.GOMAXPROCS(0),
		"demand_revalidations_per_k_cap": float64(dem) / float64(max(k, 1)),
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	if p := os.Getenv("PAIRED884_OUT"); p != "" {
		_ = os.WriteFile(p, b, 0o644)
	}
	t.Logf("%s", strings.TrimSpace(string(b)))

	if injected.Load() != k {
		t.Fatalf("PRECONDITION: injected %d declines, want exactly K=%d (too few RESTAction refreshes in the window)", injected.Load(), k)
	}
	if declined+dropped != k {
		t.Fatalf("PRECONDITION: the refresher ended %d windows declined and %d dropped in the window, want exactly K=%d "+
			"(a natural decline would add an episode the bound does not count)", declined, dropped, k)
	}
	if dem > bound {
		t.Fatalf("#884 RED: %d demand revalidations for K=%d injected declines (bound K·(1+C) = %d, C=%d customers) — "+
			"a declined cell never left KNOWN-STALE under churn, so every read kept re-resolving it", dem, k, bound, ids)
	}
	if reruns != 0 {
		t.Fatalf("#884 RED: %d demand reruns — an accepted revalidation Put must not rerun", reruns)
	}
}
