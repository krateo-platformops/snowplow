// apistage_fill_metrics_710_test.go — #710 PR-0: the content-fill counters move
// exactly as specified, through the production reader apistageContentServe.
//
// Arms:
//   - ABSENT: one cold LIST read is one absent fill with the envelope's bytes
//     and item count; a second read is a hit and moves nothing.
//   - REFRESH_FORCED: a refresher re-resolve whose trigger is the cell's GVR.
//   - NOT_SERVABLE: a dispatch the informer cannot serve.
//   - KNOWN_STALE HERD (PR-1's falsifier baseline): one LIST cell made
//     known-stale for real (an informer UPDATE dirty-marks it, its content
//     refresh is not servable across the whole budget, the drop leaves the
//     stamp), then 64 concurrent readers. On main nothing coalesces, so all 64
//     fill and fill_duplicate_total{known_stale} == 63. A barrier in the
//     dispatch seam holds every fill in flight until all 64 have started, so
//     the count is exact, not a race.
//   - PPROF + RA: the dispatch runs under the fill's labels, and a Resolve
//     threads its RESTAction and stage to them.
//   - LOG: one line per interval, with no requester and no object name.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func fill710FSA(i, managed int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": fmt.Sprintf("fsa-%05d", i)},
		"status":     map[string]any{"managed": r1ManagedList(managed)},
	}}
}

// newFill710Watcher is newK547ContentWatcher with n objects in r1Ns, all
// readable (get, list) by r1User.
func newFill710Watcher(t *testing.T, n int) dynamic.Interface {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "fill710-fsa-reader"},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fullstackapps"}, Verbs: []string{"get", "list"}},
			},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: r1Ns, Name: "fill710-fsa-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: r1User}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "fill710-fsa-reader"},
		},
	}
	for i := 0; i < n; i++ {
		seed = append(seed, fill710FSA(i, r1StaleManaged))
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		r1FsaGVR: "FullStackAppList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(r1FsaGVR)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("fsa informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 10*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

func fill710ListCall() httpcall.RequestOptions {
	return httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps",
			Verb: ptr.To(http.MethodGet),
		},
	}
}

func fill710ListKey() string {
	return cache.ComputeKey(contentKeyInputs(r1FsaGVR, r1Ns, ""))
}

// fill710Reset zeroes the fill counters now and after the test.
func fill710Reset(t *testing.T) {
	t.Helper()
	ResetContentFillStatsForTest()
	t.Cleanup(ResetContentFillStatsForTest)
}

// fill710SpyDispatch wraps the real dispatch, recording each envelope's length
// and, when gate is non-nil, holding every dispatch until gate is closed.
func fill710SpyDispatch(t *testing.T, gate <-chan struct{}, arrived *atomic.Int64, onCall func(context.Context)) *atomic.Int64 {
	t.Helper()
	var lastLen atomic.Int64
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		if onCall != nil {
			onCall(ctx)
		}
		if arrived != nil {
			arrived.Add(1)
		}
		if gate != nil {
			<-gate
		}
		raw, ok := orig(ctx, call)
		lastLen.Store(int64(len(raw)))
		return raw, ok
	}
	return &lastLen
}

// fill710MakeKnownStale fills the LIST cell for real, then drives it
// known-stale exactly as #547 F3 does: a real informer UPDATE dirty-marks it,
// its content refresh is not servable across the whole budget, and the drop
// keeps the stamp. The fill counters are reset afterwards.
func fill710MakeKnownStale(t *testing.T, dyn dynamic.Interface, store *cache.ResolvedCacheStore) string {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	cache.ResetRefresherForTest()

	if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatalf("PRECONDITION: the cold LIST read was not served (ok=%v served=%v)", ok, served)
	}
	key := fill710ListKey()
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatalf("PRECONDITION: the cold LIST read stored no content cell")
	}

	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(context.Context, string, cache.ResolvedKeyInputs) error {
		return cache.ErrContentNotServable
	})
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest() })

	d0 := cache.RefresherStatsByStat()["dirty_ended_unfresh_dropped"]
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(0, r1FreshManaged), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	for end := time.Now().Add(30 * time.Second); cache.RefresherStatsByStat()["dirty_ended_unfresh_dropped"] == d0; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("PRECONDITION: the content refresh never reached the drop point")
		}
	}
	if _, _, ok := cache.KnownStale(key); !ok {
		t.Fatalf("PRECONDITION: the LIST cell is not known-stale after the drop")
	}
	ResetContentFillStatsForTest()
	return key
}

func fill710RequireIdle(t *testing.T, s ContentFillStats) {
	t.Helper()
	if s.Inflight != 0 || s.InflightBytes != 0 {
		t.Fatalf("in-flight not released after the fills: inflight=%d inflight_bytes=%d", s.Inflight, s.InflightBytes)
	}
}

func TestContentFill710_AbsentFillCountsBytesAndItemsAndAHitMovesNothing(t *testing.T) {
	const n = 7
	newFill710Watcher(t, n)
	store := cache.ResolvedCache()
	fill710Reset(t)
	envLen := fill710SpyDispatch(t, nil, nil, nil)

	if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatalf("cold read not served (ok=%v served=%v)", ok, served)
	}
	s := ContentFillStatsSnapshot()
	want := uint64(envLen.Load())
	if want == 0 {
		t.Fatalf("PRECONDITION: the dispatch produced no envelope")
	}
	if s.Total["absent"] != 1 || s.Total["known_stale"] != 0 || s.Total["refresh_forced"] != 0 {
		t.Fatalf("fill_total = %v, want absent=1 only", s.Total)
	}
	if s.Duplicate["absent"] != 0 {
		t.Fatalf("fill_duplicate_total = %v, want 0 for a lone fill", s.Duplicate)
	}
	if s.Bytes["absent"] != want {
		t.Fatalf("fill_bytes_total[absent] = %d, want the envelope's %d bytes", s.Bytes["absent"], want)
	}
	if s.InflightPeak != 1 || s.InflightBytesPeak != int64(want) {
		t.Fatalf("peaks = (%d, %d), want (1, %d)", s.InflightPeak, s.InflightBytesPeak, want)
	}
	fill710RequireIdle(t, s)
	g, ok := s.ByGVR[r1FsaGVR.String()]
	if !ok {
		t.Fatalf("by_gvr has no row for %s: %v", r1FsaGVR, s.ByGVR)
	}
	if g != (ContentFillGVRStats{Fills: 1, Bytes: want, Items: n, MaxBytes: want, MaxItems: n}) {
		t.Fatalf("by_gvr[%s] = %+v, want fills=1 bytes=%d items=%d", r1FsaGVR, g, want, n)
	}

	// A second read is a content HIT: no fill.
	if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatalf("second read not served")
	}
	if s2 := ContentFillStatsSnapshot(); s2.Total["absent"] != 1 || s2.ByGVR[r1FsaGVR.String()].Fills != 1 {
		t.Fatalf("a content hit counted as a fill: total=%v by_gvr=%+v", s2.Total, s2.ByGVR)
	}
}

func TestContentFill710_RefreshForcedReason(t *testing.T) {
	newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatalf("cold read not served")
	}
	fill710Reset(t)

	ctx := cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(r1Ctx()), r1FsaGVR)
	if _, _, ok := apistageContentServe(ctx, store, fill710ListCall(), false); !ok {
		t.Fatalf("refresh re-resolve not content-servable")
	}
	s := ContentFillStatsSnapshot()
	if s.Total["refresh_forced"] != 1 || s.Total["absent"] != 0 || s.Total["known_stale"] != 0 {
		t.Fatalf("fill_total = %v, want refresh_forced=1 only (the cell was resident: a force-miss, not a plain miss)", s.Total)
	}
	if s.Bytes["refresh_forced"] == 0 {
		t.Fatalf("fill_bytes_total[refresh_forced] = 0")
	}
	fill710RequireIdle(t, s)
}

func TestContentFill710_NotServableCountsNoBytes(t *testing.T) {
	newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	fill710Reset(t)
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	dispatchViaInformerFn = func(context.Context, httpcall.RequestOptions) ([]byte, bool) { return nil, false }

	if _, _, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); ok {
		t.Fatalf("a not-servable dispatch must return ok=false")
	}
	s := ContentFillStatsSnapshot()
	if s.Total["absent"] != 1 || s.NotServable != 1 || s.Bytes["absent"] != 0 {
		t.Fatalf("total=%v not_servable=%d bytes=%v, want absent=1, not_servable=1, bytes=0", s.Total, s.NotServable, s.Bytes)
	}
	if g := s.ByGVR[r1FsaGVR.String()]; g.Fills != 1 || g.Bytes != 0 || g.Items != 0 {
		t.Fatalf("by_gvr = %+v, want fills=1 and no bytes/items", g)
	}
	fill710RequireIdle(t, s)
}

// TestContentFill710_KnownStaleHerd_64Readers_63Duplicates is PR-1's
// falsifier baseline: on main, 64 concurrent readers of one known-stale LIST
// each run their own fill. PR-1 (single-flight per content key and epoch)
// turns this into 1 fill and 0 duplicates.
func TestContentFill710_KnownStaleHerd_64Readers_63Duplicates(t *testing.T) {
	const readers = 64
	dyn := newFill710Watcher(t, 50)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)

	gate := make(chan struct{})
	var arrived atomic.Int64
	envLen := fill710SpyDispatch(t, gate, &arrived, nil)

	var wg sync.WaitGroup
	errs := make(chan string, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
				errs <- fmt.Sprintf("ok=%v served=%v", ok, served)
			}
		}()
	}
	// Every reader must reach the dispatch before any fill completes; a reader
	// that served from the cell would never arrive.
	for end := time.Now().Add(20 * time.Second); arrived.Load() < readers; time.Sleep(time.Millisecond) {
		if time.Now().After(end) {
			close(gate)
			wg.Wait()
			t.Fatalf("only %d of %d readers entered the fill — the rest did not read the known-stale cell through", arrived.Load(), readers)
		}
	}
	if s := ContentFillStatsSnapshot(); s.Inflight != readers {
		close(gate)
		wg.Wait()
		t.Fatalf("fill_inflight = %d with every reader held in the dispatch, want %d", s.Inflight, readers)
	}
	close(gate)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("a reader was not served: %s", e)
	}

	s := ContentFillStatsSnapshot()
	if s.Total["known_stale"] != readers || s.Total["absent"] != 0 || s.Total["refresh_forced"] != 0 {
		t.Fatalf("fill_total = %v, want known_stale=%d only", s.Total, readers)
	}
	if s.Duplicate["known_stale"] != readers-1 {
		t.Fatalf("fill_duplicate_total[known_stale] = %d, want %d: %d concurrent fills of one content key, one of them first",
			s.Duplicate["known_stale"], readers-1, readers)
	}
	if s.InflightPeak != readers {
		t.Fatalf("fill_inflight_peak = %d, want %d", s.InflightPeak, readers)
	}
	if want := uint64(readers) * uint64(envLen.Load()); s.Bytes["known_stale"] != want {
		t.Fatalf("fill_bytes_total[known_stale] = %d, want %d (%d envelopes of %d bytes)", s.Bytes["known_stale"], want, readers, envLen.Load())
	}
	if g := s.ByGVR[r1FsaGVR.String()]; g.Fills != readers || g.Duplicates != readers-1 {
		t.Fatalf("by_gvr = %+v, want fills=%d duplicates=%d", g, readers, readers-1)
	}
	fill710RequireIdle(t, s)
	t.Logf("#710 baseline: %d readers → fills=%d duplicates=%d inflight_peak=%d inflight_bytes_peak=%d (envelope %d B)",
		readers, s.Total["known_stale"], s.Duplicate["known_stale"], s.InflightPeak, s.InflightBytesPeak, envLen.Load())
}

// TestContentFill710_DispatchCarriesPprofLabelsAndResolveThreadsTheRA drives a
// real Resolve and reads the labels off the ctx the dispatch runs under.
func TestContentFill710_DispatchCarriesPprofLabelsAndResolveThreadsTheRA(t *testing.T) {
	newF1Watcher(t)
	fill710Reset(t)
	got := map[string]string{}
	var mu sync.Mutex
	fill710SpyDispatch(t, nil, nil, func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range []string{"apistage_fill_gvr", "apistage_fill_reason", "apistage_fill_ra"} {
			v, _ := pprof.Label(ctx, k)
			got[k] = v
		}
	})
	f1ResolveAs(t, f1BroadUser, f1ListStage("widgetsfill"))

	want := map[string]string{
		"apistage_fill_gvr":    f1WidgetsGVR.String(),
		"apistage_fill_reason": "absent",
		"apistage_fill_ra":     "default/f1-widget-restaction",
	}
	mu.Lock()
	defer mu.Unlock()
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("pprof label %s = %q, want %q (all: %v)", k, got[k], v, got)
		}
	}
	if s := ContentFillStatsSnapshot(); s.Total["absent"] != 1 {
		t.Fatalf("the Resolve did not go through the fill: %v", s.Total)
	}
}

// TestContentFill710_LogLineIsRateLimitedAndCarriesNoIdentityOrObjectName
// fills twice inside one interval (a LIST and a GET-by-name) and reads the
// default logger.
func TestContentFill710_LogLineIsRateLimitedAndCarriesNoIdentityOrObjectName(t *testing.T) {
	newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	ResetDispatchSummaryForTest()
	fill710Reset(t)
	var buf bytes.Buffer
	var bufMu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&lockedWriter{w: &buf, mu: &bufMu}, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	getCall := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps/fsa-00001",
		Verb: ptr.To(http.MethodGet),
	}}
	ctx := withContentFillOwner(r1Ctx(), "ra-ns", "ra-name", "stage-a")
	for _, call := range []httpcall.RequestOptions{fill710ListCall(), getCall} {
		if _, _, ok := apistageContentServe(ctx, store, call, false); !ok {
			t.Fatalf("read of %s not content-servable", call.Path)
		}
	}

	bufMu.Lock()
	out := buf.String()
	bufMu.Unlock()
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == "apistage.content_fill" {
			lines = append(lines, m)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("got %d apistage.content_fill lines for 2 fills inside one interval, want 1:\n%s", len(lines), out)
	}
	l := lines[0]
	for k, v := range map[string]any{"gvr": r1FsaGVR.String(), "ra": "ra-ns/ra-name", "stage": "stage-a", "reason": "absent", "is_list": true, "duplicate": false} {
		if l[k] != v {
			t.Fatalf("log field %s = %v, want %v: %v", k, l[k], v, l)
		}
	}
	for _, k := range []string{"bytes", "items", "duration_ms", "suppressed"} {
		if _, ok := l[k]; !ok {
			t.Fatalf("log line lacks %s: %v", k, l)
		}
	}
	if strings.Contains(out, r1User) || strings.Contains(out, "fsa-00001") {
		t.Fatalf("the fill log names the requester or the object:\n%s", out)
	}
	if got := contentFillLogSuppressed.Load(); got != 1 {
		t.Fatalf("suppressed = %d after the second fill, want 1", got)
	}
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestContentFill710_ExpvarKeysReadTheLiveCounters reads every published key
// back from expvar after one fill: the harness records the prefix, so a key
// that is missing or detached from its counter would be a silent zero.
func TestContentFill710_ExpvarKeysReadTheLiveCounters(t *testing.T) {
	newFill710Watcher(t, 2)
	store := cache.ResolvedCache()
	fill710Reset(t)
	RegisterContentFillExpvarForTest()
	if _, _, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok {
		t.Fatalf("cold read not content-servable")
	}
	s := ContentFillStatsSnapshot()
	read := func(name string) any {
		v := expvar.Get(name)
		if v == nil {
			t.Fatalf("%s is not published", name)
		}
		var out any
		if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
			t.Fatalf("%s is not JSON: %v", name, err)
		}
		return out
	}
	for _, name := range []string{"snowplow_apistage_content_fill_total", "snowplow_apistage_content_fill_duplicate_total", "snowplow_apistage_content_fill_bytes_total"} {
		m, _ := read(name).(map[string]any)
		if len(m) != int(contentFillReasons) {
			t.Fatalf("%s = %v, want every reason of the closed set", name, m)
		}
	}
	if got := read("snowplow_apistage_content_fill_total").(map[string]any)["absent"]; got != float64(1) {
		t.Fatalf("snowplow_apistage_content_fill_total[absent] = %v, want 1", got)
	}
	bytesAbsent := float64(s.Bytes["absent"])
	if bytesAbsent == 0 {
		t.Fatalf("PRECONDITION: the fill produced no bytes")
	}
	if got := read("snowplow_apistage_content_fill_bytes_total").(map[string]any)["absent"]; got != bytesAbsent {
		t.Fatalf("snowplow_apistage_content_fill_bytes_total[absent] = %v, want %v", got, bytesAbsent)
	}
	for name, want := range map[string]float64{
		"snowplow_apistage_content_fill_not_servable_total":  0,
		"snowplow_apistage_content_fill_inflight":            0,
		"snowplow_apistage_content_fill_inflight_bytes":      0,
		"snowplow_apistage_content_fill_inflight_peak":       1,
		"snowplow_apistage_content_fill_inflight_bytes_peak": bytesAbsent,
	} {
		if got := read(name); got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	row, _ := read("snowplow_apistage_content_fill_by_gvr").(map[string]any)[r1FsaGVR.String()].(map[string]any)
	if row["fills"] != float64(1) || row["items_total"] != float64(2) || row["bytes"] != bytesAbsent {
		t.Fatalf("snowplow_apistage_content_fill_by_gvr[%s] = %v", r1FsaGVR, row)
	}
}
