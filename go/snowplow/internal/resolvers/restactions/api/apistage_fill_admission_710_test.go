// apistage_fill_admission_710_test.go — #710 PR-2a: the content-fill memory
// admission gate.
//
//   - F-ADM1 BOUND: with a seamed GOMEMLIMIT / live heap, N fills admitted
//     concurrently never hold more estimated weight than the ceiling, and all N
//     complete (the inFlight == 0 escape guarantees progress even for a fill
//     heavier than the ceiling). Mutation "no gate" → RED. A real-path twin
//     drives N distinct lists through apistageContentServe.
//   - F-ADM2 CALIBRATION: after a GVR's first measured fill its weight tracks
//     the real decode expansion within 2× (the seed is replaced, not averaged).
//   - PRIORITY: a waiting customer fill admits before a waiting background
//     fill, including on the empty-gate escape.
//   - D1: a fill not admitted before its deadline errors every reader of its
//     flight with ErrContentFillAdmissionTimeout and never falls through.
package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"net/http"
)

// fill710Admission resets the gate and its counters now and after the test.
func fill710Admission(t *testing.T) {
	t.Helper()
	ResetContentFillStatsForTest()
	t.Cleanup(ResetContentFillStatsForTest)
	t.Cleanup(cache.ResetAdmissionRuntimeSeamsForTest)
}

// fill710SeamCeiling makes AdmissionCeiling() return exactly ceiling (live heap
// constant): limit L, live 0, reserve L/8 → ceiling = L − L/8.
func fill710SeamCeiling(t *testing.T, ceiling int64) {
	t.Helper()
	limit := ceiling * 8 / 7
	for limit-limit/8 < ceiling {
		limit++
	}
	restore := cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return limit }, func() int64 { return 0 })
	t.Cleanup(restore)
	if got, unlimited := cache.AdmissionCeiling(); unlimited || got < ceiling || got > ceiling+8 {
		t.Fatalf("PRECONDITION: seamed ceiling %d (unlimited=%v), want %d", got, unlimited, ceiling)
	}
}

func TestContentFill710_ADM1_ConcurrentWeightNeverExceedsCeiling_AllComplete(t *testing.T) {
	fill710Admission(t)
	const (
		raw   = int64(1000)
		fills = 12
	)
	weight := int64(float64(raw) * contentFillExpansionSeed)
	ceiling := 3*weight + weight/2 // three fit, a fourth does not
	fill710SeamCeiling(t, ceiling)

	var peak atomic.Int64
	var held atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < fills; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			release, err := admitContentFill(context.Background(), "adm1.example/v1, Resource=things", raw, fillClassCustomer)
			if err != nil {
				t.Errorf("fill %d not admitted: %v", i, err)
				return
			}
			w, _ := contentFillGateStateForTest()
			storeMax(&peak, w)
			held.Add(1)
			time.Sleep(5 * time.Millisecond) // hold the admission like a decode would
			release()
		}(i)
	}
	wg.Wait()
	if got := held.Load(); got != fills {
		t.Fatalf("F-ADM1: %d of %d fills completed", got, fills)
	}
	if peak.Load() > ceiling {
		t.Fatalf("F-ADM1 RED: in-flight estimated weight peaked at %d over the ceiling %d (weight %d per fill) — the gate admitted past the shared AdmissionCeiling",
			peak.Load(), ceiling, weight)
	}
	if peak.Load() < 2*weight {
		t.Fatalf("PRECONDITION: fills never overlapped (peak %d, weight %d) — the arm proves nothing", peak.Load(), weight)
	}
	if w, n := contentFillGateStateForTest(); w != 0 || n != 0 {
		t.Fatalf("gate not drained: weight %d count %d", w, n)
	}

	// The escape: a single fill heavier than the whole ceiling still runs alone.
	release, err := admitContentFill(context.Background(), "adm1.example/v1, Resource=huge", 100*ceiling, fillClassCustomer)
	if err != nil {
		t.Fatalf("F-ADM1: a fill heavier than the ceiling on an EMPTY gate was not admitted (no guaranteed progress): %v", err)
	}
	release()
}

func TestContentFill710_ADM2_FirstMeasuredFillCalibratesExpansionWithin2x(t *testing.T) {
	fill710Admission(t)
	const (
		gvr       = "adm2.example/v1, Resource=things"
		raw       = int64(1 << 20)
		realRatio = 3.0 // the decode really costs 3× the raw envelope
	)
	var live atomic.Int64
	restore := cache.SetAdmissionRuntimeSeamsForTest(func() int64 { return 1 << 40 }, live.Load)
	t.Cleanup(restore)

	release, err := admitContentFill(context.Background(), gvr, raw, fillClassCustomer)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if w, _ := contentFillGateStateForTest(); w != int64(float64(raw)*contentFillExpansionSeed) {
		t.Fatalf("an unmeasured GVR must weigh the seed (%v×): weight %d", contentFillExpansionSeed, w)
	}
	live.Add(int64(float64(raw) * realRatio)) // what the parse + Put retained
	release()

	got := ContentFillAdmissionStatsSnapshot().Expansion[gvr]
	if got < realRatio/2 || got > realRatio*2 {
		t.Fatalf("F-ADM2 RED: after one measured fill the expansion is %.2f, want within 2× of the real %.1f (seed %.0f)", got, realRatio, contentFillExpansionSeed)
	}
	release2, err := admitContentFill(context.Background(), gvr, raw, fillClassCustomer)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if w, _ := contentFillGateStateForTest(); float64(w) > 2*realRatio*float64(raw) || float64(w) < realRatio/2*float64(raw) {
		t.Fatalf("F-ADM2: the next fill's weight %d does not track the measured expansion", w)
	}
	release2()

	// A sample spoiled by GC (live heap fell) is ignored, never calibrates to 0.
	before := ContentFillAdmissionStatsSnapshot().Expansion[gvr]
	release3, _ := admitContentFill(context.Background(), gvr, raw, fillClassCustomer)
	live.Add(-int64(raw))
	release3()
	if after := ContentFillAdmissionStatsSnapshot().Expansion[gvr]; after != before {
		t.Fatalf("a negative heap delta moved the expansion %.2f → %.2f", before, after)
	}
}

// TestContentFill710_ADM_BackgroundNeverAdmitsAheadOfAWaitingCustomer: a
// background fill that WOULD fit still waits while a customer fill is waiting,
// and the customer admits first. Deterministic: weights, not wake-up races.
func TestContentFill710_ADM_BackgroundNeverAdmitsAheadOfAWaitingCustomer(t *testing.T) {
	fill710Admission(t)
	const gvr = "prio/v1, Resource=a"
	unit := int64(1000)
	w := int64(float64(unit) * contentFillExpansionSeed)
	fill710SeamCeiling(t, 2*w+w/2) // holder (1) + background (1) fits; holder + customer (2) does not

	holder, err := admitContentFill(context.Background(), gvr, unit, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	order := make(chan string, 2)
	admit := func(name string, raw int64, class fillAdmissionClass) {
		go func() {
			release, err := admitContentFill(context.Background(), gvr, raw, class)
			if err != nil {
				order <- name + ":err"
				return
			}
			order <- name
			time.Sleep(20 * time.Millisecond)
			release()
		}()
	}
	admit("customer", 2*unit, fillClassCustomer)
	if !fill710WaitFor(5*time.Second, func() bool { return ContentFillAdmissionStatsSnapshot().Queued["customer"] == 1 }) {
		t.Fatalf("PRECONDITION: the customer fill did not queue")
	}
	admit("background", unit, fillClassBackground)
	select {
	case got := <-order:
		t.Fatalf("priority RED: %q admitted while a customer read-through was waiting — a background fill must never admit ahead of a waiting customer", got)
	case <-time.After(100 * time.Millisecond):
	}
	if q := ContentFillAdmissionStatsSnapshot().Queued["background"]; q != 1 {
		t.Fatalf("the background fill is not queued (queued=%d)", q)
	}
	holder()
	if first, second := <-order, <-order; first != "customer" || second != "background" {
		t.Fatalf("admission order %s, %s; want customer, background", first, second)
	}
}

// --- real-path arms -----------------------------------------------------------

// newFill710MultiNSWatcher seeds perNS objects in each of nsCount namespaces,
// all readable by r1User through a ClusterRoleBinding.
func newFill710MultiNSWatcher(t *testing.T, nsCount, perNS int) {
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
			ObjectMeta: metav1.ObjectMeta{Name: "fill710-adm-reader"},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fullstackapps"}, Verbs: []string{"get", "list"}},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "fill710-adm-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: r1User}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "fill710-adm-reader"},
		},
	}
	for n := 0; n < nsCount; n++ {
		for i := 0; i < perNS; i++ {
			seed = append(seed, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "composition.krateo.io/v1",
				"kind":       "FullStackApp",
				"metadata":   map[string]any{"namespace": fill710NS(n), "name": fmt.Sprintf("fsa-%05d", i)},
				"status":     map[string]any{"managed": r1ManagedList(2)},
			}})
		}
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
	case <-time.After(20 * time.Second):
		t.Fatalf("fsa informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 20*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

func fill710NS(n int) string { return fmt.Sprintf("fill710-ns-%02d", n) }

func fill710NSListCall(n int) httpcall.RequestOptions {
	return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/composition.krateo.io/v1/namespaces/" + fill710NS(n) + "/fullstackapps",
		Verb: ptr.To(http.MethodGet),
	}}
}

// TestContentFill710_ADM1_RealPath_DistinctListsStayUnderCeiling drives N
// distinct cold lists at once through the production reader.
func TestContentFill710_ADM1_RealPath_DistinctListsStayUnderCeiling(t *testing.T) {
	const (
		lists = 8
		perNS = 300
	)
	newFill710MultiNSWatcher(t, lists, perNS)
	store := cache.ResolvedCache()
	fill710Admission(t)

	// Learn one envelope's size, then size the ceiling for two fills at the seed.
	envLen := fill710SpyDispatch(t, nil, nil, nil)
	if _, _, ok := apistageContentServe(r1Ctx(), store, fill710NSListCall(0), false); !ok {
		t.Fatalf("PRECONDITION: list 0 not content-servable")
	}
	store.DeleteForTest(cache.ComputeKey(contentKeyInputs(r1FsaGVR, fill710NS(0), "")))
	ResetContentFillStatsForTest()
	weight := int64(float64(envLen.Load()) * contentFillExpansionSeed)
	ceiling := 2*weight + weight/2
	fill710SeamCeiling(t, ceiling)

	var wg sync.WaitGroup
	var served atomic.Int64
	start := make(chan struct{})
	for n := 0; n < lists; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start
			if _, s, ok := apistageContentServe(r1Ctx(), store, fill710NSListCall(n), false); ok && s {
				served.Add(1)
			}
		}(n)
	}
	close(start)
	wg.Wait()
	if served.Load() != lists {
		t.Fatalf("F-ADM1: %d of %d distinct lists served — the gate must queue, never drop", served.Load(), lists)
	}
	if p := ContentFillAdmissionStatsSnapshot().InflightWeight; p != 0 {
		t.Fatalf("gate not drained: %d", p)
	}
	if hw := contentFillAdmissionInflightWeightPeak.Load(); hw > ceiling {
		t.Fatalf("F-ADM1 RED (real path): admitted weight peaked at %d over the ceiling %d (%d per fill)", hw, ceiling, weight)
	}
	t.Logf("#710 PR-2a: %d distinct lists, envelope %d B, weight %d, ceiling %d → admitted-weight peak %d, wait p_max %d ms",
		lists, envLen.Load(), weight, ceiling, contentFillAdmissionInflightWeightPeak.Load(), ContentFillAdmissionStatsSnapshot().Wait.MaxMS)
}

// TestContentFill710_D1_ResolveRecordsA503StageErrorNoFallThrough drives D1
// through a real Resolve: the stage fails with 503 and nothing else dials.
func TestContentFill710_D1_ResolveRecordsA503StageErrorNoFallThrough(t *testing.T) {
	newF1Watcher(t)
	store := cache.ResolvedCache()
	fill710Admission(t)
	store.SetTombstoneTTL(300 * time.Millisecond)
	fill710SeamCeiling(t, 1)
	holder, err := admitContentFill(context.Background(), "d1/v1, Resource=held", 1, fillClassCustomer)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)
	before := cache.FallthroughTotal()

	dict := f1ResolveAs(t, f1BroadUser, f1ListStage("widgets"))
	got := fmt.Sprintf("%v", dict)
	if _, served := dict["widgets"].([]any); served {
		t.Fatalf("D1 RED: the stage was served past a refused admission: %s", got)
	}
	if !strings.Contains(got, "memory admission not granted") {
		t.Fatalf("D1 RED: the resolve does not carry the admission error: %s", got)
	}
	if after := cache.FallthroughTotal(); after != before {
		t.Fatalf("D1 RED: the refused call fell through to the apiserver (fallthrough %v → %v)", before, after)
	}
	if got := ContentFillAdmissionStatsSnapshot().Timeout["customer"]; got != 1 {
		t.Fatalf("admission_timeout_total{customer} = %d, want 1", got)
	}
	// The #709 stage failure the item error becomes reports 503, not 500.
	f := newStageItemFailure("widgets", ErrContentFillAdmissionTimeout.Error(),
		fmt.Errorf("api widgets item 0 failed: %w", ErrContentFillAdmissionTimeout))
	if f.failure.Code != http.StatusServiceUnavailable {
		t.Fatalf("an admission timeout reports %d, want 503", f.failure.Code)
	}
}

// TestContentFill710_D1_AdmissionTimeoutErrorsTheFlightNeverFallsThrough is D1.
func TestContentFill710_D1_AdmissionTimeoutErrorsTheFlightNeverFallsThrough(t *testing.T) {
	newFill710MultiNSWatcher(t, 1, 5)
	store := cache.ResolvedCache()
	fill710Admission(t)
	store.SetTombstoneTTL(300 * time.Millisecond) // the write deadline the detached fill is bounded by
	fill710SeamCeiling(t, 1)

	// Something else holds the gate for longer than the deadline.
	holder, err := admitContentFill(context.Background(), "d1/v1, Resource=held", 1, fillClassCustomer)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)

	var dispatches atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		dispatches.Add(1)
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })

	type out struct {
		ok, served bool
		err        error
	}
	results := make(chan out, 3)
	for i := 0; i < 3; i++ {
		go func() {
			_, s, ok, err := apistageContentServeE(r1Ctx(), store, fill710NSListCall(0), false)
			results <- out{ok, s, err}
		}()
	}
	for i := 0; i < 3; i++ {
		select {
		case r := <-results:
			if !errors.Is(r.err, ErrContentFillAdmissionTimeout) {
				t.Fatalf("D1 RED: a reader of a never-admitted flight returned err=%v ok=%v served=%v — want ErrContentFillAdmissionTimeout (an honest stage error, no fall-through)", r.err, r.ok, r.served)
			}
			if r.served {
				t.Fatalf("D1: a never-admitted flight served a body")
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("D1: a reader is still waiting past the deadline")
		}
	}
	if d := dispatches.Load(); d != 1 {
		t.Fatalf("dispatches = %d, want 1 (one flight, two joiners)", d)
	}
	if got := ContentFillAdmissionStatsSnapshot().Timeout["customer"]; got != 1 {
		t.Fatalf("admission_timeout_total{customer} = %d, want 1", got)
	}
	if _, ok := store.GetNoTouch(cache.ComputeKey(contentKeyInputs(r1FsaGVR, fill710NS(0), ""))); ok {
		t.Fatalf("D1: a never-admitted flight stored a cell")
	}
}
