// issue354_p3_f6_e2e_test.go — #354 P3 F6: the invalidation→fresh window,
// K>1 × M>1, through the real boundaries.
//
// SHAPE (K=3 identity-bound classes × M=4 warm cells, all keyed differently):
//   - restactions — GET-by-name RESTAction served by the real restActionHandler
//     with ?page=2&perPage=10&extras={"k":"v<i>"} (4 cells, keyed by page AND
//     extras);
//   - widgets — the real widgetsHandler for a widget declaring spec.keyExtras
//     [k], served with extras {"k":"v<i>"} (4 cells keyed by extras);
//   - raFullList — the real apiref.Resolve over 4 RESTActions, each minting its
//     own raKey cell.
//
// REAL BOUNDARY: a real ResourceWatcher + published RBAC snapshot (the #423 ps
// harness), the real handlers, the real store, the real refresher loop →
// resolveAndPopulateL1 → the real restactions resolver (objects.Get serves the
// RAs from the informer). The informer UPDATE is a real configmap update. The
// widget resolver is the one seam (widgetsResolveFn / resolveOnceFn for the
// widgets class): it reads the configmap and records the dep edge exactly as
// the apiRef resolver's inner call would.
//
// ASSERTIONS
//   - every customer GET between the mark and the Put returns the OLD body and
//     counts once on stale_served_total (12 GETs → Δ 12, held while every
//     refresh is gated — one of them dequeued and in flight);
//   - after the gate opens, every cell converges and the next GET returns the
//     NEW body without counting;
//   - dirty_to_fresh_samples Δ 12 and dirty_to_fresh_ms_p95 > 0. (The OTLP
//     histogram's count is D-OTLP's assertion, internal/metrics.)
//
// RED on main (f1f18fcc): stale_served_total and dirty_to_fresh_samples do not
// exist (read 0).

package dispatchers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	p3M        = 4
	p3RAByName = "p3-ra-byname"
	p3NewValue = "after-354-p3"
)

func p3FLName(i int) string { return fmt.Sprintf("p3-rafl-%d", i) }

// p3ByNameRA is a GET-by-name RESTAction whose filter surfaces the configmap
// value outside any slice, so a page-2 cell still carries it.
func p3ByNameRA() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": p3RAByName, "namespace": h1NS},
		"spec": map[string]any{
			"api": []any{map[string]any{
				"name": "cm", "path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj, "continueOnError": true,
			}},
			"filter": `{probe: (.cm.data.password // "none")}`,
		},
	}}
}

func p3Widget() *unstructured.Unstructured {
	return h1WidgetUnstructured(map[string]any{"keyExtras": []any{"k"}})
}

func p3Extras(i int) map[string]any { return map[string]any{"k": fmt.Sprintf("v%d", i)} }

func p3ExtrasQuery(i int) string { return url.QueryEscape(fmt.Sprintf(`{"k":"v%d"}`, i)) }

type p3E2EEnv struct {
	dyn   *dynamicfake.FakeDynamicClient
	saEP  *endpoints.Endpoint
	saRC  *rest.Config
	carol context.Context
}

func p3E2EFixture(t *testing.T) *p3E2EEnv {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	extra := append(k423PortalReaders(a), k423WidgetExtras(a)...)
	for i := 1; i <= p3M; i++ {
		extra = append(extra, i431RACR(p3FLName(i)))
	}
	extra = append(extra, p3ByNameRA())
	dyn := psBuildWatcher(t, a, extra...)
	rw := cache.Global()
	if _, ch := rw.EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !rw.IsServable(h1RAGVR) {
		t.Fatalf("PRE: restactions must be servable (the refresher re-fetches the RA from the informer)")
	}
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)
	return &p3E2EEnv{dyn: dyn, saEP: &endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"},
		saRC: &rest.Config{Host: srv.URL}, carol: i431Ctx(psCarol)}
}

// cmValue reads the configmap the widget "resolver" depends on.
func (e *p3E2EEnv) cmValue(t *testing.T) string {
	u, err := e.dyn.Resource(i431CMGVR).Namespace(psTargetNS).Get(context.Background(), psTargetObj, metav1.GetOptions{})
	if err != nil {
		t.Errorf("get cm: %v", err)
		return ""
	}
	v, _, _ := unstructured.NestedString(u.Object, "data", "password")
	return v
}

// widgetBody is the widget seam's output: it records the dep edge the apiRef
// resolver's inner call would record, against the L1 key on ctx.
func (e *p3E2EEnv) widgetBody(t *testing.T, ctx context.Context) *unstructured.Unstructured {
	if k := cache.L1KeyFromContext(ctx); k != "" {
		cache.Deps().Record(ctx, k, i431CMGVR, psTargetNS, psTargetObj)
	}
	out := p3Widget()
	_ = unstructured.SetNestedField(out.Object, e.cmValue(t), "status", "widgetData", "v")
	return out
}

func (e *p3E2EEnv) serveRA(t *testing.T, i int) string {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: p3ByNameRA()}
	})
	defer restore()
	h := &restActionHandler{authnNS: psAuthnNS, saRC: e.saRC}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call?page=2&perPage=10&extras="+p3ExtrasQuery(i), nil).WithContext(e.carol))
	if rec.Code != 200 {
		t.Fatalf("restactions /call: %d %s", rec.Code, psTrunc(rec.Body.String(), 200))
	}
	return rec.Body.String()
}

func (e *p3E2EEnv) serveWidget(t *testing.T, i int) string {
	t.Helper()
	r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: p3Widget()}
	})
	r2 := setWidgetsResolveForTest(func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		return e.widgetBody(t, ctx), nil
	})
	defer func() { r2(); r1() }()
	h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call?extras="+p3ExtrasQuery(i), nil).WithContext(e.carol))
	if rec.Code != 200 {
		t.Fatalf("widgets /call: %d %s", rec.Code, psTrunc(rec.Body.String(), 200))
	}
	return rec.Body.String()
}

func (e *p3E2EEnv) serveFL(t *testing.T, i int) string {
	t.Helper()
	res, err := apiref.Resolve(e.carol, apiref.ResolveOptions{
		RC: e.saRC, ApiRef: i431ApiRef(p3FLName(i)), AuthnNS: psAuthnNS, PerPage: 1, Page: 1,
	})
	if err != nil {
		t.Fatalf("apiref.Resolve: %v", err)
	}
	return fmt.Sprintf("%v", res)
}

func TestIssue354_P3_F6_WindowKxM_GetByName(t *testing.T) {
	e := p3E2EFixture(t)

	// Mint the 12 cells (cold fills), and their keys.
	var keys []string
	for i := 1; i <= p3M; i++ {
		e.serveRA(t, i)
		k, _, _ := dispatchCacheLookupKey(e.carol, "restactions", h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource,
			h1NS, p3RAByName, 10, 2, p3Extras(i))
		keys = append(keys, k)
	}
	for i := 1; i <= p3M; i++ {
		e.serveWidget(t, i)
		w := p3Widget()
		k, _, _ := dispatchCacheLookupKey(e.carol, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource,
			h1NS, h1WName, -1, -1, effectiveKeyExtras(e.carol, w.Object, p3Extras(i)))
		keys = append(keys, k)
	}
	for i := 1; i <= p3M; i++ {
		e.serveFL(t, i)
		k, _ := i431RAKey(t, e.carol, p3FLName(i))
		keys = append(keys, k)
	}
	seen := map[string]bool{}
	for n, k := range keys {
		if seen[k] {
			t.Fatalf("PRE: cell %d shares a key with another cell — the shape must be 12 distinct cells", n)
		}
		seen[k] = true
		if b := i431Body(k); !strings.Contains(b, psSentinel) {
			t.Fatalf("PRE: cell %d must be resident with the original value; body=%s", n, psTrunc(b, 200))
		}
	}
	// Warm hit: the cells serve before any change (and count no stale serve).
	stale0 := p3StoreStat("stale_served_total")
	for i := 1; i <= p3M; i++ {
		e.serveRA(t, i)
	}
	if d := p3StoreStat("stale_served_total") - stale0; d != 0 {
		t.Fatalf("PRE: a hit on a clean cell must not count as stale (Δ=%d)", d)
	}

	// The refresher, gated so every window is open at once.
	i187RefresherEnv(t)
	gate := make(chan struct{})
	var invocations atomic.Int64
	refresh := func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		invocations.Add(1)
		<-gate
		return resolveAndPopulateL1(ctx, in, e.saEP, e.saRC)
	}
	restoreSeam := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		if in.CacheEntryClass == "widgets" {
			return encodeResolvedJSON(e.widgetBody(t, ctx).Object)
		}
		return resolveOnceProd(ctx, in)
	})
	for _, class := range []string{"restactions", "widgets", cache.CacheEntryClassRAFullList} {
		cache.RegisterRefreshFunc(class, refresh)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	t.Cleanup(func() { cancel(); cache.ResetRefresherForTest(); restoreSeam() })

	s0 := p3RefStat("dirty_to_fresh_samples")
	enq0 := p3RefStat("enqueue")
	i431UpdateCM(t, e.dyn, p3NewValue)
	p3Wait(t, 10*time.Second, "the 12 dirty-marks", func() bool { return p3RefStat("enqueue")-enq0 >= int64(len(keys)) })
	p3Wait(t, 5*time.Second, "one refresh dequeued and in flight", func() bool { return invocations.Load() >= 1 })

	// Between mark and Put: every GET returns the OLD body and counts once.
	stale1 := p3StoreStat("stale_served_total")
	for i := 1; i <= p3M; i++ {
		if b := e.serveRA(t, i); strings.Contains(b, p3NewValue) || !strings.Contains(b, psSentinel) {
			t.Fatalf("restactions cell %d must serve the OLD body before its Put; body=%s", i, psTrunc(b, 200))
		}
		if b := e.serveWidget(t, i); strings.Contains(b, p3NewValue) || !strings.Contains(b, psSentinel) {
			t.Fatalf("widgets cell %d must serve the OLD body before its Put; body=%s", i, psTrunc(b, 200))
		}
		e.serveFL(t, i)
	}
	if d := p3StoreStat("stale_served_total") - stale1; d != int64(len(keys)) {
		t.Fatalf("F6: stale_served_total Δ=%d over %d GETs of dirty cells, want %d", d, len(keys), len(keys))
	}
	if p3StoreStat("stale_served_age_ms_max") <= 0 {
		t.Fatalf("F6: stale_served_age_ms_max must be > 0 after stale serves")
	}

	close(gate)
	p3Wait(t, 20*time.Second, "every cell converged", func() bool {
		for _, k := range keys {
			if !strings.Contains(i431Body(k), p3NewValue) {
				return false
			}
		}
		return true
	})
	p3Wait(t, 5*time.Second, "the 12 samples", func() bool { return p3RefStat("dirty_to_fresh_samples")-s0 >= int64(len(keys)) })
	time.Sleep(100 * time.Millisecond)

	// After the Put: the next GET returns the NEW body and does not count.
	stale2 := p3StoreStat("stale_served_total")
	for i := 1; i <= p3M; i++ {
		if b := e.serveRA(t, i); !strings.Contains(b, p3NewValue) {
			t.Fatalf("restactions cell %d must serve the NEW body after its Put; body=%s", i, psTrunc(b, 200))
		}
		if b := e.serveWidget(t, i); !strings.Contains(b, p3NewValue) {
			t.Fatalf("widgets cell %d must serve the NEW body after its Put; body=%s", i, psTrunc(b, 200))
		}
		e.serveFL(t, i)
	}
	if d := p3StoreStat("stale_served_total") - stale2; d != 0 {
		t.Fatalf("F6: a GET after the Put must not count as stale (Δ=%d)", d)
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != int64(len(keys)) {
		t.Fatalf("F6: dirty_to_fresh_samples Δ=%d, want %d (one per window); unfresh=%v", d, len(keys), p3Unfresh())
	}
	if p3RefStat("dirty_to_fresh_ms_p95") <= 0 {
		t.Fatalf("F6: dirty_to_fresh_ms_p95 must be > 0 after %d samples", len(keys))
	}
}
