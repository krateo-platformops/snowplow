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
	restapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
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

// p3ContentCells is every resident backing content cell (the apistage class):
// the substrate the 12 cells read, which the configmap change also marks.
func p3ContentCells() []string {
	store := cache.ResolvedCache()
	var out []string
	for _, k := range store.KeysForTest() {
		if e, ok := store.GetNoTouch(k); ok && e != nil && e.Inputs != nil && e.Inputs.CacheEntryClass == cache.CacheEntryClassApistage {
			out = append(out, k)
		}
	}
	return out
}

// p3ServedBody is the body a customer read of content cell k is served: an
// incremental list cell (#854) keeps no RawJSON, its body is the generation a
// customer reader takes (Snapshot at the current dep-event seq, #854 C5).
func p3ServedBody(t *testing.T, k string) string {
	t.Helper()
	e, ok := cache.ResolvedCache().GetNoTouch(k)
	if !ok || e == nil {
		return "<absent>"
	}
	if lc := e.ListCell(); lc != nil {
		v, ok := lc.Snapshot(cache.DepGenEpochNow())
		if !ok {
			return "<unanchored>"
		}
		return string(v.Raw())
	}
	return string(e.RawJSON)
}

// p3F6WriteDeadline is F6's fixture write deadline: the arm's longest phase
// bound ("every cell converged").
const p3F6WriteDeadline = 20 * time.Second

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
	// p3M+1 is the arm's cold reader (#1033): never served until the arm's
	// final read, so its resolve reads the backing content cells through.
	for i := 1; i <= p3M+1; i++ {
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
	// i431RACR's stage is a userAccessFilter read (#961): it dials snowplow's
	// ServiceAccount endpoint.
	t.Cleanup(restapi.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, nil
	}))
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
	// The fixture server's write deadline (production: main.go writeTimeout →
	// SetTombstoneTTL), the one source of DemandRevalidationDeadline. Sized to
	// the arm's longest phase bound (p3F6WriteDeadline), so a debt refill or
	// re-resolve that wedges is reported by the bounded debt join within twice
	// that (#1033 review), never by the go-test timeout.
	prevDeadline := cache.DemandRevalidationDeadline()
	cache.ResolvedCache().SetTombstoneTTL(p3F6WriteDeadline)
	t.Cleanup(func() { cache.ResolvedCache().SetTombstoneTTL(prevDeadline) })

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

	// The backing content cells the configmap change also marks (#1033): one
	// window each, alongside the 12 cells'.
	// Exactly the fixture's two content calls: the by-name RA's configmap GET
	// and the FL RAs' configmaps LIST (pinned, so the window count is the
	// fixture's, never whatever happens to be resident).
	substrate := p3ContentCells()
	if len(substrate) != 2 {
		t.Fatalf("PRE: %d backing content cells resident, want the fixture's 2 (configmap GET by name, configmaps LIST)", len(substrate))
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
	work0 := cache.RefresherWorkSnapshot()
	ksServed0 := p3StoreStat("known_stale_served_total")
	d2Serve0 := restapi.StaleServeStatsSnapshot().ServeTotal
	enq0 := p3RefStat("enqueue")
	i431UpdateCM(t, e.dyn, p3NewValue)
	p3Wait(t, 10*time.Second, "the 12 dirty-marks", func() bool { return p3RefStat("enqueue")-enq0 >= int64(len(keys)) })
	p3Wait(t, 5*time.Second, "one refresh dequeued and in flight", func() bool { return invocations.Load() >= 1 })
	p3Wait(t, 10*time.Second, "a window open on every backing content cell", func() bool {
		for _, k := range substrate {
			if !cache.InputsMovedSincePut(k) {
				return false
			}
		}
		return true
	})
	windows := int64(len(keys) + len(substrate))

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
	// #930 — settle on the EVENT: no refresh queued or in a worker's hands (the
	// substrate cells' no-handler declines included). A fixed 100 ms here could
	// not tell a duplicated sample from a refresh still in flight.
	p3Settle(t, work0)
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d < int64(len(keys)) {
		t.Fatalf("F6: dirty_to_fresh_samples Δ=%d once the refresher is quiescent, want ≥ %d (one per converged cell)", d, len(keys))
	}

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
	// One sample per window, never two. #547: a window whose refresh DECLINED is
	// no longer erased — it stays KNOWN-STALE and ends with a sample when an
	// accepted post-mark Put lands (here: the substrate content cells the
	// configmap change also marked, refilled by the GETs' read-through). So the
	// conservation law is: every window ends fresh (one sample) or is still
	// known-stale — the 12 cells' windows plus each declined one, exactly once.
	// #930 — the GETs above can hand work to the refresher (an accepted
	// read-through Put that re-opens a window re-enqueues it): settle again, then
	// read the conservation law at rest. SCOPE of that settle: the refresher is
	// the only asynchronous writer of these windows in this arm — no customer
	// read hit a known-stale identity-bound cell (so no demand revalidation
	// started) and no D2 stale serve opened a debt (so no refill ran). Asserted,
	// not assumed: if either ever moves, the settle no longer covers every
	// carrier and the arm says so instead of passing on a partial settle.
	p3Settle(t, work0)
	if d := p3StoreStat("known_stale_served_total") - ksServed0; d != 0 {
		t.Fatalf("SCOPE: %d customer read(s) hit a known-stale cell — a demand revalidation may be in flight, "+
			"outside the refresher settle", d)
	}
	if d := restapi.StaleServeStatsSnapshot().ServeTotal - d2Serve0; d != 0 {
		t.Fatalf("SCOPE: %d D2 stale serve(s) opened a debt — its refill runs outside the refresher settle", d)
	}
	// #1033 — the law is PER WINDOW. dirty_ended_unfresh_declined counts DEQUEUES
	// that did not end a window fresh (observability.md), not windows: a backing
	// list cell's own due pass (list_cell.go maybePass → EnqueueRefresh) can
	// dequeue its key again while the window is known-stale, and that dequeue
	// declines too (the content class has no refresh handler). Under load it
	// lands outside the mark's queue slot instead of coalescing with it, so one
	// window can count two declines. What holds for every window: it ended with
	// exactly one sample, or it is still KNOWN-STALE (#547: only an accepted
	// re-Put ends it). Never an open, not-known-stale window at rest.
	for n, k := range keys {
		if cache.InputsMovedSincePut(k) {
			t.Fatalf("F6: cell %d's window is still open at rest — its accepted re-Put did not close it", n)
		}
	}
	var stillKnownStale int64
	for _, k := range substrate {
		if !cache.InputsMovedSincePut(k) {
			continue
		}
		if _, _, ok := cache.KnownStale(k); !ok {
			t.Fatalf("F6: a backing content cell's window is open at rest and NOT known-stale (no refresh is pending to end it)")
		}
		stillKnownStale++
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d+stillKnownStale != windows || d < int64(len(keys)) {
		t.Fatalf("F6: dirty_to_fresh_samples Δ=%d + still known-stale %d, want %d windows (%d cells + %d backing), each ending once; unfresh=%v",
			d, stillKnownStale, windows, len(keys), len(substrate), p3Unfresh())
	}
	// Every backing window declined at least once: the per-dequeue counter can
	// only count more, never fewer.
	if declined := p3Unfresh()["declined"]; declined < int64(len(substrate)) {
		t.Fatalf("F6: dirty_ended_unfresh_declined=%d, want ≥ %d (each backing window's refresh declines)", declined, len(substrate))
	}

	// Close what is still known-stale through the product's read path: a cold
	// resolve of a reader never served before reads every backing cell. A
	// known-stale one is either read THROUGH (#547) or, for an armable customer,
	// served at once with a debt whose immediate refill re-Puts it (#710 D2,
	// "show then replace"). Either way an accepted Put ends the window with one
	// sample. The debt refill runs outside the refresher, so settle on BOTH
	// events: every debt goroutine returned, then the refresher quiescent.
	work1 := cache.RefresherWorkSnapshot()
	e.serveRA(t, p3M+1)
	e.serveFL(t, p3M+1)
	if err := restapi.WaitStaleDebtsForTest(); err != nil {
		t.Fatalf("F6: the final read's stale-debt work never ended: %v", err)
	}
	p3Settle(t, work1)
	for _, k := range append(append([]string{}, keys...), substrate...) {
		if cache.InputsMovedSincePut(k) {
			t.Fatalf("F6: a window is still open after every backing cell was read through — it never closes")
		}
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != windows {
		t.Fatalf("F6: dirty_to_fresh_samples Δ=%d once every window has ended, want exactly %d (one per window: %d cells + %d backing); unfresh=%v",
			d, windows, len(keys), len(substrate), p3Unfresh())
	}
	// A closed window is a FRESH cell: every backing cell SERVES the change,
	// not only its window instrument. Last, because a list cell's read can
	// queue its own due pass.
	for n, k := range substrate {
		if !strings.Contains(p3ServedBody(t, k), p3NewValue) {
			t.Fatalf("F6: backing content cell %d's window closed but the body it serves does not carry the new value", n)
		}
	}
	if p3RefStat("dirty_to_fresh_ms_p95") <= 0 {
		t.Fatalf("F6: dirty_to_fresh_ms_p95 must be > 0 after %d samples", len(keys))
	}
}
