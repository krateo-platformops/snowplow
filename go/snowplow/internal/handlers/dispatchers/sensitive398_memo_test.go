package dispatchers

import (
	"context"
	"encoding/json"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets/apiref"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	pmaps "github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func s398MemoWidget(name string) *unstructured.Unstructured {
	w := h1WidgetUnstructured(map[string]any{
		"apiRef":             map[string]any{"name": psRAName, "namespace": h1NS},
		"widgetDataTemplate": []any{map[string]any{"forPath": "password", "expression": "${ .target }"}},
	})
	w.SetName(name)
	return w
}

// reviewer-416 probe, adopted (#440 BLOCKER 1): within ONE seed pass, two widgets of the same cohort
// share an apiRef'd RESTAction whose step reads a Secret. The first widget's
// resolve bumps its sensitive sink and is declined. The second is served from the
// per-pass SeedResolveMemo — no dispatch, so no bump — and must STILL decline.
func TestS398_SeedMemoHitStillDeclinesSecret(t *testing.T) {
	k423Env(t)
	a := s398Arm()
	ra := psRACR(a)
	w1, w2 := s398MemoWidget("w-one"), s398MemoWidget("w-two")
	extra := append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(w1), runtime.Object(w2))
	psBuildWatcher(t, a, extra...)
	if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("RA informer did not sync")
		}
	}
	srv := psFakeAPIServer(t, a, k423Counters())
	psSeedClientconfigs(t, srv.URL)

	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	base := cache.WithSeedResolveMemo(context.Background(), memo)
	cohort := withCohortSeedContext(base, seedTarget{Username: psAlice, Groups: psGroups(a)},
		endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, &rest.Config{Host: srv.URL, BearerToken: "tok-sa"})

	origWR := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = origWR })
	widgetsResolveFn = func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		out, _ := widgets.Resolve(ctx, opts) // REAL resolve; tail CRD-status validate error benign (a3Serve pattern)
		if out == nil {
			out = &unstructured.Unstructured{Object: map[string]any{}}
		}
		return out, nil
	}
	{
		ds, aerr := apiref.Resolve(cohort, apiref.ResolveOptions{ApiRef: templatesv1.ObjectReference{
			Reference: templatesv1.Reference{Name: psRAName, Namespace: h1NS}, Resource: h1RAGVR.Resource,
			APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version}, AuthnNS: psAuthnNS, PerPage: -7, Page: -7})
		b, _ := json.Marshal(ds)
		t.Logf("DEBUG apiref err=%v sentinel=%v ds=%.500s", aerr, psHasSentinel(b), string(b))
	}
	declinedBefore := cache.SensitiveSkippedPutForTest()
	for _, w := range []*unstructured.Unstructured{w1, w2} {
		e := navWidgetEntry{W: w, GVR: h1WidgetGVR, PerPage: -1, Page: -1, KeyPerPage: -1, KeyPage: -1}
		_ = seedOneWidget(cohort, e, psAuthnNS, seedModeBoot)
	}
	hits, misses := memo.Stats()
	t.Logf("memo hits=%d misses=%d sensitive declines=%d", hits, misses, cache.SensitiveSkippedPutForTest()-declinedBefore)
	// The arm must exercise the memo: either a sibling HIT it (the pre-fix shape,
	// where the memo held the Secret-derived body) or the producer's Store was
	// skipped for the sensitive read (the fix).
	if hits == 0 && sensitiveMemoSkipped398() == 0 {
		t.Fatalf("SETUP: the seed memo path was not exercised (hits=%d misses=%d, no sensitive Store skip)", hits, misses)
	}
	if hits > 0 {
		t.Fatalf("#398 HOLE: the seed memo HELD a body produced from a Secret read and served it to a sibling widget "+
			"with no dispatch (hits=%d) — the sibling never re-bumps the sensitive sink", hits)
	}
	c := cache.ResolvedCache()
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && e.Inputs != nil {
			t.Logf("DEBUG cell class=%s name=%s len=%d", e.Inputs.CacheEntryClass, e.Inputs.Name, len(e.RawJSON))
		}
	}
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && psHasSentinel(e.RawJSON) {
			class := ""
			if e.Inputs != nil {
				class = e.Inputs.CacheEntryClass + " " + e.Inputs.Name
			}
			t.Fatalf("#398 HOLE: a seed-memo hit persisted the Secret body in an L1 cell (%s)", class)
		}
	}
}
