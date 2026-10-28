//go:build unit

package dispatchers

// issue912_memo_declared_write_test.go — #912 re-review N1 (the reviewer's arm,
// adopted): two widgets of one seed cohort share an apiRef RESTAction that
// DECLARES a write. The first widget's resolve bumps the declared-write mark
// and is declined. The second is served from the per-pass SeedResolveMemo (no
// dispatch) and must STILL decline: a memo hit replays the producer's Put-gate
// effects (cache/put_gate_effects.go), so it is indistinguishable from the
// resolve it replaces. Customer consequence: alice's /call of the sibling is not
// an L1 hit, and sends its POST.
//
// Mutation (logic-only): drop `effects.Replay(ctx)` on the memo hit
// (apiref/resolve.go) → RED (w-two seeded; its /call sends 0 POSTs).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func n912WriteRA(a psArm) *unstructured.Unstructured {
	ra := psRACR(a)
	api := ra.Object["spec"].(map[string]any)["api"].([]any)
	api = append(api, map[string]any{"name": "render", "verb": "POST", "path": "/render",
		"payload": `{"n":1}`, "continueOnError": true, "errorKey": "renderErr"})
	ra.Object["spec"].(map[string]any)["api"] = api
	return ra
}

func n912Widget(name string) *unstructured.Unstructured {
	w := h1WidgetUnstructured(map[string]any{
		"apiRef": map[string]any{"name": psRAName, "namespace": h1NS},
	})
	w.SetName(name)
	return w
}

func TestIssue912_N1_SeedMemoHitStillDeclinesADeclaredWrite(t *testing.T) {
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	ra := n912WriteRA(a)
	w1, w2 := n912Widget("w-one"), n912Widget("w-two")
	extra := append(k423WidgetExtras(a), runtime.Object(ra), runtime.Object(w1), runtime.Object(w2))
	psBuildWatcher(t, a, extra...)
	if _, ch := cache.Global().EnsureResourceType(h1RAGVR); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("RA informer did not sync")
		}
	}
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	var posts atomic.Int64
	srvURL, _ := url.Parse(srv.URL)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		httputil.NewSingleHostReverseProxy(srvURL).ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	psSeedClientconfigs(t, proxy.URL)

	memo := cache.NewSeedResolveMemo()
	base := cache.WithSeedResolveMemo(context.Background(), memo)
	cohort := withCohortSeedContext(base, seedTarget{Username: psAlice, Groups: psGroups(a)},
		endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}, &rest.Config{Host: srv.URL, BearerToken: "tok-sa"})

	origWR := widgetsResolveFn
	t.Cleanup(func() { widgetsResolveFn = origWR })
	widgetsResolveFn = func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
		out, _ := widgets.Resolve(ctx, opts)
		if out == nil {
			out = &unstructured.Unstructured{Object: map[string]any{}}
		}
		return out, nil
	}
	for _, w := range []*unstructured.Unstructured{w1, w2} {
		e := navWidgetEntry{W: w, GVR: h1WidgetGVR, PerPage: -1, Page: -1, KeyPerPage: -1, KeyPage: -1}
		_ = seedOneWidget(cohort, e, psAuthnNS, seedModeBoot)
	}
	hits, misses := memo.Stats()
	t.Logf("memo hits=%d misses=%d", hits, misses)
	c := cache.ResolvedCache()
	var stored []string
	for _, k := range c.KeysForTest() {
		if e, ok := c.GetNoTouch(k); ok && e != nil && e.Inputs != nil && e.Inputs.CacheEntryClass == "widgets" {
			stored = append(stored, e.Inputs.Name)
		}
	}
	t.Logf("widget cells stored: %v", stored)
	if hits == 0 {
		t.Fatalf("SETUP: the second widget was not served from the memo (hits=%d misses=%d) — this arm would be vacuous", hits, misses)
	}
	// Customer consequence: alice's /call of each widget, real widgets.Resolve.
	// w-one (not stored) resolves live and dials (its POST included); a stored
	// w-two is an L1 hit that dials nothing, so its write never runs.
	aliceCtx := psUserCtx(a, psAlice)
	callDials := func(w *unstructured.Unstructured) int64 {
		r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
			return objects.Result{GVR: h1WidgetGVR, Unstructured: w.DeepCopy()}
		})
		defer r1()
		before := posts.Load()
		rec := httptest.NewRecorder()
		h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(aliceCtx))
		t.Logf("/call %s: code=%d", w.GetName(), rec.Code)
		return posts.Load() - before
	}
	d1, d2 := callDials(w1), callDials(w2)
	t.Logf("alice POSTs: w-one=%d w-two=%d", d1, d2)
	if d1 != 1 {
		t.Fatalf("PRE: the live customer /call of w-one must send its POST once, sent %d", d1)
	}
	if len(stored) != 0 || d2 != 1 {
		t.Fatalf("#912 N1 RED: a widget whose apiRef RESTAction declares a write was seeded into L1 %v (memo hits=%d) — "+
			"alice's /call of w-two sent %d POST(s), want 1", stored, hits, d2)
	}
}
