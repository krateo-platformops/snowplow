package cache

// resolve_lists_988_test.go — #988: the cache side of
// snowplow_resolve_collection_lists.
//
//	MARK    every resolve scope marks its ctx for the wire counter: each
//	        closed scope name through WithFallthroughScope, the /call route
//	        middleware, and nothing under cache-off.
//	REF     the refresher scopes every refresh it runs — a dequeued key
//	        (processNext) and a stale dependent's replace (RunRefreshNow) — and
//	        the LIST a refresh sends through a hardened client is counted.
//	REASON  RecordApiserverFallthrough returns its ctx with the reason
//	        stamped, for genuine and diagnostic reasons alike, and the LIST sent
//	        with it is labelled with that reason, not unattributed.
//	PUB     /debug/vars publishes snowplow_resolve_collection_lists as
//	        "gvr|reason" -> count; the OTLP snapshot carries the same cells and
//	        caps them like the fall-through families.

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

const cm988GVR = "/v1, Resource=configmaps"

// sendList sends one collection LIST of configmaps through a client hardened
// the way every resolve dial is (credhttp.Client), under ctx.
func sendList(t *testing.T, ctx context.Context) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ConfigMapList","apiVersion":"v1","items":[]}`))
	}))
	defer srv.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/namespaces/n/configmaps", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := credhttp.Client(&http.Client{}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
}

func reset988(t *testing.T) {
	t.Helper()
	credhttp.ResetResolveCollectionListsForTest()
	ResetFallthroughCountersForTest()
	t.Cleanup(func() {
		credhttp.ResetResolveCollectionListsForTest()
		ResetFallthroughCountersForTest()
	})
}

func TestIssue988_MARK_EveryScopeMarksItsContext(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	if len(validScopeNames) == 0 {
		t.Fatal("PRECONDITION: no scope names")
	}
	if _, ok := validScopeNames[ScopeRefresher]; !ok {
		t.Fatal("the refresher scope is not in the closed scope enum")
	}
	for name := range validScopeNames {
		ctx := WithFallthroughScope(context.Background(), name)
		if !credhttp.ResolveMarked(ctx) {
			t.Errorf("scope %q does not mark its ctx: its resolves' LISTs would never be counted", name)
		}
	}

	var seen context.Context
	h := FallthroughScopeMiddleware(ScopeCallRestactions)(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = r.Context()
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/call", nil))
	if seen == nil || !credhttp.ResolveMarked(seen) {
		t.Fatal("a /call request's ctx (FallthroughScopeMiddleware) is not marked")
	}
}

func TestIssue988_MARK_CacheOffMarksNothing(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	if credhttp.ResolveMarked(WithFallthroughScope(context.Background(), ScopeCallWidgets)) {
		t.Fatal("cache-off: WithFallthroughScope is a no-op (CFG-1), so it must not mark either")
	}
}

// refresh988 registers a widgets refresh func that captures its ctx and sends
// one LIST under it, as a real re-resolve that falls through does.
func refresh988(t *testing.T, key string) *context.Context {
	t.Helper()
	var got context.Context
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, _ ResolvedKeyInputs) error {
		if k == key {
			got = ctx
			sendList(t, ctx)
		}
		return nil
	})
	return &got
}

func assertRefreshScoped(t *testing.T, path string, ctx context.Context) {
	t.Helper()
	if ctx == nil {
		t.Fatalf("%s: the refresh func never ran", path)
	}
	if fs := FallthroughScope(ctx); fs == nil || fs.Path != ScopeRefresher {
		t.Fatalf("%s: the refresh ran with scope %+v, want %q", path, fs, ScopeRefresher)
	}
	if got := credhttp.ResolveCollectionListCount(cm988GVR, credhttp.ReasonUnattributed); got != 1 {
		t.Fatalf("%s: the LIST the refresh sent was not counted (cells %v) — the refresher is a resolve origin",
			path, credhttp.ResolveCollectionLists())
	}
}

func TestIssue988_REF_ADequeuedRefreshIsScopedAndItsListCounted(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	reset988(t)
	_, keys := terminalFixtureNoReset(t, 1)
	got := refresh988(t, keys[0])
	r := refresherSingleton()
	terminalMark(keys)
	for r.viewedQueue.Len() > 0 || r.queue.Len() > 0 {
		r.processNext(context.Background())
	}
	assertRefreshScoped(t, "processNext", *got)
}

func TestIssue988_REF_ARunRefreshNowReplaceIsScopedAndItsListCounted(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	reset988(t)
	_, keys := terminalFixtureNoReset(t, 1)
	got := refresh988(t, keys[0])
	if err := RunRefreshNow(context.Background(), keys[0]); err != nil {
		t.Fatal(err)
	}
	assertRefreshScoped(t, "RunRefreshNow", *got)
}

func TestIssue988_REASON_RecordReturnsTheReasonAndTheListCarriesIt(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	for _, reason := range []FallthroughReason{ReasonInformerNotServable, ReasonClientBuild, ReasonClusterListDispatch} {
		t.Run(string(reason), func(t *testing.T) {
			reset988(t)
			ctx := WithFallthroughScope(context.Background(), ScopeCallRestactions)
			fctx := RecordApiserverFallthrough(ctx, reason, "v1/configmaps")
			if got := FallthroughReasonOf(fctx); got != reason {
				t.Fatalf("the returned ctx carries reason %q, want %q", got, reason)
			}
			sendList(t, fctx)
			if got := credhttp.ResolveCollectionListCount(cm988GVR, string(reason)); got != 1 {
				t.Fatalf("the LIST sent with the returned ctx is not labelled %q: cells %v", reason, credhttp.ResolveCollectionLists())
			}
			// The decision is still counted on its own family, once.
			if FallthroughTotal()+DiagnosticTotal() != 1 {
				t.Fatalf("the decision must be counted exactly once; fallthrough=%d diagnostic=%d",
					FallthroughTotal(), DiagnosticTotal())
			}
		})
	}
	t.Run("an unscoped ctx gains no marker from a reason", func(t *testing.T) {
		reset988(t)
		fctx := RecordApiserverFallthrough(context.Background(), ReasonInformerNotServable, "x")
		sendList(t, fctx)
		if n := len(credhttp.ResolveCollectionLists()); n != 0 {
			t.Fatalf("an unscoped (non-resolve) LIST was counted: %v", credhttp.ResolveCollectionLists())
		}
	})
	t.Run("WithFallthroughReason carries without counting", func(t *testing.T) {
		reset988(t)
		ctx := WithFallthroughReason(WithFallthroughScope(context.Background(), ScopeCallWidgets), ReasonInformerNotSynced)
		if FallthroughReasonOf(ctx) != ReasonInformerNotSynced || FallthroughTotal() != 0 {
			t.Fatalf("reason=%q fallthrough_total=%d", FallthroughReasonOf(ctx), FallthroughTotal())
		}
	})
}

func TestIssue988_PUB_ExpvarAndOTLPSnapshot(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	reset988(t)
	RegisterExpvarForTest()
	ctx := WithFallthroughScope(context.Background(), ScopeCallRestactions)
	sendList(t, RecordApiserverFallthrough(ctx, ReasonInformerNotServable, "v1/configmaps"))
	sendList(t, ctx)
	sendList(t, ctx)

	v := expvar.Get("snowplow_resolve_collection_lists")
	if v == nil {
		t.Fatal("snowplow_resolve_collection_lists is not published")
	}
	var got map[string]uint64
	if err := json.Unmarshal([]byte(v.String()), &got); err != nil {
		t.Fatalf("expvar shape: %v (%s)", err, v.String())
	}
	want := map[string]uint64{
		cm988GVR + "|" + string(ReasonInformerNotServable): 1,
		cm988GVR + "|" + credhttp.ReasonUnattributed:       2,
	}
	if len(got) != len(want) {
		t.Fatalf("expvar = %v; want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Fatalf("expvar[%q] = %d; want %d (all %v)", k, got[k], n, got)
		}
	}

	cells, truncated := ResolveCollectionListCellsSnapshot()
	if truncated != 0 || len(cells) != 2 {
		t.Fatalf("OTLP snapshot = %v (truncated %d); want the 2 expvar cells", cells, truncated)
	}
	for _, c := range cells {
		if want[c.GVR+"|"+c.Reason] != c.Count {
			t.Fatalf("OTLP cell %+v disagrees with expvar %v", c, got)
		}
	}
}

func TestIssue988_PUB_OTLPSnapshotIsCapped(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	reset988(t)
	ResetSeriesTruncatedForTest()
	t.Cleanup(ResetSeriesTruncatedForTest)
	ctx := WithFallthroughScope(context.Background(), ScopeCallRestactions)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	hc := credhttp.Client(&http.Client{})
	const overshoot = 7
	for i := 0; i < otlpCellSeriesCap+overshoot; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/apis/g"+strconv.Itoa(i)+".example/v1/things", nil)
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	cells, truncated := ResolveCollectionListCellsSnapshot()
	if truncated != overshoot || len(cells) != otlpCellSeriesCap+1 {
		t.Fatalf("cells=%d truncated=%d; want %d + one __other__ fold, %d truncated",
			len(cells), truncated, otlpCellSeriesCap, overshoot)
	}
	if SeriesTruncatedSnapshot()["resolve_collection_lists"] < overshoot {
		t.Fatalf("the truncation is not counted: %v", SeriesTruncatedSnapshot())
	}
}
