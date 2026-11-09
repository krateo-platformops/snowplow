// refresh_memo_counters_test.go — the refresher memo's lookup counters and its
// measurement-only would-join count on /debug/vars (snowplow_resolve_memo):
// present at 0 before any memo exists, each moved only by its own event.

package dispatchers

import (
	"encoding/json"
	"expvar"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func resolveMemoVars(t *testing.T) map[string]float64 {
	t.Helper()
	v := expvar.Get("snowplow_resolve_memo")
	if v == nil {
		t.Fatal("snowplow_resolve_memo is not published")
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(v.String()), &raw); err != nil {
		t.Fatalf("snowplow_resolve_memo is not a JSON object: %v", err)
	}
	out := map[string]float64{}
	for _, k := range []string{"hits_total", "misses_total", "stale_misses_total", "would_join_total"} {
		n, ok := raw[k].(float64)
		if !ok {
			t.Fatalf("snowplow_resolve_memo.%s missing or not a number: %v", k, raw)
		}
		out[k] = n
	}
	return out
}

func TestRefreshMemoCounters_OnExpvarFromZero(t *testing.T) {
	cacheOnForMemo(t)
	resetRefreshMemoForTest()
	t.Cleanup(resetRefreshMemoForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	publishResolveMemoExpvar()

	want := func(hits, misses, stale, joins float64) {
		t.Helper()
		got := resolveMemoVars(t)
		if got["hits_total"] != hits || got["misses_total"] != misses || got["stale_misses_total"] != stale || got["would_join_total"] != joins {
			t.Fatalf("snowplow_resolve_memo = %v, want hits=%v misses=%v stale=%v would_join=%v", got, hits, misses, stale, joins)
		}
	}
	want(0, 0, 0, 0) // no memo yet: every key present at 0

	m := refreshResolveMemo()
	want(0, 0, 0, 0)
	m.Load("absent") // a miss
	want(0, 1, 0, 0)

	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "benchapps"}
	const ns = "ns-memo-counters"
	d := cache.Deps()
	d.RecordList(t.Context(), "L1_memo_counters", gvr, ns)
	dep := cache.DepKey{GVR: gvr, Namespace: ns, Name: "*"}
	m.Store("k", []byte(`{"v":1}`), []cache.DepKey{dep}, cache.DepGenEpochNow())
	m.Load("k") // a hit
	want(1, 1, 0, 0)

	a := m.BeginProduce("k2")
	want(1, 1, 0, 0)
	b := m.BeginProduce("k2") // a produce of k2 is in flight
	want(1, 1, 0, 1)
	a.End()
	b.End()
	c := m.BeginProduce("k2") // k2 no longer in flight
	c.End()
	want(1, 1, 0, 1)
	// stale_misses_total reads the same snapshot as hits and misses
	// (cache.SeedResolveMemo.LookupStats, pinned by TestMemoLookupStats_StaleIsAMiss).
}
