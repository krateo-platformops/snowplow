package metrics

// resolve_memo_otlp_test.go — the refresher memo's lookups and measurement-only
// would-join count are on OTLP through the REAL registration and callback:
// every series present before the memo is used, each moved only by its own
// event on the real refresher memo, the values the expvar accessor reports,
// and no attribute beyond the closed outcome set.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func resolveMemoOTLP(t *testing.T) (lookups map[string]int64, joins int64) {
	t.Helper()
	pts := collect1265(t)
	lookups = map[string]int64{}
	for _, p := range pointsFor(pts, "snowplow_resolve_memo_lookups_total") {
		if len(p.attrs) != 1 {
			t.Fatalf("lookups attributes %v, want exactly outcome", p.attrs)
		}
		lookups[p.attrs["outcome"]] = p.value
	}
	jp := pointsFor(pts, "snowplow_resolve_memo_would_join_total")
	if len(jp) != 1 || len(jp[0].attrs) != 0 {
		t.Fatalf("would_join points %v, want exactly one with no attributes", jp)
	}
	return lookups, jp[0].value
}

func TestResolveMemoOTLP_LookupsAndWouldJoinFromZero(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // CFG-1: a cache-side instrument
	before, joinsBefore := resolveMemoOTLP(t)
	if len(before) != 3 {
		t.Fatalf("outcomes %v, want exactly hit, miss and stale_miss, present before any lookup", before)
	}
	for _, k := range []string{"hit", "miss", "stale_miss"} {
		if _, ok := before[k]; !ok {
			t.Fatalf("outcome %q absent: %v", k, before)
		}
	}

	m := dispatchers.RefreshResolveMemoForTest()
	m.Load("otlp-absent") // miss
	m.Store("otlp-k", []byte(`{"v":1}`), nil, 1)
	m.Load("otlp-k") // hit
	m.Load("otlp-k") // hit
	a := m.BeginProduce("otlp-p")
	b := m.BeginProduce("otlp-p") // joins a
	a.Produced(nil, 1)            // a body no dependency has moved past: servable
	a.End()
	b.End()

	after, joinsAfter := resolveMemoOTLP(t)
	if after["hit"]-before["hit"] != 2 || after["miss"]-before["miss"] != 1 || after["stale_miss"] != before["stale_miss"] {
		t.Fatalf("OTLP lookups moved %v → %v, want hit +2, miss +1, stale_miss +0", before, after)
	}
	if joinsAfter-joinsBefore != 1 {
		t.Fatalf("OTLP would_join moved %d → %d, want +1", joinsBefore, joinsAfter)
	}
	hits, misses, stale := dispatchers.RefreshMemoLookups()
	if int64(hits) != after["hit"] || int64(misses-stale) != after["miss"] || int64(stale) != after["stale_miss"] {
		t.Fatalf("OTLP %v, the expvar accessor hits=%d misses=%d stale=%d", after, hits, misses, stale)
	}
	if int64(dispatchers.RefreshMemoWouldJoin()) != joinsAfter {
		t.Fatalf("OTLP would_join %d, the expvar accessor %d", joinsAfter, dispatchers.RefreshMemoWouldJoin())
	}
}
