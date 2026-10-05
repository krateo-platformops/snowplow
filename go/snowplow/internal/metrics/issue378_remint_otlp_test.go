package metrics

// issue378_remint_otlp_test.go — #378 D3 (brief issuecomment-5990884686): the
// five P1 store stats and the class-labelled re-mint counter leave the process
// on OTLP, read with an in-memory (ManualReader) collect of the REAL instrument
// set. The live-acceptance query (q378_p1_live_acceptance.sql) reads exactly
// these names: snowplow_resolved_cache{stat=…} in otel_metrics_gauge and
// snowplow_resolved_cache_remint_total{class} (a monotonic cumulative Sum, one
// series per class of the closed set of 5) in otel_metrics_sum.
//
// Every value is driven through the production store by REAL elapse; the new
// store method is reached through an interface so this file compiles on the
// parent, where it is RED.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type r378RefreshPutter interface {
	ReplaceIfGenRefresh(ctx context.Context, key string, entry *cache.ResolvedEntry, capturedGen uint64) bool
}

// r378RemintClasses is the closed class set the live query requires (5 series).
var r378RemintClasses = []string{"restactions", "widgets", "widgetContent", "apistage", "raFullList"}

func TestIssue378_D3_P1SeriesLeaveTheProcessOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "60")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "2") // L = min(60, 1) = 1s → window [1s, 2s)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	store := cache.ResolvedCache()
	rp, ok := any(store).(r378RefreshPutter)
	if !ok {
		t.Fatalf("#378 D3 RED: the store has no ReplaceIfGenRefresh, so nothing can re-mint and remint_total{class} cannot move")
	}

	put := func(class, name string, e *cache.ResolvedEntry) string {
		in := cache.ResolvedKeyInputs{CacheEntryClass: class, Namespace: "d3", Name: name}
		k := cache.ComputeKey(in)
		e.Inputs = &in
		store.Put(k, e)
		return k
	}
	// Re-mint population: class i gets i+1 cells → distinct per-class counts.
	var remint [][2]string
	for i, class := range r378RemintClasses {
		for j := 0; j <= i; j++ {
			remint = append(remint, [2]string{class, put(class, class+"-"+string(rune('a'+j)), &cache.ResolvedEntry{RawJSON: []byte(`1`)})})
		}
	}
	warm := put("widgets", "warm-cust", &cache.ResolvedEntry{RawJSON: []byte(`1`)})
	seeded := put("widgets", "warm-internal", &cache.ResolvedEntry{RawJSON: []byte(`1`), SeededAtBoot: true})
	keep := put("widgets", "warm-kept", &cache.ResolvedEntry{RawJSON: []byte(`1`)})
	ttlW := put("widgets", "ttl-warm", &cache.ResolvedEntry{RawJSON: []byte(`1`), TTLOverride: time.Second})
	t0 := time.Now()

	time.Sleep(500 * time.Millisecond)
	store.Get(ttlW)                    // customer-warm
	time.Sleep(700 * time.Millisecond) // t0+1.2s: inside the window; ttlW's 1s body has expired
	if _, ok := store.Get(ttlW); ok {
		t.Fatalf("SETUP: the 1s-override body must be TTL-evicted")
	}
	for _, r := range remint {
		in := cache.ResolvedKeyInputs{CacheEntryClass: r[0], Namespace: "d3"}
		if e, ok := store.GetNoTouch(r[1]); ok {
			in = *e.Inputs
		}
		if !rp.ReplaceIfGenRefresh(context.Background(), r[1], &cache.ResolvedEntry{RawJSON: []byte(`2`), Inputs: &in}, store.CaptureGen(r[1])) {
			t.Fatalf("SETUP: in-window refresh of %s refused", r[0])
		}
	}
	store.Get(warm)
	store.Get(keep)
	for time.Since(t0) < 3100*time.Millisecond { // past the cap; lifetime 3s > maxAge 2s
		time.Sleep(50 * time.Millisecond)
	}
	store.ReapPastMaxEntryAgeForTest() // oldest_warm_born_age_seconds over `keep` (warm, past cap)
	if _, ok := store.Get(warm); ok {
		t.Fatalf("SETUP: warm past-cap cell must be evicted on a customer GET")
	}
	if _, ok := store.GetNoTouch(seeded); ok {
		t.Fatalf("SETUP: seeded past-cap cell must be evicted on GetNoTouch")
	}

	rm := collectViaRealCallback(t, "deadbeef")
	all := flatten(rm)
	stat := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_resolved_cache") {
		stat[p.attrs["stat"]] = p.value
	}
	wantStats := map[string]func(int64) bool{
		"evict_max_age_warm_customer_total": func(v int64) bool { return v == 1 },
		"evict_max_age_warm_internal_total": func(v int64) bool { return v == 1 },
		"evict_ttl_warm_customer_total":     func(v int64) bool { return v == 1 },
		"oldest_warm_born_age_seconds":      func(v int64) bool { return v >= 3 },
		"remint_total":                      func(v int64) bool { return v == int64(len(remint)) },
	}
	for name, ok := range wantStats {
		v, present := stat[name]
		if !present {
			t.Errorf("#378 D3 RED: snowplow_resolved_cache{stat=%s} never left the process", name)
			continue
		}
		if !ok(v) {
			t.Errorf("#378 D3: snowplow_resolved_cache{stat=%s} = %d on OTLP (the mirror observed the wrong value)", name, v)
		}
	}

	byClass := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_resolved_cache_remint_total") {
		if len(p.attrs) != 1 {
			t.Errorf("#378 D3: remint point carries attributes %v, want exactly {class}", p.attrs)
		}
		byClass[p.attrs["class"]] = p.value
	}
	for i, class := range r378RemintClasses {
		v, present := byClass[class]
		if !present {
			t.Errorf("#378 D3 RED: snowplow_resolved_cache_remint_total{class=%s} is absent (the live query needs all 5 class series)", class)
			continue
		}
		if v != int64(i+1) {
			t.Errorf("#378 D3: remint_total{class=%s} = %d, want %d", class, v, i+1)
		}
	}
	if len(byClass) > len(r378RemintClasses) {
		t.Errorf("#378 D3: remint class set not closed: %v", byClass)
	}
	// Kind: a monotonic cumulative Sum (otel_metrics_sum), never a Gauge.
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if mt.Name != "snowplow_resolved_cache_remint_total" {
				continue
			}
			s, ok := mt.Data.(metricdata.Sum[int64])
			if !ok || !s.IsMonotonic || s.Temporality != metricdata.CumulativeTemporality {
				t.Errorf("#378 D3: snowplow_resolved_cache_remint_total is %T (monotonic=%v), want a monotonic cumulative Sum", mt.Data, ok && s.IsMonotonic)
			}
		}
	}
}
