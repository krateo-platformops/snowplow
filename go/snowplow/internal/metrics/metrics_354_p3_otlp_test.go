package metrics

// metrics_354_p3_otlp_test.go — #354 P3 D-OTLP. Every series the #354 driver
// queries read (scratchpad queries/gen.py DRIVER_EXPECTED, P3 block) is observed
// through the REAL instrument registration over a ManualReader, with values
// driven by the real store + refresher loop:
//
//   - hist:snowplow_refresher_dirty_to_fresh_ms — a synchronous Int64Histogram
//     with the explicit bounds [50 … 60000] ms (bracketing the 1 s north-star
//     and the 10 s AC-98.12 SLA), no attributes, count == the samples taken;
//   - gauge:snowplow_refresher_dirty_to_fresh_ms_p95 / _max;
//   - sum:snowplow_refresher{stat=dirty_to_fresh_samples | parked_ms |
//     dirty_ended_unfresh_{remarked,evicted,declined,dropped}};
//   - gauge:snowplow_resolved_cache{stat=stale_served_total |
//     stale_served_age_ms_max | warm_keyed_{page,extras}_<class>} (B3, six).
//
// The refresh handler is a stub doing what resolveAndPopulateL1 does at its
// terminal write (ReplaceIfGen + NoteRefreshWrite); the loop, the window map,
// the store and the instruments are production code. RED on main: the
// histogram and every stat are absent.

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue354_P3_DOTLP_DriverSeriesReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	cache.ResetRefresherForTest()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetRefresherForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}
	cache.Deps().SetStore(store)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}

	gvr := schema.GroupVersionResource{Group: "p3.example", Version: "v1", Resource: "things"}
	in := cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Group: gvr.Group, Version: gvr.Version,
		Resource: gvr.Resource, Namespace: "p3", Name: "o", BindingUID: "uid", Page: 2, PerPage: 10}
	key := cache.ComputeKey(in)
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	cache.Deps().Record(context.Background(), key, gvr, "p3", "o")

	gate := make(chan struct{})
	var done atomic.Int64
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, k string, in cache.ResolvedKeyInputs) error {
		<-gate
		rctx := cache.WithL1KeyContext(ctx, k)
		ok := store.ReplaceIfGen(rctx, k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":2}`), Inputs: &in}, store.CaptureGen(k))
		cache.NoteRefreshWrite(rctx, k, ok)
		done.Add(1)
		return nil
	})
	var hold atomic.Bool
	hold.Store(true)
	cache.SetCustomerInflightHook(hold.Load)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(ctx)

	cache.Deps().OnUpdate(gvr, "p3", "o")
	time.Sleep(120 * time.Millisecond) // parked behind the "customer"
	if _, ok := store.Get(key); !ok {  // a customer hit on the dirty cell: one stale serve
		t.Fatalf("PRE: the cell must be resident")
	}
	hold.Store(false)
	close(gate)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && done.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if done.Load() == 0 {
		t.Fatalf("PRE: the refresh never ran")
	}
	time.Sleep(50 * time.Millisecond)
	store.ReapPastMaxEntryAgeForTest() // B3 gauges are recomputed on the reaper walk

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	// --- the histogram ---
	wantBounds := []float64{50, 100, 250, 500, 750, 1000, 2000, 5000, 10000, 30000, 60000}
	var hist *metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "snowplow_refresher_dirty_to_fresh_ms" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Fatalf("snowplow_refresher_dirty_to_fresh_ms must be an Int64 histogram, got %T", m.Data)
			}
			if len(h.DataPoints) != 1 {
				t.Fatalf("the histogram must be ONE series (no attributes), got %d points", len(h.DataPoints))
			}
			hist = &h.DataPoints[0]
		}
	}
	if hist == nil {
		t.Fatalf("D-OTLP: hist:snowplow_refresher_dirty_to_fresh_ms is absent")
	}
	if !reflect.DeepEqual(hist.Bounds, wantBounds) {
		t.Fatalf("D-OTLP: histogram bounds %v, want %v", hist.Bounds, wantBounds)
	}
	if hist.Attributes.Len() != 0 {
		t.Fatalf("D-OTLP: the histogram must carry no attributes, got %v", hist.Attributes.ToSlice())
	}

	all := flatten(rm)
	ref := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_refresher") {
		ref[p.attrs["stat"]] = p.value
	}
	if got := ref["dirty_to_fresh_samples"]; got != 1 || int64(hist.Count) != got {
		t.Fatalf("D-OTLP: dirty_to_fresh_samples=%d, histogram count=%d — want 1 and equal", got, hist.Count)
	}
	for _, r := range []string{"remarked", "evicted", "declined", "dropped"} {
		if _, ok := ref["dirty_ended_unfresh_"+r]; !ok {
			t.Errorf("D-OTLP: sum:snowplow_refresher{stat=dirty_ended_unfresh_%s} is absent", r)
		}
	}
	if ref["parked_ms"] < 100 {
		t.Errorf("D-OTLP: sum:snowplow_refresher{stat=parked_ms}=%d, want >= 100 (the worker parked ~120 ms)", ref["parked_ms"])
	}
	for _, g := range []string{"snowplow_refresher_dirty_to_fresh_ms_p95", "snowplow_refresher_dirty_to_fresh_ms_max"} {
		pts := pointsFor(all, g)
		if len(pts) != 1 || pts[0].value <= 0 {
			t.Errorf("D-OTLP: gauge:%s must be one series > 0 after a sample, got %v", g, pts)
		}
	}
	rc := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_resolved_cache") {
		rc[p.attrs["stat"]] = p.value
	}
	if rc["stale_served_total"] != 1 {
		t.Errorf("D-OTLP: snowplow_resolved_cache{stat=stale_served_total}=%d, want 1", rc["stale_served_total"])
	}
	if rc["stale_served_age_ms_max"] < 100 {
		t.Errorf("D-OTLP: snowplow_resolved_cache{stat=stale_served_age_ms_max}=%d, want >= 100", rc["stale_served_age_ms_max"])
	}
	for _, s := range []string{
		"warm_keyed_page_restactions", "warm_keyed_page_widgets", "warm_keyed_page_ra_full_list",
		"warm_keyed_extras_restactions", "warm_keyed_extras_widgets", "warm_keyed_extras_ra_full_list",
	} {
		if _, ok := rc[s]; !ok {
			t.Errorf("D-OTLP: snowplow_resolved_cache{stat=%s} is absent", s)
		}
	}
	if rc["warm_keyed_page_restactions"] != 1 {
		t.Errorf("D-OTLP: warm_keyed_page_restactions=%d, want 1 (the page-keyed warm cell)", rc["warm_keyed_page_restactions"])
	}
}
