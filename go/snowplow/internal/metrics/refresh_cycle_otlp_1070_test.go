package metrics

// refresh_cycle_otlp_1070_test.go — #1070, the OTLP half: the refresh cycle's
// Put counter and three histograms reach OTLP through the REAL registration
// (registerInstruments) with the same values expvar publishes, per tier and
// per class|tier|content. The samples are moved by the production path — a
// queued refresh through the real refresher and a direct one through
// RunRefreshNow, each with the refresher terminal write and its publish, and
// a frame written on an open stream — on an injected clock, never set.

import (
	"context"
	"encoding/json"
	"expvar"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRefreshCycle1070_OTLPAndExpvarAgree(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	cache.ResetRefreshCycleForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(func() {
		cache.ResetRefresherForTest()
		cache.ResetRefreshBroadcasterForTest()
		cache.ResetRefreshCycleForTest()
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
	})
	cache.RegisterRefreshCycleExpvarForTest()
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC).UnixNano())
	t.Cleanup(cache.SetDirtyWindowClockForTest(func() time.Time { return time.Unix(0, clock.Load()) }))
	ms := func(n int64) { clock.Add(n * int64(time.Millisecond)) }

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "test"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	t.Cleanup(func() { cache.SetRefreshCycleRecorder(nil) })

	c := cache.ResolvedCache()
	if c == nil {
		t.Fatal("PRE: resolved cache disabled")
	}
	type cell struct {
		in        cache.ResolvedKeyInputs
		body      string
		resolveMS int64
	}
	cells := map[string]*cell{}
	keyOf := func(name, body string, resolveMS int64) string {
		in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "otlp1070", Name: name}
		k := cache.ComputeKey(in)
		c.PutResolved(fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &in})
		cells[k] = &cell{in: in, body: body, resolveMS: resolveMS}
		return k
	}
	queued := keyOf("queued", `{"v":"old"}`, 2500) // identical
	direct := keyOf("direct", `{"v":"new"}`, 700)  // changed
	st := cache.SubscribeRefreshStream(map[string]struct{}{queued: {}, direct: {}})
	t.Cleanup(st.Unsub)
	done := make(chan struct{}, 2)
	cache.RegisterRefreshFunc("widgets", func(ctx context.Context, k string, _ cache.ResolvedKeyInputs) error {
		x := cells[k]
		ms(x.resolveMS)
		ctx = cache.WithL1KeyContext(ctx, k)
		in := x.in
		if c.ReplaceResolvedIfGenRefresh(ctx, fixtureStoreVerdict, k, &cache.ResolvedEntry{RawJSON: []byte(x.body), Inputs: &in}, c.CaptureGen(k)) {
			cache.PublishRefreshed(ctx, k)
		}
		done <- struct{}{}
		return nil
	})
	frame := func(want string, afterMS int64) {
		t.Helper()
		select {
		case k := <-st.Keys:
			if k != want {
				t.Fatalf("PRE: frame for an unexpected key")
			}
			ms(afterMS)
			st.Wrote(k)
		case <-time.After(30 * time.Second):
			t.Fatal("PRE: no frame offered")
		}
	}

	cache.EnqueueRefresh(queued) // inserted now
	ms(1500)                     // waits 1500 ms for a worker
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache.StartRefresher(ctx)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("PRE: the queued refresh never ran")
	}
	frame(queued, 1200)
	if err := cache.RunRefreshNow(context.Background(), direct); err != nil {
		t.Fatalf("PRE: RunRefreshNow: %v", err)
	}
	<-done
	frame(direct, 4000)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	hists := map[string]map[string]metricdata.HistogramDataPoint[int64]{}
	puts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "snowplow_refresh_cycle_wait_ms", "snowplow_refresh_cycle_duration_ms", "snowplow_refresh_broadcaster_put_to_frame_ms":
				d, ok := m.Data.(metricdata.Histogram[int64])
				if !ok {
					t.Fatalf("%s is %T, want an int64 histogram", m.Name, m.Data)
				}
				hists[m.Name] = map[string]metricdata.HistogramDataPoint[int64]{}
				for _, dp := range d.DataPoints {
					tier, _ := dp.Attributes.Value("tier")
					hists[m.Name][tier.Emit()] = dp
				}
			case "snowplow_refresh_cycle_put_total":
				d, ok := m.Data.(metricdata.Sum[int64])
				if !ok || !d.IsMonotonic {
					t.Fatalf("%s is %T, want a monotonic int64 sum", m.Name, m.Data)
				}
				for _, dp := range d.DataPoints {
					cl, _ := dp.Attributes.Value("class")
					tr, _ := dp.Attributes.Value("tier")
					k, _ := dp.Attributes.Value("content")
					puts[cl.Emit()+"|"+tr.Emit()+"|"+k.Emit()] = dp.Value
				}
			}
		}
	}

	// The counter: OTLP == expvar, cell by cell, over the closed set.
	var ev map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_refresh_cycle_put_total").String()), &ev); err != nil {
		t.Fatal(err)
	}
	if len(puts) != len(ev) || len(ev) == 0 {
		t.Fatalf("RED: OTLP put_total has %d series, expvar %d", len(puts), len(ev))
	}
	for k, v := range ev {
		if puts[k] != int64(v) {
			t.Fatalf("RED: put_total %s: OTLP %d, expvar %d", k, puts[k], v)
		}
	}
	if ev["widgets|normal|identical"] != 1 || ev["widgets|direct|changed"] != 1 {
		t.Fatalf("RED: put_total = %v, want widgets|normal|identical and widgets|direct|changed at 1", ev)
	}

	// The histograms: OTLP == expvar per tier — count, sum and every bucket.
	type evHist struct {
		Buckets map[string]uint64 `json:"buckets"`
		Count   uint64            `json:"count"`
		SumMS   uint64            `json:"sum_ms"`
	}
	want := map[string]map[string]uint64{ // name → tier → the one sample
		"snowplow_refresh_cycle_wait_ms":               {cache.RefreshTierNormal: 1500},
		"snowplow_refresh_cycle_duration_ms":           {cache.RefreshTierNormal: 2500, cache.RefreshTierDirect: 700},
		"snowplow_refresh_broadcaster_put_to_frame_ms": {cache.RefreshTierNormal: 1200, cache.RefreshTierDirect: 4000},
	}
	for name, sums := range want {
		var m map[string]evHist
		if err := json.Unmarshal([]byte(expvar.Get(name).String()), &m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for tier, sum := range sums {
			dp, ok := hists[name][tier]
			e := m[tier]
			if !ok {
				t.Fatalf("RED: OTLP %s has no tier=%s series (expvar count=%d)", name, tier, e.Count)
			}
			if e.Count != 1 || e.SumMS != sum || uint64(dp.Sum) != e.SumMS || dp.Count != e.Count {
				t.Fatalf("RED: %s tier=%s: OTLP count=%d sum=%d, expvar count=%d sum=%d, want one sample of %d", name, tier, dp.Count, dp.Sum, e.Count, e.SumMS, sum)
			}
			if len(dp.Bounds) != len(cache.RefreshCycleBoundsMS) {
				t.Fatalf("RED: %s bounds %v, want %v", name, dp.Bounds, cache.RefreshCycleBoundsMS)
			}
			var cum uint64
			for i, b := range cache.RefreshCycleBoundsMS {
				cum += dp.BucketCounts[i]
				if got := e.Buckets["le_"+strconv.FormatInt(b, 10)]; got != cum {
					t.Fatalf("RED: %s tier=%s le_%d: expvar %d, OTLP cumulative %d", name, tier, b, got, cum)
				}
			}
		}
		for tier, dp := range hists[name] {
			if _, wanted := sums[tier]; !wanted && dp.Count != 0 {
				t.Fatalf("RED: %s tier=%s has %d samples on OTLP, none were made", name, tier, dp.Count)
			}
		}
	}
}
