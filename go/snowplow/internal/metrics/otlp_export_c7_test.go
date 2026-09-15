// otlp_export_c7_test.go — 1.12.6 C7: the OTLP half of the four-surface
// parity arm. For every tagged stats family (cache.TaggedStatFamilies) set
// every field to a distinct value, run one real export through the OTLP/HTTP
// exporter, and assert each derived stat arrived under the instrument name
// the family's naming rule predicts, with that value.
//
// The expected set is asked of the TYPE, never listed here: a field added to
// a snapshot struct with a tag and no mirror entry fails this arm; a field
// added without a tag fails the cache-side arm. RED on the commit that adds
// the tags without converting the hand-written mirror: the four
// relist_bridge_* stats never leave the process.
package metrics

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// c7Find returns the exported value of instrument name; for a shared
// (stat-labelled) instrument the data point carrying stat=<stat>. Sums and
// Gauges both count; int and double both count.
func c7Find(exports []capturedExport, name, stat string) (val float64, found bool) {
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if mt.GetName() != name {
						continue
					}
					var dps []*metricspb.NumberDataPoint
					switch d := mt.GetData().(type) {
					case *metricspb.Metric_Sum:
						dps = d.Sum.GetDataPoints()
					case *metricspb.Metric_Gauge:
						dps = d.Gauge.GetDataPoints()
					}
					for _, dp := range dps {
						if stat != "" {
							match := false
							for _, kv := range dp.GetAttributes() {
								if kv.GetKey() == "stat" && kv.GetValue().GetStringValue() == stat {
									match = true
								}
							}
							if !match {
								continue
							}
						}
						switch v := dp.GetValue().(type) {
						case *metricspb.NumberDataPoint_AsInt:
							return float64(v.AsInt), true
						case *metricspb.NumberDataPoint_AsDouble:
							return v.AsDouble, true
						}
					}
				}
			}
		}
	}
	return 0, false
}

// c7SetDistinct sets every tagged field of a snapshot struct to base+i.
func c7SetDistinct(ptr any, specs []cache.StatSpec, base int64) map[string]int64 {
	v := reflect.ValueOf(ptr).Elem()
	want := map[string]int64{}
	for i, s := range specs {
		f := v.FieldByName(s.Field)
		n := base + int64(i) + 1
		switch f.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(n)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(uint64(n))
		case reflect.Float32, reflect.Float64:
			f.SetFloat(float64(n))
		}
		want[s.Stat] = n
	}
	return want
}

func TestC7_OTLP_EveryDerivedStatLeavesTheProcess(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")

	fams := cache.TaggedStatFamilies()
	want := map[string]map[string]int64{} // family name -> stat -> value

	crd := &cache.CRDDiscoveryStats{}
	want["CRDDiscoveryStats"] = c7SetDistinct(crd, fams[0].Specs, 1000)
	cache.SetCRDDiscoveryStatsForTest(crd)
	t.Cleanup(func() { cache.SetCRDDiscoveryStatsForTest(nil) })

	rb := &cache.RefreshBroadcasterStats{}
	want["RefreshBroadcasterStats"] = c7SetDistinct(rb, fams[1].Specs, 2000)
	cache.SetRefreshBroadcasterStatsForTest(rb)
	t.Cleanup(func() { cache.SetRefreshBroadcasterStatsForTest(nil) })

	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	cache.AddRefreshTerminalCountersForTest(3001, 3002, 3003, 3004, 7)
	wantR := map[string]int64{"drop_evict": 3001, "drop_evict_suspended": 3002, "suppressed_set": 3003, "suppressed_skips": 3004, "suppressed_keys": 7}
	for i, s := range fams[2].Specs {
		if _, ok := wantR[s.Stat]; ok || s.Kind == "gauge" {
			continue // queue_depth is a live Len(): presence only
		}
		n := uint64(3100 + i)
		if !cache.AddRefresherPoolCounterForTest(s.Stat, n) {
			t.Fatalf("refresher stat %q has no test bump", s.Stat)
		}
		wantR[s.Stat] = int64(n)
	}
	want["refresherStats"] = wantR

	ctx := context.Background()
	shutdown, err := Setup(ctx, "deadbeef")
	if err != nil {
		t.Fatalf("metrics.Setup: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(flushCtx); err != nil {
		t.Fatalf("flush/shutdown: %v", err)
	}
	exports := rcv.snapshot()
	if len(exports) == 0 {
		t.Fatal("NOTHING was exported: no OTLP request reached the receiver")
	}

	for _, f := range fams {
		w := want[f.Name()]
		for _, s := range f.Specs {
			name := f.OTelInstrumentName(s)
			stat := ""
			if f.OTelShared(s) {
				stat = s.Stat
			}
			got, found := c7Find(exports, name, stat)
			if !found {
				t.Errorf("%s: stat %q (field %s) never left the process — no %q%s in the export (exported: %v)",
					f.Name(), s.Stat, s.Field, name, map[bool]string{true: "{stat=" + stat + "}", false: ""}[stat != ""],
					exportedNames(exports))
				continue
			}
			wv, ok := w[s.Stat]
			if !ok {
				continue // presence-only stat (live gauge)
			}
			if int64(got) != wv {
				t.Errorf("%s: %s%s = %v; want %d — the mirror observed the wrong field", f.Name(), name,
					map[bool]string{true: "{stat=" + stat + "}", false: ""}[stat != ""], got, wv)
			}
		}
	}
}
