package metrics

// metrics_455_wildcard_digest_otlp_test.go — #455: the three #368 wildcard
// digest-collision instruments were created and observed but never passed to
// RegisterCallback, so the SDK dropped every observation ("observable instrument
// not registered for callback") and the v7 certification rule
// (collision == 0 AND observed > 0 AND evicted == 0) could not be read from
// ClickStack.
//
// Two arms, both RED on main 65d7edd3:
//   - the value arm, folded into TestC7_OTLP_EveryDerivedStatLeavesTheProcess
//     (c7Seed455 / c7Assert455): the probe is driven through its production
//     recorder to collision=2, observed=7, evicted=3, and each series must arrive
//     on the OTLP/HTTP wire with that value;
//   - TestIssue455_EveryObservedInstrumentIsRegistered: structural, over the
//     whole callback. It captures the SDK's own global error log and fails on
//     any "not registered for callback", so the next instrument added to the
//     callback but left out of the observable list fails here by name.

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/stdr"
	"go.opentelemetry.io/otel"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

const (
	wc455Collisions    = 2
	wc455ObservedClean = 5
	wc455Evictions     = 3
)

// c7Seed455 drives the #368 probe through its production recorder and returns
// the expected wire value of each series.
func c7Seed455(t *testing.T) map[string]int64 {
	t.Helper()
	restore := dispatchers.DriveWildcardDigestProbeForTest(wc455Collisions, wc455ObservedClean, wc455Evictions)
	t.Cleanup(restore)
	// The recorder itself must agree before the wire is read (a seam that
	// drove nothing would make the wire arm vacuous).
	c, o, e := dispatchers.ShadowWildcardDigestCounts()
	if c != wc455Collisions || o != wc455Collisions+wc455ObservedClean || e != wc455Evictions {
		t.Fatalf("#455 setup: probe counters collision=%d observed=%d evicted=%d, want %d/%d/%d",
			c, o, e, wc455Collisions, wc455Collisions+wc455ObservedClean, wc455Evictions)
	}
	return map[string]int64{
		"snowplow_v7_shadow_wildcard_digest_collision_total": wc455Collisions,
		"snowplow_v7_shadow_wildcard_digest_observed_total":  wc455Collisions + wc455ObservedClean,
		"snowplow_v7_shadow_wildcard_digest_evicted_total":   wc455Evictions,
	}
}

// c7Assert455 requires each #368 series on the wire with its driven value.
func c7Assert455(t *testing.T, exports []capturedExport, want map[string]int64) {
	t.Helper()
	for name, w := range want {
		got, found := c7Find(exports, name, "")
		if !found {
			t.Errorf("#455: %s never left the process (instrument observed but not registered with the callback?)", name)
			continue
		}
		if int64(got) != w {
			t.Errorf("#455: %s = %v on the wire, want %d", name, got, w)
		}
		if k := c7Kind455(exports, name); k != "monotonic-cumulative-sum" {
			t.Errorf("#455: %s is exported as %s, want a monotonic CUMULATIVE Sum (an ObservableCounter): "+
				"a Gauge lands in otel_metrics_gauge where the certification query reads otel_metrics_sum", name, k)
		}
	}
}

// c7Kind455 names the OTLP data kind of instrument name in the export.
func c7Kind455(exports []capturedExport, name string) string {
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if mt.GetName() != name {
						continue
					}
					switch d := mt.GetData().(type) {
					case *metricspb.Metric_Sum:
						if d.Sum.GetIsMonotonic() &&
							d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
							return "monotonic-cumulative-sum"
						}
						return fmt.Sprintf("sum(monotonic=%v,%s)", d.Sum.GetIsMonotonic(), d.Sum.GetAggregationTemporality())
					case *metricspb.Metric_Gauge:
						return "gauge"
					default:
						return fmt.Sprintf("%T", d)
					}
				}
			}
		}
	}
	return "absent"
}

// TestIssue455_EveryObservedInstrumentIsRegistered — every instrument the
// callbacks observe is registered with them. The SDK reports an unregistered
// observation only through the otel global logger, so the arm installs a
// capturing logger for one collection.
func TestIssue455_EveryObservedInstrumentIsRegistered(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	var mu sync.Mutex
	var dropped []string
	otel.SetLogger(funcr.New(func(prefix, args string) {
		if strings.Contains(args, "not registered for callback") {
			mu.Lock()
			dropped = append(dropped, args)
			mu.Unlock()
		}
	}, funcr.Options{}))
	// Restore the SDK's own default (internal/global: stdr over a stderr log.Logger).
	t.Cleanup(func() { otel.SetLogger(stdr.New(log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile))) })

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	if len(all) == 0 {
		t.Fatal("non-exercise guard: the callback emitted nothing")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, d := range dropped {
		t.Errorf("#455: the SDK dropped an observation of an instrument not registered with its callback: %s", d)
	}
}
