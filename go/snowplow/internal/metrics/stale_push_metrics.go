package metrics

// stale_push_metrics.go — #1062: stale serve → the replacing frame pushed, for
// EVERY serving layer (cache/stale_push.go), hand-wired on OTLP (no
// expvar→OTLP bridge). The observables read cache.StalePushStatsSnapshot, the
// same snapshot /debug/vars renders; the histogram is synchronous, fed through
// the ledger's recorder hook. Attributes are closed sets: layer
// (cache.StaleLayers) and close (cache.StalePushCloses). Registered only when
// the cache is on (from registerStaleServeInstruments).

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// registerStalePushInstruments registers the ledger family. apistageAlias is
// snowplow_apistage_stale_to_push_ms: every apistage sample is recorded into
// it as well, so the #710 name keeps reading the same samples.
func registerStalePushInstruments(m metric.Meter, apistageAlias metric.Int64Histogram) error {
	toPush, err := m.Int64Histogram("snowplow_stale_to_push_ms",
		metric.WithDescription("Stale serve → the replacing /refreshes frame WRITTEN to a stream armed for the served key (ms), by the layer that served the stale bytes {apistage, l1}. The owner bar (#710): p95 ≤ 3000, max ≤ 10000, target 1000. #1062."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(cache.StaleToPushBoundsMS)...))
	if err != nil {
		return err
	}
	cache.SetStaleToPushRecorder(func(layer string, ms int64) {
		toPush.Record(context.Background(), ms, metric.WithAttributes(attribute.String("layer", layer)))
		if layer == cache.StaleLayerAPIStage {
			apistageAlias.Record(context.Background(), ms)
		}
	})
	serves, err := m.Int64ObservableCounter("snowplow_stale_serves_total",
		metric.WithDescription("Armed customer responses written with stale bytes, by layer: apistage (a stale-derived body) or l1 (an L1 hit on a dirty or known-stale cell). Each is timed until its replacing frame is written. #1062."))
	if err != nil {
		return err
	}
	unreplaced, err := m.Int64ObservableCounter("snowplow_stale_unreplaced_total",
		metric.WithDescription("Stale serves not replaced within the 10 s goal max, by layer, counted once each: pushed after more than 10 s, or still unpushed 10 s after the serve while a /refreshes stream is armed for the key. A serve no stream views is not counted. Alert on rate > 0. #1062."))
	if err != nil {
		return err
	}
	closes, err := m.Int64ObservableCounter("snowplow_stale_serve_closes_total",
		metric.WithDescription("Stale serves that left the ledger, by layer and close {pushed: the replacing frame was written; dropped: the L1-capacity bound, before any push}. #1062."))
	if err != nil {
		return err
	}
	open, err := m.Int64ObservableGauge("snowplow_stale_serves_open",
		metric.WithDescription("Stale serves still waiting for their replacing frame, by layer (including serves no stream has armed: nothing can push to them). #1062."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, s := range cache.StalePushStatsSnapshot() {
			layer := attribute.String("layer", s.Layer)
			o.ObserveInt64(serves, int64(s.Serves), metric.WithAttributes(layer))
			o.ObserveInt64(unreplaced, int64(s.Unreplaced), metric.WithAttributes(layer))
			o.ObserveInt64(open, s.Open, metric.WithAttributes(layer))
			for _, c := range cache.StalePushCloses {
				o.ObserveInt64(closes, int64(s.Closes[c]), metric.WithAttributes(layer, attribute.String("close", c)))
			}
		}
		return nil
	}, serves, unreplaced, closes, open)
	return err
}
