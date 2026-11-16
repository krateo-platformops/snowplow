package metrics

// refresh_cycle_metrics.go — #1070: the refresh cycle's stages on OTLP, the
// same values expvar publishes (cache/refresh_cycle.go). The Put counter is an
// observable mirror of cache's counters; the three histograms are synchronous,
// fed through cache's recorder hook. Every label is a closed set.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func registerRefreshCycleInstruments(m metric.Meter) error {
	puts, err := m.Int64ObservableCounter("snowplow_refresh_cycle_put_total",
		metric.WithDescription("Accepted refresh terminal Puts, by entry class, refresher tier {viewed, normal, cluster_list, direct} and content {identical, changed}: whether the Put replaced a prior with the same bytes (#1070)."))
	if err != nil {
		return err
	}
	bounds := make([]float64, len(cache.RefreshCycleBoundsMS))
	for i, b := range cache.RefreshCycleBoundsMS {
		bounds[i] = float64(b)
	}
	hist := func(name, desc string) (metric.Int64Histogram, error) {
		return m.Int64Histogram(name, metric.WithDescription(desc), metric.WithUnit("ms"),
			metric.WithExplicitBucketBoundaries(bounds...))
	}
	wait, err := hist("snowplow_refresh_cycle_wait_ms",
		"Wait for a refresher worker (ms), by tier: the key's latest insert into its tier until the worker starts the refresh, after the customer yield and the rate floor (#1070).")
	if err != nil {
		return err
	}
	duration, err := hist("snowplow_refresh_cycle_duration_ms",
		"Refresh duration (ms), by tier: the worker's start until the accepted refresh Put (#1070).")
	if err != nil {
		return err
	}
	toFrame, err := hist("snowplow_refresh_broadcaster_put_to_frame_ms",
		"Refresh Put to the first refresh frame for that key written on each open /refreshes stream (ms), by the refresh's tier (#1070).")
	if err != nil {
		return err
	}
	cache.SetRefreshCycleRecorder(func(kind, tier string, ms int64) {
		set := metric.WithAttributes(attribute.String("tier", tier))
		switch kind {
		case "wait":
			wait.Record(context.Background(), ms, set)
		case "duration":
			duration.Record(context.Background(), ms, set)
		case "put_to_frame":
			toFrame.Record(context.Background(), ms, set)
		}
	})
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, p := range cache.RefreshCycleStatsSnapshot().Puts {
			o.ObserveInt64(puts, int64(p.N), metric.WithAttributes(
				attribute.String("class", p.Class), attribute.String("tier", p.Tier), attribute.String("content", p.Content)))
		}
		return nil
	}, puts)
	return err
}
