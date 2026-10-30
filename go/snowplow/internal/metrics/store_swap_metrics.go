package metrics

// store_swap_metrics.go — #870: the LABELLED build-and-swap instruments,
// hand-wired on OTLP (no expvar→OTLP bridge, #311). The scalar counters ride
// the tagged family snowplow_store_swap{stat=...} (cache.TaggedStatFamilies). The labelled families are closed sets
// (cache.StoreSwapReasons × StoreSwapOutcomes / StoreSwapCloses). The two
// per-entry histograms are synchronous, fed through cache's recorder hook.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func registerStoreSwapInstruments(m metric.Meter) error {
	keysTotal, err := m.Int64ObservableCounter("snowplow_store_swap_known_stale_keys_total",
		metric.WithDescription("Known-stale coordinates per store build, by reason {divergence, schema} and outcome {swap, abort_clean, abort_refused, abort_teardown}: the confirmed-divergent keys (divergence) and the keys the swap changed (schema). Each stays known-stale until the swap and is pushed at it (#870 owner ruling 3, N5)."))
	if err != nil {
		return err
	}
	buildsTotal, err := m.Int64ObservableCounter("snowplow_store_swap_known_stale_builds_total",
		metric.WithDescription("Store builds with at least one known-stale coordinate, by reason and outcome (#870)."))
	if err != nil {
		return err
	}
	windowTotal, err := m.Float64ObservableCounter("snowplow_store_swap_known_stale_window_seconds_total",
		metric.WithDescription("Known-stale window seconds by reason and outcome: from detection (or the CRD event) until no observed viewer was still owed a frame (#870)."),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}
	closesTotal, err := m.Int64ObservableCounter("snowplow_store_swap_ledger_closes_total",
		metric.WithDescription("Push-ledger entries closed, by reason and close {pushed, pushed_on_arm, pushed_resync, known_stale, evicted, live_event} (#870)."))
	if err != nil {
		return err
	}
	keysOpen, err := m.Int64ObservableGauge("snowplow_store_swap_known_stale_keys_open",
		metric.WithDescription("Known-stale coordinates in open windows, by reason (#870)."))
	if err != nil {
		return err
	}
	windowMax, err := m.Float64ObservableGauge("snowplow_store_swap_known_stale_window_open_seconds_max",
		metric.WithDescription("Age of the oldest open known-stale window, by reason. A window that never ends is visible here (#870)."),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}
	owed, err := m.Int64ObservableGauge("snowplow_store_swap_ledger_owed",
		metric.WithDescription("Push-ledger entries refreshed with no subscriber now, by reason. An expected outcome, not a detector (#870)."))
	if err != nil {
		return err
	}
	detect, err := m.Int64Histogram("snowplow_store_swap_detect_to_push_ms",
		metric.WithDescription("Per push-ledger entry: detection → close (ms), by reason and close (#870)."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(cache.StoreSwapHistBoundsMS...))
	if err != nil {
		return err
	}
	install, err := m.Int64Histogram("snowplow_store_swap_install_to_push_ms",
		metric.WithDescription("Per push-ledger entry: install → close (ms), by reason and close; the same shape as snowplow_apistage_stale_to_push_ms (#870)."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(cache.StoreSwapHistBoundsMS...))
	if err != nil {
		return err
	}
	byGVR, err := m.Int64ObservableCounter("snowplow_store_swaps_by_gvr_total",
		metric.WithDescription("Store builds by GVR and outcome {installed, superseded, abort_clean, abort_refused, abort_teardown}: installed = the GVR was swapped (its new copy delivered M). The gvr label is GroupVersionResource.String(); no identity (#870, #928)."))
	if err != nil {
		return err
	}
	cache.SetStoreSwapCloseRecorder(func(kind, reason, close string, ms int64) {
		set := metric.WithAttributes(attribute.String("reason", reason), attribute.String("close", close))
		if kind == "detect" {
			detect.Record(context.Background(), ms, set)
			return
		}
		install.Record(context.Background(), ms, set)
	})
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := cache.StoreSwapKnownStaleSnapshot()
		ro := func(c cache.StoreSwapLabelled) metric.ObserveOption {
			return metric.WithAttributes(attribute.String("reason", c.Reason), attribute.String("outcome", c.Label))
		}
		for _, c := range s.Keys {
			o.ObserveInt64(keysTotal, int64(c.Value), ro(c))
		}
		for _, c := range s.Builds {
			o.ObserveInt64(buildsTotal, int64(c.Value), ro(c))
		}
		for _, c := range s.WindowMS {
			o.ObserveFloat64(windowTotal, float64(c.Value)/1000, ro(c))
		}
		for _, c := range s.LedgerCloses {
			o.ObserveInt64(closesTotal, int64(c.Value),
				metric.WithAttributes(attribute.String("reason", c.Reason), attribute.String("close", c.Label)))
		}
		for _, g := range s.KeysOpen {
			o.ObserveInt64(keysOpen, int64(g.Value), metric.WithAttributes(attribute.String("reason", g.Reason)))
		}
		for _, g := range s.WindowOpenMaxS {
			o.ObserveFloat64(windowMax, g.Value, metric.WithAttributes(attribute.String("reason", g.Reason)))
		}
		for _, g := range s.LedgerOwed {
			o.ObserveInt64(owed, int64(g.Value), metric.WithAttributes(attribute.String("reason", g.Reason)))
		}
		for _, c := range cache.StoreSwapsByGVRSnapshot() {
			o.ObserveInt64(byGVR, int64(c.Value),
				metric.WithAttributes(attribute.String("gvr", c.GVR), attribute.String("outcome", c.Outcome)))
		}
		return nil
	}, keysTotal, buildsTotal, windowTotal, closesTotal, keysOpen, windowMax, owed, byGVR)
	return err
}
