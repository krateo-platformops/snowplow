package metrics

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// registerResolveCollectionListsInstrument hand-wires #988's wire-level
// resolve collection-LIST counter onto OTLP (no expvar->OTLP bridge): it reads
// the capped snapshot accessor whose uncapped map the
// snowplow_resolve_collection_lists expvar key publishes. Attributes are gvr
// and the closed reason enum, nothing else (F8). Registered only when the cache
// is on (CFG-1: the expvar key is published from a Disabled()-gated init).
func registerResolveCollectionListsInstrument(m metric.Meter) error {
	lists, err := m.Int64ObservableCounter("snowplow_resolve_collection_lists_total",
		metric.WithDescription("Collection LISTs (a LIST's first page or a watch-list start) that a resolve — a /call, the boot prewarm walk or seed, or the refresher — sent to the apiserver, counted on the wire by the transport every snowplow-held credential leaves through (#988), by gvr and fall-through reason. reason=unattributed: a resolve's LIST no decision site attributed. Reflector, store-verification and store-swap LISTs are never counted. #870 promises 0 for a GVR whose store is being swapped."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		cells, _ := cache.ResolveCollectionListCellsSnapshot()
		for _, c := range cells {
			o.ObserveInt64(lists, int64(c.Count), metric.WithAttributes(
				attribute.String("gvr", c.GVR), attribute.String("reason", c.Reason)))
		}
		return nil
	}, lists)
	return err
}
