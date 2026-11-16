package metrics

// nested_loopback_guard_metrics.go — #1274 review: the HTTP-edge recursion
// guard's stops on OTLP, hand-wired (no expvar→OTLP bridge). The observable
// reads cache.NestedLoopbackGuardStopsSnapshot, the snapshot /debug/vars
// renders. The one attribute is the closed kind set
// (cache.NestedLoopbackGuardStopKinds); never a node, a RESTAction name or an
// identity. Registered only when the cache is on (CFG-1: the expvar key is
// published from a Disabled()-gated init).

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func registerNestedLoopbackGuardInstruments(m metric.Meter) error {
	stops, err := m.Int64ObservableCounter("snowplow_nested_loopback_guard_stops_total",
		metric.WithDescription("Self-loopback /call hops the HTTP-edge recursion guard stopped, by kind {cycle: the hop's own node was already on its descent path, so the raw CR was served instead of recursing (the designed stop for a self-referential RESTAction); depth: the depth-8 backstop answered 508 (a non-cyclic chain too deep, a defect to investigate)}. Both kinds are present at 0. #1274."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		snap := cache.NestedLoopbackGuardStopsSnapshot()
		for _, k := range cache.NestedLoopbackGuardStopKinds {
			o.ObserveInt64(stops, int64(snap[k]), metric.WithAttributes(attribute.String("kind", k)))
		}
		return nil
	}, stops)
	return err
}
