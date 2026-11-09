package metrics

// customer_serves_metrics.go — #1265 P0: the served-cold counter on OTLP,
// hand-wired (no expvar→OTLP bridge). The observables read the same snapshots
// /debug/vars renders (dispatchers.CustomerServesSnapshot,
// CustomerColdServesSnapshot), so the two surfaces cannot diverge. Attributes
// are closed sets: handler, outcome, class_warm and cause; never an identity, a
// key or a resource name. Registered whatever the cache mode, like the expvar
// keys: with the cache off every serve is cold{cache_off}.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func registerCustomerServeInstruments(m metric.Meter) error {
	serves, err := m.Int64ObservableCounter("snowplow_customer_serves_total",
		metric.WithDescription("Top-level customer /call serves, once each, by handler {restactions, widgets}, outcome {fresh: a resident cell; stale: a resident cell whose inputs moved or that was stored from a stale input; cold: a live resolve the user waited on; refused: ended before the serve decision} and class_warm (the requester's sign-in warm state; \"none\" until #1265 P3). Refresher, seed, prewarm, demand revalidation, stale re-run, export pages and nested self-loopbacks are never counted. Zero cold navigation reads cold == 0. #1265."))
	if err != nil {
		return err
	}
	cold, err := m.Int64ObservableCounter("snowplow_customer_serves_cold_total",
		metric.WithDescription("The cold serves of snowplow_customer_serves_total, by handler and cause {no_cell: the lookup found no cell (never stored, declined, evicted, or a D2 stale-input resolve); expired_ttl / evicted_maxage: this lookup found the cell past its TTL / the max entry age; ineligible_key: no servable key (\"\" BindingUID, no identity); dry_run: an inert request; cache_off: no resolved-output store}. #1265."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, c := range dispatchers.CustomerServesSnapshot() {
			o.ObserveInt64(serves, int64(c.Count), metric.WithAttributes(
				attribute.String("handler", c.Handler),
				attribute.String("outcome", c.Outcome),
				attribute.String("class_warm", c.ClassWarm)))
		}
		for _, c := range dispatchers.CustomerColdServesSnapshot() {
			o.ObserveInt64(cold, int64(c.Count), metric.WithAttributes(
				attribute.String("handler", c.Handler),
				attribute.String("cause", c.Cause)))
		}
		return nil
	}, serves, cold)
	return err
}
