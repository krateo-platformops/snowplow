package metrics

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/telemetry/rum"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// registerRUMIngestInstruments hand-wires #1015's browser RUM ingest counters
// onto OTLP (no expvar->OTLP bridge): each reads rum.Read, the accessor the
// snowplow_rum_ingest expvar key reads, so the two surfaces cannot diverge.
// The only attribute is the closed `outcome` enum (a key from the F8 closed set); no user, route or span name.
// Registered before the cache.Disabled() return: the ingest does not depend on
// the cache.
func registerRUMIngestInstruments(m metric.Meter) error {
	requests, err := m.Int64ObservableCounter("snowplow_rum_ingest_requests_total",
		metric.WithDescription("POST /telemetry/v1/traces requests (#1015) by outcome: accepted (decoded and filtered), dropped_auth (401), dropped_size (413), dropped_rate (429, per user), dropped_content_type (415), dropped_malformed (400), dropped_busy (429: every decode slot, across all users, in use)."))
	if err != nil {
		return err
	}
	spans, err := m.Int64ObservableCounter("snowplow_rum_ingest_spans_total",
		metric.WithDescription("Browser RUM spans (#1015) by outcome: accepted (passed the allowlist), dropped_service (resource service.name not krateo-frontend), dropped_name (span name not allowlisted), dropped_invalid (bad ids, times, kind or status), forwarded (collector 2xx), forward_failed (collector error or non-2xx), forward_overflow (forward queue full, dropped without blocking), forward_rate_limited (over the global spans/s cap into the collector; the request got 429)."))
	if err != nil {
		return err
	}
	attrs, err := m.Int64ObservableCounter("snowplow_rum_ingest_attributes_dropped_total",
		metric.WithDescription("Browser RUM span attributes (#1015) dropped by the allowlist: any key other than http.route, http.request.method and http.response.status_code, or one of those with a value it does not allow (a query string, a token-shaped segment, a non-method, a non-status)."))
	if err != nil {
		return err
	}
	routes, err := m.Int64ObservableCounter("snowplow_rum_ingest_widget_routes_unregistered_total",
		metric.WithDescription("krateo.widget.fetch http.route values (#1020 F3) dropped because they are not one of snowplow's own registered routes; a subset of snowplow_rum_ingest_attributes_dropped_total. Non-zero means the frontend sends a path or a route snowplow does not serve."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := rum.Read()
		for outcome, n := range s.Requests {
			o.ObserveInt64(requests, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
		}
		for outcome, n := range s.Spans {
			o.ObserveInt64(spans, int64(n), metric.WithAttributes(attribute.String("outcome", outcome)))
		}
		o.ObserveInt64(attrs, int64(s.Attributes))
		o.ObserveInt64(routes, int64(s.UnregisteredWidgetRoutes))
		return nil
	}, requests, spans, attrs, routes)
	return err
}
