package rum

import (
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/snowplow/internal/otelresource"
	"github.com/krateo-platformops/snowplow/internal/tracing"
)

// CollectorURL is where accepted spans are forwarded, and whether the ingest
// is enabled at all. It follows the same OTel enable contract as snowplow's
// own traces, with no knob of its own: on only when tracing resolves to
// enabled (OTEL_TRACING_ENABLED, defaulting to OTEL_ENABLED) AND
// OTEL_EXPORTER_OTLP_ENDPOINT names a collector. The URL is that endpoint's
// /v1/traces, exactly what tracing.Setup exports to. Off, the route answers
// 404 to everyone, so a cluster without a collector exposes nothing.
func CollectorURL() (string, bool) {
	if !tracing.Enabled() {
		return "", false
	}
	ep := env.String(tracing.EnvOTLPEndpoint, "")
	if ep == "" {
		return "", false
	}
	return otelresource.SignalEndpointURL(ep, otelresource.TracesPath), true
}
