// Package tracing wires snowplow's OpenTelemetry trace pipeline.
//
// COEXISTENCE CONTRACT (load-bearing): this package is ADDITIVE and must
// not interfere with the existing observability surfaces. Specifically:
//
//   - The shortid `X-Krateo-TraceId` correlation id (plumbing
//     xcontext.TraceId, surfaced as status.traceId, set as an outbound
//     header, and consumed by /refreshes) is UNTOUCHED. OTel spans carry a
//     separate W3C `traceparent`; the two coexist on the same request.
//
//   - The stdout -> otel-daemonset filelog -> ClickHouse `otel_logs` log
//     pipeline (slog JSON to os.Stdout) is UNTOUCHED. OTel adds
//     trace_id/span_id ATTRIBUTES to those log records for correlation but
//     does not reroute or replace the pipeline.
//
// GATING: Setup is a no-op unless tracing resolves to enabled. Tracing is
// gated by OTEL_TRACING_ENABLED, which DEFAULTS to the value of the
// OTEL_ENABLED master switch when unset — so OTEL_ENABLED=true turns
// tracing on, and a per-signal OTEL_TRACING_ENABLED still overrides. When
// the gate is off, Setup registers NOTHING — no TracerProvider, no
// propagator, no exporter — so the global otel.GetTracerProvider() stays
// the no-op default and every instrumentation site (otelhttp handler,
// otelhttp transport, span lookups) degrades to a zero-cost no-op. The
// off-path is therefore byte-identical to the pre-OTel binary.
//
// This matches the canonical platform OTel enablement contract (the
// chart-inspector internal/telemetry ConfigFromEnv and the runtimes):
//
//	OTEL_ENABLED            master switch                (default false)
//	OTEL_TRACING_ENABLED    gate tracing  (default: value of OTEL_ENABLED)
//	OTEL_METRICS_ENABLED    gate metrics  (default: value of OTEL_ENABLED)
//	OTEL_EXPORTER_OTLP_ENDPOINT   collector OTLP/HTTP endpoint
package tracing

import (
	"context"
	"net/http"
	"time"

	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/otelresource"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	// EnvEnabled is the master OTel switch. Default false. When set, it
	// supplies the default for EnvTracingEnabled (and the metrics gate) so a
	// single flag turns the whole pipeline on.
	EnvEnabled = "OTEL_ENABLED"

	// EnvTracingEnabled gates the entire trace pipeline. It DEFAULTS to the
	// value of EnvEnabled (OTEL_ENABLED) when unset; a per-signal value
	// overrides the master. Default (with the master unset) is false.
	EnvTracingEnabled = "OTEL_TRACING_ENABLED"

	// EnvOTLPEndpoint is the OTLP/HTTP collector endpoint
	// (e.g. "otel-collector.krateo-system.svc:4318"). Consumed by the
	// standard OTEL_EXPORTER_OTLP_ENDPOINT env contract via
	// otlptracehttp's WithEndpointURL/env auto-config.
	EnvOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"
)

// SAMPLING (#1013, Diego's ruling of 2026-10-30). This package installs NO
// sampler: NewTracerProvider below passes no WithSampler, so the SDK's
// samplerFromEnv reads OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG and
// those env vars are the live knob (chart value env.OTEL_TRACES_SAMPLER;
// installer key componentValues.snowplow.env.OTEL_TRACES_SAMPLER).
//
// The chart ships "parentbased_always_on". The platform API SLIs
// (availability, p95) are computed over snowplow's Server spans and are
// COUNTS, so they need every request's span, and Diego ruled that they come
// from server spans sampled at 100%, effective now. A root span (no
// traceparent: in-cluster callers, the LoadBalancer path) is always
// sampled; a span with a parent follows the parent's sampled flag, and the
// client spans of a request follow its server span.
//
// This overrules the 1.12.4 reasoning, which shipped "traceidratio" 0.05 to
// BOUND customer /call traffic: the agent gateway's tracing policy samples
// 100% (randomSampling "100") and propagates sampled=1, so a parent-based
// sampler spans every customer request. That is now the point, not the
// cost. On 057 that is 3.5K-35K server spans a day, mean ~19K (2026-10-23
// to -29, from the unsampled http.server.request.duration counter; the query
// is in the PR that fixes #1013), about 68 compressed bytes per span.
//
// WHAT HAPPENS WHEN THE GATEWAY SAMPLES AT ITS EDGE (installer#402). The
// parent-based sampler then follows the gateway's keep/drop flag: a request
// the gateway drops arrives with sampled=0 and snowplow records no Server
// span for it, so the SLIs silently become estimates. Which of the three
// remedies applies is Diego's decision when #402 lands (the #1013 PR describes
// each): (a) the gateway keeps propagating sampled=1 for every request it
// forwards to snowplow and applies its ratio at EXPORT (its policy `filter`)
// rather than at the sampling decision; (b) snowplow installs a sampler that
// always samples SpanKind=Server spans and is parent-based otherwise,
// accepting traces whose gateway parent was not exported; (c) the SLI moves
// to spans the gateway always keeps.
const (
	// serviceName is the span-scope name NewServerHandler passes to
	// otelhttp. The resource service.name lives in internal/otelresource
	// and is the same string.
	serviceName = otelresource.ServiceName
)

// ShutdownFunc flushes and stops the trace pipeline. Always non-nil so the
// caller can `defer shutdown(ctx)` unconditionally; it is a no-op when
// tracing was not enabled.
type ShutdownFunc func(context.Context) error

// tracingEnabled resolves the trace gate from the canonical contract:
// OTEL_TRACING_ENABLED, defaulting to the value of the OTEL_ENABLED master
// when the per-signal var is unset. With both unset it is false, preserving
// the default-off byte-identical guarantee.
func tracingEnabled() bool {
	return env.Bool(EnvTracingEnabled, env.Bool(EnvEnabled, false))
}

// Setup wires the OTLP/HTTP trace exporter and a batch TracerProvider when
// tracing resolves to enabled (OTEL_TRACING_ENABLED, defaulting to
// OTEL_ENABLED), registers it as the global TracerProvider, and installs
// the composite W3C TraceContext + Baggage propagator.
//
// When the gate is off it returns a no-op ShutdownFunc and registers
// nothing (off-path byte-identical guarantee).
//
// build is the snowplow build string (main.build, the full git commit). The
// resource (otelresource.Build) records it as vcs.ref.head.revision; its
// service.version is the pod's release label when the chart wires it (#462).
func Setup(ctx context.Context, build string) (ShutdownFunc, error) {
	noop := func(context.Context) error { return nil }

	if !tracingEnabled() {
		return noop, nil
	}

	// otlptracehttp reads OTEL_EXPORTER_OTLP_ENDPOINT (and the standard
	// OTLP env vars: headers, protocol, insecure) from the environment by
	// default, so the endpoint is configured via the env contract without
	// a hard-coded address. WithEndpointURL is applied explicitly when the
	// var is set so a bare host:port or a full URL both resolve.
	opts := []otlptracehttp.Option{}
	if ep := env.String(EnvOTLPEndpoint, ""); ep != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(otelresource.SignalEndpointURL(ep, otelresource.TracesPath)))
	}

	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return noop, err
	}

	// 1.12.4 — ONE shared resource across traces/metrics/logs
	// (internal/otelresource). It carries service.name/.version plus the
	// downward-API pod identity that makes the collector's k8sattributes
	// pod_association deterministic instead of falling through to
	// source-IP matching over a SNAT-able hostPort hop. Partial-resource
	// handling moved into the helper unchanged. logging.go's own
	// doc-comment relies on the three resources being identical; that is
	// now structural rather than three copies of one literal.
	res, err := otelresource.Build(ctx, build, kubeutil.ServiceAccountNamespace)
	if err != nil {
		return noop, err
	}

	tp := newTracerProvider(exp, sdktrace.WithResource(res))

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// newTracerProvider is the ONLY place a TracerProvider is constructed in this
// package, and it is what guarantees that NO span leaves the process with a
// credential in a URL attribute: the caller's exporter is always wrapped in
// redactingExporter (#489). Setup gets the batching pipeline; a falsifier gets
// the identical pipeline pointed at an in-memory exporter, so the arm exercises
// the production path rather than a hand-built provider.
//
// An exporter registered directly — sdktrace.WithBatcher(exp, …) on a bare
// sdktrace.NewTracerProvider — would publish url.full with its query in clear.
// The structural guard in url_redactor_test.go fails if that reappears here.
func newTracerProvider(exp sdktrace.SpanExporter, opts ...sdktrace.TracerProviderOption) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		append([]sdktrace.TracerProviderOption{
			sdktrace.WithBatcher(redactingExporter{exp},
				sdktrace.WithBatchTimeout(5*time.Second),
			),
		}, opts...)...,
	)
}

// Enabled reports whether tracing resolves to enabled (OTEL_TRACING_ENABLED,
// defaulting to the OTEL_ENABLED master). Instrumentation sites outside main
// (e.g. the outbound external-fetch transport) consult this so they only
// wrap when the trace pipeline is actually wired — keeping the off-path
// byte-identical.
func Enabled() bool {
	return tracingEnabled()
}

// WrapTransport wraps rt with otelhttp.NewTransport when tracing is
// enabled, so an outbound request created with a span-carrying context
// gets a client span and an injected W3C traceparent (via the global
// propagator). When tracing is disabled it returns rt unchanged — the
// outbound path is then byte-identical to the pre-OTel client. A nil rt is
// returned as-is (callers default to http.DefaultTransport downstream).
func WrapTransport(rt http.RoundTripper) http.RoundTripper {
	if !Enabled() || rt == nil {
		return rt
	}
	return otelhttp.NewTransport(rt)
}
