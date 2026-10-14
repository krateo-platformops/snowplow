package otelresource

import (
	"log/slog"
	"net/url"

	"go.opentelemetry.io/otel"
)

// OTLP/HTTP per-signal paths (OTLP spec, "OTLP/HTTP Request").
const (
	TracesPath  = "/v1/traces"
	MetricsPath = "/v1/metrics"
	LogsPath    = "/v1/logs"
)

// SignalEndpointURL turns the shared OTEL_EXPORTER_OTLP_ENDPOINT base URL into
// the URL one signal's exporter must post to, for use with the exporters'
// WithEndpointURL option.
//
// WHY (#596). WithEndpointURL does NOT append the signal path, and since
// otlptracehttp v1.45.0 a URL with no path is taken literally: the exporter
// posts to "/". The chart's value is a bare base, http://$(HOST_IP):4318, so
// every span batch went to http://<node>:4318/ and the collector answered 404
// (reproduced against the shipped binary). The metrics exporter, still on
// v1.44.0, defaulted an empty path to /v1/metrics, which is why metrics
// arrived over the same endpoint while traces never did. The OTLP spec says a
// signal path is appended to the generic endpoint, so an empty or "/" path
// gets signalPath. An explicit non-root path is kept as-is, so an operator
// who already wrote the full signal URL is not double-suffixed.
//
// An unparseable value is returned unchanged; the exporter reports it.
func SignalEndpointURL(raw, signalPath string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = signalPath
	}
	return u.String()
}

// InstallErrorHandler routes OTel SDK/exporter errors (a rejected export, an
// unreachable collector) to log at WARN.
//
// WHY (#596). OTel's default handler calls log.Print. main routes the std
// "log" package through slog (slog.SetDefault), which emits log.Print at INFO,
// and the chart ships LOG_LEVEL=warn — so every export failure was discarded.
// That is why 7 days of a 404 on every span batch left zero log lines. An
// export failure is a failure, not work, so WARN is its level.
func InstallErrorHandler(log *slog.Logger) {
	if log == nil {
		return
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("otel export/sdk error", slog.Any("err", err))
	}))
}
