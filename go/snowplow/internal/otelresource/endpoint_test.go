package otelresource

import (
	"bytes"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestSignalEndpointURL(t *testing.T) {
	cases := []struct {
		raw, path, want string
	}{
		// The chart's shape (#596): bare base → signal path appended.
		{"http://10.156.0.51:4318", TracesPath, "http://10.156.0.51:4318/v1/traces"},
		{"http://10.156.0.51:4318/", TracesPath, "http://10.156.0.51:4318/v1/traces"},
		{"http://10.156.0.51:4318", MetricsPath, "http://10.156.0.51:4318/v1/metrics"},
		{"http://10.156.0.51:4318", LogsPath, "http://10.156.0.51:4318/v1/logs"},
		{"https://otel.example:4318", TracesPath, "https://otel.example:4318/v1/traces"},
		// An explicit non-root path is the operator's choice — kept verbatim.
		{"http://c:4318/v1/traces", TracesPath, "http://c:4318/v1/traces"},
		{"http://c:4318/custom/ingest", TracesPath, "http://c:4318/custom/ingest"},
		// Not a URL with a host: unchanged, the exporter reports it.
		{"otel-collector:4318", TracesPath, "otel-collector:4318"},
		{"::bad", TracesPath, "::bad"},
	}
	for _, c := range cases {
		if got := SignalEndpointURL(c.raw, c.path); got != c.want {
			t.Errorf("SignalEndpointURL(%q, %q) = %q; want %q", c.raw, c.path, got, c.want)
		}
	}
}

// TestF596_ExportErrorsAreNotSwallowedAtWarn pins the second half of #596:
// seven days of a 404 on every span batch left ZERO log lines.
//
// Arm 1 (mechanism): OTel's default handler log.Print()s; once main routes the
// std log package through slog (slog.SetDefault) that becomes an INFO record,
// and at the chart's LOG_LEVEL=warn it is dropped.
// Arm 2 (fix): with InstallErrorHandler the same otel.Handle surfaces at WARN.
// Mutation: make InstallErrorHandler a no-op and arm 2 fails.
func TestF596_ExportErrorsAreNotSwallowedAtWarn(t *testing.T) {
	prevDefault := slog.Default()
	prevHandler := otel.GetErrorHandler()
	prevFlags, prevOut := log.Flags(), log.Writer()
	t.Cleanup(func() {
		slog.SetDefault(prevDefault)
		otel.SetErrorHandler(prevHandler)
		log.SetFlags(prevFlags)
		log.SetOutput(prevOut)
	})

	var buf bytes.Buffer
	warnLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	slog.SetDefault(warnLogger) // exactly what main does

	exportErr := errors.New("traces export: failed to send to http://h:4318/: 404 Not Found")

	// Arm 1: the std log path the default OTel handler uses is dropped.
	log.Print(exportErr)
	if buf.Len() != 0 {
		t.Fatalf("mechanism changed: a log.Print after slog.SetDefault reached a WARN handler: %s", buf.String())
	}

	// Arm 2: the installed handler surfaces it at WARN.
	InstallErrorHandler(warnLogger)
	otel.Handle(exportErr)
	out := buf.String()
	if !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, "404 Not Found") {
		t.Fatalf("otel.Handle did not produce a WARN record carrying the export error; got: %q", out)
	}
}
