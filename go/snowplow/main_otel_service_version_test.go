package main

// main_otel_service_version_test.go — #462: ONE service.version per pod.
//
// The collector's own receivers (kubeletstats, filelog) export rows about the
// snowplow pod with service.version derived by k8sattributes from the pod
// label app.kubernetes.io/version. The chart passes that same label to the
// process as SNOWPLOW_SERVICE_VERSION. Every OTLP signal snowplow exports
// (traces, metrics, logs) must carry exactly that value as service.version,
// so a query filtered on it sees every row about the pod. The commit stays on
// vcs.ref.head.revision.
//
// RED on main 626aef06: all three signals carry the build commit as
// service.version, which on a pod is not the label value (verified on 057:
// SDK rows c753392…, kubeletstats/filelog rows 1.12.35).
//
// The chart half (TestIssue462_ChartWiresServiceVersionFromTheVersionLabel)
// pins that the Deployment actually passes the label through.

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/krateo-platformops/snowplow/internal/logging"
	"github.com/krateo-platformops/snowplow/internal/metrics"
	"github.com/krateo-platformops/snowplow/internal/tracing"

	"go.opentelemetry.io/otel"
	otellog "go.opentelemetry.io/otel/log"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// sv462Receiver captures every resource one OTLP signal exported.
type sv462Receiver struct {
	mu        sync.Mutex
	resources []*resourcepb.Resource
	srv       *httptest.Server
}

func newSV462Receiver(t *testing.T, decode func([]byte) ([]*resourcepb.Resource, proto.Message, error)) *sv462Receiver {
	t.Helper()
	r := &sv462Receiver{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body io.Reader = req.Body
		if req.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(req.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer gz.Close()
			body = gz
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		res, resp, err := decode(raw)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.resources = append(r.resources, res...)
		r.mu.Unlock()
		out, _ := proto.Marshal(resp)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *sv462Receiver) snapshot() []*resourcepb.Resource {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*resourcepb.Resource(nil), r.resources...)
}

func sv462Attr(res *resourcepb.Resource, key string) (string, bool) {
	for _, kv := range res.GetAttributes() {
		if kv.GetKey() == key {
			if s, ok := kv.GetValue().GetValue().(*commonpb.AnyValue_StringValue); ok {
				return s.StringValue, true
			}
		}
	}
	return "", false
}

func TestIssue462_EveryOTLPSignalCarriesTheCollectorServiceVersion(t *testing.T) {
	const (
		labelVersion = "1.12.99"                                  // the pod's app.kubernetes.io/version
		build        = "0123456789abcdef0123456789abcdef01234567" // main.build: the full commit
	)
	for _, kv := range os.Environ() {
		if k := strings.SplitN(kv, "=", 2)[0]; strings.HasPrefix(k, "OTEL_") {
			t.Setenv(k, "")
			os.Unsetenv(k)
		}
	}
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_LOGS_ENABLED", "true")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")
	t.Setenv("SNOWPLOW_SERVICE_VERSION", labelVersion) // what the chart wires from the label

	traces := newSV462Receiver(t, func(raw []byte) ([]*resourcepb.Resource, proto.Message, error) {
		m := &collectortracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(raw, m); err != nil {
			return nil, nil, err
		}
		var out []*resourcepb.Resource
		for _, rs := range m.GetResourceSpans() {
			out = append(out, rs.GetResource())
		}
		return out, &collectortracepb.ExportTraceServiceResponse{}, nil
	})
	metricsRcv := newSV462Receiver(t, func(raw []byte) ([]*resourcepb.Resource, proto.Message, error) {
		m := &collectormetricspb.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(raw, m); err != nil {
			return nil, nil, err
		}
		var out []*resourcepb.Resource
		for _, rm := range m.GetResourceMetrics() {
			out = append(out, rm.GetResource())
		}
		return out, &collectormetricspb.ExportMetricsServiceResponse{}, nil
	})
	logs := newSV462Receiver(t, func(raw []byte) ([]*resourcepb.Resource, proto.Message, error) {
		m := &collectorlogspb.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(raw, m); err != nil {
			return nil, nil, err
		}
		var out []*resourcepb.Resource
		for _, rl := range m.GetResourceLogs() {
			out = append(out, rl.GetResource())
		}
		return out, &collectorlogspb.ExportLogsServiceResponse{}, nil
	})

	ctx := context.Background()
	// The Setups install global SDK providers; leave no SDK provider behind for
	// later arms (F9 asserts the globals are SDK-free).
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
	})
	// Each Setup reads OTEL_EXPORTER_OTLP_ENDPOINT when called, so each signal
	// gets its own receiver (the three exporters would otherwise share a path).
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", traces.srv.URL+"/v1/traces")
	traceShutdown, err := tracing.Setup(ctx, build)
	if err != nil {
		t.Fatalf("tracing.Setup: %v", err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", metricsRcv.srv.URL+"/v1/metrics")
	metricShutdown, err := metrics.Setup(ctx, build)
	if err != nil {
		t.Fatalf("metrics.Setup: %v", err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", logs.srv.URL+"/v1/logs")
	lp, logShutdown, err := logging.Setup(ctx, build)
	if err != nil {
		t.Fatalf("logging.Setup: %v", err)
	}
	if lp == nil {
		t.Fatal("logging.Setup returned no provider with OTEL_LOGS_ENABLED=true")
	}

	_, span := otel.Tracer("issue462").Start(ctx, "issue462")
	span.End()
	var rec otellog.Record
	rec.SetBody(otellog.StringValue("issue462"))
	lp.Logger("issue462").Emit(ctx, rec)

	flush, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for name, sd := range map[string]func(context.Context) error{
		"traces": traceShutdown, "metrics": metricShutdown, "logs": logShutdown,
	} {
		if err := sd(flush); err != nil {
			t.Fatalf("%s shutdown/flush: %v", name, err)
		}
	}

	for name, rcv := range map[string]*sv462Receiver{"traces": traces, "metrics": metricsRcv, "logs": logs} {
		got := rcv.snapshot()
		if len(got) == 0 {
			t.Errorf("%s: nothing exported (the arm would be vacuous)", name)
			continue
		}
		for _, res := range got {
			if v, _ := sv462Attr(res, "service.version"); v != labelVersion {
				t.Errorf("%s: service.version = %q, want %q (the pod's app.kubernetes.io/version, which the "+
					"collector stamps on its kubeletstats and filelog rows for this pod)", name, v, labelVersion)
			}
			if v, _ := sv462Attr(res, "vcs.ref.head.revision"); v != build {
				t.Errorf("%s: vcs.ref.head.revision = %q, want the build commit %q", name, v, build)
			}
		}
	}
}

// TestIssue462_ChartWiresServiceVersionFromTheVersionLabel — the Deployment
// passes the pod's app.kubernetes.io/version label to the process as
// SNOWPLOW_SERVICE_VERSION, and the pod template carries that label.
func TestIssue462_ChartWiresServiceVersionFromTheVersionLabel(t *testing.T) {
	root := filepath.Join("..", "..", "helm", "snowplow", "templates")
	dep, err := os.ReadFile(filepath.Join(root, "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deployment template: %v", err)
	}
	re := regexp.MustCompile(`- name: SNOWPLOW_SERVICE_VERSION\s+valueFrom:\s+fieldRef:\s+fieldPath: metadata\.labels\['app\.kubernetes\.io/version'\]`)
	if !re.Match(dep) {
		t.Fatal("#462: deployment.yaml must wire SNOWPLOW_SERVICE_VERSION from fieldRef metadata.labels['app.kubernetes.io/version']")
	}
	helpers, err := os.ReadFile(filepath.Join(root, "_helpers.tpl"))
	if err != nil {
		t.Fatalf("read _helpers.tpl: %v", err)
	}
	if !strings.Contains(string(helpers), "app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}") {
		t.Fatal("#462: the chart labels must set app.kubernetes.io/version from .Chart.AppVersion")
	}
	tmpl := regexp.MustCompile(`(?s)template:\s+metadata:.*?labels:\s+\{\{-\s*include "snowplow\.labels"`)
	if !tmpl.Match(dep) {
		t.Fatal("#462: the pod template must carry the snowplow.labels set (which includes app.kubernetes.io/version)")
	}
}
