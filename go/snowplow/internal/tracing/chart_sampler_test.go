package tracing

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// chartDir is helm/snowplow, relative to this package
// (go test runs with cwd = go/snowplow/internal/tracing).
var chartDir = filepath.Join("..", "..", "..", "..", "helm", "snowplow")

// shippedSampler is the sampler the chart ships (#1013, Diego's ruling
// 2026-10-30): the platform API SLIs are counts over snowplow's Server spans,
// so every request's span is kept. See tracing.go SAMPLING.
const shippedSampler = "parentbased_always_on"

// F3 (chart half) — the SHIPPED OTel values, read from values.yaml. It runs
// everywhere (no helm needed); TestS1013_ChartRendersParentBasedAlwaysOn below
// proves the same value survives the render into the ConfigMap the pod reads.
//
// History: 1.12.4 shipped traceidratio 0.05 to bound customer /call (C1);
// #1013 overrules that. The arm still pins that OTEL_ENABLED ships WITH an
// explicit sampler, that OTLP logs are off and that the endpoint is the
// node-local agent.
//
// MUTATION (RED, shown in the PR): values.yaml back to
// OTEL_TRACES_SAMPLER: "traceidratio".
func TestF3_ChartShipsParentBasedAlwaysOn(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(chartDir, "values.yaml"))
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	var values struct {
		Env map[string]string `json:"env"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatalf("parse chart values: %v", err)
	}
	env := values.Env

	if env["OTEL_ENABLED"] != "true" {
		t.Fatalf("chart env.OTEL_ENABLED = %q, want \"true\" (1.12.4 ships OTLP export on)", env["OTEL_ENABLED"])
	}
	if got := env["OTEL_TRACES_SAMPLER"]; got != shippedSampler {
		t.Fatalf("chart env.OTEL_TRACES_SAMPLER = %q, want %q (#1013: the SLIs need 100%% of server spans)", got, shippedSampler)
	}
	// parentbased_always_on takes no argument; a leftover ratio would read as
	// if it applied.
	if got, ok := env["OTEL_TRACES_SAMPLER_ARG"]; ok {
		t.Fatalf("chart env.OTEL_TRACES_SAMPLER_ARG = %q; %s takes no argument, do not ship one", got, shippedSampler)
	}
	// No OTLP logs receiver on the collector agent: exporting into a void.
	if got := env["OTEL_LOGS_ENABLED"]; got != "false" {
		t.Fatalf("chart env.OTEL_LOGS_ENABLED = %q, want \"false\" (the collector's logs pipeline is filelog-only)", got)
	}
	// The endpoint must be the node-local agent, expanded by the Deployment.
	if got := env["OTEL_EXPORTER_OTLP_ENDPOINT"]; got != "http://$(HOST_IP):4318" {
		t.Fatalf("chart env.OTEL_EXPORTER_OTLP_ENDPOINT = %q, want the node-local agent http://$(HOST_IP):4318", got)
	}
	// Every value must be a string (installer plumbing is string-only) —
	// guaranteed by the map[string]string decode above.
}

// renderConfigMap runs the real `helm template` over a copy of the chart (the
// release workflow's placeholder versions substituted, as lint.yaml does) and
// returns the rendered snowplow ConfigMap: the envFrom source the pod reads
// OTEL_TRACES_SAMPLER from. helm is required: SNOWPLOW_REQUIRE_HELM=1 turns
// its absence into a failure instead of a skip.
func renderConfigMap(t *testing.T, args ...string) *corev1.ConfigMap {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		if os.Getenv("SNOWPLOW_REQUIRE_HELM") == "1" {
			t.Fatal("helm not on PATH and SNOWPLOW_REQUIRE_HELM=1")
		}
		t.Skip("helm not on PATH — NOT RUN: the chart render arm needs helm")
	}
	dst := filepath.Join(t.TempDir(), "snowplow")
	if out, err := exec.Command("cp", "-R", chartDir, dst).CombinedOutput(); err != nil {
		t.Fatalf("copy chart: %v: %s", err, out)
	}
	chartYAML := filepath.Join(dst, "Chart.yaml")
	raw, err := os.ReadFile(chartYAML)
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("CHART_VERSION"), []byte("0.0.0"))
	raw = bytes.ReplaceAll(raw, []byte("APP_VERSION"), []byte("0.0.0"))
	if err := os.WriteFile(chartYAML, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cmdArgs := append([]string{"template", "t", dst, "--namespace", "krateo-system",
		"--show-only", "templates/configmap.yaml"}, args...)
	out, err := exec.Command(helm, cmdArgs...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	var cm corev1.ConfigMap
	if err := yaml.Unmarshal(out, &cm); err != nil {
		t.Fatalf("rendered ConfigMap does not parse: %v\n%s", err, out)
	}
	if cm.Kind != "ConfigMap" {
		t.Fatalf("rendered %q, want the snowplow ConfigMap\n%s", cm.Kind, out)
	}
	return &cm
}

// TestS1013_ChartRendersParentBasedAlwaysOn — the sampler default, checked on
// the RENDERED ConfigMap: with the shipped values the pod's environment gets
// OTEL_TRACES_SAMPLER=parentbased_always_on and no ratio. The second half
// renders the override exactly as the installer applies it
// (componentValues.snowplow.env.* is deep-merged into the chart's env), so the
// knob named in the PR is proven to reach the pod.
//
// MUTATION (RED, shown in the PR): values.yaml back to
// OTEL_TRACES_SAMPLER: "traceidratio" with OTEL_TRACES_SAMPLER_ARG: "0.05".
func TestS1013_ChartRendersParentBasedAlwaysOn(t *testing.T) {
	cm := renderConfigMap(t)
	if got := cm.Data["OTEL_TRACES_SAMPLER"]; got != shippedSampler {
		t.Fatalf("rendered OTEL_TRACES_SAMPLER = %q, want %q", got, shippedSampler)
	}
	if got, ok := cm.Data["OTEL_TRACES_SAMPLER_ARG"]; ok {
		t.Fatalf("rendered OTEL_TRACES_SAMPLER_ARG = %q; %s takes none", got, shippedSampler)
	}
	if got := cm.Data["OTEL_ENABLED"]; got != "true" {
		t.Fatalf("rendered OTEL_ENABLED = %q, want \"true\"", got)
	}

	over := renderConfigMap(t,
		"--set-string", "env.OTEL_TRACES_SAMPLER=traceidratio",
		"--set-string", "env.OTEL_TRACES_SAMPLER_ARG=0.25")
	if got := over.Data["OTEL_TRACES_SAMPLER"]; got != "traceidratio" {
		t.Fatalf("override env.OTEL_TRACES_SAMPLER did not reach the ConfigMap: got %q", got)
	}
	if got := over.Data["OTEL_TRACES_SAMPLER_ARG"]; got != "0.25" {
		t.Fatalf("override env.OTEL_TRACES_SAMPLER_ARG did not reach the ConfigMap: got %q", got)
	}
}
