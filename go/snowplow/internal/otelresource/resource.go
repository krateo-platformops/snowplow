// Package otelresource builds the ONE OpenTelemetry Resource that
// snowplow's three signal pipelines — traces, metrics and logs — all
// stamp onto every record they export.
//
// WHY A SHARED HELPER. Before 1.12.4 each of internal/tracing,
// internal/metrics and internal/logging constructed its own resource
// with an identical two-attribute literal. They agreed by coincidence,
// and logging.go's own doc-comment relies on that agreement ("identical
// to the TracerProvider's resource so logs and spans agree"). A single
// constructor makes the agreement structural: a future attribute cannot
// be added to spans and forgotten on metrics.
//
// # What the collector does with this, and why each attribute is here
//
// The node-local daemonset agent runs a `k8sattributes` processor whose
// `extract.metadata` list contains `service.name`, `service.version`,
// `service.namespace` and `service.instance.id` verbatim, plus every pod
// label via `key_regex: (.*)`. Two consequences drive this file.
//
// (1) ONE service.version (#462). The collector's own receivers export rows
// about the snowplow pod too: kubeletstats (container/pod CPU, memory,
// network) and filelog (the stdout log lines). Those rows carry no SDK
// resource, so k8sattributes derives their service.version from the pod
// label app.kubernetes.io/version, which the chart sets to
// {{ .Chart.AppVersion }} (the release, e.g. 1.12.35). Until #462 the SDK
// set service.version to the git commit instead, so one pod exported two
// values under one key (verified on 057: SDK metrics carried the 40-char
// SHA, kubeletstats metrics and every filelog line carried 1.12.35), and a
// query filtered on one value silently missed the other half of the pod.
//
// The fix makes the SDK read the SAME label: the chart passes it through
// the downward API as SNOWPLOW_SERVICE_VERSION (fieldRef
// metadata.labels['app.kubernetes.io/version']), so service.version is
// equal by construction on every row, SDK or collector. The commit is not
// lost: it stays on the resource as vcs.ref.head.revision and on the
// snowplow_build_info{version} metric attribute. Outside a pod (no env) the
// build string is used, as before.
//
// (2) ASSOCIATION [C5], the more dangerous one. The processor's
// `pod_association` is, in order, `k8s.pod.ip` -> `k8s.pod.uid` ->
// `connection`. Before 1.12.4 snowplow's resource carried NEITHER an IP
// nor a UID, so association fell through to `connection` — the source
// address of the OTLP connection. Snowplow exports to
// `$(HOST_IP):4318`, a hostPort on its OWN node, and pod->node-hostPort
// traffic on GKE is commonly SNAT'd to the node address. Association
// would then resolve to the NODE, not the snowplow pod, and NO `k8s.*`
// attribute and NO pod label would attach to any snowplow row. That
// silently breaks the "chart appVersion is not lost" claim above and
// every per-pod / per-replica filter on the dashboard. Setting
// `k8s.pod.uid` from the downward API removes the dependency entirely:
// it is the documented deterministic association source.
//
// # Env contract
//
// The three pod-identity values come from downward-API `env:` entries
// the chart's Deployment adds (POD_UID / POD_NAME / POD_NAMESPACE,
// alongside HOST_IP for the endpoint). They are read here rather than
// threaded through three Setup signatures so all three pipelines
// observe the same values with no call-site skew.
//
// EVERY ATTRIBUTE IS OMITTED WHEN ITS SOURCE IS EMPTY. An attribute set
// to "" is worse than an absent one: it is a resource key the collector
// sees as present-but-blank, which suppresses the processor's own
// enrichment for that key and produces a permanently empty column. So a
// binary running outside Kubernetes (a unit test, a local run) emits
// just service.name and service.version, exactly as before 1.12.4.
package otelresource

import (
	"context"

	"github.com/krateo-platformops/plumbing/env"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	// ServiceName is the resource service.name reported on every span,
	// metric and log record. It is the otel_logs.ServiceName /
	// otel_traces.ServiceName primary key every dashboard filters on, so
	// it must be identical across the three pipelines — which is now
	// structural rather than a convention.
	ServiceName = "snowplow"

	// EnvPodUID / EnvPodName / EnvPodNamespace are the downward-API
	// entries the chart's Deployment sets. Unset outside Kubernetes.
	EnvPodUID       = "POD_UID"
	EnvPodName      = "POD_NAME"
	EnvPodNamespace = "POD_NAMESPACE"

	// EnvServiceVersion is the downward-API entry carrying the pod's
	// app.kubernetes.io/version label: the value the collector's
	// k8sattributes processor stamps on the rows it produces for this pod
	// (#462). It is service.version when set.
	EnvServiceVersion = "SNOWPLOW_SERVICE_VERSION"

	// AttrVCSRevision carries the build commit once service.version is the
	// release (#462). The semconv key (vcs.ref.head.revision) postdates the
	// semconv package this module pins, so it is spelled out.
	AttrVCSRevision = attribute.Key("vcs.ref.head.revision")
)

// ServiceVersion is the service.version every pipeline reports: the pod's
// app.kubernetes.io/version label (EnvServiceVersion) when the chart wires
// it, else the build string.
func ServiceVersion(build string) string {
	if v := env.String(EnvServiceVersion, ""); v != "" {
		return v
	}
	return build
}

// Build returns the resource for all three pipelines.
//
// build is the snowplow build string (main.build, the full 40-character git
// commit the Dockerfile stamps). service.version is ServiceVersion(build):
// the pod's release label when the chart wires it, so every row about the
// pod agrees (#462). The commit is recorded as vcs.ref.head.revision.
//
// nsFallback supplies service.namespace when POD_NAMESPACE is unset —
// callers pass kubeutil.ServiceAccountNamespace, which reads the
// projected service-account token's namespace file. It is a func rather
// than a string so the file read only happens when the env var is
// actually missing, and its error is DISCARDED on purpose: outside a
// pod there is no namespace file, which is not a reason to fail a
// telemetry pipeline. The attribute is simply omitted.
//
// PARTIAL-RESOURCE HANDLING. resource.New can return a NON-NIL resource
// together with a non-fatal merge error (schema-URL skew being the
// common case). The pre-1.12.4 pipelines each handled this by keeping
// whatever came back, and that behaviour is preserved exactly: on error
// with a non-nil resource we return the partial resource and NO error,
// so a schema skew degrades the attribute set rather than disabling the
// signal. Only a nil resource falls back to resource.Default().
func Build(ctx context.Context, build string, nsFallback func() (string, error)) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(ServiceName),
		semconv.ServiceVersion(ServiceVersion(build)),
	}
	if build != "" {
		attrs = append(attrs, AttrVCSRevision.String(build))
	}

	ns := env.String(EnvPodNamespace, "")
	if ns == "" && nsFallback != nil {
		if v, err := nsFallback(); err == nil {
			ns = v
		}
	}
	if ns != "" {
		// service.namespace groups snowplow with the other Krateo
		// components in the same install; k8s.namespace.name is what the
		// k8sattributes processor would otherwise have to derive.
		attrs = append(attrs,
			semconv.ServiceNamespace(ns),
			semconv.K8SNamespaceName(ns),
		)
	}

	if podName := env.String(EnvPodName, ""); podName != "" {
		// service.instance.id distinguishes replicas: without it, two
		// pods' metrics are indistinguishable series and a per-replica
		// breakdown is impossible.
		attrs = append(attrs,
			semconv.ServiceInstanceID(podName),
			semconv.K8SPodName(podName),
		)
	}

	if podUID := env.String(EnvPodUID, ""); podUID != "" {
		// [C5] The deterministic pod_association key. Without it the
		// processor falls through to source-IP matching, which the
		// hostPort hop can defeat.
		attrs = append(attrs, semconv.K8SPodUID(podUID))
	}

	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		if res == nil {
			return resource.Default(), nil
		}
		// Partial resource + non-fatal merge error: use what came back,
		// exactly as the three pipelines did before this helper existed.
		return res, nil
	}
	return res, nil
}
