package metrics

// go_runtime_metrics.go — the effective GOGC and soft memory limit on OTLP,
// hand-wired beside their expvar twin snowplow_go_runtime (no expvar→OTLP
// bridge). Both read memlimit.Effective (runtime/metrics), so the two
// surfaces agree. source is a closed enum (env, cgroup_v2, cgroup_v1, none,
// unreadable). Registered unconditionally: GC settings are not a cache
// concept.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/memlimit"
)

func registerGoRuntimeInstruments(m metric.Meter) error {
	gogc, err := m.Int64ObservableGauge("snowplow_go_gc_percent",
		metric.WithDescription("The Go runtime's effective GOGC (heap growth percent between collections; -1 = GC off). The chart leaves it at the runtime default, 100."))
	if err != nil {
		return err
	}
	limit, err := m.Int64ObservableGauge("snowplow_go_memory_limit_bytes",
		metric.WithDescription("The Go runtime's effective soft memory limit (9223372036854775807 = none), by where it came from: env (GOMEMLIMIT set by the operator), cgroup_v2 / cgroup_v1 (7/8 of the container's cgroup memory limit), none (no cgroup limit), unreadable (the cgroup limit could not be read)."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		g, l := memlimit.Effective()
		o.ObserveInt64(gogc, g)
		o.ObserveInt64(limit, l, metric.WithAttributes(attribute.String("source", string(memlimit.Current().Source))))
		return nil
	}, gogc, limit)
	return err
}
