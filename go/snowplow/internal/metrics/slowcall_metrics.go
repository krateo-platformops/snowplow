package metrics

// slowcall_metrics.go — #1054: the slow-call instruments, hand-wired on OTLP
// (no expvar→OTLP bridge, #311). The counters read the same accessors the
// expvar closures read; the phase histogram is synchronous, fed through
// slowcall's recorder hook. route is a ServeMux pattern and phase a closed
// enum: no identity.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

func registerSlowCallInstruments(m metric.Meter) error {
	slow, err := m.Int64ObservableCounter("snowplow_slow_requests_total",
		metric.WithDescription("API requests whose server time exceeded the page goal's bound (slowcall.Bound, the 3 s p95 goal), by route (the ServeMux pattern) and work class (customer, or a background self-loopback's class). Each one logged a slowcall.request WARN line naming where its time went (#1054)."))
	if err != nil {
		return err
	}
	snaps, err := m.Int64ObservableCounter("snowplow_slow_request_snapshots_total",
		metric.WithDescription("Goroutine snapshots taken while a request was in flight past the bound (at most one a minute; each logged a slowcall.goroutines WARN line) (#1054)."))
	if err != nil {
		return err
	}
	suppressed, err := m.Int64ObservableCounter("snowplow_slow_request_snapshots_suppressed_total",
		metric.WithDescription("Slow requests that crossed the bound inside the snapshot rate window, so no snapshot covered them (#1054)."))
	if err != nil {
		return err
	}
	phase, err := m.Int64Histogram("snowplow_slow_request_phase_ms",
		metric.WithDescription("Per slow request (over slowcall.Bound): the time it spent in each phase it entered (ms), by phase. Phases may overlap (parallel stages, a wait inside a dial) (#1054)."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(slowcall.PhaseBoundsMS)...))
	if err != nil {
		return err
	}
	slowcall.SetPhaseRecorder(func(ms int64, p string) {
		phase.Record(context.Background(), ms, metric.WithAttributes(attribute.String("phase", p)))
	})
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for k, n := range slowcall.SlowRequests() {
			o.ObserveInt64(slow, int64(n), metric.WithAttributes(attribute.String("route", k.Route), attribute.String("class", k.Class)))
		}
		taken, sup := slowcall.SnapshotCounts()
		o.ObserveInt64(snaps, int64(taken))
		o.ObserveInt64(suppressed, int64(sup))
		return nil
	}, slow, snaps, suppressed)
	return err
}
