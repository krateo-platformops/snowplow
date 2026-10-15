package metrics

// metrics_720_nested_over_ceiling_otlp_test.go — #720 N4 (OTLP side): the
// nested-resolve over-ceiling gauge and its peak reach OTLP from the REAL
// registration (registerInstruments over a ManualReader) and read the LIVE
// values moved through the production recorder. No attributes. RED without
// registerNestedResolveAdmissionInstruments (both series absent). The gate
// moving the recorder is proven in the api package
// (TestIssue720_N4_OverCeilingVisibleOnExpvar).

import (
	"context"
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestIssue720_NestedOverCeilingReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetNestedOverCeilingForTest()
	t.Cleanup(restactionsapi.ResetNestedOverCeilingForTest)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}

	// A burst to 1234 bytes past the ceiling, then down to 500: the gauge reads
	// now, the peak holds the burst.
	restactionsapi.RecordNestedOverCeilingForTest(1234)
	restactionsapi.RecordNestedOverCeilingForTest(500)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)
	for name, want := range map[string]int64{
		"snowplow_nested_resolve_admission_over_ceiling_bytes":      500,
		"snowplow_nested_resolve_admission_over_ceiling_bytes_peak": 1234,
	} {
		pts := pointsFor(all, name)
		if len(pts) != 1 || pts[0].value != want || len(pts[0].attrs) != 0 {
			t.Fatalf("%s = %+v, want one attribute-free point of %d (not reading the live gauge)", name, pts, want)
		}
	}
	s := restactionsapi.NestedResolveAdmissionStatsSnapshot()
	if s.OverCeilingBytes != 500 || s.OverCeilingBytesPeak != 1234 {
		t.Fatalf("snapshot %+v diverged from OTLP", s)
	}
}
