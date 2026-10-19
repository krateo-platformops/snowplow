package metrics

// metrics_777_exempt_otlp_test.go — #777 b′: the undeclared-extras exemption
// counter reaches OTLP (hand-wired in metrics.go), as one series with no
// attributes, moving with its production counter.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func TestIssue777_ExemptCounterReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	read := func() int64 {
		pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_widget_undeclared_extras_exempt_total")
		if len(pts) != 1 {
			t.Fatalf("snowplow_widget_undeclared_extras_exempt_total: %d series, want 1 (metrics.go wiring missing?)", len(pts))
		}
		if len(pts[0].attrs) != 0 {
			t.Fatalf("the exemption counter carries attributes %v; it must have none", pts[0].attrs)
		}
		return pts[0].value
	}
	before := read()
	dispatchers.AddWidgetUndeclaredExtrasExemptForTest(1)
	if got := read() - before; got != 1 {
		t.Fatalf("the OTLP series moved %d, want 1", got)
	}
}
