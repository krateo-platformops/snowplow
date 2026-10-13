package metrics

// metrics_643_gvr_epoch_otlp_test.go — #643 PR-C: the GVR-discovery epoch reaches
// OTLP as snowplow_gvr_discovery_epoch (hand-wired in metrics.go). Driven through
// the process engine's REAL hook body and observed via the REAL callback. RED
// without the wiring: no series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue643_GVRDiscoveryEpochReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	read := func() (int64, bool) {
		pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_gvr_discovery_epoch")
		if len(pts) != 1 {
			return 0, false
		}
		return pts[0].value, true
	}
	before, ok := read()
	if !ok {
		t.Fatal("no snowplow_gvr_discovery_epoch series — metrics.go wiring missing")
	}
	dispatchers.NoteGVRDiscoveredForTest(schema.GroupVersionResource{Group: "otlp643.krateo.io", Version: "v1", Resource: "a"})
	dispatchers.NoteGVRDiscoveredForTest(schema.GroupVersionResource{Group: "otlp643.krateo.io", Version: "v1", Resource: "b"})
	after, _ := read()
	if after-before != 2 {
		t.Fatalf("snowplow_gvr_discovery_epoch moved by %d after 2 discoveries, want 2", after-before)
	}
}
