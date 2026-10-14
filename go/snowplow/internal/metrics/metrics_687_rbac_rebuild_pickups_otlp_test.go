package metrics

// metrics_687_rbac_rebuild_pickups_otlp_test.go — #687: the post-release
// pickup counter reaches OTLP (hand-wired in metrics.go) and reads the
// production counter. RED without the wiring: no series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue687_RebuildPickupsReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_rbac_rebuild_post_release_pickups_total")
	if len(pts) != 1 {
		t.Fatalf("snowplow_rbac_rebuild_post_release_pickups_total: %d series, want 1 — metrics.go wiring missing", len(pts))
	}
	if pts[0].value != int64(cache.RBACRebuildPostReleasePickups()) {
		t.Fatalf("series = %d, counter = %d", pts[0].value, cache.RBACRebuildPostReleasePickups())
	}
}
