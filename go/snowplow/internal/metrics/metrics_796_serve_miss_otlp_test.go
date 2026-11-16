package metrics

// metrics_796_serve_miss_otlp_test.go — #796 W5: the internal-dispatch
// informer serve-miss counter and the sync-wait total reach OTLP (hand-wired in
// metrics.go): one series per closed reason, no other attribute, plus one
// attribute-free wait series.

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue796_ServeMissReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	pts := flatten(collectViaRealCallback(t, "deadbeef"))
	misses := pointsFor(pts, "snowplow_internal_dispatch_informer_serve_miss_total")
	if len(misses) != len(restactionsapi.ServeMissReasons) {
		t.Fatalf("serve_miss_total: %d series, want one per reason (%d) — metrics.go wiring missing?", len(misses), len(restactionsapi.ServeMissReasons))
	}
	seen := map[string]bool{}
	for _, p := range misses {
		if len(p.attrs) != 1 || p.attrs["reason"] == "" {
			t.Fatalf("serve_miss_total carries attributes %v; want exactly {reason}", p.attrs)
		}
		seen[p.attrs["reason"]] = true
	}
	for _, r := range restactionsapi.ServeMissReasons {
		if !seen[r] {
			t.Fatalf("serve_miss_total has no %q series", r)
		}
	}
	wait := pointsFor(pts, "snowplow_internal_dispatch_informer_sync_wait_ms_total")
	if len(wait) != 1 || len(wait[0].attrs) != 0 {
		t.Fatalf("sync_wait_ms_total: %d series (attrs %v), want one with no attributes", len(wait), wait)
	}
}
