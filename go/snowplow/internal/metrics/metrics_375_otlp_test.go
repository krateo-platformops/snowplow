package metrics

// metrics_375_otlp_test.go — #375 (B): the dep-generation guard's DETECTOR
// unguarded_put_total is OTLP-NATIVE. It is registered in registerInstruments and
// observed through the REAL callback (collectViaRealCallback over a ManualReader),
// not merely present on /debug/vars. A detector buried in expvar is observable if
// you look but never alertable (#311).
//
// The counter is driven by a REAL bump. Real ACCEPTED gen-guarded Puts are made on
// a real store with a context that carries no dep-gen sink, which is the drifted
// resolve-entry shape, so it is the production funnel
// (PutIfGen → remarkIfDepsMoved → unguardedPutTotal++) that moves it. It is not set
// through a test-only adder. A Put WITH a sink is made alongside as the control,
// and it must not move the detector.

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue375_UnguardedPutTotalReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(func() {
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
	})
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}

	// Control: a guarded Put (sink installed at the resolve entry) must NOT count.
	guarded := cache.WithL1KeyContext(context.Background(), "k375-guarded")
	if !store.PutIfGen(guarded, "k375-guarded", &cache.ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen("k375-guarded")) {
		t.Fatalf("setup: guarded PutIfGen refused")
	}

	// Two drifted resolve-entry Puts (bare ctx → nil sink) + one drifted ReplaceIfGen.
	for _, k := range []string{"k375-a", "k375-b"} {
		if !store.PutIfGen(context.Background(), k, &cache.ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen(k)) {
			t.Fatalf("setup: nil-sink PutIfGen(%s) refused", k)
		}
	}
	if !store.ReplaceIfGen(context.Background(), "k375-a", &cache.ResolvedEntry{RawJSON: []byte(`{"v":2}`), CreatedAt: time.Now()}, store.CaptureGen("k375-a")) {
		t.Fatalf("setup: nil-sink ReplaceIfGen refused")
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	pts := pointsFor(all, "snowplow_deps_unguarded_put_total")
	if len(pts) != 1 {
		t.Fatalf("#375: snowplow_deps_unguarded_put_total must be REGISTERED as exactly one OTLP series "+
			"(unlabeled scalar detector) — got %d points (0 = unregistered / expvar-only, the #311 failure)", len(pts))
	}
	if pts[0].value != 3 {
		t.Fatalf("#375: the OTLP callback must observe the ACTUAL detector value driven by 3 real nil-sink "+
			"accepted Puts (and NOT the guarded control) — want 3, got %d", pts[0].value)
	}
	if len(pts[0].attrs) != 0 {
		t.Fatalf("#375: the detector must be an UNLABELED scalar; got attrs %v", pts[0].attrs)
	}
	// The expvar twin (snowplow_deps.unguarded_put_total) reads the same number.
	if got := cache.DepsStatsByStat()["unguarded_put_total"]; got != 3 {
		t.Fatalf("#375: expvar snowplow_deps.unguarded_put_total = %d, want 3 (expvar/OTLP drift)", got)
	}
}
