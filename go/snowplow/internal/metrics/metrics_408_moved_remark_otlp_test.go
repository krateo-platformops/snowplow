package metrics

// metrics_408_moved_remark_otlp_test.go — #408: the moved-remark counter is readable in
// production. It is observed through the REAL OTLP callback as
// snowplow_deps_moved_remark_total{carrier=boot|guarded}, and its expvar twin
// (snowplow_deps.moved_remark_total / .moved_remark_boot_total) reads the same numbers.
//
// The counter is driven by REAL remarks: a resolve ctx (WithL1KeyContext) records a dep,
// the dep moves (real OnUpdate), then the real Put funnel runs. Two boot Puts go through
// PutThenRemark and one guarded Put through PutIfGen, plus a quiet guarded Put (no dep
// moved) as the control that must not count.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue408_MovedRemarkTotalReachesOTLP(t *testing.T) {
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
	gvr := schema.GroupVersionResource{Group: "m408.example.io", Version: "v1", Resource: "deps"}
	moved := func(key string) context.Context {
		ctx := cache.WithL1KeyContext(context.Background(), key)
		cache.Deps().Record(ctx, key, gvr, "ns", key)
		cache.Deps().OnUpdate(gvr, "ns", key)
		return ctx
	}

	for _, k := range []string{"m408-boot-a", "m408-boot-b"} {
		store.PutThenRemark(moved(k), k, &cache.ResolvedEntry{RawJSON: []byte(`{}`)})
	}
	if !store.PutIfGen(moved("m408-guarded"), "m408-guarded", &cache.ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen("m408-guarded")) {
		t.Fatalf("setup: guarded PutIfGen refused")
	}
	quiet := cache.WithL1KeyContext(context.Background(), "m408-quiet")
	cache.Deps().Record(quiet, "m408-quiet", gvr, "ns", "m408-quiet")
	if !store.PutIfGen(quiet, "m408-quiet", &cache.ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen("m408-quiet")) {
		t.Fatalf("setup: quiet PutIfGen refused")
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	pts := pointsFor(all, "snowplow_deps_moved_remark_total")
	if len(pts) != 2 {
		t.Fatalf("#408: snowplow_deps_moved_remark_total must be REGISTERED as exactly two OTLP series "+
			"(carrier=boot, carrier=guarded); got %d points (0 = unregistered / expvar-only)", len(pts))
	}
	got := map[string]int64{}
	for _, p := range pts {
		if len(p.attrs) != 1 {
			t.Fatalf("#408: each series must carry exactly the carrier attribute; got %v", p.attrs)
		}
		got[p.attrs["carrier"]] = p.value
	}
	if got["boot"] != 2 || got["guarded"] != 1 {
		t.Fatalf("#408: OTLP must observe the real moved remarks — want boot=2 guarded=1 (the quiet Put must "+
			"not count); got %v", got)
	}
	stats := cache.DepsStatsByStat()
	if stats["moved_remark_total"] != 3 || stats["moved_remark_boot_total"] != 2 {
		t.Fatalf("#408: expvar snowplow_deps moved_remark_total=%d moved_remark_boot_total=%d, want 3 and 2 "+
			"(expvar/OTLP drift)", stats["moved_remark_total"], stats["moved_remark_boot_total"])
	}
}
