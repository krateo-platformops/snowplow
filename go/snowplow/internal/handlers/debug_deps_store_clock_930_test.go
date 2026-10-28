// debug_deps_store_clock_930_test.go — #930 S1: the /debug deps views report a
// cell's ages on the STORE clock, the clock that stamped CreatedAt and BornAt.
// The store runs on a virtual clock moved by exact amounts; a view reading
// time.Now would report ages near zero.

package handlers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue930_DebugDepsAgesReadTheStoreClock(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "3600")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	c := cache.ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	var at atomic.Int64
	at.Store(time.Now().UnixNano())
	t.Cleanup(c.SetClockForTest(func() time.Time { return time.Unix(0, at.Load()) }))

	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "c930"}
	key := cache.ComputeKey(in)
	put := func() {
		c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	}
	put()
	cache.Deps().SetStore(c)
	cache.Deps().Record(context.Background(), key, schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "r"}, "ns", "c930")

	at.Add(int64(120 * time.Second))
	if v := depsKeyViewFor(key); !v.Resident || v.AgeSeconds != 120 || v.BornAgeSeconds != 120 {
		t.Fatalf("#930 S1 RED: /debug ?key view at +120 s on the store clock: resident=%v age=%d born_age=%d, want 120/120",
			v.Resident, v.AgeSeconds, v.BornAgeSeconds)
	}
	// A re-Put keeps BornAt (the body is young, the cell is old): past the
	// standard TTL in LIFE, the stale-risk filter must list it at that age.
	at.Add(int64(3000 * time.Second))
	put()
	at.Add(int64(1000 * time.Second))
	out := depsFilterStaleRisk()
	if len(out.Sample) != 1 || out.Sample[0].BornAgeSeconds != 4120 {
		t.Fatalf("#930 S1 RED: stale-risk sample %+v, want this cell at born age 4120 s on the store clock", out.Sample)
	}
}
