package dispatchers

// refresh_cycle_1070_test.go — #1070 D1, the wiring arm: the REAL refresh
// terminal (resolveAndPopulateL1) records its Put on the cycle's tier and
// publishes with the Put's time, so the frame it causes is timed from that Put.
// Driven through cache.RunRefreshNow (tier direct) on an injected clock.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRefreshCycle1070_D1_TheRefreshTerminalTimesItsPutAndItsFrame(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	cache.CompleteRBACSnapshotForTest(t)
	c := instrumentCacheOn(t)
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	cache.ResetRefreshCycleForTest()
	t.Cleanup(cache.ResetRefreshCycleForTest)
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC).UnixNano())
	t.Cleanup(cache.SetDirtyWindowClockForTest(func() time.Time { return time.Unix(0, clock.Load()) }))

	inputs := instrumentInputs("d1-1070")
	key := cache.ComputeKey(inputs)
	c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &inputs})
	st := cache.SubscribeRefreshStream(map[string]struct{}{key: {}})
	t.Cleanup(st.Unsub)

	t.Cleanup(setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		clock.Add(int64(800 * time.Millisecond)) // the resolve
		return []byte(`{"v":"new"}`), nil
	}))
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		return resolveAndPopulateL1(ctx, in, nil, nil)
	})
	if err := cache.RunRefreshNow(context.Background(), key); err != nil {
		t.Fatalf("PRE: RunRefreshNow: %v", err)
	}
	select {
	case k := <-st.Keys:
		clock.Add(int64(300 * time.Millisecond))
		st.Wrote(k)
	case <-time.After(10 * time.Second):
		t.Fatal("PRE: the refresh published no frame to the open stream")
	}
	s := cache.RefreshCycleStatsSnapshot()
	if d := s.Duration[cache.RefreshTierDirect]; d.Count != 1 || d.SumMS != 800 {
		t.Fatalf("D1 RED: direct duration count=%d sum=%d, want one 800 ms sample (start → the terminal Put)", d.Count, d.SumMS)
	}
	if f := s.PutToFrame[cache.RefreshTierDirect]; f.Count != 1 || f.SumMS != 300 {
		t.Fatalf("D1 RED: direct put_to_frame count=%d sum=%d, want one 300 ms sample: the terminal must publish "+
			"with its Put's time (PublishRefreshed), or no frame is ever timed", f.Count, f.SumMS)
	}
	var changed uint64
	for _, p := range s.Puts {
		if p.Class == "restactions" && p.Tier == cache.RefreshTierDirect && p.Content == "changed" {
			changed = p.N
		}
	}
	if changed != 1 {
		t.Fatalf("D1 RED: restactions|direct|changed=%d, want 1", changed)
	}
}
