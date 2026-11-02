// issue1062_stale_push_test.go — #1062 at the /call boundary: an ARMED L1 hit
// on a known-stale cell (the #547 harness: the real widgets handler, the real
// refresher, the real store) is an l1 stale serve, and it is timed until the
// frame announcing its replacement is WRITTEN to the stream armed for it — the
// demand revalidation's accepted Put and its publishIfSubscribed, through the
// /refreshes handler's Wrote contract.
//
// Mutation → RED: the hit write point does not note the serve
// (staleHitWritePoint without cache.NoteStaleHit), or the stream's Wrote does
// not reach the ledger (refresh_broadcaster.go).
package dispatchers

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func k1062Layer(layer string) cache.StalePushLayerStats {
	for _, s := range cache.StalePushStatsSnapshot() {
		if s.Layer == layer {
			return s
		}
	}
	return cache.StalePushLayerStats{}
}

func TestIssue1062_D1_AnArmedL1StaleHitIsTimedUntilItsReplacingFrameIsWritten(t *testing.T) {
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	cache.ResetStalePushForTest()
	t.Cleanup(cache.ResetStalePushForTest)

	key := h.key(0, k547Changed)
	st := cache.SubscribeRefreshStream(map[string]struct{}{key: {}})
	done := make(chan struct{})
	t.Cleanup(func() { close(done); st.Unsub() })
	go func() { // the /refreshes handler: write the frame, then Wrote
		for {
			select {
			case <-done:
				return
			case k := <-st.Keys:
				st.Wrote(k)
			}
		}
	}()

	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("PRECONDITION: the hit must serve the stale v0 at once (code=%d reason=%q)", code, r)
	}
	if s := k1062Layer(cache.StaleLayerL1); s.Serves != 1 {
		t.Fatalf("D1 RED: an armed hit on a known-stale cell is not an l1 stale serve (serves=%d)", s.Serves)
	}
	if !k547WaitFor(t, 10*time.Second, "the replacing frame written", func() bool {
		return k1062Layer(cache.StaleLayerL1).Closes["pushed"] == 1
	}) {
		t.Fatalf("D1 RED: the demand revalidation replaced the cell and published, but no stale serve closed pushed (%+v)",
			k1062Layer(cache.StaleLayerL1))
	}
	s := k1062Layer(cache.StaleLayerL1)
	if s.ToPush.Count != 1 || s.ToPush.MaxMS > cache.StaleToPushMaxMS || s.Open != 0 || s.Unreplaced != 0 {
		t.Fatalf("D1: l1 to_push=%+v open=%d unreplaced=%d, want one sample within the 10 s max, nothing open", s.ToPush, s.Open, s.Unreplaced)
	}
	if a := k1062Layer(cache.StaleLayerAPIStage); a.Serves != 0 {
		t.Fatalf("D1: an L1 serve counted as apistage (%d)", a.Serves)
	}
	if r, _ := h.get(0, k547Changed); r != "v1" {
		t.Fatalf("PRECONDITION: the refetch after the frame must read the fresh v1 (got %q)", r)
	}
	if s := k1062Layer(cache.StaleLayerL1); s.Serves != 1 {
		t.Fatalf("D1: the fresh refetch was counted as a stale serve (serves=%d)", s.Serves)
	}
}
