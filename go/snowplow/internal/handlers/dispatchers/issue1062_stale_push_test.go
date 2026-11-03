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
	"context"
	"encoding/json"
	"fmt"
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
	// The requester's own stream: the handler subscribes AS the user.
	st := cache.SubscribeRefreshStreamAs(cache.StaleSubject(k547Cohorts[0].user), map[string]struct{}{key: {}})
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

// N7 — the identity-free widgetContent hit site (widgets.go, the shared shell)
// is an l1 stale serve PER REQUESTER: the #547 F7 shape (a known-stale shell
// every cohort is served at once), two cohorts, one shared key. Only cohort 0's
// stream exists; once the shell is replaced (evicted), the frame written on
// cohort 0's stream closes cohort 0's serve only — cohort 1's stays open.
// Mutation → RED: the widgetContent site notes nothing; or a frame closes every
// requester's serves of the shared key.
func TestIssue1062_N7_TheSharedShellHitIsAStaleServePerRequester(t *testing.T) {
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	h := newK547Harness(t, k547DenyBackground)
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatalf("PRECONDITION: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
	raw, _ := json.Marshal(shell.Object)
	putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
	cache.Deps().Record(context.Background(), contentKey, k547CompGVR, k547CompNS, fmt.Sprintf("comp-%d", k547Changed))
	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "shell dropped", func() bool { return k547Stat("dirty_ended_unfresh_dropped") > d0 }) {
		t.Fatalf("PRECONDITION: the shell's background refresh never reached the drop point")
	}
	k547WaitFor(t, 5*time.Second, "queue drained", func() bool { return k547Stat("queue_depth") == 0 })
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("PRECONDITION: the shell is not known-stale after the drop")
	}
	cache.ResetStalePushForTest()
	t.Cleanup(cache.ResetStalePushForTest)

	st := cache.SubscribeRefreshStreamAs(cache.StaleSubject(k547Cohorts[0].user), map[string]struct{}{contentKey: {}})
	defer st.Unsub()
	for k := range k547Cohorts {
		if r, code := h.get(k, k547Changed); code != 200 || r != "v0" {
			t.Fatalf("PRECONDITION (k=%d): the shared shell must be served stale at once (code=%d reason=%q)", k, code, r)
		}
	}
	if s := k1062Layer(cache.StaleLayerL1); s.Serves != uint64(len(k547Cohorts)) || s.Open != int64(len(k547Cohorts)) {
		t.Fatalf("N7 RED: the shared-shell hits opened serves=%d open=%d, want one per requester (%d)", s.Serves, s.Open, len(k547Cohorts))
	}
	handle.(interface{ DeleteForTest(string) }).DeleteForTest(contentKey) // replaced: the refetch is a fresh fill
	st.Wrote(contentKey)                                                  // cohort 0's frame
	s := k1062Layer(cache.StaleLayerL1)
	if s.Closes["pushed"] != 1 || s.Open != int64(len(k547Cohorts)-1) {
		t.Fatalf("N7 RED: pushed=%d open=%d after cohort 0's frame, want 1/%d — another requester's serve was closed by it",
			s.Closes["pushed"], s.Open, len(k547Cohorts)-1)
	}
}
