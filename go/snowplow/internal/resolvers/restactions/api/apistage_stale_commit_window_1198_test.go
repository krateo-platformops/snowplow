package api

// apistage_stale_commit_window_1198_test.go — #1198: the D2 serve must be open
// in the stale-push ledger before anything can replace it.
//
// DebtCleared starts the replacement the moment it reads Committed. If the serve
// opened after that, a replacing Put that won the race found nothing to mark: the
// frame that followed closed nothing, stale_to_push lost the sample, and the
// overdue scan reported a viewer who HAS the fresh frame as a miss of the 10 s
// goal. Under load this failed D2-16 about 1 run in 200 (line 563).
//
// No timing here. The debt's refill is held, so it can only complete inside the
// Commit hook, which runs at the moment Committed is visible and returns only
// when the whole real replacement has landed: the worst order, every run.
//
// Mutation → RED, every run: the serve opened after Committed is visible.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestStale1198_AReplacementInsideCommitsWindowIsStillMeasured(t *testing.T) {
	const key = "dep-1198"
	f := newD2Fixture(t)
	release := HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	f.customer(t, key)

	windowRan := false
	t.Cleanup(cache.SetStaleCommitVisibleHookForTest(func() {
		windowRan = true
		release()
		d2WaitDebtsClosed(t)
		// The replacement is over only when it has published: the Put replaces
		// the cell, and the publish that follows it is the end of the window.
		if !fill710WaitFor(10*time.Second, func() bool {
			e, ok := f.store.GetNoTouch(key)
			return ok && !e.StaleDerived && StaleServeStatsSnapshot().PushUnarmed == 1
		}) {
			t.Fatalf("PRECONDITION: the replacement did not finish inside Commit (%+v)", StaleServeStatsSnapshot())
		}
	}))
	f.storeAndCommit(key)
	if !windowRan {
		t.Fatalf("PRECONDITION: Commit never reached the window")
	}

	base := time.Now()
	t.Cleanup(cache.SetStalePushClockForTest(func() time.Time { return base.Add(2 * time.Second) }))
	d2Push(t, d2WrittenStream(t, key), key, time.Second)

	st := StaleServeStatsSnapshot()
	if st.ToPush.Count != 1 || st.ToPush.MaxMS < 2000 {
		t.Fatalf("RED (#1198): the frame closed nothing: a replacement that won the race lost its stale_to_push sample (count %d, max %d ms)",
			st.ToPush.Count, st.ToPush.MaxMS)
	}

	// The viewer has the fresh frame, so no later scan may report a miss.
	t.Cleanup(cache.SetStalePushClockForTest(func() time.Time { return base.Add(12 * time.Second) }))
	cache.ScanStalePushOverdueForTest()
	for _, l := range cache.StalePushStatsSnapshot() {
		if l.Layer == cache.StaleLayerAPIStage && (l.Unreplaced != 0 || l.Closes["pushed"] != 1 || l.Open != 0) {
			t.Fatalf("RED (#1198): apistage unreplaced=%d pushed=%d open=%d, want 0/1/0: a viewer who has the fresh frame was counted as a miss",
				l.Unreplaced, l.Closes["pushed"], l.Open)
		}
	}
}
