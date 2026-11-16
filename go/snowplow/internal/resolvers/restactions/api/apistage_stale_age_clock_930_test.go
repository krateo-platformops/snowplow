package api

// apistage_stale_age_clock_930_test.go — #930 S1: the D2 stale-serve age is the
// known-stale window's age, read on the DIRTY-WINDOW clock that stamped its
// mark (cache.DirtyWindowNow), not on the debt clock (staleNow) or time.Now.
//
// The window clock is virtual and moved by exactly 700 ms between the mark and
// the stale serve; the host's wall clock moves by whatever the fixture took.
// The serve age must be exactly 700 ms.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue930_D2_StaleServeAgeReadsTheDirtyWindowClock(t *testing.T) {
	const age = 700 * time.Millisecond
	var at atomic.Int64
	at.Store(time.Now().UnixNano())
	t.Cleanup(cache.SetDirtyWindowClockForTest(func() time.Time { return time.Unix(0, at.Load()) }))
	f := newD2Fixture(t) // marks the content cell known-stale at the virtual instant
	release := HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
	at.Add(int64(age))
	f.customer(t, "dep-930")
	s := StaleServeStatsSnapshot()
	if s.ServeTotal != 1 || s.ServeAge.Count != 1 || s.ServeAge.MaxMS != age.Milliseconds() {
		t.Fatalf("#930 S1 RED: stale_serve_total=%d serve_age=%+v, want one serve aged exactly %d ms on the "+
			"dirty-window clock — the age reads a clock other than the one that stamped the mark",
			s.ServeTotal, s.ServeAge, age.Milliseconds())
	}
	release()
	f.putDependent("dep-930")
	d2WaitDebtsClosed(t)
}
