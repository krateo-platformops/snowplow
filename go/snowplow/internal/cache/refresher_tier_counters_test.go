package cache

// refresher_tier_counters_test.go — the per-tier completions and the dequeue-age
// gauges count what they say, on the golden scenario of
// refresher_dequeue_order_golden_test.go: one held key, unviewed keys and keys
// open on a page, one worker.

import (
	"testing"
	"time"
)

func TestRefresherTierCounters_CountEachTiersCompletionsAndDequeueAges(t *testing.T) {
	const nUnviewed, nViewed = 8, 4
	keys, rec, firstIn, release := viewed765Start(t, 1, 1+nUnviewed+nViewed)
	hold, unviewed, viewed := keys[0], keys[1:1+nUnviewed], keys[1+nUnviewed:]
	viewed765Arm(t, viewed...)
	before := refresherStatsSnapshot()

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark(unviewed)
	terminalMark(viewed)
	release()
	waitFor(t, 20*time.Second, "every marked key re-resolved", func() bool {
		o, _, _ := rec.snapshot()
		return len(o) >= len(keys)
	})
	// The counters tick after the handler returns; wait for the last one.
	waitFor(t, 10*time.Second, "every completion counted", func() bool {
		s := refresherStatsSnapshot()
		return s.normalCompleted-before.normalCompleted+s.viewedCompleted-before.viewedCompleted >= uint64(len(keys))
	})
	after := refresherStatsSnapshot()
	if got := after.normalCompleted - before.normalCompleted; got != 1+nUnviewed {
		t.Fatalf("normal_completed moved %d, want %d (the held key and every unviewed one)", got, 1+nUnviewed)
	}
	if got := after.viewedCompleted - before.viewedCompleted; got != nViewed {
		t.Fatalf("viewed_completed moved %d, want %d", got, nViewed)
	}
	if got := after.completed - before.completed; got != uint64(len(keys)) {
		t.Fatalf("PRECONDITION: completed_total moved %d, want %d", got, len(keys))
	}
}

// The age gauges take each dequeue's age on its own side, and never a negative one.
func TestRefresherTierCounters_DequeueAgeIsSplitBySide(t *testing.T) {
	s := newDirtyWindowStats()
	s.observeDequeueAge(true, 40*time.Millisecond)
	s.observeDequeueAge(false, 900*time.Millisecond)
	s.observeDequeueAge(false, -5*time.Millisecond)
	_, vMax := s.dequeueAge[ageViewed].read()
	_, uMax := s.dequeueAge[ageUnviewed].read()
	if vMax != 40 || uMax != 900 {
		t.Fatalf("viewed max=%v unviewed max=%v, want 40 and 900: each dequeue's age on its own side", vMax, uMax)
	}
	var nilStats *dirtyWindowStats
	nilStats.observeDequeueAge(true, time.Second) // nil-safe
}

// The dequeue site feeds each side its own ages: unviewed keys marked 400 ms
// before the viewed ones carry the older ages, and the viewed side, dequeued first
// once the worker is free, the younger ones. A dequeue reported on the wrong side
// moves the 400 ms ages to the viewed gauge.
func TestRefresherTierCounters_EachSideGetsItsOwnDequeueAges(t *testing.T) {
	const nUnviewed, nViewed, gap = 4, 2, 400 * time.Millisecond
	keys, rec, firstIn, release := viewed765Start(t, 1, 1+nUnviewed+nViewed)
	hold, unviewed, viewed := keys[0], keys[1:1+nUnviewed], keys[1+nUnviewed:]
	viewed765Arm(t, viewed...)

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark(unviewed)
	time.Sleep(gap) // the unviewed changes age while the worker is held
	terminalMark(viewed)
	release()
	waitFor(t, 20*time.Second, "every marked key re-resolved", func() bool {
		o, _, _ := rec.snapshot()
		return len(o) >= len(keys)
	})
	s := refresherStatsSnapshot()
	if s.unviewedAgeMaxMS < gap.Milliseconds() {
		t.Fatalf("unviewed_dirty_age_ms_max=%d, want at least %d: the unviewed keys waited that long", s.unviewedAgeMaxMS, gap.Milliseconds())
	}
	if s.viewedAgeMaxMS >= gap.Milliseconds() {
		t.Fatalf("viewed_dirty_age_ms_max=%d, want under %d: the viewed keys were marked after the gap and dequeued first", s.viewedAgeMaxMS, gap.Milliseconds())
	}
}
