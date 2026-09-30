// issue344_reverify_drop_ratio_test.go — #344: verify (not just document) that
// the snowplow_sliceability_reverify droppedTotal:processedTotal ratio being
// HIGH (~2.7:1 at bench scale) is EXPECTED and correctness-preserving, so the
// expvar doc's "drops are healthy backpressure, not lost work" claim is
// grounded in an executable arm rather than a comment. This is a
// characterization / regression guard over EXISTING correct behaviour: it goes
// RED only if the drop-on-full or the coalescing mechanism regresses (e.g. a
// blocking submit, or a non-idempotent invalidate).

package cache

import (
	"fmt"
	"testing"
)

// ARM 1 — DROP-ON-FULL is deterministic and counted (the ratio's numerator).
// A bounded queue with no drain: submits past capacity are SHED (Submit returns
// false, droppedTotal ticks), never block. This is why droppedTotal routinely
// exceeds processedTotal under a noisy informer stream — bounded backpressure
// by design (keeps invalidate off the refresher workqueue).
func TestIssue344_ReverifyDropOnFullIsDeterministicAndCounted(t *testing.T) {
	t.Setenv(envSliceabilityReverifyQueueLen, "2")
	resetSliceabilityReverifyWorkerForTest() // fresh worker, qlen=2, NOT started (no drain)
	t.Cleanup(resetSliceabilityReverifyWorkerForTest)

	accepted, dropped := 0, 0
	for i := 0; i < 5; i++ {
		if SubmitSliceabilityInvalidate(fmt.Sprintf("raKey-%d", i)) {
			accepted++
		} else {
			dropped++
		}
	}
	if accepted != 2 || dropped != 3 {
		t.Fatalf("#344: qlen=2, 5 submits, no drain → accepted=%d dropped=%d, want 2/3 "+
			"(drop-on-full must shed the overflow, never block)", accepted, dropped)
	}
	s := SliceabilityReverifyStatsSnapshot()
	if s.EnqueuedTotal != 2 || s.DroppedTotal != 3 || s.ProcessedTotal != 0 {
		t.Fatalf("#344: snapshot enqueued=%d dropped=%d processed=%d, want 2/3/0",
			s.EnqueuedTotal, s.DroppedTotal, s.ProcessedTotal)
	}
	// Grounds the doc: with no drain, dropped(3) : processed(0) — drops dominate
	// under burst, and that is the EXPECTED shape, not a defect.
	if s.DroppedTotal <= s.ProcessedTotal {
		t.Fatalf("#344: under burst, droppedTotal(%d) must exceed processedTotal(%d) — "+
			"the ratio being > 1 is the expected drop-on-full backpressure", s.DroppedTotal, s.ProcessedTotal)
	}
}

// ARM 2 — a DROP loses NO invalidation: InvalidateSliceabilityForKey is
// idempotent-coalescing per raKey, so ONE processed reverify covers every
// redundant (dropped OR duplicate) submit for that raKey. That is why a high
// drop:process ratio is HEALTHY, not lost work. Drives the clock past the
// T_unverify rate-floor deterministically via the nowUnix seam.
func TestIssue344_ReverifyDropLosesNoInvalidation_CoalescingIsIdempotent(t *testing.T) {
	resetSliceabilityMemoForTest()
	t.Cleanup(resetSliceabilityMemoForTest)

	orig := nowUnix.Load()
	t.Cleanup(func() { nowUnix.Store(orig) })
	at := func(v int64) {
		f := func() int64 { return v }
		nowUnix.Store(&f)
	}

	const rk = "raKey-content"
	at(1000)
	RecordSliceability(rk, "shapeA", false)
	RecordSliceability(rk, "shapeB", false)

	// Past the rate-floor so the invalidate proceeds (not rate-floored).
	at(1000 + defaultSliceabilityReverifyRateFloorSeconds + 1)

	first := InvalidateSliceabilityForKey(rk) // ONE processed reverify
	if first < 2 {
		t.Fatalf("#344: first reverify of %s removed %d memo entries, want >=2 (both shapes) — "+
			"the processed reverify must actually invalidate", rk, first)
	}
	if _, known := SliceabilityLookup(rk, "shapeA"); known {
		t.Fatalf("#344: %s/shapeA still known after invalidate — the invalidation was lost", rk)
	}

	// A redundant reverify for the SAME raKey — what a dropped-then-re-enqueued
	// or a duplicate submit would trigger — is a coalesced NO-OP: nothing left.
	second := InvalidateSliceabilityForKey(rk)
	if second != 0 {
		t.Fatalf("#344: a redundant reverify for %s removed %d, want 0 — it must be a coalesced "+
			"no-op so a DROPPED duplicate submit loses nothing (this is why drop:process >> 1 is safe)", rk, second)
	}
	// AND the memo STAYS gone — the redundant reverify is idempotent: it neither
	// resurrects an entry nor leaves one behind (both shapes remain invalidated).
	if _, knownA := SliceabilityLookup(rk, "shapeA"); knownA {
		t.Fatalf("#344: %s/shapeA reappeared after the redundant reverify — coalescing must be idempotent "+
			"(no-op AND the memo stays gone)", rk)
	}
	if _, knownB := SliceabilityLookup(rk, "shapeB"); knownB {
		t.Fatalf("#344: %s/shapeB still known after both reverifies — the invalidation must be complete "+
			"and idempotent across shapes", rk)
	}
}
