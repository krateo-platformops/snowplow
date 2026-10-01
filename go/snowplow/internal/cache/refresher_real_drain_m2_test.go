package cache

// refresher_real_drain_m2_test.go — #386 M2, PM gate condition 1: the real drain
// rate is Δ(completed_total − skipped_no_entry_total)/interval, sourced from the
// refresher's OWN skip counter (refresher_metrics.go: snowplow_refresher_skipped_no_entry_total),
// NOT from the L1 miss_total. This falsifier proves the discrimination: under a
// workload with the production ~92% cheap-skip ratio, raw completed/s OVER-ESTIMATES
// the real resolve work, while Δ(completed − skipped_no_entry) gives the true drain.
//
// Grounding (verified in code): processNext bumps completedTotal on EVERY
// non-error dequeue — a skipped_no_entry dequeue (processOne returns nil) hits the
// success branch and increments completed too (refresher.go ~:864/:980). So
// completed INCLUDES skips; subtracting skipped_no_entry is what isolates the real
// resolves. (Post-#376 the L1 miss_total no longer carries the refresher's no-op
// dequeues, so miss_total is the WRONG source — the refresher's own skip counter
// is the independent, #376-surviving one.)

import (
	"context"
	"testing"
)

func TestIssue386_M2_RealDrainExcludesSkippedNoEntry(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 1)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "0") // no floor — every dequeue runs

	c := ResolvedCache()
	r := refresherSingleton()
	ctx := context.Background()

	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error { return nil })

	// Production shape: ~92% cheap skips, ~8% real resolves (the 057 ratio the
	// issue cites). reals = real resolve work; skips = non-resident dequeues.
	const reals = 8
	const skips = 92

	// M real keys: resident entries that resolve successfully (completed++,
	// skipped_no_entry unchanged).
	for i := 0; i < reals; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "real" + itoa(i)}
		k := ComputeKey(in)
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
		enqueueRefreshForTest(k)
		r.processNext(ctx)
	}
	// N skip keys: NON-resident (never Put) → GetNoTouch ok=false → skipped_no_entry++,
	// but processNext STILL counts the dequeue as completed++.
	for i := 0; i < skips; i++ {
		enqueueRefreshForTest("nonresident-" + itoa(i))
		r.processNext(ctx)
	}

	stats := RefresherStatsByStat()
	completed := stats["completed"]
	skipped := stats["skipped_no_entry"]
	realDrain := completed - skipped

	if skipped != skips {
		t.Fatalf("#386 M2: skipped_no_entry = %d, want %d", skipped, skips)
	}
	if completed != skips+reals {
		t.Fatalf("#386 M2: completed = %d, want %d — raw completed INCLUDES the cheap skips, which is exactly why completed/s over-estimates capacity",
			completed, skips+reals)
	}
	if realDrain != reals {
		t.Fatalf("#386 M2: real drain = completed − skipped_no_entry = %d, want %d (the real resolves)", realDrain, reals)
	}
	// The discrimination: raw completed must materially exceed the real drain by
	// the skip count — a reader using raw completed/s would over-size by ~12.5x here.
	if completed <= realDrain {
		t.Fatalf("#386 M2: raw completed %d does not exceed real drain %d — the skip contamination is not demonstrated",
			completed, realDrain)
	}
	t.Logf("#386 M2: completed=%d (raw, over-estimates), skipped_no_entry=%d, real_drain=%d — raw/real = %.1fx",
		completed, skipped, realDrain, float64(completed)/float64(realDrain))
}
