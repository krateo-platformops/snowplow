// issue316_toggle_bench_test.go — the C1 A/B seam arm (#316).
//
// SetProactiveRefreshEnabledForTest is a TEST/BENCH-ONLY toggle (default enabled,
// no production reader) that lets the in-process C1 amplification bench measure the
// SAME build with the #316 proactive-refresh pass ON (proactive-on arm) vs OFF
// (baseline arm) — isolating the pass's amplification from the #315 reaper changes a
// cross-build baseline (origin/main vs this branch) would confound. This arm proves
// the seam works so the bench's baseline is a REAL baseline (an arm that CAN fail),
// not a no-op.

package cache

import (
	"testing"
	"time"
)

// SetProactiveRefreshEnabledForTest enables (default) or disables the #316
// proactive-refresh enqueue. TEST/BENCH ONLY, and defined HERE in a _test.go so it
// is NEVER compiled into the production binary (arch-1217's build discipline: the
// pass can never be turned off in prod). An untagged _test.go compiles under any
// `go test -tags ...`, so tester-c1's in-process C1 harness (build tag
// c1amplification) and this arm both reach it. Restore to true after a baseline arm.
func SetProactiveRefreshEnabledForTest(enabled bool) {
	proactiveRefreshDisabledForTest.Store(!enabled)
}

func TestIssue316_ProactiveRefreshToggle_ForBench(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20") // TTL/4 = 5s
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	// The toggle is process-global; always restore the production default (enabled)
	// so sibling tests are unaffected.
	t.Cleanup(func() { SetProactiveRefreshEnabledForTest(true) })

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	// Warm (Get-hit), body 16s old → TTLRemaining ~4s < TTL/4 (5s): approaching TTL.
	_ = i316WarmApproachingTTL(t, c, "toggle", 16*time.Second, true)

	// BASELINE arm: pass OFF → NOT enqueued.
	SetProactiveRefreshEnabledForTest(false)
	before := c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before {
		t.Fatalf("baseline (pass OFF): a warm approaching-TTL cell must NOT be enqueued; proactive_refresh_total moved %d->%d", before, got)
	}

	// PROACTIVE-ON arm: pass ON → the SAME cell IS enqueued (+1).
	SetProactiveRefreshEnabledForTest(true)
	before = c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before+1 {
		t.Fatalf("proactive-on (pass ON): the warm approaching-TTL cell must be enqueued; proactive_refresh_total moved %d->%d (want +1)", before, got)
	}
}
