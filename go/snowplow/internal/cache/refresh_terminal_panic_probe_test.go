// refresh_terminal_panic_probe_test.go — 1.12.6 C4 panic probes. Every C4
// entry point is reachable from a path that may run BEFORE the refresher
// singleton exists (cache-off, a Put during boot, the expvar scrape) or with
// degenerate inputs (an empty key, a zero budget). None may panic.
package cache

import (
	"testing"
	"time"
)

func TestRefreshTerminal_PanicProbe_NoRefresherAndDegenerateInputs(t *testing.T) {
	t.Setenv(envRefreshDropEvictMaxPerMinute, "0")
	t.Setenv(envRefreshSuppressAfterDeclines, "0") // below the floor → default 3, never 0
	resetRefresherForTest()
	refresherInstance = nil
	t.Cleanup(resetRefresherForTest)

	// Snapshot with NO refresher: zeros, no construction (a scrape at boot).
	st := RefreshTerminalStatsSnapshot()
	if st.DropEvictTotal != 0 || st.DropEvictMaxPerMinute != 0 || st.SuppressAfterDeclines != 3 {
		t.Fatalf("snapshot with no refresher = %+v", st)
	}
	if refresherInstance != nil {
		t.Fatal("RefreshTerminalStatsSnapshot constructed the refresher singleton")
	}

	// Empty key at every entry point.
	if NoteRefreshDecline("", "stage_error", true) {
		t.Fatal("empty key suppressed")
	}
	if _, ok := RefreshSuppressedReason(""); ok {
		t.Fatal("empty key reported suppressed")
	}
	clearRefreshSuppression("")

	// A zero-budget breaker never allows and never enters a window.
	b := newDropEvictBreaker(0)
	for i := 0; i < 3; i++ {
		if allowed, first := b.allow(); allowed || first {
			t.Fatalf("zero-budget breaker allow() = (%v,%v)", allowed, first)
		}
	}
	// A negative knob falls back to the default, never to a negative budget.
	t.Setenv(envRefreshDropEvictMaxPerMinute, "-7")
	if got := RefreshDropEvictMaxPerMinute(); got != defaultRefreshDropEvictMaxPerMinute {
		t.Fatalf("negative knob → %d", got)
	}
	// One-WARN-per-window: budget 1, two refusals → exactly one firstSuspend,
	// and a refill re-arms the window.
	now := time.Unix(1000, 0)
	b = newDropEvictBreaker(1)
	b.nowFn = func() time.Time { return now }
	b.last = now
	if a, _ := b.allow(); !a {
		t.Fatal("first token refused")
	}
	if a, first := b.allow(); a || !first {
		t.Fatalf("second call = (%v,%v), want refused + first-of-window", a, first)
	}
	if a, first := b.allow(); a || first {
		t.Fatalf("third call = (%v,%v), want refused + not-first", a, first)
	}
	now = now.Add(2 * time.Minute) // refill
	if a, _ := b.allow(); !a {
		t.Fatal("refilled token refused")
	}
	if a, first := b.allow(); a || !first {
		t.Fatalf("after refill the next refusal must open a NEW window: (%v,%v)", a, first)
	}
	// B1: the counters live on package atomics (not on the breaker) so a
	// stats read never has to construct the refresher singleton.
	if dropEvictWarnTotal.Load() != 2 || dropEvictTotal.Load() != 2 || dropEvictSuspendedTotal.Load() != 3 {
		t.Fatalf("counters warn=%d evict=%d susp=%d, want 2/2/3", dropEvictWarnTotal.Load(), dropEvictTotal.Load(), dropEvictSuspendedTotal.Load())
	}
	warnDropEvictSuspended("k", 1, false) // the WARN itself must not panic on a bare key

	// Store paths with no refresher: Put / Get / deleteForDep clear markers
	// without touching the singleton.
	c := newResolvedCache(4, 1<<20, time.Hour)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "probe"}
	k := ComputeKey(in)
	NoteRefreshDecline(k, "uaf", true)
	c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
	if _, ok := RefreshSuppressedReason(k); ok {
		t.Fatal("Put did not clear the marker")
	}
	NoteRefreshDecline(k, "uaf", true)
	c.deleteForDep(k)
	if _, ok := RefreshSuppressedReason(k); ok {
		t.Fatal("deleteForDep did not clear the marker")
	}
	if refresherInstance != nil {
		t.Fatal("a store path constructed the refresher singleton")
	}
}
