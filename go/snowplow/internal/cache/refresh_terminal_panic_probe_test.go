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
	resetRefresherForTest()
	refresherInstance.Store(nil)
	t.Cleanup(resetRefresherForTest)

	// Snapshot with NO refresher: zeros, no construction (a scrape at boot).
	st := RefreshTerminalStatsSnapshot()
	if st.SuppressedSetTotal != 0 || st.SuppressedSkipsTotal != 0 || st.SuppressedKeys != 0 {
		t.Fatalf("snapshot with no refresher = %+v", st)
	}
	if refresherPeek() != nil {
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
	// #547 — the known-stale read surfaces with no refresher: false, no
	// construction.
	if _, _, ok := KnownStale(""); ok {
		t.Fatal("empty key reported known-stale")
	}
	if KnownStaleReadThrough("k") {
		t.Fatal("a key reported known-stale with no refresher")
	}
	if refresherPeek() != nil {
		t.Fatal("a known-stale read constructed the refresher singleton")
	}

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
	if refresherPeek() != nil {
		t.Fatal("a store path constructed the refresher singleton")
	}
}
