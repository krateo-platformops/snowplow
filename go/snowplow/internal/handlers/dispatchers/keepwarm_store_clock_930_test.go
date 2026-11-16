package dispatchers

// keepwarm_store_clock_930_test.go — #930 S1: the keep-warm age-skip decides a
// cell's age on the STORE clock, the clock that stamped its CreatedAt.
//
// The store runs on a virtual clock the arm moves; the host's wall clock does
// not move between the two decisions. A young cell (age 0) is age-skipped; the
// same cell one threshold later on the store clock is re-resolved. An age-skip
// that reads time.Since instead sees the cell at age ~0 both times and skips it
// twice: the keep-warm sweep becomes a no-op for every cell the store already
// considers old.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue930_KeepwarmAgeSkipReadsTheStoreClock(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	ttl, threshold := c2SetFixtureTTL(t)

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}
	quietLoggingE(t)
	store, ok := handle.(*cache.ResolvedCacheStore)
	if !ok {
		t.Fatalf("setup: the seed handle is %T, want the store", handle)
	}
	var at atomic.Int64
	at.Store(time.Now().UnixNano())
	t.Cleanup(store.SetClockForTest(func() time.Time { return time.Unix(0, at.Load()) }))

	// CreatedAt left zero: the store stamps it on its own (virtual) clock.
	putFixture(handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs})
	skips := func() uint64 {
		before := keepwarmAgeSkipTotal.Load()
		_ = seedOneWidget(granted, e, "authn-ns", seedModeKeepwarm)
		return keepwarmAgeSkipTotal.Load() - before
	}
	if got := skips(); got != 1 {
		t.Fatalf("PRE: a cell of age 0 on the store clock must be age-skipped; skips=%d", got)
	}
	at.Add(int64(threshold)) // age == threshold: re-resolved (strict `<`), still live (< TTL)
	if _, live := store.GetNoTouch(key); !live {
		t.Fatalf("PRE: at age %v the cell must still be live (TTL %v)", threshold, ttl)
	}
	if got := skips(); got != 0 {
		t.Fatalf("#930 S1 RED: the cell is %v old on the store clock (threshold %v) yet the keep-warm sweep "+
			"age-skipped it — the age-skip reads a clock other than the one that stamped CreatedAt", threshold, threshold)
	}
}
