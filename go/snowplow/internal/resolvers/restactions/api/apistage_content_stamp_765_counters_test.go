// apistage_content_stamp_765_counters_test.go — #765 option A: the surfaces that
// show the re-fills the content stamp avoided, and the refresher's dequeue need.
// (Uses #765 APIs, so unlike apistage_content_stamp_765_test.go it does not
// compile on main.)
//
//   - C1 REUSED: one change, N refresher reads → refresh_reused_total = N-1,
//     the GVR's by_gvr row moves by the same, and the expvar key reads it.
//   - C2 COALESCED: an in-flight fill, then K changes each followed by a
//     refresher reader → the first queues the pending fill, the other K-1 join
//     it: refresh_coalesced_total = K-1, through the expvar key too.
//   - N1 NEED: the refresher's real dequeue stamps a need that is at least the
//     seq of the change that triggered it and no later than the re-resolve.
package api

import (
	"context"
	"encoding/json"
	"expvar"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func stamp765Expvar(t *testing.T, name string) float64 {
	t.Helper()
	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("%s is not published", name)
	}
	var out float64
	if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
		t.Fatalf("%s = %q is not a number: %v", name, v.String(), err)
	}
	return out
}

// TestContentStamp765_C1_RefreshReusedCounted is C1.
func TestContentStamp765_C1_RefreshReusedCounted(t *testing.T) {
	const n = 5
	dyn := newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	RegisterContentFillExpvarForTest()
	stamp765Prime(t, store)
	row0 := ContentFillStatsSnapshot().ByGVR[r1FsaGVR.String()]

	stamp765Change(t, dyn, r1FreshManaged)
	for i := 0; i < n; i++ {
		stamp765Recv(t, "refresh", stamp765Serve(stamp765RefreshCtx(), store))
	}
	s := ContentFillStatsSnapshot()
	if s.RefreshReused != n-1 || s.Total["refresh_forced"] != 1 || s.RefreshCoalesced != 0 {
		t.Fatalf("C1 RED: refresh_reused=%d refresh_forced=%d refresh_coalesced=%d, want %d / 1 / 0",
			s.RefreshReused, s.Total["refresh_forced"], s.RefreshCoalesced, n-1)
	}
	if row := s.ByGVR[r1FsaGVR.String()]; row.RefreshReused-row0.RefreshReused != n-1 {
		t.Fatalf("C1 RED: by_gvr refresh_reused moved by %d, want %d", row.RefreshReused-row0.RefreshReused, n-1)
	}
	if got := stamp765Expvar(t, "snowplow_apistage_content_fill_refresh_reused_total"); got != float64(n-1) {
		t.Fatalf("C1 RED: expvar snowplow_apistage_content_fill_refresh_reused_total = %v, want %d", got, n-1)
	}
	// A customer read moves neither counter.
	stamp765Recv(t, "customer read", stamp765Serve(r1Ctx(), store))
	if s2 := ContentFillStatsSnapshot(); s2.RefreshReused != n-1 || s2.RefreshCoalesced != 0 {
		t.Fatalf("C1 RED: a customer read moved the refresher counters: reused=%d coalesced=%d", s2.RefreshReused, s2.RefreshCoalesced)
	}
}

// TestContentStamp765_C2_RefreshCoalescedCounted is C2.
func TestContentStamp765_C2_RefreshCoalescedCounted(t *testing.T) {
	const k = 4
	dyn := newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	RegisterContentFillExpvarForTest()
	stamp765Prime(t, store)
	row0 := ContentFillStatsSnapshot().ByGVR[r1FsaGVR.String()]

	calls, read, release := stamp765HoldDispatches(t, 1)
	stamp765Change(t, dyn, 2)
	lead := stamp765Serve(stamp765RefreshCtx(), store)
	stamp765WaitClosed(t, "the first refresher re-fill never dispatched", read[0])
	readers := make([]chan fill710Served, k)
	for i := range readers {
		stamp765Change(t, dyn, 3+i)
		readers[i] = stamp765Serve(stamp765RefreshCtx(), store)
		if !fill710WaitFor(10*time.Second, func() bool { return stamp765Refused() == uint64(i+1) }) {
			t.Fatalf("PRECONDITION: reader %d was not refused by the in-flight fill", i)
		}
	}
	// Every pending joiner is waiting; the pending leader waits on the in-flight fill.
	if !fill710WaitFor(10*time.Second, func() bool { return ContentFillStatsSnapshot().JoinWaiting == k-1 }) {
		t.Fatalf("C2 RED: join_waiting = %d, want %d readers waiting on the pending fill", ContentFillStatsSnapshot().JoinWaiting, k-1)
	}
	release[0]()
	stamp765Recv(t, "first refresh", lead)
	for _, ch := range readers {
		if got := fill710ManagedOf(t, stamp765Recv(t, "churn reader", ch).v, stamp765Item); got != 2+k {
			t.Fatalf("churn reader served managed=%d, want the last change %d (the pending fill started after it)", got, 2+k)
		}
	}
	s := ContentFillStatsSnapshot()
	if s.RefreshCoalesced != k-1 || calls.Load() != 2 {
		t.Fatalf("C2 RED: refresh_coalesced=%d dispatches=%d, want %d / 2", s.RefreshCoalesced, calls.Load(), k-1)
	}
	if row := s.ByGVR[r1FsaGVR.String()]; row.RefreshCoalesced-row0.RefreshCoalesced != k-1 {
		t.Fatalf("C2 RED: by_gvr refresh_coalesced moved by %d, want %d", row.RefreshCoalesced-row0.RefreshCoalesced, k-1)
	}
	if got := stamp765Expvar(t, "snowplow_apistage_content_fill_refresh_coalesced_total"); got != float64(k-1) {
		t.Fatalf("C2 RED: expvar snowplow_apistage_content_fill_refresh_coalesced_total = %v, want %d", got, k-1)
	}
	if n := ContentFlightsInFlightForTest(); n != 0 {
		t.Fatalf("C2: %d flights still registered after every reader returned", n)
	}
	contentFlightsMu.Lock()
	pending := len(contentPendingFlights)
	contentFlightsMu.Unlock()
	if pending != 0 {
		t.Fatalf("C2: %d pending flights still registered after every reader returned", pending)
	}
}

// TestContentStamp765_N1_DequeueStampsNeedAtOrAfterTheTrigger is N1.
func TestContentStamp765_N1_DequeueStampsNeedAtOrAfterTheTrigger(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
	dyn := newFill710Watcher(t, 3)
	store := cache.ResolvedCache()
	fill710Reset(t)
	resetContentFlightsForTest()
	cache.ResetRefresherForTest()
	stamp765Prime(t, store)

	type obs struct{ need, at uint64 }
	var mu sync.Mutex
	var seen []obs
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(ctx context.Context, _ string, _ cache.ResolvedKeyInputs) error {
		if cache.RefreshTriggerHas(ctx, r1FsaGVR) {
			at := cache.DepGenEpochNow()
			mu.Lock()
			seen = append(seen, obs{need: cache.RefreshTriggerAsOf(ctx), at: at})
			mu.Unlock()
		}
		return nil
	})
	rctx, rcancel := context.WithCancel(context.Background())
	cache.StartRefresher(rctx)
	// Join the dep-event workers before the watcher cleanup resets the cache under them.
	t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest(); cache.ResetDepsForTest() })

	before := cache.DepGenEpochNow()
	stamp765Change(t, dyn, r1FreshManaged)
	if !fill710WaitFor(20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) > 0 }) {
		t.Fatalf("PRECONDITION: the refresher never re-resolved the dirty-marked content cell")
	}
	mu.Lock()
	defer mu.Unlock()
	o := seen[0]
	if o.need <= before || o.need > o.at {
		t.Fatalf("N1 RED: dequeue need = %d, want in (%d, %d]: after the triggering change's seq and no later than the re-resolve",
			o.need, before, o.at)
	}
}
