// refresh_484_lifecycle_test.go — #484 fix arms at the broadcaster: the
// trailing-timer lifecycle, the trailing/pending/eviction interplay, the
// overflow (stall) rule, the producer-latency guarantee, and the counters
// (trailing_emitted, pending_coalesced, pending_overflow_resync). Run under
// -race. The leak checks count ARMED TIMERS (RefreshCoalesceWindowsForTest:
// one timer per open window), never runtime.NumGoroutine.

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const lcWin = 250 * time.Millisecond // the production default window

func lcSetup(t *testing.T) {
	t.Helper()
	withRefreshLayer(t)
	t.Setenv(envRefreshCoalesceWindowMS, "") // production default
	if w := refreshCoalesceWindowFn(); w != lcWin {
		t.Fatalf("precondition: window=%v want %v", w, lcWin)
	}
}

// collect drains ch for d and counts frames per key.
func collect(ch <-chan string, d time.Duration) map[string]int {
	got := map[string]int{}
	deadline := time.After(d)
	for {
		select {
		case k := <-ch:
			got[k]++
		case <-deadline:
			return got
		}
	}
}

func awaitWindows(t *testing.T, want int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for RefreshCoalesceWindowsForTest() != want {
		if time.Now().After(deadline) {
			t.Fatalf("armed coalesce timers=%d, want %d after %s — a timer leaked", RefreshCoalesceWindowsForTest(), want, d)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- (a) timer lifecycle -----------------------------------------------------

// Exactly ONE trailing emit per window that suppressed signals, however many.
func TestRefresh484_Trailing_ExactlyOnePerWindow(t *testing.T) {
	lcSetup(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()
	for i := 0; i < 10; i++ {
		PublishRefresh("k") // 1 leading + 9 suppressed
	}
	if got := collect(ch, 50*time.Millisecond)["k"]; got != 1 {
		t.Fatalf("leading edge: %d frames, want 1", got)
	}
	if got := collect(ch, lcWin+150*time.Millisecond)["k"]; got != 1 {
		t.Fatalf("trailing edge: %d frames, want exactly 1 for 9 suppressed signals", got)
	}
	st := RefreshBroadcasterStatsSnapshot()
	if st.Coalesced != 9 || st.TrailingEmitted != 1 {
		t.Fatalf("coalesced=%d trailing_emitted=%d, want 9 / 1", st.Coalesced, st.TrailingEmitted)
	}
	// The trailing emit opened a fresh window that saw nothing: released.
	awaitWindows(t, 0, 2*lcWin)
	if got := collect(ch, lcWin)["k"]; got != 0 {
		t.Fatalf("a quiet window emitted %d frames, want 0", got)
	}
}

// A window with no suppressed signal emits nothing at its end.
func TestRefresh484_Trailing_NoneWithoutSuppression(t *testing.T) {
	lcSetup(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()
	PublishRefresh("k")
	if got := collect(ch, lcWin+150*time.Millisecond)["k"]; got != 1 {
		t.Fatalf("%d frames for one publish, want 1 (no spurious trailing emit)", got)
	}
	if st := RefreshBroadcasterStatsSnapshot(); st.TrailingEmitted != 0 {
		t.Fatalf("trailing_emitted=%d want 0", st.TrailingEmitted)
	}
	awaitWindows(t, 0, lcWin)
}

// A key that changes continuously settles at about one emit per window, and
// the LAST change is announced after it lands.
func TestRefresh484_Trailing_ContinuousChurnBoundedAndLastAnnounced(t *testing.T) {
	lcSetup(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()
	var mu sync.Mutex
	var frames []time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ch:
				mu.Lock()
				frames = append(frames, time.Now())
				mu.Unlock()
			case <-time.After(3 * lcWin):
				return
			}
		}
	}()
	start := time.Now()
	var lastCommit time.Time
	for time.Since(start) < 1500*time.Millisecond {
		PublishRefresh("k")
		lastCommit = time.Now()
		time.Sleep(20 * time.Millisecond)
	}
	<-done
	mu.Lock()
	defer mu.Unlock()
	maxFrames := int(1500*time.Millisecond/lcWin) + 2
	if len(frames) > maxFrames {
		t.Fatalf("%d frames over 1.5s of churn, want <= %d (one per window)", len(frames), maxFrames)
	}
	if last := frames[len(frames)-1]; last.Before(lastCommit) {
		t.Fatalf("last frame %v precedes the last commit by %v — the final change was never announced",
			last.Format(time.StampMicro), lastCommit.Sub(last))
	}
}

// Unsubscribe with a trailing emit owed: the timer is torn down (last
// disarm), its late fire is inert — no panic, no send — and nothing leaks.
func TestRefresh484_Lifecycle_UnsubTearsDownWindow(t *testing.T) {
	lcSetup(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	PublishRefresh("k")
	PublishRefresh("k") // suppressed: a trailing emit is owed
	if n := RefreshCoalesceWindowsForTest(); n != 1 {
		t.Fatalf("armed timers=%d want 1", n)
	}
	<-ch // the leading frame
	unsub()
	if n := RefreshCoalesceWindowsForTest(); n != 0 {
		t.Fatalf("armed timers=%d after the last unsub, want 0", n)
	}
	time.Sleep(lcWin + 100*time.Millisecond)
	if len(ch) != 0 {
		t.Fatalf("%d frames sent to an unsubscribed sink", len(ch))
	}
	if st := RefreshBroadcasterStatsSnapshot(); st.TrailingEmitted != 0 {
		t.Fatalf("trailing_emitted=%d after unsub, want 0 (fire must be inert)", st.TrailingEmitted)
	}
}

// Unsubscribe of ONE of two subscribers: the window survives for the other,
// whose trailing frame arrives; the departed sink gets nothing.
func TestRefresh484_Lifecycle_UnsubOneOfTwo(t *testing.T) {
	lcSetup(t)
	chA, unsubA := SubscribeRefresh(map[string]struct{}{"k": {}})
	chB, unsubB := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsubB()
	PublishRefresh("k")
	PublishRefresh("k")
	<-chA
	<-chB
	unsubA()
	if got := collect(chB, lcWin+150*time.Millisecond)["k"]; got != 1 {
		t.Fatalf("remaining subscriber got %d trailing frames, want 1", got)
	}
	if len(chA) != 0 {
		t.Fatalf("departed subscriber received %d frames after unsub", len(chA))
	}
}

// Hub reset (the broadcaster stop): every window is stopped, a late fire is
// inert and moves no counter of the next hub.
func TestRefresh484_Lifecycle_HubResetStopsWindows(t *testing.T) {
	lcSetup(t)
	keys := map[string]struct{}{}
	for i := 0; i < 20; i++ {
		keys[fmt.Sprintf("k%d", i)] = struct{}{}
	}
	ch, _ := SubscribeRefresh(keys)
	for k := range keys {
		PublishRefresh(k)
		PublishRefresh(k)
	}
	h := refreshHub()
	h.cmu.Lock()
	open := len(h.windows)
	h.cmu.Unlock()
	if open != 20 {
		t.Fatalf("armed timers=%d want 20", open)
	}
	resetRefreshBroadcasterForTest()
	h.cmu.Lock()
	left := len(h.windows)
	h.cmu.Unlock()
	if left != 0 {
		t.Fatalf("old hub still holds %d armed timers after reset", left)
	}
	for len(ch) > 0 {
		<-ch
	}
	time.Sleep(lcWin + 100*time.Millisecond)
	if len(ch) != 0 {
		t.Fatalf("%d frames sent after the hub stopped", len(ch))
	}
	if st := RefreshBroadcasterStatsSnapshot(); st.TrailingEmitted != 0 || st.Published != 0 {
		t.Fatalf("a stopped hub's timer moved counters: trailing_emitted=%d published=%d", st.TrailingEmitted, st.Published)
	}
}

// Eviction of a key with a trailing emit owed: the eviction frame supersedes
// it — the subscriber gets the leading frame + the eviction frame, never a
// third (trailing) one.
func TestRefresh484_Lifecycle_EvictionSupersedesTrailing(t *testing.T) {
	lcSetup(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()
	PublishRefresh("k")
	PublishRefresh("k") // trailing owed
	PublishEviction("k")
	if n := RefreshCoalesceWindowsForTest(); n != 0 {
		t.Fatalf("armed timers=%d after eviction, want 0", n)
	}
	if got := collect(ch, lcWin+150*time.Millisecond)["k"]; got != 2 {
		t.Fatalf("%d frames, want 2 (leading + eviction; the trailing emit is superseded)", got)
	}
	if st := RefreshBroadcasterStatsSnapshot(); st.TrailingEmitted != 0 {
		t.Fatalf("trailing_emitted=%d, want 0", st.TrailingEmitted)
	}
}

// Leak check at scale: 2,000 armed keys each owed a trailing emit; armed
// timers never exceed armed keys and all are released once the keys go quiet.
func TestRefresh484_Lifecycle_TimersBoundedByArmedKeysAndReleased(t *testing.T) {
	lcSetup(t)
	const n = 2000
	keys := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		keys[fmt.Sprintf("leak-%d", i)] = struct{}{}
	}
	ch, unsub := SubscribeRefresh(keys)
	defer unsub()
	go func() {
		for range ch {
		}
	}()
	// An unarmed key never opens a window.
	for i := 0; i < 500; i++ {
		PublishRefresh(fmt.Sprintf("nobody-%d", i))
		PublishRefresh(fmt.Sprintf("nobody-%d", i))
	}
	if w := RefreshCoalesceWindowsForTest(); w != 0 {
		t.Fatalf("%d timers armed for keys nobody subscribes to", w)
	}
	for k := range keys {
		PublishRefresh(k)
		PublishRefresh(k)
		if w := RefreshCoalesceWindowsForTest(); w > n {
			t.Fatalf("armed timers=%d exceed armed keys %d", w, n)
		}
	}
	awaitWindows(t, 0, 4*lcWin+time.Second)
	if st := RefreshBroadcasterStatsSnapshot(); st.TrailingEmitted != n {
		t.Fatalf("trailing_emitted=%d want %d", st.TrailingEmitted, n)
	}
}

// --- (b) interplay -------------------------------------------------------------

// fullSink subscribes armed for keys + refreshSubChanCap fillers and fills
// the sink with the fillers (window 0 for the fill is not needed: distinct
// keys never coalesce against each other).
func fullSink(t *testing.T, keys ...string) (<-chan string, RefreshStream) {
	t.Helper()
	armed := map[string]struct{}{}
	for _, k := range keys {
		armed[k] = struct{}{}
	}
	fillers := make([]string, refreshSubChanCap)
	for i := range fillers {
		fillers[i] = fmt.Sprintf("filler-%d", i)
		armed[fillers[i]] = struct{}{}
	}
	st := SubscribeRefreshStream(armed)
	t.Cleanup(st.Unsub)
	for _, f := range fillers {
		PublishRefresh(f)
	}
	if len(st.Keys) != refreshSubChanCap {
		t.Fatalf("precondition: sink depth %d want %d", len(st.Keys), refreshSubChanCap)
	}
	return st.Keys, st
}

func countKey(ch <-chan string, k string) int {
	n := 0
	for {
		select {
		case v := <-ch:
			if v == k {
				n++
			}
		case <-time.After(150 * time.Millisecond):
			return n
		}
	}
}

// A key both in the trailing schedule and in the pending set is delivered
// ONCE, not twice.
func TestRefresh484_Interplay_TrailingAndPendingDeliverOnce(t *testing.T) {
	lcSetup(t)
	ch, _ := fullSink(t, "k")
	PublishRefresh("k") // leading: sink full -> refresh-pending; window opens
	PublishRefresh("k") // suppressed: trailing owed
	time.Sleep(lcWin + 100*time.Millisecond)
	st := RefreshBroadcasterStatsSnapshot()
	if st.TrailingEmitted != 1 {
		t.Fatalf("trailing_emitted=%d want 1", st.TrailingEmitted)
	}
	if st.PendingCoalesced != 1 {
		t.Fatalf("pending_coalesced=%d want 1 (the trailing emit found k already pending)", st.PendingCoalesced)
	}
	if got := countKey(ch, "k"); got != 1 {
		t.Fatalf("k delivered %d times, want exactly 1", got)
	}
}

// An eviction supersedes a pending refresh of the same key: ONE frame.
func TestRefresh484_Interplay_EvictionSupersedesPendingRefresh(t *testing.T) {
	withRefreshLayer(t) // window 0: isolate the pending interplay
	ch, _ := fullSink(t, "k")
	PublishRefresh("k") // refresh-pending
	PublishEviction("k")
	if s := RefreshSubSnapshotsForTest()[0]; s.RefreshPending != 0 || s.Pending != 1 {
		t.Fatalf("refresh_pending=%d evict_pending=%d, want 0 / 1 (eviction supersedes)", s.RefreshPending, s.Pending)
	}
	if got := countKey(ch, "k"); got != 1 {
		t.Fatalf("k delivered %d times, want exactly 1", got)
	}
}

// The reverse order: a refresh for a key already eviction-pending is absorbed.
func TestRefresh484_Interplay_RefreshAbsorbedByPendingEviction(t *testing.T) {
	withRefreshLayer(t)
	ch, _ := fullSink(t, "k")
	PublishEviction("k") // evict-pending
	PublishRefresh("k")
	if st := RefreshBroadcasterStatsSnapshot(); st.PendingCoalesced != 1 {
		t.Fatalf("pending_coalesced=%d want 1", st.PendingCoalesced)
	}
	if got := countKey(ch, "k"); got != 1 {
		t.Fatalf("k delivered %d times, want exactly 1", got)
	}
}

// The pending set never outgrows the armed set, however many publishes.
func TestRefresh484_Pending_BoundedByArmedSet(t *testing.T) {
	withRefreshLayer(t)
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("b-%d", i)
	}
	fullSink(t, keys...)
	for r := 0; r < 100; r++ {
		for _, k := range keys {
			PublishRefresh(k)
		}
	}
	if hw := RefreshPendingHighWaterForTest(); hw != int64(len(keys)) {
		t.Fatalf("refresh pending high-water=%d, want exactly %d (the armed non-filler keys)", hw, len(keys))
	}
	st := RefreshBroadcasterStatsSnapshot()
	if st.Deferred != uint64(len(keys)) || st.PendingCoalesced != uint64(99*len(keys)) || st.Dropped != 0 {
		t.Fatalf("deferred=%d pending_coalesced=%d dropped=%d, want %d / %d / 0",
			st.Deferred, st.PendingCoalesced, st.Dropped, len(keys), 99*len(keys))
	}
}

// --- overflow (stall) rule ------------------------------------------------------

// A consumer that accepts NO frame for the stall bound is force-resynced:
// Resync closes, both pending sets are released, nothing more is queued.
func TestRefresh484_Overflow_StallForcesResync(t *testing.T) {
	withRefreshLayer(t)
	restore := SetRefreshPendingStallBoundForTest(200 * time.Millisecond)
	defer restore()
	_, st := fullSink(t, "k1", "k2")
	PublishRefresh("k1")
	PublishEviction("k2")
	select {
	case <-st.Resync:
	case <-time.After(2 * time.Second):
		t.Fatalf("no resync 2s after a 200ms stall bound — a wedged consumer holds its backlog forever")
	}
	s := RefreshSubSnapshotsForTest()[0]
	if !s.Resynced || s.RefreshPending != 0 || s.Pending != 0 {
		t.Fatalf("after resync: resynced=%v refresh_pending=%d evict_pending=%d, want true/0/0", s.Resynced, s.RefreshPending, s.Pending)
	}
	if got := RefreshBroadcasterStatsSnapshot().PendingOverflowResync; got != 1 {
		t.Fatalf("pending_overflow_resync=%d want 1", got)
	}
	depth := len(st.Keys)
	PublishRefresh("k2")
	if len(st.Keys) != depth || RefreshSubSnapshotsForTest()[0].RefreshPending != 0 {
		t.Fatalf("a resynced subscriber still queues frames")
	}
}

// A SLOW but progressing consumer is never resynced: the stall clock tracks
// the CONSUMER (any frame leaving its sink), not only the refresh drain's own
// sends. Discriminating shape: every slot the consumer frees is taken at once
// by another sender (the eviction path's direct send: ch <- key +
// delivered++ under pmu — done here under pmu so the refresh drain cannot
// slip in between the read and the refill), so the refresh drain never gets
// a slot for 500 ms (2.5x the bound) while the consumer keeps reading. The
// drain and its stall rule are the real code under test.
func TestRefresh484_Overflow_SlowProgressingConsumerNotResynced(t *testing.T) {
	withRefreshLayer(t)
	restore := SetRefreshPendingStallBoundForTest(200 * time.Millisecond)
	defer restore()
	ch, st := fullSink(t, "k")
	PublishRefresh("k") // refresh-pending behind 64 fillers
	h := refreshHub()
	h.mu.RLock()
	var sub *refreshSub
	for _, s := range h.subs {
		sub = s
	}
	h.mu.RUnlock()
	for got := 0; got < 10; got++ {
		sub.pmu.Lock()
		select {
		case <-st.Resync:
			sub.pmu.Unlock()
			t.Fatalf("a consumer reading one frame per 50ms was resynced after %d frames", got)
		case <-ch:
		}
		sub.ch <- fmt.Sprintf("filler-%d", got) // the other sender takes the freed slot
		sub.delivered.Add(1)
		pending, resynced := len(sub.rpending), sub.resynced
		sub.pmu.Unlock()
		if resynced {
			t.Fatalf("a consumer reading one frame per 50ms was resynced after %d frames — the stall clock ignored its progress", got+1)
		}
		if pending != 1 {
			t.Fatalf("precondition: refresh_pending=%d want 1 — the drain got a slot, the arm would not discriminate", pending)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := RefreshBroadcasterStatsSnapshot().PendingOverflowResync; n != 0 {
		t.Fatalf("pending_overflow_resync=%d for a progressing consumer", n)
	}
}

// --- (e) customer path: the producer never waits on a wedged subscriber --------

// PublishRefresh is reached from the customer cold-fill (publishIfSubscribed)
// as well as the refresher. With wedged subscribers whose drains are spinning,
// every publish stays non-blocking: no lock is held across a blocking send
// (all sink sends are select/default under pmu).
func TestRefresh484_CustomerPath_PublishNeverWaitsOnWedgedSubscriber(t *testing.T) {
	withRefreshLayer(t)
	keys := make([]string, 32)
	for i := range keys {
		keys[i] = fmt.Sprintf("c-%d", i)
	}
	for i := 0; i < 8; i++ {
		fullSink(t, keys...) // 8 wedged subscribers with live refresh drains
	}
	var worst time.Duration
	for r := 0; r < 200; r++ {
		for _, k := range keys {
			t0 := time.Now()
			PublishRefresh(k)
			if d := time.Since(t0); d > worst {
				worst = d
			}
		}
	}
	// Under -race a non-blocking publish is microseconds; a blocked send would
	// hang the test. 50ms is a generous scheduler-noise ceiling.
	if worst > 50*time.Millisecond {
		t.Fatalf("worst PublishRefresh latency %v with wedged subscribers — the producer waited", worst)
	}
	t.Logf("worst PublishRefresh latency with 8 wedged subscribers: %v", worst)
}
