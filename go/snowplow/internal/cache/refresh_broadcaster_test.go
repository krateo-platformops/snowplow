// refresh_broadcaster_test.go — Ship 1 (live-refresh-coherence, option A)
// cache-layer falsifiers. feedback_falsifier_first_before_ship: these are A's
// gate. Pure in-memory (in-process broadcaster); KUBECONFIG unset; NEVER
// touches ./internal/rbac.
//
// Covers:
//   9.5a — PublishRefresh provably unreachable under cache-off (hub nil,
//          counters stay 0; the §2.4 nil-path).
//   9.6  — slow-consumer never stalls the producer (-race; blocked sink
//          DEFERS (#484, formerly dropped), OTHER subscribers still receive,
//          publish returns promptly, the slow sink gets the tail frame).
//   9.8  — the TERMINAL signal on a saturated sink is deferred and delivered
//          once the sink drains; its re-read is fresh (#484: the old premise,
//          a blind 5s-throttle refetch, does not exist in the SPA).
//   plus broadcaster mechanics: coalesce-per-key, keyRefs/HasRefreshSubscriber
//   refcount under arm/disarm/unsub, per-key routing (a sub only gets its own
//   armed keys).

package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// withRefreshLayer enables the SSE layer for a test and resets the singleton +
// counters before and after. Mirrors withCleanRefresher's setenv discipline.
func withRefreshLayer(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv(envRefreshSSEEnabled, "")        // default ON
	t.Setenv(envRefreshCoalesceWindowMS, "0") // coalescing OFF by default so
	// per-signal tests are deterministic; coalesce test sets it explicitly.
	resetRefreshBroadcasterForTest()
	t.Cleanup(resetRefreshBroadcasterForTest)
}

// drainOne reads one value from ch with a timeout; returns (val, true) or
// ("", false) on timeout.
func drainOne(t *testing.T, ch <-chan string, d time.Duration) (string, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(d):
		return "", false
	}
}

// --- 9.5a — cache-off: PublishRefresh provably unreachable -------------------

// TestRefreshBroadcaster_CacheOff_PublishUnreachable asserts that with the
// cache subsystem off, refreshHub() returns nil and PublishRefresh is a no-op:
// the broadcaster counters NEVER move. This is the §2.4 / falsifier-9.5a
// nil-path — the belt-and-braces guard complementing resolve_populate's
// pre-Put early return.
func TestRefreshBroadcaster_CacheOff_PublishUnreachable(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false") // master off
	resetRefreshBroadcasterForTest()
	t.Cleanup(resetRefreshBroadcasterForTest)

	if RefreshSSEEnabled() {
		t.Fatalf("RefreshSSEEnabled()=true under CACHE_ENABLED=false — gate broken")
	}
	if h := refreshHub(); h != nil {
		t.Fatalf("refreshHub() != nil under cache-off — want nil (the §2.4 transparent no-op path)")
	}

	// Direct PublishRefresh under cache-off must be a no-op.
	PublishRefresh("any-key")
	PublishRefresh("any-key")
	pub, del, drop, coal := RefreshBroadcasterCounters()
	if pub|del|drop|coal != 0 {
		t.Fatalf("counters moved under cache-off: published=%d delivered=%d dropped=%d coalesced=%d — PublishRefresh is NOT unreachable",
			pub, del, drop, coal)
	}

	// Subscribe under cache-off returns a closed channel (idle stream) + a
	// no-op unsub — the handler degrades to heartbeat-only.
	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()
	if _, ok := <-ch; ok {
		t.Fatalf("SubscribeRefresh under cache-off returned an OPEN channel — want a closed/idle channel")
	}
	if HasRefreshSubscriber("k") {
		t.Fatalf("HasRefreshSubscriber true under cache-off — want false")
	}
}

// TestRefreshBroadcaster_ToggleOff_PublishUnreachable asserts the per-feature
// REFRESH_SSE_ENABLED=false back-out knob also makes the layer a no-op while
// the cache itself stays on (provisional/removable, project_caching_is_provisional).
func TestRefreshBroadcaster_ToggleOff_PublishUnreachable(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv(envRefreshSSEEnabled, "false") // feature off, cache on
	resetRefreshBroadcasterForTest()
	t.Cleanup(resetRefreshBroadcasterForTest)

	if RefreshSSEEnabled() {
		t.Fatalf("RefreshSSEEnabled()=true under REFRESH_SSE_ENABLED=false")
	}
	if refreshHub() != nil {
		t.Fatalf("refreshHub() != nil under REFRESH_SSE_ENABLED=false")
	}
	PublishRefresh("k")
	if pub, del, drop, coal := RefreshBroadcasterCounters(); pub|del|drop|coal != 0 {
		t.Fatalf("counters moved with feature toggled off: %d/%d/%d/%d", pub, del, drop, coal)
	}
}

// --- per-key routing + delivery ---------------------------------------------

// TestRefreshBroadcaster_PerKeyRouting asserts a subscriber receives ONLY the
// keys it armed, and that an unarmed key is not delivered to it.
func TestRefreshBroadcaster_PerKeyRouting(t *testing.T) {
	withRefreshLayer(t)

	chA, unsubA := SubscribeRefresh(map[string]struct{}{"key-A": {}})
	defer unsubA()
	chB, unsubB := SubscribeRefresh(map[string]struct{}{"key-B": {}})
	defer unsubB()

	PublishRefresh("key-A")
	if v, ok := drainOne(t, chA, time.Second); !ok || v != "key-A" {
		t.Fatalf("A did not receive its armed key-A: got %q ok=%v", v, ok)
	}
	// B must NOT receive key-A (per-key routing).
	if v, ok := drainOne(t, chB, 150*time.Millisecond); ok {
		t.Fatalf("B received %q for key-A — per-key routing leaked", v)
	}

	PublishRefresh("key-B")
	if v, ok := drainOne(t, chB, time.Second); !ok || v != "key-B" {
		t.Fatalf("B did not receive its armed key-B: got %q ok=%v", v, ok)
	}
	// An entirely unsubscribed key delivers to nobody.
	PublishRefresh("key-UNSUB")
	if v, ok := drainOne(t, chA, 150*time.Millisecond); ok {
		t.Fatalf("A received unsubscribed key: %q", v)
	}
}

// --- keyRefs / HasRefreshSubscriber refcount --------------------------------

// TestRefreshBroadcaster_KeyRefsRefcount asserts the per-key subscriber
// reverse-index refcounts correctly across Subscribe / ArmKey / DisarmKey /
// unsub, and that HasRefreshSubscriber reflects it (the §12.3 O(1) presence
// check that A maintains for a later ship to consume).
func TestRefreshBroadcaster_KeyRefsRefcount(t *testing.T) {
	withRefreshLayer(t)

	if HasRefreshSubscriber("shared") {
		t.Fatalf("HasRefreshSubscriber(shared) true before any subscribe")
	}

	_, unsub1 := SubscribeRefresh(map[string]struct{}{"shared": {}})
	if !HasRefreshSubscriber("shared") {
		t.Fatalf("HasRefreshSubscriber(shared) false after 1 subscriber")
	}
	if n := RefreshSubscriberCount(); n != 1 {
		t.Fatalf("subscriber count=%d want 1", n)
	}

	// Second subscriber arms the same key -> refcount 2.
	_, unsub2 := SubscribeRefresh(map[string]struct{}{"shared": {}})
	if n := RefreshSubscriberCount(); n != 2 {
		t.Fatalf("subscriber count=%d want 2", n)
	}

	// Drop one — still armed by the other.
	unsub1()
	if !HasRefreshSubscriber("shared") {
		t.Fatalf("HasRefreshSubscriber(shared) false after 1 of 2 unsub — refcount underflow")
	}
	// Drop the other — now nobody is armed.
	unsub2()
	if HasRefreshSubscriber("shared") {
		t.Fatalf("HasRefreshSubscriber(shared) true after all unsub — keyRefs leak")
	}
	if n := RefreshSubscriberCount(); n != 0 {
		t.Fatalf("subscriber count=%d want 0 after all unsub", n)
	}

	// Idempotent unsub must not underflow.
	unsub1()
	unsub2()
	if HasRefreshSubscriber("shared") {
		t.Fatalf("HasRefreshSubscriber(shared) true after idempotent re-unsub")
	}
}

// TestRefreshBroadcaster_ArmDisarmLive asserts a live connection can add/remove
// keys without reconnecting, and delivery follows the armed set.
func TestRefreshBroadcaster_ArmDisarmLive(t *testing.T) {
	withRefreshLayer(t)

	// Subscribe with no keys, then arm one live.
	ch, unsub := SubscribeRefresh(map[string]struct{}{})
	defer unsub()

	// Reach the live *refreshSub to call Arm/Disarm (the handler holds it;
	// the test reaches it through the hub for the unit assertion).
	h := refreshHub()
	h.mu.RLock()
	var s *refreshSub
	for _, sub := range h.subs {
		s = sub
	}
	h.mu.RUnlock()
	if s == nil {
		t.Fatalf("no live sub found")
	}

	s.ArmKey("late-key")
	if !HasRefreshSubscriber("late-key") {
		t.Fatalf("ArmKey did not register late-key in keyRefs")
	}
	PublishRefresh("late-key")
	if v, ok := drainOne(t, ch, time.Second); !ok || v != "late-key" {
		t.Fatalf("did not receive late-armed key: got %q ok=%v", v, ok)
	}

	s.DisarmKey("late-key")
	if HasRefreshSubscriber("late-key") {
		t.Fatalf("DisarmKey did not clear late-key from keyRefs")
	}
	PublishRefresh("late-key")
	if v, ok := drainOne(t, ch, 150*time.Millisecond); ok {
		t.Fatalf("received %q after DisarmKey", v)
	}
}

// --- coalesce ----------------------------------------------------------------

// TestRefreshBroadcaster_CoalescePerKey asserts that N publishes for the SAME
// key within the coalesce window collapse to ONE fan-out (design §2.3), while
// a publish for a DIFFERENT key in the same window is NOT coalesced.
func TestRefreshBroadcaster_CoalescePerKey(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv(envRefreshSSEEnabled, "")
	t.Setenv(envRefreshCoalesceWindowMS, "500") // 500ms window
	resetRefreshBroadcasterForTest()
	t.Cleanup(resetRefreshBroadcasterForTest)

	ch, unsub := SubscribeRefresh(map[string]struct{}{"k1": {}, "k2": {}})
	defer unsub()

	// Burst of 5 publishes for k1 inside the window -> 1 delivered, 4 coalesced.
	for i := 0; i < 5; i++ {
		PublishRefresh("k1")
	}
	// k2 once -> delivered (different key, not coalesced against k1).
	PublishRefresh("k2")

	// Collect what arrives within a short drain.
	got := map[string]int{}
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		v, ok := drainOne(t, ch, 50*time.Millisecond)
		if !ok {
			continue
		}
		got[v]++
	}
	if got["k1"] != 1 {
		t.Fatalf("k1 delivered %d times in coalesce window — want exactly 1 (burst not collapsed)", got["k1"])
	}
	if got["k2"] != 1 {
		t.Fatalf("k2 delivered %d times — want 1 (different key wrongly coalesced)", got["k2"])
	}
	_, _, _, coalesced := RefreshBroadcasterCounters()
	if coalesced != 4 {
		t.Fatalf("coalesced counter=%d want 4 (the 4 suppressed k1 re-emits)", coalesced)
	}
}

// TestRefreshBroadcaster_ZeroWindowDisablesCoalescing (L11). An EXPLICIT
// REFRESH_COALESCE_WINDOW_MS=0 must DISABLE coalescing — a 5-event burst for
// the SAME key delivers all 5, coalesced==0. This guards the coalesced() win<=0
// early-return against a naive "0 means use the default 250ms" coercion that
// would silently collapse the burst to 1.
//
// The RED arm (TestRefreshBroadcaster_ZeroWindowRedArm below) proves the
// discrimination by transiently installing exactly that wrong-shaped resolver.
func TestRefreshBroadcaster_ZeroWindowDisablesCoalescing(t *testing.T) {
	withRefreshLayer(t) // sets REFRESH_COALESCE_WINDOW_MS=0 (coalescing OFF)

	// Sanity: the active window really is 0 (disable), not the default.
	if w := refreshCoalesceWindow(); w != 0 {
		t.Fatalf("refreshCoalesceWindow()=%v under REFRESH_COALESCE_WINDOW_MS=0 — want 0 (disable)", w)
	}

	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()

	const burst = 5
	for i := 0; i < burst; i++ {
		PublishRefresh("k")
	}

	// All 5 must be delivered (buffer cap 64 >> 5, so none dropped).
	got := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && got < burst {
		if _, ok := drainOne(t, ch, 50*time.Millisecond); ok {
			got++
		}
	}
	if got != burst {
		t.Fatalf("zero-window burst delivered %d/%d — coalescing was NOT disabled by window=0", got, burst)
	}
	published, delivered, dropped, coalesced := RefreshBroadcasterCounters()
	if coalesced != 0 {
		t.Fatalf("coalesced=%d want 0 — window=0 must fan out EVERY emit", coalesced)
	}
	if published != burst || delivered != burst || dropped != 0 {
		t.Fatalf("window=0 counters: published=%d delivered=%d dropped=%d, want %d/%d/0",
			published, delivered, dropped, burst, burst)
	}
}

// TestRefreshBroadcaster_ZeroWindowRedArm proves L11 DISCRIMINATES: with the
// refreshCoalesceWindowFn seam transiently swapped for the buggy resolver that
// coerces an explicit 0 into the 250ms DEFAULT, the same 5-event burst collapses
// (coalesced>0, delivered<5) — the exact defect the zero-window path prevents.
// Restores the real fn after (t.Cleanup) so the committed suite stays GREEN.
func TestRefreshBroadcaster_ZeroWindowRedArm(t *testing.T) {
	withRefreshLayer(t) // REFRESH_COALESCE_WINDOW_MS=0

	orig := refreshCoalesceWindowFn
	// Buggy resolver: treat 0 as "unset" → default window (the naive coercion
	// L11 guards against).
	refreshCoalesceWindowFn = func() time.Duration {
		w := orig()
		if w <= 0 {
			return time.Duration(defaultRefreshCoalesceWindowMS) * time.Millisecond
		}
		return w
	}
	t.Cleanup(func() { refreshCoalesceWindowFn = orig })

	ch, unsub := SubscribeRefresh(map[string]struct{}{"k": {}})
	defer unsub()

	const burst = 5
	for i := 0; i < burst; i++ {
		PublishRefresh("k")
	}

	got := 0
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, ok := drainOne(t, ch, 40*time.Millisecond); ok {
			got++
		}
	}
	_, _, _, coalesced := RefreshBroadcasterCounters()
	if coalesced == 0 {
		t.Fatalf("RED arm: coalesced=0 with 0-coerced-to-default window — L11 does not discriminate")
	}
	if got >= burst {
		t.Fatalf("RED arm: delivered %d/%d — the default-window coercion failed to collapse the burst", got, burst)
	}
	t.Logf("RED arm: 0-coerced-to-250ms → burst collapsed (delivered=%d/%d, coalesced=%d)", got, burst, coalesced)
}

// --- 9.6 — slow consumer never stalls the producer (-race) ------------------

// TestRefreshBroadcaster_SlowConsumerNeverStalls is falsifier 9.6. Run under
// -race. A subscriber whose sink is never drained must NOT block PublishRefresh:
// once its buffer (cap refreshSubChanCap) fills, further signals DEFER into its
// refresh pending set (#484 — formerly they dropped) and the producer returns
// promptly. A second, healthy subscriber must keep receiving throughout.
// Concurrent publishers + a concurrent (blocked) reader exercise the RWMutex
// fan-out path and the per-subscriber pmu for the race detector. Once the slow
// sink drains, the key's last change reaches it (it was not lost).
//
// #484 assertion changes (feedback_diff_for_deleted_tests_not_just_added):
//   - REMOVED `dropped > 0` ("the drop arm was exercised"). REPLACED by
//     `dropped == 0` AND `deferred + pending_coalesced > 0` (the full-sink arm
//     was exercised and deferred instead), the pending set <= the armed set,
//     and the slow sink receives the key after it drains.
//   - KEPT the producer-latency bound and the healthy-subscriber assertion.
func TestRefreshBroadcaster_SlowConsumerNeverStalls(t *testing.T) {
	withRefreshLayer(t)

	// Slow sink: subscribe, do not read until the publishers are done.
	chSlow, unsubSlow := SubscribeRefresh(map[string]struct{}{"hot": {}})
	defer unsubSlow()

	// Healthy sink on the same key.
	chFast, unsubFast := SubscribeRefresh(map[string]struct{}{"hot": {}})
	defer unsubFast()

	var fastReceived atomic.Int64
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for {
			select {
			case _, ok := <-chFast:
				if !ok {
					return
				}
				fastReceived.Add(1)
			case <-time.After(2 * time.Second):
				return // no more signals
			}
		}
	}()

	// Fire far more than the slow sink's buffer can hold, from several
	// goroutines (race coverage on the fan-out RLock).
	const publishers = 4
	const perPublisher = 200 // 800 >> refreshSubChanCap (64)
	publishStart := time.Now()
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				PublishRefresh("hot")
			}
		}()
	}
	wg.Wait()
	publishElapsed := time.Since(publishStart)

	// The producer must never have blocked on the full slow sink.
	if publishElapsed > 2*time.Second {
		t.Fatalf("PublishRefresh took %s for %d publishes — a slow consumer STALLED the producer",
			publishElapsed, publishers*perPublisher)
	}

	st := RefreshBroadcasterStatsSnapshot()
	if st.Dropped != 0 {
		t.Fatalf("dropped=%d — a full sink must DEFER, never drop (#484)", st.Dropped)
	}
	if st.Deferred+st.PendingCoalesced == 0 {
		t.Fatalf("deferred=%d pending_coalesced=%d — the slow sink never overflowed; the full-sink arm was not exercised",
			st.Deferred, st.PendingCoalesced)
	}
	if hw := RefreshPendingHighWaterForTest(); hw > 1 {
		t.Fatalf("refresh pending high-water=%d exceeds the slow sub's armed set (1) — dedup broken", hw)
	}

	// The healthy sink must have received signals throughout.
	<-fastDone
	if fastReceived.Load() == 0 {
		t.Fatalf("healthy subscriber received 0 signals — the slow consumer starved it")
	}

	// Drain the slow sink: everything queued arrives, the last one included —
	// the backlog's tail is a frame for "hot" sent AFTER the last publish was
	// deferred, so the slow tab learns of the final change.
	got := 0
	for {
		if _, ok := drainOne(t, chSlow, 200*time.Millisecond); !ok {
			break
		}
		got++
	}
	if got != refreshSubChanCap+1 {
		t.Fatalf("slow sink drained %d frames, want %d (the full sink + exactly one deferred tail frame)", got, refreshSubChanCap+1)
	}
	t.Logf("9.6 slow-consumer: %d publishes in %s, delivered=%d deferred=%d pending_coalesced=%d, healthy_received=%d, slow drained=%d",
		publishers*perPublisher, publishElapsed.Round(time.Millisecond), st.Delivered, st.Deferred, st.PendingCoalesced, fastReceived.Load(), got)
}

// --- 9.8 — the TERMINAL signal on a saturated sink is deferred, never lost ---

// TestRefreshBroadcaster_DroppedTerminalSignalDegrades is falsifier 9.8 at the
// cache layer. A saturated sink receives the FINAL signal for a key (no
// further commit re-signals). Since #484 that signal is DEFERRED into the
// subscriber's pending set and delivered once the sink drains, and the
// re-read it triggers returns the fresh body.
//
// #484 assertion changes (feedback_diff_for_deleted_tests_not_just_added):
//   - REMOVED the premise "the frontend's blind 5s-throttle refetch (a plain
//     Get) reads the fresh value". It is false: the SPA does not poll and
//     does not refetch on focus (#484 trace), so a Get the client never
//     issues proved nothing about the tab. REMOVED `dropped > 0` as setup.
//   - REPLACED by: setup `deferred > 0` (the terminal signal hit a full
//     sink), `dropped == 0`, and the SUBSCRIBER receives a frame for the key
//     after draining, whose re-read is the fresh body (F-DROP's cache twin,
//     TestRefresh484_FDrop_FullSinkChangeReachesClient, runs it K×M).
func TestRefreshBroadcaster_DroppedTerminalSignalDegrades(t *testing.T) {
	withRefreshLayer(t)
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "3600")

	c := ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache nil")
	}
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-a", Name: "w"}
	key := ComputeKey(in)
	t.Cleanup(func() { c.DeleteForTest(key) })

	chSlow, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()

	// Saturate the sink with signals for the stale value.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"phase":"v1"}`), Inputs: &in})
	for i := 0; i < refreshSubChanCap; i++ {
		PublishRefresh(key)
	}
	// The refresher commits the FRESH value, then the TERMINAL signal.
	const fresh = `{"phase":"v2-FRESH"}`
	c.Put(key, &ResolvedEntry{RawJSON: []byte(fresh), Inputs: &in})
	PublishRefresh(key)
	st := RefreshBroadcasterStatsSnapshot()
	if st.Deferred == 0 {
		t.Fatalf("setup: the terminal signal was not deferred (deferred=0); cannot test the full-sink case")
	}
	if st.Dropped != 0 {
		t.Fatalf("dropped=%d — the terminal signal on a full sink was discarded (#484)", st.Dropped)
	}

	// The client drains; the LAST frame it reads was sent after the fresh
	// commit, and its re-read returns the fresh body.
	frames, last := 0, ""
	for {
		k, ok := drainOne(t, chSlow, 200*time.Millisecond)
		if !ok {
			break
		}
		frames++
		if e, hit := c.Get(k); hit && e != nil {
			last = string(e.RawJSON)
		}
	}
	if frames != refreshSubChanCap+1 {
		t.Fatalf("drained %d frames, want %d (the saturated sink + the deferred terminal frame)", frames, refreshSubChanCap+1)
	}
	if last != fresh {
		t.Fatalf("the re-read after the final frame returned %q want %q — the terminal signal stranded the widget (9.8 FAIL)", last, fresh)
	}
}
