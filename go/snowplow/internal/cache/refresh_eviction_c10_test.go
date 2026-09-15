// refresh_eviction_c10_test.go — 1.12.6 item 7 (C10/C11/C12) cache-layer
// falsifiers that use the new instruments (they do not compile on main;
// their RED transcripts run on main + the named probe patch, or against an
// unpaced PublishEviction — see reports/dev-1126-c10.md).
//
//	S2  — 1000 subscribers, ONE armed for the key ⇒ the fan-out visits 1
//	      subscriber (≤ 5), asserted on the visit counter (an O(|subs|) loop
//	      visits 1000; RED on main + probe).
//	S4  — armed_keys, max_sink_depth, evict_published, evict_deferred,
//	      stream_seconds_total, streams_closed_total present AS EXPVARS after
//	      a real subscribe/evict/unsub cycle (RED on main: fields absent).
//	S10 — -race: publish from inside runEvictionBatch / EvictSelfGone under
//	      concurrent subscribe/unsub/arm/disarm; the store lock is FREE at the
//	      publish; nothing is delivered after unsub.
//	pace — hub-level pacing companions of the S9 ship gate (which drives the
//	      real handler in internal/handlers): deferred-never-dropped, dedup
//	      while deferred, full sink defers, pending ≤ armed, disarm withdraws.

package cache

import (
	"encoding/json"
	"expvar"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- S2 — fan-out visits only the armed subscribers ---------------------------

func TestRefreshEviction_S2_FanoutVisitsOnlyArmedSubscribers(t *testing.T) {
	withRefreshLayer(t)
	const others = 1000
	unsubs := make([]func(), 0, others)
	for i := 0; i < others; i++ {
		_, u := SubscribeRefresh(map[string]struct{}{fmt.Sprintf("L1_other_%d", i): {}})
		unsubs = append(unsubs, u)
	}
	defer func() {
		for _, u := range unsubs {
			u()
		}
	}()
	const target = "L1_target"
	ch, unsub := SubscribeRefresh(map[string]struct{}{target: {}})
	defer unsub()
	if RefreshSubscriberCount() != others+1 {
		t.Fatalf("S2 precondition: %d subscribers, want %d", RefreshSubscriberCount(), others+1)
	}

	before := RefreshFanoutVisitsForTest()
	PublishRefresh(target)
	visits := RefreshFanoutVisitsForTest() - before
	if got, ok := drainOne(t, ch, time.Second); !ok || got != target {
		t.Fatalf("S2 precondition: the armed subscriber did not receive %q (got %q ok=%v)", target, got, ok)
	}
	if visits > 5 {
		t.Fatalf("S2 RED: PublishRefresh visited %d subscribers for a key ONE subscriber armed — "+
			"the fan-out is O(|subs|), not O(armed-for-key) (want <= 5)", visits)
	}
	// Panic probe — the counter is live (a dead probe would also read 0).
	if visits == 0 {
		t.Fatalf("S2 probe: the visit counter did not move across a delivered publish — dead instrument")
	}

	before = RefreshFanoutVisitsForTest()
	PublishEviction(target)
	visits = RefreshFanoutVisitsForTest() - before
	if got, ok := drainOne(t, ch, time.Second); !ok || got != target {
		t.Fatalf("S2 (eviction): the armed subscriber did not receive %q (got %q ok=%v)", target, got, ok)
	}
	if visits > 5 || visits == 0 {
		t.Fatalf("S2 RED (eviction): PublishEviction visited %d subscribers (want 1..5)", visits)
	}
}

// --- S4 — the C12 numbers exist as expvars -------------------------------------

func refreshExpvarFields(t *testing.T) map[string]any {
	t.Helper()
	v := expvar.Get("snowplow_refresh_broadcaster")
	if v == nil {
		t.Fatalf("S4: expvar snowplow_refresh_broadcaster not registered")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(v.String()), &m); err != nil {
		t.Fatalf("S4: expvar JSON: %v", err)
	}
	return m
}

func TestRefreshEviction_S4_ExpvarsAfterRealCycle(t *testing.T) {
	// rate 1 / burst 1: the second key of a two-key eviction burst is
	// guaranteed to be deferred, so evict_deferred must move.
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "1")
	t.Setenv(envRefreshEvictionPublishBurst, "1")
	d, store := evictionHarness(t, 100, time.Hour)
	RegisterRefreshBroadcasterExpvarForTest()
	gvr := gvrFlexes()

	keys := []string{"L1_s4_a", "L1_s4_b"}
	putSelf(store, d, keys[0], gvr, "ns", "a")
	putSelf(store, d, keys[1], gvr, "ns", "b")
	ch, unsub := SubscribeRefresh(map[string]struct{}{keys[0]: {}, keys[1]: {}})

	d.OnDelete(gvr, "ns", "a")
	d.OnDelete(gvr, "ns", "b")
	if _, ok := drainOne(t, ch, time.Second); !ok {
		t.Fatalf("S4 precondition: first eviction not delivered")
	}

	m := refreshExpvarFields(t)
	for _, k := range []string{"armed_keys", "max_sink_depth", "evict_published", "evict_deferred", "stream_seconds_total", "streams_closed_total"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("S4 RED: expvar field %q absent from snowplow_refresh_broadcaster (have %v) — "+
				"the C12 number would be invisible in production (logs are dark at LOG_LEVEL=warn)", k, m)
		}
	}
	// Panic probe — presence discriminates: a field that must NOT exist is absent.
	if _, ok := m["evict_dropped"]; ok {
		t.Fatalf("S4 probe: unexpected field evict_dropped present — the presence check is not discriminating")
	}
	if m["armed_keys"].(float64) != 2 {
		t.Fatalf("S4: armed_keys=%v want 2", m["armed_keys"])
	}
	if m["evict_published"].(float64) != 2 {
		t.Fatalf("S4: evict_published=%v want 2", m["evict_published"])
	}
	if m["evict_deferred"].(float64) < 1 {
		t.Fatalf("S4 RED: evict_deferred=%v after a burst over the bucket — the bound did not engage (or is not counted)", m["evict_deferred"])
	}
	if m["max_sink_depth"].(float64) < 1 {
		t.Fatalf("S4: max_sink_depth=%v want >= 1 after a delivered send", m["max_sink_depth"])
	}
	if m["streams_closed_total"].(float64) != 0 {
		t.Fatalf("S4: streams_closed_total=%v before unsub, want 0", m["streams_closed_total"])
	}

	time.Sleep(5 * time.Millisecond) // a measurable lifetime
	unsub()
	m = refreshExpvarFields(t)
	if m["streams_closed_total"].(float64) != 1 {
		t.Fatalf("S4 RED: streams_closed_total=%v after unsub, want 1", m["streams_closed_total"])
	}
	if m["stream_seconds_total"].(float64) <= 0 {
		t.Fatalf("S4 RED: stream_seconds_total=%v after a closed stream, want > 0", m["stream_seconds_total"])
	}
	if m["armed_keys"].(float64) != 0 {
		t.Fatalf("S4: armed_keys=%v after unsub, want 0 (the reverse index must release the keys)", m["armed_keys"])
	}
	// The deferred second key must not have been dropped: it was withdrawn
	// only because the stream closed (the pending set dies with the sub).
	if _, _, dropped, _ := RefreshBroadcasterCounters(); dropped != 0 {
		t.Fatalf("S4: dropped=%d on the eviction path, want 0 (eviction defers, never drops)", dropped)
	}
}

// --- S10 — -race + lock order + no deliver-after-unsub ---------------------------

func TestRefreshEviction_S10_LockFreeAtPublish(t *testing.T) {
	d, store := evictionHarness(t, 100, time.Hour)
	gvr := gvrFlexes()
	var probed atomic.Int32
	orig := publishEvictionFn
	publishEvictionFn = func(l1Key string) {
		probed.Add(1)
		// The eviction path takes d.storeMu (read) then c.mu (deleteForDep).
		// Both must be FREE here: a TryLock that fails on this single
		// goroutine means the caller still holds it — the inversion S10 pins.
		if !store.mu.TryLock() {
			t.Errorf("S10 RED: c.mu still held at the eviction publish for %q — the hub would run under the store lock", l1Key)
		} else {
			store.mu.Unlock()
		}
		if !d.storeMu.TryLock() {
			t.Errorf("S10 RED: d.storeMu still held at the eviction publish for %q", l1Key)
		} else {
			d.storeMu.Unlock()
		}
		orig(l1Key)
	}
	t.Cleanup(func() { publishEvictionFn = orig })

	ch, unsub := SubscribeRefresh(map[string]struct{}{"L1_s10_a": {}, "L1_s10_b": {}})
	defer unsub()
	putSelf(store, d, "L1_s10_a", gvr, "ns", "a")
	putSelf(store, d, "L1_s10_b", gvr, "ns", "b")
	d.OnDelete(gvr, "ns", "a")        // runEvictionBatch route
	if !d.EvictSelfGone("L1_s10_b") { // self-404 route
		t.Fatalf("S10: EvictSelfGone reported no eviction")
	}
	if probed.Load() != 2 {
		t.Fatalf("S10 probe: the publish hook ran %d times, want 2 (both DELETE-semantics sites)", probed.Load())
	}
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		k, ok := drainOne(t, ch, 2*time.Second)
		if !ok {
			t.Fatalf("S10: eviction %d not delivered", i)
		}
		got[k] = true
	}
	if !got["L1_s10_a"] || !got["L1_s10_b"] {
		t.Fatalf("S10: delivered set %v, want both keys", got)
	}
}

func TestRefreshEviction_S10_RaceConcurrentSubscribeEvict(t *testing.T) {
	// A small bucket so the drain goroutines are live during the churn.
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "200")
	t.Setenv(envRefreshEvictionPublishBurst, "2")
	d, store := evictionHarness(t, 1000, time.Hour)
	gvr := gvrFlexes()
	const pool = 24
	keyOf := func(i int) string { return fmt.Sprintf("L1_race_%02d", i) }
	nameOf := func(i int) string { return fmt.Sprintf("obj-%02d", i) }

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var chans sync.Map // *<-chan string -> struct{} for dead subs
	// Subscribers: subscribe on a random 3-key subset, arm/disarm, read a bit, unsub.
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				keys := map[string]struct{}{}
				for i := 0; i < 3; i++ {
					keys[keyOf(r.Intn(pool))] = struct{}{}
				}
				ch, unsub := SubscribeRefresh(keys)
				// reach the sub for arm/disarm churn through the hub map
				h := refreshHub()
				var s *refreshSub
				h.mu.RLock()
				for _, cand := range h.subs {
					if cand.ch == ch {
						s = cand
					}
				}
				h.mu.RUnlock()
				extra := keyOf(r.Intn(pool))
				s.ArmKey(extra)
				select {
				case <-ch:
				case <-time.After(time.Duration(r.Intn(3)) * time.Millisecond):
				}
				s.DisarmKey(extra)
				unsub()
				chans.Store(&ch, struct{}{})
			}
		}(int64(g))
	}
	// Evictors: put + DELETE-evict / self-404-evict random pool keys.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(100 + seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := r.Intn(pool)
				putSelf(store, d, keyOf(i), gvr, "ns", nameOf(i))
				if r.Intn(2) == 0 {
					d.OnDelete(gvr, "ns", nameOf(i))
				} else {
					d.EvictSelfGone(keyOf(i))
				}
			}
		}(int64(g))
	}
	// Refresher path publishes concurrently too (9.6 must stay green).
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(999))
		for {
			select {
			case <-stop:
				return
			default:
			}
			PublishRefresh(keyOf(r.Intn(pool)))
		}
	}()
	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	// No deliver-after-unsub: every unsubscribed sink is quiescent — its
	// occupancy does not change once the sub is gone (the drain goroutine
	// sends only under pmu with closed==false).
	snap := map[*<-chan string]int{}
	chans.Range(func(k, _ any) bool {
		p := k.(*<-chan string)
		snap[p] = len(*p)
		return true
	})
	time.Sleep(50 * time.Millisecond)
	for p, n := range snap {
		if len(*p) != n {
			t.Fatalf("S10 RED: an unsubscribed sink changed occupancy %d -> %d — delivered after unsub", n, len(*p))
		}
	}
	if RefreshSubscriberCount() != 0 {
		t.Fatalf("S10: %d subscribers leaked", RefreshSubscriberCount())
	}
	if _, _, dropped, _ := RefreshBroadcasterCounters(); dropped != 0 {
		// PublishRefresh on a full 64-slot sink can legitimately drop under
		// this churn; the eviction path must not. We cannot attribute the
		// global counter, so only report.
		t.Logf("S10: dropped=%d (refresher-path drops under churn are legitimate)", dropped)
	}
}

// --- pacing companions of S9 -----------------------------------------------------

// readAll drains ch into a slice until n distinct values arrived or d passed.
func readAll(ch <-chan string, n int, d time.Duration) (order []string, first time.Time) {
	deadline := time.After(d)
	seen := map[string]int{}
	for len(seen) < n {
		select {
		case k := <-ch:
			if first.IsZero() {
				first = time.Now()
			}
			seen[k]++
			order = append(order, k)
		case <-deadline:
			return order, first
		}
	}
	return order, first
}

func TestRefreshEviction_Pace_DeferredNeverDroppedDedup(t *testing.T) {
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "100")
	t.Setenv(envRefreshEvictionPublishBurst, "5")
	d, store := evictionHarness(t, 1000, time.Hour)
	gvr := gvrFlexes()
	const n = 60
	armed := map[string]struct{}{}
	for i := 0; i < n; i++ {
		armed[fmt.Sprintf("L1_pace_%02d", i)] = struct{}{}
	}
	ch, unsub := SubscribeRefresh(armed)
	defer unsub()
	for i := 0; i < n; i++ {
		putSelf(store, d, fmt.Sprintf("L1_pace_%02d", i), gvr, "ns", fmt.Sprintf("o%02d", i))
	}
	// One tick: n DELETE evictions, then the SAME first key evicted again
	// three times while it is still deferred-or-delivered.
	for i := 0; i < n; i++ {
		d.OnDelete(gvr, "ns", fmt.Sprintf("o%02d", i))
	}
	for r := 0; r < 3; r++ {
		putSelf(store, d, "L1_pace_59", gvr, "ns", "o59")
		d.OnDelete(gvr, "ns", "o59")
	}

	st := RefreshBroadcasterStatsSnapshot()
	if st.EvictDeferred < n-5 {
		t.Fatalf("pace RED: evict_deferred=%d after %d evictions at burst 5 — the bucket did not defer (unpaced)", st.EvictDeferred, n)
	}
	if hw := RefreshEvictPendingHighWaterForTest(); hw > int64(n) {
		t.Fatalf("pace RED: pending high-water %d exceeds the armed set %d", hw, n)
	}
	order, _ := readAll(ch, n, 5*time.Second)
	// Keep reading past the N distinct keys: an over-delivery of the
	// re-evicted key lands AFTER the FIFO backlog, where a read that stops
	// at N distinct keys cannot see it (probe P9 stayed green until this).
	extra, _ := readAll(ch, 4, 300*time.Millisecond)
	order = append(order, extra...)
	seen := map[string]int{}
	for _, k := range order {
		seen[k]++
	}
	if len(seen) != n {
		t.Fatalf("pace RED: %d of %d evicted keys delivered — a deferred eviction was DROPPED", len(seen), n)
	}
	// Dedup while deferred: the re-evicted key 59 was queued once; it may
	// legitimately arrive a second time only if a re-eviction landed AFTER
	// its first delivery. Never more than 2.
	if seen["L1_pace_59"] > 2 {
		t.Fatalf("pace RED: key re-evicted while deferred delivered %d times, want <= 2 (dedup'd pending set)", seen["L1_pace_59"])
	}
	// FIFO: the first burst keys precede the deferred ones.
	if order[0] != "L1_pace_00" {
		t.Fatalf("pace: first delivered %q, want L1_pace_00 (FIFO)", order[0])
	}
	if _, _, dropped, _ := RefreshBroadcasterCounters(); dropped != 0 {
		t.Fatalf("pace RED: dropped=%d on the eviction path", dropped)
	}
	if snaps := RefreshSubSnapshotsForTest(); len(snaps) != 1 || snaps[0].Pending != 0 || snaps[0].Armed != n {
		t.Fatalf("pace: sub snapshot after drain %+v, want pending 0 armed %d", snaps, n)
	}
}

func TestRefreshEviction_Pace_FullSinkDefersThenDelivers(t *testing.T) {
	// Bucket wide open: only the 64-slot sink can push keys into pending.
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "100000")
	t.Setenv(envRefreshEvictionPublishBurst, "1000")
	withRefreshLayer(t)
	const n = refreshSubChanCap + 6
	armed := map[string]struct{}{}
	for i := 0; i < n; i++ {
		armed[fmt.Sprintf("L1_full_%02d", i)] = struct{}{}
	}
	ch, unsub := SubscribeRefresh(armed)
	defer unsub()
	for i := 0; i < n; i++ {
		PublishEviction(fmt.Sprintf("L1_full_%02d", i)) // nobody reading yet
	}
	st := RefreshBroadcasterStatsSnapshot()
	if st.EvictDeferred != 6 {
		t.Fatalf("full-sink RED: evict_deferred=%d, want 6 (the sink holds %d; the rest must be deferred, not dropped)", st.EvictDeferred, refreshSubChanCap)
	}
	if st.Dropped != 0 {
		t.Fatalf("full-sink RED: dropped=%d on the eviction path", st.Dropped)
	}
	if st.MaxSinkDepth != refreshSubChanCap {
		t.Fatalf("full-sink: max_sink_depth=%d want %d", st.MaxSinkDepth, refreshSubChanCap)
	}
	order, _ := readAll(ch, n, 5*time.Second)
	if len(order) != n {
		t.Fatalf("full-sink RED: %d of %d delivered once the consumer resumed", len(order), n)
	}
}

func TestRefreshEviction_Pace_DisarmWithdrawsPending(t *testing.T) {
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "1")
	t.Setenv(envRefreshEvictionPublishBurst, "1")
	withRefreshLayer(t)
	ch, unsub := SubscribeRefresh(map[string]struct{}{"L1_d_a": {}, "L1_d_b": {}})
	defer unsub()
	h := refreshHub()
	h.mu.RLock()
	var s *refreshSub
	for _, cand := range h.subs {
		s = cand
	}
	h.mu.RUnlock()
	PublishEviction("L1_d_a") // burst slot
	PublishEviction("L1_d_b") // deferred ~1s
	if snaps := RefreshSubSnapshotsForTest(); snaps[0].Pending != 1 {
		t.Fatalf("disarm precondition: pending=%d want 1", snaps[0].Pending)
	}
	s.DisarmKey("L1_d_b")
	if snaps := RefreshSubSnapshotsForTest(); snaps[0].Pending != 0 || snaps[0].Armed != 1 {
		t.Fatalf("disarm RED: after DisarmKey pending=%d armed=%d, want 0/1 (pending must never exceed armed)", snaps[0].Pending, snaps[0].Armed)
	}
	if got, ok := drainOne(t, ch, 100*time.Millisecond); !ok || got != "L1_d_a" {
		t.Fatalf("disarm: burst key not delivered (got %q ok=%v)", got, ok)
	}
	if got, ok := drainOne(t, ch, 1500*time.Millisecond); ok {
		t.Fatalf("disarm RED: withdrawn key %q was delivered after DisarmKey", got)
	}
}

// TestRefreshEviction_KnobsFallBackBelowOne pins the env contract: a rate or
// burst < 1 is not a kill switch (a zero rate would strand every eviction
// signal); both fall back to the provisional defaults.
func TestRefreshEviction_KnobsFallBackBelowOne(t *testing.T) {
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "0")
	t.Setenv(envRefreshEvictionPublishBurst, "-3")
	r, b := refreshEvictionBucket()
	if int(r) != defaultRefreshEvictionPublishRate || b != defaultRefreshEvictionPublishBurst {
		t.Fatalf("knobs: rate=%v burst=%d, want defaults %d/%d", r, b, defaultRefreshEvictionPublishRate, defaultRefreshEvictionPublishBurst)
	}
	t.Setenv(envRefreshEvictionPublishRatePerSecond, "7")
	t.Setenv(envRefreshEvictionPublishBurst, "3")
	if r, b = refreshEvictionBucket(); int(r) != 7 || b != 3 {
		t.Fatalf("knobs: rate=%v burst=%d, want 7/3", r, b)
	}
}
