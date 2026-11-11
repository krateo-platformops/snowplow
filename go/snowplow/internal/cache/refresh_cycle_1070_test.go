package cache

// refresh_cycle_1070_test.go — #1070: the refresh cycle's instruments
// (refresh_cycle.go). Every instant is placed on an injected dirty-window
// clock, never read off the wall (#930/#951), so each arm asserts exact values.
//
//   C1  every accepted refresh Put counts identical or changed, per class, on
//       the tier its ctx carries (direct without a refresher sink); a write
//       that lands between the compare and the store lock voids the compare
//       (changed); a refused Put counts nothing.
//   C2  the wait (first insert since the last dequeue → worker start) and the
//       duration (worker start → Put) record the injected timings on the
//       tier the key ran on, through the real refresher, for each tier.
//   C3  a key re-added while it is processing waits from the re-add.
//   C4  Put → frame is recorded for an open stream, at the written frame, and
//       not for an absent stream, a closed one, or a frame with no Put.
//   C5  a coalesced publish keeps the earliest Put it absorbed.

import (
	"bytes"
	"context"
	"encoding/json"
	"expvar"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
)

type cycleClock struct{ ns atomic.Int64 }

func installCycleClock(t *testing.T) *cycleClock {
	t.Helper()
	c := &cycleClock{}
	c.ns.Store(time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC).UnixNano())
	t.Cleanup(SetDirtyWindowClockForTest(func() time.Time { return time.Unix(0, c.ns.Load()) }))
	return c
}

func (c *cycleClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

func cyclePutCount(class, tier, content string) uint64 {
	for _, p := range RefreshCycleStatsSnapshot().Puts {
		if p.Class == class && p.Tier == tier && p.Content == content {
			return p.N
		}
	}
	return 0
}

func cycleEnv(t *testing.T) *ResolvedCacheStore {
	t.Helper()
	terminalEnv(t)
	t.Cleanup(withCleanRefresher(t, 1, 0))
	ResetRefreshCycleForTest()
	t.Cleanup(ResetRefreshCycleForTest)
	ResetRefreshBroadcasterForTest()
	t.Cleanup(ResetRefreshBroadcasterForTest)
	c := ResolvedCache()
	if c == nil {
		t.Fatal("PRE: resolved cache disabled")
	}
	Deps().SetStore(c)
	return c
}

// cycleRefreshPut is the refresher terminal write of key with body, on ctx.
func cycleRefreshPut(c *ResolvedCacheStore, ctx context.Context, key string, in ResolvedKeyInputs, body string) bool {
	return c.replaceIfGenRefresh(WithL1KeyContext(ctx, key), key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in}, c.CaptureGen(key)).Stored()
}

func TestRefreshCycle1070_C1_EveryRefreshPutCountsIdenticalOrChangedPerClass(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	installCycleClock(t)
	for _, class := range RefreshCycleClasses[:len(RefreshCycleClasses)-1] {
		in := ResolvedKeyInputs{CacheEntryClass: class, Namespace: "c1070", Name: "c1-" + class}
		key := ComputeKey(in)
		c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
		ctx := withRefreshCycle(context.Background(), key, RefreshTierViewed, windowNow())
		if !cycleRefreshPut(c, ctx, key, in, `{"v":1}`) || !cycleRefreshPut(c, ctx, key, in, `{"v":2}`) ||
			!cycleRefreshPut(c, ctx, key, in, `{"v":2}`) {
			t.Fatalf("PRE (%s): a refresh Put of a live cell was refused", class)
		}
		if id, ch := cyclePutCount(class, RefreshTierViewed, "identical"), cyclePutCount(class, RefreshTierViewed, "changed"); id != 2 || ch != 1 {
			t.Fatalf("C1 RED (%s): identical=%d changed=%d, want 2 identical (v1→v1, v2→v2) and 1 changed (v1→v2)", class, id, ch)
		}
		// No refresher sink on ctx (an out-of-queue refresh): tier direct.
		if !cycleRefreshPut(c, context.Background(), key, in, `{"v":3}`) {
			t.Fatalf("PRE (%s): direct Put refused", class)
		}
		if got := cyclePutCount(class, RefreshTierDirect, "changed"); got != 1 {
			t.Fatalf("C1 RED (%s): a Put with no refresher sink counted direct|changed=%d, want 1", class, got)
		}
	}
	// A class with no registered handler is "other", never a new series.
	in := ResolvedKeyInputs{CacheEntryClass: "unheard-of", Namespace: "c1070", Name: "c1-other"}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	cycleRefreshPut(c, context.Background(), key, in, `{"v":1}`)
	if got := cyclePutCount(refreshClassOther, RefreshTierDirect, "identical"); got != 1 {
		t.Fatalf("C1 RED: an unregistered class counted other|direct|identical=%d, want 1", got)
	}
	// A refused Put (the cell left the store) counts nothing.
	before := RefreshCycleStatsSnapshot().Puts
	gone := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "c1070", Name: "c1-gone"}
	if cycleRefreshPut(c, context.Background(), ComputeKey(gone), gone, `{"v":1}`) {
		t.Fatal("PRE: a refresh Put of an absent cell was accepted")
	}
	for i, p := range RefreshCycleStatsSnapshot().Puts {
		if p.N != before[i].N {
			t.Fatalf("C1 RED: a refused refresh Put moved %s|%s|%s", p.Class, p.Tier, p.Content)
		}
	}
}

// TestRefreshCycle1070_C1b_AWriteAfterTheCompareVoidsIt — the prior compared
// equal is not the one replaced (a write landed between the off-lock compare
// and the store lock), so the Put counts changed, as #406 voids its compare.
func TestRefreshCycle1070_C1b_AWriteAfterTheCompareVoidsIt(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "c1070", Name: "c1b"}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	fired := false
	hook := func() {
		if !fired {
			fired = true
			c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in}) // same bytes, another entry
		}
	}
	raLayerAfterCompareHook.Store(&hook)
	t.Cleanup(func() { raLayerAfterCompareHook.Store(nil) })
	if !cycleRefreshPut(c, context.Background(), key, in, `{"v":1}`) || !fired {
		t.Fatalf("PRE: the Put was refused or the compare window never opened (fired=%v)", fired)
	}
	if id, ch := cyclePutCount(CacheEntryClassRAFullList, RefreshTierDirect, "identical"), cyclePutCount(CacheEntryClassRAFullList, RefreshTierDirect, "changed"); id != 0 || ch != 1 {
		t.Fatalf("C1b RED: identical=%d changed=%d after a write landed between compare and lock, want 0/1", id, ch)
	}
}

// cycleRunOne marks key twice (inserted at T0, re-marked at T0+500 ms while
// queued), waits 1500 ms in all, starts the refresher, and the handler takes
// 2500 ms to its Put.
func cycleRunOne(t *testing.T, tier string) {
	c := cycleEnv(t)
	clk := installCycleClock(t)
	_, keys := terminalFixtureNoReset(t, 1)
	key := keys[0]
	switch tier {
	case RefreshTierViewed:
		viewed765Arm(t, key)
	case RefreshTierClusterList:
		RegisterClusterListKey(key)
	}
	done := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, in ResolvedKeyInputs) error {
		clk.advance(2500 * time.Millisecond)
		cycleRefreshPut(c, ctx, k, in, `{"body":"new"}`)
		done <- struct{}{}
		return nil
	})
	terminalMark([]string{key})
	clk.advance(500 * time.Millisecond)
	terminalMark([]string{key}) // dedup'd: the first insert's stamp stands
	clk.advance(1000 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base := RefresherWorkSnapshot()
	StartRefresher(ctx)
	q1Wait(t, done, "the refresh")
	if _, err := WaitRefresherQuiescentForTest(base, time.Minute); err != nil {
		t.Fatalf("PRE: %v", err)
	}
	s := RefreshCycleStatsSnapshot()
	for _, tt := range RefreshQueueTiers {
		w, d := s.Wait[tt], s.Duration[tt]
		if tt != tier {
			if w.Count != 0 || d.Count != 0 {
				t.Fatalf("C2 RED (%s): tier %s recorded wait=%d duration=%d samples; the key ran on %s", tier, tt, w.Count, d.Count, tier)
			}
			continue
		}
		if w.Count != 1 || w.SumMS != 1500 || w.MaxMS != 1500 {
			t.Fatalf("C2 RED (%s): wait count=%d sum=%d max=%d, want one sample of 1500 ms (first insert → start); all waits %+v durations %+v viewed dequeues %d", tier, w.Count, w.SumMS, w.MaxMS, s.Wait, s.Duration, refresherSingleton().viewed.dequeued.Load())
		}
		if d.Count != 1 || d.SumMS != 2500 {
			t.Fatalf("C2 RED (%s): duration count=%d sum=%d, want one sample of 2500 ms (start → Put)", tier, d.Count, d.SumMS)
		}
		// Bucket placement against the goal marks: 1500 > 1000, ≤ 2000; 2500 > 2000, ≤ 3000.
		if b := bucketAt(t, w, 1000); b != 0 {
			t.Fatalf("C2 RED (%s): le_1000 wait=%d, want 0", tier, b)
		}
		if b := bucketAt(t, w, 2000); b != 1 {
			t.Fatalf("C2 RED (%s): le_2000 wait=%d, want 1", tier, b)
		}
		if b, b3 := bucketAt(t, d, 2000), bucketAt(t, d, 3000); b != 0 || b3 != 1 {
			t.Fatalf("C2 RED (%s): duration le_2000=%d le_3000=%d, want 0/1", tier, b, b3)
		}
	}
	if got := cyclePutCount("widgets", tier, "changed"); got != 1 {
		t.Fatalf("C2 RED (%s): the refresh Put counted widgets|%s|changed=%d, want 1", tier, tier, got)
	}
}

func bucketAt(t *testing.T, h RefreshCycleHist, bound int64) uint64 {
	t.Helper()
	for i, b := range RefreshCycleBoundsMS {
		if b == bound {
			return h.Buckets[i]
		}
	}
	t.Fatalf("no bound %d", bound)
	return 0
}

func TestRefreshCycle1070_C2_WaitAndDurationRecordInjectedTimingsPerTier(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	for _, tier := range RefreshQueueTiers {
		t.Run(tier, func(t *testing.T) { cycleRunOne(t, tier) })
	}
}

func TestRefreshCycle1070_C3_AReAddWhileProcessingWaitsFromTheReAdd(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	clk := installCycleClock(t)
	_, keys := terminalFixtureNoReset(t, 1)
	key := keys[0]
	var calls atomic.Int32
	done := make(chan struct{}, 2)
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, in ResolvedKeyInputs) error {
		clk.advance(100 * time.Millisecond)
		cycleRefreshPut(c, ctx, k, in, `{"body":"new"}`)
		if calls.Add(1) == 1 {
			terminalMark([]string{k}) // re-added while processing
			clk.advance(4000 * time.Millisecond)
		}
		done <- struct{}{}
		return nil
	})
	terminalMark([]string{key})
	clk.advance(1500 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	base := RefresherWorkSnapshot()
	StartRefresher(ctx)
	q1Wait(t, done, "the first refresh")
	q1Wait(t, done, "the re-added key's refresh")
	if _, err := WaitRefresherQuiescentForTest(base, time.Minute); err != nil {
		t.Fatalf("PRE: %v", err)
	}
	w := RefreshCycleStatsSnapshot().Wait[RefreshTierNormal]
	if w.Count != 2 || w.SumMS != 1500+4000 || w.MaxMS != 4000 {
		t.Fatalf("C3 RED: wait count=%d sum=%d max=%d, want samples 1500 (first insert) and 4000 (the re-add, not the first insert)",
			w.Count, w.SumMS, w.MaxMS)
	}
}

func cycleResidentKey(t *testing.T, c *ResolvedCacheStore, name string) (string, ResolvedKeyInputs) {
	t.Helper()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "c1070", Name: name}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &in})
	return key, in
}

func cycleRecv(t *testing.T, st RefreshStream) string {
	t.Helper()
	select {
	case k := <-st.Keys:
		return k
	case <-time.After(10 * time.Second):
		t.Fatal("PRE: no frame offered to the open stream")
		return ""
	}
}

func TestRefreshCycle1070_C4_PutToFrameIsRecordedForAnOpenStreamOnly(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	t.Setenv(envRefreshCoalesceWindowMS, "0")
	clk := installCycleClock(t)
	open, openIn := cycleResidentKey(t, c, "c4-open")
	absent, absentIn := cycleResidentKey(t, c, "c4-absent")
	closed, closedIn := cycleResidentKey(t, c, "c4-closed")
	st := SubscribeRefreshStream(map[string]struct{}{open: {}})
	t.Cleanup(st.Unsub)
	gone := SubscribeRefreshStream(map[string]struct{}{closed: {}})
	gone.Unsub()

	for _, k := range []struct {
		key string
		in  ResolvedKeyInputs
	}{{open, openIn}, {absent, absentIn}, {closed, closedIn}} {
		ctx := WithL1KeyContext(withRefreshCycle(context.Background(), k.key, RefreshTierViewed, windowNow()), k.key)
		clk.advance(200 * time.Millisecond)
		if !c.replaceIfGenRefresh(ctx, k.key, &ResolvedEntry{RawJSON: []byte(`{"v":"new"}`), Inputs: &k.in}, c.CaptureGen(k.key)).Stored() {
			t.Fatal("PRE: refresh Put refused")
		}
		PublishRefreshed(ctx, k.key, PutChanged)
	}
	if got := cycleRecv(t, st); got != open {
		t.Fatalf("PRE: the open stream was offered another key")
	}
	if n := RefreshCycleStatsSnapshot().PutToFrame[RefreshTierViewed].Count; n != 0 {
		t.Fatalf("C4 RED: %d put_to_frame samples before any frame was written (an offer is not a frame)", n)
	}
	clk.advance(1200 * time.Millisecond)
	st.Wrote(open)
	gone.Wrote(closed) // a late write on a closed stream: nothing owed
	h := RefreshCycleStatsSnapshot().PutToFrame[RefreshTierViewed]
	if h.Count != 1 || h.SumMS != 1200+2*200 {
		// The open key's Put was the first of three, each 200 ms apart, so its
		// frame is 400 ms (the other two Puts) + 1200 ms after it.
		t.Fatalf("C4 RED: put_to_frame count=%d sum=%d, want exactly one sample of 1600 ms — the open stream only "+
			"(absent and closed streams record nothing)", h.Count, h.SumMS)
	}
	// A second frame for the same key with no new Put records nothing; a plain
	// publish (no refresh Put on it) records nothing either.
	PublishRefresh(open)
	cycleRecv(t, st)
	st.Wrote(open)
	if n := RefreshCycleStatsSnapshot().PutToFrame[RefreshTierViewed].Count; n != 1 {
		t.Fatalf("C4 RED: a frame owed no Put recorded a sample (count=%d, want 1)", n)
	}
}

func TestRefreshCycle1070_C5_ACoalescedPublishKeepsTheEarliestPut(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	// A real window long enough that both later publishes land inside it on a
	// loaded host; the asserted values are on the injected clock.
	t.Setenv(envRefreshCoalesceWindowMS, "2000")
	clk := installCycleClock(t)
	key, in := cycleResidentKey(t, c, "c5")
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	t.Cleanup(st.Unsub)
	put := func(body string) {
		ctx := WithL1KeyContext(withRefreshCycle(context.Background(), key, RefreshTierNormal, windowNow()), key)
		if !c.replaceIfGenRefresh(ctx, key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in}, c.CaptureGen(key)).Stored() {
			t.Fatal("PRE: refresh Put refused")
		}
		PublishRefreshed(ctx, key, PutChanged)
	}
	put(`{"v":1}`) // opens the window, offered at once
	cycleRecv(t, st)
	clk.advance(100 * time.Millisecond)
	st.Wrote(key)  // sample 1: 100 ms
	put(`{"v":2}`) // suppressed: the earliest owed Put
	clk.advance(300 * time.Millisecond)
	put(`{"v":3}`)   // suppressed
	cycleRecv(t, st) // the trailing emit at the window's end
	clk.advance(700 * time.Millisecond)
	st.Wrote(key) // sample 2: from the v2 Put, 300 + 700 = 1000 ms
	h := RefreshCycleStatsSnapshot().PutToFrame[RefreshTierNormal]
	if h.Count != 2 || h.SumMS != 100+1000 || h.MaxMS != 1000 {
		t.Fatalf("C5 RED: put_to_frame count=%d sum=%d max=%d, want 100 ms and 1000 ms — the trailing frame timed from the "+
			"EARLIEST suppressed Put (700 ms would be the latest)", h.Count, h.SumMS, h.MaxMS)
	}
}

// TestRefreshCycle1070_C7_ADirectRefreshIsTimedFromItsStart — RunRefreshNow
// (the D2 refill, no queue) times its duration on tier direct, and has no wait.
func TestRefreshCycle1070_C7_ADirectRefreshIsTimedFromItsStart(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	clk := installCycleClock(t)
	key, _ := cycleResidentKey(t, c, "c7")
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, in ResolvedKeyInputs) error {
		clk.advance(900 * time.Millisecond)
		cycleRefreshPut(c, ctx, k, in, `{"v":"new"}`)
		return nil
	})
	if err := RunRefreshNow(context.Background(), key); err != nil {
		t.Fatalf("PRE: %v", err)
	}
	s := RefreshCycleStatsSnapshot()
	if d := s.Duration[RefreshTierDirect]; d.Count != 1 || d.SumMS != 900 {
		t.Fatalf("C7 RED: direct duration count=%d sum=%d, want one sample of 900 ms", d.Count, d.SumMS)
	}
	for _, tt := range RefreshQueueTiers {
		if s.Wait[tt].Count != 0 {
			t.Fatalf("C7 RED: a direct refresh recorded a %s wait", tt)
		}
	}
	if got := cyclePutCount("widgets", RefreshTierDirect, "changed"); got != 1 {
		t.Fatalf("C7 RED: widgets|direct|changed=%d, want 1", got)
	}
}

// TestRefreshCycle1070_C6_ExpvarPublishesTheFamily — the four keys exist, the
// put counter carries every closed-set cell, and a histogram reads its samples.
func TestRefreshCycle1070_C6_ExpvarPublishesTheFamily(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	cycleEnv(t)
	RegisterRefreshCycleExpvarForTest()
	if !runRefreshCycle("c6", tierNormal, true, 1500*time.Millisecond, 2500*time.Millisecond, 1200*time.Millisecond) {
		t.Fatal("PRE: the driven cycle did not complete")
	}
	var puts map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_refresh_cycle_put_total").String()), &puts); err != nil {
		t.Fatal(err)
	}
	if want := len(RefreshCycleClasses) * len(RefreshCycleTiers) * len(RefreshPutContents); len(puts) != want {
		t.Fatalf("C6 RED: put_total has %d cells, want the closed set of %d", len(puts), want)
	}
	if puts["widgets|normal|identical"] != 1 {
		t.Fatalf("C6 RED: widgets|normal|identical=%d, want 1", puts["widgets|normal|identical"])
	}
	for key, want := range map[string]uint64{
		"snowplow_refresh_cycle_wait_ms":               1500,
		"snowplow_refresh_cycle_duration_ms":           2500,
		"snowplow_refresh_broadcaster_put_to_frame_ms": 1200,
	} {
		var m map[string]struct {
			Buckets map[string]uint64 `json:"buckets"`
			Count   uint64            `json:"count"`
			SumMS   uint64            `json:"sum_ms"`
		}
		if err := json.Unmarshal([]byte(expvar.Get(key).String()), &m); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		n := m[RefreshTierNormal]
		if n.Count != 1 || n.SumMS != want || n.Buckets["le_inf"] != 1 || n.Buckets["le_1000"] != 0 || n.Buckets["le_3000"] != 1 {
			t.Fatalf("C6 RED: %s normal = %+v, want one sample of %d ms (le_1000=0, le_3000=1, le_inf=1)", key, n, want)
		}
	}
}

// runRefreshCycle drives one refresh cycle of a widgets cell through the
// production recording path, on a private store and the dirty-window clock at
// exact instants: inserted, started wait later, Put duration after that with
// identical or changed bytes, published, and its frame written toFrame after
// the Put on an open stream.
func runRefreshCycle(name string, tier refreshTier, identical bool, wait, duration, toFrame time.Duration) bool {
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC).UnixNano())
	restore := SetDirtyWindowClockForTest(func() time.Time { return time.Unix(0, clock.Load()) })
	defer restore()

	c := newResolvedCache(16, 1<<20, time.Hour)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "refresh-cycle-1070", Name: name}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &in})
	st := SubscribeRefreshStream(map[string]struct{}{key: {}})
	defer st.Unsub()

	stamps := &enqueueStamps{}
	(&stampingQueue{TypedInterface: workqueue.NewTyped[string](), stamps: stamps}).Add(key)
	clock.Add(int64(wait))
	ctx := startRefreshCycle(WithL1KeyContext(context.Background(), key), key, tier, stamps.take(key))
	clock.Add(int64(duration))
	body := `{"v":"new"}`
	if identical {
		body = `{"v":"old"}`
	}
	if !c.replaceIfGenRefresh(ctx, key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in}, c.CaptureGen(key)).Stored() {
		return false
	}
	PublishRefreshed(ctx, key, PutChanged)
	select {
	case k := <-st.Keys:
		clock.Add(int64(toFrame))
		st.Wrote(k)
		return k == key
	case <-time.After(10 * time.Second):
		return false
	}
}

var bench1070Sink bool

// BenchmarkRefreshCycle1070_FixedCostPerRefresh is what #1070 adds to one
// refresh apart from the byte compare: the insert stamp and its take, the
// start (wait sample + sink), the Put's count and duration sample, and the
// frame's owed stamp and put_to_frame sample. Independent of the body.
func BenchmarkRefreshCycle1070_FixedCostPerRefresh(b *testing.B) {
	key := ComputeKey(ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "bench1070", Name: "b"})
	stamps := &enqueueStamps{}
	sub := &refreshSub{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		stamps.at.LoadOrStore(key, windowNow()) // stampingQueue.Add, minus the queue's own Add
		ctx := startRefreshCycle(context.Background(), key, tierNormal, stamps.take(key))
		noteRefreshPut(ctx, key, "widgets", true, windowNow())
		o := refreshOwedFrom(ctx, key)
		sub.pmu.Lock()
		sub.noteOwedLocked(key, o)
		sub.pmu.Unlock()
		sub.frameWritten(key)
	}
}

// BenchmarkRefreshCycle1070_PutCompare is the identical test of a refresh Put
// (sameBytesPrior) at its worst case, equal bodies (a full scan; a changed body
// of another length costs one length check), beside the compare every refresh
// already pays for the same bytes (#612 classifyRebuild's bytes.Equal), so the
// cost reads as a share of existing work on a loaded host. An RA full-list body
// pays nothing new: #406 compared it before, and the two now share one compare.
func BenchmarkRefreshCycle1070_PutCompare(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20} {
		body := make([]byte, size)
		for i := range body {
			body[i] = byte('a' + i%26)
		}
		c := newResolvedCache(16, 1<<30, time.Hour)
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "bench1070", Name: "b"}
		key := ComputeKey(in)
		c.put(key, &ResolvedEntry{RawJSON: body, Inputs: &in})
		next := &ResolvedEntry{RawJSON: append([]byte(nil), body...), Inputs: &in}
		b.Run(strconv.Itoa(size>>10)+"KiB/added", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				bench1070Sink = c.sameBytesPrior(key, next) != nil
			}
		})
		b.Run(strconv.Itoa(size>>10)+"KiB/existing612", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				prior, _ := c.GetNoTouch(key)
				bench1070Sink = bytes.Equal(prior.Raw(), next.RawJSON)
			}
		})
	}
}
