package cache

// issue1321_publish_on_change_test.go — #1321: a /refreshes frame is published
// only for a Put that changed the stored bytes (put_outcome.go), and the
// #1062 stale-push ledger closes a stale serve whose replacing bytes equal the
// served ones (stale_push.go). On the real store, hub and ledger, with the
// ledger clock seamed (exact delays, no sleeps outside the #484 window).
//
//	O1  the store reports the outcome: identical vs the confirmed prior,
//	    changed for new bytes or a fill of an absent key — for the refresh
//	    replace AND the cold PutIfGen. The refresh replace compares before
//	    its lock (#1070), so a write that lands between that compare and the
//	    lock voids it (changed: a frame is never withheld on a guess). The
//	    cold PutIfGen compares against the entry it actually replaced, read
//	    in its own critical section (writeOutcome): a write landing before
//	    its lock is simply what it replaces, compared exactly.
//	L1  a stale l1 serve replaced (a Put that began after the mark) by the
//	    very bytes it served: no frame is published, and the ledger closes the
//	    serve at the Put, close="identical", with the exact delay.
//	L2  identical bytes from a Put that began BEFORE the mark prove nothing:
//	    the serve stays open.
//	L3  per client: an older serve with other bytes, followed by a frame for
//	    the change and a newer serve of the changed bytes, closes with that
//	    newer one when an identical Put replaces both.
//	L4  a late-opened serve (#1198) whose window Put carried the served bytes
//	    closes "identical" at once (no frame will come for it).
//	L5  a changed replacement still closes at the frame WRITE ("pushed").
//	T1  #484: frames are never dropped. A change coalesced inside the window,
//	    followed by an identical Put, still reaches every client at the
//	    trailing edge: each client ends up HOLDING the newest bytes.
//	S1  the owed-push mark (#710) is settled only when every body written
//	    under it equals the replacement.

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ── O1 ──────────────────────────────────────────────────────────────────────

func TestIssue1321_O1_TheStoreReportsWhetherAPutChangedTheBytes(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	c := cycleEnv(t)
	for _, tc := range []struct {
		name string
		put  func(ctx context.Context, key string, e *ResolvedEntry) PutOutcome
		// racer is the bytes a write landing between the RAFullList compare and
		// the store lock stores, and want the outcome of the Put of {"v":2} then.
		racer string
		want  PutOutcome
	}{
		// The refresh replace compared {"v":2} against the prior BEFORE the racer
		// replaced it with equal bytes: void, so changed.
		{"refresh_replace", func(ctx context.Context, key string, e *ResolvedEntry) PutOutcome {
			return c.replaceIfGenRefresh(ctx, key, e, c.CaptureGen(key))
		}, `{"v":2}`, PutChanged},
		// The cold PutIfGen replaces the racer's {"v":3}: the #406 compare saw
		// {"v":2} == {"v":2}, but the bytes it actually replaced differ.
		{"cold_putifgen", func(ctx context.Context, key string, e *ResolvedEntry) PutOutcome {
			return c.putIfGenOutcome(ctx, key, e, c.CaptureGen(key))
		}, `{"v":3}`, PutChanged},
		// … and when the racer stored the same bytes, the Put replaced equal
		// bytes: identical, exactly (the racer's own write announced its change).
		{"cold_putifgen_same_racer", func(ctx context.Context, key string, e *ResolvedEntry) PutOutcome {
			return c.putIfGenOutcome(ctx, key, e, c.CaptureGen(key))
		}, `{"v":2}`, PutIdentical},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "o1-1321", Name: tc.name}
			key := ComputeKey(in)
			c.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
			body := func(b string) *ResolvedEntry { return &ResolvedEntry{RawJSON: []byte(b), Inputs: &in} }
			if got := tc.put(context.Background(), key, body(`{"v":1}`)); got != PutIdentical {
				t.Fatalf("O1 RED: same bytes as the resident prior reported %v, want identical", got)
			}
			if got := tc.put(context.Background(), key, body(`{"v":2}`)); got != PutChanged {
				t.Fatalf("O1 RED: new bytes reported %v, want changed", got)
			}
			// A write between the compare and the lock: ANOTHER entry (tc.racer).
			fired := false
			hook := func() {
				if !fired {
					fired = true
					c.put(key, body(tc.racer))
				}
			}
			raLayerAfterCompareHook.Store(&hook)
			got := tc.put(context.Background(), key, body(`{"v":2}`))
			raLayerAfterCompareHook.Store(nil)
			if !fired || got != tc.want {
				t.Fatalf("O1 RED: a write of %s that landed between compare and lock → %v (fired=%v), want %v — the outcome "+
					"must be decided against the entry the Put REPLACED, and never withhold a frame on a void compare",
					tc.racer, got, fired, tc.want)
			}
		})
	}
	// A fill of an absent key has no prior: changed. A refused Put: refused.
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "o1-1321", Name: "absent"}
	key := ComputeKey(in)
	if got := c.putIfGenOutcome(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in}, c.CaptureGen(key)); got != PutChanged {
		t.Fatalf("O1 RED: a fill of an absent key reported %v, want changed", got)
	}
	gone := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "o1-1321", Name: "gone"}
	if got := c.replaceIfGenRefresh(context.Background(), ComputeKey(gone), &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &gone}, 0); got != PutRefused {
		t.Fatalf("O1 RED: a refresh replace of an absent key reported %v, want refused", got)
	}
}

// ── the ledger ──────────────────────────────────────────────────────────────

// refreshPut1321 is the refresher terminal's write of body under rctx, then its
// gated publish — exactly what resolve_populate.go does.
func (e *k1062Env) refreshPut1321(t *testing.T, rctx context.Context, body string) PutOutcome {
	t.Helper()
	in := *mustInputs(t, e.c, e.key)
	put := e.c.replaceIfGenRefresh(rctx, e.key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &in}, e.c.CaptureGen(e.key))
	if !put.Stored() {
		t.Fatalf("PRECONDITION: the refresh Put was refused")
	}
	PublishRefreshed(rctx, e.key, put)
	return put
}

// fresh1321 is the ctx of a resolve that begins now (after any earlier mark).
func (e *k1062Env) fresh1321() context.Context { return WithL1KeyContext(context.Background(), e.key) }

func TestIssue1321_L1_AStaleServeReplacedByTheBytesItServedClosesAtThePut(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t) // served {"body":"old"}
	e.advance(700 * time.Millisecond)
	if put := e.refreshPut1321(t, e.fresh1321(), `{"body":"old"}`); put != PutIdentical {
		t.Fatalf("PRECONDITION: the replacing Put reported %v, want identical", put)
	}
	if !noFrameWithin(t, st, 50*time.Millisecond) {
		t.Fatal("L1 RED: an identical refresh Put published a frame")
	}
	s := k1062Layer(t, StaleLayerL1)
	if s.Open != 0 || s.Closes[stalePushCloseIdentical] != 1 || s.Closes["pushed"] != 0 {
		t.Fatalf("L1 RED: open=%d identical=%d pushed=%d, want 0/1/0 — the client holds the replacing bytes and no frame "+
			"follows, so an open serve here would read as unreplaced at 10 s (#1321 companion to #1062)",
			s.Open, s.Closes[stalePushCloseIdentical], s.Closes["pushed"])
	}
	if s.ToPush.Count != 1 || s.ToPush.SumMS != 700 || s.Unreplaced != 0 {
		t.Fatalf("L1 RED: stale_to_push=%+v unreplaced=%d, want one 700 ms sample (serve → the confirming Put)", s.ToPush, s.Unreplaced)
	}
	e.advance(11 * time.Second)
	ScanStalePushOverdueForTest()
	if got := k1062Layer(t, StaleLayerL1).Unreplaced; got != 0 {
		t.Fatalf("L1 RED: the overdue scan counted %d unreplaced for a client that holds the current bytes", got)
	}
}

func TestIssue1321_L2_IdenticalBytesFromAPutThatBeganBeforeTheMarkProveNothing(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	early := e.fresh1321() // the resolve begins …
	e.mark()               // … then the data moves
	e.hit(t)
	e.refreshPut1321(t, early, `{"body":"old"}`)
	if s := k1062Layer(t, StaleLayerL1); s.Open != 1 || s.Closes[stalePushCloseIdentical] != 0 {
		t.Fatalf("L2 RED: a Put that read the pre-change state closed the serve (open=%d identical=%d)", s.Open, s.Closes[stalePushCloseIdentical])
	}
}

func TestIssue1321_L3_PerClientTheNewestServeDecides(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	early := e.fresh1321()
	e.mark()
	e.hit(t) // serve 1: {"body":"old"}
	e.advance(200 * time.Millisecond)
	// A CHANGED Put that began before the mark: it replaces nothing, but it
	// publishes; the client refetches and reads it (still dirty: serve 2).
	if put := e.refreshPut1321(t, early, `{"body":"mid"}`); put != PutChanged {
		t.Fatalf("PRECONDITION: %v, want changed", put)
	}
	written(st, cycleRecv(t, st)) // closes nothing: not replaced yet
	e.hit(t)                      // serve 2: {"body":"mid"}
	e.advance(300 * time.Millisecond)
	if put := e.refreshPut1321(t, e.fresh1321(), `{"body":"mid"}`); put != PutIdentical {
		t.Fatalf("PRECONDITION: %v, want identical", put)
	}
	s := k1062Layer(t, StaleLayerL1)
	if s.Open != 0 || s.Closes[stalePushCloseIdentical] != 2 {
		t.Fatalf("L3 RED: open=%d identical=%d, want 0/2 — the client's newest read holds the replacing bytes, so both of "+
			"its replaced serves close, as a frame written to it now would close them", s.Open, s.Closes[stalePushCloseIdentical])
	}
	if s.ToPush.Count != 2 || s.ToPush.SumMS != 500+300 {
		t.Fatalf("L3 RED: stale_to_push=%+v, want samples 500 ms and 300 ms", s.ToPush)
	}
}

func TestIssue1321_L4_ALateOpenWhoseWindowPutCarriedTheServedBytesClosesAtOnce(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.readThenLateOpen(t, func() {
		e.refreshPut1321(t, e.fresh1321(), `{"body":"old"}`) // began after the mark, same bytes
	})
	if !noFrameWithin(t, st, 50*time.Millisecond) {
		t.Fatal("L4 RED: an identical refresh Put published a frame")
	}
	if s := k1062Layer(t, StaleLayerL1); s.Serves != 1 || s.Open != 0 || s.Closes[stalePushCloseIdentical] != 1 {
		t.Fatalf("L4 RED: serves=%d open=%d identical=%d, want 1/0/1 — no frame follows the Put that landed before the "+
			"serve opened", s.Serves, s.Open, s.Closes[stalePushCloseIdentical])
	}
}

func TestIssue1321_L5_AChangedReplacementStillClosesAtTheFrameWrite(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t)
	e.refreshPut1321(t, e.fresh1321(), `{"body":"new"}`)
	k := cycleRecv(t, st)
	if s := k1062Layer(t, StaleLayerL1); s.Open != 1 {
		t.Fatalf("L5 RED: a changed replacement closed the serve before its frame was written (open=%d)", s.Open)
	}
	e.advance(900 * time.Millisecond)
	written(st, k)
	if s := k1062Layer(t, StaleLayerL1); s.Open != 0 || s.Closes["pushed"] != 1 || s.Closes[stalePushCloseIdentical] != 0 || s.ToPush.SumMS != 900 {
		t.Fatalf("L5 RED: open=%d pushed=%d identical=%d %+v, want 0/1/0 and 900 ms", s.Open, s.Closes["pushed"],
			s.Closes[stalePushCloseIdentical], s.ToPush)
	}
}

// ── T1: #484 trailing edge ──────────────────────────────────────────────────

func TestIssue1321_T1_ACoalescedChangeFollowedByAnIdenticalPutReachesEveryClient(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	withRealCoalesceWindow(t)
	store := ResolvedCache()
	if store == nil {
		t.Fatal("ResolvedCache nil")
	}
	type cell struct {
		key string
		in  ResolvedKeyInputs
	}
	cells := make([]cell, f484K)
	keys := make([]string, f484K)
	for i := range cells {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-1321", Name: fmt.Sprintf("t1-%d", i)}
		cells[i] = cell{key: ComputeKey(in), in: in}
		keys[i] = cells[i].key
		store.put(cells[i].key, &ResolvedEntry{RawJSON: []byte(`{"v":0}`), Inputs: &cells[i].in})
	}
	t.Cleanup(func() {
		for _, k := range keys {
			store.DeleteForTest(k)
		}
	})
	clients := make([]*spaClient, f484M)
	for i := range clients {
		ch, unsub := SubscribeRefresh(armSet(keys))
		t.Cleanup(unsub)
		clients[i] = newSPAClient(t, store, ch, nil)
	}
	v := func(gen int) func(string) string { return func(string) string { return fmt.Sprintf(`{"v":%d}`, gen) } }
	commit := func(c cell, body string) PutOutcome {
		ctx := WithL1KeyContext(context.Background(), c.key)
		put := store.replaceIfGenRefresh(ctx, c.key, &ResolvedEntry{RawJSON: []byte(body), Inputs: &c.in}, store.CaptureGen(c.key))
		if !put.Stored() {
			t.Fatalf("PRE: refresh Put refused")
		}
		PublishRefreshed(ctx, c.key, put)
		return put
	}

	for _, c := range cells {
		commit(c, v(1)(c.key))
	}
	if miss := awaitAllHold(clients, keys, v(1), time.Second); len(miss) > 0 {
		t.Fatalf("setup: v1 not held by every client within 1s: %v", firstFew(miss))
	}
	time.Sleep(100 * time.Millisecond) // inside the 250 ms window v1 opened
	b0 := RefreshBroadcasterStatsSnapshot()
	for _, c := range cells {
		if commit(c, v(2)(c.key)) != PutChanged { // suppressed by the window: owed a trailing emit
			t.Fatal("PRE: v2 not a change")
		}
		if commit(c, v(2)(c.key)) != PutIdentical { // the same bytes again: withheld
			t.Fatal("PRE: the repeat of v2 not identical")
		}
	}
	b1 := RefreshBroadcasterStatsSnapshot()
	if b1.Coalesced-b0.Coalesced != f484K || b1.WithheldIdentical-b0.WithheldIdentical != f484K {
		t.Fatalf("precondition: coalesced Δ=%d withheld Δ=%d, want %d each (the arm must exercise a suppressed change "+
			"followed by an identical Put)", b1.Coalesced-b0.Coalesced, b1.WithheldIdentical-b0.WithheldIdentical, f484K)
	}
	if miss := awaitAllHold(clients, keys, v(2), time.Second); len(miss) > 0 {
		t.Fatalf("T1 RED (#484): %d client×key cells still hold v1 1 s after v2 — the identical Put that followed the "+
			"coalesced change cancelled its trailing emit, so the change never reached them: %v", len(miss), firstFew(miss))
	}
}

// ── S1: the owed-push mark ──────────────────────────────────────────────────

func TestIssue1321_S1_AnOwedMarkSettlesOnlyWhenEveryBodyUnderItMatches(t *testing.T) {
	ResetStaleOwedForTest()
	t.Cleanup(ResetStaleOwedForTest)
	MarkStaleOwed("k1", []byte(`{"a":1}`))
	MarkStaleOwed("k1", []byte(`{"a":1}`))
	if SettleStaleOwed("k1", []byte(`{"a":2}`)) || !StaleOwed("k1") {
		t.Fatal("S1 RED: a replacement with other bytes settled the mark")
	}
	if !SettleStaleOwed("k1", []byte(`{"a":1}`)) || StaleOwed("k1") {
		t.Fatal("S1 RED: a replacement with the marked bytes did not settle the mark")
	}
	MarkStaleOwed("k2", []byte(`{"a":1}`))
	MarkStaleOwed("k2", []byte(`{"a":2}`)) // a second response with another body
	if SettleStaleOwed("k2", []byte(`{"a":2}`)) || SettleStaleOwed("k2", []byte(`{"a":1}`)) || !StaleOwed("k2") {
		t.Fatal("S1 RED: a mark two different bodies were written under was settled — one of those clients is still owed a frame")
	}
	MarkStaleOwed("k3", nil) // unknown body
	if SettleStaleOwed("k3", []byte(`{"a":1}`)) {
		t.Fatal("S1 RED: a mark with an unknown body was settled")
	}
	if got := StaleOwedStatsSnapshot().Settled; got != 1 {
		t.Fatalf("S1: settled=%d, want 1", got)
	}
}
