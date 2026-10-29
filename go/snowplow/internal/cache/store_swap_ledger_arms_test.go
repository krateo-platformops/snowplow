package cache

// store_swap_ledger_arms_test.go — #870 owner ruling 3 / N5 / v5.1 C2, C5:
// known-stale accounting and the push ledger, end to end on the real store,
// the real refresher, the real broadcaster and a simulated /refreshes
// handler (which calls Wrote / WroteResync exactly where the real handler
// does: after its writeFrame returned true).

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ledgerRig struct {
	*swapFix
	store *ResolvedCacheStore

	mu      sync.Mutex
	decline map[string]bool          // refresh returns without a Put (a decline)
	gate    map[string]chan struct{} // refresh of key waits here
	after   map[string]chan struct{} // refresh of key waits here AFTER its publish
	refresh atomic.Int64

	// clk is the dirty-window clock (#930): every instant the swap, the
	// ledger and the refresher's windows read. The arms advance it at the
	// points they state and assert EXACT window lengths, never a wall bound.
	clk *c930Clock
}

func newLedgerRig(t *testing.T, n int) *ledgerRig {
	t.Helper()
	gvr := swapGVR(t)
	b5RefresherEnv(t)
	f := newSwapFix(t, gvr, objs(gvr, n))
	// Cleanups run LIFO: the refresher stops (and joins its workers) BEFORE
	// the store and the broadcaster it reads are torn down.
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	ResetRefreshBroadcasterForTest()
	t.Cleanup(ResetRefreshBroadcasterForTest)
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)
	r := &ledgerRig{swapFix: f, decline: map[string]bool{}, gate: map[string]chan struct{}{}, after: map[string]chan struct{}{},
		clk: newC930Clock()}
	t.Cleanup(SetDirtyWindowClockForTest(r.clk.now))
	r.store = ResolvedCache()
	if r.store == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(r.store)
	RegisterRefreshFunc("widgets", func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		r.mu.Lock()
		g, decl := r.gate[key], r.decline[key]
		r.mu.Unlock()
		if g != nil {
			<-g
		}
		r.refresh.Add(1)
		if decl {
			return nil // no Put: the dequeue ends known-stale (declined)
		}
		// As resolveAndPopulateL1 does: the resolve's own dep-gen sink (the
		// L1-key context), its generation captured before the read, its dep
		// recorded, then the gen-guarded replace.
		rctx := WithL1KeyContext(ctx, key)
		gen0 := r.store.CaptureGen(key)
		obj, _ := f.storeObj(in.Namespace + "/" + in.Name)
		Deps().Record(rctx, key, f.gvr, in.Namespace, in.Name)
		e := &ResolvedEntry{RawJSON: cellBody(obj), Inputs: &in}
		stored := r.store.replaceIfGenRefresh(rctx, key, e, gen0)
		NoteRefreshWrite(rctx, key, stored)
		if stored {
			PublishRefresh(key) // what resolveAndPopulateL1 does after an accepted re-Put
		}
		r.mu.Lock()
		a := r.after[key]
		r.mu.Unlock()
		if a != nil {
			<-a // held between the publish and settleDequeue
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	// Runs FIRST at cleanup (LIFO): stop the informers and join the dep worker,
	// so nothing marks into the refresher or the store while they are reset.
	t.Cleanup(func() {
		f.rw.Stop()
		resetDepWatchForTest()
	})
	return r
}

func cellBody(obj any) []byte {
	b, _ := json.Marshal(map[string]string{"v": specV(obj)})
	return b
}

// cell puts an L1 cell of class (default widgets) depending on object name,
// with the body the live copy serves now. Returns its key.
func (r *ledgerRig) cell(name, class string) string {
	r.t.Helper()
	if class == "" {
		class = "widgets"
	}
	in := &ResolvedKeyInputs{CacheEntryClass: class, Group: r.gvr.Group, Version: r.gvr.Version,
		Resource: r.gvr.Resource, Namespace: swapNS, Name: name}
	key := ComputeKey(*in)
	obj, _ := r.storeObj(swapKey(name))
	r.store.put(key, &ResolvedEntry{RawJSON: cellBody(obj), Inputs: in})
	Deps().Record(context.Background(), key, r.gvr, swapNS, name)
	return key
}

func (r *ledgerRig) body(key string) string {
	e, ok := r.store.GetNoTouch(key)
	if !ok {
		return "<evicted>"
	}
	return string(e.RawJSON)
}

func (r *ledgerRig) holdRefresh(key string) func() {
	ch := make(chan struct{})
	r.mu.Lock()
	r.gate[key] = ch
	r.mu.Unlock()
	var once sync.Once
	rel := func() { once.Do(func() { close(ch) }) }
	r.t.Cleanup(rel)
	return rel
}

// viewer is a simulated /refreshes stream: it reads the sink and, like the
// handler, calls Wrote after each frame it "writes".
type viewer struct {
	st     RefreshStream
	mu     sync.Mutex
	frames map[string]int
	at     map[string]time.Time
	done   chan struct{}
}

// quiesce waits until nothing more can happen without a new event: the dep
// worker's queue is drained and the refresher has no work outstanding (#930's
// event-driven wait, never a sleep).
func (r *ledgerRig) quiesce() {
	r.t.Helper()
	r.waitFor("the dep worker to drain", func() bool { return DepWatchStatsSnapshot().DeleteQueueDepth == 0 })
	if _, err := WaitRefresherQuiescentForTest(RefresherWorkSnapshot(), 30*time.Second); err != nil {
		r.t.Fatalf("refresher never quiescent: %v (work %+v, refreshes run %d)", err, RefresherWorkSnapshot(), r.refresh.Load())
	}
}

func (r *ledgerRig) view(keys ...string) *viewer {
	set := map[string]struct{}{}
	for _, k := range keys {
		set[k] = struct{}{}
	}
	v := &viewer{st: SubscribeRefreshStream(set), frames: map[string]int{}, at: map[string]time.Time{}, done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-v.done:
				return
			case k, ok := <-v.st.Keys:
				if !ok {
					return
				}
				v.mu.Lock()
				v.frames[k]++
				v.at[k] = windowNow()
				v.mu.Unlock()
				v.st.Wrote(k)
			}
		}
	}()
	r.t.Cleanup(v.close)
	return v
}

func (v *viewer) close() {
	select {
	case <-v.done:
	default:
		close(v.done)
		v.st.Unsub()
	}
}

func (v *viewer) count(k string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.frames[k]
}

func (v *viewer) when(k string) time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.at[k]
}

func gauges() (keysOpen, winMax, owed map[string]float64) {
	ko, wm, ow := storeSwapGauges()
	keysOpen, winMax, owed = map[string]float64{}, wm, map[string]float64{}
	for r, v := range ko {
		keysOpen[r] = float64(v)
	}
	for r, v := range ow {
		owed[r] = float64(v)
	}
	return
}

func closes(reason, c string) uint64 { return storeSwapStats.closes.get(reason, c) }

// windowEnded: no open build window of reason remains. The ledger rig runs on
// an injected window clock that moves only when an arm steps it, so
// window_open_seconds_max reads 0 for a window that is still open whenever the
// arm has not stepped the clock; it can never stand in for "the window ended".
func windowEnded(reason string) bool {
	l := swapLedger
	l.mu.Lock()
	defer l.mu.Unlock()
	for ra := range l.open {
		if ra.reason == reason {
			return false
		}
	}
	return true
}

// ── K16 — ruling 3, reason divergence ────────────────────────────────────────

// TestStoreSwap_K16_KnownStaleDivergence: (1) during the build a single-object
// dependent of a confirmed key keeps serving the stale body while a churned
// dependent stays fresh; (2) keys_open{divergence} = the confirmed count and
// the open window's age is read off the window clock; (3) after the window
// closes keys_total{divergence,swap} grew by exactly that count and the window
// total is EXACTLY detection → the frame's write; (4) the armed subscriber gets
// NO frame before the install and EXACTLY ONE after it, with the corrected
// object. All instants are the dirty-window clock's. RED under X1, X23, X24 and
// X32.
func TestStoreSwap_K16_KnownStaleDivergence(t *testing.T) {
	r := newLedgerRig(t, 4)
	stale := r.cell("o00", "")
	churned := r.cell("o01", "")
	v := r.view(stale)
	lists0 := r.listCount()
	release := r.holdList(lists0 + 1)

	r.clusterSet(swapObj(r.gvr, "o00", "2", "corrected"))
	t0 := r.clk.now() // detection
	r.request(reasonDivergence, swapKey("o00"))

	// (1) the live copy keeps delivering: the churned dependent refreshes.
	o := swapObj(r.gvr, "o01", "3", "churned")
	r.clusterSet(o)
	r.watcher(0).Modify(o)
	r.waitFor("the churned dependent to refresh", func() bool { return r.body(churned) == `{"v":"churned"}` })
	if got := r.body(stale); got != `{"v":"v1"}` {
		t.Fatalf("K16 (1): the known-stale dependent changed during the build: %s", got)
	}
	// (2) and (X24): 2 s into the build the window is open, counted and 2 s old.
	r.clk.advance(2 * time.Second)
	ko, wm, _ := gauges()
	if ko[reasonDivergence] != 1 || wm[reasonDivergence] != 2 {
		t.Fatalf("K16 (2) RED (X24): keys_open=%v window_open_max=%v 2 s into the build, want 1 and 2", ko, wm)
	}
	if v.count(stale) != 0 {
		t.Fatal("K16 (4): a frame for the stale dependent before the install")
	}

	relRefresh := r.holdRefresh(stale) // the dependent's refresh waits for the arm
	release()
	r.awaitSwaps(1) // installed at t0 + 2 s
	r.clk.advance(3 * time.Second)
	relRefresh() // refreshed and written at t0 + 5 s
	r.waitFor("the corrected frame", func() bool { return v.count(stale) >= 1 })
	if got := r.body(stale); got != `{"v":"corrected"}` {
		t.Fatalf("K16 (4) RED (X1): the frame announced %s, want the corrected object", got)
	}
	r.waitFor("the window to close", func() bool {
		ko, wm, _ := gauges()
		return windowEnded(reasonDivergence) && ko[reasonDivergence] == 0 && wm[reasonDivergence] == 0
	})
	r.quiesce()
	if n := v.count(stale); n != 1 {
		t.Fatalf("K16 (4) RED (X32): %d frames after the install, want exactly 1", n)
	}
	if got := v.when(stale).Sub(t0); got != 5*time.Second {
		t.Fatalf("K16: the frame was written at t0+%v, want t0+5s", got)
	}
	// (3)
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != 1 {
		t.Fatalf("K16 (3) RED (X23): keys_total{divergence,swap} = %d, want 1", got)
	}
	if got := storeSwapStats.builds.get(reasonDivergence, outcomeSwap); got != 1 {
		t.Fatalf("K16 (3) RED (X23): builds_total{divergence,swap} = %d, want 1", got)
	}
	if win := storeSwapWindowSeconds(reasonDivergence, outcomeSwap); win != 5 {
		t.Fatalf("K16 (3) RED (X24): window_seconds_total{divergence,swap} = %vs, want exactly 5 (detection → the frame written)", win)
	}
	if closes(reasonDivergence, closePushed) != 1 {
		t.Fatalf("K16: closes{pushed} = %d, want 1", closes(reasonDivergence, closePushed))
	}
	d := storeSwapStats.detectHist[[2]string{reasonDivergence, closePushed}]
	i := storeSwapStats.installHist[[2]string{reasonDivergence, closePushed}]
	if d.count.Load() != 1 || d.maxMS.Load() != 5000 || i.count.Load() != 1 || i.maxMS.Load() != 3000 {
		t.Fatalf("K16: detect_to_push=%d obs/%dms, install_to_push=%d obs/%dms; want 1/5000 and 1/3000",
			d.count.Load(), d.maxMS.Load(), i.count.Load(), i.maxMS.Load())
	}
}

// TestStoreSwap_K16_LiveEventClosesACoordinateNotInM — architect Q3 hole: a
// confirmed key that receives a REAL update before the swap is not in M (the
// consumer already acted on the new copy's object): its dependents' entries are
// recorded closed as live_event and hold nothing; the window ends.
func TestStoreSwap_K16_LiveEventClosesACoordinateNotInM(t *testing.T) {
	r := newLedgerRig(t, 3)
	dep := r.cell("o02", "")
	r.view(dep)
	lists0 := r.listCount()
	release := r.holdList(lists0 + 1)
	r.clusterSet(swapObj(r.gvr, "o02", "2", "x"))
	r.request(reasonDivergence, swapKey("o02"))
	// The real update arrives through the live copy BEFORE the swap.
	o := swapObj(r.gvr, "o02", "2", "x")
	r.watcher(0).Modify(o)
	r.waitFor("the live event's refresh", func() bool { return r.body(dep) == `{"v":"x"}` })
	release()
	r.awaitSwaps(1)
	r.waitFor("the window to close", func() bool { return windowEnded(reasonDivergence) })
	if got := closes(reasonDivergence, closeLiveEvent); got != 1 {
		t.Fatalf("K16 (Q3): closes{live_event} = %d, want 1", got)
	}
	if e, _ := openLedgerForTest(); e != 0 {
		t.Fatalf("K16 (Q3): %d ledger entries left open", e)
	}
}

// ── K16b — reason schema ─────────────────────────────────────────────────────

// TestStoreSwap_K16bSchemaWindow: a widen build serves the changed objects
// pruned during it; keys_total{schema,swap} = |M|; the window is EXACTLY the
// CRD event → the push; the armed single-object dependent gets exactly one
// frame after the install. RED under X30.
func TestStoreSwap_K16bSchemaWindow(t *testing.T) {
	r := newLedgerRig(t, 3)
	dep := r.cell("o01", "")
	v := r.view(dep)
	lists0 := r.listCount()
	release := r.holdList(lists0 + 1)
	widened := swapObj(r.gvr, "o01", "1", "widened") // same resourceVersion: a widen
	r.clusterSet(widened)
	r.request(reasonSchema) // the CRD event, at t0
	r.waitFor("the new copy's LIST", func() bool { return r.listCount() == lists0+1 })
	if got := r.body(dep); got != `{"v":"v1"}` {
		t.Fatalf("K16b: the dependent changed during the build: %s", got)
	}
	r.clk.advance(4 * time.Second)
	release()
	r.awaitSwaps(1)
	r.waitFor("the frame", func() bool { return v.count(dep) == 1 })
	r.waitFor("the window to close", func() bool { return windowEnded(reasonSchema) })
	r.quiesce()
	if n := v.count(dep); n != 1 {
		t.Fatalf("K16b: %d frames after the install, want exactly 1", n)
	}
	if got := storeSwapStats.keys.get(reasonSchema, outcomeSwap); got != 1 {
		t.Fatalf("K16b RED (X30): keys_total{schema,swap} = %d, want |M| = 1", got)
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != 0 {
		t.Fatalf("K16b RED (X30): a schema build was counted under reason divergence (%d)", got)
	}
	if win := storeSwapWindowSeconds(reasonSchema, outcomeSwap); win != 4 {
		t.Fatalf("K16b: window_seconds_total{schema,swap} = %vs, want exactly 4 (the CRD event → the push)", win)
	}
	if got := r.body(dep); got != `{"v":"widened"}` {
		t.Fatalf("K16b: after the swap the dependent serves %s", got)
	}
}

// ── K16c — install to push ───────────────────────────────────────────────────

// TestStoreSwap_K16c_InstallToPush: a push that lands after the install is
// recorded per entry on install_to_push_ms, and the window total is
// (install − detection) + install-to-push, exactly. RED under X31 (the window
// ends at the install).
func TestStoreSwap_K16c_InstallToPush(t *testing.T) {
	r := newLedgerRig(t, 3)
	dep := r.cell("o00", "")
	v := r.view(dep)
	lists0 := r.listCount()
	releaseList := r.holdList(lists0 + 1)
	rel := r.holdRefresh(dep) // the refresher is busy (the yield stand-in)
	r.clusterSet(swapObj(r.gvr, "o00", "2", "c"))
	r.request(reasonDivergence, swapKey("o00")) // t0
	r.clk.advance(1 * time.Second)
	releaseList()
	r.awaitSwaps(1) // installed at t0 + 1 s
	r.clk.advance(4 * time.Second)
	ko, wm, _ := gauges()
	if ko[reasonDivergence] != 1 || wm[reasonDivergence] != 5 {
		t.Fatalf("K16c RED (X31): keys_open=%v window_open_max=%v with the push still pending, want 1 and 5", ko, wm)
	}
	rel() // pushed at t0 + 5 s
	r.waitFor("the frame", func() bool { return v.count(dep) == 1 })
	r.waitFor("the window to close", func() bool { return windowEnded(reasonDivergence) })
	h := storeSwapStats.installHist[[2]string{reasonDivergence, closePushed}]
	if h.count.Load() != 1 || h.maxMS.Load() != 4000 {
		t.Fatalf("K16c: install_to_push_ms{divergence,pushed} count=%d max=%dms, want 1 and 4000", h.count.Load(), h.maxMS.Load())
	}
	if win := storeSwapWindowSeconds(reasonDivergence, outcomeSwap); win != 5 {
		t.Fatalf("K16c RED (X31): window_seconds_total = %vs, want exactly 5 = (install − detection) 1 + install_to_push 4", win)
	}
}

// ── K16d — every close ───────────────────────────────────────────────────────

// TestStoreSwap_K16d_LedgerCloses: each close with its label. RED under X35
// (close only on fan-out / stored=false: the declined and suppressed entries
// stay open) and X36 (close on unsubscribe: the re-armed viewer gets no frame).
func TestStoreSwap_K16d_LedgerCloses(t *testing.T) {
	r := newLedgerRig(t, 6)
	declined := r.cell("o00", "")
	suppressed := r.cell("o01", "")
	evicted := r.cell("o02", "")
	rearm := r.cell("o03", "")
	owedKey := r.cell("o04", "")
	r.mu.Lock()
	r.decline[declined] = true
	r.mu.Unlock()
	NoteRefreshDecline(suppressed, "uaf", true) // a structurally permanent decline
	relRearm := r.holdRefresh(rearm)
	relEvict := r.holdRefresh(evicted)
	first := r.view(rearm, declined, suppressed, evicted)
	for _, n := range []string{"o00", "o01", "o02", "o03", "o04"} {
		r.clusterSet(swapObj(r.gvr, n, "2", "new"))
	}
	r.request(reasonDivergence, swapKey("o00"), swapKey("o01"), swapKey("o02"), swapKey("o03"), swapKey("o04"))
	r.awaitSwaps(1)
	// The viewer of `rearm` disconnects before its refresh: nothing closes.
	first.close()
	r.store.DeleteForTest(evicted) // the key leaves the store
	relEvict()
	relRearm()
	r.waitFor("the declined / suppressed / evicted closes", func() bool {
		return closes(reasonDivergence, closeKnownStale) == 2 && closes(reasonDivergence, closeEvicted) == 1
	})
	r.waitFor("rearm refreshed (owed)", func() bool { return r.body(rearm) == `{"v":"new"}` })
	if closes(reasonDivergence, closePushed)+closes(reasonDivergence, closePushedOnArm) != 0 {
		t.Fatal("K16d RED (X36): an entry closed as pushed although its viewer had disconnected")
	}
	// It re-arms after the refresh: a frame on arm closes pushed_on_arm.
	second := r.view(rearm)
	// The arm emits at once; the broadcaster's trailing coalesce window may
	// emit once more (a spurious emit costs one refetch, #484).
	r.waitFor("the frame on arm", func() bool { return second.count(rearm) >= 1 })
	r.waitFor("pushed_on_arm", func() bool { return closes(reasonDivergence, closePushedOnArm) == 1 })
	// owedKey never had a subscriber: refreshed and owed, holding nothing.
	r.waitFor("owedKey refreshed", func() bool { return r.body(owedKey) == `{"v":"new"}` })
	r.waitFor("the window to close", func() bool { return windowEnded(reasonDivergence) })
	if _, _, ow := gauges(); ow[reasonDivergence] != 1 {
		t.Fatalf("K16d: ledger_owed = %v, want 1 (owedKey)", ow)
	}
}

// ── K16e — an offer is not a write ───────────────────────────────────────────

// TestStoreSwap_K16e_OfferIsNotAWrite: (1) a backlogged subscriber has k
// deferred and disconnects before the drain: the entry stays open (owed), never
// pushed; (2) a written refresh frame closes pushed; (3) a written resync frame
// closes the stream's REFRESHED entries as pushed_resync and leaves a stale one
// open; (4) a resync whose write fails closes nothing (the handler calls
// WroteResync only on success — TestRefreshes_ResyncWriteFailureClosesNothing
// pins the handler side). RED under X43 and X44.
func TestStoreSwap_K16e_OfferIsNotAWrite(t *testing.T) {
	t.Run("deferred_then_disconnected", func(t *testing.T) {
		r := newLedgerRig(t, 2)
		dep := r.cell("o00", "")
		// A subscriber that never reads: its sink fills with filler keys, so
		// the refresh of dep is DEFERRED into its backlog.
		set := map[string]struct{}{dep: {}}
		var fillers []string
		for i := 0; i < refreshSubChanCap+4; i++ {
			k := "filler-" + itoaForTest(i)
			fillers = append(fillers, k)
			set[k] = struct{}{}
		}
		st := SubscribeRefreshStream(set)
		for _, k := range fillers {
			PublishRefresh(k)
		}
		r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
		r.request(reasonDivergence, swapKey("o00"))
		r.awaitSwaps(1)
		r.waitFor("dep refreshed", func() bool { return r.body(dep) == `{"v":"x"}` })
		time.Sleep(50 * time.Millisecond)
		st.Unsub() // disconnects before the drain: the backlog is discarded
		time.Sleep(50 * time.Millisecond)
		if closes(reasonDivergence, closePushed) != 0 {
			t.Fatal("K16e (1) RED (X43): a deferred offer closed the entry as pushed")
		}
		if e, _ := openLedgerForTest(); e != 1 {
			t.Fatalf("K16e (1): %d entries open, want 1 (owed)", e)
		}
		if _, _, ow := gauges(); ow[reasonDivergence] != 1 {
			t.Fatalf("K16e (1): ledger_owed = %v, want 1", ow)
		}
	})
	t.Run("written_frame_closes_pushed", func(t *testing.T) {
		r := newLedgerRig(t, 2)
		dep := r.cell("o00", "")
		v := r.view(dep)
		r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
		r.request(reasonDivergence, swapKey("o00"))
		r.awaitSwaps(1)
		r.waitFor("pushed", func() bool { return closes(reasonDivergence, closePushed) == 1 })
		if v.count(dep) != 1 {
			t.Fatalf("K16e (2): %d frames", v.count(dep))
		}
	})
	t.Run("resync_closes_refreshed_only", func(t *testing.T) {
		r := newLedgerRig(t, 3)
		refreshed := r.cell("o00", "")
		stale := r.cell("o01", "")
		rel := r.holdRefresh(stale)
		set := map[string]struct{}{refreshed: {}, stale: {}}
		st := SubscribeRefreshStream(set) // never read: no refresh frame is written
		r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
		r.clusterSet(swapObj(r.gvr, "o01", "2", "y"))
		r.request(reasonDivergence, swapKey("o00"), swapKey("o01"))
		r.awaitSwaps(1)
		r.waitFor("refreshed", func() bool { return r.body(refreshed) == `{"v":"x"}` })
		st.WroteResync() // the handler wrote a resync frame
		if got := closes(reasonDivergence, closePushedResync); got != 1 {
			t.Fatalf("K16e (3) RED (X44): closes{pushed_resync} = %d, want 1 (the refreshed entry only)", got)
		}
		if e, _ := openLedgerForTest(); e != 1 {
			t.Fatalf("K16e (3) RED (X44): %d entries open, want 1 (the stale one)", e)
		}
		rel()
		st.Unsub()
	})
}

// ── K16f — who holds the window ──────────────────────────────────────────────

// TestStoreSwap_K16f_WindowHolders: an unviewed dependent whose refresh is
// deferred does not hold window_open_seconds_max; a non-armable content cell
// opens no entry; after the build ledger_owed holds only armable keys. RED
// under X45 and X46.
func TestStoreSwap_K16f_WindowHolders(t *testing.T) {
	r := newLedgerRig(t, 3)
	unviewed := r.cell("o00", "")
	content := r.cell("o00", CacheEntryClassApistage)
	rel := r.holdRefresh(unviewed)
	r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
	r.request(reasonDivergence, swapKey("o00"))
	r.awaitSwaps(1)
	// Only the armable dependent has an entry, and it never had a subscriber,
	// so nothing holds the window: it ends at the install although the
	// dependent's refresh is still held (C5a). X45 makes it hold.
	r.waitFor("the window to end with the refresh still pending", func() bool { return windowEnded(reasonDivergence) })
	if e, _ := openLedgerForTest(); e != 1 {
		t.Fatalf("K16f RED (X46): %d entries open, want 1 (the armable dependent only; the %s cell opens none)", e, content)
	}
	rel()
	r.waitFor("refreshed", func() bool { return r.body(unviewed) == `{"v":"x"}` })
	if _, _, ow := gauges(); ow[reasonDivergence] != 1 {
		t.Fatalf("K16f: ledger_owed = %v, want 1 (the armable key)", ow)
	}
}

// ── K16g — the publish can be written before the dequeue settles ─────────────

// TestStoreSwap_K16g_FrameWrittenBeforeTheDequeueSettles (design v5.3): the
// refresh goroutine is HELD between PublishRefresh and settleDequeue while the
// viewer's writeFrame completes. The entry must close as pushed, and
// window_open_seconds_max must return to 0, while the hold is still in place.
// RED under X52 (become refreshed only at windowFresh).
func TestStoreSwap_K16g_FrameWrittenBeforeTheDequeueSettles(t *testing.T) {
	r := newLedgerRig(t, 2)
	dep := r.cell("o00", "")
	v := r.view(dep)
	hold := make(chan struct{})
	r.mu.Lock()
	r.after[dep] = hold
	r.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-hold:
		default:
			close(hold)
		}
	})
	r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
	r.request(reasonDivergence, swapKey("o00"))
	r.awaitSwaps(1)
	r.waitFor("the frame", func() bool { return v.count(dep) == 1 })
	r.waitFor("pushed while the dequeue is still held", func() bool {
		return closes(reasonDivergence, closePushed) == 1 && windowEnded(reasonDivergence)
	})
	close(hold)
}
