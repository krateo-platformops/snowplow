// stale_push_1062_test.go — #1062: stale serve → the replacing frame WRITTEN,
// for every layer, on the real state machine (markDirty, the store's Put
// methods, the eviction path, the real /refreshes hub and its Wrote contract)
// with the ledger clock seamed: every delay is asserted exactly, no sleeps.
//
//	L1    an L1 stale serve (a hit on a dirty cell) is replaced by an accepted
//	      Put that began after the mark, and its frame's WRITE records the exact
//	      delay on stale_to_push_ms{layer=l1}; a frame written BEFORE the
//	      replacement closes nothing.
//	L1b   an accepted Put whose resolve began BEFORE the mark replaces nothing.
//	OVR   an L1 serve never replaced, with an armed stream, is counted
//	      unreplaced ONCE at the 10 s max and stays open.
//	NS    a serve no stream views is NOT counted unreplaced and stays open; a
//	      stream that arms later makes the next scan count it.
//	LATE  a push written past 10 s is counted unreplaced at its close (once).
//	APS   an armed hit on a stale-derived cell is an apistage serve; a
//	      stale-derived Put does not replace it, a fresh one does.
//	EVI   an eviction replaces; RSY a resync frame pushes.
//	RST   the #740 resume moves the apistage clock forward, never back.
//	CLN   a hit on a clean cell is no stale serve.
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type k1062Env struct {
	c     *ResolvedCacheStore
	r     *refresher
	key   string
	clock atomic.Int64
}

func newK1062(t *testing.T) *k1062Env {
	t.Helper()
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	c, keys := terminalFixture(t, 1)
	resetRefreshBroadcasterForTest()
	t.Cleanup(resetRefreshBroadcasterForTest)
	ResetStalePushForTest()
	t.Cleanup(ResetStalePushForTest)
	e := &k1062Env{c: c, r: refresherSingleton(), key: keys[0]}
	e.clock.Store(time.Unix(1_000_000, 0).UnixNano())
	t.Cleanup(SetStalePushClockForTest(func() time.Time { return time.Unix(0, e.clock.Load()) }))
	return e
}

func (e *k1062Env) advance(d time.Duration) { e.clock.Add(int64(d)) }

// mark dirty-marks the cell through the real mark path (a new dep event first).
func (e *k1062Env) mark() {
	depEventSeq.Add(1)
	e.r.markDirty(e.key, time.Now())
}

// hit is the dispatcher's armed hit write point on the resident cell.
func (e *k1062Env) hit(t *testing.T) {
	t.Helper()
	entry, ok := e.c.GetNoTouch(e.key)
	if !ok {
		t.Fatalf("PRECONDITION: the cell is not resident")
	}
	NoteStaleHit(e.key, entry)
}

// put is an accepted gen-guarded Put of a fresh body under rctx.
func (e *k1062Env) put(t *testing.T, rctx context.Context, staleDerived bool) {
	t.Helper()
	in := *mustInputs(t, e.c, e.key)
	if !e.c.putIfGen(rctx, e.key, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: &in, StaleDerived: staleDerived}, e.c.CaptureGen(e.key)) {
		t.Fatalf("PRECONDITION: the Put was refused")
	}
}

func k1062Layer(t *testing.T, layer string) StalePushLayerStats {
	t.Helper()
	for _, s := range StalePushStatsSnapshot() {
		if s.Layer == layer {
			return s
		}
	}
	t.Fatalf("layer %q missing from the snapshot", layer)
	return StalePushLayerStats{}
}

// written writes one frame for key on st, as the handler does after writeFrame.
func written(st RefreshStream, key string) { st.Wrote(key) }

func TestIssue1062_L1_AReplacedL1StaleServeRecordsItsDelayAtTheFrameWrite(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t)
	if s := k1062Layer(t, StaleLayerL1); s.Serves != 1 || s.Open != 1 {
		t.Fatalf("L1 RED: a hit on a dirty cell is not an l1 stale serve (serves=%d open=%d)", s.Serves, s.Open)
	}
	if s := k1062Layer(t, StaleLayerAPIStage); s.Serves != 0 {
		t.Fatalf("L1: an l1 serve counted as apistage (%d)", s.Serves)
	}
	e.advance(400 * time.Millisecond)
	written(st, e.key) // a frame of an earlier publish: the cell is still the stale one
	if s := k1062Layer(t, StaleLayerL1); s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("L1 RED: a frame written before the replacement closed the serve (open=%d samples=%d): its refetch reads the same stale cell",
			s.Open, s.ToPush.Count)
	}
	e.advance(1100 * time.Millisecond)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false) // began after the mark
	PublishRefresh(e.key)
	select {
	case k := <-st.Keys:
		written(st, k)
	case <-time.After(5 * time.Second):
		t.Fatalf("PRECONDITION: the publish reached no stream")
	}
	s := k1062Layer(t, StaleLayerL1)
	if s.ToPush.Count != 1 || s.ToPush.SumMS != 1500 || s.ToPush.MaxMS != 1500 {
		t.Fatalf("L1 RED: stale_to_push{l1} = %+v, want one sample of exactly 1500 ms (serve → the replacing frame's write)", s.ToPush)
	}
	if s.Open != 0 || s.Closes["pushed"] != 1 || s.Unreplaced != 0 {
		t.Fatalf("L1: open=%d pushed=%d unreplaced=%d, want 0/1/0", s.Open, s.Closes["pushed"], s.Unreplaced)
	}
}

func TestIssue1062_L1b_APutThatBeganBeforeTheMarkReplacesNothing(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	early := WithL1KeyContext(context.Background(), e.key) // the resolve begins …
	e.mark()                                               // … then the data moves
	e.hit(t)
	e.put(t, early, false)
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerL1); s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("L1b RED: a Put that read the pre-change state replaced the serve (open=%d samples=%d)", s.Open, s.ToPush.Count)
	}
	e.advance(time.Second)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false)
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerL1); s.Open != 0 || s.ToPush.Count != 1 || s.ToPush.MaxMS != 1000 {
		t.Fatalf("L1b: the Put that began after the mark did not close it at 1000 ms (open=%d %+v)", s.Open, s.ToPush)
	}
}

func TestIssue1062_OVR_ANeverReplacedServeWithAStreamIsCountedUnreplacedOnce(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t)
	e.advance(StaleToPushMaxMS*time.Millisecond - time.Millisecond)
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 0 {
		t.Fatalf("OVR RED: counted unreplaced before the 10 s max (%d)", s.Unreplaced)
	}
	e.advance(time.Millisecond)
	scanStalePushOverdue()
	scanStalePushOverdue()
	s := k1062Layer(t, StaleLayerL1)
	if s.Unreplaced != 1 || s.Open != 1 {
		t.Fatalf("OVR RED: unreplaced=%d open=%d at the 10 s max with an armed stream and no push, want 1 (once) and still open",
			s.Unreplaced, s.Open)
	}
	if a := k1062Layer(t, StaleLayerAPIStage); a.Unreplaced != 0 {
		t.Fatalf("OVR: counted on the wrong layer (apistage %d)", a.Unreplaced)
	}
	// Its late push still records the true delay, and is not counted again.
	e.put(t, WithL1KeyContext(context.Background(), e.key), false)
	e.advance(2 * time.Second)
	written(st, e.key)
	s = k1062Layer(t, StaleLayerL1)
	if s.ToPush.MaxMS != 12000 || s.Unreplaced != 1 || s.Open != 0 {
		t.Fatalf("OVR: late push max=%d unreplaced=%d open=%d, want 12000/1/0", s.ToPush.MaxMS, s.Unreplaced, s.Open)
	}
}

func TestIssue1062_NS_AServeNoStreamViewsIsNotUnreplacedUntilAStreamArms(t *testing.T) {
	e := newK1062(t)
	e.mark()
	e.hit(t)
	e.advance(11 * time.Second)
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 0 || s.Open != 1 {
		t.Fatalf("NS RED: a serve no stream views was counted unreplaced=%d (open=%d): nothing can push to it, so it is not a missing push",
			s.Unreplaced, s.Open)
	}
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 1 {
		t.Fatalf("NS RED: a stream armed on a serve past the 10 s max with no push, and the scan did not count it (unreplaced=%d)", s.Unreplaced)
	}
}

func TestIssue1062_LATE_APushPastTheMaxIsCountedUnreplacedAtItsClose(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false)
	e.advance(12 * time.Second)
	written(st, e.key) // no scan ran in between
	s := k1062Layer(t, StaleLayerL1)
	if s.Unreplaced != 1 || s.ToPush.MaxMS != 12000 {
		t.Fatalf("LATE RED: a 12 s push unreplaced=%d max=%d, want 1/12000", s.Unreplaced, s.ToPush.MaxMS)
	}
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 1 {
		t.Fatalf("LATE: counted twice (%d)", s.Unreplaced)
	}
}

func TestIssue1062_APS_AStaleDerivedHitIsAnApistageServeReplacedOnlyByAFreshBody(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	in := *mustInputs(t, e.c, e.key)
	e.c.put(e.key, &ResolvedEntry{RawJSON: []byte(`{"body":"stale-derived"}`), Inputs: &in, StaleDerived: true})
	e.hit(t)
	if s := k1062Layer(t, StaleLayerAPIStage); s.Serves != 1 || s.Open != 1 {
		t.Fatalf("APS RED: a hit on a stale-derived cell is not an apistage stale serve (serves=%d open=%d)", s.Serves, s.Open)
	}
	if s := k1062Layer(t, StaleLayerL1); s.Serves != 0 {
		t.Fatalf("APS: counted on l1 too (%d)", s.Serves)
	}
	e.advance(time.Second)
	e.put(t, WithL1KeyContext(context.Background(), e.key), true) // another stale-derived body
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerAPIStage); s.Open != 1 {
		t.Fatalf("APS RED: a stale-derived Put replaced the serve: its bytes are still stale")
	}
	e.advance(time.Second)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false)
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerAPIStage); s.Open != 0 || s.ToPush.Count != 1 || s.ToPush.MaxMS != 2000 {
		t.Fatalf("APS: open=%d %+v, want closed with one 2000 ms sample", s.Open, s.ToPush)
	}
}

func TestIssue1062_EVI_RSY_AnEvictionReplacesAndAResyncPushes(t *testing.T) {
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hit(t)
	e.advance(700 * time.Millisecond)
	e.c.DeleteForTest(e.key) // the refetch is a fresh fill
	st.WroteResync()         // the SPA re-reads every armed key
	if s := k1062Layer(t, StaleLayerL1); s.Open != 0 || s.ToPush.MaxMS != 700 {
		t.Fatalf("EVI/RSY RED: an evicted serve was not closed by the resync frame (open=%d %+v)", s.Open, s.ToPush)
	}
}

func TestIssue1062_RST_TheResumeMovesTheApistageClockForwardOnly(t *testing.T) {
	e := newK1062(t)
	t0 := StalePushNow()
	noteStaleServe(e.key, layerAPIStage, t0)
	RestartStaleServeClock(e.key, StaleLayerAPIStage, t0.Add(3*time.Second)) // the resume
	RestartStaleServeClock(e.key, StaleLayerAPIStage, t0.Add(time.Second))   // an older resume: never backwards
	RestartStaleServeClock(e.key, StaleLayerL1, t0.Add(5*time.Second))       // another layer: untouched
	e.advance(5 * time.Second)
	e.c.DeleteForTest(e.key)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	written(st, e.key)
	if s := k1062Layer(t, StaleLayerAPIStage); s.ToPush.MaxMS != 2000 {
		t.Fatalf("RST RED: stale_to_push = %d ms, want 2000 (measured from the resume at +3 s)", s.ToPush.MaxMS)
	}
}

func TestIssue1062_CLN_AHitOnACleanCellIsNoStaleServe(t *testing.T) {
	e := newK1062(t)
	e.hit(t)
	for _, s := range StalePushStatsSnapshot() {
		if s.Serves != 0 || s.Open != 0 {
			t.Fatalf("CLN RED: a clean hit opened a %s stale serve", s.Layer)
		}
	}
}
