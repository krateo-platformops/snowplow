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
func (e *k1062Env) hit(t *testing.T) { t.Helper(); e.hitAs(t, "") }

// hitAs is the armed hit write point for the requester subject.
func (e *k1062Env) hitAs(t *testing.T, subject string) {
	t.Helper()
	entry, ok := e.c.GetNoTouch(e.key)
	if !ok {
		t.Fatalf("PRECONDITION: the cell is not resident")
	}
	NoteStaleHit(context.Background(), e.key, entry, func(context.Context) string { return subject })
}

func noFrameWithin(t *testing.T, st RefreshStream, d time.Duration) bool {
	t.Helper()
	select {
	case <-st.Keys:
		return false
	case <-time.After(d):
		return true
	}
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	t0 := StalePushNow()
	noteStaleServeNow(e.key, "", layerAPIStage, t0)
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
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	e.hit(t)
	for _, s := range StalePushStatsSnapshot() {
		if s.Serves != 0 || s.Open != 0 {
			t.Fatalf("CLN RED: a clean hit opened a %s stale serve", s.Layer)
		}
	}
}

// RX1 (reviewer arm, adopted) — #1081 made visible: an L1 serve whose cell is
// replaced and published BEFORE the tab's stream arms is offered no frame on
// the stream that arms afterwards; the overdue scan counts it once at 10 s.
func TestIssue1062_RX1_AServeReplacedBeforeItsStreamArmsIsCountedUnreplaced(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	const subj = "user:rx1"
	e.mark()
	e.hitAs(t, subj) // the tab's /call: a stale L1 hit, armed header
	e.advance(100 * time.Millisecond)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false) // the refresh lands
	PublishRefresh(e.key)                                          // nobody armed yet
	e.advance(200 * time.Millisecond)                              // the SPA's arm debounce
	st := SubscribeRefreshStreamAs(subj, map[string]struct{}{e.key: {}})
	defer st.Unsub()
	if !noFrameWithin(t, st, 500*time.Millisecond) {
		t.Fatalf("RX1: a frame was offered on arm — #1081's no-replay premise no longer holds; revisit this arm")
	}
	e.advance(10 * time.Second)
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 1 || s.Open != 1 || s.ToPush.Count != 0 {
		t.Fatalf("RX1 RED: unreplaced=%d open=%d samples=%d, want 1/1/0 — the strand must be visible", s.Unreplaced, s.Open, s.ToPush.Count)
	}
}

// RX2 (reviewer arm, adopted for per-client keying) — two users are served the
// same stale cell under ONE key (an RBAC class, or the identity-free
// widgetContent shell). Only A's stream exists when the replacement's frame is
// written; B's stream arms afterwards and is offered nothing. A's frame must
// close A's serve ONLY: one push (A), one stranded serve (B), counted
// unreplaced at 10 s.
// Mutation → RED: a frame closes every subject's serves of the key.
func TestIssue1062_RX2_AnotherClientsFrameNeverClosesAStrandedServe(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	const a, b = "user:rx2-a", "user:rx2-b"
	stA := SubscribeRefreshStreamAs(a, map[string]struct{}{e.key: {}})
	defer stA.Unsub()
	e.mark()
	e.hitAs(t, a)
	e.hitAs(t, b) // the same key: one RBAC class, or a shared shell
	e.advance(time.Second)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false)
	PublishRefresh(e.key)
	select {
	case k := <-stA.Keys:
		stA.Wrote(k)
	case <-time.After(5 * time.Second):
		t.Fatalf("PRECONDITION: A's stream got no frame")
	}
	stB := SubscribeRefreshStreamAs(b, map[string]struct{}{e.key: {}})
	defer stB.Unsub()
	if !noFrameWithin(t, stB, 500*time.Millisecond) {
		t.Fatalf("PRECONDITION: B's late stream was offered a frame")
	}
	e.advance(10 * time.Second)
	scanStalePushOverdue()
	s := k1062Layer(t, StaleLayerL1)
	if s.ToPush.Count != 1 || s.ToPush.MaxMS != 1000 || s.Unreplaced != 1 || s.Open != 1 || s.Closes["pushed"] != 1 {
		t.Fatalf("RX2 RED: samples=%d max=%d unreplaced=%d open=%d pushed=%d, want 1/1000/1/1/1 — A's frame closed B's serve",
			s.ToPush.Count, s.ToPush.MaxMS, s.Unreplaced, s.Open, s.Closes["pushed"])
	}
}

// LAZY — the requester digest is derived only when a stale serve OPENS: a
// fresh armed hit (nearly every hit) never calls subjectOf; a stale one calls
// it exactly once.
// Mutation → RED: NoteStaleHit derives the subject before the stale check.
func TestIssue1062_LAZY_TheRequesterDigestIsDerivedOnlyForAStaleServe(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	calls := 0
	subjectOf := func(context.Context) string { calls++; return "user:lazy" }
	entry, _ := e.c.GetNoTouch(e.key)
	NoteStaleHit(context.Background(), e.key, entry, subjectOf) // clean cell
	if calls != 0 {
		t.Fatalf("LAZY RED: a fresh hit derived the requester digest %d time(s), want 0", calls)
	}
	e.mark()
	NoteStaleHit(context.Background(), e.key, entry, subjectOf) // dirty cell
	if calls != 1 || k1062Layer(t, StaleLayerL1).Open != 1 {
		t.Fatalf("LAZY: a stale hit derived the digest %d time(s) (want 1), open=%d", calls, k1062Layer(t, StaleLayerL1).Open)
	}
}

// RV-ALL (reviewer arm, adopted) — replacement is a property of the CELL: four
// users are served stale on one key 100 ms apart, then ONE accepted Put. Each
// of three users' own frames closes exactly that user's serve (3 samples:
// 1300 + 1200 + 1100 ms); the fourth user's late frame closes the fourth serve
// too — the one Put replaced it as well.
// Mutation → RED: a Put replaces only one subject's serves.
func TestIssue1062_RVALL_OnePutReplacesEveryUsersServe(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	subs := []string{"user:all-a", "user:all-b", "user:all-c", "user:all-d"}
	var streams []RefreshStream
	for _, s := range subs[:3] {
		st := SubscribeRefreshStreamAs(s, map[string]struct{}{e.key: {}})
		defer st.Unsub()
		streams = append(streams, st)
	}
	e.mark()
	for i, s := range subs {
		e.hitAs(t, s)
		if i < 3 {
			e.advance(100 * time.Millisecond)
		}
	}
	e.advance(time.Second)
	e.put(t, WithL1KeyContext(context.Background(), e.key), false) // ONE Put
	PublishRefresh(e.key)
	for i, st := range streams {
		select {
		case k := <-st.Keys:
			st.Wrote(k)
		case <-time.After(5 * time.Second):
			t.Fatalf("PRECONDITION: user %d's stream got no frame", i)
		}
	}
	s := k1062Layer(t, StaleLayerL1)
	if s.ToPush.Count != 3 || s.ToPush.SumMS != 3600 || s.ToPush.MaxMS != 1300 || s.Open != 1 {
		t.Fatalf("RV-ALL RED: samples=%d sum=%d max=%d open=%d, want 3/3600/1300/1 — each user's frame closes exactly its own serve",
			s.ToPush.Count, s.ToPush.SumMS, s.ToPush.MaxMS, s.Open)
	}
	late := SubscribeRefreshStreamAs(subs[3], map[string]struct{}{e.key: {}})
	defer late.Unsub()
	late.Wrote(e.key)
	if s := k1062Layer(t, StaleLayerL1); s.ToPush.Count != 4 || s.Open != 0 {
		t.Fatalf("RV-ALL RED: the fourth user's late frame left samples=%d open=%d, want 4/0 — the one Put did not replace that user's serve",
			s.ToPush.Count, s.Open)
	}
}

// NSX — "no stream" means no stream OF THE SERVE'S CLIENT: B is served stale on
// a key only A's stream is armed for. Past 10 s, B's serve is not counted
// unreplaced (nothing can push to B); once B's own stream arms, it is.
// Mutation → RED: the overdue scan asks for any stream on the key.
func TestIssue1062_NSX_AnotherClientsStreamDoesNotMakeAServeViewed(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	const a, b = "user:nsx-a", "user:nsx-b"
	stA := SubscribeRefreshStreamAs(a, map[string]struct{}{e.key: {}})
	defer stA.Unsub()
	e.mark()
	e.hitAs(t, b)
	e.advance(11 * time.Second)
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 0 || s.Open != 1 {
		t.Fatalf("NSX RED: unreplaced=%d open=%d — A's stream made B's serve count as viewed", s.Unreplaced, s.Open)
	}
	stB := SubscribeRefreshStreamAs(b, map[string]struct{}{e.key: {}})
	defer stB.Unsub()
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 1 {
		t.Fatalf("NSX: unreplaced=%d once B's own stream armed, want 1", s.Unreplaced)
	}
}

// N2 — the overdue count is once PER SERVE: two serves of one client on one
// key, 5 s apart, an armed stream, no push. At 10 s one is counted; at 15 s the
// second, never the first again.
// Mutation → RED: the second-pass once-guard is dropped.
func TestIssue1062_N2_OverdueIsCountedOncePerServe(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	const subj = "user:n2"
	st := SubscribeRefreshStreamAs(subj, map[string]struct{}{e.key: {}})
	defer st.Unsub()
	e.mark()
	e.hitAs(t, subj)
	e.advance(5 * time.Second)
	e.hitAs(t, subj) // the tab refetched, still stale
	e.advance(5 * time.Second)
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 1 {
		t.Fatalf("N2: unreplaced=%d at 10 s, want 1 (the first serve only)", s.Unreplaced)
	}
	e.advance(5 * time.Second)
	scanStalePushOverdue()
	scanStalePushOverdue()
	if s := k1062Layer(t, StaleLayerL1); s.Unreplaced != 2 || s.Open != 2 {
		t.Fatalf("N2 RED: unreplaced=%d open=%d at 15 s, want 2/2 — each serve once, the first never again", s.Unreplaced, s.Open)
	}
}

// N3 — the memory bound is real: with the L1 store's entry budget at 2, a third
// open serve drops the OLDEST (close=dropped), and a later push reaches only
// the two kept.
// Mutation → RED: the capacity loop never drops.
func TestIssue1062_N3_TheCapacityBoundDropsTheOldestServe(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	t.Setenv(envResolvedCacheMaxEntries, "2")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	e := newK1062(t)
	if got := stalePushCapacity(); got != 2 {
		t.Fatalf("PRECONDITION: the ledger's capacity is %d, want the store's budget 2", got)
	}
	subjects := []string{"user:n3-0", "user:n3-1", "user:n3-2"}
	for _, s := range subjects {
		noteStaleServeNow(e.key, s, layerAPIStage, StalePushNow())
		e.advance(time.Second)
	}
	s := k1062Layer(t, StaleLayerAPIStage)
	if s.Open != 2 || s.Closes["dropped"] != 1 || s.Serves != 3 {
		t.Fatalf("N3 RED: open=%d dropped=%d serves=%d, want 2/1/3 — the bound must drop", s.Open, s.Closes["dropped"], s.Serves)
	}
	e.c.DeleteForTest(e.key)
	for _, subj := range subjects {
		st := SubscribeRefreshStreamAs(subj, map[string]struct{}{e.key: {}})
		st.Wrote(e.key)
		st.Unsub()
	}
	if s := k1062Layer(t, StaleLayerAPIStage); s.Closes["pushed"] != 2 || s.ToPush.MaxMS != 2000 || s.Open != 0 {
		t.Fatalf("N3: pushed=%d max=%d open=%d, want 2/2000/0 — the OLDEST serve was the one dropped", s.Closes["pushed"], s.ToPush.MaxMS, s.Open)
	}
}
