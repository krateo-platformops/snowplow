// apistage_stale_debt_710_test.go — #710 PR-2b: the D2 stale serve, its debt
// and its push, through the production reader apistageContentServe on a real
// informer, a real known-stale content cell (#547 drive) and the real
// /refreshes broadcaster. The dependent customer cell's re-resolve is a
// registered "restactions" refresh handler standing in for
// resolveAndPopulateL1: it re-Puts the cell and publishes, as the real one does
// on an accepted Put.
//
//   - D2-1  a known-stale armable hit is served stale and its debt fills.
//   - D2-2  fill → push: the armed dependent receives event: refresh.
//   - D2-3  the push survives a dependent re-resolve that does not Put.
//   - D2-4  (inverted) with headroom, a known-stale armable hit IS served
//     stale and its refill starts at once.
//   - D2-5b a non-armable read (no sink) reads through, fresh, no debt.
//   - D2-6  non-droppable: a failed refill retries and the debt still clears.
//   - D2-7  a refill that commits while a dependent is being registered still
//     pushes it (decision + registration are one critical section).
//   - D2-8  two identities served stale from one cell each get their own push.
//   - D2-12 a debt overdue past 10 s is counted once.
//   - D2-18 a substrate fill (re-scoped L1 key) under an armable resolve
//     reads through: no stale body is baked into an intermediate cell.
//   - D2-3c a FAILED dependent re-resolve keeps the cell (never evicted on
//     failure, #547), retries on backoff, is counted overdue once past 10 s,
//     and pushes the fresh body once it recovers.
//   - D2-3d the compare-and-evict never evicts a NEWER entry.
//   - D2-19 at zero headroom with a background ticket queued, a debt's
//     dependent re-resolve (refresher origin) dispatches its second, cold
//     content input at once: customer-triggered refresh bypasses, checked
//     before the origin rule.
//   - D2-16 a tab that arms after the replacement was published still gets
//     its refresh, at arm time (the owed-push mark); D2-16b also when the
//     replacement was evicted before the arm.
//   - D2-14 a user never waits on the cap: at zero headroom with the gate held,
//     a known-stale hit returns the stale body at once and the refill
//     still runs (debt class bypasses the cap).
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/utils/ptr"
)

// d2Fixture is a known-stale LIST content cell (fsa-00000 is stale in it,
// fresh in the informer) with the "restactions" refresh handler wired.
type d2Fixture struct {
	store      *cache.ResolvedCacheStore
	contentKey string
	rereso     atomic.Int64
	unsubs     []func()
	// declineRe: the dependent re-resolve COMPLETES but stores nothing (a
	// declined Put). failReN: it FAILS (a transport error) while non-zero,
	// decrementing; negative = always.
	declineRe atomic.Bool
	failReN   atomic.Int32
	// secondInput (D2-19): the dependent re-resolve also reads a second, COLD
	// content input, under the refresher origin resolveOnceProd stamps.
	secondInput  atomic.Bool
	secondFilled atomic.Bool
	sinksMu      sync.Mutex
	sinks        map[string]*cache.StaleServeSink
}

func newD2Fixture(t *testing.T) *d2Fixture {
	t.Helper()
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	dyn := newFill710Watcher(t, 5)
	f := &d2Fixture{store: cache.ResolvedCache(), sinks: map[string]*cache.StaleServeSink{}}
	t.Cleanup(func() {
		for _, u := range f.unsubs {
			u()
		}
	})
	fill710Admission(t)
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	f.contentKey = fill710MakeKnownStale(t, dyn, f.store)
	// The dependent re-resolve (resolveAndPopulateL1's shape): re-Put, publish.
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, key string, in cache.ResolvedKeyInputs) error {
		f.rereso.Add(1)
		if n := f.failReN.Load(); n != 0 {
			if n > 0 {
				f.failReN.Add(-1)
			}
			return errors.New("re-resolve transport failure (injected)")
		}
		if f.declineRe.Load() {
			return nil // completed, Put declined
		}
		if f.secondInput.Load() {
			rctx := cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginRefresher)
			if _, _, ok, err := apistageContentServe(rctx, f.store, d2SecondCall(), false); err != nil || !ok {
				return fmt.Errorf("second content input: ok=%v err=%v", ok, err)
			}
			f.secondFilled.Store(true)
		}
		f.store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"fresh":true}`), Inputs: &in})
		cache.PublishRefresh(key)
		return nil
	})
	// Registered LAST so it runs FIRST (LIFO): the debt goroutines finish while
	// their armed tabs, the refresher and its handlers still exist.
	t.Cleanup(ResetStaleServeForTest)
	return f
}

// customer reads the list as a customer of an ARMABLE resolve producing l1Key.
func (f *d2Fixture) customer(t *testing.T, l1Key string) (any, *cache.StaleServeSink) {
	t.Helper()
	// As the dispatchers do: the L1 key is scoped first, then the sink on it.
	ctx, sink := cache.WithStaleServeSink(cache.WithL1KeyContext(r1Ctx(), l1Key), l1Key, "restactions")
	v, s, ok, err := apistageContentServe(ctx, f.store, fill710ListCall(), false)
	if err != nil || !ok || !s {
		t.Fatalf("customer read not served: ok=%v served=%v err=%v", ok, s, err)
	}
	f.sinksMu.Lock()
	f.sinks[l1Key] = sink
	f.sinksMu.Unlock()
	return v, sink
}

// putDependent does what the dispatcher does at its write point after an
// ACCEPTED Put: store the stale-derived cell, arm a tab on it (the armed
// header), and commit the sink (the owed-push mark + the replace).
func (f *d2Fixture) putDependent(l1Key string) {
	_, unsub := cache.SubscribeRefresh(map[string]struct{}{l1Key: {}})
	f.unsubs = append(f.unsubs, unsub)
	f.storeAndCommit(l1Key)
}

// storeAndCommit is putDependent without the tab (D2-16: the tab arms late).
func (f *d2Fixture) storeAndCommit(l1Key string) {
	f.store.Put(l1Key, &cache.ResolvedEntry{
		RawJSON:      []byte(`{"fresh":false}`),
		Inputs:       &cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Namespace: "d2", Name: l1Key},
		StaleDerived: true,
	})
	f.sinksMu.Lock()
	sink := f.sinks[l1Key]
	f.sinksMu.Unlock()
	CommitStaleServe(sink)
}

func d2WaitDebtsClosed(t *testing.T) {
	t.Helper()
	if !fill710WaitFor(15*time.Second, func() bool { return StaleServeStatsSnapshot().DebtOpen == 0 }) {
		t.Fatalf("debt never closed: %+v", StaleServeStatsSnapshot())
	}
}

func d2Push(t *testing.T, ch <-chan string, want string, d time.Duration) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("pushed %q, want %q", got, want)
		}
	case <-time.After(d):
		t.Fatalf("no /refreshes push for %q within %s (stats %+v)", want, d, StaleServeStatsSnapshot())
	}
}

func TestStale710_D2_1_KnownStaleArmableHitIsServedStaleAndItsDebtFills(t *testing.T) {
	f := newD2Fixture(t)
	v, sink := f.customer(t, "dep-1")
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1StaleManaged {
		t.Fatalf("D2-1: served %d managed, want the STALE %d (show then replace)", got, r1StaleManaged)
	}
	if !sink.StaleDerived() {
		t.Fatalf("D2-1: the stale serve was not noted on the sink — the customer cell would not be marked StaleDerived")
	}
	if s := StaleServeStatsSnapshot(); s.ServeTotal != 1 || s.DebtOpen != 1 {
		t.Fatalf("D2-1: stale_serve_total=%d debt_open=%d, want 1/1", s.ServeTotal, s.DebtOpen)
	}
	f.putDependent("dep-1")
	d2WaitDebtsClosed(t)
	if _, _, ok := cache.KnownStale(f.contentKey); ok {
		t.Fatalf("D2-1 RED: the debt closed but the content cell is still known-stale")
	}
	e, ok := f.store.GetNoTouch(f.contentKey)
	if !ok {
		t.Fatalf("D2-1: the refilled content cell is gone")
	}
	if got := fill710ManagedOf(t, listEnvelopeOf(t, e), "fsa-00000"); got != r1FreshManaged {
		t.Fatalf("D2-1 RED: the debt refill stored %d managed, want the fresh %d", got, r1FreshManaged)
	}
}

func TestStale710_D2_2_FillCommitPushesTheArmedDependent(t *testing.T) {
	f := newD2Fixture(t)
	keys, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-2": {}})
	defer unsub()
	f.customer(t, "dep-2")
	f.putDependent("dep-2")
	d2Push(t, keys, "dep-2", 10*time.Second)
	if e, _ := f.store.GetNoTouch("dep-2"); e == nil || string(e.RawJSON) != `{"fresh":true}` {
		t.Fatalf("D2-2: the stale-derived dependent was not re-resolved")
	}
	// The handler's own publish may arrive first; the debt lane's push follows.
	fill710WaitFor(5*time.Second, func() bool { return StaleServeStatsSnapshot().PushTotal == 1 })
	s := StaleServeStatsSnapshot()
	if s.PushTotal != 1 || s.ToPush.Count != 1 || s.ToPush.MaxMS > staleMaxMS {
		t.Fatalf("D2-2: push_total=%d to_push=%+v", s.PushTotal, s.ToPush)
	}
}

func TestStale710_D2_3_PushSurvivesAReResolveThatDoesNotPut(t *testing.T) {
	f := newD2Fixture(t)
	f.declineRe.Store(true)
	keys, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-3": {}})
	defer unsub()
	f.customer(t, "dep-3")
	f.putDependent("dep-3")
	d2Push(t, keys, "dep-3", 10*time.Second)
	fill710WaitFor(5*time.Second, func() bool { return StaleServeStatsSnapshot().PushTotal == 1 })
	if s := StaleServeStatsSnapshot(); s.DependentUnresolved != 1 || s.PushTotal != 1 {
		t.Fatalf("D2-3: unresolved=%d push=%d, want 1/1", s.DependentUnresolved, s.PushTotal)
	}
}

func TestStale710_D2_4_WithHeadroomAKnownStaleArmableHitIsServedStaleAndRefilledAtOnce(t *testing.T) {
	f := newD2Fixture(t)
	var dispatched atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		dispatched.Add(1)
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	v, _ := f.customer(t, "dep-4")
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1StaleManaged {
		t.Fatalf("D2-4 RED: with headroom the known-stale armable hit read through (%d managed) — the ruling serves it stale", got)
	}
	if !fill710WaitFor(5*time.Second, func() bool { return dispatched.Load() >= 1 }) {
		t.Fatalf("D2-4: the refill did not start")
	}
	f.putDependent("dep-4")
	d2WaitDebtsClosed(t)
}

func TestStale710_D2_5b_NonArmableReadReadsThroughFreshWithNoDebt(t *testing.T) {
	f := newD2Fixture(t)
	v, s, ok, err := apistageContentServe(r1Ctx(), f.store, fill710ListCall(), false)
	if err != nil || !ok || !s {
		t.Fatalf("read not served: ok=%v served=%v err=%v", ok, s, err)
	}
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1FreshManaged {
		t.Fatalf("D2-5b RED: a non-armable read was served %d managed — with no push path it must read through (fresh %d)", got, r1FreshManaged)
	}
	if st := StaleServeStatsSnapshot(); st.ServeTotal != 0 || st.DebtOpen != 0 {
		t.Fatalf("D2-5b: a non-armable read opened a debt: %+v", st)
	}
}

// D2-18 — a nested resolve that fills an intermediate SUBSTRATE cell (it
// re-scopes the L1 key, as the RAFullList fill does) is out of the sink's
// scope: it reads through, so a stale body is never baked into a cell that the
// debt's own dependent re-resolve would read back as fresh.
func TestStale710_D2_18_ASubstrateFillUnderAnArmableResolveReadsThrough(t *testing.T) {
	f := newD2Fixture(t)
	ctx, _ := cache.WithStaleServeSink(cache.WithL1KeyContext(r1Ctx(), "dep-15"), "dep-15", "widgets")
	ctx = cache.WithL1KeyContext(ctx, "ra-full-list-15")
	v, s, ok, err := apistageContentServe(ctx, f.store, fill710ListCall(), false)
	if err != nil || !ok || !s {
		t.Fatalf("read not served: ok=%v served=%v err=%v", ok, s, err)
	}
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1FreshManaged {
		t.Fatalf("D2-18 RED: a substrate fill under an armable resolve was served %d managed — it must read through (fresh %d)", got, r1FreshManaged)
	}
	if st := StaleServeStatsSnapshot(); st.ServeTotal != 0 || st.DebtOpen != 0 {
		t.Fatalf("D2-18: a substrate fill opened a debt: %+v", st)
	}
}

func TestStale710_D2_6_AFailedRefillRetriesAndTheDebtStillClears(t *testing.T) {
	f := newD2Fixture(t)
	var calls atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		if calls.Add(1) <= 2 {
			return nil, false // the first two refill attempts fail
		}
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	f.customer(t, "dep-6")
	f.putDependent("dep-6")
	d2WaitDebtsClosed(t)
	if r := StaleServeStatsSnapshot().DebtRetries; r < 2 {
		t.Fatalf("D2-6: retries=%d, want ≥ 2", r)
	}
	if _, _, ok := cache.KnownStale(f.contentKey); ok {
		t.Fatalf("D2-6 RED: the debt cleared without a committed refill — the cell is still known-stale")
	}
}

func TestStale710_D2_7_ACommitDuringRegistrationStillPushesTheDependent(t *testing.T) {
	f := newD2Fixture(t)
	keys, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-7b": {}})
	defer unsub()
	// Reader A opens the debt; its refill is held in the dispatch.
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		<-gate
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	f.customer(t, "dep-7a")
	f.putDependent("dep-7a")
	// Reader B: the refill is released and given time to commit WHILE B is
	// between its read and its registration.
	staleServeRegistrationHookForTest = func() {
		release()
		time.Sleep(300 * time.Millisecond)
	}
	t.Cleanup(func() { staleServeRegistrationHookForTest = nil })
	f.customer(t, "dep-7b")
	staleServeRegistrationHookForTest = nil
	f.putDependent("dep-7b")
	d2Push(t, keys, "dep-7b", 10*time.Second)
}

func TestStale710_D2_8_TwoIdentitiesEachGetTheirOwnPush(t *testing.T) {
	f := newD2Fixture(t)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		<-gate
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	ka, unA := cache.SubscribeRefresh(map[string]struct{}{"dep-8a": {}})
	defer unA()
	kb, unB := cache.SubscribeRefresh(map[string]struct{}{"dep-8b": {}})
	defer unB()
	f.customer(t, "dep-8a")
	f.customer(t, "dep-8b")
	f.putDependent("dep-8a")
	f.putDependent("dep-8b")
	release()
	d2Push(t, ka, "dep-8a", 10*time.Second)
	d2Push(t, kb, "dep-8b", 10*time.Second)
	select {
	case extra := <-ka:
		t.Fatalf("D2-8: subscriber A also received %q", extra)
	case extra := <-kb:
		t.Fatalf("D2-8: subscriber B also received %q", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestStale710_D2_12_AnOverdueDebtIsCountedOnce(t *testing.T) {
	f := newD2Fixture(t)
	var fail atomic.Bool
	fail.Store(true)
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		if fail.Load() {
			return nil, false
		}
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	setStaleClockForTest(func() time.Time { return time.Unix(0, clock.Load()) })
	t.Cleanup(func() { setStaleClockForTest(nil) })

	f.customer(t, "dep-12")
	clock.Add(int64(11 * time.Second))
	scanStaleOverdue()
	scanStaleOverdue()
	s := StaleServeStatsSnapshot()
	if s.OverdueTotal != 1 {
		t.Fatalf("D2-12 RED: stale_overdue_total=%d after two scans past 10 s, want exactly 1", s.OverdueTotal)
	}
	if s.DebtOldestAgeMS < staleMaxMS || s.DebtOpen != 1 {
		t.Fatalf("D2-12: oldest=%d open=%d — the overdue debt must stay open", s.DebtOldestAgeMS, s.DebtOpen)
	}
	fail.Store(false) // let it clear so the arm leaves nothing behind
	d2WaitDebtsClosed(t)
}

func TestStale710_D2_14_AUserNeverWaitsOnTheCap(t *testing.T) {
	f := newD2Fixture(t)
	fill710SeamCeiling(t, 1)
	holder, _, err := admitContentFillForTest(context.Background(), "d2-14/v1, Resource=held", 1, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)
	t0 := time.Now()
	v, _ := f.customer(t, "dep-14")
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("D2-14 RED: the stale hit took %s at zero headroom", d)
	}
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1StaleManaged {
		t.Fatalf("D2-14: served %d managed, want the stale %d", got, r1StaleManaged)
	}
	f.putDependent("dep-14")
	d2WaitDebtsClosed(t) // the debt refill bypassed the held gate
}

// listEnvelopeOf decodes a stored LIST content cell into the served shape.
func listEnvelopeOf(t *testing.T, e *cache.ResolvedEntry) any {
	t.Helper()
	p, ok := parseListEnvelope(r1FsaGVR, e.RawJSON)
	if !ok {
		t.Fatalf("stored content cell is not a LIST envelope")
	}
	return serveParsedListEnvelopeValue(p)
}

func serveParsedListEnvelopeValue(p parsedListEnvelope) any {
	v, _ := serveParsedListEnvelope(p)
	return v
}

// D2-16 — late arm. The stale-derived cell is committed with NO tab armed; the
// debt clears and the replacement is published to nobody. A tab that arms 12 s
// later (seamed clock) must still get `event: refresh` for the key within 1 s
// of the arm, and the owed mark is then cleared.
// F2 (architect): the arm-time delivery is a correction like any other — the
// tab showed the stale body until it — so stale_to_push records it (emit −
// servedAt), and the late arm's ≥12 s lands in the histogram (count +1, max ≥
// 12000 ms). A publish-time-only record would drop the slowest corrections.
// Mutation → RED: no owed mark (Commit without MarkStaleOwed), no emit at arm
// (SubscribeRefreshStream's owed loop removed), or no stale_to_push record at
// the arm-time emit.
func TestStale710_D2_16_ALateArmStillGetsTheRefresh(t *testing.T) {
	f := newD2Fixture(t)
	f.customer(t, "dep-16")
	f.storeAndCommit("dep-16")
	d2WaitDebtsClosed(t)
	if !fill710WaitFor(10*time.Second, func() bool {
		e, ok := f.store.GetNoTouch("dep-16")
		return ok && !e.StaleDerived
	}) {
		t.Fatalf("PRECONDITION: the dependent cell was never replaced (%+v)", StaleServeStatsSnapshot())
	}
	if st := StaleServeStatsSnapshot(); st.PushUnarmed != 1 || !cache.StaleOwed("dep-16") {
		t.Fatalf("PRECONDITION: the replacement must have been published to no tab, leaving the push owed (unarmed=%d owed=%v)",
			st.PushUnarmed, cache.StaleOwed("dep-16"))
	}
	tp0 := StaleServeStatsSnapshot().ToPush
	if tp0.Count != 0 {
		t.Fatalf("PRECONDITION: stale_to_push recorded %d sample(s) for a publish that reached no tab", tp0.Count)
	}
	base := time.Now()
	setStaleClockForTest(func() time.Time { return base.Add(12 * time.Second) })
	t.Cleanup(func() { setStaleClockForTest(nil) })
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-16": {}})
	defer unsub()
	d2Push(t, ch, "dep-16", time.Second)
	if cache.StaleOwed("dep-16") {
		t.Fatalf("D2-16: the owed mark survived its arm-time delivery")
	}
	st := StaleServeStatsSnapshot()
	if st.Owed.ArmEmits != 1 || st.Owed.Dropped != 0 {
		t.Fatalf("D2-16: owed arm_emits=%d dropped=%d, want 1/0", st.Owed.ArmEmits, st.Owed.Dropped)
	}
	if st.ToPush.Count != tp0.Count+1 || st.ToPush.MaxMS < 12000 {
		t.Fatalf("D2-16 RED (F2): the arm-time correction must be recorded on stale_to_push (count %d→%d, max %d ms, want +1 and ≥ 12000)",
			tp0.Count, st.ToPush.Count, st.ToPush.MaxMS)
	}
}

// D2-16b — the mark survives the replacement's EVICTION: the fresh cell is
// evicted before the tab arms; the arm still emits, and the refetch (a miss,
// i.e. a fresh fill) reads fresh content.
// Mutation → RED: the mark dies with the cell (staleOwedReady false when the
// cell is absent).
func TestStale710_D2_16b_ALateArmAfterTheReplacementWasEvictedStillGetsTheRefresh(t *testing.T) {
	f := newD2Fixture(t)
	f.customer(t, "dep-16b")
	f.storeAndCommit("dep-16b")
	d2WaitDebtsClosed(t)
	if !fill710WaitFor(10*time.Second, func() bool {
		e, ok := f.store.GetNoTouch("dep-16b")
		return ok && !e.StaleDerived
	}) {
		t.Fatalf("PRECONDITION: the dependent cell was never replaced (%+v)", StaleServeStatsSnapshot())
	}
	f.store.DeleteForTest("dep-16b")
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-16b": {}})
	defer unsub()
	d2Push(t, ch, "dep-16b", time.Second)
	// The refetch misses the evicted cell and fills: its content read is fresh.
	v, s, ok, err := apistageContentServe(r1Ctx(), f.store, fill710ListCall(), false)
	if err != nil || !ok || !s {
		t.Fatalf("refetch content read not served: ok=%v served=%v err=%v", ok, s, err)
	}
	if got := fill710ManagedOf(t, v, "fsa-00000"); got != r1FreshManaged {
		t.Fatalf("D2-16b: the refetch read %d managed, want fresh %d", got, r1FreshManaged)
	}
}

// D2-3b — a committed dependent whose re-resolve does not replace it is
// EVICTED (so the pushed refetch is a fresh fill, not a hit on the same stale
// body) and the push still goes out.
// Mutation → RED: drop the EvictStaleDerived call.
func TestStale710_D2_3b_AnUnreplacedStaleDerivedCellIsEvictedSoTheRefetchIsFresh(t *testing.T) {
	f := newD2Fixture(t)
	f.declineRe.Store(true)
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-3b": {}})
	defer unsub()
	f.customer(t, "dep-3b")
	f.storeAndCommit("dep-3b")
	d2Push(t, ch, "dep-3b", 10*time.Second)
	if _, ok := f.store.GetNoTouch("dep-3b"); ok {
		t.Fatalf("D2-3b RED: the unreplaced stale-derived cell is still resident — the pushed refetch would hit the stale body again")
	}
	if st := StaleServeStatsSnapshot(); st.DependentUnresolved != 1 {
		t.Fatalf("D2-3b: dependent_unresolved=%d, want 1", st.DependentUnresolved)
	}
}

// D2-3c — a FAILED dependent re-resolve (transport error) keeps the
// stale-derived cell resident (#547: never evict on failure), retries on
// #690's backoff, counts overdue once past 10 s (seamed clock), and once it
// recovers pushes the FRESH body.
// Mutation → RED: evict on failure too (treat err != nil like a decline).
func TestStale710_D2_3c_AFailedReplaceKeepsTheCellRetriesAndPushesOnRecovery(t *testing.T) {
	f := newD2Fixture(t)
	f.failReN.Store(-1)
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-3c": {}})
	defer unsub()
	f.customer(t, "dep-3c")
	f.storeAndCommit("dep-3c")
	if !fill710WaitFor(10*time.Second, func() bool { return StaleServeStatsSnapshot().ReplaceRetries >= 2 }) {
		t.Fatalf("D2-3c: the failed replace was not retried (%+v)", StaleServeStatsSnapshot())
	}
	e, ok := f.store.GetNoTouch("dep-3c")
	if !ok || !e.StaleDerived {
		t.Fatalf("D2-3c RED: a FAILED re-resolve evicted the cell (resident=%v) — #547: a failed revalidation keeps serving, never evicts", ok)
	}
	ov0 := StaleServeStatsSnapshot().OverdueTotal
	base := time.Now()
	setStaleClockForTest(func() time.Time { return base.Add(11 * time.Second) })
	t.Cleanup(func() { setStaleClockForTest(nil) })
	if !fill710WaitFor(5*time.Second, func() bool { return StaleServeStatsSnapshot().OverdueTotal > ov0 }) {
		t.Fatalf("D2-3c: a replace failing past 10 s was not counted overdue (%+v)", StaleServeStatsSnapshot())
	}
	time.Sleep(600 * time.Millisecond) // more scans: still exactly once
	if d := StaleServeStatsSnapshot().OverdueTotal - ov0; d != 1 {
		t.Fatalf("D2-3c: overdue counted %d times for one failing replace, want once", d)
	}
	f.failReN.Store(0) // recovery
	d2Push(t, ch, "dep-3c", 10*time.Second)
	e, ok = f.store.GetNoTouch("dep-3c")
	if !ok || e.StaleDerived || string(e.RawJSON) != `{"fresh":true}` {
		t.Fatalf("D2-3c: after recovery the cell is resident=%v staleDerived=%v — want the fresh replacement", ok, ok && e.StaleDerived)
	}
	if st := StaleServeStatsSnapshot(); st.DependentUnresolved != 0 {
		t.Fatalf("D2-3c: dependent_unresolved=%d, want 0 (a failure is not a decline)", st.DependentUnresolved)
	}
}

// D2-3d — EvictStaleDerived is compare-and-evict: it never removes a NEWER
// entry than the one the replace decided on.
// Mutation → RED: evict by key without the identity check.
func TestStale710_D2_3d_TheCompareAndEvictNeverEvictsANewerEntry(t *testing.T) {
	store := newD2Fixture(t).store
	in := &cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Namespace: "d2", Name: "dep-3d"}
	store.Put("dep-3d", &cache.ResolvedEntry{RawJSON: []byte(`{"fresh":false}`), Inputs: in, StaleDerived: true})
	old, _ := store.GetNoTouch("dep-3d")
	store.Put("dep-3d", &cache.ResolvedEntry{RawJSON: []byte(`{"fresh":true}`), Inputs: in})
	t.Cleanup(func() { store.DeleteForTest("dep-3d") })
	if store.EvictStaleDerived("dep-3d", old) {
		t.Fatalf("D2-3d RED: the compare-and-evict removed a newer entry")
	}
	if e, ok := store.GetNoTouch("dep-3d"); !ok || string(e.RawJSON) != `{"fresh":true}` {
		t.Fatalf("D2-3d: the newer entry is gone (resident=%v)", ok)
	}
	cur, _ := store.GetNoTouch("dep-3d")
	if store.EvictStaleDerived("dep-3d", cur) {
		t.Fatalf("D2-3d: a non-stale-derived entry was evicted")
	}
}

// d2SecondCall is a second, cold content input: a single-object GET on the
// same informer (its own content key).
func d2SecondCall() httpcall.RequestOptions {
	return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: "/apis/composition.krateo.io/v1/namespaces/" + r1Ns + "/fullstackapps/fsa-00001",
		Verb: ptr.To(http.MethodGet),
	}}
}

// D2-19 — at ZERO headroom (the gate held by a background fill) with a
// background ticket QUEUED, a committed dependent's re-resolve — under the
// refresher origin, as resolveOnceProd stamps it — reads a second, cold
// content input. That fill must dispatch at once (customer-triggered refresh
// bypasses, checked BEFORE the origin rule), so the push lands.
// Mutation → RED: the customer-triggered check placed after the origin rule
// in contentFillClassOf (the second fill queues behind the held gate).
func TestStale710_D2_19_ADependentReResolveDispatchesItsColdContentInputAtZeroHeadroom(t *testing.T) {
	f := newD2Fixture(t)
	f.secondInput.Store(true)
	fill710SeamCeiling(t, 1)
	holder, _, err := admitContentFillForTest(context.Background(), "d2-19/v1, Resource=held", 1, fillClassBackground)
	if err != nil {
		t.Fatalf("admit holder: %v", err)
	}
	t.Cleanup(holder)
	queued := make(chan struct{})
	go func() {
		defer close(queued)
		if rel, _, err := admitContentFillForTest(context.Background(), "d2-19/v1, Resource=queued", 1, fillClassBackground); err == nil {
			rel()
		}
	}()
	t.Cleanup(func() { holder(); <-queued })
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-19": {}})
	defer unsub()
	f.customer(t, "dep-19")
	f.storeAndCommit("dep-19")
	d2Push(t, ch, "dep-19", 5*time.Second)
	if !f.secondFilled.Load() {
		t.Fatalf("D2-19 RED: the push landed without the second content input being filled")
	}
}
