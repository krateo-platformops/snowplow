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
//   - D2-6  (#740) a TRANSIENT refill failure (the fill's Put refused: the
//     generation moved) retries at once and the debt clears.
//   - D2-7  a refill that commits while a dependent is being registered still
//     pushes it (decision + registration are one critical section).
//   - D2-8  two identities served stale from one cell each get their own push.
//   - D2-12 a debt overdue past 10 s is counted once; with nobody viewing it
//     the chain is abandoned and the dependent's arm resumes it (#740).
//   - D2-18 a substrate fill (re-scoped L1 key) under an armable resolve
//     reads through: no stale body is baked into an intermediate cell.
//   - D2-3c a FAILED dependent re-resolve keeps the cell (never evicted on
//     failure, #547), retries at 1 s, 2 s, 4 s while its tab is armed (#740,
//     the engine's manual clock), is counted overdue once past 10 s, and
//     pushes the fresh body once it recovers.
//   - V8a  (#740) a GENUINE refill failure with no armed dependent: abandoned
//     at the first retry, with no attempt; the debt stays open.
//   - V8b  (#740) the same with an armed dependent: retried at 1 s, 2 s, then
//     it heals and pushes.
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
	// #754 — the debt's refill runs AT ONCE (owner ruling: never queued), so on a
	// loaded machine it can commit before the debt_open read below: park it on
	// the seam until the open-state assertions have run (as D2-15b does).
	release := HoldStaleDebtRefillsForTest()
	t.Cleanup(release)
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
	release()
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

func TestStale710_D2_6_ATransientRefillFailureRetriesAtOnceAndTheDebtClears(t *testing.T) {
	f := newD2Fixture(t)
	var first atomic.Bool
	var refills atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		if contentDebtRefillFromContext(ctx) {
			refills.Add(1)
			if first.CompareAndSwap(false, true) {
				// The debt's first refill flight: the content cell's generation
				// moves mid-fill (a DELETE-eviction, then the same stale body put
				// back, so the cell stays resident), so its generation-guarded Put
				// is refused (#189) — a TRANSIENT failure.
				prior, _ := f.store.GetNoTouch(f.contentKey)
				f.store.DeleteForTest(f.contentKey)
				f.store.Put(f.contentKey, prior)
			}
		}
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	tr0 := cache.StaleRetryStatsSnapshot().StaleRetryTotalFor(cache.StaleRetryTransient, cache.StaleRetryDebt)
	g0 := cache.StaleRetryStatsSnapshot().StaleRetryTotalFor(cache.StaleRetryGenuine, cache.StaleRetryDebt)
	t0 := time.Now()
	f.customer(t, "dep-6")
	f.putDependent("dep-6")
	d2WaitDebtsClosed(t)
	if d := time.Since(t0); d > 900*time.Millisecond {
		t.Fatalf("D2-6 RED: a transient refill failure took %s to clear — it must retry at once, not on the 1 s genuine backoff", d)
	}
	st := cache.StaleRetryStatsSnapshot()
	if got := st.StaleRetryTotalFor(cache.StaleRetryTransient, cache.StaleRetryDebt) - tr0; got != 1 {
		t.Fatalf("D2-6: stale_retry_total{transient,debt} moved %d, want 1", got)
	}
	if got := st.StaleRetryTotalFor(cache.StaleRetryGenuine, cache.StaleRetryDebt) - g0; got != 0 {
		t.Fatalf("D2-6 RED: a refused fill was classified genuine (+%d) — Q4: refused ⇒ transient", got)
	}
	if r := StaleServeStatsSnapshot().DebtRetries; r < 1 {
		t.Fatalf("D2-6: retries=%d, want ≥ 1", r)
	}
	if n := refills.Load(); n != 2 {
		t.Fatalf("D2-6: %d refill dispatches, want 2 (the refused one + its immediate retry, which cleared the debt)", n)
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
	// #740 — nobody views it, so its chain is abandoned at the first retry and
	// the debt stays open; the dependent's arm resumes it at once.
	fail.Store(false)
	f.putDependent("dep-12")
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
// stale-derived cell resident (#547: never evict on failure), retries at 1 s,
// 2 s, 4 s while its tab is armed (#740, on the engine's manual clock), counts
// overdue once past 10 s (seamed debt clock), and once it recovers pushes the
// FRESH body.
// Mutation → RED: evict on failure too (treat err != nil like a decline).
func TestStale710_D2_3c_AFailedReplaceKeepsTheCellRetriesAndPushesOnRecovery(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	f.failReN.Store(-1)
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-3c": {}})
	defer unsub()
	f.customer(t, "dep-3c")
	f.storeAndCommit("dep-3c")
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		var p []time.Duration
		if !fill710WaitFor(10*time.Second, func() bool { p = clock.Pending(); return len(p) == 1 }) {
			t.Fatalf("D2-3c: replace retry #%d never scheduled (pending %v, %+v)", i+1, p, StaleServeStatsSnapshot())
		}
		if p[0] != want {
			t.Fatalf("D2-3c RED: replace retry #%d waits %v, want %v (genuine: 1 s doubling, #740)", i+1, p[0], want)
		}
		if i < 2 {
			clock.FireNext()
		}
	}
	if r := StaleServeStatsSnapshot().ReplaceRetries; r != 3 {
		t.Fatalf("D2-3c: replace_retries=%d, want 3 (the first failure + two retries)", r)
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
	clock.FireNext()
	d2Push(t, ch, "dep-3c", 10*time.Second)
	e, ok = f.store.GetNoTouch("dep-3c")
	if !ok || e.StaleDerived || string(e.RawJSON) != `{"fresh":true}` {
		t.Fatalf("D2-3c: after recovery the cell is resident=%v staleDerived=%v — want the fresh replacement", ok, ok && e.StaleDerived)
	}
	if st := StaleServeStatsSnapshot(); st.DependentUnresolved != 0 {
		t.Fatalf("D2-3c: dependent_unresolved=%d, want 0 (a failure is not a decline)", st.DependentUnresolved)
	}
}

// V8a — #740: a GENUINE refill failure whose debt has no armed dependent is
// abandoned at its first retry (no attempt); the debt stays open, still served
// stale and overdue-counted.
// Mutation → RED: D2 debts exempt from the viewer gate.
func TestStale710_V8a_AGenuinelyFailingDebtNobodyViewsIsAbandoned(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	var calls atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(context.Context, httpcall.RequestOptions) ([]byte, bool) {
		calls.Add(1)
		return nil, false // the upstream is down
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	f.customer(t, "dep-v8a")
	f.storeAndCommit("dep-v8a") // committed, but no tab armed
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("V8a: the first retry was never scheduled (%+v)", StaleServeStatsSnapshot())
	}
	c0 := calls.Load()
	a0 := cache.StaleRetryStatsSnapshot().AbandonedNoViewer
	clock.FireNext()
	if calls.Load() != c0 || len(clock.Pending()) != 0 {
		t.Fatalf("V8a RED: a debt nobody views was retried (refill dispatches +%d, pending %v)", calls.Load()-c0, clock.Pending())
	}
	if got := cache.StaleRetryStatsSnapshot().AbandonedNoViewer - a0; got != 1 {
		t.Fatalf("V8a: stale_retry_abandoned_total{no_viewer} moved %d, want 1", got)
	}
	if st := StaleServeStatsSnapshot(); st.DebtOpen != 0 || st.DebtParked != 1 {
		t.Fatalf("V8a: debt_open=%d parked=%d, want 0/1 — the abandoned debt is PARKED (kept, served stale, not overdue)", st.DebtOpen, st.DebtParked)
	}
	dispatchViaInformerFn = orig
	f.putDependent("dep-v8a-viewer") // unrelated arm: not a dependent, resumes nothing
	if len(clock.Pending()) != 0 {
		t.Fatalf("V8a: an arm of a non-dependent key resumed the chain")
	}
}

// V8b — #740: with an armed dependent the genuinely failing refill follows the
// 1 s → 60 s schedule, and heals (and pushes) on recovery.
func TestStale710_V8b_AViewedFailingDebtRetriesOnTheBackoffAndHeals(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
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
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-v8b": {}})
	defer unsub()
	f.customer(t, "dep-v8b")
	f.storeAndCommit("dep-v8b")
	for i, want := range []time.Duration{time.Second, 2 * time.Second} {
		var p []time.Duration
		if !fill710WaitFor(10*time.Second, func() bool { p = clock.Pending(); return len(p) == 1 }) {
			t.Fatalf("V8b: retry #%d never scheduled (pending %v)", i+1, p)
		}
		if p[0] != want {
			t.Fatalf("V8b RED: refill retry #%d waits %v, want %v (genuine: 1 s doubling, viewed)", i+1, p[0], want)
		}
		if i == 1 {
			fail.Store(false) // the upstream recovers
		}
		clock.FireNext()
	}
	d2WaitDebtsClosed(t)
	d2Push(t, ch, "dep-v8b", 10*time.Second)
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

// V8c — #740, the 1.12.50 release condition: a genuinely failing D2 refill
// STOPS retrying the moment its last viewer leaves (the tab's /refreshes
// connection closes): the pending wake is cancelled and no refill dispatches.
func TestStale710_V8c_AFailingDebtStopsWhenItsLastViewerLeaves(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	var calls atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(context.Context, httpcall.RequestOptions) ([]byte, bool) {
		calls.Add(1)
		return nil, false // the upstream is down
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	_, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-v8c": {}})
	f.customer(t, "dep-v8c")
	f.storeAndCommit("dep-v8c")
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("V8c: the first retry was never scheduled")
	}
	clock.FireNext() // viewed: retried
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("V8c: the second retry was never scheduled")
	}
	unsub() // the last viewer leaves
	if p := clock.Pending(); len(p) != 0 {
		t.Fatalf("V8c RED: %d wake(s) still pending after the last viewer left — D2 keeps retrying for nobody", len(p))
	}
	c0 := calls.Load()
	clock.FireNext()
	if calls.Load() != c0 {
		t.Fatalf("V8c RED: a refill dispatched after the last viewer left")
	}
	if st := StaleServeStatsSnapshot(); st.DebtOpen != 0 || st.DebtParked != 1 {
		t.Fatalf("V8c: debt_open=%d parked=%d, want 0/1 — the abandoned debt is PARKED", st.DebtOpen, st.DebtParked)
	}
	dispatchViaInformerFn = orig
}

// --- #740 PR-B review: parked debts, R1 retirement, the replace chain ---------

// d2Parked opens a debt whose refill keeps failing, with one committed but
// UNARMED dependent, and parks it (its first retry finds no viewer). It returns
// the manual engine clock, a switch that heals the upstream and the seamed debt
// clock's base.
func d2Parked(t *testing.T, f *d2Fixture, dep string) (*cache.StaleRetryManualClock, func(), *atomic.Int64) {
	t.Helper()
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	var debtNow atomic.Int64
	debtNow.Store(time.Now().UnixNano())
	setStaleClockForTest(func() time.Time { return time.Unix(0, debtNow.Load()) })
	t.Cleanup(func() { setStaleClockForTest(nil) })
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
	f.customer(t, dep)
	f.storeAndCommit(dep)
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("PRECONDITION: the first retry was never scheduled (%+v)", StaleServeStatsSnapshot())
	}
	clock.FireNext() // nobody views a dependent: parked
	if st := StaleServeStatsSnapshot(); st.DebtParked != 1 || st.DebtOpen != 0 {
		t.Fatalf("PRECONDITION: debt_open=%d parked=%d, want 0/1", st.DebtOpen, st.DebtParked)
	}
	return clock, func() { fail.Store(false) }, &debtNow
}

// D2-12b — a PARKED debt is not overdue: past 10 s with nobody viewing it,
// stale_overdue_total does not move and the oldest-age gauge reads 0.
// Mutation → RED: the overdue scan ignores parked.
func TestStale710_D2_12b_AParkedDebtIsNotOverdue(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, debtNow := d2Parked(t, f, "dep-12b")
	ov0 := StaleServeStatsSnapshot().OverdueTotal
	debtNow.Add(int64(30 * time.Second))
	scanStaleOverdue()
	if st := StaleServeStatsSnapshot(); st.OverdueTotal != ov0 || st.DebtOldestAgeMS != 0 {
		t.Fatalf("D2-12b RED: a parked debt read overdue (+%d) / oldest_age=%d — nobody views it, its clock is stopped",
			st.OverdueTotal-ov0, st.DebtOldestAgeMS)
	}
	heal()
}

// D2-12c — a resume (a dependent's arm) RESTARTS the overdue clock: overdue at
// resume+10 s, not at the resume.
func TestStale710_D2_12c_AResumeRestartsTheOverdueClock(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, debtNow := d2Parked(t, f, "dep-12c")
	ov0 := StaleServeStatsSnapshot().OverdueTotal
	debtNow.Add(int64(20 * time.Second)) // parked for 20 s
	_, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-12c": {}})
	defer unsub()
	if st := StaleServeStatsSnapshot(); st.DebtOpen != 1 || st.DebtParked != 0 {
		t.Fatalf("D2-12c: the arm did not resume the parked debt (open=%d parked=%d)", st.DebtOpen, st.DebtParked)
	}
	debtNow.Add(int64(5 * time.Second))
	scanStaleOverdue()
	if got := StaleServeStatsSnapshot().OverdueTotal - ov0; got != 0 {
		t.Fatalf("D2-12c RED: overdue +%d at resume+5 s — the clock must restart at the resume, not count the parked time", got)
	}
	debtNow.Add(int64(6 * time.Second))
	scanStaleOverdue()
	if got := StaleServeStatsSnapshot().OverdueTotal - ov0; got != 1 {
		t.Fatalf("D2-12c: overdue +%d at resume+11 s, want 1", got)
	}
	heal()
}

// D2-12d — a new stale serve JOINING a parked debt resumes it: an immediate
// attempt, resumed_total +1.
func TestStale710_D2_12d_AJoinResumesAParkedDebt(t *testing.T) {
	f := newD2Fixture(t)
	clock, heal, _ := d2Parked(t, f, "dep-12d")
	r0 := cache.StaleRetryStatsSnapshot().Resumed
	f.customer(t, "dep-12d-2") // another stale serve joins the debt
	if got := cache.StaleRetryStatsSnapshot().Resumed - r0; got != 1 {
		t.Fatalf("D2-12d RED: a stale serve joining the parked debt did not resume it (resumed +%d)", got)
	}
	if p := clock.Pending(); len(p) != 1 || p[0] != 0 {
		t.Fatalf("D2-12d: the resume scheduled %v, want one immediate attempt", p)
	}
	if st := StaleServeStatsSnapshot(); st.DebtOpen != 1 || st.DebtParked != 0 {
		t.Fatalf("D2-12d: open=%d parked=%d after the resume, want 1/0", st.DebtOpen, st.DebtParked)
	}
	heal()
	clock.FireNext()
	d2WaitDebtsClosed(t)
}

// D2-12e — overdue counts once per VIEWED episode: two episodes past 10 s → +2.
func TestStale710_D2_12e_TwoViewedEpisodesAreOverdueTwice(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, debtNow := d2Parked(t, f, "dep-12e")
	ov0 := StaleServeStatsSnapshot().OverdueTotal
	for ep := 1; ep <= 2; ep++ {
		_, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-12e": {}}) // viewed: resumed
		debtNow.Add(int64(11 * time.Second))
		scanStaleOverdue()
		scanStaleOverdue()
		if got := StaleServeStatsSnapshot().OverdueTotal - ov0; got != uint64(ep) {
			t.Fatalf("D2-12e: overdue +%d after viewed episode %d, want %d (once per episode)", got, ep, ep)
		}
		unsub() // the viewer leaves: parked again
		if st := StaleServeStatsSnapshot(); st.DebtParked != 1 {
			t.Fatalf("D2-12e: episode %d's end did not park the debt (parked=%d)", ep, st.DebtParked)
		}
	}
	heal()
}

// D2-20 — R1: a REFRESHER fill of the content (refresh-forced) retires a
// parked debt through the G2 bridge: open, parked and oldest_age all 0, the
// chain ends, and a dependent tab arming afterwards gets its refresh (the
// owed-push mark).
// Mutations → RED: own-refill-only retirement; startSeq ignored (D2-20c).
func TestStale710_D2_20_ARefresherFillRetiresAParkedDebt(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, _ := d2Parked(t, f, "dep-20")
	heal()
	rctx := cache.WithRefreshTriggerGVR(cache.WithBackgroundResolveOrigin(r1Ctx(), cache.BackgroundOriginRefresher), r1FsaGVR)
	if _, _, ok, err := apistageContentServe(rctx, f.store, fill710ListCall(), false); err != nil || !ok {
		t.Fatalf("PRECONDITION: the refresher fill failed: ok=%v err=%v", ok, err)
	}
	d2RequireRetired(t, f, "D2-20", "dep-20")
}

// D2-20b — R1 via a customer READ-THROUGH (a non-armable read).
func TestStale710_D2_20b_AReadThroughRetiresAParkedDebt(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, _ := d2Parked(t, f, "dep-20b")
	heal()
	if _, _, ok, err := apistageContentServe(r1Ctx(), f.store, fill710ListCall(), false); err != nil || !ok {
		t.Fatalf("PRECONDITION: the read-through failed: ok=%v err=%v", ok, err)
	}
	d2RequireRetired(t, f, "D2-20b", "dep-20b")
}

func d2RequireRetired(t *testing.T, f *d2Fixture, arm, dep string) {
	t.Helper()
	if !fill710WaitFor(10*time.Second, func() bool {
		st := StaleServeStatsSnapshot()
		return st.DebtOpen == 0 && st.DebtParked == 0
	}) {
		t.Fatalf("%s RED: the debt survived an accepted fill of its content (%+v) — R1: any accepted fill at or after "+
			"its mark retires it", arm, StaleServeStatsSnapshot())
	}
	scanStaleOverdue()
	if st := StaleServeStatsSnapshot(); st.DebtOldestAgeMS != 0 {
		t.Fatalf("%s: oldest_age=%d after the retire, want 0", arm, st.DebtOldestAgeMS)
	}
	if open, _ := cache.StaleRetryChainOpenForTest(f.contentKey); open {
		t.Fatalf("%s: the retired debt's chain is still held", arm)
	}
	if !fill710WaitFor(10*time.Second, func() bool {
		e, ok := f.store.GetNoTouch(dep)
		return ok && !e.StaleDerived
	}) {
		t.Fatalf("%s: the dependent was not replaced by the G2 bridge", arm)
	}
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{dep: {}})
	defer unsub()
	d2Push(t, ch, dep, 10*time.Second)
}

// D2-20c — R1's epoch rule: a fill whose epoch is BEFORE the debt's mark does
// not retire it; one at the mark does.
func TestStale710_D2_20c_AFillBeforeTheMarkDoesNotRetire(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, _ := d2Parked(t, f, "dep-20c")
	defer heal()
	staleDebtsMu.Lock()
	d := staleDebts[f.contentKey]
	staleDebtsMu.Unlock()
	if d == nil || d.markSeq == 0 {
		t.Fatalf("PRECONDITION: no parked debt with a mark")
	}
	retireStaleDebtOnFill(f.contentKey, d.markSeq-1)
	if st := StaleServeStatsSnapshot(); st.DebtParked != 1 {
		t.Fatalf("D2-20c RED: a fill that began before the mark retired the debt (%+v)", st)
	}
	retireStaleDebtOnFill(f.contentKey, d.markSeq)
	if st := StaleServeStatsSnapshot(); st.DebtParked != 0 || st.DebtOpen != 0 {
		t.Fatalf("D2-20c: a fill at the mark did not retire the debt (%+v)", st)
	}
	// G1 (dev-710) — a direct call: no Put, so no store event re-checks the
	// chain; only R1's own StaleRetryEnd can have ended it.
	if open, _ := cache.StaleRetryChainOpenForTest(f.contentKey); open {
		t.Fatalf("D2-20c RED: the retired debt's retry chain is still held — R1 must end it, not wait for a store event")
	}
}

// D2-20d — PR-B review (made required): a PARKED debt whose content cell is
// EVICTED (and never refreshed again) is retired at the eviction — the same
// rule as an abandoned chain's — through the G2 bridge: open and parked 0, the
// chain gone (abandoned{evicted}), the dependent replaced.
// Mutation → RED: no Evicted hook on the debt's chain.
func TestStale710_D2_20d_AParkedDebtWhoseContentIsEvictedIsRetired(t *testing.T) {
	f := newD2Fixture(t)
	_, heal, _ := d2Parked(t, f, "dep-20d")
	heal()
	e0 := cache.StaleRetryStatsSnapshot().AbandonedEvicted
	f.store.DeleteForTest(f.contentKey) // the content cell is evicted
	if !fill710WaitFor(10*time.Second, func() bool {
		st := StaleServeStatsSnapshot()
		return st.DebtOpen == 0 && st.DebtParked == 0
	}) {
		t.Fatalf("D2-20d RED: a parked debt outlived its evicted content cell (%+v) — it would linger forever", StaleServeStatsSnapshot())
	}
	if open, _ := cache.StaleRetryChainOpenForTest(f.contentKey); open {
		t.Fatalf("D2-20d: the evicted content's chain is still held")
	}
	if got := cache.StaleRetryStatsSnapshot().AbandonedEvicted - e0; got != 1 {
		t.Fatalf("D2-20d: abandoned{evicted} moved %d, want 1", got)
	}
	if !fill710WaitFor(10*time.Second, func() bool {
		e, ok := f.store.GetNoTouch("dep-20d")
		return ok && !e.StaleDerived
	}) {
		t.Fatalf("D2-20d: the dependent was not replaced by the G2 bridge")
	}
}

// D2-3c(b) — dev-710: a failed replace with NO viewer: its first retry finds
// nobody — no re-resolve, no pending wake, the chain abandoned, the cell kept
// resident and stale-derived.
func TestStale710_D2_3cb_AFailedReplaceNobodyViewsIsAbandoned(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	f.failReN.Store(-1)
	f.customer(t, "dep-3cb")
	f.storeAndCommit("dep-3cb") // no tab
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("D2-3c(b): the first replace retry was never scheduled")
	}
	r0 := f.rereso.Load()
	clock.FireNext()
	if f.rereso.Load() != r0 || len(clock.Pending()) != 0 {
		t.Fatalf("D2-3c(b) RED: a replace nobody views was retried (re-resolves +%d, pending %v)", f.rereso.Load()-r0, clock.Pending())
	}
	if open, abandoned := cache.StaleRetryChainOpenForTest("dep-3cb"); !open || !abandoned {
		t.Fatalf("D2-3c(b): the replace chain is not abandoned (open=%v abandoned=%v)", open, abandoned)
	}
	if e, ok := f.store.GetNoTouch("dep-3cb"); !ok || !e.StaleDerived {
		t.Fatalf("D2-3c(b): the cell must stay resident and stale-derived (#547, never evicted on failure)")
	}
}

// D2-3e — the replace chain's StillStale is a REAL check: another writer
// replaces the dependent mid-chain → the chain stops with no further attempt
// (no re-resolve, no publish) and the newer entry is untouched.
// Mutation → RED: StillStale always true (the attempt runs and publishes).
func TestStale710_D2_3e_AReplaceChainStopsWhenAnotherWriterReplacedTheCell(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	f.failReN.Store(-1)
	_, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-3e": {}})
	defer unsub()
	f.customer(t, "dep-3e")
	f.storeAndCommit("dep-3e")
	if !fill710WaitFor(10*time.Second, func() bool { return len(clock.Pending()) == 1 }) {
		t.Fatalf("D2-3e: the first replace retry was never scheduled")
	}
	newer := &cache.ResolvedEntry{RawJSON: []byte(`{"newer":true}`),
		Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: "restactions", Namespace: "d2", Name: "dep-3e"}}
	f.store.Put("dep-3e", newer) // another writer replaced the stale-derived cell
	r0, p0 := f.rereso.Load(), StaleServeStatsSnapshot().PushTotal
	clock.FireNext()
	if f.rereso.Load() != r0 || StaleServeStatsSnapshot().PushTotal != p0 || len(clock.Pending()) != 0 {
		t.Fatalf("D2-3e RED: the replace chain ran on after another writer replaced the cell (re-resolves +%d, pushes +%d, pending %v)",
			f.rereso.Load()-r0, StaleServeStatsSnapshot().PushTotal-p0, clock.Pending())
	}
	if open, _ := cache.StaleRetryChainOpenForTest("dep-3e"); open {
		t.Fatalf("D2-3e: the replace chain is still held")
	}
	if e, ok := f.store.GetNoTouch("dep-3e"); !ok || string(e.RawJSON) != `{"newer":true}` {
		t.Fatalf("D2-3e: the newer entry was touched (resident=%v)", ok)
	}
}

// D2-1b — #754, the product side of D2-1's race: the refill commits BEFORE the
// dependent reaches its write point (the debt closes while the sink is still
// pending). The dependent's Commit must still replace the stale-derived cell
// and push its armed tab.
func TestStale710_D2_1b_ARefillThatWinsTheRaceStillReplacesAtCommit(t *testing.T) {
	f := newD2Fixture(t)
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{"dep-1b": {}})
	defer unsub()
	f.customer(t, "dep-1b")
	d2WaitDebtsClosed(t) // the refill won: the debt closed before the write point
	if _, _, ok := cache.KnownStale(f.contentKey); ok {
		t.Fatalf("PRECONDITION: the content is still known-stale after the debt closed")
	}
	f.storeAndCommit("dep-1b") // the dependent's write point, after the debt cleared
	d2Push(t, ch, "dep-1b", 10*time.Second)
	if e, ok := f.store.GetNoTouch("dep-1b"); !ok || e.StaleDerived || string(e.RawJSON) != `{"fresh":true}` {
		t.Fatalf("D2-1b RED: a dependent committed after its debt cleared was not replaced (resident=%v)", ok)
	}
}

// V8d — A2 for D2 (architect-ch, #753): a refill REFUSED every time (the
// generation keeps moving: a transient failure) whose dependents have no
// viewer gets ONE immediate retry, then its next transient wake parks the
// debt — it never loops on the <= 5 s transient backoff for nobody.
// Mutation → RED: transient retries ungated (the engine's A2 gate).
func TestStale710_V8d_ARefusedRefillNobodyViewsStopsAfterOneImmediateRetry(t *testing.T) {
	f := newD2Fixture(t)
	clock := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(clock)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	var refills atomic.Int64
	orig := dispatchViaInformerFn
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		if contentDebtRefillFromContext(ctx) {
			refills.Add(1)
			// Every refill's generation moves mid-fill and the cell stays
			// resident: its Put is always refused (#189).
			prior, _ := f.store.GetNoTouch(f.contentKey)
			f.store.DeleteForTest(f.contentKey)
			f.store.Put(f.contentKey, prior)
		}
		return orig(ctx, call)
	}
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	f.customer(t, "dep-v8d")
	f.storeAndCommit("dep-v8d") // committed, no tab armed
	var p []time.Duration
	if !fill710WaitFor(10*time.Second, func() bool { p = clock.Pending(); return len(p) == 1 }) {
		t.Fatalf("V8d: the first retry was never scheduled (%+v)", StaleServeStatsSnapshot())
	}
	if p[0] != 0 {
		t.Fatalf("V8d: the refused refill's first retry waits %v, want at once (transient)", p[0])
	}
	clock.FireNext() // the one immediate, ungated retry: refused again
	if !fill710WaitFor(10*time.Second, func() bool { p = clock.Pending(); return len(p) == 1 }) {
		t.Fatalf("V8d: the next transient retry was never scheduled")
	}
	r0 := refills.Load()
	clock.FireNext() // the next transient wake: nobody views a dependent
	if refills.Load() != r0 || len(clock.Pending()) != 0 {
		t.Fatalf("V8d RED: a refused refill nobody views kept retrying (refills +%d, pending %v) — only its first, "+
			"immediate retry is ungated", refills.Load()-r0, clock.Pending())
	}
	if n := refills.Load(); n != 2 {
		t.Fatalf("V8d: %d refill dispatches, want 2 (the first attempt + its one immediate retry)", n)
	}
	if st := StaleServeStatsSnapshot(); st.DebtParked != 1 || st.DebtOpen != 0 {
		t.Fatalf("V8d: open=%d parked=%d, want 0/1 — the debt is PARKED", st.DebtOpen, st.DebtParked)
	}
}
