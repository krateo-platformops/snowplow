// issue740_viewer_retry_test.go — #740 PR-A: the #547 slots on the viewer-gated
// retry engine (architect answers issuecomment-6250211062), on the k547 harness
// (real informers, the real Widgets() handler, the production RefreshFunc
// behind the real refresher loop), a REAL /refreshes subscription as the viewer
// and the engine's manual clock (no minute-long sleeps).
//
//	S1  background, viewed: a genuinely failing shell revalidation retries at
//	    1 s, 2 s, … while its key is armed, and heals once the identity recovers.
//	S2  background, unviewed: abandoned at its first retry with no attempt;
//	    an arm resumes it at once and it heals.
//	S3  demand, viewed: an identity-bound cell's failing demand revalidation
//	    retries at 1 s, 2 s, … under the requester and heals on recovery.
//	S4  Q3: the newest same-key request is the template — a request with an
//	    expired credential is superseded by an absorbed one with a valid
//	    credential, and the rerun runs the newer one.
//	S5  Q4: a suppressed key opens no chain.
//	K5  the background slot's dequeue is customer-triggered at both gates; a
//	    plain (change-driven) refresh of the same cell stays background.
package dispatchers

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func k740Clock(t *testing.T) *cache.StaleRetryManualClock {
	t.Helper()
	c := cache.NewStaleRetryManualClockForTest()
	cache.SetStaleRetryClockForTest(c)
	t.Cleanup(func() { cache.SetStaleRetryClockForTest(nil) })
	return c
}

// k740View arms key on a real /refreshes connection; the returned func closes it.
func k740View(t *testing.T, key string) func() {
	t.Helper()
	return k740ViewStream(t, key).Unsub
}

// k740ViewStream is k740View returning the connection (its Keys channel).
func k740ViewStream(t *testing.T, key string) cache.RefreshStream {
	t.Helper()
	st := cache.SubscribeRefreshStream(map[string]struct{}{key: {}})
	if !cache.HasRefreshSubscriber(key) {
		t.Fatalf("PRECONDITION: the /refreshes layer did not arm the key")
	}
	t.Cleanup(st.Unsub)
	return st
}

// k740AwaitWake waits until exactly one wake is pending (the slot's settle runs
// on the refresher goroutine) and returns its delay.
func k740AwaitWake(t *testing.T, c *cache.StaleRetryManualClock, what string) time.Duration {
	t.Helper()
	var p []time.Duration
	if !k547WaitFor(t, 10*time.Second, what, func() bool { p = c.Pending(); return len(p) == 1 }) {
		t.Fatalf("%s: pending wakes %v, want exactly 1", what, p)
	}
	return p[0]
}

func k740RetryTotal(kind cache.StaleRetryOutcome, src cache.StaleRetrySource) uint64 {
	return cache.StaleRetryStatsSnapshot().StaleRetryTotalFor(kind, src)
}

func TestIssue740_S1_AViewedShellRetriesOnTheBackoffAndHeals(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	contentKey, handle := k740Shell(t, h)
	view := k740ViewStream(t, contentKey)
	g0 := k740RetryTotal(cache.StaleRetryGenuine, cache.StaleRetryBackground)

	cache.RevalidateKnownStaleInBackground(contentKey) // the first attempt: denied
	if d := k740AwaitWake(t, clock, "first retry scheduled"); d != time.Second {
		t.Fatalf("S1 RED: the first retry waits %v, want 1 s", d)
	}
	clock.FireNext() // retry #1: still denied
	if d := k740AwaitWake(t, clock, "second retry scheduled"); d != 2*time.Second {
		t.Fatalf("S1 RED: the second retry waits %v, want 2 s (doubling)", d)
	}
	for drained := false; !drained; { // the earlier serves' pushes, if any
		select {
		case <-view.Keys:
		default:
			drained = true
		}
	}
	h.allowBackground.Store(true) // the refresher identity recovers
	clock.FireNext()              // retry #2: heals

	if e, ok := handle.GetNoTouch(contentKey); !ok || k547Reason(e.RawJSON) != "v1" {
		t.Fatalf("S1: the viewed shell did not heal on the retry after recovery")
	}
	// A3 — the viewer is told: event: refresh for the healed key within 1 s of
	// the healing commit.
	select {
	case k := <-view.Keys:
		if k != contentKey {
			t.Fatalf("S1: the viewer was pushed %q, want the healed shell's key", k)
		}
	case <-time.After(time.Second):
		t.Fatalf("S1 RED: the viewer got no event: refresh within 1 s of the healing commit")
	}
	if open, _ := cache.StaleRetryChainOpenForTest(contentKey); open || len(clock.Pending()) != 0 {
		t.Fatalf("S1: the chain did not end on the fresh commit (pending %v)", clock.Pending())
	}
	if got := k740RetryTotal(cache.StaleRetryGenuine, cache.StaleRetryBackground) - g0; got != 2 {
		t.Fatalf("S1: retry_total{genuine,background} moved %d, want 2 (the first failure + retry #1)", got)
	}
}

func TestIssue740_S2_AnUnviewedShellIsAbandonedAndAnArmResumesIt(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	contentKey, handle := k740Shell(t, h)

	cache.RevalidateKnownStaleInBackground(contentKey)
	k740AwaitWake(t, clock, "first retry scheduled")
	bg0 := h.bgCalls.Load()
	clock.FireNext() // nobody views it
	if h.bgCalls.Load() != bg0 || len(clock.Pending()) != 0 {
		t.Fatalf("S2 RED: an unviewed shell was retried (attempts +%d, pending %v)", h.bgCalls.Load()-bg0, clock.Pending())
	}
	if open, abandoned := cache.StaleRetryChainOpenForTest(contentKey); !open || !abandoned {
		t.Fatalf("S2: the chain is not abandoned (open=%v abandoned=%v)", open, abandoned)
	}
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("S2: the abandoned shell must stay known-stale and served (#547)")
	}

	h.allowBackground.Store(true)
	r0 := cache.StaleRetryStatsSnapshot().Resumed
	k740View(t, contentKey) // a tab arms it
	if d := k740AwaitWake(t, clock, "resumed attempt scheduled"); d != 0 {
		t.Fatalf("S2 RED: the resumed attempt waits %v, want at once", d)
	}
	clock.FireNext()
	if e, ok := handle.GetNoTouch(contentKey); !ok || k547Reason(e.RawJSON) != "v1" {
		t.Fatalf("S2: the resumed attempt did not heal the shell")
	}
	if got := cache.StaleRetryStatsSnapshot().Resumed - r0; got != 1 {
		t.Fatalf("S2: resumed_total moved %d, want 1", got)
	}
}

func TestIssue740_S3_AViewedIdentityBoundCellRetriesUnderTheRequester(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	k740View(t, key)
	h.failCustomer.Store(true)
	g0 := k740RetryTotal(cache.StaleRetryGenuine, cache.StaleRetryDemand)

	h.get(0, k547Changed) // serves stale, starts the demand attempt: fails
	if d := k740AwaitWake(t, clock, "first retry scheduled"); d != time.Second {
		t.Fatalf("S3 RED: the first demand retry waits %v, want 1 s", d)
	}
	clock.FireNext()
	if d := k740AwaitWake(t, clock, "second retry scheduled"); d != 2*time.Second {
		t.Fatalf("S3 RED: the second demand retry waits %v, want 2 s", d)
	}
	h.failCustomer.Store(false) // the requester's read recovers
	clock.FireNext()
	if r, _ := h.residentReason(key); r != "v1" {
		t.Fatalf("S3: the viewed cell did not heal on the retry after recovery (reason %q)", r)
	}
	if got := k740RetryTotal(cache.StaleRetryGenuine, cache.StaleRetryDemand) - g0; got != 2 {
		t.Fatalf("S3: retry_total{genuine,demand} moved %d, want 2", got)
	}
	if open, _ := cache.StaleRetryChainOpenForTest(key); open {
		t.Fatalf("S3: the chain did not end on the fresh commit")
	}
}

type k740CredKey struct{}

func TestIssue740_S4_TheNewestSameKeyRequestIsTheTemplate(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	k740Clock(t)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	gate := func(ctx context.Context) error {
		if v, _ := ctx.Value(k740CredKey{}).(string); v == "expired" {
			return apierrors.NewUnauthorized("the request's credential has expired (injected, #740 Q3)")
		}
		return nil
	}
	h.custGate.Store(&gate)
	park := k740ParkDoomed(h, false, 1)

	expired := context.WithValue(k547ReqCtx(0), k740CredKey{}, "expired")
	if r, code := h.getCtx(expired, k547Changed); code != 200 || r != "v0" { // claims; its attempt reads the expired credential
		t.Fatalf("S4: the first serve got %q (code %d)", r, code)
	}
	park.await(t, 0)
	valid := context.WithValue(k547ReqCtx(0), k740CredKey{}, "valid")
	h.getCtx(valid, k547Changed) // absorbed: the newer request becomes the template
	close(park.release[0])

	if !k547WaitFor(t, 10*time.Second, "cell healed", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("S4 RED: the cell never healed — the rerun ran the FIRST request (expired credential) instead of the " +
			"newest same-key request (#740 Q3)")
	}
}

func TestIssue740_S5_ASuppressedKeyOpensNoChain(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	k740View(t, key)
	if !cache.NoteRefreshDecline(key, "uaf", true) {
		t.Fatalf("PRECONDITION: the key did not become suppressed")
	}
	h.failCustomer.Store(true)
	h.get(0, k547Changed)
	k547WaitFor(t, 2*time.Second, "settle", func() bool { return cache.DemandRevalidationsInFlight() == 0 })
	if open, _ := cache.StaleRetryChainOpenForTest(key); open || len(clock.Pending()) != 0 {
		t.Fatalf("S5 RED: a suppressed key opened a retry chain (pending %v) — it can never succeed (Q4)", clock.Pending())
	}
}

func TestIssue740_K5_TheBackgroundSlotDequeueIsCustomerTriggeredAtBothGates(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	contentKey, handle := k740Shell(t, h)
	h.allowBackground.Store(true)

	cache.RevalidateKnownStaleInBackground(contentKey)
	if !k547WaitFor(t, 10*time.Second, "shell healed", func() bool {
		e, ok := handle.GetNoTouch(contentKey)
		return ok && k547Reason(e.RawJSON) == "v1"
	}) {
		t.Fatalf("PRECONDITION: the background revalidation never healed the shell")
	}
	var reval []k547GateSeen
	for _, g := range h.gates() {
		if g.bg {
			reval = append(reval, g)
		}
	}
	if len(reval) == 0 {
		t.Fatalf("PRECONDITION: no background resolve recorded")
	}
	for _, g := range reval {
		if !g.triggered || g.fill != "debt" || g.nestedBg {
			t.Fatalf("K5 RED: the background revalidation dequeue resolved at %+v — a customer is waiting on it: it "+
				"must be customer-triggered, fill at the debt class and be a customer tree at the nested gate", g)
		}
	}

	// Control: a change-driven refresh of the same shell (no slot held) stays
	// background.
	n0 := len(h.gates())
	h.update(k547Changed, "v2")
	if !k547WaitFor(t, 10*time.Second, "shell refreshed", func() bool {
		e, ok := handle.GetNoTouch(contentKey)
		return ok && k547Reason(e.RawJSON) == "v2"
	}) {
		t.Fatalf("PRECONDITION: the change-driven refresh never ran")
	}
	for _, g := range h.gates()[n0:] {
		if g.bg && (g.triggered || g.fill != "background") {
			t.Fatalf("K5 control: a plain change-driven refresh resolved at %+v, want background", g)
		}
	}
}

// S5b — Q4: a key that becomes suppressed while its chain is open ends the chain
// at the next wake, with no attempt: it can never succeed.
func TestIssue740_S5b_AKeySuppressedMidChainEndsIt(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	k740View(t, key)
	h.failCustomer.Store(true)
	h.get(0, k547Changed)
	k740AwaitWake(t, clock, "first retry scheduled")
	if !cache.NoteRefreshDecline(key, "uaf", true) {
		t.Fatalf("PRECONDITION: the key did not become suppressed")
	}
	c0 := h.custCalls.Load()
	clock.FireNext()
	if open, _ := cache.StaleRetryChainOpenForTest(key); open || len(clock.Pending()) != 0 || h.custCalls.Load() != c0 {
		t.Fatalf("S5b RED: a chain on a suppressed key kept retrying (open=%v pending=%v attempts +%d) — Q4: no chain",
			open, clock.Pending(), h.custCalls.Load()-c0)
	}
}

// S6 — bounded state (PR-A review): an ABANDONED chain is retired when its cell
// heals through another carrier (here a change-driven refresher refresh), with
// nobody re-arming it — the chain set never outgrows the known-stale set.
func TestIssue740_S6_AnAbandonedChainEndsWhenTheCellHealsElsewhere(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	contentKey, handle := k740Shell(t, h)
	cache.RevalidateKnownStaleInBackground(contentKey)
	k740AwaitWake(t, clock, "first retry scheduled")
	clock.FireNext() // nobody views it: abandoned
	if cache.StaleRetryAbandonedForTest() != 1 {
		t.Fatalf("PRECONDITION: %d abandoned chains, want 1", cache.StaleRetryAbandonedForTest())
	}

	h.allowBackground.Store(true)
	h.update(k547Changed, "v2") // the next change heals the shell through the refresher
	if !k547WaitFor(t, 10*time.Second, "shell healed", func() bool {
		e, ok := handle.GetNoTouch(contentKey)
		return ok && k547Reason(e.RawJSON) == "v2"
	}) {
		t.Fatalf("PRECONDITION: the change-driven refresh never healed the shell")
	}
	if !k547WaitFor(t, 5*time.Second, "abandoned chain retired", func() bool {
		open, _ := cache.StaleRetryChainOpenForTest(contentKey)
		return !open && cache.StaleRetryAbandonedForTest() == 0
	}) {
		t.Fatalf("S6 RED: the abandoned chain outlived its healed cell (abandoned=%d) — with nobody re-arming it, "+
			"it would be held forever", cache.StaleRetryAbandonedForTest())
	}
}

// S6b — the same when the cell is EVICTED: the chain ends, counted
// abandoned{evicted}.
func TestIssue740_S6b_AnAbandonedChainEndsWhenTheCellIsEvicted(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	clock := k740Clock(t)
	contentKey, _ := k740Shell(t, h)
	cache.RevalidateKnownStaleInBackground(contentKey)
	k740AwaitWake(t, clock, "first retry scheduled")
	clock.FireNext() // abandoned
	e0 := cache.StaleRetryStatsSnapshot().AbandonedEvicted
	cache.ResolvedCache().DeleteForTest(contentKey)
	if !k547WaitFor(t, 5*time.Second, "abandoned chain retired", func() bool {
		open, _ := cache.StaleRetryChainOpenForTest(contentKey)
		return !open && cache.StaleRetryAbandonedForTest() == 0
	}) {
		t.Fatalf("S6b RED: the abandoned chain outlived its evicted cell (abandoned=%d)", cache.StaleRetryAbandonedForTest())
	}
	if got := cache.StaleRetryStatsSnapshot().AbandonedEvicted - e0; got != 1 {
		t.Fatalf("S6b: abandoned{evicted} moved %d, want 1", got)
	}
}
