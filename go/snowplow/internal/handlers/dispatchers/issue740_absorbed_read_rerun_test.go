// issue740_absorbed_read_rerun_test.go — #740 (architect ruling: BUILD): a read
// absorbed into a revalidation already in flight earns that attempt ONE rerun,
// at both slots.
//
// THE GAP (traced in #734, mode 2). A known-stale cell holds one revalidation
// slot per key. A read that finds it held is absorbed. If the attempt in flight
// read its inputs before the state that reader needs — here, the refresher
// identity's deny, just before the identity recovered — it fails, the slot is
// released, and with no new change event nothing retries before the NEXT read,
// which on a low-traffic cell is unbounded.
//
// SUBSTRATE: the #547 k547 harness (real informers, real Widgets() handler, the
// production RefreshFunc behind the real refresher loop). onDoomed parks an
// attempt AFTER it has read its injected failure, so the ordering is exact — no
// sleeps.
//
//	K1  the trace: a doomed background attempt in flight, the identity recovers,
//	    ONE serve, no further reads → the shell heals; rerun{background} +1.
//	K2  no loop: the identity stays denied; one absorbed serve → exactly one
//	    extra attempt, then the slot is released.
//	K3  a read during the rerun, which is also doomed → one more rerun (+2),
//	    then it stops.
//	K4  the demand slot (identity-bound cell, ClaimDemandRevalidation): K1's
//	    shape under the requester → heals; rerun{demand} +1.
//	K6  a HEALED attempt earns no rerun, at both slots: one serve absorbed into
//	    an attempt that succeeds → rerun unchanged, the slot released, no second
//	    attempt (architect-ch's gate: the still-known-stale check had no arm).
//
// K5 (the rerun bypasses the admission cap) lands with PR-2b's customer-
// triggered marker, which is not on main yet (ruling: "whichever lands second").
package dispatchers

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// k740Shell makes the identity-free widget shell of the changed composition
// KNOWN-STALE (its background refresh dropped under the denied refresher
// identity) with no revalidation in flight, and returns its key.
func k740Shell(t *testing.T, h *k547Harness) (string, cacheHandle) {
	t.Helper()
	ctx := k547ReqCtx(0)
	contentKey, handle, inputs := dispatchWidgetContentKey(ctx,
		k547WidgetGVR.Group, k547WidgetGVR.Version, k547WidgetGVR.Resource, k547WNS, k547WName, -1, -1,
		effectiveKeyExtras(ctx, h.widgetU.Object, k547Extras(k547Changed)))
	if handle == nil || contentKey == "" {
		t.Fatalf("PRECONDITION: no widgetContent key")
	}
	shell := k547Widget()
	shell.Object["status"] = map[string]any{"widgetData": map[string]any{"conditions": []any{
		map[string]any{"type": "Ready", "status": "True", "reason": "v0"}}}}
	raw, _ := json.Marshal(shell.Object)
	putFixture(handle, contentKey, &cache.ResolvedEntry{RawJSON: raw, Inputs: inputs})
	cache.Deps().Record(ctx, contentKey, k547CompGVR, k547CompNS, fmt.Sprintf("comp-%d", k547Changed))
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("PRECONDITION: the shared shell must serve before the UPDATE (code=%d reason=%q)", code, r)
	}
	d0 := k547Stat("dirty_ended_unfresh_dropped")
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "shell dropped", func() bool { return k547Stat("dirty_ended_unfresh_dropped") > d0 }) {
		t.Fatalf("PRECONDITION: the shell's background refresh never reached the drop point")
	}
	if !k547WaitFor(t, 10*time.Second, "no revalidation in flight", func() bool {
		return cache.BackgroundRevalidationsInFlight() == 0 && k547Stat("queue_depth") == 0
	}) {
		t.Fatalf("PRECONDITION: a background revalidation is still in flight")
	}
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("PRECONDITION: the shell is not known-stale after the drop")
	}
	return contentKey, handle
}

// k740Park parks the n-th doomed attempt of the given kind (bg or customer) for
// each n in parks: entered[i] is closed when it parks, release[i] unparks it.
type k740Park struct {
	entered, release []chan struct{}
	seen             atomic.Int32
}

func k740ParkDoomed(h *k547Harness, wantBG bool, parks int) *k740Park {
	p, fn := k740NewPark(wantBG, parks)
	h.onDoomed.Store(&fn)
	return p
}

// k740ParkHealing parks the n-th HEALING attempt (one that read no injected
// failure and will succeed) of the given kind (K6).
func k740ParkHealing(h *k547Harness, wantBG bool, parks int) *k740Park {
	p, fn := k740NewPark(wantBG, parks)
	h.onHealing.Store(&fn)
	return p
}

func k740NewPark(wantBG bool, parks int) (*k740Park, func(bg bool)) {
	p := &k740Park{}
	for i := 0; i < parks; i++ {
		p.entered = append(p.entered, make(chan struct{}))
		p.release = append(p.release, make(chan struct{}))
	}
	fn := func(bg bool) {
		if bg != wantBG {
			return
		}
		if i := int(p.seen.Add(1)) - 1; i < parks {
			close(p.entered[i])
			<-p.release[i]
		}
	}
	return p, fn
}

func (p *k740Park) await(t *testing.T, i int) {
	t.Helper()
	select {
	case <-p.entered[i]:
	case <-time.After(10 * time.Second):
		t.Fatalf("doomed attempt #%d never started", i+1)
	}
}

func k740Rerun(slot string) uint64 { return cache.KnownStaleRevalidationRerunBySlot()[slot] }

func k740WaitSlotReleased(t *testing.T, arm string) {
	t.Helper()
	if !k547WaitFor(t, 10*time.Second, "background slot released", func() bool {
		return cache.BackgroundRevalidationsInFlight() == 0 && k547Stat("queue_depth") == 0
	}) {
		t.Fatalf("%s RED: the background revalidation slot was never released — the rerun loops on itself", arm)
	}
}

func TestIssue740_K1_AnAbsorbedReadEarnsTheShellOneRerunThatHeals(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	contentKey, handle := k740Shell(t, h)
	park := k740ParkDoomed(h, true, 1)
	rr0 := k740Rerun("background")

	cache.RevalidateKnownStaleInBackground(contentKey) // the attempt in flight
	park.await(t, 0)                                   // it has read the deny
	h.allowBackground.Store(true)                      // the refresher identity recovers
	if r, code := h.get(1, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("K1: the absorbed serve got %q (code %d), want the known-stale shell v0", r, code)
	}
	close(park.release[0]) // the doomed attempt returns its 403; no further reads

	if !k547WaitFor(t, 10*time.Second, "shell healed", func() bool {
		e, ok := handle.GetNoTouch(contentKey)
		return ok && k547Reason(e.RawJSON) == "v1"
	}) {
		t.Fatalf("K1 RED: the shell never healed — the serve absorbed into the doomed attempt earned no rerun, " +
			"and nothing retries before the next read (#740)")
	}
	if got := k740Rerun("background") - rr0; got != 1 {
		t.Fatalf("K1: known_stale_revalidation_rerun{background} moved %d, want 1", got)
	}
	k740WaitSlotReleased(t, "K1")
	if _, _, ks := cache.KnownStale(contentKey); ks {
		t.Fatalf("K1: the healed shell is still known-stale")
	}
}

func TestIssue740_K2_TheRerunNeverLoopsWithoutAnotherRead(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	contentKey, _ := k740Shell(t, h)
	park := k740ParkDoomed(h, true, 1)
	rr0 := k740Rerun("background")
	bg0 := h.bgCalls.Load()

	cache.RevalidateKnownStaleInBackground(contentKey)
	park.await(t, 0)
	h.get(1, k547Changed) // absorbed; the identity stays denied
	close(park.release[0])

	k740WaitSlotReleased(t, "K2")
	if got := k740Rerun("background") - rr0; got != 1 {
		t.Fatalf("K2: known_stale_revalidation_rerun{background} moved %d, want exactly 1", got)
	}
	if got := h.bgCalls.Load() - bg0; got != 2 {
		t.Fatalf("K2 RED: %d background attempts, want exactly 2 (the doomed one + its one rerun)", got)
	}
	if _, _, ks := cache.KnownStale(contentKey); !ks {
		t.Fatalf("K2: a cell whose every attempt failed must stay known-stale (#547: served, never evicted)")
	}
}

func TestIssue740_K3_AReadDuringTheRerunEarnsOneMore(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	contentKey, _ := k740Shell(t, h)
	park := k740ParkDoomed(h, true, 2)
	rr0 := k740Rerun("background")
	bg0 := h.bgCalls.Load()

	cache.RevalidateKnownStaleInBackground(contentKey)
	park.await(t, 0)
	h.get(1, k547Changed) // absorbed into the first attempt
	close(park.release[0])
	park.await(t, 1)      // the rerun is in flight (and doomed too)
	h.get(0, k547Changed) // absorbed into the rerun
	close(park.release[1])

	k740WaitSlotReleased(t, "K3")
	if got := k740Rerun("background") - rr0; got != 2 {
		t.Fatalf("K3 RED: known_stale_revalidation_rerun{background} moved %d, want 2 — a read absorbed into the "+
			"rerun must arm one more", got)
	}
	if got := h.bgCalls.Load() - bg0; got != 3 {
		t.Fatalf("K3: %d background attempts, want exactly 3", got)
	}
}

func TestIssue740_K4_TheDemandSlotRerunsAnAbsorbedRead(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	h.failCustomer.Store(true)
	park := k740ParkDoomed(h, false, 1)
	rr0 := k740Rerun("demand")

	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" { // starts the demand attempt
		t.Fatalf("K4: the first serve got %q (code %d), want the known-stale v0", r, code)
	}
	park.await(t, 0)            // the attempt has read the requester's failure
	h.failCustomer.Store(false) // the requester's read recovers
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("K4: the absorbed serve got %q (code %d), want the known-stale v0", r, code)
	}
	close(park.release[0])

	if !k547WaitFor(t, 10*time.Second, "cell healed", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("K4 RED: the identity-bound cell never healed — the read absorbed into the doomed demand " +
			"revalidation earned no rerun (#740)")
	}
	if got := k740Rerun("demand") - rr0; got != 1 {
		t.Fatalf("K4: known_stale_revalidation_rerun{demand} moved %d, want 1", got)
	}
	if !k547WaitFor(t, 10*time.Second, "demand slot released", func() bool { return cache.DemandRevalidationsInFlight() == 0 }) {
		t.Fatalf("K4: the demand slot was never released")
	}
}

func TestIssue740_K6_AHealedAttemptEarnsNoRerunAtEitherSlot(t *testing.T) {
	t.Run("background", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		contentKey, handle := k740Shell(t, h)
		h.allowBackground.Store(true) // the attempt below will succeed
		park := k740ParkHealing(h, true, 1)
		rr0 := k740Rerun("background")
		bg0 := h.bgCalls.Load()

		cache.RevalidateKnownStaleInBackground(contentKey)
		park.await(t, 0)      // the healing attempt is in flight
		h.get(1, k547Changed) // absorbed into it: arms rerun
		close(park.release[0])

		k740WaitSlotReleased(t, "K6/background")
		if e, ok := handle.GetNoTouch(contentKey); !ok || k547Reason(e.RawJSON) != "v1" {
			t.Fatalf("K6/background: the healing attempt did not heal the shell")
		}
		if got := k740Rerun("background") - rr0; got != 0 {
			t.Fatalf("K6/background RED: known_stale_revalidation_rerun{background} moved %d, want 0 — an attempt "+
				"that HEALED the cell earned a rerun for its absorbed read (the still-known-stale check is gone)", got)
		}
		if got := h.bgCalls.Load() - bg0; got != 1 {
			t.Fatalf("K6/background RED: %d background attempts, want exactly 1 — a second dequeue ran for a cell "+
				"the first attempt had already healed", got)
		}
	})
	t.Run("demand", func(t *testing.T) {
		h := newK547Harness(t, k547DenyBackground)
		h.warm()
		h.driveChangedToKnownStale("v1")
		key := h.key(0, k547Changed)
		park := k740ParkHealing(h, false, 1)
		rr0 := k740Rerun("demand")
		c0 := h.custCalls.Load()

		h.get(0, k547Changed) // a known-stale hit: starts the demand attempt, which will succeed
		park.await(t, 0)
		h.get(0, k547Changed) // absorbed into it: arms rerun
		close(park.release[0])

		if !k547WaitFor(t, 10*time.Second, "demand slot released", func() bool { return cache.DemandRevalidationsInFlight() == 0 }) {
			t.Fatalf("K6/demand: the demand slot was never released")
		}
		if r, _ := h.residentReason(key); r != "v1" {
			t.Fatalf("K6/demand: the healing attempt did not heal the cell (reason %q)", r)
		}
		if got := k740Rerun("demand") - rr0; got != 0 {
			t.Fatalf("K6/demand RED: known_stale_revalidation_rerun{demand} moved %d, want 0 — an attempt that "+
				"HEALED the cell earned a rerun for its absorbed read", got)
		}
		if got := h.custCalls.Load() - c0; got != 1 {
			t.Fatalf("K6/demand RED: %d customer-shaped resolves, want exactly 1 — a second attempt ran for a cell "+
				"the first attempt had already healed", got)
		}
	})
}

// K7 (#740 x #744, architect-ch) — a demand RERUN is customer-triggered at BOTH
// admission gates, exactly like the first attempt (D2-19b): a customer is
// waiting on it. The first attempt is doomed and returns before the resolve
// seam records its class, so every demand resolve recorded here is the rerun's.
func TestIssue740_K7_ADemandRerunIsCustomerTriggeredAtBothGates(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	h.warm()
	h.driveChangedToKnownStale("v1")
	key := h.key(0, k547Changed)
	h.failCustomer.Store(true)
	park := k740ParkDoomed(h, false, 1)
	rr0 := k740Rerun("demand")

	h.get(0, k547Changed) // starts the doomed demand attempt
	park.await(t, 0)
	h.failCustomer.Store(false)
	h.get(0, k547Changed) // absorbed: arms the rerun
	close(park.release[0])

	if !k547WaitFor(t, 10*time.Second, "cell healed by the rerun", func() bool { r, _ := h.residentReason(key); return r == "v1" }) {
		t.Fatalf("PRECONDITION: the demand rerun never healed the cell")
	}
	if got := k740Rerun("demand") - rr0; got != 1 {
		t.Fatalf("PRECONDITION: rerun{demand} moved %d, want 1", got)
	}
	var seen []k547GateSeen
	for _, g := range h.gates() {
		if g.demand {
			seen = append(seen, g)
		}
	}
	if len(seen) != 1 {
		t.Fatalf("PRECONDITION: %d demand resolves recorded, want exactly the rerun's (%+v)", len(seen), seen)
	}
	if g := seen[0]; !g.triggered || g.fill != "debt" || g.nestedBg {
		t.Fatalf("K7 RED: the demand RERUN resolved at %+v — a customer is waiting on it: like the first attempt it "+
			"must be customer-triggered, fill at the bypass (debt) class and be a customer tree at the nested gate", g)
	}
}
