package cache

// issue506_deadline_trigger_test.go — #506 / #538: the reaper's DEADLINE-keyed
// proactive trigger.
//
// WHAT IS BEING FALSIFIED. Before this change the read-independent pass watched
// only the BODY clock (CreatedAt, via TTLRemainingSeconds < TTL/4). Nothing
// watched the BIRTH clock (BornAt) approaching maxEntryAge, so a cell could sit
// in its re-mint lead window and never be enqueued — and the refresher terminal
// is the only writer that re-mints, so no enqueue means no re-mint means the
// cell reaches the C5 cap and cold-navigates under a customer.
//
// THE ARM MUST DISCRIMINATE, which is the whole difficulty here. A cell whose
// body is ALSO old would be enqueued by the pre-existing TTL condition, so an
// arm built on one would go green with the deadline trigger deleted and prove
// nothing (feedback: a falsifier's shape must discriminate; an arm that cannot
// fail is not coverage). Every cell below therefore pins the two conditions
// apart explicitly:
//
//	                      birth age     body age    ttlApproaching  deadline
//	 deadline-only cell    150s (>=100)     ~0s        FALSE          TRUE
//	 ttl-only cell          10s             80s        TRUE           FALSE
//	 both cell             150s (>=100)     80s        TRUE           TRUE
//	 neither cell           10s             ~0s        FALSE          FALSE
//
// With TTL=100s and maxEntryAge=200s: remintLead() = min(TTL, maxAge/2) = 100s,
// so the deadline window opens at birth age >= maxAge-lead = 100s, and
// refreshBelow = TTL/4 = 25s so ttlApproaching needs a body older than 75s.
//
// The deadline-only cell is the #538 shape in miniature: a dep-quiet keepwarm
// cell whose body is kept young by the keepwarm re-Put, so its TTL never
// approaches however close to the cap its BIRTH gets.
//
// It is also deliberately NOT past the cap (150s < 200s): the cell under test is
// one the pass must reach BEFORE the deadline, not one already over it. An arm
// that installed a past-cap cell would be testing the eviction axis, not this
// trigger (feedback: drive the real boundary, do not install the crossed state).

import (
	"testing"
	"time"
)

// i506Put puts a resident, warm (SeededAtBoot), non-suppressed widgets cell with
// an explicit birth and body age, and asserts residency. Warmth comes from the
// seed flag rather than a Get so the arm does not depend on read bookkeeping.
func i506Put(t *testing.T, store *ResolvedCacheStore, name string, birthAge, bodyAge time.Duration) string {
	t.Helper()
	now := time.Now()
	inputs := widgetInputs(gvrFlexes(), "krateo-system", name)
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{
		RawJSON:      []byte(`{"cell":"` + name + `"}`),
		Inputs:       inputs,
		BornAt:       now.Add(-birthAge),
		CreatedAt:    now.Add(-bodyAge),
		SeededAtBoot: true,
	})
	if _, ok := store.MetadataForKey(key); !ok {
		t.Fatalf("precondition: %s must be resident after Put", name)
	}
	return key
}

func TestIssue506_DeadlineTriggerReachesWhatTheTTLTriggerCannot(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "100")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "200")
	// Inert summary goroutine: the reap is driven directly below, so no fast
	// ticker leaks into the -race suite.
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}

	// The bounds must be what the arm's arithmetic assumes, or every threshold
	// below is measured against the wrong number and a green is meaningless.
	if got, want := store.maxEntryAge, 200*time.Second; got != want {
		t.Fatalf("precondition: maxEntryAge = %v, want %v — the env did not take", got, want)
	}
	if got, want := store.remintLead(), 100*time.Second; got != want {
		t.Fatalf("precondition: remintLead() = %v, want %v — the window the arm targets is not where it thinks", got, want)
	}

	deadlineKey := i506Put(t, store, "deadline-only", 150*time.Second, 0)
	i506Put(t, store, "ttl-only", 10*time.Second, 80*time.Second)
	// The OVERLAP cell, and it is the one that makes the marginal counter
	// falsifiable. Without a cell in BOTH windows, dropping `&& !ttlApproaching`
	// from the attribution changes nothing and a mutation of it goes green — the
	// first version of this arm had exactly that hole.
	i506Put(t, store, "both", 150*time.Second, 80*time.Second)
	i506Put(t, store, "neither", 10*time.Second, 0)

	// The cell under test must be INSIDE the lead window and NOT past the cap,
	// otherwise this is the eviction axis rather than the trigger.
	m, ok := store.MetadataForKey(deadlineKey)
	if !ok {
		t.Fatal("precondition: the deadline cell must still be resident")
	}
	if m.LifetimeSeconds < 100 || m.LifetimeSeconds >= 200 {
		t.Fatalf("precondition: deadline cell birth age = %ds, want inside [100,200) — "+
			"the lead window opens at 100s and the cap is at 200s", m.LifetimeSeconds)
	}
	if m.TTLRemainingSeconds < 25 {
		t.Fatalf("precondition: deadline cell has ttlRemaining=%ds (<25) — its BODY is approaching TTL too, "+
			"so the pre-existing TTL trigger would enqueue it and this arm would not discriminate", m.TTLRemainingSeconds)
	}

	before := store.Stats()
	reaped := store.reapPastMaxEntryAge()
	after := store.Stats()

	if reaped != 0 {
		t.Fatalf("no cell here is past the cap or suppressed, so none may be reaped; got %d", reaped)
	}

	// Two cells are legitimately enqueued: the deadline-only one (this change)
	// and the ttl-only one (the pre-existing condition). Asserting the TOTAL
	// pins that the new trigger ADDS a cell rather than re-routing an existing
	// one, and that it does not enqueue the "neither" cell.
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 3 {
		t.Errorf("proactive_refresh_total delta = %d, want 3 (deadline-only + ttl-only + both, never the 'neither' cell)", got)
	}

	// The marginal counter must attribute exactly ONE enqueue to the deadline
	// trigger. If it counted every enqueue in the window it would read 2 here,
	// and it could not answer "is this trigger doing anything".
	got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal
	if got != 1 {
		t.Errorf("remint_deadline_enqueued_total delta = %d, want exactly 1 — "+
			"only the deadline-only cell may be attributed to this trigger (the ttl-only cell was already "+
			"covered by the TTL condition, and double-counting it would make the counter unreadable)", got)
	}
	if got == 0 {
		t.Errorf("#506/#538 RED: a WARM cell inside its re-mint lead window with a FRESH body was NOT enqueued. " +
			"Nothing watches the birth clock, so no refresher terminal fires for it, so it is never re-minted " +
			"and it will reach the C5 cap under a customer — the cold navigation this trigger exists to prevent")
	}
}

// TestIssue506_DeadlineTriggerIsGatedOnWarmth pins the half of #506 that is
// deliberately NOT implemented. Ungating cold cells is the other half of the
// ruling, and it cannot be done here: a re-mint moves BornAt, and the reaper's
// only cold-reclaim handle is `pastMaxAge && !warm`, so re-minting a cold cell
// makes it permanently unreclaimable — the #496 defect this would re-open.
//
// This arm is what makes that a DECISION rather than an oversight: if someone
// later drops the warmth conjunct from the trigger, this goes red and names the
// reason, instead of the regression surfacing months later as unbounded growth.
func TestIssue506_DeadlineTriggerDoesNotEnqueueColdCells(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "100")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "200")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}

	// COLD, and getting there is NOT as simple as "do not set SeededAtBoot".
	//
	// #494 (open): putCoreLocked's fresh-INSERT branch stamps lastRead =
	// entry.CreatedAt on every NON-seeded insert, calling it a customer
	// cold-fill. So a cell Put with a fresh body is lastRead-WARM for one whole
	// TTL, and the first version of this arm — birth 150s, body 0s, no seed
	// flag — was enqueued exactly like the warm cell above. It asserted
	// !SeededAtBoot and called that cold, which is not the same claim.
	//
	// The consequence worth recording: "cold AND inside the lead window AND
	// body-fresh" is UNREACHABLE by construction while #494 stands, because the
	// only way to age out the inherited lastRead is to age the body past TTL.
	// So this arm ages BOTH (birth 150s, body 150s > TTL 100s): still inside the
	// lead window on the birth clock, genuinely cold on the read clock.
	now := time.Now()
	inputs := widgetInputs(gvrFlexes(), "krateo-system", "cold-in-window")
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"cell":"cold"}`),
		Inputs:    inputs,
		BornAt:    now.Add(-150 * time.Second),
		CreatedAt: now.Add(-150 * time.Second),
	})
	m, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatal("precondition: the cold cell must be resident")
	}
	if m.SeededAtBoot {
		t.Fatal("precondition: the cell must NOT be seeded, or it is warm by the seed signal")
	}
	// The precondition that actually matters, and the one the first version of
	// this arm was missing: COLD means warmLocked is false, i.e. not seeded AND
	// no read within TTL. Assert it on the read clock, not on the seed flag.
	if m.LastReadSeconds >= 0 && m.LastReadSeconds < 100 {
		t.Fatalf("precondition: cell has lastRead %ds ago with TTL=100s, so it is WARM, not cold "+
			"(#494 stamps lastRead=CreatedAt on a non-seeded insert) — this arm would test nothing",
			m.LastReadSeconds)
	}
	if m.LifetimeSeconds < 100 || m.LifetimeSeconds >= 200 {
		t.Fatalf("precondition: cold cell birth age = %ds, want inside the lead window [100,200)", m.LifetimeSeconds)
	}

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	// Assert the ENQUEUE, not the marginal counter. This cold cell has an aged
	// body (forced by #494, see above), so it is ttlApproaching too — and the
	// marginal counter is `deadlineApproaching && !ttlApproaching`, which stays 0
	// for it whether or not the warmth gate is present. Asserting that counter
	// here is what made the first version of this arm survive a mutation that
	// deleted the warm conjunct outright.
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 0 {
		t.Errorf("proactive_refresh_total delta = %d, want 0 — a COLD cell was ENQUEUED. "+
			"A re-mint resets BornAt, and the reaper's only cold-reclaim handle is `pastMaxAge && !warm`, "+
			"so that cell could never be reclaimed again (#496). Ungating cold cells needs a reclamation "+
			"handle that does not exist yet; it is not a one-conjunct change", got)
	}
	if got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal; got != 0 {
		t.Errorf("remint_deadline_enqueued_total delta = %d, want 0 — a COLD cell was attributed to the deadline "+
			"trigger. A re-mint resets BornAt, and the reaper's only cold-reclaim handle is `pastMaxAge && !warm`, "+
			"so that cell can never be reclaimed again (#496). Ungating cold cells needs a reclamation handle "+
			"that does not exist yet; it is not a one-conjunct change", got)
	}
}
