package cache

// issue506_deadline_trigger_test.go — #506 / #538: the reaper's DEADLINE-keyed
// proactive trigger.
//
// WHAT IS BEING FALSIFIED. Before this change the read-independent pass watched
// only the BODY clock (CreatedAt, via TTLRemainingSeconds < TTL/4). Nothing
// watched the BIRTH clock (BornAt) approaching maxEntryAge, and the refresher
// terminal is the ONLY writer that re-mints — so no enqueue means no re-mint,
// which means the cell reaches the hard C5 cap and cold-navigates under a
// customer.
//
// ── GEOMETRY, and why these exact numbers ──────────────────────────────────
//
// TTL = 100 s, maxEntryAge = 1000 s. Therefore:
//
//	remintLead()  = min(TTL, maxAge/2) = min(100, 500) = 100   <- the two terms
//	                                                              DIFFER (asserted)
//	deadline base = maxAge - lead      = 900
//	jitter spread = lead/2             = 50     -> threshold ∈ [900, 950)
//	refreshBelow  = TTL/4              = 25     -> ttlApproaching ⟺ body > 75 s
//
// **The first version of this arm used maxEntryAge = 200, where
// min(TTL, maxAge/2) = min(100, 100) — both terms of the min COINCIDE.** Every
// mutation substituting one term for the other was therefore invisible, and
// gate-573 showed `remintLead()` → `maxAge/2` surviving not just these arms but
// the whole module. At production bounds that substitution moves the threshold
// from 82,800 to 43,200 and fires 11 hours early, drifting from
// ReplaceIfGenRefresh's predicate — the drift this design claims cannot happen.
// A degenerate ratio cannot see the axis the central claim lives on.
//
// ── POPULATIONS, and why they are not all 1 ───────────────────────────────
//
// One cell per quadrant makes every predicate that selects exactly one quadrant
// read 1, so a mutation that INVERTS the attribution (or points it at the
// overlap) is indistinguishable from the correct one. gate-573 demonstrated two
// such green mutations. Distinct populations make each reading unique:
//
//	cell           n   birth   body   deadline  ttl   | correct inverted overlap no-guard
//	deadline-only  1   980 s     0    TRUE      FALSE |    1       -        -        1
//	ttl-only       2   100 s    80    FALSE     TRUE  |    -       2        -        -
//	both           3   980 s    80    TRUE      TRUE  |    -       -        3        3
//	neither        1   100 s     0    FALSE     FALSE |    -       -        -        -
//	below-lead     1   760 s     0    FALSE*    FALSE |    -       -        -        -
//	                                        totals ->      1       2        3        4
//
// So the marginal counter reads 1 only for the correct attribution.
// proactive_refresh_total reads 6 (1 + 2 + 3).
//
// *below-lead is the anti-degeneracy cell. 760 is ABOVE the highest threshold a
// `maxAge/2` lead could produce (500 + 250 jitter = 750) and BELOW the lowest the
// real lead produces (900), so it fires under that mutation and never under the
// real code — deterministically, for every key, with no dependence on the
// per-key jitter draw.
//
// Birth 980 is likewise chosen to clear the whole jittered band: >= 950 fires for
// ANY key, and < 1000 keeps the cell short of the cap so this stays the trigger
// axis and not the eviction axis (feedback: drive the real boundary, do not
// install the crossed state).

import (
	"fmt"
	"testing"
	"time"
)

// i506Store builds a store with explicit bounds and asserts the env took.
func i506Store(t *testing.T, ttlSec, maxAgeSec int) *ResolvedCacheStore {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", fmt.Sprint(ttlSec))
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", fmt.Sprint(maxAgeSec))
	// Inert summary goroutine: the reap is driven directly, so no fast ticker
	// leaks into the -race suite.
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	if got, want := store.maxEntryAge, time.Duration(maxAgeSec)*time.Second; got != want {
		t.Fatalf("precondition: maxEntryAge = %v, want %v — the env did not take", got, want)
	}
	if got, want := store.ttl, time.Duration(ttlSec)*time.Second; got != want {
		t.Fatalf("precondition: ttl = %v, want %v — the env did not take", got, want)
	}
	return store
}

// i506Put puts a resident, warm (SeededAtBoot), non-suppressed widgets cell with
// an explicit birth and body age. Warmth comes from the seed flag rather than a
// Get so the arm does not depend on read bookkeeping.
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
	store := i506Store(t, 100, 1000)

	// THE ANTI-DEGENERACY PRECONDITION. If the two terms of the min coincide,
	// every lead-substitution mutation is invisible and this file's central claim
	// is untested — which is exactly what happened in its first version.
	lead, half := store.remintLead(), store.maxEntryAge/2
	if lead != 100*time.Second {
		t.Fatalf("precondition: remintLead() = %v, want 100s", lead)
	}
	if lead == half {
		t.Fatalf("precondition: remintLead() (%v) == maxEntryAge/2 (%v) — the geometry is DEGENERATE: "+
			"substituting either term of min(TTL, maxAge/2) for the other would change nothing and "+
			"the threshold axis would be untested", lead, half)
	}

	deadlineKey := i506Put(t, store, "deadline-only", 980*time.Second, 0)
	for i := 0; i < 2; i++ {
		i506Put(t, store, fmt.Sprintf("ttl-only-%d", i), 100*time.Second, 80*time.Second)
	}
	for i := 0; i < 3; i++ {
		i506Put(t, store, fmt.Sprintf("both-%d", i), 980*time.Second, 80*time.Second)
	}
	i506Put(t, store, "neither", 100*time.Second, 0)
	i506Put(t, store, "below-lead", 760*time.Second, 0)

	m, ok := store.MetadataForKey(deadlineKey)
	if !ok {
		t.Fatal("precondition: the deadline cell must still be resident")
	}
	if m.LifetimeSeconds < 950 || m.LifetimeSeconds >= 1000 {
		t.Fatalf("precondition: deadline cell birth age = %ds, want [950,1000) — >=950 clears the whole "+
			"jittered threshold band for any key, <1000 keeps it short of the cap", m.LifetimeSeconds)
	}
	if m.TTLRemainingSeconds < 25 {
		t.Fatalf("precondition: deadline cell ttlRemaining=%ds (<25) — its BODY is approaching TTL too, "+
			"so the pre-existing TTL trigger would enqueue it and this arm would not discriminate",
			m.TTLRemainingSeconds)
	}

	before := store.Stats()
	reaped := store.reapPastMaxEntryAge()
	after := store.Stats()

	if reaped != 0 {
		t.Fatalf("no cell here is past the cap or suppressed, so none may be reaped; got %d", reaped)
	}
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 6 {
		t.Errorf("proactive_refresh_total delta = %d, want 6 (1 deadline-only + 2 ttl-only + 3 both; "+
			"never 'neither' or 'below-lead')", got)
	}

	got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal
	switch got {
	case 1:
		// correct
	case 2:
		t.Errorf("remint_deadline_enqueued_total = 2 — that is the ttl-only population, so the " +
			"attribution is INVERTED (counting ttlApproaching && !deadlineApproaching). The counter " +
			"whose stated purpose is 'is the deadline trigger doing anything' would be reporting " +
			"TTL-driven work")
	case 3:
		t.Errorf("remint_deadline_enqueued_total = 3 — that is the overlap population, so the " +
			"attribution is counting deadlineApproaching && ttlApproaching instead of the MARGINAL case")
	case 4:
		t.Errorf("remint_deadline_enqueued_total = 4 — deadline-only + both, so the `&& !ttlApproaching` " +
			"guard is gone and the counter double-counts work the TTL condition already covered")
	case 0:
		t.Errorf("#506/#538 RED: a WARM cell inside its re-mint lead window with a FRESH body was NOT " +
			"enqueued. Nothing watches the birth clock, so no refresher terminal fires for it, so it is " +
			"never re-minted and it will reach the C5 cap under a customer")
	default:
		t.Errorf("remint_deadline_enqueued_total = %d, want 1; the quadrant populations are 1/2/3/1/1 so "+
			"every attribution has a distinct signature and this matches none of them", got)
	}
}

// TestIssue506_LeadThresholdIsRemintLeadNotHalfMaxAge is the other half of the
// anti-degeneracy work: geometry where remintLead() == maxAge/2 rather than TTL,
// so substituting TTL for the lead becomes observable. Between this and the arm
// above, BOTH terms of min(TTL, maxAge/2) are pinned.
func TestIssue506_LeadThresholdIsRemintLeadNotHalfMaxAge(t *testing.T) {
	// TTL 1000s, maxAge 200s -> lead = min(1000, 100) = 100 (the maxAge/2 branch).
	store := i506Store(t, 1000, 200)
	if got := store.remintLead(); got != 100*time.Second {
		t.Fatalf("precondition: remintLead() = %v, want 100s (the maxAge/2 branch)", got)
	}
	if store.remintLead() == store.ttl {
		t.Fatalf("precondition: lead == ttl, so substituting ttl for the lead is invisible here")
	}

	// Birth 60s: BELOW the real threshold (200-100=100, jitter spread 50 so
	// [100,150)) and therefore silent. Under a `c.ttl` lead the threshold would be
	// 200-1000 = negative, firing for every cell including this one.
	i506Put(t, store, "below-lead-b", 60*time.Second, 0)

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	if got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal; got != 0 {
		t.Errorf("remint_deadline_enqueued_total delta = %d, want 0 — a cell whose birth age (60s) is "+
			"below the lead-window threshold (100s) was enqueued. If the threshold were derived from "+
			"c.ttl (1000s) instead of remintLead() (100s) it would be NEGATIVE and fire for everything, "+
			"producing resolves outside the window that ReplaceIfGenRefresh cannot re-mint", got)
	}
}

// TestIssue506_CapDisabledEnqueuesNothing pins the two bound guards
// (`maxAgeSec > 0 && leadSec > 0`) as load-bearing rather than belt-and-braces.
// With the cap disabled the predicate would degenerate to `LifetimeSeconds >= 0`,
// which is always true, and this pass would enqueue EVERY warm cell on every
// tick — the 0.30.185 refresh-everything amplification the scope discipline
// exists to prevent. gate-573 showed both guards surviving deletion against the
// whole internal/cache package.
func TestIssue506_CapDisabledEnqueuesNothing(t *testing.T) {
	store := i506Store(t, 100, 0) // maxEntryAge = 0 -> the cap is DISABLED
	if store.maxEntryAge != 0 {
		t.Fatalf("precondition: maxEntryAge must be 0 (cap disabled), got %v", store.maxEntryAge)
	}

	// WARM, body FRESH: 99 of 100s TTL remaining, so there is nothing to refresh
	// on the body axis either. Any enqueue here is the degenerate predicate.
	i506Put(t, store, "capoff-warm-fresh", 5000*time.Second, 0)

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 0 {
		t.Errorf("proactive_refresh_total delta = %d, want 0 — with maxEntryAge=0 the deadline predicate "+
			"degenerated to `LifetimeSeconds >= 0` (always true) and enqueued a warm, body-fresh cell. "+
			"That is refresh-everything with the cap off", got)
	}
	if got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal; got != 0 {
		t.Errorf("remint_deadline_enqueued_total delta = %d, want 0 with the cap disabled", got)
	}
}

// TestIssue506_DeadlineCounterAccumulatesAcrossPasses pins COUNTER semantics
// (Add) against GAUGE semantics (Store). A gauge would hold the last pass's
// marginal count and read 0 on most scrapes once a cohort has dispersed, which is
// a number whose zero reads as health. gate-573 showed `Add` -> `Store` surviving
// the whole package.
func TestIssue506_DeadlineCounterAccumulatesAcrossPasses(t *testing.T) {
	store := i506Store(t, 100, 1000)
	i506Put(t, store, "accum", 980*time.Second, 0)

	before := store.Stats().RemintDeadlineEnqueuedTotal
	store.reapPastMaxEntryAge()
	afterOne := store.Stats().RemintDeadlineEnqueuedTotal
	store.reapPastMaxEntryAge()
	afterTwo := store.Stats().RemintDeadlineEnqueuedTotal

	if afterOne-before != 1 {
		t.Fatalf("precondition: first pass must attribute 1 enqueue, got %d", afterOne-before)
	}
	if afterTwo-before != 2 {
		t.Errorf("remint_deadline_enqueued_total after two passes = %d, want 2 — the stat is being "+
			"STORED (gauge) rather than ADDED (counter). A gauge reads 0 on every pass that enqueues "+
			"nothing, so a scrape would almost always show 0 and could not be compared with remint_total",
			afterTwo-before)
	}
}

// TestIssue506_JitterStaysInsideTheWindowAndIsStable pins the #506/#538 burst
// dispersal: a per-key offset that is DETERMINISTIC (so a cell cannot re-roll out
// of its window pass after pass) and bounded to half the lead (so every cell
// keeps at least leadSec/2 of window for retries, and no enqueue can land before
// ReplaceIfGenRefresh would accept the re-mint).
func TestIssue506_JitterStaysInsideTheWindowAndIsStable(t *testing.T) {
	const lead = 3600 // production lead
	spread := int64(lead / 2)

	seen := map[int64]int{}
	for i := 0; i < 2000; i++ {
		key := ComputeKey(*widgetInputs(gvrFlexes(), "krateo-system", fmt.Sprintf("jit-%d", i)))
		j := deadlineJitterSeconds(key, lead)
		if j < 0 || j >= spread {
			t.Fatalf("jitter %d out of [0,%d) — an offset at or past half the lead would leave a cell too "+
				"little window to retry a dropped enqueue", j, spread)
		}
		if again := deadlineJitterSeconds(key, lead); again != j {
			t.Fatalf("jitter is not STABLE for a key: %d then %d. An unstable offset lets a cell slip its "+
				"window entirely, because each pass re-rolls whether it is eligible", j, again)
		}
		seen[j]++
	}
	// Dispersal is the whole point: a constant offset would be stable and in
	// range while leaving the cohort burst exactly as it was.
	if len(seen) < 100 {
		t.Errorf("only %d distinct offsets across 2000 keys — the cohort is not being dispersed, which is "+
			"the burst this exists to spread", len(seen))
	}
	// A lead too small to halve must disable jitter rather than divide by zero.
	for _, small := range []int64{0, 1, 2} {
		if j := deadlineJitterSeconds("abc", small); j != 0 {
			t.Errorf("deadlineJitterSeconds(lead=%d) = %d, want 0", small, j)
		}
	}
}

// TestIssue506_DeadlineTriggerDoesNotEnqueueColdCells pins the half of #506 that
// is deliberately NOT implemented: the trigger stays gated on WARM. Ungating cold
// cells is the other half of the ruling and cannot be done by dropping the
// conjunct — a re-mint moves BornAt, and the reaper's only cold-reclaim handle is
// `pastMaxAge && !warm`, so re-minting a cold cell makes it permanently
// unreclaimable (the #496 defect). That needs a reclamation handle which does not
// exist yet; filed as #572.
//
// Re-geometried to TTL=100 / maxEntryAge=1000 with the rest of this file.
func TestIssue506_DeadlineTriggerDoesNotEnqueueColdCells(t *testing.T) {
	store := i506Store(t, 100, 1000)

	// COLD, and getting there is NOT as simple as "do not set SeededAtBoot".
	//
	// #494 (open): putCoreLocked's fresh-INSERT branch stamps lastRead =
	// entry.CreatedAt on every NON-seeded insert, calling it a customer cold-fill.
	// So a cell Put with a fresh body is lastRead-WARM for one whole TTL. The
	// first version of this arm asserted !SeededAtBoot and called that cold — a
	// different claim — and it was enqueued exactly like the warm cell.
	//
	// ⚠ CORRECTED. An earlier version of this comment claimed "cold AND inside the
	// lead window AND body-fresh is UNREACHABLE by construction while #494 stands,
	// because the only way to age out the inherited lastRead is to age the body
	// past TTL". **That is FALSE**, and
	// TestIssue506_KeepwarmClearedCellIsColdWithAFreshBody below constructs it.
	// The door is a REPLACE: it refreshes the body while INHERITING lastRead, which
	// is precisely what the keepwarm sweep does. The unreachability was an artifact
	// of building the cell with a single Put, whose INSERT branch stamps lastRead —
	// the fact that a replace inherits it was already known and cited; the
	// inference from it was wrong.
	//
	// This arm still ages both (birth 980s, body 150s > TTL 100s) because it is
	// testing the warm gate, not reachability.
	now := time.Now()
	inputs := widgetInputs(gvrFlexes(), "krateo-system", "cold-in-window")
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"cell":"cold"}`),
		Inputs:    inputs,
		BornAt:    now.Add(-980 * time.Second),
		CreatedAt: now.Add(-150 * time.Second),
	})
	m, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatal("precondition: the cold cell must be resident")
	}
	if m.SeededAtBoot {
		t.Fatal("precondition: the cell must NOT be seeded, or it is warm by the seed signal")
	}
	// The precondition that actually matters: COLD means warmLocked is false, i.e.
	// not seeded AND no read within TTL. Assert it on the READ clock, not the seed
	// flag.
	if m.LastReadSeconds >= 0 && m.LastReadSeconds < 100 {
		t.Fatalf("precondition: cell has lastRead %ds ago with TTL=100s, so it is WARM, not cold "+
			"(#494 stamps lastRead=CreatedAt on a non-seeded insert) — this arm would test nothing",
			m.LastReadSeconds)
	}
	if m.LifetimeSeconds < 950 || m.LifetimeSeconds >= 1000 {
		t.Fatalf("precondition: cold cell birth age = %ds, want [950,1000) so it is inside the "+
			"jittered lead window for any key", m.LifetimeSeconds)
	}

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	// Assert the ENQUEUE, not the marginal counter. This cold cell necessarily has
	// an aged body (forced by #494 above) so it is ttlApproaching too, and the
	// marginal counter — deadlineApproaching && !ttlApproaching — stays 0 for it
	// whether or not the warmth gate is present. Asserting that counter is what
	// made the first version of this arm survive a mutation deleting the warm
	// conjunct outright.
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 0 {
		t.Errorf("proactive_refresh_total delta = %d, want 0 — a COLD cell was ENQUEUED. A re-mint "+
			"resets BornAt, and the reaper's only cold-reclaim handle is `pastMaxAge && !warm`, so that "+
			"cell could never be reclaimed again (#496). Ungating cold cells needs a reclamation handle "+
			"that does not exist yet (#572); it is not a one-conjunct change", got)
	}
}

// TestIssue506_SubSecondLeadDisablesTheTrigger isolates the `leadSec > 0` guard,
// which the cap-disabled arm above CANNOT reach: with maxEntryAge=0 the sibling
// guard `maxAgeSec > 0` already blocks the path, so deleting `leadSec > 0` alone
// stays invisible there. gate-573 found exactly that survivor.
//
// The reachable config that separates them: maxEntryAge = 1s makes maxAgeSec = 1
// (so the sibling guard PASSES) while remintLead() = min(TTL, maxAge/2) = 500ms
// truncates leadSec to 0. Without the guard the predicate becomes
// `LifetimeSeconds >= maxAgeSec - 0 + 0` = `>= 1`, i.e. every resident cell more
// than a second old — refresh-everything, on every tick.
func TestIssue506_SubSecondLeadDisablesTheTrigger(t *testing.T) {
	store := i506Store(t, 100, 1)
	if store.remintLead() >= time.Second {
		t.Fatalf("precondition: remintLead() = %v, want sub-second so leadSec truncates to 0", store.remintLead())
	}
	if int64(store.maxEntryAge.Seconds()) <= 0 {
		t.Fatalf("precondition: maxAgeSec must be > 0 so the SIBLING guard passes and this arm "+
			"isolates `leadSec > 0`; got %v", store.maxEntryAge)
	}

	// WARM via the seed flag, body FRESH (nothing to refresh on the TTL axis).
	// Past the 1s cap on the birth clock, so the reaper keeps it (C3, warm) and it
	// remains a candidate for refresh candidacy.
	i506Put(t, store, "subsecond-lead", 5*time.Second, 0)

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	// The DENOMINATOR must be silent here too, and for the same guard. If it
	// counted while the trigger is correctly disabled, the documented reading
	// "eligible > 0 with enqueued == 0 is a DEFECT" would fire as a FALSE ALARM on
	// every pod with a sub-second lead.
	if got := after.WarmInLeadWindow; got != 0 {
		t.Errorf("warm_in_lead_window = %d, want 0 — leadSec is 0, so there is no lead window to be "+
			"inside. Reporting eligible cells while the trigger is correctly disabled turns the "+
			"`eligible > 0 and enqueued == 0 is a defect` reading into a false alarm", got)
	}
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 0 {
		t.Errorf("proactive_refresh_total delta = %d, want 0 — remintLead() is sub-second so leadSec is 0, "+
			"and without the `leadSec > 0` guard the deadline predicate degenerates to "+
			"`LifetimeSeconds >= maxAgeSec` with no lead at all, enqueuing every cell past a 1s cap on "+
			"every tick", got)
	}
}

// TestIssue506_KeepwarmClearedCellIsColdWithAFreshBody is the #538 gap, built
// through real writes, and it is the arm that pins the counter's ATTRIBUTION SITE.
//
// THE SHAPE, and why it matters. A dep-quiet keepwarm cell that has been
// refresher-re-Put once is:
//
//	birth OLD   (BornAt inherited through every non-re-mint write)
//	body FRESH  (the keepwarm re-Put refreshes CreatedAt)
//	read STALE  (a REPLACE inherits lastRead; only an INSERT stamps it)
//	not seeded  (the refresher terminal builds entries without SeededAtBoot, and
//	             seedProvenanceForMode then carries that false forward forever)
//
// so `warmLocked` is false and NEITHER trigger fires: ttlApproaching is false
// because the body is fresh, and deadlineApproaching is gated on warm. The cell
// sits in its lead window un-re-minted until the cap, then cold-navigates. That
// is #538, which #573 claimed to close and did not — reopened.
//
// It also disproves the "unreachable by construction" claim this file used to
// carry: the REPLACE is the door.
//
// WHAT IT GUARDS. The marginal counter must be incremented INSIDE the enqueue
// gate. Hoisting it above turns "enqueues produced" into "cells in the window" —
// and this cell is the proof that those differ, because it is in the window and
// is never enqueued. Under that mutation the counter reports an enqueue that did
// not happen, which inverts the intent-vs-effect reading its own field doc
// prescribes. No other arm in the tree notices.
func TestIssue506_KeepwarmClearedCellIsColdWithAFreshBody(t *testing.T) {
	store := i506Store(t, 100, 1000)

	inputs := widgetInputs(gvrFlexes(), "krateo-system", "keepwarm-cleared")
	key := ComputeKey(*inputs)
	now := time.Now()

	// INSERT, old in both clocks. The insert branch stamps lastRead = CreatedAt,
	// so lastRead starts 980s stale.
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"cell":"kw-insert"}`),
		Inputs:    inputs,
		BornAt:    now.Add(-980 * time.Second),
		CreatedAt: now.Add(-980 * time.Second),
	})
	// REPLACE, standing in for the keepwarm sweep: a fresh body on a resident key.
	// BornAt and lastRead are both inherited; only CreatedAt moves.
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"cell":"kw-replace"}`),
		Inputs:    inputs,
		CreatedAt: now,
	})

	m, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatal("precondition: the cell must be resident after the replace")
	}
	// All three conditions at once — the state the old comment called impossible.
	if m.SeededAtBoot {
		t.Fatalf("precondition: not seeded, got seeded")
	}
	if m.LifetimeSeconds < 950 || m.LifetimeSeconds >= 1000 {
		t.Fatalf("precondition: birth age = %ds, want [950,1000) — the replace must INHERIT BornAt; "+
			"if it reset it this arm is not testing the lead window", m.LifetimeSeconds)
	}
	if m.TTLRemainingSeconds < 25 {
		t.Fatalf("precondition: ttlRemaining = %ds, want >=25 (a FRESH body) — the replace must move "+
			"CreatedAt, or this is just the body-expired case", m.TTLRemainingSeconds)
	}
	if m.LastReadSeconds >= 0 && m.LastReadSeconds < 100 {
		t.Fatalf("precondition: lastRead %ds ago with TTL=100s, so the cell is WARM — the replace must "+
			"INHERIT lastRead rather than restamp it, which is the whole mechanism", m.LastReadSeconds)
	}
	t.Logf("#538 shape reached: birth=%ds ttlRemaining=%ds lastRead=%ds seeded=%v "+
		"(cold, in the lead window, body fresh — all three at once)",
		m.LifetimeSeconds, m.TTLRemainingSeconds, m.LastReadSeconds, m.SeededAtBoot)

	before := store.Stats()
	store.reapPastMaxEntryAge()
	after := store.Stats()

	// The #538 gap itself: nothing enqueues this cell. Asserted so the gap is
	// RECORDED rather than rediscovered — when #538 is really fixed this arm
	// changes deliberately, with the fix.
	if got := after.ProactiveRefreshTotal - before.ProactiveRefreshTotal; got != 0 {
		t.Errorf("proactive_refresh_total delta = %d, want 0 — the warm gate currently excludes this "+
			"cell. A non-zero here means #538 was fixed and this arm must be updated alongside the fix", got)
	}
	// The attribution-site guard.
	if got := after.RemintDeadlineEnqueuedTotal - before.RemintDeadlineEnqueuedTotal; got != 0 {
		t.Errorf("remint_deadline_enqueued_total delta = %d, want 0 — this cell IS inside the lead window "+
			"but was NOT enqueued, so attributing it means the counter is being incremented OUTSIDE the "+
			"enqueue gate. It would then report 'cells in the window' while its field doc says it reports "+
			"'enqueues produced' (intent), making the intent-vs-effect comparison against remint_total "+
			"meaningless", got)
	}
}

// TestIssue506_WarmInLeadWindowIsTheJitterFreeWarmPopulation arms the
// denominator. It exists because a zero on remint_deadline_enqueued_total is
// unreadable without it: oldest_warm_born_age_seconds is a MAX, so it says at
// least one warm cell is in the band and never how many.
//
// Two properties, each with its own failure mode:
//
//	WARM-ONLY  — a cold cell in the window must NOT count, or the denominator
//	             inflates and `eligible > 0 ∧ enqueued == 0` stops meaning
//	             "eligible cells are not being enqueued".
//	JITTER-FREE — the jitter decides WHICH pass inside the window a cell fires
//	             on, so applying it here would report this pass's slice instead
//	             of the population and the denominator would undercount.
func TestIssue506_WarmInLeadWindowIsTheJitterFreeWarmPopulation(t *testing.T) {
	store := i506Store(t, 100, 1000)
	const base = 900 // maxAge 1000 - lead 100

	// (a) WARM, deep in the window: counts under every variant.
	i506Put(t, store, "den-warm-deep", 980*time.Second, 0)

	// (b) COLD, deep in the window, via the keepwarm REPLACE shape. Must NOT count.
	coldIn := widgetInputs(gvrFlexes(), "krateo-system", "den-cold-in")
	coldKey := ComputeKey(*coldIn)
	now := time.Now()
	store.Put(coldKey, &ResolvedEntry{RawJSON: []byte(`1`), Inputs: coldIn,
		BornAt: now.Add(-980 * time.Second), CreatedAt: now.Add(-980 * time.Second)})
	store.Put(coldKey, &ResolvedEntry{RawJSON: []byte(`2`), Inputs: coldIn, CreatedAt: now})
	if m, _ := store.MetadataForKey(coldKey); m.LastReadSeconds >= 0 && m.LastReadSeconds < 100 {
		t.Fatalf("precondition: the cold cell is warm (lastRead %ds) — the replace restamped it", m.LastReadSeconds)
	}

	// (c) WARM, inside the jitter-free window but BELOW its own jittered
	// threshold. Counts here; would NOT count if the jitter were applied. Found
	// by asking the real function for an offset, so this is deterministic rather
	// than hoping a key lands well.
	lead := int64(store.remintLead().Seconds())
	var jitterKey string
	var jit int64
	for i := 0; i < 500 && jit < 2; i++ {
		name := fmt.Sprintf("den-jit-%d", i)
		k := ComputeKey(*widgetInputs(gvrFlexes(), "krateo-system", name))
		if j := deadlineJitterSeconds(k, lead); j >= 2 {
			jitterKey, jit = name, j
			break
		}
	}
	if jitterKey == "" {
		t.Skip("no key with a jitter offset >= 2 in 500 tries — cannot place the boundary cell")
	}
	// base + jit - 1: at or above the jitter-free threshold, strictly below the
	// jittered one.
	i506Put(t, store, jitterKey, time.Duration(base+jit-1)*time.Second, 0)
	t.Logf("boundary cell: jitter=%ds, birth=%ds (jitter-free threshold %d, jittered %d)",
		jit, base+jit-1, base, base+jit)

	store.reapPastMaxEntryAge()
	got := store.Stats().WarmInLeadWindow

	if got != 2 {
		t.Errorf("warm_in_lead_window = %d, want 2 (the warm-deep cell + the boundary cell; "+
			"NOT the cold one). %d means the COLD in-window cell is being counted, which inflates the "+
			"denominator and breaks the `eligible > 0 and enqueued == 0 is a defect` reading; "+
			"%d means the per-key JITTER is being applied, which reports this pass's slice instead of "+
			"the population and undercounts", got, 3, 1)
	}
}
