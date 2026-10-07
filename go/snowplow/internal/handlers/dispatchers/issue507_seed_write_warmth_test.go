// issue507_seed_write_warmth_test.go — #507: a seed write must carry boot-seed
// PROVENANCE, never MANUFACTURE warmth.
//
// THE DEFECT. Both seed-cell literals (seedOneRestaction's tail, seedOneWidget)
// hardcoded `SeededAtBoot: true`, mode-INDEPENDENTLY, and the keepwarm sweep
// re-Puts every keepwarm-scoped cell whose body is older than TTL/4 through that
// same code. SeededAtBoot is also half of THE warm predicate (cache warmLocked =
// SeededAtBoot || lastRead-within-TTL), so each sweep pass handed warmth to cells
// no customer had ever read — and #496's re-mint gate, which refuses to re-mint a
// COLD in-window cell precisely so the reaper can still reclaim it, never fired
// for that set. Those cells were re-minted indefinitely: the #259/#191 24h bound
// was gone for them. The #376 lesson surviving in a flag — internal READS were
// stopped from faking warmth via lastRead (GetNoTouch), an internal WRITE could
// still confer it.
//
// WHY THESE ARMS DRIVE seedOneWidget AND NOT seedTerminalPut. The value under
// test is decided where the seed cell is BUILT. An arm that handed
// seedTerminalPut an entry of its own would be asserting against its own
// fixture; these arms drive the real primitive so the production literal is what
// answers. Every arm here compiles and runs on the parent SHA unchanged, which is
// what let the five RED arms be watched going red there.
//
// RED on origin/main (79a6e5a0):
//   - TestIssue507_KeepwarmSweepLeavesANeverReadCellCold
//   - TestIssue507_KeepwarmFirstFillIsNotSeedAttributed
//   - TestIssue507_GuardCaptureReadsProvenanceWithGetNoTouch
//   - TestIssue507_TheColdCellStaysReclaimableAfterTheSweep
//   - TestIssue507_NoSeedCellLiteralHardcodesSeedProvenance (structural; the only
//     cover for the RESTACTIONS literal, which needs an apiserver fixture to
//     drive behaviourally)
//
// GREEN on both (anti-overshoot: the fix must not "work" by hardcoding false):
//   - TestIssue507_SweepPreservesGenuineBootProvenance
//   - TestIssue507_BootSeedStillStampsProvenance

package dispatchers

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// i507Bounds sets the REAL store bounds for one arm. Must run BEFORE
// a1BuildTwoTenantWatcher (which resets the store; it is rebuilt lazily from the
// env). The reaper tick is pushed out of the run: these arms drive the max-age
// walk explicitly with ReapPastMaxEntryAgeForTest so a background tick can never
// evict the cell under test between the act and the assertion.
func i507Bounds(t *testing.T, ttlS, maxAgeS int) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", fmt.Sprint(ttlS))
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", fmt.Sprint(maxAgeS))
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
}

// i507Fixture is the shared widget-seed fixture: the live store, the production
// key/handle/inputs for the apiRef'd widget under a co-bound cohort, and the seed
// ctx. Same fixture the #258/#378 dispatcher arms use (reseed_strategy_test.go),
// so the seed reaches its terminal Put deterministically.
type i507Fixture struct {
	store   *cache.ResolvedCacheStore
	entry   navWidgetEntry
	ctx     context.Context
	key     string
	handle  cacheHandle
	inputs  *cache.ResolvedKeyInputs
	hits0   uint64
	seeded0 uint64
}

func i507Setup(t *testing.T, ttlS, maxAgeS int) i507Fixture {
	t.Helper()
	i507Bounds(t, ttlS, maxAgeS)
	a1BuildTwoTenantWatcher(t)
	stubWidgetResolve(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled")
	}
	e := reseedWidgetEntry()
	ctx := reseedSeedCtx()
	key, handle, inputs := reseedWidgetKey(t, ctx, e)
	return i507Fixture{
		store: store, entry: e, ctx: ctx, key: key, handle: handle, inputs: inputs,
		hits0: store.Stats().HitTotal, seeded0: hitsSeedAttributable.Load(),
	}
}

// coldResidentUnseeded builds, the way production reaches it and NOT by hand, the
// exact cell the defect needs: resident, body fresh enough to serve but old
// enough that the sweep will NOT age-skip it, and COLD by the store's own
// predicate — unseeded with a ZERO lastRead.
//
//	(1) the boot seed INSERTS it: SeededAtBoot=true takes putCoreLocked's
//	    insert branch, which leaves lastRead ZERO (it stamps lastRead only on a
//	    non-seeded insert — the cold-fill assumption that is open #494);
//	(2) a refresher re-Put REPLACES it in place: the replace branch inherits
//	    BornAt and recentHitters but NOT SeededAtBoot, so the flag clears, and it
//	    does not stamp lastRead, so the zero is inherited.
//
// That is the state #505 argued made seeded cells safe ("a boot-seeded cell loses
// seed warmth at its first refresh"), and it is the state the sweep write then
// un-did.
func (f i507Fixture) coldResidentUnseeded(t *testing.T, bodyAge time.Duration) time.Time {
	t.Helper()
	f.handle.Put(f.key, &cache.ResolvedEntry{RawJSON: []byte(`{"seed":1}`), Inputs: f.inputs, SeededAtBoot: true})
	born, ok := f.handle.GetNoTouch(f.key)
	if !ok {
		t.Fatal("SETUP: the boot-seed insert left no resident cell")
	}
	if !f.store.ReplaceIfGen(context.Background(), f.key,
		&cache.ResolvedEntry{RawJSON: []byte(`{"refreshed":1}`), Inputs: f.inputs, CreatedAt: time.Now().Add(-bodyAge)},
		f.store.CaptureGen(f.key)) {
		t.Fatal("SETUP: the refresher re-Put must be ACCEPTED on a live cell")
	}
	after, ok := f.handle.GetNoTouch(f.key)
	if !ok {
		t.Fatal("SETUP: the refresher re-Put removed the cell")
	}
	if after.SeededAtBoot {
		t.Fatal("SETUP: a refresher re-Put must clear SeededAtBoot — this arm needs an UNSEEDED resident cell")
	}
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 0 {
		t.Fatalf("SETUP: no cell may be seed-warm before the sweep; warm_seeded=%d", s.WarmSeeded)
	}
	return born.BornAt
}

// sweepOnce runs the REAL keepwarm sweep primitive over the fixture's target and
// returns the cell it left behind.
func (f i507Fixture) sweepOnce(t *testing.T, mode seedScopeMode) *cache.ResolvedEntry {
	t.Helper()
	if err := seedOneWidget(f.ctx, f.entry, h1NS, mode); err != nil {
		t.Fatalf("seedOneWidget(%s): %v", mode, err)
	}
	got, ok := f.handle.GetNoTouch(f.key)
	if !ok {
		t.Fatalf("SETUP: the %s seed left no resident cell", mode)
	}
	return got
}

// assertNoCustomerRead pins the claim every arm here rests on: nothing in the
// sweep is a customer read. hit_total is the store's own customer-hit counter and
// GetNoTouch is metric-neutral, so a move here would mean the arm proved
// something about a cell a customer had in fact read.
func (f i507Fixture) assertNoCustomerRead(t *testing.T) {
	t.Helper()
	if got := f.store.Stats().HitTotal; got != f.hits0 {
		t.Fatalf("SETUP: hit_total moved %d → %d — a CUSTOMER read happened, so this arm is no longer about a never-read cell",
			f.hits0, got)
	}
}

// assertNoSeedAttribution pins the OTHER flag reader for a never-served cell.
//
// The two readers have DIFFERENT shapes and the difference matters when reading
// these arms: warm_seeded is a GAUGE recomputed from residency on every max-age
// walk (warmSeededGauge.Store at resolved.go reapPastMaxEntryAge), so it is
// expected to DROP for the population this fix stops warming — that drop IS the
// fix, which is why the arms assert it per population (0 for a never-read cell,
// 1 for a genuinely boot-seeded one) rather than asserting a total does not move.
// hits_seed_attributable is a MONOTONIC counter with exactly one write site
// (l1_lookup_metrics.go recordL1Lookup, under `hit && seededAtBoot`), not derived
// from the gauge, so for a cell nobody has served it must not move in EITHER
// direction: there is no hit to attribute, and the fix must not quietly change
// what an unserved cell reports.
func (f i507Fixture) assertNoSeedAttribution(t *testing.T) {
	t.Helper()
	if got := hitsSeedAttributable.Load(); got != f.seeded0 {
		t.Errorf("hits_seed_attributable moved %d → %d for a cell that was never SERVED. It is bumped only on a "+
			"HIT (recordL1Lookup), so nothing here may move it in either direction", f.seeded0, got)
	}
}

// TestIssue507_KeepwarmSweepLeavesANeverReadCellCold — THE falsifier.
//
// The keepwarm sweep re-Puts a RESIDENT, never-read, unseeded cell. The cell must
// come back UNSEEDED: provenance is carried through (there is none), not minted.
//
// RED on origin/main: the literal's `SeededAtBoot: true` is written, the cell
// becomes seed-warm (warm_seeded 0 → 1) and #496's gate can no longer reclaim it
// — with zero customer reads at any point.
func TestIssue507_KeepwarmSweepLeavesANeverReadCellCold(t *testing.T) {
	f := i507Setup(t, 60, 86400) // body TTL 60s → keepwarmAgeSkipThreshold = TTL/4 = 15s
	f.coldResidentUnseeded(t, 30*time.Second)

	got := f.sweepOnce(t, seedModeKeepwarm)
	if string(got.RawJSON) == `{"refreshed":1}` {
		t.Fatal("SETUP: the sweep did not re-Put the cell (age-skipped?) — nothing was under test")
	}
	if got.SeededAtBoot {
		t.Errorf("#507 RED: the keepwarm sweep wrote SeededAtBoot=true onto a cell NO CUSTOMER HAS EVER READ. "+
			"That flag is half of warmLocked, so the cell now tests WARM, #496's re-mint gate stops refusing it and "+
			"the reaper's cold-evict can never reclaim it: the #259/#191 24h bound is gone for the whole "+
			"keepwarm-scoped set, re-conferred every sweep pass. Carry the resident cell's provenance through "+
			"instead of stamping it (key_hash=%s)", f.key)
	}
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 0 {
		t.Errorf("#507 RED: warm_seeded=%d after the sweep, want 0 — only the seed primitives write SeededAtBoot, "+
			"so this gauge counts exactly the cells an internal write made seed-warm", s.WarmSeeded)
	}
	f.assertNoSeedAttribution(t)
	f.assertNoCustomerRead(t)
}

// TestIssue507_KeepwarmFirstFillIsNotSeedAttributed — the sweep's INSERT half.
//
// The sweep also re-fills cells whose TTL lapsed between passes (that is why its
// terminal write is PutIfGen and not ReplaceIfGen). An ABSENT cell carries no
// provenance, so a keepwarm first fill must be unseeded too — otherwise the sweep
// mints seed warmth on a cell that never saw the boot seed at all.
//
// #494 (OPEN, NOT closed by #507): such an insert now takes putCoreLocked's
// fresh-insert branch, which stamps lastRead because it calls every non-seeded
// insert a customer cold-fill. The cell is therefore lastRead-warm for ONE TTL
// instead of seed-warm indefinitely — strictly narrower than what it replaces,
// but the same class with a different carrier. This arm asserts the #507 half
// (the seed flag) and deliberately does not assert the lastRead half, which is
// #494's to fix.
//
// RED on origin/main: a first fill is written seed-attributed.
func TestIssue507_KeepwarmFirstFillIsNotSeedAttributed(t *testing.T) {
	f := i507Setup(t, 60, 86400)
	if _, ok := f.handle.GetNoTouch(f.key); ok {
		t.Fatal("SETUP: the cell must be ABSENT — this arm is the first-fill case")
	}

	got := f.sweepOnce(t, seedModeKeepwarm)
	if got.SeededAtBoot {
		t.Errorf("#507 RED: a keepwarm FIRST FILL was written SeededAtBoot=true. The boot seed never touched this "+
			"cell; an absent cell carries no provenance, so the sweep is minting warmth, not preserving it (key_hash=%s)", f.key)
	}
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 0 {
		t.Errorf("#507 RED: warm_seeded=%d after a keepwarm first fill, want 0", s.WarmSeeded)
	}
	f.assertNoSeedAttribution(t)
	f.assertNoCustomerRead(t)
}

// TestIssue507_SweepPreservesGenuineBootProvenance — the ANTI-OVERSHOOT arm.
//
// Carrying provenance through is not the same as clearing it. A cell the BOOT
// seed warmed and that no refresher has replaced yet is genuinely seed-warm; the
// sweep re-Putting its body must keep that attribution, or #130 F3's two readers
// (hits_seed_attributable, the warm_seeded gauge) start under-reporting the boot
// seed and "did the seed warm this cell" stops being answerable.
//
// GREEN on origin/main (it hardcodes true) and GREEN on the fix. It REDs on a
// fix-by-hardcoding-false.
func TestIssue507_SweepPreservesGenuineBootProvenance(t *testing.T) {
	f := i507Setup(t, 60, 86400)
	// A genuine boot seed: SeededAtBoot=true, lastRead zero, body old enough that
	// the sweep will not age-skip it. No refresher has replaced it.
	f.handle.Put(f.key, &cache.ResolvedEntry{
		RawJSON: []byte(`{"seed":1}`), Inputs: f.inputs, SeededAtBoot: true,
		CreatedAt: time.Now().Add(-30 * time.Second),
	})
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 1 {
		t.Fatalf("SETUP: the boot-seeded cell must be seed-warm before the sweep; warm_seeded=%d", s.WarmSeeded)
	}

	got := f.sweepOnce(t, seedModeKeepwarm)
	if string(got.RawJSON) == `{"seed":1}` {
		t.Fatal("SETUP: the sweep did not re-Put the cell (age-skipped?) — nothing was under test")
	}
	if !got.SeededAtBoot {
		t.Errorf("#507 OVERSHOOT: the sweep re-Put a genuinely BOOT-SEEDED cell and LOST its provenance. " +
			"The flag must be carried through from the resident entry, not cleared: hits_seed_attributable and " +
			"warm_seeded are the #130 F3 observables and would under-report every boot")
	}
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 1 {
		t.Errorf("#507 OVERSHOOT: warm_seeded=%d after the sweep re-Put of a boot-seeded cell, want 1", s.WarmSeeded)
	}
	// The OTHER reader of the flag: a hit on this cell must still attribute to the
	// seed. Driven through the production emitter, with the flag the sweep left.
	emitResolvedCacheLookup(nil, "widgets", h1WidgetGVR.String(), f.key, true, got.SeededAtBoot, len(got.RawJSON))
	if d := hitsSeedAttributable.Load() - f.seeded0; d != 1 {
		t.Errorf("#507 OVERSHOOT: hits_seed_attributable Δ=%d, want 1 — a hit on a cell the boot seed warmed must "+
			"still read hit_source:\"seed\"", d)
	}
	f.assertNoCustomerRead(t)
}

// TestIssue507_BootSeedStillStampsProvenance — the second ANTI-OVERSHOOT arm.
//
// The boot seed IS the provenance, so a boot-mode seed of an absent cell still
// stamps it. That is load-bearing and not just a label: a boot-seeded cell rides
// seed warmth until real traffic arrives, which is what keeps the reaper and the
// refresher treating the prewarmed set as warm before anyone has browsed.
//
// GREEN on both trees. It REDs if the fix carried provenance through for the boot
// mode as well (the cell is absent, so the carried value would be false).
func TestIssue507_BootSeedStillStampsProvenance(t *testing.T) {
	f := i507Setup(t, 60, 86400)
	if _, ok := f.handle.GetNoTouch(f.key); ok {
		t.Fatal("SETUP: the cell must be ABSENT — a boot seed first-fills it")
	}

	got := f.sweepOnce(t, seedModeBoot)
	if !got.SeededAtBoot {
		t.Errorf("#507 OVERSHOOT: the BOOT seed must stamp SeededAtBoot — it is the one writer that genuinely is " +
			"the boot seed. Without it the #130 F3 observables read zero on every boot and the prewarmed set is " +
			"cold to the reaper and the refresher before any customer has browsed")
	}
	f.store.ReapPastMaxEntryAgeForTest()
	if s := f.store.Stats(); s.WarmSeeded != 1 {
		t.Errorf("#507 OVERSHOOT: warm_seeded=%d after a boot seed, want 1", s.WarmSeeded)
	}
	f.assertNoSeedAttribution(t)
	f.assertNoCustomerRead(t)
}

// TestIssue507_GuardCaptureReadsProvenanceWithGetNoTouch — the WIRING arm, and
// the #376 lesson applied to the fix itself.
//
// Reading the resident cell's provenance is an INTERNAL read. It must go through
// GetNoTouch (no MoveToFront, no lastRead stamp, no hit_total — metric-neutral on
// the miss side too), never the warmth-stamping Get: a fix that read the flag
// with Get would stop manufacturing SEED warmth by manufacturing lastRead warmth
// instead, which is the very trade #376 exists to forbid.
//
// RED on origin/main: the guard capture reads NOTHING there (the flag was a
// constant), so no GetNoTouch is recorded.
func TestIssue507_GuardCaptureReadsProvenanceWithGetNoTouch(t *testing.T) {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "i507", Name: "wiring"}
	key := cache.ComputeKey(in)

	for _, mode := range []seedScopeMode{seedModeKeepwarm, seedModeGVRDiscovered, seedModeRBACShift} {
		h := &getRecordingHandle{}
		_ = seedTerminalGuardFor(mode, h, key)
		if !h.sawGetNoTouch(key) {
			t.Errorf("#507 RED: seedTerminalGuardFor(%s) must read the resident cell's provenance so the terminal "+
				"write can CARRY it instead of stamping it; noTouchKeys=%v", mode, h.noTouchKeys)
		}
		if h.sawGetTouch(key) {
			t.Errorf("#376/#507: seedTerminalGuardFor(%s) read the provenance with the warmth-stamping Get. An "+
				"internal read must not fake lastRead warmth — use GetNoTouch; getKeys=%v", mode, h.getKeys)
		}
	}

	// The boot mode needs no read at all (it IS the provenance), and above all it
	// must not take the touching one.
	hBoot := &getRecordingHandle{}
	_ = seedTerminalGuardFor(seedModeBoot, hBoot, key)
	if hBoot.sawGetTouch(key) {
		t.Errorf("#376/#507: the boot-mode guard capture must not use the warmth-stamping Get; getKeys=%v", hBoot.getKeys)
	}
}

// TestIssue507_TheColdCellStaysReclaimableAfterTheSweep — the CONSEQUENCE arm:
// the gate's own probe (PR #505 finding 2) turned into a guard.
//
// It runs the published probe's three passes against the REAL seed primitive at
// the production TTL:maxAge INVERSION, so remintLead() = TTL and a cell can be
// read-COLD while its body is still fresh — the geometry in which #496's gate is
// reachable at all:
//
//	pass1  the gate REFUSES the re-mint of the cold in-window cell (BornAt held,
//	       remint_refused_cold +1) — the gate working, and the arm's precondition.
//	sweep  the real keepwarm seedOneWidget re-Puts the cell.
//	pass2  the same refresh must STILL be refused (BornAt held, refused_cold +1).
//
// RED on origin/main: after the sweep the cell is seed-warm, pass2 is ACCEPTED,
// BornAt advances and refused_cold Δ=0 — the cell has been made immortal with
// zero customer reads, which is precisely the half of #259 that #496 exists to
// close.
//
// Every age here comes from REAL elapse. The cell is kept resident across the
// wait by ordinary ReplaceIfGen refreshes (never ReplaceIfGenRefresh, which is
// the one write that re-mints) because the body TTL is far below the cap — which
// is itself the production shape.
func TestIssue507_TheColdCellStaysReclaimableAfterTheSweep(t *testing.T) {
	const ttl, maxAge = 4 * time.Second, 12 * time.Second // lead = min(TTL, maxAge/2) = TTL → window [8s, 12s)
	windowStart := maxAge - ttl
	f := i507Setup(t, int(ttl.Seconds()), int(maxAge.Seconds()))
	born := f.coldResidentUnseeded(t, ttl/2) // body "aged" TTL/2: past the TTL/4 age-skip, still live

	// Hold the cell resident until its BornAt age reaches the lead window. Poll to
	// the edge rather than sleeping a computed duration (#501): machine load then
	// shifts WHEN this returns, never WHETHER the arm is in the right state.
	for time.Since(born) < windowStart {
		time.Sleep(200 * time.Millisecond)
		if time.Since(born) >= windowStart {
			break
		}
		if !f.store.ReplaceIfGen(context.Background(), f.key,
			&cache.ResolvedEntry{RawJSON: []byte(`{"refreshed":1}`), Inputs: f.inputs, CreatedAt: time.Now().Add(-ttl / 2)},
			f.store.CaptureGen(f.key)) {
			t.Fatalf("SETUP: the keep-resident refresh was refused at BornAt age %v", time.Since(born))
		}
	}
	i507MustBeInWindow(t, born, windowStart, maxAge, "before pass1")

	// pass1 — the gate REFUSES the cold cell. This is #496 working, and it is what
	// makes the post-sweep comparison meaningful.
	refused0 := f.store.Stats().RemintRefusedColdTotal
	if !f.store.ReplaceIfGenRefresh(context.Background(), f.key,
		&cache.ResolvedEntry{RawJSON: []byte(`{"pass1":1}`), Inputs: f.inputs, CreatedAt: time.Now().Add(-ttl / 2)},
		f.store.CaptureGen(f.key)) {
		t.Fatal("SETUP: the refresh WRITE must be accepted on a live cell — only the re-mint is gated")
	}
	e1, ok := f.handle.GetNoTouch(f.key)
	if !ok {
		t.Fatal("SETUP: the accepted refresh must replace the cell, not remove it")
	}
	reminted1 := !e1.BornAt.Equal(born)
	refused1 := f.store.Stats().RemintRefusedColdTotal
	t.Logf("pass1: re-minted=%v seeded_now=%v refused_cold Δ=%d", reminted1, e1.SeededAtBoot, refused1-refused0)
	if reminted1 {
		t.Fatalf("SETUP: #496's gate did not refuse the COLD in-window cell (BornAt %v → %v) — the gate under "+
			"discussion is not in this tree, so the sweep's effect on it cannot be measured", born, e1.BornAt)
	}
	if refused1 != refused0+1 {
		t.Fatalf("SETUP: remint_refused_cold_total did not move (%d → %d); the refusal must be observable or this "+
			"arm cannot tell a working gate from an unreached window", refused0, refused1)
	}

	// THE SWEEP WRITE — the real keepwarm primitive, nothing replayed.
	i507MustBeInWindow(t, born, windowStart, maxAge, "before the sweep")
	got := f.sweepOnce(t, seedModeKeepwarm)
	if !got.BornAt.Equal(born) {
		t.Fatalf("SETUP: the sweep INSERTED instead of replacing (BornAt %v → %v) — the body lapsed during the "+
			"run, so this is an environment failure, not the #507 defect", born, got.BornAt)
	}
	t.Logf("after the sweep re-Put (PutIfGen, keepwarm): seeded_now=%v  (no customer has EVER read this cell)", got.SeededAtBoot)

	// pass2 — the same refresh on the same cell. It must still be refused.
	i507MustBeInWindow(t, born, windowStart, maxAge, "before pass2")
	if !f.store.ReplaceIfGenRefresh(context.Background(), f.key,
		&cache.ResolvedEntry{RawJSON: []byte(`{"pass2":1}`), Inputs: f.inputs, CreatedAt: time.Now().Add(-ttl / 2)},
		f.store.CaptureGen(f.key)) {
		t.Fatal("SETUP: the pass2 refresh WRITE must be accepted on a live cell")
	}
	e2, ok := f.handle.GetNoTouch(f.key)
	if !ok {
		t.Fatal("SETUP: the pass2 refresh must replace the cell, not remove it")
	}
	reminted2 := !e2.BornAt.Equal(born)
	refused2 := f.store.Stats().RemintRefusedColdTotal
	t.Logf("PROBE RESULT: pass2 re-minted=%v refused_cold Δ=%d — customer reads: %d",
		reminted2, refused2-refused1, f.store.Stats().HitTotal-f.hits0)
	if reminted2 {
		t.Errorf("#507 RED: the keepwarm sweep's write made a NEVER-READ cell warm, so #496's gate re-minted it "+
			"(BornAt %v → %v, advanced %v). The max-age clock has moved out from under the reaper's cold-evict and "+
			"the TTL cannot reclaim it either, every sweep pass — the cell is immortal with zero customer reads",
			born, e2.BornAt, e2.BornAt.Sub(born))
	}
	if refused2 != refused1+1 {
		t.Errorf("#507 RED: remint_refused_cold_total Δ=%d across pass2, want 1 — after the sweep write the gate "+
			"stopped refusing a cell nobody has read", refused2-refused1)
	}
	f.assertNoSeedAttribution(t)
	f.assertNoCustomerRead(t)
}

// TestIssue507_NoSeedCellLiteralHardcodesSeedProvenance — the STRUCTURAL arm,
// the #394 enumeration guard's sibling and the only cover for the RESTACTIONS
// site.
//
// The behavioural arms above drive seedOneWidget; the restactions tail
// (seedRestactionResolveAndPutProd) needs a full apiserver fixture to reach, so
// its literal is pinned here instead: no production seed-cell literal in this
// package may set SeededAtBoot to the CONSTANT true. The value must be an
// expression — in practice the guard field the mode decided at seed entry —
// because a constant is exactly how this defect was written the first time, in
// both literals at once, and a re-introduction at either one would otherwise be
// caught by nothing (the #394 guard checks that the function READS a terminal
// guard, not that the literal USES its provenance).
//
// Enumerated by AST walk over the package sources, like the #394 guard, so a new
// seed Put site is covered on the day it is written. Non-vacuity is asserted: the
// walk must still find both known sites.
//
// RED on origin/main: both literals are `SeededAtBoot: true`.
func TestIssue507_NoSeedCellLiteralHardcodesSeedProvenance(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	type site struct {
		file, fn, value string
		line            int
	}
	var sites []site
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		src, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range src.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || !isResolvedEntryLit(lit.Type) {
					return true
				}
				for _, el := range lit.Elts {
					kv, ok := el.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if id, ok := kv.Key.(*ast.Ident); !ok || id.Name != s394SeedCellMarker {
						continue
					}
					value := "<expr>"
					if id, ok := kv.Value.(*ast.Ident); ok {
						value = id.Name
					}
					sites = append(sites, site{file: path, fn: fn.Name.Name, value: value, line: fset.Position(kv.Pos()).Line})
				}
				return true
			})
		}
	}

	found := map[string]bool{}
	for _, s := range sites {
		found[s.fn] = true
		if s.value == "true" {
			t.Errorf("#507 RED: %s:%d (%s) hardcodes %s: true. That flag is half of warmLocked, so a constant here "+
				"makes EVERY seed mode confer warmth — including the keepwarm sweep's re-Put of a cell no customer "+
				"has ever read, which defeats #496's gate indefinitely. Carry the provenance the guard decided at "+
				"seed entry (seedProvenanceForMode) instead.", s.file, s.line, s.fn, s394SeedCellMarker)
		}
	}
	for _, want := range []string{"seedRestactionResolveAndPutProd", "seedOneWidget"} {
		if !found[want] {
			t.Fatalf("VACUOUS GUARD: the AST walk found no %s field in %s (sites=%v) — the literal shape changed; "+
				"fix the walk, do not delete the expectation", s394SeedCellMarker, want, sites)
		}
	}
}

// i507MustBeInWindow fails LOUDLY when the cell is not inside #378's lead window.
// Past the cap is an environment stall, not the defect: the arm must never report
// #507 on a cell whose geometry drifted.
func i507MustBeInWindow(t *testing.T, born time.Time, windowStart, maxAge time.Duration, when string) {
	t.Helper()
	age := time.Since(born)
	if age >= maxAge {
		t.Fatalf("SETUP (%s): the cell is PAST the cap (age %v >= %v) — the machine stalled; environment failure, "+
			"not the #507 defect", when, age, maxAge)
	}
	if age < windowStart {
		t.Fatalf("SETUP (%s): the cell is not yet in the lead window (age %v < %v)", when, age, windowStart)
	}
}
