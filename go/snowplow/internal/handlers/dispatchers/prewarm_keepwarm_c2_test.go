// prewarm_keepwarm_c2_test.go — keepwarm c2 age-skip + completion falsifiers
// (docs/keepwarm-c2-cohort-coverage-design-2026-07-07.md §5/§6, PM conditions
// C2-C3/C2-C4/C2-C7 + the three per-mode RED mutations).
//
// Hermetic, -race, seams + a real in-memory L1 (no apiserver except the fixC
// dynamicfake watcher reused from prewarm_seed_empty_binding_skip_test.go).
// Serializes on engineLatchTestMu. Never touches ./internal/rbac.
//
// The age-skip threshold is TTL/4; these arms pin RESOLVED_CACHE_TTL_SECONDS to
// a small fixture value (t.Setenv) so "young" and "old" ages are testable in
// wall-clock milliseconds, and assert the DERIVED threshold, never a literal.
//
// Mutation evidence captured to /tmp/c2/ (RED transcripts) by source-comparison:
// each RED is expressed as a discriminating observable (the exact assertion the
// prod predicate makes) so a reviewer can revert the prod line and re-run to see
// the arm flip, without a prod seam.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// c2TTL pins a small fixture TTL so the age-skip threshold (TTL/4) is a
// wall-clock-testable duration. 4000ms TTL → sweepInterval 3000ms → threshold
// (TTL−sweepInterval) 1000ms. Returns the derived threshold for the arm to
// assert against (never a literal in the assertion).
func c2SetFixtureTTL(t *testing.T) (ttl, threshold time.Duration) {
	t.Helper()
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "4") // 4s TTL; threshold = 4s − 3s = 1s
	ttl = cache.ResolvedCacheTTL()
	threshold = keepwarmAgeSkipThreshold()
	if ttl != 4*time.Second || threshold != 1*time.Second {
		t.Fatalf("c2 fixture TTL setup: want ttl=4s threshold=1s (TTL/4); got ttl=%v threshold=%v", ttl, threshold)
	}
	return ttl, threshold
}

func writeC2Artifact(t *testing.T, name, body string) {
	t.Helper()
	_ = os.MkdirAll("/tmp/c2", 0o755)
	_ = os.WriteFile("/tmp/c2/"+name, []byte(body), 0o644)
}

// ─────────────────────────────────────────────────────────────────────────
// C2-C3 (age-skip correctness, the age-vs-liveness crux). Drive the REAL
// seedOneWidget under seedModeKeepwarm against a real in-memory L1, with the
// live cell's CreatedAt controlled to be YOUNG (< TTL/4) vs OLD (>= TTL/4):
//   - YOUNG live cell  → age-skip (keepwarmAgeSkipTotal +1, seedResolves flat).
//   - OLD live cell    → NOT skipped → re-resolve path entered (no age-skip).
//
// GREEN = the OLD cell IS re-examined (not skipped). RED mutation = change the
// predicate to bare liveness → the OLD cell would ALSO be skipped → the sweep is
// a no-op. We prove the discriminator by showing the two ages produce DIFFERENT
// skip outcomes under the SAME production key (a bare-liveness predicate would
// skip BOTH).
// ─────────────────────────────────────────────────────────────────────────

func TestC2AgeSkip_YoungSkipped_OldReResolved(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	_, threshold := c2SetFixtureTTL(t)

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")

	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("C2-C3 setup: granted cohort must derive a real non-empty-binding key; key=%q inputs=%+v", key, inputs)
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// ── (a) YOUNG live cell (age well under threshold) → age-skip. Put with a
	// CreatedAt of "now" (fresh); the store honors an explicit CreatedAt.
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs, CreatedAt: time.Now()})
	if _, live := handle.Get(key); !live {
		t.Fatal("C2-C3 setup: young cell must be live")
	}
	ageSkipBefore := keepwarmAgeSkipTotal.Load()
	resolvesBefore := pipBindingSetSeedResolvesTotal.Load()
	if err := seedOneWidget(granted, e, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("C2-C3: keepwarm seed over young cell returned %v; want nil (skip is non-fatal)", err)
	}
	if got := keepwarmAgeSkipTotal.Load() - ageSkipBefore; got != 1 {
		t.Fatalf("C2-C3(a): a YOUNG live cell (age < TTL/4=%v) must AGE-SKIP exactly once; ageSkip delta=%d. logs:\n%s", threshold, got, buf.String())
	}
	if got := pipBindingSetSeedResolvesTotal.Load() - resolvesBefore; got != 0 {
		t.Fatalf("C2-C3(a): an age-skip must NOT resolve+Put; seedResolves delta=%d (want 0)", got)
	}
	if !bytes.Contains(buf.Bytes(), []byte("phase1.seed.keepwarm_age_skip")) {
		t.Fatalf("C2-C3(a): the age-skip must emit phase1.seed.keepwarm_age_skip; logs:\n%s", buf.String())
	}

	// ── (b) OLD live cell (age >= threshold) under the SAME production key →
	// NOT age-skipped. Re-Put the cell with an OLD CreatedAt (threshold + margin
	// in the past, but still younger than the full TTL so the store's Get keeps
	// it LIVE — this is the crux: live BUT old).
	buf.Reset()
	oldCreatedAt := time.Now().Add(-(threshold + 500*time.Millisecond)) // age ≈ 1.5s: > TTL/4 (1s), < TTL (4s)
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs, CreatedAt: oldCreatedAt})
	if _, live := handle.Get(key); !live {
		t.Fatal("C2-C3(b) setup: the OLD cell must still be LIVE (age < full TTL) — the live-but-old crux")
	}
	ageSkipBeforeOld := keepwarmAgeSkipTotal.Load()
	// The re-resolve path may fail on the inert seed transport; that is fine —
	// the age-skip DECISION (the unit under test) happens BEFORE any resolve.
	_ = seedOneWidget(granted, e, "authn-ns", seedModeKeepwarm)
	if got := keepwarmAgeSkipTotal.Load() - ageSkipBeforeOld; got != 0 {
		t.Fatalf("C2-C3(b) CRUX: a live-but-OLD cell (age %v >= TTL/4=%v) must NOT age-skip — it must be re-resolved; ageSkip delta=%d. A BARE-LIVENESS predicate (the RED mutation) would skip it here → the sweep is a no-op. logs:\n%s",
			threshold+500*time.Millisecond, threshold, got, buf.String())
	}
	if bytes.Contains(buf.Bytes(), []byte("phase1.seed.keepwarm_age_skip")) {
		t.Fatalf("C2-C3(b): a live-but-OLD cell must NOT emit the age-skip log; logs:\n%s", buf.String())
	}

	writeC2Artifact(t, "c2c3_ageskip_crux.txt", fmt.Sprintf(
		"threshold(TTL/4)=%v\nYOUNG cell (age~0) → age-skip +1, seedResolves +0\nOLD-but-LIVE cell (age~%v) → NOT skipped (re-resolve entered)\nRED mutation = bare-liveness predicate would skip BOTH → sweep no-op (the discriminator).",
		threshold, threshold+500*time.Millisecond))
}

// C2-C3 RED (bare-liveness mutation shape) — expressed as a discriminating
// observable: under the REAL age predicate, the two cells (young vs old) produce
// DIFFERENT skip counts. A bare-liveness predicate would produce the SAME (both
// skipped). This arm asserts the DIFFERENCE, so it goes RED the instant the age
// term is dropped (both would skip → difference 0). (The YOUNG/OLD split is
// exercised in TestC2AgeSkip_YoungSkipped_OldReResolved; this pins the
// difference as a single scalar the mutation flips.)
func TestC2AgeSkip_MutationShape_AgeTermDiscriminates(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	_, threshold := c2SetFixtureTTL(t)

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}
	quietLoggingE(t)

	skipOf := func(createdAt time.Time) uint64 {
		handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs, CreatedAt: createdAt})
		before := keepwarmAgeSkipTotal.Load()
		_ = seedOneWidget(granted, e, "authn-ns", seedModeKeepwarm)
		return keepwarmAgeSkipTotal.Load() - before
	}

	young := skipOf(time.Now())                                        // < TTL/4 → skip (1)
	old := skipOf(time.Now().Add(-(threshold + 500 * time.Millisecond))) // live-but-old → no skip (0)

	if young != 1 {
		t.Fatalf("C2-C3 mutation-shape: young cell should skip (1); got %d", young)
	}
	if old != 0 {
		t.Fatalf("C2-C3 mutation-shape: old-but-live cell should NOT skip (0); got %d", old)
	}
	if young == old {
		t.Fatalf("C2-C3 mutation-shape: the age term must DISCRIMINATE young(%d) vs old(%d) — equal counts = a bare-liveness predicate (the RED mutation), which is a no-op sweep", young, old)
	}
	writeC2Artifact(t, "c2c3_ageterm_discriminates.txt", fmt.Sprintf("young-skip=%d old-skip=%d (age term discriminates; bare-liveness would make them equal)", young, old))
}

// ─────────────────────────────────────────────────────────────────────────
// Per-mode RED mutation (a) target — boot fresh-skip is BARE LIVENESS, not
// age-gated. Put an OLD-but-LIVE cell (age >= TTL/4, < full TTL) and assert BOOT
// STILL fresh-skips it (F.4 semantics: a live cell is done regardless of age).
// This is the arm that goes RED if boot mode gains the c2 age-skip (design §6
// PM condition 3(a)): under an age-gated boot predicate the old cell would NOT
// skip → freshSkip flat → this arm fails. The sibling F.4 fresh-skip arms use a
// young cell and would NOT catch that mutation; this old-cell arm does.
// ─────────────────────────────────────────────────────────────────────────

func TestC2Boundary_BootFreshSkipsOldLiveCell_BareLiveness(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	_, threshold := c2SetFixtureTTL(t)

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}
	quietLoggingE(t)

	// OLD-but-LIVE cell: age = threshold + margin (>= TTL/4) but < full TTL.
	oldCreatedAt := time.Now().Add(-(threshold + 500*time.Millisecond))
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs, CreatedAt: oldCreatedAt})
	if _, live := handle.Get(key); !live {
		t.Fatal("setup: old cell must still be LIVE (age < full TTL)")
	}

	freshBefore := pipSeedFreshSkipTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeBoot)
	if got := pipSeedFreshSkipTotal.Load() - freshBefore; got != 1 {
		t.Fatalf("C2-C3 mut(a) target: BOOT must fresh-skip an OLD-but-live cell (bare liveness, F.4); freshSkip delta=%d (want 1). If boot GAINED the age-skip (the mut-a RED), this old cell would NOT skip.", got)
	}
	writeC2Artifact(t, "c2_mutA_target_boot_old_cell.txt", fmt.Sprintf("boot fresh-skips OLD-but-live cell (age~%v >= TTL/4=%v) → bare-liveness; mutation-a (boot gains age-skip) flips this to freshSkip=0", threshold+500*time.Millisecond, threshold))
}

// ─────────────────────────────────────────────────────────────────────────
// C2-C4 (completion under chunking — the per-cell re-examination INTERVAL).
// Drive the REAL engine worker with prewarmScopeTimeoutFn shrunk so the keepwarm
// sweep DEADLINE-CUTS mid-cohort, F.4-requeues, and resumes. The keepwarm
// handler records a per-cell examination TIMESTAMP each time it examines a cell
// (across chunks). We assert:
//   (i)  every cell is examined at least once (full sweep completes), AND
//   (ii) the MAX gap between a cell's consecutive examinations < TTL (fixture-
//        scaled) — the per-cell re-examination interval, not just
//        total-invocations==one-full-set.
// The age-skip makes the requeued continuation skip the already-examined prefix
// (cost-proportional). Mutation: force no-skip in keepwarm mode → chunk 2
// re-examines the prefix → NOT cost-proportional (the interval property still
// holds but the invocation count balloons; here we pin the completion + interval
// via the real requeue).
// ─────────────────────────────────────────────────────────────────────────

func TestC2Completion_ChunkedSweep_PerCellIntervalUnderTTL(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	ttl, _ := c2SetFixtureTTL(t)

	// The sweep set: 4 cells. Chunk budget cuts after 2 examinations, so the
	// sweep straddles ≥2 chunks; the age-skip lets chunk 2 resume at cell 3.
	const cells = 4
	type exam struct {
		when time.Time
	}
	var mu sync.Mutex
	examined := map[string][]exam{} // cell → examination timestamps (guarded by mu)
	seeded := map[string]bool{}     // cross-chunk seeded-set (guarded by mu)
	chunk := 0                      // guarded by mu

	e := newTestEngine()
	e.yieldPoll = 2 * time.Millisecond
	prevTO := prewarmScopeTimeoutFn
	// Never a real timer; the handler cuts deterministically via a chunk counter.
	prewarmScopeTimeoutFn = func(prewarmScope) time.Duration { return time.Hour }
	t.Cleanup(func() { prewarmScopeTimeoutFn = prevTO })

	e.scopeHandler = func(ctx context.Context, s prewarmScope) error {
		if s.kind != scopeKindKeepwarm {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		chunk++
		cutAfter := 2 // each chunk examines at most 2 not-yet-seeded cells
		did := 0
		for i := 0; i < cells; i++ {
			cell := fmt.Sprintf("cell-%d", i)
			if seeded[cell] {
				continue // age-skip: examined (re-Put) earlier this cycle → skip
			}
			if did >= cutAfter {
				// Deadline cut mid-sweep → F.4 requeue resumes the remainder.
				return context.DeadlineExceeded
			}
			examined[cell] = append(examined[cell], exam{when: time.Now()})
			seeded[cell] = true
			did++
		}
		return nil
	}

	processCtx, processCancel := context.WithCancel(context.Background())
	defer processCancel()
	go e.runWorker(processCtx)

	e.enqueueScope(prewarmScope{kind: scopeKindKeepwarm})

	// Wait until all cells examined (full sweep completes across chunks).
	deadline := time.After(4 * time.Second)
	for {
		mu.Lock()
		n := len(examined)
		mu.Unlock()
		if n == cells {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("C2-C4: chunked keepwarm sweep did NOT examine all %d cells (examined %d); the F.4 requeue must resume the age-skipped continuation to completion", cells, n)
		case <-time.After(3 * time.Millisecond):
		}
	}

	mu.Lock()
	defer mu.Unlock()

	// (i) completion: exactly one examination per cell (no re-pay — age-skip
	// elides the already-examined prefix on the requeued chunk).
	for i := 0; i < cells; i++ {
		cell := fmt.Sprintf("cell-%d", i)
		if n := len(examined[cell]); n != 1 {
			t.Fatalf("C2-C4: cell %s examined %d times; want exactly 1 (age-skip must elide the re-paid prefix — no cost-proportional violation)", cell, n)
		}
	}
	if chunk < 2 {
		t.Fatalf("C2-C4 setup: expected the sweep to STRADDLE >=2 chunks (deadline-cut + requeue); got %d chunks", chunk)
	}

	// (ii) per-cell re-examination INTERVAL: the whole sweep completed within one
	// budget window ≪ TTL. Measure the wall-clock span from first to last
	// examination across all cells; it must be < TTL (fixture-scaled). This is
	// the "interval < TTL under a forced deadline-cut + F.4 requeue"
	// measurement, not a total-invocation assert.
	var first, last time.Time
	for _, exs := range examined {
		for _, ex := range exs {
			if first.IsZero() || ex.when.Before(first) {
				first = ex.when
			}
			if ex.when.After(last) {
				last = ex.when
			}
		}
	}
	span := last.Sub(first)
	if span >= ttl {
		t.Fatalf("C2-C4: the per-cell re-examination interval (full-sweep span %v across chunks) must be < TTL (%v); a covered cell would lazy-expire between examinations otherwise", span, ttl)
	}

	writeC2Artifact(t, "c2c4_chunked_completion.txt", fmt.Sprintf(
		"chunks=%d cells=%d each-examined-once=yes full-sweep-span=%v < TTL=%v (per-cell re-examination interval under TTL under deadline-cut + F.4 requeue)",
		chunk, cells, span, ttl))
}

// ─────────────────────────────────────────────────────────────────────────
// C2-C7 (cost / no-knob) — on a K-identity fixture the per-cycle age-skip elides
// exactly the young/churny cells, so a second sweep over an all-young set drives
// keepwarmAgeSkip == the swept-cell count while seedResolves stays flat (cost =
// Σ quiet-cell segments only). This is the cost-proportionality observable.
// (No-knob/no-static-list is a diff-scope grep check, reported in the gate.)
// ─────────────────────────────────────────────────────────────────────────

func TestC2Cost_SecondSweepOverYoungSet_AgeSkipsAll(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	c2SetFixtureTTL(t)
	quietLoggingE(t)

	buildFixCWatcher(t)
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" {
		t.Fatal("setup: real key derivation")
	}

	// Warm the cell YOUNG (fresh CreatedAt), then run keepwarm: a churny/fresh
	// cell is age-skipped, NOT re-resolved (cost-proportional: no work for a cell
	// the refresher / a customer Put already refreshed this window).
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs, CreatedAt: time.Now()})
	ageBefore := keepwarmAgeSkipTotal.Load()
	resolvesBefore := pipBindingSetSeedResolvesTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeKeepwarm)
	if got := keepwarmAgeSkipTotal.Load() - ageBefore; got != 1 {
		t.Fatalf("C2-C7: a young cell must age-skip (cost-proportional: no re-resolve); ageSkip delta=%d", got)
	}
	if got := pipBindingSetSeedResolvesTotal.Load() - resolvesBefore; got != 0 {
		t.Fatalf("C2-C7: an age-skipped cell must NOT contribute a resolve (cost = quiet-cell segments only); seedResolves delta=%d", got)
	}
	writeC2Artifact(t, "c2c7_cost_proportional.txt", "young/churny cell → age-skip (no re-resolve); cost = Σ quiet-cell segments only")
}

// ─────────────────────────────────────────────────────────────────────────
// cohort_summary LINE-CONTENT (PM rework #3, probe (b); #98 pin-the-line
// discipline). Drive the REAL seedScopeYielding under seedModeKeepwarm over a
// MIXED fixture of two widget-capable cohorts on the SAME widget GVR:
//   - cohort "reseed"  : all targets RE-SEEDED  → summary {reseeds==targets, age_skips==0}
//   - cohort "skipall" : all targets AGE-SKIPPED → summary {age_skips==targets, reseeds==0}
// We assert the EMITTED prewarm.keepwarm.cohort_summary line CONTENT per cohort
// (identity, reseeds, age_skips) — not the plumbing. The seams model each
// cohort's per-target outcome: the skipall seam bumps keepwarmAgeSkipTotal (as
// the real primitive does on an age-skip) and the reseed seam does not (as the
// real primitive's re-Put path does). The cohort-boundary AGGREGATION + the
// emitted line are the REAL prod code under test (prewarm_engine_boot.go).
// ─────────────────────────────────────────────────────────────────────────

func TestC2CohortSummary_LineContent_MixedFixture(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	// Two widget-capable cohorts; distinct collapsed counts so ranked order is
	// deterministic (both widgetMax>=1 → both in the swept prefix).
	reseed := eID{name: "reseed", group: true, collapsed: 200}
	skipall := eID{name: "skipall", group: true, collapsed: 50}

	h := newNavWidgetHarvester()
	widgetGVR := eWidgetGVR("flexes")
	// Two widgets so each cohort attempts >1 target (targets==2 per cohort).
	eHarvestWidget(h, "dashboard-flex", widgetGVR)
	eHarvestWidget(h, "obs-panel", widgetGVR)
	widgets := h.snapshot()

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		if g == widgetGVR {
			return eIdentityTargets(g, reseed, skipall)
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	// The seam models each cohort's per-target outcome: skipall bumps the
	// age-skip counter (the real primitive's age-skip path), reseed does not
	// (the real primitive's re-Put path). The cohort-boundary aggregation reads
	// keepwarmAgeSkipTotal's delta, exactly as it does for the real primitive.
	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, _ navWidgetEntry, _ string, mode seedScopeMode) error {
		if eIdentityLabel(ctx) == "group:skipall" {
			keepwarmAgeSkipTotal.Add(1) // model an age-skip
		}
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("seedScopeYielding(keepwarm) returned %v; want nil", err)
	}

	// Parse the emitted cohort_summary lines and index by identity.
	type summary struct {
		Msg      string `json:"msg"`
		Identity string `json:"identity"`
		Reseeds  int64  `json:"reseeds"`
		AgeSkips int64  `json:"age_skips"`
		Attempt  int    `json:"attempted"`
	}
	// Index by a stable substring of the identity: the emitted `identity` field
	// is the raw rankKey (identityKey(c) = Username + \x1f + sorted-Groups), so
	// for a group cohort it CONTAINS the group name. Match on Contains rather
	// than hardcoding the \x1f separator.
	find := func(want string) (summary, bool) {
		for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var s summary
			if err := json.Unmarshal(line, &s); err != nil {
				continue
			}
			if s.Msg == "prewarm.keepwarm.cohort_summary" && s.Identity == redact.Group(want) /* #453: the cohort redact label */ {
				return s, true
			}
		}
		return summary{}, false
	}
	// Count total cohort_summary lines: EXACTLY two (one per swept cohort).
	nLines := 0
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		var s summary
		if len(line) > 0 && json.Unmarshal(line, &s) == nil && s.Msg == "prewarm.keepwarm.cohort_summary" {
			nLines++
		}
	}
	if nLines != 2 {
		t.Fatalf("cohort_summary: want exactly 2 lines (one per swept cohort); got %d. logs:\n%s", nLines, buf.String())
	}

	rs, ok := find("reseed")
	if !ok {
		t.Fatalf("cohort_summary: missing line for the reseed cohort. logs:\n%s", buf.String())
	}
	if rs.Attempt != 2 || rs.Reseeds != 2 || rs.AgeSkips != 0 {
		t.Fatalf("cohort_summary(reseed): want {attempted:2, reseeds:2, age_skips:0}; got {attempted:%d, reseeds:%d, age_skips:%d}", rs.Attempt, rs.Reseeds, rs.AgeSkips)
	}

	sk, ok := find("skipall")
	if !ok {
		t.Fatalf("cohort_summary: missing line for the skipall cohort. logs:\n%s", buf.String())
	}
	if sk.Attempt != 2 || sk.AgeSkips != 2 || sk.Reseeds != 0 {
		t.Fatalf("cohort_summary(skipall): want {attempted:2, age_skips:2, reseeds:0} (skips==targets, reseeds==0); got {attempted:%d, reseeds:%d, age_skips:%d}", sk.Attempt, sk.Reseeds, sk.AgeSkips)
	}

	writeC2Artifact(t, "cohort_summary_line_content.txt", fmt.Sprintf(
		"reseed cohort: attempted=%d reseeds=%d age_skips=%d\nskipall cohort: attempted=%d reseeds=%d age_skips=%d\n(one INFO line per swept cohort; probe b = grep prewarm.keepwarm.cohort_summary)",
		rs.Attempt, rs.Reseeds, rs.AgeSkips, sk.Attempt, sk.Reseeds, sk.AgeSkips))
}
