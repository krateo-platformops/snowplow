// phase1_pip_seed_test.go — Ship 0.30.187 D2 falsifier.
//
// D2 (defect): the Phase-1 prewarm PIP seed Put used the walker's
// RESOLUTION pagination (prewarmPageLimit()=5 default) as the dispatcher
// cache key tuple. The dispatcher at serve time computes the key from
// the request URL's `?page=N&perPage=M` query params via paginationInfo
// (helpers.go:50-76), which DEFAULTS to (-1, -1) when the URL carries
// no slice. Seed→serve cells mismatch on every no-slice widget, so the
// PIP-seeded entries never hit and the first nav looks cold.
//
// THE FIX (architect's TRACED design 2026-05-27): decouple the seed-key
// tuple from the resolution tuple. The seed-key tuple MUST match the
// dispatcher's paginationInfo for an equivalent request:
//   - widget reached via /call Path with NO page/perPage params:
//       seed-key tuple = (-1, -1)  (matches paginationInfo's default).
//   - widget reached via /call Path WITH declared page/perPage:
//       seed-key tuple = (declared page, declared perPage)
//       (matches paginationInfo when the frontend hits the same URL).
// #966 then removed the remaining split: D2 kept RESOLVING a no-slice widget
// at prewarmPageLimit()=5 while KEYING it (-1,-1), so the cell held a body no
// unpaginated request computes. Since #966 the seed resolves and keys with
// ONE pagination (prewarmPaginationFor), and these tests pin that derivation.
//
// THESE TESTS pin the derivation in isolation — they do not require a live
// apiserver, do not require an informer, and run in <1ms. The writer-level
// arms (stored body == a live request's resolve) are in
// prewarm_pagination_966_test.go.

package dispatchers

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// TestPipCohortTimeout_RestoredToFixed120s is the Ship 0.30.191 SCOPE
// CORRECTION falsifier. It pins the contract that the per-cohort
// timeout is the FIXED 120s value (0.30.179 baseline) — the 0.30.190
// proportional-timeout model (computeCohortTimeout) has been reverted
// because the underlying premise (a measured "1.5s/widget × 132
// widgets = 198s" projection) was an INFERENCE from a file header
// comment, not an empirical measurement. Per
// feedback_data_driven_workflow + feedback_empirical_root_cause_trace_
// before_fix the 0.30.191 ship instruments the abort cause FIRST;
// any future timeout change must follow from that measurement.
//
// A regression that re-adds the proportional model would either
// re-introduce computeCohortTimeout (caught by compile-error in this
// package — the symbol no longer exists) or change the constant value
// (caught by this test).
func TestPipCohortTimeout_RestoredToFixed120s(t *testing.T) {
	if pipCohortTimeout != 120*time.Second {
		t.Fatalf("Ship 0.30.191 invariant violated: pipCohortTimeout = %v; want 120s — "+
			"the 0.30.190 proportional-timeout model was reverted per the SCOPE "+
			"CORRECTION; instrument the abort cause first, then change the timeout "+
			"if-and-only-if the data says so", pipCohortTimeout)
	}
	if pipGlobalTimeout != 8*time.Minute {
		t.Fatalf("Ship 0.30.191 invariant violated: pipGlobalTimeout = %v; want 8m — "+
			"reverted in lockstep with pipCohortTimeout", pipGlobalTimeout)
	}
}

// TestPhase1PIPSeedKey_NoSliceUsesDispatcherDefaultTuple is the D2
// falsifier, restated for #966. A widget reached via a /call Path with NO
// page/perPage params gets the pagination the dispatcher's paginationInfo
// derives for that URL, (-1, -1) — and since #966 that ONE pagination is both
// the seed cell's key and the seed's resolve, so the cell is unpaginated
// through and through.
func TestPhase1PIPSeedKey_NoSliceUsesDispatcherDefaultTuple(t *testing.T) {
	pg := prewarmPaginationFor(noSlicePath)
	if pg.PerPage != -1 || pg.Page != -1 {
		t.Fatalf("D2: no-slice widget pagination = (perPage=%d, page=%d), "+
			"want (-1, -1) — the dispatcher's paginationInfo defaults to "+
			"(-1, -1) when the request URL carries no ?page/?perPage, so the "+
			"seed Put must use the same tuple or it hashes to a different "+
			"cell and the serve-time lookup misses (the 0.30.186 14/17 first-"+
			"nav-hit defect)",
			pg.PerPage, pg.Page)
	}
}

// TestPhase1PIPSeedKey_DeclaredSlicePreserved pins the symmetric
// contract: a /call Path that carries explicit page/perPage yields exactly
// the declared slice. The frontend hits the same URL at serve time, so
// paginationInfo returns the same (page, perPage).
func TestPhase1PIPSeedKey_DeclaredSlicePreserved(t *testing.T) {
	// Dashboard table: declared slice page=1 perPage=5.
	pg := prewarmPaginationFor(dashboardTablePath)
	if pg.PerPage != 5 || pg.Page != 1 {
		t.Fatalf("D2: declared-slice widget pagination = (perPage=%d, page=%d), "+
			"want (perPage=5, page=1) — paginationInfo at serve time returns "+
			"the URL's declared page/perPage so the seed Put must use the "+
			"same tuple",
			pg.PerPage, pg.Page)
	}
}

// TestPhase1PIPSeedKey_RootWidgetUsesDispatcherDefaultTuple pins the
// root-navigation case. A root has no /call Path (it is fetched directly
// via objects.Get from a listed ObjectReference), so the walker passes
// an empty path string — prewarmPaginationFor must yield the dispatcher's
// no-slice default tuple. The frontend's first hit on a root navigation
// widget URL carries no slice params, so paginationInfo returns
// (-1, -1); the root's pagination must match.
func TestPhase1PIPSeedKey_RootWidgetUsesDispatcherDefaultTuple(t *testing.T) {
	pg := prewarmPaginationFor("")
	if pg.PerPage != -1 || pg.Page != -1 {
		t.Fatalf("D2: root widget (empty path) pagination = "+
			"(perPage=%d, page=%d), want (-1, -1) — a root widget has no "+
			"/call Path and the dispatcher's first request for it carries "+
			"no slice params, so paginationInfo returns (-1, -1)",
			pg.PerPage, pg.Page)
	}
}

// TestSeedScopeYielding_CtxCancelEmitsAbortLog is the 0.30.191 Fix-C
// abort-observability falsifier, RE-POINTED 2026-07-03 (docs/prewarm-engine-
// implicit-on-cache-2026-07-03.md §4.3b) from the deleted legacy seedCohort to
// the surviving ENGINE seed path (seedScopeYielding, prewarm_engine_boot.go).
//
// COVERAGE MIGRATION + NEW EMIT: the engine's ctx-cancel exits previously just
// `return ctx.Err()` with no greppable line — a coverage GAP, not a duplicate.
// This ship ADDS a `prewarm.seed.abort` emit to seedScopeYielding carrying the
// Fix-C load-bearing fields (phase / cause / targets_processed / elapsed_ms) so
// the post-deploy "seed finished or cut off?" grep survives on the engine path.
// This test drives seedScopeYielding with a PRE-CANCELLED ctx (1 restaction ref,
// 0 widgets): the restactions-loop ctx-check fires on the first iteration,
// emits the abort line, and returns ctx.Err().
//
// A regression that removes the emit (or fails to thread the phase/counters)
// fails this test. RED against the pre-emit engine: seedScopeYielding returned
// ctx.Err() with NO greppable abort line → the assertions below cannot hold.
func TestSeedScopeYielding_CtxCancelEmitsAbortLog(t *testing.T) {
	buf := captureDefaultSlogForTest(t, slog.LevelInfo)

	// Pre-cancelled ctx — seedScopeYielding hits ctx.Err() != nil on the first
	// restactions-loop iteration, emits the abort, and returns.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 1 restaction ref — never resolved because the ctx-check at the top of the
	// loop fires first. The ref's content is irrelevant; the abort path runs
	// before any seed work.
	refs := []templatesv1.ObjectReference{{
		Reference: templatesv1.Reference{
			Name:      "test-restaction",
			Namespace: "test-ns",
		},
		APIVersion: "templates.krateo.io/v1",
		Resource:   "restactions",
	}}

	// Zero-value endpoints + nil REST config — never dereferenced (the ctx-check
	// returns before any target seed).
	err := seedScopeYielding(ctx, refs, nil /* widgets */, endpoints.Endpoint{}, nil /* rc */, "test-authn-ns", seedModeBoot)
	if err == nil {
		t.Fatalf("Fix-C(engine): seedScopeYielding with pre-cancelled ctx returned nil; want the ctx error")
	}

	logText := buf.String()
	if !strings.Contains(logText, "prewarm.seed.abort") {
		t.Fatalf("Fix-C(engine): expected `prewarm.seed.abort` log line; got:\n%s", logText)
	}

	// Decode the JSON record lines and find the abort line.
	var found map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logText), "\n") {
		var rec map[string]any
		if jerr := json.Unmarshal([]byte(line), &rec); jerr != nil {
			continue
		}
		if msg, _ := rec["msg"].(string); msg == "prewarm.seed.abort" {
			found = rec
			break
		}
	}
	if found == nil {
		t.Fatalf("Fix-C(engine): could not decode prewarm.seed.abort record from:\n%s", logText)
	}

	// Pin the load-bearing fields the post-deploy grep relies on.
	if phase, _ := found["phase"].(string); phase != "restactions" {
		t.Errorf("Fix-C(engine): phase = %q; want %q (full record: %+v)", phase, "restactions", found)
	}
	// cause carries the underlying cancellation reason — non-empty is the
	// contract; the exact string ("context canceled") is stdlib-set.
	if s, _ := found["cause"].(string); s == "" {
		t.Errorf("Fix-C(engine): cause is empty; want non-empty cancellation reason (full record: %+v)", found)
	}
	// targets_processed decoded as float64 by encoding/json; 0 (aborted before
	// the first target seed completed).
	if tp, _ := found["targets_processed"].(float64); tp != 0 {
		t.Errorf("Fix-C(engine): targets_processed = %v; want 0 (aborted on first iteration)", tp)
	}
	if _, ok := found["elapsed_ms"]; !ok {
		t.Errorf("Fix-C(engine): elapsed_ms missing (full record: %+v)", found)
	}
}
