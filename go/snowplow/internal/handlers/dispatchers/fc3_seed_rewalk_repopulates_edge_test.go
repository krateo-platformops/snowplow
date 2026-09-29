// fc3_seed_rewalk_repopulates_edge_test.go — #279 F-C3 NO-REGRESSION guard.
//
// WHAT C3 IS (the DEFERRED defect, #279 stays OPEN). The iterator-empty
// short-circuit in the resolver (restactions/api resolve.go — the `len(tmp)==0`
// / crds.items[]-empty continue): when a stage's iterator evaluates to zero
// items on a cold first pass (e.g. before a CompositionDefinition's CRD is
// registered), the stage records NO LIST dep edge. The cell can be warm yet
// carry no (gvr, ns, "*") edge, so a later CR ADD against that GVR dirty-marks
// NOTHING → a stale-negative. The close-now fix stays DEFERRED (a narrow,
// TTL-bounded residual); #279 remains OPEN.
//
// WHAT THIS GUARD PINS (not a fix — a regression fence). The EXISTING mitigation
// is the seed RE-WALK: the GVR-discovered re-prewarm (prewarm_engine_boot.go's
// "Ship 2 Stage 2" note) re-resolves already-warm cells with NO skip, so the
// resolve re-runs over the now-populated iterator and re-records the LIST edge.
// The PM required a no-regression guard so the edge-3 fix (#277/#281) — and the
// later Step 2E — cannot SILENTLY break that re-population. The load-bearing
// mechanism that guarantees it is the seedModeGVRDiscovered NEVER-SKIP rule
// (phase1_pip_seed.go seedSkipDecision, the F4-C3 boundary): a re-walk that
// skipped the warm-but-edge-less cell would never re-resolve it, and the edge
// would never come back.
//
// THE ARM CAN FAIL (it is a real fence, not vacuous):
//   - Part A/B: if a regression collapses seedModeGVRDiscovered to a boot-style
//     skip (skips warm cells), seedSkipDecision(gvr-discovered, liveCell) flips
//     true / seedOneWidget starts fresh-skipping under gvr-discovered → RED. The
//     boot arm is the discriminating control: it MUST still skip the same live
//     cell, so the two modes are proven to genuinely differ.
//   - Part C: the re-populated LIST edge must make the cell invalidatable —
//     OnAdd goes 0→≥1 across the (modeled) re-resolve's RecordList.
//
// Established-idiom reuse: the live-cell / real-primitive skip harness is the F.4
// boot-resume idiom (buildFixCWatcher + f4WidgetKey + real seedOneWidget); the
// edge-re-population half is the gvr-discovered-integration tracker contract
// (no edge → OnAdd 0; RecordList → OnAdd matches) over a UNIQUE synthetic GVR so
// no dep-tracker reset is needed and no cross-test bleed can perturb it.

package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestFC3_SeedRewalk_GVRDiscovered_ReResolvesWarmCell_RepopulatesEdge pins that
// the seed re-walk (seedModeGVRDiscovered) STILL re-resolves an already-warm
// cell (never skips it) so the LIST dep edge an iterator-empty first pass missed
// gets re-populated — the #279 mitigation the edge-3 fix must not break.
func TestFC3_SeedRewalk_GVRDiscovered_ReResolvesWarmCell_RepopulatesEdge(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	buildFixCWatcher(t) // cache ON; grants userGranted get on the widget GVR
	e := fixCWidgetEntry()
	granted := fixCCohortCtx("userGranted")

	// Derive the production cell key + handle EXACTLY as the seed primitive does
	// (single derivation — the skip lookup re-derives the SAME key internally).
	key, handle, inputs := f4WidgetKey(granted, e)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("F-C3 setup: granted cohort must derive a real non-empty-binding key; key=%q inputs=%+v", key, inputs)
	}

	// Install a LIVE cell under the production key — the "already warm from the
	// iterator-empty first pass" state (warm, but the empty iterator recorded no
	// LIST edge).
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"warm":true}`), Inputs: inputs})
	if _, live := handle.Get(key); !live {
		t.Fatal("F-C3 setup: the seeded cell must be live under the production key")
	}

	// ── Part A — the load-bearing predicate (F4-C3 boundary), driven directly ──
	// Over the SAME live cell: the seed re-walk NEVER skips (so it re-resolves and
	// re-records the edge), while a boot re-pass DOES fresh-skip it (the residual
	// #279 gap that stays OPEN). If the two agree, the re-walk's re-population
	// guarantee is gone.
	if seedSkipDecision(granted, seedModeGVRDiscovered, handle, key, "widgets", "dashboard-flex", "cohort") {
		t.Fatalf("F-C3 REGRESSION: seedModeGVRDiscovered SKIPPED a live/warm cell — the re-walk would " +
			"NOT re-resolve it, so the LIST edge an iterator-empty first pass missed can never be " +
			"re-populated (the #279 mitigation is broken). It must NEVER skip (F4-C3 boundary).")
	}
	if !seedSkipDecision(granted, seedModeBoot, handle, key, "widgets", "dashboard-flex", "cohort") {
		t.Fatalf("F-C3 CONTROL: seedModeBoot must fresh-skip the SAME live cell (the discriminator that " +
			"proves the modes genuinely differ). It did not — the control arm is inert, so the Part-A " +
			"never-skip assertion would not discriminate a regression.")
	}

	// ── Part B — the predicate flows through the REAL seed primitive ──────────
	// The real seedOneWidget must NOT fresh-skip under seedModeGVRDiscovered (it
	// re-resolves), while it DOES fresh-skip under seedModeBoot. We assert only
	// the skip-counter delta; the downstream re-resolve may error on the inert
	// hermetic transport (its outcome is not what this guard fences).
	skipBefore := pipSeedFreshSkipTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeGVRDiscovered)
	if got := pipSeedFreshSkipTotal.Load() - skipBefore; got != 0 {
		t.Fatalf("F-C3 REGRESSION: real seedOneWidget under seedModeGVRDiscovered fresh-skipped a warm "+
			"cell (freshSkip delta=%d, want 0) — the re-walk must re-resolve warm cells so the missed "+
			"LIST edge re-records.", got)
	}
	skipMid := pipSeedFreshSkipTotal.Load()
	_ = seedOneWidget(granted, e, "authn-ns", seedModeBoot)
	if got := pipSeedFreshSkipTotal.Load() - skipMid; got != 1 {
		t.Fatalf("F-C3 CONTROL: real seedOneWidget under seedModeBoot must fresh-skip the warm cell "+
			"exactly once (freshSkip delta=%d, want 1) — the mode-discriminating control.", got)
	}

	// ── Part C — the re-populated edge makes the cell invalidatable ───────────
	// Model the transition on a UNIQUE synthetic GVR (gvr-discovered-integration
	// idiom, no dep reset needed): the iterator-empty first pass recorded NO LIST
	// edge → OnAdd matches 0; the seed re-walk's re-resolve over the now-populated
	// iterator records the type-3 LIST edge (Deps().RecordList — the same record
	// the resolver runs at resolve.go's dep site) → OnAdd matches ≥1. The edge
	// exists post-re-walk.
	fc3GVR := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1-2-2",
		Resource: "githubscaffoldingwithcompositionpages-fc3-test",
	}
	const fc3NS, fc3Name = "bench-ns-fc3-test", "bench-app-fc3-test-01"
	const fc3CellKey = "widget-cell-key-fc3-c279"

	deps := cache.Deps()
	if deps == nil {
		t.Skip("cache.Deps() returned nil — cache off in this test environment")
	}

	// PRE (iterator-empty first pass): no LIST edge for this GVR → 0 matches.
	if pre := deps.OnAdd(fc3GVR, fc3NS, fc3Name); pre != 0 {
		t.Fatalf("F-C3 setup: expected 0 matches before the re-walk (iterator-empty first pass records "+
			"no LIST edge); got %d — cross-test edge bleed on a supposedly-unique GVR.", pre)
	}

	// The seed re-walk re-resolves over the now-populated iterator and re-records
	// the LIST edge (the type-3 (gvr, ns, "*") dep).
	deps.RecordList(fc3CellKey, fc3GVR, fc3NS)

	// POST: the edge is live — a CR ADD now dirty-marks the cell (no stale-negative).
	if post := deps.OnAdd(fc3GVR, fc3NS, fc3Name); post < 1 {
		t.Fatalf("F-C3: expected ≥1 match after the re-walk re-recorded the LIST edge; got %d — the "+
			"edge did not re-populate, the cell would serve a stale-negative on a CR ADD.", post)
	}
}
