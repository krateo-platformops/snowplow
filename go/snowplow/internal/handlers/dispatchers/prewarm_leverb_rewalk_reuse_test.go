// prewarm_leverb_rewalk_reuse_test.go — #135 F4b Lever B falsifiers
// (docs/f4b-leverb-discovery-snapshot-reuse-design-2026-07-22.md).
//
// Lever B skips the ~255s discovery re-walk on an F.4 deadline-cut RESUME pass
// and reuses the process-lived (monotonic / first-write-wins / never-cleared)
// harvester snapshot. Two arms, both driving the REAL prod decision surface — no
// shadow copy (the #64/#66 anti-drift lesson):
//
//   LB-1 SYMPTOM — the reuse predicate. bootShouldWalk is the SINGLE source of
//     the walk-vs-reuse decision rePrewarmBootScoped uses. Truth table over a
//     REAL harvester snapshot count:
//       attempt==0             → WALK (pass 0 / a Forget-reset config-vars redrive)
//       attempt>0, snapshot>0  → REUSE (F.4 resume, same topology — the ~255s save)
//       attempt>0, snapshot==0 → WALK  (boot-race give-up guard: nothing to reuse)
//     An "always-reuse-on-attempt>0" predicate (drop the empty-guard) is RED on
//     the (1,0) case.
//
//   LB-2 SAFETY (LOAD-BEARING) — the invalidation seam. A genuine config-vars
//     redrive MUST reset the boot scope's workqueue requeue count to 0 so it
//     re-dequeues at attempt==0 → bootShouldWalk WALKS the new topology.
//       GREEN = the prod redrive sequence (forgetScope THEN enqueueScope, exactly
//         enqueueBootReDrive): NumRequeues→0 → bootShouldWalk(0, N>0)==WALK.
//       RED (mutation = drop forgetScope, plain enqueueScope only — the pre-fix
//         WIP): NumRequeues stays >0 → bootShouldWalk(k, N>0)==REUSE → the redrive
//         reuses the STALE nav set across the topology change. This is the exact
//         bug the WIP shipped before the seam.
//
// Hermetic, -race, engine/queue + real harvester only. No apiserver, no
// ./internal/rbac. LB-2 serializes on engineLatchTestMu (engine convention).

package dispatchers

import (
	"fmt"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func leverBArtifact(t *testing.T, name, body string) {
	t.Helper()
	_ = os.MkdirAll("/tmp/leverb", 0o755)
	_ = os.WriteFile("/tmp/leverb/"+name, []byte(body), 0o644)
}

// leverBHarvestN builds a navWidgetHarvester holding n distinct widgets through
// the REAL harvestNavWidget, so the count fed to bootShouldWalk is the production
// snapshot() length — not a literal.
func leverBHarvestN(n int) *navWidgetHarvester {
	h := newNavWidgetHarvester()
	gvr := engineSeedWidgetGVR
	h.BeginWalk()
	h.BeginRoot()
	for i := 0; i < n; i++ {
		w := &unstructured.Unstructured{}
		w.SetNamespace("krateo-system")
		w.SetName(fmt.Sprintf("w%d", i))
		w.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: "W"})
		h.harvestNavWidget(w, gvr, unpaginatedPrewarm)
	}
	return h
}

// LB-1 — the reuse predicate (B-symptom), over a REAL harvester snapshot count.
func TestLeverB_ReusePredicate_WalkVsReuse(t *testing.T) {
	full := leverBHarvestN(3)
	if got := len(full.snapshot()); got != 3 {
		t.Fatalf("setup: expected 3 harvested widgets; snapshot len=%d", got)
	}
	empty := newNavWidgetHarvester()
	if got := len(empty.snapshot()); got != 0 {
		t.Fatalf("setup: expected empty harvester; snapshot len=%d", got)
	}

	// pass 0 (fresh enqueue / Forget-reset config-vars redrive) → WALK.
	if !bootShouldWalk(0, len(full.snapshot())) {
		t.Fatal("LB-1: attempt==0 MUST walk")
	}
	if !bootShouldWalk(0, len(empty.snapshot())) {
		t.Fatal("LB-1: attempt==0 MUST walk even with an empty harvester")
	}
	// F.4 resume (attempt>0) with a populated harvester → REUSE (the ~255s save).
	if bootShouldWalk(1, len(full.snapshot())) {
		t.Fatal("LB-1: attempt>0 with a populated harvester MUST reuse (skip the re-walk)")
	}
	if bootShouldWalk(5, len(full.snapshot())) {
		t.Fatal("LB-1: any attempt>0 with a populated harvester MUST reuse")
	}
	// GUARD: attempt>0 but EMPTY harvester → WALK (boot-race give-up self-heal).
	if !bootShouldWalk(1, len(empty.snapshot())) {
		t.Fatal("LB-1 GUARD: attempt>0 with an EMPTY harvester MUST walk (nothing to reuse; the give-up→appear self-heal path). An always-reuse-on-attempt>0 predicate is RED here.")
	}
	leverBArtifact(t, "lb1_reuse_predicate.txt",
		"bootShouldWalk: (0,3)=walk (0,0)=walk (1,3)=reuse (5,3)=reuse (1,0)=WALK-guard. Mutation (drop empty-guard) → (1,0) reuses empty → RED.")
}

// LB-2 — the invalidation seam (B-safety, LOAD-BEARING).
func TestLeverB_ConfigVarsRedrive_ResetsAttemptSoNewTopologyReWalks(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	boot := prewarmScope{kind: scopeKindBoot}
	const harvested = 3 // a populated harvester = the PRIOR topology's nav set

	// Model an in-flight F.4 deadline-cut requeue streak: each AddRateLimited
	// bumps the rate limiter's failure count (NumRequeues), independent of the
	// delay-queue timing.
	streak := func(e *prewarmEngine) int {
		e.queue.AddRateLimited(boot)
		e.queue.AddRateLimited(boot)
		return e.queue.NumRequeues(boot)
	}

	// GREEN — the PROD redrive sequence (enqueueBootReDrive): forgetScope THEN
	// enqueueScope → NumRequeues reset to 0 → next dequeue is attempt==0 → WALK.
	eg := newTestEngine()
	if n := streak(eg); n == 0 {
		t.Fatalf("LB-2 setup: expected a non-zero requeue streak before the redrive; NumRequeues=%d", n)
	}
	eg.forgetScope(boot)  // <-- the Lever B invalidation seam
	eg.enqueueScope(boot) // plain Add — does NOT touch NumRequeues
	greenAttempt := eg.queue.NumRequeues(boot)
	if greenAttempt != 0 {
		t.Fatalf("LB-2 GREEN: forgetScope must reset the boot requeue count to 0; NumRequeues=%d", greenAttempt)
	}
	if !bootShouldWalk(greenAttempt, harvested) {
		t.Fatalf("LB-2 GREEN: after the redrive's forgetScope, attempt=%d + populated harvester MUST re-WALK the new topology", greenAttempt)
	}

	// RED — the MUTATION (drop forgetScope; the pre-fix WIP): plain enqueueScope
	// only. NumRequeues persists >0 → attempt>0 → bootShouldWalk REUSES the stale
	// nav set across the topology change (the bug).
	er := newTestEngine()
	if n := streak(er); n == 0 {
		t.Fatalf("LB-2 RED setup: expected a non-zero requeue streak; NumRequeues=%d", n)
	}
	// (no forgetScope — the mutation)
	er.enqueueScope(boot)
	redAttempt := er.queue.NumRequeues(boot)
	if redAttempt == 0 {
		t.Fatalf("LB-2 RED: without forgetScope the requeue count MUST persist (>0); NumRequeues=%d — if enqueueScope reset it, the seam would be unnecessary", redAttempt)
	}
	if bootShouldWalk(redAttempt, harvested) {
		t.Fatalf("LB-2 RED: the mutation must REUSE the stale set (bootShouldWalk==false), but it walked; attempt=%d", redAttempt)
	}

	// The discriminator: the forgetScope seam FLIPS the decision — GREEN walks,
	// RED reuses. If both agree, the seam did not discriminate.
	if bootShouldWalk(greenAttempt, harvested) == bootShouldWalk(redAttempt, harvested) {
		t.Fatal("LB-2: the forgetScope seam must CHANGE the walk decision (GREEN walk vs RED reuse); it did not discriminate")
	}
	leverBArtifact(t, "lb2_invalidation_seam.txt",
		fmt.Sprintf("GREEN redrive (forgetScope+enqueueScope): NumRequeues=%d walk=%v (new topology re-walked).\nRED mutation (enqueueScope only): NumRequeues=%d walk=%v (STALE reuse — the bug the seam prevents).",
			greenAttempt, bootShouldWalk(greenAttempt, harvested), redAttempt, bootShouldWalk(redAttempt, harvested)))
}
