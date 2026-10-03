// prewarm_rbac_shift.go — #258. The RBAC-shift scoped-reseed engine path: the
// cache→engine hook registration, and the scope handler that reseeds the rotated
// cohorts' NEW-sub-gen cells under their current identity (so a rotated identity's
// next navigation is a warm L1 hit, not a cold fill). Mirrors the Ship-2
// scopeKindGVRDiscovered wiring (registerEngineGVRDiscoveredHook) with a
// merge-accumulator payload (the rotated subjects) instead of a single GVR.

package dispatchers

import (
	"context"
	"log/slog"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// registerEngineRBACShiftHook subscribes the engine to the cache-side RBAC-shift
// hook (#258). Called once from StartPrewarmEngine's startedOnce.Do, BEFORE the
// worker spawns, so a flush during boot is accumulated + queued, not dropped.
//
// The callback is intentionally TINY and NON-BLOCKING (the cache fires it
// synchronously on the sub-gen flush / rebuildRBACSnapshot publish goroutine —
// see RegisterRBACShiftHook's contract): MERGE the flush's rotated subjects into
// the engine's merge-accumulator and enqueue the PAYLOAD-FREE scopeKindRBACShift
// scope. The scope coalesces on identity (payload-free), and client-go marks an
// in-flight scope dirty on re-enqueue, so a burst of flushes collapses to one
// run and a rotation arriving mid-reseed re-arms exactly one follow-up
// (≤2 reseeds per burst). The scoped reseed itself runs on the engine worker
// (rePrewarmRBACShift), customer-priority-yielded + bounded.
func registerEngineRBACShiftHook(e *prewarmEngine) {
	cache.RegisterRBACShiftHook(func(rotated cache.RotatedSubjectSet) {
		if rotated.Len() == 0 {
			return
		}
		e.rbacShift.Merge(rotated)
		e.enqueueScope(prewarmScope{kind: scopeKindRBACShift})
	})
}

// rePrewarmRBACShift is the #258 scopeKindRBACShift handler. It reseeds ONLY the
// resident cohorts' cells whose folded subject is in the drained `rotated` set,
// under their current identity, so the rotated identity's NEW per-subject-sub-gen
// SERVE key is minted warm. Reachability is per-subject (RotatedSubjectSet.Rotated),
// matching the key fold (RBACSubGenForSubject): a target's key rotates IFF its
// folded subject bumped.
//
// WIDENING needs no separate walk for resident units: the unit side of the
// enumeration is the identity-free harvester snapshot and the identity side is
// the LIVE binding index read at reseed time, so a subject newly authorized for a
// resident unit is already in the reseed set (TestS258_WideningSubjectAppearsInReseedSet
// drives a real binding ADD through the informer → flush → hook path). Units the
// harvester has never seen (a runtime-new GVR / CR) are scopeKindGVRDiscovered's
// domain, not this scope's.
//
// BOUND (C1 sizing in the PR): work per run = |rotated ∩ resident (unit × identity)|
// re-resolves, never a walk; one run in flight + one coalesced pending union per
// burst; each target yields to customers (engineYieldCheckpoint) and is bounded by
// pipCohortTimeout. No-op when nothing drained.
func rePrewarmRBACShift(ctx context.Context, deps rePrewarmDeps, rotated cache.RotatedSubjectSet) error {
	if rotated.Len() == 0 {
		// Coalesced away / already drained by a prior run — nothing to do.
		return nil
	}
	slog.Info("prewarm.engine.rbac_shift.start",
		slog.String("subsystem", "cache"),
		slog.Int("rotated_subjects", rotated.Len()),
	)

	// #262 — a rotation can make a learned class DISTINCT (a binding added on a
	// group it presents moves its binding set away from its representative's);
	// re-apply the learned-class bound over the resident units first, so the
	// reseed below (which reseeds only ADMITTED learned classes) includes it.
	refreshLearnedAdmission(ctx, deps)

	// Q1 TARGET-FILTER (snapshot-reuse re-key): reseed the RESIDENT (unit ×
	// identity) targets whose folded subject rotated — no fresh nav walk.
	reqs := enumerateRotatedResidentTargets(ctx, deps, rotated)
	reEnqueue := reseedTargets(ctx, deps, reqs)

	if cut := ctx.Err() != nil; cut || len(reEnqueue) > 0 {
		// The run was CUT (ctx done — during the enumeration, which returns a
		// partial target list, or during the reseed loop, which returns its
		// unprocessed tail): the drained rotation must not be dropped
		// (no-dropped-rotation). Re-merge the WHOLE drained set and re-arm the
		// payload-free scope; the workqueue coalesces it to a single follow-up run,
		// and the seedModeRBACShift liveness skip makes the already-minted prefix
		// cost a read, not a resolve. Gated on ctx.Err() too, not only on a tail:
		// a cut inside the enumeration can leave reqs (and so the tail) empty. A
		// refused terminal Put is NOT in reEnqueue — it took the #394 one-shot
		// inline re-seed (reseedWithRefusalPolicy), so one removed cell never
		// re-runs the whole set.
		if e := prewarmEngineSingleton(); e != nil {
			e.rbacShift.Merge(rotated)
			e.enqueueScope(prewarmScope{kind: scopeKindRBACShift})
		}
	}

	slog.Info("prewarm.engine.rbac_shift.done",
		slog.String("subsystem", "cache"),
		slog.Int("rotated_subjects", rotated.Len()),
		slog.Int("targets", len(reqs)),
		slog.Int("processed", len(reqs)-len(reEnqueue)),
		slog.Int("re_enqueued", len(reEnqueue)),
	)
	return nil
}
