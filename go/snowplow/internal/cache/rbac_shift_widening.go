// rbac_shift_widening.go — #258. Content-classification of WIDENING rotations at
// the delta site, and the fallback counter. A rotation is WIDENING when it can
// expand a subject's visible topology (so a snapshot reuse would leave
// newly-visible cells cold and the reseed must do a per-subject scoped walk):
//   - binding UPDATE that GAINED subjects or changed its roleRef;
//   - (role UPDATE widening is tagged inline in onRoleRulesChanged — a real,
//     non-semantic-noop rule change, fail-toward-warm.)
//
// Widening is signalled by OR-ing the bumpSrcWiden TAG onto the affected
// subjects' pending mask. The tag is NOT a #260 source: it never enters
// snowplow_rbac_subgen_bumps_by_source_total and countSubGenBumpSources ignores
// it, so the by-source sum/attribution invariant is unchanged. A NON-widening
// update (pure re-delivery: same subjects, same roleRef — the #253 relist-storm
// shape) tags nothing, so its subjects keep only bumpSrcBindingUpdate and take
// the cheap snapshot re-key path.

package cache

import (
	"expvar"
	"sync/atomic"
)

// unclassifiedWideningBumps counts rotations tagged WIDENING by FALLBACK — a
// binding UPDATE whose OLD object did not normalise, so gained-vs-same cannot be
// determined and the rotation fails toward WARM (widening). Exposed + published
// so we can see whether the fallback fires at storm scale (#258 / TL). NOT a
// #260 by-source key.
var unclassifiedWideningBumps atomic.Uint64

// UnclassifiedWideningBumpsTotal returns the fallback-widening count.
func UnclassifiedWideningBumpsTotal() uint64 { return unclassifiedWideningBumps.Load() }

func init() {
	expvar.Publish("snowplow_rbac_shift_unclassified_widening_total", expvar.Func(func() any {
		return UnclassifiedWideningBumpsTotal()
	}))
}

// recordBindingUpdateWidening OR's the bumpSrcWiden TAG onto the subjects of a
// binding UPDATE whose content WIDENED. Called from onBindingUpdate AFTER
// recordBindingUpdateNoop; it is additive to the existing bumpSrcBindingUpdate
// bumps (so the #260 binding_update count is unchanged — the subject drains once
// with mask = binding_update|widen, and countSubGenBumpSources counts
// binding_update exactly once, ignoring the tag).
func recordBindingUpdateWidening(oldSide, newSide bindingUpdateSide) {
	if len(newSide.subjects) == 0 {
		return // new side did not normalise / has no subjects → nothing to re-warm
	}
	if oldSide.uid == "" {
		// OLD object did not normalise → cannot tell gained-vs-same → fail toward WARM.
		unclassifiedWideningBumps.Add(1)
		recordPendingSubGenBumps(newSide.subjects, bumpSrcWiden)
		return
	}
	if oldSide.roleRef != newSide.roleRef {
		// roleRef changed → every new-side subject may reach new access via the new role.
		recordPendingSubGenBumps(newSide.subjects, bumpSrcWiden)
		return
	}
	// Same roleRef → only the subjects GAINED (in new, not in old) widened.
	if gained := subjectsGained(oldSide.subjects, newSide.subjects); len(gained) > 0 {
		recordPendingSubGenBumps(gained, bumpSrcWiden)
	}
}

// subjectsGained returns the subjects present in updated but not in old.
func subjectsGained(old, updated []subjectKey) []subjectKey {
	if len(old) == 0 {
		return updated
	}
	oldSet := make(map[subjectKey]struct{}, len(old))
	for _, s := range old {
		oldSet[s] = struct{}{}
	}
	var gained []subjectKey
	for _, s := range updated {
		if _, ok := oldSet[s]; !ok {
			gained = append(gained, s)
		}
	}
	return gained
}
