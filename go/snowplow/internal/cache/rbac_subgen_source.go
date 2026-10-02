// rbac_subgen_source.go — #260 Phase-0 linchpin: source-attributed sub-gen
// bumps. bumps_total (rbac_subgen_expvar.go) collapses every source — binding
// ADD/UPDATE/DELETE, role ADD/UPDATE/DELETE, CDC ServiceAccount churn — into one
// unattributed count, so a spike alert cannot tell CDC churn (rotates zero cells
// at scale) from a real fleet-wide rotation. This file adds a BOUNDED per-source
// breakdown (6 fixed compile-time keys — no per-subject/per-RA cardinality),
// counted at flush on the SAME single writer as bumps_total (the chain of
// necessity is preserved). It is the substrate #258 (reseed) / #259 (reclaim)
// consume and the amendment-2 item-10 CDC-churn alert reads. INSTRUMENT ONLY —
// no behaviour change, no RV-keyed skip.

package cache

import "sync/atomic"

// subGenBumpSource is a bitmask of the event sources that recorded a sub-gen
// bump for a subject within one debounce window. A subject bumped by more than
// one source in the window (e.g. a binding UPDATE that also re-routed a role)
// carries every contributing bit; flush counts each set bit.
type subGenBumpSource uint8

const (
	bumpSrcBindingAdd subGenBumpSource = 1 << iota
	bumpSrcBindingUpdate
	bumpSrcBindingDelete
	bumpSrcRoleAdd
	bumpSrcRoleUpdate
	bumpSrcRoleDelete
)

// bumpSrcWiden is a #258 TAG bit, NOT a #260 source. It is OR'd into a subject's
// pending mask when a WIDENING content change (a binding that gained subjects or
// changed its roleRef, or a non-semantic-noop role update) means the reseed must
// do a per-subject scoped WALK rather than reuse the harvester snapshot (new
// topology may be visible). It is DELIBERATELY NOT counted by
// countSubGenBumpSources and is NOT a key in snowplow_rbac_subgen_bumps_by_source_total
// — it rides the same uint8 mask ONLY to reach #258's RotatedSubjectSet.Widening.
// Pinned at 1<<6, above the six source bits (1<<0..1<<5), so it never collides
// with a source and the by-source sum/attribution invariant is unchanged.
const bumpSrcWiden subGenBumpSource = 1 << 6

// The six fixed metric label keys (bounded, compile-time).
const (
	bumpSrcKeyBindingAdd    = "binding_add"
	bumpSrcKeyBindingUpdate = "binding_update"
	bumpSrcKeyBindingDelete = "binding_delete"
	bumpSrcKeyRoleAdd       = "role_add"
	bumpSrcKeyRoleUpdate    = "role_update"
	bumpSrcKeyRoleDelete    = "role_delete"
)

var subGenBumpsBySource struct {
	bindingAdd    atomic.Uint64
	bindingUpdate atomic.Uint64
	bindingDelete atomic.Uint64
	roleAdd       atomic.Uint64
	roleUpdate    atomic.Uint64
	roleDelete    atomic.Uint64
}

// countSubGenBumpSources increments, per set bit of mask, the matching
// per-source counter. Called from flushPendingSubGenBumps once per drained
// subject, so the attribution counts (subject, source, publish) exactly as
// bumps_total counts (subject, publish) — same single writer, same dedup.
func countSubGenBumpSources(mask subGenBumpSource) {
	if mask&bumpSrcBindingAdd != 0 {
		subGenBumpsBySource.bindingAdd.Add(1)
	}
	if mask&bumpSrcBindingUpdate != 0 {
		subGenBumpsBySource.bindingUpdate.Add(1)
	}
	if mask&bumpSrcBindingDelete != 0 {
		subGenBumpsBySource.bindingDelete.Add(1)
	}
	if mask&bumpSrcRoleAdd != 0 {
		subGenBumpsBySource.roleAdd.Add(1)
	}
	if mask&bumpSrcRoleUpdate != 0 {
		subGenBumpsBySource.roleUpdate.Add(1)
	}
	if mask&bumpSrcRoleDelete != 0 {
		subGenBumpsBySource.roleDelete.Add(1)
	}
}

// RBACSubGenBumpsBySource returns the process-wide bump count for one source
// label. Exported for the §3 per-source liveness assertions (a source with no
// counter prints NOT OBSERVED, never 0).
func RBACSubGenBumpsBySource(key string) uint64 {
	switch key {
	case bumpSrcKeyBindingAdd:
		return subGenBumpsBySource.bindingAdd.Load()
	case bumpSrcKeyBindingUpdate:
		return subGenBumpsBySource.bindingUpdate.Load()
	case bumpSrcKeyBindingDelete:
		return subGenBumpsBySource.bindingDelete.Load()
	case bumpSrcKeyRoleAdd:
		return subGenBumpsBySource.roleAdd.Load()
	case bumpSrcKeyRoleUpdate:
		return subGenBumpsBySource.roleUpdate.Load()
	case bumpSrcKeyRoleDelete:
		return subGenBumpsBySource.roleDelete.Load()
	default:
		return 0
	}
}

// subGenBumpsBySourceSnapshot is the labeled expvar value — exactly the 6
// bounded keys. Published under snowplow_rbac_subgen_bumps_by_source_total in
// RegisterRBACSubGenExpvar (rbac_subgen_expvar.go), the same main()-bootstrap,
// cache-mode-agnostic surface as bumps_total.
func subGenBumpsBySourceSnapshot() any {
	return map[string]uint64{
		bumpSrcKeyBindingAdd:    subGenBumpsBySource.bindingAdd.Load(),
		bumpSrcKeyBindingUpdate: subGenBumpsBySource.bindingUpdate.Load(),
		bumpSrcKeyBindingDelete: subGenBumpsBySource.bindingDelete.Load(),
		bumpSrcKeyRoleAdd:       subGenBumpsBySource.roleAdd.Load(),
		bumpSrcKeyRoleUpdate:    subGenBumpsBySource.roleUpdate.Load(),
		bumpSrcKeyRoleDelete:    subGenBumpsBySource.roleDelete.Load(),
	}
}

// RBACSubGenBumpsBySourceSnapshot returns the 6 bounded per-source bump counts,
// exported so the OTLP layer (metrics.go, #260 change-4) can range the same map
// the expvar surface does (C7 anti-drift).
func RBACSubGenBumpsBySourceSnapshot() map[string]uint64 {
	return map[string]uint64{
		bumpSrcKeyBindingAdd:    subGenBumpsBySource.bindingAdd.Load(),
		bumpSrcKeyBindingUpdate: subGenBumpsBySource.bindingUpdate.Load(),
		bumpSrcKeyBindingDelete: subGenBumpsBySource.bindingDelete.Load(),
		bumpSrcKeyRoleAdd:       subGenBumpsBySource.roleAdd.Load(),
		bumpSrcKeyRoleUpdate:    subGenBumpsBySource.roleUpdate.Load(),
		bumpSrcKeyRoleDelete:    subGenBumpsBySource.roleDelete.Load(),
	}
}

// ResetSubGenBumpsBySourceForTest zeroes the per-source counters. TEST-ONLY —
// production never resets (monotonic process-wide counters).
func ResetSubGenBumpsBySourceForTest() {
	subGenBumpsBySource.bindingAdd.Store(0)
	subGenBumpsBySource.bindingUpdate.Store(0)
	subGenBumpsBySource.bindingDelete.Store(0)
	subGenBumpsBySource.roleAdd.Store(0)
	subGenBumpsBySource.roleUpdate.Store(0)
	subGenBumpsBySource.roleDelete.Store(0)
}
