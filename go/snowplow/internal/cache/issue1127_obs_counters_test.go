// issue1127_obs_counters_test.go — 1.12.7 observability, cache side.
//
// One arm per counter, each shaped to FAIL IF THE COUNTER STOPS MOVING ON THE
// EVENT IT NAMES. That is the only property worth pinning for a diagnostic: a
// counter that silently stops counting is worse than no counter, because the
// zero reads as health. Each arm therefore drives the real event and asserts a
// delta, and each also asserts the counter does NOT move on the neighbouring
// event it must not claim.
package cache

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func obsGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
}

// TestIssue1127Obs_DegradedNoEvict_CountsTheConsequenceNotTheDegrade — the
// counter must move when a DEGRADED verdict reaches dependent entries and
// evicts nothing, and must not move for the verdicts that do evict or that had
// nothing at stake.
func TestIssue1127Obs_DegradedNoEvict_CountsTheConsequence(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	gvr := obsGVR()
	const ns, name, key = "demo-system", "obs-flex", "L1_obs-flex"
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &ResolvedKeyInputs{
		CacheEntryClass: "widgets", Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource,
		Namespace: ns, Name: name,
	}})
	Deps().Record(key, gvr, ns, name)

	// A degraded verdict with a dependent entry: nothing can be evicted, and
	// that is the consequence worth counting.
	before := Deps().Stats().OnObjectEventDegradedNoEvict
	Deps().OnObjectEvent(gvr, ns, name, objUnknownDegraded)
	if got := Deps().Stats().OnObjectEventDegradedNoEvict - before; got != 1 {
		t.Fatalf("RED (1.12.7 obs): a DEGRADED verdict that reached a dependent entry moved "+
			"on_object_event_degraded_no_evict_total by %d, want 1. The consequence of a degrade — "+
			"an eviction decision deferred while the entry stays resident — is invisible again", got)
	}
	if _, alive := store.Get(key); !alive {
		t.Fatalf("premise: the degraded verdict evicted the entry; it must never evict")
	}

	// An EXISTS verdict is a dirty-mark, not a degrade.
	before = Deps().Stats().OnObjectEventDegradedNoEvict
	Deps().OnObjectEvent(gvr, ns, name, objExists)
	if got := Deps().Stats().OnObjectEventDegradedNoEvict; got != before {
		t.Fatalf("RED (1.12.7 obs): an EXISTS verdict moved the degraded counter by %d", got-before)
	}

	// An ABSENT verdict evicts; it is the opposite of this counter's meaning.
	before = Deps().Stats().OnObjectEventDegradedNoEvict
	Deps().OnObjectEvent(gvr, ns, name, objAbsent)
	if got := Deps().Stats().OnObjectEventDegradedNoEvict; got != before {
		t.Fatalf("RED (1.12.7 obs): an ABSENT verdict moved the degraded counter by %d", got-before)
	}

	// A degraded verdict for a coordinate NOTHING depends on cost nothing, so
	// it must not be counted — otherwise the number is dominated by events
	// that had nothing at stake and the ones that did are buried.
	before = Deps().Stats().OnObjectEventDegradedNoEvict
	Deps().OnObjectEvent(gvr, ns, "nothing-depends-on-me", objUnknownDegraded)
	if got := Deps().Stats().OnObjectEventDegradedNoEvict; got != before {
		t.Fatalf("RED (1.12.7 obs): a degraded verdict with an EMPTY match set moved the counter "+
			"by %d. Nothing was at stake, so counting it buries the events that were", got-before)
	}
}

// TestIssue1127Obs_ConfirmRetracted_NotCountedForANeverConfirmedGVR — the
// retraction sites delete unconditionally, so this is the easy mistake: count
// the call and the number climbs on every ordinary teardown.
func TestIssue1127Obs_ConfirmRetracted_NotCountedForANeverConfirmedGVR(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetInformerWatchStatsForTest()
	t.Cleanup(ResetInformerWatchStatsForTest)

	rw := newSyntheticRemoveWatcher(t, obsGVR())
	t.Cleanup(rw.Stop)
	gvr := obsGVR()

	before := InformerWatchStatsSnapshot().ConfirmRetractedTotal
	// Never confirmed — tearing it down retracts nothing.
	rw.RemoveResourceType(gvr)
	if got := InformerWatchStatsSnapshot().ConfirmRetractedTotal; got != before {
		t.Fatalf("RED (1.12.7 obs): tearing down a GVR that was NEVER confirmed moved "+
			"confirm_retracted_total by %d. The delete is unconditional, so counting the call "+
			"rather than the effect makes this climb on every ordinary teardown and the signal "+
			"is lost in the noise", got-before)
	}

	// Now confirm it, then tear it down: that IS a retraction.
	rw.mu.Lock()
	rw.ensureConfirmMapsLocked()
	rw.confirmed[gvr] = struct{}{}
	rw.mu.Unlock()

	before = InformerWatchStatsSnapshot().ConfirmRetractedTotal
	rw.removeResourceTypeWithReason(gvr, confirmRetractCRDDeleted)
	if got := InformerWatchStatsSnapshot().ConfirmRetractedTotal - before; got != 1 {
		t.Fatalf("RED (1.12.7 obs): tearing down a CONFIRMED GVR moved confirm_retracted_total "+
			"by %d, want 1 — a GVR silently stopped serving from its informer and nothing counted "+
			"it, which is why #217 took a day", got)
	}
	if by := ConfirmRetractedByReasonSnapshot(); by[confirmRetractCRDDeleted] != 1 {
		t.Fatalf("1.12.7 obs: the teardown was not attributed to crd_deleted: %v", by)
	}
}
