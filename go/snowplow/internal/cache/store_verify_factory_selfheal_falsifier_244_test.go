package cache

// store_verify_factory_selfheal_falsifier_244_test.go — #244.
//
// A factory-built (!ownsInformer) GVR whose indexer diverges from the apiserver
// CANNOT be relist-repaired (a shared-factory teardown hands back the STOPPED
// informer), so enqueueStoreRepair REFUSES it and the ONLY repair is the passive
// watch-reconnect self-heal: the reflector re-LISTs on the next watch
// re-establishment and Replaces the indexer (proven end-to-end by the real
// setSilently→breakWatches cycle in store_verify_falsifier_test.go, where the
// store catches up to rv=2 after the reconnect).
//
// The FALSE PREMISE this class invites — "just rebuild the RBAC snapshot" — is
// debunked in TestFactory244_Debunk_RebuildRBACSnapshotCannotClearIndexerDivergence:
// rebuildRBACSnapshot re-derives the snapshot FROM the stale indexer, so it
// cannot clear an indexer-vs-apiserver divergence, and wiring it would falsely
// latch the repair_ineffective breaker.
//
// This file locks: (1) the REFUSAL (active repair unavailable for the factory
// class → passive self-heal is the sole path), and (2) the
// store_factory_divergence_age_seconds signal that makes the bounded self-heal
// observable AND stuck-detectable — the age-discriminating detector (pm C1),
// read against the cluster's watch-reconnect cadence (--min-request-timeout).

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var factory244GVR = schema.GroupVersionResource{
	Group: "widgets.krateo.io", Version: "v1beta1", Resource: "factory244panels",
}
var owned244GVR = schema.GroupVersionResource{
	Group: "widgets.krateo.io", Version: "v1beta1", Resource: "owned244panels",
}

// factory244Since reads a GVR's factoryDivergentSince directly (package-internal).
func factory244Since(gvr schema.GroupVersionResource) time.Time {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	if st, ok := storeVerify.gvrs[gvr]; ok {
		return st.factoryDivergentSince
	}
	return time.Time{}
}

// factory244Backdate ages a GVR's divergence by d, to exercise the age math /
// the within-window-vs-stuck discrimination without a real wall-clock wait.
func factory244Backdate(gvr schema.GroupVersionResource, d time.Duration) {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	storeVerify.gvrs[gvr].factoryDivergentSince = time.Now().Add(-d)
}

func TestFactory244_RefusedAndAgeSignalDiscriminates(t *testing.T) {
	resetVerificationForArm(t)

	// A factory-built (!ownsInformer) GVR: its shared-factory informer cannot be
	// torn down + rebuilt, so the active relist-repair is refused.
	rememberStoreVerification(factory244GVR, true, false)

	// (1) REFUSAL — the active repair verb is UNAVAILABLE for the factory class,
	// so the passive self-heal is the sole path.
	before := storeRepairUnsupportedTotal.Load()
	if enqueueStoreRepair(factory244GVR, "div") {
		t.Fatal("#244: a factory-built GVR must be REFUSED — a shared-factory teardown hands back " +
			"the STOPPED informer and kills the cache; the active relist-repair is not available")
	}
	if got := storeRepairUnsupportedTotal.Load(); got != before+1 {
		t.Fatalf("#244: the refusal must be counted (store_repair_unsupported_total); got %d want %d", got, before+1)
	}

	// (1b) POSITIVE CONTROL — the refusal is CLASS-DISCRIMINATING (the !ownsInformer
	// boundary), NOT "repair is off everywhere". An OWNED-informer GVR is ACCEPTED
	// (relistGVRForRepair IS enqueued) and does NOT advance store_repair_unsupported_total.
	// Without this the factory-refused assertion could pass vacuously if
	// enqueueStoreRepair refused every GVR.
	rememberStoreVerification(owned244GVR, true, true)
	unsupBefore := storeRepairUnsupportedTotal.Load()
	if !enqueueStoreRepair(owned244GVR, "owned") {
		t.Fatal("#244 positive control: an OWNED GVR must be ACCEPTED — its informer can be torn down + " +
			"rebuilt; a refusal here would mean the predicate is not reading ownership, or repair is " +
			"globally off (the vacuous pass this control guards against)")
	}
	if got := storeRepairUnsupportedTotal.Load(); got != unsupBefore {
		t.Fatalf("#244 positive control: an owned GVR's acceptance must NOT increment "+
			"store_repair_unsupported_total; it moved %d→%d", unsupBefore, got)
	}

	// (2) AGE SIGNAL OFF before any divergence.
	if got := FactoryDivergenceMaxAgeSeconds(); got != 0 {
		t.Fatalf("#244: age must be 0 with no unrepaired factory divergence; got %d", got)
	}

	// (3) A real verify pass finds it divergent → the age signal turns ON
	// (noteVerified is the production sink that sets factoryDivergentSince).
	noteVerified(factory244GVR, 5, 1)
	if factory244Since(factory244GVR).IsZero() {
		t.Fatal("#244: a divergent verify pass on a factory GVR must SET factoryDivergentSince (age signal on)")
	}

	// (4) DISCRIMINATION (pm C1): the age reads within-window vs stuck differently.
	// A boolean "awaiting self-heal" could not tell a healthy 30s divergence from
	// a stuck 2h one; the AGE can, read against the cluster's --min-request-timeout.
	factory244Backdate(factory244GVR, 30*time.Second)
	if got := FactoryDivergenceMaxAgeSeconds(); got < 25 || got > 45 {
		t.Fatalf("#244 within-window age = %ds, want ~30 (an operator reads this as expected self-heal)", got)
	}
	factory244Backdate(factory244GVR, 2*time.Hour)
	if got := FactoryDivergenceMaxAgeSeconds(); got < 7000 {
		t.Fatalf("#244 stuck age = %ds, want ~7200 (beyond any --min-request-timeout window = genuinely stuck)", got)
	}

	// (5) SELF-HEAL CLEARS it: a clean verify pass (the reflector re-List landed)
	// resets the age to 0 — the bounded self-heal is observable end-to-end.
	noteVerified(factory244GVR, 5, 0)
	if got := FactoryDivergenceMaxAgeSeconds(); got != 0 {
		t.Fatalf("#244: age must reset to 0 after a clean verify (self-heal landed); got %d", got)
	}
	if !factory244Since(factory244GVR).IsZero() {
		t.Fatal("#244: factoryDivergentSince must be cleared after a clean verify")
	}
}

func TestFactory244_OwnedGVRNeverAges(t *testing.T) {
	resetVerificationForArm(t)
	// An OWNED GVR is relist-repaired, so it must NEVER contribute to the
	// factory-divergence age signal — else the signal would fire for a class the
	// active repair already handles, and the refusal predicate (ownership, not
	// group name) would be silently bypassed.
	rememberStoreVerification(owned244GVR, true, true)
	noteVerified(owned244GVR, 5, 1) // divergent, but owned
	if !factory244Since(owned244GVR).IsZero() {
		t.Fatal("#244: an OWNED GVR must never set factoryDivergentSince (it is relist-repaired)")
	}
	if got := FactoryDivergenceMaxAgeSeconds(); got != 0 {
		t.Fatalf("#244: an owned GVR's divergence must not appear in the factory age signal; got %d", got)
	}
}

// TestFactory244_SelfHeal_ReflectorRelistRefreshesIndexer proves the (c)
// resolution: an indexer-vs-apiserver divergence is cleared by the reflector's
// own re-List on the next watch RE-ESTABLISHMENT — the passive self-heal a
// factory informer relies on because it cannot be torn down. Drives the REAL
// boundary (a real Reflector over a mutable source): setSilently is the lost
// event, breakWatches is the reconnect. RED-capture: WITHOUT the reconnect the
// indexer stays stale (the divergence persists), which is exactly why this
// class needs the self-heal.
func TestFactory244_SelfHeal_ReflectorRelistRefreshesIndexer(t *testing.T) {
	resetVerificationForArm(t)
	src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
	inf, _ := verifiedInformer(t, src)

	if rv, ok := storeRV(t, inf, "krateo/hdr"); !ok || rv != "1" {
		t.Fatalf("precondition: store rv=%q ok=%v, want 1", rv, ok)
	}

	// THE LOST EVENT — the authoritative set moves with NO watch event: a real
	// indexer-vs-apiserver divergence, the shape a factory GVR cannot relist-repair.
	src.setSilently(fakeObj{ns: "krateo", name: "hdr", rv: "2", uid: "u-1"})

	// RED-CAPTURE — WITHOUT a watch re-establishment the indexer stays stale; the
	// divergence persists. (rebuildRBACSnapshot, which only re-derives from THIS
	// stale indexer, would leave it here too — see the debunk arm.)
	time.Sleep(50 * time.Millisecond)
	if rv, _ := storeRV(t, inf, "krateo/hdr"); rv != "1" {
		t.Fatalf("#244 RED-capture: without a reconnect the indexer must stay stale at rv=1; got %q", rv)
	}

	// THE PASSIVE SELF-HEAL — a real watch re-establishment: ListAndWatch returns
	// and BackoffUntil re-invokes it → a fresh apiserver LIST Replaces the indexer.
	// No teardown; this is the reflector's own loop, available to a factory informer.
	src.breakWatches()

	waitForVerify(t, "the reflector re-List to refresh the indexer (self-heal)", verifyBound, func() bool {
		rv, ok := storeRV(t, inf, "krateo/hdr")
		return ok && rv == "2"
	})
}

// TestFactory244_Debunk_RebuildRBACSnapshotCannotClearIndexerDivergence is the
// load-bearing false-premise debunk, proven EMPIRICALLY (not asserted). Store
// verification compares the raw indexer (inf.GetIndexer()) to a fresh apiserver
// LIST, so its divergence is indexer-vs-apiserver. rebuildRBACSnapshot re-derives
// the snapshot by READING that same indexer — it recovers nothing the indexer
// lost — so it CANNOT clear such a divergence; it only PROPAGATES the staleness,
// and wiring it as the repair would falsely latch the store_repair_ineffective
// breaker. The real repair is the reflector's re-List on watch reconnect
// (TestFactory244_SelfHeal), which refreshes the indexer from the apiserver.
//
// Discriminating: if rebuildRBACSnapshot ever issued an apiserver LIST (the
// tempting "fix"), it would recover the lost object and this arm would FAIL.
func TestFactory244_Debunk_RebuildRBACSnapshotCannotClearIndexerDivergence(t *testing.T) {
	crb := mkCRB("crb-a", userSub("alice"))
	rw := newSnapshotTestWatcher(t, crb)

	// Precondition: the snapshot authorizes alice via crb-a.
	if !snapshot244HasCRB(rw.Snapshot(), "crb-a") {
		t.Fatalf("precondition: snapshot must contain crb-a after seed+sync")
	}

	// Inject an indexer-vs-apiserver divergence: the INDEXER loses crb-a while the
	// apiserver (fake) still has it — a lost-DELETE-shaped stale indexer, exactly
	// the shape store verification detects and a factory GVR cannot relist-repair.
	rw.mu.RLock()
	gi := rw.informers[clusterRoleBindingsTypedGVR]
	rw.mu.RUnlock()
	idx := gi.Informer().GetIndexer()
	if err := idx.Delete(crb); err != nil {
		t.Fatalf("indexer.Delete(crb-a): %v", err)
	}

	// THE DEBUNK — rebuildRBACSnapshot re-derives from the stale indexer. If it
	// could clear the divergence it would re-List crb-a from the apiserver and the
	// snapshot would keep it. It does not: the staleness PROPAGATES.
	RebuildRBACSnapshotForTest(rw)
	if snapshot244HasCRB(rw.Snapshot(), "crb-a") {
		t.Fatalf("#244 DEBUNK FAILED: rebuildRBACSnapshot RECOVERED crb-a after the indexer lost it — " +
			"that would mean it issued an apiserver LIST, which it does not. It re-derives from the " +
			"indexer, so it CANNOT clear an indexer-vs-apiserver divergence; wiring it as the repair " +
			"would falsely latch the ineffective breaker. The real repair is the reflector re-List.")
	}
}

func snapshot244HasCRB(s *RBACSnapshot, name string) bool {
	if s == nil {
		return false
	}
	for _, crb := range s.ClusterRoleBindings {
		if crb.Name == name {
			return true
		}
	}
	return false
}
