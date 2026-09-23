package cache

// store_verify_falsifier_test.go — the falsifiers for #237 deliverable B.
//
// Every arm drives a REAL SharedIndexInformer over a REAL Reflector with the
// decorator in the position production puts it, and performs at least TWO real
// reflector invocations: an initial sync, then a real re-establishment. No arm
// installs a crossed store by hand and no arm dispatches through a seam — a
// divergence exists here only because the authoritative set moved while the
// watch delivered no event, which is the boundary the defect actually crosses.

import (
	"testing"

	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var verifyGVR = schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1beta1", Resource: "pageheaders"}

// TestStoreVerification_LostUpdate_DetectedAtSnapshot — B-1.
//
// RED BEFORE THE FIX for two independent reasons, and the second is the one
// that matters: the symbols did not exist, AND no code path anywhere compared
// resourceVersions at Replace time. The three repair paths snowplow already had
// are all keyed on disappearance, so an object present before and present after
// produced nothing from any of them — which is precisely what #237 measured.
func TestStoreVerification_LostUpdate_DetectedAtSnapshot(t *testing.T) {
	resetVerificationForArm(t)
	src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
	inf, _ := verifiedInformer(t, src)

	// The initial sync must NOT have reported 50,000 lost ADDs against an
	// empty store. This is the guard that keeps the detector usable at all.
	if _, _, lostAdd, _ := divergenceCounts(verifySiteSnapshot); lostAdd != 0 {
		t.Fatalf("initial sync reported %d lost ADDs; the empty-store guard is not holding", lostAdd)
	}
	if got := VerifySkippedByReasonSnapshot()[verifySkipInitialSync]; got == 0 {
		t.Fatalf("initial sync was not counted as a skip; a silent skip is indistinguishable from a clean comparison")
	}
	if rv, ok := storeRV(t, inf, "krateo/hdr"); !ok || rv != "1" {
		t.Fatalf("precondition: store holds rv=%q ok=%v, want rv=1", rv, ok)
	}

	// THE LOST EVENT. The authoritative set moves and no watch event is
	// emitted, which is exactly what a dropped UPDATE looks like from here.
	src.setSilently(fakeObj{ns: "krateo", name: "hdr", rv: "2", uid: "u-1"})

	// A real re-establishment: the watch ends, ListAndWatch returns, and
	// BackoffUntil re-invokes it. Second reflector invocation.
	src.breakWatches()

	waitForVerify(t, "the lost UPDATE to be detected at the snapshot", verifyBound, func() bool {
		lostUpdate, _, _, _ := divergenceCounts(verifySiteSnapshot)
		return lostUpdate >= 1
	})
	lostUpdate, lostDelete, lostAdd, uidMismatch := divergenceCounts(verifySiteSnapshot)
	if lostUpdate != 1 {
		t.Errorf("lost_update_snapshot = %d, want exactly 1", lostUpdate)
	}
	if lostDelete != 0 || lostAdd != 0 || uidMismatch != 0 {
		t.Errorf("classes collided: lost_delete=%d lost_add=%d uid_mismatch=%d, want all 0",
			lostDelete, lostAdd, uidMismatch)
	}
	if got := storeDivergentLostUpdate[verifySiteForced].Load(); got != 0 {
		t.Errorf("forced-site counter moved (%d) with no forced pass running — the site split is not holding", got)
	}

	// THE ASSERTION B-1 IS ACTUALLY ABOUT. Not the counter: the STORE. On the
	// snapshot path the repair is client-go's — processDeltas writes the store
	// and only then calls the handler — so after the Replace the indexer must
	// hold the NEW object. A divergence counter climbing over a store that
	// stayed stale is the failure mode this whole deliverable was corrected to
	// prevent.
	waitForVerify(t, "client-go to repair the store from the snapshot", verifyBound, func() bool {
		rv, ok := storeRV(t, inf, "krateo/hdr")
		return ok && rv == "2"
	})

	if _, _, watchListed := src.counts(); watchListed != 0 {
		t.Logf("note: %d watch-list requests were issued — this arm runs under the package gate override", watchListed)
	}
}

// TestStoreVerification_LostDelete_And_UIDMismatch — B-3.
//
// The uid sub-arm is the load-bearing one. #237's ten-day staleness is a
// delete-and-recreate phantom, and every mechanism snowplow had is structurally
// blind to it: the C2 relist bridge diffs ns/name key sets, and
// ResolvedKeyInputs carries no object uid, so a recreate under the same name
// reuses the byte-identical L1 cell. The arm asserts the classes do NOT
// collide, because a uid phantom counted as a lost UPDATE is a phantom hidden
// inside the common class.
func TestStoreVerification_LostDelete_And_UIDMismatch(t *testing.T) {
	t.Run("lost_delete", func(t *testing.T) {
		resetVerificationForArm(t)
		src := newFakeSource(verifyGVR,
			fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"},
			fakeObj{ns: "krateo", name: "ghost", rv: "1", uid: "u-2"},
		)
		inf, _ := verifiedInformer(t, src)
		if _, ok := storeRV(t, inf, "krateo/ghost"); !ok {
			t.Fatal("precondition: the store should hold the object that is about to vanish")
		}

		// Vanishes upstream with NO delete event.
		src.setSilently(fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
		src.breakWatches()

		waitForVerify(t, "the lost DELETE to be detected", verifyBound, func() bool {
			_, lostDelete, _, _ := divergenceCounts(verifySiteSnapshot)
			return lostDelete >= 1
		})
		lostUpdate, lostDelete, _, uidMismatch := divergenceCounts(verifySiteSnapshot)
		if lostDelete != 1 {
			t.Errorf("lost_delete_snapshot = %d, want 1", lostDelete)
		}
		if lostUpdate != 0 || uidMismatch != 0 {
			t.Errorf("classes collided: lost_update=%d uid_mismatch=%d, want 0", lostUpdate, uidMismatch)
		}
		waitForVerify(t, "client-go to remove the phantom from the store", verifyBound, func() bool {
			_, ok := storeRV(t, inf, "krateo/ghost")
			return !ok
		})
	})

	t.Run("uid_mismatch", func(t *testing.T) {
		resetVerificationForArm(t)
		src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
		inf, _ := verifiedInformer(t, src)

		// Deleted and recreated under the SAME NAME with a new uid, with no
		// events. A key-set diff sees nothing at all here.
		src.setSilently(fakeObj{ns: "krateo", name: "hdr", rv: "9", uid: "u-REBORN"})
		src.breakWatches()

		waitForVerify(t, "the uid phantom to be detected", verifyBound, func() bool {
			_, _, _, uidMismatch := divergenceCounts(verifySiteSnapshot)
			return uidMismatch >= 1
		})
		lostUpdate, lostDelete, lostAdd, uidMismatch := divergenceCounts(verifySiteSnapshot)
		if uidMismatch != 1 {
			t.Errorf("uid_mismatch_snapshot = %d, want 1", uidMismatch)
		}
		// The whole point of checking uid BEFORE resourceVersion: a recreated
		// object almost always has a different rv too, and counting it as a
		// lost UPDATE would bury the phantom in the common class.
		if lostUpdate != 0 {
			t.Errorf("lost_update_snapshot = %d, want 0 — the uid phantom was counted as an ordinary update", lostUpdate)
		}
		if lostDelete != 0 || lostAdd != 0 {
			t.Errorf("classes collided: lost_delete=%d lost_add=%d, want 0", lostDelete, lostAdd)
		}
		waitForVerify(t, "the store to hold the recreated object", verifyBound, func() bool {
			rv, ok := storeRV(t, inf, "krateo/hdr")
			return ok && rv == "9"
		})
	})

	t.Run("lost_add", func(t *testing.T) {
		resetVerificationForArm(t)
		src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
		inf, _ := verifiedInformer(t, src)

		src.setSilently(
			fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"},
			fakeObj{ns: "krateo", name: "newcomer", rv: "1", uid: "u-3"},
		)
		src.breakWatches()

		waitForVerify(t, "the lost ADD to be detected", verifyBound, func() bool {
			_, _, lostAdd, _ := divergenceCounts(verifySiteSnapshot)
			return lostAdd >= 1
		})
		lostUpdate, lostDelete, lostAdd, uidMismatch := divergenceCounts(verifySiteSnapshot)
		if lostAdd != 1 {
			t.Errorf("lost_add_snapshot = %d, want 1", lostAdd)
		}
		if lostUpdate != 0 || lostDelete != 0 || uidMismatch != 0 {
			t.Errorf("classes collided: lost_update=%d lost_delete=%d uid_mismatch=%d, want 0",
				lostUpdate, lostDelete, uidMismatch)
		}
		waitForVerify(t, "the store to gain the missing object", verifyBound, func() bool {
			_, ok := storeRV(t, inf, "krateo/newcomer")
			return ok
		})
	})
}

// TestStoreVerification_RunsUnderWatchList — B-4. AN ACCEPTANCE ARM, NOT AN
// EMPHASIS.
//
// The decorator has two independent branches — ExtractList on the list path,
// accumulate-Added-until-bookmark on the watch-list path — and
// featuregate_watchlist_test.go disables WatchListClient package-wide, so
// without this arm B could ship with every other arm green having NEVER
// executed the branch production actually uses.
//
// The TestMain is not edited: SetFeatureDuringTest goes through Set, and
// Enabled consults the set-method map before the env map, so the per-test
// override wins. No t.Parallel() anywhere in this arm — SetFeatureDuringTest
// refuses a concurrent override from a different test.
//
// TWO STAGES, because a RED with two independent causes cannot attribute its
// own failure (gate condition B-2): stage 1 proves the watch-list branch is NOT
// entered under the package default, so a green there would be vacuous; stage 2
// proves it IS entered with the gate on, and that the detection is real.
func TestStoreVerification_RunsUnderWatchList(t *testing.T) {
	t.Run("stage1_gate_off_watchlist_branch_never_entered", func(t *testing.T) {
		resetVerificationForArm(t)
		src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
		verifiedInformer(t, src)
		_, watchCalls, watchListed := src.counts()
		if watchCalls == 0 {
			t.Fatal("no watch was established at all — the instrument is not wired")
		}
		if watchListed != 0 {
			t.Fatalf("watch-list branch entered %d times under the package gate override; "+
				"stage 2's green would then prove nothing it does not already", watchListed)
		}
		t.Logf("gate OFF: watchCalls=%d watchListed=%d — the watch-list branch is unreachable here", watchCalls, watchListed)
	})

	t.Run("stage2_gate_on_watchlist_branch_detects", func(t *testing.T) {
		clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
		resetVerificationForArm(t)

		src := newFakeSource(verifyGVR, fakeObj{ns: "krateo", name: "hdr", rv: "1", uid: "u-1"})
		inf, _ := verifiedInformer(t, src)

		_, _, watchListed := src.counts()
		if watchListed == 0 {
			t.Fatal("the reflector did not request watch-list semantics with the gate ON — " +
				"this arm would be asserting the LIST branch under a watch-list name")
		}
		t.Logf("gate ON: the reflector issued %d watch-list request(s); the accumulate-until-bookmark branch ran", watchListed)

		// The same lost UPDATE, now detected through the bookmark branch.
		src.setSilently(fakeObj{ns: "krateo", name: "hdr", rv: "2", uid: "u-1"})
		src.breakWatches()

		waitForVerify(t, "the watch-list branch to detect the lost UPDATE", verifyBound, func() bool {
			lostUpdate, _, _, _ := divergenceCounts(verifySiteSnapshot)
			return lostUpdate >= 1
		})
		if lostUpdate, _, _, _ := divergenceCounts(verifySiteSnapshot); lostUpdate != 1 {
			t.Errorf("lost_update_snapshot = %d, want 1", lostUpdate)
		}
		waitForVerify(t, "the watch-list Replace to repair the store", verifyBound, func() bool {
			rv, ok := storeRV(t, inf, "krateo/hdr")
			return ok && rv == "2"
		})

		// The branch attribution, positively: at least two watch-list requests
		// (initial plus the re-establishment), and the detection happened after
		// the second.
		if _, _, watchListedAfter := src.counts(); watchListedAfter < 2 {
			t.Errorf("only %d watch-list requests recorded; the arm needs a real re-establishment, not one sync", watchListedAfter)
		}
	})
}
