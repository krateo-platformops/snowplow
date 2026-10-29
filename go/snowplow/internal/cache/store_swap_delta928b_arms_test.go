package cache

// store_swap_delta928b_arms_test.go — the arms the #928 delta review asked
// for beyond its own (issuecomment-6509871392): a loss judged against a copy a
// swap replaced requests no build (the residual), and the swap's completion
// event closes only after M is delivered (D9).

import (
	"sync"
	"testing"
)

// TestStoreSwap_K20_ALossJudgedOnAReplacedCopyRequestsNoBuild: once a swap has
// replaced the copy a loss was judged against, the loss's repair request is
// dropped under the same rw.mu as the build decision — the swap's own LIST
// already corrected that store, and a request would start a second, redundant
// 50K build. Both bindings: the delivery log whose pending key resolved (the
// resolver, publishResolvedLoss) and the instance a forced pass compared. The
// live copy's judgement still requests one. RED under B1 (the request ignores
// its binding), B2 (the log binding always holds) and B3 (the instance
// binding always holds).
func TestStoreSwap_K20_ALossJudgedOnAReplacedCopyRequestsNoBuild(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	old := f.live()
	oldLog := old.dl
	if oldLog == nil {
		t.Fatal("setup: the live copy has no delivery log")
	}
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	cur := f.live()
	if cur == old || cur.dl == nil {
		t.Fatal("setup: no swap, or the new copy has no log")
	}
	lost := []divergenceCandidate{{key: swapKey("o00"), class: classLostUpdate}}
	fired0 := storeRepairsFiredTotal.Load()
	skip0 := VerifySkippedByReasonSnapshot()[verifySkipInstanceChanged]

	// The replaced copy's log resolves a loss after the swap: nothing is requested.
	publishResolvedLoss(oldLog, lost)
	if b := f.rw.storeBuildForTest(gvr); b != nil {
		t.Fatal("K20 RED (B1/B2): a loss resolved on the replaced copy's log started a build")
	}
	// A forced pass that compared the replaced instance: nothing is requested.
	if requestStoreRepair(gvr, []string{swapKey("o00")}, liveInstanceIs(gvr, old)) {
		t.Fatal("K20 RED (B1/B3): a forced pass's loss on the replaced instance started a build")
	}
	if got := storeRepairsFiredTotal.Load() - fired0; got != 0 {
		t.Fatalf("K20 RED: %d repair(s) requested for losses judged on a replaced copy", got)
	}
	if got := VerifySkippedByReasonSnapshot()[verifySkipInstanceChanged] - skip0; got != 2 {
		t.Fatalf("K20: verification_skipped{instance_changed} moved by %d, want 2 (both dropped requests)", got)
	}

	// The live copy's own judgement does request one.
	holdBuildsForTest.Store(true)
	t.Cleanup(func() { holdBuildsForTest.Store(false) })
	publishResolvedLoss(cur.dl, lost)
	if f.rw.storeBuildForTest(gvr) == nil {
		t.Fatal("K20: a loss resolved on the LIVE copy's log requested no build")
	}
}

// TestStoreSwap_K21_TheSwapCompletesOnlyAfterM: the swap's completion event
// (the instance's delivered channel, what awaitSwaps and every arm reading M
// or the ledger waits on) is still open when M has just been computed and is
// not yet delivered. RED under D9 (delivered closes before M is computed).
func TestStoreSwap_K21_TheSwapCompletesOnlyAfterM(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	var once sync.Once
	openAtM := make(chan bool, 1)
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr || step != "after_m" {
			return
		}
		once.Do(func() {
			select {
			case <-c.delivered:
				openAtM <- false
			default:
				openAtM <- true
			}
		})
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	if !<-openAtM {
		t.Fatal("K21 RED (D9): the swap's completion event fired before M was delivered")
	}
}
