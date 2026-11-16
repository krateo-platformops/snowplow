package cache

// store_swap_k19_test.go — #870 × #918 (the rebase onto #923): each informer
// copy has its own delivery log, and the GVR's is the live copy's.

import "testing"

func dlClosed(l *deliveryLog) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// TestStoreSwap_K19_DeliveryLogFollowsTheLiveCopy: a build's copy's log is NOT
// the GVR's until its install; the install makes it the GVR's and closes the
// replaced copy's (#918: its pending keys and resolver go with its informer);
// an aborted build's log is closed with it. RED under K19a (the install keeps
// the replaced log open), K19b (an abort leaves the copy's log open) and K19c
// (a build's log becomes the GVR's at construction).
func TestStoreSwap_K19_DeliveryLogFollowsTheLiveCopy(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	logOf := func() *deliveryLog {
		f.rw.mu.RLock()
		defer f.rw.mu.RUnlock()
		return f.rw.deliveries[gvr]
	}
	old := f.live()
	if old.dl == nil || logOf() != old.dl {
		t.Fatal("precondition: the live copy's log is the GVR's")
	}
	lists0 := f.listCount()
	release := f.holdList(lists0 + 1)
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	cand := f.rw.storeBuildForTest(gvr)
	if cand == nil || cand.dl == nil {
		t.Fatal("precondition: the build's copy has its own log")
	}
	if logOf() != old.dl {
		t.Fatal("K19 RED (K19c): a build's log became the GVR's before its install")
	}
	release()
	f.awaitSwaps(1)
	if logOf() != cand.dl {
		t.Fatal("K19 RED: after the install the GVR's log is not the new copy's")
	}
	if !dlClosed(old.dl) {
		t.Fatal("K19 RED (K19a): the replaced copy's log is still open after the install")
	}

	// An aborted build's log is closed with it.
	lists1 := f.listCount()
	f.holdList(lists1 + 1)
	f.request(reasonDivergence, swapKey("o00"))
	c2 := f.rw.storeBuildForTest(gvr)
	if c2 == nil || c2.dl == nil {
		t.Fatal("precondition: a second build with its own log")
	}
	f.rw.noteCleanPass(gvr, f.live())
	if f.rw.storeBuildForTest(gvr) != nil {
		t.Fatal("precondition: the clean pass aborted the divergence-only build")
	}
	if !dlClosed(c2.dl) {
		t.Fatal("K19 RED (K19b): an aborted build's log is still open")
	}
	if logOf() != cand.dl || dlClosed(cand.dl) {
		t.Fatal("K19 RED: the abort touched the live copy's log")
	}
}
