package cache

// store_swap_k8d_test.go — #870: a teardown stops EVERY copy of the GVR,
// including a retiring copy whose install has not yet reached its own close
// loop (the reviewer's RX7: the install's step 2 would close it later, but a
// teardown must not depend on the install being scheduled).

import (
	"sync"
	"testing"
	"time"
)

// TestStoreSwap_K8d_TeardownStopsARetiringCopyAtOnce: the install is held
// between its step-1 unlock and its step-2 close loop (the hook stands for the
// swap goroutine not being scheduled there). The GVR is torn down. The copy the
// install replaced must stop now — its Run returns while the install is still
// held — not whenever the install resumes. RED under RX7 (the teardown leaves
// retiring copies to the install's own close loop).
func TestStoreSwap_K8d_TeardownStopsARetiringCopyAtOnce(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	old := f.live()
	held := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr == gvr && step == "before_close_retiring" {
			once.Do(func() {
				close(held)
				<-release
			})
		}
	})
	var rOnce sync.Once
	rel := func() { rOnce.Do(func() { close(release) }) }
	t.Cleanup(rel)

	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatal("setup: the install never reached its close loop")
	}
	if r := f.rw.retiringForTest(gvr); len(r) != 1 || r[0] != old {
		t.Fatalf("setup: the retiring set is %v, want the replaced live copy", r)
	}
	f.rw.RemoveResourceType(gvr) // a CRD delete, while the install is held
	select {
	case <-old.done:
	case <-time.After(30 * time.Second):
		t.Fatal("K8d RED (RX7): the retiring copy is still running after the teardown — it waits for the held install to stop it")
	}
	rel()
}
