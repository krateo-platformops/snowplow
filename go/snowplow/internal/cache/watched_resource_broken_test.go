package cache

import "testing"

// A GVR whose watch is broken is not servable: the indexer is not
// authoritative, so neither the plural nor the object is reported.
func TestWatchedResource_NotServableKnowsNothing(t *testing.T) {
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	createObj(t, rw, dyn, gvr, "demo", "w1", "x")
	if known, exists := rw.probeResource(gvr.Group, gvr.Resource, "demo", "w1"); !known || !exists {
		t.Fatalf("control: known=%v exists=%v, want true true", known, exists)
	}
	rw.mu.Lock()
	rw.watchBroken[gvr] = struct{}{}
	rw.mu.Unlock()
	if known, exists := rw.probeResource(gvr.Group, gvr.Resource, "demo", "w1"); known || exists {
		t.Errorf("broken watch: known=%v exists=%v, want false false", known, exists)
	}
}
