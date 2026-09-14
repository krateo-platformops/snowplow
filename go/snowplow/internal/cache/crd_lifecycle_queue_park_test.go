// crd_lifecycle_queue_park_test.go — 1.12.5 / #187: the CRD lifecycle queue
// must never silently drop an event.
//
// THE CARRIER. submitCRDLifecycleEvent used to drop on a full queue. The
// rationale on the books was about ADD/UPDATE — DiscoverGroupResources is
// singleflighted per-group, so a duplicate for an in-flight group is harmless
// — and it simply does not extend to DELETE, which is the event class #187 is
// about. A dropped CRD DELETE means triggerCRDDelete never runs: the
// per-resource informer is never torn down and dependent L1 entries are never
// dirty-marked, so every entry of that type is stale until TTL with no further
// trigger of any kind. Same shape as the defects #187 already found, different
// door.
//
// HOW THE ARM DRIVES IT. Real crdDiscovery singleton, real
// submitCRDLifecycleEvent, real channel. The worker's sync.Once is consumed
// with a no-op BEFORE any submit, so no drain goroutine exists and the queue
// genuinely fills — a burst larger than the buffer then hits the park path for
// real, rather than by simulation. The test itself becomes the drain, and
// counts what comes out.
//
// RED on main: the overflow events are dropped, so the drain sees only
// crdDiscoveryQueueDepth of them and eventsDropped is non-zero.
// GREEN with park-and-drain: every event arrives.

package cache

import (
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// crdLifecycleObj is a minimal CRD-shaped object for the queue arm. Its
// content is irrelevant — nothing processes it; the arm is about delivery.
func crdLifecycleObj(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": name},
	}}
}

func TestCRDLifecycleQueue_OverflowParksAndNeverDrops(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	withCleanCRDDiscovery(t)

	c := crdDiscoverySingleton()
	// Consume the worker's sync.Once with a no-op so NO drain goroutine is
	// started. The queue then genuinely fills and the overflow submits reach
	// the park path for real.
	c.startOnce.Do(func() {})

	const overflow = 12
	total := crdDiscoveryQueueDepth + overflow

	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// DELETE deliberately: it is the kind whose loss is unrecoverable.
			c.submitCRDLifecycleEvent(crdLifecycleObj("crd-"+itoaForTest(i)), crdLifecycleDelete)
		}(i)
	}

	// Wait until the buffer has filled and the overflow submits have either
	// parked (fixed) or been dropped (main). Waiting on EITHER outcome is what
	// lets this arm report the real defect on main instead of failing its own
	// precondition.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) &&
		c.eventsParked.Load() == 0 && c.eventsDropped.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if dropped := c.eventsDropped.Load(); dropped != 0 {
		t.Fatalf("#187 RED: %d CRD lifecycle event(s) were DROPPED the instant the queue "+
			"filled, with no attempt to park. A dropped DELETE means triggerCRDDelete never "+
			"runs — the per-resource informer is not torn down and its dependent L1 entries "+
			"stay resident until TTL, with no further trigger. (enqueued=%d of %d)",
			dropped, c.eventsEnqueued.Load(), total)
	}
	if c.eventsParked.Load() == 0 {
		t.Fatalf("no submit parked and none was dropped — the queue did not fill, so the arm "+
			"is not exercising the overflow path (enqueued=%d)", c.eventsEnqueued.Load())
	}

	// Become the drain. Everything that was submitted must come out.
	received := 0
	drainDeadline := time.Now().Add(20 * time.Second)
	for received < total && time.Now().Before(drainDeadline) {
		select {
		case <-c.queue:
			received++
		case <-time.After(200 * time.Millisecond):
		}
	}
	wg.Wait()

	if dropped := c.eventsDropped.Load(); dropped != 0 {
		t.Fatalf("#187 RED: %d CRD lifecycle event(s) were DROPPED on a full queue. A dropped "+
			"DELETE means triggerCRDDelete never runs — the per-resource informer is not torn "+
			"down and its dependent L1 entries stay resident until TTL, with no further "+
			"trigger. (received=%d/%d parked=%d)",
			dropped, received, total, c.eventsParked.Load())
	}
	if received != total {
		t.Fatalf("#187 RED: drained %d of %d submitted events — %d went missing without being "+
			"counted as dropped", received, total, total-received)
	}
	if enq := c.eventsEnqueued.Load(); enq != uint64(total) {
		t.Fatalf("eventsEnqueued = %d, want %d — the counter must account for parked-then-"+
			"enqueued events too", enq, total)
	}
}

// TestCRDLifecycleQueue_ParkGivesUpOnShutdown pins the liveness half: parking
// must not outlive the bridge. A submit blocked on a full queue has to return
// when the worker is stopped, or a shutdown would hang on the informer
// processor goroutine.
func TestCRDLifecycleQueue_ParkGivesUpOnShutdown(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	withCleanCRDDiscovery(t)

	c := crdDiscoverySingleton()
	c.startOnce.Do(func() {}) // no drain

	// Fill the buffer.
	for i := 0; i < crdDiscoveryQueueDepth; i++ {
		c.queue <- crdDiscoveryEvent{obj: crdLifecycleObj("fill"), kind: crdLifecycleAdd}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.submitCRDLifecycleEvent(crdLifecycleObj("blocked"), crdLifecycleDelete)
	}()

	// It must be parked, not returned.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.eventsParked.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-done:
		t.Fatalf("the submit returned without parking — the queue was not full")
	default:
	}

	close(c.stopCh)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("a parked submit did not return after shutdown. Parking happens on the "+
			"informer processor goroutine; if it cannot be interrupted, a shutdown hangs "+
			"(park_max is %s).", crdLifecycleParkMax)
	}
	if c.eventsDropped.Load() != 1 {
		t.Fatalf("eventsDropped = %d, want 1 — a shutdown-abandoned event must be counted, "+
			"not silently forgotten", c.eventsDropped.Load())
	}
}

// itoaForTest avoids pulling strconv in just for the burst labels.
func itoaForTest(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
