// refresher_viewed_wake_799_test.go — #799 B3 (architect-623, #799
// issuecomment-6289886350): with the viewed reservation FULL and viewed keys
// still queued, the reserved worker busy-spun on the wake key — viewed probe
// refused, cluster_list empty, the normal Get handed it the wake key, which it
// re-added because viewed Len()>1 — ~85% of a core on an idle refresher (RUN).
//
// B3: p=2 (one viewed slot). The slot is held by a blocked viewed re-resolve and
// three more viewed keys wait. Over an idle 1s window the wake key is dequeued a
// BOUNDED number of times (≤2), not a number proportional to the window. Then
// the slot is released, and every waiting viewed key is re-resolved promptly:
// the release itself must wake them (the reserved worker is parked in the
// normal tier's Get by then). Compiles on 0fadf00b: the wake dequeues are
// counted by wrapping the normal queue's Get.
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// wakeCountingQueue counts the wake keys a worker dequeues from the normal tier.
type wakeCountingQueue struct {
	workqueue.TypedRateLimitingInterface[string]
	wakes atomic.Int64
}

func (q *wakeCountingQueue) Get() (string, bool) {
	k, shutdown := q.TypedRateLimitingInterface.Get()
	if k == viewedWakeKey {
		q.wakes.Add(1)
	}
	return k, shutdown
}

func TestViewed799_B3_FullReservationDoesNotSpinOnTheWakeKey(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 2, 0) // p=2: one viewed slot
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 4)
	gate := make(chan struct{})
	var started, done atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		started.Add(1)
		<-gate // the one viewed slot stays full
		done.Add(1)
		return nil
	})
	r := refresherSingleton()
	cq := &wakeCountingQueue{TypedRateLimitingInterface: r.queue}
	r.queue = cq // before the workers start: every normal-tier Get is counted
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, keys...)
	terminalMark(keys) // 4 viewed keys: 1 holds the slot, 3 wait
	waitFor(t, 5*time.Second, "one viewed re-resolve holding the slot", func() bool { return started.Load() >= 1 })
	time.Sleep(200 * time.Millisecond) // let the wakes from the marks settle

	w0 := cq.wakes.Load()
	time.Sleep(time.Second)
	wakes := cq.wakes.Load() - w0
	if wakes > 2 {
		close(gate)
		t.Fatalf("B3 RED (#799): the wake key was dequeued %d times in an idle 1s with the viewed reservation full — "+
			"the reserved worker busy-spins on it (want ≤ 2)", wakes)
	}
	if s := started.Load(); s != 1 {
		t.Fatalf("PRECONDITION: %d viewed re-resolves started with one slot, want 1", s)
	}

	// The slot is released: every waiting viewed key must run promptly.
	close(gate)
	deadline := time.Now().Add(2 * time.Second)
	for done.Load() < 4 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := done.Load(); got < 4 {
		t.Fatalf("B3 RED (#799): after the viewed slot was released only %d of 4 viewed keys were re-resolved within 2s — "+
			"the release did not wake a worker for the waiting keys", got)
	}
	t.Logf("B3: %d wake dequeues in an idle 1s with the reservation full; all 4 viewed keys done after the release", wakes)
}
