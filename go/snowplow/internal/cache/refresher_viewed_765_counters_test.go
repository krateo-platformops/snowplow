// refresher_viewed_765_counters_test.go — #765 viewed tier: the counters on the
// tag-derived snowplow_refresher{stat} family (expvar + OTLP) move exactly as the
// tier works. Uses #765 stats, so it does not compile on main.
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestViewedTier765_V2b_NormalCopyStepsAsideForThePendingViewedCopy is V2b: a
// promoted key sits in two tiers, and the NORMAL copy reaches a worker first
// (with the #799 reservation / p=1 alternation the normal tier always gets
// turns, so this is the common case, not a race). It must step aside: the
// viewed copy carries the triggers and runs at the viewer's priority, and the
// key is re-resolved exactly once, customer-triggered. Driven step by step with
// processNext (no workers), so the order is exact.
func TestViewedTier765_V2b_NormalCopyStepsAsideForThePendingViewedCopy(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	var runs, customer atomic.Int32
	RegisterRefreshFunc("widgets", func(ctx context.Context, key string, _ ResolvedKeyInputs) error {
		if key == k {
			runs.Add(1)
			if CustomerTriggeredRefreshFromContext(ctx) {
				customer.Add(1)
			}
		}
		return nil
	})
	r := refresherSingleton()
	ctx := context.Background()

	terminalMark([]string{k}) // unviewed: the normal tier
	viewed765Arm(t, k)        // promoted: now also on the viewed tier
	if r.viewedQueue.Len() != 1 {
		t.Fatalf("PRECONDITION: the arm did not promote the key (viewed depth %d)", r.viewedQueue.Len())
	}
	vk, _ := r.viewedQueue.Get() // the viewed copy is held back...
	if !r.processNext(ctx) {     // ...while a worker takes the normal copy
		t.Fatalf("processNext stopped")
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("V2b RED (#799): the normal copy re-resolved the promoted key (as background) while its viewed copy was still queued")
	}
	r.viewedQueue.Done(vk)
	r.viewedQueue.Add(vk) // the viewed copy comes back
	for r.viewedQueue.Len() > 0 || r.queue.Len() > 0 {
		r.processNext(ctx)
	}
	if runs.Load() != 1 || customer.Load() != 1 {
		t.Fatalf("V2b RED: runs=%d customer-triggered=%d, want exactly one re-resolve, at the viewer's priority", runs.Load(), customer.Load())
	}
	if got := r.viewed.skippedConsumed.Load(); got != 1 {
		t.Fatalf("V2b: viewed_skipped_consumed = %d, want 1 (the normal copy)", got)
	}
}

// TestViewedTier765_V2c_ViewedCopyAfterTheOtherTierRanIsSkipped is V2c: a key
// lands in two tiers WITHOUT a promotion: marked while armed (viewed tier), the
// tab closes, a second mark routes it normal. With the #799 reservation the
// normal copy can run first; it consumes the merged trigger set. The viewed copy
// that follows has nothing left to do and must not re-resolve. Driven step by
// step with processNext (no workers), so the order is exact.
func TestViewedTier765_V2c_ViewedCopyAfterTheOtherTierRanIsSkipped(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	var runs atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, key string, _ ResolvedKeyInputs) error {
		if key == k {
			runs.Add(1)
		}
		return nil
	})
	r := refresherSingleton()
	ctx := context.Background()

	unsub := viewed765Arm(t, k)
	terminalMark([]string{k}) // armed: the viewed tier
	unsub()
	terminalMark([]string{k}) // no longer armed: the normal tier too
	if r.viewedQueue.Len() != 1 {
		t.Fatalf("PRECONDITION: the key is not on the viewed tier (depth %d)", r.viewedQueue.Len())
	}
	vk, _ := r.viewedQueue.Get() // the viewed copy is held back...
	for r.queue.Len() > 0 {      // ...while a worker runs the normal copy
		r.processNext(ctx)
	}
	if runs.Load() != 1 {
		t.Fatalf("PRECONDITION: the normal copy did not re-resolve (runs=%d)", runs.Load())
	}
	r.viewedQueue.Done(vk)
	r.viewedQueue.Add(vk) // the held viewed copy comes back
	for r.viewedQueue.Len() > 0 || r.queue.Len() > 0 {
		r.processNext(ctx)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("V2c RED (#765): the key was re-resolved %d times — the viewed copy re-ran a refresh the normal copy had already done", got)
	}
}

// TestViewedTier765_C_CountersMoveWithTheTier drives one routed key, one promoted
// key (whose second copy is skipped) and one key whose tab closed before its
// dequeue, and reads every viewed_* stat.
func TestViewedTier765_C_CountersMoveWithTheTier(t *testing.T) {
	keys, rec, firstIn, release := viewed765Start(t, 1, 5)
	hold, routed, promoted, closed, other := keys[0], keys[1], keys[2], keys[3], keys[4]
	viewed765Arm(t, routed)
	unsub := viewed765Arm(t, closed)

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark([]string{other, promoted, routed, closed})
	unsub()
	viewed765Arm(t, promoted) // promote on arm: it now sits in two tiers
	if d := RefresherStatsByStat()["viewed_queue_depth"]; d != 3 {
		t.Fatalf("viewed_queue_depth = %d with routed + closed + promoted waiting, want 3", d)
	}
	release()
	waitFor(t, 20*time.Second, "every key re-resolved and the queues drained", func() bool {
		_, _, c := rec.snapshot()
		r := refresherSingleton()
		return c[routed] == 1 && c[promoted] == 1 && c[closed] == 1 && c[other] == 1 &&
			r.queue.Len() == 0 && r.viewedQueue.Len() == 0
	})
	time.Sleep(200 * time.Millisecond)
	s := RefresherStatsByStat()
	want := map[string]int64{
		"viewed_enqueue":            3, // routed, closed (at mark), promoted (on arm)
		"viewed_promoted":           1,
		"viewed_dequeued":           3,
		"viewed_skipped_consumed":   1, // the promoted key's normal-tier copy
		"viewed_customer_triggered": 2, // routed, promoted
		"viewed_unsubscribed":       1, // closed
		"viewed_queue_depth":        0,
	}
	for k, w := range want {
		if s[k] != w {
			t.Errorf("C RED: %s = %d, want %d (all: %v)", k, s[k], w, s)
		}
	}
	if _, _, c := rec.snapshot(); c[promoted] != 1 {
		t.Fatalf("the promoted key was re-resolved %d times, want 1", c[promoted])
	}
}
