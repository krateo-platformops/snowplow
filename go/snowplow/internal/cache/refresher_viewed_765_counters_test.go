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

// TestViewedTier765_V2b_ViewedCopyAfterTheOtherTierRanIsSkipped is V2b: the
// interleaving the drain order makes rare but a multi-worker Len/Get race or a
// floor deferral allows — a promoted key's NORMAL copy is dequeued and run
// first. The viewed copy that follows has nothing left to do and must not
// re-resolve. Driven step by step with processNext (no workers), so the order
// is exact.
func TestViewedTier765_V2b_ViewedCopyAfterTheOtherTierRanIsSkipped(t *testing.T) {
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

	terminalMark([]string{k}) // unviewed: the normal tier
	viewed765Arm(t, k)        // promoted: now also on the viewed tier
	if r.viewedQueue.Len() != 1 {
		t.Fatalf("PRECONDITION: the arm did not promote the key (viewed depth %d)", r.viewedQueue.Len())
	}
	vk, _ := r.viewedQueue.Get() // another worker holds the viewed copy...
	if !r.processNext(ctx) {     // ...while this one runs the normal copy
		t.Fatalf("processNext stopped")
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
		t.Fatalf("V2b RED (#765): the key was re-resolved %d times — the viewed copy re-ran a refresh the normal copy had already done", got)
	}
	if got := r.viewed.skippedConsumed.Load(); got != 1 {
		t.Fatalf("V2b: viewed_skipped_consumed = %d, want 1", got)
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
