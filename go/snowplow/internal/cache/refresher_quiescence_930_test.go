package cache

// refresher_quiescence_930_test.go — #930: the refresher's outstanding-work
// count (refresher_quiescence.go), the settle the rewritten timing arms wait on.
//
//   Q1  per tier (normal, viewed, cluster_list): a key in a worker's hands
//       counts (its tier is empty then: a Len() probe reads 0); a re-add while
//       it is processing counts once more; the count reaches 0 only after the
//       LAST Done, and the wait returns on it.
//   Q2  a deferred re-add (an error backoff) is not in any tier, so the wait
//       reports it instead of returning quiescent.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// q1TierLen is the keys waiting in every tier (not those in a worker's hands).
func q1TierLen(r *refresher) int {
	return r.queue.Len() + r.clusterListQueue.Len() + r.viewedQueue.Len()
}

// TestIssue930_Q1_OutstandingCountsQueuedAndInFlightUntilTheLastDone — for EACH
// tier (normal, viewed, cluster_list), through the production dirty-mark route
// (onDirtyMark → routeDirty): with the one worker holding the key in its
// handler, the count is exactly the processings owed plus every key waiting in
// any tier (the viewed tier's idle-wake key included). A tier counted anywhere
// but the shared total reads short here.
func TestIssue930_Q1_OutstandingCountsQueuedAndInFlightUntilTheLastDone(t *testing.T) {
	for _, tier := range []string{"normal", "viewed", "cluster_list"} {
		t.Run(tier, func(t *testing.T) {
			terminalEnv(t)
			cleanup := withCleanRefresher(t, 1, 0)
			t.Cleanup(cleanup)
			_, keys := terminalFixtureNoReset(t, 1)
			key := keys[0]
			switch tier {
			case "viewed":
				viewed765Arm(t, key)
			case "cluster_list":
				RegisterClusterListKey(key)
			}
			var calls atomic.Int64
			entered := make(chan struct{}, 2)
			release := make(chan struct{}, 2)
			RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
				calls.Add(1)
				entered <- struct{}{}
				<-release
				return nil
			})
			r := refresherSingleton()
			own := map[string]workqueue.TypedRateLimitingInterface[string]{
				"normal": r.queue, "viewed": r.viewedQueue, "cluster_list": r.clusterListQueue,
			}[tier]
			// Marked before the worker starts: its first iteration probes the
			// priority tiers, so the key is taken from its own tier whichever it is.
			terminalMark([]string{key})
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			StartRefresher(ctx)
			base := RefresherWorkSnapshot()

			q1Wait(t, entered, "the first re-resolve")
			if w := RefresherWorkSnapshot(); w.Outstanding != 1+int64(q1TierLen(r)) || own.Len() != 0 {
				t.Fatalf("Q1 RED (%s): the key in the worker's hands must count: outstanding=%d, want 1 + %d waiting "+
					"(own tier len=%d)", tier, w.Outstanding, q1TierLen(r), own.Len())
			}
			terminalMark([]string{key}) // a re-mark while it is processing: one more processing owed
			if w := RefresherWorkSnapshot(); w.Outstanding != 2+int64(q1TierLen(r)) {
				t.Fatalf("Q1 RED (%s): a re-add during processing must count once more: outstanding=%d, want 2 + %d waiting",
					tier, w.Outstanding, q1TierLen(r))
			}
			release <- struct{}{}
			q1Wait(t, entered, "the re-marked key's second re-resolve")
			if w := RefresherWorkSnapshot(); w.Outstanding != 1+int64(q1TierLen(r)) {
				t.Fatalf("Q1 RED (%s): after the first Done the re-marked key is still owed: outstanding=%d, want 1 + %d waiting",
					tier, w.Outstanding, q1TierLen(r))
			}
			release <- struct{}{}
			w, err := WaitRefresherQuiescentForTest(base, time.Minute)
			if err != nil || w.Outstanding != 0 || q1TierLen(r) != 0 {
				t.Fatalf("Q1 (%s): the wait must return quiescent after the last Done: outstanding=%d waiting=%d err=%v",
					tier, w.Outstanding, q1TierLen(r), err)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("Q1 (%s): the wait returned with %d of 2 re-resolves run", tier, got)
			}
			// SETUP: both re-resolves ran on the tier under test.
			viewed, cl := r.viewed.dequeued.Load(), r.clusterListCompletedTotal.Load()
			if want := map[string][2]uint64{"normal": {0, 0}, "viewed": {2, 0}, "cluster_list": {0, 2}}[tier]; viewed != want[0] || cl != want[1] {
				t.Fatalf("SETUP (%s): viewed dequeues=%d cluster_list completions=%d, want %d/%d — the key did not run on its tier",
					tier, viewed, cl, want[0], want[1])
			}
		})
	}
}

func q1Wait(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Minute): // a stuck worker only; never the settle
		t.Fatalf("PRE: %s never ran", what)
	}
}

func TestIssue930_Q2_ADeferredReAddIsNotQuiescent(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherBaseDelayMS, "60000") // the backoff outlives the arm: the re-add stays deferred
	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "q930", Name: "q2"}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"q":2}`), Inputs: &in})
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
		return errors.New("q2: transient")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	base := RefresherWorkSnapshot()
	enqueueRefreshForTest(key)
	w, err := WaitRefresherQuiescentForTest(base, time.Minute)
	if !errors.Is(err, errRefresherDeferred) || w.Retried != base.Retried+1 {
		t.Fatalf("Q2 RED: a key waiting out its error backoff must not read quiescent: err=%v retried Δ=%d outstanding=%d",
			err, w.Retried-base.Retried, w.Outstanding)
	}
}
