package cache

// refresher_quiescence_930_test.go — #930: the refresher's outstanding-work
// count (refresher_quiescence.go), the settle the rewritten timing arms wait on.
//
//   Q1  a key in a worker's hands counts (the tier itself is empty then: a
//       Len() probe reads 0); a re-add while it is processing counts once more;
//       the count reaches 0 only after the LAST Done, and the wait returns on it.
//   Q2  a deferred re-add (an error backoff) is not in any tier, so the wait
//       reports it instead of returning quiescent.

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIssue930_Q1_OutstandingCountsQueuedAndInFlightUntilTheLastDone(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "q930", Name: "q1"}
	key := ComputeKey(in)
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{"q":1}`), Inputs: &in})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, _ ResolvedKeyInputs) error {
		entered <- struct{}{}
		<-release
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	base := RefresherWorkSnapshot()

	enqueueRefreshForTest(key)
	<-entered
	if w := RefresherWorkSnapshot(); w.Outstanding != 1 || refresherSingleton().queue.Len() != 0 {
		t.Fatalf("Q1 RED: one key in the worker's hands must count 1 with the tier empty; outstanding=%d len=%d",
			w.Outstanding, refresherSingleton().queue.Len())
	}
	enqueueRefreshForTest(key) // a re-add while it is processing: one more processing owed
	if w := RefresherWorkSnapshot(); w.Outstanding != 2 {
		t.Fatalf("Q1 RED: a re-add during processing must count once more; outstanding=%d, want 2", w.Outstanding)
	}
	release <- struct{}{}
	<-entered // the re-added key is in the worker's hands again
	if w := RefresherWorkSnapshot(); w.Outstanding != 1 {
		t.Fatalf("Q1 RED: after the first Done the re-added key is still owed; outstanding=%d, want 1", w.Outstanding)
	}
	close(release)
	w, err := WaitRefresherQuiescentForTest(base, time.Minute)
	if err != nil || w.Outstanding != 0 {
		t.Fatalf("Q1: the wait must return quiescent after the last Done: outstanding=%d err=%v", w.Outstanding, err)
	}
	if got := refresherSingleton().completedTotal.Load(); got != 2 {
		t.Fatalf("Q1: the wait returned with %d of 2 refreshes complete", got)
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
