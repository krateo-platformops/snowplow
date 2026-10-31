package rum

import (
	"fmt"
	"testing"
	"time"
)

// TestS1015_LimiterTableIsBounded — the per-user limiter table never holds
// more than its bound, whether the users it would have to forget are idle
// (bucket full again: pruned) or still active (evicted). Without a bound,
// every distinct authenticated user would leave an entry behind forever.
//
// MUTATION (RED, shown in the PR): both pruning loops in userLimiter.allow
// removed → the table grows to one entry per user.
func TestS1015_LimiterTableIsBounded(t *testing.T) {
	const bound = 8
	now := time.Unix(1_761_800_000, 0)

	// Active users: each has just spent a token, so none is idle.
	l := newUserLimiter(1, 10, bound)
	for i := 0; i < 100; i++ {
		if ok, _ := l.allow(fmt.Sprintf("active-%d", i), now); !ok {
			t.Fatalf("first request of a new user was refused")
		}
		if n := l.size(); n > bound {
			t.Fatalf("after %d active users the table holds %d entries, bound %d", i+1, n, bound)
		}
	}

	// Idle users: 20 s later every bucket is full again, so the next new user
	// prunes them all instead of evicting.
	l = newUserLimiter(1, 10, bound)
	for i := 0; i < bound; i++ {
		l.allow(fmt.Sprintf("idle-%d", i), now)
	}
	l.allow("late", now.Add(20*time.Second))
	if n := l.size(); n != 1 {
		t.Fatalf("after the idle users refilled, the table holds %d entries, want 1 (only the new user)", n)
	}

	// The limit itself still applies per user after evictions.
	l = newUserLimiter(1, 2, bound)
	l.allow("u", now)
	l.allow("u", now)
	if ok, wait := l.allow("u", now); ok || wait <= 0 {
		t.Fatalf("third back-to-back request within a burst of 2 was allowed (ok=%v wait=%v)", ok, wait)
	}
}
