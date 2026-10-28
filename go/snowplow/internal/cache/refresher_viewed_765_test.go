// refresher_viewed_765_test.go — #765 viewed tier arms, through the real
// refresher (onDirtyMark → tier → processNext → processOne) and the real
// /refreshes arm (SubscribeRefreshStream). This file compiles against main
// (7f179eac), so the RED-on-main run is this code.
//
//   - V1 VIEWED FIRST: one worker held on a first key; N unviewed dirty keys are
//     queued, then one key open on a page. The viewed key is re-resolved NEXT.
//     main: last (FIFO).
//   - V2 PROMOTE ON ARM: the key is marked while unviewed (behind N keys), then a
//     tab arms it. It is re-resolved next, and EXACTLY ONCE after everything drains.
//   - V3 UNSUBSCRIBED BEFORE DEQUEUE: routed viewed, the tab closes before the
//     dequeue; the re-resolve runs as BACKGROUND (not customer-triggered).
//   - V4 CAP BYPASS: a viewed re-resolve whose tab is still open runs
//     customer-triggered (WithCustomerTriggeredRefresh: the fill admission class
//     the cap never queues — the api-package V4b arm asserts the class).
//   - V5 RATE FLOOR KEPT: a viewed key whose entry is younger than the floor is
//     deferred, and re-resolved only after the floor.
//   - V6 IDLE WAKE: every worker blocked on an empty normal queue; a viewed mark
//     is still re-resolved promptly.
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// viewed765Rec records the order and context of the refresher's re-resolves.
type viewed765Rec struct {
	mu       sync.Mutex
	order    []string
	customer map[string]bool
	count    map[string]int
}

func (r *viewed765Rec) note(ctx context.Context, k string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(r.order, k)
	r.customer[k] = CustomerTriggeredRefreshFromContext(ctx)
	r.count[k]++
}

func (r *viewed765Rec) snapshot() ([]string, map[string]bool, map[string]int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cust := map[string]bool{}
	for k, v := range r.customer {
		cust[k] = v
	}
	cnt := map[string]int{}
	for k, v := range r.count {
		cnt[k] = v
	}
	return append([]string(nil), r.order...), cust, cnt
}

// viewed765Start starts a parallelism-p refresher whose handler records every
// re-resolve; the FIRST re-resolve blocks until release is called, so the keys
// marked meanwhile queue up behind it.
func viewed765Start(t *testing.T, p, n int) (keys []string, rec *viewed765Rec, firstIn chan struct{}, release func()) {
	t.Helper()
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys = terminalFixtureNoReset(t, n)
	rec = &viewed765Rec{customer: map[string]bool{}, count: map[string]int{}}
	firstIn = make(chan struct{})
	gate := make(chan struct{})
	release = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	var first atomic.Bool
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, _ ResolvedKeyInputs) error {
		rec.note(ctx, k)
		if first.CompareAndSwap(false, true) {
			close(firstIn)
			<-gate
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	return keys, rec, firstIn, release
}

// terminalFixtureNoReset is terminalFixture's entry seeding without its own
// refresher reset (viewed765Start owns the harness).
func terminalFixtureNoReset(t *testing.T, n int) (*ResolvedCacheStore, []string) {
	t.Helper()
	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled")
	}
	Deps().SetStore(c)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Group: "widgets.templates.krateo.io",
			Version: "v1beta1", Resource: "flexes", Namespace: "ns", Name: "v765-" + string(rune('a'+i%26)) + string(rune('a'+i/26))}
		key := ComputeKey(inputs)
		c.put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"old"}`), Inputs: &inputs})
		keys = append(keys, key)
	}
	return c, keys
}

func viewed765Wait(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("PRECONDITION: %s", what)
	}
}

func viewed765Arm(t *testing.T, keys ...string) func() {
	t.Helper()
	set := map[string]struct{}{}
	for _, k := range keys {
		set[k] = struct{}{}
	}
	st := SubscribeRefreshStream(set)
	t.Cleanup(st.Unsub)
	for _, k := range keys {
		if !HasRefreshSubscriber(k) {
			t.Fatalf("PRECONDITION: the /refreshes arm did not register %s", k[:8])
		}
	}
	return st.Unsub
}

func indexOf(order []string, k string) int {
	for i, x := range order {
		if x == k {
			return i
		}
	}
	return -1
}

// TestViewedTier765_V1_ViewedDirtyKeyIsReResolvedFirst is V1.
func TestViewedTier765_V1_ViewedDirtyKeyIsReResolvedFirst(t *testing.T) {
	const n = 12
	keys, rec, firstIn, release := viewed765Start(t, 1, n+2)
	hold, unviewed, viewed := keys[0], keys[1:n+1], keys[n+1]
	viewed765Arm(t, viewed)

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark(unviewed)
	terminalMark([]string{viewed})
	release()
	waitFor(t, 20*time.Second, "every marked key re-resolved", func() bool {
		o, _, _ := rec.snapshot()
		return len(o) >= n+2
	})
	order, _, _ := rec.snapshot()
	if i := indexOf(order, viewed); i != 1 {
		t.Fatalf("V1 RED (#765): the dirty key open on a page was re-resolved at position %d of %d — behind %d unviewed keys "+
			"(FIFO), want position 1 (right after the in-flight one)", i, len(order), i-1)
	}
}

// TestViewedTier765_V2_ArmAfterMarkPromotesAndReResolvesOnce is V2.
func TestViewedTier765_V2_ArmAfterMarkPromotesAndReResolvesOnce(t *testing.T) {
	const n = 12
	keys, rec, firstIn, release := viewed765Start(t, 1, n+2)
	hold, unviewed, late := keys[0], keys[1:n+1], keys[n+1]

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark(unviewed)
	terminalMark([]string{late}) // unviewed at mark time: the normal tier
	viewed765Arm(t, late)        // a tab opens the page while it waits
	release()
	waitFor(t, 20*time.Second, "every marked key re-resolved", func() bool {
		o, _, _ := rec.snapshot()
		return len(o) >= n+2
	})
	// Let any second copy of the promoted key drain.
	waitFor(t, 10*time.Second, "the refresher idle", func() bool {
		r := refresherSingleton()
		return r.queue.Len() == 0 && r.clusterListQueue.Len() == 0
	})
	time.Sleep(200 * time.Millisecond)
	order, _, count := rec.snapshot()
	if i := indexOf(order, late); i != 1 {
		t.Fatalf("V2 RED (#765): a key armed while it waited dirty was re-resolved at position %d of %d, want 1 (promoted on arm)", i, len(order))
	}
	if c := count[late]; c != 1 {
		t.Fatalf("V2 RED (#765): the promoted key was re-resolved %d times, want exactly 1 (its second queued copy has nothing left to do)", c)
	}
}

// TestViewedTier765_V3_UnsubscribedBeforeDequeueRunsAsBackground is V3.
func TestViewedTier765_V3_UnsubscribedBeforeDequeueRunsAsBackground(t *testing.T) {
	keys, rec, firstIn, release := viewed765Start(t, 1, 2)
	hold, k := keys[0], keys[1]
	unsub := viewed765Arm(t, k)

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark([]string{k})
	unsub() // the tab closes before the dequeue
	release()
	waitFor(t, 20*time.Second, "the key re-resolved", func() bool { _, _, c := rec.snapshot(); return c[k] == 1 })
	if _, cust, _ := rec.snapshot(); cust[k] {
		t.Fatalf("V3 RED (#765): a key whose tab closed before the dequeue ran customer-triggered (cap bypass) — want background")
	}
}

// TestViewedTier765_V4_ViewedReResolveBypassesTheCap is V4.
func TestViewedTier765_V4_ViewedReResolveBypassesTheCap(t *testing.T) {
	keys, rec, firstIn, release := viewed765Start(t, 1, 3)
	hold, unviewed, k := keys[0], keys[1], keys[2]
	viewed765Arm(t, k)

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	terminalMark([]string{unviewed, k})
	release()
	waitFor(t, 20*time.Second, "both re-resolved", func() bool { _, _, c := rec.snapshot(); return c[k] == 1 && c[unviewed] == 1 })
	_, cust, _ := rec.snapshot()
	if !cust[k] {
		t.Fatalf("V4 RED (#765): the re-resolve of a dirty key open on a page ran as background — a viewer waits at the admission cap")
	}
	if cust[unviewed] {
		t.Fatalf("V4: an unviewed key's re-resolve ran customer-triggered — the bypass must be only for viewed cells")
	}
}

// TestViewedTier765_V5_RateFloorStillAppliesOnTheViewedTier is V5.
func TestViewedTier765_V5_RateFloorStillAppliesOnTheViewedTier(t *testing.T) {
	const floor = 2 * time.Second
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	t.Setenv(envRefresherRateFloorSeconds, "2") // read on every dequeue
	c, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	e, _ := c.GetNoTouch(k)
	born := e.CreatedAt
	var at atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, key string, _ ResolvedKeyInputs) error {
		if key == k {
			at.CompareAndSwap(0, time.Now().UnixNano())
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, k)
	terminalMark([]string{k})
	waitFor(t, 20*time.Second, "the viewed key re-resolved after its floor", func() bool { return at.Load() != 0 })
	if got := time.Unix(0, at.Load()).Sub(born); got < floor-50*time.Millisecond {
		t.Fatalf("V5 RED (#765): the viewed key was re-resolved %v after its entry was stored, inside the %v rate floor — "+
			"the viewed tier must not bypass the floor", got, floor)
	}
	if refresherSingleton().flooredTotal.Load() == 0 {
		t.Fatalf("V5: floored_total = 0 — the viewed dequeue was never deferred by the floor")
	}
}

// TestViewedTier765_V6_IdleWorkersWakeForAViewedMark is V6.
func TestViewedTier765_V6_IdleWorkersWakeForAViewedMark(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	var done atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, key string, _ ResolvedKeyInputs) error {
		if key == k {
			done.Add(1)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	time.Sleep(100 * time.Millisecond) // every worker parks in the empty normal tier's Get
	viewed765Arm(t, k)
	terminalMark([]string{k})
	deadline := time.Now().Add(3 * time.Second)
	for done.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if done.Load() == 0 {
		t.Fatalf("V6 RED (#765): a viewed mark with every worker idle was not re-resolved within 3s — no worker woke for it")
	}
}
