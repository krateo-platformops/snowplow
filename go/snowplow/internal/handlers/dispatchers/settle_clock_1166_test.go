package dispatchers

// settle_clock_1166_test.go — #1166: settleRegisteredSet on a virtual clock.
//
// settleRegisteredSet waits until the registered-GVR count has held for
// phase1SettleWindow (750 ms), polling every phase1SettlePoll (150 ms). Every
// engine pass runs it once, so an arm that drives N passes used to spend
// N × 750 ms of real time in it (225 calls, 167 s of the dispatchers package on
// b01336a1). On a virtualSettleClock the same loop runs on exact instants: each
// poll tick advances the clock by exactly one poll period, at the moment the loop
// asks for it. The arms below pin the window logic on that clock; it had no arm
// of its own before.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/utils/clock"
)

// virtualSettleClock is a clock.WithTicker whose time moves only when the
// settle loop takes its next tick: the tick advances it by the ticker's period.
// onTick, when set, runs at each tick before the loop reads anything (an arm
// uses it to change the registered set at an exact instant).
type virtualSettleClock struct {
	mu     sync.Mutex
	now    time.Time
	ticks  int
	onTick func(tick int)
}

func newVirtualSettleClock() *virtualSettleClock {
	return &virtualSettleClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *virtualSettleClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *virtualSettleClock) Since(t time.Time) time.Duration { return c.Now().Sub(t) }

func (c *virtualSettleClock) NewTicker(d time.Duration) clock.Ticker {
	return &virtualSettleTicker{c: c, d: d}
}

// settleRegisteredSet uses Now, Since and NewTicker only; the rest of
// clock.Clock fails loudly if the settle loop ever starts to use it.
func (c *virtualSettleClock) After(time.Duration) <-chan time.Time { panic("settle: After unused") }
func (c *virtualSettleClock) NewTimer(time.Duration) clock.Timer   { panic("settle: NewTimer unused") }
func (c *virtualSettleClock) Sleep(time.Duration)                  { panic("settle: Sleep unused") }
func (c *virtualSettleClock) Tick(time.Duration) <-chan time.Time  { panic("settle: Tick unused") }

type virtualSettleTicker struct {
	c *virtualSettleClock
	d time.Duration
}

// C delivers the next tick at once, the clock advanced by one period.
func (t *virtualSettleTicker) C() <-chan time.Time {
	t.c.mu.Lock()
	t.c.now = t.c.now.Add(t.d)
	t.c.ticks++
	n, tick, hook := t.c.now, t.c.ticks, t.c.onTick
	t.c.mu.Unlock()
	if hook != nil {
		hook(tick)
	}
	ch := make(chan time.Time, 1)
	ch <- n
	return ch
}

func (t *virtualSettleTicker) Stop() {}

// useVirtualSettleClock installs c as the settle clock for the rest of the test.
func useVirtualSettleClock(t *testing.T, c *virtualSettleClock) {
	t.Helper()
	var wc clock.WithTicker = c
	prev := settleClock.Swap(&wc)
	t.Cleanup(func() { settleClock.Store(prev) })
}

func settleTestWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true") // the informer cache on: registrations count
	gvr := schema.GroupVersionResource{Group: "settle1166.example", Version: "v1", Resource: "things"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	rbac := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		gvr: "ThingList", settleLateGVR: "LateList",
		rbac("clusterrolebindings"): "ClusterRoleBindingList", rbac("clusterroles"): "ClusterRoleList",
		rbac("rolebindings"): "RoleBindingList", rbac("roles"): "RoleList",
	})
	ctx, cancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(ctx, dyn)
	if err != nil {
		cancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); cancel() })
	rw.EnsureResourceType(gvr)
	if rw.RegisteredCount() == 0 {
		t.Fatalf("precondition: the watcher registered nothing")
	}
	return rw
}

var settleLateGVR = schema.GroupVersionResource{Group: "settle1166.example", Version: "v1", Resource: "lates"}

// A registered set that never moves settles after exactly one window: the
// first poll that finds it unchanged for phase1SettleWindow ends the wait.
func TestIssue1166_SettleEndsOneWindowAfterTheLastChange(t *testing.T) {
	rw := settleTestWatcher(t)
	c := newVirtualSettleClock()
	useVirtualSettleClock(t, c)
	start := c.Now()

	settleRegisteredSet(context.Background(), rw)

	wantTicks := int((phase1SettleWindow + phase1SettlePoll - 1) / phase1SettlePoll)
	if c.ticks != wantTicks {
		t.Fatalf("a set that never changed settled after %d polls, want %d (%v window / %v poll)",
			c.ticks, wantTicks, phase1SettleWindow, phase1SettlePoll)
	}
	if got := c.Now().Sub(start); got < phase1SettleWindow || got >= phase1SettleWindow+phase1SettlePoll {
		t.Fatalf("settled %v after its start, want the first poll at or past %v", got, phase1SettleWindow)
	}
}

// A registration during the wait restarts the window at the poll that saw it:
// the settle ends one full window after the CHANGE, not after its own start.
func TestIssue1166_SettleRestartsTheWindowWhenTheSetGrows(t *testing.T) {
	rw := settleTestWatcher(t)
	c := newVirtualSettleClock()
	const changeAt = 3
	var seen time.Time
	c.onTick = func(tick int) {
		if tick == changeAt {
			rw.EnsureResourceType(settleLateGVR) // synchronous registration: RegisteredCount moves now
			c.mu.Lock()
			seen = c.now
			c.mu.Unlock()
		}
	}
	useVirtualSettleClock(t, c)
	before := rw.RegisteredCount()

	settleRegisteredSet(context.Background(), rw)

	if rw.RegisteredCount() != before+1 {
		t.Fatalf("precondition: the late registration did not land (%d → %d)", before, rw.RegisteredCount())
	}
	if got := c.Now().Sub(seen); got < phase1SettleWindow || got >= phase1SettleWindow+phase1SettlePoll {
		t.Fatalf("settled %v after the poll that saw the set grow, want one window (%v) after it", got, phase1SettleWindow)
	}
	wantTicks := changeAt + int((phase1SettleWindow+phase1SettlePoll-1)/phase1SettlePoll)
	if c.ticks != wantTicks {
		t.Fatalf("settled after %d polls, want %d (the change at poll %d, then one window)", c.ticks, wantTicks, changeAt)
	}
}
