// issue354_p3_f6d_test.go — #354 P3 F6d (map leak): mark 1,000 resident keys,
// evict every one by DELETE, drain: the window map is EMPTY and each window
// ended exactly once as evicted. RED on main: DirtyWindowsOpenForTest (and the
// map it reads) does not exist.

package cache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// --- F6d ---------------------------------------------------------------------

func TestIssue354_P3_F6d_WindowMapNeverOutlivesItsKeys(t *testing.T) {
	c := p3CacheEnv(t)
	gvr := schema.GroupVersionResource{Group: "p3.example", Version: "v1", Resource: "things"}
	const n = 1000
	var resolves atomic.Int64
	RegisterRefreshFunc("restactions", func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		resolves.Add(1)
		return nil
	})
	var hold atomic.Bool
	hold.Store(true)
	SetCustomerInflightHook(hold.Load)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	ev0 := p3RefresherStat("dirty_ended_unfresh_evicted")
	for i := 0; i < n; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "restactions", Group: gvr.Group, Version: gvr.Version,
			Resource: gvr.Resource, Namespace: "p3", Name: fmt.Sprintf("o%d", i), BindingUID: "uid"}
		key := ComputeKey(in)
		c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
		Deps().Record(context.Background(), key, gvr, "p3", in.Name)
	}
	for i := 0; i < n; i++ {
		Deps().OnUpdate(gvr, "p3", fmt.Sprintf("o%d", i))
	}
	if got := DirtyWindowsOpenForTest(); got != n {
		t.Fatalf("PRE: %d marked resident keys must hold %d open windows, got %d", n, n, got)
	}
	for i := 0; i < n; i++ {
		Deps().OnDelete(gvr, "p3", fmt.Sprintf("o%d", i))
	}
	// The bound is the RESIDENT set, not the queue: the evictions end the windows
	// at once, before any dequeue runs (the worker is parked).
	if got := DirtyWindowsOpenForTest(); got != 0 {
		t.Fatalf("F6d LEAK: %d windows outlive their DELETE-evicted keys (the map must never outgrow the resident set)", got)
	}
	hold.Store(false)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && refresherQueueDepth(refresherPeek()) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if got := DirtyWindowsOpenForTest(); got != 0 {
		t.Fatalf("F6d LEAK: %d windows still open after every key was DELETE-evicted and the queue drained", got)
	}
	if d := p3RefresherStat("dirty_ended_unfresh_evicted") - ev0; d != n {
		t.Fatalf("F6d: dirty_ended_unfresh_evicted Δ=%d, want %d (each window ends exactly once)", d, n)
	}
	if resolves.Load() != 0 {
		t.Fatalf("F6d: an evicted key must not be re-resolved (%d resolves)", resolves.Load())
	}
}

// F6d (refill) — a key evicted while dirty and cold-filled again before its
// queued dequeue is a NEW cell: a customer hit on it is not a stale serve.
func TestIssue354_P3_F6d_RefilledKeyIsNotDirty(t *testing.T) {
	c := p3CacheEnv(t)
	gvr := schema.GroupVersionResource{Group: "p3.example", Version: "v1", Resource: "things"}
	RegisterRefreshFunc("restactions", func(context.Context, string, ResolvedKeyInputs) error { return nil })
	var hold atomic.Bool
	hold.Store(true)
	SetCustomerInflightHook(hold.Load)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	defer hold.Store(false)

	in := ResolvedKeyInputs{CacheEntryClass: "restactions", Group: gvr.Group, Version: gvr.Version,
		Resource: gvr.Resource, Namespace: "p3", Name: "refill", BindingUID: "uid"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in})
	Deps().Record(context.Background(), key, gvr, "p3", in.Name)
	Deps().OnUpdate(gvr, "p3", in.Name)
	if _, dirty := dirtySince(key); !dirty {
		t.Fatalf("PRE: the mark must open a window")
	}
	Deps().OnDelete(gvr, "p3", in.Name)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":2}`), Inputs: &in}) // the customer's cold refill
	s0 := c.Stats().StaleServedTotal
	if _, ok := c.Get(key); !ok {
		t.Fatalf("PRE: the refill must be resident")
	}
	if d := c.Stats().StaleServedTotal - s0; d != 0 {
		t.Fatalf("F6d: a hit on a cell refilled after the DELETE counted as stale (Δ=%d) — the evicted window outlived its cell", d)
	}
}
