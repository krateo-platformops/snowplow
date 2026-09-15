// refresh_eviction_falsifier_test.go — 1.12.6 item 7 (C10/C11) cache-layer
// falsifiers that compile on main 10f4951 (no new symbols), so each carries
// a RED/GREEN-on-main transcript: S1c, S1d, S12. The arms that need the C10/
// C11/C12 instruments live in refresh_eviction_c10_test.go.
//
// Pure in-memory (real dep tracker, real store, real hub); KUBECONFIG unset;
// NEVER touches ./internal/rbac.
//
//	S1c — no subscriber armed ⇒ a DELETE eviction publishes nothing and the
//	      broadcaster counters stay flat (the bench/cleanup zero-cost claim).
//	S1d — a TTL eviction (resolved.go Get, the TTLOverride/max-age site) and
//	      an LRU eviction (Put over cap) emit NOTHING to an armed subscriber.
//	S12 — MEASURED per-entry cost of the hub's reverse index at N subscribers
//	      × M keys (heap delta, not an estimate; design §6.3 takes this number).

package cache

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// evictionHarness wires the SSE layer, a clean dep tracker and a store the
// tracker evicts from. The store parameters are the caller's (TTL/cap arms
// need small ones).
func evictionHarness(t *testing.T, maxEntries int, ttl time.Duration) (*DepTracker, *ResolvedCacheStore) {
	t.Helper()
	withRefreshLayer(t)
	cleanup := withCleanDepWatch(t)
	t.Cleanup(cleanup)
	store := newResolvedCache(maxEntries, 1<<20, ttl)
	d := Deps()
	d.SetStore(store)
	return d, store
}

// putSelf stores a self-representation entry for (gvr, ns, name) under key
// and records its self dep edge — the shape a DELETE of that object evicts.
func putSelf(store *ResolvedCacheStore, d *DepTracker, key string, gvr schema.GroupVersionResource, ns, name string) {
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: widgetInputs(gvr, ns, name)})
	d.Record(key, gvr, ns, name)
}

// --- S1c — no subscriber: eviction publishes nothing --------------------------

func TestRefreshEviction_S1c_NoSubscriber_PublishesNothing(t *testing.T) {
	d, store := evictionHarness(t, 100, time.Hour)
	gvr := gvrFlexes()
	const key = "L1_s1c"
	putSelf(store, d, key, gvr, "ns", "gone")

	if n := d.OnDelete(gvr, "ns", "gone"); n != 1 {
		t.Fatalf("S1c precondition: OnDelete evicted %d entries, want 1", n)
	}
	if _, ok := store.Get(key); ok {
		t.Fatalf("S1c precondition: the entry survived the DELETE")
	}
	pub, del, drop, coal := RefreshBroadcasterCounters()
	if pub|del|drop|coal != 0 {
		t.Fatalf("S1c RED: an eviction with NO subscriber moved the broadcaster counters "+
			"(published=%d delivered=%d dropped=%d coalesced=%d) — the bench/cleanup teardown must cost zero",
			pub, del, drop, coal)
	}

	// Panic probe — the instrument is live: the SAME eviction shape WITH a
	// subscriber armed must deliver, or the zero above proves nothing.
	putSelf(store, d, key, gvr, "ns", "gone")
	ch, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()
	if n := d.OnDelete(gvr, "ns", "gone"); n != 1 {
		t.Fatalf("S1c probe: OnDelete evicted %d, want 1", n)
	}
	if got, ok := drainOne(t, ch, 2*time.Second); !ok || got != key {
		t.Fatalf("S1c probe RED: an armed subscriber received nothing for a DELETE eviction (got %q ok=%v) — "+
			"deps.go runEvictionBatch does not publish", got, ok)
	}
}

// --- S1d — TTL and LRU evictions stay silent ----------------------------------

func TestRefreshEviction_S1d_TTLAndLRU_Silent(t *testing.T) {
	gvr := gvrFlexes()

	t.Run("ttl", func(t *testing.T) {
		d, store := evictionHarness(t, 100, 10*time.Millisecond)
		const key = "L1_s1d_ttl"
		putSelf(store, d, key, gvr, "ns", "ttl")
		ch, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
		defer unsub()

		time.Sleep(25 * time.Millisecond)
		if _, ok := store.Get(key); ok {
			t.Fatalf("S1d precondition: the entry did not TTL-expire")
		}
		if st := store.Stats(); st.EvictTTLTotal != 1 {
			t.Fatalf("S1d precondition: evict_ttl_total=%d want 1", st.EvictTTLTotal)
		}
		if got, ok := drainOne(t, ch, 150*time.Millisecond); ok {
			t.Fatalf("S1d RED: a TTL eviction published %q to an armed subscriber — resolved.go's Get "+
				"eviction site must stay silent (a periodic refetch storm otherwise)", got)
		}
		if _, del, _, _ := RefreshBroadcasterCounters(); del != 0 {
			t.Fatalf("S1d RED: delivered=%d after a TTL eviction, want 0", del)
		}
	})

	t.Run("lru", func(t *testing.T) {
		d, store := evictionHarness(t, 1, time.Hour) // cap 1 entry: the 2nd Put evicts the 1st
		const key = "L1_s1d_lru"
		putSelf(store, d, key, gvr, "ns", "lru")
		ch, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
		defer unsub()

		putSelf(store, d, "L1_s1d_other", gvr, "ns", "other")
		if _, ok := store.Get(key); ok {
			t.Fatalf("S1d precondition: the entry was not LRU-evicted at cap 1")
		}
		if st := store.Stats(); st.EvictLRUTotal < 1 {
			t.Fatalf("S1d precondition: evict_lru_total=%d want >=1", st.EvictLRUTotal)
		}
		if got, ok := drainOne(t, ch, 150*time.Millisecond); ok {
			t.Fatalf("S1d RED: an LRU eviction published %q to an armed subscriber — resolved.go's cap "+
				"eviction site must stay silent", got)
		}
		if _, del, _, _ := RefreshBroadcasterCounters(); del != 0 {
			t.Fatalf("S1d RED: delivered=%d after an LRU eviction, want 0", del)
		}
	})

	t.Run("probe-delete-delivers", func(t *testing.T) {
		// Panic probe — the same armed subscriber DOES hear a DELETE
		// eviction, so the silence above is a property of the TTL/LRU
		// sites, not of a dead sink.
		d, store := evictionHarness(t, 100, time.Hour)
		const key = "L1_s1d_probe"
		putSelf(store, d, key, gvr, "ns", "probe")
		ch, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
		defer unsub()
		d.OnDelete(gvr, "ns", "probe")
		if got, ok := drainOne(t, ch, 2*time.Second); !ok || got != key {
			t.Fatalf("S1d probe RED: DELETE eviction not delivered (got %q ok=%v)", got, ok)
		}
	})

	t.Run("drop-point-non404-silent", func(t *testing.T) {
		// 1.12.6 C4 (Track B) shares EvictSelfGone's body with EvictDropPoint:
		// a deterministic NON-404 refresh failure (403 / 500 / timeout) that
		// exhausts the requeue budget is evicted at the drop point behind
		// the breaker. That is NOT a confirmed deletion — during an apiserver
		// outage it fires for every entry that comes up for refresh — so it
		// must never publish: an armed tab told "your widgets were deleted"
		// would refetch into the same outage. Drives the REAL breaker route
		// (the refresher loop, the same shape as Track B's B3b/F4 arms with
		// a 500 instead of ErrSelfObjectGone) with an armed subscriber.
		//
		// RED discipline: main will already contain Track B when this lands,
		// so this arm is not RED on main; it is RED on the variant that
		// publishes from the shared body evictSelfEntry instead of the
		// EvictSelfGone arm (probe P12 in the dev report).
		withRefreshLayer(t)
		t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
		t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
		t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
		t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
		t.Setenv("REFRESH_DROP_EVICT_MAX_PER_MINUTE", "64") // the default, pinned: the breaker grants
		resetRefresherForTest()
		defer resetRefresherForTest()
		defer withCleanDepWatch(t)()

		store := ResolvedCache()
		if store == nil {
			t.Fatalf("ResolvedCache() nil — RESOLVED_CACHE_ENABLED not honoured")
		}
		Deps().SetStore(store)
		const ns, name = "krateo-system", "outage-widget"
		inputs := widgetInputs(gvr, ns, name)
		key := ComputeKey(*inputs)
		store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"stale"}`), Inputs: inputs})
		t.Cleanup(func() { store.DeleteForTest(key) })
		Deps().Record(key, gvr, ns, name)

		ch, unsub := SubscribeRefresh(map[string]struct{}{key: {}})
		defer unsub()
		beforeDropPoint := DepsStatsByStat()["evict_drop_point_total"]
		beforeSelfGone := Deps().Stats().EvictSelfGoneTotal

		var attempts atomic.Int64
		RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
			attempts.Add(1)
			return fmt.Errorf("resolveAndPopulateL1 %s/%s: re-fetch %s/%s: the server reported an internal error (500)",
				in.CacheEntryClass, in.Name, in.Resource, in.Name)
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		StartRefresher(ctx)
		EnqueueRefresh(key)

		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, ok := store.Get(key); !ok && attempts.Load() > maxRefreshRequeues {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, ok := store.Get(key); ok {
			t.Fatalf("S1d precondition: after %d non-404 failures the entry is still resident — the drop point did not evict", attempts.Load())
		}
		if got := DepsStatsByStat()["evict_drop_point_total"]; got != beforeDropPoint+1 {
			t.Fatalf("S1d precondition: evict_drop_point_total=%d want %d — the eviction did not route through EvictDropPoint", got, beforeDropPoint+1)
		}
		if got := Deps().Stats().EvictSelfGoneTotal; got != beforeSelfGone {
			t.Fatalf("S1d precondition: evict_self_gone_total moved (%d -> %d) for a 500", beforeSelfGone, got)
		}
		if got, ok := drainOne(t, ch, 500*time.Millisecond); ok {
			t.Fatalf("S1d RED: a NON-404 drop-point eviction published %q to an armed subscriber — "+
				"the publish must live in the EvictSelfGone (confirmed 404) arm, never in the shared "+
				"evictSelfEntry body: an apiserver outage would tell every armed tab its widgets were deleted", got)
		}
		if st := RefreshBroadcasterStatsSnapshot(); st.EvictPublished != 0 || st.Delivered != 0 {
			t.Fatalf("S1d RED: evict_published=%d delivered=%d after a drop-point eviction, want 0/0", st.EvictPublished, st.Delivered)
		}
	})
}

// --- S12 — measured reverse-index cost ------------------------------------------

// heapAlloc returns HeapAlloc after two GCs (the second collects what the
// first's finalizers freed).
func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// measureSubscribe subscribes n connections with keysFor(i) armed each and
// returns the heap delta they retain (bytes) plus the unsubs.
func measureSubscribe(n int, keysFor func(i int) map[string]struct{}) (int64, []func()) {
	unsubs := make([]func(), 0, n)
	before := heapAlloc()
	for i := 0; i < n; i++ {
		_, u := SubscribeRefresh(keysFor(i))
		unsubs = append(unsubs, u)
	}
	after := heapAlloc()
	return int64(after) - int64(before), unsubs
}

// TestRefreshEviction_S12_KeySubsPerEntryCost measures — does not assert
// from an estimate — what one (subscriber, armed key) reverse-index entry
// costs, at the design's scale point (1000 tabs × 30 widgets). Three
// measurements separate the per-connection base (the sub struct, its 64-slot
// sink, the limiter) from the per-key increment, and the identity-free
// widgetContent case (30 keys SHARED by all 1000 tabs) from the per-identity
// case (30 000 distinct keys). The tripwire is deliberately loose: it exists
// to catch the 180× class of surprise, not to pin a byte count. Run WITHOUT
// -race for the reportable number (the race runtime inflates allocations).
func TestRefreshEviction_S12_KeySubsPerEntryCost(t *testing.T) {
	withRefreshLayer(t)
	const n, m = 1000, 30

	// Warm the hub so its own construction is not in the base delta.
	_, u0 := SubscribeRefresh(map[string]struct{}{"warm": {}})
	u0()

	base, unsubsBase := measureSubscribe(n, func(int) map[string]struct{} { return map[string]struct{}{} })
	for _, u := range unsubsBase {
		u()
	}
	resetRefreshBroadcasterForTest()
	_, u1 := SubscribeRefresh(map[string]struct{}{"warm": {}})
	u1()

	unique, unsubsUnique := measureSubscribe(n, func(i int) map[string]struct{} {
		ks := make(map[string]struct{}, m)
		for j := 0; j < m; j++ {
			ks[fmt.Sprintf("L1_%04d_%02d", i, j)] = struct{}{}
		}
		return ks
	})
	uniqueArmed := n * m // distinct keys in the reverse index
	for _, u := range unsubsUnique {
		u()
	}
	resetRefreshBroadcasterForTest()
	_, u2 := SubscribeRefresh(map[string]struct{}{"warm": {}})
	u2()

	shared, unsubsShared := measureSubscribe(n, func(int) map[string]struct{} {
		ks := make(map[string]struct{}, m)
		for j := 0; j < m; j++ {
			ks[fmt.Sprintf("L1_shared_%02d", j)] = struct{}{}
		}
		return ks
	})
	sharedArmed := m // the 30 shared keys
	for _, u := range unsubsShared {
		u()
	}

	perSubBase := base / n
	perEntryUnique := (unique - base) / (n * m)
	perEntryShared := (shared - base) / (n * m)
	t.Logf("S12 keySubs cost @ %d subs × %d keys (race=%v): per-sub base %d B; "+
		"per (sub,key) entry: unique keys %d B (armed_keys=%d, total %d B), shared keys %d B (armed_keys=%d, total %d B)",
		n, m, raceEnabled, perSubBase, perEntryUnique, uniqueArmed, unique, perEntryShared, sharedArmed, shared)

	if perEntryUnique <= 0 || perEntryShared <= 0 {
		t.Fatalf("S12: non-positive per-entry cost (unique=%d shared=%d) — the measurement did not retain the index", perEntryUnique, perEntryShared)
	}
	const tripwire = 2048
	if perEntryUnique > tripwire || perEntryShared > tripwire {
		t.Fatalf("S12 tripwire: per-entry cost unique=%d B shared=%d B exceeds %d B — re-derive the §6.3 memory figure",
			perEntryUnique, perEntryShared, tripwire)
	}
}
