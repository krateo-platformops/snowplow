// refresher_two_tier_test.go — Path 3.2 / 0.30.218 — two-tier priority
// queue falsifiers for the refresher.
//
// Three invariants under test:
//
//   1. CLUSTER-LIST PRIORITY: when keys are enqueued into BOTH tiers,
//      the worker drains the cluster_list tier FIRST (the high-priority
//      property — design §2.2).
//
//   2. TIER-ROUTING: a dep-tracker dirty-mark on a key REGISTERED via
//      RegisterClusterListKey routes to clusterListQueue; an unregistered
//      key routes to the normal queue (the tier-membership lookup is
//      load-bearing).
//
//   3. SUSTAINED-BURST NO-STARVATION (AC-P3.2.13 in unit form): under a
//      sustained mix of cluster_list + per-user enqueues at a sustained
//      rate, BOTH tiers make progress; the per-user tier p99 wait-time
//      does not exceed 60s (we use a much tighter unit-test ceiling to
//      catch starvation in a few hundred ms).

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// resetRefresherForTwoTierTest builds a fresh refresher singleton + a
// fresh ResolvedCache so each test runs in isolation. The Cleanup
// re-resets everything so a subsequent test (in the same package) sees
// a fully cleared state — without this, the dep tracker's enqueueFn
// closure from StartRefresher persists across test boundaries and can
// race the next test's L1 invariants.
func resetRefresherForTwoTierTest(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// Task #321 (#318-R1a) — disable the per-key re-resolve rate-floor by
	// default: the two-tier priority-drain tests Put fresh cells then assert
	// IMMEDIATE drain ordering, which the floor (default 2s) would defer.
	// floor=0 is the byte-identical kill-switch; the floor's own falsifiers
	// live in refresher_rate_floor_test.go.
	t.Setenv(envRefresherRateFloorSeconds, "0")
	ResetResolvedCacheForTest()
	ResetDepsForTest()
	resetRefresherForTest()
	t.Cleanup(func() {
		// Full reset on exit — the StartRefresher hook installed during
		// this test's body must not leak into the next test's
		// Deps().OnDelete classifier path.
		resetRefresherForTest()
		ResetDepsForTest()
		ResetResolvedCacheForTest()
	})
}

// putAndKey inserts a minimal ResolvedEntry into ResolvedCache under a
// fresh key + returns the key. The entry's Inputs.CacheEntryClass picks
// the handler the refresher will dispatch to.
func putAndKey(t *testing.T, class, name string) string {
	t.Helper()
	c := ResolvedCache()
	if c == nil {
		t.Fatalf("ResolvedCache nil under cache=on")
	}
	in := ResolvedKeyInputs{
		CacheEntryClass: class,
		Group:           "test.io",
		Version:         "v1",
		Resource:        "things",
		Name:            name,
	}
	key := ComputeKey(in)
	in2 := in
	c.put(key, &ResolvedEntry{RawJSON: []byte("{}"), Inputs: &in2})
	return key
}

// TestRefresher_ClusterListPriorityDrain — when both tiers have keys
// queued AND the worker pool is single-threaded, the cluster_list-tier
// keys MUST drain before any normal-tier key.
func TestRefresher_ClusterListPriorityDrain(t *testing.T) {
	resetRefresherForTwoTierTest(t)
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")

	// Record order via a shared slice + mutex.
	var (
		mu        sync.Mutex
		order     []string
		clKeys    []string
		normalKey string
	)

	// Pre-populate cells. Class doesn't matter for the test — we just
	// need entries the refresher's processOne won't skip.
	for i := 0; i < 4; i++ {
		k := putAndKey(t, CacheEntryClassApistage, "cl-cell-"+string(rune('a'+i)))
		clKeys = append(clKeys, k)
		RegisterClusterListKey(k)
	}
	normalKey = putAndKey(t, "restactions", "per-user-cell-0")

	// Trip a handler that records the order.
	called := make(chan string, 8)
	RegisterRefreshFunc(CacheEntryClassApistage, func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		mu.Lock()
		order = append(order, "CL:"+key[:6])
		mu.Unlock()
		called <- key
		return nil
	})
	RegisterRefreshFunc("restactions", func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		mu.Lock()
		order = append(order, "N:"+key[:6])
		mu.Unlock()
		called <- key
		return nil
	})

	// Enqueue interleaved BEFORE starting the worker so the priority
	// decision is purely on tier selection, not on temporal arrival
	// order. The normal-tier key arrives FIRST in enqueue time. If the
	// worker dispatches in enqueue order, normalKey would fire first.
	// Path 3.2's two-tier discipline must instead drain ALL cluster_list
	// keys before the normal one.
	r := refresherSingleton()
	r.enqueue(normalKey)
	for _, k := range clKeys {
		r.enqueueClusterList(k)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	StartRefresher(ctx)
	defer StopRefresher()

	// Wait for all 5 keys to fire.
	deadline := time.After(3 * time.Second)
	for i := 0; i < 5; i++ {
		select {
		case <-called:
		case <-deadline:
			t.Fatalf("refresher did not drain all keys; got %d/5 — order=%v", i, order)
		}
	}

	// Assert: every CL entry appears in `order` before any N entry.
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 5 {
		t.Fatalf("order: expected 5 entries; got %d (%v)", len(order), order)
	}
	firstNormal := -1
	lastCL := -1
	for i, e := range order {
		if len(e) >= 2 && e[0] == 'N' {
			if firstNormal == -1 {
				firstNormal = i
			}
		} else if len(e) >= 3 && e[0] == 'C' && e[1] == 'L' {
			lastCL = i
		}
	}
	if firstNormal != -1 && lastCL > firstNormal {
		t.Fatalf("Path 3.2 priority FAIL: a normal-tier key drained before a cluster_list key. "+
			"order=%v firstNormal_idx=%d lastCL_idx=%d", order, firstNormal, lastCL)
	}
}

// TestRefresher_TierRouting — RegisterClusterListKey marks a key as
// belonging to the cluster_list tier; IsClusterListKey reports it; and
// the dep-tracker dirty-mark hook installed by StartRefresher routes
// the key to the correct queue.
func TestRefresher_TierRouting(t *testing.T) {
	resetRefresherForTwoTierTest(t)

	clKey := putAndKey(t, CacheEntryClassApistage, "registered-cl")
	normalKey := putAndKey(t, "restactions", "unregistered-normal")
	RegisterClusterListKey(clKey)

	// Block any drain so the keys remain queued for inspection. Use a
	// shutdown-free queue by calling enqueue directly + checking
	// pending lengths.
	r := refresherSingleton()
	if !IsClusterListKey(clKey) {
		t.Errorf("RegisterClusterListKey: IsClusterListKey returned false on registered key")
	}
	if IsClusterListKey(normalKey) {
		t.Errorf("IsClusterListKey(unregistered)=true; want false")
	}

	// Simulate the dep-tracker dirty-mark hook the refresher installs.
	// StartRefresher wires it; bypass StartRefresher to inspect routing
	// without a draining worker.
	hook := func(l1Key string) {
		if _, isCL := r.clusterListKeys.Load(l1Key); isCL {
			r.enqueueClusterList(l1Key)
		} else {
			r.enqueue(l1Key)
		}
	}
	hook(clKey)
	hook(normalKey)

	if got := r.clusterListEnqueueTotal.Load(); got != 1 {
		t.Errorf("clusterListEnqueueTotal: got %d want 1 (registered key did NOT route to cluster_list tier)", got)
	}
	// enqueueTotal counts BOTH tiers' enqueues (a grand total).
	if got := r.enqueueTotal.Load(); got != 2 {
		t.Errorf("enqueueTotal: got %d want 2 (one cluster_list + one normal)", got)
	}
	if got := r.clusterListQueue.Len(); got != 1 {
		t.Errorf("clusterListQueue.Len()=%d want 1", got)
	}
	if got := r.queue.Len(); got != 1 {
		t.Errorf("normal queue.Len()=%d want 1 (cluster_list key did not leak into normal queue)", got)
	}

	// EnqueueClusterListRefresh API also routes correctly + registers.
	newCL := putAndKey(t, CacheEntryClassApistage, "auto-registered-cl")
	EnqueueClusterListRefresh(newCL)
	if !IsClusterListKey(newCL) {
		t.Errorf("EnqueueClusterListRefresh did not auto-register the key")
	}
	if got := r.clusterListEnqueueTotal.Load(); got != 2 {
		t.Errorf("clusterListEnqueueTotal after EnqueueClusterListRefresh: got %d want 2", got)
	}
}

// TestRefresher_SustainedBurstNoStarvation — AC-P3.2.13 in unit form.
// Under a sustained mix of cluster_list + normal-tier enqueues, BOTH
// tiers make progress and the normal-tier p99 wait-time stays under
// a tight unit-test ceiling (1 second). Production AC-P3.2.13 ceiling
// is 60s.
func TestRefresher_SustainedBurstNoStarvation(t *testing.T) {
	resetRefresherForTwoTierTest(t)
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "2")

	// Pre-populate 50 normal-tier cells + 5 cluster_list cells (scaled
	// down from 100/10 to keep test wall-clock + L1 footprint low —
	// the starvation property scales identically at this size).
	const (
		clCells     = 5
		normalCells = 50
	)
	clKeyList := make([]string, 0, clCells)
	for i := 0; i < clCells; i++ {
		k := putAndKey(t, CacheEntryClassApistage, "cl-"+itoaTT(i))
		clKeyList = append(clKeyList, k)
		RegisterClusterListKey(k)
	}
	normalKeyList := make([]string, 0, normalCells)
	for i := 0; i < normalCells; i++ {
		normalKeyList = append(normalKeyList, putAndKey(t, "restactions", "n-"+itoaTT(i)))
	}

	// Latency capture per key.
	enqueueTimes := make(map[string]time.Time, normalCells+clCells)
	completionLatency := make(map[string]time.Duration, normalCells+clCells)
	var latMu sync.Mutex
	var completedCount atomic.Int32

	RegisterRefreshFunc(CacheEntryClassApistage, func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		latMu.Lock()
		if t0, ok := enqueueTimes[key]; ok {
			completionLatency[key] = time.Since(t0)
		}
		latMu.Unlock()
		completedCount.Add(1)
		// Simulate cluster_list cell refresh: ~5ms (~100× faster than
		// real prod 500-2000ms — unit test only).
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	RegisterRefreshFunc("restactions", func(ctx context.Context, key string, in ResolvedKeyInputs) error {
		latMu.Lock()
		if t0, ok := enqueueTimes[key]; ok {
			completionLatency[key] = time.Since(t0)
		}
		latMu.Unlock()
		completedCount.Add(1)
		// Per-user cell: ~1ms (faster than cluster_list).
		time.Sleep(1 * time.Millisecond)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	StartRefresher(ctx)
	defer StopRefresher()

	// Burst: interleaved enqueue. The worker pool drains while we
	// enqueue — sustained burst, not a one-shot.
	r := refresherSingleton()
	t0 := time.Now()
	for i := 0; i < normalCells; i++ {
		// Every 10 normal enqueues, fire a cluster_list one too.
		if i%10 == 0 && i/10 < clCells {
			clKey := clKeyList[i/10]
			latMu.Lock()
			enqueueTimes[clKey] = time.Now()
			latMu.Unlock()
			r.enqueueClusterList(clKey)
		}
		nKey := normalKeyList[i]
		latMu.Lock()
		enqueueTimes[nKey] = time.Now()
		latMu.Unlock()
		r.enqueue(nKey)
		// Small pacing — simulates real-world dirty-mark rate.
		time.Sleep(100 * time.Microsecond)
	}

	// Wait for all to complete.
	deadline := time.After(8 * time.Second)
	for completedCount.Load() < int32(normalCells+clCells) {
		select {
		case <-deadline:
			t.Fatalf("not all keys drained in 8s — completed=%d/%d (cluster_list starved normal tier or vice versa). burst_started=%v",
				completedCount.Load(), normalCells+clCells, t0)
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Compute p99 for normal tier.
	latMu.Lock()
	defer latMu.Unlock()
	if len(completionLatency) < normalCells+clCells {
		t.Fatalf("captured %d latencies; want %d", len(completionLatency), normalCells+clCells)
	}
	var normalLatencies []time.Duration
	for _, k := range normalKeyList {
		if d, ok := completionLatency[k]; ok {
			normalLatencies = append(normalLatencies, d)
		}
	}
	// Sort + take p99.
	for i := 1; i < len(normalLatencies); i++ {
		for j := i; j > 0 && normalLatencies[j-1] > normalLatencies[j]; j-- {
			normalLatencies[j-1], normalLatencies[j] = normalLatencies[j], normalLatencies[j-1]
		}
	}
	p99idx := (len(normalLatencies) * 99) / 100
	if p99idx >= len(normalLatencies) {
		p99idx = len(normalLatencies) - 1
	}
	p99 := normalLatencies[p99idx]
	const p99Ceiling = time.Second
	if p99 > p99Ceiling {
		t.Fatalf("AC-P3.2.13 (unit form) FAIL: normal-tier p99=%v exceeds %v — cluster_list tier starved per-user tier. "+
			"normalCells=%d clCells=%d", p99, p99Ceiling, normalCells, clCells)
	}
	t.Logf("AC-P3.2.13 (unit form) PASS: normal-tier p99=%v (ceiling %v); cluster_list p99 not measured separately but both tiers drained.", p99, p99Ceiling)
}

// itoaTT is a tiny test helper local to this file (avoids collision
// with deps_test.go's itoa).
func itoaTT(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	p := len(buf)
	for i > 0 {
		p--
		buf[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		buf[p] = '-'
	}
	return string(buf[p:])
}
