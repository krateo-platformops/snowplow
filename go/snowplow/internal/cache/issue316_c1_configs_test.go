//go:build c1amplification

// issue316_c1_configs_test.go — the C1 GATE CONFIGS (arch-1217 scenario,
// pm-1217 result-gates) on the substrate in issue316_c1_amplification_test.go.
// Run against the FROZEN land SHA f9c85e34 (PR #357 HEAD).
//
//	go test -tags c1amplification -run TestC1Gate -timeout 30m ./internal/cache/
//	C1_WARMSET=5000 go test -tags c1amplification -run TestC1Gate ...   # reduced-scale validate
//
// A/B (arch/pm/TL, same-build, no cross-build confound): the #316 pass is
// toggled by cache.SetProactiveRefreshEnabledForTest(bool) (test-only seam in
// issue316_toggle_bench_test.go) — OFF = baseline (the hole is live), ON =
// proactive-on. Both arms run reapPastMaxEntryAge (so #315 cold-evict + #248
// reap are identical); only the #316 refresh-enqueue differs → the A/B
// isolates the pass's amplification. Restore to true after the OFF arm.
//
// SIZING (arch-1217, from the 057 read: mean=75.6 KiB, cap=2 GiB binds at
// ~27.7K): PRIMARY warm-set = ~27K at 75.6 KiB mean, byte cap ENABLED at 2 GiB
// → sits just under the cap, so the gate measures reaper+#316 amplification
// with NO capacity-eviction confound (asserted: evict_lru_total delta == 0).
// SKEW variant: same mean, 1 MiB p99 tail. SATURATING falsifier: warm-set past
// the cap so capacity eviction FIRES — confirm the reaper does not storm.
//
// Time-compression = option (c): discrete pass-ticks at the production ratio
// with REALISTIC ABSOLUTE latency (5ms RTT + 10ms/MB; never zero — the
// admissibility precondition). tick-1 = the synchronized burst (the ceiling).

package cache

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- small local helpers ---

func c1EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Production anchors (arch-1217).
const (
	c1TTLSeconds      = 3600  // RESOLVED_CACHE_TTL_SECONDS default (resolved.go:126)
	c1MaxAgeSeconds   = 86400 // RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS default (:127)
	c1CapBytes        = 2147483648 // 2 GiB — defaultResolvedCacheMaxBytes (:125)
	c1PassInterval    = 300 * time.Second
	c1Parallelism     = 4     // production refresher parallelism
	c1WarmSetDefault  = 27000 // ~27.7K: the 2 GiB cap ceiling at 75.6 KiB mean (just under)
	c1RefreshQualify  = (c1TTLSeconds * 3 / 4) * time.Second // ~2700s: a warm cell re-qualifies each ~TTL×3/4

	// c1CustRate — 057-anchored customer Get-rate (arch): raw 505/s minus the
	// refresher's 374/s counted no-op lookups ≈ 131/s customer Gets, >90% hit.
	// (Conservative upper bound; the ~4.3K real working set is 6.5× under our 27K.)
	c1CustRate = 131
	// c1HandlerSpan — the server-side warm-handler span (parse+RBAC+c.Get+
	// marshal+write); the inflight window the yield keys on. NOT the 500ms
	// end-to-end north-star. The verdict is proven insensitive across 1-10ms.
	c1HandlerSpan = 1500 * time.Microsecond
)

func c1WarmSet(t *testing.T) int {
	t.Helper()
	if v := c1EnvInt("C1_WARMSET", c1WarmSetDefault); v > 0 {
		return v
	}
	return c1WarmSetDefault
}

// c1SetEnv sets the production-anchored cache/refresher env WITH the 2 GiB byte
// cap ENABLED (so the primary gate sits just under it — capacity eviction is
// off unless a variant deliberately oversizes). capBytes lets a variant raise
// it out of the way. floorSeconds is the refresher rate-floor.
func c1SetEnv(t *testing.T, floorSeconds string, capBytes int64) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv(envResolvedCacheMaxEntries, "200000") // entry-count non-binding; the byte cap is the ceiling
	t.Setenv(envResolvedCacheMaxBytes, fmt.Sprintf("%d", capBytes))
	t.Setenv(envResolvedCacheTTLSeconds, fmt.Sprintf("%d", c1TTLSeconds))
	t.Setenv(envResolvedCacheMaxEntryAgeSeconds, fmt.Sprintf("%d", c1MaxAgeSeconds))
	t.Setenv(envResolvedCacheSummaryEvery, "36000") // inert auto-ticker; drive the pass by hand
	t.Setenv(envRefresherRateFloorSeconds, floorSeconds)
}

// c1AvgResolveCost is the realistic re-resolve latency at the 057 mean size —
// RTT-dominated (~5ms + ~0.74ms decode at 75.6 KiB), per arch (latency is
// size-insensitive; entry size is budget-critical, not latency-critical).
func c1AvgResolveCost() time.Duration { return c1RealisticResolveCost(c1MeanBytes) }

func c1Capacity(parallelism int) float64 { return float64(parallelism) / c1AvgResolveCost().Seconds() }

// ---------------------------------------------------------------------------
// customer-serve load — concurrent /call Gets on the working set, driving the
// customer-inflight hook (Ship #98 yield) and recording serve-latency.
// ---------------------------------------------------------------------------

type c1CustomerLoad struct {
	mu       sync.Mutex
	lat      []time.Duration
	inflight atomic.Int64
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// startC1CustomerLoad drives concurrent customer /call serves. serveHold models
// the realistic /call in-flight window (handler+network budget) during which
// the customer-inflight signal stays TRUE — so the refresher's Ship #98 yield
// is exercised even though the L1 Get itself is µs-fast. It records ONLY the
// c.Get latency (the store-lock contention signal), NOT the modelled hold, so
// the reported serve p95 is the true mechanism cost, not the injected budget.
func startC1CustomerLoad(c *ResolvedCacheStore, keys []string, ratePerSec int, serveHold time.Duration, neuterYield bool) *c1CustomerLoad {
	cl := &c1CustomerLoad{stopCh: make(chan struct{})}
	if neuterYield {
		// NEUTER positive control: the refresher NEVER sees a customer in flight,
		// so Ship #98 never parks (yielded stays 0) — customer serves still run
		// and are measured. Un-neutered, the hook reflects the handler-span
		// inflight. The yielded 0↔>0 flip proves the yield term is non-vacuous.
		SetCustomerInflightHook(func() bool { return false })
	} else {
		SetCustomerInflightHook(func() bool { return cl.inflight.Load() > 0 })
	}
	if ratePerSec < 1 {
		ratePerSec = 1
	}
	interval := time.Second / time.Duration(ratePerSec)
	cl.wg.Add(1)
	go func() {
		defer cl.wg.Done()
		tk := time.NewTicker(interval)
		defer tk.Stop()
		i := 0
		for {
			select {
			case <-cl.stopCh:
				return
			case <-tk.C:
				key := keys[i%len(keys)]
				i++
				cl.inflight.Add(1) // request enters — inflight ON for the whole /call window
				s := time.Now()
				c.Get(key)
				d := time.Since(s) // MEASURE only the L1 lookup (the contention signal)
				if serveHold > d {
					time.Sleep(serveHold - d) // model the rest of the /call budget; inflight stays ON
				}
				cl.inflight.Add(-1) // request done
				cl.mu.Lock()
				cl.lat = append(cl.lat, d)
				cl.mu.Unlock()
			}
		}
	}()
	return cl
}

func (cl *c1CustomerLoad) stopAndStats() (p50, p95 time.Duration, n int) {
	close(cl.stopCh)
	cl.wg.Wait()
	SetCustomerInflightHook(func() bool { return false })
	cl.mu.Lock()
	defer cl.mu.Unlock()
	n = len(cl.lat)
	if n == 0 {
		return 0, 0, 0
	}
	cp := make([]time.Duration, n)
	copy(cp, cl.lat)
	for i := 1; i < len(cp); i++ {
		for j := i; j > 0 && cp[j-1] > cp[j]; j-- {
			cp[j-1], cp[j] = cp[j], cp[j-1]
		}
	}
	return cp[n*50/100], cp[min(n*95/100, n-1)], n
}

// ---------------------------------------------------------------------------
// drain measurement — poll the refresher until the burst drains, tracking peak
// queueDepth and the sample time-series.
// ---------------------------------------------------------------------------

type c1DrainResult struct {
	drainTime  time.Duration
	peakDepth  int64
	midDepth   int64
	endDepth   int64
	completed  uint64
	dropped    uint64
	enqueued   uint64
	yielded    uint64
	capped     uint64 // Ship #98 max-parked (5s) cap hits — 0 = healthy (yield never maxes out)
	depthTrace []int64
}

func c1DrainMeasure(base refresherStats, expectComplete uint64, timeout time.Duration) c1DrainResult {
	start := time.Now()
	deadline := start.Add(timeout)
	var r c1DrainResult
	for {
		s := refresherStatsSnapshot()
		r.depthTrace = append(r.depthTrace, s.queueDepth)
		if s.queueDepth > r.peakDepth {
			r.peakDepth = s.queueDepth
		}
		done := (s.completed-base.completed) >= expectComplete && s.queueDepth == 0
		if done || !time.Now().Before(deadline) {
			r.drainTime = time.Since(start)
			r.completed = s.completed - base.completed
			r.dropped = s.dropped - base.dropped
			r.enqueued = s.enqueued - base.enqueued
			r.yielded = s.yielded - base.yielded
			r.capped = s.capped - base.capped
			r.endDepth = s.queueDepth
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := len(r.depthTrace); n > 0 {
		r.midDepth = r.depthTrace[n/2]
	}
	return r
}

// ===========================================================================
// CONFIG 1 — BASELINE HEADROOM (A3). Event-refresh only (pass OFF) at the ~27K
// warm-set, 75.6 KiB mean, 2 GiB cap. Refresher keeps up + >=20% headroom for
// the projected proactive feed? HALT if not (fix void regardless of C3).
// ===========================================================================

func TestC1Gate_Config1_BaselineHeadroom(t *testing.T) {
	warmset := c1WarmSet(t)
	cleanup := withCleanRefresher(t, c1Parallelism, 0)
	defer cleanup()
	c1SetEnv(t, "0", c1CapBytes)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()
	SetProactiveRefreshEnabledForTest(false) // baseline: pass OFF
	t.Cleanup(func() { SetProactiveRefreshEnabledForTest(true) })

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	ttl := c1TTLSeconds * time.Second
	maxAge := c1MaxAgeSeconds * time.Second
	cells := c1Populate(t, c, warmset, ttl, maxAge, func(i int) (c1CellState, bool) {
		return c1WarmGet, false // warm, fresh
	}, c1UniformSize75)
	meter := newC1PickupMeter()
	c1RegisterRealisticRefresh(c, meter, c1SizeIndex(cells), 1.0)

	// Capacity-eviction confound check: the warm-set must sit UNDER the cap.
	popEvict := c.Stats().EvictLRUTotal
	residentBytes := c.Stats().Bytes
	if popEvict != 0 {
		t.Fatalf("CONFIG-1 SIZING ERROR: evict_lru_total=%d after populate — warm-set (%d @ ~75.6KiB = %d bytes) exceeds the 2 GiB cap; reduce C1_WARMSET so the gate measures the reaper, not the capacity evictor",
			popEvict, warmset, residentBytes)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	base := refresherStatsSnapshot()
	gvr := c1WidgetGVR()
	changeN := warmset * 5 / 100
	for i := 0; i < changeN; i++ {
		meter.markEnqueue(cells[i].key)
		Deps().OnUpdate(gvr, c1NS, cells[i].name)
	}
	r := c1DrainMeasure(base, uint64(changeN), 5*time.Minute)

	completedRatio := float64(r.completed) / float64(maxU64(r.enqueued, 1))
	droppedRatio := float64(r.dropped) / float64(maxU64(r.enqueued, 1))
	pickup := meter.p95()

	baselineOfferedRate := float64(changeN) / c1PassInterval.Seconds()
	projectedProactiveRate := float64(warmset) / c1RefreshQualify.Seconds()
	capacity := c1Capacity(c1Parallelism)
	utilization := (baselineOfferedRate + projectedProactiveRate) / capacity

	t.Logf("CONFIG-1 HEADROOM (warmset=%d @75.6KiB, residentBytes=%d, cap=%d): enq=%d completed=%d dropped=%d completed/enq=%.4f dropped/enq=%.4f queueDepth end=%d peak=%d drain=%s pickup_p95=%s evict_lru=%d",
		warmset, residentBytes, int64(c1CapBytes), r.enqueued, r.completed, r.dropped, completedRatio, droppedRatio, r.endDepth, r.peakDepth, r.drainTime, pickup, c.Stats().EvictLRUTotal)
	t.Logf("CONFIG-1 HEADROOM: baselineOfferedRate=%.2f/s projectedProactive=%.2f/s capacity=%.1f/s utilization=%.4f (headroom slack=%.1f%%)",
		baselineOfferedRate, projectedProactiveRate, capacity, utilization, (1-utilization)*100)

	if completedRatio < 0.95 {
		t.Fatalf("CONFIG-1 HALT: completed/enqueued=%.4f < 0.95 (refresher not keeping up at baseline)", completedRatio)
	}
	if droppedRatio >= 0.01 {
		t.Fatalf("CONFIG-1 HALT: dropped/enqueued=%.4f >= 0.01 (baseline saturation)", droppedRatio)
	}
	if r.endDepth != 0 {
		t.Fatalf("CONFIG-1 HALT: queueDepth did not drain to 0 (end=%d) — baseline saturating", r.endDepth)
	}
	if pickup > c1PassInterval {
		t.Fatalf("CONFIG-1 HALT: pickup_p95=%s > interval=%s", pickup, c1PassInterval)
	}
	if utilization > 0.80 {
		t.Fatalf("CONFIG-1 HALT: projected utilization=%.4f > 0.80 — no headroom for the proactive feed (fix void)", utilization)
	}
	t.Logf("CONFIG-1 HEADROOM: PASS (completed/enq=%.4f, dropped/enq=%.4f, drained, pickup<interval, utilization=%.4f<0.80)",
		completedRatio, droppedRatio, utilization)
}

// ===========================================================================
// CONFIG 2 — THROTTLE-STRESS / F1-REACHABILITY (A2 meta-check). parallelism=1
// + elevated latency + sustained churn = 3× single-worker throughput MUST trip
// the saturation HALT (queueDepth growing >1.5× over the window) while lazy-
// holes stay LOW (cold-navs ~0) AND the customer path is never blocked — the
// "throttle dominates" regime F1 must detect. If it does NOT saturate →
// GATE-INCOMPLETE (raise throttle), NOT a pass. Uses a light warm-set (the
// saturation is a refresher-queue property, independent of cache byte size).
// ===========================================================================

func TestC1Gate_Config2_ThrottleStress_F1Reachable(t *testing.T) {
	const churnCells = 5000 // light — saturation is a queue property, not a byte-budget one
	const elevation = 5.0
	cleanup := withCleanRefresher(t, 1, 0) // parallelism=1 — THE throttle
	defer cleanup()
	c1SetEnv(t, "0", c1CapBytes)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	ttl := c1TTLSeconds * time.Second
	maxAge := c1MaxAgeSeconds * time.Second
	cells := c1Populate(t, c, churnCells, ttl, maxAge, func(i int) (c1CellState, bool) {
		return c1WarmGet, false
	}, c1UniformSize75)
	c1RegisterRealisticRefresh(c, nil, c1SizeIndex(cells), elevation)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	// customer load throughout — assert the /call path is NEVER blocked even as
	// the refresher saturates (customer-over-refresher). serveHold models the
	// realistic in-flight window so the Ship #98 yield is exercised alongside
	// the queue growth (pm: yielded>0 must be shown live under the same
	// saturation where drain fails — both halves of the primary gate reachable).
	keys := make([]string, len(cells))
	for i := range cells {
		keys[i] = cells[i].key
	}
	cust := startC1CustomerLoad(c, keys, c1CustRate, c1HandlerSpan, false)

	singleWorker := 1.0 / (c1AvgResolveCost().Seconds() * elevation)
	offeredRate := 3.0 * singleWorker
	interval := time.Duration(float64(time.Second) / offeredRate)
	gvr := c1WidgetGVR()

	base := refresherStatsSnapshot()
	window := 15 * time.Second
	midAt := time.Now().Add(window / 2)
	deadline := time.Now().Add(window)
	var midDepth int64
	sampledMid := false
	tk := time.NewTicker(interval)
	i := 0
	for time.Now().Before(deadline) {
		<-tk.C
		Deps().OnUpdate(gvr, c1NS, cells[i%len(cells)].name)
		i++
		if !sampledMid && time.Now().After(midAt) {
			midDepth = refresherStatsSnapshot().queueDepth
			sampledMid = true
		}
	}
	tk.Stop()

	end := refresherStatsSnapshot()
	custP50, custP95, custN := cust.stopAndStats()
	enqDelta := end.enqueued - base.enqueued
	compDelta := end.completed - base.completed
	dropDelta := end.dropped - base.dropped
	yieldDelta := end.yielded - base.yielded
	cappedDelta := end.capped - base.capped
	completedRatio := float64(compDelta) / float64(maxU64(enqDelta, 1))
	droppedRatio := float64(dropDelta) / float64(maxU64(enqDelta, 1))
	growing := end.queueDepth > midDepth*3/2
	_, miss := c1Navigate(c, cells)

	t.Logf("CONFIG-2 THROTTLE (churn=%d parallelism=1 elevation=%.0f× offered=%.0f/s singleWorker=%.0f/s): enqueued=%d completed=%d completed/enq=%.3f dropped=%d dropped/enq=%.4f queueDepth mid=%d end=%d growing=%v yielded=%d cold-navs=%d customer p50=%s p95=%s (n=%d)",
		churnCells, elevation, offeredRate, singleWorker, enqDelta, compDelta, completedRatio, dropDelta, droppedRatio, midDepth, end.queueDepth, growing, yieldDelta, miss, custP50, custP95, custN)

	// This config is the REACHABLE HALT for the primary customer-priority gate
	// (pm teeth check): under saturation the refresher FALLS BEHIND (queue grows,
	// drain fails) — the failing half of "yielded>0 AND queueDepth→0" — WHILE
	// yield fires (yielded>0) and customers stay served (dropped==0, p95 sub-ms,
	// lossless). Both halves live in one run.
	saturated := growing || droppedRatio >= 0.01
	if !saturated {
		t.Fatalf("CONFIG-2 GATE-INCOMPLETE: queueDepth did NOT grow >1.5× (mid=%d end=%d) and dropped/enq=%.4f<0.01 — F1 HALT NOT reachable; raise throttle/rate before trusting the gate",
			midDepth, end.queueDepth, droppedRatio)
	}
	if completedRatio >= 0.98 {
		t.Fatalf("CONFIG-2 GATE-INCOMPLETE: completed/enq=%.3f>=0.98 — refresher kept up despite the throttle; F1 not exercised", completedRatio)
	}
	if custN == 0 {
		t.Fatalf("CONFIG-2 INVALID: no customer serves recorded — cannot assert the /call path stayed unblocked")
	}
	// pm PRIMARY-gate non-vacuity: yield MUST fire alongside the growth, else the
	// "yielded>0" term is an arm-that-cannot-fail here.
	if yieldDelta == 0 {
		t.Fatalf("CONFIG-2 INVALID: yielded=0 under saturation+customer-load — the customer-priority yield term is vacuous; the primary gate's yield half is not exercised")
	}
	// lossless invariant: no dirty-mark dropped even under saturation.
	if dropDelta != 0 {
		t.Fatalf("CONFIG-2 HALT: dropped=%d under saturation — the lossless 'never drop a dirty-mark' invariant broke", dropDelta)
	}
	// customer NOT starved even while the refresher is saturated: absolute serve
	// p95 stays sub-ms (0.1%% of the 500ms warm north-star).
	if custP95 >= time.Millisecond {
		t.Fatalf("CONFIG-2 HALT: customer serve p95=%s >= 1ms under saturation — /call path starved", custP95)
	}
	t.Logf("CONFIG-2 THROTTLE: F1-HALT REACHABLE CONFIRMED — saturation (queueDepth %d→%d growing=%v, completed/enq=%.3f<0.98) with yield FIRING (yielded=%d, cappedTotal=%d), lossless (dropped=0), customer served sub-ms (p95=%s, n=%d), lazy-holes LOW (cold-navs=%d). Both halves of the primary gate exercised; the gate has teeth.",
		midDepth, end.queueDepth, growing, completedRatio, yieldDelta, cappedDelta, custP95, custN, miss)
}

// ===========================================================================
// CONFIG 3 (worst-case burst) — the SYNCHRONIZED ~27K all-warm crossing: one
// proactive pass enqueues ~all resident warm cells at once (the per-pod
// ceiling). THE amplification gate: does the refresher drain the burst at
// realistic latency WITHOUT customer starvation (yield fires, customer p95
// holds) within the >2×-projection bound, and with NO capacity eviction? Uses
// the toggle ON. sizeFn selects the uniform (primary) or skew variant.
// ===========================================================================

type c1BurstResult struct {
	enqueued, completed, dropped, yielded, capped uint64
	endDepth, peakDepth                           int64
	drainTime                                     time.Duration
	projectedDrain                                float64
	custBaseP95, custP95, custP50                 time.Duration
	custN, coldNavs                               int
	evictLRUDelta                                 uint64
	completedRatio, droppedRatio                  float64
}

// c1RunWorstCaseBurst populates a synchronized ~warmset all-warm-approaching set
// and drives a burst of ~warmset large-body re-Puts, measured with concurrent
// customer load. proactiveOn=true drives the burst via the #316 proactive pass
// (reapPastMaxEntryAge); false drives the SAME re-Put volume via the EVENT path
// (OnUpdate all) — the normal-mutation comparison baseline with no #316. Returns
// the measurements; the caller applies the gate.
func c1RunWorstCaseBurst(t *testing.T, warmset int, sizeFn func(i int) int, label string, proactiveOn bool, serveHold time.Duration, neuterYield bool) c1BurstResult {
	cleanup := withCleanRefresher(t, c1Parallelism, 0)
	defer cleanup()
	c1SetEnv(t, "0", c1CapBytes)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()
	SetProactiveRefreshEnabledForTest(proactiveOn)
	t.Cleanup(func() { SetProactiveRefreshEnabledForTest(true) })

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	ttl := c1TTLSeconds * time.Second
	maxAge := c1MaxAgeSeconds * time.Second
	cells := c1Populate(t, c, warmset, ttl, maxAge, func(i int) (c1CellState, bool) {
		return c1WarmGet, true // warm, approaching TTL (synchronized)
	}, sizeFn)
	meter := newC1PickupMeter()
	c1RegisterRealisticRefresh(c, meter, c1SizeIndex(cells), 1.0)

	popEvict := c.Stats().EvictLRUTotal
	residentBytes := c.Stats().Bytes
	if popEvict != 0 {
		t.Fatalf("CONFIG-3 %s SIZING ERROR: evict_lru_total=%d after populate — warm-set exceeds the cap (residentBytes=%d > %d); would measure the capacity evictor.",
			label, popEvict, residentBytes, int64(c1CapBytes))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	keys := make([]string, len(cells))
	for i := range cells {
		keys[i] = cells[i].key
	}

	// baseline customer serve p95 (no burst). Customer Get-rate anchored to the
	// 057 10-min delta (arch): ~131/s customer Gets (raw 505/s minus the
	// refresher's 374/s counted no-op lookups), >90% hit. serveHold models the
	// SERVER-SIDE warm-handler span (parse+RBAC+c.Get+marshal+write, ~1-2ms; NOT
	// the 500ms end-to-end north-star) during which markCustomerInFlight keeps
	// the yield signal TRUE (restactions.go:66 / widgets.go:58 defer-at-entry).
	blLoad := startC1CustomerLoad(c, keys, c1CustRate, serveHold, neuterYield)
	time.Sleep(3 * time.Second)
	blP50, blP95, blN := blLoad.stopAndStats()

	base := refresherStatsSnapshot()
	beforeCache := c.Stats()
	gvr := c1WidgetGVR()
	for i := range cells {
		meter.markEnqueue(cells[i].key)
	}
	var enqueued uint64
	if proactiveOn {
		c.reapPastMaxEntryAge() // the #316 synchronized proactive burst
		enqueued = c.Stats().ProactiveRefreshTotal - beforeCache.ProactiveRefreshTotal
	} else {
		for i := range cells { // OFF: same re-Put volume via the event path (normal mutations)
			Deps().OnUpdate(gvr, c1NS, cells[i].name)
		}
		enqueued = refresherStatsSnapshot().enqueued - base.enqueued
	}

	wlLoad := startC1CustomerLoad(c, keys, c1CustRate, serveHold, neuterYield)
	r := c1DrainMeasure(base, enqueued, 12*time.Minute)
	wlP50, wlP95, wlN := wlLoad.stopAndStats()

	projectedBurstDrain := float64(enqueued) * c1AvgResolveCost().Seconds() / float64(c1Parallelism)
	measuredRate := float64(r.completed) / r.drainTime.Seconds()
	hit, miss := c1Navigate(c, cells)
	evictLRUDelta := c.Stats().EvictLRUTotal - beforeCache.EvictLRUTotal
	completedRatio := float64(r.completed) / float64(maxU64(r.enqueued, 1))
	droppedRatio := float64(r.dropped) / float64(maxU64(r.enqueued, 1))
	custDelta := float64(wlP95) / float64(maxDur(blP95, time.Nanosecond))

	t.Logf("CONFIG-3 WORST-BURST[%s proactive=%v] (warmset=%d residentBytes=%d): enqueued=%d completed=%d dropped=%d completed/enq=%.4f dropped/enq=%.4f evict_lru_delta=%d",
		label, proactiveOn, warmset, residentBytes, r.enqueued, r.completed, r.dropped, completedRatio, droppedRatio, evictLRUDelta)
	t.Logf("CONFIG-3 WORST-BURST[%s]: peakDepth=%d endDepth=%d drain=%s projectedDrain=%.1fs measuredRate=%.1f/s yielded=%d pickup_p95=%s",
		label, r.peakDepth, r.endDepth, r.drainTime, projectedBurstDrain, measuredRate, r.yielded, meter.p95())
	t.Logf("CONFIG-3 WORST-BURST[%s]: customer serve p95 baseline=%s (n=%d) with-burst=%s (n=%d) [ratio=%.3f× DIAGNOSTIC ONLY] p50 base=%s burst=%s ; cold-navs=%d (hit=%d)",
		label, blP95, blN, wlP95, wlN, custDelta, blP50, wlP50, miss, hit)

	return c1BurstResult{
		enqueued: enqueued, completed: r.completed, dropped: r.dropped, yielded: r.yielded, capped: r.capped,
		endDepth: r.endDepth, peakDepth: r.peakDepth, drainTime: r.drainTime,
		projectedDrain: projectedBurstDrain, custBaseP95: blP95, custP95: wlP95, custP50: wlP50,
		custN: wlN, coldNavs: miss, evictLRUDelta: evictLRUDelta,
		completedRatio: completedRatio, droppedRatio: droppedRatio,
	}
}

// c1AssertBurstPasses applies pm's FINALIZED gate to a proactive-ON burst. The
// customer-serve RATIO is DIAGNOSTIC only (demoted — µs-scale, doesn't predict
// customer wall-clock). Gate: pass ran, kept up + drained, <=2× projection,
// lossless, no capacity-evict, cold-navs ~0, AND the customer-priority signals —
// yield fired (yielded>0) + completion (custN>0) + absolute serve p95 sub-ms.
func c1AssertBurstPasses(t *testing.T, label string, warmset int, r c1BurstResult) {
	if r.enqueued == 0 {
		t.Fatalf("CONFIG-3 %s INVALID: burst enqueued 0 — the pass did not run (toggle off?)", label)
	}
	if r.completedRatio < 0.98 {
		t.Fatalf("CONFIG-3 %s HALT: completed/enqueued=%.4f < 0.98 (refresher not keeping up with the burst)", label, r.completedRatio)
	}
	if r.droppedRatio >= 0.01 {
		t.Fatalf("CONFIG-3 %s HALT: dropped/enqueued=%.4f >= 0.01 (burst saturated the refresher)", label, r.droppedRatio)
	}
	if r.endDepth != 0 {
		t.Fatalf("CONFIG-3 %s HALT: queueDepth did not drain (end=%d) — burst amplification", label, r.endDepth)
	}
	// AMPLIFICATION bound (WORK-based, yield-independent): the burst must not
	// cause the refresher to do >2× the projected re-resolves (the distinct warm
	// set = one re-resolve per approaching cell; a storm = re-work beyond that).
	// Wall-clock drain is EXTENDED by the customer-protective Ship #98 yield
	// (bounded by the 5s yield cap) and is a DIAGNOSTIC, not an amplification
	// signal — a long drain under heavy customer load is prioritization, not a
	// storm. (arch/pm: >2×-projection reframed wall-clock→work; see report.)
	if r.completed > uint64(2*warmset) {
		t.Fatalf("CONFIG-3 %s HALT: completed=%d > 2× warm-set (%d) — refresh-amplification storm (re-work beyond the distinct burst)", label, r.completed, 2*warmset)
	}
	// FRESHNESS WINDOW (pm/arch reporting requirement): the wall-clock drain is
	// the aggregate freshness window, EXTENDED by the bounded customer-protective
	// yield — NOT amplification (work is 1:1: completed=enqueued). Per-cell
	// worst-case deferral is bounded by the 5s yield cap + ≤3s resolve = the
	// AC-98.12 10s SLA. cappedTotal counts 5s-cap hits: 0 = healthy (yield fires
	// but never maxes out); a GROWING capped = refresh deferred to the max
	// (a freshness/leak finding to flag, not a silent pass).
	t.Logf("CONFIG-3 %s FRESHNESS: burst drain=%.1fs (yield-extended; no-yield projection=%.1fs), yielded=%d, cappedTotal=%d (0=healthy, yield never maxed). Per-cell deferral bounded by 5s cap + ≤3s resolve, within the AC-98.12 10s SLA. Work 1:1 (completed=%d=enqueued=%d) → no amplification.",
		label, r.drainTime.Seconds(), r.projectedDrain, r.yielded, r.capped, r.completed, r.enqueued)
	if r.capped > 0 {
		t.Logf("CONFIG-3 %s FRESHNESS FINDING: cappedTotal=%d > 0 — some refreshes hit the 5s yield cap (deferred to max). Expected ~0 at the 1.5ms inflight anchor; flag a GROWING capped across cycles as a freshness finding, not a silent pass.", label, r.capped)
	}
	if r.evictLRUDelta != 0 {
		t.Fatalf("CONFIG-3 %s HALT: evict_lru fired (delta=%d) — capacity eviction confounded the burst measurement", label, r.evictLRUDelta)
	}
	if r.yielded == 0 { // pm PRIMARY customer-priority: yield fires (drain asserted above)
		t.Fatalf("CONFIG-3 %s HALT: yielded=0 during the burst — customer-priority yield not exercised; customer-over-refresher unproven", label)
	}
	if r.custN == 0 { // pm COMPLETION
		t.Fatalf("CONFIG-3 %s INVALID: no customer serves recorded during the burst", label)
	}
	if r.custP95 >= time.Millisecond { // pm ABSOLUTE ceiling (ratio demoted)
		t.Fatalf("CONFIG-3 %s HALT: customer serve p95=%s >= 1ms — customer-visible serve starvation from the burst", label, r.custP95)
	}
	if r.coldNavs > warmset/100 {
		t.Fatalf("CONFIG-3 %s HALT: cold-navs=%d > 1%% of working set (%d) after the pass", label, r.coldNavs, warmset/100)
	}
	t.Logf("CONFIG-3 WORST-BURST[%s]: PASS — kept up (completed/enq=%.4f, drained, <=2× projection), lossless (dropped=0), no capacity-evict, yield FIRED (yielded=%d) + customers served sub-ms (p95=%s, n=%d), cold-navs=%d. Customer-over-refresher holds.",
		label, r.completedRatio, r.yielded, r.custP95, r.custN, r.coldNavs)
}

func TestC1Gate_Config3_WorstCaseBurst(t *testing.T) {
	warmset := c1WarmSet(t)
	r := c1RunWorstCaseBurst(t, warmset, c1UniformSize75, "uniform-75.6KiB", true, c1HandlerSpan, false)
	c1AssertBurstPasses(t, "uniform-75.6KiB", warmset, r)
}

// SKEW variant — same 75.6 KiB mean, 1 MiB p99 tail. pm: report ABSOLUTE customer
// serve p95 for BOTH arms (OFF via event re-Puts, ON via #316 proactive) and
// attribute large-body Put-lock contention. ON<1ms → clean; ON>=1ms while OFF<1ms
// → #316-caused (blocker); OFF also >=1ms → pre-existing Put-lock granularity.
func TestC1Gate_Config3_WorstCaseBurst_Skew(t *testing.T) {
	warmset := c1WarmSet(t)
	off := c1RunWorstCaseBurst(t, warmset, c1SkewSize75, "skew-OFF(event)", false, c1HandlerSpan, false)
	on := c1RunWorstCaseBurst(t, warmset, c1SkewSize75, "skew-ON(proactive)", true, c1HandlerSpan, false)

	t.Logf("CONFIG-3 SKEW A/B ABSOLUTE customer serve p95 (1MiB-p99 bodies): OFF(event re-Puts)=%s (n=%d) vs ON(#316 proactive)=%s (n=%d) ; OFF yielded=%d ON yielded=%d",
		off.custP95, off.custN, on.custP95, on.custN, off.yielded, on.yielded)

	c1AssertBurstPasses(t, "skew-ON(proactive)", warmset, on)

	if on.custP95 >= time.Millisecond && off.custP95 < time.Millisecond {
		t.Fatalf("CONFIG-3 SKEW FINDING: ON serve p95=%s >= 1ms while OFF=%s < 1ms — #316's proactive large-body re-Puts introduced customer-visible contention (a #316 land-blocker pending a bound)", on.custP95, off.custP95)
	}
	t.Logf("CONFIG-3 SKEW: PASS — ON large-body (1MiB p99) proactive re-Puts keep customer serve p95 sub-ms (%s); #316 adds no customer-visible Put-lock contention vs the OFF event baseline (%s).", on.custP95, off.custP95)
}

// WINDOW-INSENSITIVITY sweep (pm condition 1): the gate verdict must NOT depend
// on the modelled handler-span window. The operative variable is the DUTY CYCLE
// = rate × window (Little's law: mean concurrent in-flight L = λ·W). At the
// 131/s anchor: 1ms→13%, 2ms→26%, 5ms→66% duty — all in the realistic band
// (customers sometimes in flight), the refresher drains (extended by yield) and
// the verdict must hold. NOTE the boundary: window ≥ ~7.6ms → duty ≥ 100%
// (customers CONTINUOUSLY in flight) → the refresher enters the DESIGNED
// full-yield regime (Ship #98 customer-priority; background drain bounded only
// by the 5s yield cap), which is outside the 057 ~20% operating point and is a
// customer-priority feature, NOT a #316 amplification defect — so it is
// documented, not run (its drain is ~hours by construction). Light (3K).
func TestC1Gate_Config3_WindowInsensitivity(t *testing.T) {
	const sweepWarm = 3000
	for _, w := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond} {
		lbl := fmt.Sprintf("window-%s", w)
		duty := float64(c1CustRate) * w.Seconds() * 100
		r := c1RunWorstCaseBurst(t, sweepWarm, c1UniformSize75, lbl, true, w, false)
		c1AssertBurstPasses(t, lbl, sweepWarm, r)
		t.Logf("CONFIG-3 WINDOW-INSENSITIVITY[%s duty≈%.0f%%]: yielded=%d cust_p95=%s drain=%s completed=%d — PASS", w, duty, r.yielded, r.custP95, r.drainTime, r.completed)
	}
	t.Logf("CONFIG-3 WINDOW-INSENSITIVITY: PASS — the gate verdict (yield fires + drains + completion + sub-ms serve) is INVARIANT across handler-span windows 1/2/5ms (13-66%% duty, spanning the 057 ~20%% anchor); customer serve p95 stays sub-ms at every window (the window is modeled outside the measured Get). The mechanism, not the window, carries the result. Windows ≥~7.6ms (duty≥100%%) are the designed full-yield regime, not a #316 defect (see header).")
}

// YIELD-NEUTER positive control (pm condition 3, mechanism-response teeth). The
// harness re-resolve is a SLEEP (injected RTT+decode latency), NOT real decode
// CPU — so the yield's customer BENEFIT is architectural (freeing real apiserver
// connections + CPU in prod) and is NOT measurable in-process as a customer
// delta (pm+arch: do not manufacture one). Instead prove the "yielded>0" term is
// non-vacuous via the MECHANISM RESPONSE: realistic inflight → yielded>0 (the
// refresher PARKS during the /call window); neuter (hook always false) →
// yielded==0 (the refresher RUNS). The 0↔>0 flip under identical load is the
// reachable-failure arm: it HALTs if the yield does not react to the signal.
func TestC1Gate_Config3_YieldNeuter(t *testing.T) {
	const w = 5000
	realistic := c1RunWorstCaseBurst(t, w, c1UniformSize75, "neuter-OFF(realistic)", true, c1HandlerSpan, false)
	neuter := c1RunWorstCaseBurst(t, w, c1UniformSize75, "neuter-ON(hook=false)", true, c1HandlerSpan, true)

	t.Logf("CONFIG-3 YIELD-NEUTER: realistic yielded=%d (parks on inflight) vs NEUTER yielded=%d (hook=false, never parks) ; drained realistic=%v neuter=%v ; completed realistic=%d neuter=%d ; cust_p95 realistic=%s neuter=%s",
		realistic.yielded, neuter.yielded, realistic.endDepth == 0, neuter.endDepth == 0, realistic.completed, neuter.completed, realistic.custP95, neuter.custP95)

	if realistic.yielded == 0 {
		t.Fatalf("CONFIG-3 YIELD-NEUTER INVALID: realistic yielded=0 — the inflight modeling did not fire the yield; the term cannot be demonstrated")
	}
	if neuter.yielded != 0 {
		t.Fatalf("CONFIG-3 YIELD-NEUTER HALT: neuter yielded=%d != 0 — the yield fired despite the neutered inflight hook; 'yielded>0' is not gated by the inflight signal (stuck-positive, hollow term)", neuter.yielded)
	}
	// both must still complete the work (the refresher is correct either way).
	if realistic.endDepth != 0 || neuter.endDepth != 0 {
		t.Fatalf("CONFIG-3 YIELD-NEUTER HALT: burst did not drain (realistic end=%d, neuter end=%d)", realistic.endDepth, neuter.endDepth)
	}
	t.Logf("CONFIG-3 YIELD-NEUTER: PASS — yielded flips %d→0 when the inflight signal is neutered under identical load; the customer-priority yield term is non-vacuous and reacts to the real handler-span signal (sleep-harness mechanism-response teeth). Customer BENEFIT delta is architectural (real apiserver/CPU), not in-process measurable — correctly not asserted.",
		realistic.yielded)
}

// ===========================================================================
// CONFIG 3 — COLD-NAV A/B (realistic steady-state, the missed-dirty-mark hole).
// Same-build A/B via SetProactiveRefreshEnabledForTest: OFF (baseline, hole
// live) vs ON (pass catches the missed cells). BOTH arms run reapPastMaxEntryAge
// (so #315 cold-evict + #248 are identical); only #316 differs. Cold-navs are
// ACTUAL c.Get misses via the option-(c) fast-forward.
// ===========================================================================

type c1ColdNavArm struct {
	coldNavs  int
	warmCount int
	proactive uint64
	eventEnq  int
}

func c1RunColdNavArm(t *testing.T, warmset, warmFrac int, proactiveOn bool) c1ColdNavArm {
	cleanup := withCleanRefresher(t, c1Parallelism, 0)
	defer cleanup()
	c1SetEnv(t, "0", c1CapBytes)
	resetResolvedCacheForTest()
	defer resetResolvedCacheForTest()
	resetRefresherForTest()
	SetProactiveRefreshEnabledForTest(proactiveOn)
	defer SetProactiveRefreshEnabledForTest(true)

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	ttl := c1TTLSeconds * time.Second
	maxAge := c1MaxAgeSeconds * time.Second
	warmN := warmset * warmFrac / 100
	// warm working set [0,warmN) approaching-TTL; remainder cold (never navigated).
	total := warmset
	cells := c1Populate(t, c, total, ttl, maxAge, func(i int) (c1CellState, bool) {
		if i < warmN {
			return c1WarmGet, true
		}
		return c1Cold, false
	}, c1UniformSize75)
	warm := cells[:warmN]
	c1RegisterRealisticRefresh(c, nil, c1SizeIndex(cells), 1.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	base := refresherStatsSnapshot()
	b2 := c.Stats()
	gvr := c1WidgetGVR()
	const missedPct = 20
	eventEnq := 0
	for i := 0; i < warmN; i++ {
		if i%100 < missedPct {
			continue // MISSED dirty-mark (the #316 boundary)
		}
		Deps().OnUpdate(gvr, c1NS, warm[i].name)
		eventEnq++
	}
	c1DrainMeasure(base, uint64(eventEnq), 5*time.Minute)

	// BOTH arms run the read-independent pass; the toggle gates only #316.
	c.reapPastMaxEntryAge()
	proactive := c.Stats().ProactiveRefreshTotal - b2.ProactiveRefreshTotal
	dl := time.Now().Add(120 * time.Second)
	for time.Now().Before(dl) {
		if refresherStatsSnapshot().queueDepth == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	coldNavs := c1FastForwardAndCountColdNavs(c, warm)
	return c1ColdNavArm{coldNavs: coldNavs, warmCount: warmN, proactive: proactive, eventEnq: eventEnq}
}

// c1FastForwardAndCountColdNavs ages every un-refreshed (still-approaching:
// TTLRemaining < TTL/4) warm cell past TTL — the option-(c) fast-forward — then
// navigates and counts c.Get MISSES (a refreshed cell, TTLRemaining ≈ TTL, is
// left fresh → hits; an un-refreshed cell is aged → misses = a working-set cold-nav).
func c1FastForwardAndCountColdNavs(c *ResolvedCacheStore, warm []c1Cell) int {
	pastTTL := time.Now().Add(-time.Duration(c1TTLSeconds+10) * time.Second)
	for i := range warm {
		m, ok := c.MetadataForKey(warm[i].key)
		if !ok {
			continue // already gone → miss on navigation
		}
		if m.TTLRemainingSeconds < c1TTLSeconds/4 {
			in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: c1NS, Name: warm[i].name}
			c.Put(warm[i].key, &ResolvedEntry{RawJSON: c1Body(warm[i].bytes), Inputs: &in, CreatedAt: pastTTL})
		}
	}
	_, miss := c1Navigate(c, warm)
	return miss
}

func TestC1Gate_Config3_ColdNavABMissedDirtyMark(t *testing.T) {
	warmset := c1WarmSet(t)
	const warmFrac = 40 // realistic: 40% warm working set, 60% cold

	baseline := c1RunColdNavArm(t, warmset, warmFrac, false) // pass OFF
	withPass := c1RunColdNavArm(t, warmset, warmFrac, true)  // pass ON

	baselineRate := float64(baseline.coldNavs) / float64(maxInt(baseline.warmCount, 1))
	withPassRate := float64(withPass.coldNavs) / float64(maxInt(withPass.warmCount, 1))
	t.Logf("CONFIG-3 COLD-NAV A/B (warmset=%d warm=%d%%): OFF(baseline) cold-navs=%d/%d (%.4f) eventEnq=%d proactive=%d ; ON(with-pass) cold-navs=%d/%d (%.4f) proactive=%d",
		warmset, warmFrac, baseline.coldNavs, baseline.warmCount, baselineRate, baseline.eventEnq, baseline.proactive,
		withPass.coldNavs, withPass.warmCount, withPassRate, withPass.proactive)

	if baseline.coldNavs == 0 {
		t.Fatalf("CONFIG-3 A/B INVALID: baseline(OFF) cold-navs=0 — the scenario created no missed-dirty-mark hole to close")
	}
	if withPass.proactive == 0 {
		t.Fatalf("CONFIG-3 A/B INVALID: with-pass(ON) proactive_refresh_total=0 — the pass did not run")
	}
	if baseline.proactive != 0 {
		t.Fatalf("CONFIG-3 A/B INVALID: baseline(OFF) proactive_refresh_total=%d — toggle did not disable the pass", baseline.proactive)
	}
	maxWithPass := baseline.coldNavs * 5 / 100
	absCap := withPass.warmCount / 100
	if withPass.coldNavs > maxWithPass {
		t.Fatalf("CONFIG-3 HALT: with-pass cold-navs=%d > 0.05×baseline=%d — the pass did not close the hole to near-zero", withPass.coldNavs, maxWithPass)
	}
	if withPass.coldNavs > absCap {
		t.Fatalf("CONFIG-3 HALT: with-pass cold-navs=%d > 1%% of working set (%d)", withPass.coldNavs, absCap)
	}
	t.Logf("CONFIG-3 COLD-NAV A/B: PASS — hole existed (OFF baseline=%d cold-navs) and CLOSED to %d ON (<=0.05×baseline=%d AND <=1%%WS=%d). Zero-cold-nav honored.",
		baseline.coldNavs, withPass.coldNavs, maxWithPass, absCap)
}

// ===========================================================================
// CONFIG 3 — SATURATING FALSIFIER. Oversize the warm-set PAST the cap so
// capacity eviction FIRES alongside the reaper. Confirms the reaper+#316 pass
// does NOT storm when the evictor is active (refresh re-puts are byte-neutral
// replace-in-place, disjoint from the LRU-tail evictions). Expected clean:
// refresher keeps up, no dropped, cold-navs bounded, evict_lru_total > 0.
// ===========================================================================

func TestC1Gate_Config3_SaturatingFalsifier(t *testing.T) {
	// Oversize: warm-set × 75.6KiB > 2 GiB so the cap binds (capacity eviction fires).
	warmset := c1EnvInt("C1_SAT_WARMSET", 32000) // 32000 × 75.6KiB ≈ 2.36 GiB > 2 GiB
	cleanup := withCleanRefresher(t, c1Parallelism, 0)
	defer cleanup()
	c1SetEnv(t, "0", c1CapBytes)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()
	SetProactiveRefreshEnabledForTest(true)
	t.Cleanup(func() { SetProactiveRefreshEnabledForTest(true) })

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}
	Deps().SetStore(c)

	ttl := c1TTLSeconds * time.Second
	maxAge := c1MaxAgeSeconds * time.Second
	cells := c1Populate(t, c, warmset, ttl, maxAge, func(i int) (c1CellState, bool) {
		return c1WarmGet, true
	}, c1UniformSize75)
	meter := newC1PickupMeter()
	c1RegisterRealisticRefresh(c, meter, c1SizeIndex(cells), 1.0)

	popEvict := c.Stats().EvictLRUTotal
	if popEvict == 0 {
		t.Fatalf("CONFIG-3 SATURATING INVALID: evict_lru_total=0 after populate — the warm-set (%d) did NOT exceed the cap; raise C1_SAT_WARMSET so capacity eviction fires (this falsifier requires the evictor active)", warmset)
	}
	residentBytes := c.Stats().Bytes

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	// only the still-RESIDENT cells can be refreshed; capacity eviction removed the LRU tail.
	base := refresherStatsSnapshot()
	beforeCache := c.Stats()
	for i := range cells {
		meter.markEnqueue(cells[i].key)
	}
	reaped := c.reapPastMaxEntryAge()
	proactive := c.Stats().ProactiveRefreshTotal - beforeCache.ProactiveRefreshTotal

	r := c1DrainMeasure(base, proactive, 12*time.Minute)
	evictLRUDelta := c.Stats().EvictLRUTotal - beforeCache.EvictLRUTotal
	completedRatio := float64(r.completed) / float64(maxU64(r.enqueued, 1))
	droppedRatio := float64(r.dropped) / float64(maxU64(r.enqueued, 1))

	t.Logf("CONFIG-3 SATURATING (warmset=%d residentBytes=%d cap=%d, evict_lru pop=%d burst-delta=%d): reaped=%d proactive=%d enqueued=%d completed=%d dropped=%d completed/enq=%.4f dropped/enq=%.4f peakDepth=%d endDepth=%d drain=%s",
		warmset, residentBytes, int64(c1CapBytes), popEvict, evictLRUDelta, reaped, proactive, r.enqueued, r.completed, r.dropped, completedRatio, droppedRatio, r.peakDepth, r.endDepth, r.drainTime)

	// FALSIFIER: with the evictor active, the reaper+refresh must NOT storm.
	if r.endDepth != 0 {
		t.Fatalf("CONFIG-3 SATURATING HALT: queueDepth did not drain (end=%d) — the reaper storms when capacity eviction is active", r.endDepth)
	}
	if droppedRatio >= 0.01 {
		t.Fatalf("CONFIG-3 SATURATING HALT: dropped/enqueued=%.4f >= 0.01 — refresher storm under capacity eviction", droppedRatio)
	}
	if completedRatio < 0.98 {
		t.Fatalf("CONFIG-3 SATURATING HALT: completed/enqueued=%.4f < 0.98 under capacity eviction", completedRatio)
	}
	t.Logf("CONFIG-3 SATURATING: PASS (capacity eviction fired: evict_lru pop=%d delta=%d; reaper+refresh did NOT storm: completed/enq=%.4f, dropped/enq=%.4f, drained). Falsifier clean.",
		popEvict, evictLRUDelta, completedRatio, droppedRatio)
}
