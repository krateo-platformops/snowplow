//go:build c1amplification

// issue316_c1_amplification_test.go — the C1 refresh-amplification GATE
// harness for #315/#316 (proactive-refresh). VERDICT: GREEN (all 8 arms) on
// the landed fix; recorded gate evidence for PR #357.
//
// SUBSTRATE (arch-1217 ruling, joint build): in-process Go, extending the
// fix's own issue315_316_lazy_eviction_test.go approach — a real
// ResolvedCacheStore populated to the per-pod warm-set ceiling (~27K resident
// at the 057-anchored 75.6 KiB mean under the 2 GiB cap; "50K" is the
// deployment composition count — the RESIDENT warm-set is cap-bound at ~27K),
// a LIVE refresher goroutine (StartRefresher + RegisterRefreshFunc), the
// read-independent pass (reapPastMaxEntryAge) driven on-demand, real Gets to
// warm the working set and to measure cold-navs, and the customer-inflight
// hook to exercise the Ship #98 cooperative yield. "Hermetic OFF 057" = this:
// no cluster, no external deps.
//
// This file holds the substrate (population, the realistic re-resolve latency
// model, metric-snapshot capture, working-set navigation, pickup-latency
// instrumentation) plus a reduced-scale smoke test. The 8 GATE CONFIGS live in
// issue316_c1_configs_test.go. Anchors are FINAL (arch-confirmed from the 057
// read) — see the constants below.
//
// Build-tagged (c1amplification) so `go test ./internal/cache/` does NOT run
// this heavy harness in normal CI. Run explicitly:
//
//	C1_WARMSET=27000 go test -tags c1amplification -run TestC1 -timeout 30m ./internal/cache/
//
// COLD-NAV MEASUREMENT (arch-confirmed): measured as a cache-layer
// c.Get MISS on a navigated warm-set cell. This is the mechanism-true signal
// — the #315/#316 hole IS a cache eviction of a warm cell — and it is 1:1
// with the named dispatch_l1_lookups MISS: the dispatcher emits hit=true
// inside `if entry,ok := cacheHandle.Get(cacheKey); ok` and hit=false in the
// else (restactions.go:198-200/254, widgets.go:238/257), so a dispatch miss
// on a navigated warm cell ⇔ this c.Get miss (normal identity; the only
// divergence is serveFromCacheEligible's empty-identity gate, which a normal
// navigation does not hit).

package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ---------------------------------------------------------------------------
// Calibration constants — FINAL (arch-confirmed from the 057 read): GET-by-name
// RTT 5ms + ~10ms/MB decode; entry-size mean 75.6 KiB (c1MeanBytes, configs
// file). The refresh workload is GET-by-name (single object), so re-resolve
// latency is RTT-dominated. No longer pending.
// ---------------------------------------------------------------------------
const (
	// Decode irreducibility (feedback_cluster_list_decode_irreducibility): a
	// re-resolve pays ~10ms per MB of body decoded — a re-resolve is never
	// free, which is what keeps the in-process refresher-throughput honest.
	c1DecodeMsPerMB = 10.0

	// GET-by-name apiserver round-trip (arch-confirmed: single-digit-ms
	// intra-cluster GET-by-name; the workload is RTT-dominated).
	c1GetByNameRTT = 5 * time.Millisecond
)

// c1RealisticResolveCost is the realistic per-entry re-resolve latency the
// RegisterRefreshFunc sleeps for. pm-1217's HARD admissibility precondition:
// if this is zero the refresher trivially keeps up and any "keeps up" verdict
// is INVALID (faked-fast), not green.
func c1RealisticResolveCost(bodyBytes int) time.Duration {
	decode := time.Duration(float64(bodyBytes) / (1 << 20) * c1DecodeMsPerMB * float64(time.Millisecond))
	return c1GetByNameRTT + decode
}

// c1SampleBodySize is a mixed body-size distribution (median 8KB / p90 64KB /
// p99 256KB / max 1MB) used ONLY by the reduced-scale smoke test. The GATE
// configs use c1UniformSize75 (the 057-anchored 75.6 KiB mean) and
// c1SkewSize75 (same mean, 1 MiB p99 tail) instead — see the configs file.
func c1SampleBodySize(i int) int {
	switch r := i % 1000; {
	case r < 900: // 90% — body-median class
		return 8 * 1024
	case r < 990: // next 9% — p90 class
		return 64 * 1024
	case r < 999: // next 0.9% — p99 class
		return 256 * 1024
	default: // top 0.1% — p99.9/max large-LIST tail
		return 1024 * 1024
	}
}

// c1MeanBytes is the 057-anchored mean resolved-entry size (TL relay: the
// snowplow_resolved_cache resident_bytes/resident_entries read = 75.6 KiB).
// The 2 GiB cache byte cap (defaultResolvedCacheMaxBytes) therefore binds at
// ~27.7K resident entries — the per-pod warm-set ceiling at this size.
const c1MeanBytes = 77414 // 75.6 * 1024

// c1UniformSize75 returns the 057-mean size for every cell (the PRIMARY-gate
// sizing: warm-set × mean sits right under the 2 GiB cap, so the run measures
// reaper+#316 amplification with no capacity-eviction confound).
func c1UniformSize75(i int) int { return c1MeanBytes }

// c1SkewSize75 keeps the SAME 75.6 KiB mean but skews the mix to a 1 MiB p99
// tail (the platform's ~11×-median skew; large-LIST cells), byte-neutral to
// the cap point (0.99×66KiB + 0.01×1MiB ≈ 75.6KiB). Stresses per-refresh
// decode/throughput without changing the resident byte budget.
func c1SkewSize75(i int) int {
	if i%100 < 99 {
		return 66 * 1024 // 66 KiB body class
	}
	return 1024 * 1024 // 1 MiB tail
}

// c1Body builds a JSON-ish body of ~n bytes (valid enough for RawJSON; the
// cache stores opaque bytes, it does not parse).
func c1Body(n int) []byte {
	if n < 16 {
		n = 16
	}
	b := make([]byte, n)
	const head = `{"status":{"filler":"`
	const tail = `"}}`
	copy(b, head)
	for i := len(head); i < n-len(tail); i++ {
		b[i] = 'x'
	}
	copy(b[n-len(tail):], tail)
	return b
}

// c1CellState is a cell's warmth/lifetime class at population time.
type c1CellState int

const (
	c1Cold          c1CellState = iota // never navigated, not seeded → COLD
	c1WarmGet                          // navigated (lastRead stamped) → WARM
	c1WarmSeed                         // SeededAtBoot=true → WARM via seed
	c1WarmPastMaxAge                   // WARM + BornAt past maxEntryAge (C3 kept)
)

// c1Cell records a populated cell so a config can navigate / age / assert it.
type c1Cell struct {
	key   string
	name  string // the CR name ("w-<i>") — for Deps().OnUpdate + re-Put aging.
	bytes int
	state c1CellState
}

// c1WidgetGVR / c1NS are the fixed coordinates every populated cell shares
// (mirrors coherenceProbeGVR + "team-a" used in c1Populate) so a config can
// drive Deps().OnUpdate(gvr, ns, name) on the recorded dep edge.
func c1WidgetGVR() schema.GroupVersionResource { return coherenceProbeGVR() }

const c1NS = "team-a"

// c1Populate builds n cells in the requested state mix against store c.
func c1Populate(t *testing.T, c *ResolvedCacheStore, n int, ttl, maxAge time.Duration, stateOf func(i int) (c1CellState, bool), sizeFn func(i int) int) []c1Cell {
	t.Helper()
	gvr := coherenceProbeGVR()
	const ns = "team-a"
	cells := make([]c1Cell, 0, n)
	now := time.Now()
	for i := 0; i < n; i++ {
		state, warmApproaching := stateOf(i)
		name := fmt.Sprintf("w-%d", i)
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: name}
		key := ComputeKey(in)
		nbytes := sizeFn(i)

		createdAt := now
		bornAt := now
		switch state {
		case c1WarmPastMaxAge:
			bornAt = now.Add(-(maxAge + time.Minute))
		case c1Cold:
			bornAt = now.Add(-(maxAge + time.Minute))
			createdAt = bornAt
		}
		if warmApproaching && (state == c1WarmGet || state == c1WarmSeed) {
			createdAt = now.Add(-(ttl - ttl/4 + time.Second))
		}

		e := &ResolvedEntry{RawJSON: c1Body(nbytes), Inputs: &in, CreatedAt: createdAt, BornAt: bornAt}
		if state == c1WarmSeed {
			e.SeededAtBoot = true
		}
		c.Put(key, e)
		Deps().Record(context.Background(), key, gvr, ns, name)
		if state == c1WarmGet || state == c1WarmPastMaxAge {
			c.Get(key)
		}
		cells = append(cells, c1Cell{key: key, name: name, bytes: nbytes, state: state})
	}
	return cells
}

type c1Snap struct {
	at    time.Time
	refr  refresherStats
	cache ResolvedCacheStats
}

func c1Capture(c *ResolvedCacheStore) c1Snap {
	return c1Snap{at: time.Now(), refr: refresherStatsSnapshot(), cache: c.Stats()}
}

func c1Navigate(c *ResolvedCacheStore, cells []c1Cell) (hit, miss int) {
	for i := range cells {
		if _, ok := c.Get(cells[i].key); ok {
			hit++
		} else {
			miss++
		}
	}
	return hit, miss
}

type c1PickupMeter struct {
	mu        sync.Mutex
	enqueued  map[string]time.Time
	latencies []time.Duration
}

func newC1PickupMeter() *c1PickupMeter { return &c1PickupMeter{enqueued: make(map[string]time.Time)} }

func (m *c1PickupMeter) markEnqueue(key string) {
	m.mu.Lock()
	if _, seen := m.enqueued[key]; !seen {
		m.enqueued[key] = time.Now()
	}
	m.mu.Unlock()
}

func (m *c1PickupMeter) observe(key string) {
	m.mu.Lock()
	if t0, ok := m.enqueued[key]; ok {
		m.latencies = append(m.latencies, time.Since(t0))
		delete(m.enqueued, key)
	}
	m.mu.Unlock()
}

func (m *c1PickupMeter) p95() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latencies) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(m.latencies))
	copy(cp, m.latencies)
	for i := 1; i < len(cp); i++ {
		for j := i; j > 0 && cp[j-1] > cp[j]; j-- {
			cp[j-1], cp[j] = cp[j], cp[j-1]
		}
	}
	idx := (len(cp) * 95) / 100
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func c1RegisterRealisticRefresh(c *ResolvedCacheStore, meter *c1PickupMeter, sizeOf func(key string) int, elevation float64) {
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		cost := c1RealisticResolveCost(sizeOf(k))
		if elevation > 1 {
			cost = time.Duration(float64(cost) * elevation)
		}
		time.Sleep(cost)
		// FAITHFULNESS (arch review): this uses a plain c.Put, NOT the real #189
		// ReplaceIfGen. Deliberate + faithful here — the controlled burst has no
		// raced DELETE, so plain Put ≡ ReplaceIfGen outcome; the harness models
		// refresh LATENCY/WORK (the amplification question), not the gen-guard.
		c.Put(k, &ResolvedEntry{RawJSON: c1Body(sizeOf(k)), Inputs: &used})
		if meter != nil {
			meter.observe(k)
		}
		return nil
	})
}

func c1SizeIndex(cells []c1Cell) func(key string) int {
	idx := make(map[string]int, len(cells))
	for i := range cells {
		idx[cells[i].key] = cells[i].bytes
	}
	return func(key string) int {
		if b, ok := idx[key]; ok {
			return b
		}
		return 8 * 1024
	}
}

func TestC1Smoke_SubstratePlumbing(t *testing.T) {
	const n = 2000
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv(envResolvedCacheMaxEntries, "60000")
	t.Setenv(envResolvedCacheMaxBytes, "0")
	t.Setenv(envResolvedCacheTTLSeconds, "20")
	t.Setenv(envResolvedCacheMaxEntryAgeSeconds, "86400")
	t.Setenv(envResolvedCacheSummaryEvery, "36000")
	t.Setenv(envRefresherRateFloorSeconds, "0")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	ttl := 20 * time.Second
	maxAge := 86400 * time.Second

	cells := c1Populate(t, c, n, ttl, maxAge, func(i int) (c1CellState, bool) {
		switch r := i % 10; {
		case r < 7:
			return c1WarmGet, true
		case r < 9:
			return c1Cold, false
		default:
			return c1WarmGet, false
		}
	}, c1SampleBodySize)

	meter := newC1PickupMeter()
	sizeOf := c1SizeIndex(cells)
	c1RegisterRealisticRefresh(c, meter, sizeOf, 1.0)

	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); resetRefresherForTest() }() // harness teardown-race fix (#366): stop+wait refresher (resetRefresherForTest waits the worker WaitGroup) before the cache reset — processNext calls ResolvedCache() sync.Once
	StartRefresher(ctx)

	before := c1Capture(c)

	for i := range cells {
		if cells[i].state == c1WarmGet {
			meter.markEnqueue(cells[i].key)
		}
	}
	reaped := c.reapPastMaxEntryAge()

	after := c1Capture(c)

	proactiveDelta := after.cache.ProactiveRefreshTotal - before.cache.ProactiveRefreshTotal
	maxAgeDelta := after.cache.EvictMaxAgeTotal - before.cache.EvictMaxAgeTotal
	t.Logf("SMOKE: reaped=%d proactive_refresh delta=%d evict_max_age delta=%d warm_past_max_age=%d refr.enqueued=%d queueDepth=%d",
		reaped, proactiveDelta, maxAgeDelta, after.cache.WarmPastMaxAge, after.refr.enqueued, after.refr.queueDepth)

	if proactiveDelta == 0 {
		t.Fatalf("SMOKE: proactive pass enqueued nothing — substrate not wiring warm-approaching → EnqueueRefresh")
	}
	if maxAgeDelta == 0 {
		t.Fatalf("SMOKE: no cold cells reaped on evict_max_age — cold-past-cap population not taking")
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		s := refresherStatsSnapshot()
		if s.completed >= uint64(proactiveDelta) && s.queueDepth == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	drained := refresherStatsSnapshot()
	t.Logf("SMOKE: after drain — refr.enqueued=%d completed=%d dropped=%d queueDepth=%d pickup_p95=%s",
		drained.enqueued, drained.completed, drained.dropped, drained.queueDepth, meter.p95())

	warm := make([]c1Cell, 0, n)
	for i := range cells {
		if cells[i].state == c1WarmGet {
			warm = append(warm, cells[i])
		}
	}
	hit, miss := c1Navigate(c, warm)
	t.Logf("SMOKE: working-set navigation hit=%d miss=%d (cold-navs=%d)", hit, miss, miss)

	if drained.completed == 0 {
		t.Fatalf("SMOKE: refresher completed 0 re-resolves — realistic-latency refresh path not running")
	}
}
