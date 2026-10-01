// refresher_live_refresh_coherence_test.go — Phase-0 falsifier + baseline for
// the snowplow per-key live-refresh-coherence feature (plan:
// ~/.claude/plans/snowplow-live-refresh-coherence-plan.md; design:
// docs/live-refresh-coherence-design-2026-06-18.md; artifact:
// docs/live-refresh-stalewindow-phase0-2026-06-18.md).
//
// Two concerns, both PRE-CODE gates (feedback_falsifier_first_before_ship):
//
//   A. STALE-WINDOW falsifier (the premise). After a backing resource
//      changes, snowplow /call serves a STALE widget result for a window —
//      between the informer dirty-marking the dependent L1 key (deps.go
//      OnUpdate -> enqueue) and the refresher re-resolving + committing it
//      (stale-while-revalidate). The proposal hedged "sub-ms–ms, usually
//      fresh"; these tests prove the window is dominated by the #318-R1a
//      rate-floor (default 2s; refresher.go:105) and the Ship #98 customer
//      yield (cap 5s; refresher.go:124), NOT the ms-scale resolve. This is
//      the deterministic 2.0s / up-to-~5s window option B's floor-bypass
//      must close.
//
//   B. FLOOR-ON AMPLIFICATION baseline (the denominator option B must NOT
//      regress). With the floor ON (today's behaviour), a CRUD-install churn
//      wave of N rapid OnUpdates on the same subscribed key collapses into
//      FEW re-resolves. We quantify the collapse (re-resolves run vs marks
//      fired, sourced from the real completedTotal/flooredTotal counters) and
//      the refresher cost (alloc bytes + wall) so the floor-bypass design can
//      be measured against it.
//
// Drives the REAL path end-to-end: ResolvedCache().Put (models the first
// /call commit) -> Deps().Record (real dep edge) -> Deps().OnUpdate (the real
// informer-event entry point watcher.go calls) -> real refresh hook -> real
// refresher queue + processNext (rate-floor + yield active) -> a registered
// RefreshFunc that re-resolves -> ResolvedCache().Get (the same read /call
// serves from). NO apiserver, NO mechanism faked.
//
// Pure in-memory (in-process ResolvedCache + fake handler) — satisfies
// feedback_no_go_test_against_remote_kubeconfig; run in internal/cache/ with
// KUBECONFIG unset, NEVER ./internal/rbac/...
//
// Real-clock windows per the suite idiom (no clock-injection seam in the
// refresher/workqueue — see refresher_rate_floor_test.go header).

package cache

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// coherenceProbeGVR is an arbitrary backing-object GVR for the dep edge —
// shaped like a composition claim (the live-refresh feature's headline case:
// a composition reconcile fans to its widget cards).
func coherenceProbeGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1alpha1", Resource: "democlaims"}
}

const (
	coherenceV1 = `{"status":{"widgetData":{"phase":"v1-OLD"}}}`
	coherenceV2 = `{"status":{"widgetData":{"phase":"v2-FRESH"}}}`
)

// servedSample is one observation of what the dispatcher read returns.
type servedSample struct {
	at    time.Time
	fresh bool
}

// runStaleWindowProbe runs ONE stale-window scenario and returns the measured
// stale window (reconcile -> first fresh served via the dispatcher read), the
// commit latency (reconcile -> handler Put), and observed/sample counts.
//
//   - floorSeconds: RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS ("" = prod
//     default 2s; "0" = kill-switch baseline).
//   - customerInflight: pin a sustained customer /call so the refresher
//     yields (models a busy 1000-user deployment; adds the 5s yield cap).
//   - resolveCost: models the in-process resolve+populate latency.
func runStaleWindowProbe(t *testing.T, floorSeconds string, customerInflight bool, resolveCost time.Duration) (staleWindow time.Duration, observedFresh bool, refetchSamples int, commitLatency time.Duration) {
	t.Helper()
	cleanup := withCleanRefresher(t, 4, 0) // 4 workers = production default parallelism
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, floorSeconds)
	resetRefresherForTest()

	c := ResolvedCache()
	Deps().SetStore(c)

	gvr := coherenceProbeGVR()
	const ns, objName = "team-a", "demo-1"
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: "widget-detail"}
	key := ComputeKey(in)

	// (1) Commit the first /call result (v1). Put stamps CreatedAt=now — the
	// realistic case: a live-refresh refetch hits an entry the first /call
	// JUST populated, i.e. YOUNGER than the 2s floor.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(coherenceV1), Inputs: &in})
	// (2) Real dep edge: widget L1 key depends on the backing object.
	Deps().Record(context.Background(), key, gvr, ns, objName)

	// Registered RefreshFunc re-resolves to v2 (models the resolver re-running
	// against the now-reconciled cluster state).
	var committedAt atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		if resolveCost > 0 {
			time.Sleep(resolveCost)
		}
		c.Put(k, &ResolvedEntry{RawJSON: []byte(coherenceV2), Inputs: &used})
		committedAt.Store(time.Now().UnixNano())
		return nil
	})

	if customerInflight {
		SetCustomerInflightHook(func() bool { return true })
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// Entry ages a beat (the refetch lands shortly after the first /call) but
	// stays well inside the floor. 50ms << 2s.
	time.Sleep(50 * time.Millisecond)

	// (3)+(4) The reconcile happens and the REAL informer-event entry point
	// fires the dirty-mark. reconcileAt is when true cluster state flipped to
	// v2 — the stale window is measured from here.
	reconcileAt := time.Now()
	marked := Deps().OnUpdate(gvr, ns, objName)
	if marked != 1 {
		t.Fatalf("OnUpdate dirty-marked %d keys, want 1 (dep edge not wired)", marked)
	}

	// (5) Frontend-refetch loop: read L1 via the dispatcher read path at
	// ~100Hz; record the first sample that sees fresh v2. Cap at 15s (above
	// the documented 10s worst-case SLA).
	var (
		samples     []servedSample
		firstFresh  time.Time
		sawFresh    bool
		observeStop = reconcileAt.Add(15 * time.Second)
	)
	for time.Now().Before(observeStop) {
		e, ok := c.Get(key)
		now := time.Now()
		isFresh := ok && e != nil && string(e.RawJSON) == coherenceV2
		samples = append(samples, servedSample{at: now, fresh: isFresh})
		if isFresh && !sawFresh {
			sawFresh = true
			firstFresh = now
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Release the customer pin so the deferred refresh proceeds and cleanup's
	// worker-drain does not hang on a 5s park.
	if customerInflight {
		SetCustomerInflightHook(func() bool { return false })
	}

	if sawFresh {
		staleWindow = firstFresh.Sub(reconcileAt)
	} else {
		staleWindow = 15 * time.Second // floor of the unobserved window
	}
	if ca := committedAt.Load(); ca != 0 {
		commitLatency = time.Unix(0, ca).Sub(reconcileAt)
	}
	return staleWindow, sawFresh, len(samples), commitLatency
}

// --- A. STALE-WINDOW falsifier ----------------------------------------------

// TestLiveRefreshCoherence_StaleWindow_ProductionDefault is THE headline
// falsifier: floor at the production default (2s, env unset), no customer
// load, a sub-ms-modelled resolve. A fresh L1 entry (just served by the first
// /call) is dirty-marked by a reconcile; the refetch reads stale v1 until the
// FLOORED re-resolve commits v2. The window is ~2s (the rate-floor), NOT the
// "sub-ms–ms" the proposal assumed.
func TestLiveRefreshCoherence_StaleWindow_ProductionDefault(t *testing.T) {
	win, observed, n, commit := runStaleWindowProbe(t, "", false, 5*time.Millisecond)
	t.Logf("STALE-WINDOW[prod-default floor=2s, no customer load]: stale_window=%s observed_fresh=%v refetch_samples=%d commit_latency_from_reconcile=%s",
		win.Round(time.Millisecond), observed, n, commit.Round(time.Millisecond))
	if !observed {
		t.Fatalf("never observed fresh within 15s — would indicate a hang, not a window")
	}
	// Guard the FINDING: the window must be floor-dominated (>= ~1.5s), well
	// above the ms-scale resolve. RED if a future change makes the floor stop
	// applying to a young dirty-marked entry (the premise would dissolve).
	if win < 1500*time.Millisecond {
		t.Fatalf("stale_window=%s < 1.5s — the 2s rate-floor no longer dominates the window; re-baseline the premise", win)
	}
}

// TestLiveRefreshCoherence_StaleWindow_FloorZeroBaseline is the proposal's
// ASSUMED world: floor=0 (kill-switch), no load, sub-ms resolve. Isolates the
// floor's contribution — the window collapses to ~ms here.
func TestLiveRefreshCoherence_StaleWindow_FloorZeroBaseline(t *testing.T) {
	win, observed, n, commit := runStaleWindowProbe(t, "0", false, 5*time.Millisecond)
	t.Logf("STALE-WINDOW[floor=0 baseline, no customer load]: stale_window=%s observed_fresh=%v refetch_samples=%d commit_latency_from_reconcile=%s",
		win.Round(time.Millisecond), observed, n, commit.Round(time.Millisecond))
	if !observed {
		t.Fatalf("never observed fresh within 15s on floor=0 — unexpected")
	}
	// On floor=0 the window is mechanism+resolve only — assert it is small,
	// confirming the floor (not the mechanism) owns the production window.
	if win > 500*time.Millisecond {
		t.Fatalf("floor=0 window=%s > 500ms — the mechanism itself is slow; re-investigate (premise attribution wrong)", win)
	}
}

// TestLiveRefreshCoherence_StaleWindow_FloorPlusCustomerYield is the
// documented WORST CASE: production floor (2s) + a sustained customer /call in
// flight (refresher yields up to the 5s cap). The window pushes toward the
// yield cap. Records; does not hard-fail on the cap (the refresher proceeds
// after 5s regardless).
func TestLiveRefreshCoherence_StaleWindow_FloorPlusCustomerYield(t *testing.T) {
	win, observed, n, commit := runStaleWindowProbe(t, "2", true, 5*time.Millisecond)
	t.Logf("STALE-WINDOW[floor=2s + sustained customer-inflight yield]: stale_window=%s observed_fresh=%v refetch_samples=%d commit_latency_from_reconcile=%s",
		win.Round(time.Millisecond), observed, n, commit.Round(time.Millisecond))
	if !observed {
		t.Logf("NOTE: did not observe fresh within 15s under sustained yield — window >= 15s")
		return
	}
	// Under sustained yield the window must exceed the floor-only case — the
	// yield cap stacks on top. Assert it cleared the 5s yield cap region.
	if win < 4500*time.Millisecond {
		t.Fatalf("yield window=%s < 4.5s — the 5s customer-yield cap did not stack on the floor as documented", win)
	}
}

// TestLiveRefreshCoherence_RefetchLandsInsideWindow quantifies HOW OFTEN a
// realistic frontend refetch lands inside the stale window. The frontend
// fires the refetch on the SSE event (arriving ~when the change is observed
// cluster-side, i.e. ~reconcile time). We fire refetches at fixed offsets
// 0..2500ms post-reconcile, each a concurrent dispatcher read, and report the
// stale fraction. Run under -race (concurrent readers vs the refresher).
func TestLiveRefreshCoherence_RefetchLandsInsideWindow(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "") // production default 2s
	resetRefresherForTest()

	c := ResolvedCache()
	Deps().SetStore(c)
	gvr := coherenceProbeGVR()
	const ns, objName = "team-a", "demo-1"
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: "widget-detail"}
	key := ComputeKey(in)

	c.Put(key, &ResolvedEntry{RawJSON: []byte(coherenceV1), Inputs: &in})
	Deps().Record(context.Background(), key, gvr, ns, objName)

	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		time.Sleep(5 * time.Millisecond)
		c.Put(k, &ResolvedEntry{RawJSON: []byte(coherenceV2), Inputs: &used})
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	time.Sleep(50 * time.Millisecond) // entry is young (just served)

	reconcileAt := time.Now()
	Deps().OnUpdate(gvr, ns, objName)

	var offsets []time.Duration
	for ms := 0; ms <= 2500; ms += 100 {
		offsets = append(offsets, time.Duration(ms)*time.Millisecond)
	}
	var stale, fresh atomic.Int32
	var wg sync.WaitGroup
	for _, off := range offsets {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			fire := reconcileAt.Add(d)
			for time.Now().Before(fire) {
				time.Sleep(2 * time.Millisecond)
			}
			e, ok := c.Get(key)
			if ok && e != nil && string(e.RawJSON) == coherenceV2 {
				fresh.Add(1)
			} else {
				stale.Add(1)
			}
		}(off)
	}
	wg.Wait()
	total := stale.Load() + fresh.Load()
	t.Logf("REFETCH-LANDING[floor=2s]: %d refetches across 0-2500ms post-reconcile -> STALE=%d FRESH=%d (%.0f%% landed in the stale window)",
		total, stale.Load(), fresh.Load(), 100*float64(stale.Load())/float64(total))
	// A majority of refetches in the first 2.5s must land stale — the premise.
	if stale.Load() <= fresh.Load() {
		t.Fatalf("stale=%d <= fresh=%d — refetches no longer predominantly land stale; the premise weakened", stale.Load(), fresh.Load())
	}
}

// TestLiveRefreshCoherence_RefresherMechanismOverhead confirms the refresher
// re-resolve is the sub-ms in-process work the proposal assumes (floor=0, zero
// modelled resolve cost) — so the proposed /refreshes signal closes only the
// residual queue+commit gap, and the production window is owned by the
// floor/yield gates, not the resolve.
func TestLiveRefreshCoherence_RefresherMechanismOverhead(t *testing.T) {
	win, observed, n, commit := runStaleWindowProbe(t, "0", false, 0)
	t.Logf("MECHANISM-OVERHEAD[floor=0, zero resolve cost]: stale_window=%s observed_fresh=%v refetch_samples=%d commit_latency_from_reconcile=%s",
		win.Round(time.Microsecond), observed, n, commit.Round(time.Microsecond))
	if !observed {
		t.Fatalf("never observed fresh — unexpected")
	}
	if commit > 50*time.Millisecond {
		t.Fatalf("mechanism commit latency=%s > 50ms — the dequeue->Put path is not sub-ms; re-investigate", commit)
	}
}

// --- B. FLOOR-ON AMPLIFICATION baseline -------------------------------------

// burstCollapseResult is the measured outcome of one floor-ON churn-wave.
type burstCollapseResult struct {
	floor          time.Duration
	marksFired     int    // OnUpdate dirty-marks issued by the churn wave
	resolvesRun    uint64 // completedTotal — actual re-resolves the refresher ran
	flooredCycles  uint64 // flooredTotal — floored re-cycles (>= collapse delta, per C1)
	enqueued       uint64 // enqueueTotal
	allocBytes     uint64 // refresher-attributed heap alloc delta over the wave
	wall           time.Duration
	convergeToV2   time.Duration // reconcile-wave-start -> final v2 committed
}

// runFloorOnBurstCollapse fires a CRUD-install churn wave: `marks` rapid
// OnUpdates on ONE subscribed key over `spread`, with the floor at
// `floorSeconds`. It measures how many re-resolves actually run (the collapse)
// and the refresher cost. The handler writes a monotonically-increasing
// version so we can confirm last-write-wins convergence.
func runFloorOnBurstCollapse(t *testing.T, floorSeconds string, marks int, spread time.Duration) burstCollapseResult {
	t.Helper()
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, floorSeconds)
	resetRefresherForTest()

	c := ResolvedCache()
	Deps().SetStore(c)
	gvr := coherenceProbeGVR()
	const ns, objName = "team-a", "claim-1"
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: "widget-card"}
	key := ComputeKey(in)

	// Seed an aged entry so the FIRST mark re-resolves immediately (sets the
	// "1 immediate + collapsed-rest" shape deterministically — mirrors
	// putAgedEntry's intent in refresher_rate_floor_test.go). agedBackoff
	// (30s) is past every test floor yet under the cache TTL.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(coherenceV1), Inputs: &in, CreatedAt: time.Now().Add(-agedBackoff)})
	Deps().Record(context.Background(), key, gvr, ns, objName)

	var resolves atomic.Int32
	var lastCommitAt atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		// Models a real ~50-100ms per-user widget re-resolve compacted to a
		// small but non-zero cost so the alloc/wall figures are meaningful
		// without making the test slow.
		time.Sleep(2 * time.Millisecond)
		c.Put(k, &ResolvedEntry{RawJSON: []byte(coherenceV2), Inputs: &used})
		resolves.Add(1)
		lastCommitAt.Store(time.Now().UnixNano())
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	r := refresherSingleton()
	complBefore := r.completedTotal.Load()
	flBefore := r.flooredTotal.Load()
	enqBefore := r.enqueueTotal.Load()

	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)
	waveStart := time.Now()

	// The churn wave: `marks` OnUpdates on the same key, spread over `spread`.
	// All but the first land while the entry is young -> floored (deferred),
	// collapsing into ~1 deferred re-resolve at expiry regardless of `marks`.
	gap := spread / time.Duration(marks)
	for i := 0; i < marks; i++ {
		Deps().OnUpdate(gvr, ns, objName)
		if gap > 0 {
			time.Sleep(gap)
		}
	}

	// Let the wave fully converge: wait past the floor for the final deferred
	// re-resolve to commit, then settle.
	floor := r.rateFloor()
	settleDeadline := time.Now().Add(floor + 3*time.Second)
	for time.Now().Before(settleDeadline) {
		// Converged when the last commit is older than the floor (no pending
		// deferred re-resolve) AND queue drained.
		if r.queue.Len() == 0 && r.clusterListQueue.Len() == 0 {
			lc := lastCommitAt.Load()
			if lc != 0 && time.Since(time.Unix(0, lc)) > floor+200*time.Millisecond {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	wall := time.Since(waveStart)
	runtime.ReadMemStats(&ms1)

	var converge time.Duration
	if lc := lastCommitAt.Load(); lc != 0 {
		converge = time.Unix(0, lc).Sub(waveStart)
	}

	// Confirm convergence correctness: the cell holds fresh v2.
	if e, ok := c.Get(key); !ok || string(e.RawJSON) != coherenceV2 {
		t.Fatalf("post-wave cell not converged to v2: ok=%v", ok)
	}

	return burstCollapseResult{
		floor:         floor,
		marksFired:    marks,
		resolvesRun:   r.completedTotal.Load() - complBefore,
		flooredCycles: r.flooredTotal.Load() - flBefore,
		enqueued:      r.enqueueTotal.Load() - enqBefore,
		allocBytes:    ms1.TotalAlloc - ms0.TotalAlloc,
		wall:          wall,
		convergeToV2:  converge,
	}
}

// TestLiveRefreshCoherence_FloorOnAmplificationBaseline is the DENOMINATOR
// option B's floor-bypass must NOT regress: with the floor ON (production
// default 2s), a 200-mark churn wave on one subscribed key over ~400ms
// collapses into a SMALL number of re-resolves (the floor's whole purpose —
// #318-R1a docstring: "under the install storm completed_total collapses while
// flooredTotal rises"). Captures re-resolves-run, floored-cycles, alloc, wall.
//
// This is the baseline the architect's 9.7 comparison plugs into. The
// floor-bypass design must keep re-resolves-run at this collapsed level for a
// CHURN wave (only the stale-window for a SUBSCRIBED key shrinks) — it must
// NOT turn N marks back into N re-resolves (that would reintroduce the
// pre-#318 amplification the floor exists to prevent).
func TestLiveRefreshCoherence_FloorOnAmplificationBaseline(t *testing.T) {
	const marks = 200
	const spread = 400 * time.Millisecond // whole wave inside the 2s floor
	res := runFloorOnBurstCollapse(t, "", marks, spread)

	collapseRatio := float64(res.marksFired) / float64(res.resolvesRun)
	t.Logf("FLOOR-ON-BASELINE[floor=%s]: %d marks over %s -> resolves_run=%d (collapse %.0f:1) floored_cycles=%d enqueued=%d alloc=%d KiB wall=%s converge_to_v2=%s",
		res.floor, res.marksFired, spread, res.resolvesRun, collapseRatio, res.flooredCycles, res.enqueued,
		res.allocBytes/1024, res.wall.Round(time.Millisecond), res.convergeToV2.Round(time.Millisecond))

	// The floor MUST collapse the wave: re-resolves far below marks fired.
	if res.resolvesRun >= uint64(marks)/2 {
		t.Fatalf("resolves_run=%d for %d marks — the floor did not collapse the wave (expected a small handful)", res.resolvesRun, marks)
	}
	if res.flooredCycles == 0 {
		t.Fatalf("floored_cycles=0 — the floor gate never fired under a 200-mark wave inside the 2s floor")
	}
	// Convergence is bounded by ~floor + resolve (last-write-wins at expiry).
	if res.convergeToV2 > res.floor+2*time.Second {
		t.Fatalf("converge_to_v2=%s exceeded floor+2s (%s) — wave did not converge within the documented bound", res.convergeToV2, res.floor+2*time.Second)
	}
}

// TestLiveRefreshCoherence_FloorOffAmplificationContrast is the RED contrast:
// floor=0 (the kill-switch) on the SAME wave. Each fully-distinct mark that
// the worker drains before the next re-resolves, so the collapse is far weaker
// and re-resolves climb toward marks. This is what the floor BUYS today, and
// what a naive always-bypass would cost — the explicit amplification the
// floor-bypass design must avoid for churn (non-subscribed) keys.
func TestLiveRefreshCoherence_FloorOffAmplificationContrast(t *testing.T) {
	const marks = 200
	const spread = 400 * time.Millisecond
	res := runFloorOnBurstCollapse(t, "0", marks, spread)
	t.Logf("FLOOR-OFF-CONTRAST[floor=0]: %d marks over %s -> resolves_run=%d floored_cycles=%d enqueued=%d alloc=%d KiB wall=%s converge_to_v2=%s",
		res.marksFired, spread, res.resolvesRun, res.flooredCycles, res.enqueued,
		res.allocBytes/1024, res.wall.Round(time.Millisecond), res.convergeToV2.Round(time.Millisecond))
	// floor=0 ⇒ the gate never defers.
	if res.flooredCycles != 0 {
		t.Fatalf("floor=0 floored_cycles=%d want 0 (kill-switch must short-circuit the gate)", res.flooredCycles)
	}
	// And it must run MORE re-resolves than the floor-ON baseline (the whole
	// point of the contrast) — workqueue dedup still coalesces in-flight marks,
	// so this is "materially more", not literally `marks`.
	if res.resolvesRun < 2 {
		t.Fatalf("floor=0 resolves_run=%d — expected materially more than the floor-ON collapsed handful", res.resolvesRun)
	}
}
