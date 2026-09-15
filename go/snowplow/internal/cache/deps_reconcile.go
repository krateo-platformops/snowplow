// deps_reconcile.go — 1.12.6 C3: the sampled reconcile audit
// (docs/design 1.12.6 event-pipeline hardening §5).
//
// WHY. C1 made every informer event state-derived and C2 bridged the relist
// window, but an event pipeline is still a pipeline: a handler that panicked
// before its recover landed, a queue shut during teardown, a coordinate
// dropped by a future edit — each leaves a resident entry whose object is
// gone and NO future trigger. Before 1.12.6 the only thing that ever caught
// such an entry was its TTL (3600 s), which is exactly the #187 shape. The
// reconcile audit is the safety net UNDER the event path: every tick it
// samples a bounded number of resident entries, asks the indexer whether
// each entry's own object still exists, and hands every ABSENT coordinate to
// the SAME dep-event worker the informer handlers feed. It never evicts
// directly and never consults the apiserver — it re-derives the DELETE the
// pipeline lost, from the same authority the pipeline reads.
//
// COST. Phase 1 is O(sample) under the store mutex (RangeMetadataSample);
// phase 2 is one indexer GetByKey per unique coordinate under rw.mu.RLock.
// The two locks are NEVER nested — phase 1 releases c.mu before phase 2
// takes rw.mu — so the audit cannot join the resolved.go / watcher.go lock
// order (design §5.3; TestIssue1126_C3_LockOrder).
//
// COVERAGE — a DETECTOR, not a staleness bound. Each tick visits a random
// window of `sample` entries, so against a residency of N the chance one
// entry is still unvisited after T ticks is ≈ (1 − sample/N)^T. At the
// signed defaults (512 / 30 s) and N = 100K: the expected first visit is
// ≈ 195 ticks ≈ 1.6 h, and 99 % coverage needs ≈ 900 ticks ≈ 7.5 h —
// LONGER than the 3600 s TTL. So for most entries the TTL fires first; the
// audit is the backstop for entries that keep being re-Put (keep-warm
// cells, entries C4 suppression keeps alive) and the detector that turns
// a lost DELETE into reconcile_divergence_total > 0. It does NOT bound how
// long a stranded entry can be served; nobody should expect it to catch a
// lost DELETE "within a tick". Tune via DEPS_RECONCILE_SAMPLE for a
// tighter bound (cost is O(sample) under the store mutex per tick).
//
// OFF SWITCHES. CACHE_ENABLED=false builds nothing (no goroutine, no ticker).
// DEPS_RECONCILE_PERIOD_SECONDS=0 disables the ticker with the cache on.
// GET /debug/reconcile (JWT-gated, opt-in) runs ONE full walk on demand,
// CHUNKED: batches of reconcileFullBatch under the store mutex, probes
// between batches outside it, wall time capped (reconcileFullMaxWall).
//
// WHAT IT CANNOT DO. It drives the tracker's own eviction, so an entry with
// NO self dep edge (its Record was dropped at DEPS_MAX_RECORDS —
// dropped_cap) cannot be evicted by anything but its TTL. Such an entry is
// counted under reconcile_skipped_no_edge_total, NOT under
// reconcile_divergence_total, and its coordinate is not submitted (it would
// evict nothing): divergence stays a clean "the pipeline lost a DELETE"
// signal that falls back to zero once the worker has evicted, and the
// no-edge counter is the "dropped_cap is leaving entries un-evictable"
// signal — read it next to snowplow_deps dropped_cap.

package cache

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	envDepsReconcileSample        = "DEPS_RECONCILE_SAMPLE"
	envDepsReconcilePeriodSeconds = "DEPS_RECONCILE_PERIOD_SECONDS"
	defaultDepsReconcileSample    = 512
	defaultDepsReconcilePeriod    = 30
	// reconcileWarnMaxCoords bounds the coordinates named in the per-tick
	// WARN (metadata only: gvr/ns/name, never a key or a body).
	reconcileWarnMaxCoords = 3
	// reconcileFullBatch is the per-batch store-mutex hold of the on-demand
	// full walk (/debug/reconcile): the same order as the periodic sample,
	// so one batch costs a customer Get what one tick already does.
	reconcileFullBatch = 512
	// reconcileFullMaxWall caps the on-demand full walk's wall time; a walk
	// that overruns stops between batches and reports truncated=true.
	reconcileFullMaxWall = 60 * time.Second
)

// depsReconcileSample is the per-tick sample size (<=0 or unparseable →
// default).
func depsReconcileSample() int {
	n := intFromEnv(envDepsReconcileSample, defaultDepsReconcileSample)
	if n <= 0 {
		return defaultDepsReconcileSample
	}
	return n
}

// depsReconcilePeriod is the tick period; 0 means DISABLED ("0" or any
// non-positive value). Unset/unparseable → default.
func depsReconcilePeriod() time.Duration {
	s := intFromEnv(envDepsReconcilePeriodSeconds, defaultDepsReconcilePeriod)
	if s <= 0 {
		return 0
	}
	return time.Duration(s) * time.Second
}

// depsReconcile holds the ticker's lifecycle + counters.
type depsReconcile struct {
	startOnce sync.Once
	started   atomic.Bool
	// stopCh is closed by resetDepsReconcileForTest so a test can retire a
	// ticker deterministically; done is closed when run exits. Production
	// stops only through ctx (the process-lifetime cacheCtx).
	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}

	ticks         atomic.Uint64 // periodic ticks that ran (store + watcher present)
	sampled       atomic.Uint64 // resident entries visited in phase 1 (all callers)
	probed        atomic.Uint64 // unique coordinates probed (all callers)
	divergence    atomic.Uint64 // coordinates whose object was ABSENT → enqueued
	unknown       atomic.Uint64 // coordinates skipped: indexer not authoritative
	skippedNoEdge atomic.Uint64 // ABSENT entries with no self dep edge (dropped_cap): not submitted
	panics        atomic.Uint64 // ticks that panicked (recovered; the ticker survives)
}

var depsReconcileInstance = newDepsReconcile()

func newDepsReconcile() *depsReconcile {
	return &depsReconcile{stopCh: make(chan struct{}), done: make(chan struct{})}
}

// DepsReconcileStats is the read-only counter snapshot (deps_expvar.go).
type DepsReconcileStats struct {
	Ticks, Sampled, Probed, Divergence, Unknown, SkippedNoEdge, Panics uint64
	Started                                                            bool
}

// DepsReconcileStatsSnapshot loads the counters. Cheap; creates nothing.
func DepsReconcileStatsSnapshot() DepsReconcileStats {
	r := depsReconcileInstance
	return DepsReconcileStats{
		Ticks:         r.ticks.Load(),
		Sampled:       r.sampled.Load(),
		Probed:        r.probed.Load(),
		Divergence:    r.divergence.Load(),
		Unknown:       r.unknown.Load(),
		SkippedNoEdge: r.skippedNoEdge.Load(),
		Panics:        r.panics.Load(),
		Started:       r.started.Load(),
	}
}

// StartDepsReconcile starts the periodic sampled audit. No-op (builds NO
// goroutine) unless the resolved cache is enabled AND the period is > 0.
// Idempotent. Bound to ctx (the process-lifetime cacheCtx in main.go).
func StartDepsReconcile(ctx context.Context) {
	if !ResolvedCacheEnabled() {
		return
	}
	period := depsReconcilePeriod()
	if period <= 0 {
		slog.Info("cache.deps_reconcile.disabled",
			slog.String("subsystem", "cache"),
			slog.String("hint", envDepsReconcilePeriodSeconds+" is 0 — the sampled reconcile audit "+
				"is off; stranded L1 entries are covered only by their TTL"))
		return
	}
	r := depsReconcileInstance
	r.startOnce.Do(func() {
		r.started.Store(true)
		sample := depsReconcileSample()
		slog.Info("cache.deps_reconcile.started",
			slog.String("subsystem", "cache"),
			slog.Duration("period", period),
			slog.Int("sample", sample))
		go r.run(ctx, period, sample)
	})
}

func (r *depsReconcile) run(ctx context.Context, period time.Duration, sample int) {
	defer close(r.done)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-t.C:
			// A tick and the cancellation can be ready together; never run
			// an audit after the context that owns this ticker is gone.
			if ctx.Err() != nil {
				return
			}
			r.tick(sample)
		}
	}
}

// tick runs one sampled audit against the process store + watcher. Panic-
// guarded: a panicking tick is counted and logged, the ticker survives.
func (r *depsReconcile) tick(sample int) {
	defer func() {
		if rec := recover(); rec != nil {
			r.panics.Add(1)
			slog.Error("cache.deps_reconcile.panic",
				slog.String("subsystem", "cache"),
				slog.Any("panic", rec),
				slog.String("stack", string(debug.Stack())))
		}
	}()
	store := ResolvedCache()
	rw := Global()
	if store == nil || rw == nil {
		return
	}
	r.ticks.Add(1)
	res := reconcileOnce(store, rw, sample)
	r.account(res)
	if res.Divergent > 0 {
		coords := make([]string, 0, reconcileWarnMaxCoords)
		for _, e := range res.Entries {
			if len(coords) == reconcileWarnMaxCoords {
				break
			}
			coords = append(coords, e.GVR+" "+e.Namespace+"/"+e.Name)
		}
		slog.Warn("cache.deps_reconcile.divergence",
			slog.String("subsystem", "cache"),
			slog.Int("sampled", res.Sampled),
			slog.Int("probed", res.Probed),
			slog.Int("divergent", res.Divergent),
			slog.Int("unknown", res.Unknown),
			slog.Int("skipped_no_edge", res.SkippedNoEdge),
			slog.Any("sample", coords),
			slog.String("effect", "resident L1 entries whose own object is ABSENT from a synced indexer "+
				"were handed to the dep-event worker (evict on its ABSENT verdict). Each one is a "+
				"DELETE the event pipeline lost; a rate that does not fall to zero between ticks means "+
				"a handler, the queue or the relist bridge is dropping events (1.12.6 C3)"))
	}
}

func (r *depsReconcile) account(res ReconcileReport) {
	r.sampled.Add(uint64(res.Sampled))
	r.probed.Add(uint64(res.Probed))
	r.divergence.Add(uint64(res.Divergent))
	r.unknown.Add(uint64(res.Unknown))
	r.skippedNoEdge.Add(uint64(res.SkippedNoEdge))
}

// ReconcileEntry is one divergent coordinate, METADATA ONLY (the same fields
// ResolvedEntryMeta already exposes — no key inputs beyond the GVR
// coordinates, no body, no hash).
type ReconcileEntry struct {
	KeyHash         string `json:"keyHash"`
	CacheEntryClass string `json:"class"`
	GVR             string `json:"gvr"`
	Namespace       string `json:"namespace"`
	Name            string `json:"name"`
}

// ReconcileReport is the outcome of one audit pass.
type ReconcileReport struct {
	// Sampled is the number of resident entries visited in phase 1
	// (including entries with no self coordinate, which are not probed).
	Sampled int `json:"sampled"`
	// Probed is the number of UNIQUE self coordinates probed in phase 2 —
	// per-cohort copies of one widget share a coordinate and one probe.
	Probed int `json:"probed"`
	// Divergent is the number of probed coordinates whose object was ABSENT
	// from a synced, servable indexer AND that at least one resident entry
	// holds a self dep edge for; each was submitted to the dep-event worker.
	Divergent int `json:"divergent"`
	// Unknown is the number of probed coordinates whose GVR the indexer is
	// not authoritative for right now (unregistered, unsynced, watch broken,
	// relist window) — skipped, never guessed.
	Unknown int `json:"unknown"`
	// SkippedNoEdge is the number of resident ENTRIES whose object was
	// ABSENT but that hold no self dep edge (their Record was dropped at
	// DEPS_MAX_RECORDS — dropped_cap): the worker could not evict them, so
	// they are counted here instead of as divergence and not submitted.
	// Such an entry is served until its TTL; a non-zero value is the
	// dropped_cap symptom, not a lost DELETE.
	SkippedNoEdge int `json:"skippedNoEdge"`
	// Entries lists the divergent ENTRIES (one row per resident entry, so a
	// coordinate held under three cohorts yields three rows).
	Entries []ReconcileEntry `json:"entries"`
	// Batches / SnapshotHoldMicros / MaxBatchHoldMicros / Truncated describe
	// the CHUNKED full walk (ReconcileFull only; zero for the sampled tick):
	// how many store-mutex batches ran, how long the one-off key snapshot
	// held the mutex, the longest single batch hold, and whether the walk
	// stopped at reconcileFullMaxWall before visiting everything.
	Batches            int   `json:"batches,omitempty"`
	SnapshotHoldMicros int64 `json:"snapshotHoldMicros,omitempty"`
	MaxBatchHoldMicros int64 `json:"maxBatchHoldMicros,omitempty"`
	Truncated          bool  `json:"truncated,omitempty"`
}

// reconcileCandidate is the phase-1 projection of one entry.
type reconcileCandidate struct {
	keyHash string
	class   string
	key     depEventKey
}

// reconcileOnce runs one audit pass over store against rw. limit > 0 samples
// up to limit entries (RangeMetadataSample, one bounded lock hold); limit
// <= 0 walks EVERY entry (the /debug/reconcile full walk) in batches of
// reconcileFullBatch — RangeMetadataBatched holds the store mutex per
// batch only, each batch is probed before the next is collected, and the
// walk stops between batches at reconcileFullMaxWall.
//
// Two phases per batch, two locks, never nested:
//
//	phase 1 — under c.mu: collect (keyHash, class, gvr, ns, name) for entries
//	          with a self coordinate (Name != "" && Resource != "").
//	phase 2 — outside c.mu: probeObjectState per unique coordinate (takes
//	          rw.mu.RLock inside); ABSENT + edge → submitDepEvent on the
//	          shared worker; ABSENT + no edge → skippedNoEdge; UNKNOWN →
//	          count and skip; EXISTS → nothing.
func reconcileOnce(store *ResolvedCacheStore, rw *ResourceWatcher, limit int) ReconcileReport {
	st := reconcileState{rw: rw, w: depWatchSingleton(), deps: Deps(), verdicts: map[depEventKey]objectState{}}
	if store == nil || rw == nil {
		return st.rep
	}
	if limit > 0 {
		var cands []reconcileCandidate
		store.RangeMetadataSample(limit, func(m ResolvedEntryMeta) bool {
			cands = st.collect(cands, m)
			return true
		})
		// c.mu is released here. Phase 2 takes rw.mu (inside
		// probeObjectState) with NO store lock held.
		st.probe(cands)
		return st.rep
	}
	start := time.Now()
	var maxHold time.Duration
	snap := store.RangeMetadataBatched(reconcileFullBatch, func(metas []ResolvedEntryMeta, held time.Duration) bool {
		st.rep.Batches++
		if held > maxHold {
			maxHold = held
		}
		cands := make([]reconcileCandidate, 0, len(metas))
		for _, m := range metas {
			cands = st.collect(cands, m)
		}
		st.probe(cands) // outside the store lock
		if time.Since(start) > reconcileFullMaxWall {
			st.rep.Truncated = true
			return false
		}
		return true
	})
	st.rep.SnapshotHoldMicros = snap.Microseconds()
	st.rep.MaxBatchHoldMicros = maxHold.Microseconds()
	return st.rep
}

// reconcileState carries one audit pass across batches: the verdict memo
// dedupes probes per coordinate for the whole pass (cohort copies and
// batches share one probe), submitted dedupes the worker hand-off.
type reconcileState struct {
	rw        *ResourceWatcher
	w         *depWatch
	deps      *DepTracker
	rep       ReconcileReport
	verdicts  map[depEventKey]objectState
	submitted map[depEventKey]struct{}
}

func (s *reconcileState) collect(cands []reconcileCandidate, m ResolvedEntryMeta) []reconcileCandidate {
	s.rep.Sampled++
	if m.Name == "" || m.Resource == "" {
		return cands
	}
	return append(cands, reconcileCandidate{
		keyHash: m.KeyHash,
		class:   m.CacheEntryClass,
		key: depEventKey{
			gvr:       schema.GroupVersionResource{Group: m.Group, Version: m.Version, Resource: m.Resource},
			namespace: m.Namespace,
			name:      m.Name,
		},
	})
}

// probe is phase 2 for one batch of candidates. Must be called with NO
// store lock held.
func (s *reconcileState) probe(cands []reconcileCandidate) {
	for _, c := range cands {
		st, seen := s.verdicts[c.key]
		if !seen {
			st = s.rw.probeObjectState(c.key.gvr, c.key.namespace, c.key.name)
			s.verdicts[c.key] = st
			s.rep.Probed++
			if st == objUnknown {
				s.rep.Unknown++
			}
		}
		if st != objAbsent {
			continue
		}
		dk := DepKey{GVR: c.key.gvr, Namespace: c.key.namespace, Name: c.key.name}
		if !s.deps.hasEdge(c.keyHash, dk) {
			// dropped_cap: the worker's ABSENT verdict could not reach this
			// entry. Count it apart; do not pin divergence on it forever.
			s.rep.SkippedNoEdge++
			continue
		}
		if s.submitted == nil {
			s.submitted = map[depEventKey]struct{}{}
		}
		if _, done := s.submitted[c.key]; !done {
			s.submitted[c.key] = struct{}{}
			s.rep.Divergent++
			s.w.submitDepEvent(s.rw, c.key)
		}
		s.rep.Entries = append(s.rep.Entries, ReconcileEntry{
			KeyHash:         c.keyHash,
			CacheEntryClass: c.class,
			GVR:             c.key.gvr.String(),
			Namespace:       c.key.namespace,
			Name:            c.key.name,
		})
	}
}

// ReconcileFull runs ONE full-walk audit on demand (GET /debug/reconcile).
// Returns ok=false (and an empty report) when the resolved cache is off or
// no watcher is installed. Divergent coordinates ARE submitted to the
// dep-event worker — the endpoint is a reconcile, not a dry run; that is
// why it is opt-in and JWT-gated. Counted on sampled/probed/divergence/
// unknown/skipped_no_edge (not on ticks). CHUNKED: the store mutex is held
// per batch of reconcileFullBatch, never across the residency; the
// measured holds are in the report and in the INFO line.
func ReconcileFull() (ReconcileReport, bool) {
	store := ResolvedCache()
	rw := Global()
	if store == nil || rw == nil {
		return ReconcileReport{}, false
	}
	start := time.Now()
	rep := reconcileOnce(store, rw, 0)
	depsReconcileInstance.account(rep)
	slog.Info("cache.deps_reconcile.full_walk",
		slog.String("subsystem", "cache"),
		slog.Int("sampled", rep.Sampled),
		slog.Int("probed", rep.Probed),
		slog.Int("divergent", rep.Divergent),
		slog.Int("unknown", rep.Unknown),
		slog.Int("skipped_no_edge", rep.SkippedNoEdge),
		slog.Int("batches", rep.Batches),
		slog.Int64("snapshot_hold_micros", rep.SnapshotHoldMicros),
		slog.Int64("max_batch_hold_micros", rep.MaxBatchHoldMicros),
		slog.Bool("truncated", rep.Truncated),
		slog.Duration("elapsed", time.Since(start)))
	return rep, true
}

// resetDepsReconcileForTest retires the current ticker (if one was started)
// and WAITS for its goroutine to exit, then replaces the singleton so a test
// sees zeroed counters and a fresh startOnce. The wait is load-bearing: a
// ticker from a previous test that is mid-tick would otherwise audit the
// NEXT test's store and watcher (it reads ResolvedCache() and Global() at
// tick time). TEST-ONLY.
func resetDepsReconcileForTest() {
	r := depsReconcileInstance
	if r.started.Load() {
		r.stopOnce.Do(func() { close(r.stopCh) })
		<-r.done
	}
	depsReconcileInstance = newDepsReconcile()
}
