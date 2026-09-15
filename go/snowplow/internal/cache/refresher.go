// refresher.go — Ship C (0.30.112): the runtime L1 resolved-output
// cache refresher, rebuilt on a client-go workqueue.
//
// The dep tracker dirty-marks an L1 key (OnAdd/OnUpdate -> refreshHook)
// when an informer event invalidates the resolved output. The refresher
// is the worker pool that drains those dirty-marks and RE-RESOLVES the
// entry — never evicts (feedback_l1_invalidation_delete_only.md: UPDATE
// uses stale-while-revalidate).
//
// QUEUE FOUNDATION (Ship C — replaces the 0.30.8 hand-rolled
// enqueueCh + inFlight + dedupWindow):
//
//   * workqueue.NewTypedRateLimitingQueue[string] backed by a
//     NewTypedItemExponentialFailureRateLimiter[string] (base/max delay
//     from the env knobs). The queue gives us, for free, the three
//     properties the hand-rolled version lacked or got wrong:
//       - idempotent dedup: Add(key) of an already-queued key is a
//         no-op — M rapid dirty-marks of one key coalesce to one
//         processing;
//       - NEVER drops: the queue is unbounded; a burst past any buffer
//         is queued, not dropped (the F-drop falsifier);
//       - bounded exponential-backoff retry: AddRateLimited re-enqueues
//         a failed key after an exponentially-growing delay; Forget
//         stops the backoff once it succeeds (the F-backoff falsifier).
//
//   * N workers (RESOLVED_CACHE_REFRESHER_PARALLELISM) each run
//     wait.UntilWithContext(processNext): Get -> process -> Done; on
//     success Forget, on error AddRateLimited.
//
//   * ShutDown() on ctx-cancel drains the workers cleanly — Get returns
//     shutdown=true, the worker loop returns, no goroutine leak.
//
// Lifecycle: one pool launched at process start by StartRefresher
// (after ResolvedCache() is built and after the dispatchers register
// their RefreshFuncs). Exits on context cancellation or StopRefresher.

package cache

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

// Refresher env knobs. The base/max backoff delays drive the
// exponential-failure rate limiter; parallelism sizes the worker pool.
const (
	envRefresherParallelism = "RESOLVED_CACHE_REFRESHER_PARALLELISM"
	envRefresherBaseDelayMS = "RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS"
	envRefresherMaxDelayMS  = "RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS"

	// Task #321 (#318-R1a) — per-key re-resolve RATE-FLOOR. The minimum
	// interval, in seconds, between SUCCESSFUL re-resolves of any one L1
	// key on the refresher dequeue path. When an entry is younger than
	// this floor (time.Since(entry.CreatedAt) < floor — CreatedAt is the
	// wall-clock of the last successful Put, resolved.go:767-768), the
	// dequeued key is NOT re-resolved now: it is Forgotten (reset backoff,
	// we are NOT failing) and re-scheduled via AddAfter(remaining) onto the
	// SAME tier queue it was drawn from, then the worker returns. The
	// deferred re-resolve fires at >= floor expiry against LATEST cluster
	// state. This collapses the install-churn storm's repeated within-wave
	// re-marks of the same cluster-LIST cell (R1 trace: 141,381 redundant
	// completed re-resolves driven by ~28 configmap events fanning into the
	// cluster-wide bucket) into one re-resolve per floor window.
	//
	// LOSSLESS (the invariant): the floored branch never drops a dirty
	// mark — AddAfter always eventually calls the idempotent base Add
	// (client-go@v0.33.0 delaying_queue.go:305), and the workqueue's Done
	// re-pushes any mark that arrived during processing (queue.go:303-306),
	// so the FINAL state change inside the floor still re-resolves at
	// expiry (last-write-wins convergence ≤ floor). NOT in the 6-revert
	// stale-clean class.
	//
	// floor=0 (RATE_FLOOR_SECONDS unset / 0 / negative) short-circuits the
	// entire gate — byte-identical to pre-#321 behaviour. This is the
	// kill-switch (project_caching_is_provisional). It is a TUNING var
	// UNDER the cache, not a feature flag: the gate runs only on the
	// refresher dequeue path, which never executes under cache-off
	// (StartRefresher early-returns, refresher.go:309). Read via
	// int64FromEnv per the SLICEABILITY_REVERIFY_RATE_FLOOR_SECONDS
	// prior-art pattern (ra_full_list_slice.go:419-425).
	envRefresherRateFloorSeconds = "RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS"

	defaultRefresherParallelism = 4
	// Exponential-failure backoff: first retry after baseDelay, doubling
	// each requeue, capped at maxDelay. 500ms -> 1s -> 2s -> ... -> 60s.
	defaultRefresherBaseDelayMS = 500
	defaultRefresherMaxDelayMS  = 60_000

	// defaultRefresherRateFloorSeconds — the #318-R1a rate-floor default,
	// 2s per the PM gate ruling (docs/ship-318-r1a-pm-gate-2026-06-11.md
	// §3): 2s keeps the pathological worst-case refresher convergence AT
	// the documented 10s SLA (yield 5s + resolve ≤3s + floor 2s = 10s),
	// not over it, so the existing hard SLA stands with NO re-documentation;
	// and it already coalesces a whole sub-second within-wave configmap
	// burst into one re-resolve (the incremental collapse of 3s over 2s is
	// small — inter-wave gaps already exceed 3s). 3s remains a valid
	// env-knob tune-up (RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS) if
	// Phase-6 shows 2s under-collapses; the conservative default ships.
	defaultRefresherRateFloorSeconds int64 = 2

	// Ship #98 / 0.30.215 — customer-priority cooperative yield. The
	// refresher worker parks while a customer /call is in flight, mirroring
	// the prewarm engine's pattern at prewarm_engine.go:295-322. The poll
	// cadence is the same constant the prewarm engine uses
	// (defaultEngineYieldPoll = 25ms) so a customer burst clearing fast is
	// observed promptly without busy-spinning the refresher worker.
	refresherYieldPoll = 25 * time.Millisecond

	// refresherYieldMaxParked caps how long a single yieldToCustomer() call
	// can park before it proceeds anyway. Tightened from the architect's
	// initial 10s to 5s per PM verdict C2: this leaves ~5s headroom under
	// the 10s convergence SLA for the actual resolve+populate work, so a
	// sustained customer burst can delay a CRUD-triggered refresh by at
	// MOST refresherYieldMaxParked + one resolve = 5s + ≤3s ≈ 8s under the
	// 10s budget. Also acts as a defense-in-depth bound against a buggy
	// never-decrementing customer-inflight counter (the refresher proceeds
	// regardless after the cap).
	refresherYieldMaxParked = 5 * time.Second

	// maxRefreshRequeues caps how many times a single key may be
	// re-enqueued via AddRateLimited before the refresher gives up on it
	// (Ship 0.30.113 Part A — the poison-pill bound). This is the standard
	// client-go controller idiom: a key whose handler keeps failing is
	// almost always a DETERMINISTIC failure (a deleted CR, a malformed
	// spec, a missing dependency) — re-enqueuing it forever just spins the
	// worker pool with no chance of success. Once NumRequeues exceeds the
	// cap, the key is Forgotten and DROPPED: the entry falls back to its
	// TTL outer-net (no resurrection, no spin). The bound is GENERAL — it
	// is not specific to any one handler kind or failure; it protects
	// against ANY future deterministic refresh failure.
	maxRefreshRequeues = 5
)

// customerInflightHook is a process-global predicate the dispatchers
// subsystem injects at start-up. The refresher reads it at every
// processNext to decide whether to yield to a customer /call.
//
// Nil-default behaviour: when no hook is set (unit tests, cache-off
// production) the refresher proceeds without yielding — byte-identical
// to the pre-Ship-#98 path. The hook is the ONE seam between cache and
// dispatchers; the cache package cannot import dispatchers (cycle), so
// the dispatcher subsystem injects its predicate via SetCustomerInflightHook.
//
// Concurrency: the load + store run under customerInflightHookMu (a
// sync.RWMutex). Reads are hot (per yield-poll); writes happen ONCE at
// process start. The RWMutex serves the contract — never gives the
// caller a stale pointer mid-flight.
var (
	customerInflightHookMu sync.RWMutex
	customerInflightHook   func() bool
)

// SetCustomerInflightHook injects the customer-inflight predicate from
// the dispatchers subsystem. Wired once during main.go's
// dispatchers.RegisterRefreshHandlers + cache.StartRefresher pair. Safe
// to call multiple times (the latest wins); production wires it BEFORE
// StartRefresher so the worker pool sees a populated hook on its first
// processNext.
//
// Passing nil clears the hook (the refresher reverts to never-yield).
// Tests do this between runs to keep the singleton clean.
func SetCustomerInflightHook(fn func() bool) {
	customerInflightHookMu.Lock()
	customerInflightHook = fn
	customerInflightHookMu.Unlock()
}

// customerInFlightLocked reads the current hook under the RWMutex and
// returns the predicate's result (false when no hook is set). Called by
// the refresher worker at yield-poll cadence (25ms × N workers).
func customerInFlightLocked() bool {
	customerInflightHookMu.RLock()
	fn := customerInflightHook
	customerInflightHookMu.RUnlock()
	if fn == nil {
		return false
	}
	return fn()
}

// RefreshFunc is the callback the cache package invokes on a refresh.
// It MUST re-resolve the entry described by inputs and Put the fresh
// bytes back into the L1 store under the same cache key. The cache
// package supplies the matching key string for convenience.
//
// Implementations live in `internal/handlers/dispatchers` (one per
// handlerKind). A non-nil error makes the refresher requeue the key
// with exponential backoff; nil makes it Forget the key.
type RefreshFunc func(ctx context.Context, key string, inputs ResolvedKeyInputs) error

// refresher is the singleton worker pool. Constructed lazily by
// StartRefresher; one per process.
type refresher struct {
	parallelism int

	// queue is the rate-limiting workqueue. Add(key) is idempotent
	// dedup; AddRateLimited(key) re-enqueues with exponential backoff;
	// the queue is unbounded so a dirty-mark is NEVER dropped.
	queue workqueue.TypedRateLimitingInterface[string]

	// Path 3.2 / 0.30.218 — TWO-TIER PRIORITY QUEUE for cluster_list cell
	// refresh. clusterListQueue is the HIGH-PRIORITY tier: cluster_list
	// apistage cells (identity-free, cluster-scope LIST envelopes of size
	// 30-174 MiB on the prod cluster) refresh through this queue. The
	// worker's processNext drains clusterListQueue FIRST when non-empty,
	// then falls back to the normal queue. Mirrors the prewarm engine's
	// priority-scope drain pattern (prewarm_engine.go:280-303). The
	// rationale: cluster_list cells are FEW (~10-15 distinct GVRs) but
	// EXPENSIVE per refresh (~2-3s each), and a stale cluster_list cell
	// affects EVERY broad-RBAC user; per-user cells are MANY but CHEAP
	// per refresh (~50-100ms) and only affect one user at a time. The
	// two-tier discipline ensures cluster_list cells get fresh ahead of
	// the long per-user tail under a CRUD storm.
	//
	// Customer-priority invariant PRESERVED — yieldToCustomer is called
	// BEFORE the handler regardless of which queue the key was drawn from
	// (see processNext below). The two-tier discipline is INSIDE
	// processNext's source selection, ORTHOGONAL to the customer-yield
	// gate.
	//
	// Same rate-limiter wiring as `queue` — exponential-failure backoff,
	// idempotent dedup, unbounded so a dirty-mark is NEVER dropped.
	clusterListQueue workqueue.TypedRateLimitingInterface[string]

	// clusterListKeys is the set of L1 keys known to belong to the
	// cluster_list cell tier. Populated by EnqueueClusterListRefresh on
	// every cluster_list cell registration (PIP boot pre-warm Step 7.5,
	// async populate on cold-miss, or the dep-tracker dirty-mark hook
	// when the dirtied key matches a known cluster_list cell). The
	// dep-tracker SetRefreshHook callback consults this set to decide
	// whether a dirty-marked key should go to clusterListQueue
	// (high-priority tier) or the normal queue.
	//
	// Concurrency: sync.Map — read-mostly. Cluster_list cell cardinality
	// is bounded (~10-15 GVRs at 50K prod scale) so contention is trivial.
	clusterListKeys sync.Map

	handlersMu sync.RWMutex
	handlers   map[string]RefreshFunc

	// triggerGVRByKey carries the GVR whose dirty-mark enqueued each key
	// (R1 Layer 1). The SetRefreshHook callback stores key→trigger-GVR
	// before the queue.Add; processNext loads+deletes it at dequeue and
	// stamps WithRefreshTriggerGVR on the re-resolve ctx so
	// apistageContentServe force-misses a content entry keyed on that GVR.
	// A key present in the queue but ABSENT here (e.g. a self-enqueue, or
	// a value overwritten by a later dirty-mark before dequeue) simply
	// carries no trigger GVR — the re-resolve then behaves exactly as
	// pre-R1 (no forced miss). Last-write-wins on concurrent re-marks of
	// the same key is acceptable: any of the dirtying GVRs is a valid
	// force-miss target, and the rate-floor/dedup already collapses the
	// re-marks to one re-resolve. sync.Map: write-on-enqueue,
	// delete-on-dequeue, bounded by the in-flight key set.
	triggerGVRByKey sync.Map

	// Falsifier counters (atomic).
	enqueueTotal        atomic.Uint64
	completedTotal      atomic.Uint64
	failedTotal         atomic.Uint64
	retriedTotal        atomic.Uint64 // keys re-enqueued via AddRateLimited
	droppedTotal        atomic.Uint64 // keys dropped after exceeding maxRefreshRequeues
	skippedNoEntryTotal atomic.Uint64
	skippedNoHandler    atomic.Uint64

	// Task #321 (#318-R1a) — rate-floor falsifier counter. flooredTotal
	// ticks every time the dequeue-side floor gate DEFERS a key (entry
	// younger than the floor): Forget + AddAfter(remaining) + return,
	// instead of re-resolving now. THE primary falsifier readout — under
	// the install storm completed_total collapses while flooredTotal rises.
	//
	// ACCOUNTING (PM gate condition C1): flooredTotal counts EVERY floored
	// re-cycle — including the bounded immediate-repush re-cycles when a
	// concurrent re-mark sets the dirty bit during a floored dequeue (Done
	// re-pushes, the key re-dequeues inside the floor and is floored again
	// without a re-resolve). So flooredTotal EXCEEDS the completed-collapse
	// delta (old completed − new completed); it is NOT equal to it. GREEN =
	// completed_total collapses AND flooredTotal > 0 and rising. Do NOT
	// equate flooredTotal to the collapse delta.
	flooredTotal atomic.Uint64

	// Ship 0.30.120 layer (b) — error-aware Put-gate counter.
	// refresherSkippedStageError counts L1 Puts the error-aware gate
	// declined because a stage error was observed during the refresh
	// re-resolve (a continueOnError'd failure — a genuine RBAC denial,
	// an apiserver fault). The prior good entry is kept; TTL is the
	// outer net.
	//
	// (The Ship 0.30.120 layer-(a) refresherSkippedExportJwt counter was
	// REMOVED at Ship 0.30.123 (#155) — in-process nested /call resolves
	// an exportJwt loopback stage correctly, so the layer-(a) skip-to-TTL
	// net is obsolete. Layer (b) stays as the general backstop.)
	refresherSkippedStageError atomic.Uint64

	// 1.12.5 / #187 (i) — selfNotFoundEvict counts entries EVICTED because
	// the refresh re-fetch of the entry's OWN object came back with a
	// definite apiserver 404. Pre-1.12.5 that 404 was flattened to a
	// string, requeued five times and then dropped (refresh_dropped) with
	// the stale body still resident until the 1h TTL — the mechanism the
	// portal actually felt in #187. Non-zero here is NORMAL on a cluster
	// that deletes CRs; it is the healthy counterpart of the
	// refresh_dropped lines it replaces.
	selfNotFoundEvict atomic.Uint64

	// External-no-cache (proposal 2026-06-22) — external-touched Put-gate
	// counter. externalSkippedPut counts L1 Puts declined because the
	// resolve reached the live external fetch (httpFetchAllowingNonJSON),
	// which has no informer/dep edge to invalidate it. Process-wide
	// falsifier for "did the external gate fire?"; read via
	// ExternalSkippedPut(), bumped via BumpExternalSkippedPut() at each of
	// the 5 Put surfaces.
	externalSkippedPut atomic.Uint64

	// Ship #98 / 0.30.215 — customer-priority yield falsifier counter.
	// yieldedTotal ticks every time a worker spent at least one yield-poll
	// parked in yieldToCustomer waiting for the customer-inflight signal
	// to clear. cappedTotal ticks when yieldToCustomer hit
	// refresherYieldMaxParked and proceeded anyway (the defense-in-depth
	// bound). These two counters are the post-deploy mechanism-gate
	// evidence: if yieldedTotal stays 0 under burst the hook is broken; if
	// cappedTotal climbs the customer-inflight signal is leaking
	// (never-decrementing counter or a true sustained-burst pathological
	// case).
	yieldedTotal atomic.Uint64
	cappedTotal  atomic.Uint64

	// Path 3.2 / 0.30.218 — two-tier queue falsifier counters.
	// clusterListEnqueueTotal ticks every Enqueue into clusterListQueue;
	// clusterListCompletedTotal ticks every successful drain of
	// clusterListQueue. Used by AC-P3.2.5 (cluster_list cell refresh
	// within 30s) and AC-P3.2.13 (per-user tier non-starvation under
	// sustained burst). Per-tier observability is mandatory because the
	// dominant residual risk is tier-skew — the two counters let us
	// distinguish "high-priority tier starves low-priority tier" from
	// "both tiers progress in proportion".
	clusterListEnqueueTotal   atomic.Uint64
	clusterListCompletedTotal atomic.Uint64

	startedOnce sync.Once
	stopOnce    sync.Once
	// workersWG lets test cleanup block until every worker goroutine
	// has actually exited (Get returned shutdown).
	workersWG sync.WaitGroup

	// 1.12.6 C4 (§6.3) — the drop-point eviction breaker. See
	// refresher_terminal.go. Constructed with the singleton from
	// REFRESH_DROP_EVICT_MAX_PER_MINUTE; perMinute 0 means "never evict a
	// non-404 deterministic failure" (today's drop-to-TTL, byte-for-byte).
	dropEvict *dropEvictBreaker
}

var (
	// refresherInstance is the process-wide singleton, stored ONCE by
	// refresherSingleton and read WITHOUT constructing by refresherPeek.
	// An atomic pointer (1.12.6 C7 / #203): the telemetry surfaces
	// (/debug/vars, the OTLP callback) read it concurrently with the first
	// enqueue's construction, and a plain pointer read there is the B1 race.
	refresherInstance atomic.Pointer[refresher]
	refresherInit     sync.Once
)

// refresherPeek returns the singleton if it has been built and nil
// otherwise, WITHOUT constructing it. Every telemetry accessor reads through
// this (#203): a /debug/vars scrape or an OTLP collection on a cache-off pod
// must not build the two workqueues (and their waitingLoop goroutines) it is
// reporting on.
func refresherPeek() *refresher { return refresherInstance.Load() }

// refresherSingleton returns the process-wide refresher, constructing
// it lazily.
func refresherSingleton() *refresher {
	refresherInit.Do(func() {
		parallelism := positiveIntFromEnv(envRefresherParallelism, defaultRefresherParallelism)
		baseMS := positiveIntFromEnv(envRefresherBaseDelayMS, defaultRefresherBaseDelayMS)
		maxMS := positiveIntFromEnv(envRefresherMaxDelayMS, defaultRefresherMaxDelayMS)
		rl := workqueue.NewTypedItemExponentialFailureRateLimiter[string](
			time.Duration(baseMS)*time.Millisecond,
			time.Duration(maxMS)*time.Millisecond,
		)
		// Path 3.2 — separate rate limiter instance for the cluster_list tier
		// so per-key NumRequeues counts do not leak between tiers (a
		// per-user cell's retry history must not affect a cluster_list
		// cell's retry budget and vice versa).
		clRL := workqueue.NewTypedItemExponentialFailureRateLimiter[string](
			time.Duration(baseMS)*time.Millisecond,
			time.Duration(maxMS)*time.Millisecond,
		)
		refresherInstance.Store(&refresher{
			parallelism:      parallelism,
			queue:            workqueue.NewTypedRateLimitingQueue[string](rl),
			clusterListQueue: workqueue.NewTypedRateLimitingQueue[string](clRL),
			handlers:         map[string]RefreshFunc{},
			dropEvict:        newDropEvictBreaker(RefreshDropEvictMaxPerMinute()),
		})
	})
	return refresherInstance.Load()
}

// rateFloor returns the active #318-R1a per-key re-resolve rate-floor as a
// time.Duration. Read on every dequeue from the env so deployers can
// re-tune at pod start without a code change (matches the prior-art
// sliceabilityReverifyRateFloorSeconds pattern, ra_full_list_slice.go:419-425).
// A value <= 0 disables the floor (byte-identical to pre-#321); the caller's
// `floor > 0` guard short-circuits the gate. int64FromEnv returns the default
// for unset/empty/unparseable (resolved.go:1332-1342).
func (r *refresher) rateFloor() time.Duration {
	return time.Duration(int64FromEnv(envRefresherRateFloorSeconds, defaultRefresherRateFloorSeconds)) * time.Second
}

// RegisterRefreshFunc wires a refresh handler for handlerKind ("restactions",
// "widgets"). Safe to call multiple times; later calls replace the
// earlier wiring (used by tests + by hot-reload scenarios).
//
// MUST be called BEFORE StartRefresher so the worker pool sees a fully
// populated handler map on its first dequeue.
func RegisterRefreshFunc(handlerKind string, fn RefreshFunc) {
	r := refresherSingleton()
	r.handlersMu.Lock()
	r.handlers[handlerKind] = fn
	r.handlersMu.Unlock()
}

// StartRefresher launches the worker pool. Idempotent — repeated calls
// are no-ops (the second StartRefresher does NOT spawn more workers).
// The pool exits cleanly when ctx is canceled OR when StopRefresher is
// called (both ShutDown the queue).
func StartRefresher(ctx context.Context) {
	if !ResolvedCacheEnabled() {
		return
	}
	r := refresherSingleton()
	r.startedOnce.Do(func() {
		// Wire the dep tracker's dirty-mark hook to the queue. Add is
		// idempotent — repeat marks of one key coalesce; the queue is
		// unbounded — a mark is never dropped.
		//
		// Ship #91 / 0.30.211 — Lever C: for EVERY dirty-marked L1 key
		// also submit the key to the bounded async invalidator worker.
		// The worker's invalidate() walks the sliceability memo and
		// removes entries whose raKey matches; for non-RAFullList keys
		// the walk is a fast O(memo-size, ≤4096) scan that finds zero
		// matches — a no-op. We cannot gate on the L1 class here because
		// a stuck-false (Mode-3) RAFullList raKey has NO L1 entry by
		// construction (PutRAFullList is only called on the verdict=true
		// branch); the memo holds the entry, so we MUST consult the memo
		// directly, not the L1 store. The invalidator is non-blocking
		// (drop-on-full) so this never delays the refresher enqueue path.
		Deps().SetRefreshHook(func(l1Key string, triggerGVR schema.GroupVersionResource) {
			// R1 Layer 1 — record the GVR whose dirty-mark enqueued this key
			// BEFORE the queue.Add, so processNext can stamp it on the
			// re-resolve ctx (dep-edge-equality force-miss). Last-write-wins
			// on concurrent re-marks (any dirtying GVR is a valid target);
			// an empty (zero) GVR is still stored — RefreshTriggerGVRFromContext
			// only matches a non-empty equality so a zero never force-misses.
			r.triggerGVRByKey.Store(l1Key, triggerGVR)
			// Path 3.2 / 0.30.218 — two-tier dispatch. If the key is a
			// registered cluster_list cell, route it to the
			// HIGH-PRIORITY tier; otherwise the normal tier. The
			// clusterListKeys set is populated by PIP boot pre-warm
			// (phase1_clusterlist_prewarm.go) AND by
			// EnqueueClusterListRefresh from cluster_list.go's
			// cold-miss async-populate path.
			if _, isClusterList := r.clusterListKeys.Load(l1Key); isClusterList {
				r.enqueueClusterList(l1Key)
			} else {
				r.enqueue(l1Key)
			}
			SubmitSliceabilityInvalidate(l1Key)
		})

		for i := 0; i < r.parallelism; i++ {
			r.workersWG.Add(1)
			go func(id int) {
				defer r.workersWG.Done()
				// UntilWithContext re-invokes processNext until ctx is
				// done. processNext blocks in queue.Get, so the period
				// only matters after the queue ShutsDown (Get returns
				// immediately) — a small period keeps the post-shutdown
				// spin bounded; the loop exits on ctx.Done regardless.
				wait.UntilWithContext(ctx, func(c context.Context) {
					for r.processNext(c) {
					}
				}, time.Second)
			}(i)
		}

		// Shut the queues down when ctx is cancelled — Get unblocks on
		// both tiers, every worker's processNext returns false, the
		// loop ends.
		go func() {
			<-ctx.Done()
			r.queue.ShutDown()
			r.clusterListQueue.ShutDown()
		}()

		slog.Info("refresher.started",
			slog.String("subsystem", "cache"),
			slog.Int("parallelism", r.parallelism),
			slog.String("queue", "workqueue.RateLimiting"),
		)
	})
}

// StopRefresher shuts both queues down. Safe to call multiple times.
// Used by tests; production lets the context-cancel path drive shutdown.
func StopRefresher() {
	r := refresherSingleton()
	r.stopOnce.Do(func() {
		r.queue.ShutDown()
		r.clusterListQueue.ShutDown()
	})
}

// enqueue adds l1Key to the workqueue. Add is idempotent: a key already
// queued (or being processed) is coalesced — never duplicated, never
// dropped. The counter ticks on every accepted enqueue call; dedup is
// invisible to it by design (the queue owns coalescing).
func (r *refresher) enqueue(l1Key string) {
	if l1Key == "" {
		return
	}
	r.queue.Add(l1Key)
	r.enqueueTotal.Add(1)
}

// enqueueClusterList adds l1Key to the HIGH-PRIORITY cluster_list
// workqueue tier (Path 3.2 / 0.30.218). Same idempotent-dedup +
// unbounded contract as the normal tier; the only difference is the
// drain priority — processNext attempts clusterListQueue first.
//
// Callers MUST first register l1Key via RegisterClusterListKey so the
// dep-tracker dirty-mark hook also routes future dirty-marks of the
// same key to this tier (not just the immediate enqueue). The
// registration is the source-of-truth for tier membership; this method
// only schedules ONE refresh.
func (r *refresher) enqueueClusterList(l1Key string) {
	if l1Key == "" {
		return
	}
	r.clusterListQueue.Add(l1Key)
	r.enqueueTotal.Add(1)
	r.clusterListEnqueueTotal.Add(1)
}

// RegisterClusterListKey marks l1Key as belonging to the cluster_list
// cell tier. The dep-tracker dirty-mark hook consults the registered
// set when deciding which tier to route a dirty-marked key to. Idempotent
// (sync.Map.Store of a struct{} sentinel) and read-mostly: registration
// happens at PIP boot pre-warm and on the cold-miss async-populate path
// (the FIRST customer to hit an unpopulated cell); reads happen on every
// dep-tracker dirty-mark.
//
// Empty l1Key is a no-op. Path 3.2 / 0.30.218.
func RegisterClusterListKey(l1Key string) {
	if l1Key == "" {
		return
	}
	refresherSingleton().clusterListKeys.Store(l1Key, struct{}{})
}

// EnqueueClusterListRefresh schedules an L1 re-resolve via the
// HIGH-PRIORITY cluster_list tier. Used by cluster_list.go's cold-miss
// async-populate path: a customer /call hits an unpopulated cluster_list
// cell, falls back to the per-NS iterator path for THAT request, and
// triggers an async populate via this method so the NEXT customer of
// the same cell hits warm. Path 3.2 / 0.30.218.
//
// Also registers l1Key in the cluster_list tier (so future
// dep-tracker dirty-marks of the same key route here too). Idempotent
// + non-blocking; safe to call from any goroutine.
func EnqueueClusterListRefresh(l1Key string) {
	RegisterClusterListKey(l1Key)
	refresherSingleton().enqueueClusterList(l1Key)
}

// IsClusterListKey reports whether l1Key has been registered as a
// cluster_list cell. Read-only — used by tests and metrics. Path 3.2.
func IsClusterListKey(l1Key string) bool {
	_, ok := refresherSingleton().clusterListKeys.Load(l1Key)
	return ok
}

// EnqueueRefresh is the package-level enqueue entry point for callers
// OUTSIDE the deps/refresher wiring that want to schedule a re-resolve on
// the refresher's workqueue. Used by Ship #91 / 0.30.211 Lever A's TTL
// gate: when SliceabilityLookup observes a stuck-false memo entry that has
// aged past T_unverify, it asks the refresher to schedule an L1 re-resolve
// on the refresher's workqueue.
//
// For an EXISTING L1 entry this warms the cell ahead of the next /call
// (≈50-100ms hot re-resolve). For a stuck-false RAFullList raKey with NO
// L1 entry (PutRAFullList never fired on the verdict=false branch), the
// workqueue is a no-op: processOne does c.Get → ok=false →
// skippedNoEntryTotal++ → return. The next /call after worker-invalidate
// pays the cold first-sight cost on first reach. This is CORRECT:
// customer-priority preserved (the TTL-expired /call is fast on the
// cached false verdict); cold cost is bounded to ≤ retryCap × one unlucky
// /call per (raKey, cohort) per pod life.
//
// Idempotent (the workqueue coalesces) and non-blocking (Add is a no-op
// when the queue has shut down). Empty l1Key is a no-op.
//
// Cache-off: when ResolvedCacheEnabled() is false the refresher singleton
// is constructed but its worker pool is never started; Add still buffers
// keys in the unbounded queue but no worker will process them. Production
// only reaches this code from inside the cache package, which itself is
// gated on cache-on at the same Start* call site as the refresher — so
// the buffer-up-but-never-drain case does not arise in practice.
//
// Future ship hook: if Phase-6 data shows post-worker /call cold-tail is
// painful, consider reconstructing ResolvedKeyInputs from memo labels to
// enable refresher-driven L1 warm. Out of scope for #91.
func EnqueueRefresh(l1Key string) {
	refresherSingleton().enqueue(l1Key)
}

// yieldToCustomer parks the worker while any customer /call is in flight,
// re-checking every refresherYieldPoll. Returns promptly once no customer
// call is in flight, OR after refresherYieldMaxParked (defense-in-depth
// cap so a buggy counter cannot stall refresh forever), OR on ctx cancel.
//
// Mirrors the prewarm engine's cooperative yield at
// prewarm_engine.go:295-322 (Ship #98 prior art). The yield is BEFORE the
// handler call; processOne / completedTotal++ / Forget(key) all happen
// AFTER the yield releases. Cache settle-time after a CRUD informer event
// is bounded by refresherYieldMaxParked + the actual resolve time (see
// AC-98.12).
//
// Cooperative customer-priority discipline (feedback_customer_priority_over_refresher):
// the refresher does NOT preempt customer /call work; it steps aside for
// the duration of the burst. The customer-tax surface is one
// atomic-int64 Load per yield tick — negligible (4 workers × 40 Hz = 160
// reads/s steady-state; no cache-line bouncing on the read side because
// each worker reads on its own poll cycle without serializing).
func (r *refresher) yieldToCustomer(ctx context.Context) {
	if !customerInFlightLocked() {
		return
	}
	t := time.NewTicker(refresherYieldPoll)
	defer t.Stop()
	cap := time.NewTimer(refresherYieldMaxParked)
	defer cap.Stop()
	parked := false
	for customerInFlightLocked() {
		if !parked {
			// First park — count it ONCE per yield call (mirrors the
			// prewarm engine's per-call yieldTotal semantics).
			r.yieldedTotal.Add(1)
			parked = true
		}
		select {
		case <-ctx.Done():
			return
		case <-cap.C:
			// Max-parked cap fired — proceed regardless. Counts toward
			// cappedTotal so a leaking customer-inflight counter is
			// observable in the falsifier suite + post-deploy ledger.
			r.cappedTotal.Add(1)
			return
		case <-t.C:
		}
	}
}

// processNext pulls one key, processes it, and reports whether the
// worker loop should continue (false once the queue has ShutDown).
//
//   - success: Forget(key) — clear the backoff — then Done(key).
//   - error, under the requeue cap: AddRateLimited(key) — requeue after
//     exponential backoff — then Done(key). The key WILL be retried.
//   - error, requeue cap exceeded: Forget(key) and DROP the key — then
//     Done(key). The key is NOT retried; the entry falls back to its TTL
//     outer-net. This is the Ship 0.30.113 Part A poison-pill bound — a
//     deterministic failure (one that can never succeed on retry) must
//     not re-enqueue forever and spin the worker pool.
//
// Done(key) is always called (deferred) so the queue can release the
// key for re-add; Forget/AddRateLimited only touch the rate limiter.
//
// Ship #98 / 0.30.215 — CUSTOMER PRIORITY YIELD. Before invoking the
// handler we cooperatively yield to any in-flight customer /call. The
// yield is bounded by refresherYieldMaxParked (5s) so a never-decrementing
// inflight counter cannot stall refresh forever. AC-98.12 (CRUD-to-
// completed Δt ≤ 10s under quiescent load) is the convergence falsifier.
//
// Path 3.2 / 0.30.218 — TWO-TIER PRIORITY DRAIN. The worker probes the
// HIGH-PRIORITY clusterListQueue FIRST (non-blocking Len() probe; a
// non-zero Len() means at least one key is queued, and the Get is then
// guaranteed to return promptly because the queue is non-empty AND
// workqueue.Get races safely across workers). On Len() == 0 (the steady
// state — cluster_list events are infrequent), the worker falls through
// to the blocking normal-tier Get. This preserves the prewarm engine's
// priority-scope drain shape (prewarm_engine.go:280-303) for the
// cluster_list cell tier while keeping the normal tier on the same
// blocking-Get path it had pre-Path-3.2.
//
// CORRECTNESS under concurrent workers: workqueue.Get is concurrency-safe
// (k8s.io/client-go/util/workqueue/queue.go) — if N workers race on a
// queue with M items, only min(N,M) get items, the rest block. A worker
// that probes Len()>0 then races into Get may lose to a peer (peer drains
// it first); the loser's Get blocks. That is a correctness wash — the
// loser still drains the next available item from EITHER tier on the
// next iteration. NO key is lost; the priority property is preserved
// in expectation (every cluster_list arrival wakes at least one worker
// that will Get it on its next loop iteration).
func (r *refresher) processNext(ctx context.Context) bool {
	// Path 3.2 — high-priority drain probe. If the cluster_list tier
	// has work pending, pick it up first. Len() is cheap (atomic read
	// inside the workqueue); a non-zero value guarantees the next
	// Get() either returns an item promptly OR returns shutdown=true
	// (queue ShutDown). On shutdown of clusterListQueue we fall back
	// to the normal tier (which may still be live during a partial
	// shutdown window — production shuts both together via the
	// ctx.Done goroutine, but tests may shutdown only one).
	var (
		key      string
		shutdown bool
		fromCL   bool
	)
	if r.clusterListQueue != nil && r.clusterListQueue.Len() > 0 {
		key, shutdown = r.clusterListQueue.Get()
		if !shutdown {
			fromCL = true
		}
	}
	if !fromCL {
		// Normal tier blocking Get. This is the steady-state path
		// (cluster_list events are infrequent — boot pre-warm
		// populates ~10-15 cells then refreshes only on informer
		// events on the underlying GVRs).
		key, shutdown = r.queue.Get()
		if shutdown {
			return false
		}
	}
	if fromCL {
		defer r.clusterListQueue.Done(key)
	} else {
		defer r.queue.Done(key)
	}

	// Customer-priority cooperative yield (Ship #98). Mirrors prewarm
	// engine's yield-before-scope pattern (prewarm_engine.go:275). The
	// yield reads the dispatcher-injected customer-inflight hook; with
	// no hook (unit tests, cache-off) it is a single zero-cost read +
	// immediate return. Applied IDENTICALLY for both tiers — Path 3.2
	// preserves the Ship #98 customer-priority invariant: NO refresher
	// work, cluster_list-tier or otherwise, races a customer /call.
	r.yieldToCustomer(ctx)

	// Select the queue to mutate on success/failure (Forget /
	// AddRateLimited / NumRequeues all read per-tier rate-limiter
	// state). Task #321 — the floored branch's Forget/AddAfter use this
	// SAME q, so a floored cluster_list key defers onto clusterListQueue
	// and the deferred Done (refresher.go above) is on the matching tier.
	q := r.queue
	if fromCL {
		q = r.clusterListQueue
	}

	// Task #321 (#318-R1a) — per-key re-resolve RATE-FLOOR (S1: reuse
	// ResolvedEntry.CreatedAt; NO new map). Single Get site (PM gate C5):
	// the entry fetched here is passed into processOne below, so there is
	// exactly one c.Get per dequeue. A miss / nil cache falls through to
	// processOne, which bumps skipped_no_entry exactly as pre-#321 — the
	// floor never fires on an absent (DELETE-evicted / TTL-expired) entry,
	// so DELETE invalidation of the SELF entry stays immediate
	// (feedback_l1_invalidation_delete_only).
	//
	// C3 note: a DELETE's LIST-dep / dependent-GET-dep refreshes are
	// dirty-marks (deps.go:613-624 → enqueue), NOT evictions, so they
	// reach this gate and are floor-delayed by ≤ floor — the deleted row
	// disappears from the dependent LIST cell within ≤ floor (same
	// stale-while-revalidate posture as any UPDATE; the self entry's own
	// eviction via RemoveL1Key remains immediate).
	c := ResolvedCache()
	var (
		entry *ResolvedEntry
		ok    bool
	)
	if c != nil {
		entry, ok = c.Get(key)
	}
	// 1.12.6 C4 (§6.4) — SUPPRESSION consult, before the rate floor and
	// before any resolve. A key marked refresh-by-traffic-only (K
	// consecutive stage-error declines, or a structurally permanent decline)
	// is Forgotten and skipped: no resolve, no WARN, the counter carries the
	// rate. The marker is cleared by the next real Put of this key (a
	// customer /call or the seed writes it), and by any eviction of it, so
	// a suppressed key never outlives its entry. #191: 23 keys re-resolved
	// every ~3 min forever because nothing ever stopped asking.
	if _, suppressed := refreshSuppressed.Load(key); suppressed && ok {
		q.Forget(key)
		refreshSuppressedSkipsTotal.Add(1)
		// Consume (and discard) the trigger GVR recorded at enqueue. The
		// R1 stamp below is skipped on this path, so without this the GVR
		// would sit in the map for as long as the key stays suppressed and
		// be applied — as a force-miss — to whatever dequeue first runs
		// after a real Put clears the marker, arbitrarily later (architect
		// N3). The rate-floor branch deliberately keeps its GVR because it
		// re-dispatches the same mark; a suppressed skip does not.
		r.triggerGVRByKey.LoadAndDelete(key)
		return true
	}
	if floor := r.rateFloor(); floor > 0 && ok && entry != nil {
		if elapsed := time.Since(entry.CreatedAt); elapsed < floor {
			// Floored: the entry's content is younger than the floor, so a
			// re-resolve now is redundant. Do NOT drop the mark — defer it.
			// Forget clears any backoff (we are NOT failing); AddAfter
			// schedules the deferred re-resolve at floor expiry. remaining
			// > 0 by construction (elapsed < floor), so AddAfter always
			// takes the async delaying-queue path (delaying_queue.go:260),
			// never the immediate Add. The delaying-heap dedup is
			// earliest-wins (insert(), delaying_queue.go), so concurrent
			// re-marks collapse to ONE pending deferred add. At expiry the
			// re-dequeue finds elapsed >= floor → NOT floored → re-resolves
			// against LATEST cluster state. LOSSLESS: see the rate-floor
			// invariant comment at envRefresherRateFloorSeconds.
			// FAIL-OPEN on declines: CreatedAt is stamped ONLY by a
			// successful Put (resolved.go:767-768); a refresh that DECLINES
			// to cache (never-cache-partials sink skip) leaves CreatedAt
			// old, so a persistently-declining key is never floored — it
			// re-resolves on every dequeue, favoring freshness over collapse.
			q.Forget(key)
			q.AddAfter(key, floor-elapsed)
			r.flooredTotal.Add(1)
			return true
		}
	}

	// R1 Layer 1 — stamp the trigger GVR (recorded at enqueue by the
	// SetRefreshHook callback) onto the re-resolve ctx, so
	// apistageContentServe force-misses a content entry keyed on that GVR
	// during this whole-RA re-resolve. Load-and-delete: the entry is
	// consumed exactly once per dispatch; a key with no recorded GVR (self-
	// enqueue) carries none and the re-resolve is pre-R1-identical. Done
	// here (not in the floored-defer branch above) so the GVR survives a
	// floor deferral and is consumed at the eventual real dispatch.
	rctx := ctx
	if v, present := r.triggerGVRByKey.LoadAndDelete(key); present {
		if tg, isGVR := v.(schema.GroupVersionResource); isGVR && !tg.Empty() {
			rctx = WithRefreshTriggerGVR(ctx, tg)
		}
	}
	if err := r.processOne(rctx, key, entry, ok); err != nil {
		r.failedTotal.Add(1)
		// Poison-pill bound (Part A). NumRequeues is how many times this
		// exact key has already been AddRateLimited. Once it exceeds the
		// cap the failure is treated as deterministic: Forget the key
		// (clear the rate limiter so a FUTURE genuine dirty-mark of the
		// same key starts clean) and DROP it — no AddRateLimited. The
		// entry stays in L1, stale, until its TTL purges it; a later
		// informer event can re-enqueue it fresh.
		if q.NumRequeues(key) >= maxRefreshRequeues {
			q.Forget(key)
			r.droppedTotal.Add(1)
			// 1.12.5 / #187 (i) — THE DROP POINT IS THE EVICTION POINT for a
			// self-object 404.
			//
			// The entry is the resolved output of an object the apiserver has
			// now said is gone maxRefreshRequeues+1 times in a row. Dropping it
			// to the TTL outer-net is what left the stale body resident for the
			// rest of the hour on krateo-057 (264 refresh_failed over six self
			// entries, then 44 refresh_dropped, with the portal still walking a
			// child list that no longer existed 90 minutes later).
			//
			// WHY HERE AND NOT ON THE FIRST 404. The five-requeue budget IS the
			// determinism proof: ~15.5s of backoff at the 500ms/60s defaults,
			// so any window that clears inside it — a CRD re-registration, an
			// apiserver blip that happens to 404 — never reaches this line. A
			// first-404 eviction has no such bound, and the type-exists
			// conjunct alone cannot supply one: objects.Get LAZY-REGISTERS the
			// GVR on its own not-servable path before the error is ever
			// classified, so IsRegistered is true again by the time anything
			// downstream could consult it (proven by the BCRD arm).
			//
			// Eviction routes through the dep tracker, never store.deleteForDep,
			// so RemoveL1Key clears the entry's dep edges alongside it
			// (the resolved.go DELETE-eviction invariant), and it counts on its
			// OWN counter — evict_delete_total stays informer-DELETE-driven so
			// the H1 live discriminator keeps working.
			if errors.Is(err, ErrSelfObjectGone) {
				evicted := Deps().EvictSelfGone(key)
				r.selfNotFoundEvict.Add(1)
				slog.Warn("refresher.self_object_gone_evicted",
					slog.String("subsystem", "cache"),
					slog.String("key_hash", key),
					slog.Int("requeues", maxRefreshRequeues),
					slog.Bool("cluster_list_tier", fromCL),
					slog.Bool("evicted", evicted),
					slog.String("err", err.Error()),
					slog.String("effect", "the entry's own object returned a confirmed apiserver 404 "+
						"on every attempt across the full requeue budget — evicted instead of left "+
						"resident until TTL (#187)"),
				)
				return true
			}
			// 1.12.6 C4 (§6.2 row 3, §6.3) — EVERY OTHER deterministic failure
			// (403 / 500 / timeout / parse failure / apistage not-servable) that
			// exhausts the same budget is now EVICTED at the same drop point,
			// behind the breaker. A body whose basis cannot be re-resolved
			// cannot be vouched for; "forget the key and keep the entry" is the
			// one outcome the rule forbids. The bound is the SAME budget (~15.5 s
			// of backoff at the defaults), so an apiserver blip that clears
			// inside it never reaches this line.
			//
			// THE BREAKER is what makes this safe under an outage: over
			// REFRESH_DROP_EVICT_MAX_PER_MINUTE the key keeps today's
			// drop-to-TTL (entry resident and SERVED), the suspended counter
			// ticks and one WARN per suspension window names the suspected
			// outage. Budget "0" disables this branch entirely (byte-for-byte
			// 1.12.5 for non-404 failures). Eviction routes through the dep
			// tracker's EvictDropPoint — the same store.deleteForDep + RemoveL1Key
			// pair the 404 route uses (deleteForDep is what bumps the STORE's
			// evict_delete_total under snowplow_resolved_cache and clears the
			// suppression marker; RemoveL1Key drops the dep edges) — counted on
			// evict_drop_point_total so NEITHER evict_delete_total (the
			// informer-DELETE-only H1 discriminator) NOR evict_self_gone_total
			// (the confirmed-404 signal) moves for a non-404 eviction. During an
			// apiserver outage this is the counter that climbs, alongside
			// snowplow_refresher_drop_evict_suspended_total.
			if r.dropEvict != nil {
				if allowed, firstSuspend := r.dropEvict.allow(); allowed {
					evicted := Deps().EvictDropPoint(key)
					slog.Warn("refresher.refresh_dropped",
						slog.String("subsystem", "cache"),
						slog.String("key_hash", key),
						slog.Int("requeues", maxRefreshRequeues),
						slog.Bool("cluster_list_tier", fromCL),
						slog.Bool("evicted", evicted),
						slog.String("err", err.Error()),
						slog.String("effect", "deterministic refresh failure — entry EVICTED (was: dropped to TTL outer-net); "+
							"a body whose basis cannot be re-resolved cannot be vouched for (1.12.6 C4)"),
					)
					return true
				} else if firstSuspend {
					warnDropEvictSuspended(key, r.dropEvict.perMinute, fromCL)
				}
			}
			slog.Warn("refresher.refresh_dropped",
				slog.String("subsystem", "cache"),
				slog.String("key_hash", key),
				slog.Int("requeues", maxRefreshRequeues),
				slog.Bool("cluster_list_tier", fromCL),
				slog.String("effect", "deterministic refresh failure — key dropped to TTL outer-net, not retried "+
					"(drop-point eviction disabled or suspended by the breaker)"),
			)
			return true
		}
		r.retriedTotal.Add(1)
		// Bounded exponential-backoff retry. The key is NOT Forgotten,
		// so the rate limiter's NumRequeues climbs and the next delay
		// doubles (capped at maxDelay).
		q.AddRateLimited(key)
		return true
	}
	// Success — stop the rate limiter tracking this key so a future
	// dirty-mark of the same key starts from a clean backoff.
	q.Forget(key)
	r.completedTotal.Add(1)
	if fromCL {
		r.clusterListCompletedTotal.Add(1)
	}
	return true
}

// processOne handles a single refresh: dispatch the registered handler
// for the entry's kind. Returns the handler's error (drives the requeue
// decision). A missing entry / missing handler / legacy nil-Inputs entry
// is a non-error skip (counted, not retried).
//
// Task #321 (#318-R1a) — SINGLE-GET refactor (PM gate C5): the entry is
// fetched ONCE in processNext (it serves the rate-floor's CreatedAt read)
// and passed in here, so there is exactly one c.Get per dequeue. `ok` is
// the Get result (false for a nil cache or a miss); the skipped_no_entry
// semantics are byte-identical to the pre-#321 in-line Get (a nil cache
// or a miss both bump skipped_no_entry and return nil).
func (r *refresher) processOne(ctx context.Context, key string, entry *ResolvedEntry, ok bool) error {
	if !ok || entry == nil {
		// L1 may have evicted between the dirty-mark and us picking up
		// the key (e.g. a DELETE raced the UPDATE), or the cache is nil
		// (cache-off; not reached in production from this path). Stale-
		// while-revalidate degrades to next-cold-miss; not an error, not a
		// retry — the entry is gone.
		r.skippedNoEntryTotal.Add(1)
		return nil
	}
	if entry.Inputs == nil {
		// Legacy pre-0.30.8 entry — no Inputs to drive a re-resolve.
		// Skip silently; TTL will purge. Not an error, not a retry.
		r.skippedNoHandler.Add(1)
		return nil
	}
	r.handlersMu.RLock()
	fn := r.handlers[entry.Inputs.CacheEntryClass]
	r.handlersMu.RUnlock()
	if fn == nil {
		r.skippedNoHandler.Add(1)
		return nil
	}
	if err := fn(ctx, key, *entry.Inputs); err != nil {
		slog.Warn("refresher.refresh_failed",
			slog.String("subsystem", "cache"),
			slog.String("handler_kind", entry.Inputs.CacheEntryClass),
			slog.String("key_hash", key),
			slog.Int("requeues", r.queue.NumRequeues(key)),
			slog.Any("err", err),
		)
		return err
	}
	return nil
}

// refresherStats is the read-only snapshot the summary log, the
// snowplow_refresher_* expvars and the OTLP mirror consume.
//
// 1.12.6 C7: the `stat` tag is the published name; the expvar key is
// snowplow_refresher_<stat>_total for a counter and snowplow_refresher_<stat>
// for a gauge, and the OTLP mirror publishes counters under the shared
// snowplow_refresher{stat=...} instrument and gauges under
// snowplow_refresher_<stat> (StatFamily.ExpvarKey / OTelInstrumentName).
// `stat:"-"` marks a field published elsewhere or not at all — say where.
type refresherStats struct {
	enqueued          uint64 `stat:"enqueue"`
	completed         uint64 `stat:"completed"`
	failed            uint64 `stat:"failed"`
	retried           uint64 `stat:"retried"`
	dropped           uint64 `stat:"dropped"`
	skippedNoEntry    uint64 `stat:"skipped_no_entry"`
	skippedNoHandler  uint64 `stat:"skipped_no_handler"`
	skippedStageError uint64 `stat:"skipped_stage_error"`                                                                                               // Ship 0.30.120 layer (b)
	selfNotFoundEvict uint64 `stat:"-"`                                                                                                                 // 1.12.5 / #187 (i) — published as snowplow_deps.self_notfound_evict_total (deps_expvar.go), beside its tracker-side twin
	yielded           uint64 `stat:"yielded"`                                                                                                           // Ship #98 — customer-priority yields
	capped            uint64 `stat:"capped"`                                                                                                            // Ship #98 — max-parked cap hits
	floored           uint64 `stat:"floored"`                                                                                                           // Task #321 (#318-R1a) — rate-floor deferrals
	queueDepth        int64  `stat:"queue_depth" kind:"gauge" desc:"Live refresher workqueue depth; climbing with stagnant completed = workers stuck."` // 0 before the pool is built

	// Path 3.2 / 0.30.218 — per-tier observability for the two-tier
	// priority queue. Read by ClusterListRefresherStats for the Path 3.2
	// falsifiers; not published on expvar or OTLP.
	clusterListEnqueued  uint64 `stat:"-"`
	clusterListCompleted uint64 `stat:"-"`
}

func refresherStatsSnapshot() refresherStats {
	// #203: a telemetry read never constructs the pool. Before the first
	// enqueue (and forever on a cache-off pod) every counter reads zero.
	r := refresherPeek()
	if r == nil {
		return refresherStats{}
	}
	return refresherStats{
		enqueued:             r.enqueueTotal.Load(),
		completed:            r.completedTotal.Load(),
		failed:               r.failedTotal.Load(),
		retried:              r.retriedTotal.Load(),
		dropped:              r.droppedTotal.Load(),
		skippedNoEntry:       r.skippedNoEntryTotal.Load(),
		skippedNoHandler:     r.skippedNoHandler.Load(),
		skippedStageError:    r.refresherSkippedStageError.Load(),
		selfNotFoundEvict:    r.selfNotFoundEvict.Load(),
		yielded:              r.yieldedTotal.Load(),
		capped:               r.cappedTotal.Load(),
		floored:              r.flooredTotal.Load(),
		clusterListEnqueued:  r.clusterListEnqueueTotal.Load(),
		clusterListCompleted: r.clusterListCompletedTotal.Load(),
		queueDepth:           refresherQueueDepth(r),
	}
}

// refresherQueueDepth is the live workqueue Len(), 0 before the pool exists.
func refresherQueueDepth(r *refresher) int64 {
	if r == nil || r.queue == nil {
		return 0
	}
	return int64(r.queue.Len())
}

// AddRefresherPoolCounterForTest bumps ONE pool counter by stat name so the
// C7 value arms can drive every published refresher stat to a distinct value
// through the real atomics. Returns false for a stat this helper does not
// know — the arm treats that as a failure, so a stat added to refresherStats
// without a line here is caught, not skipped. TEST-ONLY.
func AddRefresherPoolCounterForTest(stat string, n uint64) bool {
	r := refresherSingleton()
	switch stat {
	case "enqueue":
		r.enqueueTotal.Add(n)
	case "completed":
		r.completedTotal.Add(n)
	case "failed":
		r.failedTotal.Add(n)
	case "retried":
		r.retriedTotal.Add(n)
	case "dropped":
		r.droppedTotal.Add(n)
	case "skipped_no_entry":
		r.skippedNoEntryTotal.Add(n)
	case "skipped_no_handler":
		r.skippedNoHandler.Add(n)
	case "skipped_stage_error":
		r.refresherSkippedStageError.Add(n)
	case "yielded":
		r.yieldedTotal.Add(n)
	case "capped":
		r.cappedTotal.Add(n)
	case "floored":
		r.flooredTotal.Add(n)
	default:
		return false
	}
	return true
}

// ErrSelfObjectGone is the 1.12.5 / #187 (i) sentinel: a refresh's re-fetch of
// the entry's OWN object came back with a confirmed apiserver 404. The
// dispatchers package wraps it (%w) into the re-fetch error, preserving the
// existing error TEXT for anyone grepping logs, and processNext tests it with
// errors.Is AT THE DROP POINT — after the full requeue budget has been spent
// re-confirming the deletion.
//
// It lives here, not in dispatchers, because the refresher is the consumer:
// the eviction decision belongs where the deterministic-failure budget is
// enforced, not where the single 404 is observed.
//
// It says SELF by construction. The only site that wraps it is the re-fetch of
// the CR named by the entry's own Inputs, which runs BEFORE the resolver and
// therefore before any inner call can fail. An inner call's NotFound is the
// dirty-mark class (OnDelete bucket 2/3) and never produces this.
var ErrSelfObjectGone = errors.New("self object gone (apiserver 404 on the entry's own object)")

// RefresherSelfNotFoundEvictTotal returns the process-wide count of L1
// entries evicted because the refresh re-fetch of the entry's OWN object
// returned a definite apiserver 404 (1.12.5 / #187 (i)). Read by the
// expvar + OTLP surfaces.
func RefresherSelfNotFoundEvictTotal() uint64 {
	r := refresherPeek() // #203: never construct from a scrape
	if r == nil {
		return 0
	}
	return r.selfNotFoundEvict.Load()
}

// ClusterListRefresherStats exposes the Path 3.2 two-tier counters for
// OBS-1 expvar wiring + falsifier tests. Read-only snapshot.
func ClusterListRefresherStats() (enqueued, completed uint64) {
	r := refresherPeek() // #203: never construct from a scrape
	if r == nil {
		return 0, 0
	}
	return r.clusterListEnqueueTotal.Load(), r.clusterListCompletedTotal.Load()
}

// resetRefresherForTest tears the singleton down so each test sees a
// clean refresher. Exported only via the *_test.go shim — production
// code MUST NOT call this.
//
// CRITICAL: ShutDown the queue then block until every worker goroutine
// has actually exited. Without this barrier, a worker mid-processOne
// can race with the next test's resetResolvedCacheForTest.
func resetRefresherForTest() {
	if r := refresherInstance.Load(); r != nil {
		// Clear cluster_list tier registry so a future singleton starts
		// with no inherited memberships.
		r.clusterListKeys.Range(func(k, _ any) bool {
			r.clusterListKeys.Delete(k)
			return true
		})
		r.queue.ShutDown()
		if r.clusterListQueue != nil {
			r.clusterListQueue.ShutDown()
		}
		// Wait for workers to drain + exit. Capped at 5s as a defensive
		// deadline that should never fire.
		done := make(chan struct{})
		go func() {
			r.workersWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			// Worker stuck — defensive; the test will likely fail
			// downstream because of corruption.
		}
	}
	refresherInstance.Store(nil)
	refresherInit = sync.Once{}
	// 1.12.6 C4 — suppression markers + counters are package state.
	resetRefreshTerminalForTest()
	// Ship #98 — also reset the customer-inflight hook so a previous
	// test's hook does not leak into the next refresher singleton's
	// yield decisions.
	customerInflightHookMu.Lock()
	customerInflightHook = nil
	customerInflightHookMu.Unlock()
}

// ResetRefresherForTest is the exported variant of resetRefresherForTest
// for cross-package tests (e.g. internal/handlers/dispatchers' Ship C
// falsifier). Production code MUST NOT call it.
func ResetRefresherForTest() {
	resetRefresherForTest()
}

// refresherEnqueueTotalForTest is the test-only accessor for the
// refresher singleton's enqueueTotal counter. Used by Ship #91 / 0.30.211
// Lever A tests to assert that lookup() at TTL expiry calls
// EnqueueRefresh(raFullListKey) — i.e. schedules an L1 warm — exactly
// once. TEST-ONLY; lives in refresher.go (not _test.go) so the package
// cache test file in another _test.go can reach it.
func refresherEnqueueTotalForTest() uint64 {
	return refresherSingleton().enqueueTotal.Load()
}

// RefreshFuncForTest returns the RefreshFunc registered for handlerKind,
// or nil when none is registered. Exported for cross-package tests
// (internal/handlers/dispatchers' Ship C falsifier invokes the
// dispatcher-registered handler directly). Production code MUST NOT
// call it.
func RefreshFuncForTest(handlerKind string) RefreshFunc {
	r := refresherSingleton()
	r.handlersMu.RLock()
	defer r.handlersMu.RUnlock()
	return r.handlers[handlerKind]
}

// enqueueRefreshForTest pushes l1Key into the refresher's queue via the
// same enqueue path the dep-tracker refresh hook uses. TEST-ONLY — a
// stable seam so refresher tests (and the Ship C falsifiers) do not
// depend on the queue's internal shape. Production code MUST NOT call
// it.
func enqueueRefreshForTest(l1Key string) {
	refresherSingleton().enqueue(l1Key)
}
