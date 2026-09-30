// sliceability_reverify_worker.go — Ship #91 / 0.30.211 Lever C
// async invalidator worker.
//
// PURPOSE. Lever A (TTL on verdict=false, lookup-gated) handles the
// stuck-since-boot case by letting the NEXT /call after T_unverify re-run
// first-sight under its own /call ctx. Lever C handles the OTHER mode: an
// informer event fires against an object an RAFullList cell depends on,
// dirty-marking the L1 key — but pre-Ship-#91 the verdict=false memo entry
// was untouched, so even after the underlying data changed the memo never
// flipped.
//
// THE WORKER. The refresher's dep-event hook (refresher.go) inspects each
// dirty-marked L1 key; for RAFullList-class entries it enqueues the raKey
// onto this worker's bounded channel. The worker drains the channel and
// calls InvalidateSliceabilityForKey(raKey), which deletes every memo
// entry whose raKey matches (subject to the same T_unverify rate-floor
// Lever A uses). The NEXT /call against that (raKey × sliceShape) then
// sees `known=false` and runs first-sight under its own /call ctx,
// recording a fresh verdict.
//
// BOUNDED + DROP-ON-FULL (customer-priority).
//   - Queue length: 16 (envSliceabilityReverifyQueueLen, default 16).
//   - Workers: 1 (envSliceabilityReverifyWorkers, default 1).
//   - Submit policy: non-blocking. If the channel is full the enqueue is
//     DROPPED (counter ticks) and the worker pool is unaffected. A
//     subsequent dep-event for the same raKey will re-enqueue.
//
// This keeps the invalidate path OFF the refresher workqueue — Phase B
// (0.30.185) demonstrated that adding populate work to the refresher
// workqueue amplifies the work-per-cycle by entry-count × refresh-period.
// A separate bounded channel + drop-on-full bounds the dep-event
// amplification by a constant. feedback_refresher_populate_amplification.
//
// LIFECYCLE. Started by StartSliceabilityReverifier(ctx) from main.go
// alongside StartRefresher; the worker goroutine exits cleanly on
// ctx.Done() — the channel stays open for any race with late Submit calls
// (the select in Submit's TrySend uses default-drop, not blocking-send,
// so a late submit never panics on closed-channel).

package cache

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// envSliceabilityReverifyQueueLen — depth of the bounded enqueue channel.
// Default 16; PM brief value.
const envSliceabilityReverifyQueueLen = "SLICEABILITY_REVERIFY_QUEUE_LEN"

const defaultSliceabilityReverifyQueueLen = 16

// envSliceabilityReverifyWorkers — worker goroutine count. Default 1.
const envSliceabilityReverifyWorkers = "SLICEABILITY_REVERIFY_WORKERS"

const defaultSliceabilityReverifyWorkers = 1

// sliceabilityReverifyWorker is the per-process worker singleton.
type sliceabilityReverifyWorker struct {
	queue   chan string
	startMu sync.Mutex
	started bool

	enqueueTotal atomic.Uint64 // accepted enqueues
	dropTotal    atomic.Uint64 // dropped — queue full
	processed    atomic.Uint64 // processed by worker
	invalidated  atomic.Uint64 // SUM of memo entries invalidated across all calls
}

var reverifyWorker = newSliceabilityReverifyWorker()

func newSliceabilityReverifyWorker() *sliceabilityReverifyWorker {
	qlen := positiveIntFromEnv(envSliceabilityReverifyQueueLen, defaultSliceabilityReverifyQueueLen)
	return &sliceabilityReverifyWorker{
		queue: make(chan string, qlen),
	}
}

// StartSliceabilityReverifier launches the async invalidator worker pool.
// Idempotent — repeated calls are no-ops. Workers exit on ctx.Done.
//
// Called from main.go alongside StartRefresher.
//
// CACHE-OFF: when ResolvedCacheEnabled() is false this is a no-op (no
// memo entries are recorded, no dep events fire that target raFullList,
// the channel stays empty for the pod's life). Mirrors the StartRefresher
// short-circuit.
func StartSliceabilityReverifier(ctx context.Context) {
	if !ResolvedCacheEnabled() {
		return
	}
	w := reverifyWorker
	w.startMu.Lock()
	defer w.startMu.Unlock()
	if w.started {
		return
	}
	w.started = true

	workers := positiveIntFromEnv(envSliceabilityReverifyWorkers, defaultSliceabilityReverifyWorkers)

	for i := 0; i < workers; i++ {
		go w.runWorker(ctx, i)
	}

	slog.Info("sliceability_reverifier.started",
		slog.String("subsystem", "cache"),
		slog.Int("workers", workers),
		slog.Int("queue_len", cap(w.queue)),
	)
}

// runWorker drains the queue and invalidates memo entries for each raKey
// received. Exits on ctx.Done().
func (w *sliceabilityReverifyWorker) runWorker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case raKey, ok := <-w.queue:
			if !ok {
				return
			}
			n := InvalidateSliceabilityForKey(raKey)
			w.processed.Add(1)
			if n > 0 {
				w.invalidated.Add(uint64(n))
			}
		}
	}
}

// SubmitSliceabilityInvalidate is the non-blocking enqueue entry point for
// the dep-event hook (refresher.go). Returns true if the raKey was
// accepted onto the queue, false if the queue was full and the enqueue
// was DROPPED. Drops are counted in the worker stats — they are EXPECTED
// behaviour under a noisy informer stream and are NOT an error.
//
// raKey must be the L1 cache key of a CacheEntryClassRAFullList entry.
// An empty raKey is rejected (returns false, no counter tick).
func SubmitSliceabilityInvalidate(raKey string) bool {
	if raKey == "" {
		return false
	}
	w := reverifyWorker
	select {
	case w.queue <- raKey:
		w.enqueueTotal.Add(1)
		return true
	default:
		w.dropTotal.Add(1)
		return false
	}
}

// SliceabilityReverifyStats is a read-only snapshot of the worker
// counters. Used by /debug/vars + the bench validator (F-7 / F-9 / F-12).
//
// READING THE droppedTotal:processedTotal RATIO (#344). A HIGH ratio is
// EXPECTED and HEALTHY, not a defect — observed ~2.7:1 at bench scale. Under a
// noisy informer stream many dep-events fire for the SAME raKey faster than the
// single worker drains the bounded (queueCap, drop-on-full) queue, so most
// submits are SHED at enqueue (droppedTotal++) rather than blocking the
// dep-event hook. Every drop is a REDUNDANT re-enqueue that loses NOTHING:
//   - InvalidateSliceabilityForKey is COALESCING — one processed reverify
//     deletes EVERY memo entry for that raKey, covering the dropped duplicates;
//   - and it is RATE-FLOORED (T_unverify, SLICEABILITY_REVERIFY_RATE_FLOOR_
//     SECONDS, default 60s), so redundant reverifies within the floor are no-ops
//     whether they were dropped at submit OR processed and floored at invalidate.
//
// So a rising drop:process ratio just means MORE coalescing. This is the
// bounded-amplification design — a separate bounded channel keeps invalidate
// OFF the refresher workqueue (feedback_refresher_populate_amplification).
// TestIssue344_* pins both halves: drop-on-full is deterministic/counted, and a
// drop loses no invalidation (coalescing is idempotent).
//
// droppedTotal is therefore a DIAGNOSTIC, not a detector — its non-zero is
// HEALTH (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector), so
// it stays expvar-only (no OTLP alert bridge, #311). The signals that ACTUALLY
// mean trouble are the inverse: processedTotal frozen at 0 while enqueuedTotal
// climbs (the worker is dead), or queueLen pinned at queueCap for a sustained
// window (overload the coalescing cannot keep pace with).
type SliceabilityReverifyStats struct {
	EnqueuedTotal    uint64 `json:"enqueuedTotal"`
	DroppedTotal     uint64 `json:"droppedTotal"`
	ProcessedTotal   uint64 `json:"processedTotal"`
	InvalidatedTotal uint64 `json:"invalidatedTotal"`
	QueueLen         int    `json:"queueLen"`
	QueueCap         int    `json:"queueCap"`
}

// SliceabilityReverifyStatsSnapshot returns the live counters.
func SliceabilityReverifyStatsSnapshot() SliceabilityReverifyStats {
	w := reverifyWorker
	return SliceabilityReverifyStats{
		EnqueuedTotal:    w.enqueueTotal.Load(),
		DroppedTotal:     w.dropTotal.Load(),
		ProcessedTotal:   w.processed.Load(),
		InvalidatedTotal: w.invalidated.Load(),
		QueueLen:         len(w.queue),
		QueueCap:         cap(w.queue),
	}
}

// resetSliceabilityReverifyWorkerForTest reinitialises the singleton with
// a fresh queue/counters/started flag. TEST-ONLY.
func resetSliceabilityReverifyWorkerForTest() {
	reverifyWorker = newSliceabilityReverifyWorker()
}
