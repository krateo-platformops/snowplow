// dirty_window.go — #354 P3: the invalidation→fresh window of an L1 cell.
//
// WHAT IS MEASURED. For a RESIDENT cell, the window opens at the FIRST dirty-mark
// (an informer event fanned out to the key, or a #375 remark) and closes at the
// refresher's accepted re-Put that is NOT itself remarked. Its length is the time
// customers could be served the pre-change body. It is recorded:
//   - as a real OTLP histogram, snowplow_refresher_dirty_to_fresh_ms (the
//     internal/metrics hook installed by SetDirtyToFreshRecorder);
//   - as windowed p95 / max gauges plus a sample counter on the refresher's
//     tagged family (dirty_to_fresh_ms_p95, dirty_to_fresh_ms_max,
//     dirty_to_fresh_samples).
//
// A window that ends without a fresh body is counted by reason on the same family
// (dirty_ended_unfresh_<reason>, a CLOSED set):
//   - remarked — the re-Put was accepted but #375 remarked it (a dep moved
//     mid-resolve). The window STAYS OPEN from the original mark: that Put does not
//     sample, the next clean Put does;
//   - evicted — the cell left the store while dirty (DELETE, TTL/LRU/max-age,
//     #444 no-representative, a refused ReplaceIfGen);
//   - declined — the refresh returned without a Put (stage error, external, UAF,
//     sensitive, empty full, unsupported kind, suppressed, no handler);
//   - dropped — the poison-pill bound gave up on the key.
//
// A retried error and a rate-floor deferral keep the window open: both are part
// of the time a customer waits for the fresh body.
//
// THE STAMP. firstDirtyAt (refresher.dirty) maps l1Key → *dirtyStamp, an
// IMMUTABLE value replaced by CompareAndSwap. A mark LoadOrStores, so the first
// mark wins. The dequeue does NOT delete it: it flips it to inflight, so a
// customer hit DURING the re-resolve still counts as a stale serve, and a mark
// that lands during the re-resolve is kept as `next` (a new window, if this
// dequeue ends fresh). The map holds only resident keys: a mark on a
// non-resident key never stores (the #383 drop branch), and every store removal
// (removeElementLocked, deleteForDep) deletes the key's stamp, so the map can
// never outgrow the resident key set.
//
// THE OUTCOME SEAM. The accept/refuse/remark outcome reaches processNext without
// widening RefreshFunc: processNext installs a per-dequeue refreshOutcome on the
// re-resolve ctx (WithRefreshOutcome, same idiom as the #375 dep-gen sink). The
// refresher's terminal write reports accepted/refused through NoteRefreshWrite
// (resolve_populate.go, at the ReplaceIfGen call site); remarkIfDepsMoved and the
// #375 C3 re-check report remarked. A sink only listens for the dequeued key, so
// a nested content-cell Put on the same ctx never reads as the cell's outcome.
// "Declined" is derived: a nil error with no Put outcome and the cell still
// resident.
//
// CUSTOMER HOT PATH. noteServeWhileDirty runs inside ResolvedCacheStore.Get on a
// HIT, after c.mu is released: one sync.Map Load (and nothing else when the cell
// is clean). Get is the one funnel every hit_total-counted hit goes through, so
// stale_served_total and hit_total share a denominator by construction.
//
// No identity, key or body reaches any series: the series are scalars.

package cache

import (
	"context"
	"math"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// dirtyStamp is one key's open window. IMMUTABLE once published.
type dirtyStamp struct {
	t0       time.Time // the first mark of the window (monotonic)
	inflight bool      // a dequeue is re-resolving the key now
	next     time.Time // a mark that landed during the in-flight re-resolve (zero = none)
}

// unfreshReason indexes the closed set of unfresh outcomes.
type unfreshReason int

const (
	unfreshRemarked unfreshReason = iota
	unfreshEvicted
	unfreshDeclined
	unfreshDropped
	unfreshReasons
)

// dirtyWindowStats are the refresher-side window counters and estimators.
type dirtyWindowStats struct {
	samples atomic.Uint64
	unfresh [unfreshReasons]atomic.Uint64
	d2f     *windowedQuantile
}

func newDirtyWindowStats() *dirtyWindowStats {
	return &dirtyWindowStats{d2f: newWindowedQuantile(metricExportInterval(), true)}
}

// dirtyToFreshRecorder is the OTLP histogram hook (internal/metrics installs it
// at registration; nil = no histogram, e.g. metrics off). A synchronous
// instrument, so the refresher records into it at the sample.
var dirtyToFreshRecorder atomic.Pointer[func(ms int64)]

// SetDirtyToFreshRecorder installs the hook the refresher records every
// dirty→fresh sample (ms) into. nil clears it.
func SetDirtyToFreshRecorder(fn func(ms int64)) {
	if fn == nil {
		dirtyToFreshRecorder.Store(nil)
		return
	}
	dirtyToFreshRecorder.Store(&fn)
}

// DirtyToFreshBoundsMS are the explicit histogram bounds: they bracket the 1 s
// north-star and the 10 s AC-98.12 convergence SLA.
var DirtyToFreshBoundsMS = []float64{50, 100, 250, 500, 750, 1000, 2000, 5000, 10000, 30000, 60000}

// markDirty opens key's window at now unless one is open (first mark wins). On
// an in-flight stamp the mark is kept as `next`: a new window if the in-flight
// dequeue ends fresh. Called from onDirtyMark's RESIDENT branch only.
func (r *refresher) markDirty(key string, now time.Time) {
	if key == "" {
		return
	}
	for {
		cur, loaded := r.dirty.LoadOrStore(key, &dirtyStamp{t0: now})
		if !loaded {
			return
		}
		s := cur.(*dirtyStamp)
		if !s.inflight || !s.next.IsZero() {
			return
		}
		if r.dirty.CompareAndSwap(key, cur, &dirtyStamp{t0: s.t0, inflight: true, next: now}) {
			return
		}
	}
}

// beginWindow flips key's open window to in-flight at dequeue. open is false when
// the key has no window (a proactive / Lever-A enqueue, not a mark).
func (r *refresher) beginWindow(key string) (t0 time.Time, open bool) {
	for {
		cur, ok := r.dirty.Load(key)
		if !ok {
			return time.Time{}, false
		}
		s := cur.(*dirtyStamp)
		if r.dirty.CompareAndSwap(key, cur, &dirtyStamp{t0: s.t0, inflight: true, next: s.next}) {
			return s.t0, true
		}
	}
}

// settleWindow closes (keepOpen=false) or re-opens (keepOpen=true, the earliest
// mark kept) key's in-flight window. gone reports that the stamp was removed by
// an eviction during the dequeue.
func (r *refresher) settleWindow(key string, t0 time.Time, keepOpen bool) (gone bool) {
	for {
		cur, ok := r.dirty.Load(key)
		if !ok {
			return true
		}
		s := cur.(*dirtyStamp)
		if !s.inflight || !s.t0.Equal(t0) {
			// Not this dequeue's stamp (a re-opened window after an eviction and a
			// fresh mark): leave it alone.
			return true
		}
		var next any
		switch {
		case keepOpen:
			next = &dirtyStamp{t0: t0}
		case !s.next.IsZero():
			next = &dirtyStamp{t0: s.next}
		default:
			if r.dirty.CompareAndDelete(key, cur) {
				return false
			}
			continue
		}
		if r.dirty.CompareAndSwap(key, cur, next) {
			return false
		}
	}
}

// windowFresh closes the window with a sample of now − t0.
func (r *refresher) windowFresh(key string, t0 time.Time) {
	r.settleWindow(key, t0, false)
	ms := time.Since(t0).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	r.dirtyStats.samples.Add(1)
	r.dirtyStats.d2f.observe(float64(ms))
	if fn := dirtyToFreshRecorder.Load(); fn != nil {
		(*fn)(ms)
	}
}

// windowUnfresh ends (or, for remarked, keeps open) the window and counts the
// reason; a window an eviction removed mid-dequeue counts as evicted.
func (r *refresher) windowUnfresh(key string, t0 time.Time, reason unfreshReason) {
	if r.settleWindow(key, t0, reason == unfreshRemarked) {
		reason = unfreshEvicted
	}
	r.dirtyStats.unfresh[reason].Add(1)
}

// windowContinue keeps the window open from t0 (a retried error).
func (r *refresher) windowContinue(key string, t0 time.Time) {
	if r.settleWindow(key, t0, true) {
		r.dirtyStats.unfresh[unfreshEvicted].Add(1)
	}
}

// evictDirtyWindow is called by every store removal (under c.mu: sync.Map ops
// only, no store re-entry). A window that is not in flight ends here as evicted;
// an in-flight one is ended by its dequeue's outcome (the stamp is gone).
func evictDirtyWindow(key string) {
	r := refresherPeek()
	if r == nil {
		return
	}
	cur, ok := r.dirty.LoadAndDelete(key)
	if !ok {
		return
	}
	if s := cur.(*dirtyStamp); !s.inflight {
		r.dirtyStats.unfresh[unfreshEvicted].Add(1)
	}
}

// dirtySince reports key's open window start (the stale-serve probe).
func dirtySince(key string) (time.Time, bool) {
	r := refresherPeek()
	if r == nil {
		return time.Time{}, false
	}
	v, ok := r.dirty.Load(key)
	if !ok {
		return time.Time{}, false
	}
	return v.(*dirtyStamp).t0, true
}

// DirtyWindowsOpenForTest counts the open windows (F6d). TEST-ONLY.
func DirtyWindowsOpenForTest() int {
	r := refresherPeek()
	if r == nil {
		return 0
	}
	n := 0
	r.dirty.Range(func(_, _ any) bool { n++; return true })
	return n
}

// --- per-dequeue outcome sink -------------------------------------------------

type refreshOutcome struct {
	key      string
	accepted atomic.Bool
	refused  atomic.Bool
	remarked atomic.Bool
}

type ctxKeyRefreshOutcomeType struct{}

var ctxKeyRefreshOutcome = ctxKeyRefreshOutcomeType{}

// withRefreshOutcome installs the dequeue's outcome sink for key on ctx.
func withRefreshOutcome(ctx context.Context, key string) (context.Context, *refreshOutcome) {
	o := &refreshOutcome{key: key}
	return context.WithValue(ctx, ctxKeyRefreshOutcome, o), o
}

func refreshOutcomeFor(ctx context.Context, key string) *refreshOutcome {
	if ctx == nil {
		return nil
	}
	o, _ := ctx.Value(ctxKeyRefreshOutcome).(*refreshOutcome)
	if o == nil || o.key != key {
		return nil
	}
	return o
}

// NoteRefreshWrite reports the refresher's terminal write for key: stored is the
// gen-guarded replace's result. A no-op off the refresher (no sink on ctx) and for
// any key but the dequeued one (a nested content-cell Put on the same ctx).
func NoteRefreshWrite(ctx context.Context, key string, stored bool) {
	o := refreshOutcomeFor(ctx, key)
	if o == nil {
		return
	}
	if stored {
		o.accepted.Store(true)
	} else {
		o.refused.Store(true)
	}
}

// noteRefreshRemark reports a #375 remark of key on the dequeue's ctx.
func noteRefreshRemark(ctx context.Context, key string) {
	if o := refreshOutcomeFor(ctx, key); o != nil {
		o.remarked.Store(true)
	}
}

// --- windowed estimator -------------------------------------------------------

// metricExportInterval is the OTLP export interval the windowed gauges align
// with: the SDK's own OTEL_METRIC_EXPORT_INTERVAL (ms; default 60 s). Not a new
// knob — the window is the period the gauge is sampled at.
func metricExportInterval() time.Duration {
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return 60 * time.Second
}

// windowedQuantile is a p95 + max over roughly the last export interval W. Two
// estimators run staggered by W/2, each reset every W and fed every sample; the
// read reports the OLDER one, so the gauge always covers between W/2 and W of
// history and a sample is visible immediately. P² for the p95 (the
// recordResolveLatency prior art), exact max. O(1) per sample, under one mutex.
type windowedQuantile struct {
	mu     sync.Mutex
	w      time.Duration
	base   time.Time
	withQ  bool
	halves [2]windowHalf
}

type windowHalf struct {
	epoch int64
	q     *p2Quantile
	max   float64
	n     int
}

func newWindowedQuantile(w time.Duration, withQuantile bool) *windowedQuantile {
	if w <= 0 {
		w = 60 * time.Second
	}
	wq := &windowedQuantile{w: w, base: time.Now(), withQ: withQuantile}
	for i := range wq.halves {
		wq.halves[i].epoch = math.MinInt64
	}
	return wq
}

// rotateLocked resets each half whose epoch has passed.
func (wq *windowedQuantile) rotateLocked(now time.Time) {
	for i := range wq.halves {
		off := time.Duration(i) * wq.w / 2
		e := int64(now.Sub(wq.base)+off) / int64(wq.w)
		h := &wq.halves[i]
		if h.epoch != e {
			*h = windowHalf{epoch: e}
			if wq.withQ {
				h.q = newP2Quantile(0.95)
			}
		}
	}
}

func (wq *windowedQuantile) observe(v float64) {
	if wq == nil {
		return
	}
	wq.mu.Lock()
	wq.rotateLocked(time.Now())
	for i := range wq.halves {
		h := &wq.halves[i]
		if h.q != nil {
			h.q.Observe(v)
		}
		if h.n == 0 || v > h.max {
			h.max = v
		}
		h.n++
	}
	wq.mu.Unlock()
}

// read returns (p95, max) of the older half (the longer coverage).
func (wq *windowedQuantile) read() (p95, max float64) {
	if wq == nil {
		return 0, 0
	}
	wq.mu.Lock()
	defer wq.mu.Unlock()
	now := time.Now()
	wq.rotateLocked(now)
	older := 0
	start := func(i int) time.Duration {
		off := time.Duration(i) * wq.w / 2
		return time.Duration(wq.halves[i].epoch)*wq.w - off
	}
	if start(1) < start(0) {
		older = 1
	}
	h := wq.halves[older]
	if h.n == 0 {
		return 0, 0
	}
	if h.q != nil {
		p95 = h.q.Value()
	}
	return p95, h.max
}
