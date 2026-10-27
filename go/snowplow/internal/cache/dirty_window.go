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
// #547 — KNOWN-STALE. A declined or dropped window (and the suppressed skip,
// which is a decline) is counted as before, but its stamp is NOT deleted: it is
// converted to {t0, stale: <reason>} and stays until an accepted re-Put ends
// it (windowFresh, a remarked accepted re-Put, an accepted demand revalidation
// or any other carrier's accepted Put — #884: known-stale means the last
// refresh FAILED; a mark newer than that Put's resolve leaves an ordinary open
// window) or the cell leaves the store (evictDirtyWindow). Before #547 the stamp was erased there
// (CompareAndDelete), so the cell looked exactly like a fresh one: every reload
// was a hit on a body the system knew was stale, nothing re-validated it short
// of a later informer event or the TTL, and stale_served_total went blind at the
// very moment the user started seeing stale data. The serve path reads the
// reason (known_stale.go): an identity-bound cell is served (zero cold
// navigation) and revalidated on demand under the requester; an identity-free
// substrate cell is read through. Only evicted ends a window without a fresh
// body, because then there is no body left to be stale.
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
// TWO CONSERVATIVE BIASES (both over-report, never under-report):
//   - only the REFRESHER's terminal write closes a window. A non-refresher
//     replace of a dirty cell (keep-warm sweep, seed, gvr-discovered, #258
//     reseed) leaves it open, so a window can outlast the moment a fresh body
//     actually landed;
//   - stale_served counts hits on a dirty-marked cell even when its refresh then
//     proves the content unchanged (an unchanged refresh is accepted, and FRESH —
//     F6u). It means "served while potentially stale".
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
	// stale (#547) is non-zero once a refresh of this window ENDED without an
	// accepted re-Put: the body is KNOWN-STALE since t0. It survives later marks
	// and later in-flight dequeues; an accepted Put (any carrier, #884) or an
	// eviction clears it.
	stale knownStaleReason
	// seq (#547) is the dep-event sequence at the LATEST mark of this window. An
	// accepted Put whose resolve began at or after it (its #375 sink startSeq)
	// read the post-mark state, so it closes the window whatever carrier wrote
	// it; one that began before it ends only the known-stale flag and leaves an
	// ordinary open window (#884, endKnownStaleOnPut).
	seq uint64
	// began (#870) is when the in-flight dequeue began (set by beginWindow).
	// The store-swap push ledger applies a refresher close only to a dequeue
	// that began after the ledger opened (store_swap_ledger.go, Q3): one that
	// began earlier may have read the pre-swap copy.
	began time.Time
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

// dirtyWindowClock (#930) is the clock of the invalidation→fresh window: a
// mark's t0 and every age measured from it (the dirty→fresh sample, the stale-
// served age) are read from it, so a window's length is the difference of two
// readings of ONE clock. nil reads time.Now, which is all production ever uses.
// A test installs a virtual clock (SetDirtyWindowClockForTest) to place the
// mark, a remark and the clean Put at exact instants, so an arm about WHICH mark
// starts the window asserts the exact length instead of a wall-clock bound.
var dirtyWindowClock atomic.Pointer[func() time.Time]

// windowNow reads dirtyWindowClock.
func windowNow() time.Time {
	if f := dirtyWindowClock.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

// DirtyWindowNow reads the dirty-window clock. Every age measured from a
// window's t0 (KnownStale / KnownStaleMark) outside this package — the D2
// stale-serve age — is DirtyWindowNow() minus that t0, never another clock.
func DirtyWindowNow() time.Time { return windowNow() }

// SetDirtyWindowClockForTest installs now as the dirty-window clock and returns
// the restore. TEST-ONLY: production never calls it, so the window reads
// time.Now.
func SetDirtyWindowClockForTest(now func() time.Time) (restore func()) {
	prev := dirtyWindowClock.Load()
	if now == nil {
		dirtyWindowClock.Store(nil)
	} else {
		dirtyWindowClock.Store(&now)
	}
	return func() { dirtyWindowClock.Store(prev) }
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
	seq := depEventSeq.Load()
	for {
		cur, loaded := r.dirty.LoadOrStore(key, &dirtyStamp{t0: now, seq: seq})
		if !loaded {
			return
		}
		s := cur.(*dirtyStamp)
		// First mark wins t0; #547 — the LATEST mark's seq is kept, so a Put whose
		// resolve predates a later mark never closes the window.
		next := &dirtyStamp{t0: s.t0, inflight: s.inflight, next: s.next, stale: s.stale, seq: seq, began: s.began}
		if s.inflight && s.next.IsZero() {
			next.next = now
		}
		if s.seq >= seq && (!s.inflight || !s.next.IsZero()) {
			return
		}
		if r.dirty.CompareAndSwap(key, cur, next) {
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
		if r.dirty.CompareAndSwap(key, cur, &dirtyStamp{t0: s.t0, inflight: true, next: s.next, stale: s.stale, seq: s.seq, began: time.Now()}) {
			return s.t0, true
		}
	}
}

// windowKeep says whether a dequeue's settle keeps its window open, and what
// happens to a KNOWN-STALE flag when it does.
type windowKeep uint8

const (
	// windowEnd closes the window (or, with a reason, marks it known-stale).
	windowEnd windowKeep = iota
	// windowKeepAccepted re-opens the window after an ACCEPTED re-Put that was
	// remarked (a dep moved during the resolve). #884 (owner ruling F5): the
	// refresh succeeded, so a known-stale flag ENDS; the window stays an
	// ordinary open one from the earliest mark, replaced by the remark's refresh.
	windowKeepAccepted
	// windowKeepFailed re-opens the window after a retried error: the refresh
	// FAILED, so a known-stale flag is KEPT (#547).
	windowKeepFailed
)

// settleWindow closes (windowEnd, stale=none) or re-opens (windowKeepAccepted /
// windowKeepFailed, the earliest mark kept) key's in-flight window, or — #547 —
// marks it KNOWN-STALE (windowEnd, stale != none: the refresh ended without an
// accepted re-Put, the earliest mark kept). gone reports that the stamp was
// removed by an eviction during the dequeue.
func (r *refresher) settleWindow(key string, t0 time.Time, keep windowKeep, stale knownStaleReason) (gone bool) {
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
		case keep == windowKeepAccepted:
			// #884 — known-stale means the last refresh FAILED; this one was
			// accepted. Before #884 the flag was carried over (stale: s.stale), so
			// under steady churn a remarked re-Put never ended it.
			next = &dirtyStamp{t0: t0, seq: s.seq}
		case keep == windowKeepFailed:
			next = &dirtyStamp{t0: t0, stale: s.stale, seq: s.seq}
		case stale != knownStaleNone:
			// #547 — the mark outlives the failed refresh. A mark that landed
			// during it (s.next) is already re-queued by the workqueue, and the
			// body has been stale since the EARLIER t0, so t0 is kept.
			next = &dirtyStamp{t0: t0, stale: stale, seq: s.seq}
		case !s.next.IsZero():
			next = &dirtyStamp{t0: s.next, seq: s.seq}
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
	began := r.inflightBegan(key, t0)
	r.settleWindow(key, t0, windowEnd, knownStaleNone)
	r.sampleFresh(t0)
	ledgerRefreshed(key, began) // #870 — the push ledger's refreshed state
}

// inflightBegan is when key's in-flight dequeue for the window t0 began (the
// zero time when it is not this dequeue's stamp).
func (r *refresher) inflightBegan(key string, t0 time.Time) time.Time {
	if cur, ok := r.dirty.Load(key); ok {
		if s := cur.(*dirtyStamp); s.inflight && s.t0.Equal(t0) {
			return s.began
		}
	}
	return time.Time{}
}

// sampleFresh records one dirty→fresh sample of now − t0.
func (r *refresher) sampleFresh(t0 time.Time) {
	ms := windowNow().Sub(t0).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	r.dirtyStats.samples.Add(1)
	r.dirtyStats.d2f.observe(float64(ms))
	if fn := dirtyToFreshRecorder.Load(); fn != nil {
		(*fn)(ms)
	}
}

// windowUnfresh ends (evicted), keeps open (remarked) or — #547 — marks
// KNOWN-STALE (declined / dropped) the window and counts the reason; a window an
// eviction removed mid-dequeue counts as evicted. ks names the known-stale
// reason for declined / dropped (knownStaleNone derives it from reason).
func (r *refresher) windowUnfresh(key string, t0 time.Time, reason unfreshReason, ks knownStaleReason) {
	if ks == knownStaleNone {
		switch reason {
		case unfreshDeclined:
			ks = knownStaleDeclined
		case unfreshDropped:
			ks = knownStaleDropped
		}
	}
	if reason == unfreshRemarked || reason == unfreshEvicted {
		ks = knownStaleNone
	}
	keep := windowEnd
	if reason == unfreshRemarked {
		keep = windowKeepAccepted // a remark follows an ACCEPTED re-Put (settleDequeue)
	}
	began := r.inflightBegan(key, t0)
	if r.settleWindow(key, t0, keep, ks) {
		reason = unfreshEvicted
	}
	r.dirtyStats.unfresh[reason].Add(1)
	// #870 — the push ledger: a dequeue that ended known-stale closes its
	// entries known_stale (an eviction closes them through evictDirtyWindow).
	if reason != unfreshEvicted && reason != unfreshRemarked && ks != knownStaleNone {
		ledgerKnownStale(key, began)
	}
}

// windowContinue keeps the window open from t0 (a retried error: a FAILED
// refresh, so a known-stale flag is kept).
func (r *refresher) windowContinue(key string, t0 time.Time) {
	if r.settleWindow(key, t0, windowKeepFailed, knownStaleNone) {
		r.dirtyStats.unfresh[unfreshEvicted].Add(1)
	}
}

// evictDirtyWindow is called by every store removal (under c.mu: sync.Map ops
// only, no store re-entry). A window that is not in flight ends here as evicted;
// an in-flight one is ended by its dequeue's outcome (the stamp is gone).
func evictDirtyWindow(key string) {
	ledgerEvicted(key) // #870 — the key left the store: its push-ledger entries close evicted
	r := refresherPeek()
	if r == nil {
		return
	}
	noteStaleRetryCellChanged(key) // #740 — a chain on a removed cell ends
	cur, ok := r.dirty.LoadAndDelete(key)
	if !ok {
		return
	}
	// #547 — a KNOWN-STALE stamp's window was already counted when its refresh
	// ended (declined / dropped); its eviction ends the known-stale state, not a
	// second window.
	if s := cur.(*dirtyStamp); !s.inflight && s.stale == knownStaleNone {
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

// dirtyStampFor returns key's open window stamp (the stale-serve probe, #547:
// with its known-stale reason).
func dirtyStampFor(key string) (*dirtyStamp, bool) {
	r := refresherPeek()
	if r == nil {
		return nil, false
	}
	v, ok := r.dirty.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*dirtyStamp), true
}

// InputsMovedSincePut is THE single predicate for "an input of this cell moved
// and no accepted re-Put has carried it in yet" (#643 PR-C: a resumed
// gvr-discovered pass never skips such a cell). It is true in BOTH states that
// mean it:
//   - an OPEN invalidation→fresh window (a dep event dirty-marked the cell and
//     its refresh has not ended), and
//   - KNOWN-STALE (#547): the refresh ended WITHOUT a Put — declined (a stage
//     error, "declining to overwrite good entry") or dropped — so the stored
//     pointer is unchanged while its inputs moved.
//
// Both live on the same stamp, which only an accepted re-Put (or the cell's
// removal) clears. Read-only.
func InputsMovedSincePut(key string) bool {
	// The stamp exists exactly while a window is open (stale == none) OR the
	// cell is known-stale (stale == a reason); only an accepted re-Put or the
	// cell's removal clears it. So "a stamp exists" is the whole predicate.
	s, ok := dirtyStampFor(key)
	return ok && s != nil
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
		// #870 — the store-swap push ledger's "refreshed" is the accepted,
		// un-remarked re-Put: the same event windowFresh records, taken HERE,
		// at the write, because the publish that follows (PublishRefresh, after
		// this call) can reach a subscriber and be WRITTEN before this dequeue
		// returns to settle its window — and a frame written for an entry still
		// marked stale would close nothing, leaving it open forever.
		if !o.remarked.Load() {
			ledgerRefreshed(key, refreshDequeueBegan(key))
		}
	} else {
		o.refused.Store(true)
	}
}

// refreshDequeueBegan is when key's in-flight dequeue began (zero when none).
func refreshDequeueBegan(key string) time.Time {
	r := refresherPeek()
	if r == nil {
		return time.Time{}
	}
	if cur, ok := r.dirty.Load(key); ok {
		if s := cur.(*dirtyStamp); s.inflight {
			return s.began
		}
	}
	return time.Time{}
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
