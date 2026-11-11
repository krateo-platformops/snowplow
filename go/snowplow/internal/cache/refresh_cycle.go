// refresh_cycle.go — #1070: the refresh cycle, timed stage by stage. This is
// measurement only: nothing here decides anything.
//
// A viewer waits from a change to the frame that replaces their widget. Before
// this file snowplow timed that end to end (dirty_to_fresh) and timed the
// resolve on its own (resolve_latency_p95), so run 20 had to infer how a
// 2.4–3.2 s wait split up. The four instruments below time each stage in turn:
//
//		insert into a tier ──wait──▶ worker starts ──duration──▶ refresh Put ──put_to_frame──▶ frame written
//
//	  - put_total: every accepted refresh terminal Put (ReplaceIfGenRefresh),
//	    identical or changed bytes, by entry class and tier. A refresh whose
//	    bytes did not change still costs its whole resolve and its Put, but no
//	    frame (#1321: snowplow_refresh_broadcaster.withheld_identical).
//	  - wait_ms: from the key's latest insert into the tier it was dequeued from
//	    until the worker starts the refresh. That is after the customer yield and
//	    the rate floor, the last points where a dequeue can still not run, so the
//	    yield's park counts as waiting. A rate-floor deferral and an error
//	    backoff are not in it: the key re-enters its tier when the delay expires
//	    (floored_total and retried_total count those). A key re-added while it
//	    was being processed is timed from that re-add.
//	  - duration_ms: from the worker's start until the accepted refresh Put.
//	    Refreshes that end with no Put (failed, declined, an incremental list
//	    cell's snapshot) record no sample.
//	  - put_to_frame_ms: from the refresh Put until the first refresh frame for
//	    that key written on each open /refreshes stream armed for it (the #870
//	    Wrote point, after the write and flush succeeded). Only a CHANGED Put
//	    publishes a frame (#1321), so only changed Puts are sampled. A coalesced publish
//	    keeps the Put time of the earliest refresh it absorbed. A frame on a
//	    stream that was never offered the Put records nothing, and so does an
//	    absent stream.
//
// Tiers: viewed, normal and cluster_list are the refresher's queues. direct is
// RunRefreshNow (the D2 stale-dependent refill), which never queues, so it has
// no wait. Every label is a closed set fixed at init, so every series exists
// from the first scrape and a zero is a reading. No key, body or identity
// reaches any series.
//
// Every instant comes from the dirty-window clock (windowNow), the clock the
// viewer's invalidation-to-fresh window already uses, so the stages add up
// against dirty_to_fresh on one clock and an arm can place each instant.
package cache

import (
	"context"
	"expvar"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// Tier labels.
const (
	RefreshTierViewed      = "viewed"
	RefreshTierNormal      = "normal"
	RefreshTierClusterList = "cluster_list"
	RefreshTierDirect      = "direct"
)

// RefreshCycleTiers is every tier a refresh Put, a duration or a frame can be
// labelled with. RefreshQueueTiers are the queued ones, which have a wait.
var (
	RefreshCycleTiers = []string{RefreshTierViewed, RefreshTierNormal, RefreshTierClusterList, RefreshTierDirect}
	RefreshQueueTiers = []string{RefreshTierViewed, RefreshTierNormal, RefreshTierClusterList}
)

// RefreshCycleClasses is the closed class set: the entry classes with a
// registered refresh handler (dispatchers.go), plus "other" for anything else.
var RefreshCycleClasses = []string{"restactions", "widgets", CacheEntryClassApistage, CacheEntryClassWidgetContent, CacheEntryClassRAFullList, refreshClassOther}

const refreshClassOther = "other"

// RefreshPutContents are the put_total content labels.
var RefreshPutContents = []string{"identical", "changed"}

// RefreshCycleBoundsMS are the histogram bounds. They include the goal marks:
// 1000 ms (the north-star), 3000 ms (p95) and 10000 ms (max).
var RefreshCycleBoundsMS = []int64{10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000, 60000}

func (t refreshTier) label() string {
	switch t {
	case tierViewed:
		return RefreshTierViewed
	case tierClusterList:
		return RefreshTierClusterList
	default:
		return RefreshTierNormal
	}
}

func refreshCycleClass(class string) string {
	for _, c := range RefreshCycleClasses[:len(RefreshCycleClasses)-1] {
		if c == class {
			return c
		}
	}
	return refreshClassOther
}

// ── histogram ────────────────────────────────────────────────────────────────

// cycleHist is a fixed-bound millisecond histogram. Lock-free. buckets[i]
// counts samples with bounds[i-1] < ms ≤ bounds[i]; the last slot is +Inf.
type cycleHist struct {
	buckets []atomic.Uint64
	count   atomic.Uint64
	sumMS   atomic.Uint64
	maxMS   atomic.Int64
}

func newCycleHist() *cycleHist {
	return &cycleHist{buckets: make([]atomic.Uint64, len(RefreshCycleBoundsMS)+1)}
}

func (h *cycleHist) record(ms int64) {
	ms = max(ms, 0)
	i := 0
	for i < len(RefreshCycleBoundsMS) && ms > RefreshCycleBoundsMS[i] {
		i++
	}
	h.buckets[i].Add(1)
	h.count.Add(1)
	h.sumMS.Add(uint64(ms))
	for {
		cur := h.maxMS.Load()
		if ms <= cur || h.maxMS.CompareAndSwap(cur, ms) {
			return
		}
	}
}

func (h *cycleHist) reset() {
	for i := range h.buckets {
		h.buckets[i].Store(0)
	}
	h.count.Store(0)
	h.sumMS.Store(0)
	h.maxMS.Store(0)
}

// RefreshCycleHist is one histogram's reading. Buckets are CUMULATIVE, in
// RefreshCycleBoundsMS order, with +Inf last (equal to Count).
type RefreshCycleHist struct {
	Buckets []uint64
	Count   uint64
	SumMS   uint64
	MaxMS   int64
}

func (h *cycleHist) snapshot() RefreshCycleHist {
	d := RefreshCycleHist{Buckets: make([]uint64, len(h.buckets)), Count: h.count.Load(), SumMS: h.sumMS.Load(), MaxMS: h.maxMS.Load()}
	var cum uint64
	for i := range h.buckets {
		cum += h.buckets[i].Load()
		d.Buckets[i] = cum
	}
	return d
}

// expvarValue has the shape of snowplow_apistage_stale_to_push_ms: cumulative
// le_<bound> buckets plus le_inf, count, sum_ms and max_ms.
func (d RefreshCycleHist) expvarValue() map[string]any {
	b := make(map[string]uint64, len(d.Buckets))
	for i, bound := range RefreshCycleBoundsMS {
		b["le_"+strconv.FormatInt(bound, 10)] = d.Buckets[i]
	}
	b["le_inf"] = d.Buckets[len(d.Buckets)-1]
	return map[string]any{"buckets": b, "count": d.Count, "sum_ms": d.SumMS, "max_ms": d.MaxMS}
}

// ── state ────────────────────────────────────────────────────────────────────

// Histogram kinds, as the OTLP recorder receives them.
const (
	refreshCycleWait       = "wait"
	refreshCycleDuration   = "duration"
	refreshCyclePutToFrame = "put_to_frame"
)

type refreshCycleState struct {
	puts       map[[3]string]*atomic.Uint64 // class × tier × content
	wait       map[string]*cycleHist        // queue tier
	duration   map[string]*cycleHist        // tier
	putToFrame map[string]*cycleHist        // tier
}

var refreshCycleStats = func() *refreshCycleState {
	s := &refreshCycleState{
		puts:       map[[3]string]*atomic.Uint64{},
		wait:       map[string]*cycleHist{},
		duration:   map[string]*cycleHist{},
		putToFrame: map[string]*cycleHist{},
	}
	for _, c := range RefreshCycleClasses {
		for _, t := range RefreshCycleTiers {
			for _, k := range RefreshPutContents {
				s.puts[[3]string{c, t, k}] = new(atomic.Uint64)
			}
		}
	}
	for _, t := range RefreshQueueTiers {
		s.wait[t] = newCycleHist()
	}
	for _, t := range RefreshCycleTiers {
		s.duration[t] = newCycleHist()
		s.putToFrame[t] = newCycleHist()
	}
	return s
}()

// refreshCycleRecorder is the OTLP histogram hook (kind is wait, duration or
// put_to_frame). nil = no OTLP (metrics off).
var refreshCycleRecorder atomic.Pointer[func(kind, tier string, ms int64)]

// SetRefreshCycleRecorder installs the OTLP histogram hook. nil clears it.
func SetRefreshCycleRecorder(fn func(kind, tier string, ms int64)) {
	if fn == nil {
		refreshCycleRecorder.Store(nil)
		return
	}
	refreshCycleRecorder.Store(&fn)
}

func recordRefreshCycle(kind string, hists map[string]*cycleHist, tier string, d time.Duration) {
	h := hists[tier]
	if h == nil {
		return
	}
	ms := d.Milliseconds()
	h.record(ms)
	if fn := refreshCycleRecorder.Load(); fn != nil {
		(*fn)(kind, tier, max(ms, 0))
	}
}

// ── the per-refresh sink ─────────────────────────────────────────────────────

// refreshCycle is one refresh's timing, carried on its ctx from the worker's
// start to its terminal Put and the publish after it.
type refreshCycle struct {
	key       string
	tier      string
	startedAt time.Time
	putAt     atomic.Int64 // UnixNano of the accepted refresh Put; 0 = none yet
}

type ctxKeyRefreshCycleType struct{}

var ctxKeyRefreshCycle = ctxKeyRefreshCycleType{}

// withRefreshCycle starts key's refresh timing on ctx at now.
func withRefreshCycle(ctx context.Context, key, tier string, now time.Time) context.Context {
	return context.WithValue(ctx, ctxKeyRefreshCycle, &refreshCycle{key: key, tier: tier, startedAt: now})
}

// refreshCycleFor returns ctx's refresh timing for key (nil for another key:
// a nested Put on the same ctx is not the refresh's terminal write).
func refreshCycleFor(ctx context.Context, key string) *refreshCycle {
	if ctx == nil {
		return nil
	}
	rc, _ := ctx.Value(ctxKeyRefreshCycle).(*refreshCycle)
	if rc == nil || rc.key != key {
		return nil
	}
	return rc
}

// sameBytesPrior is the identical-content test of every refresh Put: key's
// resident prior if it answers as entry does (SameAnswer: its bytes, and since
// #1319 its stored stage outcomes, equal entry's), else nil. It reads the prior
// under c.mu (a pointer read) and compares outside it, like raLayerEqualPrior
// (#406), whose compare it replaces on the refresh Put so a body is compared
// once. The caller confirms under c.mu that the same prior is the one it
// replaces. Entries are never mutated once stored. Raw(), not RawJSON: an
// incremental list cell (#854) stores no RawJSON. Its refresh normally ends at
// its snapshot with no Put; one that unanchored falls through to this Put, and
// its prior's body is built here once, as #604's classifyRebuild already does.
// Every other entry's Raw() is its RawJSON.
func (c *ResolvedCacheStore) sameBytesPrior(key string, entry *ResolvedEntry) *ResolvedEntry {
	c.mu.Lock()
	prior := c.raLayerResidentEntryLocked(key)
	c.mu.Unlock()
	if SameAnswer(prior, entry) { // #1319: the body and the stored stage outcomes
		return prior
	}
	return nil
}

// noteRefreshPut records one accepted refresh terminal Put of key at now:
// identical says whether it replaced a prior with the same bytes. The tier is
// the refresh's own (direct when ctx carries no refresher timing).
func noteRefreshPut(ctx context.Context, key, class string, identical bool, now time.Time) {
	tier := RefreshTierDirect
	rc := refreshCycleFor(ctx, key)
	if rc != nil {
		tier = rc.tier
	}
	content := "changed"
	if identical {
		content = "identical"
	}
	if c := refreshCycleStats.puts[[3]string{refreshCycleClass(class), tier, content}]; c != nil {
		c.Add(1)
	}
	if rc == nil {
		return
	}
	rc.putAt.Store(now.UnixNano())
	recordRefreshCycle(refreshCycleDuration, refreshCycleStats.duration, tier, now.Sub(rc.startedAt))
}

// PublishRefreshed is PublishRefresh for the refresher terminal: it announces
// key exactly as PublishRefresh does, carrying the time of the refresh Put on
// ctx so the frame it causes is timed from that Put (put_to_frame).
//
// #1321 — only when put changed the stored bytes (put_outcome.go): an
// identical refresh Put is counted withheld_identical and publishes nothing.
func PublishRefreshed(ctx context.Context, key string, put PutOutcome) {
	publishPut(key, put, refreshOwedFrom(ctx, key))
}

// refreshOwed is the Put a frame for a key is owed for: when and by which
// tier. Zero = no Put to time from.
type refreshOwed struct {
	at   time.Time
	tier string
}

func (o refreshOwed) earlier(than refreshOwed) bool {
	return !o.at.IsZero() && (than.at.IsZero() || o.at.Before(than.at))
}

func refreshOwedFrom(ctx context.Context, key string) refreshOwed {
	rc := refreshCycleFor(ctx, key)
	if rc == nil {
		return refreshOwed{}
	}
	ns := rc.putAt.Load()
	if ns == 0 {
		return refreshOwed{}
	}
	return refreshOwed{at: time.Unix(0, ns), tier: rc.tier}
}

// noteOwedLocked keeps the earliest Put a frame for key is owed for. Caller
// holds s.pmu and has checked the sub is live.
func (s *refreshSub) noteOwedLocked(key string, o refreshOwed) {
	if o.at.IsZero() {
		return
	}
	if s.owed == nil {
		s.owed = map[string]refreshOwed{}
	}
	if cur, ok := s.owed[key]; !ok || o.earlier(cur) {
		s.owed[key] = o
	}
}

// frameWritten is the #870 write point: a refresh frame for key was written
// on this stream. The first one after a Put records put_to_frame.
func (s *refreshSub) frameWritten(key string) {
	s.pmu.Lock()
	o, ok := s.owed[key]
	if ok {
		delete(s.owed, key)
	}
	s.pmu.Unlock()
	if ok {
		recordRefreshCycle(refreshCyclePutToFrame, refreshCycleStats.putToFrame, o.tier, windowNow().Sub(o.at))
	}
}

// ── enqueue stamps ───────────────────────────────────────────────────────────

// enqueueStamps holds, per key, when it was inserted into one tier's queue and
// not yet dequeued. Bounded by the keys queued in the tier.
type enqueueStamps struct{ at sync.Map }

// stampingQueue is a tier's underlying queue: every insert (direct, or the
// delaying queue's when a deferral expires) stamps the key first, and the
// first stamp since the last dequeue wins, as the queue's own dedup does.
type stampingQueue struct {
	workqueue.TypedInterface[string]
	stamps *enqueueStamps
}

func (q *stampingQueue) Add(item string) {
	q.stamps.at.LoadOrStore(item, windowNow())
	q.TypedInterface.Add(item)
}

// take returns and clears key's insert time (zero when none was stamped: a key
// re-added between its dequeue and this take keeps no stamp, and records no
// wait, rather than a wrong one).
func (s *enqueueStamps) take(key string) time.Time {
	if s == nil {
		return time.Time{}
	}
	if v, ok := s.at.LoadAndDelete(key); ok {
		return v.(time.Time)
	}
	return time.Time{}
}

// tierStamps is the enqueue-stamp set of tier t.
func (r *refresher) tierStamps(t refreshTier) *enqueueStamps {
	switch t {
	case tierViewed:
		return r.viewedStamps
	case tierClusterList:
		return r.clusterListStamps
	default:
		return r.normalStamps
	}
}

// startRefreshCycle is the worker's start of key's refresh (dequeued from t at
// enqueuedAt; zero = unstamped): it records the wait and returns the ctx that
// times the rest of the cycle.
func startRefreshCycle(ctx context.Context, key string, t refreshTier, enqueuedAt time.Time) context.Context {
	now := windowNow()
	tier := t.label()
	if !enqueuedAt.IsZero() {
		recordRefreshCycle(refreshCycleWait, refreshCycleStats.wait, tier, now.Sub(enqueuedAt))
	}
	return withRefreshCycle(ctx, key, tier, now)
}

// ── readings ─────────────────────────────────────────────────────────────────

// RefreshPutCount is one put_total cell.
type RefreshPutCount struct {
	Class, Tier, Content string
	N                    uint64
}

// RefreshCycleSnapshot is the family's reading (expvar + OTLP + arms).
type RefreshCycleSnapshot struct {
	Puts       []RefreshPutCount
	Wait       map[string]RefreshCycleHist
	Duration   map[string]RefreshCycleHist
	PutToFrame map[string]RefreshCycleHist
}

// RefreshCycleStatsSnapshot reads the family. It never builds the refresher.
func RefreshCycleStatsSnapshot() RefreshCycleSnapshot {
	s := refreshCycleStats
	out := RefreshCycleSnapshot{Wait: map[string]RefreshCycleHist{}, Duration: map[string]RefreshCycleHist{}, PutToFrame: map[string]RefreshCycleHist{}}
	for _, c := range RefreshCycleClasses {
		for _, t := range RefreshCycleTiers {
			for _, k := range RefreshPutContents {
				out.Puts = append(out.Puts, RefreshPutCount{c, t, k, s.puts[[3]string{c, t, k}].Load()})
			}
		}
	}
	for t, h := range s.wait {
		out.Wait[t] = h.snapshot()
	}
	for t, h := range s.duration {
		out.Duration[t] = h.snapshot()
	}
	for t, h := range s.putToFrame {
		out.PutToFrame[t] = h.snapshot()
	}
	return out
}

func refreshCycleHistsExpvar(m map[string]RefreshCycleHist) map[string]any {
	out := make(map[string]any, len(m))
	for t, d := range m {
		out[t] = d.expvarValue()
	}
	return out
}

// ResetRefreshCycleForTest zeroes the family. TEST-ONLY.
func ResetRefreshCycleForTest() {
	s := refreshCycleStats
	for _, c := range s.puts {
		c.Store(0)
	}
	for _, m := range []map[string]*cycleHist{s.wait, s.duration, s.putToFrame} {
		for _, h := range m {
			h.reset()
		}
	}
}

// ── expvar ───────────────────────────────────────────────────────────────────

var refreshCycleExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerRefreshCycleExpvar()
}

// registerRefreshCycleExpvar publishes the family. The keys are literals (the
// CFG-1 structural guard reads them) and stay outside snowplow_refresher_,
// which C7 reserves for tag-derived scalars. The bench captures
// snowplow_refresh_broadcaster already; it must add snowplow_refresh_cycle_.
func registerRefreshCycleExpvar() {
	refreshCycleExpvarOnce.Do(func() {
		expvar.Publish("snowplow_refresh_cycle_put_total", expvar.Func(func() any {
			out := map[string]uint64{}
			for _, p := range RefreshCycleStatsSnapshot().Puts {
				out[p.Class+"|"+p.Tier+"|"+p.Content] = p.N
			}
			return out
		}))
		expvar.Publish("snowplow_refresh_cycle_wait_ms", expvar.Func(func() any {
			return refreshCycleHistsExpvar(RefreshCycleStatsSnapshot().Wait)
		}))
		expvar.Publish("snowplow_refresh_cycle_duration_ms", expvar.Func(func() any {
			return refreshCycleHistsExpvar(RefreshCycleStatsSnapshot().Duration)
		}))
		expvar.Publish("snowplow_refresh_broadcaster_put_to_frame_ms", expvar.Func(func() any {
			return refreshCycleHistsExpvar(RefreshCycleStatsSnapshot().PutToFrame)
		}))
	})
}

// RegisterRefreshCycleExpvarForTest forces the publish under a test that set
// CACHE_ENABLED after init ran. Idempotent. TEST-ONLY.
func RegisterRefreshCycleExpvarForTest() { registerRefreshCycleExpvar() }
