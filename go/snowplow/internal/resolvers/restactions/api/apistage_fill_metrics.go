// apistage_fill_metrics.go — #710 PR-0: observe the apistage content FILL.
//
// #710: 1000 users arriving together at 50K objects drove transient resolve
// memory from 0.6 to 6.35 GB in 16 s and OOMKilled the pod. The heap profile
// puts it in apistageContentServe's MISS branch — dispatchViaInformer's
// marshalAsListRaw envelope plus parseListEnvelope's full decode — and NOT in
// the hit path. A heap profile carries no labels and the content key is
// identity-free, so the run could not say WHICH list, GVR or RESTAction drove
// it, nor WHY readers were filling. This file makes the next bench say so.
// Instrumentation only: nothing here changes what is fetched, stored or served.
//
// A FILL is one reader entering the MISS branch (dispatch + parse + PutIfGen).
// Its reason is which of readContent's three cases sent it there, a closed,
// code-defined set:
//
//   - refresh_forced — a refresher whole-RA re-resolve whose trigger set holds
//     this cell's GVR (forceContentMiss, R1 content-shield);
//   - known_stale    — #547 read-through of a known-stale identity-free cell;
//   - absent         — a plain miss: absent, evicted or expired.
//
// Surfaces (expvar, under the reserved-free prefix
// snowplow_apistage_content_fill; the bench harness records the prefix):
//
//   - _total{reason}           fills started;
//   - _duplicate_total{reason} fills started while another fill of the SAME
//     content key was already in flight — the herd, measured directly. With
//     the duplicates, total/(total-duplicate) is parses per in-flight group;
//   - _bytes_total{reason}     raw envelope bytes the fills produced;
//   - _joined_total{reason}    (#710 PR-1) readers that JOINED an in-flight
//     fill of their content key instead of filling (apistage_fill_flight.go);
//   - _join_refused_total{reason} readers that found a flight but did not join
//     it because the cell's coordinate moved after the flight started (the
//     epoch-at-entry rule): they lead the next flight, so they are also fills
//     and duplicates;
//   - _join_waiting            gauge: joiners waiting on a flight right now;
//   - _refresh_reused_total    (#765) refresher reads whose trigger set held
//     the cell's GVR but whose stored cell's content stamp covered every
//     trigger: served from the cell, NO fill (each one a re-fill avoided);
//   - _refresh_coalesced_total (#765) background refresher readers that joined
//     their key's PENDING flight instead of starting a concurrent fill of the
//     same list (each one a concurrent re-fill avoided);
//   - _not_servable_total      fills whose dispatch was not pivot-servable
//     (no bytes; the caller falls through);
//   - _inflight, _inflight_bytes            gauges, right now;
//   - _inflight_peak, _inflight_bytes_peak  high-water marks since boot, so a
//     /debug/vars poll cannot miss a burst shorter than its interval;
//   - _duration_ms  histogram of one fill, from the dispatch's start to the
//     end of its parse and Put, over ContentFillDurationBoundsMS (cumulative
//     le_* buckets, count, sum_ms, max_ms). PR-2's predicted admission wait is
//     built on it;
//   - _by_gvr  map gvr → {fills, duplicates, bytes, items_total, max_bytes,
//     max_items, duration_ms_ewma, duration_ms_max};
//   - _by_ra   map RESTAction ns/name → {fills, duplicates, bytes, max_items}:
//     "which RA" answered from the same /debug/vars sample the bench records
//     (the chart ships LOG_LEVEL=warn, so a log line is not enough). Bounded by
//     the RESTAction definitions; a fill no Resolve attributed is keyed
//     contentFillUnattributedRA.
//
// OTLP (hand-wired in metrics.go, no expvar bridge): the closed-label series
// only — _total, _duplicate_total and _bytes_total by reason,
// _not_servable_total, the two in-flight gauges and the duration histogram.
// _by_gvr and _by_ra stay expvar-only: their keys are data-derived (CRDs,
// RESTActions), so as OTLP attributes they would be unbounded (the #311
// bounded-attribute discipline).
//
// The in-flight bytes of a fill are held from the dispatch's return to the end
// of apistageContentServe, so the window covers the envelope, its parse, the
// Put and the serve-time gate — the transient the OOM was made of. They are
// the RAW envelope bytes; the decoded trees are several times larger, and no
// per-fill measure of them is cheap under concurrency, so items is the proxy.
//
// CPU attribution: the dispatch and the parse run under pprof labels
// apistage_fill_{gvr,reason,ra}. Heap profiles do not record labels; a CPU or
// goroutine profile does.
//
// One Debug log line per fill, rate-limited to contentFillLogEvery with a
// suppressed count: gvr, namespace, RESTAction, stage, reason, bytes, items,
// duration and duplicate. It is written to slog.Default(), never the request
// logger (which carries the requester), and never names the object: a
// GET-by-name may be a Secret. Nothing here names a user, a group, a
// certificate or a cache body.
//
// CFG-1: gated on cache.Disabled(). With the cache off nothing is served from
// the content layer, so the family is structurally zero and is not published.
package api

import (
	"context"
	"expvar"
	"log/slog"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// contentFillReason is why a reader entered the content fill.
type contentFillReason uint8

const (
	contentFillAbsent contentFillReason = iota
	contentFillKnownStale
	contentFillRefreshForced
	contentFillReasons
)

// contentFillReasonNames is the closed `reason` key set, in enum order.
var contentFillReasonNames = [contentFillReasons]string{"absent", "known_stale", "refresh_forced"}

func (r contentFillReason) String() string {
	if r >= contentFillReasons {
		return contentFillReasonNames[contentFillAbsent]
	}
	return contentFillReasonNames[r]
}

// ContentFillReasons returns the closed `reason` set, in a fixed order.
func ContentFillReasons() []string { return contentFillReasonNames[:] }

// contentFillLogEvery bounds the per-fill log line to one per interval.
const contentFillLogEvery = time.Second

// contentFillDurationEWMAAlpha weights a new fill duration in a GVR's EWMA.
const contentFillDurationEWMAAlpha = 0.2

// contentFillUnattributedRA keys _by_ra for a fill no Resolve attributed (a
// direct content serve with no owning RESTAction on its ctx).
const contentFillUnattributedRA = "(unattributed)"

// ContentFillDurationBoundsMS are the fill-duration histogram's upper bounds
// (ms), shared by the expvar buckets and the OTLP histogram.
var ContentFillDurationBoundsMS = []int64{10, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

var (
	contentFillTotal       [contentFillReasons]atomic.Uint64
	contentFillDuplicate   [contentFillReasons]atomic.Uint64
	contentFillBytes       [contentFillReasons]atomic.Uint64
	contentFillNotServable atomic.Uint64
	contentFillJoined      [contentFillReasons]atomic.Uint64
	contentFillJoinRefused [contentFillReasons]atomic.Uint64
	contentFillJoinWaiting atomic.Int64
	// #765 — re-fills the content stamp avoided.
	contentFillRefreshReused    atomic.Uint64
	contentFillRefreshCoalesced atomic.Uint64

	contentFillInflight          atomic.Int64
	contentFillInflightBytes     atomic.Int64
	contentFillInflightPeak      atomic.Int64
	contentFillInflightBytesPeak atomic.Int64

	// contentFillMu guards the per-key in-flight count (the duplicate
	// detector) and the per-GVR and per-RA tables. The key map holds only
	// keys with a fill in flight, so it is bounded by the concurrent fills.
	contentFillMu       sync.Mutex
	contentFillByKey    = map[string]int{}
	contentFillByGVRMap = map[string]*ContentFillGVRStats{}
	contentFillByRAMap  = map[string]*ContentFillRAStats{}

	// contentFillDurHist is one fill's duration; contentFillAdmWaitHist (#710
	// PR-2a) one flight's wait for memory admission, by the class it admitted at.
	contentFillDurHist     = newMSHistogram()
	contentFillAdmWaitHist = [fillAdmissionClasses]*msHistogram{newMSHistogram(), newMSHistogram(), newMSHistogram()}
	// contentFillAdmWaitRecorder is the admission-wait OTLP hook (class-labelled).
	contentFillAdmWaitRecorder atomic.Pointer[func(ms int64, class string)]

	// #710 PR-2a — the content-fill admission gate (apistage_fill_admission.go).
	contentFillAdmissionQueued         [fillAdmissionClasses]atomic.Int64
	contentFillAdmissionTimeout        [fillAdmissionClasses]atomic.Uint64
	contentFillAdmissionInflightWeight atomic.Int64
	// contentFillAdmissionInflightWeightPeak is the admitted weight's
	// high-water mark since boot: it must never exceed the ceiling except by
	// the inFlight == 0 escape of a single fill.
	contentFillAdmissionInflightWeightPeak atomic.Int64
	// contentFillAdmittedCount mirrors the gate's admitted-flight count for
	// lock-free reads (tests check every dispatch runs for an admitted flight).
	contentFillAdmittedCount atomic.Int64
	// contentFillAdmissionOverCeiling (+ _Peak) is how far bypass flights (users
	// never queue) pushed the admitted weight past the ceiling: the early
	// warning for the accepted memory residual (owner ruling 2, #710).
	contentFillAdmissionOverCeiling     atomic.Int64
	contentFillAdmissionOverCeilingPeak atomic.Int64

	contentFillLogLast       atomic.Int64 // unix nanos of the last emitted line
	contentFillLogSuppressed atomic.Uint64
)

// ContentFillGVRStats is one GVR's row of _by_gvr. Guarded by contentFillMu.
type ContentFillGVRStats struct {
	Fills      uint64 `json:"fills"`
	Duplicates uint64 `json:"duplicates"`
	Joined     uint64 `json:"joined"`
	// #765 — refresher reads served from the stamped cell / joined to the
	// pending flight instead of filling.
	RefreshReused    uint64 `json:"refresh_reused"`
	RefreshCoalesced uint64 `json:"refresh_coalesced"`
	Bytes            uint64 `json:"bytes"`
	Items            uint64 `json:"items_total"`
	MaxBytes         uint64 `json:"max_bytes"`
	MaxItems         uint64 `json:"max_items"`
	// One fill's duration (dispatch start to the end of its parse and Put):
	// EWMA and max.
	DurationMSEWMA float64 `json:"duration_ms_ewma"`
	DurationMSMax  int64   `json:"duration_ms_max"`
	// #710 PR-2a — the admission gate's calibrated decode expansion (0 = still
	// on the seed) and its weight-prediction accuracy (exact / predicted raw).
	DecodeExpansion float64 `json:"decode_expansion"`
	PredictRatio    float64 `json:"admission_predict_ratio"`
}

// ContentFillRAStats is one RESTAction's row of _by_ra. Guarded by
// contentFillMu.
type ContentFillRAStats struct {
	Fills      uint64 `json:"fills"`
	Duplicates uint64 `json:"duplicates"`
	Bytes      uint64 `json:"bytes"`
	MaxItems   uint64 `json:"max_items"`
}

// ContentFillDurationStats is the fill-duration histogram. Buckets[i] is the
// CUMULATIVE count of fills ≤ ContentFillDurationBoundsMS[i]; the last entry
// is +Inf (== Count).
type ContentFillDurationStats struct {
	Buckets []uint64
	Count   uint64
	SumMS   uint64
	MaxMS   int64
}

// msHistogram is a fixed-bound millisecond histogram over
// ContentFillDurationBoundsMS (or its own bounds) with an optional OTLP hook.
// Lock-free.
type msHistogram struct {
	bounds []int64
	// buckets[i] counts samples with bounds[i-1] < ms ≤ bounds[i]; the last
	// slot is +Inf.
	buckets  []atomic.Uint64
	count    atomic.Uint64
	sumMS    atomic.Uint64
	maxMS    atomic.Int64
	recorder atomic.Pointer[func(ms int64)]
}

func newMSHistogram() *msHistogram { return newMSHistogramBounds(ContentFillDurationBoundsMS) }

func newMSHistogramBounds(bounds []int64) *msHistogram {
	return &msHistogram{bounds: bounds, buckets: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *msHistogram) record(ms int64) {
	ms = max(ms, 0)
	i := 0
	for i < len(h.bounds) && ms > h.bounds[i] {
		i++
	}
	h.buckets[i].Add(1)
	h.count.Add(1)
	h.sumMS.Add(uint64(ms))
	storeMax(&h.maxMS, ms)
	if rec := h.recorder.Load(); rec != nil {
		(*rec)(ms)
	}
}

func (h *msHistogram) setRecorder(fn func(ms int64)) {
	if fn == nil {
		h.recorder.Store(nil)
		return
	}
	h.recorder.Store(&fn)
}

func (h *msHistogram) snapshot() ContentFillDurationStats {
	d := ContentFillDurationStats{
		Buckets: make([]uint64, len(h.buckets)),
		Count:   h.count.Load(),
		SumMS:   h.sumMS.Load(),
		MaxMS:   h.maxMS.Load(),
	}
	var cum uint64
	for i := range h.buckets {
		cum += h.buckets[i].Load()
		d.Buckets[i] = cum
	}
	return d
}

// expvarValue renders the histogram for /debug/vars: cumulative le_<bound>
// buckets plus le_inf, count, sum_ms and max_ms.
func (h *msHistogram) expvarValue() map[string]any {
	d := h.snapshot()
	buckets := make(map[string]uint64, len(d.Buckets))
	for i, b := range h.bounds {
		buckets["le_"+strconv.FormatInt(b, 10)] = d.Buckets[i]
	}
	buckets["le_inf"] = d.Buckets[len(d.Buckets)-1]
	return map[string]any{"buckets": buckets, "count": d.Count, "sum_ms": d.SumMS, "max_ms": d.MaxMS}
}

func (h *msHistogram) reset() {
	for i := range h.buckets {
		h.buckets[i].Store(0)
	}
	h.count.Store(0)
	h.sumMS.Store(0)
	h.maxMS.Store(0)
}

// SetContentFillDurationRecorder installs the fill-duration OTLP histogram
// hook (this package cannot import metrics). nil uninstalls it.
func SetContentFillDurationRecorder(fn func(ms int64)) { contentFillDurHist.setRecorder(fn) }

// SetContentFillAdmissionWaitRecorder installs the admission-wait OTLP
// histogram hook (#710 PR-2a), labelled by class. nil uninstalls it.
func SetContentFillAdmissionWaitRecorder(fn func(ms int64, class string)) {
	if fn == nil {
		contentFillAdmWaitRecorder.Store(nil)
		return
	}
	contentFillAdmWaitRecorder.Store(&fn)
}

// recordContentFillAdmissionWait folds one flight's admission wait.
func recordContentFillAdmissionWait(class fillAdmissionClass, ms int64) {
	if class >= fillAdmissionClasses {
		class = fillClassBackground
	}
	contentFillAdmWaitHist[class].record(ms)
	if rec := contentFillAdmWaitRecorder.Load(); rec != nil {
		(*rec)(max(ms, 0), class.String())
	}
}

// contentFillOwner names the RESTAction and stage a content call belongs to,
// for the fill's pprof labels, _by_ra and log line. Neither carries identity:
// both come from the RESTAction spec.
type contentFillOwner struct {
	ra    string // namespace/name, "" when the caller did not thread it
	stage string
}

type ctxKeyContentFillOwnerType struct{}

var ctxKeyContentFillOwner = ctxKeyContentFillOwnerType{}

// withContentFillOwner records the owning RESTAction and stage on ctx.
func withContentFillOwner(ctx context.Context, raNamespace, raName, stage string) context.Context {
	o := contentFillOwner{stage: stage}
	if raName != "" {
		o.ra = raNamespace + "/" + raName
	}
	return context.WithValue(ctx, ctxKeyContentFillOwner, o)
}

func contentFillOwnerFrom(ctx context.Context) contentFillOwner {
	o, _ := ctx.Value(ctxKeyContentFillOwner).(contentFillOwner)
	return o
}

// contentFill is one in-flight fill. It is owned by the one goroutine running
// it; only the package-level counters it moves are shared.
type contentFill struct {
	key       string
	gvr       string
	namespace string
	isList    bool
	reason    contentFillReason
	owner     contentFillOwner
	duplicate bool
	start     time.Time
	bytes     int64
	items     int
	servable  bool
	filled    bool
	durMS     int64
}

// beginContentFill opens a fill of content key `key` and counts it. It must be
// paired with end() (deferred by the caller).
func beginContentFill(ctx context.Context, key, gvr, namespace string, isList bool, reason contentFillReason) *contentFill {
	f := &contentFill{
		key:       key,
		gvr:       gvr,
		namespace: namespace,
		isList:    isList,
		reason:    reason,
		owner:     contentFillOwnerFrom(ctx),
		start:     time.Now(),
	}
	contentFillMu.Lock()
	f.duplicate = contentFillByKey[key] > 0
	contentFillByKey[key]++
	g := contentFillGVRRow(gvr)
	g.Fills++
	ra := contentFillRARow(f.owner.ra)
	ra.Fills++
	if f.duplicate {
		g.Duplicates++
		ra.Duplicates++
	}
	contentFillMu.Unlock()

	contentFillTotal[reason].Add(1)
	if f.duplicate {
		contentFillDuplicate[reason].Add(1)
	}
	storeMax(&contentFillInflightPeak, contentFillInflight.Add(1))
	return f
}

// pprofLabels is the fill's CPU-profile label set. The values are a GVR, a
// closed-enum reason and a RESTAction namespace/name — no identity.
func (f *contentFill) pprofLabels() pprof.LabelSet {
	return pprof.Labels(
		"apistage_fill_gvr", f.gvr,
		"apistage_fill_reason", f.reason.String(),
		"apistage_fill_ra", f.owner.ra,
	)
}

// dispatched records the raw envelope the fill's dispatch produced.
func (f *contentFill) dispatched(raw []byte) {
	f.servable = true
	f.bytes = int64(len(raw))
	contentFillBytes[f.reason].Add(uint64(f.bytes))
	storeMax(&contentFillInflightBytesPeak, contentFillInflightBytes.Add(f.bytes))
}

// notServable records a fill whose dispatch could not be served in-process.
func (f *contentFill) notServable() {
	contentFillNotServable.Add(1)
}

// parsed records the item count of the fill's decoded LIST envelope.
func (f *contentFill) parsed(items int) {
	f.items = items
}

// done closes the fill's timed window (dispatch start → parse and Put done)
// and records its duration. Called once, after the content Put.
func (f *contentFill) done() {
	if f.filled {
		return
	}
	f.filled = true
	ms := max(time.Since(f.start).Milliseconds(), 0)
	f.durMS = ms
	recordContentFillDuration(ms)
	contentFillMu.Lock()
	g := contentFillGVRRow(f.gvr)
	if g.DurationMSEWMA == 0 {
		g.DurationMSEWMA = float64(ms)
	} else {
		g.DurationMSEWMA += contentFillDurationEWMAAlpha * (float64(ms) - g.DurationMSEWMA)
	}
	g.DurationMSMax = max(g.DurationMSMax, ms)
	contentFillMu.Unlock()
}

// recordContentFillDuration folds one fill duration into the histogram and
// the OTLP hook.
func recordContentFillDuration(ms int64) { contentFillDurHist.record(ms) }

// end closes the fill: releases its in-flight share, folds its size into the
// GVR and RA rows and emits the rate-limited log line.
func (f *contentFill) end() {
	contentFillInflightBytes.Add(-f.bytes)
	contentFillInflight.Add(-1)

	contentFillMu.Lock()
	if n := contentFillByKey[f.key]; n <= 1 {
		delete(contentFillByKey, f.key)
	} else {
		contentFillByKey[f.key] = n - 1
	}
	if f.servable {
		g := contentFillGVRRow(f.gvr)
		g.Bytes += uint64(f.bytes)
		g.Items += uint64(f.items)
		g.MaxBytes = max(g.MaxBytes, uint64(f.bytes))
		g.MaxItems = max(g.MaxItems, uint64(f.items))
		ra := contentFillRARow(f.owner.ra)
		ra.Bytes += uint64(f.bytes)
		ra.MaxItems = max(ra.MaxItems, uint64(f.items))
	}
	contentFillMu.Unlock()

	f.log()
}

// noteContentFillJoined counts one reader that joined an in-flight fill.
func noteContentFillJoined(reason contentFillReason, gvr string) {
	contentFillJoined[reason].Add(1)
	contentFillMu.Lock()
	contentFillGVRRow(gvr).Joined++
	contentFillMu.Unlock()
}

// noteContentRefreshReused counts one stamp-gated refresher read served from
// the stored cell because its stamp covered every trigger (#765).
func noteContentRefreshReused(gvr string) {
	contentFillRefreshReused.Add(1)
	contentFillMu.Lock()
	contentFillGVRRow(gvr).RefreshReused++
	contentFillMu.Unlock()
}

// noteContentRefreshCoalesced counts one background stamp-gated refresher
// reader that joined its key's pending flight (#765).
func noteContentRefreshCoalesced(gvr string) {
	contentFillRefreshCoalesced.Add(1)
	contentFillMu.Lock()
	contentFillGVRRow(gvr).RefreshCoalesced++
	contentFillMu.Unlock()
}

// noteContentFillJoinRefused counts one reader that found a flight it may not
// join (the coordinate moved after the flight started).
func noteContentFillJoinRefused(reason contentFillReason) {
	contentFillJoinRefused[reason].Add(1)
}

// contentFillGVRRow returns gvr's row, creating it. Caller holds contentFillMu.
func contentFillGVRRow(gvr string) *ContentFillGVRStats {
	g := contentFillByGVRMap[gvr]
	if g == nil {
		g = &ContentFillGVRStats{}
		contentFillByGVRMap[gvr] = g
	}
	return g
}

// contentFillRARow returns ra's row, creating it. Caller holds contentFillMu.
func contentFillRARow(ra string) *ContentFillRAStats {
	if ra == "" {
		ra = contentFillUnattributedRA
	}
	r := contentFillByRAMap[ra]
	if r == nil {
		r = &ContentFillRAStats{}
		contentFillByRAMap[ra] = r
	}
	return r
}

// log emits apistage.content_fill at Debug, at most once per
// contentFillLogEvery.
func (f *contentFill) log() {
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	now := time.Now()
	last := contentFillLogLast.Load()
	if now.UnixNano()-last < int64(contentFillLogEvery) || !contentFillLogLast.CompareAndSwap(last, now.UnixNano()) {
		contentFillLogSuppressed.Add(1)
		return
	}
	slog.Default().Debug("apistage.content_fill",
		slog.String("subsystem", "cache"),
		slog.String("gvr", f.gvr),
		slog.String("ns", f.namespace),
		slog.String("ra", f.owner.ra),
		slog.String("stage", f.owner.stage),
		slog.String("reason", f.reason.String()),
		slog.Bool("is_list", f.isList),
		slog.Bool("servable", f.servable),
		slog.Int64("bytes", f.bytes),
		slog.Int("items", f.items),
		slog.Int64("duration_ms", f.durMS),
		slog.Int64("serve_ms", now.Sub(f.start).Milliseconds()),
		slog.Bool("duplicate", f.duplicate),
		slog.Int64("inflight", contentFillInflight.Load()),
		slog.Int64("inflight_bytes", contentFillInflightBytes.Load()),
		slog.Uint64("suppressed", contentFillLogSuppressed.Swap(0)),
	)
}

// storeMax raises *m to v if v is larger.
func storeMax(m *atomic.Int64, v int64) {
	for {
		cur := m.Load()
		if v <= cur || m.CompareAndSwap(cur, v) {
			return
		}
	}
}

// ContentFillStats is a snapshot of the fill counters — the single source the
// expvar closures and the OTLP callback read, exported so tests assert exact
// deltas.
type ContentFillStats struct {
	Total             map[string]uint64
	Duplicate         map[string]uint64
	Bytes             map[string]uint64
	Joined            map[string]uint64
	JoinRefused       map[string]uint64
	JoinWaiting       int64
	RefreshReused     uint64 // #765
	RefreshCoalesced  uint64 // #765
	NotServable       uint64
	Inflight          int64
	InflightBytes     int64
	InflightPeak      int64
	InflightBytesPeak int64
	ByGVR             map[string]ContentFillGVRStats
	ByRA              map[string]ContentFillRAStats
	Duration          ContentFillDurationStats
}

// ContentFillStatsSnapshot returns every series of the closed reason set,
// zeros included.
func ContentFillStatsSnapshot() ContentFillStats {
	s := ContentFillStats{
		Total:             make(map[string]uint64, contentFillReasons),
		Duplicate:         make(map[string]uint64, contentFillReasons),
		Bytes:             make(map[string]uint64, contentFillReasons),
		Joined:            make(map[string]uint64, contentFillReasons),
		JoinRefused:       make(map[string]uint64, contentFillReasons),
		JoinWaiting:       contentFillJoinWaiting.Load(),
		RefreshReused:     contentFillRefreshReused.Load(),
		RefreshCoalesced:  contentFillRefreshCoalesced.Load(),
		NotServable:       contentFillNotServable.Load(),
		Inflight:          contentFillInflight.Load(),
		InflightBytes:     contentFillInflightBytes.Load(),
		InflightPeak:      contentFillInflightPeak.Load(),
		InflightBytesPeak: contentFillInflightBytesPeak.Load(),
		Duration:          contentFillDurHist.snapshot(),
	}
	for r := contentFillReason(0); r < contentFillReasons; r++ {
		s.Total[r.String()] = contentFillTotal[r].Load()
		s.Duplicate[r.String()] = contentFillDuplicate[r].Load()
		s.Bytes[r.String()] = contentFillBytes[r].Load()
		s.Joined[r.String()] = contentFillJoined[r].Load()
		s.JoinRefused[r.String()] = contentFillJoinRefused[r].Load()
	}
	contentFillMu.Lock()
	s.ByGVR = make(map[string]ContentFillGVRStats, len(contentFillByGVRMap))
	for gvr, g := range contentFillByGVRMap {
		s.ByGVR[gvr] = *g
	}
	contentFillMu.Unlock()
	// #710 PR-2a — the admission gate's per-GVR calibration, beside the fills.
	expansion, ratio := contentFillCalibrationSnapshot()
	for gvr, row := range s.ByGVR {
		row.DecodeExpansion, row.PredictRatio = expansion[gvr], ratio[gvr]
		s.ByGVR[gvr] = row
	}
	contentFillMu.Lock()
	s.ByRA = make(map[string]ContentFillRAStats, len(contentFillByRAMap))
	for ra, r := range contentFillByRAMap {
		s.ByRA[ra] = *r
	}
	contentFillMu.Unlock()
	return s
}

// ContentFillAdmissionStats is the #710 PR-2a admission gate's state: waiters
// and timeouts by class (the closed ContentFillAdmissionClasses set), the
// in-flight estimated weight, the wait histogram and the calibrated decode
// expansion per GVR.
type ContentFillAdmissionStats struct {
	Queued         map[string]int64
	Timeout        map[string]uint64
	InflightWeight int64
	OverCeiling    int64
	OverCeilingPk  int64
	Wait           map[string]ContentFillDurationStats
	Expansion      map[string]float64
	PredictRatio   map[string]float64
}

// ContentFillAdmissionStatsSnapshot returns every class of the closed set.
func ContentFillAdmissionStatsSnapshot() ContentFillAdmissionStats {
	s := ContentFillAdmissionStats{
		Queued:         make(map[string]int64, fillAdmissionClasses),
		Timeout:        make(map[string]uint64, fillAdmissionClasses),
		InflightWeight: contentFillAdmissionInflightWeight.Load(),
		OverCeiling:    contentFillAdmissionOverCeiling.Load(),
		OverCeilingPk:  contentFillAdmissionOverCeilingPeak.Load(),
		Wait:           make(map[string]ContentFillDurationStats, fillAdmissionClasses),
	}
	s.Expansion, s.PredictRatio = contentFillCalibrationSnapshot()
	for c := fillAdmissionClass(0); c < fillAdmissionClasses; c++ {
		s.Queued[c.String()] = contentFillAdmissionQueued[c].Load()
		s.Timeout[c.String()] = contentFillAdmissionTimeout[c].Load()
		s.Wait[c.String()] = contentFillAdmWaitHist[c].snapshot()
	}
	return s
}

// ResetContentFillStatsForTest zeroes every counter, gauge, peak and table. It
// must not run while a fill is in flight. Production MUST NOT use it.
func ResetContentFillStatsForTest() {
	for r := range contentFillTotal {
		contentFillTotal[r].Store(0)
		contentFillDuplicate[r].Store(0)
		contentFillBytes[r].Store(0)
		contentFillJoined[r].Store(0)
		contentFillJoinRefused[r].Store(0)
	}
	contentFillJoinWaiting.Store(0)
	contentFillRefreshReused.Store(0)
	contentFillRefreshCoalesced.Store(0)
	contentFillNotServable.Store(0)
	contentFillInflight.Store(0)
	contentFillInflightBytes.Store(0)
	contentFillInflightPeak.Store(0)
	contentFillInflightBytesPeak.Store(0)
	contentFillDurHist.reset()
	for _, h := range contentFillAdmWaitHist {
		h.reset()
	}
	for c := range contentFillAdmissionTimeout {
		contentFillAdmissionTimeout[c].Store(0)
	}
	resetContentFillGateForTest()
	resetContentFlightsForTest()
	contentFillLogLast.Store(0)
	contentFillLogSuppressed.Store(0)
	contentFillMu.Lock()
	contentFillByKey = map[string]int{}
	contentFillByGVRMap = map[string]*ContentFillGVRStats{}
	contentFillByRAMap = map[string]*ContentFillRAStats{}
	contentFillMu.Unlock()
}

// AddContentFillForTest moves the closed-label series through their production
// atomics (n fills of reason, d of them duplicates, b bytes) so a cross-package
// arm can prove the OTLP mirror reads the LIVE counters. false: not a reason of
// the set. Production MUST NOT use it.
func AddContentFillForTest(reason string, n, d, b uint64) bool {
	for i, name := range contentFillReasonNames {
		if name == reason {
			contentFillTotal[i].Add(n)
			contentFillDuplicate[i].Add(d)
			contentFillBytes[i].Add(b)
			return true
		}
	}
	return false
}

// AddContentFillJoinForTest moves the single-flight series (#710 PR-1) through
// their production atomics: j joined and r join-refused readers of reason, and
// w to the waiting gauge. false: not a reason of the set. Production MUST NOT
// use it.
func AddContentFillJoinForTest(reason string, j, r uint64, w int64) bool {
	for i, name := range contentFillReasonNames {
		if name == reason {
			contentFillJoined[i].Add(j)
			contentFillJoinRefused[i].Add(r)
			contentFillJoinWaiting.Add(w)
			return true
		}
	}
	return false
}

// AddContentFillAdmissionForTest moves the admission series of class (#710
// PR-2a) through their production atomics: q queued, n timeouts, w to the
// in-flight weight. false: not a class of the set. Production MUST NOT use it.
func AddContentFillAdmissionForTest(class string, q int64, n uint64, w int64) bool {
	for i, name := range fillAdmissionClassNames {
		if name == class {
			contentFillAdmissionQueued[i].Add(q)
			contentFillAdmissionTimeout[i].Add(n)
			contentFillAdmissionInflightWeight.Add(w)
			return true
		}
	}
	return false
}

// RecordContentFillAdmissionWaitForTest records one admission wait through
// the production histogram path. Production MUST NOT use it.
func RecordContentFillAdmissionWaitForTest(class string, ms int64) bool {
	for i, name := range fillAdmissionClassNames {
		if name == class {
			recordContentFillAdmissionWait(fillAdmissionClass(i), ms)
			return true
		}
	}
	return false
}

// AddContentFillInflightForTest moves the in-flight gauges through their
// production atomics. Production MUST NOT use it.
func AddContentFillInflightForTest(n, bytes int64) {
	contentFillInflight.Add(n)
	contentFillInflightBytes.Add(bytes)
}

// RecordContentFillDurationForTest records one fill duration through the
// production histogram path (buckets + the OTLP hook). Production MUST NOT
// use it.
func RecordContentFillDurationForTest(ms int64) { recordContentFillDuration(ms) }

var contentFillExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerContentFillExpvar()
}

func registerContentFillExpvar() {
	contentFillExpvarOnce.Do(func() {
		expvar.Publish("snowplow_apistage_content_fill_total", expvar.Func(func() any {
			return ContentFillStatsSnapshot().Total
		}))
		expvar.Publish("snowplow_apistage_content_fill_duplicate_total", expvar.Func(func() any {
			return ContentFillStatsSnapshot().Duplicate
		}))
		expvar.Publish("snowplow_apistage_content_fill_bytes_total", expvar.Func(func() any {
			return ContentFillStatsSnapshot().Bytes
		}))
		expvar.Publish("snowplow_apistage_content_fill_joined_total", expvar.Func(func() any {
			return ContentFillStatsSnapshot().Joined
		}))
		expvar.Publish("snowplow_apistage_content_fill_join_refused_total", expvar.Func(func() any {
			return ContentFillStatsSnapshot().JoinRefused
		}))
		expvar.Publish("snowplow_apistage_content_fill_join_waiting", expvar.Func(func() any {
			return contentFillJoinWaiting.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_refresh_reused_total", expvar.Func(func() any {
			return contentFillRefreshReused.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_refresh_coalesced_total", expvar.Func(func() any {
			return contentFillRefreshCoalesced.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_not_servable_total", expvar.Func(func() any {
			return contentFillNotServable.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_inflight", expvar.Func(func() any {
			return contentFillInflight.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_inflight_bytes", expvar.Func(func() any {
			return contentFillInflightBytes.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_inflight_peak", expvar.Func(func() any {
			return contentFillInflightPeak.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_inflight_bytes_peak", expvar.Func(func() any {
			return contentFillInflightBytesPeak.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_duration_ms", expvar.Func(func() any {
			return contentFillDurHist.expvarValue()
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_wait_ms", expvar.Func(func() any {
			out := make(map[string]any, fillAdmissionClasses)
			for c := fillAdmissionClass(0); c < fillAdmissionClasses; c++ {
				out[c.String()] = contentFillAdmWaitHist[c].expvarValue()
			}
			return out
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_queued", expvar.Func(func() any {
			return ContentFillAdmissionStatsSnapshot().Queued
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_timeout_total", expvar.Func(func() any {
			return ContentFillAdmissionStatsSnapshot().Timeout
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_inflight_weight", expvar.Func(func() any {
			return contentFillAdmissionInflightWeight.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_over_ceiling_bytes", expvar.Func(func() any {
			return contentFillAdmissionOverCeiling.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_over_ceiling_bytes_peak", expvar.Func(func() any {
			return contentFillAdmissionOverCeilingPeak.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_admission_inflight_weight_peak", expvar.Func(func() any {
			return contentFillAdmissionInflightWeightPeak.Load()
		}))
		expvar.Publish("snowplow_apistage_content_fill_by_gvr", expvar.Func(func() any {
			return ContentFillStatsSnapshot().ByGVR
		}))
		expvar.Publish("snowplow_apistage_content_fill_by_ra", expvar.Func(func() any {
			return ContentFillStatsSnapshot().ByRA
		}))
	})
}

// RegisterContentFillExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterContentFillExpvarForTest() { registerContentFillExpvar() }
