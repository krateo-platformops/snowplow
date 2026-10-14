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

	// contentFillDurBuckets[i] counts fills with bounds[i-1] < duration ≤
	// bounds[i]; the last slot is +Inf.
	contentFillDurBuckets = make([]atomic.Uint64, len(ContentFillDurationBoundsMS)+1)
	contentFillDurCount   atomic.Uint64
	contentFillDurSumMS   atomic.Uint64
	contentFillDurMaxMS   atomic.Int64

	// contentFillDurRecorder is the OTLP histogram hook metrics installs.
	contentFillDurRecorder atomic.Pointer[func(ms int64)]

	contentFillLogLast       atomic.Int64 // unix nanos of the last emitted line
	contentFillLogSuppressed atomic.Uint64
)

// ContentFillGVRStats is one GVR's row of _by_gvr. Guarded by contentFillMu.
type ContentFillGVRStats struct {
	Fills      uint64 `json:"fills"`
	Duplicates uint64 `json:"duplicates"`
	Bytes      uint64 `json:"bytes"`
	Items      uint64 `json:"items_total"`
	MaxBytes   uint64 `json:"max_bytes"`
	MaxItems   uint64 `json:"max_items"`
	// One fill's duration (dispatch start to the end of its parse and Put):
	// EWMA and max.
	DurationMSEWMA float64 `json:"duration_ms_ewma"`
	DurationMSMax  int64   `json:"duration_ms_max"`
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

// SetContentFillDurationRecorder installs the OTLP histogram hook (this
// package cannot import metrics). nil uninstalls it.
func SetContentFillDurationRecorder(fn func(ms int64)) {
	if fn == nil {
		contentFillDurRecorder.Store(nil)
		return
	}
	contentFillDurRecorder.Store(&fn)
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
func recordContentFillDuration(ms int64) {
	i := 0
	for i < len(ContentFillDurationBoundsMS) && ms > ContentFillDurationBoundsMS[i] {
		i++
	}
	contentFillDurBuckets[i].Add(1)
	contentFillDurCount.Add(1)
	contentFillDurSumMS.Add(uint64(ms))
	storeMax(&contentFillDurMaxMS, ms)
	if rec := contentFillDurRecorder.Load(); rec != nil {
		(*rec)(ms)
	}
}

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
		NotServable:       contentFillNotServable.Load(),
		Inflight:          contentFillInflight.Load(),
		InflightBytes:     contentFillInflightBytes.Load(),
		InflightPeak:      contentFillInflightPeak.Load(),
		InflightBytesPeak: contentFillInflightBytesPeak.Load(),
		Duration:          contentFillDurationSnapshot(),
	}
	for r := contentFillReason(0); r < contentFillReasons; r++ {
		s.Total[r.String()] = contentFillTotal[r].Load()
		s.Duplicate[r.String()] = contentFillDuplicate[r].Load()
		s.Bytes[r.String()] = contentFillBytes[r].Load()
	}
	contentFillMu.Lock()
	s.ByGVR = make(map[string]ContentFillGVRStats, len(contentFillByGVRMap))
	for gvr, g := range contentFillByGVRMap {
		s.ByGVR[gvr] = *g
	}
	s.ByRA = make(map[string]ContentFillRAStats, len(contentFillByRAMap))
	for ra, r := range contentFillByRAMap {
		s.ByRA[ra] = *r
	}
	contentFillMu.Unlock()
	return s
}

func contentFillDurationSnapshot() ContentFillDurationStats {
	d := ContentFillDurationStats{
		Buckets: make([]uint64, len(contentFillDurBuckets)),
		Count:   contentFillDurCount.Load(),
		SumMS:   contentFillDurSumMS.Load(),
		MaxMS:   contentFillDurMaxMS.Load(),
	}
	var cum uint64
	for i := range contentFillDurBuckets {
		cum += contentFillDurBuckets[i].Load()
		d.Buckets[i] = cum
	}
	return d
}

// contentFillDurationExpvar renders the histogram for /debug/vars: cumulative
// le_<bound> buckets plus le_inf, count, sum_ms and max_ms.
func contentFillDurationExpvar() map[string]any {
	d := contentFillDurationSnapshot()
	buckets := make(map[string]uint64, len(d.Buckets))
	for i, b := range ContentFillDurationBoundsMS {
		buckets["le_"+strconv.FormatInt(b, 10)] = d.Buckets[i]
	}
	buckets["le_inf"] = d.Buckets[len(d.Buckets)-1]
	return map[string]any{"buckets": buckets, "count": d.Count, "sum_ms": d.SumMS, "max_ms": d.MaxMS}
}

// ResetContentFillStatsForTest zeroes every counter, gauge, peak and table. It
// must not run while a fill is in flight. Production MUST NOT use it.
func ResetContentFillStatsForTest() {
	for r := range contentFillTotal {
		contentFillTotal[r].Store(0)
		contentFillDuplicate[r].Store(0)
		contentFillBytes[r].Store(0)
	}
	contentFillNotServable.Store(0)
	contentFillInflight.Store(0)
	contentFillInflightBytes.Store(0)
	contentFillInflightPeak.Store(0)
	contentFillInflightBytesPeak.Store(0)
	for i := range contentFillDurBuckets {
		contentFillDurBuckets[i].Store(0)
	}
	contentFillDurCount.Store(0)
	contentFillDurSumMS.Store(0)
	contentFillDurMaxMS.Store(0)
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
			return contentFillDurationExpvar()
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
