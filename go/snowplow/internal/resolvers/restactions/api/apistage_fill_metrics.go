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
// Surfaces (expvar only, under the reserved-free prefix
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
//   - _by_gvr  map gvr → {fills, duplicates, bytes, items_total, max_bytes,
//     max_items}. The GVR set is data-derived (CRDs), which is why none of this
//     is mirrored to OTLP: metrics.go has no apistage-fill family, and an OTLP
//     gvr attribute would be unbounded (the #311 bounded-attribute discipline).
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
// One log line per fill, rate-limited to contentFillLogEvery with a suppressed
// count: gvr, namespace, RESTAction, stage, reason, bytes, items, duration and
// duplicate. It is written to slog.Default(), never the request logger (which
// carries the requester), and never names the object: a GET-by-name may be a
// Secret. Nothing here names a user, a group, a certificate or a cache body.
//
// CFG-1: gated on cache.Disabled(). With the cache off nothing is served from
// the content layer, so the family is structurally zero and is not published.
package api

import (
	"context"
	"expvar"
	"log/slog"
	"runtime/pprof"
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

// contentFillLogEvery bounds the per-fill log line to one per interval.
const contentFillLogEvery = time.Second

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
	// detector) and the per-GVR table. The key map holds only keys with a
	// fill in flight, so it is bounded by the concurrent fills.
	contentFillMu       sync.Mutex
	contentFillByKey    = map[string]int{}
	contentFillByGVRMap = map[string]*ContentFillGVRStats{}

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
}

// contentFillOwner names the RESTAction and stage a content call belongs to,
// for the fill's pprof labels and log line. Neither carries identity: both
// come from the RESTAction spec.
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
	if f.duplicate {
		g.Duplicates++
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

// end closes the fill: releases its in-flight share, folds its size into the
// GVR row and emits the rate-limited log line.
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

// log emits apistage.content_fill at most once per contentFillLogEvery.
func (f *contentFill) log() {
	now := time.Now()
	last := contentFillLogLast.Load()
	if now.UnixNano()-last < int64(contentFillLogEvery) || !contentFillLogLast.CompareAndSwap(last, now.UnixNano()) {
		contentFillLogSuppressed.Add(1)
		return
	}
	slog.Default().Info("apistage.content_fill",
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
		slog.Int64("duration_ms", now.Sub(f.start).Milliseconds()),
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
// expvar closures read, exported so tests assert exact deltas.
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
	contentFillMu.Unlock()
	return s
}

// ResetContentFillStatsForTest zeroes every counter, gauge and peak. It must
// not run while a fill is in flight. Production MUST NOT use it.
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
	contentFillLogLast.Store(0)
	contentFillLogSuppressed.Store(0)
	contentFillMu.Lock()
	contentFillByKey = map[string]int{}
	contentFillByGVRMap = map[string]*ContentFillGVRStats{}
	contentFillMu.Unlock()
}

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
		expvar.Publish("snowplow_apistage_content_fill_by_gvr", expvar.Func(func() any {
			return ContentFillStatsSnapshot().ByGVR
		}))
	})
}

// RegisterContentFillExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterContentFillExpvarForTest() { registerContentFillExpvar() }
