// stale_push.go — #1062: stale serve → the replacing frame pushed, for EVERY
// layer that answers with stale bytes.
//
// THE GOAL (#710 ruling): users never queue; under pressure they get a
// temporary stale page, then it is replaced, p95 ≤ 3 s and max ≤ 10 s. Before
// #1062 only the apistage stale serve (D2) was timed, and run 20b served 3,982
// stale L1 pages with no apistage serve at all, so the bench read "not
// exercised" while the dominant path went unmeasured. This ledger times every
// stale serve the same way, keyed by the layer that served it.
//
// # WHAT A STALE SERVE IS
//
// A customer response written with bytes snowplow knows may be stale, under an
// ARMED refresh key (the response stamped X-Snowplow-Refresh-Key, so a
// /refreshes stream can be told when it changes). The layers are a closed set:
//
//   - l1: an L1 HIT on a cell whose invalidation→fresh window is open or that
//     is known-stale (it carries a dirty stamp, dirty_window.go) — the same
//     condition resolved_cache.stale_served_total counts, restricted to the
//     armed responses of the three customer hit sites (restactions, widgets,
//     widgetContent). A nested cell served inside a resolve is not a response
//     and is not counted here.
//   - apistage: a response whose body is STALE-DERIVED (#710 PR-2b D2): the
//     cold resolve that served a known-stale content cell at once and
//     committed (CommitStaleServe), or an L1 hit on such a stale-derived cell
//     before its replace landed.
//
// A response that is not armed (an external-TTL widget, a declined Put) is not
// a stale serve here: nothing could ever push to it, by design.
//
// # WHEN IT IS REPLACED (one rule for every layer)
//
// A serve is REPLACED when its key's cell no longer holds the stale bytes:
//
//   - an ACCEPTED Put of the key whose body is not stale-derived and, when the
//     serve saw a dirty stamp, whose resolve began at or after that stamp's
//     latest mark (its #375 startSeq ≥ the mark's seq — the rule that ends the
//     known-stale state, endKnownStaleOnPut). A Put that began before the mark
//     carries the pre-change state and replaces nothing; or
//   - the key LEAVES the store: a refetch is then a fresh fill.
//
// # WHEN IT IS PUSHED
//
// When the /refreshes handler WROTE a frame for the key (RefreshStream.Wrote,
// after writeFrame returned true — a write, not an offer) after the serve was
// replaced, or wrote a resync frame to a stream armed for it (the SPA then
// re-reads every armed key). That closes the serve: stale_to_push_ms{layer}
// records now − servedAt. A frame written before the replacement closes
// nothing: the refetch it triggers reads the same stale cell.
//
// The client is identified by the key it was served under (the key folds every
// identity input), as the #870 push ledger and the D2 owed marks do: a frame
// written for the key on any stream closes every replaced serve of the key.
//
// # UNREPLACED (the 10 s goal max)
//
// stale_unreplaced_total{layer} counts, ONCE per serve, a serve whose
// stale-to-push exceeded the 10 s max: recorded above it at its push, or — so a
// push that never comes is visible, not silent — still unpushed 10 s after its
// serve while a /refreshes stream is armed for its key (the overdue scanner).
//
// # A CLIENT WITHOUT A STREAM (decided here)
//
// A serve whose key no stream has armed cannot be pushed: there is no stream.
// It is NOT counted unreplaced while no stream is armed (the #740 ruling: with
// nobody viewing, nothing is an alert; a k6 or API client would otherwise make
// the detector fire on every stale read). It stays OPEN, so a stream that arms
// later and is written a frame records the full delay since the serve (the #710
// F2 ruling: the late tab showed the stale body all that time) and, past 10 s,
// counts unreplaced. A serve no stream ever pushes leaves the ledger only
// through its capacity bound (close=dropped): the L1 store's entry budget, the
// bound the D2 owed marks use. stale_serves_open{layer} shows what is waiting.
//
// # THE #740 RESUME
//
// A parked D2 debt (nobody viewed a dependent while its refill kept failing)
// restarts its dependents' clocks when it resumes (RestartStaleServeClock): the
// architect ruling measures a resumed debt's stale-to-push from the resume.
//
// # COST AND LOCKS
//
// stalePush.mu is a LEAF: nothing is called under it but map/list operations.
// Put and eviction hooks run first through one atomic load (no open serve → no
// lock); eviction runs under the store's c.mu (evictDirtyWindow), which is why
// the ledger never takes c.mu or the hub lock. The overdue scanner reads the
// hub (HasRefreshSubscriber) with the ledger lock released.
//
// # KNOWN BIAS
//
// The hit sites read the dirty stamp just after their Get: a refresher Put
// landing in those microseconds hides the serve (under-count by that race).
//
// Metadata only: no key leaves this file except the opaque L1 key digest in a
// WARN line, as the store's own logs print it.
package cache

import (
	"container/list"
	"context"
	"expvar"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// The serving layers (the `layer` label), a closed set.
const (
	StaleLayerAPIStage = "apistage"
	StaleLayerL1       = "l1"
)

// StaleLayers is the `layer` label set, in export order.
var StaleLayers = []string{StaleLayerAPIStage, StaleLayerL1}

// The close labels (the `close` label of stale_serve_closes_total).
const (
	stalePushClosePushed  = "pushed"
	stalePushCloseDropped = "dropped"
)

// StalePushCloses is the `close` label set, in export order.
var StalePushCloses = []string{stalePushClosePushed, stalePushCloseDropped}

// StaleToPushMaxMS is the goal's maximum from a stale serve to its push (owner
// bar: max ≤ 10 s). A design constant, never an env knob.
const StaleToPushMaxMS = 10_000

// StaleToPushBoundsMS are stale_to_push_ms's bucket bounds: they include
// exactly 1000 (the target), 3000 (the p95 bar) and 10000 (the max bar), so the
// bench decides the bar from /debug/vars or OTLP.
var StaleToPushBoundsMS = []int64{100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000}

// stalePushScanInterval is the overdue scanner's period. It decides only when
// an unpushed serve past the 10 s bar is COUNTED (never a sample): one second
// against a ten-second bar.
const stalePushScanInterval = time.Second

const (
	layerAPIStage = iota
	layerL1
	stalePushLayers
)

const (
	closePushedIdx = iota
	closeDroppedIdx
	stalePushCloseCount
)

func layerIndex(layer string) (int, bool) {
	switch layer {
	case StaleLayerAPIStage:
		return layerAPIStage, true
	case StaleLayerL1:
		return layerL1, true
	}
	return 0, false
}

// staleServe is one open stale serve.
type staleServe struct {
	key   string
	layer int
	// servedAt is when snowplow answered with the stale bytes (moved forward by
	// a #740 resume, RestartStaleServeClock).
	servedAt time.Time
	// needSeq/seq: the serve saw a dirty stamp whose latest mark is seq; only a
	// Put whose resolve began at or after it replaces the serve.
	needSeq  bool
	seq      uint64
	replaced bool
	// overdue: counted on stale_unreplaced_total (once).
	overdue bool
	el      *list.Element
}

type stalePushLedger struct {
	mu    sync.Mutex
	byKey map[string][]*staleServe
	order *list.List // every open serve, oldest first (the capacity bound)
}

var stalePush = &stalePushLedger{byKey: map[string][]*staleServe{}, order: list.New()}

// stalePushOpenTotal is the number of open serves: the Put / eviction / frame
// hooks' lock-free fast path.
var stalePushOpenTotal atomic.Int64

// stalePushStats is the family's counters and histograms.
var stalePushStats = struct {
	serves     [stalePushLayers]atomic.Uint64
	unreplaced [stalePushLayers]atomic.Uint64
	open       [stalePushLayers]atomic.Int64
	closes     [stalePushLayers][stalePushCloseCount]atomic.Uint64
	toPush     [stalePushLayers]*staleMSHistogram
}{toPush: [stalePushLayers]*staleMSHistogram{newStaleMSHistogram(StaleToPushBoundsMS), newStaleMSHistogram(StaleToPushBoundsMS)}}

// stalePushRecorder is the OTLP histogram hook (internal/metrics installs it).
var stalePushRecorder atomic.Pointer[func(layer string, ms int64)]

// SetStaleToPushRecorder installs the hook every stale_to_push sample (layer,
// ms) is recorded into. nil clears it.
func SetStaleToPushRecorder(fn func(layer string, ms int64)) {
	if fn == nil {
		stalePushRecorder.Store(nil)
		return
	}
	stalePushRecorder.Store(&fn)
}

// stalePushClock is the ledger's clock (seamed in tests). nil reads time.Now.
var stalePushClock atomic.Pointer[func() time.Time]

// StalePushNow reads the ledger's clock: every servedAt a caller hands the
// ledger is read from it, so a delay is the difference of two readings of ONE
// clock.
func StalePushNow() time.Time {
	if f := stalePushClock.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

// SetStalePushClockForTest installs now as the ledger clock and returns the
// restore. Production MUST NOT call it.
func SetStalePushClockForTest(now func() time.Time) (restore func()) {
	prev := stalePushClock.Load()
	if now == nil {
		stalePushClock.Store(nil)
	} else {
		stalePushClock.Store(&now)
	}
	return func() { stalePushClock.Store(prev) }
}

// ── serves ───────────────────────────────────────────────────────────────────

// NoteStaleHit records the stale serve of an ARMED customer L1 hit on key, if
// entry's bytes are stale: a stale-derived body (apistage), else a cell with a
// dirty stamp (l1). Call it at the hit's write point, only when the refresh
// key header was stamped. Nil-safe.
func NoteStaleHit(key string, entry *ResolvedEntry) {
	if key == "" || entry == nil {
		return
	}
	if entry.StaleDerived {
		noteStaleServe(key, layerAPIStage, StalePushNow())
		return
	}
	if _, dirty := dirtyStampFor(key); dirty {
		noteStaleServe(key, layerL1, StalePushNow())
	}
}

// noteStaleServe opens one serve of key at servedAt. It reads key's dirty stamp
// (the mark a replacing Put must have read past) before taking the leaf lock.
func noteStaleServe(key string, layer int, servedAt time.Time) {
	if key == "" {
		return
	}
	if servedAt.IsZero() {
		servedAt = StalePushNow()
	}
	s := &staleServe{key: key, layer: layer, servedAt: servedAt}
	if st, ok := dirtyStampFor(key); ok && st != nil {
		s.needSeq, s.seq = true, st.seq
	}
	limit := defaultResolvedCacheMaxEntries
	if c := ResolvedCache(); c != nil && c.maxEntries > 0 {
		limit = c.maxEntries
	}
	stalePushStats.serves[layer].Add(1)
	l := stalePush
	l.mu.Lock()
	s.el = l.order.PushBack(s)
	l.byKey[key] = append(l.byKey[key], s)
	stalePushStats.open[layer].Add(1)
	stalePushOpenTotal.Add(1)
	for l.order.Len() > limit {
		l.closeLocked(l.order.Front().Value.(*staleServe), closeDroppedIdx)
	}
	l.mu.Unlock()
	ensureStalePushScanner()
}

// closeLocked removes s from the ledger and counts its close. Caller holds mu.
func (l *stalePushLedger) closeLocked(s *staleServe, closeIdx int) {
	l.order.Remove(s.el)
	serves := l.byKey[s.key]
	for i, o := range serves {
		if o == s {
			serves = append(serves[:i], serves[i+1:]...)
			break
		}
	}
	if len(serves) == 0 {
		delete(l.byKey, s.key)
	} else {
		l.byKey[s.key] = serves
	}
	stalePushStats.open[s.layer].Add(-1)
	stalePushOpenTotal.Add(-1)
	stalePushStats.closes[s.layer][closeIdx].Add(1)
}

// RestartStaleServeClock moves the clock of key's open serves of layer to at
// (never backwards): the #740 resume of a parked debt measures its dependents'
// stale-to-push from the resume.
func RestartStaleServeClock(key, layer string, at time.Time) {
	li, ok := layerIndex(layer)
	if !ok || stalePushOpenTotal.Load() == 0 {
		return
	}
	l := stalePush
	l.mu.Lock()
	for _, s := range l.byKey[key] {
		if s.layer == li && s.servedAt.Before(at) {
			s.servedAt = at
		}
	}
	l.mu.Unlock()
}

// ── replacement ──────────────────────────────────────────────────────────────

// noteStalePushPut is every ACCEPTED store Put of key (the four Put methods,
// after c.mu is released, before the caller publishes): it replaces key's open
// serves the Put corrects (see the file comment).
func noteStalePushPut(ctx context.Context, key string, entry *ResolvedEntry) {
	if stalePushOpenTotal.Load() == 0 || entry == nil || entry.StaleDerived {
		return
	}
	start, hasStart := DepGenStartSeqFromContext(ctx)
	l := stalePush
	l.mu.Lock()
	for _, s := range l.byKey[key] {
		if s.replaced || (s.needSeq && (!hasStart || start < s.seq)) {
			continue
		}
		s.replaced = true
	}
	l.mu.Unlock()
}

// noteStalePushEvicted: key left the store (evictDirtyWindow, under c.mu: the
// ledger lock is a leaf). Its serves are replaced: a refetch is a fresh fill.
func noteStalePushEvicted(key string) {
	if stalePushOpenTotal.Load() == 0 {
		return
	}
	l := stalePush
	l.mu.Lock()
	for _, s := range l.byKey[key] {
		s.replaced = true
	}
	l.mu.Unlock()
}

// ── pushes ───────────────────────────────────────────────────────────────────

// stalePushSample is one closed serve's observation, recorded after the lock.
type stalePushSample struct {
	layer int
	ms    int64
}

// stalePushFrameWritten: the /refreshes handler WROTE a refresh frame for key.
// Every replaced serve of key closes pushed.
func stalePushFrameWritten(key string) {
	if stalePushOpenTotal.Load() == 0 {
		return
	}
	l := stalePush
	now := StalePushNow()
	l.mu.Lock()
	samples := l.pushLocked(key, now, nil)
	l.mu.Unlock()
	recordStalePushSamples(samples)
}

// stalePushResyncWritten: a resync frame was WRITTEN to a stream armed for
// keys (the SPA re-reads every armed key): every replaced serve of them closes
// pushed.
func stalePushResyncWritten(keys map[string]struct{}) {
	if stalePushOpenTotal.Load() == 0 {
		return
	}
	l := stalePush
	now := StalePushNow()
	var samples []stalePushSample
	l.mu.Lock()
	for k := range keys {
		samples = l.pushLocked(k, now, samples)
	}
	l.mu.Unlock()
	recordStalePushSamples(samples)
}

// pushLocked closes key's replaced serves at now. Caller holds mu.
func (l *stalePushLedger) pushLocked(key string, now time.Time, out []stalePushSample) []stalePushSample {
	serves := l.byKey[key]
	if len(serves) == 0 {
		return out
	}
	var done []*staleServe
	for _, s := range serves {
		if s.replaced {
			done = append(done, s)
		}
	}
	for _, s := range done {
		ms := max(0, now.Sub(s.servedAt).Milliseconds())
		if ms > StaleToPushMaxMS && !s.overdue {
			s.overdue = true
			stalePushStats.unreplaced[s.layer].Add(1)
		}
		l.closeLocked(s, closePushedIdx)
		out = append(out, stalePushSample{layer: s.layer, ms: ms})
	}
	return out
}

func recordStalePushSamples(samples []stalePushSample) {
	for _, s := range samples {
		stalePushStats.toPush[s.layer].record(s.ms)
		if fn := stalePushRecorder.Load(); fn != nil {
			(*fn)(StaleLayers[s.layer], s.ms)
		}
	}
}

// ── overdue (the 10 s max) ───────────────────────────────────────────────────

var stalePushScannerOnce sync.Once

// ensureStalePushScanner starts the process-lifetime overdue scanner on the
// first stale serve (never when nothing is ever served stale).
func ensureStalePushScanner() {
	stalePushScannerOnce.Do(func() {
		go func() {
			workclass.ClearGoroutine() // #871: process-lifetime; never charged to the unit that started it
			t := time.NewTicker(stalePushScanInterval)
			defer t.Stop()
			for range t.C {
				scanStalePushOverdue()
			}
		}()
	})
}

// scanStalePushOverdue counts, once each, the open serves past the 10 s max
// whose key a /refreshes stream is armed for. The hub is read with the ledger
// lock released; a serve closed in between is simply gone at the re-check.
func scanStalePushOverdue() {
	if stalePushOpenTotal.Load() == 0 {
		return
	}
	now := StalePushNow()
	l := stalePush
	l.mu.Lock()
	var keys []string
	for k, serves := range l.byKey {
		for _, s := range serves {
			if !s.overdue && now.Sub(s.servedAt).Milliseconds() >= StaleToPushMaxMS {
				keys = append(keys, k)
				break
			}
		}
	}
	l.mu.Unlock()
	viewed := keys[:0]
	for _, k := range keys {
		if HasRefreshSubscriber(k) {
			viewed = append(viewed, k)
		}
	}
	if len(viewed) == 0 {
		return
	}
	var newly [stalePushLayers]int
	var sample [stalePushLayers]string
	var oldest [stalePushLayers]int64
	l.mu.Lock()
	for _, k := range viewed {
		for _, s := range l.byKey[k] {
			age := now.Sub(s.servedAt).Milliseconds()
			if s.overdue || age < StaleToPushMaxMS {
				continue
			}
			s.overdue = true
			stalePushStats.unreplaced[s.layer].Add(1)
			newly[s.layer]++
			if age > oldest[s.layer] {
				oldest[s.layer], sample[s.layer] = age, k
			}
		}
	}
	l.mu.Unlock()
	for li, n := range newly {
		if n == 0 {
			continue
		}
		// One line per layer per scan (metadata only): a failure, never per-serve volume.
		slog.Warn("cache.stale_push_unreplaced",
			slog.String("subsystem", "cache"),
			slog.String("layer", StaleLayers[li]),
			slog.Int("serves", n),
			slog.Int64("oldest_age_ms", oldest[li]),
			slog.String("key_hash", sample[li]),
			slog.String("effect", "a stale-served customer response has not been replaced on its armed /refreshes stream within the 10 s goal max; the tab keeps showing stale data until a frame is written"),
		)
	}
}

// ── histogram ────────────────────────────────────────────────────────────────

// staleMSHistogram is a fixed-bound millisecond histogram (lock-free), whose
// /debug/vars value is the shape restactions/api's histograms export:
// cumulative le_<bound> buckets plus le_inf, count, sum_ms and max_ms.
type staleMSHistogram struct {
	bounds  []int64
	buckets []atomic.Uint64 // buckets[i]: bounds[i-1] < ms ≤ bounds[i]; last = +Inf
	count   atomic.Uint64
	sumMS   atomic.Uint64
	maxMS   atomic.Int64
}

func newStaleMSHistogram(bounds []int64) *staleMSHistogram {
	return &staleMSHistogram{bounds: bounds, buckets: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *staleMSHistogram) record(ms int64) {
	ms = max(ms, 0)
	i := 0
	for i < len(h.bounds) && ms > h.bounds[i] {
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

// StaleToPushHistogram is one layer's histogram snapshot: cumulative counts
// per bound (the last one is +Inf), count, sum and max.
type StaleToPushHistogram struct {
	Bounds  []int64
	Buckets []uint64
	Count   uint64
	SumMS   uint64
	MaxMS   int64
}

func (h *staleMSHistogram) snapshot() StaleToPushHistogram {
	d := StaleToPushHistogram{Bounds: h.bounds, Buckets: make([]uint64, len(h.buckets)),
		Count: h.count.Load(), SumMS: h.sumMS.Load(), MaxMS: h.maxMS.Load()}
	var cum uint64
	for i := range h.buckets {
		cum += h.buckets[i].Load()
		d.Buckets[i] = cum
	}
	return d
}

func (h *staleMSHistogram) reset() {
	for i := range h.buckets {
		h.buckets[i].Store(0)
	}
	h.count.Store(0)
	h.sumMS.Store(0)
	h.maxMS.Store(0)
}

// ExpvarValue renders the snapshot for /debug/vars.
func (d StaleToPushHistogram) ExpvarValue() map[string]any {
	buckets := make(map[string]uint64, len(d.Buckets))
	for i, b := range d.Bounds {
		buckets["le_"+strconv.FormatInt(b, 10)] = d.Buckets[i]
	}
	buckets["le_inf"] = d.Buckets[len(d.Buckets)-1]
	return map[string]any{"buckets": buckets, "count": d.Count, "sum_ms": d.SumMS, "max_ms": d.MaxMS}
}

// ── snapshot + expvar ────────────────────────────────────────────────────────

// StalePushLayerStats is one layer's figures.
type StalePushLayerStats struct {
	Layer      string
	Serves     uint64
	Unreplaced uint64
	Open       int64
	Closes     map[string]uint64 // close label → count
	ToPush     StaleToPushHistogram
}

// StalePushStatsSnapshot reads the family, one entry per layer in StaleLayers
// order — the single source of the expvar keys and the OTLP callback.
func StalePushStatsSnapshot() []StalePushLayerStats {
	out := make([]StalePushLayerStats, 0, stalePushLayers)
	for li, layer := range StaleLayers {
		closes := make(map[string]uint64, stalePushCloseCount)
		for ci, c := range StalePushCloses {
			closes[c] = stalePushStats.closes[li][ci].Load()
		}
		out = append(out, StalePushLayerStats{
			Layer:      layer,
			Serves:     stalePushStats.serves[li].Load(),
			Unreplaced: stalePushStats.unreplaced[li].Load(),
			Open:       stalePushStats.open[li].Load(),
			Closes:     closes,
			ToPush:     stalePushStats.toPush[li].snapshot(),
		})
	}
	return out
}

// StaleToPushSnapshot is one layer's histogram (a zero histogram for a layer
// outside the set).
func StaleToPushSnapshot(layer string) StaleToPushHistogram {
	li, ok := layerIndex(layer)
	if !ok {
		return newStaleMSHistogram(StaleToPushBoundsMS).snapshot()
	}
	return stalePushStats.toPush[li].snapshot()
}

var stalePushExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerStalePushExpvar()
}

func registerStalePushExpvar() {
	stalePushExpvarOnce.Do(func() {
		byLayer := func(f func(StalePushLayerStats) any) expvar.Func {
			return func() any {
				m := map[string]any{}
				for _, s := range StalePushStatsSnapshot() {
					m[s.Layer] = f(s)
				}
				return m
			}
		}
		expvar.Publish("snowplow_stale_to_push_ms", byLayer(func(s StalePushLayerStats) any { return s.ToPush.ExpvarValue() }))
		expvar.Publish("snowplow_stale_serves_total", byLayer(func(s StalePushLayerStats) any { return s.Serves }))
		expvar.Publish("snowplow_stale_unreplaced_total", byLayer(func(s StalePushLayerStats) any { return s.Unreplaced }))
		expvar.Publish("snowplow_stale_serves_open", byLayer(func(s StalePushLayerStats) any { return s.Open }))
		expvar.Publish("snowplow_stale_serve_closes_total", byLayer(func(s StalePushLayerStats) any { return s.Closes }))
	})
}

// RegisterStalePushExpvarForTest forces the registration under tests that set
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterStalePushExpvarForTest() { registerStalePushExpvar() }

// ── test seams ───────────────────────────────────────────────────────────────

// ScanStalePushOverdueForTest runs one overdue scan now. Production MUST NOT
// use it (the scanner goroutine runs it).
func ScanStalePushOverdueForTest() { scanStalePushOverdue() }

// AddStaleToPushForTest records one stale_to_push sample of layer through the
// production record path (histogram + OTLP hook), so a cross-package arm can
// prove the mirrors read the live histogram. false: not a layer of the set.
// Production MUST NOT use it.
func AddStaleToPushForTest(layer string, ms int64) bool {
	li, ok := layerIndex(layer)
	if !ok {
		return false
	}
	recordStalePushSamples([]stalePushSample{{layer: li, ms: ms}})
	return true
}

// ResetStalePushForTest drops every open serve and zeroes the family.
// Production MUST NOT use it.
func ResetStalePushForTest() {
	l := stalePush
	l.mu.Lock()
	l.byKey = map[string][]*staleServe{}
	l.order = list.New()
	stalePushOpenTotal.Store(0)
	l.mu.Unlock()
	for li := 0; li < stalePushLayers; li++ {
		stalePushStats.serves[li].Store(0)
		stalePushStats.unreplaced[li].Store(0)
		stalePushStats.open[li].Store(0)
		for ci := 0; ci < stalePushCloseCount; ci++ {
			stalePushStats.closes[li][ci].Store(0)
		}
		stalePushStats.toPush[li].reset()
	}
}
