// store_swap_stats.go — #870: the build-and-swap instruments.
//
// Two surfaces, by shape:
//
//   - the SCALAR counters are a tagged stats family (stats_by_tag.go):
//     expvar snowplow_store_swap, OTLP snowplow_store_swap{stat=...}, derived
//     from StoreSwapStats' tags so the two can never drift;
//   - the LABELLED known-stale family (owner ruling 3: `reason` × `outcome`,
//     `reason` × `close`, per-reason gauges, two per-entry histograms) cannot
//     ride a stat tag (the tag system has no labels), so it is hand-wired on
//     OTLP (internal/metrics/store_swap_metrics.go, #311) and published on
//     expvar as snowplow_store_swap_known_stale.
//
// The labelled families are CLOSED sets fixed at init (StoreSwapReasons ×
// StoreSwapOutcomes / StoreSwapCloses), so every series exists from the first
// scrape and its zero is a reading, not an absence. No key, body or identity
// reaches any series.
package cache

import (
	"expvar"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// labelled is a fixed two-label counter family.
type labelled struct {
	cells map[[2]string]*atomic.Uint64
}

func newLabelled(a, b []string) *labelled {
	l := &labelled{cells: map[[2]string]*atomic.Uint64{}}
	for _, x := range a {
		for _, y := range b {
			l.cells[[2]string{x, y}] = new(atomic.Uint64)
		}
	}
	return l
}

func (l *labelled) add(a, b string, n uint64) {
	if c := l.cells[[2]string{a, b}]; c != nil {
		c.Add(n)
	}
}

func (l *labelled) get(a, b string) uint64 {
	if c := l.cells[[2]string{a, b}]; c != nil {
		return c.Load()
	}
	return 0
}

func (l *labelled) reset() {
	for _, c := range l.cells {
		c.Store(0)
	}
}

// histCell is one {count, sum, max} of a per-entry histogram, for expvar. The
// OTLP histogram is recorded through storeSwapCloseRecorder.
type histCell struct {
	count, sumMS, maxMS atomic.Int64
}

type swapStats struct {
	keys     *labelled // reason × outcome: store_swap_known_stale_keys_total
	builds   *labelled // reason × outcome: store_swap_known_stale_builds_total
	windowNS *labelled // reason × outcome: store_swap_known_stale_window_seconds_total (ns)
	closes   *labelled // reason × close: ledger entries closed

	detectHist  map[[2]string]*histCell // reason × close: detect → close
	installHist map[[2]string]*histCell // reason × close: install → close

	swaps         atomic.Uint64
	superseded    atomic.Uint64
	deliveries    atomic.Uint64
	mKeys         atomic.Uint64
	reused        atomic.Uint64
	regressedKeys atomic.Uint64
	schemaUnowned atomic.Uint64
}

var storeSwapStats = newSwapStats()

func newSwapStats() *swapStats {
	s := &swapStats{
		keys:        newLabelled(StoreSwapReasons, StoreSwapOutcomes),
		builds:      newLabelled(StoreSwapReasons, StoreSwapOutcomes),
		windowNS:    newLabelled(StoreSwapReasons, StoreSwapOutcomes),
		closes:      newLabelled(StoreSwapReasons, StoreSwapCloses),
		detectHist:  map[[2]string]*histCell{},
		installHist: map[[2]string]*histCell{},
	}
	for _, r := range StoreSwapReasons {
		for _, c := range StoreSwapCloses {
			s.detectHist[[2]string{r, c}] = &histCell{}
			s.installHist[[2]string{r, c}] = &histCell{}
		}
	}
	return s
}

// ── the scalar family (tagged) ───────────────────────────────────────────────

// StoreSwapStats is the scalar build-and-swap family (snowplow_store_swap).
type StoreSwapStats struct {
	Swaps         uint64 `stat:"store_swaps_total" desc:"Store builds installed: a replacement built beside the live informer and swapped in (#870). Each is a repair or a schema change that left the GVR servable throughout."`
	Superseded    uint64 `stat:"store_swap_superseded_total" desc:"Installs superseded by a later install during their retiring wait; the later swap delivered for both (Q6)."`
	Deliveries    uint64 `stat:"store_swap_deliveries_total" desc:"Swap deliveries the dep handler ACTED on (a key of M whose consumer had not already acted on its current state — v5.1 C1). Equals the swaps' Σ|M| minus the keys a live event delivered first."`
	MKeys         uint64 `stat:"store_swap_m_keys_total" desc:"Σ|M| over all swaps: the keys whose bytes differed between the new copy and what the consumers last acted on. A whole-type mark is gone; this is all a swap marks."`
	Reused        uint64 `stat:"store_swap_reused_objects_total" desc:"Objects a new copy SHARED with the live copy because their bytes were equal (the reuse transform): the new copy costs only what changed."`
	RegressedKeys uint64 `stat:"store_swap_regressed_keys" desc:"Deliveries that stepped a consumer BACK to an older resourceVersion than it had acted on: the new copy synced behind the live one (F8), and the older body came through the swap delivery or through the new copy's lagging watch after the unmute. Each converges as the new copy's watch catches up. Ordered with the API's resourceVersion order (#918); a pair it does not let us order is not counted. Counted, not avoided (design v5 §5)."`
	SchemaUnowned uint64 `stat:"store_swap_schema_unowned_total" desc:"CRD schema changes on a GVR whose informer comes from a shared factory: no copy can be listed under the new schema beside it, so objects listed under the old schema keep being served until that informer re-lists by itself (#928 F5; divergences on such a GVR count store_repair_unsupported_total). Healthy: 0 in production, where the streaming path owns nearly every GVR."`
}

// StoreSwapStatsSnapshot reads the scalar family.
func StoreSwapStatsSnapshot() StoreSwapStats {
	s := storeSwapStats
	return StoreSwapStats{
		Swaps:         s.swaps.Load(),
		Superseded:    s.superseded.Load(),
		Deliveries:    s.deliveries.Load(),
		MKeys:         s.mKeys.Load(),
		Reused:        s.reused.Load(),
		RegressedKeys: s.regressedKeys.Load(),
		SchemaUnowned: s.schemaUnowned.Load(),
	}
}

// StoreSwapStatsByStat is the scalar family as a stat → value map.
func StoreSwapStatsByStat() map[string]any { return statsByTag(StoreSwapStatsSnapshot()) }

// ── the labelled known-stale family ──────────────────────────────────────────

// storeSwapCloseRecorder is the OTLP histogram hook: kind is "detect" or
// "install". nil = no histogram (metrics off).
var storeSwapCloseRecorder atomic.Pointer[func(kind, reason, close string, ms int64)]

// SetStoreSwapCloseRecorder installs the per-entry histogram hook
// (store_swap_detect_to_push_ms / store_swap_install_to_push_ms).
func SetStoreSwapCloseRecorder(fn func(kind, reason, close string, ms int64)) {
	if fn == nil {
		storeSwapCloseRecorder.Store(nil)
		return
	}
	storeSwapCloseRecorder.Store(&fn)
}

// StoreSwapHistBoundsMS are the explicit bounds: they bracket #710's 3 s p95 /
// 10 s max and the 20–35 s rebuild window #918 measured at 50K.
var StoreSwapHistBoundsMS = []float64{100, 250, 500, 1000, 3000, 10000, 30000, 60000, 120000, 300000}

func observeHist(h *histCell, ms int64) {
	h.count.Add(1)
	h.sumMS.Add(ms)
	for {
		cur := h.maxMS.Load()
		if ms <= cur || h.maxMS.CompareAndSwap(cur, ms) {
			return
		}
	}
}

func (s *swapStats) observeClose(r closeRecord) {
	k := [2]string{r.reason, r.close}
	if h := s.detectHist[k]; h != nil {
		observeHist(h, r.detectMS)
	}
	if h := s.installHist[k]; h != nil {
		observeHist(h, r.instMS)
	}
	if fn := storeSwapCloseRecorder.Load(); fn != nil {
		(*fn)("detect", r.reason, r.close, r.detectMS)
		(*fn)("install", r.reason, r.close, r.instMS)
	}
}

// StoreSwapLabelled is one labelled counter cell.
type StoreSwapLabelled struct {
	Reason string
	Label  string // outcome or close
	Value  uint64
}

// StoreSwapReasonGauge is one per-reason gauge reading.
type StoreSwapReasonGauge struct {
	Reason string
	Value  float64
}

// StoreSwapKnownStale is the labelled family's snapshot (OTLP + expvar).
type StoreSwapKnownStale struct {
	Keys           []StoreSwapLabelled // reason × outcome
	Builds         []StoreSwapLabelled // reason × outcome
	WindowMS       []StoreSwapLabelled // reason × outcome, milliseconds
	LedgerCloses   []StoreSwapLabelled // reason × close
	KeysOpen       []StoreSwapReasonGauge
	WindowOpenMaxS []StoreSwapReasonGauge
	LedgerOwed     []StoreSwapReasonGauge
}

// StoreSwapKnownStaleSnapshot reads the labelled family.
func StoreSwapKnownStaleSnapshot() StoreSwapKnownStale {
	s := storeSwapStats
	var out StoreSwapKnownStale
	for _, r := range StoreSwapReasons {
		for _, o := range StoreSwapOutcomes {
			out.Keys = append(out.Keys, StoreSwapLabelled{r, o, s.keys.get(r, o)})
			out.Builds = append(out.Builds, StoreSwapLabelled{r, o, s.builds.get(r, o)})
			out.WindowMS = append(out.WindowMS, StoreSwapLabelled{r, o, s.windowNS.get(r, o) / 1e6})
		}
		for _, c := range StoreSwapCloses {
			out.LedgerCloses = append(out.LedgerCloses, StoreSwapLabelled{r, c, s.closes.get(r, c)})
		}
	}
	keysOpen, winMax, owed := storeSwapGauges()
	for _, r := range StoreSwapReasons {
		out.KeysOpen = append(out.KeysOpen, StoreSwapReasonGauge{r, float64(keysOpen[r])})
		out.WindowOpenMaxS = append(out.WindowOpenMaxS, StoreSwapReasonGauge{r, winMax[r]})
		out.LedgerOwed = append(out.LedgerOwed, StoreSwapReasonGauge{r, float64(owed[r])})
	}
	return out
}

// storeSwapWindowSeconds is the window total for (reason, outcome), in seconds.
func storeSwapWindowSeconds(reason, outcome string) float64 {
	return float64(storeSwapStats.windowNS.get(reason, outcome)) / 1e9
}

func storeSwapKnownStaleExpvar() any {
	snap := StoreSwapKnownStaleSnapshot()
	s := storeSwapStats
	m := map[string]any{}
	keys, builds, win := map[string]uint64{}, map[string]uint64{}, map[string]float64{}
	for _, r := range StoreSwapReasons {
		for _, o := range StoreSwapOutcomes {
			lbl := r + "|" + o
			keys[lbl] = s.keys.get(r, o)
			builds[lbl] = s.builds.get(r, o)
			win[lbl] = storeSwapWindowSeconds(r, o)
		}
	}
	m["store_swap_known_stale_keys_total"] = keys
	m["store_swap_known_stale_builds_total"] = builds
	m["store_swap_known_stale_window_seconds_total"] = win
	closes := map[string]uint64{}
	detect, install := map[string]map[string]int64{}, map[string]map[string]int64{}
	for _, r := range StoreSwapReasons {
		for _, c := range StoreSwapCloses {
			lbl := r + "|" + c
			closes[lbl] = s.closes.get(r, c)
			d, i := s.detectHist[[2]string{r, c}], s.installHist[[2]string{r, c}]
			detect[lbl] = map[string]int64{"count": d.count.Load(), "sum_ms": d.sumMS.Load(), "max_ms": d.maxMS.Load()}
			install[lbl] = map[string]int64{"count": i.count.Load(), "sum_ms": i.sumMS.Load(), "max_ms": i.maxMS.Load()}
		}
	}
	m["store_swap_ledger_closes_total"] = closes
	m["store_swap_detect_to_push_ms"] = detect
	m["store_swap_install_to_push_ms"] = install
	ko, wm, ow := map[string]float64{}, map[string]float64{}, map[string]float64{}
	for i, r := range StoreSwapReasons {
		ko[r] = snap.KeysOpen[i].Value
		wm[r] = snap.WindowOpenMaxS[i].Value
		ow[r] = snap.LedgerOwed[i].Value
	}
	m["store_swap_known_stale_keys_open"] = ko
	m["store_swap_known_stale_window_open_seconds_max"] = wm
	m["store_swap_ledger_owed"] = ow
	return m
}

// ── per-GVR build outcomes (#928 bench review) ───────────────────────────────

// StoreSwapGVROutcomes is the closed set of per-GVR build outcomes; each build
// ends in exactly one. installed: the swap delivered M (step 6) — the GVR was
// swapped; superseded: a later install absorbed it before its step-3 check
// (Q6) and delivered for both; abort_clean / abort_refused / abort_teardown:
// the build ended without a swap.
var StoreSwapGVROutcomes = []string{gvrOutcomeInstalled, gvrOutcomeSuperseded, outcomeAbortClean, outcomeAbortRefused, outcomeAbortTeardown}

const (
	gvrOutcomeInstalled  = "installed"
	gvrOutcomeSuperseded = "superseded"
)

// gvrSwapCells is one GVR's outcome counters, in StoreSwapGVROutcomes order.
type gvrSwapCells struct{ n [5]atomic.Uint64 }

// storeSwapsByGVR: GroupVersionResource.String() → *gvrSwapCells. The label is
// the GVR alone — bounded by the cluster's type set, never an identity.
var storeSwapsByGVR sync.Map

func noteSwapOutcome(gvr schema.GroupVersionResource, outcome string) {
	for i, o := range StoreSwapGVROutcomes {
		if o == outcome {
			v, _ := storeSwapsByGVR.LoadOrStore(gvr.String(), &gvrSwapCells{})
			v.(*gvrSwapCells).n[i].Add(1)
			return
		}
	}
}

// StoreSwapByGVRCell is one (gvr, outcome) reading.
type StoreSwapByGVRCell struct {
	GVR, Outcome string
	Value        uint64
}

// StoreSwapsByGVRSnapshot reads every GVR that has had a build, each with its
// whole closed outcome set (a zero is a reading, not an absence), sorted by
// GVR.
func StoreSwapsByGVRSnapshot() []StoreSwapByGVRCell {
	var gvrs []string
	cells := map[string]*gvrSwapCells{}
	storeSwapsByGVR.Range(func(k, v any) bool {
		gvrs = append(gvrs, k.(string))
		cells[k.(string)] = v.(*gvrSwapCells)
		return true
	})
	sort.Strings(gvrs)
	var out []StoreSwapByGVRCell
	for _, g := range gvrs {
		for i, o := range StoreSwapGVROutcomes {
			out = append(out, StoreSwapByGVRCell{GVR: g, Outcome: o, Value: cells[g].n[i].Load()})
		}
	}
	return out
}

func storeSwapsByGVRExpvar() any {
	m := map[string]map[string]uint64{}
	for _, c := range StoreSwapsByGVRSnapshot() {
		if m[c.GVR] == nil {
			m[c.GVR] = map[string]uint64{}
		}
		m[c.GVR][c.Outcome] = c.Value
	}
	return m
}

// NoteStoreSwapOutcomeForTest moves one GVR's outcome through the production
// counter (the internal/metrics OTLP mirror arm). TEST-ONLY.
func NoteStoreSwapOutcomeForTest(gvr schema.GroupVersionResource, outcome string) {
	noteSwapOutcome(gvr, outcome)
}

var storeSwapExpvarOnce sync.Once

// registerStoreSwapExpvar publishes the tagged family and the labelled
// known-stale family. Once-guarded; RegisterExpvarForTest calls it too.
func registerStoreSwapExpvar() {
	storeSwapExpvarOnce.Do(func() {
		expvar.Publish("snowplow_store_swap", expvar.Func(func() any { return StoreSwapStatsByStat() }))
		expvar.Publish("snowplow_store_swap_known_stale", expvar.Func(storeSwapKnownStaleExpvar))
		expvar.Publish("snowplow_store_swaps_by_gvr", expvar.Func(storeSwapsByGVRExpvar))
	})
}

func init() {
	if Disabled() {
		return
	}
	registerStoreSwapExpvar()
}

// resetStoreSwapStatsForTest zeroes every #870 instrument and the ledger, in
// place (a goroutine of a previous arm may still hold the family).
func resetStoreSwapStatsForTest() {
	s := storeSwapStats
	for _, l := range []*labelled{s.keys, s.builds, s.windowNS, s.closes} {
		l.reset()
	}
	for _, m := range []map[[2]string]*histCell{s.detectHist, s.installHist} {
		for _, h := range m {
			h.count.Store(0)
			h.sumMS.Store(0)
			h.maxMS.Store(0)
		}
	}
	for _, c := range []*atomic.Uint64{&s.swaps, &s.superseded, &s.deliveries, &s.mKeys, &s.reused, &s.regressedKeys, &s.schemaUnowned} {
		c.Store(0)
	}
	storeSwapsByGVR.Range(func(k, _ any) bool {
		storeSwapsByGVR.Delete(k)
		return true
	})
	resetSwapLedgerForTest()
}

// ResetStoreSwapForTest is the cross-package twin (internal/metrics arms).
func ResetStoreSwapForTest() { resetStoreSwapStatsForTest() }

// RunStoreSwapAbortForTest runs one build account through the PRODUCTION
// accounting path — noted at detection, ended as abort_clean after held — so a
// cross-package arm (internal/metrics' OTLP mirror) reads counters moved by the
// real code, not constants. TEST-ONLY.
func RunStoreSwapAbortForTest(gvr schema.GroupVersionResource, reason string, keys []string, held time.Duration) {
	at := windowNow()
	acc := newBuildAccount()
	acc.note(gvr, reason, keys, at)
	acc.endReason(reason, outcomeAbortClean, at.Add(held))
}
