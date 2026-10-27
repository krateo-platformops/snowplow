// store_swap_stats.go — #870: the build-and-swap counters, on expvar
// (snowplow_store_swap) and, through StoreSwapStatsSnapshot and the histogram
// recorder hook, on hand-wired OTLP instruments (internal/metrics). The
// labelled families are CLOSED sets fixed at init (StoreSwapReasons ×
// StoreSwapOutcomes / StoreSwapCloses), so every series exists from the first
// scrape and its zero is a reading, not an absence. No key, body or identity
// reaches any series.
package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
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

	swaps         atomic.Uint64 // installs
	superseded    atomic.Uint64 // installs a later install superseded during the retiring wait
	deliveries    atomic.Uint64 // swap deliveries acted on by the dep handler (= |M| minus C1 skips)
	mKeys         atomic.Uint64 // Σ |M|
	reused        atomic.Uint64 // objects the reuse transform shared with the live copy
	regressedKeys atomic.Uint64 // F8: a swap pushed a body older than the consumer had
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

// StoreSwapStats is the snapshot the OTLP callback and expvar read.
type StoreSwapStats struct {
	KnownStaleKeys    []StoreSwapLabelled // reason × outcome
	KnownStaleBuilds  []StoreSwapLabelled // reason × outcome
	KnownStaleWindowS []StoreSwapLabelled // reason × outcome, Value in ms (seconds = Value/1000)
	LedgerCloses      []StoreSwapLabelled // reason × close
	KeysOpen          []StoreSwapReasonGauge
	WindowOpenMaxS    []StoreSwapReasonGauge
	LedgerOwed        []StoreSwapReasonGauge

	Swaps, Superseded, Deliveries, MKeys, Reused, RegressedKeys uint64
}

// StoreSwapStatsSnapshot reads every #870 instrument.
func StoreSwapStatsSnapshot() StoreSwapStats {
	s := storeSwapStats
	var out StoreSwapStats
	for _, r := range StoreSwapReasons {
		for _, o := range StoreSwapOutcomes {
			out.KnownStaleKeys = append(out.KnownStaleKeys, StoreSwapLabelled{r, o, s.keys.get(r, o)})
			out.KnownStaleBuilds = append(out.KnownStaleBuilds, StoreSwapLabelled{r, o, s.builds.get(r, o)})
			out.KnownStaleWindowS = append(out.KnownStaleWindowS, StoreSwapLabelled{r, o, s.windowNS.get(r, o) / 1e6})
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
	out.Swaps = s.swaps.Load()
	out.Superseded = s.superseded.Load()
	out.Deliveries = s.deliveries.Load()
	out.MKeys = s.mKeys.Load()
	out.Reused = s.reused.Load()
	out.RegressedKeys = s.regressedKeys.Load()
	return out
}

// storeSwapWindowSeconds is the window total for (reason, outcome), in seconds.
func storeSwapWindowSeconds(reason, outcome string) float64 {
	return float64(storeSwapStats.windowNS.get(reason, outcome)) / 1e9
}

func storeSwapExpvar() any {
	snap := StoreSwapStatsSnapshot()
	s := storeSwapStats
	m := map[string]any{
		"store_swaps_total":               snap.Swaps,
		"store_swap_superseded_total":     snap.Superseded,
		"store_swap_deliveries_total":     snap.Deliveries,
		"store_swap_m_keys_total":         snap.MKeys,
		"store_swap_reused_objects_total": snap.Reused,
		"store_swap_regressed_keys":       snap.RegressedKeys,
	}
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

var storeSwapExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	storeSwapExpvarOnce.Do(func() {
		expvar.Publish("snowplow_store_swap", expvar.Func(storeSwapExpvar))
	})
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
	for _, c := range []*atomic.Uint64{&s.swaps, &s.superseded, &s.deliveries, &s.mKeys, &s.reused, &s.regressedKeys} {
		c.Store(0)
	}
	resetSwapLedgerForTest()
}

// ResetStoreSwapForTest is the cross-package twin (internal/metrics arms).
func ResetStoreSwapForTest() { resetStoreSwapStatsForTest() }
