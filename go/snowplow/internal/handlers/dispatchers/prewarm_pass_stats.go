// prewarm_pass_stats.go — #643 PR-A: per-pass detectors for the prewarm engine.
//
// WHY. On 057 the gvr-discovered pass ran for 29h without (as far as anyone
// could tell) ever finishing: 190 scope_incomplete Warn lines over 51 GVRs, and
// nothing else. How many units a pass planned and how many it got through were
// logged at Info, which is not shipped, so whether a pass "fits" its budget was
// inferred, never measured. These detectors make it a reading:
//
//   - snowplow_prewarm_pass_total{kind,outcome}: one count per processed scope,
//     outcome "complete" or "cut". During the defect the gvr-discovered "cut"
//     cell climbs ~7.5/h; it is not a counter whose zero reads as health.
//   - snowplow_prewarm_pass_units_planned / _units_done {kind}: the LAST pass's
//     unit plan and how far it got (resolved + every skip). planned > done on a
//     pass is the gap the 8-minute budget never closes.
//   - snowplow_prewarm_pass_budget_ms / _elapsed_ms {kind}: the last pass's
//     budget and its wall time.
//   - snowplow_prewarm_skip_declined_external_total{kind} and
//     snowplow_prewarm_skip_resume_done_total{kind}: POSITIVE CONTROLS for the
//     #643 skips. A skip that never fires must not pass for a healthy one, so
//     these exist before the skips do. Today only boot's existing #132 F4b
//     Lever A skip bumps skip_declined_external{kind=boot}; every other cell
//     reads 0 until the skip it controls lands.
//
// HOW. processScope installs one *prewarmPassStats on the scope ctx for every
// scope kind. The seed (seedScopeYielding) and the reseed loop (reseedTargets)
// write planned/done into it; processScope reads it after the handler returns
// and publishes. Pure observation: no decision reads any of it.
//
// CLOSED LABEL SETS. kind is the prewarmScopeKind enum, outcome is
// {complete, cut}. Every cell is pre-populated so a 0 is an observation, not a
// missing series.

package dispatchers

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// Pass outcomes (closed set).
const (
	prewarmPassOutcomeComplete = "complete"
	prewarmPassOutcomeCut      = "cut"
)

// prewarmPassKinds is the closed kind label set: every prewarmScopeKind the
// engine dispatches. A kind outside it (an unwired future scope) is not
// recorded, so the label set cannot grow from data.
var prewarmPassKinds = []string{
	string(scopeKindBoot),
	string(scopeKindGVRDiscovered),
	string(scopeKindKeepwarm),
	string(scopeKindRBACShift),
	string(scopeKindLearnedClass),
}

// prewarmPassOutcomes is the closed outcome label set.
var prewarmPassOutcomes = []string{prewarmPassOutcomeComplete, prewarmPassOutcomeCut}

// prewarmPassStats is one pass's unit accounting. planned is set (and, for the
// learned phase, trimmed) by the seed before its units run; done counts every
// unit the pass got through, resolved or skipped. All atomic: the stats are
// written on the worker goroutine and read by processScope on the same one, but
// a test may read them concurrently.
type prewarmPassStats struct {
	planned atomic.Int64
	done    atomic.Int64
	// plannedSet records that some loop declared a plan, so a handler that
	// never reaches a seed (an empty rbac-shift drain, a roots-list failure)
	// does not read as "planned 0, done 0, complete" by accident of zero values.
	plannedSet atomic.Bool

	// The units of this pass that really resolved (a skip never reaches the
	// resolve), and their summed wall cost, split widget / RESTAction — the
	// same measurement measuredSeedUnit feeds the process-wide t_unit means,
	// kept per pass so the per-unit cost can be read per scope kind.
	resolvedWidgetUnits atomic.Int64
	resolvedWidgetNs    atomic.Int64
	resolvedRAUnits     atomic.Int64
	resolvedRANs        atomic.Int64
}

func (p *prewarmPassStats) addResolved(isWidget bool, d time.Duration) {
	if p == nil {
		return
	}
	if isWidget {
		p.resolvedWidgetUnits.Add(1)
		p.resolvedWidgetNs.Add(int64(d))
		return
	}
	p.resolvedRAUnits.Add(1)
	p.resolvedRANs.Add(int64(d))
}

func (p *prewarmPassStats) addPlanned(n int) {
	if p == nil {
		return
	}
	p.plannedSet.Store(true)
	p.planned.Add(int64(n))
}

func (p *prewarmPassStats) unitDone() {
	if p == nil {
		return
	}
	p.done.Add(1)
}

type ctxKeyPrewarmPassStatsType struct{}

var ctxKeyPrewarmPassStats = ctxKeyPrewarmPassStatsType{}

func withPrewarmPassStats(ctx context.Context, p *prewarmPassStats) context.Context {
	if ctx == nil || p == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyPrewarmPassStats, p)
}

// prewarmPassStatsFromContext returns the pass stats processScope installed, or
// nil (the methods are nil-receiver-safe, so a seed run outside the engine —
// a unit test calling seedScopeYielding directly — records nothing).
func prewarmPassStatsFromContext(ctx context.Context) *prewarmPassStats {
	if ctx == nil {
		return nil
	}
	p, _ := ctx.Value(ctxKeyPrewarmPassStats).(*prewarmPassStats)
	return p
}

// PrewarmPassLast is the last pass's reading for one kind.
type PrewarmPassLast struct {
	Kind      string
	Planned   int64
	Done      int64
	BudgetMS  int64
	ElapsedMS int64
	// Units that really resolved this pass and their summed wall cost, per
	// unit class. Per-unit cost for the kind = ResolvedXUS / ResolvedXUnits.
	ResolvedWidgetUnits int64
	ResolvedWidgetUS    int64
	ResolvedRAUnits     int64
	ResolvedRAUS        int64
}

// PrewarmPassCount is one {kind, outcome} cell of pass_total.
type PrewarmPassCount struct {
	Kind    string
	Outcome string
	Count   uint64
}

// PrewarmSkipCount is one {kind} cell of a skip positive-control counter.
type PrewarmSkipCount struct {
	Kind  string
	Count uint64
}

var (
	prewarmPassMu    sync.Mutex
	prewarmPassTotal = newPrewarmPassTotal()
	prewarmPassLast  = map[string]PrewarmPassLast{}

	prewarmSkipMu               sync.Mutex
	prewarmSkipDeclinedExternal = newPrewarmKindCounter()
	prewarmSkipResumeDone       = newPrewarmKindCounter()
)

func newPrewarmPassTotal() map[string]map[string]uint64 {
	m := make(map[string]map[string]uint64, len(prewarmPassKinds))
	for _, k := range prewarmPassKinds {
		m[k] = map[string]uint64{}
		for _, o := range prewarmPassOutcomes {
			m[k][o] = 0
		}
	}
	return m
}

func newPrewarmKindCounter() map[string]uint64 {
	m := make(map[string]uint64, len(prewarmPassKinds))
	for _, k := range prewarmPassKinds {
		m[k] = 0
	}
	return m
}

func knownPrewarmPassKind(kind string) bool {
	for _, k := range prewarmPassKinds {
		if k == kind {
			return true
		}
	}
	return false
}

// prewarmPassOutcome classifies a finished pass. A pass is CUT when its handler
// returned an error (the engine requeues it) OR it declared a plan and did not
// get through it. The second half is not redundant: the rbac-shift and
// learned-class handlers return nil on a ctx cut and re-enqueue themselves, so
// err alone would count their cut passes as complete.
func prewarmPassOutcome(err error, p *prewarmPassStats) string {
	if err != nil {
		return prewarmPassOutcomeCut
	}
	if p != nil && p.plannedSet.Load() && p.done.Load() < p.planned.Load() {
		return prewarmPassOutcomeCut
	}
	return prewarmPassOutcomeComplete
}

// recordPrewarmPass publishes one finished pass. Returns the outcome so the
// caller logs the same classification it counted.
func recordPrewarmPass(kind string, err error, p *prewarmPassStats, budget, elapsed time.Duration) string {
	outcome := prewarmPassOutcome(err, p)
	if !knownPrewarmPassKind(kind) {
		return outcome
	}
	last := PrewarmPassLast{
		Kind:      kind,
		BudgetMS:  budget.Milliseconds(),
		ElapsedMS: elapsed.Milliseconds(),
	}
	if p != nil {
		last.Planned = p.planned.Load()
		last.Done = p.done.Load()
		last.ResolvedWidgetUnits = p.resolvedWidgetUnits.Load()
		last.ResolvedWidgetUS = time.Duration(p.resolvedWidgetNs.Load()).Microseconds()
		last.ResolvedRAUnits = p.resolvedRAUnits.Load()
		last.ResolvedRAUS = time.Duration(p.resolvedRANs.Load()).Microseconds()
	}
	prewarmPassMu.Lock()
	prewarmPassTotal[kind][outcome]++
	prewarmPassLast[kind] = last
	prewarmPassMu.Unlock()
	return outcome
}

// bumpPrewarmSkipDeclinedExternal counts one declined-external skip under the
// scope kind stamped on ctx (cache.WithPrewarmScope, set by the engine's scope
// handler). A seed run outside the engine carries no kind and is not counted.
func bumpPrewarmSkipDeclinedExternal(ctx context.Context) {
	bumpPrewarmKindCounter(ctx, prewarmSkipDeclinedExternal)
}

// prewarmSkipResumeDone has no bump site until #643 PR-C (the resume
// done-set); the counter is published first so its zero is a reading.

func bumpPrewarmKindCounter(ctx context.Context, m map[string]uint64) {
	kind, ok := cache.PrewarmScopeFromContext(ctx)
	if !ok || !knownPrewarmPassKind(kind) {
		return
	}
	prewarmSkipMu.Lock()
	m[kind]++
	prewarmSkipMu.Unlock()
}

// PrewarmPassTotalSnapshot returns every {kind, outcome} cell, zeros included,
// in the closed-set order.
func PrewarmPassTotalSnapshot() []PrewarmPassCount {
	prewarmPassMu.Lock()
	defer prewarmPassMu.Unlock()
	out := make([]PrewarmPassCount, 0, len(prewarmPassKinds)*len(prewarmPassOutcomes))
	for _, k := range prewarmPassKinds {
		for _, o := range prewarmPassOutcomes {
			out = append(out, PrewarmPassCount{Kind: k, Outcome: o, Count: prewarmPassTotal[k][o]})
		}
	}
	return out
}

// PrewarmPassLastSnapshot returns the last pass's reading for every kind that
// has run at least one pass (a kind that never ran has no "last pass").
func PrewarmPassLastSnapshot() []PrewarmPassLast {
	prewarmPassMu.Lock()
	defer prewarmPassMu.Unlock()
	out := make([]PrewarmPassLast, 0, len(prewarmPassLast))
	for _, k := range prewarmPassKinds {
		if l, ok := prewarmPassLast[k]; ok {
			out = append(out, l)
		}
	}
	return out
}

// PrewarmSkipDeclinedExternalSnapshot / PrewarmSkipResumeDoneSnapshot return
// every {kind} cell of the two positive controls, zeros included.
func PrewarmSkipDeclinedExternalSnapshot() []PrewarmSkipCount {
	return prewarmKindCounterSnapshot(prewarmSkipDeclinedExternal)
}

func PrewarmSkipResumeDoneSnapshot() []PrewarmSkipCount {
	return prewarmKindCounterSnapshot(prewarmSkipResumeDone)
}

func prewarmKindCounterSnapshot(m map[string]uint64) []PrewarmSkipCount {
	prewarmSkipMu.Lock()
	defer prewarmSkipMu.Unlock()
	out := make([]PrewarmSkipCount, 0, len(prewarmPassKinds))
	for _, k := range prewarmPassKinds {
		out = append(out, PrewarmSkipCount{Kind: k, Count: m[k]})
	}
	return out
}

// RecordPrewarmPassForTest records one pass of kind through the production
// recordPrewarmPass (planned/done as given, one resolved widget + one resolved
// RA unit; cut forces an error outcome) and one declined-external skip under
// kind — the OTLP falsifier's way to move real series. Production MUST NOT use
// it.
func RecordPrewarmPassForTest(kind string, cut bool, planned, done int, budget, elapsed time.Duration) {
	p := &prewarmPassStats{}
	p.addPlanned(planned)
	for i := 0; i < done; i++ {
		p.unitDone()
	}
	p.addResolved(true, 3*time.Millisecond)
	p.addResolved(false, 5*time.Millisecond)
	var err error
	if cut {
		err = context.DeadlineExceeded
	}
	recordPrewarmPass(kind, err, p, budget, elapsed)
	bumpPrewarmSkipDeclinedExternal(cache.WithPrewarmScope(context.Background(), kind))
}

// resetPrewarmPassStatsForTest zeroes every pass detector. Tests only.
func resetPrewarmPassStatsForTest() {
	prewarmPassMu.Lock()
	prewarmPassTotal = newPrewarmPassTotal()
	prewarmPassLast = map[string]PrewarmPassLast{}
	prewarmPassMu.Unlock()
	prewarmSkipMu.Lock()
	for k := range prewarmSkipDeclinedExternal {
		prewarmSkipDeclinedExternal[k] = 0
		prewarmSkipResumeDone[k] = 0
	}
	prewarmSkipMu.Unlock()
}

var prewarmPassExpvarOnce sync.Once

// CFG-1: published only with the cache on (no engine runs cache-off), from an
// init like every other dispatchers-side publisher.
func init() {
	if cache.Disabled() {
		return
	}
	prewarmPassExpvarOnce.Do(func() {
		expvar.Publish("snowplow_prewarm_pass_total", expvar.Func(func() any {
			m := map[string]map[string]uint64{}
			for _, c := range PrewarmPassTotalSnapshot() {
				if m[c.Kind] == nil {
					m[c.Kind] = map[string]uint64{}
				}
				m[c.Kind][c.Outcome] = c.Count
			}
			return m
		}))
		expvar.Publish("snowplow_prewarm_pass_last", expvar.Func(func() any {
			m := map[string]map[string]int64{}
			for _, l := range PrewarmPassLastSnapshot() {
				m[l.Kind] = map[string]int64{
					"units_planned":         l.Planned,
					"units_done":            l.Done,
					"budget_ms":             l.BudgetMS,
					"elapsed_ms":            l.ElapsedMS,
					"resolved_widget_units": l.ResolvedWidgetUnits,
					"resolved_widget_us":    l.ResolvedWidgetUS,
					"resolved_ra_units":     l.ResolvedRAUnits,
					"resolved_ra_us":        l.ResolvedRAUS,
				}
			}
			return m
		}))
		expvar.Publish("snowplow_prewarm_skip_declined_external_total", expvar.Func(func() any {
			return prewarmKindCounterMap(PrewarmSkipDeclinedExternalSnapshot())
		}))
		expvar.Publish("snowplow_prewarm_skip_resume_done_total", expvar.Func(func() any {
			return prewarmKindCounterMap(PrewarmSkipResumeDoneSnapshot())
		}))
	})
}

func prewarmKindCounterMap(s []PrewarmSkipCount) map[string]uint64 {
	m := make(map[string]uint64, len(s))
	for _, c := range s {
		m[c.Kind] = c.Count
	}
	return m
}
