// apistage_stale_debt.go — #710 PR-2b: the D2 stale serve and its debt.
//
// OWNER RULINGS (#710, issuecomment-6187374111): "No one waits in queue", and
// for a known-stale content cell under an ARMABLE customer resolve "Yes, show
// then replace". Diego: "Serving stale data is correct only if the updated
// value will be served later in few seconds"; the bar is stale_to_push p95 ≤
// 3 s, max ≤ 10 s, with 1 s as the target.
//
// THE STALE SERVE (decided in apistageContentServeAttempt's readContent). A
// CUSTOMER read whose resolve carries a cache.StaleServeSink (installed only by
// an armable customer dispatcher: restactions, widgets) and that finds its
// identity-free content cell KNOWN-STALE (#547) is served that cell AT ONCE —
// stale — instead of reading through. In ONE critical section under the debt
// registry lock it (1) re-checks the known-stale mark, (2) reads the cell, (3)
// registers the dependent customer key on the cell's DEBT (opening it, with
// its refill, if none), and (4) notes the stale serve on the sink. The
// registration therefore precedes the response, and a refill's commit — which
// clears the stamp BEFORE it takes the registry lock — either beat the
// decision (no stamp: no stale serve, the fresh cell is read) or sees this
// dependent (D2-7). Any other reader keeps #547's read-through on an unqueued
// fill: a background resolve, an inert one, a resolve with no armable cell (no
// push could ever correct the screen).
//
// THE DEBT (G1). One per content key, opened by its first stale serve. Its
// refill runs at once at the DEBT class (it bypasses the admission cap like a
// customer) through the ordinary single-flight fill, and is NON-DROPPABLE: a
// failed attempt retries on #690's backoff (10 ms doubling to a 5 s cap),
// counted, until a fill whose epoch is at or after the debt's mark commits
// (its #375 sink startSeq ≥ the known-stale mark seq — the same rule that
// closes the known-stale window, closeKnownStaleOnPut). Only then does the
// debt clear.
//
// THE PUSH (G2). On clear, each dependent customer key is re-resolved at once
// through its class's refresh handler (cache.RunRefreshNow — outside the
// refresher queue: never behind background work, never dropped), which
// replaces the stale-derived cell and publishes. The /refreshes signal is then
// delivered ONCE THE TAB HAS ARMED the key: a dependent cell is Put and armed
// only after its response is written, which can be after a fast refill, and a
// publish before the subscription exists reaches nobody. So delivery waits
// (bounded by staleMaxMS from the serve) for the cell to exist, re-resolves
// it, then waits for the subscriber and publishes — in EVERY outcome: a
// re-resolve that did not replace the cell hands the key to the refresher as
// well and still publishes (the refetch then revalidates).
//
// OVERDUE (G3). A debt open longer than staleMaxMS is counted ONCE
// (stale_overdue_total), logged ONCE at WARN with metadata only, and stays
// open and non-droppable. No breaker: a breaker would make a user wait.
//
// Nothing here logs or publishes a cache body, a user, a group or an object
// name: content keys are hashes, GVRs and namespaces are coordinates.
package api

import (
	"context"
	"errors"
	"expvar"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// staleMaxMS is the ratified maximum time from a stale serve to its push
// (owner bar: max ≤ 10 s). A design constant, never an env knob.
const staleMaxMS = 10_000

// StaleToPushBoundsMS are stale_to_push_ms's bucket bounds: they include
// exactly 1000 (the target), 3000 (the p95 bar) and 10000 (the max bar), so
// the bench can decide the bar from /debug/vars.
var StaleToPushBoundsMS = []int64{100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000}

// staleDebtBackoffMin / Max: #690's retry backoff.
const (
	staleDebtBackoffMin = 10 * time.Millisecond
	staleDebtBackoffMax = 5 * time.Second
)

// staleClock is the debt clock (seamed in tests; read by debt goroutines, so
// atomic).
var staleClock atomic.Pointer[func() time.Time]

func staleNow() time.Time {
	if f := staleClock.Load(); f != nil {
		return (*f)()
	}
	return time.Now()
}

// setStaleClockForTest installs a fake debt clock; restore with nil. Test-only.
func setStaleClockForTest(f func() time.Time) {
	if f == nil {
		staleClock.Store(nil)
		return
	}
	staleClock.Store(&f)
}

// staleServeRegistrationHookForTest runs between a stale serve's read and its
// registration (D2-7). nil in production.
var staleServeRegistrationHookForTest func()

// --- ctx markers --------------------------------------------------------------

type ctxKeyContentDebtRefillType struct{}

var ctxKeyContentDebtRefill = ctxKeyContentDebtRefillType{}

// withContentDebtRefill marks ctx as a debt's refill: it forces the content
// miss, fills at the debt class (bypassing the cap) and skips the serve gate
// (there is no requester).
func withContentDebtRefill(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyContentDebtRefill, true)
}

func contentDebtRefillFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(ctxKeyContentDebtRefill).(bool)
	return v
}

// fillOutcome is what a fill (led or joined) reports to a debt refill.
type fillOutcome struct {
	mu       sync.Mutex
	set      bool
	stored   bool
	startSeq uint64
}

type ctxKeyFillOutcomeType struct{}

var ctxKeyFillOutcome = ctxKeyFillOutcomeType{}

func withFillOutcome(ctx context.Context) (context.Context, *fillOutcome) {
	o := &fillOutcome{}
	return context.WithValue(ctx, ctxKeyFillOutcome, o), o
}

// noteFillOutcome records the flight a debt refill took part in: whether its
// Put was accepted, and its epoch. No sink → no-op.
func noteFillOutcome(ctx context.Context, stored bool, startSeq uint64) {
	o, _ := ctx.Value(ctxKeyFillOutcome).(*fillOutcome)
	if o == nil {
		return
	}
	o.mu.Lock()
	o.set, o.stored, o.startSeq = true, stored, startSeq
	o.mu.Unlock()
}

// --- the registry -------------------------------------------------------------

// staleDependent is one customer response served stale from a debt's
// content: its sink carries the outermost dispatcher's armed L1 key and its
// write-point outcome (pending, committed, withdrawn).
type staleDependent struct {
	servedAt time.Time
}

// staleDebt is one content key's open refill obligation.
type staleDebt struct {
	contentKey string
	gvr        string
	call       httpcall.RequestOptions
	markSeq    uint64
	opened     time.Time
	dependents map[*cache.StaleServeSink]staleDependent
	overdue    bool
}

// staleReplace is one committed dependent whose replace is in flight (it may
// be retrying a failed re-resolve); the overdue scanner reads it.
type staleReplace struct {
	key      string
	servedAt time.Time
	overdue  bool
}

var (
	staleDebtsMu   sync.Mutex
	staleDebts     = map[string]*staleDebt{}
	staleReplacing = map[*cache.StaleServeSink]*staleReplace{}
)

// staleServeEligible reports whether ctx's resolve may be served stale: a
// CUSTOMER read (not background, not a refill, not inert) under an armable
// customer dispatch (a stale-serve sink).
func staleServeEligible(ctx context.Context) (*cache.StaleServeSink, bool) {
	s := cache.StaleServeSinkFromContext(ctx)
	if s == nil || cache.Inert(ctx) || contentFillClassOf(ctx) != fillClassCustomer {
		return nil, false
	}
	return s, true
}

// serveStaleWithDebt is the D2 decision for content key k: under the registry
// lock, if k is still known-stale, read its cell, register sink's customer key
// as a dependent of k's debt (opening the debt and its refill if needed) and
// note the stale serve. ok=false: not known-stale (or not resident) — the
// caller reads as usual.
func serveStaleWithDebt(ctx context.Context, sink *cache.StaleServeSink, store *cache.ResolvedCacheStore, k string, call httpcall.RequestOptions, gvr string, dep cache.DepKey) (*cache.ResolvedEntry, bool) {
	if cache.Inert(ctx) {
		return nil, false // a dry run never opens a debt
	}
	staleDebtsMu.Lock()
	defer staleDebtsMu.Unlock()
	seq, t0, ok := cache.KnownStaleMark(k)
	if !ok {
		return nil, false
	}
	entry, hit := cache.ServeCell(ctx, store, k, true, dep)
	if !hit || entry == nil {
		return nil, false
	}
	if staleServeRegistrationHookForTest != nil {
		staleServeRegistrationHookForTest() // D2-7: a refill may commit here; it waits on this lock
	}
	now := staleNow()
	d := staleDebts[k]
	if d == nil {
		d = &staleDebt{
			contentKey: k,
			gvr:        gvr,
			call:       contentRefillCall(call),
			markSeq:    seq,
			opened:     now,
			dependents: map[*cache.StaleServeSink]staleDependent{},
		}
		staleDebts[k] = d
		staleDebtOpen.Add(1)
		staleDebtWG.Add(1)
		go runStaleDebt(d)
	} else if seq > d.markSeq {
		// A later mark: the refill must now start at or after it.
		d.markSeq = seq
	}
	if _, dup := d.dependents[sink]; !dup {
		d.dependents[sink] = staleDependent{servedAt: now}
	}
	sink.NoteServedAt(now)
	sink.Note(k)
	staleServeTotal.Add(1)
	staleServeAgeHist.record(now.Sub(t0).Milliseconds())
	ensureStaleOverdueScanner()
	return entry, true
}

// contentRefillCall is the content call a refill re-dispatches: the same path
// and query, a plain GET.
func contentRefillCall(call httpcall.RequestOptions) httpcall.RequestOptions {
	return httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{
		Path: call.Path,
		Verb: ptr.To(http.MethodGet),
	}}
}

// --- the refill (G1) ----------------------------------------------------------

// runStaleDebt refills d until a fill whose epoch is at or after d's mark
// commits, then clears d and delivers its pushes. Non-droppable.
func runStaleDebt(d *staleDebt) {
	defer staleDebtWG.Done()
	backoff := staleDebtBackoffMin
	for {
		if staleDebtRefillOnce(d) {
			clearStaleDebt(d)
			return
		}
		staleDebtRetries.Add(1)
		if !staleDebtSleep(backoff) {
			return // quiesced (test reset only)
		}
		backoff = min(2*backoff, staleDebtBackoffMax)
	}
}

// staleDebtRefillOnce runs one refill attempt through the ordinary content
// fill at the debt class and reports whether it satisfied the debt.
func staleDebtRefillOnce(d *staleDebt) bool {
	staleDebtsMu.Lock()
	mark := d.markSeq
	staleDebtsMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), cache.DemandRevalidationDeadline())
	defer cancel()
	ctx, out := withFillOutcome(withContentDebtRefill(ctx))
	store := cache.ResolvedCache()
	if store == nil {
		return false
	}
	_, _, _, err := apistageContentServe(ctx, store, d.call, false)
	out.mu.Lock()
	defer out.mu.Unlock()
	return err == nil && out.set && out.stored && out.startSeq >= mark
}

// clearStaleDebt retires d (if it is still the key's debt) and replaces every
// COMMITTED dependent now; a dependent still pending (its dispatcher has not
// reached its write point) is replaced at its Commit instead; a withdrawn one
// was never written stale and owes nothing.
func clearStaleDebt(d *staleDebt) {
	staleDebtsMu.Lock()
	if staleDebts[d.contentKey] == d {
		delete(staleDebts, d.contentKey)
		staleDebtOpen.Add(-1)
	}
	sinks := make([]*cache.StaleServeSink, 0, len(d.dependents))
	for sink := range d.dependents {
		sinks = append(sinks, sink)
	}
	staleDebtsMu.Unlock()
	for _, sink := range sinks {
		if sink.DebtCleared() {
			goReplaceStaleDependent(sink)
		}
	}
}

// CommitStaleServe is the dispatcher's write-point outcome for a stale-derived
// response whose Put was ACCEPTED (and whose refresh header is therefore
// armed): it sets the key's owed-push mark and, if a content debt already
// cleared, replaces the cell now. Call it before writing the body.
func CommitStaleServe(sink *cache.StaleServeSink) {
	if sink.Commit() {
		goReplaceStaleDependent(sink)
	}
}

// WithdrawStaleServe is the dispatcher's write-point outcome for a
// stale-derived response whose Put was DECLINED: no push could ever reach it,
// so the stale body is never written — the dispatcher re-runs the resolve with
// D2 disabled (cache.WithStaleServeDisabled) and serves that fresh body. The
// content debt still clears; only this dependent is dropped.
func WithdrawStaleServe(sink *cache.StaleServeSink) {
	if sink.Withdraw() {
		staleServeWithdrawnUnarmable.Add(1)
	}
}

func goReplaceStaleDependent(sink *cache.StaleServeSink) {
	staleDebtWG.Add(1)
	go func() {
		defer staleDebtWG.Done()
		replaceStaleDependent(sink)
	}()
}

// replaceStaleDependent re-resolves one committed dependent cell at once and
// publishes its /refreshes signal. Why a re-resolve did not replace the cell
// decides what happens to it (architect ruling, #710 PR-2b):
//
//   - it COMPLETED but its Put was DECLINED (stage error, external / UAF /
//     sensitive / ext-TTL, or no handler for the class): the stale-derived cell
//     is compare-and-evicted, so the pushed refetch is a miss that fills from
//     the now-fresh content rather than a hit on the same stale body;
//   - it FAILED (transport, timeout, ctx, a resolve error): the cell is KEPT
//     (#547: a failed revalidation keeps serving, never evicts) and the replace
//     retries on #690's backoff, non-droppable; past 10 s it is counted
//     overdue once (scanStaleOverdue).
//
// A tab that arms after this publish is served by the owed-push mark at arm
// time (cache.SubscribeRefreshStream).
func replaceStaleDependent(sink *cache.StaleServeSink) {
	l1Key := sink.L1Key()
	staleDebtsMu.Lock()
	staleReplacing[sink] = &staleReplace{key: l1Key, servedAt: sink.ServedAt()}
	staleDebtsMu.Unlock()
	defer func() {
		staleDebtsMu.Lock()
		delete(staleReplacing, sink)
		staleDebtsMu.Unlock()
	}()
	backoff := staleDebtBackoffMin
	for {
		store := cache.ResolvedCache()
		if store == nil {
			return
		}
		//serve-probe: D2 replace — identity + StaleDerived of the dependent cell, serves no body
		before, ok := store.GetNoTouch(l1Key)
		if !ok || before == nil || !before.StaleDerived {
			break // evicted (the refetch fills) or already replaced by another writer
		}
		ctx, cancel := context.WithTimeout(context.Background(), cache.DemandRevalidationDeadline())
		err := cache.RunRefreshNow(ctx, l1Key)
		cancel()
		if err == nil || errors.Is(err, cache.ErrRefreshNowNoHandler) {
			//serve-probe: D2 replace — was the dependent cell replaced (pointer compare), serves no body
			if after, _ := store.GetNoTouch(l1Key); after == before {
				staleDependentUnresolved.Add(1)
				store.EvictStaleDerived(l1Key, before)
			}
			break
		}
		if errors.Is(err, cache.ErrRefreshNowNoEntry) {
			break // evicted between the read and the re-resolve
		}
		staleReplaceRetries.Add(1)
		if !staleDebtSleep(backoff) {
			return // quiesced (test reset only)
		}
		backoff = min(2*backoff, staleDebtBackoffMax)
	}
	if cache.HasRefreshSubscriber(l1Key) {
		stalePushTotal.Add(1)
		staleToPushHist.record(staleNow().Sub(sink.ServedAt()).Milliseconds())
	} else {
		// Not armed yet: the owed-push mark announces it when the tab arms.
		stalePushUnarmed.Add(1)
	}
	cache.PublishRefresh(l1Key)
}

// --- overdue (G3) -------------------------------------------------------------

var staleOverdueOnce sync.Once

// ensureStaleOverdueScanner starts the process-lifetime overdue scanner on
// the first stale serve (never when D2 never fires).
func ensureStaleOverdueScanner() {
	staleOverdueOnce.Do(func() {
		go func() {
			t := time.NewTicker(250 * time.Millisecond)
			defer t.Stop()
			for range t.C {
				scanStaleOverdue()
			}
		}()
	})
}

// scanStaleOverdue counts every debt that crossed staleMaxMS, once, and
// refreshes the oldest-age gauge.
func scanStaleOverdue() {
	now := staleNow()
	staleDebtsMu.Lock()
	var oldest int64
	var newly []*staleDebt
	for _, d := range staleDebts {
		age := now.Sub(d.opened).Milliseconds()
		oldest = max(oldest, age)
		if !d.overdue && age >= staleMaxMS {
			d.overdue = true
			newly = append(newly, d)
		}
	}
	var newlyRe []*staleReplace
	for _, r := range staleReplacing {
		if r.servedAt.IsZero() {
			continue
		}
		age := now.Sub(r.servedAt).Milliseconds()
		oldest = max(oldest, age)
		if !r.overdue && age >= staleMaxMS {
			r.overdue = true
			newlyRe = append(newlyRe, r)
		}
	}
	staleDebtOldestAge.Store(oldest)
	staleDebtsMu.Unlock()
	for _, r := range newlyRe {
		staleOverdueTotal.Add(1)
		slog.Warn("apistage.stale_replace_overdue",
			slog.String("subsystem", "cache"),
			slog.String("key_hash", r.key),
			slog.Int64("age_ms", now.Sub(r.servedAt).Milliseconds()),
			slog.String("effect", "a stale-served customer cell has not been replaced within the 10 s bar; its re-resolve keeps failing and is retried (the cell is kept, never evicted on failure)"),
		)
	}
	for _, d := range newly {
		staleOverdueTotal.Add(1)
		slog.Warn("apistage.stale_debt_overdue",
			slog.String("subsystem", "cache"),
			slog.String("gvr", d.gvr),
			slog.String("key_hash", d.contentKey),
			slog.Int64("age_ms", now.Sub(d.opened).Milliseconds()),
			slog.Int("dependents", len(d.dependents)),
			slog.String("effect", "a stale-served content cell has not been refilled within the 10 s bar; the debt stays open and keeps retrying"),
		)
	}
}

// --- metrics (G3) -------------------------------------------------------------

var (
	staleServeTotal          atomic.Uint64
	staleDebtOpen            atomic.Int64
	staleDebtRetries         atomic.Uint64
	staleOverdueTotal        atomic.Uint64
	staleDebtOldestAge       atomic.Int64
	stalePushTotal           atomic.Uint64
	stalePushUnarmed         atomic.Uint64
	staleDependentUnresolved atomic.Uint64
	// staleReplaceRetries counts failed dependent re-resolves retried.
	staleReplaceRetries atomic.Uint64
	// staleServeWithdrawnUnarmable is stale_serve_withdrawn_total{reason=
	// "unarmable"}: a stale serve whose Put was declined, re-run fresh.
	staleServeWithdrawnUnarmable atomic.Uint64
	staleServeAgeHist            = newMSHistogramBounds(StaleToPushBoundsMS)
	staleToPushHist              = newMSHistogramBounds(StaleToPushBoundsMS)

	// staleDebtWG joins every debt goroutine (tests wait it out).
	staleDebtWG sync.WaitGroup

	// staleDebtStop is closed only by quiesceStaleDebts (a test reset of the
	// singletons the debt goroutines read); every debt wait selects on it.
	staleDebtStopMu sync.Mutex
	staleDebtStop   = make(chan struct{})
)

func init() { cache.RegisterStaleServeQuiesce(quiesceStaleDebts) }

// staleDebtSleep waits d; false when the debts are being quiesced.
func staleDebtSleep(d time.Duration) bool {
	staleDebtStopMu.Lock()
	stop := staleDebtStop
	staleDebtStopMu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-stop:
		return false
	}
}

// quiesceStaleDebts stops every debt goroutine at its next wait, joins them,
// and drops the open debts. It is the cache's registered D2 quiesce, run ONLY
// by the test resets of the store, refresher and deps singletons (a process
// never resets them). A running refill attempt or re-resolve finishes first;
// both are bounded by DemandRevalidationDeadline.
func quiesceStaleDebts() {
	staleDebtStopMu.Lock()
	close(staleDebtStop)
	staleDebtStopMu.Unlock()
	staleDebtWG.Wait()
	staleDebtsMu.Lock()
	staleDebts = map[string]*staleDebt{}
	staleReplacing = map[*cache.StaleServeSink]*staleReplace{}
	staleDebtsMu.Unlock()
	staleDebtOpen.Store(0)
	staleDebtStopMu.Lock()
	staleDebtStop = make(chan struct{})
	staleDebtStopMu.Unlock()
}

// StaleServeStats is the D2 family's state — the single source of the expvar
// keys and the OTLP callback.
type StaleServeStats struct {
	// ServeTotal is stale_serve_total{reason="known_stale"} (the closed set).
	ServeTotal          uint64
	DebtOpen            int64
	DebtRetries         uint64
	OverdueTotal        uint64
	DebtOldestAgeMS     int64
	PushTotal           uint64
	PushUnarmed         uint64
	DependentUnresolved uint64
	// WithdrawnUnarmable is stale_serve_withdrawn_total{reason="unarmable"}.
	WithdrawnUnarmable uint64
	// ReplaceRetries is stale_replace_retries_total: failed dependent
	// re-resolves retried (the cell kept, never evicted on failure).
	ReplaceRetries uint64
	// Owed is the owed-push mark family (cache.StaleOwedStats): open marks,
	// marks dropped by the L1-capacity bound (a guarantee failure, expected
	// 0), pushes emitted at arm time, marks cleared by a delivered publish.
	Owed     cache.StaleOwedStats
	ServeAge ContentFillDurationStats
	ToPush   ContentFillDurationStats
}

// StaleServeStatsSnapshot returns the family's state.
func StaleServeStatsSnapshot() StaleServeStats {
	return StaleServeStats{
		ServeTotal:          staleServeTotal.Load(),
		DebtOpen:            staleDebtOpen.Load(),
		DebtRetries:         staleDebtRetries.Load(),
		OverdueTotal:        staleOverdueTotal.Load(),
		DebtOldestAgeMS:     staleDebtOldestAge.Load(),
		PushTotal:           stalePushTotal.Load(),
		PushUnarmed:         stalePushUnarmed.Load(),
		DependentUnresolved: staleDependentUnresolved.Load(),
		WithdrawnUnarmable:  staleServeWithdrawnUnarmable.Load(),
		ReplaceRetries:      staleReplaceRetries.Load(),
		Owed:                cache.StaleOwedStatsSnapshot(),
		ServeAge:            staleServeAgeHist.snapshot(),
		ToPush:              staleToPushHist.snapshot(),
	}
}

// SetStaleToPushRecorder / SetStaleServeAgeRecorder install the OTLP
// histogram hooks (this package cannot import metrics).
func SetStaleToPushRecorder(fn func(ms int64))   { staleToPushHist.setRecorder(fn) }
func SetStaleServeAgeRecorder(fn func(ms int64)) { staleServeAgeHist.setRecorder(fn) }

var staleExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerStaleExpvar()
}

func registerStaleExpvar() {
	staleExpvarOnce.Do(func() {
		expvar.Publish("snowplow_apistage_stale_serve_total", expvar.Func(func() any {
			return map[string]uint64{"known_stale": staleServeTotal.Load()}
		}))
		expvar.Publish("snowplow_apistage_stale_serve_age_ms", expvar.Func(func() any {
			return staleServeAgeHist.expvarValue()
		}))
		expvar.Publish("snowplow_apistage_stale_to_push_ms", expvar.Func(func() any {
			return staleToPushHist.expvarValue()
		}))
		expvar.Publish("snowplow_apistage_stale_debt_open", expvar.Func(func() any {
			return staleDebtOpen.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_debt_retries_total", expvar.Func(func() any {
			return staleDebtRetries.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_overdue_total", expvar.Func(func() any {
			return staleOverdueTotal.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_debt_oldest_age_ms", expvar.Func(func() any {
			return staleDebtOldestAge.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_push_total", expvar.Func(func() any {
			return stalePushTotal.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_push_unarmed_total", expvar.Func(func() any {
			return stalePushUnarmed.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_dependent_unresolved_total", expvar.Func(func() any {
			return staleDependentUnresolved.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_replace_retries_total", expvar.Func(func() any {
			return staleReplaceRetries.Load()
		}))
		expvar.Publish("snowplow_apistage_stale_serve_withdrawn_total", expvar.Func(func() any {
			return map[string]uint64{"unarmable": staleServeWithdrawnUnarmable.Load()}
		}))
		expvar.Publish("snowplow_apistage_stale_owed_open", expvar.Func(func() any {
			return cache.StaleOwedStatsSnapshot().Open
		}))
		expvar.Publish("snowplow_apistage_stale_owed_dropped_total", expvar.Func(func() any {
			return cache.StaleOwedStatsSnapshot().Dropped
		}))
		expvar.Publish("snowplow_apistage_stale_owed_arm_emits_total", expvar.Func(func() any {
			return cache.StaleOwedStatsSnapshot().ArmEmits
		}))
	})
}

// RegisterStaleExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterStaleExpvarForTest() { registerStaleExpvar() }

// WaitStaleDebtsForTest blocks until every debt goroutine (refill and push
// delivery) has returned — a cross-package fixture calls it before tearing down
// the refresher, the store or the broadcaster those goroutines use. Production
// MUST NOT use it.
func WaitStaleDebtsForTest() { staleDebtWG.Wait() }

// AddStaleServeForTest moves the stale-serve and overdue counters and records
// one stale_to_push sample through their production paths, so a cross-package
// arm can prove the OTLP mirror reads the LIVE values. Production MUST NOT use
// it.
func AddStaleServeForTest(serves, overdue uint64, toPushMS int64) {
	staleServeTotal.Add(serves)
	staleOverdueTotal.Add(overdue)
	staleToPushHist.record(toPushMS)
}

// ResetStaleServeForTest stops and joins every debt goroutine and zeroes the
// family (owed marks included). Production MUST NOT use it.
func ResetStaleServeForTest() {
	quiesceStaleDebts()
	cache.ResetStaleOwedForTest()
	staleServeWithdrawnUnarmable.Store(0)
	staleReplaceRetries.Store(0)
	staleServeTotal.Store(0)
	staleDebtOpen.Store(0)
	staleDebtRetries.Store(0)
	staleOverdueTotal.Store(0)
	staleDebtOldestAge.Store(0)
	stalePushTotal.Store(0)
	stalePushUnarmed.Store(0)
	staleDependentUnresolved.Store(0)
	staleServeAgeHist.reset()
	staleToPushHist.reset()
}
