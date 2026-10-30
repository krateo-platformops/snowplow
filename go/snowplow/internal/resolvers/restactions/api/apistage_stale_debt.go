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
// customer) through the ordinary single-flight fill. It clears when a fill
// whose epoch is at or after the debt's mark commits (its #375 sink startSeq ≥
// the known-stale mark seq — the same rule that ends the known-stale state,
// endKnownStaleOnPut) — and (#740 R1) ANY accepted fill of the content at or
// after that mark retires it, whoever made it (its own refill, the refresher,
// a customer read-through). #740 (owner ruling, architect Q2): a FAILED refill
// joins the content cell's viewer-gated retry chain (cache/stale_retry.go) —
// a refused fill (the generation moved, or a newer mark) is TRANSIENT and
// retried at once, anything else is GENUINE and retried at 1 s doubling to
// 60 s, ONLY while an armed /refreshes tab views a dependent. With nobody
// viewing it the debt is PARKED (architect ruling): the cell is kept and
// served, the record stays, its timer and overdue clock stop, and it is not in
// stale_debt_open (stale_debt_parked counts it). An arm of a dependent or a new
// stale serve joining it resumes it and restarts its overdue clock; an
// eviction of its content cell retires it, compare-and-evicting its
// stale-derived dependents instead of re-resolving them for nobody (#755).
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
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// staleMaxMS is the ratified maximum time from a stale serve to its push
// (owner bar: max ≤ 10 s). A design constant, never an env knob.
const staleMaxMS = 10_000

// StaleToPushBoundsMS are stale_to_push_ms's bucket bounds: they include
// exactly 1000 (the target), 3000 (the p95 bar) and 10000 (the max bar), so
// the bench can decide the bar from /debug/vars.
var StaleToPushBoundsMS = []int64{100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000}

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
	// opened starts the overdue clock of the current VIEWED episode: the first
	// stale serve, then each resume (architect ruling, #740).
	opened     time.Time
	dependents map[*cache.StaleServeSink]staleDependent
	overdue    bool
	// parked (#740): its refill chain was abandoned because nobody views a
	// dependent. The cell is kept and served, the record stays, and the overdue
	// clock stops; an arm of a dependent or a new stale serve joining it resumes
	// it. resumedAt is the latest resume: a resumed debt's stale_to_push is
	// measured from it.
	parked    bool
	resumedAt time.Time
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
// customer dispatch (a stale-serve sink) whose request has not yet resolved a
// RESTAction that DECLARES a write stage (#905, #913 re-review). The mark is
// set from the spec before that RESTAction runs any stage, so it precedes every
// write it can send: a write RESTAction is never stale-derived, and a read
// after any write of the request reads through (a stale serve there would get
// the request withdrawn and re-run, sending the write twice).
//
// This is the fast path. A read and a NESTED write CAN run in parallel (nested
// resolve:true items of one stage, each resolving a RESTAction with its own
// stages): a read may pass here just before a sibling marks the sink. So the
// decision itself is atomic: serveStaleWithDebt notes the stale serve with
// NoteIfNoWrite, under the lock BeginWrite takes — either the read is served
// stale first and the write is held (the request is re-run fresh), or the write
// is begun first and the read reads through.
func staleServeEligible(ctx context.Context) (*cache.StaleServeSink, bool) {
	s := cache.StaleServeSinkFromContext(ctx)
	if s == nil || cache.Inert(ctx) || contentFillClassOf(ctx) != fillClassCustomer || s.DeclaredWrite() {
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
	var resume string
	defer func() {
		staleDebtsMu.Unlock()
		if resume != "" {
			// #740 — a new stale serve joining a PARKED debt is a new reason to
			// retry: resume its chain with an immediate attempt.
			cache.StaleRetryResume(resume)
		}
	}()
	seq, t0, ok := cache.KnownStaleMark(k)
	if !ok {
		return nil, false
	}
	// Metric-neutral read: the response may still be withdrawn at the write
	// point; CommitStaleServe counts it as the request's hit (through Get).
	entry, hit := cache.ServeCell(ctx, store, k, false, dep)
	if !hit || entry == nil {
		return nil, false
	}
	// #905 — the stale serve is noted atomically with BeginWrite: when a write
	// of this request was already sent (a parallel nested item), the read is
	// not served stale (it reads through) and no debt is opened for it.
	if !sink.NoteIfNoWrite(k) {
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
	if d.parked {
		resume = k
	}
	if _, dup := d.dependents[sink]; !dup {
		d.dependents[sink] = staleDependent{servedAt: now}
	}
	sink.NoteServedAt(now)
	staleServeTotal.Add(1)
	// #930 — t0 is the known-stale window's mark, stamped on the dirty-window
	// clock: its age is read on that clock, not on the debt clock (staleNow).
	staleServeAgeHist.record(cache.DirtyWindowNow().Sub(t0).Milliseconds())
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

// runStaleDebt runs d's first refill at once; on success it clears d and
// delivers its pushes, on failure it hands d to the content cell's viewer-gated
// retry chain (#740).
func runStaleDebt(d *staleDebt) {
	defer staleDebtWG.Done()
	// #871 — measurement only: started by a customer's stale serve, inherited
	// its label; detached work no user waits on is fill_bg.
	_, endWork := workclass.Continue(context.Background(), workclass.FillBG)
	defer endWork()
	start := time.Now()
	ok, kind := staleDebtRefillOnce(d)
	if ok {
		clearStaleDebt(d)
		return
	}
	if kind == cache.StaleRetryFresh {
		return // quiesced (test reset only)
	}
	staleDebtRetries.Add(1)
	cache.StaleRetryFailed(d.contentKey, kind, time.Since(start), debtRetrySpec(d))
}

// debtRetrySpec is d's chain: each attempt is one refill (clearing d on
// success); its viewers are d's dependents' armed keys; it lasts while d is
// still the content key's open debt.
func debtRetrySpec(d *staleDebt) cache.StaleRetrySpec {
	return cache.StaleRetrySpec{
		Source: cache.StaleRetryDebt,
		Attempt: func(context.Context) cache.StaleRetryOutcome {
			ok, kind := staleDebtRefillOnce(d)
			if ok {
				clearStaleDebt(d)
				return cache.StaleRetryFresh
			}
			if kind == cache.StaleRetryFresh {
				return cache.StaleRetryFresh // quiesced
			}
			staleDebtRetries.Add(1)
			return kind
		},
		Dependents: func() []string {
			staleDebtsMu.Lock()
			defer staleDebtsMu.Unlock()
			keys := make([]string, 0, len(d.dependents))
			for sink := range d.dependents {
				keys = append(keys, sink.L1Key())
			}
			return keys
		},
		StillStale: func() (bool, bool) {
			staleDebtsMu.Lock()
			open := staleDebts[d.contentKey] == d
			staleDebtsMu.Unlock()
			store := cache.ResolvedCache()
			return open, store != nil && store.Has(d.contentKey)
		},
		// The content cell left the store (evicted, never refreshed again): the
		// stale body the debt stood for is gone, so the debt is retired (PR-B
		// review: a parked debt must not linger on an evicted cell). A PARKED
		// debt's dependents are compare-and-evicted, not re-resolved: nobody
		// views them (#755); an active one keeps the G2 bridge. Runs under the
		// engine lock: retireStaleDebt never calls the engine.
		Evicted: func() { retireStaleDebt(d, true) },
		Park: func() {
			staleDebtsMu.Lock()
			defer staleDebtsMu.Unlock()
			if staleDebts[d.contentKey] == d && !d.parked {
				d.parked = true
				staleDebtOpen.Add(-1)
				staleDebtParked.Add(1)
			}
		},
		Resume: func() {
			staleDebtsMu.Lock()
			defer staleDebtsMu.Unlock()
			if staleDebts[d.contentKey] == d && d.parked {
				now := staleNow()
				d.parked = false
				d.opened, d.overdue, d.resumedAt = now, false, now // a new viewed episode
				staleDebtParked.Add(-1)
				staleDebtOpen.Add(1)
			}
		},
	}
}

// retireStaleDebtOnFill is R1 (#740, architect ruling): ANY accepted fill of
// content key k whose epoch is at or after k's debt's mark retires the debt —
// its own refill, the refresher's forced fill, a customer read-through —
// through the same G2 bridge as its own commit (replace the dependents and
// publish), parked or not, and ends its retry chain. Called at the leader's
// accepted Put; a fill that started before the mark proves nothing.
func retireStaleDebtOnFill(k string, startSeq uint64) {
	staleDebtsMu.Lock()
	d := staleDebts[k]
	retire := d != nil && startSeq >= d.markSeq
	staleDebtsMu.Unlock()
	if !retire {
		return
	}
	clearStaleDebt(d)
	cache.StaleRetryEnd(k)
}

// staleDebtRefillOnce runs one refill attempt through the ordinary content
// fill at the debt class and reports whether it satisfied the debt and, if
// not, the failure's kind (#740 Q4): a fill that completed but was REFUSED (the
// generation moved) or committed under an older mark than the debt's is
// TRANSIENT — a newer change exists, retry at once; anything else is GENUINE.
// StaleRetryFresh with ok=false: quiesced (test reset only).
func staleDebtRefillOnce(d *staleDebt) (bool, cache.StaleRetryOutcome) {
	if g := staleRefillHoldForTest.Load(); g != nil {
		staleDebtStopMu.Lock()
		stop := staleDebtStop
		staleDebtStopMu.Unlock()
		select {
		case <-*g:
		case <-stop:
			return false, cache.StaleRetryFresh
		}
	}
	staleDebtsMu.Lock()
	mark := d.markSeq
	staleDebtsMu.Unlock()
	// #871 — measurement only: one refill attempt is one fill_bg unit. Its ctx
	// carries the label (the content fill's own pprof.Do restores to it), and
	// the goroutine stays fill_bg when it returns.
	ctx, endWork := workclass.Begin(workclass.Background(workclass.FillBG), workclass.FillBG)
	defer endWork()
	ctx, cancel := context.WithTimeout(ctx, cache.DemandRevalidationDeadline())
	defer cancel()
	ctx, out := withFillOutcome(withContentDebtRefill(ctx))
	store := cache.ResolvedCache()
	if store == nil {
		return false, cache.StaleRetryGenuine
	}
	_, _, _, _, err := apistageContentServe(ctx, store, d.call, false)
	out.mu.Lock()
	defer out.mu.Unlock()
	switch {
	case err == nil && out.set && out.stored && out.startSeq >= mark:
		return true, cache.StaleRetryFresh
	case err == nil && out.set:
		return false, cache.StaleRetryTransient
	}
	return false, cache.StaleRetryGenuine
}

// clearStaleDebt retires d (if it is still the key's debt) and replaces every
// COMMITTED dependent now; a dependent still pending (its dispatcher has not
// reached its write point) is replaced at its Commit instead; a withdrawn one
// was never written stale and owes nothing.
func clearStaleDebt(d *staleDebt) { retireStaleDebt(d, false) }

// retireStaleDebt is clearStaleDebt; contentEvicted says d's content cell left
// the store. #755 (architect ruling): a debt PARKED at that moment does not
// re-resolve its committed dependents for nobody — at the customer-triggered
// class, past the cap, with a cold content fill — it compare-and-evicts each
// still stale-derived one (the D2-3b mechanism: a newer entry is never
// touched). Their owed-push marks stay: a tab that arms later gets its
// refresh, and its refetch is an ordinary miss under a viewer. A pending
// dependent is a customer at its write point right now, so it keeps the
// replace at its Commit.
func retireStaleDebt(d *staleDebt, contentEvicted bool) {
	staleDebtsMu.Lock()
	if staleDebts[d.contentKey] != d {
		// Already retired (a concurrent clear, or a test quiesce dropped it).
		staleDebtsMu.Unlock()
		return
	}
	delete(staleDebts, d.contentKey)
	if d.parked {
		staleDebtParked.Add(-1)
	} else {
		staleDebtOpen.Add(-1)
	}
	evict := contentEvicted && d.parked
	resumedAt := d.resumedAt
	sinks := make([]*cache.StaleServeSink, 0, len(d.dependents))
	for sink := range d.dependents {
		sinks = append(sinks, sink)
	}
	staleDebtsMu.Unlock()
	for _, sink := range sinks {
		if !sink.DebtCleared() {
			continue
		}
		if evict {
			evictStaleDependent(sink, resumedAt)
			continue
		}
		goReplaceStaleDependent(sink, resumedAt)
	}
}

// staleParkedEvictHookForTest runs just before a parked retire evicts a
// dependent cell (D2-20g: a tab arms in that window). nil in production.
var staleParkedEvictHookForTest func(l1Key string)

// evictStaleDependent compare-and-evicts sink's cell if it is still the
// stale-derived body (#755), then publishes, exactly as D2-3b's evict does: a
// tab that armed while the cell was still stale-derived found no owed push to
// emit, so this publish is its refresh (an unarmed key keeps its owed mark).
func evictStaleDependent(sink *cache.StaleServeSink, from time.Time) {
	store := cache.ResolvedCache()
	if store == nil {
		return
	}
	l1Key := sink.L1Key()
	//serve-probe: D2 parked retire — is the dependent cell still stale-derived, serves no body
	e, ok := store.GetNoTouch(l1Key)
	if ok && e != nil && !e.StaleDerived {
		return // replaced by a writer, which published
	}
	if ok && e != nil {
		if staleParkedEvictHookForTest != nil {
			staleParkedEvictHookForTest(l1Key)
		}
		if !store.EvictStaleDerived(l1Key, e) {
			//serve-probe: D2 parked retire — did a writer replace the cell in between, serves no body
			if _, still := store.GetNoTouch(l1Key); still {
				return // a writer replaced it in between, and published
			}
		}
	}
	// Evicted now, or already gone (an LRU eviction publishes nothing, and a
	// tab may have armed while the cell was still stale-derived).
	finishStaleReplace(sink, from)
}

// CommitStaleServe is the dispatcher's write-point outcome for a stale-derived
// response whose Put was ACCEPTED (and whose refresh header is therefore
// armed): it sets the key's owed-push mark and, if a content debt already
// cleared, replaces the cell now. Call it before writing the body.
func CommitStaleServe(sink *cache.StaleServeSink) {
	if sink.Commit() {
		goReplaceStaleDependent(sink, time.Time{})
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

// goReplaceStaleDependent replaces sink's cell on its own goroutine. from, when
// later than the serve, is the resume a parked debt cleared after: its
// stale_to_push is measured from there (architect ruling, #740).
func goReplaceStaleDependent(sink *cache.StaleServeSink, from time.Time) {
	staleDebtWG.Add(1)
	go func() {
		defer staleDebtWG.Done()
		// #871 — measurement only: see runStaleDebt.
		_, endWork := workclass.Continue(context.Background(), workclass.FillBG)
		defer endWork()
		replaceStaleDependent(sink, from)
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
//     joins the dependent cell's viewer-gated retry chain (#740): 1 s doubling
//     to 60 s while its tab is armed; past 10 s it is counted overdue once
//     (scanStaleOverdue); with nobody viewing it the chain is abandoned and an
//     arm resumes it.
//
// A tab that arms after this publish is served by the owed-push mark at arm
// time (cache.SubscribeRefreshStream).
func replaceStaleDependent(sink *cache.StaleServeSink, from time.Time) {
	l1Key := sink.L1Key()
	staleDebtsMu.Lock()
	staleReplacing[sink] = &staleReplace{key: l1Key, servedAt: sink.ServedAt()}
	staleDebtsMu.Unlock()
	start := time.Now()
	if !replaceStaleDependentOnce(sink) {
		// #740 — a FAILED re-resolve keeps the cell and joins the dependent
		// cell's viewer-gated retry chain; it stays in staleReplacing (overdue
		// scan) until the chain lets go of it.
		staleReplaceRetries.Add(1)
		cache.StaleRetryFailed(l1Key, cache.StaleRetryGenuine, time.Since(start), replaceRetrySpec(sink, from))
		return
	}
	finishStaleReplace(sink, from)
}

// replaceRetrySpec is a failed dependent replace's chain: each attempt is one
// re-resolve (publishing on success); its viewer is the dependent cell's own
// armed key; letting go of it ends the overdue tracking.
func replaceRetrySpec(sink *cache.StaleServeSink, from time.Time) cache.StaleRetrySpec {
	l1Key := sink.L1Key()
	return cache.StaleRetrySpec{
		Source: cache.StaleRetryDebt,
		Attempt: func(context.Context) cache.StaleRetryOutcome {
			if !replaceStaleDependentOnce(sink) {
				staleReplaceRetries.Add(1)
				return cache.StaleRetryGenuine
			}
			finishStaleReplace(sink, from)
			return cache.StaleRetryFresh
		},
		Dependents: func() []string { return []string{l1Key} },
		// The chain lasts while the dependent cell is still the stale-derived
		// body: once another writer replaced it (or it left the store) there is
		// nothing to replace, and the newer entry is never touched (#740).
		StillStale: func() (bool, bool) {
			store := cache.ResolvedCache()
			if store == nil {
				return false, false
			}
			//serve-probe: D2 replace chain — is the dependent cell still stale-derived, serves no body
			e, ok := store.GetNoTouch(l1Key)
			return ok && e != nil && e.StaleDerived, ok
		},
		Release: func() {
			staleDebtsMu.Lock()
			delete(staleReplacing, sink)
			staleDebtsMu.Unlock()
		},
	}
}

// replaceStaleDependentOnce runs one replace of sink's dependent cell and
// reports whether it is done: replaced, already replaced or evicted, or
// completed with a declined Put (compare-and-evicted). false: it FAILED.
func replaceStaleDependentOnce(sink *cache.StaleServeSink) bool {
	l1Key := sink.L1Key()
	store := cache.ResolvedCache()
	if store == nil {
		return true
	}
	//serve-probe: D2 replace — identity + StaleDerived of the dependent cell, serves no body
	before, ok := store.GetNoTouch(l1Key)
	if !ok || before == nil || !before.StaleDerived {
		return true // evicted (the refetch fills) or already replaced by another writer
	}
	// #871 — measurement only: one replace is one fill_bg unit (see
	// staleDebtRefillOnce).
	wctx, endWork := workclass.Begin(workclass.Background(workclass.FillBG), workclass.FillBG)
	ctx, cancel := context.WithTimeout(cache.WithCustomerTriggeredRefresh(wctx), cache.DemandRevalidationDeadline())
	err := cache.RunRefreshNow(ctx, l1Key)
	cancel()
	endWork()
	if err == nil || errors.Is(err, cache.ErrRefreshNowNoHandler) {
		//serve-probe: D2 replace — was the dependent cell replaced (pointer compare), serves no body
		if after, _ := store.GetNoTouch(l1Key); after == before {
			staleDependentUnresolved.Add(1)
			store.EvictStaleDerived(l1Key, before)
		}
		return true
	}
	return errors.Is(err, cache.ErrRefreshNowNoEntry) // evicted between the read and the re-resolve
}

// finishStaleReplace publishes a done replace's /refreshes signal and stops its
// overdue tracking.
func finishStaleReplace(sink *cache.StaleServeSink, from time.Time) {
	l1Key := sink.L1Key()
	staleDebtsMu.Lock()
	delete(staleReplacing, sink)
	staleDebtsMu.Unlock()
	if from.Before(sink.ServedAt()) {
		from = sink.ServedAt()
	}
	if cache.HasRefreshSubscriber(l1Key) {
		stalePushTotal.Add(1)
		staleToPushHist.record(staleNow().Sub(from).Milliseconds())
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
			workclass.ClearGoroutine() // #871: process-lifetime; never charged to the unit that started it
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
		if d.parked {
			continue // #740 — parked: nobody views it, its overdue clock is stopped
		}
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
	staleServeTotal atomic.Uint64
	staleDebtOpen   atomic.Int64
	// staleDebtParked (#740): debts whose refill chain is parked (nobody views a
	// dependent). Not an alert.
	staleDebtParked          atomic.Int64
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

func init() {
	cache.RegisterStaleServeQuiesce(quiesceStaleDebts)
	// #710 F2 — an owed push delivered at ARM time is a correction too: record
	// its stale_to_push, so late-arming tabs are not dropped from p95/max.
	cache.SetStaleOwedArmRecorder(func(servedAt time.Time) {
		staleToPushHist.record(staleNow().Sub(servedAt).Milliseconds())
	})
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
	// #740 — a viewer-gated retry attempt (a refill or a replace) runs on the
	// engine, not on staleDebtWG: join it first, so it cannot start a D2
	// goroutine after the join below.
	cache.WaitStaleRetryForTest()
	staleDebtWG.Wait()
	staleDebtsMu.Lock()
	staleDebts = map[string]*staleDebt{}
	staleReplacing = map[*cache.StaleServeSink]*staleReplace{}
	staleDebtsMu.Unlock()
	staleDebtOpen.Store(0)
	staleDebtParked.Store(0)
	staleDebtStopMu.Lock()
	staleDebtStop = make(chan struct{})
	staleDebtStopMu.Unlock()
}

// StaleServeStats is the D2 family's state — the single source of the expvar
// keys and the OTLP callback.
type StaleServeStats struct {
	// ServeTotal is stale_serve_total{reason="known_stale"} (the closed set).
	ServeTotal uint64
	// DebtOpen counts ACTIVE debts only; parked ones are DebtParked (#740).
	DebtOpen            int64
	DebtParked          int64
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
		DebtParked:          staleDebtParked.Load(),
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
		expvar.Publish("snowplow_apistage_stale_debt_parked", expvar.Func(func() any {
			return staleDebtParked.Load()
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

// staleRefillHoldForTest, when set, parks every debt refill attempt until the
// channel closes (HoldStaleDebtRefillsForTest). Nil in production.
var staleRefillHoldForTest atomic.Pointer[chan struct{}]

// HoldStaleDebtRefillsForTest parks every debt refill until the returned
// release runs, so a cross-package arm can keep a content cell known-stale
// across a whole request (D2-15b). Production MUST NOT use it.
func HoldStaleDebtRefillsForTest() (release func()) {
	ch := make(chan struct{})
	staleRefillHoldForTest.Store(&ch)
	var once sync.Once
	return func() {
		once.Do(func() {
			staleRefillHoldForTest.Store(nil)
			close(ch)
		})
	}
}

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
	staleDebtParked.Store(0)
	staleDebtRetries.Store(0)
	staleOverdueTotal.Store(0)
	staleDebtOldestAge.Store(0)
	stalePushTotal.Store(0)
	stalePushUnarmed.Store(0)
	staleDependentUnresolved.Store(0)
	staleServeAgeHist.reset()
	staleToPushHist.reset()
}
