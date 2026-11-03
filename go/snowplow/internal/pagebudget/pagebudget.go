// Package pagebudget is a customer request's share of the page-latency goal
// (#1115, owner ruling 2026-11-03: "Bound it by the 10 s goal").
//
// A page never takes longer than PageMaxGoal (#710's bar, #1054's goal). A
// customer request's network exchanges — every stage dial, its retry, a /call
// passthrough, a client-go read — therefore share ONE deadline: the request's
// arrival plus PageMaxGoal. It replaces the library's arbitrary 30 s connect
// timeout and the missing response timeout as the bound a user can wait on.
//
// THE BUDGET BOUNDS THE NETWORK, NOT THE WORK. It is a value on the request's
// context, not a context deadline: in-process work (a JQ filter, a single-flight
// wait, an informer sync wait, an export's serialisation) is not cut by it. Only
// credhttp, which builds every client the module uses (#723 guard), reads it and
// runs each outbound exchange under it.
//
// DETACHING LEAVES THE BUDGET. Work that outlives the request (a #547 demand
// revalidation, a #710 fill leader) detaches with Detach, never with a bare
// context.WithoutCancel: a value survives WithoutCancel, and a detached unit
// must not be cut by a deadline that belonged to the request that started it.
//
// This package is a leaf (standard library only), so every layer — slowcall
// (#1059) included — can import it.
package pagebudget

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// PageMaxGoal is the goal: a page loads in at most 10 s (#710's bar; #1054's
// PageMaxGoal). It is the goal, not a tunable: it moves only if the goal does.
const PageMaxGoal = 10 * time.Second

// PageP95Goal is the goal's typical-case bound: a page loads in p95 ≤ 3 s
// (#710's bar; #1054's PageP95Goal). It is the time the goal allows a healthy
// call to answer in, so it is the retry reserve before any connection has been
// measured (credhttp's reserve).
const PageP95Goal = 3 * time.Second

// ErrExhausted is the cause of an exchange cut by its request's budget. Every
// *ExhaustedError matches it with errors.Is.
var ErrExhausted = errors.New("page budget exhausted")

// Result is what the budget did to a request's exchanges (closed set).
type Result uint32

const (
	// None: no budget (a background unit, or a detached one).
	None Result = iota
	// Within: the request has a budget and it never had to act.
	Within
	// Retried: a connection attempt was abandoned (or failed) and the one
	// retry inside the budget answered.
	Retried
	// NotSent: the budget was spent before an exchange started; nothing was
	// sent.
	NotSent
	// ExhaustedConnect: the budget ran out before a connection was obtained.
	ExhaustedConnect
	// ExhaustedAnswer: the budget ran out after the request was on a
	// connection, before its answer was read.
	ExhaustedAnswer
	numResults
)

var resultNames = [numResults]string{"none", "within", "retried", "not_sent", "exhausted_connect", "exhausted_answer"}

// Results is the closed set, in publication order.
var Results = []Result{None, Within, Retried, NotSent, ExhaustedConnect, ExhaustedAnswer}

func (r Result) String() string {
	if r < numResults {
		return resultNames[r]
	}
	return "unknown"
}

// Budget is one customer request's budget. Its deadline is fixed at entry; its
// result is the most severe one any of its exchanges reported (Results order).
type Budget struct {
	deadline time.Time
	result   atomic.Uint32
}

// Deadline is when the request's network budget runs out.
func (b *Budget) Deadline() time.Time { return b.deadline }

// Note records one exchange's result on the request and counts it.
func (b *Budget) Note(r Result) {
	if r >= numResults {
		return
	}
	exchanges[r].Add(1)
	if b == nil {
		return
	}
	for {
		cur := b.result.Load()
		if uint32(r) <= cur || b.result.CompareAndSwap(cur, uint32(r)) {
			return
		}
	}
}

type ctxKey struct{}

// WithCustomer gives ctx a customer budget: start + PageMaxGoal. start is the
// request's arrival (tests inject an earlier start to leave a short budget).
func WithCustomer(ctx context.Context, start time.Time) context.Context {
	b := &Budget{deadline: start.Add(PageMaxGoal)}
	b.result.Store(uint32(Within))
	return context.WithValue(ctx, ctxKey{}, b)
}

// From returns ctx's budget, if it carries one.
func From(ctx context.Context) (*Budget, bool) {
	if ctx == nil {
		return nil, false
	}
	b, _ := ctx.Value(ctxKey{}).(*Budget)
	return b, b != nil
}

// Outcome is ctx's request result: None without a budget.
func Outcome(ctx context.Context) Result {
	b, ok := From(ctx)
	if !ok {
		return None
	}
	return Result(b.result.Load())
}

// Detach is context.WithoutCancel minus the budget: every other value, none of
// the cancellation, and no customer budget. The ONLY way module code detaches a
// request's context (TestDetach1115_NoBareWithoutCancel).
func Detach(ctx context.Context) context.Context {
	return context.WithValue(context.WithoutCancel(ctx), ctxKey{}, (*Budget)(nil))
}

// ExhaustedError is an exchange its request's budget ended. Its text names the
// phase and the cause of the last attempt (#1101's vocabulary: dns, dial,
// tls_handshake, connection), never a host, a URL or an identity.
type ExhaustedError struct {
	Result   Result // NotSent, ExhaustedConnect or ExhaustedAnswer
	Cause    string // the last attempt's cause; "" when nothing was attempted
	Attempts int
	// MayHaveBeenApplied: a write whose request bytes were written before the
	// budget ran out. It is never re-sent; its outcome is unknown.
	MayHaveBeenApplied bool
}

func (e *ExhaustedError) Error() string {
	var what string
	switch e.Result {
	case NotSent:
		what = "before the request was sent; nothing was sent"
	case ExhaustedConnect:
		what = "while connecting"
	default:
		what = "while waiting for the answer"
	}
	if e.Cause != "" {
		what += " (" + e.Cause + ")"
	}
	if e.Attempts > 0 {
		what += fmt.Sprintf(" after %d attempt(s)", e.Attempts)
	}
	if e.MayHaveBeenApplied {
		what += "; the write was sent and may have been applied"
	}
	return fmt.Sprintf("the page's %s budget ran out %s", PageMaxGoal, what)
}

// Is matches ErrExhausted and context.DeadlineExceeded: a budget cut is a
// deadline (#1059's CauseOf reads it as a timeout).
func (e *ExhaustedError) Is(target error) bool {
	return target == ErrExhausted || target == context.DeadlineExceeded
}

// Timeout marks it as a timeout for net.Error-style checks.
func (e *ExhaustedError) Timeout() bool { return true }

var exchanges [numResults]atomic.Uint64

// ExchangeCount is one snowplow_page_budget_exchanges_total cell.
type ExchangeCount struct {
	Result Result
	Count  uint64
}

// ExchangesSnapshot returns every exchange count by result (None excluded:
// background exchanges are not counted), zeros included.
func ExchangesSnapshot() []ExchangeCount {
	out := make([]ExchangeCount, 0, len(Results)-1)
	for _, r := range Results[1:] {
		out = append(out, ExchangeCount{Result: r, Count: exchanges[r].Load()})
	}
	return out
}
