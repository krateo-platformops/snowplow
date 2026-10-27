// Package workclass names the kind of work a goroutine is doing, for CPU
// profiles and for /debug/vars. #871, owner ruling 2026-10-25 "measure first".
//
// WHY. The #871 pre-build check (issuecomment-6396882049) could attribute storm
// CPU only by the ROOT frame of each stack. That hides two things: a refresher
// refill runs its stages on errgroup goroutines whose root is errgroup, and a
// prewarm walk's 4-wide stage work lands in the same errgroup bucket. A label
// set when a unit of work STARTS reaches every goroutine the unit starts, so a
// CPU sample on any of them names the unit's class.
//
// WHAT. Every background unit runs under the pprof label
//
//	snowplow_work = refresh | refresh_viewed | prewarm | drain | seed | comparator | fill_bg
//
// and every customer resolve-path /call under snowplow_work=customer. The same
// units are counted, started and completed per class, in the expvar
// snowplow_work_class (outside the C7-reserved snowplow_refresher_ prefix), so
// the bench can read the unit counts without a profile.
//
// MEASUREMENT ONLY. Nothing here schedules, waits, yields or locks: a label is
// a pointer on the goroutine and a value on the context, and a counter is an
// atomic add. No code reads the label to decide anything.
//
// HOW A LABEL TRAVELS (Go runtime semantics this package relies on):
//   - A goroutine inherits its creator's labels at the `go` statement. So
//     errgroup workers, flight leaders and any other goroutine a unit starts
//     carry the unit's class with no code at the spawn site.
//   - pprof.Do(ctx, …) sets the goroutine's labels to ctx's labels plus its
//     own, and on return RESETS them to ctx's labels. A pprof.Do on a context
//     that does not carry the work label therefore drops it for the rest of
//     the goroutine. That is why a unit puts the label on its context too, and
//     why a site that detaches to context.Background() must use Background or
//     Detach below instead.
//   - A goroutine that lives longer than the unit that happened to start it (an
//     informer started lazily by a /call, a periodic summary started on first
//     use) would be charged to that unit for ever. Such goroutines call
//     ClearGoroutine first.
package workclass

import (
	"context"
	"expvar"
	"runtime/pprof"
	"sync/atomic"
)

// Key is the pprof label key every unit carries.
const Key = "snowplow_work"

// Header carries a background unit's class across snowplow's own HTTP
// self-loopback (resolve.go egress → restactions.go / widgets.go ingest). The
// ingest honours it only on a trusted self-loopback; it is a label, never an
// input to any decision.
const Header = "X-Snowplow-Work-Class"

// Class is the kind of work a unit does.
type Class uint8

const (
	// None is "not inside a unit": no label.
	None Class = iota
	// Customer is a resolve-path /call a user is waiting on (restactions /
	// widgets ServeHTTP, the same population markCustomerInFlight counts).
	Customer
	// Refresh is one refresher dequeue of the normal or cluster_list tier.
	Refresh
	// RefreshViewed is one refresher dequeue of the #765 viewed tier.
	RefreshViewed
	// Prewarm is one prewarm-engine scope (boot, gvr-discovered, keepwarm,
	// rbac-shift, learned-class), the discovery walk included.
	Prewarm
	// Drain is one pagination-drain job or re-collection candidate.
	Drain
	// Seed is one seed target (engine seed and reseed: measuredSeedUnit).
	Seed
	// Comparator is one #854 comparator pass of an incremental list cell.
	Comparator
	// FillBG is detached fill work no user waits on: a known-stale demand
	// revalidation, a stale-serve debt refill or dependent replace, a stale
	// retry chain attempt, and a cluster-list async populate a customer started.
	FillBG

	numClasses
)

var names = [numClasses]string{
	None:          "",
	Customer:      "customer",
	Refresh:       "refresh",
	RefreshViewed: "refresh_viewed",
	Prewarm:       "prewarm",
	Drain:         "drain",
	Seed:          "seed",
	Comparator:    "comparator",
	FillBG:        "fill_bg",
}

// Classes is every labelled class, in expvar order.
var Classes = []Class{Customer, Refresh, RefreshViewed, Prewarm, Drain, Seed, Comparator, FillBG}

// String is the label value ("" for None).
func (c Class) String() string {
	if c >= numClasses {
		return ""
	}
	return names[c]
}

// Background reports whether c is background work (not None, not Customer).
func (c Class) Background() bool { return c > Customer && c < numClasses }

// Parse maps a label value back to its class.
func Parse(s string) (Class, bool) {
	for _, c := range Classes {
		if names[c] == s {
			return c, true
		}
	}
	return None, false
}

var (
	// sets[c] is c's label set, built once: pprof.Labels allocates.
	sets [numClasses]pprof.LabelSet
	// bg[c] is context.Background() carrying c's label, built once: a detached
	// site derives its context from it at no cost.
	bg [numClasses]context.Context

	started   [numClasses]atomic.Uint64
	completed [numClasses]atomic.Uint64
)

func init() {
	bg[None] = context.Background()
	for _, c := range Classes {
		sets[c] = pprof.Labels(Key, names[c])
		bg[c] = pprof.WithLabels(context.Background(), sets[c])
	}
	expvar.Publish("snowplow_work_class", expvar.Func(func() any { return Snapshot() }))
}

// Counts is one class's unit counters.
type Counts struct {
	Started   uint64 `json:"started"`
	Completed uint64 `json:"completed"`
}

// Snapshot is the snowplow_work_class expvar body: every class, always
// present (an absent class would read as zero work, not as "not measured").
func Snapshot() map[string]Counts {
	out := make(map[string]Counts, len(Classes))
	for _, c := range Classes {
		out[names[c]] = Counts{Started: started[c].Load(), Completed: completed[c].Load()}
	}
	return out
}

// Of is the class ctx carries (None when unlabelled).
func Of(ctx context.Context) Class {
	if ctx == nil {
		return None
	}
	v, ok := pprof.Label(ctx, Key)
	if !ok {
		return None
	}
	c, _ := Parse(v)
	return c
}

// Do runs f as one unit of class c: f's context and goroutine carry the label,
// every goroutine f starts inherits it, and the unit is counted. The caller's
// labels are restored when f returns.
func Do(ctx context.Context, c Class, f func(context.Context)) {
	ctx, end := Begin(ctx, c)
	defer end()
	f(ctx)
}

// Begin starts a unit of class c on the calling goroutine — the form for a
// call site where wrapping the unit in a closure would restructure a long
// function. It returns ctx carrying the label (the unit must run on it, or on a
// context derived from it) and end, which counts the completion and restores
// the goroutine's labels to the caller's ctx. end must run on the same
// goroutine, exactly once (defer it).
func Begin(ctx context.Context, c Class) (context.Context, func()) {
	if c == None || c >= numClasses {
		return ctx, func() {}
	}
	started[c].Add(1)
	lctx := pprof.WithLabels(ctx, sets[c])
	pprof.SetGoroutineLabels(lctx)
	return lctx, func() {
		completed[c].Add(1)
		pprof.SetGoroutineLabels(ctx)
	}
}

// Continue labels the calling goroutine and ctx with c WITHOUT counting a new
// unit: the same unit, continued on the far side of a hop that loses the
// label (the HTTP self-loopback; a detached goroutine a background unit
// started). end restores the caller's labels.
func Continue(ctx context.Context, c Class) (context.Context, func()) {
	if c == None || c >= numClasses {
		return ctx, func() {}
	}
	lctx := pprof.WithLabels(ctx, sets[c])
	pprof.SetGoroutineLabels(lctx)
	return lctx, func() { pprof.SetGoroutineLabels(ctx) }
}

// Label is ctx carrying c's label, for a context handed to code that labels its
// own goroutine (a re-entered ServeHTTP restores the goroutine to its request
// context's labels when it returns). It neither labels the calling goroutine
// nor counts a unit.
func Label(ctx context.Context, c Class) context.Context {
	if c == None || c >= numClasses {
		return ctx
	}
	return pprof.WithLabels(ctx, sets[c])
}

// Background is context.Background() carrying c's label (no allocation). A
// detached site that would start from context.Background() starts from this,
// so a pprof.Do further down keeps the label.
func Background(c Class) context.Context {
	if c >= numClasses {
		return bg[None]
	}
	return bg[c]
}

// Detach starts detached work from parent on the CALLING goroutine (call it
// first thing in the new goroutine) and returns a label-carrying replacement
// for context.Background(), plus end (defer it).
//
// The class rule: work a background unit detached is still that unit's work
// (its class, continued, not a new unit); work a customer — or unlabelled code
// — detached is nobody's request any more: it is a new FillBG unit.
func Detach(parent context.Context) (context.Context, func()) {
	if c := Of(parent); c.Background() {
		return Continue(context.Background(), c)
	}
	return Begin(context.Background(), FillBG)
}

// ClearGoroutine drops every pprof label from the calling goroutine. A
// long-lived goroutine that may be started lazily from inside a unit (an
// informer, a dep-event worker, a periodic summary) calls it first, so its
// CPU is not charged to whichever unit happened to start it.
func ClearGoroutine() { pprof.SetGoroutineLabels(context.Background()) }
