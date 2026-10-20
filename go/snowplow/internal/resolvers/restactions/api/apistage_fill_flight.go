// apistage_fill_flight.go — #710 PR-1: single-flight the apistage content fill.
//
// #710: N readers of one missing or known-stale content cell each built the
// whole-list envelope and decoded it — N × list memory, one tree kept. PR-0
// measured it on main: 200 readers of one known-stale 5K list → 200 fills, 199
// duplicates, 113 envelopes in flight at once.
//
// A FLIGHT is one fill (dispatch + parse + PutIfGen) of one content key that
// other readers of the same key may JOIN instead of filling themselves. A
// joiner gets the flight's result — the raw envelope and the parsed items —
// and runs its own serve-time gate over it, exactly as a content HIT does over
// entry.Items (Ship 2a: the parsed items are shared and immutable). JQ
// therefore sees byte-identical trees to the leader's.
//
// THE JOIN RULE (epoch at entry, #375 / #547). A reader may join a flight only
// if the flight's read covers every change the reader's own fill would have
// seen. The flight records startSeq, the dep-event seq at its entry (the
// content cell's own #375 sink startSeq, captured before its data read). The
// reader joins iff its cell's coordinate has NOT moved since startSeq
// (cache.ContentCoordinateMovedSince). An event on the coordinate after the
// flight started — the change that made the cell known-stale, a refresher
// trigger, any write in between — means the flight may have read the old
// body, so the reader does not join; it leads the next flight, which replaces
// this one as the key's current flight. Nobody is served a body older than the
// one its own fill would have produced, and #547's "a known-stale input is
// read through, never served stale" holds for joiners too.
//
// The rule is uniform over the three fill reasons: a known-stale mark and a
// refresher trigger both come from a dep event on the coordinate, and a plain
// miss with no event since the flight began has nothing newer to see. A cold
// coordinate (no forward bucket yet) is checked against its GVR floor, the
// conservative #375 fix (ii): any event on the GVR refuses the join.
//
// The residual torn window — an event whose seq is already past the flight's
// startSeq but whose bucket stamp has not landed when the joiner checks — is
// the same one every content HIT has today, and it heals the same way: the
// joiner Records the coordinate on its own resolve's sink, whose Put-check
// (remarkIfDepsMoved) sees the stamp and remarks the joiner's cell.
//
// CANCELLATION. The leader runs the fill under a DETACHED ctx bounded by
// cache.DemandRevalidationDeadline (the server write deadline: the longest any
// customer resolve may run), so a leader whose request goes away cannot fail
// its joiners. A joiner's ctx cancels only its own wait.
//
// No admission bound here: that is #710 PR-2.
//
// #765 option A — THE STAMP-GATED READER. A refresher re-resolve whose trigger
// set holds the cell's GVR (contentStampGate.on) must be fresh only as of its
// dequeue (need, cache.RefreshTriggerAsOf). It joins the current flight when
// the epoch rule above admits it OR the flight's startSeq ≥ need (its read
// began after every trigger the refresh consumed). Refused, a BACKGROUND-class
// stamp-gated reader does not start a concurrent fill of the same list: it
// joins the key's PENDING flight, creating it if none — a flight queued behind
// the current one whose leader, once the current flight completes, captures its
// own content sink (a fresh startSeq) and fills. Every pending joiner joined
// before that capture, so the pending fill is never older than any joiner's
// need: lossless. Per key that is at most one flight in flight plus one
// pending, whatever the number of dependents a change dirtied. Customer- and
// debt-class readers (#710: users never queue; #740 K5 customer-triggered
// refreshes, #547 demand revalidation, the D2 refill) never wait on a pending
// flight: they keep the path above.
package api

import (
	"context"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// contentFlightResult is what a flight hands its joiners.
type contentFlightResult struct {
	// ok is false when the dispatch was not pivot-servable (the joiner falls
	// through exactly as the leader did) or the leader exited without a result.
	ok         bool
	envelope   []byte
	parsed     parsedListEnvelope
	haveParsed bool
	// stored is true when the leader's PutIfGen was accepted; entry is the
	// stored cell. A joiner Records its dep only then, as the leader does (#189).
	stored bool
	entry  *cache.ResolvedEntry
	// err (#710 PR-2a) is the flight's admission failure: every reader in it
	// errors with it (D1).
	err error
}

// contentStampGate is a stamp-gated (#765) reader's state: on for a refresher
// read whose trigger set holds the cell's GVR, need the seq it must be fresh as
// of.
type contentStampGate struct {
	on   bool
	need uint64
	// viewed (#799 fix 1): a viewed-tier refresh (cache.ViewedRefreshFromContext).
	viewed bool
}

// contentFlight is one in-flight fill of one content key.
type contentFlight struct {
	key string
	// startSeq is the flight's read epoch. A PENDING flight (#765) sets it only
	// when it starts, under contentFlightsMu; it is read under the same lock, or
	// after done is closed.
	startSeq uint64
	// prior (#765) is, for a pending flight, the flight it is queued behind.
	prior *contentFlight
	// ticket (#710 PR-2a) is the flight's place in the memory-admission gate;
	// its class is the max over the flight's participants.
	ticket *fillTicket
	done   chan struct{}
	once   sync.Once
	res    contentFlightResult
}

var (
	contentFlightsMu sync.Mutex
	// contentFlights is each content key's CURRENT flight: the one new readers
	// may join. A flight a reader refused (the coordinate moved) is replaced
	// here but still completes for the joiners it already has. Bounded by the
	// concurrent fills.
	contentFlights = map[string]*contentFlight{}
	// contentPendingFlights (#765) is each content key's PENDING flight: queued
	// behind the current one, joinable by background stamp-gated readers. At most
	// one per key; bounded by the concurrent fills.
	contentPendingFlights = map[string]*contentFlight{}
)

// enterContentFlight is the MISS branch's entry. It installs the content cell's
// #375 (A) child dep-gen sink (fillCtx) BEFORE any data read — its startSeq is
// the epoch of the flight this reader would lead — and joins or leads the key's
// flight. fillCtx is meaningful only to a leader.
//
// #765 — a reader that LEADS a pending flight blocks here until the flight it is
// queued behind completes, then captures its content sink and starts.
func enterContentFlight(ctx context.Context, key string, gvr schema.GroupVersionResource, namespace, name string, reason contentFillReason, stamp contentStampGate) (flight *contentFlight, fillCtx context.Context, lead bool) {
	fillCtx = cache.WithContentDepGenSink(ctx, gvr, namespace, name)
	startSeq, _ := cache.DepGenStartSeqFromContext(fillCtx)
	flight, lead = joinOrLeadContentFill(key, startSeq, gvr, namespace, name, reason, contentFillClassOf(ctx), stamp)
	if lead && flight.prior != nil {
		// The prior flight is bounded by its own detached write deadline, so this
		// wait is too; it is not cut by this reader's ctx, because the pending
		// flight's joiners are waiting on THIS leader.
		<-flight.prior.done
		fillCtx = cache.WithContentDepGenSink(ctx, gvr, namespace, name)
		startSeq, _ = cache.DepGenStartSeqFromContext(fillCtx)
		startPendingContentFlight(flight, startSeq)
	}
	return flight, fillCtx, lead
}

// startPendingContentFlight turns a pending flight into its key's current one
// at read epoch startSeq. From here new readers judge it by the ordinary join
// rule, and a refused background reader queues the NEXT pending flight behind
// it.
func startPendingContentFlight(f *contentFlight, startSeq uint64) {
	contentFlightsMu.Lock()
	defer contentFlightsMu.Unlock()
	f.startSeq = startSeq
	f.prior = nil
	if contentPendingFlights[f.key] == f {
		delete(contentPendingFlights, f.key)
	}
	contentFlights[f.key] = f
}

// joinOrLeadContentFill returns the flight the reader is in and whether it
// LEADS it. startSeq is the leader-to-be's content-sink entry seq; it is used
// only when the reader leads. (gvr, namespace, name) is the cell's own
// coordinate (name "" = LIST).
//
// #765 — stamp (a stamp-gated refresher reader) adds the need-based join and,
// for the background class only, the pending flight (see the file comment). A
// returned flight with a non-nil prior that the reader LEADS is pending: the
// caller (enterContentFlight) waits for prior before filling.
func joinOrLeadContentFill(key string, startSeq uint64, gvr schema.GroupVersionResource, namespace, name string, reason contentFillReason, class fillAdmissionClass, stamp contentStampGate) (*contentFlight, bool) {
	contentFlightsMu.Lock()
	defer contentFlightsMu.Unlock()
	cur := contentFlights[key]
	if cur != nil {
		if !cache.ContentCoordinateMovedSince(gvr, namespace, name, cur.startSeq) ||
			(stamp.on && cur.startSeq >= stamp.need) {
			noteContentFillJoined(reason, gvr.String())
			// #710 PR-2a — a joiner of a higher class promotes a queued flight, or
			// coalescing would let a customer wait at background priority.
			theContentFillGate().promote(cur.ticket, class)
			return cur, false
		}
		noteContentFillJoinRefused(reason)
	}
	// #799 fix 1 — a viewed-tier refresh (a passive viewer, no customer request
	// waiting) coalesces too, at its own (debt) admission class: without it, N
	// viewed refreshes under churn led N concurrent re-fills of the same list.
	coalesce := stamp.on && (class == fillClassBackground || stamp.viewed)
	if coalesce {
		// A pending flight has not captured its read epoch yet: it will start
		// after this reader's need, whatever that is.
		if p := contentPendingFlights[key]; p != nil {
			noteContentRefreshCoalesced(gvr.String())
			return p, false
		}
		if cur != nil {
			p := &contentFlight{key: key, prior: cur, done: make(chan struct{}), ticket: newFillTicket(class)}
			contentPendingFlights[key] = p
			return p, true
		}
	}
	f := &contentFlight{key: key, startSeq: startSeq, done: make(chan struct{}), ticket: newFillTicket(class)}
	contentFlights[key] = f
	return f, true
}

// complete publishes the flight's result to its joiners and retires it as the
// key's current flight. Idempotent: the leader calls it on its success path
// and defers it for every other exit (a zero result, ok=false).
func (f *contentFlight) complete(res contentFlightResult) {
	f.once.Do(func() {
		f.res = res
		contentFlightsMu.Lock()
		if contentFlights[f.key] == f {
			delete(contentFlights, f.key)
		}
		if contentPendingFlights[f.key] == f {
			delete(contentPendingFlights, f.key)
		}
		contentFlightsMu.Unlock()
		close(f.done)
	})
}

// wait blocks until the flight completes or ctx is done. ok=false: the
// joiner's own ctx ended first, or the flight produced no servable result.
func (f *contentFlight) wait(ctx context.Context) (contentFlightResult, bool) {
	contentFillJoinWaiting.Add(1)
	defer contentFillJoinWaiting.Add(-1)
	select {
	case <-f.done:
		return f.res, f.res.ok
	case <-ctx.Done():
		return contentFlightResult{}, false
	}
}

// detachedFillContext is the leader's fill ctx: every value of ctx (identity,
// sinks, markers), none of its cancellation, bounded by the server write
// deadline — so the joiners never inherit the leader's cancellation.
func detachedFillContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cache.DemandRevalidationDeadline())
}

// resetContentFlightsForTest forgets every current flight, so a flight an
// earlier, failed arm abandoned cannot be joined by the next one. Test-only.
func resetContentFlightsForTest() {
	contentFlightsMu.Lock()
	contentFlights = map[string]*contentFlight{}
	contentPendingFlights = map[string]*contentFlight{}
	contentFlightsMu.Unlock()
}

// ContentFlightsInFlightForTest reports how many content keys have a current
// flight. Production MUST NOT use it.
func ContentFlightsInFlightForTest() int {
	contentFlightsMu.Lock()
	defer contentFlightsMu.Unlock()
	return len(contentFlights)
}
