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
}

// contentFlight is one in-flight fill of one content key.
type contentFlight struct {
	key      string
	startSeq uint64
	done     chan struct{}
	once     sync.Once
	res      contentFlightResult
}

var (
	contentFlightsMu sync.Mutex
	// contentFlights is each content key's CURRENT flight: the one new readers
	// may join. A flight a reader refused (the coordinate moved) is replaced
	// here but still completes for the joiners it already has. Bounded by the
	// concurrent fills.
	contentFlights = map[string]*contentFlight{}
)

// enterContentFlight is the MISS branch's entry. It installs the content cell's
// #375 (A) child dep-gen sink (fillCtx) BEFORE any data read — its startSeq is
// the epoch of the flight this reader would lead — and joins or leads the key's
// flight. fillCtx is meaningful only to a leader.
func enterContentFlight(ctx context.Context, key string, gvr schema.GroupVersionResource, namespace, name string, reason contentFillReason) (flight *contentFlight, fillCtx context.Context, lead bool) {
	fillCtx = cache.WithContentDepGenSink(ctx, gvr, namespace, name)
	startSeq, _ := cache.DepGenStartSeqFromContext(fillCtx)
	flight, lead = joinOrLeadContentFill(key, startSeq, gvr, namespace, name, reason)
	return flight, fillCtx, lead
}

// joinOrLeadContentFill returns the flight the reader is in and whether it
// LEADS it. startSeq is the leader-to-be's content-sink entry seq; it is used
// only when the reader leads. (gvr, namespace, name) is the cell's own
// coordinate (name "" = LIST).
func joinOrLeadContentFill(key string, startSeq uint64, gvr schema.GroupVersionResource, namespace, name string, reason contentFillReason) (*contentFlight, bool) {
	contentFlightsMu.Lock()
	defer contentFlightsMu.Unlock()
	if cur := contentFlights[key]; cur != nil {
		if !cache.ContentCoordinateMovedSince(gvr, namespace, name, cur.startSeq) {
			noteContentFillJoined(reason, gvr.String())
			return cur, false
		}
		noteContentFillJoinRefused(reason)
	}
	f := &contentFlight{key: key, startSeq: startSeq, done: make(chan struct{})}
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

// ContentFlightsInFlightForTest reports how many content keys have a current
// flight. Production MUST NOT use it.
func ContentFlightsInFlightForTest() int {
	contentFlightsMu.Lock()
	defer contentFlightsMu.Unlock()
	return len(contentFlights)
}
