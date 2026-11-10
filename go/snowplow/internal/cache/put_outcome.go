// put_outcome.go — #1321: an accepted store write says whether it changed the
// stored bytes, and a /refreshes frame is published only when it did.
//
// WHY. A refresh frame tells a browser "the bytes under this key changed;
// refetch". Before #1321 the publish was gated on the write being ACCEPTED (a
// new generation), not on the bytes moving. On 057 (1.12.67, idle) 95.2% of
// viewed-tier refresh Puts were byte-identical and ~84% of all frames
// announced bytes the subscriber already held: each made the browser refetch,
// hit L1 and get the same bytes back. The design always said otherwise
// (resolve_populate.go: "ONLY on a genuine L1 change"; refresh_broadcaster.go:
// "fires only when L1 actually changed", design §1.1).
//
// THE RULE (one rule, every Put that publishes). The Put compares its bytes
// with the resident prior it replaces, off the store lock, and confirms under
// the lock that the prior it compared is the one it replaced (sameBytesPrior,
// the #1070 compare). PutIdentical: same bytes as that prior. PutChanged: no
// prior (a fill), or different bytes, or a write landed between the compare
// and the lock (the compare is void, so the Put counts as a change: a frame is
// never withheld on a guess). The refresh and its Put still run either way;
// only the frame for an identical Put is withheld.
//
// WHY NOTHING IS LOST. A subscriber armed when a CHANGED Put lands is offered
// that frame, and a frame offered is never dropped (#484: a full sink defers,
// a coalesced signal is re-announced at its window's trailing edge). An
// identical Put that follows a coalesced change does not touch the window, so
// the trailing edge still announces the change. So the subscriber's last
// frame is always for the newest bytes, and an identical Put announces
// nothing it does not already hold.
//
// THE PUBLISH SITES (TestIssue1321_PublishSiteCensus pins every one):
//
//   - the refresher terminal (resolve_populate.go): PublishRefreshed with the
//     Put's outcome;
//   - the cold-dispatch fill (refresh_publish.go publishIfSubscribed, from
//     restactions.go and widgets.go): its PutIfGen also re-enters over a
//     resident cell on a #547 demand revalidation, so it takes the outcome;
//   - the D2 stale-dependent replace (restactions/api finishStaleReplace): it
//     publishes unless the cell now holds exactly the bytes that dependent
//     was served (the client already has them).
//
// MEASUREMENT. snowplow_refresh_broadcaster.withheld_identical counts the
// identical Puts whose frame was withheld, so frames (published) and
// identical Puts are distinguishable in /debug/vars. For the refresher it
// equals snowplow_refresh_cycle_put_total{content="identical"}.
package cache

import "sync/atomic"

// PutOutcome is a store write's result: refused, or stored with or without a
// change to the stored bytes (#1321).
type PutOutcome uint8

const (
	// PutRefused: nothing was stored (declined, inert, generation moved).
	PutRefused PutOutcome = iota
	// PutIdentical: stored; the bytes equal those of the prior it replaced.
	PutIdentical
	// PutChanged: stored; no prior, different bytes, or a void compare.
	PutChanged
)

// Stored reports whether the write was accepted.
func (o PutOutcome) Stored() bool { return o != PutRefused }

// Changed reports whether the write changed the stored bytes: the one
// condition under which a /refreshes frame announces anything.
func (o PutOutcome) Changed() bool { return o == PutChanged }

func (o PutOutcome) String() string {
	switch o {
	case PutIdentical:
		return "identical"
	case PutChanged:
		return "changed"
	}
	return "refused"
}

// putOutcomeOf is a stored Put's outcome: identical iff the confirmed prior's
// bytes equal the new ones.
func putOutcomeOf(identical bool) PutOutcome {
	if identical {
		return PutIdentical
	}
	return PutChanged
}

// refreshWithheldIdenticalTotal counts accepted Puts at a publish site whose
// bytes were identical, so no frame was published (#1321).
var refreshWithheldIdenticalTotal atomic.Uint64

// publishPut is the one gate every Put-driven publish goes through: a frame
// only for a Put that changed the stored bytes. owed carries the refresh Put
// time for put_to_frame (#1070).
func publishPut(key string, put PutOutcome, owed refreshOwed) {
	switch put {
	case PutChanged:
		publishRefresh(key, owed)
	case PutIdentical:
		refreshWithheldIdenticalTotal.Add(1)
	}
}

// PublishPut announces a cold-dispatch Put of l1Key iff it changed the stored
// bytes (#1321). See publishPut.
func PublishPut(l1Key string, put PutOutcome) { publishPut(l1Key, put, refreshOwed{}) }
