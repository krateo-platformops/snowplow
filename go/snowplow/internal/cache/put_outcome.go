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
// THE RULE (one rule, every write of an armable cell). A write compares its
// bytes with the prior it replaces. PutIdentical: same bytes as that prior.
// PutChanged: no prior (a fill), or different bytes, or (the refresh replace,
// which compares before its lock for #1070) a write landed between the compare
// and the lock: the compare is void, so the Put counts as a change and a frame
// is never withheld on a guess. Every other outcome-reporting write compares
// against the entry it actually replaced, read in its own critical section
// (writeOutcome), so no compare of it can be void. The refresh and its Put
// still run either way; only the frame for an identical Put is withheld.
//
// THE INVARIANT IT RESTS ON (#1321 B1). Withholding the frame of an identical
// Put is right only if the bytes it replaced were themselves announced. So
// EVERY write that can change the bytes of a cell a /refreshes stream can be
// armed on hands its outcome to the publish gate (publishPut, through
// PublishRefreshed / PublishPut / publishIfSubscribed). A change is announced
// by the write that made it, whoever made it. Before B1 the keep-warm seed
// terminal Put and the walker's widgetContent shell re-Put stored new bytes
// over a resident, armed cell with no frame, and the refresher's later Put of
// the same bytes was then withheld as identical: a tab holding the old bytes
// was never told.
//
// WHY NOTHING IS LOST. A subscriber armed when a CHANGED Put lands is offered
// that frame, and a frame offered is never dropped (#484: a full sink defers,
// a coalesced signal is re-announced at its window's trailing edge). An
// identical Put that follows a coalesced change does not touch the window, so
// the trailing edge still announces the change. So the subscriber's last
// frame is always for the newest bytes, and an identical Put announces
// nothing it does not already hold. A subscriber that was not armed when the
// change landed (a /call → arm gap, a stream re-open) is caught up on arm from
// the digest its tab holds (#1324, refresh_digest.go).
//
// THE WRITE SITES (TestIssue1321_C1w_EveryWriteSiteHandsItsOutcomeToThePublishGate
// pins every one; TestIssue1321_C1_PublishSiteCensus pins the publishers):
//
//   - the refresher terminal (resolve_populate.go): ReplaceResolvedIfGenRefresh
//     → PublishRefreshed;
//   - the cold-dispatch fills (restactions.go, widgets.go):
//     PutResolvedIfGenOutcome → publishIfSubscribed. Their PutIfGen also
//     re-enters over a resident cell on a #547 demand revalidation;
//   - the seed terminal Put (seed_terminal_put_guard.go seedTerminalPut, every
//     seed mode): PutResolvedIfGenOutcome or PutResolvedThenRemark →
//     publishIfSubscribed;
//   - the walker's widgetContent shell Put (widget_content.go):
//     PutResolvedIfGenOutcome or PutResolvedThenRemark → publishIfSubscribed;
//   - the external-TTL widget Put (widgets.go): PutResolvedEndingKnownStale →
//     publishIfSubscribed;
//   - the raFullList serve-path Put (widgets/apiref raFullListServe):
//     PutRAFullListIfGenOutcome → PublishPutIfSubscribed (a connection may
//     arm raFullList coordinates, #426);
//   - the D2 stale-dependent replace (restactions/api finishStaleReplace): it
//     publishes unless the cell now holds exactly the bytes that dependent
//     was served (the client already has them).
//
// The one write that takes no frame decision is the GET-only content cell
// (PutContentIfGen, which refuses anything but the apistage class). No stream
// can be armed on an apistage key: DeriveSubscriptionKey fails it closed
// (#449, identityFreeArmable) and no response stamps the class. A change in a
// content cell reaches the armed keys built from it through their own dirty
// mark and refresh. The C1w census lists those sites with that reason, and
// TestIssue1321_C1w_ApistageCellsAreNeverArmable pins it.
//
// GATED KEYS: WHAT A TAB IS SERVED, NOT ONLY WHAT IS STORED (#1328). A
// widgetContent shell is one identity-free cell narrowed per requester at
// serve time (dispatchers gateWidgetEnvelope re-derives each action's
// `allowed` with rbac.UserCan). What a tab is served from it therefore moves
// with the shell's bytes, which the rule above announces, AND with the
// requester's RBAC, which no write touches. Nothing else announced the second:
// the key folds no identity, so #819 never rotates it (dispatchers
// refresh_rotation.go Derive tracks identity-bound keys only); the #258
// RBAC-shift re-seed re-mints identity-bound keys only; and no RBAC event
// dirty-marks a shell (its edges are the widget, its apiRef and its action
// targets). On main a tab healed only when some unrelated refresh of the shell
// re-Put identical bytes under a new generation; withholding those frames
// would make the stale view permanent. So the frame decision for a gated key
// also follows the SERVED view, per subscriber (dispatchers
// refresh_gated_view.go): the /refreshes connection keeps, per armed gated
// key, the #1324 served digest its tab holds; when an RBAC publish moves THIS
// identity's class (cache.IdentityClassesMayHaveMoved, rbac.IdentityClassOf),
// it recomputes the digest for this identity and sends one frame for every key
// whose view moved (RefreshStream.GatedMoved, counted
// snowplow_refresh_broadcaster.gated_view_moved). Equal classes give equal
// verdicts (#423, #435), so a publish that leaves the class alone cannot move
// the view. Pinned end to end by TestRefreshes1328_G1 (internal/handlers).
//
// "IDENTICAL" IS THE ANSWER (#1321 M2, #1319). The compare and the #1324
// digest cover what an armed client renders. Until #1319 that was the body
// only. Since #1319 the SPA renders a widget's X-Snowplow-Stage-Outcomes (an
// exception-only notice that must clear when a refetch comes back complete),
// so the stored stage outcomes (entry.StageOutcomes) joined both, as this
// paragraph required: a Put whose body is unchanged but whose stage outcomes
// moved is PutChanged and publishes a frame, and its digest moves (SameAnswer,
// AnswerDigest in content_digest.go). The rule is the same for every class: a
// RESTAction cell's stored outcomes are non-empty only for a denial-only cell
// (#681), so a RESTAction tab sees an extra frame only when a denial appears
// or clears under the same key. TestIssue1321_M2_StageOutcomesArePartOfIdentical
// pins the rule.
//
// MEASUREMENT. snowplow_refresh_broadcaster.withheld_identical counts the
// identical Puts whose frame was withheld, so frames (published) and
// identical Puts are distinguishable in /debug/vars. For the refresher it
// equals snowplow_refresh_cycle_put_total{content="identical"}.
package cache

import (
	"bytes"
	"sync/atomic"
)

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

// replacedForOutcomeLocked is the entry an accepted write is about to replace,
// read in the write's own critical section, when the caller consumes the
// outcome (else nil, and nothing is compared). Caller holds c.mu, before
// putCoreLocked.
func (c *ResolvedCacheStore) replacedForOutcomeLocked(outcome bool, key string) *ResolvedEntry {
	if !outcome {
		return nil
	}
	return c.raLayerResidentEntryLocked(key)
}

// writeOutcome is a stored write's outcome against the entry it replaced
// (replacedForOutcomeLocked). That entry was read under c.mu in the same
// critical section as the write, so it is exactly the one replaced: no write
// can land between the compare's subject and the commit, and no extra c.mu
// acquisition is needed. The compare runs after the unlock (entries are never
// mutated once stored), and only for a caller that consumes the outcome:
// with outcome unset a stored write is PutChanged, read only as Stored().
//
// samePrior is #406's confirmed RAFullList prior (raLayerConfirmPriorLocked):
// non-nil means #406 already compared these bytes against the replaced entry,
// so they are not compared twice. RawLen first: bodies of different length
// differ without reading either one, and RawLen never builds an incremental
// list cell's envelope (#854).
func writeOutcome(outcome bool, replaced, samePrior, entry *ResolvedEntry) PutOutcome {
	switch {
	case !outcome, replaced == nil:
		return PutChanged
	case replaced.StageOutcomes != entry.StageOutcomes:
		return PutChanged // #1319: the same body with other stage outcomes is another answer
	case samePrior != nil:
		return PutIdentical
	case replaced.RawLen() != entry.RawLen():
		return PutChanged
	}
	return putOutcomeOf(bytes.Equal(replaced.Raw(), entry.Raw()))
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

// PublishPutIfSubscribed is PublishPut for a write outside the refresher: a
// no-op unless the write was stored and a /refreshes stream is armed on l1Key
// (HasRefreshSubscriber, an O(1) read), so a write nobody watches touches no
// coalescer window and no counter. Every such write of an armable cell calls
// it with its own outcome (put_outcome.go, WRITE SITES).
func PublishPutIfSubscribed(l1Key string, put PutOutcome) {
	if l1Key == "" || !put.Stored() {
		return
	}
	if HasRefreshSubscriber(l1Key) {
		PublishPut(l1Key, put)
	}
}
