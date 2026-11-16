// store_swap_ledger.go — #870 owner ruling 3 and N5: one known-stale rule for
// every build reason, counted, and the push ledger that says when every
// observed viewer was written the corrected body (design v5 §7, v5.1 C2/C5,
// architect answers Q3/Q6).
//
// # THE RULE
//
// During a build, every key whose served bytes the build will change stays
// KNOWN-STALE until the swap: the live copy keeps serving it, and nothing reads
// live instead (no special case, for any reason). The known-stale coordinates
// are the confirmed-divergent keys (reason divergence) and M at the swap
// (reason schema). Every other key stays fully fresh: the live copy keeps
// watching and delivering.
//
// # THE WINDOW
//
// A reason's window opens at detection (the forced pass's verdict, or the CRD
// event) and ends when no OBSERVED viewer is still owed a frame:
//
//   - at an abort (abort_clean, abort_refused, abort_teardown);
//   - after the swap, when every ledger entry that HOLDS the window has closed.
//
// An entry holds the window only while it has, or has had since the swap, a
// subscriber (C5a). The ledger opens entries only for L1 keys of ARMABLE
// classes — the #710 / #548 split: a class whose responses stamp a refresh key
// (C5b) — and only for dependents of known-stale coordinates IN M (Q3): a
// coordinate not in M was already corrected by a live event before the swap,
// so its dependents' entries are recorded closed at the open with
// close=live_event and hold nothing.
//
// # HOW AN ENTRY CLOSES (exactly once)
//
//   - pushed / pushed_on_arm: the /refreshes handler's writeFrame for the key
//     RETURNED TRUE (C2: a write, not an offer) after the entry was refreshed;
//     pushed_on_arm when the stream armed the key after the refresh;
//   - pushed_resync: a resync frame was written to a stream armed for the key
//     after the refresh (the SPA re-reads every armed key);
//   - known_stale: the refresher's dequeue window ended known-stale (a decline,
//     a drop, a suppressed skip) for a dequeue that BEGAN after the open (Q3);
//   - evicted: the L1 key left the store;
//   - live_event: see above.
//
// An entry refreshed while nobody is subscribed stays open as OWED
// (store_swap_ledger_owed) until a later arm writes it a frame or the key is
// evicted. An unsubscribe closes nothing (P4).
//
// # LOCKS
//
// swapLedger.mu is a LEAF. The hub's arm path, the /refreshes handler and the
// refresher take it with no other lock held (the hub path after releasing
// hub.mu); nothing is called under it but map operations and the clock.
// Histogram records run after it is released.
package cache

import (
	"log/slog"
	"sort"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Build reasons (design v5 §2.2).
const (
	reasonDivergence = "divergence"
	reasonSchema     = "schema"
)

// Build outcomes (the `outcome` label).
const (
	outcomeSwap          = "swap"
	outcomeAbortClean    = "abort_clean"
	outcomeAbortRefused  = "abort_refused"
	outcomeAbortTeardown = "abort_teardown"
)

// Ledger close labels (the `close` label).
const (
	closePushed       = "pushed"
	closePushedOnArm  = "pushed_on_arm"
	closePushedResync = "pushed_resync"
	closeKnownStale   = "known_stale"
	closeEvicted      = "evicted"
	closeLiveEvent    = "live_event"
)

// The closed label sets, in export order.
var (
	StoreSwapReasons  = []string{reasonDivergence, reasonSchema}
	StoreSwapOutcomes = []string{outcomeSwap, outcomeAbortClean, outcomeAbortRefused, outcomeAbortTeardown}
	StoreSwapCloses   = []string{closePushed, closePushedOnArm, closePushedResync, closeKnownStale, closeEvicted, closeLiveEvent}
)

// armableEntryClass is the #710 / #548 class split: the classes whose
// responses can carry a refresh key (the two dispatchers that stamp it through
// setRefreshKeyHeaderIfArmable). Apistage content, widget content and the RA
// full list are never armed. A CLASS property, not a key list.
func armableEntryClass(class string) bool {
	return class == "restactions" || class == "widgets"
}

// ── accounting ───────────────────────────────────────────────────────────────

// reasonAccount is one reason's known-stale accounting inside a build: its
// coordinates with their earliest detection, and its window. Every field is
// guarded by swapLedger.mu.
type reasonAccount struct {
	reason   string
	gvr      schema.GroupVersionResource
	detected time.Time            // the earliest detection / CRD event
	keys     map[string]time.Time // coordinate → earliest detection

	installed time.Time
	holding   int // open entries that hold the window
	ended     bool
	entries   map[string]*ledgerEntry
}

// buildAccount is a build's accounting: one reasonAccount per reason. Guarded
// by swapLedger.mu.
type buildAccount struct {
	reasons map[string]*reasonAccount
}

func newBuildAccount() *buildAccount {
	return &buildAccount{reasons: map[string]*reasonAccount{}}
}

// note adds a reason and its coordinates, keeping the earliest detection per
// coordinate and per reason. A reason's window opens when it is first noted.
func (b *buildAccount) note(gvr schema.GroupVersionResource, reason string, keys []string, at time.Time) {
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	ra, ok := b.reasons[reason]
	if !ok {
		ra = &reasonAccount{reason: reason, gvr: gvr, detected: at, keys: map[string]time.Time{}}
		b.reasons[reason] = ra
		swapLedger.open[ra] = struct{}{}
	}
	if at.Before(ra.detected) {
		ra.detected = at
	}
	for _, k := range keys {
		if t, seen := ra.keys[k]; !seen || at.Before(t) {
			ra.keys[k] = at
		}
	}
}

// absorb merges other into b (Q6): the union of reasons and coordinates, the
// earliest detection per coordinate and per reason. other is emptied, so each
// coordinate is counted once, under b's outcome.
func (b *buildAccount) absorb(other *buildAccount) {
	if other == nil || other == b {
		return
	}
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	for reason, o := range other.reasons {
		ra, ok := b.reasons[reason]
		if !ok {
			b.reasons[reason] = o
			continue
		}
		if o.detected.Before(ra.detected) {
			ra.detected = o.detected
		}
		for k, t := range o.keys {
			if cur, seen := ra.keys[k]; !seen || t.Before(cur) {
				ra.keys[k] = t
			}
		}
		delete(swapLedger.open, o)
	}
	other.reasons = map[string]*reasonAccount{}
}

func (b *buildAccount) has(reason string) bool {
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	_, ok := b.reasons[reason]
	return ok
}

func (b *buildAccount) reasonList() []string {
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	out := make([]string, 0, len(b.reasons))
	for r := range b.reasons {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

// coordinates returns a reason's coordinates with their detection times.
func (b *buildAccount) coordinates(reason string) (map[string]time.Time, *reasonAccount) {
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	ra, ok := b.reasons[reason]
	if !ok {
		return nil, nil
	}
	out := make(map[string]time.Time, len(ra.keys))
	for k, t := range ra.keys {
		out[k] = t
	}
	return out, ra
}

// setSchemaKeys records M as the schema reason's coordinates (N5), detected at
// the reason's CRD event.
func (b *buildAccount) setSchemaKeys(m []string) {
	swapLedger.mu.Lock()
	defer swapLedger.mu.Unlock()
	ra, ok := b.reasons[reasonSchema]
	if !ok {
		return
	}
	for _, k := range m {
		if _, seen := ra.keys[k]; !seen {
			ra.keys[k] = ra.detected
		}
	}
}

// endReason ends one reason's window without a swap (an abort).
func (b *buildAccount) endReason(reason, outcome string, at time.Time) {
	swapLedger.mu.Lock()
	ra, ok := b.reasons[reason]
	if ok {
		delete(b.reasons, reason)
		swapLedger.decideLocked(ra, outcome)
		swapLedger.endLocked(ra, outcome, at)
	}
	swapLedger.mu.Unlock()
	if ok {
		logBuildOutcome(ra, outcome)
	}
}

// endAll ends every reason's window without a swap.
func (b *buildAccount) endAll(outcome string, at time.Time) {
	for _, r := range b.reasonList() {
		b.endReason(r, outcome, at)
	}
}

func logBuildOutcome(ra *reasonAccount, outcome string) {
	swapLedger.mu.Lock()
	n := len(ra.keys)
	swapLedger.mu.Unlock()
	// Metadata only: never a key, a body or an identity.
	slog.Info("cache.store.swap.build",
		slog.String("subsystem", "cache"),
		slog.String("gvr", ra.gvr.String()),
		slog.String("reason", ra.reason),
		slog.String("outcome", outcome),
		slog.Int("known_stale_keys", n),
	)
}

// ── the ledger ───────────────────────────────────────────────────────────────

type ledgerEntry struct {
	ra          *reasonAccount
	key         string
	detected    time.Time
	refreshed   bool
	refreshedAt time.Time
	hadSub      bool // had a subscriber at some point since the swap
	holds       bool // counted in ra.holding (C5a)
	closed      bool
}

type ledgerState struct {
	mu sync.Mutex
	// open: every reason account whose window has not ended (the gauges).
	open map[*reasonAccount]struct{}
	// byKey: L1 key → its OPEN entries.
	byKey map[string]map[*ledgerEntry]struct{}
}

var swapLedger = &ledgerState{
	open:  map[*reasonAccount]struct{}{},
	byKey: map[string]map[*ledgerEntry]struct{}{},
}

// decideLocked counts a reason's outcome: keys_total and builds_total.
func (l *ledgerState) decideLocked(ra *reasonAccount, outcome string) {
	n := uint64(len(ra.keys))
	storeSwapStats.keys.add(ra.reason, outcome, n)
	if n > 0 {
		storeSwapStats.builds.add(ra.reason, outcome, 1)
	}
}

// endLocked ends a reason's window: window_seconds_total and the gauges.
func (l *ledgerState) endLocked(ra *reasonAccount, outcome string, at time.Time) {
	if ra.ended {
		return
	}
	ra.ended = true
	delete(l.open, ra)
	storeSwapStats.windowNS.add(ra.reason, outcome, uint64(max(0, at.Sub(ra.detected).Nanoseconds())))
}

func (l *ledgerState) maybeEndLocked(ra *reasonAccount, at time.Time) {
	if !ra.installed.IsZero() && ra.holding == 0 {
		l.endLocked(ra, outcomeSwap, at)
	}
}

func (l *ledgerState) indexLocked(e *ledgerEntry) {
	m := l.byKey[e.key]
	if m == nil {
		m = map[*ledgerEntry]struct{}{}
		l.byKey[e.key] = m
	}
	m[e] = struct{}{}
}

// closeLocked closes e with label c. Returns the histogram observation.
func (l *ledgerState) closeLocked(e *ledgerEntry, c string, at time.Time) closeRecord {
	e.closed = true
	if m := l.byKey[e.key]; m != nil {
		delete(m, e)
		if len(m) == 0 {
			delete(l.byKey, e.key)
		}
	}
	if e.holds {
		e.holds = false
		e.ra.holding--
	}
	storeSwapStats.closes.add(e.ra.reason, c, 1)
	l.maybeEndLocked(e.ra, at)
	return closeRecordFor(e, c, at)
}

// closeRecord is one per-entry histogram observation, recorded after the lock.
type closeRecord struct {
	reason, close    string
	detectMS, instMS int64
}

func closeRecordFor(e *ledgerEntry, c string, at time.Time) closeRecord {
	return closeRecord{
		reason:   e.ra.reason,
		close:    c,
		detectMS: max(0, at.Sub(e.detected).Milliseconds()),
		instMS:   max(0, at.Sub(e.ra.installed).Milliseconds()),
	}
}

func recordCloses(recs []closeRecord) {
	for _, r := range recs {
		storeSwapStats.observeClose(r)
	}
}

// ledgerOpenEntry is one entry to open: its L1 key, its coordinate's
// detection, whether that coordinate is in M, and whether a subscriber is armed
// for the key now.
type ledgerOpenEntry struct {
	key      string
	detected time.Time
	inM      bool
	hasSub   bool
}

// openLedger opens ra's entries at the install (after step 5, before M is
// delivered — Q3) and decides its swap outcome. An entry whose coordinates
// are all outside M closes at once as live_event. The window ends at once when
// nothing holds it.
func openLedger(ra *reasonAccount, installed time.Time, entries []ledgerOpenEntry) {
	var recs []closeRecord
	l := swapLedger
	l.mu.Lock()
	ra.installed = installed
	l.decideLocked(ra, outcomeSwap)
	ra.entries = map[string]*ledgerEntry{}
	inM := map[string]bool{}
	for _, oe := range entries {
		e, ok := ra.entries[oe.key]
		if !ok {
			e = &ledgerEntry{ra: ra, key: oe.key, detected: oe.detected, hadSub: oe.hasSub}
			ra.entries[oe.key] = e
		} else if oe.detected.Before(e.detected) {
			e.detected = oe.detected
		}
		if oe.inM {
			inM[oe.key] = true
		}
	}
	for _, e := range ra.entries {
		if !inM[e.key] {
			e.closed = true
			storeSwapStats.closes.add(ra.reason, closeLiveEvent, 1)
			recs = append(recs, closeRecordFor(e, closeLiveEvent, installed))
			continue
		}
		l.indexLocked(e)
		if e.hadSub {
			e.holds = true
			ra.holding++
		}
	}
	l.maybeEndLocked(ra, installed)
	l.mu.Unlock()
	recordCloses(recs)
}

// ── close events ─────────────────────────────────────────────────────────────

// ledgerRefreshed: the refresher's dequeue of key ended FRESH. Only a dequeue
// that began after an entry's ledger opened refreshes it (Q3): one that began
// earlier may have read the pre-swap copy.
func ledgerRefreshed(key string, began time.Time) {
	l := swapLedger
	l.mu.Lock()
	defer l.mu.Unlock()
	for e := range l.byKey[key] {
		if e.refreshed || began.Before(e.ra.installed) {
			continue
		}
		e.refreshed = true
		e.refreshedAt = windowNow()
	}
}

// ledgerKnownStale: the refresher's dequeue of key ended KNOWN-STALE.
func ledgerKnownStale(key string, began time.Time) {
	var recs []closeRecord
	l := swapLedger
	l.mu.Lock()
	now := windowNow()
	for e := range l.byKey[key] {
		if e.refreshed || began.Before(e.ra.installed) {
			continue
		}
		recs = append(recs, l.closeLocked(e, closeKnownStale, now))
	}
	l.mu.Unlock()
	recordCloses(recs)
}

// ledgerEvicted: key left the store.
func ledgerEvicted(key string) {
	l := swapLedger
	l.mu.Lock()
	if len(l.byKey[key]) == 0 {
		l.mu.Unlock()
		return
	}
	var recs []closeRecord
	now := windowNow()
	for e := range l.byKey[key] {
		recs = append(recs, l.closeLocked(e, closeEvicted, now))
	}
	l.mu.Unlock()
	recordCloses(recs)
}

// ledgerFrameWritten: the /refreshes handler WROTE a refresh frame for key
// (C2: after writeFrame returned true). A refreshed entry closes pushed, or
// pushed_on_arm when the stream armed the key AFTER the entry was refreshed
// (onArm: the arm found it refreshed — decided by the order of the two events,
// never by comparing instants). A still-stale entry stays open: the frame
// announced a body that is not the corrected one yet.
func ledgerFrameWritten(key string, onArm bool) {
	l := swapLedger
	l.mu.Lock()
	if len(l.byKey[key]) == 0 {
		l.mu.Unlock()
		return
	}
	var recs []closeRecord
	now := windowNow()
	for e := range l.byKey[key] {
		if !e.refreshed {
			continue
		}
		c := closePushed
		if onArm {
			c = closePushedOnArm
		}
		recs = append(recs, l.closeLocked(e, c, now))
	}
	l.mu.Unlock()
	recordCloses(recs)
}

// ledgerResyncWritten: a resync frame was WRITTEN to a stream armed for keys.
// The SPA re-reads every armed key, so each REFRESHED entry closes
// pushed_resync; a stale one stays open (the re-read would serve the stale
// body, and the reopened stream re-arms it).
func ledgerResyncWritten(keys map[string]struct{}) {
	l := swapLedger
	l.mu.Lock()
	var recs []closeRecord
	now := windowNow()
	for k := range keys {
		for e := range l.byKey[k] {
			if !e.refreshed {
				continue
			}
			recs = append(recs, l.closeLocked(e, closePushedResync, now))
		}
	}
	l.mu.Unlock()
	recordCloses(recs)
}

// ledgerArmed: a stream armed keys. Each open entry for them now has a
// subscriber (it holds the window from here, C5a, unless the window already
// ended). Returns the keys whose entry is already refreshed: the arm path
// offers them a frame at once (pushed_on_arm, P4).
func ledgerArmed(keys map[string]struct{}) []string {
	l := swapLedger
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.byKey) == 0 {
		return nil
	}
	var owed []string
	for k := range keys {
		emit := false
		for e := range l.byKey[k] {
			if !e.hadSub {
				e.hadSub = true
				if !e.ra.ended {
					e.holds = true
					e.ra.holding++
				}
			}
			if e.refreshed {
				emit = true
			}
		}
		if emit {
			owed = append(owed, k)
		}
	}
	return owed
}

// ── gauges ───────────────────────────────────────────────────────────────────

// storeSwapGauges reads the §7 gauges per reason. owed counts refreshed open
// entries whose key has no armed subscriber now (an expected outcome, not a
// detector).
func storeSwapGauges() (keysOpen map[string]int64, windowMaxSec map[string]float64, owed map[string]int64) {
	keysOpen = map[string]int64{}
	windowMaxSec = map[string]float64{}
	owed = map[string]int64{}
	for _, r := range StoreSwapReasons {
		keysOpen[r], windowMaxSec[r], owed[r] = 0, 0, 0
	}
	l := swapLedger
	now := windowNow()
	type refreshedKey struct{ key, reason string }
	var refreshed []refreshedKey
	l.mu.Lock()
	for ra := range l.open {
		keysOpen[ra.reason] += int64(len(ra.keys))
		if age := now.Sub(ra.detected).Seconds(); age > windowMaxSec[ra.reason] {
			windowMaxSec[ra.reason] = age
		}
	}
	for k, m := range l.byKey {
		for e := range m {
			if e.refreshed {
				refreshed = append(refreshed, refreshedKey{k, e.ra.reason})
			}
		}
	}
	l.mu.Unlock()
	for _, rk := range refreshed {
		if !HasRefreshSubscriber(rk.key) {
			owed[rk.reason]++
		}
	}
	return keysOpen, windowMaxSec, owed
}

// openEntriesForTest counts open ledger entries and windows.
func openLedgerForTest() (entries, windows int) {
	l := swapLedger
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.byKey {
		entries += len(m)
	}
	return entries, len(l.open)
}

func resetSwapLedgerForTest() {
	l := swapLedger
	l.mu.Lock()
	l.open = map[*reasonAccount]struct{}{}
	l.byKey = map[string]map[*ledgerEntry]struct{}{}
	l.mu.Unlock()
}
