// deps_list_interest.go — #603 PR-2 / #765 option B, SHADOW mode.
//
// WHAT THIS IS. Today one object event dirty-marks every list_dep cell on its
// (GVR, namespace), whatever field moved (#603: 99.66% of all marks). The
// jqinterest analyzer (#632, PR-1) derives from a cell's JQ the item fields
// the cell can observe (its read set). An UPDATE that leaves the projection of
// the object on that read set unchanged cannot change the cell. This file
// computes, on EVERY list_dep mark, whether the mark WOULD be avoided by that
// rule, counts the decision, and then marks EXACTLY as before. Nothing reads a
// decision to change what is marked: the shadow observes, never acts. The
// owner's enforcement gate (#603 Q2: >= 7 days on 057, >= 100K would-avoid,
// ZERO would-change, plus a 50K bench) reads the counters published here.
//
// THE THREE PARTS.
//
//  1. Record side. A list edge's interest lives on the REVERSE index value of
//     (l1Key, DepKey) — the forward index and DepKey are untouched, so the
//     match set of an event cannot change. Three states:
//     - all (sticky): struct{} — what every plain Record/RecordList stores
//     (content cells, cluster-list, ReplayEdges, deps_extract, anything
//     un-annotated), and what a plain re-record forces. Unknown = all.
//     - pending: an *edgeInterest whose pointer is nil, stored only by
//     RecordListNarrowable (the two edge-3 sites in restactions/api). Reads
//     as all.
//     - narrowed: the *edgeInterest points at an interned read set. Set only by
//     CommitListInterest, at the end of a CLEAN resolve whose every call on
//     the edge was informer-served (restactions/api narrow.go). A later
//     commit can only WIDEN it (union of read sets); WidenListInterest and
//     any plain re-record make it all for good (until RemoveL1Key).
//  2. Event side. The informer handler, which holds the old AND the new
//     object, computes per transition, for each live interest on the GVR,
//     whether Project(old, R) != Project(new, R) (deps_watch.go). ADD and
//     DELETE are membership changes. Masks are OR-ed per coordinate BEFORE
//     the coordinate is enqueued (the queue coalesces: A→B→A must still
//     set the bit) and swapped out by the worker.
//  3. Decision + audit. OnObjectEvent classifies each list_dep mark
//     (narrowDecide) and still marks it. A mark classified "avoided" hands the
//     refresher a one-shot token; every other refresh enqueue taints the key.
//     A refresh whose every mark since the last accepted refresh Put was
//     avoided is AUDITED at the refresher terminal write: its rebuilt bytes
//     are compared with the bytes it replaces (deps_list_interest_audit.go).
//     A difference is a would_change: a mark enforcement would have skipped
//     whose cell really changed.
//
// UNKNOWN NEVER SKIPS. Every path that cannot prove the read set, or cannot
// decide the transition, classifies the mark as kept: a sticky-all or pending
// edge, a degraded probe, a missing mask (a dep event that did not come with
// its transition), an undecodable object, a reconcile or relist submit, an
// interest registered after the transition was evaluated, a full registry.
package cache

import (
	"bytes"
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache/jqinterest"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// listNarrowShadowOff is the TEST-ONLY off switch of the whole shadow (the S0
// arm compares shadow on against off). Production never sets it: there is no
// env knob, by rule.
var listNarrowShadowOff atomic.Bool

func listNarrowShadowOn() bool { return !listNarrowShadowOff.Load() }

// ListNarrowShadowOn reports whether the shadow runs (always, outside tests).
func ListNarrowShadowOn() bool { return listNarrowShadowOn() }

// SetListNarrowShadowForTest turns the shadow off (on=false) or back on and
// returns a restore func. Test-only; production code MUST NOT call it.
func SetListNarrowShadowForTest(on bool) func() {
	prev := listNarrowShadowOff.Load()
	listNarrowShadowOff.Store(!on)
	return func() { listNarrowShadowOff.Store(prev) }
}

// maxListInterests bounds the interned interests process-wide. A commit that
// would need a new one past the bound widens its edge to all instead
// (why=registry_full): the handler-side cost and the registry memory stay
// bounded whatever the templates do. Not a knob: a fixed structural bound.
const maxListInterests = 4096

// listInterest is one interned read set, registered on one GVR.
type listInterest struct {
	id    uint32
	gvr   schema.GroupVersionResource
	reads []jqinterest.Read
	// gen is the GVR registry generation this interest was registered at. A
	// transition evaluated against an older generation did not evaluate it.
	gen uint64
}

// edgeNarrow is the immutable state an *edgeInterest points at.
type edgeNarrow struct {
	all bool
	li  *listInterest
}

var edgeNarrowAll = &edgeNarrow{all: true}

// edgeInterest is the reverse-index value of a narrowable list edge. A nil
// pointer is pending (reads as all).
type edgeInterest struct {
	p atomic.Pointer[edgeNarrow]
}

// gvrInterests is the per-GVR list of live interests the handler evaluates.
// Append-only (an interest is never unregistered in v1: bounded by
// maxListInterests).
type gvrInterests struct {
	mu   sync.RWMutex
	gen  uint64
	list []*listInterest
}

func (g *gvrInterests) snapshot() (uint64, []*listInterest) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.gen, g.list
}

// listNarrow is the DepTracker's shadow state.
type listNarrow struct {
	regMu sync.Mutex
	byKey map[string]*listInterest
	byGVR sync.Map // schema.GroupVersionResource -> *gvrInterests
	n     atomic.Int64

	// decisions[decision][reason]: avoided/kept by reason. Σ == the list_dep
	// object-path dirty marks (single writer, the OnObjectEvent mark loop).
	decisions [narrowReasonCount]atomic.Uint64
	// verdicts of record-side commits, by verdict x why (closed enums).
	verdicts sync.Map // commitVerdictKey -> *atomic.Uint64

	evalTransitions atomic.Uint64
	evalNS          atomic.Uint64

	// replays[producer state][site]: replayed LIST edges by the producer
	// edge's state (narrowed/all/pending/absent) and the replay site
	// (memo/full_list). #765 B.
	replays [4][2]atomic.Uint64
}

func newListNarrow() *listNarrow {
	n := &listNarrow{byKey: map[string]*listInterest{}}
	// The closed {verdict, why} set exists from boot at zero, so every series
	// is present (a cumulative counter that appears only on its first
	// increment is indistinguishable from one that is not wired).
	n.verdicts.Store(commitVerdictKey{CommitVerdictNarrowed, CommitWhyOK}, new(atomic.Uint64))
	for _, why := range []string{CommitWhyStageError, CommitWhyNotInformer, CommitWhyReadsWholeItem,
		CommitWhyRegistryFull, CommitWhySticky, CommitWhyListLevelRead, CommitWhyQueryUnapplied,
		CommitWhyParse, CommitWhyUnknownConstruct} {
		n.verdicts.Store(commitVerdictKey{CommitVerdictAll, why}, new(atomic.Uint64))
	}
	return n
}

// --- record side --------------------------------------------------------------

// recordMode says how a record treats the edge's interest.
type recordMode uint8

const (
	recordPlain      recordMode = iota // unknown interest: sticky all
	recordNarrowable                   // edge-3 site: pending until a commit
)

// initialEdgeValue is the reverse-index value a NEW edge gets.
func initialEdgeValue(dk DepKey, mode recordMode) any {
	if dk.Name == listWildcard && mode == recordNarrowable {
		return &edgeInterest{}
	}
	return struct{}{}
}

// noteReRecord applies a record to an EXISTING reverse value: a plain record
// of a narrowable list edge widens it to all, for good. A narrowable record
// never narrows anything (only a commit does).
func noteReRecord(v any, dk DepKey, mode recordMode) {
	if dk.Name != listWildcard || mode != recordPlain {
		return
	}
	if ei, ok := v.(*edgeInterest); ok {
		ei.p.Store(edgeNarrowAll)
	}
}

// RecordListNarrowable is RecordList for the two restactions edge-3 sites
// whose resolve commits (or widens) the edge's interest at its end. Same edge,
// same forward index, same dedup and cap as RecordList: only the reverse value
// differs (pending instead of all), and pending reads as all.
func (d *DepTracker) RecordListNarrowable(ctx context.Context, l1Key string, gvr schema.GroupVersionResource, namespace string) {
	if d == nil {
		return
	}
	if Inert(ctx) {
		return
	}
	if l1Key == "" {
		loudFailEmptyL1Key("RecordListNarrowable")
		return
	}
	mode := recordNarrowable
	if !listNarrowShadowOn() {
		mode = recordPlain
	}
	d.recordInternalMode(ctx, l1Key, DepKey{GVR: gvr, Namespace: namespace, Name: listWildcard}, mode)
}

// edgeInterestOf returns the narrowable state of (l1Key, dk), or nil when the
// edge is absent or sticky all.
func (d *DepTracker) edgeInterestOf(l1Key string, dk DepKey) *edgeInterest {
	dsI, ok := d.reverse.Load(l1Key)
	if !ok {
		return nil
	}
	v, ok := dsI.(*depSet).deps.Load(dk)
	if !ok {
		return nil
	}
	ei, _ := v.(*edgeInterest)
	return ei
}

// Commit verdicts (closed enums, OTLP attributes).
const (
	CommitVerdictNarrowed = "narrowed"
	CommitVerdictAll      = "all"
)

// Commit "why" values. The analysis's own reasons are folded onto a closed
// set by commitWhyOf; the runtime ones are produced by restactions/api.
const (
	CommitWhyOK               = "ok"
	CommitWhyStageError       = "stage_error"
	CommitWhyNotInformer      = "not_informer_served"
	CommitWhyReadsWholeItem   = "reads_whole_item"
	CommitWhyRegistryFull     = "registry_full"
	CommitWhySticky           = "sticky_all"
	CommitWhyListLevelRead    = "list_level_read"
	CommitWhyQueryUnapplied   = "query_unapplied"
	CommitWhyParse            = "parse"
	CommitWhyUnknownConstruct = "jq_unprovable"
)

// CommitWhyOfAnalysis folds a jqinterest Interest.Why onto the closed set.
func CommitWhyOfAnalysis(why string) string {
	switch {
	case strings.HasPrefix(why, "list_level_read"):
		return CommitWhyListLevelRead
	case strings.HasPrefix(why, "query_unapplied"):
		return CommitWhyQueryUnapplied
	case strings.HasPrefix(why, "parse"):
		return CommitWhyParse
	}
	return CommitWhyUnknownConstruct
}

type commitVerdictKey struct{ verdict, why string }

func (n *listNarrow) noteVerdict(verdict, why string) {
	c, _ := n.verdicts.LoadOrStore(commitVerdictKey{verdict, why}, new(atomic.Uint64))
	c.(*atomic.Uint64).Add(1)
}

// CommitListInterest narrows (l1Key, dk) to reads, or widens an already
// narrowed edge to the union of its read set and reads. A sticky-all edge, an
// absent edge and a plain (non-narrowable) edge are left as they are. Called
// by restactions/api at the end of a clean, informer-served resolve. Returns
// the verdict it counted.
func (d *DepTracker) CommitListInterest(l1Key string, dk DepKey, reads []jqinterest.Read) string {
	if d == nil || !listNarrowShadowOn() || dk.Name != listWildcard {
		return ""
	}
	verdict, why := d.commitReads(l1Key, dk, reads)
	d.narrow.noteVerdict(verdict, why)
	return verdict
}

// commitReads is CommitListInterest without the verdict count (a replay counts
// on its own counter, never as a resolve's commit).
func (d *DepTracker) commitReads(l1Key string, dk DepKey, reads []jqinterest.Read) (verdict, why string) {
	n := d.narrow
	ei := d.edgeInterestOf(l1Key, dk)
	if ei == nil {
		return CommitVerdictAll, CommitWhySticky
	}
	for {
		cur := ei.p.Load()
		if cur != nil && cur.all {
			return CommitVerdictAll, CommitWhySticky
		}
		want := reads
		if cur != nil {
			want = unionReads(cur.li.reads, reads)
		}
		li := n.intern(dk.GVR, want)
		next := edgeNarrowAll
		if li != nil {
			if cur != nil && cur.li == li {
				return CommitVerdictNarrowed, CommitWhyOK
			}
			next = &edgeNarrow{li: li}
		}
		if ei.p.CompareAndSwap(cur, next) {
			if li == nil {
				return CommitVerdictAll, CommitWhyRegistryFull
			}
			return CommitVerdictNarrowed, CommitWhyOK
		}
	}
}

// --- replays carry the producer's interest (#765 B, reason 2) ------------------
//
// A widget that does not resolve its RESTAction itself receives the producer's
// edges by REPLAY: a SeedResolveMemo hit (apiref resolve.go) or a slice of the
// shared RAFullList cell (apiref ra_full_list.go). Until #765 B those replays
// were plain records, i.e. sticky all, so no sibling widget could ever be
// decided. The v1 interest is derived from the RESTAction chain ONLY (restactions
// /api narrow.go: Chain{Widget: nil}); a replay receiver's body is that same
// RESTAction output (the memo key is RA × identity × RBAC class × extras ×
// page) or a slice of the raKey cell's output. So the receiver may take the
// PRODUCER edge's state — narrowed → the same read set (a union with whatever
// the receiver had); anything else → plain (sticky all). Monotone as ever: a
// receiver already sticky all stays all. Sound per architect-623
// (#765 issuecomment-6285013799 / -6285026365): the producer's commit runs inside
// restactions.Resolve (deferred, before it returns), i.e. before any raKey Put,
// so the live state covers every version already stored.

// EdgeNarrowState is one edge's interest state, snapshotted for a replay.
// Opaque outside the cache package.
type EdgeNarrowState struct {
	li     *listInterest
	reason uint8
}

// Replay outcomes (closed enum, OTLP attribute outcome) and sites (site).
const (
	ReplayNarrowed = "narrowed"
	ReplayAll      = "all"
	ReplayPending  = "pending"
	ReplayAbsent   = "absent"

	ReplaySiteMemo     = "memo"
	ReplaySiteFullList = "full_list"
)

var replayOutcomes = [...]string{ReplayNarrowed, ReplayAll, ReplayPending, ReplayAbsent}
var replaySites = [...]string{ReplaySiteMemo, ReplaySiteFullList}

const (
	rsNarrowed uint8 = iota
	rsAll
	rsPending
	rsAbsent
)

// EdgeNarrowStates snapshots, for each of dks, the interest state of
// (l1Key, dk). Exact edges carry no state (they replay as Record, as before).
// Read-only. nil while the shadow is off.
func (d *DepTracker) EdgeNarrowStates(l1Key string, dks []DepKey) []EdgeNarrowState {
	if d == nil || !listNarrowShadowOn() {
		return nil
	}
	out := make([]EdgeNarrowState, len(dks))
	var ds *depSet
	if dsI, ok := d.reverse.Load(l1Key); ok {
		ds = dsI.(*depSet)
	}
	for i, dk := range dks {
		if dk.Name != listWildcard {
			continue
		}
		if ds == nil {
			out[i] = EdgeNarrowState{reason: rsAbsent}
			continue
		}
		v, ok := ds.deps.Load(dk)
		if !ok {
			out[i] = EdgeNarrowState{reason: rsAbsent}
			continue
		}
		ei, isEI := v.(*edgeInterest)
		if !isEI {
			out[i] = EdgeNarrowState{reason: rsAll}
			continue
		}
		switch cur := ei.p.Load(); {
		case cur == nil:
			out[i] = EdgeNarrowState{reason: rsPending}
		case cur.all:
			out[i] = EdgeNarrowState{reason: rsAll}
		default:
			out[i] = EdgeNarrowState{li: cur.li, reason: rsNarrowed}
		}
	}
	return out
}

// replayListEdge records one replayed LIST edge under dst: narrowable plus the
// producer's read set when the producer edge was narrowed, else plain. Both go
// through recordInternalMode (the [C1-A] invariant: an open capture on dst sees
// the edge). Counts the producer state per site.
func (d *DepTracker) replayListEdge(ctx context.Context, dst string, dk DepKey, st *EdgeNarrowState, site string) {
	if st == nil || !listNarrowShadowOn() {
		d.RecordList(ctx, dst, dk.GVR, dk.Namespace)
		return
	}
	d.narrow.noteReplay(st.reason, site)
	if st.li == nil {
		d.RecordList(ctx, dst, dk.GVR, dk.Namespace)
		return
	}
	d.RecordListNarrowable(ctx, dst, dk.GVR, dk.Namespace)
	if Inert(ctx) {
		return
	}
	d.commitReads(dst, dk, st.li.reads)
}

func (n *listNarrow) noteReplay(reason uint8, site string) {
	si := 0
	if site == ReplaySiteFullList {
		si = 1
	}
	if int(reason) < len(replayOutcomes) {
		n.replays[reason][si].Add(1)
	}
}

// ListNarrowReplay is one {outcome, site} cell of the replay counter.
type ListNarrowReplay struct {
	Outcome string
	Site    string
	Count   uint64
}

// ListNarrowReplaySnapshot returns every {outcome, site} cell (zeros included).
// outcome = the PRODUCER edge's state carried by the replay: narrowed (the
// receiver narrows), or all / pending / absent (a plain record — pending and
// absent are the lossy fall-backs architect-623's C3 asks to see as a number).
func ListNarrowReplaySnapshot() []ListNarrowReplay {
	n := Deps().narrow
	out := make([]ListNarrowReplay, 0, len(replayOutcomes)*len(replaySites))
	for r := range replayOutcomes {
		for s := range replaySites {
			out = append(out, ListNarrowReplay{Outcome: replayOutcomes[r], Site: replaySites[s], Count: n.replays[r][s].Load()})
		}
	}
	return out
}

// WidenListInterest makes (l1Key, dk) all for good, counting why.
func (d *DepTracker) WidenListInterest(l1Key string, dk DepKey, why string) {
	if d == nil || !listNarrowShadowOn() || dk.Name != listWildcard {
		return
	}
	if ei := d.edgeInterestOf(l1Key, dk); ei != nil {
		ei.p.Store(edgeNarrowAll)
	}
	d.narrow.noteVerdict(CommitVerdictAll, why)
}

// unionReads is the sorted, deduplicated union of two read sets. A union is a
// sound read set: projections equal on the union are equal on each part.
func unionReads(a, b []jqinterest.Read) []jqinterest.Read {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]jqinterest.Read, 0, len(a)+len(b))
	for _, rs := range [][]jqinterest.Read{a, b} {
		for _, r := range rs {
			if k := r.String(); !seen[k] {
				seen[k] = true
				out = append(out, r)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func readsKey(reads []jqinterest.Read) string {
	parts := make([]string, len(reads))
	for i, r := range reads {
		parts[i] = r.String()
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

// intern returns the interest for (gvr, reads), registering it on the GVR when
// new. nil when the registry is full.
func (n *listNarrow) intern(gvr schema.GroupVersionResource, reads []jqinterest.Read) *listInterest {
	key := gvr.String() + "|" + readsKey(reads)
	n.regMu.Lock()
	defer n.regMu.Unlock()
	if li, ok := n.byKey[key]; ok {
		return li
	}
	if n.n.Load() >= maxListInterests {
		return nil
	}
	gI, _ := n.byGVR.LoadOrStore(gvr, &gvrInterests{})
	g := gI.(*gvrInterests)
	g.mu.Lock()
	g.gen++
	li := &listInterest{id: uint32(n.n.Add(1)), gvr: gvr, reads: append([]jqinterest.Read(nil), reads...), gen: g.gen}
	g.list = append(g.list[:len(g.list):len(g.list)], li)
	g.mu.Unlock()
	n.byKey[key] = li
	return li
}

func (n *listNarrow) forGVR(gvr schema.GroupVersionResource) *gvrInterests {
	if g, ok := n.byGVR.Load(gvr); ok {
		return g.(*gvrInterests)
	}
	return nil
}

// --- event side ---------------------------------------------------------------

// eventMask is what the handler learned about the transitions of ONE
// coordinate since the worker last processed it.
type eventMask struct {
	// unknown: a submit that carried no transition (reconcile, relist bridge)
	// or an object that would not decode. Decides nothing: every mark kept.
	unknown bool
	// membership: an ADD or a DELETE. Piece 1 never narrows those.
	membership bool
	// gen is the SMALLEST registry generation any merged transition was
	// evaluated at: an interest registered after it was not evaluated by at
	// least one transition.
	gen uint64
	// changed: the interests whose projection differed on some transition.
	changed map[uint32]struct{}
}

func (m *eventMask) merge(o *eventMask) {
	if o == nil {
		return
	}
	m.unknown = m.unknown || o.unknown
	m.membership = m.membership || o.membership
	if o.gen < m.gen {
		m.gen = o.gen
	}
	for id := range o.changed {
		if m.changed == nil {
			m.changed = map[uint32]struct{}{}
		}
		m.changed[id] = struct{}{}
	}
}

// unknownMask is the mask of a submit that carries no transition.
func unknownMask() *eventMask { return &eventMask{unknown: true} }

// membershipMask is the mask of an ADD or a DELETE.
func (d *DepTracker) membershipMask(gvr schema.GroupVersionResource) *eventMask {
	m := &eventMask{membership: true}
	if g := d.narrow.forGVR(gvr); g != nil {
		m.gen, _ = g.snapshot()
	}
	return m
}

// updateMask evaluates one UPDATE transition against every live interest on
// the GVR. Costs nothing on a GVR with no narrowed edge.
func (d *DepTracker) updateMask(gvr schema.GroupVersionResource, oldObj, newObj interface{}) *eventMask {
	g := d.narrow.forGVR(gvr)
	if g == nil {
		return &eventMask{}
	}
	gen, list := g.snapshot()
	m := &eventMask{gen: gen}
	if len(list) == 0 {
		return m
	}
	t0 := time.Now()
	defer func() {
		d.narrow.evalTransitions.Add(1)
		d.narrow.evalNS.Add(uint64(time.Since(t0).Nanoseconds()))
	}()
	ou, okOld := decodeBytesObject(oldObj)
	nu, okNew := decodeBytesObject(newObj)
	if !okOld || !okNew || ou == nil || nu == nil {
		m.unknown = true
		return m
	}
	for _, li := range list {
		if !bytes.Equal(jqinterest.Project(ou.Object, li.reads), jqinterest.Project(nu.Object, li.reads)) {
			if m.changed == nil {
				m.changed = map[uint32]struct{}{}
			}
			m.changed[li.id] = struct{}{}
		}
	}
	return m
}

// --- decision -----------------------------------------------------------------

// narrowReason is one cell of the {decision, reason} counter. Kept reasons are
// ordered by precedence (lowest wins when a key has several list edges).
type narrowReason uint8

const (
	nrKeptDegraded narrowReason = iota
	nrKeptNoMask
	nrKeptUndecided
	nrKeptMembership
	nrKeptRegistryLate
	nrKeptInterestAll
	nrKeptInterestPending
	nrKeptReadField
	nrAvoidUnreadField
	narrowReasonCount
)

func (r narrowReason) avoided() bool { return r == nrAvoidUnreadField }

func (r narrowReason) decision() string {
	if r.avoided() {
		return "avoided"
	}
	return "kept"
}

func (r narrowReason) String() string {
	switch r {
	case nrKeptDegraded:
		return "degraded"
	case nrKeptNoMask:
		return "no_mask"
	case nrKeptUndecided:
		return "undecided"
	case nrKeptMembership:
		return "membership"
	case nrKeptRegistryLate:
		return "registry_late"
	case nrKeptInterestAll:
		return "interest_all"
	case nrKeptInterestPending:
		return "interest_pending"
	case nrKeptReadField:
		return "read_field"
	case nrAvoidUnreadField:
		return "unread_field"
	}
	return "unknown"
}

// narrowDecide classifies ONE list_dep mark of a key whose depSet is ds, for
// an event on (gvr, namespace) in the given probe state with the given mask.
// Read-only: it never changes what is marked.
func narrowDecide(ds *depSet, gvr schema.GroupVersionResource, namespace string, state objectState, mask *eventMask) narrowReason {
	// The list edges an event on (gvr, namespace) matches (rangeMatchBuckets).
	dks := [2]DepKey{{GVR: gvr, Namespace: namespace, Name: listWildcard}}
	nd := 1
	if namespace != "" {
		dks[1] = DepKey{GVR: gvr, Namespace: "", Name: listWildcard}
		nd = 2
	}
	var narrowed [2]*listInterest
	nn := 0
	best := narrowReasonCount
	keep := func(r narrowReason) {
		if r < best {
			best = r
		}
	}
	if ds == nil {
		return nrKeptInterestAll
	}
	for i := 0; i < nd; i++ {
		v, ok := ds.deps.Load(dks[i])
		if !ok {
			continue
		}
		ei, isEI := v.(*edgeInterest)
		if !isEI {
			keep(nrKeptInterestAll)
			continue
		}
		cur := ei.p.Load()
		switch {
		case cur == nil:
			keep(nrKeptInterestPending)
		case cur.all:
			keep(nrKeptInterestAll)
		default:
			narrowed[nn] = cur.li
			nn++
		}
	}
	if nn == 0 {
		if best == narrowReasonCount {
			// No list edge found on the depSet (a racing RemoveL1Key):
			// unknown, so kept.
			return nrKeptInterestAll
		}
		return best
	}
	switch {
	case state == objUnknownDegraded:
		keep(nrKeptDegraded)
	case mask == nil:
		keep(nrKeptNoMask)
	case mask.unknown:
		keep(nrKeptUndecided)
	case mask.membership || state == objAbsent:
		// An ADD or a DELETE (or the worker already sees the object gone
		// although the DELETE's own transition has not been merged yet).
		keep(nrKeptMembership)
	default:
		for i := 0; i < nn; i++ {
			li := narrowed[i]
			if li.gen > mask.gen {
				keep(nrKeptRegistryLate)
				continue
			}
			if _, ch := mask.changed[li.id]; ch {
				keep(nrKeptReadField)
			}
		}
	}
	if best < narrowReasonCount {
		return best
	}
	return nrAvoidUnreadField
}

// ListInterestStateForTest reports the interest state of (l1Key, dk):
// "absent", "all" (sticky), "pending", or "narrowed:<read set>". Cross-package
// test seam (restactions/api arms); production code MUST NOT call it.
func ListInterestStateForTest(l1Key string, dk DepKey) string {
	dsI, ok := Deps().reverse.Load(l1Key)
	if !ok {
		return "absent"
	}
	v, ok := dsI.(*depSet).deps.Load(dk)
	if !ok {
		return "absent"
	}
	ei, isEI := v.(*edgeInterest)
	if !isEI {
		return "all"
	}
	switch cur := ei.p.Load(); {
	case cur == nil:
		return "pending"
	case cur.all:
		return "all"
	default:
		return "narrowed:" + readsKey(cur.li.reads)
	}
}

// --- snapshots ----------------------------------------------------------------

// ListNarrowDecision is one {decision, reason} counter cell.
type ListNarrowDecision struct {
	Decision string
	Reason   string
	Count    uint64
}

// ListNarrowDecisionsSnapshot returns every {decision, reason} cell (zeros
// included, so the series exist from boot).
func ListNarrowDecisionsSnapshot() []ListNarrowDecision {
	n := Deps().narrow
	out := make([]ListNarrowDecision, 0, narrowReasonCount)
	for r := narrowReason(0); r < narrowReasonCount; r++ {
		out = append(out, ListNarrowDecision{Decision: r.decision(), Reason: r.String(), Count: n.decisions[r].Load()})
	}
	return out
}

// ListNarrowCommitVerdict is one {verdict, why} cell of the record-side commit
// counter.
type ListNarrowCommitVerdict struct {
	Verdict string
	Why     string
	Count   uint64
}

// ListNarrowCommitVerdictsSnapshot returns the commit verdicts seen so far.
func ListNarrowCommitVerdictsSnapshot() []ListNarrowCommitVerdict {
	var out []ListNarrowCommitVerdict
	Deps().narrow.verdicts.Range(func(k, v any) bool {
		kk := k.(commitVerdictKey)
		out = append(out, ListNarrowCommitVerdict{Verdict: kk.verdict, Why: kk.why, Count: v.(*atomic.Uint64).Load()})
		return true
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Verdict != out[j].Verdict {
			return out[i].Verdict < out[j].Verdict
		}
		return out[i].Why < out[j].Why
	})
	return out
}

// ListNarrowEvalStats returns the handler-side evaluation cost: transitions
// evaluated against >= 1 interest, and the nanoseconds they took.
func ListNarrowEvalStats() (transitions, ns uint64) {
	n := Deps().narrow
	return n.evalTransitions.Load(), n.evalNS.Load()
}

// ListNarrowInterests returns the number of interned interests.
func ListNarrowInterests() int64 { return Deps().narrow.n.Load() }

// ListNarrowExpvar is the /debug/vars view (expvar snowplow_deps_list_narrow).
// would_avoid_total and would_change_total are the two numbers the owner's
// enforcement gate reads; the rest is their denominator and their breakdown.
func ListNarrowExpvar() any {
	kept := map[string]uint64{}
	var avoided, keptTotal uint64
	for _, c := range ListNarrowDecisionsSnapshot() {
		if c.Decision == "avoided" {
			avoided += c.Count
			continue
		}
		kept[c.Reason] = c.Count
		keptTotal += c.Count
	}
	verdicts := map[string]uint64{}
	for _, v := range ListNarrowCommitVerdictsSnapshot() {
		verdicts[v.Verdict+"/"+v.Why] = v.Count
	}
	a := ListNarrowAuditSnapshot()
	tr, ns := ListNarrowEvalStats()
	return map[string]any{
		"would_avoid_total":    avoided,
		"would_mark_total":     keptTotal,
		"kept_by_reason":       kept,
		"would_change_total":   a[AuditVerdictChanged],
		"replays":              ListNarrowReplaySnapshot(),
		"gate":                 ListNarrowGateSnapshot(),
		"audit":                a,
		"commit_verdicts":      verdicts,
		"interests":            ListNarrowInterests(),
		"eval_transitions":     tr,
		"eval_ns_total":        ns,
		"list_dep_marks_total": avoided + keptTotal,
	}
}
