// list_cell.go — #854 (#765 C): the incremental list content cell.
//
// THE COST IT REMOVES (#606 run 12, investigator-storm2). The 50K-object apistage
// LIST content cell (71 MB stored) was re-filled — re-listed, re-concatenated and
// re-decoded (parseListEnvelope) — on every informer event that reached a
// refresher re-resolve of a dependent: 13–45 fills/min, ~12–28% of CPU, 26.5% of
// allocation and a ~1.3 GB transient per fill (run 13's OOM). The #765 A content
// stamp shares a fill between dependents but cannot avoid it: every object event
// moves a LIST cell's coordinate.
//
// THE MECHANISM. A LIST cell stored by a fill leader is ANCHORED: its items are
// kept as a sorted table of per-object slots, and every informer event on its
// GVR is applied to that table instead of re-filling it.
//
//   - A slot holds the indexer's OWN per-object JSON (bytesObject.raw — never
//     mutated after construction, returned by storeItemJSON with no copy) and the
//     object decoded from exactly those bytes by the full path's own per-item
//     decoder (encoding/json → map[string]any, then stripManagedFields, supplied
//     by the apistage layer). An unchanged object keeps its decoded form across
//     generations.
//   - The table is chunked (≈√N slots per chunk, recomputed at every anchor) and
//     copy-on-write: a generation copies only the chunks its changed keys touch.
//   - The envelope (RawJSON) is never stored; it is built on demand by the same
//     marshalAsListRaw over the same slices in the same order (Raw), and its exact
//     length is maintained arithmetically for size accounting.
//
// THE STAMP IS PART OF THE GENERATION (design MC1). A reader gets a generation
// only through Snapshot(need). The generation carries its own asOf; there is no
// cell-level stamp that could run ahead of the data:
//
//   - the dep hook records a changed key in `pending` BEFORE the event's seq bump
//     (noteListCellsPending, first thing in onObjectEventMasked);
//   - a build loads s := depEventSeq FIRST, then drains `pending` against the
//     indexer's CURRENT state (state-based: idempotent and order-free), then
//     publishes {asOf: s, body} in one atomic store.
//
// Any event e with seq(e) <= s had its key recorded before seq(e) was taken, and
// its indexer write happened before its hook (client-go notifies after the store
// write; #375 R1), so the drain read the key at or after e. No generation can
// claim an event it does not contain.
//
// WHEN THE CELL STOPS TRUSTING ITSELF (unanchor → the next read takes today's
// full fill, which re-anchors):
//   - the GVR's servability epoch moved (#870 owner ruling 1: it moves on any
//     EXIT FROM SERVABLE — a watch break, a confirmation retraction, a removal;
//     a re-creation after a removal is covered by the removal's bump), or the
//     GVR is not servable now. A store swap (store_swap.go) is NOT an exit: the
//     GVR stays servable throughout, and the keys whose bytes changed reach
//     the cell as ordinary per-key events (M), so the cell keeps its anchor;
//   - any lost-event signal moved: a dep-worker panic, an ADD seen with a nil
//     syncCh, a submit dropped onto a shut-down dep queue;
//   - a type-level event on the GVR (CRD available / removed);
//   - the shadow comparator found a divergence;
//   - the store dropped or replaced the entry (byte / entry-count pressure
//     (LRU), DELETE, a later Put).
//
// NO AGE BOUND (#882 F1, owner ruling). An anchored cell is not subject to the
// store TTL or the max entry age (itemExpiredLocked): its body is rebuilt from
// informer deltas, never by a Put, so its CreatedAt / BornAt say nothing about
// its freshness. It leaves only through the signals above or byte / entry-count
// pressure (LRU). The comparator pass below is its from-scratch check and
// REPLACES the #378 24 h max-age rebuild for it (no timer).
//
// THE COMPARATOR (zero tolerance). A pass compares the latest generation with a
// fresh, decode-free listing of the same partition and query: key set and raw
// bytes, the obj-from-raw identity of every slot, and a re-decode of one rotating
// chunk against its shared decoded objects (a consumer that mutates a shared item
// is caught here). A key whose event is still in flight — still in `pending`,
// queued or being processed by the dep worker, or with a mask awaiting the worker
// — is skipped. A key is divergent only when it mismatches in two consecutive
// passes with the same identities on both sides. The cadence is driven by READS
// and derived from cost: one pass per ⌈passCost / readCost⌉ Snapshot calls
// (per cell, EWMAs), so comparator CPU stays at or below read CPU. No timer, no
// knob. Divergence is counted, logged as metadata only, and unanchors the cell.
// A pass is REFRESHER work, never a reader's: a read that makes one due only
// marks it and enqueues the cell's own key, and the refresher runs it on that
// dequeue, after its customer yield (#710). At 50K objects of ~1.4 KB a pass
// allocates ~24 MB and takes ~0.1 s (list_cell_854_cost_test.go).
//
// NOT IN PASSTHROUGH. With CACHE_ENABLED=false there is no informer and no event:
// a cell is never anchored there.
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// ListCellFuncs are the apistage layer's own functions, so the incremental cell
// reproduces the full fill exactly: the per-item decoder parseListEnvelope uses,
// the query predicate filterRaw uses, and the envelope writer marshalAsListRaw.
type ListCellFuncs struct {
	// Decode decodes ONE item exactly as parseListEnvelope decodes an element of
	// the envelope (encoding/json into map[string]any, then stripManagedFields).
	Decode func(raw []byte) (*unstructured.Unstructured, error)
	// Match reports whether the item belongs to the cell's query (always true for
	// an empty query). ok=false when the item cannot be read (the full path falls
	// through then; the cell unanchors).
	Match func(raw []byte) (match, ok bool)
	// Marshal is marshalAsListRaw over the cell's apiVersion and list kind.
	Marshal func(raws [][]byte) ([]byte, error)
}

type listObjKey struct{ ns, name string }

func (k listObjKey) less(o listObjKey) bool {
	if k.ns != o.ns {
		return k.ns < o.ns
	}
	return k.name < o.name
}

// rawID is the identity of a raw slice: its first byte and its length.
type rawID struct {
	p *byte
	n int
}

func idOf(b []byte) rawID {
	if len(b) == 0 {
		return rawID{}
	}
	return rawID{p: &b[0], n: len(b)}
}

type listSlot struct {
	key    listObjKey
	raw    []byte
	obj    *unstructured.Unstructured
	objSrc rawID // the raw obj was decoded from
}

type listChunk struct {
	slots []listSlot // sorted; immutable once published
}

// listBody is one immutable state of the table.
type listBody struct {
	// seq is the body's identity (#643 served cells): a new body — a change
	// applied — takes a new one; a re-stamp keeps the body and its seq.
	seq    uint64
	chunks []*listChunk
	n      int
	sumRaw int64
	items  atomic.Pointer[[]*unstructured.Unstructured]
	raw    atomic.Pointer[[]byte]
}

// listGen is a body plus the dep-event seq it is complete as of.
type listGen struct {
	asOf uint64
	body *listBody
}

// ListView is one published generation, as a reader sees it.
type ListView struct {
	cell *ListCell
	gen  *listGen
}

// ListCell is one anchored LIST content cell.
type ListCell struct {
	key              string
	gvr              schema.GroupVersionResource
	namespace        string
	apiVersion, kind string
	funcs            ListCellFuncs
	chunkSize        int
	emptyEnv         int64 // len(Marshal(nil)): the envelope's scaffolding

	anchored atomic.Bool
	gen      atomic.Pointer[listGen]

	mu      sync.Mutex
	pending map[listObjKey]struct{}

	// anchor-time trust state.
	servEpoch uint64
	lostSig   uint64

	// comparator.
	reads      atomic.Uint64
	lastPassAt atomic.Uint64 // reads at the last pass
	passDue    atomic.Bool   // a read asked for a pass; the refresher runs it
	passNS     atomic.Int64  // EWMA of one pass, ns
	readNS     atomic.Int64  // EWMA of one Snapshot, ns
	cmpMu      sync.Mutex
	suspects   map[listObjKey][2]rawID
	rotate     int

	// store sizing (entry bytes are re-accounted after each build).
	store *ResolvedCacheStore
	entry atomic.Pointer[ResolvedEntry]
}

// listBodySeq issues body identities. Never reused; never 0 for a body.
var listBodySeq atomic.Uint64

// listBodySeq returns the identity of the entry's current incremental body
// (0 for a plain entry). Called under the store's lock: one atomic load.
func (e *ResolvedEntry) listBodySeq() uint64 {
	if e == nil || e.listCell == nil {
		return 0
	}
	if g := e.listCell.gen.Load(); g != nil {
		return g.body.seq
	}
	return 0
}

// ── registry ────────────────────────────────────────────────────────────────

type listCellSet struct {
	mu sync.Mutex
	// cells holds every cell of the GVR that events must reach: the published
	// cells AND the cells a fill is building (#882 F2: a fill registers its
	// cell BEFORE it lists the informer, so every edit during the fill is
	// recorded in that cell's pending). Two cells of one content key — the
	// published one and its replacement being filled — are both here.
	cells map[*ListCell]struct{}
	// #870 F5 — the comparator's "delivered" record is no longer kept here: it
	// is the live instance's dep-handler `seen` record (store_instance.go),
	// exact for its listener's order from the instance's construction, muted
	// deliveries included, so a swap leaves no key looking permanently in
	// flight. deliveredMismatch reads it.
}

var (
	listCellsByGVR sync.Map // schema.GroupVersionResource → *listCellSet
	listCellByKey  sync.Map // content key → *ListCell (published cells only)
)

func listCellSetFor(gvr schema.GroupVersionResource) *listCellSet {
	v, _ := listCellsByGVR.LoadOrStore(gvr, &listCellSet{cells: map[*ListCell]struct{}{}})
	return v.(*listCellSet)
}

// watchListCell makes c receive its GVR's events (pending, deliveries, type
// events). A fill calls it before its informer listing (BeginListCell).
func watchListCell(c *ListCell) {
	s := listCellSetFor(c.gvr)
	s.mu.Lock()
	s.cells[c] = struct{}{}
	s.mu.Unlock()
}

// publishListCell makes c its key's cell, unanchoring the one it replaces.
func publishListCell(c *ListCell) {
	if prev, ok := listCellByKey.Swap(c.key, c); ok {
		if p := prev.(*ListCell); p != c {
			p.unanchor("replaced")
		}
	}
}

func deregisterListCell(c *ListCell) {
	listCellByKey.CompareAndDelete(c.key, c)
	s := listCellSetFor(c.gvr)
	s.mu.Lock()
	delete(s.cells, c)
	s.mu.Unlock()
}

// cellsOf copies the GVR set's cells (the per-cell work runs outside s.mu).
func (s *listCellSet) cellsOf() []*ListCell {
	s.mu.Lock()
	defer s.mu.Unlock()
	cells := make([]*ListCell, 0, len(s.cells))
	for c := range s.cells {
		cells = append(cells, c)
	}
	return cells
}

// deliveredMismatch reports whether the indexer's current object for k (idx, the
// zero rawID when absent) is not the last one delivered to the dep handler of
// the GVR's live instance (#870 F5: its `seen` record, store_instance.go). A
// key with no delivery and no indexer object is settled; one present in the
// indexer and never delivered is an ADD still on its way — in flight. With no
// record at all (the GVR has no live instance) nothing can be judged, and the
// key reads as in flight.
func deliveredMismatch(gvr schema.GroupVersionResource, k listObjKey, idx rawID) bool {
	id, _, known := deliveredRaw(gvr, k)
	if !known {
		return true
	}
	return id != idx
}

// noteListCellsPending records (gvr, namespace, name) as changed in every live
// cell of the GVR whose partition holds it. Called FIRST in
// onObjectEventMasked, before the event's seq bump (MC1). Cheap: one sync.Map
// load and, per cell of the GVR, one map insert under the cell's lock.
func noteListCellsPending(gvr schema.GroupVersionResource, namespace, name string) {
	v, ok := listCellsByGVR.Load(gvr)
	if !ok {
		return
	}
	cells := v.(*listCellSet).cellsOf()
	k := listObjKey{ns: namespace, name: name}
	for _, c := range cells {
		if c.namespace != "" && c.namespace != namespace {
			continue
		}
		c.mu.Lock()
		if c.pending != nil {
			c.pending[k] = struct{}{}
		}
		c.mu.Unlock()
		listCellStats.pendingRecorded.Add(1)
	}
}

// unanchorListCellsForGVR drops every cell of the GVR back to the full fill (a
// type-level event: CRD available / removed / schema relisted / store repaired).
func unanchorListCellsForGVR(gvr schema.GroupVersionResource, reason string) {
	v, ok := listCellsByGVR.Load(gvr)
	if !ok {
		return
	}
	for _, c := range v.(*listCellSet).cellsOf() {
		c.unanchor(reason)
	}
}

// closeListCellOf unanchors the cell an entry carries (the store dropped or
// replaced it). Called under the store's lock: takes only the registry locks.
func closeListCellOf(e *ResolvedEntry, reason string) {
	if e == nil || e.listCell == nil {
		return
	}
	e.listCell.unanchor(reason)
}

func (c *ListCell) unanchor(reason string) {
	if c == nil || !c.anchored.CompareAndSwap(true, false) {
		return
	}
	deregisterListCell(c)
	listCellStats.unanchor(reason)
}

// ── anchor ──────────────────────────────────────────────────────────────────

// ListCellAnchor is what AnchorListCell takes: a fill's envelope and its parsed
// items, with the cell's identity and functions.
type ListCellAnchor struct {
	Key        string
	GVR        schema.GroupVersionResource
	Namespace  string // the partition ("" = cluster-wide)
	Envelope   []byte // the dispatched marshalAsListRaw envelope
	Items      []*unstructured.Unstructured
	APIVersion string
	Kind       string
	Funcs      ListCellFuncs
}

// ListCellFill is a cell being built by one fill (#882 F2). BeginListCell
// creates it BEFORE the fill lists the informer, so every edit from then on is
// recorded in the cell's pending; Anchor turns the fill's envelope into the
// cell; Abandon drops it when the fill ends without an Anchor.
type ListCellFill struct {
	c     *ListCell
	shape envelopeShape
	done  bool
}

// BeginListCell starts a cell for a fill that is about to list the informer for
// (gvr, namespace). nil when no cell can be anchored here (passthrough, cache
// off, anchoring off for a test, a writer whose envelope shape is not
// derivable); the fill then stays a plain fill.
//
// The cell receives its GVR's events from here on (pending, deliveries, type
// events) and captures its trust state (servability epoch, lost-event signal):
// an exit from servable or a lost event during the fill makes its first read
// unanchor it.
func BeginListCell(key string, gvr schema.GroupVersionResource, namespace string, funcs ListCellFuncs) *ListCellFill {
	if listCellAnchorOffForTest.Load() {
		return nil
	}
	rw := Global()
	if rw == nil || rw.mode == modePassthrough || Disabled() || key == "" ||
		funcs.Decode == nil || funcs.Match == nil || funcs.Marshal == nil {
		listCellStats.anchorSkipped.Add(1)
		return nil
	}
	shape, ok := deriveEnvelopeShape(funcs.Marshal)
	if !ok {
		listCellStats.anchorSkipped.Add(1)
		return nil
	}
	c := &ListCell{
		key:       key,
		gvr:       gvr,
		namespace: namespace,
		funcs:     funcs,
		emptyEnv:  int64(len(shape.head) + len(shape.tail)),
		pending:   map[listObjKey]struct{}{},
		suspects:  map[listObjKey][2]rawID{},
	}
	c.servEpoch = rw.ServabilityEpoch(gvr)
	c.lostSig = depWatchLostEventSignal()
	c.anchored.Store(true)
	watchListCell(c)
	return &ListCellFill{c: c, shape: shape}
}

// ListCellAnchorable reports whether a LIST fill of gvr would anchor an
// incremental cell if its envelope pairs: anchoring is available (informer
// mode, cache on, not turned off for a test) and the GVR is servable now. The
// content fill marks such a flight incremental (#882 F2).
func ListCellAnchorable(gvr schema.GroupVersionResource) bool {
	if listCellAnchorOffForTest.Load() || Disabled() {
		return false
	}
	rw := Global()
	return rw != nil && rw.mode != modePassthrough && rw.IsServable(gvr)
}

// Abandon drops a cell whose fill ended without an Anchor (not servable, not
// admitted, a malformed envelope). nil-safe; a no-op after Anchor.
func (f *ListCellFill) Abandon() {
	if f == nil || f.done {
		return
	}
	f.done = true
	f.c.unanchor("anchor_abandoned")
}

// AnchorListCell is BeginListCell then Anchor, for a caller whose envelope was
// listed before the call (the cache package's own arms). The fill path calls
// BeginListCell before its listing, which is what makes an edit during the
// fill a recorded key.
func AnchorListCell(a ListCellAnchor) *ListCell {
	return BeginListCell(a.Key, a.GVR, a.Namespace, a.Funcs).Anchor(a.Envelope, a.Items, a.APIVersion, a.Kind)
}

// Anchor builds the cell from the fill's just-dispatched envelope and the items
// parsed from it (decode once), or returns nil (the entry stays a plain fill).
//
// It reads the dep-event seq s, lists the partition again (decode-free) and
// pairs it key by key with the envelope. A key whose envelope bytes are the
// listing's keeps the item parsed from them: the pairing proves it was decoded
// from exactly the bytes the indexer holds. A key that differs — changed,
// created or deleted while the fill ran — is an edit DURING the fill (#882 F2):
// the cell has been watching since BeginListCell, so that edit is recorded in
// pending or is still on its way to the dep hook (in flight). Such a key takes
// the listing's bytes, decoded once; it is never an abort. A key that differs
// with neither explanation is refused (anchor_raced): the envelope is not this
// informer's.
//
// The body is complete as of s (the R1 argument: every event <= s wrote the
// indexer before the listing), and that is all correctness needs: every reader
// takes the cell through Snapshot(need), which drains pending itself. The drain
// before the publish below is an OPTIMISATION: the listing already reflects
// almost every edit the fill overlapped, so it only advances the first
// generation's stamp past the edits recorded since s, sparing the first readers
// that build, and gives the surfaces that read the published generation without
// a Snapshot (ResolvedEntry.Raw / RawLen / itemCount: debug and fallback
// readers) a body with those edits.
func (f *ListCellFill) Anchor(envelope []byte, items []*unstructured.Unstructured, apiVersion, kind string) *ListCell {
	if f == nil || f.done {
		return nil
	}
	f.done = true
	c := f.c
	if !c.anchored.Load() {
		return nil // dropped during the fill (a type-level event): counted by its reason
	}
	c.apiVersion, c.kind = apiVersion, kind
	rw := Global()
	if rw == nil {
		c.unanchor("anchor_not_servable")
		return nil
	}
	s := depEventSeq.Load()
	keyed, ok := rw.listRawKeyedServable(c.gvr, c.namespace)
	if !ok {
		c.unanchor("anchor_not_servable")
		return nil
	}
	keyed, ok = c.filter(keyed)
	if !ok {
		c.unanchor("anchor_unreadable")
		return nil
	}
	slots, settling, ok := c.pair(rw, f.shape, envelope, items, keyed)
	if !ok {
		c.unanchor("anchor_raced")
		return nil
	}
	n := len(slots)
	c.chunkSize = chunkSizeFor(n)
	body := &listBody{n: n, seq: listBodySeq.Add(1)}
	for i := 0; i < n; i += c.chunkSize {
		j := min(i+c.chunkSize, n)
		// Each chunk owns its slice (as a copy-on-write replacement does), so a
		// chunk that is never touched again does not pin the whole table.
		ch := &listChunk{slots: append(make([]listSlot, 0, j-i), slots[i:j]...)}
		for _, sl := range ch.slots {
			body.sumRaw += int64(len(sl.raw))
		}
		body.chunks = append(body.chunks, ch)
	}
	c.gen.Store(&listGen{asOf: s, body: body})
	// #882 F2 — the edits recorded while the fill ran, applied before anyone
	// can read the cell (an optimisation, see the doc above: Snapshot drains
	// pending for every reader anyway).
	if _, err := c.build(); err != nil {
		c.unanchor("build_" + err.Error())
		return nil
	}
	publishListCell(c)
	if !c.anchored.Load() {
		// Dropped between the drain and the publish: never leave it the key's.
		listCellByKey.CompareAndDelete(c.key, c)
		return nil
	}
	listCellStats.anchors.Add(1)
	listCellStats.anchorSettling.Add(uint64(settling))
	return c
}

// pair walks the envelope and the listing in key order (both sorted by
// namespace, then name: indexerPartition) and returns the cell's slots and how
// many keys an edit during the fill explained (settling). ok=false when the
// envelope is not this writer's shape, an item has no key, a differing key is
// neither pending nor in flight, or a changed object does not decode.
func (c *ListCell) pair(rw *ResourceWatcher, sh envelopeShape, env []byte, items []*unstructured.Unstructured, keyed []keyedRaw) (slots []listSlot, settling int, ok bool) {
	if !bytes.HasPrefix(env, sh.head) {
		return nil, 0, false
	}
	rest := env[len(sh.head):]
	consumed := 0 // envelope items stepped onto
	// next positions rest at the next envelope item and returns its length.
	next := func() (int, bool) {
		if consumed > 0 {
			if !bytes.HasPrefix(rest, sh.sep) {
				return 0, false
			}
			rest = rest[len(sh.sep):]
		}
		consumed++
		return jsonValueLen(rest)
	}
	// fromListing takes an edited key from the listing, decoded once.
	fromListing := func(kr keyedRaw) bool {
		if !c.settling(rw, kr.key, kr.raw) {
			return false
		}
		obj, err := c.funcs.Decode(kr.raw)
		if err != nil {
			return false
		}
		listCellStats.decodes.Add(1)
		slots = append(slots, listSlot{key: kr.key, raw: kr.raw, obj: obj, objSrc: idOf(kr.raw)})
		settling++
		return true
	}
	slots = make([]listSlot, 0, len(keyed))
	i, j := 0, 0
	for i < len(items) || j < len(keyed) {
		var ki listObjKey
		if i < len(items) {
			if ki, ok = itemKey(items[i]); !ok {
				return nil, 0, false
			}
		}
		switch {
		case i < len(items) && j < len(keyed) && ki == keyed[j].key:
			kr := keyed[j]
			l, ok := next()
			if !ok {
				return nil, 0, false
			}
			if l == len(kr.raw) && bytes.Equal(rest[:l], kr.raw) {
				slots = append(slots, listSlot{key: kr.key, raw: kr.raw, obj: items[i], objSrc: idOf(kr.raw)})
			} else if !fromListing(kr) {
				return nil, 0, false
			}
			rest = rest[l:]
			i++
			j++
		case i < len(items) && (j == len(keyed) || ki.less(keyed[j].key)):
			// In the envelope, not in the listing: deleted (or out of the
			// selector) while the fill ran.
			l, ok := next()
			if !ok || !c.settling(rw, ki, nil) {
				return nil, 0, false
			}
			rest = rest[l:]
			settling++
			i++
		default:
			// In the listing, not in the envelope: created (or into the
			// selector) while the fill ran.
			if !fromListing(keyed[j]) {
				return nil, 0, false
			}
			j++
		}
	}
	if consumed != len(items) || !bytes.Equal(rest, sh.tail) {
		return nil, 0, false
	}
	return slots, settling, true
}

// itemKey is a parsed item's (namespace, name): indexerSlotKey's key.
func itemKey(u *unstructured.Unstructured) (listObjKey, bool) {
	if u == nil || u.GetName() == "" {
		return listObjKey{}, false
	}
	return listObjKey{ns: u.GetNamespace(), name: u.GetName()}, true
}

// jsonValueLen is the length of the JSON object or array b starts with, or
// ok=false. A byte scan (strings and escapes honoured), no decode: the anchor
// uses it only to step over an envelope item whose bytes are not the listing's.
func jsonValueLen(b []byte) (int, bool) {
	depth := 0
	inStr := false
	for i := 0; i < len(b); i++ {
		ch := b[i]
		if inStr {
			switch ch {
			case '\\':
				i++
			case '"':
				inStr = false
			}
			continue
		}
		switch ch {
		case '"':
			if depth == 0 {
				return 0, false
			}
			inStr = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth == 0 {
				return i + 1, true
			}
			if depth < 0 {
				return 0, false
			}
		default:
			if depth == 0 {
				return 0, false
			}
		}
	}
	return 0, false
}

// settling reports whether k's latest edit is explained: still on its way to
// the dep hook (inFlight), or recorded in pending. In flight is read FIRST: an
// edit leaves flight only after its hook recorded it in pending, so a key that
// is neither, read in this order, has no edit outstanding. idxRaw is the key's
// object in the (filtered) listing, nil if absent there.
func (c *ListCell) settling(rw *ResourceWatcher, k listObjKey, idxRaw []byte) bool {
	if c.inFlight(rw, k, idxRaw) {
		return true
	}
	c.mu.Lock()
	_, p := c.pending[k]
	c.mu.Unlock()
	return p
}

// inFlight reports whether k's latest edit has not reached the dep hook yet:
// queued or being processed by the dep worker, or not yet delivered (its
// indexer object is not the last one delivered). idxRaw is the key's object in
// the filtered listing, nil when absent there: then the indexer's own object
// (unfiltered) is compared, so an object that left the selector is not
// mistaken for one still on its way.
func (c *ListCell) inFlight(rw *ResourceWatcher, k listObjKey, idxRaw []byte) bool {
	if depWatchInFlight(depEventKey{gvr: c.gvr, namespace: k.ns, name: k.name}) {
		return true
	}
	if idxRaw == nil {
		raw, exists, servable := rw.rawByKey(c.gvr, k.ns, k.name)
		if !servable {
			return true // nothing can be judged; Snapshot unanchors on the next read
		}
		if exists {
			idxRaw = raw
		}
	}
	return deliveredMismatch(c.gvr, k, idOf(idxRaw))
}

// envelopeShape is the scaffolding of the cell's own envelope writer
// (marshalAsListRaw), DERIVED from that writer, never spelled here:
// Marshal(raws) == head + raws joined by sep + tail.
type envelopeShape struct{ head, tail, sep []byte }

// deriveEnvelopeShape reads the shape off the writer's output for zero, one and
// two one-byte items, and checks it reproduces all three exactly. The separator
// must be one byte: envLen counts one per separator. Any other writer is not
// anchored (the entry stays a plain fill).
func deriveEnvelopeShape(marshal func([][]byte) ([]byte, error)) (envelopeShape, bool) {
	item := []byte("0")
	zero, err0 := marshal(nil)
	one, err1 := marshal([][]byte{item})
	two, err2 := marshal([][]byte{item, item})
	if err0 != nil || err1 != nil || err2 != nil {
		return envelopeShape{}, false
	}
	p := 0
	for p < len(zero) && p < len(one) && zero[p] == one[p] {
		p++
	}
	sh := envelopeShape{head: zero[:p:p], tail: zero[p:]}
	if len(two) < len(zero)+2 {
		return envelopeShape{}, false
	}
	mid := two[len(sh.head) : len(two)-len(sh.tail)]
	sh.sep = mid[len(item) : len(mid)-len(item)]
	if len(sh.sep) != 1 ||
		!sh.holds(one, []keyedRaw{{raw: item}}) ||
		!sh.holds(two, []keyedRaw{{raw: item}, {raw: item}}) {
		return envelopeShape{}, false
	}
	return sh, true
}

// holds reports whether env is exactly the envelope the writer produces over
// keyed's raws: the anchor's pairing proof, compared in place (no envelope is
// built).
func (sh envelopeShape) holds(env []byte, keyed []keyedRaw) bool {
	if !bytes.HasPrefix(env, sh.head) {
		return false
	}
	rest := env[len(sh.head):]
	for i, kr := range keyed {
		if i > 0 {
			if !bytes.HasPrefix(rest, sh.sep) {
				return false
			}
			rest = rest[len(sh.sep):]
		}
		if !bytes.HasPrefix(rest, kr.raw) {
			return false
		}
		rest = rest[len(kr.raw):]
	}
	return bytes.Equal(rest, sh.tail)
}

// chunkSizeFor is ⌈√n⌉ (at least 16): a change copies one chunk (≈√n slots) plus
// the chunk list (≈√n pointers), which minimises O(d·c + n/c) for a single
// change. Recomputed at every anchor; not a knob.
func chunkSizeFor(n int) int {
	c := int(math.Ceil(math.Sqrt(float64(n))))
	if c < 16 {
		c = 16
	}
	return c
}

// filter keeps the objects the cell's selector matches. keyed is returned as is
// when every object matches (a selector-free cell): out is allocated only at
// the first object dropped.
func (c *ListCell) filter(keyed []keyedRaw) ([]keyedRaw, bool) {
	var out []keyedRaw
	for i, kr := range keyed {
		m, ok := c.funcs.Match(kr.raw)
		if !ok {
			return nil, false
		}
		switch {
		case !m && out == nil:
			out = make([]keyedRaw, i, len(keyed))
			copy(out, keyed[:i])
		case m && out != nil:
			out = append(out, kr)
		}
	}
	if out == nil {
		return keyed, true
	}
	return out, true
}

// ── read ────────────────────────────────────────────────────────────────────

// Snapshot returns a generation complete as of at least need, building one from
// pending when the published one is older. ok=false means the cell is (now)
// unanchored: the caller takes today's full fill.
func (c *ListCell) Snapshot(need uint64) (ListView, bool) {
	if c == nil || !c.anchored.Load() {
		return ListView{}, false
	}
	t0 := time.Now()
	defer func() { ewmaNS(&c.readNS, time.Since(t0)) }()
	if reason, ok := c.trusted(); !ok {
		c.unanchor(reason)
		return ListView{}, false
	}
	g := c.gen.Load()
	if g == nil {
		return ListView{}, false
	}
	if g.asOf < need {
		var err error
		if g, err = c.build(); err != nil {
			c.unanchor("build_" + err.Error())
			return ListView{}, false
		}
	}
	c.maybePass()
	listCellStats.reads.Add(1)
	return ListView{cell: c, gen: g}, true
}

// trusted re-checks the anchor's trust state: the GVR still servable with an
// unchanged servability epoch, and no lost-event signal since the anchor.
func (c *ListCell) trusted() (string, bool) {
	rw := Global()
	if rw == nil {
		return "no_watcher", false
	}
	if rw.ServabilityEpoch(c.gvr) != c.servEpoch {
		return "servability_epoch", false
	}
	if !rw.IsServable(c.gvr) {
		return "not_servable", false
	}
	if depWatchLostEventSignal() != c.lostSig {
		return "lost_event", false
	}
	return "", true
}

type buildErr string

func (e buildErr) Error() string { return string(e) }

// build drains pending into a new generation (MC1: s is read BEFORE the drain).
func (c *ListCell) build() (*listGen, error) {
	c.mu.Lock()
	s := depEventSeq.Load()
	keys := c.pending
	c.pending = make(map[listObjKey]struct{}, len(keys))
	cur := c.gen.Load()
	var g *listGen
	if len(keys) == 0 {
		g = &listGen{asOf: s, body: cur.body}
		c.gen.Store(g)
		c.mu.Unlock()
		listCellStats.restamps.Add(1)
		return g, nil
	}
	body, err := c.apply(cur.body, keys)
	if err != nil {
		// The keys stay unapplied; the cell unanchors in the caller.
		c.mu.Unlock()
		return nil, err
	}
	if h := listCellBuildHookForTest.Load(); h != nil {
		(*h)() // TEST-ONLY: between the drain and the publish (F3a)
	}
	if body == cur.body {
		// Nothing changed (a re-delivery of the same indexer object): keep the
		// body — and its identity — and only advance the stamp.
		g = &listGen{asOf: s, body: body}
		c.gen.Store(g)
		c.mu.Unlock()
		listCellStats.restamps.Add(1)
		return g, nil
	}
	g = &listGen{asOf: s, body: body}
	c.gen.Store(g)
	c.mu.Unlock()
	listCellStats.generations.Add(1)
	listCellStats.appliedKeys.Add(uint64(len(keys)))
	c.resize(body)
	return g, nil
}

// apply returns a new body with every key in keys re-read from the indexer.
// Copy-on-write per chunk: only the chunks the keys touch are copied.
func (c *ListCell) apply(cur *listBody, keys map[listObjKey]struct{}) (*listBody, error) {
	rw := Global()
	if rw == nil {
		return nil, buildErr("no_watcher")
	}
	sorted := make([]listObjKey, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].less(sorted[j]) })

	chunks := append([]*listChunk(nil), cur.chunks...)
	copied := map[int]bool{}
	changed := false
	nb := &listBody{n: cur.n, sumRaw: cur.sumRaw, seq: listBodySeq.Add(1)}
	alloc := int64(len(chunks)) * 8
	for _, k := range sorted {
		raw, exists, servable := rw.rawByKey(c.gvr, k.ns, k.name)
		if !servable {
			return nil, buildErr("not_servable")
		}
		if exists {
			m, ok := c.funcs.Match(raw)
			if !ok {
				return nil, buildErr("unreadable_item")
			}
			exists = m
		}
		ci, si, found := locate(chunks, k)
		switch {
		case exists && found:
			old := chunks[ci].slots[si]
			if idOf(old.raw) == idOf(raw) {
				continue // the same indexer slice: nothing changed
			}
			if !copied[ci] {
				chunks[ci] = &listChunk{slots: append([]listSlot(nil), chunks[ci].slots...)}
				copied[ci] = true
				alloc += int64(len(chunks[ci].slots)) * slotBytes
			}
			changed = true
			ns := old
			ns.raw = raw
			if bytes.Equal(old.raw, raw) {
				// C6 — a byte-equal re-delivery (resync, a relist that changed
				// nothing): rebind to the indexer's current slice so the old one
				// is not pinned. The decoded object decodes the same bytes.
				ns.objSrc = idOf(raw)
				listCellStats.rebinds.Add(1)
			} else {
				obj, err := c.funcs.Decode(raw)
				if err != nil {
					return nil, buildErr("decode")
				}
				ns.obj, ns.objSrc = obj, idOf(raw)
				listCellStats.decodes.Add(1)
			}
			chunks[ci].slots[si] = ns
			nb.sumRaw += int64(len(raw)) - int64(len(old.raw))
		case exists && !found:
			obj, err := c.funcs.Decode(raw)
			if err != nil {
				return nil, buildErr("decode")
			}
			listCellStats.decodes.Add(1)
			changed = true
			slot := listSlot{key: k, raw: raw, obj: obj, objSrc: idOf(raw)}
			if len(chunks) == 0 {
				chunks = append(chunks, &listChunk{slots: []listSlot{slot}})
				copied[0] = true
			} else {
				if !copied[ci] {
					chunks[ci] = &listChunk{slots: append([]listSlot(nil), chunks[ci].slots...)}
					copied[ci] = true
					alloc += int64(len(chunks[ci].slots)) * slotBytes
				}
				sl := chunks[ci].slots
				sl = append(sl, listSlot{})
				copy(sl[si+1:], sl[si:])
				sl[si] = slot
				chunks[ci].slots = sl
				if len(sl) > 2*c.chunkSize {
					half := len(sl) / 2
					left := &listChunk{slots: append([]listSlot(nil), sl[:half]...)}
					right := &listChunk{slots: append([]listSlot(nil), sl[half:]...)}
					chunks = append(chunks[:ci], append([]*listChunk{left, right}, chunks[ci+1:]...)...)
					copied = shiftCopied(copied, ci)
				}
			}
			nb.n++
			nb.sumRaw += int64(len(raw))
		case !exists && found:
			if !copied[ci] {
				chunks[ci] = &listChunk{slots: append([]listSlot(nil), chunks[ci].slots...)}
				copied[ci] = true
				alloc += int64(len(chunks[ci].slots)) * slotBytes
			}
			changed = true
			old := chunks[ci].slots[si]
			chunks[ci].slots = append(chunks[ci].slots[:si], chunks[ci].slots[si+1:]...)
			if len(chunks[ci].slots) == 0 {
				chunks = append(chunks[:ci], chunks[ci+1:]...)
				copied = dropCopied(copied, ci)
			}
			nb.n--
			nb.sumRaw -= int64(len(old.raw))
		}
	}
	if !changed {
		return cur, nil
	}
	nb.chunks = chunks
	listCellStats.noteGenAlloc(alloc)
	return nb, nil
}

// slotBytes is the size of one listSlot (key strings' headers, raw's header, two
// pointers and the identity), for the per-generation allocation report.
const slotBytes = int64(2*16 + 24 + 8 + 16)

func shiftCopied(m map[int]bool, split int) map[int]bool {
	out := make(map[int]bool, len(m)+1)
	for i := range m {
		switch {
		case i < split:
			out[i] = true
		case i > split:
			out[i+1] = true
		}
	}
	out[split], out[split+1] = true, true
	return out
}

func dropCopied(m map[int]bool, dropped int) map[int]bool {
	out := make(map[int]bool, len(m))
	for i := range m {
		switch {
		case i < dropped:
			out[i] = true
		case i > dropped:
			out[i-1] = true
		}
	}
	return out
}

// locate finds k: the chunk it is (or would be inserted) in, its index there,
// and whether it is present.
func locate(chunks []*listChunk, k listObjKey) (ci, si int, found bool) {
	if len(chunks) == 0 {
		return 0, 0, false
	}
	// The first chunk whose last key is >= k (or the last chunk).
	ci = sort.Search(len(chunks), func(i int) bool {
		sl := chunks[i].slots
		return !sl[len(sl)-1].key.less(k)
	})
	if ci == len(chunks) {
		ci = len(chunks) - 1
	}
	sl := chunks[ci].slots
	si = sort.Search(len(sl), func(i int) bool { return !sl[i].key.less(k) })
	return ci, si, si < len(sl) && sl[si].key == k
}

// resize re-accounts the entry's bytes in the store after a build.
func (c *ListCell) resize(b *listBody) {
	if c.store == nil {
		return
	}
	if e := c.entry.Load(); e != nil {
		c.store.reaccountEntry(c.key, e)
	}
}

// ── views ───────────────────────────────────────────────────────────────────

// AsOf is the dep-event seq the generation is complete as of.
func (v ListView) AsOf() uint64 {
	if v.gen == nil {
		return 0
	}
	return v.gen.asOf
}

// Len is the number of items.
func (v ListView) Len() int {
	if v.gen == nil {
		return 0
	}
	return v.gen.body.n
}

// APIVersion / Kind are the envelope's.
func (v ListView) APIVersion() string { return v.cell.apiVersion }
func (v ListView) Kind() string       { return v.cell.kind }

// Items is the contiguous item slice of this generation, built once per
// generation and shared by every reader of it (the same sharing contract as
// EnsureItems' published memo: readers never mutate it).
func (v ListView) Items() []*unstructured.Unstructured {
	if v.gen == nil {
		return nil
	}
	b := v.gen.body
	if p := b.items.Load(); p != nil {
		return *p
	}
	out := make([]*unstructured.Unstructured, 0, b.n)
	for _, ch := range b.chunks {
		for _, s := range ch.slots {
			out = append(out, s.obj)
		}
	}
	listCellStats.itemsBuilt.Add(1)
	listCellStats.noteGenAlloc(int64(len(out)) * 8)
	b.items.CompareAndSwap(nil, &out)
	return *b.items.Load()
}

// Raw is the envelope marshalAsListRaw would produce for this generation, built
// once per generation and only on demand (counted: rawjson_built).
func (v ListView) Raw() []byte {
	if v.gen == nil {
		return nil
	}
	b := v.gen.body
	if p := b.raw.Load(); p != nil {
		return *p
	}
	raws := make([][]byte, 0, b.n)
	for _, ch := range b.chunks {
		for _, s := range ch.slots {
			raws = append(raws, s.raw)
		}
	}
	env, err := v.cell.funcs.Marshal(raws)
	if err != nil {
		return nil
	}
	listCellStats.rawBuilt.Add(1)
	b.raw.CompareAndSwap(nil, &env)
	return *b.raw.Load()
}

// envLen is the exact length Raw would have, without building it.
func (b *listBody) envLen(emptyEnv int64) int64 {
	l := emptyEnv + b.sumRaw
	if b.n > 1 {
		l += int64(b.n - 1)
	}
	return l
}

// ── comparator ──────────────────────────────────────────────────────────────

// maybePass makes a comparator pass due when this cell's reads since the last
// pass reach ⌈passCost / readCost⌉ (at least 1; the first read after the anchor,
// while the pass cost is unknown, makes one due). It does not run the pass: it
// enqueues the cell's own key, and the refresher runs the pass on that dequeue
// (runDuePass) after yielding to customers. The workqueue coalesces the key, and
// at most one pass is due at a time.
func (c *ListCell) maybePass() {
	r := c.reads.Add(1)
	last := c.lastPassAt.Load()
	every := uint64(1)
	if p, rd := c.passNS.Load(), c.readNS.Load(); p > 0 && rd > 0 {
		every = uint64(math.Ceil(float64(p) / float64(rd)))
	}
	if r-last < every || !c.passDue.CompareAndSwap(false, true) {
		return
	}
	c.lastPassAt.Store(r)
	EnqueueRefresh(c.key)
}

// runDuePass runs the pass a read made due, if any. Called by the refresher on
// its dequeue of the cell's key, after the customer yield and the rate floor.
// #871 — a pass that runs is one comparator unit (pprof label + counter); ctx is
// the refresher unit's, so its label is restored when the pass returns.
func (c *ListCell) runDuePass(ctx context.Context) {
	if c == nil || !c.passDue.CompareAndSwap(true, false) {
		return
	}
	workclass.Do(ctx, workclass.Comparator, func(ctx context.Context) {
		if h := comparePassHookForTest.Load(); h != nil {
			(*h)(ctx)
		}
		c.ComparePass()
	})
}

// comparePassHookForTest, when set, runs inside the comparator unit right
// before the pass (the #871 label arm). Production never sets it.
var comparePassHookForTest atomic.Pointer[func(context.Context)]

// ComparePass runs one comparator pass now (exported for the falsifiers).
// It returns the number of divergences found (0 means none, or skipped).
func (c *ListCell) ComparePass() int {
	if c == nil || !c.anchored.Load() {
		return 0
	}
	// Passes are SERIAL (cmpMu held for the whole pass): the two-pass rule
	// confirms a mismatch only when two consecutive passes, each with its own
	// fresh view, saw it. Two concurrent passes sharing one stale view would
	// "confirm" each other.
	c.cmpMu.Lock()
	defer c.cmpMu.Unlock()
	t0 := time.Now()
	defer func() { ewmaNS(&c.passNS, time.Since(t0)) }()
	listCellStats.passes.Add(1)
	rw := Global()
	if rw == nil {
		return 0
	}
	// The generation and the pending keys are read in ONE critical section of
	// the cell's lock — a build holds it from its drain to its publish — so a
	// key a build took from pending is in g, never in neither. Then the
	// indexer is listed: anything it holds beyond g was recorded in pending
	// after this snapshot or is still on its way (in flight, checked below).
	c.mu.Lock()
	g := c.gen.Load()
	inPending := make(map[listObjKey]struct{}, len(c.pending))
	for k := range c.pending {
		inPending[k] = struct{}{}
	}
	c.mu.Unlock()
	if g == nil {
		return 0
	}
	keyed, ok := rw.listRawKeyedServable(c.gvr, c.namespace)
	if !ok {
		return 0 // not servable: Snapshot unanchors on the next read
	}
	keyed, ok = c.filter(keyed)
	if !ok {
		return 0
	}
	inFlight := func(k listObjKey, idxRaw []byte) bool {
		if _, p := inPending[k]; p {
			return true
		}
		return c.inFlight(rw, k, idxRaw)
	}

	diverged := 0
	reason := ""
	// (a) every slot's decoded object comes from its current raw.
	for _, ch := range g.body.chunks {
		for _, s := range ch.slots {
			if s.objSrc != idOf(s.raw) {
				diverged++
				reason = "obj_src"
			}
		}
	}
	// (b) key set and bytes against the fresh listing; two-pass for mismatches.
	next := map[listObjKey][2]rawID{}
	mismatch := func(k listObjKey, cellRaw, idxRaw []byte) {
		if inFlight(k, idxRaw) {
			return
		}
		ids := [2]rawID{idOf(cellRaw), idOf(idxRaw)}
		if prev, seen := c.suspects[k]; seen && prev == ids {
			diverged++
			if reason == "" {
				reason = "key_mismatch"
			}
			if h := listCellDivergenceHookForTest.Load(); h != nil {
				(*h)(fmt.Sprintf("key=%s/%s cell=%v idx=%v cellLen=%d idxLen=%d gen=%d", k.ns, k.name, ids[0].p != nil, ids[1].p != nil, len(cellRaw), len(idxRaw), g.body.seq))
			}
			return
		}
		next[k] = ids
		listCellStats.suspects.Add(1)
	}
	i, j := 0, 0
	var slots []listSlot
	for _, ch := range g.body.chunks {
		slots = append(slots, ch.slots...)
	}
	for i < len(slots) || j < len(keyed) {
		switch {
		case j == len(keyed) || (i < len(slots) && slots[i].key.less(keyed[j].key)):
			mismatch(slots[i].key, slots[i].raw, nil)
			i++
		case i == len(slots) || keyed[j].key.less(slots[i].key):
			mismatch(keyed[j].key, nil, keyed[j].raw)
			j++
		default:
			if !bytes.Equal(slots[i].raw, keyed[j].raw) {
				mismatch(slots[i].key, slots[i].raw, keyed[j].raw)
			}
			i++
			j++
		}
	}
	c.suspects = next
	// (c) one rotating chunk: re-decode and compare with the shared objects.
	if n := len(g.body.chunks); n > 0 {
		ch := g.body.chunks[c.rotate%n]
		c.rotate++
		for _, s := range ch.slots {
			fresh, err := c.funcs.Decode(s.raw)
			if err != nil || !reflect.DeepEqual(fresh.Object, s.obj.Object) {
				diverged++
				if reason == "" {
					reason = "mutated"
				}
			}
		}
	}

	if diverged > 0 {
		listCellStats.divergence(reason, uint64(diverged))
		sum := sha256.Sum256([]byte(c.key))
		slog.Error("cache.list_cell.divergence",
			slog.String("subsystem", "cache"),
			slog.String("gvr", c.gvr.String()),
			slog.Int("count", diverged),
			slog.String("reason", reason),
			slog.String("key_hash", hex.EncodeToString(sum[:8])),
			slog.String("effect", "the incremental list cell is dropped; the next read re-fills it in full (#854)"),
		)
		c.unanchor("divergence")
	}
	return diverged
}

func ewmaNS(a *atomic.Int64, d time.Duration) {
	const alpha = 0.2
	for {
		cur := a.Load()
		next := int64(d)
		if cur > 0 {
			next = int64(alpha*float64(d) + (1-alpha)*float64(cur))
		}
		if a.CompareAndSwap(cur, next) {
			return
		}
	}
}

// ── entry integration ───────────────────────────────────────────────────────

// NewListCellEntry is the store entry for an anchored cell: no RawJSON, no Items
// (both come from the cell's generations), ContentAsOf the anchor generation's.
func NewListCellEntry(c *ListCell, inputs *ResolvedKeyInputs) *ResolvedEntry {
	var asOf uint64
	if g := c.gen.Load(); g != nil {
		asOf = g.asOf
	}
	// uaf-scope-waiver: PRE-refilter substrate, the same apistage content cell apistage.go stores (identity-free by key, narrowed only downstream on the way to the requester); never holds refilter output.
	// scope-waiver:TTLOverride: apistage identity-free content substrate, stored only while its GVR is servable (an anchor requires it) — the CATALOG_UNSERVABLE TTLOverride never applies; NOT the UAF-cap class (uaf_shortttl.go R-d-4 SITE MAP).
	e := &ResolvedEntry{Inputs: inputs, listCell: c, ContentAsOf: asOf, ItemsAPIVersion: c.apiVersion, ItemsKind: c.kind}
	c.entry.Store(e)
	return e
}

// AttachStore lets the cell re-account its entry's bytes after a build.
func (c *ListCell) AttachStore(s *ResolvedCacheStore) {
	if c != nil {
		c.store = s
	}
}

// incremental reports whether e is an anchored incremental list cell: no TTL
// or max entry age applies to it (#882 F1, itemExpiredLocked). One atomic load.
func (e *ResolvedEntry) incremental() bool {
	return e != nil && e.listCell != nil && e.listCell.anchored.Load()
}

// Close unanchors the cell (a refused Put stored no entry for it).
func (c *ListCell) Close() { c.unanchor("put_refused") }

// Anchored reports whether the cell is still maintained incrementally.
func (c *ListCell) Anchored() bool { return c != nil && c.anchored.Load() }

// ListCell returns the incremental cell an entry carries (nil for a plain entry).
func (e *ResolvedEntry) ListCell() *ListCell {
	if e == nil {
		return nil
	}
	return e.listCell
}

// Raw returns the entry's body: RawJSON for a plain entry, the current
// generation's envelope (built on demand, counted) for an incremental one.
// Every reader that may see an apistage content entry goes through here
// (scripts/check_rawjson_readers.sh).
func (e *ResolvedEntry) Raw() []byte {
	if e == nil {
		return nil
	}
	if e.listCell == nil {
		return e.RawJSON
	}
	g := e.listCell.gen.Load()
	if g == nil {
		return nil
	}
	return ListView{cell: e.listCell, gen: g}.Raw()
}

// RawLen is len(Raw()) without building an incremental envelope.
func (e *ResolvedEntry) RawLen() int {
	if e == nil {
		return 0
	}
	if e.listCell == nil {
		return len(e.RawJSON)
	}
	g := e.listCell.gen.Load()
	if g == nil {
		return 0
	}
	return int(g.body.envLen(e.listCell.emptyEnv))
}

// itemCount is len(Items) for either shape (metadata surfaces).
func (e *ResolvedEntry) itemCount() int {
	if e.listCell != nil {
		if g := e.listCell.gen.Load(); g != nil {
			return g.body.n
		}
		return 0
	}
	return len(e.Items)
}

// ── stats ───────────────────────────────────────────────────────────────────

type listCellCounters struct {
	anchors, anchorSkipped, reads, restamps, generations, appliedKeys atomic.Uint64
	decodes, rebinds, itemsBuilt, rawBuilt, pendingRecorded           atomic.Uint64
	passes, suspects                                                  atomic.Uint64
	refreshSkipped, anchorSettling                                    atomic.Uint64
	genAllocBytes, genAllocMax                                        atomic.Int64
	genAllocCount                                                     atomic.Uint64
	mu                                                                sync.Mutex
	unanchors                                                         map[string]uint64
	divergences                                                       map[string]uint64
}

var listCellStats = &listCellCounters{unanchors: map[string]uint64{}, divergences: map[string]uint64{}}

func (s *listCellCounters) unanchor(reason string) {
	s.mu.Lock()
	s.unanchors[reason]++
	s.mu.Unlock()
}

func (s *listCellCounters) divergence(reason string, n uint64) {
	s.mu.Lock()
	s.divergences[reason] += n
	s.mu.Unlock()
}

func (s *listCellCounters) noteGenAlloc(b int64) {
	s.genAllocBytes.Add(b)
	s.genAllocCount.Add(1)
	for {
		cur := s.genAllocMax.Load()
		if b <= cur || s.genAllocMax.CompareAndSwap(cur, b) {
			return
		}
	}
}

// NoteListCellRefreshSkipped counts a refresher re-dispatch of a content key the
// incremental cell made unnecessary.
func NoteListCellRefreshSkipped() { listCellStats.refreshSkipped.Add(1) }

// ListCellStats is the snowplow_list_incremental readout.
type ListCellStats struct {
	Anchors, AnchorSkipped, Reads, Restamps, Generations, AppliedKeys uint64
	Decodes, Rebinds, ItemsBuilt, RawJSONBuilt, PendingRecorded       uint64
	Passes, Suspects, RefreshSkipped                                  uint64
	// AnchorSettling (#882 F2) — keys an anchor took from the informer listing
	// because an edit landed while the fill ran (pending or in flight).
	AnchorSettling                       uint64
	GenAllocBytesTotal, GenAllocBytesMax int64
	GenAllocCount                        uint64
	Unanchors, Divergences               map[string]uint64
	DivergenceTotal                      uint64
	Live                                 int
}

// ListCellStatsSnapshot reads every counter.
func ListCellStatsSnapshot() ListCellStats {
	s := listCellStats
	out := ListCellStats{
		Anchors: s.anchors.Load(), AnchorSkipped: s.anchorSkipped.Load(), Reads: s.reads.Load(),
		Restamps: s.restamps.Load(), Generations: s.generations.Load(), AppliedKeys: s.appliedKeys.Load(),
		Decodes: s.decodes.Load(), Rebinds: s.rebinds.Load(), ItemsBuilt: s.itemsBuilt.Load(),
		RawJSONBuilt: s.rawBuilt.Load(), PendingRecorded: s.pendingRecorded.Load(),
		Passes: s.passes.Load(), Suspects: s.suspects.Load(), RefreshSkipped: s.refreshSkipped.Load(),
		AnchorSettling:     s.anchorSettling.Load(),
		GenAllocBytesTotal: s.genAllocBytes.Load(), GenAllocBytesMax: s.genAllocMax.Load(),
		GenAllocCount: s.genAllocCount.Load(),
		Unanchors:     map[string]uint64{}, Divergences: map[string]uint64{},
	}
	s.mu.Lock()
	for k, v := range s.unanchors {
		out.Unanchors[k] = v
	}
	for k, v := range s.divergences {
		out.Divergences[k] = v
		out.DivergenceTotal += v
	}
	s.mu.Unlock()
	listCellByKey.Range(func(_, _ any) bool { out.Live++; return true })
	return out
}

// listCellBuildHookForTest runs inside build, after the drain and before the
// publish. TEST-ONLY (nil in production): F3a lands an event in that window.
var listCellBuildHookForTest atomic.Pointer[func()]

// SetListCellBuildHookForTest installs (nil clears) the build hook. TEST-ONLY.
func SetListCellBuildHookForTest(fn func()) (restore func()) {
	var p *func()
	if fn != nil {
		p = &fn
	}
	prev := listCellBuildHookForTest.Swap(p)
	return func() { listCellBuildHookForTest.Store(prev) }
}

// listCellDivergenceHookForTest receives a metadata-only description of each
// confirmed key mismatch. TEST-ONLY (nil in production).
var listCellDivergenceHookForTest atomic.Pointer[func(detail string)]

// SetListCellDivergenceHookForTest installs (nil clears) the hook. TEST-ONLY.
func SetListCellDivergenceHookForTest(fn func(detail string)) (restore func()) {
	var p *func(string)
	if fn != nil {
		p = &fn
	}
	prev := listCellDivergenceHookForTest.Swap(p)
	return func() { listCellDivergenceHookForTest.Store(prev) }
}

// listCellAnchorOffForTest keeps every fill a PLAIN fill. TEST-ONLY: the arms
// that pin the plain fill path (#710 / #765 A), which still serves every
// unanchored cell, set it; production never does.
var listCellAnchorOffForTest atomic.Bool

// SetListCellAnchoringForTest turns anchoring off (false) or back on, returning
// the restore. TEST-ONLY.
func SetListCellAnchoringForTest(enabled bool) (restore func()) {
	prev := listCellAnchorOffForTest.Swap(!enabled)
	return func() { listCellAnchorOffForTest.Store(prev) }
}

// ResetListCellsForTest drops every cell and zeroes the counters. TEST-ONLY.
func ResetListCellsForTest() {
	listCellByKey.Range(func(k, v any) bool {
		v.(*ListCell).unanchor("reset")
		return true
	})
	// Clear in place: informer handlers of a still-running watcher read these
	// maps concurrently (a reassignment would race them).
	listCellsByGVR.Range(func(k, _ any) bool { listCellsByGVR.Delete(k); return true })
	listCellByKey.Range(func(k, _ any) bool { listCellByKey.Delete(k); return true })
	s := listCellStats
	for _, a := range []*atomic.Uint64{&s.anchors, &s.anchorSkipped, &s.reads, &s.restamps, &s.generations,
		&s.appliedKeys, &s.decodes, &s.rebinds, &s.itemsBuilt, &s.rawBuilt, &s.pendingRecorded, &s.passes,
		&s.suspects, &s.refreshSkipped, &s.anchorSettling, &s.genAllocCount} {
		a.Store(0)
	}
	s.genAllocBytes.Store(0)
	s.genAllocMax.Store(0)
	s.mu.Lock()
	s.unanchors = map[string]uint64{}
	s.divergences = map[string]uint64{}
	s.mu.Unlock()
}

// CorruptSlotForTest replaces one slot's raw with a copy whose bytes differ, so
// the comparator's key/bytes check sees a divergence (F5a). TEST-ONLY.
func (c *ListCell) CorruptSlotForTest() bool {
	g := c.gen.Load()
	if g == nil || len(g.body.chunks) == 0 {
		return false
	}
	ch := g.body.chunks[0]
	if len(ch.slots) == 0 {
		return false
	}
	bad := append([]byte(nil), ch.slots[0].raw...)
	bad = append(bad[:len(bad)-1], ' ', '}')
	ch.slots[0].raw = bad
	ch.slots[0].objSrc = idOf(bad)
	return true
}

// CorruptObjSrcForTest replaces one slot's raw with an equal-bytes copy WITHOUT
// moving its objSrc, so only the comparator's obj-from-raw identity check can see
// it (F5c). TEST-ONLY.
func (c *ListCell) CorruptObjSrcForTest() bool {
	g := c.gen.Load()
	if g == nil || len(g.body.chunks) == 0 || len(g.body.chunks[0].slots) == 0 {
		return false
	}
	s := &g.body.chunks[0].slots[0]
	s.raw = append([]byte(nil), s.raw...)
	return true
}

// VerifyItemsForTest re-decodes every slot of this generation from its raw and
// counts the shared objects that no longer equal it (a consumer mutated one).
// TEST-ONLY (F6).
func (v ListView) VerifyItemsForTest() (bad int) {
	if v.gen == nil {
		return 0
	}
	for _, ch := range v.gen.body.chunks {
		for _, s := range ch.slots {
			fresh, err := v.cell.funcs.Decode(s.raw)
			if err != nil || !reflect.DeepEqual(fresh.Object, s.obj.Object) {
				bad++
			}
		}
	}
	return bad
}

// EntryBytesForTest is entryBytes (the store's size accounting). TEST-ONLY.
func EntryBytesForTest(e *ResolvedEntry) int64 { return entryBytes(e) }

// ChunkSizeForTest is the cell's chunk size. TEST-ONLY.
func (c *ListCell) ChunkSizeForTest() int { return c.chunkSize }

func (c *ListCell) String() string {
	return fmt.Sprintf("listCell{gvr=%s ns=%q anchored=%v}", c.gvr, c.namespace, c.anchored.Load())
}
