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
//   - the GVR's servability epoch moved (any exit from servable, any informer
//     (re)creation — so an informer rebuild whose initial-list ADDs the dep hook
//     drops, #688, is never missed), or the GVR is not servable now;
//   - any lost-event signal moved: a dep-worker panic, an ADD seen with a nil
//     syncCh, a submit dropped onto a shut-down dep queue;
//   - a type-level event on the GVR (CRD available / removed / schema relisted /
//     store repaired);
//   - the shadow comparator found a divergence;
//   - the store dropped or replaced the entry (TTL / LRU / max-age / DELETE /
//     a later Put).
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
//
// NOT IN PASSTHROUGH. With CACHE_ENABLED=false there is no informer and no event:
// a cell is never anchored there.
package cache

import (
	"bytes"
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
	clientcache "k8s.io/client-go/tools/cache"
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

// ListCellAnchor is what a fill leader hands AnchorListCell: the envelope it just
// dispatched and the items it parsed from it.
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
	reads       atomic.Uint64
	lastPassAt  atomic.Uint64 // reads at the last pass
	passRunning atomic.Bool
	passNS      atomic.Int64 // EWMA of one pass, ns
	readNS      atomic.Int64 // EWMA of one Snapshot, ns
	cmpMu       sync.Mutex
	suspects    map[listObjKey][2]rawID
	rotate      int

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
	mu    sync.Mutex
	cells map[string]*ListCell // content key → cell
	// delivered — the raw identity of the last informer delivery per key (the
	// zero rawID for a delete), recorded while the GVR has a cell. The
	// comparator's in-flight test: an indexer object whose identity is not the
	// delivered one has not reached the dep handler yet (it sits in the
	// client-go listener buffer); a key never delivered since recording began
	// still holds its anchor-time object, which the cell matched at the anchor.
	delivered map[listObjKey]rawID
}

var (
	listCellsByGVR sync.Map // schema.GroupVersionResource → *listCellSet
	listCellByKey  sync.Map // content key → *ListCell
)

func listCellSetFor(gvr schema.GroupVersionResource) *listCellSet {
	v, _ := listCellsByGVR.LoadOrStore(gvr, &listCellSet{cells: map[string]*ListCell{}})
	return v.(*listCellSet)
}

func registerListCell(c *ListCell) {
	if prev, ok := listCellByKey.Swap(c.key, c); ok {
		if p := prev.(*ListCell); p != c {
			p.unanchor("replaced")
		}
	}
	s := listCellSetFor(c.gvr)
	s.mu.Lock()
	s.cells[c.key] = c
	s.mu.Unlock()
}

func deregisterListCell(c *ListCell) {
	listCellByKey.CompareAndDelete(c.key, c)
	s := listCellSetFor(c.gvr)
	s.mu.Lock()
	if s.cells[c.key] == c {
		delete(s.cells, c.key)
	}
	if len(s.cells) == 0 {
		s.delivered = nil // no cell left to compare: stop recording
	}
	s.mu.Unlock()
}

// noteListCellDelivery records an informer delivery for the GVR's cells (see
// listCellSet.delivered). One sync.Map load when the GVR has no cell.
func noteListCellDelivery(gvr schema.GroupVersionResource, obj interface{}, deleted bool) {
	v, ok := listCellsByGVR.Load(gvr)
	if !ok {
		return
	}
	s := v.(*listCellSet)
	var key listObjKey
	var id rawID
	if t, isTomb := obj.(clientcache.DeletedFinalStateUnknown); isTomb {
		ns, name, err := clientcache.SplitMetaNamespaceKey(t.Key)
		if err != nil {
			return
		}
		key, deleted = listObjKey{ns: ns, name: name}, true
	} else {
		ns, name, ok := indexerSlotKey(obj)
		if !ok {
			return
		}
		key = listObjKey{ns: ns, name: name}
		if !deleted {
			if b, ok := storeItemJSON(obj); ok {
				id = idOf(b)
			}
		}
	}
	s.mu.Lock()
	if len(s.cells) > 0 {
		if s.delivered == nil {
			s.delivered = map[listObjKey]rawID{}
		}
		s.delivered[key] = id
	}
	s.mu.Unlock()
}

// seedDelivered records the anchor listing as delivered for every key with no
// delivery recorded yet: the pairing proved the cell equals those objects, and
// a notification still buffered for one of them carries the same identity. A
// key already recorded keeps its (newer or equal) delivery.
func seedDelivered(gvr schema.GroupVersionResource, keyed []keyedRaw) {
	s := listCellSetFor(gvr)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil {
		s.delivered = make(map[listObjKey]rawID, len(keyed))
	}
	for _, kr := range keyed {
		if _, seen := s.delivered[kr.key]; !seen {
			s.delivered[kr.key] = idOf(kr.raw)
		}
	}
}

// deliveredMismatch reports whether the indexer's current object for k (idx, the
// zero rawID when absent) is not the last one delivered to the dep handler.
func deliveredMismatch(gvr schema.GroupVersionResource, k listObjKey, idx rawID) bool {
	v, ok := listCellsByGVR.Load(gvr)
	if !ok {
		return false
	}
	s := v.(*listCellSet)
	s.mu.Lock()
	d, seen := s.delivered[k]
	s.mu.Unlock()
	if !seen {
		// Neither seeded by an anchor nor delivered since: an object created
		// after the anchor whose ADD has not reached the handler — in flight.
		return true
	}
	return d != idx
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
	s := v.(*listCellSet)
	s.mu.Lock()
	cells := make([]*ListCell, 0, len(s.cells))
	for _, c := range s.cells {
		cells = append(cells, c)
	}
	s.mu.Unlock()
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
	s := v.(*listCellSet)
	s.mu.Lock()
	cells := make([]*ListCell, 0, len(s.cells))
	for _, c := range s.cells {
		cells = append(cells, c)
	}
	s.mu.Unlock()
	for _, c := range cells {
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

// AnchorListCell builds an incremental cell from a fill leader's just-dispatched
// envelope and its parsed items, or returns nil (the entry stays a plain fill).
//
// The cell is registered FIRST, so every event from here on is recorded in
// pending; then the dep-event seq is read, then the partition is listed again
// (decode-free) and paired item by item with the envelope's own bytes. The
// pairing proves the items were decoded from exactly the bytes the indexer holds
// at that listing, so the body is complete as of the seq read before it (the R1
// argument above); any event that raced the anchor shows as a byte mismatch and
// the anchor is abandoned (the next fill retries).
func AnchorListCell(a ListCellAnchor) *ListCell {
	if listCellAnchorOffForTest.Load() {
		return nil
	}
	rw := Global()
	if rw == nil || rw.mode == modePassthrough || Disabled() || a.Key == "" ||
		a.Funcs.Decode == nil || a.Funcs.Match == nil || a.Funcs.Marshal == nil {
		listCellStats.anchorSkipped.Add(1)
		return nil
	}
	shape, ok := deriveEnvelopeShape(a.Funcs.Marshal)
	if !ok {
		listCellStats.anchorSkipped.Add(1)
		return nil
	}
	c := &ListCell{
		key:        a.Key,
		gvr:        a.GVR,
		namespace:  a.Namespace,
		apiVersion: a.APIVersion,
		kind:       a.Kind,
		funcs:      a.Funcs,
		emptyEnv:   int64(len(shape.head) + len(shape.tail)),
		pending:    map[listObjKey]struct{}{},
		suspects:   map[listObjKey][2]rawID{},
	}
	// Trust state BEFORE the listing: an exit from servable or a lost event
	// during the anchor makes the first read unanchor.
	c.servEpoch = rw.ServabilityEpoch(a.GVR)
	c.lostSig = depWatchLostEventSignal()
	c.anchored.Store(true)
	registerListCell(c)

	s := depEventSeq.Load()
	keyed, ok := rw.listRawKeyedServable(a.GVR, a.Namespace)
	if !ok {
		c.unanchor("anchor_not_servable")
		return nil
	}
	keyed, ok = c.filter(keyed)
	if !ok || len(keyed) != len(a.Items) || !shape.holds(a.Envelope, keyed) {
		c.unanchor("anchor_raced")
		return nil
	}
	n := len(keyed)
	c.chunkSize = chunkSizeFor(n)
	body := &listBody{n: n, seq: listBodySeq.Add(1)}
	for i := 0; i < n; i += c.chunkSize {
		j := min(i+c.chunkSize, n)
		ch := &listChunk{slots: make([]listSlot, 0, j-i)}
		for k := i; k < j; k++ {
			kr := keyed[k]
			ch.slots = append(ch.slots, listSlot{key: kr.key, raw: kr.raw, obj: a.Items[k], objSrc: idOf(kr.raw)})
			body.sumRaw += int64(len(kr.raw))
		}
		body.chunks = append(body.chunks, ch)
	}
	c.gen.Store(&listGen{asOf: s, body: body})
	seedDelivered(a.GVR, keyed)
	listCellStats.anchors.Add(1)
	return c
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

// maybePass starts one asynchronous comparator pass when this cell's reads since
// the last pass reach ⌈passCost / readCost⌉ (at least 1). The first pass runs on
// the first read after the anchor's cost is unknown (passNS 0 ⇒ due).
func (c *ListCell) maybePass() {
	r := c.reads.Add(1)
	last := c.lastPassAt.Load()
	every := uint64(1)
	if p, rd := c.passNS.Load(), c.readNS.Load(); p > 0 && rd > 0 {
		every = uint64(math.Ceil(float64(p) / float64(rd)))
	}
	if r-last < every || !c.passRunning.CompareAndSwap(false, true) {
		return
	}
	c.lastPassAt.Store(r)
	go func() {
		defer c.passRunning.Store(false)
		c.ComparePass()
	}()
}

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
		if depWatchInFlight(depEventKey{gvr: c.gvr, namespace: k.ns, name: k.name}) {
			return true
		}
		return deliveredMismatch(c.gvr, k, idOf(idxRaw))
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
	refreshSkipped                                                    atomic.Uint64
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
	GenAllocBytesTotal, GenAllocBytesMax                              int64
	GenAllocCount                                                     uint64
	Unanchors, Divergences                                            map[string]uint64
	DivergenceTotal                                                   uint64
	Live                                                              int
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
		&s.suspects, &s.refreshSkipped, &s.genAllocCount} {
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
