// store_instance.go — #870: one informer copy of a GVR, and the adapter every
// snowplow handler is delivered through.
//
// # WHY AN INSTANCE
//
// A store repair (or a schema widen) builds the GVR's replacement BESIDE the
// live copy and swaps it in (store_swap.go). For a while there are then two
// informers for one GVR, or more: the live copy, a muted candidate, and copies
// that were live and are being retired. Each is a storeInstance: its informer,
// its own stop channel, a done channel closed when its Run returns, and the
// adapters through which its listeners reach the consumers.
//
// # THE TWO DELIVERY RECORDS (design v5 §2.5)
//
//   - seen[k], per (instance, handler): updated by EVERY delivery through that
//     instance's listener, muted or not, the initial list included. It decides
//     suppression (§2.7) and it is the #854 comparator's "delivered" record
//     (F5): the live instance's dep-handler seen replaces the old
//     listCellSet.delivered map and its anchor seeding.
//   - acted[k], per (GVR, handler) and SURVIVING swaps: updated only by the
//     deliveries a consumer acted on — unmuted live deliveries and swap
//     deliveries. The swap's M is defined from it, and it is the `old` side of
//     the consumer's OnUpdate / OnDelete. Kept only where a build can happen
//     (the recorded ownsInformer property, the same class rule that gates a
//     build): its only reader is the build path.
//
// Each entry is a reference to the store object (or absent). Identity, bytes
// and resourceVersion are read from it; nothing is copied.
//
// # LOCKS
//
// The adapter's mutex and the record's mutex are LEAVES: nothing is called
// while holding either except map operations and storeItemJSON. The consumer
// handler is always called with no adapter or record lock held.
package cache

import (
	"bytes"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// instanceShape is the construction branch that built an informer. A build
// constructs its copy through the SAME branch as the live copy (F9), so it is
// recorded at registration — it is not re-derivable from the handle.
type instanceShape int

const (
	shapeFactoryDynamic     instanceShape = iota // shared dynamic factory: not owned
	shapeStreaming                               // the H5 streaming constructor: owned
	shapeStandaloneDynamic                       // NewFilteredDynamicInformer: owned
	shapeFactoryMetadata                         // shared metadata factory: not owned
	shapeStandaloneMetadata                      // NewFilteredMetadataInformer: owned
)

func (s instanceShape) owned() bool {
	return s == shapeStreaming || s == shapeStandaloneDynamic || s == shapeStandaloneMetadata
}

func (s instanceShape) metadataOnly() bool {
	return s == shapeFactoryMetadata || s == shapeStandaloneMetadata
}

// storeInstance is one informer copy of a GVR.
type storeInstance struct {
	gvr   schema.GroupVersionResource
	gi    informers.GenericInformer
	shape instanceShape

	stop     chan struct{}
	stopOnce sync.Once
	// done is closed when THIS instance's Run returns (ownRun). An informer
	// run by the shared factory's Start is never ours to wait on; it is never
	// owned, so it is never retired.
	done   chan struct{}
	ownRun bool

	syncCh chan struct{}
	dl     *deliveryLog

	// startMuted: the instance was constructed as a build candidate, so each
	// adapter attached to it starts MUTED (records `seen`, does nothing else).
	// An adapter is unmuted only by the swapUnmute notification, in its own
	// listener's order (#928 F2) — never by flipping a flag the listener reads
	// out of order. Set before any adapter attaches.
	startMuted atomic.Bool

	// delivered is closed when this instance's swap has delivered M (step 6
	// done): the swap's completion event.
	delivered chan struct{}
	// syncDone is closed when a candidate's sync goroutine has returned: it
	// installed the copy, or gave up (stopped first, or never synced).
	syncDone chan struct{}

	// reuse is the LIVE copy's indexer while this instance is a candidate: the
	// transform hands back the live object when the bytes are equal, so a new
	// copy of an unchanged store costs only what changed. The install sets it
	// to nil. Read lock-free from the transform, which runs under the FIFO
	// lock and must never take rw.mu.
	reuse atomic.Pointer[clientcache.Indexer]

	mu       sync.Mutex
	adapters []*handlerAdapter
	depAdpt  *handlerAdapter // the dep handler's adapter: the comparator record

	// Candidate-only watch state, guarded by rw.mu: the latest reflector error
	// the instance-aware watch-error handler saw while this instance was not
	// the live one, and the instance's position (LastSyncResourceVersion) at
	// that error. At the install it is judged NOW by the live copy's own
	// recovery rule (watchHealed): a position that advanced past brokenAt on a
	// synced informer means the reflector has listed successfully since the
	// error, which is healed (#928 F1).
	broken, refused bool
	brokenAt        string

	// account is the known-stale accounting of the build that installed this
	// instance, until its swap goroutine claims it at step 3 (or a later
	// install absorbs it). Guarded by rw.mu.
	account   *buildAccount
	passedSt3 bool // guarded by rw.mu (design Q6)
}

func newStoreInstance(gvr schema.GroupVersionResource, gi informers.GenericInformer, shape instanceShape, stop chan struct{}) *storeInstance {
	return &storeInstance{
		gvr:       gvr,
		gi:        gi,
		shape:     shape,
		stop:      stop,
		done:      make(chan struct{}),
		syncCh:    make(chan struct{}),
		delivered: make(chan struct{}),
		syncDone:  make(chan struct{}),
	}
}

// watchStateNow is the candidate's watch state judged at the install by the
// live copy's conjunct-3 recovery rule (watchHealed): an error the reflector
// has since recovered from (its position advanced past the one at the error
// and it is synced) is not copied. Callers hold rw.mu.
func (in *storeInstance) watchStateNow() (broken, refused bool) {
	if !in.broken {
		return false, false
	}
	inf := in.gi.Informer()
	if watchHealed(inf.LastSyncResourceVersion(), in.brokenAt, inf.HasSynced()) {
		return false, false
	}
	return true, in.refused
}

// closeStop closes the instance's stop channel exactly once, from any caller
// (design v5.1 C3: every waiter closes every retiring instance's stop).
func (in *storeInstance) closeStop() {
	in.stopOnce.Do(func() { close(in.stop) })
}

func (in *storeInstance) stopped() bool {
	select {
	case <-in.stop:
		return true
	default:
		return false
	}
}

// runInformer runs the informer until its stop channel closes, then frees
// the adapters' per-instance state and closes done. Called on the instance's
// own goroutine.
func (in *storeInstance) runInformer() {
	in.gi.Informer().Run(in.stop)
	in.freeAdapters()
	close(in.done)
}

// freeAdapters drops every per-instance record of a stopped instance: the
// suppression state dies with the instance (X34) and so does seen.
func (in *storeInstance) freeAdapters() {
	in.mu.Lock()
	adapters := append([]*handlerAdapter(nil), in.adapters...)
	in.mu.Unlock()
	for _, a := range adapters {
		a.free()
	}
}

// reuseTransform wraps next: once next has produced the object the store will
// hold, an object whose bytes equal the live copy's object for the same key is
// replaced by the live copy's object, so the new copy shares it (design v5
// §2.3, K6). Bytes are read through storeItemJSON, which covers every store
// item shape (X10). No lock is taken but the live indexer's read lock.
func (in *storeInstance) reuseTransform(next clientcache.TransformFunc) clientcache.TransformFunc {
	return func(obj any) (any, error) {
		out := obj
		if next != nil {
			var err error
			if out, err = next(obj); err != nil {
				return out, err
			}
		}
		p := in.reuse.Load()
		if p == nil {
			return out, nil
		}
		key, err := clientcache.MetaNamespaceKeyFunc(out)
		if err != nil {
			return out, nil
		}
		live, exists, err := (*p).GetByKey(key)
		if err != nil || !exists || live == out {
			return out, nil
		}
		if sameBytes(live, out) {
			storeSwapStats.reused.Add(1)
			return live, nil
		}
		return out, nil
	}
}

// adapt wraps a consumer handler for this instance. acted is the (GVR,
// handler) record, nil where no build can happen.
func (in *storeInstance) adapt(name string, inner clientcache.ResourceEventHandler, acted *actedRecord) *handlerAdapter {
	a := &handlerAdapter{
		inst:     in,
		name:     name,
		inner:    inner,
		acted:    acted,
		muted:    in.startMuted.Load(),
		seen:     map[string]any{},
		suppress: map[string]suppressTarget{},
	}
	in.mu.Lock()
	in.adapters = append(in.adapters, a)
	if name == depHandlerName {
		in.depAdpt = a
	}
	in.mu.Unlock()
	return a
}

func (in *storeInstance) adaptersSnapshot() []*handlerAdapter {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]*handlerAdapter(nil), in.adapters...)
}

// adaptingAttacher wraps every handler an extension attaches.
type adaptingAttacher struct {
	in     *storeInstance
	inf    clientcache.SharedIndexInformer
	record func(name string) *actedRecord
	next   int
}

func (a *adaptingAttacher) AddEventHandler(h clientcache.ResourceEventHandler) (clientcache.ResourceEventHandlerRegistration, error) {
	a.next++
	name := extensionHandlerName(a.next)
	return a.inf.AddEventHandler(a.in.adapt(name, h, a.record(name)))
}

// depHandlerName is the dep handler's record name. Extension handlers are
// named by their attach order, which is the registry's order and identical
// for every instance of a GVR.
const depHandlerName = "deps"

func extensionHandlerName(i int) string { return "ext/" + strconv.Itoa(i) }

// ── the records ───────────────────────────────────────────────────────────────

// actedRecord is acted[k] for one (GVR, handler). It survives swaps; it is
// purged with the GVR.
type actedRecord struct {
	mu sync.Mutex
	m  map[string]any
}

func newActedRecord() *actedRecord { return &actedRecord{m: map[string]any{}} }

func (r *actedRecord) get(k string) (any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v, ok := r.m[k]
	return v, ok
}

func (r *actedRecord) set(k string, obj any, present bool) {
	r.mu.Lock()
	if present {
		r.m[k] = obj
	} else {
		delete(r.m, k)
	}
	r.mu.Unlock()
}

// snapshot copies the record (M's computation, step 5).
func (r *actedRecord) snapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]any, len(r.m))
	for k, v := range r.m {
		out[k] = v
	}
	return out
}

// objState is presence plus resourceVersion: what C1 compares (string
// equality, never ordering — safe for every API, aggregated ones included) and
// what the F8 regression count matches.
type objState struct {
	present bool
	rv      string
}

func stateOf(obj any, present bool) objState {
	if !present || obj == nil {
		return objState{}
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return objState{present: true}
	}
	return objState{present: true, rv: acc.GetResourceVersion()}
}

// sameBytes compares two store objects by their item bytes, with pointer
// identity as the fast path.
func sameBytes(a, b any) bool {
	if a == b {
		return true
	}
	ab, ok1 := itemBytes(a)
	bb, ok2 := itemBytes(b)
	if !ok1 || !ok2 {
		return false
	}
	return bytes.Equal(ab, bb)
}

// itemBytes is a store object's bytes for COMPARISON: storeItemJSON, which
// covers every item shape a read can serve (bytes-backed, Unstructured,
// typed), and the object's own JSON for a shape no read renders as a whole
// item (a metadata-only PartialObjectMetadata). One rule for every store
// shape, so reuse and the swap's diff work the same on all of them (X10).
func itemBytes(obj any) ([]byte, bool) {
	if b, ok := storeItemJSON(obj); ok {
		return b, true
	}
	if obj == nil {
		return nil, false
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, false
	}
	return b, true
}

// sameDelivered is C1's test (design v5.1): the consumer has already acted on
// S iff presence matches and, when both are present, the resourceVersions are
// equal AND the bytes are equal (a same-version schema change differs only in
// bytes).
func sameDelivered(acted any, hadActed bool, s any, present bool) bool {
	if hadActed != present {
		return false
	}
	if !present {
		return true
	}
	if stateOf(acted, true) != stateOf(s, true) {
		return false
	}
	return sameBytes(acted, s)
}

// ── the adapter ───────────────────────────────────────────────────────────────

// suppressTarget is the notification that ends a key's suppression: S's OWN
// notification, matched by OBJECT IDENTITY (architect, #870 on client-go
// v0.36.5: snowplow's handlers are registered with resync period 0, so every
// store write — a same-resourceVersion one included — queues exactly one
// notification per listener, and the one carrying S is in the queue).
// Matching by version instead would end suppression on an OLDER same-version
// write and let it reach the consumer after S (X50, K11c-6). An absent S is
// matched by the first delete (P2, the stated limit).
type suppressTarget struct {
	present bool
	obj     any
}

func (t suppressTarget) matches(obj any, present bool) bool {
	if !t.present {
		return !present
	}
	return present && obj == t.obj
}

// swapDelivery is the notification a swap enqueues for key k through the new
// informer's own OnUpdate, so it reaches every listener IN ORDER with that
// listener's real notifications. It has no meta accessor, so client-go never
// treats it as a resync.
type swapDelivery struct{ key string }

// swapUnmute is the notification the install enqueues, through the same
// OnUpdate, to unmute the new copy IN EACH LISTENER'S ORDER (#928 F2): every
// notification the listener received before it is muted (seen only), every
// one after it is live. passed is called once by each adapter as it processes
// it; the install computes M only after every adapter has passed it, so a
// notification still queued at the unmute can never reach a consumer that
// already acted on a newer version.
type swapUnmute struct{ passed func() }

// handlerAdapter wraps one consumer handler on one instance.
type handlerAdapter struct {
	inst  *storeInstance
	name  string
	inner clientcache.ResourceEventHandler
	acted *actedRecord // nil where no build can happen

	mu    sync.Mutex
	freed bool
	// muted: this adapter records `seen` and nothing else. Starts as the
	// instance's startMuted; cleared only by the swapUnmute notification.
	muted    bool
	seen     map[string]any
	suppress map[string]suppressTarget
	// displaced[k] is the resourceVersion a swap delivery replaced for k. It
	// is recorded only when the swap replaced a DIFFERENT version (a version
	// can come back only then: the new copy synced behind and its watch is
	// still to deliver it), and it ends at k's next real delivery or with the
	// instance. A real delivery carrying exactly that version means the swap
	// pushed an OLDER body than the consumer had (F8):
	// store_swap_regressed_keys. A schema widen replaces every key at the
	// same version, so it records nothing (#928 F5).
	displaced map[string]string
}

var _ clientcache.ResourceEventHandler = (*handlerAdapter)(nil)

func (a *handlerAdapter) OnAdd(obj any, isInInitialList bool) {
	if sd, ok := obj.(swapDelivery); ok {
		a.swapDeliver(sd.key)
		return
	}
	a.deliver(obj, nil, true, isInInitialList, false)
}

func (a *handlerAdapter) OnUpdate(oldObj, newObj any) {
	switch n := newObj.(type) {
	case swapDelivery:
		a.swapDeliver(n.key)
		return
	case swapUnmute:
		a.mu.Lock()
		a.muted = false
		a.mu.Unlock()
		n.passed()
		return
	}
	a.deliver(newObj, oldObj, true, false, true)
}

func (a *handlerAdapter) OnDelete(obj any) {
	a.deliver(obj, nil, false, false, false)
}

// deliver handles one real notification (design v5 §2.7 step 3, P2).
func (a *handlerAdapter) deliver(obj, old any, present, isInInitialList, update bool) {
	key, err := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		a.callInner(obj, old, present, isInInitialList, update)
		return
	}
	st := stateOf(obj, present)
	if _, tomb := obj.(clientcache.DeletedFinalStateUnknown); tomb {
		st = objState{}
	}

	a.mu.Lock()
	if a.freed {
		a.mu.Unlock()
		return
	}
	if t, suppressing := a.suppress[key]; suppressing {
		// Drop every notification for k until S's own arrives; swallow that
		// one too (seen only).
		a.recordSeenLocked(key, obj, st.present)
		if t.matches(obj, st.present) {
			delete(a.suppress, key)
		}
		a.mu.Unlock()
		return
	}
	if a.muted {
		a.recordSeenLocked(key, obj, st.present)
		a.mu.Unlock()
		return
	}
	if rv, ok := a.displaced[key]; ok {
		delete(a.displaced, key)
		if st.present && rv != "" && st.rv == rv && a.name == depHandlerName {
			storeSwapStats.regressedKeys.Add(1)
		}
	}
	a.mu.Unlock()

	if a.acted != nil {
		if prev, had := a.acted.get(key); had && update {
			old = prev
		}
	}
	a.callInner(obj, old, present, isInInitialList, update)
	if a.acted != nil {
		a.acted.set(key, obj, st.present)
	}
	// seen AFTER the consumer: the comparator must see a key either not yet
	// delivered or already submitted by the dep handler (#854 ordering).
	a.mu.Lock()
	if !a.freed {
		a.recordSeenLocked(key, obj, st.present)
	}
	a.mu.Unlock()
}

func (a *handlerAdapter) callInner(obj, old any, present, isInInitialList, update bool) {
	switch {
	case !present:
		a.inner.OnDelete(obj)
	case update:
		a.inner.OnUpdate(old, obj)
	default:
		a.inner.OnAdd(obj, isInInitialList)
	}
}

func (a *handlerAdapter) recordSeenLocked(key string, obj any, present bool) {
	if present {
		a.seen[key] = obj
	} else {
		delete(a.seen, key)
	}
}

// swapDeliver handles a swapDelivery{k} (design v5 §2.7 as amended by v5.1 C1).
func (a *handlerAdapter) swapDeliver(key string) {
	s, present := a.inst.storeGet(key)
	var acted any
	hadActed := false
	if a.acted != nil {
		acted, hadActed = a.acted.get(key)
	}
	if sameDelivered(acted, hadActed, s, present) {
		return // C1: the consumer already acted on S
	}
	if a.name == depHandlerName {
		storeSwapStats.deliveries.Add(1)
	}
	// A consumer that attributes its work by source is told this is a swap
	// delivery (the dep handler: submission source store_swap).
	sh, sourced := a.inner.(swapSourcedHandler)
	switch {
	case present && !hadActed && sourced:
		sh.onSwapAdd(s)
	case present && !hadActed:
		a.inner.OnAdd(s, false)
	case present && sourced:
		sh.onSwapUpdate(acted, s)
	case present:
		a.inner.OnUpdate(acted, s)
	case sourced:
		sh.onSwapDelete(acted)
	default:
		a.inner.OnDelete(acted)
	}
	if a.acted != nil {
		a.acted.set(key, s, present)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.freed {
		return
	}
	if hadActed && present {
		if prev := stateOf(acted, true).rv; prev != stateOf(s, true).rv {
			if a.displaced == nil {
				a.displaced = map[string]string{}
			}
			a.displaced[key] = prev
		}
	}
	seenObj, seenOK := a.seen[key]
	if seenOK != present || (present && seenObj != s) {
		// The listener has not delivered S itself yet: S's own notification
		// is still queued behind this one.
		a.suppress[key] = suppressTarget{present: present, obj: s}
	}
}

// seenObject is the comparator's read of the record.
func (a *handlerAdapter) seenObject(key string) (any, bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.freed {
		return nil, false, false
	}
	v, ok := a.seen[key]
	return v, ok, true
}

func (a *handlerAdapter) seenSnapshot() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]any, len(a.seen))
	for k, v := range a.seen {
		out[k] = v
	}
	return out
}

func (a *handlerAdapter) free() {
	a.mu.Lock()
	a.freed = true
	a.seen = nil
	a.suppress = nil
	a.displaced = nil
	a.mu.Unlock()
}

// suppressingForTest reports how many keys the adapter is suppressing.
func (a *handlerAdapter) suppressingForTest() (int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.suppress), a.freed
}

func (in *storeInstance) storeGet(key string) (any, bool) {
	obj, exists, err := in.gi.Informer().GetIndexer().GetByKey(key)
	if err != nil || !exists {
		return nil, false
	}
	return obj, true
}

// ── the comparator record ────────────────────────────────────────────────────

// comparatorRecord: GVR → the dep-handler adapter of the instance whose seen
// is the #854 comparator's delivered record (F5). Set at registration and at a
// swap's unmute; dropped with the GVR. A sync.Map: read by the comparator off
// rw.mu.
var comparatorRecord sync.Map

func setComparatorRecord(gvr schema.GroupVersionResource, in *storeInstance) {
	if in == nil {
		return
	}
	in.mu.Lock()
	a := in.depAdpt
	in.mu.Unlock()
	if a != nil {
		comparatorRecord.Store(gvr, a)
	}
}

func dropComparatorRecord(gvr schema.GroupVersionResource, in *storeInstance) {
	if in == nil {
		return
	}
	in.mu.Lock()
	a := in.depAdpt
	in.mu.Unlock()
	if a != nil {
		comparatorRecord.CompareAndDelete(gvr, a)
	}
}

// deliveredRaw returns the raw identity of the object last delivered for k to
// the comparator instance's dep handler. known=false when there is no record
// (the comparator then treats the key as in flight).
func deliveredRaw(gvr schema.GroupVersionResource, k listObjKey) (id rawID, delivered, known bool) {
	v, ok := comparatorRecord.Load(gvr)
	if !ok {
		return rawID{}, false, false
	}
	key := k.name
	if k.ns != "" {
		key = k.ns + "/" + k.name
	}
	obj, seen, live := v.(*handlerAdapter).seenObject(key)
	if !live {
		return rawID{}, false, false
	}
	if !seen {
		return rawID{}, false, true
	}
	if b, ok := storeItemJSON(obj); ok {
		return idOf(b), true, true
	}
	return rawID{}, true, true
}

// swapSourcedHandler is implemented by a consumer that attributes its work by
// source: a swap delivery reaches it through these instead of the plain
// handler methods, with the same semantics.
type swapSourcedHandler interface {
	onSwapAdd(obj any)
	onSwapUpdate(oldObj, newObj any)
	onSwapDelete(obj any)
}
