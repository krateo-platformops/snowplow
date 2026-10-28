// store_verify_delivery.go — #875: which keys the reflector delivered while a
// forced pass was open. Only those can be "store ahead of the LIST", so only
// those are re-read (store_verify_recheck.go); every other candidate is counted
// as it stands, with no GET.
//
// # WHY A SIGNAL AND NOT A CAP
//
// The re-check exists to explain one thing: the store moved past the LIST
// while the LIST was in flight. The store moves only when the reflector
// delivers to it. So a key nothing was delivered for since the pass opened
// cannot be ahead of a LIST that is no older than the store was at that
// moment; if it disagrees with the LIST, the store is behind, and that is
// what the pass exists to find. Re-reading it would be a GET spent proving
// what the comparison already proved. Re-reading only delivered keys bounds
// the GETs by the churn during the pass, by construction: a wholly divergent
// store (#237's broken-store class) with no churn issues zero GETs and goes
// straight to its one repair, where a per-candidate re-check issued two
// quorum GETs per stored object before it (#875 review: 400 GETs for 200
// objects). No count limit is involved, so none can be mistuned.
//
// # WHAT IS RECORDED, AND WHERE
//
// The informer's transform. Every object the reflector delivers passes through
// it exactly once, synchronously, on the reflector goroutine, BEFORE the
// object is queued for the store and BEFORE the reflector publishes the
// delivery's position (LastSyncResourceVersion): watch ADD, UPDATE and DELETE
// (the deleted object itself is transformed), and every item of a relist,
// classic or streamed (client-go@v0.36.5 reflector.go handleAnyWatch, list,
// watchList; the_real_fifo.go:255 addToItems_locked for watch events and
// :295 addReplaceToItemsLocked, which transforms a whole relist at once under
// AtomicFIFO). A watch-list relist is transformed twice: once into the
// reflector's temporary store and again by the Replace (reflector.go:833-835,
// :901); recording a key twice is idempotent. An event handler would be the
// wrong place: handlers run on the processor's listener goroutines, after the
// store write, so a delivery could already show in the store and not yet in
// the record — the race this file exists to close.
//
// While a pass is open the transform records the delivered key; always, it
// keeps the latest object it saw.
//
// # THE TRANSFORM TAKES NO LOCK BUT ITS OWN
//
// It runs under the FIFO's lock: AtomicFIFO's UnlockWhileProcessingFIFO
// releases that lock only around PROCESSING a popped delta
// (the_real_fifo.go:454 whileProcessing_locked), never around the enqueue the
// transform belongs to. At client-go v0.35.3 the informer's accessors took
// startedLock and HasSynced then took the FIFO's lock (HasSynced ->
// controller.HasSynced -> RealFIFO.HasSynced), so reading the informer's
// position from inside the transform was an AB/BA deadlock with any
// concurrent HasSynced — one this file's first draft had, and the kind churn
// falsifier hung on. v0.36.5 made HasSynced a channel read
// (shared_informer.go:791), which removes that particular cycle, but
// LastSyncResourceVersion still takes startedLock and the reflector's mutex
// (shared_informer.go:819, controller.go:223) and LastStoreSyncResourceVersion
// takes the store's lock (thread_safe_store.go:436), and a transform-time read
// of either is not the position of the delivery being transformed anyway. So
// the rule stands: the transform touches only deliveryLog.mu, under which
// nothing but map writes happen, and nothing ever waits for another lock while
// holding deliveryLog.mu except the indexer's read lock, whose holders never
// wait for this one.
//
// # OPENING A PASS
//
//  1. Under this log's mutex: start recording, take the store's key set
//     (presence at open, for relist deletions below), and note the latest
//     object the transform saw.
//  2. With no lock held, read the published position. That is the
//     resourceVersion the pass then LISTs no older than.
//  3. Record the latest object's key, unless the position just read is that
//     object's own resourceVersion (then its delivery is published and the
//     LIST includes it).
//
// # WHY A DELIVERED KEY CANNOT BE MISCLASSIFIED
//
// Let d be a watch delivery whose effect is in the store but not in the LIST.
// The LIST is no older than the position read in step 2, so d's position was
// not yet published at step 2, which is after step 1. Either d's transform ran
// after step 1, saw a pass open and recorded its key; or it ran before step 1,
// in which case d was IN FLIGHT at step 1 (transformed, not yet published).
// The reflector delivers one at a time and publishes each delivery before it
// transforms the next, so an in-flight delivery is always the latest object
// the transform saw, and step 3 records it: its position was not published at
// step 2, so the position read there is the one before it, not its own
// resourceVersion. A transform that started before step 1 but had not yet
// taken the mutex is ordered after step 1 by the mutex and records too. Hence
// every key a watch delivery can have moved past the LIST is in the record.
// The argument needs only what the API guarantees: a LIST with NotOlderThan p
// reflects every change up to p, resourceVersions are unique, and the
// reflector publishes positions in delivery order. It does not assume the
// watch and the LIST are served by the same apiserver. Step 3 costs at most
// one key (two GETs), and only when that key is also a candidate.
//
// # RELISTS
//
// A relist delivers many objects and publishes one position, after all of
// them, and it deletes the keys its new list lacks with synthesized
// DeletedFinalStateUnknown tombstones that skip the transform. Under
// AtomicFIFO the relist is ONE queued ReplacedAll delta: its items are
// transformed when it is queued, and its tombstones are computed from the
// store only when it is processed, which replaces the store and then hands
// the tombstones to the handlers (controller.go:756 processReplacedAllInfo,
// the Replace at :784). A tombstone can only produce a lost ADD (the store
// lacks a key the LIST has), so a lost-ADD candidate that WAS in the store at
// step 1 and is gone now counts as delivered: something removed it during the
// pass. A relist that starts after step 1 transforms its items first and its
// tombstones apply only after its items are queued, so they apply after the
// key set was taken and the presence check sees them.
//
// Not covered, stated rather than assumed away: a relist already IN PROGRESS
// at step 1 (its earlier items and any tombstones it has applied are not in
// the record; under AtomicFIFO the store applies a relist all at once, so
// "in progress" narrows to "queued or being transformed"), and a relist that
// deletes a key whose ADD was still queued at step 1. Either needs, in addition, a LIST served by an apiserver whose cache
// is older than that relist; on a single apiserver the LIST is no older than
// anything the watch had already delivered. The result is one unnecessary
// repair, never a hidden loss: an uncovered key is counted as a divergence,
// not cleared.
package cache

import (
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// deliveryLog is one informer's record of the reflector's deliveries.
type deliveryLog struct {
	// position reads the informer's published LastSyncResourceVersion. NEVER
	// called from the transform or under mu (see the file header).
	position func() string

	mu sync.Mutex
	// latest is the latest object the transform saw.
	latest any
	// open is the set of keys delivered since a pass opened; nil when none is.
	open map[string]struct{}
}

func newDeliveryLog(position func() string) *deliveryLog {
	return &deliveryLog{position: position}
}

// rememberDeliveryLogLocked records gvr's delivery log once its transform is
// installed. Callers MUST hold rw.mu.Lock(); deletePerGVRStateLocked drops it.
func (rw *ResourceWatcher) rememberDeliveryLogLocked(gvr schema.GroupVersionResource, dl *deliveryLog) {
	if rw.deliveries == nil {
		rw.deliveries = map[schema.GroupVersionResource]*deliveryLog{}
	}
	rw.deliveries[gvr] = dl
}

// transform returns the informer transform: it records the delivery, then
// applies next (nil for none).
func (l *deliveryLog) transform(next clientcache.TransformFunc) clientcache.TransformFunc {
	return func(obj any) (any, error) {
		l.mu.Lock()
		l.latest = obj
		if l.open != nil {
			if key, _, ok := identityOf(obj); ok {
				l.open[key] = struct{}{}
			}
		}
		l.mu.Unlock()
		if next == nil {
			return obj, nil
		}
		return next(obj)
	}
}

// deliveryWindow is one open pass.
type deliveryWindow struct {
	log *deliveryLog
	// position is the published position read once the pass was open: the
	// forced LIST's NotOlderThan resourceVersion. Empty when none is
	// published yet.
	position string
	// presentAtOpen is the store's key set at open, sorted.
	presentAtOpen []string

	// closed and keys are set by the first close (under log.mu).
	closed bool
	keys   map[string]struct{}
}

// openPass opens a pass (see the file header).
func (l *deliveryLog) openPass(idx clientcache.Indexer) *deliveryWindow {
	l.mu.Lock()
	l.open = map[string]struct{}{}
	latest := l.latest
	present := idx.ListKeys()
	l.mu.Unlock()

	w := &deliveryWindow{log: l, position: l.position(), presentAtOpen: present}
	sort.Strings(w.presentAtOpen)
	// The latest object may be in flight: transformed, its position not yet
	// published. Unless the position just read IS its resourceVersion — a
	// watch delivery publishes exactly its object's resourceVersion, so then
	// it is published, the LIST includes it, and it needs no re-read. Any
	// other value (a bookmark since, a relist item) keeps it, at the cost of
	// at most one key.
	if latest != nil {
		if key, id, ok := identityOf(latest); ok && id.rv != w.position {
			l.mu.Lock()
			if l.open != nil {
				l.open[key] = struct{}{}
			}
			l.mu.Unlock()
		}
	}
	return w
}

// close stops recording and returns the keys delivered while the pass was
// open. Idempotent: forcedVerify closes explicitly once the comparison is done
// and again, deferred, on every early return.
func (w *deliveryWindow) close() map[string]struct{} {
	w.log.mu.Lock()
	defer w.log.mu.Unlock()
	if !w.closed {
		w.closed, w.keys = true, w.log.open
		w.log.open = nil
	}
	return w.keys
}

// delivered reports whether key may be ahead of the LIST: it was delivered
// while the pass was open, or (a lost ADD) it was in the store at open and is
// gone now. Only such a candidate is re-read.
func (w *deliveryWindow) delivered(keys map[string]struct{}, key string, class divergenceClass) bool {
	if _, ok := keys[key]; ok {
		return true
	}
	if class != classLostAdd {
		return false
	}
	i := sort.SearchStrings(w.presentAtOpen, key)
	return i < len(w.presentAtOpen) && w.presentAtOpen[i] == key
}
