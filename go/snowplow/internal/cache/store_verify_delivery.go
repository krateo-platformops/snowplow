// store_verify_delivery.go — #875, #918: what each informer's store has
// RECEIVED and not yet applied, which deletes it applied, and the keys a forced
// pass could not decide yet. The lost-update rule (store_verify_judge.go) reads
// these records together with the store's applied position.
//
// # WHY THE STORE'S APPLIED STATE IS NOT ENOUGH
//
// The reflector hands each object to the informer's queue, and the queue is
// applied to the store later (client-go@v0.36.5 AtomicFIFO: the_real_fifo.go
// addToItems_locked / addReplaceToItemsLocked enqueue, processDeltas applies).
// Until it is applied the store is BEHIND what it has received, and comparing a
// read with the store alone calls that a lost update (#878: the store behind
// the read). Until #918 a 250 ms wait papered over it; the wait is gone, and
// these records replace it:
//
//   - RECEIVED. The informer's transform records, per key, the {rv, uid} of
//     the latest delivery it handed on, and the transform's sequence number.
//     An entry is "still queued" while the store's applied position
//     (Store.LastStoreSyncResourceVersion) is older than the entry's rv: the
//     queue applies in resourceVersion order, so once the position has passed
//     an entry, the entry has been applied — or dropped, in which case the
//     store, not the record, is the truth (store_verify_judge.go). Our own
//     handler removes an entry when it sees that very delivery applied.
//   - APPLIED DELETES. The handler is the first place the event TYPE is known:
//     a TransformFunc receives the object only, and a watch DELETE's object is
//     indistinguishable from an update by content. The handler (processDeltas
//     writes the store, then calls OnAdd / OnUpdate / OnDelete) records which
//     keys were deleted and at which resourceVersion — or by a relist
//     tombstone, which carries none.
//
// # WHERE THE TRANSFORM RECORDS (F1 of the #923 review)
//
// AFTER the strip, and only when the strip returned no error and a non-nil
// object. A delivery the strip failed on is never queued (an error) or never
// applied (a nil object client-go cannot key), so recording it would say
// "received" for a state the store will never hold, and hide a real loss.
//
// # THE TRANSFORM TAKES NO LOCK BUT ITS OWN
//
// It runs under the FIFO's lock: AtomicFIFO's UnlockWhileProcessingFIFO
// releases that lock only around PROCESSING a popped delta
// (the_real_fifo.go:454 whileProcessing_locked), never around the enqueue the
// transform belongs to. At client-go v0.35.3 the informer's accessors took
// startedLock and HasSynced then took the FIFO's lock, so reading the
// informer's position from inside the transform was an AB/BA deadlock with
// any concurrent HasSynced — one this file's first draft had, and the kind
// churn falsifier hung on. v0.36.5 made HasSynced a channel read
// (shared_informer.go:791), but LastSyncResourceVersion still takes
// startedLock and the reflector's mutex (shared_informer.go:819,
// controller.go:223) and LastStoreSyncResourceVersion takes the store's lock
// (thread_safe_store.go:436), and a transform-time read of either is not the
// position of the delivery being transformed anyway. So the rule stands: the
// transform touches only deliveryLog.mu, under which nothing but map writes
// happen, and no informer accessor or indexer read ever happens under mu.
//
// # PENDING KEYS, AND WHAT RE-JUDGES THEM
//
// A forced-pass candidate the rule cannot decide yet — the store's applied
// position has not reached the read — is held here as PENDING, with the read
// it is held to. A resolver goroutine re-judges the pending keys whenever our
// handler sees anything applied to this GVR's store: the handler runs after
// the store write, and every applied add, update or delete advances the
// store's position (thread_safe_store.go Add / Update / Delete set c.rv), so
// the resolver reads a position that includes that event. Nothing is timed
// and the walker never waits: a resolved loss is published and repaired from
// the resolver (publishResolvedLoss).
//
// A BOOKMARK advances the store's position too, but client-go v0.36.5 gives no
// callback for it: processDeltas `case Bookmark` calls only
// clientState.Bookmark (controller.go:656-661), which sets the position and a
// metrics gauge and nothing else (thread_safe_store.go:447-458), and
// ResourceEventHandler has only OnAdd / OnUpdate / OnDelete (controller.go:279).
// There is NO event for a bookmark-only advance, and this code does not poll
// and does not read the gauge. So a key whose read only a bookmark passes is
// re-judged by the GVR's next applied event or by its next forced pass, and
// while keys are pending the GVR goes to the front of the walker
// (markReverifyFirst). The next forced pass supersedes whatever is still
// pending, with its own, newer read.
package cache

import (
	"reflect"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// storePendingKeys is the store_divergence_pending_keys gauge: pending keys
// across every GVR.
var storePendingKeys atomic.Int64

// received is one key's latest delivery: what it carried, and its number on
// the log's transform counter.
type received struct {
	id  objIdentity
	seq uint64
}

// appliedDelete is one applied delete: its resourceVersion, or tombstone for a
// relist's synthesized delete, which carries none.
type appliedDelete struct {
	rv        string
	tombstone bool
}

// deliveryLog is one informer's records (see the file header).
type deliveryLog struct {
	gvr schema.GroupVersionResource

	// bound once, before the informer runs (bind); read without mu.
	//   published: the reflector's published position — the forced LIST is no
	//              older than it.
	//   applied:   the store's applied position.
	published func() string
	applied   func() string
	indexer   clientcache.Indexer

	mu sync.Mutex
	// seq numbers the transform's deliveries.
	seq uint64
	// unapplied: key -> its latest delivery, until the handler sees it applied.
	unapplied map[string]received
	// deletes / prevDeletes: deletes applied since the latest pass opened, and
	// since the one before. Two generations, rotated by beginPass, so a delete
	// applied just before a pass opened is still known to it; an older one is
	// not needed — a LIST no older than the store then lacks the key.
	deletes, prevDeletes map[string]appliedDelete
	// pending: keys a forced pass could not decide yet, with the read each is
	// held to. gen numbers the pending sets: a resolver acts only on its own.
	pending map[string]readState
	gen     uint64
	running bool
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	// sweeps counts the resolver's judgements of its pending set, so a test
	// can tell "the resolver has looked" from "the resolver has not run yet".
	sweeps atomic.Uint64
}

func newDeliveryLog(gvr schema.GroupVersionResource) *deliveryLog {
	return &deliveryLog{
		gvr:       gvr,
		unapplied: map[string]received{},
		deletes:   map[string]appliedDelete{},
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
}

// bind attaches the informer the records describe. Called once, after the
// transform and the handler are installed and before the informer runs.
func (l *deliveryLog) bind(inf clientcache.SharedIndexInformer) {
	l.published = inf.LastSyncResourceVersion
	l.indexer = inf.GetIndexer()
	l.applied = l.indexer.LastStoreSyncResourceVersion
}

// rememberDeliveryLogLocked records gvr's delivery log once its transform is
// installed. Callers MUST hold rw.mu.Lock(); deletePerGVRStateLocked drops it.
func (rw *ResourceWatcher) rememberDeliveryLogLocked(gvr schema.GroupVersionResource, dl *deliveryLog) {
	if rw.deliveries == nil {
		rw.deliveries = map[schema.GroupVersionResource]*deliveryLog{}
	}
	rw.deliveries[gvr] = dl
}

// isNilObject reports a nil interface or a typed nil pointer.
func isNilObject(obj any) bool {
	if obj == nil {
		return true
	}
	v := reflect.ValueOf(obj)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// transform returns the informer transform: next (the strip; nil for none),
// then the record of what it handed on (see "WHERE THE TRANSFORM RECORDS").
func (l *deliveryLog) transform(next clientcache.TransformFunc) clientcache.TransformFunc {
	return func(obj any) (any, error) {
		out, err := obj, error(nil)
		if next != nil {
			out, err = next(obj)
		}
		if err != nil || isNilObject(out) {
			return out, err
		}
		if key, id, ok := identityOf(out); ok {
			l.mu.Lock()
			l.seq++
			l.unapplied[key] = received{id: id, seq: l.seq}
			l.mu.Unlock()
		}
		return out, err
	}
}

// mark returns the transform counter now: deliveries numbered at or below it
// were handed on before this call.
func (l *deliveryLog) mark() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// handler returns the event handler that sees each delivery applied.
func (l *deliveryLog) handler() clientcache.ResourceEventHandler {
	return clientcache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { l.appliedEvent(obj, false) },
		UpdateFunc: func(_, obj any) { l.appliedEvent(obj, false) },
		DeleteFunc: func(obj any) { l.appliedEvent(obj, true) },
	}
}

// appliedEvent records one applied event. The unapplied entry goes only when
// the applied object IS the latest received one ({rv, uid} equal): an older
// delivery's apply leaves the newer, still queued one in place.
func (l *deliveryLog) appliedEvent(obj any, deleted bool) {
	l.mu.Lock()
	if t, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		l.deletes[t.Key] = appliedDelete{tombstone: true}
	} else if key, id, ok := identityOf(obj); ok {
		if e, has := l.unapplied[key]; has && e.id == id {
			delete(l.unapplied, key)
		}
		if deleted {
			l.deletes[key] = appliedDelete{rv: id.rv}
		}
	}
	wake := len(l.pending) > 0
	l.mu.Unlock()
	if wake {
		l.signal()
	}
}

func (l *deliveryLog) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// appliedPosition is the store's applied position ("" when unknown).
func (l *deliveryLog) appliedPosition() string {
	if l.applied == nil {
		return ""
	}
	return l.applied()
}

// receivedFor returns key's latest delivery not yet seen applied, if any.
func (l *deliveryLog) receivedFor(key string) (received, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.unapplied[key]
	return e, ok
}

// deleteFor returns the latest applied delete of key, if one is known.
func (l *deliveryLog) deleteFor(key string) (appliedDelete, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if d, ok := l.deletes[key]; ok {
		return d, true
	}
	d, ok := l.prevDeletes[key]
	return d, ok
}

// beginPass opens a forced pass: the delete generations rotate, received
// entries the store's position has already passed are dropped (they are
// applied or were dropped by the queue; the store is the truth for them — a
// relist that re-delivers an unchanged object is applied without telling the
// handler, sharedIndexInformer.OnUpdate treating an unchanged resourceVersion
// as a sync), whatever is still pending from the previous pass is superseded
// (this pass reads again), and the reflector's published position is returned:
// the forced LIST is no older than it.
func (l *deliveryLog) beginPass() string {
	applied := l.appliedPosition()
	l.mu.Lock()
	l.prevDeletes, l.deletes = l.deletes, map[string]appliedDelete{}
	if applied != "" {
		for key, e := range l.unapplied {
			if c, err := orderRV(e.id.rv, applied); err == nil && c <= 0 {
				delete(l.unapplied, key)
			}
		}
	}
	superseded := len(l.pending)
	l.pending = nil
	l.gen++
	l.mu.Unlock()
	if superseded > 0 {
		storePendingKeys.Add(-int64(superseded))
		storePendingSupersededTotal.Add(uint64(superseded))
	}
	if l.published == nil {
		return ""
	}
	return l.published()
}

// setPending holds keys a pass could not decide yet and starts the resolver.
// It wakes the resolver once: an apply that landed between the pass's verdict
// and this call found nothing pending and woke no one.
func (l *deliveryLog) setPending(reads map[string]readState) {
	if len(reads) == 0 {
		return
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.pending = reads
	l.gen++
	start := !l.running
	l.running = true
	l.mu.Unlock()
	storePendingKeys.Add(int64(len(reads)))
	if start {
		go l.resolve()
	}
	l.signal()
}

// pendingCount is the number of keys pending on this GVR.
func (l *deliveryLog) pendingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending)
}

// close stops the resolver and drops what is pending: the GVR's informer is
// being torn down (deletePerGVRStateLocked).
func (l *deliveryLog) close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	n := len(l.pending)
	l.pending = nil
	l.gen++
	l.mu.Unlock()
	storePendingKeys.Add(-int64(n))
	close(l.stop)
}

// resolve re-judges the pending keys on every wake until none is left.
func (l *deliveryLog) resolve() {
	for {
		select {
		case <-l.stop:
			return
		case <-l.wake:
		}
		l.mu.Lock()
		if len(l.pending) == 0 {
			l.running = false
			l.mu.Unlock()
			return
		}
		gen := l.gen
		keys := make(map[string]readState, len(l.pending))
		for k, r := range l.pending {
			keys[k] = r
		}
		l.mu.Unlock()

		var lost []divergenceCandidate
		var settled []string
		var resolvedReceived, undecided uint64
		for key, read := range keys {
			switch v := judgeKey(l.indexer, l, positionOf(l.appliedPosition()), key, read); v.outcome {
			case outcomeLost:
				lost = append(lost, divergenceCandidate{key, v.class})
				settled = append(settled, key)
			case outcomeAgree, outcomeStoreAhead, outcomeReceived:
				settled = append(settled, key)
				resolvedReceived++
			case outcomeUndecided:
				settled = append(settled, key)
				undecided++
			}
		}

		l.sweeps.Add(1)
		l.mu.Lock()
		if l.gen != gen {
			// Superseded by a newer pass, or closed, while judging: its read
			// is the one that counts now.
			l.mu.Unlock()
			continue
		}
		for _, k := range settled {
			delete(l.pending, k)
		}
		empty := len(l.pending) == 0
		if empty {
			l.running = false
		}
		l.mu.Unlock()
		storePendingKeys.Add(-int64(len(settled)))
		storePendingResolvedReceivedTotal.Add(resolvedReceived)
		storeUndecidedTotal.Add(undecided)
		if len(lost) > 0 {
			storePendingResolvedLostTotal.Add(uint64(len(lost)))
			publishResolvedLoss(l.gvr, lost)
		}
		if empty {
			return
		}
	}
}

// installDeliveryLogLocked installs gvr's delivery record on gi: the transform
// (next, the strip, runs first; nil for none) and the handler, both before the
// informer runs. Callers MUST hold rw.mu.Lock(). An error means the informer
// already runs (a re-registered shared-factory GVR gets its started informer
// back): the GVR then has no record, and the forced pass judges it without one.
func (rw *ResourceWatcher) installDeliveryLogLocked(gvr schema.GroupVersionResource, gi informers.GenericInformer, next clientcache.TransformFunc) error {
	dl := newDeliveryLog(gvr)
	inf := gi.Informer()
	if err := inf.SetTransform(dl.transform(next)); err != nil {
		return err
	}
	if _, err := inf.AddEventHandler(dl.handler()); err != nil {
		return err
	}
	dl.bind(inf)
	if s, ok := gi.(*streamingDynamicInformer); ok && s.verifier != nil {
		s.verifier.setDeliveryLog(dl)
	}
	rw.rememberDeliveryLogLocked(gvr, dl)
	return nil
}
