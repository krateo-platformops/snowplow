// store_verify_delivery.go — #875, #918: what each informer's store has
// RECEIVED and not yet applied, what it has applied, and the keys a forced pass
// could not decide yet. The lost-update rule (store_verify_judge.go) reads
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
//     the latest delivery it handed on. An entry is "still queued" while the
//     store's applied position (Store.LastStoreSyncResourceVersion) is older
//     than the entry's rv: the queue applies in resourceVersion order, so once
//     the position has passed an entry, the entry has been applied — or
//     dropped, in which case the store, not the record, is the truth
//     (store_verify_judge.go). Our own handler removes an entry when it sees
//     that very delivery applied.
//   - APPLIED. The handler also keeps, per key, the {rv, uid} it last saw
//     applied. A delivery identical to it is a relist re-delivering an object
//     that did not change: the store already holds it, and a relist applies
//     such an object without telling any handler (sharedIndexInformer.OnUpdate
//     treats an unchanged resourceVersion as a sync), so the transform does not
//     record it as received in the first place (#923 review L8).
//   - APPLIED DELETES. The handler is the first place the event TYPE is known:
//     a TransformFunc receives the object only, and a watch DELETE's object is
//     indistinguishable from an update by content. The handler (processDeltas
//     writes the store, then calls OnAdd / OnUpdate / OnDelete) records which
//     keys were deleted and at which resourceVersion — or by a relist
//     tombstone, which carries none — and during which pass.
//
// The handler runs on its own goroutine, after the store write; the transform's
// entry is what lets the rule read a delete the store has applied before the
// handler has caught up (store_verify_judge.go, #923 review H3).
//
// # WHERE THE TRANSFORM RECORDS (F1 of the #923 review)
//
// AFTER the strip, and only when the strip returned no error and a non-nil
// object. A delivery the strip failed on is never queued (an error) or never
// applied (a nil object client-go cannot key), so recording it would say
// "received" for a state the store will never hold, and hide a real loss.
//
// # A GVR WHOSE TRANSFORM CANNOT BE INSTALLED
//
// SetTransform refuses an informer that has started. If the informer still
// runs, its log is installed without the RECEIVED record (recording=false) but
// with the handler, which a running informer accepts: applied deletes and the
// pending keys work as for every other GVR; what it cannot see is a delivery
// queued and not yet applied. A re-registered shared-factory GVR gets its
// informer back STOPPED (#244), which takes no handler either: that GVR has no
// log at all, and the forced pass judges it on the store alone (#940).
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
// transform touches only deliveryLog.mu, under which nothing but map reads and
// writes happen, and no informer accessor or indexer read ever happens under
// mu.
//
// # PENDING KEYS, AND THE EVENTS THAT RE-JUDGE THEM
//
// A forced-pass candidate the rule cannot decide yet is held here as PENDING,
// with the read it is held to: the store's applied position has not reached
// the read, or the store has received the read's state and not applied it, or
// a relist tombstone removed it during the pass. Two events re-judge it:
//
//   - our handler seeing anything applied to this GVR's store. The handler
//     runs after the store write, and every applied add, update or delete
//     advances the store's position (thread_safe_store.go Add / Update /
//     Delete set c.rv), so the resolver reads a position that includes that
//     event. A resolved loss is published and repaired from the resolver; the
//     walker never waits.
//   - the GVR's next forced pass, which judges every key still pending against
//     its OWN read and the store's position BEFORE its own LIST (judgePending),
//     and publishes those it decides as part of its report. Whatever it cannot
//     decide is superseded by its own, newer read, keeping the time the key
//     first became pending.
//
// A BOOKMARK advances the store's position too, but client-go v0.36.5 gives no
// callback for it: processDeltas `case Bookmark` calls only
// clientState.Bookmark (controller.go:656-661), which sets the position and a
// metrics gauge and nothing else (thread_safe_store.go:447-458), and
// ResourceEventHandler has only OnAdd / OnUpdate / OnDelete (controller.go:279).
// There is NO event for a bookmark-only advance, and this code does not poll
// and does not read the gauge. So on a GVR that only receives bookmarks, a
// pending key is decided by the next forced pass. While a GVR has pending
// keys it is flagged, and the walker serves flagged GVRs and the most overdue
// ones in turn (nextOverdueGVR); the flag is set and cleared with the pending
// set itself, so it never outlives the keys it was raised for (#923 review H2).
package cache

import (
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// storePendingKeys is the store_divergence_pending_keys gauge: pending keys
// across every GVR.
var storePendingKeys atomic.Int64

// received is one key's latest delivery handed on and not yet seen applied:
// what it carried, and its number on the log's transform counter.
type received struct {
	id  objIdentity
	seq uint64
}

// appliedDelete is one applied delete: its resourceVersion, or tombstone for a
// relist's synthesized delete, which carries none; and the pass generation it
// was applied in.
type appliedDelete struct {
	rv        string
	tombstone bool
	gen       uint64
}

// pendingKey is one key held pending: the read it is held to, and when it
// first became pending (kept across the passes that supersede it).
type pendingKey struct {
	read  readState
	since time.Time
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
	// recording is whether the transform is installed (see "A GVR WHOSE
	// TRANSFORM CANNOT BE INSTALLED").
	recording bool
	// inf and reg remove the handler when the log closes.
	inf clientcache.SharedIndexInformer
	reg clientcache.ResourceEventHandlerRegistration

	mu sync.Mutex
	// seq numbers the transform's deliveries.
	seq uint64
	// unapplied: key -> its latest delivery, until the handler sees it applied.
	unapplied map[string]received
	// stored: key -> the {rv, uid} the handler last saw applied.
	stored map[string]objIdentity
	// deletes: key -> its latest applied delete, from this pass generation or
	// the one before (beginPass prunes older ones: a LIST no older than the
	// store then lacks the key).
	deletes map[string]appliedDelete
	// gen numbers the forced passes: beginPass increments it.
	gen uint64
	// pending: keys a forced pass could not decide yet. pgen numbers the
	// pending sets: a resolver acts only on its own. carried keeps the first
	// pending time of keys a pass superseded, for the pass that re-holds them.
	pending map[string]pendingKey
	pgen    uint64
	carried map[string]time.Time
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
		stored:    map[string]objIdentity{},
		deletes:   map[string]appliedDelete{},
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
}

// bind attaches the informer the records describe. Called once, after the
// transform and the handler are installed.
func (l *deliveryLog) bind(inf clientcache.SharedIndexInformer) {
	l.published = inf.LastSyncResourceVersion
	l.indexer = inf.GetIndexer()
	l.applied = l.indexer.LastStoreSyncResourceVersion
}

// rememberDeliveryLogLocked records gvr's delivery log once installed.
// Callers MUST hold rw.mu.Lock(); deletePerGVRStateLocked drops it.
func (rw *ResourceWatcher) rememberDeliveryLogLocked(gvr schema.GroupVersionResource, dl *deliveryLog) {
	if rw.deliveries == nil {
		rw.deliveries = map[schema.GroupVersionResource]*deliveryLog{}
	}
	rw.deliveries[gvr] = dl
	liveDeliveryLogs.Store(dl, struct{}{})
}

// liveDeliveryLogs is every open log, for the per-GVR pending breakdown.
var liveDeliveryLogs sync.Map // *deliveryLog -> struct{}

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
			if l.stored[key] != id {
				l.unapplied[key] = received{id: id, seq: l.seq}
			}
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
		delete(l.stored, t.Key)
		l.deletes[t.Key] = appliedDelete{tombstone: true, gen: l.gen}
	} else if key, id, ok := identityOf(obj); ok {
		if e, has := l.unapplied[key]; has && e.id == id {
			delete(l.unapplied, key)
		}
		if deleted {
			delete(l.stored, key)
			l.deletes[key] = appliedDelete{rv: id.rv, gen: l.gen}
		} else {
			l.stored[key] = id
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
	d, ok := l.deletes[key]
	return d, ok
}

// passGen is the current pass generation: a read a pass takes carries it.
func (l *deliveryLog) passGen() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gen
}

// judgePending is the "next forced pass" event of the file header: before
// the pass reads anything, every key still pending is judged against its OWN
// read and the store's position now. A loss is removed from the pending set and
// returned, for the pass to publish in its report; a key the store has caught
// up with is settled; any other stays pending — until the pass's own read
// supersedes it (supersede), or, if the pass cannot take one, until a later
// event or pass.
func (l *deliveryLog) judgePending() []divergenceCandidate {
	pos := positionOf(l.appliedPosition()) // first (store_verify_judge.go)
	l.mu.Lock()
	gen := l.pgen
	keys := make(map[string]pendingKey, len(l.pending))
	for k, p := range l.pending {
		keys[k] = p
	}
	l.mu.Unlock()
	if len(keys) == 0 {
		return nil
	}

	var lost []divergenceCandidate
	var settled []string
	var resolvedReceived uint64
	for key, p := range keys {
		switch v := judgeKey(l.indexer, l, pos, key, p.read); v.outcome {
		case outcomeLost:
			lost = append(lost, divergenceCandidate{key, v.class})
			settled = append(settled, key)
		case outcomeAgree, outcomeStoreAhead:
			settled = append(settled, key)
			resolvedReceived++
		}
	}

	l.mu.Lock()
	if l.pgen != gen {
		l.mu.Unlock()
		return nil // closed meanwhile
	}
	for _, k := range settled {
		delete(l.pending, k)
	}
	empty := len(l.pending) == 0
	l.mu.Unlock()
	storePendingKeys.Add(-int64(len(settled)))
	storePendingResolvedLostTotal.Add(uint64(len(lost)))
	storePendingResolvedReceivedTotal.Add(resolvedReceived)
	if empty {
		setPendingFlag(l.gvr, false)
	}
	return lost
}

// beginPass opens a forced pass: the pass generation advances, deletes older
// than the previous pass are dropped, and the reflector's published position
// is returned — the forced LIST is no older than it.
func (l *deliveryLog) beginPass() string {
	l.mu.Lock()
	l.gen++
	for key, d := range l.deletes {
		if d.gen+1 < l.gen {
			delete(l.deletes, key)
		}
	}
	l.mu.Unlock()
	if l.published == nil {
		return ""
	}
	return l.published()
}

// supersede drops the keys still pending once the pass has taken its own,
// newer read of the GVR; each keeps its first pending time for the pass that
// re-holds it (setPending). The flag goes with the keys.
func (l *deliveryLog) supersede() {
	l.mu.Lock()
	n := len(l.pending)
	carried := make(map[string]time.Time, n)
	for k, p := range l.pending {
		carried[k] = p.since
	}
	l.pending = nil
	l.pgen++
	l.carried = carried
	l.mu.Unlock()
	if n > 0 {
		storePendingKeys.Add(-int64(n))
		storePendingSupersededTotal.Add(uint64(n))
		setPendingFlag(l.gvr, false)
	}
}

// setPending holds keys a pass could not decide yet, flags the GVR for the
// walker and starts the resolver. A key the previous pass also held keeps its
// first pending time. It wakes the resolver once: an apply that landed between
// the pass's verdict and this call found nothing pending and woke no one.
func (l *deliveryLog) setPending(reads map[string]readState) {
	if len(reads) == 0 {
		return
	}
	now := time.Now()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.pending = make(map[string]pendingKey, len(reads))
	for k, r := range reads {
		since := now
		if t, ok := l.carried[k]; ok {
			since = t
		}
		l.pending[k] = pendingKey{read: r, since: since}
	}
	l.carried = nil
	l.pgen++
	start := !l.running
	l.running = true
	l.mu.Unlock()
	storePendingKeys.Add(int64(len(reads)))
	setPendingFlag(l.gvr, true)
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

// oldestPending is when this GVR's oldest pending key first became pending
// (zero when none is).
func (l *deliveryLog) oldestPending() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	var oldest time.Time
	for _, p := range l.pending {
		if oldest.IsZero() || p.since.Before(oldest) {
			oldest = p.since
		}
	}
	return oldest
}

// close stops the resolver, drops what is pending and removes the handler:
// the GVR's informer is being torn down (deletePerGVRStateLocked).
func (l *deliveryLog) close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	n := len(l.pending)
	l.pending = nil
	l.pgen++
	l.mu.Unlock()
	storePendingKeys.Add(-int64(n))
	liveDeliveryLogs.Delete(l)
	if l.inf != nil && l.reg != nil {
		_ = l.inf.RemoveEventHandler(l.reg)
	}
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
		gen := l.pgen
		keys := make(map[string]pendingKey, len(l.pending))
		for k, p := range l.pending {
			keys[k] = p
		}
		l.mu.Unlock()

		var lost []divergenceCandidate
		var settled []string
		var resolvedReceived, undecided uint64
		pos := positionOf(l.appliedPosition()) // first (store_verify_judge.go)
		for key, p := range keys {
			switch v := judgeKey(l.indexer, l, pos, key, p.read); v.outcome {
			case outcomeLost:
				lost = append(lost, divergenceCandidate{key, v.class})
				settled = append(settled, key)
			case outcomeAgree, outcomeStoreAhead:
				settled = append(settled, key)
				resolvedReceived++
			case outcomeUndecided:
				if !v.reverify {
					// Not comparable: no later event can order it.
					settled = append(settled, key)
					undecided++
				}
			}
		}

		l.sweeps.Add(1)
		l.mu.Lock()
		if l.pgen != gen {
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
			setPendingFlag(l.gvr, false)
			return
		}
	}
}

// PendingByGVRSnapshot is the store_divergence_pending_keys breakdown: pending
// keys per GVR, for every GVR that has any. Bounded by the GVR count.
func PendingByGVRSnapshot() map[string]int64 {
	out := map[string]int64{}
	liveDeliveryLogs.Range(func(k, _ any) bool {
		if n := k.(*deliveryLog).pendingCount(); n > 0 {
			out[k.(*deliveryLog).gvr.String()] += int64(n)
		}
		return true
	})
	return out
}

// pendingOldestAgeSeconds is how long the oldest pending key, on any GVR, has
// been pending. 0 when none is.
func pendingOldestAgeSeconds() float64 {
	var oldest time.Time
	liveDeliveryLogs.Range(func(k, _ any) bool {
		if t := k.(*deliveryLog).oldestPending(); !t.IsZero() && (oldest.IsZero() || t.Before(oldest)) {
			oldest = t
		}
		return true
	})
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest).Seconds()
}

// pendingGVRsSorted lists the GVRs with pending keys, for logs and tests.
func pendingGVRsSorted() []string {
	m := PendingByGVRSnapshot()
	out := make([]string, 0, len(m))
	for g := range m {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// installDeliveryLogLocked installs gvr's delivery log on gi: the transform
// (next, the strip, runs first; nil for none) and the handler. Callers MUST
// hold rw.mu.Lock(). The returned error is the transform's: the informer
// already runs (a re-registered shared-factory GVR gets its started informer
// back), and the log is installed without its RECEIVED record. Only when the
// handler cannot be added either does the GVR get no log at all.
func (rw *ResourceWatcher) installDeliveryLogLocked(gvr schema.GroupVersionResource, gi informers.GenericInformer, next clientcache.TransformFunc) error {
	dl := newDeliveryLog(gvr)
	inf := gi.Informer()
	transformErr := inf.SetTransform(dl.transform(next))
	dl.recording = transformErr == nil
	reg, err := inf.AddEventHandler(dl.handler())
	if err != nil {
		if transformErr != nil {
			return transformErr
		}
		return err
	}
	dl.inf, dl.reg = inf, reg
	dl.bind(inf)
	if s, ok := gi.(*streamingDynamicInformer); ok && s.verifier != nil {
		s.verifier.setDeliveryLog(dl)
	}
	rw.rememberDeliveryLogLocked(gvr, dl)
	return transformErr
}
