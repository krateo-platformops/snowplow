// list_cell_watcher.go — #854: what the incremental list cell needs from the
// ResourceWatcher and the dep bridge.
//
//   - ServabilityEpoch: a per-GVR counter bumped on every transition OUT of
//     servable (watch broken, type confirmation retracted, informer removed) and
//     on every informer (re)creation. A cell captures it at anchor and re-checks
//     it on every read, so an outage or a rebuild — even one with no event the
//     dep tracker sees — drops the cell back to a full fill.
//   - listRawKeyedServable / rawByKey: the same per-object bytes ListRawServable
//     serves, with their keys, and one object by key.
//   - depWatchLostEventSignal / depWatchInFlight: the dep bridge's lost-event
//     counters, and whether a coordinate's event is still on its way.
package cache

import (
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	servEpochByGVR sync.Map // schema.GroupVersionResource → *atomic.Uint64
	servEpochAll   atomic.Uint64
)

// bumpServabilityEpoch marks a servability transition of gvr.
func bumpServabilityEpoch(gvr schema.GroupVersionResource) {
	v, _ := servEpochByGVR.LoadOrStore(gvr, new(atomic.Uint64))
	v.(*atomic.Uint64).Add(1)
}

// bumpServabilityEpochAll marks a transition of every GVR at once (a reset of
// the confirmed set).
func bumpServabilityEpochAll() { servEpochAll.Add(1) }

// ServabilityEpoch returns gvr's servability epoch.
func (rw *ResourceWatcher) ServabilityEpoch(gvr schema.GroupVersionResource) uint64 {
	e := servEpochAll.Load()
	if v, ok := servEpochByGVR.Load(gvr); ok {
		e += v.(*atomic.Uint64).Load()
	}
	return e
}

// keyedRaw is one indexer object's raw JSON with its key.
type keyedRaw struct {
	key listObjKey
	raw []byte
}

// listRawKeyedServable is ListRawServable with each item's key (indexer order:
// namespace, then name). Informer mode only.
func (rw *ResourceWatcher) listRawKeyedServable(gvr schema.GroupVersionResource, namespace string) ([]keyedRaw, bool) {
	if rw == nil || rw.mode == modePassthrough {
		return nil, false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false
	}
	items := indexerPartition(gi, namespace)
	out := make([]keyedRaw, 0, len(items))
	for _, it := range items {
		ns, name, ok := indexerSlotKey(it)
		if !ok {
			return nil, false
		}
		b, ok := storeItemJSON(it)
		if !ok {
			return nil, false
		}
		out = append(out, keyedRaw{key: listObjKey{ns: ns, name: name}, raw: b})
	}
	return out, true
}

// rawByKey reads one object's raw JSON from the indexer. servable=false when the
// GVR is not servable (the caller must not trust exists then).
func (rw *ResourceWatcher) rawByKey(gvr schema.GroupVersionResource, namespace, name string) (raw []byte, exists, servable bool) {
	if rw == nil || rw.mode == modePassthrough {
		return nil, false, false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false, false
	}
	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}
	it, exists, err := gi.Informer().GetIndexer().GetByKey(key)
	if err != nil {
		return nil, false, false
	}
	if !exists {
		return nil, false, true
	}
	b, ok := storeItemJSON(it)
	if !ok {
		return nil, false, false
	}
	return b, true, true
}

// depWatchLostEventSignal is the sum of the dep bridge's lost-event counters: a
// worker panic (an event's action was lost), an ADD seen with a nil syncCh, and a
// submit dropped onto a shut-down queue. Any increase means some event may never
// reach the incremental cells.
func depWatchLostEventSignal() uint64 {
	w := depWatchSingleton()
	return w.counters.workerPanics.Load() + w.counters.addNilSyncCh.Load() + w.counters.submitDroppedShutdown.Load()
}

// depWatchInFlight reports whether k's event is still on its way to the hook:
// queued or rate-limit-requeued (or holding a mask awaiting the worker), or
// being processed by the worker (between takeMask and the hook's return).
func depWatchInFlight(k depEventKey) bool {
	w := depWatchSingleton()
	w.maskMu.Lock()
	_, masked := w.masks[k]
	_, queued := w.queued[k]
	_, processing := w.processing[k]
	w.maskMu.Unlock()
	return masked || queued || processing
}
