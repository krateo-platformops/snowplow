// list_raw.go — #578: serve an informer LIST partition as the per-item JSON
// the indexer ALREADY HOLDS, without decoding it into map trees.
//
// WHY THIS EXISTS. The apistage content refresh path did three full passes
// over every list it rebuilt:
//
//  1. listFromIndexer   bytesObject.Decode  bytes -> map[string]interface{}
//  2. marshalAsList     json.Marshal        map   -> bytes
//  3. parseListEnvelope json.Unmarshal      bytes -> map
//
// Passes 2 and 3 are a net identity on the data. Measured on krateo-057 with
// the portal IDLE for 38 minutes (nothing read in that window, so this is
// refresher cost, not serve cost): the pod sat at 3,950m of a 4,000m limit —
// 98.75% — allocating 568 MB/s, 4.89M mallocs/s, with GC pause at 846 ms per
// wall second and `gcAssistAlloc` at 24.7% of the profile (the resolving
// goroutines conscripted into collecting their own garbage). bytesObject.Decode
// was 28.0% and parseListEnvelope 27.6% of a 30s CPU profile.
//
// WHY IT IS SAFE TO SKIP THE DECODE. Ship H1 stores each informer item as a
// *bytesObject whose `raw` is the COMPLETE object JSON, produced by
// json.Marshal of the already-stripped Unstructured (defaultStripUnstructured
// drops managedFields and the last-applied annotation BEFORE newBytesObject
// marshals). So `raw` is:
//
//   - valid JSON by construction — it came out of json.Marshal;
//   - already stripped, so concatenating it cannot reintroduce managedFields;
//   - key-ordered exactly as json.Marshal orders a map (alphabetical), which is
//     what makes marshalAsListRaw byte-identical to marshalAsList.
//
// Ship H5 made the bytes representation UNIVERSAL — streaming unless excepted,
// and the only exception is isStreamingException (the 4 typed-RBAC GVRs, which
// genuinely need a typed Go representation). So virtually every GVR's indexer
// holds raw bytes, and this path is general rather than a carve-out
// (feedback_no_special_cases).
//
// THE FALLBACK IS NOT A SPECIAL CASE. An indexer slot can hold a plain
// *unstructured.Unstructured in two situations: the typed-RBAC exception, and a
// newBytesObject marshal failure (the transform stores the Unstructured rather
// than stalling the informer). rawItemJSON marshals that one item on the spot,
// so the result is complete and correct for every GVR uniformly — the fast path
// is simply free when the bytes are already there.
package cache

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// ListRawServable returns gvr's namespace partition as per-item JSON, under
// EXACTLY the servability contract ListObjectsServable enforces: the
// four-conjunct servableLocked predicate (registered AND HasSynced AND
// watchHealthy AND resourceTypeConfirmed), with the indexer read off the same
// `gi` handle under one rw.mu read hold.
//
// (nil, false) means the caller MUST fall through to the apiserver — identical
// to ListObjectsServable, and for the same reason: an empty slice from an
// unsynced or unconfirmed informer is indistinguishable from a genuine "no
// objects" answer, and serving it broke the Compositions feature at S4
// (regression journal 2026-05-15). A genuinely-empty-but-synced informer
// returns ([], true), which is a real answer the watcher can vouch for.
//
// The returned slices ALIAS the bytesObject `raw` arrays, which are immutable
// after construction (the SB-4 contract: no field of a bytesObject is ever
// mutated once newBytesObject returns). Callers MUST NOT write to them; every
// caller today only appends them into a fresh envelope buffer.
//
// Safe for concurrent use; takes rw.mu in read mode.
func (rw *ResourceWatcher) ListRawServable(gvr schema.GroupVersionResource, namespace string) ([][]byte, bool) {
	if rw == nil {
		return nil, false
	}
	if rw.mode == modePassthrough {
		// Passthrough routes to the apiserver and is authoritative; it yields
		// decoded objects, so marshal them to match this method's contract.
		return rawFromUnstructured(rw.listPassthrough(gvr, namespace)), true
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return nil, false
	}
	// Defense-in-depth, mirroring ListObjectsServable: assert the invariant AT
	// THE SERVE. gi was just resolved by servableLocked so this holds by
	// construction today; the assert is the guard that trips if a future
	// refactor returns data,true here without the predicate.
	if !rw.assertServeRequiresServableLocked(gvr, "ListRawServable") {
		return nil, false
	}
	return rawFromIndexer(gi, namespace), true
}

// rawFromIndexer mirrors listFromIndexer's partition selection EXACTLY — same
// namespace index, same ByIndex-error fallback to a filtered full List — and
// differs only in what it extracts per item.
//
// DROP PARITY: listFromIndexer drops an item decodeBytesObject cannot convert;
// this drops an item rawItemJSON cannot render. Both are "should never happen"
// populations, and both fail the same way (the item is absent from the list
// rather than the list failing), so a malformed object never stalls a serve.
func rawFromIndexer(gi informers.GenericInformer, namespace string) [][]byte {
	store := gi.Informer().GetIndexer()
	var items []interface{}
	if namespace == "" {
		items = store.List()
	} else {
		idx, err := store.ByIndex(clientcache.NamespaceIndex, namespace)
		if err != nil {
			items = filterByNamespace(store.List(), namespace)
		} else {
			items = idx
		}
	}

	out := make([][]byte, 0, len(items))
	for _, it := range items {
		if b, ok := rawItemJSON(it); ok {
			out = append(out, b)
		}
	}
	return out
}

// rawItemJSON renders one indexer slot as JSON.
//
// The *bytesObject arm is the whole point: it returns the stored array with no
// decode, no marshal and no allocation. The *unstructured.Unstructured arm
// covers the typed-RBAC streaming exception and the newBytesObject
// marshal-failure fallback; it costs one marshal for that one item, which is
// what the pre-#578 path paid for EVERY item.
func rawItemJSON(obj interface{}) ([]byte, bool) {
	switch v := obj.(type) {
	case *bytesObject:
		if v == nil || len(v.raw) == 0 {
			return nil, false
		}
		return v.raw, true
	case *unstructured.Unstructured:
		if v == nil {
			return nil, false
		}
		b, err := json.Marshal(v.Object)
		if err != nil {
			return nil, false
		}
		return b, true
	default:
		return nil, false
	}
}

// rawFromUnstructured marshals an already-decoded list, for the passthrough
// mode where no stored bytes exist.
func rawFromUnstructured(items []*unstructured.Unstructured) [][]byte {
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		if it == nil {
			continue
		}
		b, err := json.Marshal(it.Object)
		if err != nil {
			continue
		}
		out = append(out, b)
	}
	return out
}
