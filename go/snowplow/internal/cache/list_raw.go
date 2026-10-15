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
// THE FALLBACK IS NOT A SPECIAL CASE. A slot that is not a *bytesObject is one
// of two things. For the typed-RBAC exception it is a TYPED *rbacv1 object: the
// typed transform converts the Unstructured. Before #624 this was wrongly
// described as Unstructured, and every such slot was dropped. After a
// newBytesObject marshal failure it is a plain *unstructured.Unstructured. In
// both cases storeItemJSON (store_item.go) renders that one item through the
// same converter the decoded branch uses. The result is complete and correct
// for every GVR uniformly, and the fast path costs nothing when the bytes are
// already there. A slot that nothing can render fails the read closed.
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
	raws, err := rawFromIndexer(gi, namespace)
	if err != nil {
		// #624: fail closed, counted — never a shorter list served as true.
		recordUnrenderableStoreItem(unrenderableSiteListRaw, gvr, err)
		return nil, false
	}
	return raws, true
}

// PartitionLen returns how many objects gvr's namespace partition holds
// ("" = cluster-wide), under ListRawServable's servability contract, without
// rendering, copying or sorting them — index keys only. #710 PR-2a: the
// content-fill admission gate predicts a cold LIST's envelope size from it
// before the dispatch builds the envelope. false = unknown (not servable).
func (rw *ResourceWatcher) PartitionLen(gvr schema.GroupVersionResource, namespace string) (int, bool) {
	if rw == nil || rw.mode == modePassthrough {
		return 0, false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	gi, ok := rw.servableLocked(gvr)
	if !ok {
		return 0, false
	}
	store := gi.Informer().GetIndexer()
	if namespace == "" {
		return len(store.ListKeys()), true
	}
	keys, err := store.IndexKeys(clientcache.NamespaceIndex, namespace)
	if err != nil {
		return 0, false
	}
	return len(keys), true
}

// rawFromIndexer mirrors listFromIndexer's partition selection EXACTLY — same
// namespace index, same ByIndex-error fallback to a filtered full List — and
// differs only in what it extracts per item.
//
// FAIL-CLOSED PARITY (#624): listFromIndexer fails the whole read on a slot
// storeItemToUnstructured cannot render; this fails it on a slot
// storeItemJSON cannot render — the same population, since storeItemJSON
// renders every non-bytes slot THROUGH storeItemToUnstructured. Before #624
// both silently skipped such a slot, and the typed-RBAC slots were all of
// them.
func rawFromIndexer(gi informers.GenericInformer, namespace string) ([][]byte, error) {
	// #579: deterministic (namespace, name) order — a LIST rebuilt from
	// unchanged data must produce identical bytes. See indexer_order.go.
	items := indexerPartition(gi, namespace)

	out := make([][]byte, 0, len(items))
	for _, it := range items {
		b, ok := storeItemJSON(it)
		if !ok {
			return nil, newUnrenderableStoreItemError(it)
		}
		out = append(out, b)
	}
	return out, nil
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
