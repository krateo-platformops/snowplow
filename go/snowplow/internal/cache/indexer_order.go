// indexer_order.go — #579: a LIST rebuilt from UNCHANGED data must produce
// IDENTICAL bytes.
//
// THE DEFECT. Both indexer read paths — listFromIndexer (decoded) and
// rawFromIndexer (#578's raw path) — enumerate store.List() / store.ByIndex(),
// which are backed by a Go map. Map iteration order is randomised per read, so
// rebuilding a cell from data that has not changed yields DIFFERENT bytes every
// time. Measured: an order-sensitive byte comparison over one live indexer read
// twice flaked in 3 of 6 consecutive runs, and never on a 1-item partition —
// the signature of iteration order, since a single item has only one order.
//
// WHY IT IS WORTH FIXING ON ITS OWN. It forecloses the cheapest available
// reduction in refresher work. Measured on 057/1.12.43 over one pod lifetime:
// 2,028,429 refresh completions against 19,845 customer reads — 102 refreshes
// per read. With a deterministic envelope the refresher can compare freshly
// rebuilt bytes against the stored bytes and, when equal, skip the Put, the dep
// re-record, the refresh broadcast and the downstream dirty propagation. Today
// that comparison is IMPOSSIBLE: a no-op refresh is indistinguishable from a
// real one because the bytes differ either way.
//
// It also decides an open design question rather than just saving work. #603
// established that dirty-mark load grows with the SQUARE of the population
// (99.66% of 16.1M marks come from list_dep edges). The fix for that is either
// narrowing what a list dependency matches, or refreshing less (#567) — and the
// latter collides with the hard zero-cold-navigations requirement (#604).
// Knowing what SHARE of those 2M refreshes produce byte-identical output tells
// us which: a high share means the invalidation is over-broad and precision is
// the answer, a low share means the data genuinely churns and it is not.
// That measurement needs this fix first.
//
// WHY (namespace, name), AND WHY THAT IS NOT AN ARBITRARY CHOICE. It is a total
// order over the keys the informer indexer already maintains, uniform across
// every GVR, with no per-resource carve-out. It is also the order the apiserver
// itself returns a LIST in, so this makes the CACHED path agree with the LIVE
// apiserver path rather than merely being self-consistent — the agreement #579
// explicitly asks the fix to confirm.
//
// That agreement is MEASURED, not assumed. Against krateo-057 on 2026-10-12,
// GET /api/v1/configmaps?limit=400 returned 400 items ALREADY in (namespace,
// name) order, and two consecutive reads returned the identical sequence. So
// sorting the indexer read this way converges the two paths; another total
// order would have made them disagree.
//
// SORTING IN PLACE IS SAFE, and this is load-bearing. client-go's
// threadSafeMap.List() and ByIndex() each allocate a FRESH slice
// (`make([]interface{}, 0, n)` then append) before returning; neither hands back
// a slice aliased to the store's own state. filterByNamespace likewise builds
// its own. So the sort cannot perturb the informer. Verified against
// client-go@v0.36.2, thread_safe_store.go:366 and :470.

package cache

import (
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// indexerPartition selects the indexer slots for a namespace ("" = all) and
// returns them in deterministic (namespace, name) order.
//
// This is the SINGLE partition-selection site shared by both read paths. Before
// #579 the selection was duplicated verbatim in listFromIndexer and
// rawFromIndexer; keeping one copy is what guarantees the decoded and raw
// envelopes cannot drift apart in ordering, which #579 notes is a property of
// the indexer read rather than of either serialisation.
func indexerPartition(gi informers.GenericInformer, namespace string) []interface{} {
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
	sortIndexerSlots(items)
	return items
}

// sortIndexerSlots orders slots by (namespace, name), in place.
func sortIndexerSlots(items []interface{}) {
	sort.SliceStable(items, func(i, j int) bool {
		nsI, nameI, okI := indexerSlotKey(items[i])
		nsJ, nameJ, okJ := indexerSlotKey(items[j])
		// A keyless slot sorts last. It cannot affect the emitted bytes —
		// see indexerSlotKey — so its position is a tidiness choice, not a
		// correctness one. Deciding it here rather than leaving it to the
		// comparator's fallthrough keeps the order total.
		if !okI || !okJ {
			return okI && !okJ
		}
		if nsI != nsJ {
			return nsI < nsJ
		}
		return nameI < nameJ
	})
}

// indexerSlotKey extracts the sort key from an indexer slot.
//
// Every renderable slot type carries it. *bytesObject embeds metav1.ObjectMeta
// precisely so that it satisfies the full metav1.Object interface (bytesobject.go
// FINDING 2). *unstructured.Unstructured and the typed kinds (e.g. *rbacv1.Role)
// implement it directly. ok=false therefore means the slot is not a Kubernetes
// object at all. The shared converter (store_item.go) cannot render such a slot,
// so since #624 the whole read fails closed and no bytes are emitted. That is
// why a missing key is not a determinism hole.
func indexerSlotKey(it interface{}) (namespace, name string, ok bool) {
	if o, isMeta := it.(metav1.Object); isMeta && o != nil {
		return o.GetNamespace(), o.GetName(), true
	}
	return "", "", false
}
