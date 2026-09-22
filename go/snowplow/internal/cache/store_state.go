// store_state.go — snowplow#237 deliverable A: the per-object read of what
// OUR informer store holds.
//
// # WHY THIS FILE EXISTS
//
// Every surface snowplow had was DOWNSTREAM of the informer store:
//
//	/debug/servable   per-GVR conjuncts + lastSyncRV + indexer COUNT
//	/debug/apistage   L1 entry metadata
//	/debug/reconcile  L1 probed against the informer's own indexer
//	/call             the resolved output
//
// So the question #237 spent two sessions failing to answer — "is the STORE
// stale, or is the L1 cell stale?" — was unanswerable in-process: both states
// render identically on every one of those surfaces (the executable proof is
// internal/handlers/debug_store_falsifier_test.go, which asserts /debug/apistage
// and /debug/reconcile are BYTE-EQUAL across a store at rv=1 and a store at
// rv=2 with L1 held constant). StoreObjectState is the missing read, and it is
// deliberately the smallest one that answers the question.
//
// THREE PROPERTIES THAT ARE NOT NEGOTIABLE.
//
//  1. IT DOES NOT GATE ON servableLocked. probeObjectState (deps_watch.go)
//     returns objUnknown for a non-servable GVR by design — correct for an
//     eviction decision, useless for a diagnostic, because "the GVR is
//     retracted" is one of the answers an operator needs. #237's own capture
//     read confirm_retracted_total: 10158. The four conjuncts are reported as
//     FIELDS here and gate nothing. That is why this is a new method and not a
//     call to probeObjectState.
//
//  2. METADATA AND A HASH, NEVER A BODY. The route this backs is behind the
//     /debug/* JWT gate, which admits any valid Krateo token — so it must never
//     emit content for an object the caller's own RBAC may forbid. The guard is
//     the TYPE: StoreObjectState has no []byte, no map, no slice and no decoded
//     tree, exactly like ResolvedEntryMeta for /debug/apistage. It is
//     structurally incapable of carrying a body.
//
//  3. NO APISERVER CONTACT. There is no client field on this type and no client
//     reachable from this file. Reading the apiserver as the snowplow
//     ServiceAccount would tell any valid-JWT holder the existence and
//     resourceVersion of objects their own RBAC forbids — a cross-identity read
//     of exactly the class the standing debug-surface rule exists to prevent.
//     The operator does that half themselves, under their own RBAC, with
//     kubectl. Enforced by the absence of a client, not by this comment.
//
// AND ONE THING IT DELIBERATELY DOES NOT DO. There is no per-object "last
// seen". Tracking one would be a 50K-entry map[key]int64 per GVR written on the
// informer's processor goroutine — a real memory and contention cost for a
// diagnostic. The GVR-level clocks (GVRLastEventAgeSeconds,
// GVRLastSyncResourceVersion) are what the #237 capture actually needed: the
// per-object resourceVersion answers "is this object stale", and the GVR clock
// answers "is this GVR receiving events at all". Do not re-propose the map.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// StoreObjectState is what the informer store holds for one (gvr, namespace,
// name), as METADATA ONLY.
//
// STRUCTURAL LEAK GUARD: every field is a string, bool or number. No []byte, no
// map, no slice — the type cannot carry an object body, and that is the guard
// (see TestStoreObjectState_StructurallyCannotLeakContent).
type StoreObjectState struct {
	GVR       string `json:"gvr"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`

	// Registered is membership in rw.informers. False means snowplow has no
	// informer for this GVR at all, which is a different answer from "the
	// object is gone" and must not be confused with it.
	Registered bool `json:"registered"`

	// The four servability conjuncts, REPORTED AND NOT GATING (property 1).
	// Identical derivation to /debug/servable's row so the two surfaces can
	// never disagree: HasSynced/WatchBroken/Confirmed come from the same maps
	// and Servable from servableLocked itself.
	//
	// Confirmed is raw membership in rw.confirmed, exactly as
	// ServableSnapshot reports it. When no discovery client is wired
	// (rw.disco == nil, the degraded/test configuration) conjunct 4 is
	// satisfied without the map, so Confirmed can read false while Servable
	// reads true. That is the pre-existing /debug/servable semantics and this
	// route mirrors it rather than inventing a second reading.
	HasSynced   bool `json:"hasSynced"`
	WatchBroken bool `json:"watchBroken"`
	Confirmed   bool `json:"confirmed"`
	Servable    bool `json:"servable"`

	// Found is whether the indexer holds the key. A registered GVR with
	// Found=false is the phantom-DELETE answer; Found=true with an old
	// ResourceVersion is the lost-UPDATE answer. Distinguishing those two is
	// the whole deliverable.
	Found bool `json:"found"`

	// The stored object's own metadata. Empty when Found is false.
	ResourceVersion   string `json:"resourceVersion,omitempty"`
	UID               string `json:"uid,omitempty"`
	Generation        int64  `json:"generation,omitempty"`
	CreationTimestamp string `json:"creationTimestamp,omitempty"`

	// BodySHA256 is the sha256 of the STORED bytes, hex-encoded.
	//
	// COMPARABLE ONLY BETWEEN TWO SNOWPLOW READS. It is NOT comparable with
	// `kubectl get -o json | sha256sum`: SetTransform(StripBulkyFieldsForResourceType)
	// (watcher.go) has already stripped managedFields and the
	// last-applied-configuration annotation before the object reached the
	// store. An operator who compares it against kubectl gets a divergence
	// that is not one — and the whole point of this route is that it is used
	// under time pressure. Use it to compare the same object across two reads,
	// or two cohorts' view of it; use ResourceVersion against the apiserver.
	BodySHA256 string `json:"bodySHA256,omitempty"`

	// Representation is the stored Go shape: "bytesObject" (the default H1
	// GC-lean form), "unstructured", "partialObjectMetadata", or the concrete
	// type name for a typed-RBAC object. It answers "which storage path did
	// this GVR take", which is otherwise only inferable from a log line.
	Representation string `json:"representation,omitempty"`

	// GVR-level clocks (see the file header on why these are not per-object).
	// GVRLastEventAgeSeconds is -1 when the bridge has never delivered an
	// event for this GVR — "never", not "just now".
	GVRLastSyncResourceVersion string  `json:"gvrLastSyncResourceVersion,omitempty"`
	GVRLastEventAgeSeconds     float64 `json:"gvrLastEventAgeSeconds"`
	IndexerCount               int     `json:"indexerCount"`
}

// StoreObjectState reports what the informer store holds for the coordinate.
//
// READ-ONLY: one rw.mu.RLock, one ListKeys, one GetByKey. No EnsureResourceType,
// no confirm, no Put, no key derivation, no client call — the same contract
// /debug/servable documents. Nil-receiver safe; in passthrough mode there are
// no informers and it reports Registered=false rather than doing a live GET.
func (rw *ResourceWatcher) StoreObjectState(gvr schema.GroupVersionResource, namespace, name string) StoreObjectState {
	out := StoreObjectState{
		GVR:                    gvr.String(),
		Namespace:              namespace,
		Name:                   name,
		GVRLastEventAgeSeconds: -1,
	}
	if rw == nil || rw.mode == modePassthrough {
		return out
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	gi, ok := rw.informers[gvr]
	if !ok || gi == nil {
		return out
	}
	out.Registered = true

	inf := gi.Informer()
	if inf == nil {
		return out
	}
	out.HasSynced = inf.HasSynced()
	_, out.WatchBroken = rw.watchBroken[gvr]
	_, out.Confirmed = rw.confirmed[gvr]
	// servableLocked is the single source of truth for the composite — reuse
	// it rather than re-deriving the conjunction, so a future conjunct change
	// cannot make this diagnostic disagree with the gate it describes.
	_, out.Servable = rw.servableLocked(gvr)
	out.GVRLastSyncResourceVersion = rw.lastSyncRV[gvr]
	if ns := rw.lastEventUnixNano(gvr); ns > 0 {
		out.GVRLastEventAgeSeconds = time.Since(time.Unix(0, ns)).Seconds()
	}

	idx := inf.GetIndexer()
	if idx == nil {
		return out
	}
	out.IndexerCount = len(idx.ListKeys())

	key := name
	if namespace != "" {
		key = namespace + "/" + name
	}
	obj, exists, err := idx.GetByKey(key)
	if err != nil || !exists || obj == nil {
		return out
	}
	out.Found = true
	describeStoredObject(&out, obj)
	return out
}

// describeStoredObject fills the per-object fields from whatever the indexer
// holds. The three production shapes are covered explicitly; anything else
// (a typed-RBAC object) reports its concrete type name rather than a guess.
//
// Concurrency: what makes every branch here safe is IMMUTABILITY, not the
// caller's lock. rw.mu protects the informer MAP, not the objects in the
// indexer — an object handed out by GetByKey is read by other goroutines
// regardless of who holds rw.mu. bytesObject is immutable after construction
// (the SB-4 contract, bytesobject.go), and the Unstructured / PartialObjectMetadata
// branches only marshal. Do not add a branch that mutates the stored object on
// the authority of the lock: the lock does not grant it.
func describeStoredObject(out *StoreObjectState, obj any) {
	switch o := obj.(type) {
	case *bytesObject:
		// O(1) metadata off the embedded ObjectMeta and ONE pass over the
		// stored bytes for the hash — no decode of the object tree.
		out.Representation = "bytesObject"
		sum := sha256.Sum256(o.raw)
		out.BodySHA256 = hex.EncodeToString(sum[:])
	case *unstructured.Unstructured:
		out.Representation = "unstructured"
		out.BodySHA256 = marshalSHA256(o)
	case *metav1.PartialObjectMetadata:
		out.Representation = "partialObjectMetadata"
		out.BodySHA256 = marshalSHA256(o)
	default:
		out.Representation = fmt.Sprintf("%T", obj)
		if ro, ok := obj.(runtime.Object); ok {
			out.BodySHA256 = marshalSHA256(ro)
		}
	}
	acc, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	out.ResourceVersion = acc.GetResourceVersion()
	out.UID = string(acc.GetUID())
	out.Generation = acc.GetGeneration()
	if ct := acc.GetCreationTimestamp(); !ct.IsZero() {
		out.CreationTimestamp = ct.UTC().Format(time.RFC3339)
	}
}

// marshalSHA256 hashes an object's JSON encoding. The bytes never leave this
// function — only the hex digest does.
func marshalSHA256(obj any) string {
	raw, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
