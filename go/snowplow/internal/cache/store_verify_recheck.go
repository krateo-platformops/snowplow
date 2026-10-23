// store_verify_recheck.go — #872: the forced pass re-checks every confirmed
// divergence against the object's CURRENT state before it counts.
//
// # WHY THE FORCED PASS NEEDS THIS AND THE SNAPSHOT PATH DOES NOT
//
// On the snapshot path the reflector is blocked inside our call, so the store
// cannot move while we compare. The forced pass has no such guarantee: it
// LISTs, and only when the LIST returns does it compare against the LIVE
// indexer, which kept applying watch events the whole time. An object updated
// after the LIST's snapshot is NEWER in the store than in the LIST, and
// compareStore's `st.rv != up.rv` — an unordered inequality, because
// resourceVersions are opaque — counts it as a lost UPDATE. The confirm loop
// cannot clear it either: it waits for the store to EQUAL the LIST, which a
// store that is ahead never does. Under churn every forced pass therefore
// "found" lost updates the store never lost, and each one queued a repair: a
// full relist that makes the GVR unservable until the replacement syncs (#870).
// Measured in kind (#872): 100% of the reported keys had the store ahead of
// the LIST, 0 were really lost, and the count rose with the churn rate.
// Create/expire churn produces the same thing in the other classes (deleted
// after the LIST reads as a lost ADD, created after it as a lost DELETE).
//
// # WHICH CANDIDATES ARE RE-CHECKED (#875)
//
// Only a candidate the store may have MOVED during the pass: its key was
// delivered by the reflector while the pass was open, or, for a lost ADD, it
// was in the store when the pass opened and is gone now. Every other candidate
// is counted as it stands, with no GET: nothing reached the store for it, so it
// cannot be ahead of a LIST no older than the store was at open, and its
// disagreement with that LIST is a real one. Why "delivered" is exactly the
// set that can be ahead, and how it is recorded, is store_verify_delivery.go.
//
// Re-checking every candidate instead was #875's FAIL: a wholly divergent store
// (#237's broken-store class) makes every stored object a candidate, so the
// re-check issued two quorum GETs per object before the one repair (400 for 200
// objects; ~100K at 50K), and any one of them failing made the pass
// inconclusive, so the store most in need of a repair could be kept from ever
// getting one.
//
// # THE RE-CHECK
//
// Each such candidate that survived the confirm loop is read again, ONE object,
// as a quorum metadata GET (resourceVersion unset: the apiserver serves the
// current state from etcd, not the watch cache). That read is the object's
// current truth, and the candidate counts only if the store still disagrees
// with it after the same bounded drain wait the confirm loop already uses — the
// store is allowed to lag the quorum read by the events in flight, exactly as
// it is allowed to lag the LIST. No resourceVersion is ever ordered: equality
// with a fresh read is the whole test, so the API contract (RVs are opaque)
// holds.
//
// A candidate still divergent is read ONCE more. If the object moved between
// the two reads, the second read is the one the store is held to; either way
// what remains divergent after the second read counts. That bounds the GETs at
// two per re-checked candidate, and the re-checked candidates at the keys
// delivered during the pass: the churn in that window, never the GVR's object
// count.
//
// The class is re-derived from the fresh read rather than carried over: a
// candidate the LIST called a lost UPDATE may be a lost DELETE now (it was
// deleted since), and it must be counted, if at all, as what it is now.
//
// # A FAILED RE-CHECK MAKES THE PASS INCONCLUSIVE
//
// If any re-check read fails (other than NotFound, which is an answer), the
// pass publishes nothing, queues no repair and does NOT mark the GVR verified,
// so its epoch stays old and the deadline walker picks it up again. It is
// counted as a skip under its own reason, so "could not finish" is never
// spelled like "found nothing". Neither alternative is safer:
//   - counting the unverified candidates as lost turns a transient apiserver
//     error into a repair — the very false repair (and #870 window) this file
//     removes, now triggered by an error instead of by churn;
//   - dropping them and marking the GVR verified would hide a real divergence
//     for a whole maxVerificationAge behind a pass that did not finish.
//
// A real divergence does not go away by waiting, so the next pass finds it;
// the cost is at most one pass interval, and it is visible meanwhile as the
// GVR's verification age rising.
package cache

import (
	"context"
	"strings"
	"sync/atomic"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/metadata"
	clientcache "k8s.io/client-go/tools/cache"
)

var (
	storeRecheckGETsTotal        atomic.Uint64
	storeRecheckClearedTotal     atomic.Uint64
	storeRecheckUndeliveredTotal atomic.Uint64
	storeRecheckUnavailableTotal atomic.Uint64
)

// recheckReads is the number of quorum reads per candidate (see the header).
const recheckReads = 2

// recheckAgainstCurrent returns rep with only the divergences that still hold:
// a candidate mayHaveMoved reports false is kept as it stands; every other one
// is kept only if it still holds against the object's current state. ok=false
// means a re-check read failed and the pass is inconclusive (see the header).
func recheckAgainstCurrent(ctx context.Context, cli metadata.Interface, gvr schema.GroupVersionResource,
	idx clientcache.Indexer, rep divergenceReport, mayHaveMoved func(string, divergenceClass) bool) (divergenceReport, bool) {

	out := divergenceReport{objectsCompared: rep.objectsCompared, candidates: rep.candidates}
	var keys []string
	for _, set := range []struct {
		class divergenceClass
		keys  []string
		out   *[]string
	}{
		{classLostUpdate, rep.lostUpdate, &out.lostUpdate},
		{classLostDelete, rep.lostDelete, &out.lostDelete},
		{classLostAdd, rep.lostAdd, &out.lostAdd},
		{classUIDMismatch, rep.uidMismatch, &out.uidMismatch},
	} {
		for _, key := range set.keys {
			if mayHaveMoved(key, set.class) {
				keys = append(keys, key)
				continue
			}
			*set.out = append(*set.out, key)
		}
	}
	// Counted only on the paths that return the report: an inconclusive pass
	// publishes none of these, and the counter says each one was published.
	counted := out.total()
	if len(keys) == 0 {
		storeRecheckUndeliveredTotal.Add(counted)
		return out, true
	}

	current := make(map[string]objIdentity, len(keys))
	pending := keys
	for read := 0; read < recheckReads && len(pending) > 0; read++ {
		for _, key := range pending {
			id, found, err := currentIdentity(ctx, cli, gvr, key)
			if err != nil {
				return divergenceReport{}, false
			}
			if found {
				current[key] = id
			} else {
				delete(current, key)
			}
		}
		var cands []divergenceCandidate
		for _, key := range pending {
			if c, divergent := classifyKey(idx, current, key); divergent {
				cands = append(cands, divergenceCandidate{key, c})
			}
		}
		still := confirmDivergences(idx, current, cands)
		pending = pending[:0:0]
		for _, c := range still {
			pending = append(pending, c.key)
		}
	}

	for _, key := range pending {
		c, divergent := classifyKey(idx, current, key)
		if !divergent {
			continue
		}
		switch c {
		case classLostAdd:
			out.lostAdd = append(out.lostAdd, key)
		case classLostUpdate:
			out.lostUpdate = append(out.lostUpdate, key)
		case classLostDelete:
			out.lostDelete = append(out.lostDelete, key)
		case classUIDMismatch:
			out.uidMismatch = append(out.uidMismatch, key)
		}
	}
	storeRecheckClearedTotal.Add(uint64(len(keys)) - (out.total() - counted))
	storeRecheckUndeliveredTotal.Add(counted)
	return out, true
}

// currentIdentity reads one object's current {rv, uid} with a quorum metadata
// GET. found=false is the apiserver saying the object does not exist.
func currentIdentity(ctx context.Context, cli metadata.Interface, gvr schema.GroupVersionResource, key string) (objIdentity, bool, error) {
	ns, name := "", key
	if i := strings.IndexByte(key, '/'); i >= 0 {
		ns, name = key[:i], key[i+1:]
	}
	storeRecheckGETsTotal.Add(1)
	obj, err := cli.Resource(gvr).Namespace(ns).Get(withForcedVerifyTag(ctx), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return objIdentity{}, false, nil
	}
	if err != nil {
		return objIdentity{}, false, err
	}
	return objIdentity{rv: obj.GetResourceVersion(), uid: string(obj.GetUID())}, true, nil
}

// classifyKey applies compareStore's predicate to ONE key against the given
// upstream view: its class, and whether the store diverges from it at all.
func classifyKey(idx clientcache.Indexer, upstream map[string]objIdentity, key string) (divergenceClass, bool) {
	up, wanted := upstream[key]
	obj, exists, err := idx.GetByKey(key)
	if err != nil {
		return classLostUpdate, true
	}
	if !exists || obj == nil {
		return classLostAdd, wanted
	}
	_, st, ok := identityOf(obj)
	if !ok {
		return classLostUpdate, true
	}
	switch {
	case !wanted:
		return classLostDelete, true
	case st.uid != "" && up.uid != "" && st.uid != up.uid:
		return classUIDMismatch, true
	case st.rv != up.rv:
		return classLostUpdate, true
	}
	return classLostUpdate, false
}
