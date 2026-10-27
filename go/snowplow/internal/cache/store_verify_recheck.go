// store_verify_recheck.go — #872, #875, #918: how the forced pass judges its
// candidates while the store keeps moving.
//
// # WHY THE FORCED PASS NEEDS THIS AND THE SNAPSHOT PATH DOES NOT
//
// On the snapshot path the reflector is blocked inside our call, so the store
// cannot move while we compare. The forced pass has no such guarantee: it
// LISTs, and only when the LIST returns does it compare against the LIVE
// indexer, which kept applying watch events the whole time. An object updated
// after the LIST's snapshot is NEWER in the store than in the LIST, and
// diffStore's `st.rv != up.rv` — an unordered inequality, because
// resourceVersions are opaque — reports it. Under churn every forced pass
// therefore "found" lost updates the store never lost, and each one queued a
// repair: a full relist that makes the GVR unservable until the replacement
// syncs (#870). Measured in kind (#872): 100% of the reported keys had the
// store ahead of the LIST, 0 were really lost, and the count rose with the
// churn rate. Create/expire churn produces the same thing in the other classes
// (deleted after the LIST reads as a lost ADD, created after it as a lost
// DELETE).
//
// # THE RULE (#918)
//
// Every candidate is held to ONE read of the object — the forced LIST, or a
// quorum re-read — and counts only under the lost-update rule that
// confirmDivergences applies at every comparison site (store_verify.go):
// nothing was delivered for its key after that read began, and the store,
// counting what it has received, still disagrees with that read. The delivery
// record (store_verify_delivery.go) numbers every delivery and stays open
// until the last candidate is judged, so "after the read began" is a
// comparison of two numbers, never of two resourceVersions, and never of
// durations.
//
// What #875 did instead, and #918 measured, is why the rule has to be this one:
// it read every re-checked candidate first and compared the store with those
// reads only after the whole sweep. In bench run 16 the sweep read 379
// candidates at the metadata client's 5 reads/s and took 60-76 s, while the
// churned objects changed every ~20 s. Most had changed — and been delivered —
// after their read, so the store was ahead of the read, and 264 "lost
// updates" queued a full repair of the 50K composition store. Reading twice
// did not help: the object moved after the second read too. The verdict
// depended on how long the sweep took; under the rule it cannot.
//
// # WHICH CANDIDATES ARE RE-READ (#875)
//
// Only a candidate the store may have MOVED since the pass opened: its key was
// delivered while the pass was open, or, for a lost ADD, it was in the store
// when the pass opened and is gone now (a relist tombstone, which skips the
// transform). Such a candidate is no longer comparable with the LIST — the
// store moved after the LIST began, so under the rule the LIST cannot convict
// it — and it is read again, so that a real loss among the keys that churned
// is still found in this pass rather than the next. Every other candidate is
// held to the LIST, with no GET: nothing reached the store for it since the
// pass opened, so the LIST, no older than the store was then, is a read it can
// be held to. It is judged with the others at the end, so a delivery for it
// during the re-check settles it too.
//
// Re-reading every candidate instead was #875's FAIL: a wholly divergent store
// (#237's broken-store class) makes every stored object a candidate, so the
// re-check issued quorum GETs per object before the one repair (~100K at
// 50K), and any one of them failing made the pass inconclusive, so the store
// most in need of a repair could be kept from ever getting one.
//
// # THE RE-READ
//
// One quorum metadata GET per re-read candidate (resourceVersion unset: the
// apiserver serves the current state from etcd, not the watch cache), with
// the delivery counter marked just before it is issued. The read's own instant
// lies somewhere inside the request, so a delivery during the request counts
// as after the read. That is the direction that never repairs a store that
// lost nothing; the cost, a real loss whose state reached etcd inside one
// request while another delivery for the same key also landed inside it, is
// found by the next pass — a real divergence does not go away by waiting.
//
// There is no second read. #875 read a still-divergent candidate again to
// catch an object that moved between the read and the comparison; a move that
// reaches the store is now a delivery after the read, which settles the key by
// itself, and one that does not reach the store is a loss either read shows.
// A second read only bought the drain more time, and was a retry bound.
//
// The class is re-derived from the fresh read rather than carried over: a
// candidate the LIST called a lost UPDATE may be a lost DELETE now (it was
// deleted since), and it must be counted, if at all, as what it is now.
//
// # WHAT THE RULE CANNOT SEE, STATED
//
// A read whose state has not reached this process's reflector at all when the
// pass decides — the watch stream lagging the read by more than the drain
// wait, with no other delivery for the key since the read — looks exactly like
// a loss. Telling the two apart needs knowing whether the watch has passed the
// read's resourceVersion, which means ordering resourceVersions, which the API
// forbids. Every candidate read before the last ones has the rest of the sweep
// for its delivery to arrive; the last ones have the drain wait, as the
// snapshot path's candidates always did.
//
// # A FAILED RE-READ MAKES THE PASS INCONCLUSIVE
//
// If any re-read fails (other than NotFound, which is an answer), the pass
// publishes nothing, queues no repair and does NOT mark the GVR verified, so
// its epoch stays old and the deadline walker picks it up again. It is counted
// as a skip under its own reason, so "could not finish" is never spelled like
// "found nothing". Neither alternative is safer:
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
	storeRecheckGETsTotal              atomic.Uint64
	storeRecheckClearedTotal           atomic.Uint64
	storeRecheckUndeliveredTotal       atomic.Uint64
	storeRecheckUnavailableTotal       atomic.Uint64
	storeRecheckDeliveredAfterTotal    atomic.Uint64
	storeRecheckReceivedReadStateTotal atomic.Uint64
)

// judgeForcedPass returns the forced pass's report: candidates (diffStore's,
// against the LIST upstream) judged under the lost-update rule, each held to
// the read described in the file header. ok=false means a re-read failed and
// the pass is inconclusive. win must stay open until this returns.
func judgeForcedPass(ctx context.Context, cli metadata.Interface, gvr schema.GroupVersionResource,
	idx clientcache.Indexer, upstream map[string]objIdentity, candidates []divergenceCandidate,
	win *deliveryWindow) (divergenceReport, bool) {

	// reads[key] is what the read key is held to says (absent: no object);
	// marks[key] is the delivery counter when that read began.
	reads := make(map[string]objIdentity, len(candidates))
	marks := make(map[string]uint64, len(candidates))
	reread := map[string]bool{}
	var judged []divergenceCandidate
	var rereadKeys []string
	for _, c := range candidates {
		if win.moved(c.key, c.class) {
			reread[c.key] = true
			rereadKeys = append(rereadKeys, c.key)
			continue
		}
		if up, ok := upstream[c.key]; ok {
			reads[c.key] = up
		}
		marks[c.key] = win.openedAt
		judged = append(judged, c)
	}

	for _, key := range rereadKeys {
		mark := win.mark()
		id, found, err := currentIdentity(ctx, cli, gvr, key)
		if err != nil {
			return divergenceReport{}, false
		}
		marks[key] = mark
		if found {
			reads[key] = id
		}
	}
	for _, key := range rereadKeys {
		if c, divergent := classifyKey(idx, reads, key); divergent {
			judged = append(judged, divergenceCandidate{key, c})
		}
	}

	settledBy := func(key string) (after, received bool) {
		d, ok := win.latestFor(key)
		if !ok {
			return false, false
		}
		if d.seq > marks[key] {
			return true, false
		}
		read, found := reads[key]
		return false, found && d.id == read
	}
	confirmed := confirmDivergences(idx, reads, judged, func(key string) bool {
		after, received := settledBy(key)
		return after || received
	})

	counted := map[string]bool{}
	for _, c := range confirmed {
		counted[c.key] = true
		if !reread[c.key] {
			storeRecheckUndeliveredTotal.Add(1)
		}
	}
	for _, c := range candidates {
		if counted[c.key] {
			continue
		}
		storeRecheckClearedTotal.Add(1)
		switch after, received := settledBy(c.key); {
		case after:
			storeRecheckDeliveredAfterTotal.Add(1)
		case received:
			storeRecheckReceivedReadStateTotal.Add(1)
		}
	}
	return reportOf(len(upstream), len(candidates), confirmed), true
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

// classifyKey applies diffStore's predicate to ONE key against the given
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
