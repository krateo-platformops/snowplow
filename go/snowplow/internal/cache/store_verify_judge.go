// store_verify_judge.go — #872, #875, #878, #918: THE lost-update rule, applied
// to every candidate at every comparison site, by ordering resourceVersions.
//
// # WHAT A LOST UPDATE IS
//
// The store missed a change: the apiserver's read shows a state the store
// never received, and the store has applied everything up to that read. Every
// candidate is held to ONE read — the forced LIST's own revisions, or the
// snapshot a relist hands the reflector — and the store's view is ordered
// against it:
//
//   - the store holds the read's state                       -> agrees;
//   - the store holds a NEWER state (or deleted the object
//     after the read, or created it after the read)          -> not lost: the
//     store is ahead of the read (#872, #918);
//   - the store has RECEIVED the read's state or a newer
//     one, and it is still queued for the store              -> not lost: it
//     is behind only by its pending apply (#878);
//   - otherwise the store is behind the read. If its APPLIED
//     position (Store.LastStoreSyncResourceVersion) has
//     reached the read, every delivery up to the read has
//     been applied and this one is not among them           -> LOST;
//     if not, the watch has not caught up with the read      -> PENDING: the
//     key is held and re-judged when the store's position moves
//     (store_verify_delivery.go "PENDING KEYS").
//
// Nothing is timed and nothing depends on how long a pass takes: each verdict
// compares versions the store and the read carry. #875 re-read each candidate
// with a quorum GET at the metadata client's 5 reads/s and compared after the
// whole sweep, which in bench run 16 took 60-76 s against objects changing
// every 20 s, so the store was ahead of most reads and 264 "lost updates"
// repaired the 50K composition store (#918). With ordering the LIST is a read
// the store can be ordered against directly, and the re-read is gone.
//
// # WHY RESOURCE VERSIONS MAY BE ORDERED (and when they may not)
//
// The API contract now allows it. kubernetes.io/docs/reference/using-api/
// api-concepts/#resource-versions: "Resource version strings are orderable as
// monotonically increasing integers within the same resource type for all
// types served by kube-apiserver. This includes built-in API types and types
// backed by custom resource definitions", and "Starting with Kubernetes 1.35,
// orderability of resource versions for all Kubernetes types is included in
// Certified Kubernetes requirements" (KEP-5504, comparable resource version,
// stable in v1.35). For an extension (aggregated) API server the same page
// says the client "needs to check whether the resource version string parses
// as a decimal number … If either of two resource version strings does not
// parse as a decimal number, the two strings can be checked for equality but
// you cannot rely on comparisons for ordering". There is no discovery or
// capability signal; that check is the contract.
//
// So ONE generic rule, with no resource named: every ordering goes through
// apimachinery's resourceversion.CompareResourceVersion, and a pair it refuses
// (InvalidResourceVersion: empty, leading zero, non-digit, "0") leaves the key
// UNDECIDED — not published, counted as store_divergence_undecided_total.
// Equality is still decided: a store that holds exactly the read's state
// agrees whatever its versions look like.
//
// # THE ORDER OF READS
//
// The store's applied position is read FIRST, then the record, then the store
// itself. Everything read after the position is at least as new as it, so a
// position that has reached the read means every delivery up to the read is
// in the store by the time the store is read.
package cache

import (
	"math"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	clientcache "k8s.io/client-go/tools/cache"
)

// orderRV orders two resourceVersions of the same resource: -1, 0, +1. An
// error means the pair is not comparable (see the file header).
func orderRV(a, b string) (int, error) {
	return resourceversion.CompareResourceVersion(a, b)
}

// readState is the read a candidate is held to: whether the object exists in
// it, its {rv, uid} if so, and the read's revision — the object's
// resourceVersion when it exists, the LIST's resourceVersion when it does not.
type readState struct {
	present bool
	id      objIdentity
	rev     string
}

// readOf is key's read in a LIST (upstream) whose resourceVersion is listRV.
func readOf(upstream map[string]objIdentity, key, listRV string) readState {
	if id, ok := upstream[key]; ok {
		return readState{present: true, id: id, rev: id.rv}
	}
	return readState{rev: listRV}
}

// positionView is the store's applied position for one judgement. always is
// the snapshot path's: the reflector is blocked inside the read, the read IS
// the next state of the store, and a store behind it is a divergence by
// definition.
type positionView struct {
	applied string
	always  bool
}

func positionOf(applied string) positionView { return positionView{applied: applied} }

type outcome int

const (
	outcomeAgree outcome = iota
	outcomeStoreAhead
	outcomeReceived
	outcomeLost
	outcomePending
	outcomeUndecided
)

// verdict is one key's judgement. class is set for outcomeLost; reverify for
// an undecided key a later pass can decide (a relist tombstone), so its GVR
// goes to the front of the walker.
type verdict struct {
	outcome  outcome
	class    divergenceClass
	reverify bool
}

// judge carries what one sweep of judgements reads: the store, its records
// (nil when the GVR has none), the position read before them, and, on the
// snapshot path, the transform counter when the read began — a delivery
// handed on after that is the snapshot itself, not something the store had
// received before it.
type judge struct {
	idx          clientcache.Indexer
	log          *deliveryLog
	pos          positionView
	receivedMark uint64
}

func undecided() verdict { return verdict{outcome: outcomeUndecided} }

// judgeKey applies the rule (file header) to one key.
func judgeKey(idx clientcache.Indexer, log *deliveryLog, pos positionView, key string, read readState) verdict {
	return judge{idx: idx, log: log, pos: pos, receivedMark: math.MaxUint64}.key(key, read)
}

func (j judge) key(key string, read readState) verdict {
	// The record before the store (THE ORDER OF READS).
	var rec received
	var hasRec bool
	if j.log != nil {
		rec, hasRec = j.log.receivedFor(key)
		hasRec = hasRec && rec.seq <= j.receivedMark
	}
	obj, exists, err := j.idx.GetByKey(key)
	if err != nil || (exists && obj == nil) {
		return undecided()
	}
	var st objIdentity
	if exists {
		var ok bool
		if _, st, ok = identityOf(obj); !ok {
			return undecided()
		}
	}

	// The store holds the read's state, or a newer one.
	switch {
	case read.present && exists:
		if st == read.id {
			return verdict{outcome: outcomeAgree}
		}
		c, err := orderRV(st.rv, read.rev)
		if err != nil {
			return undecided()
		}
		if c >= 0 {
			// Newer (recreated after the read when the uid differs too);
			// equal versions are the same state.
			return verdict{outcome: outcomeStoreAhead}
		}
	case !read.present && !exists:
		return verdict{outcome: outcomeAgree}
	case !read.present && exists:
		c, err := orderRV(st.rv, read.rev)
		if err != nil {
			return undecided()
		}
		if c > 0 {
			return verdict{outcome: outcomeStoreAhead} // created after the read
		}
	}

	// The store has received the read's state or a newer one, still queued.
	if hasRec {
		queued := true
		if j.pos.applied != "" {
			c, err := orderRV(rec.id.rv, j.pos.applied)
			if err != nil {
				return undecided()
			}
			// Applied (or dropped by the queue) once the position has passed
			// it: then the store, read above, is the truth.
			queued = c > 0
		}
		if queued {
			c, err := orderRV(rec.id.rv, read.rev)
			if err != nil {
				return undecided()
			}
			if c >= 0 {
				return verdict{outcome: outcomeReceived}
			}
		}
	}

	// The store deleted the object after the read.
	if read.present && !exists && j.log != nil {
		if d, ok := j.log.deleteFor(key); ok {
			if d.tombstone {
				// A relist removed it and says nothing of when; the next
				// pass reads a store that relist already settled.
				return verdict{outcome: outcomeUndecided, reverify: true}
			}
			c, err := orderRV(d.rv, read.rev)
			if err != nil {
				return undecided()
			}
			if c > 0 {
				return verdict{outcome: outcomeStoreAhead}
			}
		}
	}

	// Behind the read. Lost only if the store has applied everything up to it.
	if !j.pos.always {
		if j.pos.applied == "" {
			return verdict{outcome: outcomePending}
		}
		c, err := orderRV(j.pos.applied, read.rev)
		if err != nil {
			return undecided()
		}
		if c < 0 {
			return verdict{outcome: outcomePending}
		}
	}
	switch {
	case !read.present:
		return verdict{outcome: outcomeLost, class: classLostDelete}
	case !exists:
		return verdict{outcome: outcomeLost, class: classLostAdd}
	case st.uid != "" && read.id.uid != "" && st.uid != read.id.uid:
		return verdict{outcome: outcomeLost, class: classUIDMismatch}
	}
	return verdict{outcome: outcomeLost, class: classLostUpdate}
}

// sweepTally is what a sweep of judgements decided, for the counters.
type sweepTally struct {
	lost                          []divergenceCandidate
	pending                       map[string]readState
	cleared, ahead, received, und uint64
	reverify                      bool
}

// sweep judges candidates against their reads in upstream (a LIST at listRV).
func (j judge) sweep(upstream map[string]objIdentity, listRV string, candidates []divergenceCandidate) sweepTally {
	var t sweepTally
	for _, c := range candidates {
		read := readOf(upstream, c.key, listRV)
		v := j.key(c.key, read)
		switch v.outcome {
		case outcomeAgree:
			t.cleared++
		case outcomeStoreAhead:
			t.cleared++
			t.ahead++
		case outcomeReceived:
			t.cleared++
			t.received++
		case outcomeLost:
			t.lost = append(t.lost, divergenceCandidate{c.key, v.class})
		case outcomePending:
			if t.pending == nil {
				t.pending = map[string]readState{}
			}
			t.pending[c.key] = read
		case outcomeUndecided:
			t.und++
			t.reverify = t.reverify || v.reverify
		}
	}
	storeClearedTotal.Add(t.cleared)
	storeAheadTotal.Add(t.ahead)
	storeReceivedTotal.Add(t.received)
	storeUndecidedTotal.Add(t.und)
	return t
}

// judgeForcedPass judges a forced pass's candidates. log may be nil (a GVR
// whose transform could not be installed): then nothing can re-judge a
// pending key but the next pass, so a pending key is undecided and its GVR
// goes to the front of the walker. Returns the report, the keys left pending
// (held by log), and whether the GVR should be verified again next.
func judgeForcedPass(gvr schema.GroupVersionResource, idx clientcache.Indexer, log *deliveryLog,
	upstream map[string]objIdentity, listRV string, candidates []divergenceCandidate) (divergenceReport, bool) {

	// The position first (THE ORDER OF READS).
	pos := positionOf(idx.LastStoreSyncResourceVersion())
	t := judge{idx: idx, log: log, pos: pos, receivedMark: math.MaxUint64}.sweep(upstream, listRV, candidates)
	reverify := t.reverify
	if len(t.pending) > 0 {
		if log != nil {
			log.setPending(t.pending)
		} else {
			storeUndecidedTotal.Add(uint64(len(t.pending)))
		}
		// A bookmark advances the position with no callback
		// (store_verify_delivery.go), so the next pass must not wait for
		// the deadline.
		reverify = true
	}
	if log == nil {
		storeUnrecordedTotal.Add(uint64(len(t.lost)))
	}
	return reportOf(len(upstream), len(candidates), t.lost), reverify
}

// publishResolvedLoss publishes losses a pending key resolved to after its
// pass ended, and queues the repair.
func publishResolvedLoss(gvr schema.GroupVersionResource, lost []divergenceCandidate) {
	rep := reportOf(0, len(lost), lost)
	recordDivergence(gvr, verifySiteForced, rep)
	enqueueStoreRepair(gvr, "store_divergence")
}

var (
	storeClearedTotal                 atomic.Uint64
	storeAheadTotal                   atomic.Uint64
	storeReceivedTotal                atomic.Uint64
	storeUndecidedTotal               atomic.Uint64
	storePendingResolvedLostTotal     atomic.Uint64
	storePendingResolvedReceivedTotal atomic.Uint64
	storePendingSupersededTotal       atomic.Uint64
	storeUnrecordedTotal              atomic.Uint64
)
