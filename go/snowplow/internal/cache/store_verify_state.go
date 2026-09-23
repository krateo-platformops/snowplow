// store_verify_state.go — snowplow#237 deliverable B: the per-GVR bookkeeping
// that says WHEN each GVR was last verified against an authoritative object
// set, and whether it is even reachable by the verifier.
//
// # WHY THIS IS ITS OWN FILE
//
// Gate condition S-1. Deliverable A's per-object store accessor lives in
// store_state.go for the same reason: the two deliverables must be revertible
// independently, so neither may own state the other needs. This file is the
// state; store_verify.go is the snapshot-time comparison; store_verify_deadline.go
// is the deadline walker; store_repair.go is the repair queue. Reverting any
// one of the three leaves this file compiling on its own.
//
// # THE ONE FIELD THAT CARRIES THREE MEANINGS, AND WHY IT IS ONE FIELD
//
// `epoch` is the instant from which a GVR counts as UNVERIFIED. It is set at
// three moments and read as one number:
//
//  1. REGISTRATION. Gate condition B-6. If a never-verified GVR were left at
//     the zero time and the age computed as now-zero, the gauge would read
//     ~1.8e9 seconds — absurd but loud, and therefore safe. The natural
//     implementation — exclude never-verified GVRs from the max — is the
//     dangerous one: the gauge then reads 0 at boot and through any window
//     where verification has never run at all. A zero that reads as perfect
//     health while nothing has been verified is the exact defect class this
//     whole ship exists to delete. So registration seeds the epoch and
//     store_verification_max_age_seconds reads age-since-registration from the
//     first scrape, never 0.
//
//  2. INVALIDATION on a confirmation retraction (§2.4). While a GVR is
//     retracted the store is not being served from, so a comparison against it
//     would be meaningless — but the verification it had is no longer current
//     either. Resetting the epoch to NOW (rather than to the zero time) is what
//     keeps meaning 1 intact: an invalidated GVR reports "unverified for
//     however long it has been retracted", not "unverified since the process
//     started", which would be a false alarm on a pod that has retracted 10,011
//     times in 18 h (the measured krateo-057 figure).
//
//  3. A SUCCESSFUL VERIFICATION. The ordinary case.
//
// One field, three writes, and `age = now - epoch` is correct after every one
// of them. Do not split it back into lastVerified + a never-verified sentinel:
// that shape is what B-6 was written to prevent.
//
// # LOCK ORDER
//
// storeVerify.mu is taken INSIDE rw.mu at the lifecycle sites
// (rememberStoreVerification from the two registration sites,
// forgetStoreVerification from deletePerGVRStateLocked, invalidateStoreVerification
// from the discovery-refresh retraction) and ALONE everywhere else. NOTHING
// takes rw.mu while holding storeVerify.mu — the gauge snapshot deliberately
// gathers the watcher's facts first, releases rw.mu, and only then takes this
// mutex. Same discipline, and for the same reason, as reflector_path.go: a
// sync.RWMutex is not reentrant and an instrument must never be the thing that
// turns a future refactor into a hung pod.
package cache

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// gvrVerification is one GVR's verification bookkeeping.
type gvrVerification struct {
	// epoch — see the file header. Never the zero time for a registered GVR.
	epoch time.Time
	// verified is true once a verification has completed since the current
	// epoch was set. It drives the "never" reading on /debug/servable's row,
	// which is a different question from the age the gauge reports.
	verified bool

	// decorated records whether this GVR's informer took the decorated
	// ListerWatcher path. Gate condition S-2: coverage hangs on
	// compositionStreamingListEnabled(), an env toggle defaulting true, and if
	// it is ever set false the routing falls to an undecorated informer,
	// snapshot detection silently drops to zero, and every store_divergent_*
	// counter reads 0 — which reads as health. This bool is what makes that
	// state say "undecorated" out loud instead.
	decorated bool

	// lastObjects is the object count of the last completed verification —
	// the R4 coverage assertion's own number (Δ objects_verified == IndexerCount
	// means the deadline chose WHEN, never WHAT).
	lastObjects int
	// divergentSinceBoot is the per-GVR sum of all four classes, for the
	// /debug/servable row. Never reset by a repair: it is a "has this GVR ever
	// been wrong" flag an operator reads after the fact.
	divergentSinceBoot uint64

	// repairPending is true from enqueue until the repair completes — the
	// dedupe half of RG-2 (bound 1 forbids concurrent relists OF THE SAME GVR;
	// it does not by itself forbid a second queue entry for one already
	// waiting, and a GVR re-detected at the next deadline while still pending
	// would otherwise enqueue twice).
	repairPending bool
	// repairedAwaitingVerify is true from a completed repair until the next
	// verification of that GVR judges it. That next verification is the
	// circuit breaker's input: still divergent means the relist did not fix it,
	// which does not mean a lost event — it means our comparison is wrong, or
	// our view and the apiserver's differ for a reason a relist cannot fix.
	repairedAwaitingVerify bool
	// suppressed latches the breaker. Detection CONTINUES under suppression
	// (bound 4): silence must never be produced by the breaker.
	suppressed bool
}

// storeVerify is the process-wide registry. A plain map under one mutex, not a
// sync.Map: it is written at informer registration and teardown (a handful per
// GVR per lifetime) and at verification completion (at most one per GVR per
// maxVerificationAge), and read by a JWT-gated scrape and the walker.
var storeVerify = struct {
	mu   sync.Mutex
	gvrs map[schema.GroupVersionResource]*gvrVerification
}{
	gvrs: map[schema.GroupVersionResource]*gvrVerification{},
}

// rememberStoreVerification seeds a GVR's bookkeeping at registration.
// Called from both informer-registration sites, next to
// rememberReflectorPathGVR, which holds rw.mu — see the lock-order note.
//
// decorated says whether this GVR's informer got the verifying ListerWatcher.
// It is recorded at registration rather than inferred later because the
// routing decision is made once, in addResourceTypeLocked, and is not
// re-derivable from the informer handle afterwards.
func rememberStoreVerification(gvr schema.GroupVersionResource, decorated bool) {
	storeVerify.mu.Lock()
	storeVerify.gvrs[gvr] = &gvrVerification{epoch: time.Now(), decorated: decorated}
	storeVerify.mu.Unlock()
	if !decorated {
		// Gate condition B-7. An undecorated informer is one the snapshot
		// comparison will NEVER see, for its whole lifetime, so the
		// registration itself is the event worth counting — there is no later
		// moment at which a snapshot "fails" to verify it, because no snapshot
		// ever reaches it.
		//
		// Healthy this reads the typed-RBAC count, not 0: those four GVRs take
		// the stock factory informer by design, and client-go builds their
		// ListWatch where snowplow cannot decorate it. What matters is the
		// JUMP — under the coverage cliff (RESOLVER_COMPOSITION_STREAMING_LIST
		// off) this goes to nearly every GVR while every
		// store_divergent_*_snapshot_total silently drops to 0, and a zero
		// divergence count must never be reachable without something else
		// saying why.
		recordVerifySkipped(verifySkipUndecorated)
	}
}

// forgetStoreVerification drops every trace of gvr. Called from
// deletePerGVRStateLocked — the single de-registration site — so this per-GVR
// map cannot be the one that gets forgotten. Leaving it out would be #219's
// leak shape verbatim: pruneUnservedGVRs retires a composition version on every
// CRD upgrade, and the registry would accumulate a permanent row per retired
// version, each one past its deadline forever, driving store_gvrs_unverified
// up with GVRs that no longer exist.
func forgetStoreVerification(gvr schema.GroupVersionResource) {
	storeVerify.mu.Lock()
	delete(storeVerify.gvrs, gvr)
	storeVerify.mu.Unlock()
}

// invalidateStoreVerification resets a GVR's epoch after its servability
// confirmation was retracted (§2.4). A retraction does not change the store; it
// makes the GVR non-servable so resolves fall through to the apiserver. So it
// is not a divergence event — but it IS a verification-VALIDITY event, and the
// deadline must re-verify the GVR as soon as it is confirmed again.
//
// Counts store_verification_invalidated_total. That counter climbs in both
// regimes and is explicitly NOT a detector: it is the context that explains why
// a GVR's age reset without a verification having run.
func invalidateStoreVerification(gvr schema.GroupVersionResource) {
	storeVerify.mu.Lock()
	if st, ok := storeVerify.gvrs[gvr]; ok {
		st.epoch = time.Now()
		st.verified = false
		storeVerifyInvalidatedTotal.Add(1)
	}
	storeVerify.mu.Unlock()
}

// noteVerified records a completed verification: the epoch advances, the object
// count is kept for the R4 coverage assertion, and the breaker reads its verdict.
//
// The breaker's decision lives here because this is the only place that sees
// BOTH "a repair completed and we were waiting to judge it" and "this pass's
// divergence count". Returns true when the caller should latch suppression —
// the caller logs it, because this function holds the registry mutex and must
// not do I/O under it.
func noteVerified(gvr schema.GroupVersionResource, objects int, divergences uint64) (latched bool) {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	st, ok := storeVerify.gvrs[gvr]
	if !ok {
		// Torn down between the snapshot and here. The caller counts this as
		// a skip; there is nothing left to record against.
		return false
	}
	st.epoch = time.Now()
	st.verified = true
	st.lastObjects = objects
	st.divergentSinceBoot += divergences

	if !st.repairedAwaitingVerify {
		return false
	}
	// A repair fired for this GVR and this is the verification that judges it.
	st.repairedAwaitingVerify = false
	if divergences == 0 {
		return false
	}
	// Still divergent AFTER a full relist. Hammering a 50K GVR on this basis
	// is self-harm, so latch and raise. store_repair_ineffective_total is the
	// only counter in this family whose non-zero means OUR detector is wrong
	// rather than the store.
	storeRepairIneffectiveTotal.Add(1)
	if !st.suppressed {
		st.suppressed = true
		storeRepairSuppressedTotal.Add(1)
		return true
	}
	return false
}

// verificationDecorated reports whether gvr's informer got the decorator. Used
// by the walker to attribute an unverifiable GVR to the S-2 coverage cliff.
func verificationDecorated(gvr schema.GroupVersionResource) (decorated, known bool) {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	st, ok := storeVerify.gvrs[gvr]
	if !ok {
		return false, false
	}
	return st.decorated, true
}

// verificationRow is one GVR's bookkeeping, copied out for a reader.
type verificationRow struct {
	epoch                  time.Time
	verified               bool
	decorated              bool
	lastObjects            int
	divergentSinceBoot     uint64
	repairPending          bool
	repairedAwaitingVerify bool
	suppressed             bool
}

// verificationRows copies the whole registry. Callers MUST NOT hold rw.mu.
func verificationRows() map[schema.GroupVersionResource]verificationRow {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	out := make(map[schema.GroupVersionResource]verificationRow, len(storeVerify.gvrs))
	for gvr, st := range storeVerify.gvrs {
		out[gvr] = verificationRow{
			epoch:                  st.epoch,
			verified:               st.verified,
			decorated:              st.decorated,
			lastObjects:            st.lastObjects,
			divergentSinceBoot:     st.divergentSinceBoot,
			repairPending:          st.repairPending,
			repairedAwaitingVerify: st.repairedAwaitingVerify,
			suppressed:             st.suppressed,
		}
	}
	return out
}

// verificationRowFor copies one GVR's bookkeeping, for /debug/servable's row.
func verificationRowFor(gvr schema.GroupVersionResource) (verificationRow, bool) {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	st, ok := storeVerify.gvrs[gvr]
	if !ok {
		return verificationRow{}, false
	}
	return verificationRow{
		epoch:                  st.epoch,
		verified:               st.verified,
		decorated:              st.decorated,
		lastObjects:            st.lastObjects,
		divergentSinceBoot:     st.divergentSinceBoot,
		repairPending:          st.repairPending,
		repairedAwaitingVerify: st.repairedAwaitingVerify,
		suppressed:             st.suppressed,
	}, true
}

// maxVerificationAge is the deadline: every object of every servable GVR has
// been compared against an authoritative set within this long.
//
// DERIVED, NOT DECLARED (feedback_self_adapt_no_magic_env_knobs). The anchor is
// ResolvedCacheTTL() — the interval after which any non-rank-1 L1 cell
// re-resolves from the store anyway. A store error that persists longer than
// one TTL is, by construction, an error a user can be served TWICE; verifying
// at exactly one TTL therefore makes "a user can be served a stale object
// twice" the detection deadline. There is no separate knob and there must not
// become one.
//
// The rate this implies is NOT bounded by this value alone — see
// store_verify_deadline.go, where the walker is strictly serial so the forced-LIST
// rate is bounded by the apiserver's own service time rather than by any
// constant we invent. That is deliberate: a deployment with a short
// RESOLVED_CACHE_TTL_SECONDS must not be able to turn this deadline into a
// LIST flood, and a floor constant would have been exactly the magic number the
// rule forbids.
func maxVerificationAge() time.Duration { return ResolvedCacheTTL() }

// resetStoreVerificationStateForTest clears the registry.
func resetStoreVerificationStateForTest() {
	storeVerify.mu.Lock()
	storeVerify.gvrs = map[schema.GroupVersionResource]*gvrVerification{}
	storeVerify.mu.Unlock()
}
