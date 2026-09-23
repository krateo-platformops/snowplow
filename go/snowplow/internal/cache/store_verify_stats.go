// store_verify_stats.go — snowplow#237 deliverable B's instrument.
//
// # THE RULE THIS FILE IS WRITTEN AGAINST
//
// feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector. Every stat
// below carries BOTH readings in its desc: what it reads DURING the #237 defect
// and what it reads healthy. This investigation has already produced four
// instances of two-regimes-one-number (`divergent`, `watch_errors_total`,
// `confirm_retracted_total`, `dirty_mark_total`), and A3's own classifier nearly
// shipped a fifth. A stat that cannot be non-zero is not an instrument and does
// not belong here — the A3 developer dropped one for exactly that reason.
//
// # THE SCOPE LIMIT THAT MUST BE READ WITH EVERY DIVERGENCE COUNTER
//
// B's oracle is THE SNAPSHOT WE JUST RECEIVED, not the apiserver.
// `store_divergent_*` answers "does our store match the object set the
// apiserver last handed us", NOT "does our store match the apiserver". The two
// differ in a case #237 has on record as one of its two live candidates: if the
// incoming snapshot is itself a stale watch-cache read — the
// "the relist happened and did not fix it" reading — the comparison finds
// equality and every divergence counter reads 0 WITH THE DEFECT ACTIVE.
//
// The forced pass (store_verify_deadline.go) is only PARTLY immune:
// ResourceVersionMatch=NotOlderThan at our own lastSyncRV makes the cacher block
// until it reaches OUR resourceVersion, which is a guarantee about our store's
// position, not about etcd's.
//
// We do not close this. The only oracle that would is a quorum read per
// snapshot, which is precisely the apiserver load this programme exists to
// avoid. It is recorded here, in the counter descriptions, and on #237, so that
// nobody reads a zero as proof.
//
// WHAT THIS DELIVERABLE MAY THEREFORE CLAIM, AND THE QUALIFIER IS NOT OPTIONAL:
// it closes invisibility for every loss class EXCEPT A STALE CACHER. Root cause
// on #237 is still OPEN and the stale-cacher candidate is live, so this work
// must NOT be described as having made #237 detectable — only its other
// candidate classes. That qualifier travels with the claim everywhere it is
// made: PR body, feature journal, release note.
//
// # WHY THE DIVERGENCE COUNTERS CARRY A SITE
//
// On the snapshot path repair is client-go's and already correct: processDeltas
// writes the store and THEN calls the handler, so a detected divergence there is
// repaired without B doing anything, and store_repairs_fired_total correctly
// stays 0. Without the site split, the design's own B-1-recurring alarm —
// "divergence climbing while repairs_fired flat" — would fire on every correctly
// repaired snapshot-path divergence. An alarm that cries wolf gets muted, and
// the forced-path signal it exists for goes with it. Split by site, the alarm is
// "FORCED-site divergence climbing while repairs_fired flat", which nothing can
// dilute.
package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Detection sites. A divergence found at a snapshot is repaired by client-go;
// one found by the forced pass needs B's own repair verb. They are different
// regimes and never share a counter.
const (
	verifySiteSnapshot = "snapshot"
	verifySiteForced   = "forced"
)

// Skip reasons. Closed set, enumerated from the sites that can decline to run a
// comparison — every value is emitted by exactly one caller, so a non-zero
// bucket names the code path rather than a category (the doctrine
// informer_watch_stats.go states for confirm_retracted_by_reason).
const (
	// The comparison was declined because the informer had never synced. The
	// FIRST snapshot for a GVR lands on an EMPTY indexer, and comparing a full
	// upstream set against it would report one lost ADD per object — 50,000 of
	// them at boot for the composition GVR — burying every real divergence
	// forever. This is the ordinary, expected skip: it ticks once per GVR per
	// informer lifetime and is the only reason that is non-zero on a healthy
	// process.
	verifySkipInitialSync = "initial_sync"
	// The GVR was torn down between the snapshot and the comparison.
	verifySkipTornDown = "torn_down"
	// The forced pass had no metadata client to list with. main.go's
	// metadata.NewForConfig can legitimately fail, leaving rw.metaClient nil,
	// and the pass must say so rather than silently not run.
	verifySkipMetaClientNil = "metaclient_nil"
	// The forced LIST itself failed.
	verifySkipListError = "list_error"
	// The GVR's informer never got the verifying ListerWatcher, so the
	// snapshot path cannot see it. Gate condition B-7.
	verifySkipUndecorated = "undecorated_informer"
	// The watcher holds no recorded sync position for this GVR, so the forced
	// pass cannot issue a LIST whose result is provably at least as fresh as
	// our store. Skipped rather than downgraded — see forcedVerify.
	verifySkipNoSyncPosition = "no_sync_position"
)

// Unverifiable reasons — a registered GVR that no verification mechanism
// reaches right now.
const (
	unverifiableRetracted = "retracted"
	unverifiableNotSynced = "not_synced"
	// unreachable = the informer is undecorated AND there is no metadata
	// client, so neither the snapshot path nor the forced pass can reach it.
	unverifiableUnreachable = "unreachable"
)

var (
	storeVerificationsTotal     atomic.Uint64
	storeObjectsVerifiedTotal   atomic.Uint64
	storeVerifyForcedListsTotal atomic.Uint64
	storeDivergenceCandidates   atomic.Uint64
	storeVerifySkippedTotal     atomic.Uint64
	storeVerifyInvalidatedTotal atomic.Uint64
	storeRepairsFiredTotal      atomic.Uint64
	storeRepairIneffectiveTotal atomic.Uint64
	storeRepairSuppressedTotal  atomic.Uint64
	storeRepairUnsupportedTotal atomic.Uint64

	// Four classes × two sites. Indexed [site][class] through
	// storeDivergentCounter so a new site cannot silently reuse another's
	// counter.
	storeDivergentLostUpdate = map[string]*atomic.Uint64{
		verifySiteSnapshot: {}, verifySiteForced: {},
	}
	storeDivergentLostDelete = map[string]*atomic.Uint64{
		verifySiteSnapshot: {}, verifySiteForced: {},
	}
	storeDivergentLostAdd = map[string]*atomic.Uint64{
		verifySiteSnapshot: {}, verifySiteForced: {},
	}
	storeDivergentUIDMismatch = map[string]*atomic.Uint64{
		verifySiteSnapshot: {}, verifySiteForced: {},
	}

	storeVerifySkipMu       sync.Mutex
	storeVerifySkipByReason = map[string]uint64{}
)

// recordVerifySkipped counts one declined comparison under its reason.
// feedback_negative_evidence_needs_its_scope: "no divergence found" and "no
// comparison ran" are different statements, and only this counter can tell them
// apart.
func recordVerifySkipped(reason string) {
	storeVerifySkippedTotal.Add(1)
	storeVerifySkipMu.Lock()
	storeVerifySkipByReason[reason]++
	storeVerifySkipMu.Unlock()
}

// VerifySkippedByReasonSnapshot returns a copy of the {reason} breakdown.
func VerifySkippedByReasonSnapshot() map[string]uint64 {
	storeVerifySkipMu.Lock()
	defer storeVerifySkipMu.Unlock()
	out := make(map[string]uint64, len(storeVerifySkipByReason))
	for k, v := range storeVerifySkipByReason {
		out[k] = v
	}
	return out
}

// StoreVerificationStats is the tagged family.
//
// The gauges are computed from live state at scrape time (storeVerificationGauges),
// not accumulated, because "how many GVRs are past their deadline right now" is
// a question about the present. Counters are atomics.
type StoreVerificationStats struct {
	// --- denominators. NOT detectors: a zero here makes every zero below meaningless ---
	VerificationsTotal   uint64 `stat:"store_verifications_total" desc:"Completed store verifications (snapshot comparisons plus forced passes). DENOMINATOR, not a detector. DURING the #237 defect: climbs, because snapshots keep happening. Healthy: climbs. A zero here means nothing is being verified at all, which makes every store_divergent_* zero meaningless."`
	ObjectsVerifiedTotal uint64 `stat:"store_objects_verified_total" desc:"Objects compared across all verifications. DENOMINATOR. DURING: climbs. Healthy: climbs. Per forced pass this must advance by the GVR's FULL IndexerCount — a shortfall means sampling crept in and the coverage guarantee (the deadline chooses WHEN, never WHAT) has been silently lost."`
	ForcedListsTotal     uint64 `stat:"store_verification_forced_lists_total" desc:"PartialObjectMetadata LISTs issued by the deadline walker. DENOMINATOR for the forced path. DURING the ten-day-phantom shape (a GVR whose watch never errors, so no snapshot ever occurs): climbs — this is that GVR's only coverage. Healthy: climbs at no more than one per GVR per maxVerificationAge. A rate above that means the deadline or the round-robin is broken, which is the failure mode that could pressure the apiserver."`

	// --- detectors, split by site (see the file header on why) ---
	DivergentLostUpdateSnapshot  uint64 `stat:"store_divergent_lost_update_snapshot_total" desc:"Objects present in both the store and the incoming SNAPSHOT with the same uid but a different resourceVersion — a lost UPDATE, found at a snapshot. DURING #237: >=1 at the first snapshot after the loss. Healthy: 0. Already repaired by client-go when counted here (processDeltas writes the store before calling the handler), so store_repairs_fired_total staying 0 alongside this is CORRECT. Reads 0 if the snapshot itself is stale — see the scope limit in this file's header."`
	DivergentLostUpdateForced    uint64 `stat:"store_divergent_lost_update_forced_total" desc:"Same class, found by the deadline's forced metadata LIST. DURING #237's ten-day case: >=1. Healthy: 0. This one is NOT self-repairing — if it climbs while store_repairs_fired_total stays flat, detection is running without repair, which is the B-1 failure the design was corrected to prevent."`
	DivergentLostDeleteSnapshot  uint64 `stat:"store_divergent_lost_delete_snapshot_total" desc:"Objects in the store that the incoming snapshot no longer carries — a lost DELETE, found at a snapshot. DURING the phantom case: >=1. Healthy: 0. Repaired by client-go's synthesised Deleted delta."`
	DivergentLostDeleteForced    uint64 `stat:"store_divergent_lost_delete_forced_total" desc:"Same class, found by the forced pass. DURING: >=1. Healthy: 0. Needs B's repair verb; pair it with store_repairs_fired_total."`
	DivergentLostAddSnapshot     uint64 `stat:"store_divergent_lost_add_snapshot_total" desc:"Objects the snapshot carries that the store does not — a lost ADD, found at a snapshot. DURING: >=1 if an ADD was dropped. Healthy: 0. NOT counted on a GVR's first sync, where an empty store against a full set is the normal case, not a divergence (that skip is store_verification_skipped_total{initial_sync})."`
	DivergentLostAddForced       uint64 `stat:"store_divergent_lost_add_forced_total" desc:"Same class, found by the forced pass. DURING: >=1. Healthy: 0."`
	DivergentUIDMismatchSnapshot uint64 `stat:"store_divergent_uid_mismatch_snapshot_total" desc:"Same (namespace,name) on both sides with a DIFFERENT uid — a delete-and-recreate phantom, found at a snapshot. DURING: >=1. Healthy: 0. This is the class every existing repair path is structurally blind to: the C2 relist bridge diffs ns/name key sets, and ResolvedKeyInputs carries no object uid, so a recreate under the same name reuses the byte-identical L1 cell."`
	DivergentUIDMismatchForced   uint64 `stat:"store_divergent_uid_mismatch_forced_total" desc:"Same class, found by the forced pass. DURING #237's ten-day staleness: >=1. Healthy: 0. Load-bearing rather than defensive — the ten-day case IS a uid phantom."`
	DivergenceCandidatesTotal    uint64 `stat:"store_divergence_candidates_total" desc:"RAW divergences seen before the bounded re-read confirmed them. DURING: >= the sum of the confirmed counters. Healthy: ~0. A large candidates-minus-confirmed gap is a DeltaFIFO backlog (the indexer lagging the queue), NOT a store defect — which is why the raw number is published rather than hidden."`

	// --- the detectors for a broken detector ---
	GVRsUnverified            int64   `stat:"store_gvrs_unverified" kind:"gauge" desc:"Servable, synced, reachable GVRs whose last verification is older than maxVerificationAge. DURING #237 with B working this reads 0 — the deadline reached the GVR and found the divergence. It is NOT the #237 detector; it is the DENOMINATOR that makes a zero divergence readable. (unverified 0, divergent 0) = I looked and found nothing. (unverified >0, divergent 0) = I DID NOT LOOK. That distinction is exactly what the 1.12.6 audit's divergent:0 could not express. Healthy: 0."`
	GVRsUnverifiable          int64   `stat:"store_gvrs_unverifiable" kind:"gauge" desc:"Registered GVRs no verification mechanism reaches right now, broken down in snowplow_store_verification_unverifiable_by_reason (retracted / not_synced / unreachable). Split from store_gvrs_unverified so a retracted GVR cannot be dropped from that gauge's set and produce a zero that reads as health. DURING a retraction storm: >0 with reason=retracted. Healthy: 0."`
	GVRsUndecorated           int64   `stat:"store_gvrs_undecorated" kind:"gauge" desc:"Registered GVRs whose informer did not get the verifying ListerWatcher, so the snapshot path is blind to them and only the forced pass covers them. Healthy baseline is NOT 0: it is the size of the non-streaming class, whose membership follows from informer routing and must be read as a class rather than as today's list. DURING the coverage cliff (RESOLVER_COMPOSITION_STREAMING_LIST=false): jumps to nearly every GVR while every store_divergent_*_snapshot_total silently drops to 0 — which would otherwise read as health. A jump here is what names that."`
	VerificationMaxAgeSeconds float64 `stat:"store_verification_max_age_seconds" kind:"gauge" desc:"Oldest age-since-last-verified across the same GVR set store_gvrs_unverified counts. At boot, before anything has been verified, this reads AGE SINCE REGISTRATION and never 0 — a never-verified GVR excluded from the max would make this read 0 while nothing had been verified at all. DURING suppressed or failing forced passes: rises past maxVerificationAge without bound. Healthy: at or below maxVerificationAge."`
	SkippedTotal              uint64  `stat:"store_verification_skipped_total" desc:"Comparisons declined, broken down in snowplow_store_verification_skipped_by_reason. Gives 'no divergence found' its scope: without this, a pass that never ran and a pass that found nothing are the same zero. DURING: >0 naming which path could not run (torn_down / metaclient_nil / list_error). Healthy: only initial_sync (once per GVR per informer lifetime) and undecorated_informer (once per undecorated registration, whose healthy baseline is the size of the non-streaming class, not 0) tick. Read the BREAKDOWN, never this total: it deliberately spans expected and unexpected reasons, so the total alone is two regimes in one number."`
	InvalidatedTotal          uint64  `stat:"store_verification_invalidated_total" desc:"Verifications invalidated by a servability retraction, which makes a GVR's prior verification no longer current. CONTEXT, NOT A DETECTOR — it climbs in both regimes (krateo-057 measured 10,011 discovery_refresh retractions in 18h). It is what explains a GVR's age resetting without a verification having run."`

	// --- repair ---
	RepairsFiredTotal      uint64  `stat:"store_repairs_fired_total" desc:"Targeted relists fired to repair a divergence the forced pass found. DURING a confirmed forced-path divergence: >=1. Healthy: 0. Read it AGAINST store_divergent_*_forced_total: divergence climbing while this stays flat is detection without repair — a green counter over a still-wrong store, which is the failure the design was corrected to prevent."`
	RepairIneffectiveTotal uint64  `stat:"store_repair_ineffective_total" desc:"Repairs after which the NEXT verification still reported divergence. ALARMABLE, and the only stat in this family whose non-zero means B ITSELF is wrong rather than the store: a full relist that does not clear a divergence does not indicate a lost event, it indicates our comparison is wrong or our view and the apiserver's differ for a reason a relist cannot fix. Healthy: 0."`
	RepairUnsupportedTotal uint64  `stat:"store_repair_unsupported_total" desc:"Divergences whose GVR CANNOT be repaired by a relist, because its informer comes from the shared dynamic factory — which has no eviction API, so a teardown plus re-register hands back the STOPPED informer and kills the cache for that GVR instead of repairing it. Healthy: 0 in production, where the streaming path owns the informer for the overwhelming majority of GVRs. Non-zero is the honest statement that this GVR is DETECTED and NEVER repaired — a state that would otherwise look identical to a divergence nobody acted on, and one the permanence bound does NOT cover. The refused set is a CLASS determined by construction and recorded at registration, never inferred from the GVR's group."`
	RepairSuppressedTotal  uint64  `stat:"store_repair_suppressed_total" desc:"GVRs the circuit breaker has latched after an ineffective repair. Non-zero means the permanence bound no longer holds for that GVR: its staleness is UNBOUNDED and the alarm is the only mitigation. Detection continues under suppression — the breaker never produces silence. Healthy: 0."`
	RepairsPending         int64   `stat:"store_repairs_pending" kind:"gauge" desc:"GVRs waiting in the serialised repair queue. Repairs run ONE AT A TIME across all GVRs so a correlated divergence cannot take the cache non-servable everywhere at once; the cost of that choice is queue wait, and this is where it is visible. DURING correlated divergence: >0, the queue depth. Healthy: 0."`
	RepairQueueAgeSeconds  float64 `stat:"store_repair_queue_age_seconds" kind:"gauge" desc:"Age of the oldest entry in the repair queue. DURING: rising without bound means the serialised queue is not draining, i.e. the staleness bound (maxVerificationAge + queue wait + one relist) has stopped holding and the real figure is hours, not minutes. Healthy: 0."`
}

// storeVerificationStatsOverride is the test seam every C7 family carries.
var storeVerificationStatsOverride atomic.Pointer[StoreVerificationStats]

// SetStoreVerificationStatsForTest installs (or clears, with nil) a snapshot.
func SetStoreVerificationStatsForTest(s *StoreVerificationStats) {
	storeVerificationStatsOverride.Store(s)
}

// storeVerificationGauges computes the live gauges.
//
// LOCK ORDER (store_verify_state.go): the watcher's facts are gathered under
// rw.mu and rw.mu is RELEASED before the registry mutex is taken. Nothing here
// holds both.
func storeVerificationGauges() (unverified, unverifiable, undecorated int64, maxAge float64, byReason map[string]uint64) {
	byReason = map[string]uint64{}
	rows := verificationRows()
	if len(rows) == 0 {
		return 0, 0, 0, 0, byReason
	}

	type watcherFacts struct {
		registered, servable, synced bool
	}
	facts := make(map[schema.GroupVersionResource]watcherFacts, len(rows))
	haveMetaClient := false
	if rw := Global(); rw != nil && rw.mode != modePassthrough {
		rw.mu.RLock()
		haveMetaClient = rw.metaClient != nil
		for gvr := range rows {
			gi, ok := rw.informers[gvr]
			if !ok || gi == nil {
				continue
			}
			f := watcherFacts{registered: true}
			if inf := gi.Informer(); inf != nil {
				f.synced = inf.HasSynced()
			}
			_, f.servable = rw.servableLocked(gvr)
			facts[gvr] = f
		}
		rw.mu.RUnlock()
	}

	deadline := maxVerificationAge()
	now := time.Now()
	for gvr, row := range rows {
		if !row.decorated {
			undecorated++
		}
		f, ok := facts[gvr]
		if !ok || !f.registered {
			// In the registry but not in rw.informers: a teardown in flight.
			// Not counted in either gauge — forgetStoreVerification runs under
			// the same rw.mu that deleted the informer, so this window is a
			// scrape landing mid-teardown, not a state a GVR can rest in.
			continue
		}
		switch {
		case !f.servable:
			unverifiable++
			byReason[unverifiableRetracted]++
			continue
		case !f.synced:
			unverifiable++
			byReason[unverifiableNotSynced]++
			continue
		case !row.decorated && !haveMetaClient:
			unverifiable++
			byReason[unverifiableUnreachable]++
			continue
		}
		// Reachable and servable: it is in the set the deadline governs, so
		// its age counts toward the max whether or not it is overdue.
		age := now.Sub(row.epoch).Seconds()
		if age > maxAge {
			maxAge = age
		}
		if now.Sub(row.epoch) > deadline {
			unverified++
		}
	}
	return unverified, unverifiable, undecorated, maxAge, byReason
}

// StoreVerificationStatsSnapshot reads the family coherently.
func StoreVerificationStatsSnapshot() StoreVerificationStats {
	if o := storeVerificationStatsOverride.Load(); o != nil {
		return *o
	}
	unverified, unverifiable, undecorated, maxAge, _ := storeVerificationGauges()
	return StoreVerificationStats{
		VerificationsTotal:   storeVerificationsTotal.Load(),
		ObjectsVerifiedTotal: storeObjectsVerifiedTotal.Load(),
		ForcedListsTotal:     storeVerifyForcedListsTotal.Load(),

		DivergentLostUpdateSnapshot:  storeDivergentLostUpdate[verifySiteSnapshot].Load(),
		DivergentLostUpdateForced:    storeDivergentLostUpdate[verifySiteForced].Load(),
		DivergentLostDeleteSnapshot:  storeDivergentLostDelete[verifySiteSnapshot].Load(),
		DivergentLostDeleteForced:    storeDivergentLostDelete[verifySiteForced].Load(),
		DivergentLostAddSnapshot:     storeDivergentLostAdd[verifySiteSnapshot].Load(),
		DivergentLostAddForced:       storeDivergentLostAdd[verifySiteForced].Load(),
		DivergentUIDMismatchSnapshot: storeDivergentUIDMismatch[verifySiteSnapshot].Load(),
		DivergentUIDMismatchForced:   storeDivergentUIDMismatch[verifySiteForced].Load(),
		DivergenceCandidatesTotal:    storeDivergenceCandidates.Load(),

		GVRsUnverified:            unverified,
		GVRsUnverifiable:          unverifiable,
		GVRsUndecorated:           undecorated,
		VerificationMaxAgeSeconds: maxAge,
		SkippedTotal:              storeVerifySkippedTotal.Load(),
		InvalidatedTotal:          storeVerifyInvalidatedTotal.Load(),

		RepairsFiredTotal:      storeRepairsFiredTotal.Load(),
		RepairIneffectiveTotal: storeRepairIneffectiveTotal.Load(),
		RepairSuppressedTotal:  storeRepairSuppressedTotal.Load(),
		RepairUnsupportedTotal: storeRepairUnsupportedTotal.Load(),
		RepairsPending:         storeRepairQueueDepth(),
		RepairQueueAgeSeconds:  storeRepairQueueAgeSeconds(),
	}
}

// StoreVerificationStatsByStat is the family's Values function.
func StoreVerificationStatsByStat() map[string]any {
	return statsByTag(StoreVerificationStatsSnapshot())
}

// UnverifiableByReasonSnapshot returns the {reason} breakdown behind
// store_gvrs_unverifiable.
func UnverifiableByReasonSnapshot() map[string]uint64 {
	_, _, _, _, byReason := storeVerificationGauges()
	return byReason
}

var storeVerifyExpvarOnce sync.Once

// registerStoreVerificationExpvar publishes the tagged family plus the two
// by-reason maps. The maps ride alongside rather than inside the family: a stat
// tag yields one scalar and the C7 tag system has no label facility, so a
// breakdown has to be its own map — the same shape as
// snowplow_informer_confirm_retracted_by_reason and snowplow_reflector_path_by_gvr.
func registerStoreVerificationExpvar() {
	storeVerifyExpvarOnce.Do(func() {
		expvar.Publish("snowplow_store_verification", expvar.Func(func() any {
			return StoreVerificationStatsByStat()
		}))
		expvar.Publish("snowplow_store_verification_skipped_by_reason", expvar.Func(func() any {
			return VerifySkippedByReasonSnapshot()
		}))
		expvar.Publish("snowplow_store_verification_unverifiable_by_reason", expvar.Func(func() any {
			return UnverifiableByReasonSnapshot()
		}))
	})
}

// RegisterStoreVerificationExpvarForTest exposes the publish for tests.
func RegisterStoreVerificationExpvarForTest() { registerStoreVerificationExpvar() }

func init() {
	if Disabled() {
		return
	}
	registerStoreVerificationExpvar()
}

// ResetStoreVerificationStatsForTest zeroes every counter, the breakdowns, the
// registry, the repair queue and the override.
func ResetStoreVerificationStatsForTest() {
	storeVerificationsTotal.Store(0)
	storeObjectsVerifiedTotal.Store(0)
	storeVerifyForcedListsTotal.Store(0)
	storeDivergenceCandidates.Store(0)
	storeVerifySkippedTotal.Store(0)
	storeVerifyInvalidatedTotal.Store(0)
	storeRepairsFiredTotal.Store(0)
	storeRepairIneffectiveTotal.Store(0)
	storeRepairSuppressedTotal.Store(0)
	storeRepairUnsupportedTotal.Store(0)
	for _, m := range []map[string]*atomic.Uint64{
		storeDivergentLostUpdate, storeDivergentLostDelete,
		storeDivergentLostAdd, storeDivergentUIDMismatch,
	} {
		for _, c := range m {
			c.Store(0)
		}
	}
	storeVerifySkipMu.Lock()
	storeVerifySkipByReason = map[string]uint64{}
	storeVerifySkipMu.Unlock()
	storeVerificationStatsOverride.Store(nil)
	resetStoreVerificationStateForTest()
	resetStoreRepairForTest()
}
