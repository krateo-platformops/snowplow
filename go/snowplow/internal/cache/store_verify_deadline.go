// store_verify_deadline.go — snowplow#237 deliverable B: the backstop for the
// GVR whose watch never errors.
//
// # THE RESIDUAL THIS CLOSES
//
// The snapshot comparison fires whenever a snapshot arrives, and a snapshot
// arrives on every watch RE-ESTABLISHMENT. But a watch that ends cleanly on its
// own TimeoutSeconds returns nil from handleWatch, and the reflector re-watches
// from LastSyncResourceVersion with w = nil — NO snapshot. So the ordinary
// 5-10 minute watch recycle is not a verification, and a GVR that only ever
// recycles is never verified at all. That interval is unbounded today, and
// #237's ten-day phantom on a pod with zero restarts is the observed lower
// bound on how bad it gets.
//
// # WHY A DEADLINE IS NOT SAMPLING
//
// The sampled oracle was proposed in #237 and withdrawn, correctly: sampling
// checks SOME objects and infers the rest, so a single phantom among 50K is
// found with probability ~sample/N. This checks EVERY object in the GVR at each
// firing. The deadline chooses WHEN, never WHAT. The guarantee is therefore
// "for every GVR, every object has been compared against an authoritative set
// within the last maxVerificationAge" — a statement with no probability in it,
// which a sampled audit cannot make at any rate.
//
// If a future change makes store_objects_verified_total advance by less than
// the GVR's object count on a forced pass, sampling has crept back in and this
// guarantee is gone. That is what the B-5 arm asserts.
//
// # THE LIST SHAPE, AND WHY IT CANNOT BE A QUORUM READ
//
//	PartialObjectMetadata          no bodies, ~100 B/object: ~5 MB for a 50K GVR
//	ResourceVersion = the store's  our store's own position, read when the pass
//	  published position           opens (store_verify_delivery.go, #875)
//	ResourceVersionMatch =         the cacher BLOCKS until it reaches that RV,
//	  NotOlderThan                 so the snapshot is provably at least as fresh
//	                               as our store was when the pass opened — which
//	                               removes the false positives a naive RV="0"
//	                               read would invent. The store keeps moving
//	                               after that; each candidate is ordered
//	                               against the LIST's own revisions
//	                               (store_verify_judge.go, #872, #918)
//	Limit = 0                      REQUIRED. client-go's own rule: a LIST with a
//	                               Limit and a non-zero resourceVersion is
//	                               delegated to etcd and SKIPS the watch cache.
//	                               This must never become an etcd read.
//
// THE METADATA LIST IS A DETECTOR ONLY. PartialObjectMetadata carries no body,
// so it can prove the store is wrong and cannot make it right. Repair is a
// separate verb, in store_repair.go.
//
// And it is only PARTLY an independent oracle: NotOlderThan guarantees the
// result is at least as fresh as OUR resourceVersion, not that it is as fresh
// as etcd. See the scope limit in store_verify_stats.go.
//
// # THE PACE IS SELF-LIMITING, WITH NO MAGIC NUMBER
//
// maxVerificationAge is derived from ResolvedCacheTTL rather than declared, but
// that alone does not bound the LIST RATE: at a short TTL with 200 GVRs a naive
// tick would issue several unpaginated metadata LISTs per second. A floor
// constant would have been exactly the magic number
// feedback_self_adapt_no_magic_env_knobs forbids, so instead the walker is
// STRICTLY SERIAL — the next forced LIST starts only after the previous one
// RETURNS — and paces itself at maxVerificationAge divided by the number of
// GVRs it governs. The rate is therefore bounded by the apiserver's own service
// time, a quantity the system already owns, rather than by anything we invented.
package cache

import (
	"context"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/metadata"
	clientcache "k8s.io/client-go/tools/cache"
)

// StartStoreVerification starts the deadline walker. Called from main.go
// alongside StartDepsReconcile; a no-op with the cache off, where there are no
// informers to verify. (#870: there is no repair worker — a confirmed
// divergence requests a build, which ends on its own events, store_swap.go.)
func StartStoreVerification(ctx context.Context) {
	if !ResolvedCacheEnabled() {
		return
	}
	slog.Info("cache.store_verification.started",
		slog.String("subsystem", "cache"),
		slog.Duration("max_verification_age", maxVerificationAge()),
		slog.String("hint", "every servable GVR's full object set is compared against the apiserver "+
			"within max_verification_age; snapshots do it for free, this walker is the backstop "+
			"for GVRs whose watch only ever recycles"))
	if positionUnavailable() == 1 {
		// #923 review L2 / N1 — loud, once: with AtomicFIFO off the store's
		// applied position is always empty, so no forced-pass candidate
		// behind its read can ever be decided a loss. The forced pass then
		// does not run, every GVR reads as unverifiable with reason
		// position_unavailable, and store_position_unavailable is 1. A
		// disabled detector, so ERROR, as repair_ineffective is. Nothing
		// changes the gate here.
		slog.Error("cache.store_verification.position_unavailable",
			slog.String("subsystem", "cache"),
			slog.String("hint", "client-go's AtomicFIFO feature is off (KUBE_FEATURE_AtomicFIFO=false?), so "+
				"Store.LastStoreSyncResourceVersion is always empty and the forced store verification cannot "+
				"decide a lost update: it does not run, and every GVR reads as unverifiable "+
				"(store_gvrs_unverifiable, reason position_unavailable). Snapshot detection is unaffected. "+
				"Re-enable AtomicFIFO to restore forced-pass detection."))
	}
	go runStoreVerificationWalker(ctx)
}

// positionUnavailable is 1 when client-go's AtomicFIFO is off: the store then
// reports no applied position (thread_safe_store.go LastStoreSyncResourceVersion
// returns "" unless the gate is on).
func positionUnavailable() int64 {
	if clientfeatures.FeatureGates().Enabled(clientfeatures.AtomicFIFO) {
		return 0
	}
	return 1
}

// runStoreVerificationWalker picks the most overdue eligible GVR, verifies it,
// and paces itself. One goroutine, one LIST at a time — see the file header.
func runStoreVerificationWalker(ctx context.Context) {
	for {
		gvr, governed, ok := nextOverdueGVR()
		if !ok {
			if !sleepCtx(ctx, storeVerifyIdlePoll) {
				return
			}
			continue
		}
		forcedVerify(ctx, gvr)
		if ctx.Err() != nil {
			return
		}
		// Pace: spread the governed set across one deadline window. With 204
		// GVRs at a 3600 s window this is ~17 s between passes, so a quiet
		// cluster — which is the WORST case for this walker, because it
		// generates no watch errors and therefore no free snapshots — still
		// issues at most one forced LIST per GVR per window.
		pace := maxVerificationAge() / time.Duration(max(1, governed))
		if !sleepCtx(ctx, pace) {
			return
		}
	}
}

// storeVerifyIdlePoll is how often the walker re-checks when nothing is overdue.
const storeVerifyIdlePoll = 30 * time.Second

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextOverdueGVR returns the GVR with the oldest epoch that is past the
// deadline, plus the size of the governed set for pacing.
//
// Oldest-first rather than round-robin by registration order: a GVR that keeps
// being missed must eventually reach the head, and ordering by epoch is what
// guarantees it rather than assuming it.
//
// "Oldest" is the later of the epoch and the last ATTEMPT (#875). Whether a GVR
// is overdue is still its epoch alone, so its age and the gauges stay true; but
// a GVR whose pass keeps ending inconclusive (a failing LIST or re-check) never
// advances its epoch, and ordered by epoch alone it would be re-picked at every
// pace interval, re-LISTed in full each time, while every other overdue GVR
// waited behind it. With the attempt in the order it goes to the back after
// each try and the others get their turn. Nothing is limited or skipped: every
// overdue GVR is still attempted, in turn.
//
// #918 — A GVR holding PENDING keys is due now, deadline or not: on a GVR that
// only receives bookmarks, its next pass is the only event that decides them
// (store_verify_delivery.go). Flagged GVRs and overdue ones ALTERNATE (owner
// ruling, 2026-10-27): after a pass on a flagged GVR the next turn goes to the
// most overdue one, then to a flagged one again, and so on, so a GVR whose
// watch keeps lagging can never starve the others, and an overdue GVR is
// served within one turn whatever is flagged.
func nextOverdueGVR() (schema.GroupVersionResource, int, bool) {
	rows := verificationRows()
	if len(rows) == 0 {
		return schema.GroupVersionResource{}, 0, false
	}
	deadline := maxVerificationAge()
	now := time.Now()

	type best struct {
		gvr   schema.GroupVersionResource
		turn  time.Time
		found bool
	}
	var flagged, overdue best
	consider := func(b *best, gvr schema.GroupVersionResource, turn time.Time) {
		if !b.found || turn.Before(b.turn) {
			*b = best{gvr: gvr, turn: turn, found: true}
		}
	}
	governed := 0
	for gvr, row := range rows {
		// #870 (architect Q2) — a build sets nothing the walker reads: the
		// live copy keeps serving while its replacement is built, so the
		// deadline alone decides when it is verified. A re-detection joins
		// the build; a clean pass removes the build's divergence reason.
		governed++
		turn := row.epoch
		if row.lastAttempt.After(turn) {
			turn = row.lastAttempt
		}
		switch {
		case row.hasPending:
			consider(&flagged, gvr, turn)
		case now.Sub(row.epoch) > deadline:
			consider(&overdue, gvr, turn)
		}
	}
	first, second := flagged, overdue
	if lastTurnFlagged() {
		first, second = overdue, flagged
	}
	if first.found {
		return first.gvr, governed, true
	}
	return second.gvr, governed, second.found
}

func lastTurnFlagged() bool {
	storeVerify.mu.Lock()
	defer storeVerify.mu.Unlock()
	return storeVerify.lastTurnFlagged
}

// forcedPassInputs gathers everything the forced pass needs, under rw.mu, and
// releases the lock BEFORE any apiserver call. Nothing in this file holds rw.mu
// across I/O, and nothing holds rw.mu and the verification registry mutex at
// the same time.
//
// #870 (design v5 §3) — it also returns the informer INSTANCE the pass
// compares, so the result is judged against that instance only.
func forcedPassInputs(gvr schema.GroupVersionResource) (
	cli metadata.Interface, rv string, idx clientcache.Indexer, dl *deliveryLog, inst *storeInstance, skip string,
) {
	rw := Global()
	if rw == nil || rw.mode == modePassthrough {
		return nil, "", nil, nil, nil, verifySkipTornDown
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	gi, ok := rw.informers[gvr]
	if !ok || gi == nil {
		return nil, "", nil, nil, nil, verifySkipTornDown
	}
	inf := gi.Informer()
	if inf == nil {
		return nil, "", nil, nil, nil, verifySkipTornDown
	}
	if !inf.HasSynced() {
		// Not yet synced: the store is still filling, and comparing a partial
		// store against a complete apiserver set would report the remainder as
		// lost ADDs. Same guard, same reason, as the snapshot path's.
		return nil, "", nil, nil, nil, verifySkipInitialSync
	}
	if _, servable := rw.servableLocked(gvr); !servable {
		// Retracted. The store is not being served from, so a comparison
		// against it would measure a cache nobody is reading — and the
		// retraction has already invalidated this GVR's verification, so the
		// deadline re-verifies it as soon as it is confirmed again.
		return nil, "", nil, nil, nil, verifySkipTornDown
	}
	if rw.metaClient == nil {
		// main.go's metadata.NewForConfig can legitimately fail, and the pass
		// must SAY it cannot run rather than silently not running. A forced
		// pass that quietly does nothing is a zero that reads as health.
		return nil, "", nil, nil, nil, verifySkipMetaClientNil
	}
	// #875 — nil when the informer's transform could not be installed (a
	// re-registered shared-factory GVR gets the already-started informer back,
	// and SetTransform refuses it). The pass still runs; see forcedVerify.
	dl = rw.deliveries[gvr]
	return rw.metaClient, rw.lastSyncRV[gvr], inf.GetIndexer(), dl, rw.instances[gvr], ""
}

// forcedVerify runs one full comparison of a GVR against the apiserver and, on
// a confirmed divergence, queues a repair.
func forcedVerify(ctx context.Context, gvr schema.GroupVersionResource) {
	// #875 — stamped before anything can end the pass, so a GVR that keeps
	// ending inconclusive rotates to the back of the walker's order.
	noteVerifyAttempt(gvr)
	cli, rv, idx, dl, inst, skip := forcedPassInputs(gvr)
	if skip != "" {
		recordVerifySkipped(skip)
		return
	}
	if idx == nil {
		recordVerifySkipped(verifySkipTornDown)
		return
	}
	if positionUnavailable() == 1 {
		// #923 review N1 — no applied position, so nothing behind its read
		// can be decided: a pass would hold keys pending forever and still
		// mark the GVR verified, a zero that reads as health. It does not
		// run; the GVR reads as unverifiable (storeVerificationGauges).
		recordVerifySkipped(verifySkipPositionUnavailable)
		return
	}

	if rv == "" {
		// NO SYNC POSITION — SKIP, NEVER FALL BACK TO RV="0".
		//
		// The soundness of this entire pass rests on ONE property: with
		// NotOlderThan at our own lastSyncRV the cacher BLOCKS until it
		// reaches OUR resourceVersion, so the returned set is provably at
		// least as fresh as the store. That is what licenses compareStore's
		// `st.rv != up.rv` to mean "the store is behind" — resourceVersions
		// are opaque and cannot be ordered, so inequality only carries a
		// direction because one side is known to be no older.
		//
		// Under RV="0" that property is GONE: the cacher serves whatever it
		// has, which may be OLDER than our store, and then no class is safe —
		// an object created after the cacher's snapshot reads as a lost
		// DELETE, one deleted after it reads as a lost ADD. Follow the chain:
		// false divergence -> a repair that was never needed -> a REAL
		// multi-minute non-servable window -> still "divergent" at the next
		// pass while the cacher lags -> the breaker latches and
		// store_repair_ineffective_total fires, which this family's own
		// description defines as "OUR detector is wrong". A transient cacher
		// lag would manufacture precisely the alarm that says B is broken.
		//
		// Reachable, not theoretical. rw.lastSyncRV has exactly one writer
		// (applyConfirmLocked, servable.go) and it writes only when the
		// informer reports a non-empty LastSyncResourceVersion — AFTER, and
		// independently of, the `confirmed` write. A confirm pass landing
		// before the informer syncs therefore leaves
		// HasSynced && servable && rv == "" until the next pass.
		//
		// So it is counted as a skip, under its own reason. "I could not run
		// a sound comparison" must never be spelled the same way as "I
		// compared and found nothing".
		recordVerifySkipped(verifySkipNoSyncPosition)
		return
	}
	// #918 — first, the keys the previous pass left pending are judged
	// against their own reads (store_verify_delivery.go judgePending): the
	// losses among them are published with this pass, or on their own if
	// this pass cannot take its read. Then the pass opens: the LIST is no
	// older than the reflector's position read now (beginPass).
	//
	// No delivery log (dl == nil): the LIST carries the confirm-time position
	// as it did before #875.
	listRV := rv
	var carried []divergenceCandidate
	var gen uint64
	if dl != nil {
		var release func()
		carried, release = dl.judgePending()
		// The carried losses stay counted as pending until this pass has
		// published them, whichever way it ends (storePendingKeys' invariant).
		defer release()
		if listRV = dl.beginPass(); listRV == "" {
			publishCarried(gvr, carried)
			recordVerifySkipped(verifySkipNoSyncPosition) // same reason, same rule: never RV="0"
			return
		}
		gen = dl.passGen()
	}
	opts := metav1.ListOptions{
		ResourceVersion:      listRV,
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Limit:                0, // MUST stay 0 — see the file header.
	}
	storeVerifyForcedListsTotal.Add(1)
	// #263 — tag THIS List's ctx so the reflector-path classifier
	// (reflector_path.go isForcedVerifyRequest) excludes it from establishment
	// attribution: a forced-verification LIST carries RV=lastSyncRV and would
	// otherwise flip the GVR's reflector path to 'list' and WARN falsely. Scoped to
	// this single List call — a metaClient.List spawns no nested establishment LIST.
	list, err := cli.Resource(gvr).Namespace(metav1.NamespaceAll).List(withForcedVerifyTag(ctx), opts)
	if err != nil {
		publishCarried(gvr, carried)
		recordVerifySkipped(verifySkipListError)
		slog.Warn("cache.store.verification_list_failed",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("error", err.Error()),
			slog.String("hint", "the deadline pass could not read this GVR, so it stays unverified and "+
				"store_gvrs_unverified will count it — a divergence zero for this GVR means "+
				"nothing until this clears"))
		return
	}

	upstream := make(map[string]objIdentity, len(list.Items))
	for i := range list.Items {
		if key, id, ok := identityOf(&list.Items[i]); ok {
			upstream[key] = id
		}
	}
	// The pass has its own read now: what is still pending is superseded by it.
	if dl != nil {
		dl.supersede()
	}
	candidates := withoutKeys(diffStore(idx, upstream), carried)
	// #872 / #875 / #878 / #918 — the store kept moving while the LIST was in
	// flight and keeps moving now, so each candidate is ordered against the
	// LIST's own revisions under the lost-update rule (store_verify_judge.go):
	// lost only if the store is behind the read and has applied everything up
	// to it. A key the store has not caught up with yet is PENDING, re-judged
	// by the GVR's next applied event or its next pass; a pair of
	// resourceVersions the API does not let us order is UNDECIDED.
	//
	// #875 — a re-registered shared-factory GVR gets its already-started
	// informer back and the transform cannot be installed; its log has the
	// handler only. The pass still runs — skipping it would leave the #237
	// class unverified and starve #244's factory-divergence age
	// (noteVerified) — and store_divergence_unrecorded_total counts what it
	// publishes.
	report := reportOf(len(upstream), 0, carried)
	if len(candidates) > 0 {
		report = judgeForcedPass(gvr, idx, dl, upstream, list.ResourceVersion, gen, candidates, carried)
	}
	// #870 (design v5 §3, F6) — the result belongs to the instance it
	// compared. A pass open across a swap is discarded here: it neither
	// publishes nor latches nor requests, and the new copy is judged by its
	// own next pass.
	latched, discarded := Global().noteVerifiedFor(gvr, inst, report.objectsCompared, report.total())
	if discarded {
		return
	}
	publishDivergence(gvr, verifySiteForced, report)

	if latched {
		slog.Error("cache.store.repair_ineffective",
			slog.String("subsystem", "cache"),
			slog.String("gvr", gvr.String()),
			slog.String("hint", "a full relist of this GVR did NOT clear its divergence, so repairs for "+
				"it are now suppressed. This does not indicate a lost event — it indicates the "+
				"comparison is wrong, or our view and the apiserver's differ for a reason a "+
				"relist cannot fix. Detection continues; staleness for this GVR is now UNBOUNDED "+
				"and this alarm is the only mitigation."))
	}
	if report.total() == 0 {
		// #870 — a clean pass on the live copy removes only a build's
		// divergence reason (N1); the build aborts only when no reason is left.
		// A pass that left keys pending or undecided (#918) did not verify
		// them, so it is not clean: an open repair keeps its reason, and a key
		// that later resolves to a loss joins it.
		if report.unsettled == 0 {
			Global().noteCleanPass(gvr, inst)
		}
		return
	}
	// ORDERING IS LOAD-BEARING, and it reads as incidental: noteVerified above
	// runs BEFORE this request, so when it latches the breaker the request
	// below is already refused (requestStoreBuild refuses a suppressed GVR).
	// Reverse the two and an ineffective repair would latch AND rebuild in the
	// same pass, which is the hammering the breaker exists to stop.
	requestStoreRepair(gvr, report.confirmedKeys())
}

// publishCarried publishes the losses judgePending decided when the pass that
// decided them cannot take its own read: they do not depend on it.
func publishCarried(gvr schema.GroupVersionResource, carried []divergenceCandidate) {
	if len(carried) > 0 {
		publishResolvedLoss(gvr, carried)
	}
}

// withoutKeys drops the candidates already decided by judgePending: an older
// read of theirs has already shown the loss.
func withoutKeys(candidates, decided []divergenceCandidate) []divergenceCandidate {
	if len(decided) == 0 {
		return candidates
	}
	skip := make(map[string]bool, len(decided))
	for _, d := range decided {
		skip[d.key] = true
	}
	out := candidates[:0:0]
	for _, c := range candidates {
		if !skip[c.key] {
			out = append(out, c)
		}
	}
	return out
}
