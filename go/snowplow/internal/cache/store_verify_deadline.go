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
//	ResourceVersion = lastSyncRV   our store's own position
//	ResourceVersionMatch =         the cacher BLOCKS until it reaches that RV,
//	  NotOlderThan                 so the snapshot is provably at least as fresh
//	                               as our store — which removes the false
//	                               positives a naive RV="0" read would invent
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
	"k8s.io/client-go/metadata"
	clientcache "k8s.io/client-go/tools/cache"
)

// StartStoreVerification starts the deadline walker and the serialised repair
// worker. Called from main.go alongside StartDepsReconcile; a no-op with the
// cache off, where there are no informers to verify.
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
	go runStoreVerificationWalker(ctx)
	go runStoreRepairQueue(ctx)
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
func nextOverdueGVR() (schema.GroupVersionResource, int, bool) {
	rows := verificationRows()
	if len(rows) == 0 {
		return schema.GroupVersionResource{}, 0, false
	}
	deadline := maxVerificationAge()
	now := time.Now()

	var pick schema.GroupVersionResource
	var oldest time.Time
	governed, found := 0, false
	for gvr, row := range rows {
		if row.repairPending {
			// A repair for this GVR is queued or running: its store is about
			// to be rebuilt wholesale, so verifying it now would measure a
			// state that is already being replaced.
			continue
		}
		governed++
		if now.Sub(row.epoch) <= deadline {
			continue
		}
		if !found || row.epoch.Before(oldest) {
			pick, oldest, found = gvr, row.epoch, true
		}
	}
	return pick, governed, found
}

// forcedPassInputs gathers everything the forced pass needs, under rw.mu, and
// releases the lock BEFORE any apiserver call. Nothing in this file holds rw.mu
// across I/O, and nothing holds rw.mu and the verification registry mutex at
// the same time.
func forcedPassInputs(gvr schema.GroupVersionResource) (
	cli metadata.Interface, rv string, idx clientcache.Indexer, skip string,
) {
	rw := Global()
	if rw == nil || rw.mode == modePassthrough {
		return nil, "", nil, verifySkipTornDown
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	gi, ok := rw.informers[gvr]
	if !ok || gi == nil {
		return nil, "", nil, verifySkipTornDown
	}
	inf := gi.Informer()
	if inf == nil {
		return nil, "", nil, verifySkipTornDown
	}
	if !inf.HasSynced() {
		// Not yet synced: the store is still filling, and comparing a partial
		// store against a complete apiserver set would report the remainder as
		// lost ADDs. Same guard, same reason, as the snapshot path's.
		return nil, "", nil, verifySkipInitialSync
	}
	if _, servable := rw.servableLocked(gvr); !servable {
		// Retracted. The store is not being served from, so a comparison
		// against it would measure a cache nobody is reading — and the
		// retraction has already invalidated this GVR's verification, so the
		// deadline re-verifies it as soon as it is confirmed again.
		return nil, "", nil, verifySkipTornDown
	}
	if rw.metaClient == nil {
		// main.go's metadata.NewForConfig can legitimately fail, and the pass
		// must SAY it cannot run rather than silently not running. A forced
		// pass that quietly does nothing is a zero that reads as health.
		return nil, "", nil, verifySkipMetaClientNil
	}
	return rw.metaClient, rw.lastSyncRV[gvr], inf.GetIndexer(), ""
}

// forcedVerify runs one full comparison of a GVR against the apiserver and, on
// a confirmed divergence, queues a repair.
func forcedVerify(ctx context.Context, gvr schema.GroupVersionResource) {
	cli, rv, idx, skip := forcedPassInputs(gvr)
	if skip != "" {
		recordVerifySkipped(skip)
		return
	}
	if idx == nil {
		recordVerifySkipped(verifySkipTornDown)
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
	opts := metav1.ListOptions{
		ResourceVersion:      rv,
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Limit:                0, // MUST stay 0 — see the file header.
	}
	storeVerifyForcedListsTotal.Add(1)
	list, err := cli.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, opts)
	if err != nil {
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
	report := compareStore(idx, upstream)
	publishDivergence(gvr, verifySiteForced, report)

	if latched := noteVerified(gvr, report.objectsCompared, report.total()); latched {
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
		return
	}
	// ORDERING IS LOAD-BEARING, and it reads as incidental: noteVerified above
	// runs BEFORE this enqueue, so when it latches the breaker the enqueue
	// below is already refused (enqueueStoreRepair returns false for a
	// suppressed GVR). Reverse the two and an ineffective repair would latch
	// AND re-enqueue in the same pass, which is the hammering the breaker
	// exists to stop.
	enqueueStoreRepair(gvr, "store_divergence")
}
