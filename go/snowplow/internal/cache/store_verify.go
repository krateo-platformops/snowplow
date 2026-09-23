// store_verify.go — snowplow#237 deliverable B: compare the incoming
// authoritative object set against what the store held an instant earlier,
// AT THE MOMENT TRUTH ARRIVES.
//
// # ROOT CAUSE THIS ADDRESSES
//
// Nothing in snowplow handled "still there, but different". Every safety net
// was keyed on DISAPPEARANCE, verified three times over in #237:
//
//	DepTracker.OnUpdate     fires on the watch UPDATE event — for a LOST event
//	                        the signal and the data share one failure
//	C2 relist bridge        diffs ns/name key SETS (relist_bridge.go): a key
//	                        present on both sides takes the `continue` no
//	                        matter how much its content changed. That is why
//	                        relist_bridge_enqueued_total read 0 across 108 runs
//	                        — correct by construction, not a broken bridge
//	C3 reconcile audit      acts only on objAbsent (deps_reconcile.go): a
//	                        present-but-stale object is skipped BEFORE any
//	                        field is compared, which is why the live capture
//	                        read divergent:0 with the defect active
//
// The audit's blindness is structural, not a coverage shortfall. At
// DEPS_RECONCILE_SAMPLE=infinity and PERIOD=1s it would still report
// divergent:0. Anyone reaching for those knobs is treating a predicate bug as
// a sampling bug.
//
// Meanwhile the oracle already exists and is thrown away: every snapshot the
// reflector takes IS the complete authoritative object set for that GVR, and it
// is handed to Replace() without anything comparing it to what the store
// believed. So repair already happens at every snapshot — only DETECTION was
// missing. This file inserts the comparison. Total coverage per GVR, zero
// additional apiserver load, fires when truth arrives.
//
// # WHY THE DECORATOR HOOKS THE ListerWatcher AND NOT THE ListFunc
//
// Production does not take the LIST path. client-go is v0.35.3, where the
// WatchListClient feature gate defaults to TRUE, nothing in this repo, the
// image, the chart's env or the externally-managed override ConfigMap sets
// KUBE_FEATURE_WatchListClient, and no ListerWatcher in the process implements
// the only opt-out (IsWatchListSemanticsUnSupported). So
// ListAndWatchWithContext calls watchList() FIRST and r.list() — and therefore
// snowplow's whole streamingList paged walk — runs only as a FALLBACK.
//
// A design that hooked the ListFunc would have covered only the fallback: a
// detector that never runs in production. The ListerWatcher is the only place
// snowplow owns that sees BOTH paths, so the decorator covers both:
//
//	LIST path       after inner.List returns, meta.ExtractList over the result
//	WATCHLIST path  accumulate one {rv,uid} per watch.Added, compare at the
//	                k8s.io/initial-events-end bookmark, THEN forward it
//
// In both cases the reflector is blocked inside our call at the moment we
// compare, so the store cannot move under us. On the watch-list path that is
// stronger than it sounds: the reflector accumulates into its own
// temporaryStore and calls r.store.Replace() only AFTER handleListWatch returns
// on the bookmark, so comparing before we forward the bookmark is comparing
// against a store the snapshot has not touched.
//
// # THE DECORATOR SUBMITS NOTHING. IT COUNTS AND LOGS.
//
// An earlier draft had it call submitDepEvent per divergent coordinate. That
// was wrong twice:
//
//  1. REDUNDANT. processDeltas (controller.go) does clientState.Update(obj)
//     and THEN handler.OnUpdate(old, obj) for a Replaced delta, and
//     sharedIndexInformer.OnUpdate computes isSync = (newRV == oldRV), which is
//     FALSE for a lost UPDATE — so it is distributed to snowplow's own
//     UpdateFunc as an ordinary update, on an ALREADY-REPAIRED store. Lost
//     DELETEs arrive the same way through the synthesised Deleted /
//     DeletedFinalStateUnknown, lost ADDs through OnAdd.
//
//  2. ACTIVELY HARMFUL. The decorator runs BEFORE Replace. A dep event
//     submitted here races the worker onto probeObjectState -> GetByKey on the
//     NOT-YET-REPLACED indexer -> objExists -> dirty-mark -> the refresher
//     re-resolves from the stale store, and the counter reads "handled". It
//     self-corrects one hop later, but the first re-resolve is wasted and there
//     is a window in which the instrument lies.
//
// So on the snapshot path repair is client-go's and it is already correct;
// B's job here is purely to NAME what client-go silently fixed. The forced pass
// in store_verify_deadline.go is the one with no Replace, and it is the one
// that needs a repair verb.
//
// # SCOPE LIMIT — READ THIS BEFORE READING A ZERO
//
// The oracle is the snapshot we just received, not the apiserver. If the
// snapshot is ITSELF stale — #237's "the relist happened and did not fix it"
// candidate — the comparison finds equality and every counter reads 0 with the
// defect active. See store_verify_stats.go's header; it is not closed, because
// the only thing that would close it is a quorum read per snapshot.
package cache

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	clientcache "k8s.io/client-go/tools/cache"
)

// objIdentity is everything the comparison needs about one object. Metadata
// only — no body is read, decoded or retained on either side.
type objIdentity struct {
	rv  string
	uid string
}

// divergenceReport is one comparison's outcome.
type divergenceReport struct {
	objectsCompared int
	lostUpdate      []string
	lostDelete      []string
	lostAdd         []string
	uidMismatch     []string
	candidates      int
}

func (r divergenceReport) total() uint64 {
	return uint64(len(r.lostUpdate) + len(r.lostDelete) + len(r.lostAdd) + len(r.uidMismatch))
}

// verifyingListerWatcher decorates the ListerWatcher of a streaming informer.
//
// It holds the informer handle DIRECTLY rather than resolving it through
// ResourceWatcher. That is deliberate and it is the same hazard A3's transport
// wrapper refused: this code runs on the reflector's own goroutine, and a lock
// taken inside a path the reflector drives is one refactor away from a hung pod
// (sync.RWMutex is not reentrant, so rw.mu.RLock on a goroutine already holding
// rw.mu.Lock deadlocks outright). Binding the handle at construction removes
// the hazard by construction — the compare path takes NO watcher lock at all.
type verifyingListerWatcher struct {
	inner    clientcache.ListerWatcher
	innerCtx clientcache.ListerWatcherWithContext
	gvr      schema.GroupVersionResource
	informer clientcache.SharedIndexInformer
}

var _ clientcache.ListerWatcher = (*verifyingListerWatcher)(nil)
var _ clientcache.ListerWatcherWithContext = (*verifyingListerWatcher)(nil)

// newVerifyingListerWatcher wraps lw. bind() must be called with the informer
// constructed over the returned value before the informer is run.
func newVerifyingListerWatcher(lw clientcache.ListerWatcher, gvr schema.GroupVersionResource) *verifyingListerWatcher {
	return &verifyingListerWatcher{
		inner:    lw,
		innerCtx: clientcache.ToListerWatcherWithContext(lw),
		gvr:      gvr,
	}
}

// bind attaches the informer whose store this decorator verifies. Called once,
// immediately after construction and before Run, so there is no publication
// race: the reflector that reads this field does not exist until Run.
func (v *verifyingListerWatcher) bind(inf clientcache.SharedIndexInformer) { v.informer = inf }

func (v *verifyingListerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return v.ListWithContext(context.Background(), options)
}

func (v *verifyingListerWatcher) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	list, err := v.innerCtx.ListWithContext(ctx, options)
	if err != nil || list == nil {
		return list, err
	}
	// The reflector is blocked inside this call, so the store cannot move
	// while we read it. Errors extracting the list are NOT propagated: a
	// diagnostic must never be able to fail a relist. They are counted.
	items, exErr := meta.ExtractList(list)
	if exErr != nil {
		recordVerifySkipped(verifySkipListError)
		return list, nil
	}
	upstream := make(map[string]objIdentity, len(items))
	for _, item := range items {
		if key, id, ok := identityOf(item); ok {
			upstream[key] = id
		}
	}
	v.verify(upstream)
	return list, nil
}

func (v *verifyingListerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return v.WatchWithContext(context.Background(), options)
}

func (v *verifyingListerWatcher) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	w, err := v.innerCtx.WatchWithContext(ctx, options)
	if err != nil || w == nil {
		return w, err
	}
	// ONLY a watch-list request establishes a complete object set. An ordinary
	// re-watch is a delta stream and carries no snapshot to compare against —
	// wrapping it would accumulate for the life of the watch and compare
	// against nothing.
	if options.SendInitialEvents == nil || !*options.SendInitialEvents {
		return w, nil
	}
	return newInitialEventsVerifier(v, w), nil
}

// verify runs the comparison against the bound informer's indexer and reports.
// site is always verifySiteSnapshot here; the forced pass calls compareStore
// directly with verifySiteForced.
func (v *verifyingListerWatcher) verify(upstream map[string]objIdentity) {
	if v.informer == nil {
		recordVerifySkipped(verifySkipTornDown)
		return
	}
	// THE INITIAL-SYNC GUARD. The first snapshot for a GVR lands on an EMPTY
	// indexer, so an ungated comparison would report one lost ADD per object —
	// 50,000 of them at boot for the composition GVR, on every restart, burying
	// every real divergence the detector exists to surface. The guard is the
	// difference between an instrument and a detector that destroys itself on
	// day one. Counted, not silent, so "it skipped" is never mistaken for
	// "it found nothing".
	if !v.informer.HasSynced() {
		recordVerifySkipped(verifySkipInitialSync)
		return
	}
	idx := v.informer.GetIndexer()
	if idx == nil {
		recordVerifySkipped(verifySkipTornDown)
		return
	}
	report := compareStore(idx, upstream)
	publishDivergence(v.gvr, verifySiteSnapshot, report)
}

// identityOf derives the store key and the {rv,uid} of one object. The key
// derivation is MetaNamespaceKeyFunc's — "namespace/name", or "name" when
// cluster-scoped — because that is the key the indexer is keyed by, and a
// second derivation here would be a way for the two sides to disagree about
// what "the same object" means.
func identityOf(obj any) (string, objIdentity, bool) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return "", objIdentity{}, false
	}
	key := acc.GetName()
	if key == "" {
		return "", objIdentity{}, false
	}
	if ns := acc.GetNamespace(); ns != "" {
		key = ns + "/" + key
	}
	return key, objIdentity{rv: acc.GetResourceVersion(), uid: string(acc.GetUID())}, true
}

// compareStore is the whole of B's classification, and it is deliberately one
// function shared by both detection sites so the two can never drift about what
// a lost UPDATE is.
//
// The UID class is the one that matters most and the one a key-set diff is
// structurally blind to: ResolvedKeyInputs carries class/G/V/R/namespace/name/
// BindingUID and NO object UID, so a delete-and-recreate under the same name
// reuses the byte-identical L1 cell. The ten-day staleness in #237 is a UID
// phantom, which is why that arm is load-bearing rather than defensive.
func compareStore(idx clientcache.Indexer, upstream map[string]objIdentity) divergenceReport {
	var rep divergenceReport
	stored := make(map[string]objIdentity, len(upstream))
	for _, obj := range idx.List() {
		if key, id, ok := identityOf(obj); ok {
			stored[key] = id
		}
	}
	rep.objectsCompared = len(upstream)

	var candidates []divergenceCandidate
	for key, up := range upstream {
		st, present := stored[key]
		switch {
		case !present:
			candidates = append(candidates, divergenceCandidate{key, classLostAdd})
		case st.uid != "" && up.uid != "" && st.uid != up.uid:
			// Checked BEFORE the resourceVersion comparison: a recreated
			// object almost always has a different rv too, and counting it as
			// a lost UPDATE would hide the phantom inside the common class.
			candidates = append(candidates, divergenceCandidate{key, classUIDMismatch})
		case st.rv != up.rv:
			candidates = append(candidates, divergenceCandidate{key, classLostUpdate})
		}
	}
	for key := range stored {
		if _, present := upstream[key]; !present {
			candidates = append(candidates, divergenceCandidate{key, classLostDelete})
		}
	}
	rep.candidates = len(candidates)
	if rep.candidates == 0 {
		return rep
	}
	storeDivergenceCandidates.Add(uint64(rep.candidates))

	for _, c := range confirmDivergences(idx, upstream, candidates) {
		switch c.class {
		case classLostAdd:
			rep.lostAdd = append(rep.lostAdd, c.key)
		case classLostUpdate:
			rep.lostUpdate = append(rep.lostUpdate, c.key)
		case classLostDelete:
			rep.lostDelete = append(rep.lostDelete, c.key)
		case classUIDMismatch:
			rep.uidMismatch = append(rep.uidMismatch, c.key)
		}
	}
	return rep
}

// divergenceClass is which of the four classes a candidate falls in.
type divergenceClass int

const (
	classLostAdd divergenceClass = iota
	classLostUpdate
	classLostDelete
	classUIDMismatch
)

type divergenceCandidate struct {
	key   string
	class divergenceClass
}

// confirmDivergenceBudget bounds the re-read that separates a real divergence
// from the DeltaFIFO drain race. Paid only on a divergence, which in a healthy
// process is never.
const confirmDivergenceBudget = 250 * time.Millisecond

// confirmDivergenceTick is the re-read cadence inside the budget.
const confirmDivergenceTick = 10 * time.Millisecond

// confirmDivergences re-reads the candidate keys until they agree with upstream
// or the budget expires, and returns those that still disagree.
//
// THE DRAIN RACE. The indexer lags the DeltaFIFO: a delta already queued but
// not yet popped makes the store look older than upstream, and counting that as
// a divergence would be a false positive on a perfectly healthy process.
// Because the reflector is blocked inside our call we can afford to wait.
//
// THE BUDGET IS FOR THE WHOLE COMPARISON, NOT PER KEY. A correlated divergence
// across thousands of keys must not be able to stall the watch stream for
// keys x 250ms. Whatever still disagrees when the budget expires is counted as
// confirmed, which is the safe direction: the alternative is discarding a real
// divergence because the FIFO happened to be busy.
//
// The raw count is published as store_divergence_candidates_total alongside the
// confirmed classes, rather than hidden, because a large
// candidates-minus-confirmed gap is itself a signal — it says the FIFO is
// backing up, which is a different problem from a stale store and deserves to
// be readable as one.
func confirmDivergences(idx clientcache.Indexer, upstream map[string]objIdentity, candidates []divergenceCandidate) []divergenceCandidate {
	deadline := time.Now().Add(confirmDivergenceBudget)
	pending := candidates
	for {
		still := pending[:0:0]
		for _, c := range pending {
			if stillDivergent(idx, upstream, c) {
				still = append(still, c)
			}
		}
		pending = still
		if len(pending) == 0 || !time.Now().Before(deadline) {
			return pending
		}
		time.Sleep(confirmDivergenceTick)
	}
}

// stillDivergent re-reads ONE key from the indexer and re-applies the same
// predicate the first pass used. It re-derives the class rather than trusting
// the recorded one so a candidate that changed class while the FIFO drained
// (a lost ADD that arrived, leaving only an rv difference) is not confirmed
// under a class it no longer belongs to.
func stillDivergent(idx clientcache.Indexer, upstream map[string]objIdentity, c divergenceCandidate) bool {
	obj, exists, err := idx.GetByKey(c.key)
	if err != nil {
		return true
	}
	up, wanted := upstream[c.key]
	if !exists || obj == nil {
		// Absent from the store: a real lost ADD if upstream has it, and
		// nothing at all if upstream does not (the lost DELETE drained).
		return wanted && c.class == classLostAdd
	}
	_, st, ok := identityOf(obj)
	if !ok {
		return true
	}
	if !wanted {
		return c.class == classLostDelete
	}
	switch c.class {
	case classUIDMismatch:
		return st.uid != "" && up.uid != "" && st.uid != up.uid
	case classLostUpdate:
		return st.rv != up.rv
	case classLostAdd:
		// The object arrived while we waited — the ADD was not lost.
		return false
	case classLostDelete:
		return false
	}
	return false
}

// publishDivergence moves a report onto the counters and, when it is non-empty,
// emits ONE warn-level line naming the classes and a bounded sample of the
// coordinates.
//
// WARN, not INFO, and one line per comparison rather than per coordinate. The
// chart ships LOG_LEVEL=warn — measured on the live pod, 217 WARN + 30 ERROR
// and ZERO INFO across 20h — so an INFO line here would be invisible at exactly
// the moment it is needed, which is the trap that made #237's reflector-path
// question unanswerable from a log in the first place. The counters are the
// instrument (feedback_measurement_use_expvar_not_log_tails); the line is the
// wake-up.
func publishDivergence(gvr schema.GroupVersionResource, site string, rep divergenceReport) {
	storeVerificationsTotal.Add(1)
	storeObjectsVerifiedTotal.Add(uint64(rep.objectsCompared))

	addN(storeDivergentLostUpdate[site], len(rep.lostUpdate))
	addN(storeDivergentLostDelete[site], len(rep.lostDelete))
	addN(storeDivergentLostAdd[site], len(rep.lostAdd))
	addN(storeDivergentUIDMismatch[site], len(rep.uidMismatch))

	if rep.total() == 0 {
		return
	}
	slog.Warn("cache.store.divergent",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("site", site),
		slog.Int("objects_compared", rep.objectsCompared),
		slog.Int("lost_update", len(rep.lostUpdate)),
		slog.Int("lost_delete", len(rep.lostDelete)),
		slog.Int("lost_add", len(rep.lostAdd)),
		slog.Int("uid_mismatch", len(rep.uidMismatch)),
		slog.Int("candidates", rep.candidates),
		slog.Any("sample", divergenceSample(rep)),
		slog.String("hint", "the informer store disagreed with the authoritative object set for this GVR. "+
			"At site=snapshot client-go has already repaired the store and fired the handlers; at site=forced "+
			"the repair is snowplow's and store_repairs_fired_total must advance. A uid_mismatch is a "+
			"delete-and-recreate under the same name, which name-only L1 keying cannot see."),
	)
}

func addN(c *atomic.Uint64, n int) {
	if n > 0 {
		c.Add(uint64(n))
	}
}

// divergenceSampleLimit bounds the coordinates named in the WARN. A correlated
// divergence can span thousands of objects and a log line per coordinate would
// be unreadable and expensive; the counters carry the magnitude, the sample
// carries enough to start with kubectl.
const divergenceSampleLimit = 10

func divergenceSample(rep divergenceReport) []string {
	out := make([]string, 0, divergenceSampleLimit)
	for _, set := range [][]string{rep.uidMismatch, rep.lostUpdate, rep.lostDelete, rep.lostAdd} {
		for _, k := range set {
			if len(out) == divergenceSampleLimit {
				return out
			}
			out = append(out, k)
		}
	}
	return out
}
