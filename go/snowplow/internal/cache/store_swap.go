// store_swap.go — #870: repair a store by building its replacement beside the
// live one and swapping it in, marking only what changed.
//
// Design: issue #870, v5 (issuecomment-6442546269) + v5.1
// (issuecomment-6443025562), owner rulings 1–3 and N5.
//
// # WHAT THIS REPLACES
//
// Before #870 a confirmed divergence (and a CRD schema widen) tore the GVR's
// informer down and registered an empty replacement: the GVR was NOT SERVABLE
// for the whole rebuild (20–35 s at 50K), every resolve that missed fell
// through to a live apiserver List, and every dependent of the type was
// dirty-marked twice (before the teardown and after the sync) with every
// incremental list cell unanchored. Run 14's 6.4 GB heap peak was that window.
//
// # WHAT HAPPENS NOW
//
//  1. A request (a confirmed divergence, a schema change) CONSTRUCTS a build or
//     JOINS the running one, and returns. The new copy is built in
//     rw.builds[gvr] through the SAME construction branch as the live copy
//     (F9), muted, sharing every unchanged object with the live copy (reuse).
//     The servability check never reads that slot, so the GVR stays servable.
//  2. Every build ends on an EVENT — never a timer, never a queue (F2, F3,
//     F11, owner ruling 2): the new copy's sync installs it; a terminal LIST
//     refusal aborts it; a GVR teardown aborts it; a clean verification pass
//     that leaves its reason set empty aborts it.
//  3. The install (installBuild): under rw.mu the new copy's state is written
//     over exactly the per-GVR map set a teardown purges (perGVRFields), the
//     previous copy joins the retiring set, and the lock is released. Then,
//     with no lock held, every retiring copy is stopped and waited for; the new
//     copy is unmuted; M — every key whose bytes differ between the new store
//     and what each consumer last ACTED on — is computed; the push ledger is
//     opened; and M is delivered, one swapDelivery per key through the new
//     informer's own OnUpdate, so each handler receives it in its own order
//     (store_instance.go).
//  4. The servability epoch does NOT move at a swap (owner ruling 1) and the
//     GVR keeps its confirmation. No whole-type mark, no unanchor: the only
//     apiserver List per repair is the new copy's own.
//
// # LOCK ORDER (design v5 §2.6, re-cited for client-go v0.36.5 by the
// architect's #870 evaluation, issuecomment-6446166419)
//
//	(HasSynced is a channel read on v0.36.5: rw.mu takes no informer lock)
//	new copy's transform → live indexer read lock (the reuse transform); a
//	  transform can run concurrently with processing of the same informer, so
//	  it NEVER takes rw.mu
//	blockDeltas → indexer (processDeltas / processDeltasInBatch, controller.go
//	  :607-667 / :676-754); FIFO → blockDeltas when the queue holds ≥2 batches
//	transform → deliveryLog.mu (released before next)
//	listener → rw.mu.RLock (syncChState); → listCellSet.mu; → depWatch maskMu
//	listener (adapter) → adapter / record mutex (leaves)
//	swap goroutine OnUpdate (shared_informer.go :969) → listenersLock.RLock +
//	  one send per listener (distribute :1094-1117)
//	hub.mu → (released) → ledger mutex (leaf); /refreshes handler → ledger mutex
//	rw.mu → storeVerify.mu; rw.mu → ledger mutex (leaf)
//
// THE TRANSFORM CHAIN IS IDEMPOTENT. Under watch-list each object is
// transformed twice (the reflector's temporary store, then
// addReplaceToItemsLocked): the strip passes a bytes-backed object through
// unchanged, the #875 delivery log records a key and the latest object (a set
// and a pointer), and reuse hands back the same live object again
// (TestStoreSwap_TransformChainIsIdempotent).
//
// The swap goroutine holds rw.mu only in step 1 and in the step-3 check, and
// does no waiting and no delivery under it. NO DELIVERY MAY EVER BE PLACED
// INSIDE rw.mu: the dep ADD path takes rw.mu.RLock on the listener goroutine
// the retiring wait is waiting for (X29: a hang).
package cache

import (
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata/metadatainformer"
	clientcache "k8s.io/client-go/tools/cache"

	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// storeBuild is one GVR's build in progress: the current candidate and the
// accounting of every reason it exists for.
type storeBuild struct {
	inst    *storeInstance
	account *buildAccount
}

// ── the per-GVR state, as ONE list ───────────────────────────────────────────

// perGVRField is one per-GVR map of the watcher. The teardown purges every
// entry (deletePerGVRStateLocked) and the install writes every entry from the
// new copy, or KEEPS it when it describes the GVR rather than the copy
// (install == nil). One list for both, so a future map cannot be purged and
// not swapped, or swapped and not purged (design v5 §2.6 step 1; the parity
// arm TestStoreSwap_PerGVRFieldsCoverEveryMap pins it against the struct).
type perGVRField struct {
	name    string
	has     func(rw *ResourceWatcher, gvr schema.GroupVersionResource) bool
	del     func(rw *ResourceWatcher, gvr schema.GroupVersionResource)
	install func(rw *ResourceWatcher, gvr schema.GroupVersionResource, c *storeInstance)
}

func setMember(m *map[schema.GroupVersionResource]struct{}, gvr schema.GroupVersionResource, on bool) {
	if on {
		if *m == nil {
			*m = map[schema.GroupVersionResource]struct{}{}
		}
		(*m)[gvr] = struct{}{}
		return
	}
	delete(*m, gvr)
}

var perGVRFields = []perGVRField{
	{"informers",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.informers[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.informers, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) { rw.informers[g] = c.gi }},
	{"syncCh",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.syncCh[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.syncCh, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) { rw.syncCh[g] = c.syncCh }},
	// confirmed describes the TYPE, not the copy: the GVR keeps its
	// confirmation across a swap (design v5 §1).
	{"confirmed",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.confirmed[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.confirmed, g) },
		nil},
	{"watchBroken",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.watchBroken[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.watchBroken, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			setMember(&rw.watchBroken, g, c.broken)
		}},
	{"listRefused",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.listRefused[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.listRefused, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			setMember(&rw.listRefused, g, c.refused)
		}},
	{"informerStop",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.informerStop[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.informerStop, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			rw.informerStop[g] = c.stop
		}},
	{"metadataOnly",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.metadataOnly[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.metadataOnly, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			setMember(&rw.metadataOnly, g, c.shape.metadataOnly())
		}},
	// lastSyncRV is the conjunct-3 recovery baseline: the position the
	// confirm pass last observed. The new copy's own position.
	{"lastSyncRV",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.lastSyncRV[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.lastSyncRV, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			if rw.lastSyncRV == nil {
				rw.lastSyncRV = map[schema.GroupVersionResource]string{}
			}
			if rv := c.gi.Informer().LastSyncResourceVersion(); rv != "" {
				rw.lastSyncRV[g] = rv
			} else {
				delete(rw.lastSyncRV, g)
			}
		}},
	{"deliveries",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.deliveries[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.deliveries, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			if c.dl != nil {
				rw.rememberDeliveryLogLocked(g, c.dl)
			} else {
				delete(rw.deliveries, g)
			}
		}},
	{"watchHandlerInstalled",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool {
			_, ok := rw.watchHandlerInstalled[g]
			return ok
		},
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.watchHandlerInstalled, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) {
			rw.markWatchHandlerInstalledLocked(g)
		}},
	{"instances",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.instances[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.instances, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) { rw.instances[g] = c }},
	// The build slot: the install consumes it.
	{"builds",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.builds[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.builds, g) },
		func(rw *ResourceWatcher, g schema.GroupVersionResource, c *storeInstance) { delete(rw.builds, g) }},
	// The retiring set is appended by the install itself (step 1) and pruned
	// by the retiring wait; kept here.
	{"retiring",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.retiring[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.retiring, g) },
		nil},
	// acted survives swaps (P1): it is the GVR's, not the copy's.
	{"acted",
		func(rw *ResourceWatcher, g schema.GroupVersionResource) bool { _, ok := rw.acted[g]; return ok },
		func(rw *ResourceWatcher, g schema.GroupVersionResource) { delete(rw.acted, g) },
		nil},
}

// ensureSwapMapsLocked allocates the #870 maps. Callers hold rw.mu.Lock.
func (rw *ResourceWatcher) ensureSwapMapsLocked() {
	if rw.instances == nil {
		rw.instances = map[schema.GroupVersionResource]*storeInstance{}
	}
	if rw.builds == nil {
		rw.builds = map[schema.GroupVersionResource]*storeBuild{}
	}
	if rw.retiring == nil {
		rw.retiring = map[schema.GroupVersionResource][]*storeInstance{}
	}
	if rw.acted == nil {
		rw.acted = map[schema.GroupVersionResource]map[string]*actedRecord{}
	}
	if rw.syncCh == nil {
		rw.syncCh = map[schema.GroupVersionResource]chan struct{}{}
	}
	if rw.informerStop == nil {
		rw.informerStop = map[schema.GroupVersionResource]chan struct{}{}
	}
}

// newLiveInstanceLocked records gvr's FIRST instance at registration: its sync
// channel and its per-GVR stop channel (R6) become the GVR's. Callers hold
// rw.mu.Lock.
func (rw *ResourceWatcher) newLiveInstanceLocked(gvr schema.GroupVersionResource, gi informers.GenericInformer, shape instanceShape) *storeInstance {
	rw.ensureSwapMapsLocked()
	in := newStoreInstance(gvr, gi, shape, rw.perGVRStopLocked(gvr))
	rw.syncCh[gvr] = in.syncCh
	rw.instances[gvr] = in
	return in
}

// actedRecordLocked returns gvr's acted record for handler name, or nil when
// the GVR's informer is not owned: acted's only reader is the build path, and
// only an owned informer can be rebuilt (architect Q4). Callers hold rw.mu.
func (rw *ResourceWatcher) actedRecordLocked(gvr schema.GroupVersionResource, name string, owned bool) *actedRecord {
	if !owned {
		return nil
	}
	rw.ensureSwapMapsLocked()
	m := rw.acted[gvr]
	if m == nil {
		m = map[string]*actedRecord{}
		rw.acted[gvr] = m
	}
	r := m[name]
	if r == nil {
		r = newActedRecord()
		m[name] = r
	}
	return r
}

// wireInstanceLocked attaches the handler adapters (the dep handler, then
// every matching extension, in registry order) and the instance-aware watch
// error handler. Called before Run. Callers hold rw.mu.Lock.
func (rw *ResourceWatcher) wireInstanceLocked(gvr schema.GroupVersionResource, in *storeInstance, path string) {
	resourceType := gvrResourceTypeString(gvr)
	owned := in.shape.owned()
	inf := in.gi.Informer()
	if _, regErr := inf.AddEventHandler(in.adapt(depHandlerName, rw.depEventHandler(gvr),
		rw.actedRecordLocked(gvr, depHandlerName, owned))); regErr != nil {
		slog.Warn("cache.deps.add_event_handler_failed",
			slog.String("subsystem", "cache"),
			slog.String("resource_type", resourceType),
			slog.String("path", path),
			slog.String("error", regErr.Error()),
		)
	}
	attachMatchingHandlerExtensions(rw, gvr, &adaptingAttacher{
		in:  in,
		inf: inf,
		record: func(name string) *actedRecord {
			return rw.actedRecordLocked(gvr, name, owned)
		},
	})
	rw.installWatchErrorHandlerFor(gvr, in)
}

// ── the request path ─────────────────────────────────────────────────────────

// requestStoreBuild is the ONE request path for a build (design v5 §2.1/§2.2).
// reason is reasonDivergence (keys = the confirmed-divergent keys) or
// reasonSchema. It constructs a build or joins the running one, and returns
// without waiting. Returns false when the request was refused.
func (rw *ResourceWatcher) requestStoreBuild(gvr schema.GroupVersionResource, reason string, keys []string) bool {
	if rw == nil || rw.mode == modePassthrough {
		return false
	}
	now := windowNow()
	rw.mu.Lock()
	live := rw.instances[gvr]
	if live == nil {
		rw.mu.Unlock()
		return false
	}
	// BOUND 6 / #244 — ONLY OWNED INFORMERS CAN BE REBUILT, decided by the
	// RECORDED property (set by the constructing branch), never by a group
	// name. A factory-built informer's construction would return the SAME
	// cached instance (factory.ForResource), and the swap would install the
	// live informer over itself and stop it.
	owned, known, suppressed := storeVerificationOwnership(gvr)
	if !known || !owned || !live.shape.owned() {
		rw.mu.Unlock()
		refuseUnownedBuild(gvr, reason)
		return false
	}
	if reason == reasonDivergence && suppressed {
		// The breaker (bound 3): detection continues, repairs do not.
		rw.mu.Unlock()
		return false
	}
	rw.ensureSwapMapsLocked()
	b := rw.builds[gvr]
	constructed, superseded := false, false
	switch {
	case b == nil:
		c := rw.constructCandidateLocked(gvr, live)
		if c == nil {
			rw.mu.Unlock()
			return false
		}
		b = &storeBuild{inst: c, account: newBuildAccount()}
		rw.builds[gvr] = b
		constructed = true
	case reason == reasonSchema:
		// A schema change SUPERSEDES the running copy: it was listing under the
		// old schema. A fresh copy is constructed under the new one, carrying
		// the build's reasons and keys (F7).
		c := rw.constructCandidateLocked(gvr, live)
		if c == nil {
			rw.mu.Unlock()
			return false
		}
		b.inst.closeStop()
		b.inst = c
		superseded = true
	}
	hadDivergence := b.account.has(reasonDivergence)
	b.account.note(gvr, reason, keys, now)
	if reason == reasonDivergence {
		markRepairPending(gvr, true)
		if !hadDivergence {
			storeRepairsFiredTotal.Add(1)
		}
	}
	rw.mu.Unlock()
	slog.Info("cache.store.swap.requested",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("reason", reason),
		slog.Int("keys", len(keys)),
		slog.Bool("constructed", constructed),
		slog.Bool("superseded", superseded),
		slog.String("hint", "a replacement store is being built beside the live one; the GVR stays "+
			"servable and every key the build will change stays known-stale until the swap"),
	)
	return true
}

// refuseUnownedBuild counts and logs a refused build of a factory-built GVR
// (store_repair_unsupported_total). Detection continues.
func refuseUnownedBuild(gvr schema.GroupVersionResource, reason string) {
	storeRepairUnsupportedTotal.Add(1)
	slog.Warn("cache.store.repair_unsupported",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("reason", reason),
		slog.String("hint", "this GVR's informer comes from a shared informer factory, which hands back "+
			"the same cached informer: a replacement cannot be built beside it. The divergence is "+
			"still reported; it is not repaired."))
}

// constructCandidateLocked builds a muted copy of gvr through the live copy's
// construction branch (F9) and starts it. Nothing here touches the
// verification registry (F6a: rememberStoreVerification would wipe its
// pending and suppression state). Callers hold rw.mu.Lock.
func (rw *ResourceWatcher) constructCandidateLocked(gvr schema.GroupVersionResource, live *storeInstance) *storeInstance {
	gi := rw.newOwnedInformerLocked(gvr, live.shape)
	if gi == nil {
		return nil
	}
	c := newStoreInstance(gvr, gi, live.shape, make(chan struct{}))
	c.muted.Store(true)
	liveIdx := live.gi.Informer().GetIndexer()
	c.reuse.Store(&liveIdx)
	var tf clientcache.TransformFunc
	if !live.shape.metadataOnly() {
		tf = StripBulkyFieldsForResourceType(gvrResourceTypeString(gvr), gvr)
	}
	dl := newDeliveryLog(gi.Informer().LastSyncResourceVersion)
	if err := gi.Informer().SetTransform(c.reuseTransform(dl.transform(tf))); err == nil {
		c.dl = dl
	}
	rw.wireInstanceLocked(gvr, c, "swap-candidate")
	if rw.stopRequestedLocked() {
		return c
	}
	c.ownRun = true
	rw.goroutineWG.Add(2)
	go func() {
		defer rw.goroutineWG.Done()
		workclass.ClearGoroutine() // #871: process-lifetime; never charged to the unit that started it
		c.runInformer()
	}()
	go func() {
		defer rw.goroutineWG.Done()
		waitInformerSync(c.gi.Informer().HasSynced, c.syncCh, c.stop)
		if c.stopped() || !c.gi.Informer().HasSynced() {
			return
		}
		rw.installBuild(c)
	}()
	return c
}

// newOwnedInformerLocked constructs a fresh informer for gvr through shape's
// branch. nil when the branch cannot construct one now.
func (rw *ResourceWatcher) newOwnedInformerLocked(gvr schema.GroupVersionResource, shape instanceShape) informers.GenericInformer {
	indexers := clientcache.Indexers{clientcache.NamespaceIndex: clientcache.MetaNamespaceIndexFunc}
	switch shape {
	case shapeStreaming:
		gi, ok := newStreamingDynamicInformer(rw.restConfig, rw.dyn, gvr, indexers, listOptionsTweak)
		if !ok {
			return nil
		}
		return gi
	case shapeStandaloneDynamic:
		return dynamicinformer.NewFilteredDynamicInformer(rw.dyn, gvr, metav1.NamespaceAll, 0, indexers, listOptionsTweak)
	case shapeStandaloneMetadata:
		if rw.metaClient == nil {
			return nil
		}
		return metadatainformer.NewFilteredMetadataInformer(rw.metaClient, gvr, metav1.NamespaceAll, 0, indexers, listOptionsTweak)
	default:
		return nil
	}
}

// ── ends without a swap ──────────────────────────────────────────────────────

// abortBuildLocked removes gvr's build and stops its copy. The caller ends the
// account (outside rw.mu where it can). Callers hold rw.mu.Lock.
func (rw *ResourceWatcher) abortBuildLocked(gvr schema.GroupVersionResource) *buildAccount {
	b := rw.builds[gvr]
	if b == nil {
		return nil
	}
	delete(rw.builds, gvr)
	b.inst.closeStop()
	markRepairPending(gvr, false)
	return b.account
}

// noteCleanPass: a verification pass on the LIVE copy in found no divergence.
// It removes only the divergence reason (a schema widen keeps resourceVersions
// unchanged, so a clean pass says nothing about it — N1); the build aborts only
// when its reason set is empty.
func (rw *ResourceWatcher) noteCleanPass(gvr schema.GroupVersionResource, in *storeInstance) {
	if rw == nil {
		return
	}
	rw.mu.Lock()
	b := rw.builds[gvr]
	if b == nil || rw.instances[gvr] != in || !b.account.has(reasonDivergence) {
		rw.mu.Unlock()
		return
	}
	markRepairPending(gvr, false)
	acc := b.account
	var aborted *buildAccount
	if len(acc.reasonList()) == 1 {
		aborted = rw.abortBuildLocked(gvr)
	}
	rw.mu.Unlock()
	acc.endReason(reasonDivergence, outcomeAbortClean, windowNow())
	_ = aborted
}

// ── the install ──────────────────────────────────────────────────────────────

// installBuild is the swap (design v5 §2.6, v5.1 C1/C3/Q3, architect Q3/Q6).
// Runs on the candidate's sync goroutine.
func (rw *ResourceWatcher) installBuild(c *storeInstance) {
	gvr := c.gvr
	// Step 1 — under rw.mu: re-check, write the state, retire the previous
	// copy, unlock. The new copy serves reads from here, still muted.
	rw.mu.Lock()
	b := rw.builds[gvr]
	live := rw.instances[gvr]
	if b == nil || b.inst != c || live == nil || c == live || c.stopped() || !c.gi.Informer().HasSynced() {
		rw.mu.Unlock()
		return
	}
	for _, f := range perGVRFields {
		if f.install != nil {
			f.install(rw, gvr, c)
		}
	}
	c.reuse.Store(nil)
	acc := b.account
	if live.account != nil && !live.passedSt3 {
		// Q6 — the previous install has not passed its step-3 check: it will
		// never deliver, so this build owns its accounting.
		acc.absorb(live.account)
		live.account = nil
		storeSwapStats.superseded.Add(1)
	}
	c.account = acc
	rw.retiring[gvr] = append(rw.retiring[gvr], live)
	retiring := append([]*storeInstance(nil), rw.retiring[gvr]...)
	resetVerificationAfterSwap(gvr, acc.has(reasonDivergence))
	installed := windowNow()
	rw.mu.Unlock()
	storeSwapStats.swaps.Add(1)
	swapStep(c, "installed")

	// Step 2 — no lock held. C3: close EVERY retiring copy's stop (once
	// each), then wait for every one's Run to return. The wait ends when each
	// listener's in-flight handler call returns (an inherited bound: the CRD
	// lifecycle park, crdLifecycleParkMax).
	swapStep(c, "before_close_retiring")
	for _, r := range retiring {
		r.closeStop()
	}
	for _, r := range retiring {
		if r.ownRun {
			<-r.done
		}
	}

	// Step 3 — give up if a later build installed during the wait (an
	// optimisation, not a correctness condition: Q3). The flag is set and
	// read under the same rw.mu as the absorb in step 1 (Q6).
	rw.mu.Lock()
	rw.pruneRetiredLocked(gvr, retiring)
	if rw.instances[gvr] != c {
		rw.mu.Unlock()
		return
	}
	c.passedSt3 = true
	acc = c.account
	c.account = nil
	rw.mu.Unlock()
	swapStep(c, "after_step3")

	// Step 4 — unmute. First every key whose object is not the one the old
	// copy held is made pending in the incremental cells (F5: the comparator
	// then never judges a key whose delivery is still on its way, and a
	// byte-equal key with a new identity is re-pointed by the C6 rebind);
	// then the comparator's record becomes the new copy's `seen`; then the
	// handlers act.
	markChangedPending(gvr, live, c)
	setComparatorRecord(gvr, c)
	swapStep(c, "before_unmute")
	c.muted.Store(false)

	// Step 5 — M, after the unmute: a write the new copy took while muted
	// cannot fall between the two.
	m := computeM(c)
	storeSwapStats.mKeys.Add(uint64(len(m)))
	swapStep(c, "after_m")

	// Q3 — the ledger opens BEFORE M is delivered, so no dependent can be
	// refreshed and pushed before its entry exists.
	acc.setSchemaKeys(m)
	openSwapLedger(gvr, acc, m, installed)

	// Step 6 — deliver M through the new informer's own OnUpdate: every
	// listener gets one swapDelivery per key, in order with its real ones.
	deliverM(c, m)
	slog.Info("cache.store.swap.installed",
		slog.String("subsystem", "cache"),
		slog.String("gvr", gvr.String()),
		slog.String("reasons", strings.Join(acc.reasonList(), ",")),
		slog.Int("m_keys", len(m)),
		slog.Int("retired_copies", len(retiring)),
	)
}

// swapStepHookForTest, when non-nil, runs at named points of installBuild
// ("installed", "before_close_retiring", "after_step3", "before_unmute",
// "after_m"), never under a lock. TEST-ONLY: production pays one atomic load
// per point. The interleaving arms (K11d, K11e, K11f) use it to land a write
// or another install at an exact step.
var swapStepHookForTest atomic.Pointer[func(c *storeInstance, step string)]

func swapStep(c *storeInstance, step string) {
	if h := swapStepHookForTest.Load(); h != nil {
		(*h)(c, step)
	}
}

// pruneRetiredLocked drops the waited-for copies from the retiring set.
func (rw *ResourceWatcher) pruneRetiredLocked(gvr schema.GroupVersionResource, waited []*storeInstance) {
	done := map[*storeInstance]bool{}
	for _, r := range waited {
		done[r] = true
	}
	var keep []*storeInstance
	for _, r := range rw.retiring[gvr] {
		if !done[r] {
			keep = append(keep, r)
		}
	}
	if len(keep) == 0 {
		delete(rw.retiring, gvr)
		return
	}
	rw.retiring[gvr] = keep
}

// computeM is M = {k ∈ keys(new) ∪ keys(acted) : bytes(new[k]) ≠
// bytes(acted[k])}, over every handler's acted record, an absent key differing
// from a present one (design v5 §2.6 step 5). Sorted, for a deterministic
// delivery order.
func computeM(c *storeInstance) []string {
	idx := c.gi.Informer().GetIndexer()
	newObjs := map[string]any{}
	for _, obj := range idx.List() {
		if k, err := clientcache.MetaNamespaceKeyFunc(obj); err == nil {
			newObjs[k] = obj
		}
	}
	set := map[string]struct{}{}
	for _, a := range c.adaptersSnapshot() {
		if a.acted == nil {
			continue
		}
		acted := a.acted.snapshot()
		for k, nv := range newObjs {
			if av, ok := acted[k]; !ok || !sameBytes(av, nv) {
				set[k] = struct{}{}
			}
		}
		for k := range acted {
			if _, ok := newObjs[k]; !ok {
				set[k] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// swapNotifier is the exported OnUpdate every client-go shared informer has:
// it distributes to every listener (shared_informer.go OnUpdate).
type swapNotifier interface {
	OnUpdate(oldObj, newObj any)
}

func deliverM(c *storeInstance, m []string) {
	n, ok := c.gi.Informer().(swapNotifier)
	if !ok {
		slog.Error("cache.store.swap.undeliverable",
			slog.String("subsystem", "cache"),
			slog.String("gvr", c.gvr.String()),
			slog.Int("m_keys", len(m)),
			slog.String("hint", "the informer has no OnUpdate to distribute through; the keys that changed "+
				"were not delivered"))
		return
	}
	for _, k := range m {
		n.OnUpdate(swapDelivery{key: k}, swapDelivery{key: k})
	}
}

// markChangedPending makes pending, in every incremental cell of gvr, each key
// whose object in the new copy is not the very object the old copy held
// (absent on one side included).
func markChangedPending(gvr schema.GroupVersionResource, old, c *storeInstance) {
	if _, ok := listCellsByGVR.Load(gvr); !ok {
		return
	}
	oldIdx := old.gi.Informer().GetIndexer()
	newIdx := c.gi.Informer().GetIndexer()
	keys := map[string]struct{}{}
	for _, k := range oldIdx.ListKeys() {
		keys[k] = struct{}{}
	}
	for _, k := range newIdx.ListKeys() {
		keys[k] = struct{}{}
	}
	for k := range keys {
		o, oOK, _ := oldIdx.GetByKey(k)
		n, nOK, _ := newIdx.GetByKey(k)
		if oOK == nOK && (!oOK || o == n) {
			continue
		}
		ns, name, err := clientcache.SplitMetaNamespaceKey(k)
		if err != nil {
			continue
		}
		noteListCellsPending(gvr, ns, name)
	}
}

// openSwapLedger opens one ledger per reason of acc (store_swap_ledger.go).
// Dependents come from collectMatchesForMark over the reason's coordinates;
// only resident L1 keys of armable classes get an entry (C5b).
func openSwapLedger(gvr schema.GroupVersionResource, acc *buildAccount, m []string, installed time.Time) {
	inM := make(map[string]bool, len(m))
	for _, k := range m {
		inM[k] = true
	}
	d := Deps()
	store := resolvedCachePublished.Load()
	for _, reason := range acc.reasonList() {
		coords, ra := acc.coordinates(reason)
		if ra == nil {
			continue
		}
		var entries []ledgerOpenEntry
		for coord, detected := range coords {
			ns, name, err := clientcache.SplitMetaNamespaceKey(coord)
			if err != nil || d == nil {
				continue
			}
			for l1 := range d.collectMatchesForMark(gvr, ns, name) {
				if store == nil {
					continue
				}
				e, ok := store.GetNoTouch(l1)
				if !ok || e == nil || e.Inputs == nil || !armableEntryClass(e.Inputs.CacheEntryClass) {
					continue
				}
				entries = append(entries, ledgerOpenEntry{
					key:      l1,
					detected: detected,
					inM:      inM[coord],
					hasSub:   HasRefreshSubscriber(l1),
				})
			}
		}
		openLedger(ra, installed, entries)
	}
}

// ── teardown ─────────────────────────────────────────────────────────────────

// teardownSwapStateLocked aborts gvr's build and stops every copy of it (the
// candidate, the retiring ones). Called by deletePerGVRStateLocked, which then
// purges every per-GVR field. Returns the accounts to end as abort_teardown.
func (rw *ResourceWatcher) teardownSwapStateLocked(gvr schema.GroupVersionResource) []*buildAccount {
	var accs []*buildAccount
	if acc := rw.abortBuildLocked(gvr); acc != nil {
		accs = append(accs, acc)
	}
	for _, r := range rw.retiring[gvr] {
		r.closeStop()
	}
	if live := rw.instances[gvr]; live != nil {
		if live.account != nil {
			accs = append(accs, live.account)
			live.account = nil
		}
		dropComparatorRecord(gvr, live)
	}
	return accs
}

// ── verification bindings (design v5 §3) ─────────────────────────────────────

// liveInstance returns gvr's live instance.
func (rw *ResourceWatcher) liveInstance(gvr schema.GroupVersionResource) *storeInstance {
	if rw == nil {
		return nil
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.instances[gvr]
}

// noteVerifiedFor is noteVerified bound to the instance the pass compared: a
// result for an instance that is no longer live is DISCARDED (F6 — a pass open
// across a swap must not judge the new copy by the old one's comparison).
// rw.mu is held (read) across the registry write, so a swap cannot land
// between the check and the write. Returns discarded=true when the result was
// dropped.
func (rw *ResourceWatcher) noteVerifiedFor(gvr schema.GroupVersionResource, in *storeInstance, objects int, divergences uint64) (latched, discarded bool) {
	if rw == nil {
		return noteVerified(gvr, objects, divergences), false
	}
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	if in == nil || rw.instances[gvr] != in {
		recordVerifySkipped(verifySkipInstanceChanged)
		return false, true
	}
	return noteVerified(gvr, objects, divergences), false
}

// ── test seams ───────────────────────────────────────────────────────────────

// storeBuildForTest returns gvr's build candidate (nil when none).
func (rw *ResourceWatcher) storeBuildForTest(gvr schema.GroupVersionResource) *storeInstance {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	if b := rw.builds[gvr]; b != nil {
		return b.inst
	}
	return nil
}

// retiringForTest returns gvr's retiring copies.
func (rw *ResourceWatcher) retiringForTest(gvr schema.GroupVersionResource) []*storeInstance {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return append([]*storeInstance(nil), rw.retiring[gvr]...)
}
