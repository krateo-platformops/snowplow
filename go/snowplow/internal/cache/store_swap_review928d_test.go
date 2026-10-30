package cache

// store_swap_review928d_test.go — the reviewer's arms from the #928 delta
// review at 21905c27 (issuecomment-6511787720), adopted as written except
// where noted. H1: the walker flag is written under the log's own mutex and
// only the log that set it can clear it (RV-J1..J4). G2: an unorderable pair
// is no step back (RV-K). The repair binding holds on the join path (RV-L) and
// at the forced pass's call site (RV-M).

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// RV-J1 (G1; H1's owner check): a build copy's log that is dropped
// (superseded) holds nothing pending; its close must not clear the flag the
// LIVE log set. Pinned the n>0 guard at 21905c27; pins the owner check now.
func TestR928d_J1_ADroppedCandidateLogLeavesTheLiveFlag(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	live := f.live()
	live.dl.setPending(map[string]readState{swapKey("o00"): {present: true, rev: "99999"}})
	if row, ok := verificationRowFor(gvr); !ok || !row.hasPending {
		t.Fatal("setup: not flagged")
	}
	holdBuildsForTest.Store(true)
	t.Cleanup(func() { holdBuildsForTest.Store(false) })
	f.request(reasonSchema)
	first := f.rw.storeBuildForTest(gvr)
	if first == nil || first.dl == nil {
		t.Fatal("setup: the held build has no copy or no log")
	}
	f.request(reasonSchema) // supersedes: the first copy's log is closed
	if f.rw.storeBuildForTest(gvr) == first {
		t.Fatal("setup: not superseded")
	}
	if n := live.dl.pendingCount(); n != 1 {
		t.Fatalf("setup: live log holds %d pending", n)
	}
	if row, _ := verificationRowFor(gvr); !row.hasPending {
		t.Fatal("RV-J1 RED: closing a superseded candidate's (empty) log cleared the flag the live log's pending key set")
	}
}

// RV-J2 (G1 exactness, natural race, no hook): when setPending wins l.mu and
// close() then drops its key, close's flag clear must land AFTER setPending's
// set. Written outside l.mu (head 21905c27), the clear could land first: the
// closed log held nothing and the GVR stayed flagged (G1 again).
func TestR928d_J2_SetPendingRacingCloseNeverStrandsTheFlag(t *testing.T) {
	resetVerificationForArm(t)
	gvr := swapGVR(t)
	rememberStoreVerification(gvr, true, true)
	unflag := func() { // adopted change: the flag now names its log
		storeVerify.mu.Lock()
		storeVerify.gvrs[gvr].pendingLog = nil
		storeVerify.mu.Unlock()
	}
	const iters = 20000
	stranded := 0
	for i := 0; i < iters; i++ {
		unflag()
		l := newDeliveryLog(gvr)
		l.indexer = clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
		l.applied = func() string { return "1" }
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			l.setPending(map[string]readState{"ns/k": {present: true, rev: "99999"}})
		}()
		go func() { defer wg.Done(); <-start; l.close() }()
		close(start)
		wg.Wait()
		row, _ := verificationRowFor(gvr)
		if l.pendingCount() == 0 && row.hasPending {
			stranded++
		}
	}
	unflag()
	t.Logf("RV-J2: %d/%d iterations left a closed, empty log's GVR flagged", stranded, iters)
	if stranded > 0 {
		t.Fatalf("RV-J2 RED: %d/%d setPending/close interleavings stranded the walker flag (G1 by a race)", stranded, iters)
	}
}

// RV-J3 (G1 converse: a replaced log's resolver clears the NEW log's flag).
// The resolver of the live log L1 resolves a loss and publishes it; the swap
// installs L2 and L2 holds a key pending while L1's resolver is still inside
// its publish. At 21905c27 L1's "pending set is empty" clear came after that
// publish and so hid L2's pending key from the walker; it is now made under
// L1's mutex as the set empties, and owner-checked.
func TestR928d_J3_AReplacedLogsResolverDoesNotClearTheNewLogsFlag(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	l1 := f.live().dl
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	hook := func() {
		once.Do(func() {
			close(reached)
			<-release
		})
	}
	resolvedLossPublishedHookForTest.Store(&hook)
	t.Cleanup(func() { resolvedLossPublishedHookForTest.Store(nil) })
	// A read behind the list RV (4242) and ahead of the store (rv 1): the
	// resolver's first judgement is a loss, and its repair builds a copy.
	l1.setPending(map[string]readState{swapKey("o00"): {present: true, rev: "3"}})
	select {
	case <-reached:
	case <-time.After(30 * time.Second):
		t.Fatal("setup: the resolver never published a loss")
	}
	f.awaitSwaps(1)
	l2 := f.live().dl
	if l2 == nil || l2 == l1 {
		t.Fatal("setup: no new log")
	}
	l2.setPending(map[string]readState{swapKey("o01"): {present: true, rev: "99999"}})
	if row, _ := verificationRowFor(gvr); !row.hasPending {
		t.Fatal("setup: the new log's pending key did not flag the GVR")
	}
	sw := l1.sweeps.Load()
	close(release)
	f.waitFor("the old resolver to finish", func() bool { return l1.sweeps.Load() > sw })
	if n := l2.pendingCount(); n != 1 {
		t.Fatalf("setup: the new log holds %d pending", n)
	}
	if row, _ := verificationRowFor(gvr); !row.hasPending {
		t.Fatal("RV-J3 RED: the replaced log's resolver cleared the GVR's walker flag while the NEW live log holds a pending key")
	}
}

// RV-J4 (G1, the closed check): a pass that straddles the swap holds keys on
// the replaced log AFTER close(): nothing may be held and nothing flagged.
func TestR928d_J4_SetPendingOnAClosedLogFlagsNothing(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	old := f.live()
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	old.dl.setPending(map[string]readState{swapKey("o00"): {present: true, rev: "99999"}})
	if n := old.dl.pendingCount(); n != 0 {
		t.Fatalf("RV-J4 RED: the closed log holds %d pending keys", n)
	}
	if row, _ := verificationRowFor(gvr); row.hasPending {
		t.Fatal("RV-J4 RED: a closed log's setPending flagged the GVR")
	}
}

// RV-K (G2, unorderable pair): a live step whose resourceVersions the API does
// not let us order is not counted.
func TestR928d_K_UnorderablePairIsNotCounted(t *testing.T) {
	r := newAdapterRigNamed(t, liveActedOn(t, r928dObj("b7", "v2")), false, depHandlerName)
	before := storeSwapStats.regressedKeys.Load()
	r.write(r928dObj("a3", "v3"))
	r.nUpdate(r928dObj("b7", "v2"), r928dObj("a3", "v3"))
	if got := storeSwapStats.regressedKeys.Load() - before; got != 0 {
		t.Fatalf("RV-K RED: an unorderable pair moved store_swap_regressed_keys by %d", got)
	}
}

func r928dObj(rv, v string) any {
	return swapObj(adapterRigGVRForR928d, "k", rv, v)
}

var adapterRigGVRForR928d = schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}

// RV-L (binding, join path): a loss judged on a replaced log must not JOIN a
// running build either.
func TestR928d_L_AStaleLossDoesNotJoinARunningBuild(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	oldLog := f.live().dl
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	holdBuildsForTest.Store(true)
	t.Cleanup(func() { holdBuildsForTest.Store(false) })
	f.request(reasonSchema)
	if f.rw.storeBuildForTest(gvr) == nil {
		t.Fatal("setup: no running build")
	}
	fired0 := storeRepairsFiredTotal.Load()
	publishResolvedLoss(oldLog, []divergenceCandidate{{key: swapKey("o00"), class: classLostUpdate}})
	f.rw.mu.RLock()
	hasDiv := f.rw.builds[gvr] != nil && f.rw.builds[gvr].account.has(reasonDivergence)
	f.rw.mu.RUnlock()
	if hasDiv || storeRepairsFiredTotal.Load() != fired0 {
		t.Fatal("RV-L RED: a loss judged on the replaced copy joined the running build as a divergence")
	}
}

// RV-M (binding, forced-pass call site): a swap landing after the pass's
// noteVerifiedFor and before its repair request drops the request.
type r928dSlogHook struct {
	inner slog.Handler
	on    func(r slog.Record)
}

func (h *r928dSlogHook) Enabled(context.Context, slog.Level) bool { return true }
func (h *r928dSlogHook) Handle(ctx context.Context, r slog.Record) error {
	h.on(r)
	if h.inner.Enabled(ctx, r.Level) {
		return h.inner.Handle(ctx, r)
	}
	return nil
}
func (h *r928dSlogHook) WithAttrs(a []slog.Attr) slog.Handler {
	return &r928dSlogHook{inner: h.inner.WithAttrs(a), on: h.on}
}
func (h *r928dSlogHook) WithGroup(n string) slog.Handler {
	return &r928dSlogHook{inner: h.inner.WithGroup(n), on: h.on}
}

func TestR928d_M_ForcedPassRequestIsBoundToTheComparedInstance(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	if got := storePendingKeys.Load(); got != 1 {
		t.Fatalf("precondition: %d pending keys, want 1", got)
	}
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	j9Bookmark(t, rw, repairGVR, "5100")

	var swapped atomic.Bool
	var orig *storeInstance
	prev := slog.Default()
	slog.SetDefault(slog.New(&r928dSlogHook{inner: slog.NewTextHandler(io.Discard, nil), on: func(r slog.Record) {
		if r.Message != "cache.store.divergent" || !swapped.CompareAndSwap(false, true) {
			return
		}
		rw.mu.Lock()
		orig = rw.instances[repairGVR]
		rw.instances[repairGVR] = newStoreInstance(repairGVR, orig.gi, orig.shape, orig.stop)
		rw.mu.Unlock()
	}}))
	t.Cleanup(func() {
		slog.SetDefault(prev)
		if orig != nil {
			rw.mu.Lock()
			rw.instances[repairGVR] = orig
			rw.mu.Unlock()
		}
	})
	r0 := storeRepairsRequestedForTest()
	s0 := VerifySkippedByReasonSnapshot()[verifySkipInstanceChanged]
	cli.answer("5150", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	if !swapped.Load() {
		t.Fatal("setup: the pass published no divergence")
	}
	if got := j9Class("update"); got != 1 {
		t.Fatalf("setup: %d lost updates published, want 1", got)
	}
	if d := storeRepairsRequestedForTest() - r0; d != 0 {
		t.Fatalf("RV-M RED: a forced pass's loss on the replaced instance requested %d repair(s)", d)
	}
	if d := VerifySkippedByReasonSnapshot()[verifySkipInstanceChanged] - s0; d != 1 {
		t.Fatalf("RV-M: instance_changed moved by %d, want 1", d)
	}
}
