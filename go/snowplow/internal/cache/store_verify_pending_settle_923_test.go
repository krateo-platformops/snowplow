package cache

// store_verify_pending_settle_923_test.go — THE ONE RULE of the pending set
// (store_verify_delivery.go settleLocked): a pending key is settled EXACTLY
// ONCE, by whichever path removes it from the pending set under the log's lock.
// The arms are reviewer-923's probes RP1–RP4 (#923 delta review), adopted.
//
//   RP1 a loss carried into the next pass, whose LIST fails: published with
//       that pass (publishCarried), repaired, and no longer pending.
//   RP2 the same when the pass has no published position (beginPass == ""):
//       its other early exit.
//   RP3 the resolver and the next pass's judgePending judge the SAME pending
//       key: the resolver is parked after its copy of the set (at its position
//       read), the pass decides the loss and its LIST fails, then the resolver
//       resumes. One key, one loss: published once, counted once, and the gauge
//       ends at 0 — never below. RED before the rule: published twice,
//       resolved_lost_total 2, gauge -1 (and lower on every repeat: the gauge is
//       process-wide).
//   RP4 the same with a LIST that succeeds: the parked resolver finishes while
//       the pass's LIST is in flight, before the pass supersedes anything.
//   J2  the resolver meets an unknown (empty) applied position: the key stays
//       pending, no loss is published (#923 review F4).
//
// No sleeps: the resolver's sweep counter moves once its judgement is complete
// (settled, published and counted), and the arms wait on it.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPendingSettle923_RP1_CarriedLossOnAPassWhoseListFails(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	if got := storePendingKeys.Load(); got != 1 {
		t.Fatalf("precondition: %d pending keys, want 1", got)
	}
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	j9Bookmark(t, rw, repairGVR, "5100")
	var during int64
	cli.duringList = func() { during = storePendingKeys.Load() }
	cli.mu.Lock()
	cli.listErr = errors.New("unavailable")
	cli.mu.Unlock()
	forcedVerify(context.Background(), repairGVR)
	if during != 1 {
		t.Errorf("RP1: %d pending during the failing LIST, want 1 (the carried loss is not published yet)", during)
	}
	if got := j9Class("update"); got != 1 {
		t.Fatalf("RP1 RED: the pass whose LIST failed published %d lost updates, want its carried 1", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Errorf("RP1 RED: store_repairs_pending = %d, want 1", d)
	}
	if got := storePendingKeys.Load(); got != 0 {
		t.Errorf("RP1 RED: %d pending after the pass, want 0", got)
	}
	if got := PendingByGVRSnapshot()[repairGVR.String()]; got != 0 {
		t.Errorf("RP1 RED: per-GVR %d pending after the pass, want 0", got)
	}
}

func TestPendingSettle923_RP2_CarriedLossOnAPassWithNoPublishedPosition(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	j9Bookmark(t, rw, repairGVR, "5100")
	dl.published = func() string { return "" }
	forcedVerify(context.Background(), repairGVR)
	if got := j9Class("update"); got != 1 {
		t.Fatalf("RP2 RED: the pass with no published position published %d lost updates, want its carried 1", got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Errorf("RP2 RED: store_repairs_pending = %d, want 1", d)
	}
	if got := storePendingKeys.Load(); got != 0 {
		t.Errorf("RP2 RED: %d pending after the pass, want 0", got)
	}
	if got := PendingByGVRSnapshot()[repairGVR.String()]; got != 0 {
		t.Errorf("RP2 RED: per-GVR %d pending after the pass, want 0", got)
	}
}

// rp923ParkResolver arms a one-shot park of dl's resolver at its position read,
// which comes after its copy of the pending set; then delivers the applied
// event that wakes it and waits until it is parked. It returns un-park and the
// sweep count before the wake.
func rp923ParkResolver(t *testing.T, rw *ResourceWatcher, dl *deliveryLog) (unpark func(), sweeps0 uint64) {
	t.Helper()
	orig := dl.applied
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	dl.applied = func() string {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		return orig()
	}
	armed.Store(true)
	sweeps0 = dl.sweeps.Load()
	j9Deliver(t, rw, repairGVR, jmod(j9Object{jclock.name, "5100", jclock.uid}))
	select {
	case <-entered:
	case <-time.After(verifyBound):
		t.Fatalf("precondition: the resolver never reached its position read")
	}
	var once atomic.Bool
	return func() {
		if once.CompareAndSwap(false, true) {
			close(release)
		}
	}, sweeps0
}

func rp923AssertSettledOnce(t *testing.T, arm string) {
	t.Helper()
	if got := j9Class("update"); got != 1 {
		t.Errorf("%s RED: one lost key was published %d times, want 1 — both paths settled it", arm, got)
	}
	if got := storePendingResolvedLostTotal.Load(); got != 1 {
		t.Errorf("%s RED: store_divergence_pending_resolved_lost_total = %d for one key, want 1", arm, got)
	}
	if got := storePendingKeys.Load(); got != 0 {
		t.Errorf("%s RED: store_divergence_pending_keys = %d, want 0 (below 0 is a double decrement of one key)", arm, got)
	}
	if got := PendingByGVRSnapshot()[repairGVR.String()]; got != 0 {
		t.Errorf("%s RED: per-GVR %d pending, want 0", arm, got)
	}
	if d := storeRepairQueueDepth(); d != 1 {
		t.Errorf("%s: store_repairs_pending = %d, want 1", arm, d)
	}
}

func TestPendingSettle923_RP3_TheResolverAndThePassJudgeTheSameKey(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })

	unpark, sweeps0 := rp923ParkResolver(t, rw, dl)
	t.Cleanup(unpark)
	cli.mu.Lock()
	cli.listErr = errors.New("unavailable")
	cli.mu.Unlock()
	forcedVerify(context.Background(), repairGVR) // judgePending decides the loss; the LIST fails
	if got := j9Class("update"); got != 1 {
		t.Fatalf("SETUP: the pass published %d lost updates, want 1 (it must decide the loss first)", got)
	}
	unpark()
	j9WaitFor(t, "the parked resolver completing its judgement", func() bool { return dl.sweeps.Load() > sweeps0 })
	rp923AssertSettledOnce(t, "RP3")
}

func TestPendingSettle923_RP4_TheResolverFinishesDuringThePassesList(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })

	unpark, sweeps0 := rp923ParkResolver(t, rw, dl)
	t.Cleanup(unpark)
	var duringResolved bool
	cli.duringList = func() {
		// judgePending has decided the loss; the pass has not superseded yet.
		unpark()
		j9WaitFor(t, "the parked resolver completing its judgement", func() bool { return dl.sweeps.Load() > sweeps0 })
		duringResolved = true
	}
	cli.answer("5150", ja2, j9Object{jclock.name, "5100", jclock.uid})
	forcedVerify(context.Background(), repairGVR)
	if !duringResolved {
		t.Fatalf("SETUP: the resolver did not complete during the pass's LIST")
	}
	rp923AssertSettledOnce(t, "RP4")
}

// TestPendingSettle923_J2_AnUnknownAppliedPositionKeepsTheKeyPending — #923
// review F4 (mutation J2 survived). The lost-update rule decides "lost" only
// once the store's applied position has reached the read; a position the store
// cannot report (empty) orders nothing, so the key stays pending. A forced pass
// never runs without a position (it is skipped, verifySkipNoSyncPosition), so
// the resolver is the one path that can meet it: woken by an applied event
// while the position reads empty, it must not publish a loss.
func TestPendingSettle923_J2_AnUnknownAppliedPositionKeepsTheKeyPending(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	if got := storePendingKeys.Load(); got != 1 {
		t.Fatalf("precondition: %d pending keys, want 1", got)
	}
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	dl.applied = func() string { return "" }
	sweeps0 := dl.sweeps.Load()
	j9Deliver(t, rw, repairGVR, jmod(j9Object{jclock.name, "5100", jclock.uid}))
	j9WaitFor(t, "the resolver judging with no applied position", func() bool { return dl.sweeps.Load() > sweeps0 })
	if got := j9Class("update"); got != 0 {
		t.Fatalf("J2 RED: %d lost updates published while the store's applied position was unknown, want 0", got)
	}
	if got := storePendingKeys.Load(); got != 1 {
		t.Fatalf("J2 RED: %d pending keys, want the key still pending (1)", got)
	}
}
