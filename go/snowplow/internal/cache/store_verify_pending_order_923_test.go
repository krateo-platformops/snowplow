package cache

// store_verify_pending_order_923_test.go — the store_divergence_pending_keys
// invariant (store_verify_delivery.go storePendingKeys): a pending key leaves
// the gauge only after its resolved outcome is FULLY published — divergence
// recorded and repair enqueued. A reader that waits for the gauge to reach 0
// (every #918 arm does) must find the outcome there.
//
// Before, the gauge was decremented first: on #923 rebased onto main, under
// load, TestForcedPass918_PendingKey_ResolvedLostByAnAppliedEventPastIt/owned
// read 0 pending and then no repair queued (store_repairs_pending = 0). These
// arms make that window deterministic instead of load-dependent: they look at
// the gauge AT the instant between publishing and decrementing.
//
//   O1  the resolver (an applied event resolves a pending key to a loss):
//       at the hook right after the loss is published, the key is still
//       pending in the gauge and in the per-GVR breakdown, and the divergence
//       and (owned GVR) the repair are already there.
//   O2  the next forced pass (a quiet GVR: the pass judges the carried key
//       against its own read): while the pass's LIST is in flight the carried
//       loss is not published yet, so the key is still pending; once the pass
//       returns it is published and no longer pending.

import (
	"context"
	"testing"
)

func TestPendingOrder923_O1_TheResolverPublishesBeforeTheKeyLeavesTheGauge(t *testing.T) {
	for _, origin := range []struct {
		name  string
		owned bool
	}{{"owned", true}, {"factory", false}} {
		t.Run(origin.name, func(t *testing.T) {
			gvr := factory875GVR
			if origin.owned {
				gvr = repairGVR
			}
			rw, cli := j9Setup(t, gvr, origin.owned, ja1)
			cli.answer("5002", ja2, jclock)
			forcedVerify(context.Background(), gvr)
			if got := storePendingKeys.Load(); got != 1 {
				t.Fatalf("precondition: %d pending keys, want 1", got)
			}
			type seen struct {
				pending, perGVR int64
				lostUpdates     uint64
				repairs         int64
			}
			at := make(chan seen, 1)
			hook := func() {
				s := seen{pending: storePendingKeys.Load(), perGVR: PendingByGVRSnapshot()[gvr.String()],
					lostUpdates: j9Class("update"), repairs: storeRepairsRequestedForTest()}
				select {
				case at <- s:
				default:
				}
			}
			resolvedLossPublishedHookForTest.Store(&hook)
			t.Cleanup(func() { resolvedLossPublishedHookForTest.Store(nil) })

			dl := j9Log(t, rw, gvr)
			j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
			j9Deliver(t, rw, gvr, jmod(j9Object{jclock.name, "5100", jclock.uid}))
			var s seen
			j9WaitFor(t, "the resolver publishing the loss", func() bool {
				select {
				case s = <-at:
					return true
				default:
					return false
				}
			})
			if s.lostUpdates != 1 {
				t.Fatalf("SETUP: at the hook %d lost updates are published, want 1", s.lostUpdates)
			}
			if origin.owned && s.repairs != 1 {
				t.Fatalf("SETUP: at the hook %d repairs are queued, want 1", s.repairs)
			}
			if s.pending != 1 || s.perGVR != 1 {
				t.Fatalf("RED: the key left the pending gauge before its loss was fully published "+
					"(at the hook: store_divergence_pending_keys=%d, per-GVR=%d, want 1/1) — a reader that "+
					"waits for 0 can then find the repair not yet queued", s.pending, s.perGVR)
			}
			j9WaitFor(t, "the key leaving the gauge", func() bool { return storePendingKeys.Load() == 0 })
			if got := PendingByGVRSnapshot()[gvr.String()]; got != 0 {
				t.Fatalf("per-GVR breakdown still shows %d pending after the gauge reached 0", got)
			}
		})
	}
}

func TestPendingOrder923_O2_TheNextPassPublishesBeforeTheCarriedKeyLeavesTheGauge(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	if got := storePendingKeys.Load(); got != 1 {
		t.Fatalf("precondition: %d pending keys, want 1", got)
	}
	dl := j9Log(t, rw, repairGVR)
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	j9Bookmark(t, rw, repairGVR, "5100") // moves the position; no callback (see the quiet-GVR arm)

	var duringPending, duringPerGVR int64
	var duringLost uint64
	cli.duringList = func() {
		duringPending = storePendingKeys.Load()
		duringPerGVR = PendingByGVRSnapshot()[repairGVR.String()]
		duringLost = j9Class("update")
	}
	cli.answer("5150", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)

	if duringLost != 0 {
		t.Fatalf("SETUP: %d lost updates were published before the pass's LIST, want 0 (the carried loss is "+
			"published with the pass)", duringLost)
	}
	if duringPending != 1 || duringPerGVR != 1 {
		t.Fatalf("RED: during the deciding pass's LIST the carried loss was not published yet but had left the "+
			"pending gauge (store_divergence_pending_keys=%d, per-GVR=%d, want 1/1)", duringPending, duringPerGVR)
	}
	if got := j9Class("update"); got != 1 {
		t.Fatalf("the deciding pass published %d lost updates, want 1", got)
	}
	if d := storeRepairsRequestedForTest(); d != 1 {
		t.Fatalf("repairs requested = %d after the deciding pass, want 1", d)
	}
	if got := storePendingKeys.Load(); got != 0 {
		t.Fatalf("%d keys still pending after the deciding pass published its loss", got)
	}
	if got := PendingByGVRSnapshot()[repairGVR.String()]; got != 0 {
		t.Fatalf("per-GVR breakdown still shows %d pending after the deciding pass", got)
	}
}
