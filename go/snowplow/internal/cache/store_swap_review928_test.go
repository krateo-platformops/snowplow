package cache

// store_swap_review928_test.go — the independent reviewer's arms for PR #928
// (#870 review, issuecomment-6500063618), adopted as written. Each arm makes
// its interleaving deterministic with hooks (the swap step hook, a held adapter
// mutex standing in for an unscheduled listener goroutine, a LIST reactor
// error), never with timing. The one change: RV-C and RV-D unmute the rig's
// adapter with the swapUnmute notification (r.nUnmute) instead of a flag,
// because the fix for F2 made the unmute a notification in listener order.

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func rev928ExtAdapter(t *testing.T, in *storeInstance) *handlerAdapter {
	t.Helper()
	for _, a := range in.adaptersSnapshot() {
		if strings.HasPrefix(a.name, "ext/") {
			return a
		}
	}
	t.Fatal("no extension adapter on the instance")
	return nil
}

// RV-A: the candidate's listener has NOT drained its muted backlog when the
// install unmutes it. Writes v2, v3 to o00 reach both copies during the build;
// the old copy delivers both to the consumer (acted = v3); the candidate's
// listener is still on v2 when the unmute happens. o00 is not in M (acted ==
// the new store's bytes). Expected (design v5 §2.7 / v5.1 C1: "each consumer's
// per-key sequence never goes backwards"; "delivered exactly once"): the
// consumer receives NOTHING more for o00 after the swap.
func TestRev928_A_LaggingCandidateListenerAtUnmute(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2)) // o00, o01 at rv 1

	installedCh := make(chan *storeInstance, 1)
	proceed := make(chan struct{})
	unmuteSent := make(chan struct{})
	var fired, sent atomic.Bool
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr {
			return
		}
		if step == "unmute_sent" && sent.CompareAndSwap(false, true) {
			close(unmuteSent)
			return
		}
		if step != "installed" || !fired.CompareAndSwap(false, true) {
			return
		}
		installedCh <- c
		<-proceed
	})

	// The divergence that makes M non-empty: o01 changed on the cluster, the
	// live copy never heard.
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	c := <-installedCh // step 1 done: the new copy serves reads; old copy still live

	ext := rev928ExtAdapter(t, c)
	f.waitFor("the candidate's muted initial list", func() bool {
		s := ext.seenSnapshot()
		return len(s) == 2
	})
	// Hold the candidate's consumer listener (as if its goroutine were not
	// scheduled): the next notification it pops blocks on the adapter mutex.
	ext.mu.Lock()
	held := true
	defer func() {
		if held {
			ext.mu.Unlock()
		}
	}()

	n0 := len(f.rec.all())
	for _, rv := range []string{"2", "3"} {
		o := swapObj(gvr, "o00", rv, "v"+rv)
		f.clusterSet(o)
		f.watcher(0).Modify(o.DeepCopy()) // the old (still running) copy
		f.watcher(1).Modify(o.DeepCopy()) // the candidate
	}
	f.settle(n0 + 2) // the old copy delivered v2 and v3 to the consumer
	f.waitFor("the candidate store at rv 3", func() bool {
		o, ok := c.storeGet(swapKey("o00"))
		return ok && rvOf(o) == "3"
	})
	pre := f.rec.forKey(swapKey("o00"))
	if last := pre[len(pre)-1]; last.rv != "3" {
		t.Fatalf("setup: the consumer's last o00 event is %v, want rv 3", last)
	}
	nPre := len(pre)

	// Every consumer of the OLD copy (the dep handler too) has acted on rv 3.
	f.waitFor("every acted record at o00 rv 3", func() bool {
		f.rw.mu.RLock()
		defer f.rw.mu.RUnlock()
		if len(f.rw.acted[gvr]) < 2 {
			return false
		}
		for _, r := range f.rw.acted[gvr] {
			if v, ok := r.get(swapKey("o00")); !ok || rvOf(v) != "3" {
				return false
			}
		}
		return true
	})
	close(proceed)
	// (adopted with one change, #928 F2's fix) The install has sent its
	// unmute while this listener is still held behind its muted backlog: the
	// listener is released here, so the arm observes the unmute's ORDER (the
	// backlog stays muted), whatever the install does next.
	<-unmuteSent
	ext.mu.Unlock()
	held = false
	f.awaitSwaps(1)
	if s := StoreSwapStatsSnapshot(); s.MKeys != 1 {
		t.Fatalf("setup: |M| = %d, want 1 (o01 only)", s.MKeys)
	}

	// o01's swap delivery is queued BEHIND every o00 notification on this
	// listener: once it reached the consumer, every o00 notification has too.
	f.waitFor("o01's swap delivery", func() bool {
		e := f.rec.forKey(swapKey("o01"))
		return len(e) > 0 && e[len(e)-1].rv == "2"
	})
	post := f.rec.forKey(swapKey("o00"))[nPre:]
	if len(post) != 0 {
		t.Fatalf("RV-A RED: after the swap the consumer received %d more o00 event(s) %v, having already "+
			"acted on rv 3 — a backwards step (rv 3 → 2) and a second application of rv 3", len(post), post)
	}
}

// RV-B: the candidate's FIRST LIST fails with a transient 500, the reflector
// retries, the copy syncs and is installed. Expected: the GVR is servable
// after the swap (the copy that was installed has listed successfully), and
// if it is not, the servability epoch must have moved (owner ruling 1).
func TestRev928_B_CandidateTransientListErrorThenSync(t *testing.T) {
	gvr := swapGVR(t)
	var failAt atomic.Int64
	f := newSwapFix(t, gvr, objs(gvr, 2), withListErr(func(n int64) error {
		if n == failAt.Load() {
			return apierrors.NewInternalError(errors.New("transient: etcd leader change"))
		}
		return nil
	}))
	f.confirm()
	if !f.rw.IsServable(gvr) {
		t.Fatal("setup: not servable before the build")
	}
	epoch0 := f.rw.ServabilityEpoch(gvr)
	f.mu.Lock()
	failAt.Store(f.lists + 1) // the candidate's first LIST at the reactor
	f.mu.Unlock()

	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	f.mu.Lock()
	lists := f.lists
	f.mu.Unlock()
	if lists < failAt.Load()+1 {
		t.Fatalf("setup: the candidate never retried its LIST (lists=%d)", lists)
	}
	servable := f.rw.IsServable(gvr)
	epoch := f.rw.ServabilityEpoch(gvr)
	if !servable {
		t.Fatalf("RV-B RED: the GVR is NOT servable after the swap of a copy that synced (epoch %d → %d): "+
			"the candidate's healed transient LIST error was copied into watchBroken at the install%s",
			epoch0, epoch, map[bool]string{true: "", false: " — and the epoch did not move (ruling 1)"}[epoch != epoch0])
	}
	_ = fmt.Sprint
}

// RV-C (kills RX2): after a suppression ends on S's own notification, the
// #854 comparator must judge the key SETTLED (the listener did deliver S): a
// stale `seen` leaves the key "in flight" forever, so a corrupted cell slot for
// it is never detected.
func TestRev928_C_SeenFollowsSuppressedDeliveries(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	// Store objects as production holds them: *bytesObject (the H5 streaming
	// shape), whose item bytes have a stable identity the comparator reads.
	bo := func(rv, v string) any {
		o, err := newBytesObject(swapObj(gvr, "k", rv, v))
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	v2 := bo("2", "v2")
	r := newAdapterRig(t, liveActedOn(t, v2), true)
	v3 := bo("3", "v3")
	r.write(v3)
	r.nAdd(v3)
	r.nUnmute()
	v4, v5 := bo("4", "v4"), bo("5", "v5")
	r.write(v4)
	r.write(v5)
	r.nSwap()
	r.nUpdate(v3, v4)
	r.nUpdate(v4, v5)
	comparatorRecord.Store(r.gvr, r.a)
	t.Cleanup(func() { comparatorRecord.Delete(r.gvr) })
	b, _ := storeItemJSON(v5)
	if deliveredMismatch(r.gvr, listObjKey{ns: swapNS, name: "k"}, idOf(b)) {
		t.Fatal("RV-C RED: after suppression ended on S's own notification the comparator still reads k as in flight (seen is stale)")
	}
}

// RV-D (kills RX3): S is absent and the listener still holds an older update
// for k AHEAD of the delete. The consumer must receive the delete exactly once
// (from the swap delivery): the pending update is suppressed and the delete's
// own notification swallowed.
func TestRev928_D_AbsentSSuppressesAPendingUpdate(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	v2 := swapObj(gvr, "k", "2", "v2")
	r := newAdapterRig(t, liveActedOn(t, v2), true)
	r.write(v2)
	r.nAdd(v2) // muted initial list: seen = v2
	r.nUnmute()
	v3 := r.obj("3", "v3")
	r.write(v3)
	r.del(v3) // S is absent
	r.nSwap()
	r.nUpdate(v2, v3)
	r.nDelete(v3)
	if got := r.seq(); !eqSeq(got, []string{"delete"}) {
		t.Fatalf("RV-D RED: consumer sequence %v, want exactly one delete", got)
	}
}

// RV-E (arm determinism): the fixture's awaitSwaps documents "every key of M
// has been offered", but it returns at step 3. Hold the install at
// after_step3 (before M, the ledger open and the delivery): awaitSwaps must
// NOT return while the hold is in place.
func TestRev928_E_AwaitSwapsWaitsForStep6(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	hold := make(chan struct{})
	held := make(chan struct{}, 1)
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr == gvr && step == "after_step3" {
			held <- struct{}{}
			<-hold
		}
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	<-held
	returned := make(chan struct{})
	go func() { f.awaitSwaps(1); close(returned) }()
	// awaitSwaps polls every 2 ms; it either returns now (RED) or keeps
	// waiting. The decision is read after the hold is released below, from
	// the ORDER of two events, not from a timeout.
	got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap)
	select {
	case <-returned:
		close(hold)
		t.Fatalf("RV-E RED: awaitSwaps returned while the install is held before step 5 (keys_total{divergence,swap}=%d at that moment)", got)
	case <-time.After(2 * time.Second):
		close(hold)
		<-returned
	}
}
