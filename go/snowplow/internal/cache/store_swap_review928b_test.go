package cache

// store_swap_review928b_test.go — the independent reviewer's delta arms for
// PR #928 (#870 delta review at a2e5e055, issuecomment-6509871392), adopted as
// written: RV-F1/RV-F2/RV-A' (the F2b equivalence under attack), RV-G (the
// pending flag after a swap, G1), RV-H (the latest candidate error decides)
// and RV-I (a behind copy's live step back is counted, G2). Deterministic: the
// swap step hook, a held adapter mutex standing in for an unscheduled listener
// goroutine, direct log seams. No timing decides a verdict.

import (
	"errors"
	"sync/atomic"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// r928bHold requests a divergence build on o01, stops the install at step
// `at`, holds the candidate's ext adapter (its listener blocks on the next
// notification it pops) after its muted initial list, runs during, then lets
// the install go on. extra (optional) runs at every step of the install.
func r928bHold(t *testing.T, f *swapFix, at string, during func(c *storeInstance), extra func(in *storeInstance, step string)) (c *storeInstance, release func()) {
	t.Helper()
	gvr := f.gvr
	reached := make(chan *storeInstance, 1)
	proceed := make(chan struct{})
	var fired atomic.Bool
	setSwapStepHook(t, func(in *storeInstance, step string) {
		if in.gvr != gvr {
			return
		}
		if extra != nil {
			extra(in, step)
		}
		if step != at || !fired.CompareAndSwap(false, true) {
			return
		}
		reached <- in
		<-proceed
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	c = <-reached
	ext := rev928ExtAdapter(t, c)
	f.waitFor("the candidate's muted initial list", func() bool { return len(ext.seenSnapshot()) == 2 })
	ext.mu.Lock()
	held := true
	t.Cleanup(func() {
		if held {
			ext.mu.Unlock()
		}
	})
	during(c)
	close(proceed)
	return c, func() {
		if held {
			held = false
			ext.mu.Unlock()
		}
	}
}

// landOnCandidate writes o00 at rv 5 on the candidate's watch only and waits
// until the candidate's STORE holds it.
func landOnCandidate(f *swapFix, in *storeInstance) {
	o := swapObj(f.gvr, "o00", "5", "w")
	f.clusterSet(o)
	f.watcher(1).Modify(o.DeepCopy())
	f.waitFor("the new store at rv 5", func() bool {
		s, ok := in.storeGet(swapKey("o00"))
		return ok && rvOf(s) == "5"
	})
}

// RV-F1 (attack on the F2b equivalence): with a consumer's listener held
// BEFORE the unmute marker, a write W lands on the new copy AFTER the marker is
// enqueued and before M is computed (hook unmute_sent). Each consumer must
// receive W exactly once, and nothing else for that key.
func TestR928b_F1_WriteBetweenMarkerAndMWithAHeldListener(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	var once atomic.Bool
	n0 := 0
	_, release := r928bHold(t, f, "installed", func(c *storeInstance) {
		n0 = len(f.rec.forKey(swapKey("o00")))
	}, func(in *storeInstance, step string) {
		if step == "unmute_sent" && once.CompareAndSwap(false, true) {
			landOnCandidate(f, in)
		}
	})
	f.awaitSwaps(1)
	release()
	f.fence()
	got := f.rec.forKey(swapKey("o00"))[n0:]
	if len(got) != 1 || got[0].rv != "5" {
		t.Fatalf("RV-F1 RED: after the swap the consumer saw %v for o00, want exactly one event at rv 5", got)
	}
}

// RV-F2: W lands BEFORE the marker (hook before_unmute) while the consumer's
// listener is held: W's own notification is queued before the marker (muted,
// seen only), so W must reach the consumer exactly once, through M.
func TestR928b_F2_WriteBeforeMarkerWithAHeldListener(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	var once atomic.Bool
	n0 := 0
	_, release := r928bHold(t, f, "installed", func(c *storeInstance) {
		n0 = len(f.rec.forKey(swapKey("o00")))
	}, func(in *storeInstance, step string) {
		if step == "before_unmute" && once.CompareAndSwap(false, true) {
			landOnCandidate(f, in)
		}
	})
	f.awaitSwaps(1)
	release()
	f.fence()
	got := f.rec.forKey(swapKey("o00"))[n0:]
	if len(got) != 1 || got[0].rv != "5" {
		t.Fatalf("RV-F2 RED: after the swap the consumer saw %v for o00, want exactly one event at rv 5", got)
	}
}

// RV-A' (my original RV-A form): the listener stays held through the WHOLE
// install (released only after awaitSwaps), so the arm also proves the install
// does not wait on listeners.
func TestR928b_A_HeldThroughTheWholeInstall(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	nPre := 0
	_, release := r928bHold(t, f, "installed", func(c *storeInstance) {
		n0 := len(f.rec.all())
		for _, rv := range []string{"2", "3"} {
			o := swapObj(gvr, "o00", rv, "v"+rv)
			f.clusterSet(o)
			f.watcher(0).Modify(o.DeepCopy())
			f.watcher(1).Modify(o.DeepCopy())
		}
		f.settle(n0 + 2)
		f.waitFor("the candidate store at rv 3", func() bool {
			o, ok := c.storeGet(swapKey("o00"))
			return ok && rvOf(o) == "3"
		})
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
		nPre = len(f.rec.forKey(swapKey("o00")))
	}, nil)
	f.awaitSwaps(1) // must complete with the listener still held
	release()
	f.fence()
	if post := f.rec.forKey(swapKey("o00"))[nPre:]; len(post) != 0 {
		t.Fatalf("RV-A' RED: %v after the swap", post)
	}
}

// RV-G (#923 handover): the live copy's delivery log holds a pending key when a
// swap installs. The install closes that log and its pending keys go with it,
// so the GVR's walker flag must not keep reading "pending": a flag that
// outlives its keys keeps the GVR in the walker's flagged slot, served every
// other turn whatever its deadline.
func TestR928b_G_PendingFlagDoesNotOutliveTheReplacedLog(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	live := f.live()
	if live.dl == nil {
		t.Fatal("setup: the live copy has no delivery log")
	}
	// A read ahead of the store's applied position: the judge holds it pending.
	live.dl.setPending(map[string]readState{swapKey("o00"): {present: true, rev: "99999"}})
	if row, ok := verificationRowFor(gvr); !ok || !row.hasPending || live.dl.pendingCount() != 1 {
		t.Fatalf("setup: the GVR is not flagged pending (pending=%d)", live.dl.pendingCount())
	}
	var atInstall atomic.Int64
	var flagAtInstall atomic.Bool
	atInstall.Store(-1)
	setSwapStepHook(t, func(in *storeInstance, step string) {
		if in.gvr == gvr && step == "before_install" {
			atInstall.Store(int64(live.dl.pendingCount()))
			row, _ := verificationRowFor(gvr)
			flagAtInstall.Store(row.hasPending)
		}
	})
	t.Logf("debug: pending after setPending=%d resolved_received=%d lost=%d undecided=%d", live.dl.pendingCount(),
		storePendingResolvedReceivedTotal.Load(), storePendingResolvedLostTotal.Load(), storeUndecidedTotal.Load())
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	t.Logf("debug: pending after clusterSet=%d", live.dl.pendingCount())
	f.request(reasonDivergence, swapKey("o01"))
	t.Logf("debug: pending after request=%d", live.dl.pendingCount())
	f.awaitSwaps(1)
	t.Logf("debug: counters after swap received=%d lost=%d undecided=%d superseded=%d", storePendingResolvedReceivedTotal.Load(),
		storePendingResolvedLostTotal.Load(), storeUndecidedTotal.Load(), storePendingSupersededTotal.Load())
	if atInstall.Load() != 1 || !flagAtInstall.Load() {
		t.Fatalf("setup: at the install the replaced log held %d pending keys (flag %v), want 1 (true)", atInstall.Load(), flagAtInstall.Load())
	}
	if f.live() == live {
		t.Fatal("setup: no swap")
	}
	if n := f.live().dl.pendingCount(); n != 0 {
		t.Fatalf("setup: the new log holds %d pending keys", n)
	}
	if row, _ := verificationRowFor(gvr); row.hasPending {
		// The consequence: the walker picks this GVR although its epoch was
		// just reset by the install (not overdue) and nothing is pending.
		picked, _, ok := nextOverdueGVR()
		t.Fatalf("RV-G RED: the replaced log's pending key was dropped at the install, but the GVR is still flagged "+
			"pending (walker picks %v ok=%v; this GVR=%v)", picked, ok, gvr)
	}
}

// RV-H (F1, two errors): the candidate's first LIST fails (an error before any
// position), the retry syncs, and then its watch drops AFTER the sync with no
// advance. The install must judge the LATEST error: the copy's watch is down
// now, so the GVR must not be servable after the install.
func TestR928b_H_LatestErrorDecidesNotTheFirst(t *testing.T) {
	gvr := swapGVR(t)
	var failAt atomic.Int64
	f := newSwapFix(t, gvr, objs(gvr, 2), withListErr(func(n int64) error {
		if n == failAt.Load() {
			return apierrors.NewInternalError(errors.New("transient"))
		}
		return nil
	}))
	f.confirm()
	f.mu.Lock()
	failAt.Store(f.lists + 1)
	f.mu.Unlock()
	var once atomic.Bool
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr || step != "before_install" || !once.CompareAndSwap(false, true) {
			return
		}
		f.rw.watchErrorHandlerForInstance(gvr, c)(nil, apierrors.NewInternalError(errors.New("watch dropped after sync")))
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	f.mu.Lock()
	lists := f.lists
	f.mu.Unlock()
	if lists < failAt.Load()+1 {
		t.Fatalf("setup: the candidate never retried its LIST (lists=%d)", lists)
	}
	if f.rw.IsServable(gvr) {
		t.Fatal("RV-H RED: the install judged the FIRST error (healed by the retry) instead of the latest (a watch drop after the sync): the GVR serves a copy whose watch is down")
	}
}

// RV-I (F8 counting): the new copy's WATCH is behind the old copy's at the
// swap (old delivered v4; the new store holds v2), and the new copy's v3
// reaches the consumer LIVE after the unmute, before o00's swap delivery. The
// consumer then steps backwards (v4 → v3). F8 says such a regression is
// counted (store_swap_regressed_keys).
func TestR928b_I_BehindCopyLiveRegressionIsCounted(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	var atInstall, atM atomic.Bool
	n0 := 0
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr {
			return
		}
		switch {
		case step == "installed" && atInstall.CompareAndSwap(false, true):
			n0 = len(f.rec.all())
			v2 := swapObj(gvr, "o00", "2", "v2")
			f.clusterSet(v2)
			f.watcher(0).Modify(v2.DeepCopy())
			f.watcher(1).Modify(v2.DeepCopy())
			for _, rv := range []string{"3", "4"} {
				o := swapObj(gvr, "o00", rv, "v"+rv)
				f.clusterSet(o)
				f.watcher(0).Modify(o.DeepCopy()) // the old copy only
			}
			f.settle(n0 + 3)
			f.waitFor("every acted record at o00 rv 4 and the new store at rv 2", func() bool {
				s, ok := c.storeGet(swapKey("o00"))
				if !ok || rvOf(s) != "2" {
					return false
				}
				f.rw.mu.RLock()
				defer f.rw.mu.RUnlock()
				for _, r := range f.rw.acted[gvr] {
					if v, ok := r.get(swapKey("o00")); !ok || rvOf(v) != "4" {
						return false
					}
				}
				return len(f.rw.acted[gvr]) >= 2
			})
		case step == "after_m" && atM.CompareAndSwap(false, true):
			o := swapObj(gvr, "o00", "3", "v3")
			f.watcher(1).Modify(o.DeepCopy()) // the new copy's lagging watch catches up to v3
			f.waitFor("the consumer to receive v3 live", func() bool {
				e := f.rec.forKey(swapKey("o00"))
				return len(e) > 0 && e[len(e)-1].rv == "3"
			})
		}
	})
	r0 := StoreSwapStatsSnapshot().RegressedKeys
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	f.fence()
	evs := f.rec.forKey(swapKey("o00"))
	backwards := false
	for i := 1; i < len(evs); i++ {
		if evs[i-1].rv == "4" && evs[i].rv == "3" {
			backwards = true
		}
	}
	if !backwards {
		t.Fatalf("setup: no backwards step observed: %v", evs)
	}
	if got := StoreSwapStatsSnapshot().RegressedKeys - r0; got == 0 {
		t.Fatalf("RV-I RED: the consumer stepped back v4 → v3 at the swap (%v) and store_swap_regressed_keys did not move", evs)
	}
}
