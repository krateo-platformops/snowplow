// issue734_known_stale_put_settle_test.go — #734: an accepted re-Put ends the
// KNOWN-STALE state in the same store critical section that publishes the body.
//
// THE RACE (traced on 44fada34). The refresher's own re-Put of a known-stale cell
// lands while its dequeue stamp is IN FLIGHT. closeKnownStaleOnPut left an
// in-flight stamp to the dequeue, and ReplaceIfGenRefresh did not call it at all,
// so the stale flag cleared only when processNext later reached settleDequeue.
// In that gap every reader saw the FRESH body flagged known-stale: Get counted
// known_stale_served for it, an identity-bound read started a demand revalidation
// of a body that was already fresh (demand_revalidate.go), and a substrate reader
// read the fresh cell through as a MISS (KnownStaleReadThrough). F7 of #547 caught
// it as "the healed shell is still known-stale" (1/60 at -count=60; 3/3 with a
// 100 ms sleep before settleDequeue).
//
// ARMS. Both drive the REAL refresher loop over a real store; the seam is the
// RefreshFunc, which performs the refresher terminal write (ReplaceIfGenRefresh
// under a WithL1KeyContext resolve ctx, then NoteRefreshWrite) exactly as
// resolveAndPopulateL1 does, and observes the cell from INSIDE the dequeue —
// after the Put, before processNext settles the window.
//
//	K1  an accepted re-Put of a known-stale cell: inside the dequeue the cell is
//	    no longer known-stale, a customer Get counts no known-stale serve, and
//	    no substrate read-through fires. After the settle the window is closed
//	    with ONE dirty→fresh sample (the dequeue's, not a second one).
//	K2  a mark that lands DURING the resolve (the dep moved after it began) keeps
//	    the cell known-stale through the Put: the body was read from a state the
//	    latest mark has superseded, so nothing may report it fresh.
package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type k734Seen struct {
	knownStale  bool
	servedDelta uint64
	readThrough bool
	body        string
}

// k734KnownStaleCell makes one resident widgets cell KNOWN-STALE (dropped) through
// the real loop, then returns the running refresher's controls: ok flips the
// handler to an accepted terminal write; during, when set, runs inside the
// handler after the resolve began and before the Put.
func k734KnownStaleCell(t *testing.T) (c *ResolvedCacheStore, key string, ok *atomic.Bool,
	during *atomic.Pointer[func()], seen chan k734Seen) {
	t.Helper()
	terminalEnv(t)
	c, keys := terminalFixture(t, 1)
	key = keys[0]
	ok = new(atomic.Bool)
	during = new(atomic.Pointer[func()])
	seen = make(chan k734Seen, 4)
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, in ResolvedKeyInputs) error {
		if !ok.Load() {
			return errors.New("deterministic 500 — the refresher identity cannot resolve")
		}
		gen := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k) // the resolve begins: its dep-gen sink's startSeq
		if fn := during.Load(); fn != nil {
			(*fn)()
		}
		stored := c.ReplaceIfGenRefresh(rctx, k, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: &in}, gen)
		NoteRefreshWrite(rctx, k, stored)
		// Still inside the dequeue: processNext has not settled the window.
		served0 := ResolvedCacheStatsByStat()["known_stale_served_dropped_total"]
		e, hit := c.Get(k) // a customer serve in the gap
		var s k734Seen
		if hit {
			s.body = string(e.RawJSON)
		}
		s.servedDelta = uint64(ResolvedCacheStatsByStat()["known_stale_served_dropped_total"] - served0)
		_, _, s.knownStale = KnownStale(k)
		rt0 := knownStaleReadThroughTotal.Load()
		s.readThrough = KnownStaleReadThrough(k)
		knownStaleReadThroughTotal.Store(rt0)
		seen <- s
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		resetRefresherForTestKeepTerminalCounters()
	})
	StartRefresher(ctx)
	terminalMark(keys)
	waitFor(t, 10*time.Second, "the cell's refresh dropped", func() bool { return refresherSingleton().droppedTotal.Load() >= 1 })
	if _, reason, ks := KnownStale(key); !ks || reason != "dropped" {
		t.Fatalf("PRECONDITION: the cell is not known-stale (dropped) after its refresh was dropped (ks=%v reason=%q)", ks, reason)
	}
	return c, key, ok, during, seen
}

func k734Await(t *testing.T, seen chan k734Seen) k734Seen {
	t.Helper()
	select {
	case s := <-seen:
		return s
	case <-time.After(10 * time.Second):
		t.Fatalf("the re-marked cell was never re-resolved")
	}
	return k734Seen{}
}

func TestIssue734_K1_AcceptedRePutEndsKnownStaleBeforeTheDequeueSettles(t *testing.T) {
	c, key, ok, _, seen := k734KnownStaleCell(t)
	samples0 := refresherSingleton().dirtyStats.samples.Load()
	completed0 := refresherSingleton().completedTotal.Load()

	ok.Store(true) // the refresher identity recovers
	terminalMark([]string{key})
	s := k734Await(t, seen)

	if s.body != `{"body":"new"}` {
		t.Fatalf("K1: the accepted re-Put is not what a customer Get serves inside the dequeue (body %q)", s.body)
	}
	if s.servedDelta != 0 {
		t.Fatalf("K1 RED: a customer Get of the FRESH body, after the refresher's accepted re-Put, was counted as "+
			"a known-stale serve (+%d) — the stale flag outlives the Put until the dequeue settles (#734)", s.servedDelta)
	}
	if s.knownStale {
		t.Fatalf("K1 RED: the cell still reads KNOWN-STALE after the refresher's accepted re-Put published the fresh " +
			"body — a reader in this gap starts a demand revalidation of a fresh body (#734)")
	}
	if s.readThrough {
		t.Fatalf("K1 RED: a substrate reader would read the freshly re-Put cell through as a MISS (#734)")
	}

	waitFor(t, 10*time.Second, "the dequeue settled", func() bool {
		return refresherSingleton().completedTotal.Load() > completed0
	})
	if _, _, ks := KnownStale(key); ks {
		t.Fatalf("K1: the cell is known-stale after its accepted re-Put settled")
	}
	if _, open := dirtySince(key); open {
		t.Fatalf("K1: the window is still open after an accepted, un-remarked re-Put")
	}
	if got := refresherSingleton().dirtyStats.samples.Load() - samples0; got != 1 {
		t.Fatalf("K1: dirty→fresh samples moved %d, want exactly 1 (the dequeue's own close; the Put must not sample twice)", got)
	}
	if e, hit := c.GetNoTouch(key); !hit || string(e.RawJSON) != `{"body":"new"}` {
		t.Fatalf("K1: the re-Put body is not resident after the settle")
	}
}

func TestIssue734_K2_AMarkDuringTheResolveKeepsTheCellKnownStaleThroughThePut(t *testing.T) {
	_, key, ok, during, seen := k734KnownStaleCell(t)

	var once atomic.Bool
	moved := func() {
		if once.CompareAndSwap(false, true) {
			depEventSeq.Add(1)
			refresherSingleton().markDirty(key, time.Now()) // the dep moved after the resolve began
		}
	}
	during.Store(&moved)
	ok.Store(true)
	terminalMark([]string{key})
	s := k734Await(t, seen)

	if !s.knownStale {
		t.Fatalf("K2 RED: a re-Put whose resolve began BEFORE the latest mark ended the known-stale state — that " +
			"body was read from a state the mark superseded, and nothing may report it fresh")
	}
}
