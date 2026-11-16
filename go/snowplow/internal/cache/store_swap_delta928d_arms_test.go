package cache

// store_swap_delta928d_arms_test.go — the arms the #928 delta review's H1 fix
// needs beyond the reviewer's own (store_swap_review928d_test.go): a log's
// "my pending set is empty" flag clear is ordered with the next hold on the
// SAME log, which the owner check cannot tell apart (K22); and every way a
// pending set empties clears the flag, a pass that decides it and then
// cannot LIST included (K23).

import (
	"context"
	"errors"
	"testing"

	clientcache "k8s.io/client-go/tools/cache"
)

// TestStoreSwap_K22_AResolversEmptyIsOrderedWithTheNextHoldOnItsLog: the
// resolver settles a log's last key (a loss) and, while it is still
// publishing that loss, a pass holds a new key on the same log. The resolver's
// clear must not land after the new hold: the log then holds a key and the
// walker never serves its GVR (#918's stranded-key shape, inverted). Its clear
// is made under l.mu as the set empties, so it precedes the hold.
//
// Deterministic: the resolver runs on the test goroutine, and the hold is made
// from inside its publish (resolvedLossPublishedHookForTest), so when resolve
// returns every write it made has landed. RED when the clear moves back out of
// l.mu, after the publish (mutation H1c); the owner check alone does not catch
// it, because both writes are the same log's.
func TestStoreSwap_K22_AResolversEmptyIsOrderedWithTheNextHoldOnItsLog(t *testing.T) {
	resetVerificationForArm(t)
	gvr := swapGVR(t)
	rememberStoreVerification(gvr, true, true)
	l := newDeliveryLog(gvr)
	l.indexer = clientcache.NewIndexer(clientcache.MetaNamespaceKeyFunc, clientcache.Indexers{})
	l.applied = func() string { return "10" }
	t.Cleanup(l.close)

	held := false
	hook := func() {
		if held {
			return
		}
		held = true
		// A read ahead of the store's position: undecidable until it moves,
		// so it stays pending on the resolver this hold starts.
		l.setPending(map[string]readState{"ns/held": {present: true, rev: "99999"}})
	}
	resolvedLossPublishedHookForTest.Store(&hook)
	t.Cleanup(func() { resolvedLossPublishedHookForTest.Store(nil) })

	// The store has applied past the read (10 ≥ 3) and lacks the key: the
	// resolver's first judgement is a loss, which empties the set.
	l.mu.Lock()
	l.running = true // this goroutine is the resolver
	l.mu.Unlock()
	l.setPending(map[string]readState{"ns/lost": {present: true, rev: "3"}})
	l.resolve()

	if !held {
		t.Fatal("setup: the resolver published no loss")
	}
	if n := l.pendingCount(); n != 1 {
		t.Fatalf("setup: the log holds %d pending keys, want the new hold's 1", n)
	}
	if row, _ := verificationRowFor(gvr); !row.hasPending {
		t.Fatal("K22 RED: the resolver's \"empty\" landed after a new hold on its own log: the log holds a key and the walker flag is off")
	}
}

// TestStoreSwap_K23_APassThatDecidesTheLastPendingKeyThenCannotListUnflags:
// the next pass's judgePending decides the GVR's only pending key (a loss: the
// position has passed its read with no event), then its LIST fails. Nothing
// is pending, so the walker flag must be off: left on, the walker would serve
// the GVR on every other turn, each turn a full LIST, until some later pass
// held and resolved a key (G1's cost, by the judgePending path). The
// following pass's supersede would clear it, but only a pass that takes its
// read gets there. RED when judgePending does not clear the flag (H1g).
func TestStoreSwap_K23_APassThatDecidesTheLastPendingKeyThenCannotListUnflags(t *testing.T) {
	rw, cli := j9Setup(t, repairGVR, true, ja1)
	cli.answer("5002", ja2, jclock)
	forcedVerify(context.Background(), repairGVR)
	dl := j9Log(t, rw, repairGVR)
	if dl.pendingCount() != 1 {
		t.Fatal("setup: nothing pending")
	}
	j9WaitFor(t, "the resolver's first judgement", func() bool { return dl.sweeps.Load() >= 1 })
	j9Bookmark(t, rw, repairGVR, "5100") // moves the position; no callback, so no resolver wake
	cli.mu.Lock()
	cli.listErr = errors.New("the apiserver is unavailable")
	cli.mu.Unlock()
	lost0 := j9Class("update")
	forcedVerify(context.Background(), repairGVR)
	if got := j9Class("update") - lost0; got != 1 {
		t.Fatalf("setup: the pass's judgePending published %d lost updates, want 1", got)
	}
	if got := dl.pendingCount(); got != 0 {
		t.Fatalf("setup: %d keys still pending after the loss was published", got)
	}
	if row, _ := verificationRowFor(repairGVR); row.hasPending {
		t.Fatal("K23 RED: the pass decided the GVR's last pending key and could not LIST, and the walker flag stayed on with nothing pending")
	}
}
