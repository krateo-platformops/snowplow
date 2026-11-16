// identity_class_signal.go — #819: the "identity classes may have moved"
// signal for armed /refreshes connections.
//
// An identity-bound key folds the requester's class (identity_class.go). When
// the class moves, every key the requester's tabs armed stops being the key
// their cells are stored and refreshed under, and nothing is ever published on
// the old key again. The tab holds only the old key, so it must be told
// (handlers/refreshes.go sends one refresh frame on it; the SPA refetches and
// re-arms under the new key from the response header).
//
// A class moves only through (a) an RBAC snapshot publish (the binding-set
// digest, and the per-subject sub-gen bumps it flushes) and (b) the #682 epoch
// bump. Both call noteIdentityClassesMayHaveMoved. The signal says "re-check",
// not "your class moved": a connection recomputes its own class and compares.
//
// COALESCING. The signal is a channel closed at the next note and replaced; a
// waiter that re-acquires it before re-checking misses nothing, and any number
// of notes while it re-checks fold into one more re-check. Non-blocking for the
// noter (one mutex, one close), which runs under the RBAC writer slot and,
// for the epoch, under the bindings index lock.

package cache

import "sync"

var (
	identityClassSignalMu sync.Mutex
	identityClassSignalCh = make(chan struct{})
)

// IdentityClassesMayHaveMoved returns a channel closed at the next event that
// can move a requester's identity class. Re-acquire it before each re-check.
func IdentityClassesMayHaveMoved() <-chan struct{} {
	identityClassSignalMu.Lock()
	defer identityClassSignalMu.Unlock()
	return identityClassSignalCh
}

// noteIdentityClassesMayHaveMoved wakes every waiter on the current channel.
func noteIdentityClassesMayHaveMoved() {
	identityClassSignalMu.Lock()
	close(identityClassSignalCh)
	identityClassSignalCh = make(chan struct{})
	identityClassSignalMu.Unlock()
}
