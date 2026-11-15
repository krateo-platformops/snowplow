// served_view_memo.go — #1430: the digest of what one identity is served from
// a gated cell, computed once per (answer, identity, RBAC snapshot).
//
// THE COST. A gated cell (an identity-free widgetContent shell) is narrowed per
// requester at serve time, so the digest of what a tab is served (#1324, #1328)
// needs the gate: unmarshal the whole shell, one RBAC verdict per action,
// re-encode, hash. Since 1.12.70 every arm of a /refreshes stream ran it for
// every armed gated key, also when nothing it reads had changed (bench run
// 26i: 55 cpu-s per 1,533 profiled seconds, +4.8% customer-path CPU).
//
// THE KEY IS EVERY INPUT OF THE GATE. The served digest is a function of three
// things, which the memo names, and of the mode the RBAC evaluator runs in,
// which it does not name: outside the one mode in which the evaluator reads
// the published snapshot, nothing is stored or reused (THE MODE, below).
//
//   - the cell's answer (its body and its stored stage outcomes): the entry's
//     ContentDigest. The memo belongs to the answer, not to the entry object:
//     a re-Put carries it to the new entry (putCoreLocked), and it is dropped
//     whole the first time it is read for an entry whose answer differs. An
//     identical re-Put (most refresh Puts) therefore keeps it;
//   - the requester: the username and the group set, as one fixed-size
//     digest of their length-prefixed encoding (ServedViewIdentity). Every
//     verdict is about this requester; the snapshot does not name it;
//   - the RBAC state every verdict is read from: the published RBAC snapshot,
//     named by its PublishSeq (ServedViewRBAC). A result is stored only when
//     that snapshot is complete (Synced) and is still the published one after
//     the gate ran, so a stored digest was computed against one snapshot, and
//     it is reused only while that snapshot is the published one.
//
// THE MODE. Before it reads the snapshot the evaluator branches twice
// (rbac.EvaluateRBAC): with the cache disabled it asks the apiserver with the
// requester's credential, and with no watcher wired it denies everything. In
// both, the verdicts are not a function of the published snapshot, so a
// digest gated there must not be stored under its sequence, and a stored one
// must not be reused. ServedViewRBACNow therefore reads the mode at the same
// point as the snapshot and is storable only when the cache is enabled, the
// watcher is wired and the snapshot is complete; the before/after compare
// around the gate covers a mode that changes while the gate runs.
//
// The identity CLASS is deliberately not the key. It moves after the snapshot
// it reflects (the sub-generation bumps are flushed after the Store). The
// snapshot is what the evaluator reads, so it is the name that cannot miss a
// change.
//
// LIFETIME. Nothing here has a size cap or a TTL. The memo is reachable only
// from its entry: it goes when the answer changes, and with the entry when the
// key is deleted or evicted. It holds one slot per identity, replaced in place
// when the snapshot moves, so RBAC churn never accumulates slots.
//
// CONCURRENCY. The memo's mutex covers only the slot lookup. The gate runs
// inside the slot's sync.Once, outside the mutex: callers for the same
// (answer, identity, snapshot) wait for the one gate and share its digest;
// callers for another identity are never blocked by it.
//
// NO BODY, NO NAME. A slot holds a digest string and a sequence number, under
// a 32-byte identity digest: neither a served byte nor a username or group
// name is kept, and a slot's size does not depend on how many groups the
// requester carries (an external identity provider may send hundreds). The
// gated bytes are discarded by the caller exactly as before. Nothing of it is
// logged, exported on /debug or copied into ResolvedEntryMeta.
package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"
)

// ServedViewIdentity names the requester a served digest was gated for: the
// SHA-256 of the username and of every group, each prefixed by its length, the
// groups in sorted order. The length prefixes make the encoding injective (no
// two (username, group multiset) pairs encode alike, whatever bytes a name
// carries), so two identities share a slot only on a SHA-256 collision. Fixed
// size: a slot costs the same for one group or for hundreds.
type ServedViewIdentity [sha256.Size]byte

// ServedViewIdentityOf builds the identity of (username, groups). The group
// order does not matter (no RBAC verdict depends on it); duplicates are kept.
// It sorts and hashes the groups: build it ONCE per connection or request and
// pass it down, never once per key.
func ServedViewIdentityOf(username string, groups []string) ServedViewIdentity {
	sorted := groups
	if !sort.StringsAreSorted(groups) {
		sorted = make([]string, len(groups))
		copy(sorted, groups)
		sort.Strings(sorted)
	}
	size := 8 + len(username) + 8
	for _, g := range sorted {
		size += 8 + len(g)
	}
	enc := make([]byte, 0, size)
	enc = binary.LittleEndian.AppendUint64(enc, uint64(len(username)))
	enc = append(enc, username...)
	enc = binary.LittleEndian.AppendUint64(enc, uint64(len(sorted)))
	for _, g := range sorted {
		enc = binary.LittleEndian.AppendUint64(enc, uint64(len(g)))
		enc = append(enc, g...)
	}
	return sha256.Sum256(enc)
}

// ServedViewRBAC names the RBAC state a gate reads: the published snapshot's
// PublishSeq, and whether a result gated against it may be stored (the
// evaluator is in the mode that reads that snapshot, and it is complete).
type ServedViewRBAC struct {
	seq      uint64
	complete bool
}

// ServedViewRBACNow reads the evaluator's mode and the published RBAC
// snapshot's name, at one point. Read it BEFORE the gate runs. Not storable:
//
//   - when the cache is disabled (the evaluator asks the apiserver) or no
//     watcher is wired (it denies everything): the verdicts are then not read
//     from the snapshot at all;
//   - when there is no snapshot, when it carries no sequence, or when it was
//     built before every RBAC informer had synced (a role not yet listed
//     denies, and its later arrival is not always a new class).
func ServedViewRBACNow() ServedViewRBAC {
	if Disabled() || Global() == nil {
		return ServedViewRBAC{}
	}
	snap := rbacSnap.Load()
	if snap == nil {
		return ServedViewRBAC{}
	}
	return ServedViewRBAC{seq: snap.PublishSeq, complete: snap.Synced && snap.PublishSeq != 0}
}

// Storable reports whether a digest gated under r may be stored.
func (r ServedViewRBAC) Storable() bool { return r.complete }

// servedViewSlot is one identity's digest under one RBAC snapshot.
type servedViewSlot struct {
	seq    uint64
	once   sync.Once
	digest string
	// stored is set, inside once, only when the gate served and the snapshot
	// did not move while it ran.
	stored bool
}

// servedViewMemo holds the served digests of one answer.
type servedViewMemo struct {
	// answer is the ContentDigest of the entry the digests were gated from.
	answer string
	mu     sync.Mutex
	slots  map[ServedViewIdentity]*servedViewSlot
}

var (
	// servedViewGateRunsTotal counts gate runs asked for by a /refreshes
	// connection (arm, catch-up check, frame written, class move).
	servedViewGateRunsTotal atomic.Uint64
	// servedViewReusedTotal counts those answered from a stored digest.
	servedViewReusedTotal atomic.Uint64
	// servedViewCallFillsTotal counts digests a /call hit stored.
	servedViewCallFillsTotal atomic.Uint64
	// servedViewSnapshotMovedTotal counts stored digests discarded because the
	// RBAC snapshot they were gated under is no longer the published one.
	servedViewSnapshotMovedTotal atomic.Uint64
)

// servedViewMemoOf returns the memo of e's answer, replacing one carried from
// an entry with another answer. Nil for an entry whose body is not immutable
// (an incremental list cell): nothing is stored for it.
func (e *ResolvedEntry) servedViewMemoOf() *servedViewMemo {
	if e == nil || e.listCell != nil {
		return nil
	}
	answer := e.ContentDigest()
	for {
		cur := e.servedViews.Load()
		if cur != nil && cur.answer == answer {
			return cur
		}
		fresh := &servedViewMemo{answer: answer, slots: map[ServedViewIdentity]*servedViewSlot{}}
		if e.servedViews.CompareAndSwap(cur, fresh) {
			return fresh
		}
	}
}

// slotFor returns id's slot under seq, creating it (and replacing a slot of
// another snapshot) when there is none.
func (m *servedViewMemo) slotFor(id ServedViewIdentity, seq uint64) *servedViewSlot {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.slots[id]
	if s != nil && s.seq == seq {
		return s
	}
	if s != nil {
		servedViewSnapshotMovedTotal.Add(1)
	}
	s = &servedViewSlot{seq: seq}
	m.slots[id] = s
	return s
}

// drop removes id's slot if it is still s.
func (m *servedViewMemo) drop(id ServedViewIdentity, s *servedViewSlot) {
	m.mu.Lock()
	if m.slots[id] == s {
		delete(m.slots, id)
	}
	m.mu.Unlock()
}

// ServedViewDigest returns the digest of what id is served from e: gate()'s
// digest, run once per (e's answer, id, published RBAC snapshot) and reused
// until one of the three changes. gate returns the digest and whether the cell
// would be served at all; a refusal is "" and is never stored. The result is
// always the one gate() returns for the current inputs.
func (e *ResolvedEntry) ServedViewDigest(id ServedViewIdentity, gate func() (string, bool)) string {
	rbacState := ServedViewRBACNow() // before the gate
	m := e.servedViewMemoOf()
	if m == nil || !rbacState.Storable() {
		servedViewGateRunsTotal.Add(1)
		d, _ := gate()
		return d
	}
	s := m.slotFor(id, rbacState.seq)
	var mine string
	ran := false
	s.once.Do(func() {
		ran = true
		servedViewGateRunsTotal.Add(1)
		d, served := gate()
		if !served {
			return
		}
		mine = d
		if after := ServedViewRBACNow(); after == rbacState {
			s.digest, s.stored = d, true
		}
	})
	switch {
	case s.stored:
		if !ran {
			servedViewReusedTotal.Add(1)
		}
		return s.digest
	case ran:
		m.drop(id, s)
		return mine
	}
	// A caller that found the slot's gate already run, with nothing stored (a
	// refusal, a snapshot or a mode that moved while it ran, a gate that
	// panicked): the slot has no answer for it. Answer live, as before #1430.
	m.drop(id, s)
	servedViewGateRunsTotal.Add(1)
	d, _ := gate()
	return d
}

// NoteServedView stores the digest of the bytes a /call hit wrote to id from
// e. before is ServedViewRBACNow() read BEFORE that hit's gate ran: the digest
// is stored only if that snapshot is storable and still the published one. The
// /call path never reads the memo; it only fills it.
func (e *ResolvedEntry) NoteServedView(id ServedViewIdentity, before ServedViewRBAC, digest string) {
	if digest == "" || !before.Storable() || ServedViewRBACNow() != before {
		return
	}
	m := e.servedViewMemoOf()
	if m == nil {
		return
	}
	s := &servedViewSlot{seq: before.seq, digest: digest, stored: true}
	s.once.Do(func() {})
	m.mu.Lock()
	if old := m.slots[id]; old != nil && old.seq != s.seq {
		servedViewSnapshotMovedTotal.Add(1)
	}
	m.slots[id] = s
	m.mu.Unlock()
	servedViewCallFillsTotal.Add(1)
}

// ServedViewSlotsForTest is the number of slots e's memo holds for e's answer
// (0 when it holds none or the memo it carries is of another answer).
func (e *ResolvedEntry) ServedViewSlotsForTest() int {
	m := e.servedViews.Load()
	if m == nil || m.answer != e.ContentDigest() {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.slots)
}
