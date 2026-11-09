// seed_resolve_memo_shadow.go — MEASUREMENT ONLY: how much refresher work an
// in-flight join of the resolve memo could remove. Nothing here changes what
// the memo serves, stores or drops; it only counts.
//
// THE QUESTION (paired run 23's profile study). The refresher pool is ~55% of
// snowplow's CPU at 50K x 1000, most of it one RESTAction filter, and ~96% of
// normal-tier refreshes Put byte-identical bytes. The memo has no in-flight
// join: the refresher's workers can each miss on the same memo key and
// evaluate the same RESTAction at the same time, and only the first Store is
// kept (Store is first-writer-wins among equal stamps). Before anyone designs
// a join, this counts how often one would have applied:
//
//	WOULD JOIN — a memo miss whose produce starts while another produce of the
//	SAME memo key is already in flight.
//
// The check runs when the produce starts (BeginProduce, right after the miss),
// against the produces already registered, so of two overlapping produces of a
// key only the later one counts: the one a join would have saved. The memo key
// already folds the identity, so a would-join is always same-identity work;
// the counter is a number with no attributes and the in-flight set holds only
// the memo keys the memo itself already holds.

package cache

import (
	"sync"
	"sync/atomic"
)

// memoShadow is one memo's in-flight shadow. mu is its own lock, never held
// with the memo's mu, so the memo's Load / Store critical sections are
// unchanged.
type memoShadow struct {
	mu sync.Mutex
	// inflight counts the produces in flight per memo key.
	inflight map[string]int

	wouldJoin atomic.Uint64
}

// WouldJoin returns how many produces started while a produce of the same
// memo key was in flight. nil-receiver-safe.
func (mo *SeedResolveMemo) WouldJoin() uint64 {
	if mo == nil {
		return 0
	}
	return mo.shadow.wouldJoin.Load()
}

// MemoProduce follows one memo miss's produce from BeginProduce to End. Its
// methods are nil-receiver-safe.
type MemoProduce struct {
	mo    *SeedResolveMemo
	key   string
	ended atomic.Bool
}

// BeginProduce registers a produce of key after a miss and counts a would-join
// when another produce of key is already in flight. The caller ends it (End)
// when the produce returns, stored or not. nil memo -> nil (inert).
func (mo *SeedResolveMemo) BeginProduce(key string) *MemoProduce {
	if mo == nil {
		return nil
	}
	s := &mo.shadow
	s.mu.Lock()
	if s.inflight == nil {
		s.inflight = make(map[string]int)
	}
	n := s.inflight[key]
	s.inflight[key] = n + 1
	s.mu.Unlock()
	if n > 0 {
		s.wouldJoin.Add(1)
	}
	return &MemoProduce{mo: mo, key: key}
}

// End takes the produce out of the in-flight set. Idempotent.
func (p *MemoProduce) End() {
	if p == nil || !p.ended.CompareAndSwap(false, true) {
		return
	}
	s := &p.mo.shadow
	s.mu.Lock()
	if n := s.inflight[p.key] - 1; n > 0 {
		s.inflight[p.key] = n
	} else {
		delete(s.inflight, p.key)
	}
	s.mu.Unlock()
}
