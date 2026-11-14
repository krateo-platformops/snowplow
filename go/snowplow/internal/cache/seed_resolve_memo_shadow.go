// seed_resolve_memo_shadow.go — MEASUREMENT ONLY: how much refresher work an
// in-flight join of the resolve memo could remove. Nothing here changes what
// the memo serves, stores or drops; it only counts.
//
// THE QUESTION (paired run 23's profile study). The refresher pool is ~55% of
// snowplow's CPU at 50K x 1000, most of it one RESTAction filter, and ~96% of
// normal-tier refreshes Put byte-identical bytes. The memo has no in-flight
// join: the refresher's workers can each miss on the same memo key and
// evaluate the same RESTAction at the same time. Before anyone designs a join,
// this counts the produces one would have saved.
//
// THE JOIN IT MODELS, exactly. A join is a singleflight on the memo key: the
// first produce of a key that starts while none is in flight is the LEADER;
// every produce of the same key that starts while the leader is in flight is a
// JOINER, and would have waited for the leader's result instead of producing.
// The shadow still lets every joiner produce; it only records which ones
// attached to which leader.
//
// WHEN A JOINER IS SAVED. A joiner can take the leader's result only if the
// memo itself would serve it: the leader reached the memo Store with a body
// (not a #398 sensitive one, not an error), and that body passes the memo's
// own freshness rule (#411: none of its captured deps moved past its stamp,
// seedMemoDepsMovedSince) at the moment it exists — the leader's End, which is
// when a join would hand it over. A dep that moved after the leader started
// and before (or while) a joiner waited fails that test, exactly as the memo
// refuses that body at Load or right after Store, so the joiner is NOT
// counted: it would have had to produce anyway. Judged once per leader, at its
// End, for all of its joiners.
//
//	WOULD JOIN — joiners whose leader's result the memo would have served
//	them at hand-over.
//
// IDENTITY. The memo key folds username + sorted groups + RBAC class, so a
// join is always same-identity work. The in-flight set holds the memo keys the
// memo itself already holds (process memory only); the counter is a number
// with no attributes.

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
	// inflight maps a memo key to its in-flight leader.
	inflight map[string]*MemoProduce

	wouldJoin atomic.Uint64
}

// WouldJoin returns how many produces would have been saved by joining their
// key's in-flight leader (see the file comment). nil-receiver-safe.
func (mo *SeedResolveMemo) WouldJoin() uint64 {
	if mo == nil {
		return 0
	}
	return mo.shadow.wouldJoin.Load()
}

// MemoProduce follows one memo miss's produce from BeginProduce to End. Its
// methods are nil-receiver-safe.
type MemoProduce struct {
	mo  *SeedResolveMemo
	key string
	// joiners counts the produces that attached to this one while it led its
	// key; written under the shadow's mu, read by End after the leader left
	// the in-flight set (no joiner can attach after that).
	joiners int
	leader  bool

	// produced / deps / stamp: what the produce handed the memo Store
	// (Produced), read by End. Written and read by the producing goroutine.
	produced bool
	deps     []DepKey
	stamp    uint64

	ended atomic.Bool
}

// BeginProduce registers a produce of key after a miss: the leader of key when
// none is in flight, else a joiner of the in-flight leader. The caller ends it
// (End) when the produce returns, stored or not. nil memo -> nil (inert).
func (mo *SeedResolveMemo) BeginProduce(key string) *MemoProduce {
	if mo == nil {
		return nil
	}
	p := &MemoProduce{mo: mo, key: key}
	s := &mo.shadow
	s.mu.Lock()
	if l := s.inflight[key]; l != nil {
		l.joiners++
	} else {
		if s.inflight == nil {
			s.inflight = make(map[string]*MemoProduce)
		}
		s.inflight[key] = p
		p.leader = true
	}
	s.mu.Unlock()
	if h := beginProduceHookForTest.Load(); h != nil {
		(*h)(key)
	}
	return p
}

// beginProduceHookForTest, when set, runs after BeginProduce registered a
// produce, on the producing goroutine: an arm driving the real apiref.Resolve
// uses it to act while that produce is in flight. nil in production.
var beginProduceHookForTest atomic.Pointer[func(key string)]

// SetBeginProduceHookForTest installs fn (see beginProduceHookForTest) and
// returns a restore func. TEST-ONLY: production code MUST NOT call it.
func SetBeginProduceHookForTest(fn func(key string)) func() {
	prev := beginProduceHookForTest.Swap(&fn)
	return func() { beginProduceHookForTest.Store(prev) }
}

// Produced records the body this produce hands the memo Store: its captured
// deps and its stamp, the two inputs of the memo's freshness rule. Called by
// the produce right before its Store; a produce that never calls it (an error,
// a sensitive body) has no result a joiner could take.
func (p *MemoProduce) Produced(deps []DepKey, stamp uint64) {
	if p == nil {
		return
	}
	p.produced, p.deps, p.stamp = true, deps, stamp
}

// End takes the produce out of flight. A leader leaves its key's in-flight
// slot and judges its joiners: they count as would-joins only when its result
// passes the memo's freshness rule now (seedMemoDepsMovedSince — the dep
// tracker's atomics, no memo lock). Idempotent.
func (p *MemoProduce) End() {
	if p == nil || !p.ended.CompareAndSwap(false, true) || !p.leader {
		return
	}
	s := &p.mo.shadow
	s.mu.Lock()
	if s.inflight[p.key] == p {
		delete(s.inflight, p.key)
	}
	joiners := p.joiners
	s.mu.Unlock()
	if joiners > 0 && p.produced && !seedMemoDepsMovedSince(p.deps, p.stamp) {
		s.wouldJoin.Add(uint64(joiners))
	}
}
