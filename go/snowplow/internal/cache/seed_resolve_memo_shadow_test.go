//go:build unit || integration

// seed_resolve_memo_shadow_test.go — the memo's measurement-only would-join
// counter counts a joiner exactly when its key's in-flight leader hands over a
// body the memo itself would serve (#411), and not otherwise: not when a dep
// moved between the leader's start and the hand-over, not when the leader
// produced nothing, not without a leader in flight. It never changes what the
// memo serves, and it costs nothing next to the miss it follows.

package cache

import (
	"fmt"
	"sync"
	"testing"
)

func wantJoins(t *testing.T, m *SeedResolveMemo, want uint64) {
	t.Helper()
	if got := m.WouldJoin(); got != want {
		t.Fatalf("WouldJoin = %d, want %d", got, want)
	}
}

// shadowDep is a dep with a tracker bucket, so a bump on it moves exactly it.
func shadowDep(t *testing.T, ns string) DepKey {
	t.Helper()
	Deps().RecordList(t.Context(), "L1_shadow_"+ns, memo411GVR, ns)
	return DepKey{GVR: memo411GVR, Namespace: ns, Name: listWildcard}
}

// The reviewer's scenario and its control: A misses and starts; B misses and
// starts while A is in flight. With no dep move, A's body is served and B is a
// would-join. With A's dep moved between A's start and B's start, the memo
// refuses A's body, so B is not.
func TestMemoWouldJoin_OnlyWhenTheLeadersBodyIsServable(t *testing.T) {
	for _, move := range []bool{false, true} {
		t.Run(fmt.Sprintf("dep_moved=%v", move), func(t *testing.T) {
			ResetDepsForTest()
			t.Cleanup(ResetDepsForTest)
			dep := shadowDep(t, "ns-join")
			m := NewSeedResolveMemo()

			m.Load("k") // A misses
			e0 := DepGenEpochNow()
			a := m.BeginProduce("k")
			if move {
				Deps().bumpCoordinateGen(memo411GVR, "ns-join", "obj")
			}
			m.Load("k") // B misses
			b := m.BeginProduce("k")
			a.Produced([]DepKey{dep}, e0)
			m.Store("k", memoBody(map[string]any{"v": 1.0}), []DepKey{dep}, e0)
			a.End()
			b.End()

			_, _, _, served := m.Load("k")
			if served == move {
				t.Fatalf("setup: dep moved=%v but the memo served A's body=%v", move, served)
			}
			want := uint64(1)
			if move {
				want = 0 // the memo refuses A's body: B would have produced anyway
			}
			wantJoins(t, m, want)
		})
	}
}

// A dep that moves after B started but before A's hand-over also refuses the
// body (a join hands it over at A's End, through the memo's rule).
func TestMemoWouldJoin_MoveWhileTheJoinerWaitsRefuses(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	dep := shadowDep(t, "ns-wait")
	m := NewSeedResolveMemo()
	e0 := DepGenEpochNow()
	a := m.BeginProduce("k")
	b := m.BeginProduce("k")
	Deps().bumpCoordinateGen(memo411GVR, "ns-wait", "obj")
	a.Produced([]DepKey{dep}, e0)
	a.End()
	b.End()
	wantJoins(t, m, 0)
}

func TestMemoWouldJoin_LeadersAndJoiners(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	m := NewSeedResolveMemo()
	fresh := func(p *MemoProduce) { p.Produced(nil, DepGenEpochNow()) }

	// One leader, three joiners, a produce of another key: 3.
	a := m.BeginProduce("k1")
	j1, j2, j3 := m.BeginProduce("k1"), m.BeginProduce("k1"), m.BeginProduce("k1")
	other := m.BeginProduce("k2") // its own leader
	fresh(a)
	a.End()
	wantJoins(t, m, 3) // judged at the leader's End, whatever the joiners do
	for _, p := range []*MemoProduce{j1, j2, j3} {
		p.End()
	}
	other.End()
	wantJoins(t, m, 3)

	// A leader that produced nothing (an error, a sensitive body) saves no one.
	e := m.BeginProduce("k3")
	f := m.BeginProduce("k3")
	e.End()
	f.End()
	wantJoins(t, m, 3)

	// After the leader ended, a still-running joiner is not a leader: the next
	// produce of the key leads, and a joiner ending first changes nothing.
	l := m.BeginProduce("k4")
	j := m.BeginProduce("k4")
	j.End() // the joiner finishes before its leader
	fresh(l)
	l.End()
	wantJoins(t, m, 4)
	n := m.BeginProduce("k4") // nothing in flight: a new leader, no join
	fresh(n)
	n.End()
	wantJoins(t, m, 4)

	// End is idempotent: a second End counts nothing and frees nothing.
	g := m.BeginProduce("k5")
	h := m.BeginProduce("k5")
	fresh(g)
	g.End()
	g.End()
	h.End()
	wantJoins(t, m, 5)

	m.shadow.mu.Lock()
	left := len(m.shadow.inflight)
	m.shadow.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d keys left in flight after every produce ended", left)
	}

	// A nil memo is inert.
	var nilMemo *SeedResolveMemo
	p := nilMemo.BeginProduce("k")
	p.Produced(nil, 1)
	p.End()
	if p != nil || nilMemo.WouldJoin() != 0 {
		t.Fatal("nil memo: BeginProduce is not inert")
	}
}

// The counter changes nothing the memo serves: the same Load / Store sequence
// with and without overlapping produces leaves the same bodies, stamps,
// entries and lookup counters.
func TestMemoWouldJoin_ServesExactlyWhatTheMemoServes(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	plain, probed := NewSeedResolveMemo(), NewSeedResolveMemo()
	type op struct {
		key   string
		v     float64
		stamp uint64
	}
	ops := []op{{"k1", 1, 5}, {"k1", 2, 5}, {"k1", 3, 9}, {"k2", 4, 3}, {"k1", 5, 7}, {"k3", 6, 1}}
	var open []*MemoProduce
	for _, o := range ops {
		body := memoBody(map[string]any{"v": o.v})
		plain.Load(o.key)
		plain.StoreLayeredWithStates(o.key, "", body, nil, nil, o.stamp, nil, PutGateEffects{})
		probed.Load(o.key)
		p := probed.BeginProduce(o.key) // left in flight: overlaps every later one
		p.Produced(nil, o.stamp)
		open = append(open, p)
		probed.StoreLayeredWithStates(o.key, "", body, nil, nil, o.stamp, nil, PutGateEffects{})
	}
	for _, p := range open {
		p.End()
	}
	if probed.WouldJoin() == 0 {
		t.Fatal("setup: the overlapping produces counted no would-join")
	}
	for _, k := range []string{"k1", "k2", "k3", "absent"} {
		pb, _, ps, pok := plain.Load(k)
		qb, _, qs, qok := probed.Load(k)
		if pok != qok || ps != qs || fmt.Sprint(pb) != fmt.Sprint(qb) {
			t.Fatalf("%s: plain (%v,%d,%v) vs probed (%v,%d,%v)", k, pb, ps, pok, qb, qs, qok)
		}
	}
	ph, pm, pst := plain.LookupStats()
	qh, qm, qst := probed.LookupStats()
	if ph != qh || pm != qm || pst != qst || plain.Entries() != probed.Entries() || plain.BodyBytes() != probed.BodyBytes() {
		t.Fatalf("lookups/gauges differ: plain %d/%d/%d %d/%d, probed %d/%d/%d %d/%d",
			ph, pm, pst, plain.Entries(), plain.BodyBytes(), qh, qm, qst, probed.Entries(), probed.BodyBytes())
	}
}

// LookupStats is one snapshot: a stale miss is also a miss, and so is an entry
// whose body cannot be decoded (the caller resolves fresh), so would-joins can
// never exceed misses.
func TestMemoLookupStats_StaleAndUndecodableAreMisses(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	dep := shadowDep(t, "ns-lookup")
	m := NewSeedResolveMemo()
	m.Load("absent")
	m.Store("k", memoBody(map[string]any{"v": 1.0}), []DepKey{dep}, DepGenEpochNow())
	m.Load("k")
	Deps().bumpCoordinateGen(memo411GVR, "ns-lookup", "obj")
	m.Load("k")
	if h, mi, st := m.LookupStats(); h != 1 || mi != 2 || st != 1 {
		t.Fatalf("LookupStats = %d/%d/%d, want 1 hit, 2 misses (one stale), 1 stale", h, mi, st)
	}
	m.Store("bad", []byte(`{not json`), nil, DepGenEpochNow())
	if _, _, _, ok := m.Load("bad"); ok {
		t.Fatal("an undecodable body was served")
	}
	if h, mi, _ := m.LookupStats(); h != 1 || mi != 3 {
		t.Fatalf("after the undecodable Load: hits=%d misses=%d, want 1 and 3 (the Load is a miss)", h, mi)
	}
	var nilMemo *SeedResolveMemo
	if h, mi, st := nilMemo.LookupStats(); h|mi|st != 0 {
		t.Fatal("nil memo: LookupStats not zero")
	}
}

// Concurrent produces under -race: of eight produces of one key started while
// the first leads, seven are its joiners; unique keys never join; the
// in-flight set is empty afterwards.
func TestMemoWouldJoin_Concurrent_Race(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	m := NewSeedResolveMemo()
	lead := m.BeginProduce("hot")
	var wg sync.WaitGroup
	start := make(chan struct{})
	const joiners = 7
	held := make([]*MemoProduce, joiners)
	for w := 0; w < joiners; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			held[w] = m.BeginProduce("hot")
		}(w)
	}
	close(start)
	wg.Wait()
	lead.Produced(nil, DepGenEpochNow())
	lead.End()
	wantJoins(t, m, joiners)
	for _, p := range held {
		p.End()
	}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				p := m.BeginProduce(fmt.Sprintf("k%d-%d", w, i)) // unique keys: never a join
				p.Produced(nil, 1)
				p.End()
			}
		}(w)
	}
	wg.Wait()
	wantJoins(t, m, joiners)
	m.shadow.mu.Lock()
	defer m.shadow.mu.Unlock()
	if len(m.shadow.inflight) != 0 {
		t.Fatalf("%d keys left in flight", len(m.shadow.inflight))
	}
}

// BenchmarkMemoWouldJoin — what the counter adds to a memo miss. op=miss is
// the Load miss the refresher already pays (unchanged); op=produce is
// BeginProduce + Produced + End of a leader with no joiner (the common case:
// End runs no freshness check); op=produce_joined is a leader with one joiner
// and one captured dep, so End runs the memo's freshness check once;
// op=produce_parallel is every worker producing the SAME key at once (lock
// contention, joiners and freshness checks: the worst case). A realistic key.
//
//	go test -tags unit ./internal/cache/ -run '^$' -bench MemoWouldJoin -benchmem -count=6 -cpu 1,4
func BenchmarkMemoWouldJoin(b *testing.B) {
	ResetDepsForTest()
	b.Cleanup(ResetDepsForTest)
	m := NewSeedResolveMemo()
	key := m.Key("krateo-system", "compositions-list", "user-0042", []string{"devs", "bench"}, "class-0123456789abcdef", HashExtras(map[string]any{"status": "failed"}), 10, 1)
	Deps().RecordList(b.Context(), "L1_bench", memo411GVR, "ns-bench")
	deps := []DepKey{{GVR: memo411GVR, Namespace: "ns-bench", Name: listWildcard}}
	stamp := DepGenEpochNow()
	b.Run("op=miss", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.LoadLayeredWithStates(key, "")
			}
		})
	})
	b.Run("op=produce", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			p := m.BeginProduce(key) // a leader with no joiner
			p.Produced(deps, stamp)
			p.End()
		}
	})
	b.Run("op=produce_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				p := m.BeginProduce(key) // one key: contention, joiners, freshness checks
				p.Produced(deps, stamp)
				p.End()
			}
		})
	})
	b.Run("op=produce_joined", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			l := m.BeginProduce(key)
			j := m.BeginProduce(key)
			l.Produced(deps, stamp)
			l.End()
			j.End()
		}
	})
}
