//go:build unit || integration

// seed_resolve_memo_shadow_test.go — the memo's measurement-only would-join
// counter increments exactly when a produce starts while a produce of the
// SAME memo key is in flight, and not otherwise; it never changes what the
// memo serves; and it costs nothing next to the miss it follows.

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

func TestMemoWouldJoin_OnlyWhileTheSameKeyIsInFlight(t *testing.T) {
	m := NewSeedResolveMemo()

	a := m.BeginProduce("k1")
	wantJoins(t, m, 0) // first produce of k1: nothing to join

	b := m.BeginProduce("k1") // a is in flight
	wantJoins(t, m, 1)

	c := m.BeginProduce("k2") // another key (another identity, page or extras)
	wantJoins(t, m, 1)

	d := m.BeginProduce("k1") // a and b in flight: one more would-join
	wantJoins(t, m, 2)

	a.End()
	b.End()
	e := m.BeginProduce("k1") // d still in flight
	wantJoins(t, m, 3)
	d.End()
	e.End()
	c.End()

	f := m.BeginProduce("k1") // k1 no longer in flight
	wantJoins(t, m, 3)
	f.End()

	// End is idempotent: a second End must not free a slot another produce
	// of the key holds.
	g := m.BeginProduce("k3")
	g.End()
	h := m.BeginProduce("k3")
	g.End()
	i := m.BeginProduce("k3") // h still in flight
	wantJoins(t, m, 4)
	h.End()
	i.End()

	m.shadow.mu.Lock()
	left := len(m.shadow.inflight)
	m.shadow.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d keys left in flight after every produce ended", left)
	}

	// A nil memo is inert.
	var nilMemo *SeedResolveMemo
	p := nilMemo.BeginProduce("k")
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
		open = append(open, probed.BeginProduce(o.key)) // left in flight: overlaps every later one
		probed.StoreLayeredWithStates(o.key, "", body, nil, nil, o.stamp, nil, PutGateEffects{})
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
	for _, p := range open {
		p.End()
	}
}

// LookupStats is one snapshot: a stale miss is also a miss.
func TestMemoLookupStats_StaleIsAMiss(t *testing.T) {
	ResetDepsForTest()
	d := Deps()
	ns := "ns-lookup"
	dep := DepKey{GVR: memo411GVR, Namespace: ns, Name: listWildcard}
	d.RecordList(t.Context(), "L1_lookup", memo411GVR, ns)
	m := NewSeedResolveMemo()
	m.Load("absent")
	m.Store("k", memoBody(map[string]any{"v": 1.0}), []DepKey{dep}, DepGenEpochNow())
	m.Load("k")
	d.bumpCoordinateGen(memo411GVR, ns, "obj")
	m.Load("k")
	if h, mi, st := m.LookupStats(); h != 1 || mi != 2 || st != 1 {
		t.Fatalf("LookupStats = %d/%d/%d, want 1 hit, 2 misses (one stale), 1 stale", h, mi, st)
	}
	var nilMemo *SeedResolveMemo
	if h, mi, st := nilMemo.LookupStats(); h|mi|st != 0 {
		t.Fatal("nil memo: LookupStats not zero")
	}
}

// Concurrent produces under -race: the count equals the model's (every
// produce that registered while another of its key was registered), and the
// in-flight set is empty afterwards.
func TestMemoWouldJoin_Concurrent_Race(t *testing.T) {
	m := NewSeedResolveMemo()
	var wg sync.WaitGroup
	start := make(chan struct{})
	const workers = 8
	held := make([]*MemoProduce, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			held[w] = m.BeginProduce("hot")
		}(w)
	}
	close(start)
	wg.Wait()
	// All eight registered while the others were held: every one but the
	// first found the key in flight.
	wantJoins(t, m, workers-1)
	for _, p := range held {
		p.End()
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				m.BeginProduce(fmt.Sprintf("k%d-%d", w, i)).End() // unique keys: never a join
			}
		}(w)
	}
	wg.Wait()
	wantJoins(t, m, workers-1)
	m.shadow.mu.Lock()
	defer m.shadow.mu.Unlock()
	if len(m.shadow.inflight) != 0 {
		t.Fatalf("%d keys left in flight", len(m.shadow.inflight))
	}
}

// BenchmarkMemoWouldJoin — what the counter adds to a memo miss. op=miss is
// the Load miss the refresher already pays (unchanged); op=produce is
// BeginProduce + End, the whole cost of the counter per miss. Run parallel
// (the refresher's workers share one memo) with a realistic key.
//
//	go test -tags unit ./internal/cache/ -run '^$' -bench MemoWouldJoin -benchmem -count=6 -cpu 1,4
func BenchmarkMemoWouldJoin(b *testing.B) {
	m := NewSeedResolveMemo()
	key := m.Key("krateo-system", "compositions-list", "user-0042", []string{"devs", "bench"}, "class-0123456789abcdef", HashExtras(map[string]any{"status": "failed"}), 10, 1)
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
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.BeginProduce(key).End()
			}
		})
	})
}
