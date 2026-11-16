package cache

// refresher_dequeue_order_golden_test.go — the refresher's dequeue order, pinned
// as a golden. The tier counters and dequeue-age gauges only count: this arm is
// the proof, because it compiles on main as well and must give the same results
// there and with the counters. It has two halves, one instrument:
//
//   - ONE WORKER: a first key held in flight, then unviewed and open (viewed)
//     keys marked behind it in a fixed interleaving, once without and once with
//     cluster_list keys. The re-resolve order is recorded by tier: it pins the
//     tier priority and every turn rule, a cluster_list dequeue's included.
//   - FOUR WORKERS (production's default): every re-resolve is held at a gate,
//     and once each phase settles the arm records how many workers run viewed
//     work and how many normal. It pins the viewed cap, the unviewed cap and how
//     the workers split between the tiers, which one worker cannot show.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// The one-worker goldens: H the held key, V a key open on a page, U an unviewed
// key, C a cluster_list key. Two interleavings: without cluster_list keys the
// turn between viewed and normal work shows on its own; with them, a cluster_list
// dequeue taking its turn shows as well (a cluster_list turn also clears the
// viewed one, so it hides a normal turn that stopped clearing it, and the first
// interleaving is what shows that).
//
// #1085 changes ONE of these, on purpose (#1069: the wake key is not a turn).
// main (924d4aee) reads HVUVUVVUUUUUU: an idle worker's wake key cleared the
// viewed turn, so two viewed keys ran back to back. Here the wake key leaves the
// turn as it was, so viewed and normal alternate strictly while both have work.
const (
	dequeueOrderGolden        = "HVUVUVUVUUUUU"    // main: HVUVUVVUUUUUU
	dequeueOrderClusterGolden = "HVCVCVCVUUUUUUUU" // main: the same
)

// dequeueSplitGolden is the four-worker split: viewed and normal workers busy in
// each phase once it settles. #1085 changes the last phase, on purpose: with only
// normal work left, the reservation keeps one worker free for viewed work, so
// normal work runs on three. main (924d4aee) reads 3V0U|3V1U|3V1U|0V4U.
const dequeueSplitGolden = "3V0U|3V1U|3V1U|0V3U"

func TestRefresherDequeueOrder_IsTheGoldenSequence(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	t.Run("viewed_and_normal", func(t *testing.T) { oneWorkerGolden(t, 0, dequeueOrderGolden) })
	t.Run("with_cluster_list", func(t *testing.T) { oneWorkerGolden(t, 3, dequeueOrderClusterGolden) })
}

// oneWorkerGolden runs one worker on a held key, then 8 unviewed, 4 viewed and
// nCluster cluster_list keys marked behind it in a fixed interleaving, and
// compares the re-resolve order by tier with want.
func oneWorkerGolden(t *testing.T, nCluster int, want string) {
	t.Helper()
	const nUnviewed, nViewed = 8, 4
	keys, rec, firstIn, release := viewed765Start(t, 1, 1+nUnviewed+nViewed+nCluster)
	hold := keys[0]
	unviewed := keys[1 : 1+nUnviewed]
	viewed := keys[1+nUnviewed : 1+nUnviewed+nViewed]
	cluster := keys[1+nUnviewed+nViewed:]
	viewed765Arm(t, viewed...)
	for _, k := range cluster {
		RegisterClusterListKey(k) // a dirty mark of it now routes to the cluster_list tier
	}

	role := map[string]string{hold: "H"}
	for _, k := range unviewed {
		role[k] = "U"
	}
	for _, k := range viewed {
		role[k] = "V"
	}
	for _, k := range cluster {
		role[k] = "C"
	}

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	// A fixed interleaving: unviewed keys, with a viewed key after every second
	// one and a cluster_list key after every third.
	vi, ci := 0, 0
	for i, k := range unviewed {
		terminalMark([]string{k})
		if i%2 == 1 && vi < len(viewed) {
			terminalMark([]string{viewed[vi]})
			vi++
		}
		if i%3 == 2 && ci < len(cluster) {
			terminalMark([]string{cluster[ci]})
			ci++
		}
	}
	for ; vi < len(viewed); vi++ {
		terminalMark([]string{viewed[vi]})
	}
	for ; ci < len(cluster); ci++ {
		terminalMark([]string{cluster[ci]})
	}
	release()
	waitFor(t, 20*time.Second, "every marked key re-resolved", func() bool {
		o, _, _ := rec.snapshot()
		return len(o) >= len(keys)
	})
	order, _, count := rec.snapshot()
	var got strings.Builder
	for _, k := range order {
		got.WriteString(role[k])
	}
	for k, n := range count {
		if n != 1 {
			t.Fatalf("PRECONDITION: key %s re-resolved %d times, want once", role[k], n)
		}
	}
	t.Logf("dequeue order: %s", got.String())
	if got.String() != want {
		t.Fatalf("the refresher's one-worker dequeue order is %s, want %s: what runs next changed",
			got.String(), want)
	}
}

// goldenGate holds every re-resolve until its phase is released, and counts the
// re-resolves in flight by the phase they entered under, so a snapshot never
// mixes calls leaving one phase with calls entering the next.
type goldenGate struct {
	mu       sync.Mutex
	phase    int
	gate     chan struct{}
	inflight map[int]map[string]int
	done     int
	opened   bool // the gate is open for good: nothing left to release
}

func (g *goldenGate) enter(role string) {
	g.mu.Lock()
	p, gate := g.phase, g.gate
	if g.inflight[p] == nil {
		g.inflight[p] = map[string]int{}
	}
	g.inflight[p][role]++
	g.mu.Unlock()
	<-gate
	g.mu.Lock()
	g.inflight[p][role]--
	g.done++
	g.mu.Unlock()
}

func (g *goldenGate) in(phase int) (v, u int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inflight[phase]["V"], g.inflight[phase]["U"]
}

// next releases the current phase; open leaves every later re-resolve unheld.
func (g *goldenGate) next(open bool) {
	g.mu.Lock()
	if g.opened {
		g.mu.Unlock()
		return
	}
	g.opened = open
	old := g.gate
	g.phase++
	g.gate = make(chan struct{})
	if open {
		close(g.gate)
	}
	g.mu.Unlock()
	close(old)
}

func TestRefresherDequeueOrder_FourWorkersSplitAsTheGolden(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	const p, nViewed, nUnviewed = 4, 6, 8
	terminalEnv(t)
	t.Cleanup(withCleanRefresher(t, p, 0))
	_, keys := terminalFixtureNoReset(t, nViewed+nUnviewed)
	viewed, unviewed := keys[:nViewed], keys[nViewed:]
	role := map[string]string{}
	for _, k := range viewed {
		role[k] = "V"
	}
	for _, k := range unviewed {
		role[k] = "U"
	}
	g := &goldenGate{gate: make(chan struct{}), inflight: map[int]map[string]int{}}
	t.Cleanup(func() { g.next(true) })
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		g.enter(role[k])
		return nil
	})
	viewed765Arm(t, viewed...)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)

	var split []string
	// settled snapshots a phase once its workers stop changing: at least one is
	// busy and the count holds for 300 ms. It never assumes how many workers a
	// phase may hold, so a rule that caps a tier shows as a smaller count.
	settled := func(phase int) {
		t.Helper()
		last, since := -1, time.Now()
		waitFor(t, 15*time.Second, fmt.Sprintf("phase %d settled", phase), func() bool {
			v, u := g.in(phase)
			if n := v + u; n != last {
				last, since = n, time.Now()
				return false
			}
			return last > 0 && time.Since(since) >= 300*time.Millisecond
		})
		v, u := g.in(phase)
		split = append(split, fmt.Sprintf("%dV%dU", v, u))
	}
	// Viewed work alone first: how many workers it may take.
	terminalMark(viewed)
	settled(0)
	// Then normal work beside it, phase after phase, each held until it settles.
	terminalMark(unviewed)
	for phase := 0; phase < 3; phase++ {
		settled(phase)
		g.next(phase == 2)
	}
	waitFor(t, 20*time.Second, "every key re-resolved", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.done >= len(keys)
	})
	got := strings.Join(split, "|")
	t.Logf("four-worker split: %s", got)
	if got != dequeueSplitGolden {
		t.Fatalf("the refresher's four-worker split is %s, want %s: how the workers split changed",
			got, dequeueSplitGolden)
	}
}
