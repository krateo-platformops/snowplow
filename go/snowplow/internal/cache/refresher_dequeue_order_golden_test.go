package cache

// refresher_dequeue_order_golden_test.go — the refresher's dequeue order, pinned
// as a golden. The tier counters and dequeue-age gauges only count: this arm is
// the proof, because it compiles on main as well and must give the same sequence
// there and with the counters.
//
// One worker, a first key held in flight, then unviewed and open (viewed) keys
// marked behind it in a fixed interleaving. The re-resolve order is recorded by
// tier and compared with the sequence main produces.

import (
	"strings"
	"testing"
	"time"
)

// dequeueOrderGolden is main's sequence (RUN on ade29795): H the held key, V a
// key open on a page, U an unviewed key.
const dequeueOrderGolden = "HVUVUVVUUUUUU"

func TestRefresherDequeueOrder_IsTheGoldenSequence(t *testing.T) {
	const nUnviewed, nViewed = 8, 4
	keys, rec, firstIn, release := viewed765Start(t, 1, 1+nUnviewed+nViewed)
	hold, unviewed, viewed := keys[0], keys[1:1+nUnviewed], keys[1+nUnviewed:]
	viewed765Arm(t, viewed...)

	role := map[string]string{hold: "H"}
	for _, k := range unviewed {
		role[k] = "U"
	}
	for _, k := range viewed {
		role[k] = "V"
	}

	terminalMark([]string{hold})
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	// A fixed interleaving: two unviewed, one viewed, and so on.
	vi := 0
	for i, k := range unviewed {
		terminalMark([]string{k})
		if i%2 == 1 && vi < len(viewed) {
			terminalMark([]string{viewed[vi]})
			vi++
		}
	}
	for ; vi < len(viewed); vi++ {
		terminalMark([]string{viewed[vi]})
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
	if got.String() != dequeueOrderGolden {
		t.Fatalf("the refresher's dequeue order is %s, want main's %s: something that should only count changed what runs next",
			got.String(), dequeueOrderGolden)
	}
}
