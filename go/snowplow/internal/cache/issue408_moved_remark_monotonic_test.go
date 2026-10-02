package cache

// issue408_moved_remark_monotonic_test.go — #408 review nit. The OTLP series
// snowplow_deps_moved_remark_total{carrier} are ObservableCounters, so neither carrier's
// value may ever DECREASE between two scrapes: a backend reads a decrease as a counter
// reset. The series are read from MovedRemarkByCarrier, the exact function the OTLP
// callback observes.
//
// This arm drives real noteMovedRemark increments on both carriers from several
// goroutines while a scraper reads MovedRemarkByCarrier in a tight loop. It fails on the
// first decrease of either series. It also pins total = guarded + boot.
//
// RED on the derived shape (guarded = total − boot, with total bumped before boot): a
// scrape between a boot remark's two increments over-reads guarded by one, and the next
// scrape reads it lower.

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestIssue408_MovedRemarkCarrierSeriesNeverDecrease(t *testing.T) {
	resetDepsForTest()
	t.Cleanup(resetDepsForTest)
	d := Deps()

	const writers, perWriter = 4, 200000
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				d.noteMovedRemark((i+w)%2 == 0)
			}
		}(w)
	}
	var decreases atomic.Int64
	var firstBad atomic.Value
	scraped := make(chan struct{})
	go func() {
		defer close(scraped)
		var lastG, lastB uint64
		for !stop.Load() {
			g, b := MovedRemarkByCarrier()
			if g < lastG || b < lastB {
				if decreases.Add(1) == 1 {
					firstBad.Store([4]uint64{lastG, g, lastB, b})
				}
			}
			lastG, lastB = g, b
		}
	}()
	wg.Wait()
	stop.Store(true)
	<-scraped

	if n := decreases.Load(); n != 0 {
		v := firstBad.Load().([4]uint64)
		t.Fatalf("#408 RED: a moved-remark OTLP counter series DECREASED between scrapes %d times "+
			"(first: guarded %d→%d, boot %d→%d) — a backend reads that as a counter reset. Each carrier "+
			"must be its own monotonic atomic, never derived as total−boot", n, v[0], v[1], v[2], v[3])
	}
	g, b := MovedRemarkByCarrier()
	if g+b != writers*perWriter || g != b {
		t.Fatalf("#408: after %d remarks (half per carrier) want guarded=boot=%d, got guarded=%d boot=%d",
			writers*perWriter, writers*perWriter/2, g, b)
	}
	if s := d.Stats(); s.MovedRemarkTotal != g+b || s.MovedRemarkBootTotal != b {
		t.Fatalf("#408: expvar snapshot must keep total = guarded + boot; got %+v (guarded=%d boot=%d)", s, g, b)
	}
}
