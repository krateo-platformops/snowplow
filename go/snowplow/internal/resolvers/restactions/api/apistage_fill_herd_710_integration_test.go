//go:build integration

// apistage_fill_herd_710_integration_test.go — #710 PR-0: the arrival burst at
// reduced scale, kind-free. One namespace holds 5,000 objects; their LIST cell
// is made known-stale for real (#547 drive, fill710MakeKnownStale); then 200
// readers released together each make a first load of it — the shape design
// §1 predicts for the 50K OOM (every arriving reader re-fills the same list).
//
// No dispatch barrier here: the readers arrive as fast as goroutines start,
// so the counts are what the code does under a real burst, not a staged one.
//
// PR-0 measured main here (-race): 200 fills, 199 duplicates, 113 envelopes in
// flight at once (102 MB raw), HeapInuse 25 → 1,089 MiB. PR-1 (single-flight)
// asserts the herd is gone: no duplicate fill (no event moves the coordinate
// during the burst, so nobody may refuse a join), one envelope in flight at a
// time, and every reader joined or hit. More than one fill is allowed only for
// a reader that missed the cell just before the flight's Put and found no
// flight just after it — a sequential refill, never a concurrent one.
package api

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestContentFill710_Integration_5KItems200Readers_KnownStaleBurst(t *testing.T) {
	const (
		items   = 5000
		readers = 200
	)
	dyn := newFill710Watcher(t, items)
	store := cache.ResolvedCache()
	fill710Reset(t)
	fill710MakeKnownStale(t, dyn, store)
	envLen := fill710SpyDispatch(t, nil, nil, nil)

	// Heap high-water while the burst runs (HeapInuse, sampled every 1 ms).
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peakHeap atomic.Uint64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peakHeap.Load() {
				peakHeap.Store(m.HeapInuse)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	var notServed atomic.Int64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
				notServed.Add(1)
			}
		}()
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	close(stop)
	<-sampled

	if n := notServed.Load(); n != 0 {
		t.Fatalf("%d of %d readers were not served", n, readers)
	}
	s := ContentFillStatsSnapshot()
	fill710RequireIdle(t, s)
	fills := s.Total["known_stale"] + s.Total["absent"]
	dups := s.Duplicate["known_stale"] + s.Duplicate["absent"]
	env := envLen.Load()
	joined := s.Joined["known_stale"] + s.Joined["absent"]
	t.Logf("#710 burst (PR-1): %d items, envelope %d B; %d readers in %s → fills=%v joined=%v duplicates=%v "+
		"inflight_peak=%d inflight_bytes_peak=%d (%.1f envelopes); HeapInuse %d → peak %d MiB",
		items, env, readers, elapsed, s.Total, s.Joined, s.Duplicate,
		s.InflightPeak, s.InflightBytesPeak, float64(s.InflightBytesPeak)/float64(max(env, 1)),
		base.HeapInuse>>20, peakHeap.Load()>>20)

	if s.Total["known_stale"] == 0 {
		t.Fatalf("no reader read the known-stale cell through: fills=%v", s.Total)
	}
	if dups != 0 || s.InflightPeak != 1 || s.InflightBytesPeak != env {
		t.Fatalf("herd RED: %d readers of one known-stale list → fills=%d duplicates=%d inflight_peak=%d inflight_bytes_peak=%d "+
			"— concurrent fills of one content key were not coalesced (PR-0 main: 200/199/113 envelopes)",
			readers, fills, dups, s.InflightPeak, s.InflightBytesPeak)
	}
	if joined == 0 {
		t.Fatalf("no reader joined the flight (fills=%d) — the burst did not overlap the fill", fills)
	}
}
