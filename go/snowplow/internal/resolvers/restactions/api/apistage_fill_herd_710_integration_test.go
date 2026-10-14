//go:build integration

// apistage_fill_herd_710_integration_test.go — #710 PR-0: the arrival burst at
// reduced scale, kind-free. One namespace holds 5,000 objects; their LIST cell
// is made known-stale for real (#547 drive, fill710MakeKnownStale); then 200
// readers released together each make a first load of it — the shape design
// §1 predicts for the 50K OOM (every arriving reader re-fills the same list).
//
// No dispatch barrier here: the readers arrive as fast as goroutines start,
// so the counts are what main does under a real burst, not a staged one. The
// arm asserts only what holds under any interleaving of a release-together
// burst (duplicates occurred, more than one envelope was in flight at once,
// and everything was released) and LOGS the peaks: those are the RED numbers
// PR-1 (single-flight) and PR-2 (admission) must move.
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
			if _, served, ok := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
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
	t.Logf("#710 burst (main): %d items, envelope %d B; %d readers in %s → fills=%v duplicates=%v "+
		"inflight_peak=%d inflight_bytes_peak=%d (%.1f envelopes); HeapInuse %d → peak %d MiB",
		items, env, readers, elapsed, s.Total, s.Duplicate,
		s.InflightPeak, s.InflightBytesPeak, float64(s.InflightBytesPeak)/float64(max(env, 1)),
		base.HeapInuse>>20, peakHeap.Load()>>20)

	if s.Total["known_stale"] == 0 {
		t.Fatalf("no reader read the known-stale cell through: fills=%v", s.Total)
	}
	if dups == 0 || s.InflightPeak < 2 {
		t.Fatalf("a release-together burst of %d readers on one known-stale list produced no concurrent duplicate fill "+
			"(fills=%d duplicates=%d inflight_peak=%d) — the herd the arm exists to measure did not happen", readers, fills, dups, s.InflightPeak)
	}
	if s.InflightBytesPeak < 2*env {
		t.Fatalf("inflight_bytes_peak = %d, want ≥ 2 envelopes (%d B) with %d fills in flight at once", s.InflightBytesPeak, 2*env, s.InflightPeak)
	}
}
