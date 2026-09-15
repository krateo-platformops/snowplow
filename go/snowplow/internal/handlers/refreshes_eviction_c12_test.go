// refreshes_eviction_c12_test.go — 1.12.6 item 7 (C10/C12) wire-level arms
// that use the new broadcaster API (they do not compile on main):
//
//	S9b — during the S9 burst the pending set never exceeds the armed set
//	      (PM condition 4), sampled through the real handler's subscriber.
//	S5  — the SSE dimension, server-side guarantees over the real handler:
//	      a full sink defers (never drops) and delivers once the connection
//	      goroutine catches up — covered by S9 (100 keys > the 64-slot sink);
//	      a closed stream folds into streams_closed_total and an eviction
//	      with nobody armed costs nothing; a RECONNECT arms afresh and the
//	      next eviction reaches the new stream; a HUB RESET (the disabled-
//	      mid-stream / restart shape) leaves the old stream idle and the
//	      new stream delivering. Client-side convergence after a missed
//	      publish (S3/S7) is the SPA's contract (frontend#256), not the
//	      server's — the server keeps no replay.

package handlers

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRefreshes_S9b_PendingNeverExceedsArmed(t *testing.T) {
	frames, _, _, cancel := s9BurstSetup(t)
	defer cancel()

	// Sample the live subscriber while the drain runs.
	samples := 0
	deadline := time.Now().Add(6 * time.Second)
	delivered := 0
	for delivered < s9N && time.Now().Before(deadline) {
		for _, s := range cache.RefreshSubSnapshotsForTest() {
			samples++
			if s.Pending > s.Armed {
				t.Fatalf("S9b RED: pending=%d exceeds armed=%d on the live subscriber", s.Pending, s.Armed)
			}
			if s.Armed != s9N {
				t.Fatalf("S9b: armed=%d want %d", s.Armed, s9N)
			}
		}
		select {
		case <-frames:
			delivered++
		case <-time.After(10 * time.Millisecond):
		}
	}
	if delivered < s9N {
		t.Fatalf("S9b: %d/%d delivered within the window", delivered, s9N)
	}
	if samples == 0 {
		t.Fatalf("S9b probe: zero samples — the assertion never ran")
	}
	st := cache.RefreshBroadcasterStatsSnapshot()
	if hw := cache.RefreshEvictPendingHighWaterForTest(); hw > s9N || hw < int64(s9N-s9Burst-s9Rate) {
		t.Fatalf("S9b: pending high-water %d outside [%d, %d] (armed %d; burst %d)", hw, s9N-s9Burst-s9Rate, s9N, s9N, s9Burst)
	}
	if st.EvictDeferred < uint64(s9N-s9Burst-s9Rate) {
		t.Fatalf("S9b RED: evict_deferred=%d for a %d-key burst at burst %d — the bound did not engage", st.EvictDeferred, s9N, s9Burst)
	}
	if st.Dropped != 0 {
		t.Fatalf("S9b RED: dropped=%d — an eviction signal was dropped instead of deferred", st.Dropped)
	}
	if st.EvictPublished != s9N {
		t.Fatalf("S9b: evict_published=%d want %d", st.EvictPublished, s9N)
	}
	t.Logf("S9b: %d samples, pending high-water %d / armed %d, evict_deferred %d, max_sink_depth %d",
		samples, cache.RefreshEvictPendingHighWaterForTest(), s9N, st.EvictDeferred, st.MaxSinkDepth)
}

func TestRefreshes_S5_ReconnectArmsAfreshAndDelivers(t *testing.T) {
	const name = "dashboard-piechart"
	seedPanels(t, []string{name})
	var keys []string
	store := evictionWiring(t, &keys)
	key := expectedKey(t, name)
	keys = append(keys, key)
	base := refreshServer(t)

	// Stream 1 opens, then the tab goes away.
	_, cancel1 := openArmedStream(t, base, []string{name}, keys)
	cancel1()
	deadline := time.Now().Add(5 * time.Second)
	for cache.RefreshSubscriberCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("S5: stream 1 did not unsubscribe after the client went away")
		}
		time.Sleep(5 * time.Millisecond)
	}
	st := cache.RefreshBroadcasterStatsSnapshot()
	if st.StreamsClosedTotal != 1 || st.StreamSecondsTotal <= 0 {
		t.Fatalf("S5 RED: after a closed stream streams_closed_total=%d stream_seconds_total=%v", st.StreamsClosedTotal, st.StreamSecondsTotal)
	}

	// An eviction while nobody is armed: nothing published, nothing counted.
	putSelfEntry(store, key, name)
	cache.Deps().OnDelete(panelGVR, evictTestNS, name)
	if st := cache.RefreshBroadcasterStatsSnapshot(); st.EvictPublished != 0 || st.Delivered != 0 {
		t.Fatalf("S5: eviction with no subscriber moved counters: %+v", st)
	}

	// Reconnect: the new stream re-derives and re-arms; the next eviction
	// reaches it. (The missed one is the SPA's reconnect-refetch, S3/S7.)
	frames2, cancel2 := openArmedStream(t, base, []string{name}, keys)
	defer cancel2()
	putSelfEntry(store, key, name)
	cache.Deps().OnDelete(panelGVR, evictTestNS, name)
	f, ok := awaitFrame(t, frames2, 3*time.Second)
	if !ok || f.key != key {
		t.Fatalf("S5 RED: reconnected stream got no frame for the eviction (ok=%v key=%q)", ok, f.key)
	}
}

func TestRefreshes_S5_HubResetOldStreamIdleNewStreamDelivers(t *testing.T) {
	const name = "dashboard-piechart"
	seedPanels(t, []string{name})
	var keys []string
	store := evictionWiring(t, &keys)
	key := expectedKey(t, name)
	keys = append(keys, key)
	base := refreshServer(t)

	frames1, cancel1 := openArmedStream(t, base, []string{name}, keys)
	defer cancel1()

	// The hub is torn down under the live stream (the restart / disabled-
	// mid-stream shape). The old connection stays open and idle — its sink
	// is never closed, so the handler keeps heartbeating.
	cache.ResetRefreshBroadcasterForTest()
	if cache.RefreshSubscriberCount() != 0 {
		t.Fatalf("S5: subscribers=%d after hub reset, want 0", cache.RefreshSubscriberCount())
	}

	frames2, cancel2 := openArmedStream(t, base, []string{name}, keys)
	defer cancel2()
	putSelfEntry(store, key, name)
	cache.Deps().OnDelete(panelGVR, evictTestNS, name)
	f, ok := awaitFrame(t, frames2, 3*time.Second)
	if !ok || f.key != key {
		t.Fatalf("S5 RED: post-reset stream got no frame for the eviction (ok=%v key=%q)", ok, f.key)
	}
	if _, stale := awaitFrame(t, frames1, 200*time.Millisecond); stale {
		t.Fatalf("S5: the pre-reset stream received a frame from the new hub")
	}
}
