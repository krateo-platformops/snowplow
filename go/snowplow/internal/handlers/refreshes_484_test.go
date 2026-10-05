// refreshes_484_test.go — #484 wire-level RED-first falsifiers.
//
// The REAL chain (middleware.RefreshAuth → handlers.Refreshes →
// validateSubscription → cache.SubscribeRefresh → the handler's connection
// goroutine → the HTTP body) with K>1 armed keys × M>1 streams. The client
// models the SPA (refreshSse.ts dispatchRefresh → scheduleRefetch): on every
// `event: refresh` frame it re-reads the announced key from L1 (the /call HIT
// the frame announces) and records the body it now renders. Assertions are
// on that body.
//
//	F-COALESCE (wire) — the PRODUCTION 250 ms coalesce window (seedPanels sets
//	   0; this arm restores the default and proves the window engaged via the
//	   coalesced counter). v1 is signalled and fetched, v2 lands 100 ms later
//	   and is coalesced, then silence: the client must hold v2 within 1 s.
//	F-DROP (wire) — the client stops reading its body, so the handler blocks
//	   in Write on TCP backpressure and the 64-slot sink fills (the production
//	   mechanism: refreshes.go Fprintf+Flush with no write deadline). v2
//	   commits into the full sinks, the client resumes, then silence: the
//	   client must hold v2 within 1 s of resuming. Window 0 here so the flood
//	   that fills the socket buffers is not itself coalesced; the cache-level
//	   twin (internal/cache/refresh_484_falsifier_test.go) runs F-DROP on the
//	   real window.

package handlers

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	w484K = 3 // target keys
	w484M = 3 // streams armed for every key
)

// gatedBody blocks every Read until open is closed — a browser tab whose
// reader has stalled, so the server's writes back up into the socket.
type gatedBody struct {
	r    io.Reader
	open <-chan struct{}
}

func (g gatedBody) Read(p []byte) (int, error) {
	<-g.open
	return g.r.Read(p)
}

// spaStream is one /refreshes connection plus the SPA model reading it.
type spaStream struct {
	mu     sync.Mutex
	holds  map[string]string
	events map[string]int // event name -> count (refresh, and anything else)
	eof    chan struct{}  // closed when the body ends
}

func (s *spaStream) holding(k string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.holds[k]
}

func (s *spaStream) eventCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events[name]
}

// openSPAStream opens GET /refreshes armed for names as userA and starts the
// SPA model on its body. gate==nil reads immediately.
func openSPAStream(t *testing.T, base string, store *cache.ResolvedCacheStore, names []string, gate <-chan struct{}) (*spaStream, context.CancelFunc) {
	t.Helper()
	resp, cancel := openStream(t, base, "?sub="+subParamForNames(t, names), func(req *http.Request) {
		req.Header.Set("Authorization", "Bearer "+mintToken(t, "userA"))
	})
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("stream: status=%d want 200", resp.StatusCode)
	}
	s := &spaStream{holds: map[string]string{}, events: map[string]int{}, eof: make(chan struct{})}
	var body io.Reader = resp.Body
	if gate != nil {
		body = gatedBody{r: resp.Body, open: gate}
	}
	go func() {
		defer close(s.eof)
		defer resp.Body.Close()
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 64*1024), 1<<20)
		event := ""
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
				s.mu.Lock()
				s.events[event]++
				s.mu.Unlock()
			case event == "refresh" && strings.HasPrefix(line, "data: "):
				k := strings.TrimPrefix(line, "data: ")
				body := ""
				if e, ok := store.Get(k); ok && e != nil {
					body = string(e.RawJSON)
				}
				s.mu.Lock()
				s.holds[k] = body
				s.mu.Unlock()
			case line == "":
				event = ""
			}
		}
	}()
	return s, cancel
}

func w484Names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%02d", prefix, i)
	}
	return out
}

func w484Body(k string, gen int) string { return fmt.Sprintf(`{"key":%q,"v":%d}`, k, gen) }

func w484Commit(store *cache.ResolvedCacheStore, k, name, body string) {
	store.Put(k, &cache.ResolvedEntry{RawJSON: []byte(body), Inputs: &cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           panelGVR.Group, Version: panelGVR.Version, Resource: panelGVR.Resource,
		Namespace: evictTestNS, Name: name,
	}})
	cache.PublishRefresh(k) // strictly post-commit, as resolve_populate does
}

func awaitArmed(t *testing.T, keys []string, subs int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cache.RefreshSubscriberCount() == subs {
			all := true
			for _, k := range keys {
				if !cache.HasRefreshSubscriber(k) {
					all = false
				}
			}
			if all {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams not armed within 5s (subs=%d want %d)", cache.RefreshSubscriberCount(), subs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// awaitHold waits until every stream holds want(k) for every key and returns
// the cells that never got there.
func awaitHold(streams []*spaStream, keys []string, gen int, d time.Duration) []string {
	deadline := time.Now().Add(d)
	var miss []string
	for {
		miss = miss[:0]
		for si, s := range streams {
			for _, k := range keys {
				if s.holding(k) != w484Body(k, gen) {
					miss = append(miss, fmt.Sprintf("stream%d/%s…", si, k[:12]))
				}
			}
		}
		if len(miss) == 0 || time.Now().After(deadline) {
			return miss
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRefreshes484_FCoalesce_Wire(t *testing.T) {
	names := w484Names("coal484", w484K)
	seedPanels(t, names)
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "") // the PRODUCTION default (250 ms)
	var keys []string
	store := evictionWiring(t, &keys)
	for _, n := range names {
		keys = append(keys, expectedKey(t, n))
	}
	base := refreshServer(t)

	streams := make([]*spaStream, w484M)
	for i := range streams {
		s, cancel := openSPAStream(t, base, store, names, nil)
		t.Cleanup(cancel)
		streams[i] = s
	}
	awaitArmed(t, keys, w484M)

	for i, k := range keys {
		w484Commit(store, k, names[i], w484Body(k, 1))
	}
	if miss := awaitHold(streams, keys, 1, time.Second); len(miss) > 0 {
		t.Fatalf("setup: v1 not held within 1s by %v", miss)
	}

	time.Sleep(100 * time.Millisecond) // v2 inside the 250 ms window
	_, _, _, coal0 := cache.RefreshBroadcasterCounters()
	for i, k := range keys {
		w484Commit(store, k, names[i], w484Body(k, 2))
	}
	if _, _, _, coal := cache.RefreshBroadcasterCounters(); coal-coal0 != w484K {
		t.Fatalf("precondition: coalesced=%d for the v2 publishes, want %d — the real window did not engage, the arm cannot fail",
			coal-coal0, w484K)
	}
	if miss := awaitHold(streams, keys, 2, time.Second); len(miss) > 0 {
		t.Fatalf("F-COALESCE(wire) RED: %d/%d stream×key cells still render v1 1s after v2's commit — "+
			"the coalesced emit was the LAST change and no trailing frame was written: %v",
			len(miss), w484K*w484M, miss)
	}
}

func TestRefreshes484_FDrop_Wire(t *testing.T) {
	targets := w484Names("drop484", w484K)
	fillers := w484Names("fill484", 40)
	names := append(append([]string{}, targets...), fillers...)
	seedPanels(t, names) // window 0: the socket-filling flood must not be coalesced
	var keys []string
	store := evictionWiring(t, &keys)
	for _, n := range names {
		keys = append(keys, expectedKey(t, n))
	}
	tkeys, fkeys := keys[:w484K], keys[w484K:]
	base := refreshServer(t)

	// v1 is what each tab loaded on mount.
	for i, k := range tkeys {
		store.Put(k, &cache.ResolvedEntry{RawJSON: []byte(w484Body(k, 1))})
		_ = i
	}
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	streams := make([]*spaStream, w484M)
	for i := range streams {
		s, cancel := openSPAStream(t, base, store, names, gate)
		t.Cleanup(cancel)
		s.mu.Lock()
		for _, k := range tkeys {
			s.holds[k] = w484Body(k, 1)
		}
		s.mu.Unlock()
		streams[i] = s
	}
	awaitArmed(t, keys, w484M)

	// Flood fillers until every handler is blocked in Write and every sink
	// is full: the per-connection delivered counters stop moving while we
	// keep publishing.
	totalDelivered := func() (n uint64) {
		for _, s := range cache.RefreshSubSnapshotsForTest() {
			n += s.Delivered
		}
		return n
	}
	deadline := time.Now().Add(20 * time.Second)
	last, stableSince := totalDelivered(), time.Now()
	for {
		for i, k := range fkeys {
			w484Commit(store, k, fillers[i], `{"filler":true}`)
		}
		if cur := totalDelivered(); cur != last {
			last, stableSince = cur, time.Now()
		} else if time.Since(stableSince) > 300*time.Millisecond {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("precondition: the streams never blocked (delivered still moving after 20s)")
		}
		time.Sleep(time.Millisecond)
	}

	// v2 commits into the full sinks, then silence.
	for i, k := range tkeys {
		w484Commit(store, k, targets[i], w484Body(k, 2))
	}
	openGate()
	if miss := awaitHold(streams, tkeys, 2, time.Second); len(miss) > 0 {
		_, _, dropped, _ := cache.RefreshBroadcasterCounters()
		t.Fatalf("F-DROP(wire) RED: %d/%d stream×key cells still render v1 1s after the tab resumed reading "+
			"(dropped=%d: the full-sink send discarded the LAST change): %v",
			len(miss), w484K*w484M, dropped, miss)
	}
}

// floodUntilBlocked publishes the fillers until every handler is parked in
// Write and every sink is full: the per-connection delivered counters stop
// moving while publishing continues.
func floodUntilBlocked(t *testing.T, store *cache.ResolvedCacheStore, fkeys, fillers []string) {
	t.Helper()
	totalDelivered := func() (n uint64) {
		for _, s := range cache.RefreshSubSnapshotsForTest() {
			n += s.Delivered
		}
		return n
	}
	deadline := time.Now().Add(20 * time.Second)
	last, stableSince := totalDelivered(), time.Now()
	for {
		for i, k := range fkeys {
			w484Commit(store, k, fillers[i], `{"filler":true}`)
		}
		if cur := totalDelivered(); cur != last {
			last, stableSince = cur, time.Now()
		} else if time.Since(stableSince) > 300*time.Millisecond {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("precondition: the streams never blocked (delivered still moving after 20s)")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRefreshes484_StallForcesResyncAndReconnectRepairs — the #484 overflow
// rule end to end. Wedged tabs (body reader stalled) with v2 deferred in their
// pending sets: once the consumer has made no progress for the stall bound,
// the broadcaster force-resyncs each subscriber; when the tab's reader
// resumes it reads `event: resync` and the stream ENDS. The SPA's contract
// for an ended stream (refreshSse.ts:572,582-583 → scheduleRetry, :620-626 sets
// revalidateOnConnect; :558-563 connect() → revalidateArmed, :599-614 a
// re-fetch of EVERY armed widget) is modelled literally: reconnect with the
// same coordinates, then re-read every armed key. Assertions: the old stream
// carried the resync frame and ended, the reconnect is armed and LIVE (a
// later commit reaches it), and the re-validation renders v2 everywhere.
func TestRefreshes484_StallForcesResyncAndReconnectRepairs(t *testing.T) {
	targets := w484Names("rsy484", w484K)
	fillers := w484Names("rfil484", 40)
	names := append(append([]string{}, targets...), fillers...)
	seedPanels(t, names)
	restore := cache.SetRefreshPendingStallBoundForTest(300 * time.Millisecond)
	t.Cleanup(restore)
	var keys []string
	store := evictionWiring(t, &keys)
	for _, n := range names {
		keys = append(keys, expectedKey(t, n))
	}
	tkeys, fkeys := keys[:w484K], keys[w484K:]
	base := refreshServer(t)

	for _, k := range tkeys {
		store.Put(k, &cache.ResolvedEntry{RawJSON: []byte(w484Body(k, 1))})
	}
	gate := make(chan struct{})
	var gateOnce sync.Once
	openGate := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(openGate)
	streams := make([]*spaStream, w484M)
	for i := range streams {
		s, cancel := openSPAStream(t, base, store, names, gate)
		t.Cleanup(cancel)
		streams[i] = s
	}
	awaitArmed(t, keys, w484M)
	floodUntilBlocked(t, store, fkeys, fillers)
	for i, k := range tkeys {
		w484Commit(store, k, targets[i], w484Body(k, 2))
	}

	deadline := time.Now().Add(5 * time.Second)
	for cache.RefreshBroadcasterStatsSnapshot().PendingOverflowResync < w484M {
		if time.Now().After(deadline) {
			t.Fatalf("pending_overflow_resync=%d after 5s, want %d — wedged subscribers were never resynced",
				cache.RefreshBroadcasterStatsSnapshot().PendingOverflowResync, w484M)
		}
		time.Sleep(10 * time.Millisecond)
	}

	openGate()
	for i, s := range streams {
		select {
		case <-s.eof:
		case <-time.After(5 * time.Second):
			t.Fatalf("stream%d did not END after its subscriber was resynced", i)
		}
		if n := s.eventCount("resync"); n != 1 {
			t.Fatalf("stream%d carried %d `event: resync` frames before ending, want 1", i, n)
		}
	}

	// SPA: the ended stream → reconnect (same coordinates) → revalidateArmed.
	re := make([]*spaStream, w484M)
	for i := range re {
		s, cancel := openSPAStream(t, base, store, names, nil)
		t.Cleanup(cancel)
		re[i] = s
	}
	awaitArmed(t, keys, w484M)
	for i := range re {
		re[i].mu.Lock()
		for _, k := range tkeys { // revalidateArmed: one re-fetch per armed widget
			if e, ok := store.Get(k); ok && e != nil {
				re[i].holds[k] = string(e.RawJSON)
			}
		}
		re[i].mu.Unlock()
	}
	if miss := awaitHold(re, tkeys, 2, time.Second); len(miss) > 0 {
		t.Fatalf("after resync + reconnect re-validation, %d cells still render v1: %v", len(miss), miss)
	}
	// The reconnected subscribers are live: a later commit reaches them.
	for i, k := range tkeys {
		w484Commit(store, k, targets[i], w484Body(k, 3))
	}
	if miss := awaitHold(re, tkeys, 3, time.Second); len(miss) > 0 {
		t.Fatalf("reconnected streams are not delivering: %d cells missed v3: %v", len(miss), miss)
	}
	for _, s := range cache.RefreshSubSnapshotsForTest() {
		if s.Resynced {
			t.Fatalf("a reconnected subscriber is marked resynced")
		}
	}
}

// TestRefreshes484_WriteDeadlineReleasesWedgedClient — the SSE write deadline:
// a tab that stops reading entirely cannot park its handler goroutine in Write
// forever. With the deadline shortened (production: the 20s heartbeat), the
// blocked write fails, the handler returns and the subscriber is released.
// The stall rule is disabled (bound far beyond the test) so only the
// deadline can end the stream.
func TestRefreshes484_WriteDeadlineReleasesWedgedClient(t *testing.T) {
	fillers := w484Names("wdl484", 40)
	seedPanels(t, fillers)
	prev := refreshWriteDeadlineNS.Swap(int64(300 * time.Millisecond))
	t.Cleanup(func() { refreshWriteDeadlineNS.Store(prev) })
	restore := cache.SetRefreshPendingStallBoundForTest(time.Hour)
	t.Cleanup(restore)
	var keys []string
	store := evictionWiring(t, &keys)
	for _, n := range fillers {
		keys = append(keys, expectedKey(t, n))
	}
	base := refreshServer(t)
	gate := make(chan struct{}) // never opened during the arm
	t.Cleanup(func() { close(gate) })
	for i := 0; i < w484M; i++ {
		_, cancel := openSPAStream(t, base, store, fillers, gate)
		t.Cleanup(cancel)
	}
	awaitArmed(t, keys, w484M)
	closed0 := cache.RefreshBroadcasterStatsSnapshot().StreamsClosedTotal
	floodUntilBlocked(t, store, keys, fillers)

	deadline := time.Now().Add(5 * time.Second)
	for cache.RefreshSubscriberCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d wedged subscribers still held 5s after a 300ms write deadline — a slow client stalls its handler forever",
				cache.RefreshSubscriberCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := cache.RefreshBroadcasterStatsSnapshot().StreamsClosedTotal - closed0; got != w484M {
		t.Fatalf("streams_closed_total moved by %d, want %d", got, w484M)
	}
	if n := cache.RefreshBroadcasterStatsSnapshot().PendingOverflowResync; n != 0 {
		t.Fatalf("pending_overflow_resync=%d — the deadline arm must not be satisfied by the stall rule", n)
	}
}
