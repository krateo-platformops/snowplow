// refresh_484_falsifier_test.go — #484 RED-first falsifiers at the
// broadcaster boundary (the wire-level twins live in
// internal/handlers/refreshes_484_test.go).
//
// The defect: the broadcaster loses a key's LAST change signal two ways, and
// the SPA never repairs either (no polling, no refetch on focus):
//
//	F-COALESCE — leading-edge coalescing with no trailing flush. v1 commits
//	             and is signalled, the client refetches v1, v2 commits 100 ms
//	             later inside the 250 ms window and is suppressed, then the key
//	             goes quiet: no frame for v2 ever arrives.
//	F-DROP     — the per-connection sink is full when v2 commits; the send
//	             drops and nothing remembers the key.
//
// Both run with K>1 keys × M>1 subscribers (feedback_falsifier_shape_must_
// discriminate) so a fix that repairs one subscriber cannot starve another.
// F-COALESCE uses the REAL default window: withRefreshLayer sets the window
// to 0, which would make the arm unable to fail, so these arms set it back
// to "" (the production default) and assert the resolved value is 250 ms.
//
// The "client" models the SPA (refreshSse.ts dispatchRefresh →
// scheduleRefetch): on every `refresh` frame it re-reads the key's L1 body
// (the /call HIT the frame announces) and records it. The assertion is on
// the body the client ends up HOLDING, not a counter.

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

const (
	f484K = 3 // keys per subscriber
	f484M = 3 // subscribers armed for every key
)

// withRealCoalesceWindow enables the layer with the PRODUCTION coalesce
// window (the env unset → defaultRefreshCoalesceWindowMS) and proves it.
func withRealCoalesceWindow(t *testing.T) {
	t.Helper()
	withRefreshLayer(t)
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "3600")
	t.Setenv(envRefreshCoalesceWindowMS, "")
	if w := refreshCoalesceWindowFn(); w != 250*time.Millisecond {
		t.Fatalf("precondition: coalesce window=%v, want the production 250ms (an arm on a 0 window cannot fail)", w)
	}
}

// spaClient is the SPA model for one /refreshes connection: it reads frames
// off the sink and, per frame, re-reads the announced key from L1.
type spaClient struct {
	mu     sync.Mutex
	holds  map[string]string    // key -> body the client currently renders
	frames map[string]int       // key -> frames received
	lastAt map[string]time.Time // key -> time of the last frame
	gate   chan struct{}        // closed = reading; nil = always reading
}

func newSPAClient(t *testing.T, store *ResolvedCacheStore, ch <-chan string, gate chan struct{}) *spaClient {
	t.Helper()
	c := &spaClient{holds: map[string]string{}, frames: map[string]int{}, lastAt: map[string]time.Time{}, gate: gate}
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		if gate != nil {
			select {
			case <-gate:
			case <-stop:
				return
			}
		}
		for {
			select {
			case <-stop:
				return
			case k, ok := <-ch:
				if !ok {
					return
				}
				body := ""
				if e, hit := store.Get(k); hit && e != nil {
					body = string(e.RawJSON)
				}
				c.mu.Lock()
				c.holds[k] = body
				c.frames[k]++
				c.lastAt[k] = time.Now()
				c.mu.Unlock()
			}
		}
	}()
	return c
}

func (c *spaClient) holding(k string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holds[k]
}

// firstFew keeps a RED message readable.
func firstFew(s []string) []string {
	if len(s) > 3 {
		return s[:3]
	}
	return s
}

// awaitAllHold waits until every client holds want(k) for every key.
func awaitAllHold(clients []*spaClient, keys []string, want func(k string) string, d time.Duration) (missing []string) {
	deadline := time.Now().Add(d)
	for {
		missing = missing[:0]
		for ci, c := range clients {
			for _, k := range keys {
				if c.holding(k) != want(k) {
					missing = append(missing, fmt.Sprintf("client%d/%s holds %q", ci, k, c.holding(k)))
				}
			}
		}
		if len(missing) == 0 || time.Now().After(deadline) {
			return missing
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func f484Keys(t *testing.T, prefix string, n int) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-484", Name: fmt.Sprintf("%s-%d", prefix, i)}
		keys[i] = ComputeKey(in)
	}
	return keys
}

func f484Commit(store *ResolvedCacheStore, k, body string) {
	store.Put(k, &ResolvedEntry{RawJSON: []byte(body)})
	PublishRefresh(k) // strictly post-commit, as resolve_populate does
}

func armSet(keys ...[]string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, ks := range keys {
		for _, k := range ks {
			m[k] = struct{}{}
		}
	}
	return m
}

// TestRefresh484_FCoalesce_TrailingChangeReachesClient — F-COALESCE.
func TestRefresh484_FCoalesce_TrailingChangeReachesClient(t *testing.T) {
	withRealCoalesceWindow(t)
	store := ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache nil")
	}
	keys := f484Keys(t, "coalesce", f484K)
	t.Cleanup(func() {
		for _, k := range keys {
			store.DeleteForTest(k)
		}
	})

	clients := make([]*spaClient, f484M)
	for i := range clients {
		ch, unsub := SubscribeRefresh(armSet(keys))
		t.Cleanup(unsub)
		clients[i] = newSPAClient(t, store, ch, nil)
	}

	v := func(gen int) func(string) string {
		return func(k string) string { return fmt.Sprintf(`{"key":%q,"v":%d}`, k, gen) }
	}

	// v1: committed + signalled; every client refetches and holds v1.
	for _, k := range keys {
		f484Commit(store, k, v(1)(k))
	}
	if miss := awaitAllHold(clients, keys, v(1), time.Second); len(miss) > 0 {
		t.Fatalf("setup: v1 not held by every client within 1s: %v", miss)
	}

	// v2 lands 100 ms after v1 — inside the 250 ms window — then silence.
	time.Sleep(100 * time.Millisecond)
	_, _, _, coalBefore := RefreshBroadcasterCounters()
	v2At := time.Now()
	for _, k := range keys {
		f484Commit(store, k, v(2)(k))
	}
	if _, _, _, coal := RefreshBroadcasterCounters(); coal-coalBefore != f484K {
		t.Fatalf("precondition: v2 publishes coalesced=%d, want %d (the arm must exercise the suppressed emit)", coal-coalBefore, f484K)
	}

	// North-star: the client holds v2 within 1 s of v2's commit.
	if miss := awaitAllHold(clients, keys, v(2), time.Second); len(miss) > 0 {
		t.Fatalf("F-COALESCE RED: %d client×key cells still hold the pre-change body 1s after v2's commit "+
			"(the coalesced emit was the key's LAST change and no trailing frame re-announced it): %v",
			len(miss), firstFew(miss))
	}
	t.Logf("F-COALESCE GREEN: all %d×%d cells converged to v2 within %s of its commit",
		f484M, f484K, time.Since(v2At).Round(time.Millisecond))
}

// TestRefresh484_FDrop_FullSinkChangeReachesClient — F-DROP.
func TestRefresh484_FDrop_FullSinkChangeReachesClient(t *testing.T) {
	withRealCoalesceWindow(t)
	store := ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache nil")
	}
	keys := f484Keys(t, "drop", f484K)
	// Distinct filler keys (one emit each, so coalescing cannot absorb them)
	// that fill every subscriber's 64-slot sink.
	fillers := f484Keys(t, "filler", refreshSubChanCap)
	t.Cleanup(func() {
		for _, k := range append(append([]string{}, keys...), fillers...) {
			store.DeleteForTest(k)
		}
	})

	v := func(gen int) func(string) string {
		return func(k string) string { return fmt.Sprintf(`{"key":%q,"v":%d}`, k, gen) }
	}

	// v1 is committed BEFORE anyone subscribes; each client "loaded" v1 on
	// mount (it reads L1 directly — the initial /call).
	for _, k := range keys {
		store.Put(k, &ResolvedEntry{RawJSON: []byte(v(1)(k))})
	}
	gate := make(chan struct{}) // the clients' drains are BLOCKED until closed
	clients := make([]*spaClient, f484M)
	for i := range clients {
		ch, unsub := SubscribeRefresh(armSet(keys, fillers))
		t.Cleanup(unsub)
		clients[i] = newSPAClient(t, store, ch, gate)
		clients[i].mu.Lock()
		for _, k := range keys {
			clients[i].holds[k] = v(1)(k)
		}
		clients[i].mu.Unlock()
	}

	// Fill every sink with the fillers.
	for _, f := range fillers {
		f484Commit(store, f, `{"filler":true}`)
	}
	snap := RefreshBroadcasterStatsSnapshot()
	if snap.MaxSinkDepth != refreshSubChanCap {
		t.Fatalf("precondition: max sink depth=%d, want %d (the sinks must be full)", snap.MaxSinkDepth, refreshSubChanCap)
	}

	// v2 commits into full sinks, then silence.
	for _, k := range keys {
		f484Commit(store, k, v(2)(k))
	}

	// Unblock the drains.
	unblockAt := time.Now()
	close(gate)
	if miss := awaitAllHold(clients, keys, v(2), time.Second); len(miss) > 0 {
		_, _, dropped, _ := RefreshBroadcasterCounters()
		t.Fatalf("F-DROP RED: %d client×key cells still hold v1 1s after the sinks drained "+
			"(dropped=%d: the full-sink send discarded the key's LAST change and nothing re-signalled it): %v",
			len(miss), dropped, firstFew(miss))
	}
	t.Logf("F-DROP GREEN: all %d×%d cells converged to v2 within %s of the unblock",
		f484M, f484K, time.Since(unblockAt).Round(time.Millisecond))
}
