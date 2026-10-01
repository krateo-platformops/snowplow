// refresh_emit_seam_test.go — Ship 1 (live-refresh-coherence, option A)
// emit-seam falsifiers. feedback_falsifier_first_before_ship: these are A's
// gate for the emit point (resolve_populate.go:291). Hermetic: the
// setResolveOnceForTest seam + in-process ResolvedCache + in-process
// broadcaster — NO live cluster, NO apiserver (KUBECONFIG unset). NEVER
// ./internal/rbac.
//
// Covers:
//   9.2 / 9.3 — coherence + emit-once: a refresher cycle that Puts fires
//          EXACTLY ONE PublishRefresh, and the signal arrives only AFTER the
//          fresh bytes are committed to L1 (the cell holds FRESH at signal
//          time — emit is strictly post-Put).
//   9.3 — the §1.1 CORRECTION: a cycle that hits any of the FOUR no-Put
//          success-returns (cache-off, declined encoded==nil, evicted-during-
//          refresh, stage-error decline) fires ZERO PublishRefresh. This is
//          the encoded test that the emit seam is at the Put, not in processOne
//          (which would over-fire on all four).
//   9.1 — the cache-respecting invariant: after the signal, the refetch reads
//          the freshly-committed L1 entry as a HIT (fresh bytes present); no
//          apiserver read is involved (there is none in the harness — a
//          non-HIT would mean the refetch had to leave L1).

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	emitStaleBytes = `{"phase":"v1-OLD"}`
	emitFreshBytes = `{"phase":"v2-FRESH"}`
)

// withEmitSeamHarness enables cache + the SSE layer (coalescing OFF so each
// emit is observable) and resets all singletons. Returns the inputs + key the
// refresher will Put.
func withEmitSeamHarness(t *testing.T) (cache.ResolvedKeyInputs, string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0") // no coalescing — observe every emit
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(func() {
		cache.ResetRefreshBroadcasterForTest()
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
	})
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  "widgets.templates.krateo.io",
		Version:                "v1beta1",
		Resource:               "buttons",
		Namespace:              "demo",
		Name:                   "save-btn",
		BindingUID:             "uid-c01dface",
		RepresentativeUsername: "cyberjoker",
		RepresentativeGroups:   []string{"devs"},
	}
	return inputs, cache.ComputeKey(inputs)
}

// --- 9.2 / 9.3 — real Put fires exactly one coherent signal -----------------

// TestEmitSeam_PutFiresExactlyOneCoherentSignal asserts that a refresher cycle
// which commits a fresh entry fires EXACTLY ONE PublishRefresh, the signal
// carries the committed key, and at signal-arrival time L1 already holds the
// FRESH bytes (coherent-by-construction: emit is strictly post-Put). It then
// asserts the refetch is an L1 HIT for the fresh bytes (9.1, cache-respecting).
func TestEmitSeam_PutFiresExactlyOneCoherentSignal(t *testing.T) {
	inputs, key := withEmitSeamHarness(t)

	c := cache.ResolvedCache()
	// Seed the prior (stale) entry — the realistic live-refresh case.
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(emitStaleBytes), Inputs: &inputs})

	// Arm a /refreshes subscriber for this key BEFORE the re-resolve.
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()

	// The re-resolve commits the FRESH bytes.
	restore := setResolveOnceForTest(func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(emitFreshBytes), nil
	})
	t.Cleanup(restore)

	pubBefore, _, _, _ := cache.RefreshBroadcasterCounters()

	if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1 returned error: %v", err)
	}

	// The signal must arrive (post-Put).
	select {
	case got := <-ch:
		if got != key {
			t.Fatalf("signal carried key %q want %q", got, key)
		}
	case <-time.After(time.Second):
		t.Fatalf("no PublishRefresh signal after a real Put — the emit seam did not fire")
	}

	// COHERENCE (9.2): at signal time L1 holds the FRESH bytes (emit is
	// strictly AFTER c.Put). The refetch reads them as a HIT (9.1) — no
	// apiserver involved.
	entry, ok := c.Get(key)
	if !ok || entry == nil {
		t.Fatalf("entry absent after refresh — incoherent")
	}
	if string(entry.RawJSON) != emitFreshBytes {
		t.Fatalf("9.2 incoherent: signal fired but L1 holds %q want %q (emit fired before/without the fresh Put)",
			entry.RawJSON, emitFreshBytes)
	}

	// EMIT-ONCE (9.3): exactly one PublishRefresh for the one Put.
	pubAfter, _, _, _ := cache.RefreshBroadcasterCounters()
	if pubAfter != pubBefore+1 {
		t.Fatalf("published counter %d -> %d; want +1 (one Put must fire exactly one signal)", pubBefore, pubAfter)
	}
	// And no further signal queued (the Put fired once, not per-stage).
	select {
	case extra := <-ch:
		t.Fatalf("a SECOND signal %q arrived for one Put — emit over-fires", extra)
	case <-time.After(150 * time.Millisecond):
	}
}

// --- 9.3 — the four no-Put success-returns fire ZERO signals ----------------

// runNoPutCase seeds a prior entry, arms a subscriber, runs resolveAndPopulateL1
// with the given stub, and returns the PublishRefresh delta + whether a signal
// was observed on the channel. Used by each no-Put case.
func runNoPutCase(t *testing.T, stub func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error), preSeed bool) (delta uint64, signalled bool) {
	t.Helper()
	inputs, key := withEmitSeamHarness(t)
	c := cache.ResolvedCache()
	if preSeed && c != nil {
		c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(emitStaleBytes), Inputs: &inputs})
	}
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()

	restore := setResolveOnceForTest(stub)
	t.Cleanup(restore)

	pubBefore, _, _, _ := cache.RefreshBroadcasterCounters()
	if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
		// declined / evicted / stage-error cases all return nil; a non-nil
		// error here is a different failure.
		t.Fatalf("resolveAndPopulateL1 returned error: %v", err)
	}
	pubAfter, _, _, _ := cache.RefreshBroadcasterCounters()

	select {
	case <-ch:
		signalled = true
	case <-time.After(150 * time.Millisecond):
	}
	return pubAfter - pubBefore, signalled
}

// TestEmitSeam_DeclinedEncodedNil_NoSignal — resolve_populate.go:214-218: the
// handler declined (encoded==nil). No Put -> no signal.
func TestEmitSeam_DeclinedEncodedNil_NoSignal(t *testing.T) {
	delta, signalled := runNoPutCase(t, func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		return nil, nil // declined
	}, true)
	if delta != 0 || signalled {
		t.Fatalf("declined (encoded==nil) fired %d signals (observed=%v) — want 0; emit must not fire on the no-Put decline return",
			delta, signalled)
	}
}

// TestEmitSeam_StageErrorDecline_NoSignal — resolve_populate.go:243-260: the
// re-resolve observed a stage error; the Put is declined to keep the prior good
// entry. No Put -> no signal.
func TestEmitSeam_StageErrorDecline_NoSignal(t *testing.T) {
	delta, signalled := runNoPutCase(t, func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		if sink := cache.StageErrorSinkFromContext(ctx); sink != nil {
			sink.Bump("test-stage", "emulated stage error")
		}
		return []byte(`{"list":[]}`), nil // under-served result, declined by the gate
	}, true)
	if delta != 0 || signalled {
		t.Fatalf("stage-error decline fired %d signals (observed=%v) — want 0; emit must not fire when the Put is declined",
			delta, signalled)
	}
}

// TestEmitSeam_EvictedDuringRefresh_NoSignal — resolve_populate.go:223-229: the
// entry was DELETE-evicted during the re-resolve; the fresh bytes are dropped
// (not resurrected). No Put -> no signal. We model the eviction by NOT
// pre-seeding the entry: the re-Get at :223 finds it absent.
func TestEmitSeam_EvictedDuringRefresh_NoSignal(t *testing.T) {
	delta, signalled := runNoPutCase(t, func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(emitFreshBytes), nil // resolve succeeds...
	}, false) // ...but the entry is gone (never seeded) -> :223 alive==false -> no Put
	if delta != 0 || signalled {
		t.Fatalf("evicted-during-refresh fired %d signals (observed=%v) — want 0; a non-resurrecting refresh must not signal",
			delta, signalled)
	}
}

// TestEmitSeam_S189_RefreshRePutRacingReinsert_RefusedNoClobberNoSignal is the
// MANDATORY #189 positive refresher-race-closure arm. It DISCRIMINATES the
// generation guard from the old racy Get-before-Put: a real DELETE-evict AND a
// cold RE-INSERT (fresh body at a new generation) both land DURING the
// re-resolve. The old code's Get-before-Put would find the key ALIVE (the
// re-inserted entry) and CLOBBER it with the refresher's stale body; the #189
// ReplaceIfGen(gen0) sees the generation moved and REFUSES, so the fresh
// re-inserted body survives and no signal fires. RED on the pre-#189 tree
// (clobbered → stale), GREEN after.
func TestEmitSeam_S189_RefreshRePutRacingReinsert_RefusedNoClobberNoSignal(t *testing.T) {
	inputs, key := withEmitSeamHarness(t)
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatal("setup: resolved cache is nil with the harness gates on")
	}
	// Seed the entry so the refresher captures a LIVE generation (gen0) at the
	// head of resolveAndPopulateL1, before the resolve below.
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(emitStaleBytes), Inputs: &inputs})

	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{key: {}})
	defer unsub()

	const freshBody = `{"reinserted":"by-a-cold-request-mid-refresh"}`
	// The resolve stub performs, DURING the re-resolve (after gen0 was captured,
	// before the tail write): a REAL DELETE-evict, then a cold RE-INSERT of a
	// FRESH body at the new generation. The refresher then tries to write its
	// now-STALE body.
	restore := setResolveOnceForTest(func(_ context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		c.DeleteForTest(key)             // real deleteForDep — bumps the generation
		reinsertGen := c.CaptureGen(key) // the fresh generation post-delete
		c.PutIfGen(context.Background(), key, &cache.ResolvedEntry{RawJSON: []byte(freshBody), Inputs: &in}, reinsertGen)
		return []byte(emitStaleBytes), nil // the refresher's STALE body
	})
	t.Cleanup(restore)

	pubBefore, _, _, _ := cache.RefreshBroadcasterCounters()
	refusedBefore := c.Stats().PutRefusedGenerationMovedTotal
	if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1 error: %v", err)
	}
	pubAfter, _, _, _ := cache.RefreshBroadcasterCounters()

	// EXPLICIT REFUSAL discriminator (not just no-clobber): ReplaceIfGen must have
	// REFUSED the stale re-Put because the generation moved — the refusal counter
	// bumps exactly once.
	if got := c.Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
		t.Fatalf("#189 refresher race RED: the stale re-Put was NOT refused (refused_total %d->%d) — ReplaceIfGen must refuse when the generation moved during the re-resolve", refusedBefore, got)
	}

	// The fresh re-inserted body must survive — the stale refresh must NOT clobber it.
	got, ok := c.Get(key)
	if !ok {
		t.Fatalf("#189 refresher race: the re-inserted entry is gone")
	}
	if string(got.RawJSON) != freshBody {
		t.Fatalf("#189 refresher race RED: the refresher's stale body CLOBBERED the re-inserted fresh body "+
			"(got %s) — ReplaceIfGen must refuse when the generation moved", got.RawJSON)
	}
	// A refused re-Put changed no L1 state → no signal.
	if pubAfter != pubBefore {
		t.Fatalf("#189 refresher race: a refused re-Put emitted a signal (delta=%d)", pubAfter-pubBefore)
	}
	select {
	case <-ch:
		t.Fatalf("#189 refresher race: a refused re-Put fired a refresh signal")
	case <-time.After(150 * time.Millisecond):
	}
}

// TestEmitSeam_CacheOff_NoSignal — resolve_populate.go:96-99: cache off, the
// function returns before the Put. No Put -> no signal. (Belt-and-braces to the
// broadcaster's own cache-off unreachability test 9.5a.)
func TestEmitSeam_CacheOff_NoSignal(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	cache.ResetResolvedCacheForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(func() {
		cache.ResetRefreshBroadcasterForTest()
		cache.ResetResolvedCacheForTest()
	})
	inputs := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "demo", Name: "save-btn"}

	restore := setResolveOnceForTest(func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(emitFreshBytes), nil
	})
	t.Cleanup(restore)

	pubBefore, _, _, _ := cache.RefreshBroadcasterCounters()
	if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
		t.Fatalf("resolveAndPopulateL1 returned error under cache-off: %v", err)
	}
	pubAfter, _, _, _ := cache.RefreshBroadcasterCounters()
	if pubAfter != pubBefore {
		t.Fatalf("cache-off fired %d signals — want 0; the emit line is unreachable when cache is off (resolve_populate.go:96-99 returns first)",
			pubAfter-pubBefore)
	}
}
