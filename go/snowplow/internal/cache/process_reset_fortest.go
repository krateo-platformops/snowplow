// process_reset_fortest.go — #471: the ONE process-state reset entry point.
//
// WHY THIS FILE EXISTS. internal/cache carries 64 independent, opt-in
// `reset*ForTest` hooks and had no single "make this process look freshly
// booted" entry point. Every cross-package harness therefore depended on the
// author guessing the right subset AND on the hook they needed happening to be
// exported. Both assumptions had already failed in the tree:
//
//   - resetSliceabilityMemoForTest is unexported, so widgets/apiref — the
//     package that lives on that memo — could not reach it from any of its 30
//     ResetResolvedCacheForTest call sites. It worked around that with a
//     per-test raKey (a1_uaf_ra_full_list_bypass_test.go), which is powerless
//     at -count>1, where a test's second iteration IS its own sibling. That is
//     the whole of #471's -count non-idempotence.
//   - ResetServedGroupsMemoForTest / ResetResourceVerbsMemoForTest ARE
//     exported, and widgets/apiref had 0 call sites for either, so the two
//     30s-TTL discovery memos carried one fake discovery client's answers into
//     the next test's differently-registered fake client.
//
// The durable half of the fix is not this function — it is
// process_reset_guard_test.go, which fails when a `reset*ForTest` in a non-test
// file of this package is neither reachable from ResetCacheProcessStateForTest
// nor on that guard's commented exemption list. A new process-global's reset
// cannot silently fail to be composed; the author has to choose, in writing.
//
// WHAT THIS DOES NOT FIX: a process-global with NO reset function at all is
// invisible to the guard by construction. sensitiveSkippedPut
// (sensitive_touched_sink.go:87) is today's example — see the guard header.

package cache

import (
	"fmt"
	"time"
)

// ResetCacheProcessStateForTest returns this process's internal/cache globals
// to their freshly-booted state. TEST-ONLY — production code MUST NOT call it.
//
// CONTRACT: this is a SETUP/TEARDOWN reset, not a mid-test one. It TEARS DOWN
// LIVE INFRASTRUCTURE: ResetDepsForTest stops and JOINS the ResourceWatcher
// currently bound to the dep-watch bridge (deps.go, stopBoundDepWatcherForTest
// -> rw.Stop), and resetRefresherForTest shuts the refresher queue down and
// joins its workers. Call it BEFORE a harness builds its watcher, or from
// t.Cleanup AFTER the arm is done — never between an arm's setup and its
// assertions, and never while a harness-installed watcher must stay live.
//
// For the narrow, mid-test-safe reset — drop the L1 singleton and the
// sliceability index derived from it — keep using ResetResolvedCacheForTest.
// That is what the existing 230 code references want and it is what #471 needed.
//
// ORDER IS LOAD-BEARING, and each step's own doc comment says why:
//
//  1. ResetDepsForTest FIRST. It joins the three readers before writing any
//     tracker field (its own comment enumerates them): the resolved_cache
//     summary goroutine, the bound watcher's goroutines — which includes
//     waitAndPublishInitialRBACSnapshot, joined by rw.Stop's goroutineWG.Wait
//     (watcher.go) — and the dep-event worker. Doing this first eliminates the
//     reader-vs-teardown window for everything that follows, by construction.
//  2. the refresher next: its own comment warns that a worker mid-processOne
//     races the resolved-cache teardown, so it must be joined before step 3.
//  3. the resolved cache, which since #471 also drops the sliceability memo
//     and the reverify worker derived from it.
//  4. the two 30s-TTL discovery memos, so the next test's discovery double is
//     read instead of the previous one's answers.
//  5. the RBAC sub-generation counters and the pending-bump accumulator.
//  6. the published RBAC snapshot LAST, and only after the incremental rebuild
//     has quiesced — see WaitRBACRebuildQuiesceForTest. Step 1 stopped the
//     only thing that can SCHEDULE a rebuild, so by the time we get here the
//     quiesce has a terminating condition.
//
// A quiesce timeout is reported, not swallowed: a caller that ignores the error
// gets today's behaviour, and one that checks it fails loud instead of carrying
// a live rebuild goroutine into the next test.
func ResetCacheProcessStateForTest() error {
	// 1 — deps + the three readers it joins (summary, bound watcher, dep worker).
	ResetDepsForTest()

	// 2 — refresher: ShutDown + join workers.
	resetRefresherForTest()

	// 3 — L1 singleton, plus the sliceability memo/reverifier derived from it.
	resetResolvedCacheForTest()

	// 4 — the two 30s-TTL discovery memos (servable.go). These are what let one
	// test's fake discovery client answer for the next test's, which is how a
	// `restactions` informer gets lazily registered on an RBAC-only fake.
	ResetServedGroupsMemoForTest()
	ResetResourceVerbsMemoForTest()

	// 5 — RBAC sub-generation state.
	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()

	// 5b — #578 lazy LIST-item materialisation counters. Process-global
	// (EnsureItems is a method on ResolvedEntry, which has no back-pointer to a
	// store), so a test that asserts on the trio would otherwise read a sibling
	// test's increments. Composed here rather than exempted because the hook
	// exists precisely so those assertions are deterministic — #471.
	ResetLazyItemsStatsForTest()

	// 6 — the publish container, after the incremental rebuild has quiesced.
	err := WaitRBACRebuildQuiesceForTest(5 * time.Second)
	PublishRBACSnapshotForTest(nil)
	ResetRBACGenForTest()
	return err
}

// rbacRebuildQuiescePollInterval is the poll step of the quiesce below. Matches
// WaitCRDDiscoveryProcessedForTest's 5ms (crd_discovery_side_effect.go) — the
// only other bounded poll over an atomic in this package.
const rbacRebuildQuiescePollInterval = 5 * time.Millisecond

// WaitRBACRebuildQuiesceForTest blocks until no incremental RBAC-snapshot
// rebuild is in flight and none is queued, or until timeout. TEST-ONLY.
//
// WHY A POLL AND NOT A JOIN. scheduleRBACRebuild's goroutine is deliberately
// NOT watcher-owned and NOT joinable: the function returns immediately and
// spawns at most one detached goroutine (rbac_snapshot.go, AC-B.5), because
// k8s informer event handlers must not block. That is a PRODUCTION property and
// #471's design explicitly rejected making it awaited. So the only honest
// test-side seam is to observe the two atomics the rebuild loop already
// maintains: rbacRebuildLock (the single writer slot, released by the
// goroutine's defer) and rbacRebuildDirty (a queued follow-up rebuild rides as
// the next iteration INSIDE that same goroutine, so lock=false AND dirty=false
// together mean "no rebuild is running and none is pending").
//
// This is NOT a general-purpose "wait for RBAC to settle": it terminates only
// once nothing can schedule a further rebuild. ResetCacheProcessStateForTest
// satisfies that by stopping the bound watcher first. Called against a LIVE
// watcher under a steady informer stream it can legitimately time out, and the
// error says so rather than passing quietly.
func WaitRBACRebuildQuiesceForTest(timeout time.Duration) error {
	quiet := func() bool { return !rbacRebuildLock.Load() && !rbacRebuildDirty.Load() }
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if quiet() {
			return nil
		}
		time.Sleep(rbacRebuildQuiescePollInterval)
	}
	if quiet() {
		return nil
	}
	return fmt.Errorf("cache: RBAC rebuild did not quiesce within %s (in_flight=%t dirty=%t)",
		timeout, rbacRebuildLock.Load(), rbacRebuildDirty.Load())
}
