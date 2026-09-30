// lazy_register_skip_metrics_test.go — #215 observability counter falsifier.
//
// The counter lazy_register_skipped_unserved_group_total must bump by EXACTLY 1
// each time EnsureResourceType skips a lazy informer registration because the
// GVR's API GROUP is authoritatively absent from ServerGroups() (the #119
// unserved-GROUP pre-check). This suite drives the REAL skip path through
// EnsureResourceType (not the counter helper directly) and discriminates the
// unserved-GROUP cause from the other outcomes that share the funnel:
//
//   - unserved GROUP  → +1   (the counted cause)
//   - served GROUP    → +0   (registers; no skip)
//   - flag OFF        → +0   (pre-check disabled; registers, no skip counted)
//   - nil discovery   → +0   (fail-safe open; registers)
//
// It reuses the #119 harness (skip119Disco / newSkip119Watcher / the shared
// GVRs) so the arms exercise the identical production funnel the #119 falsifier
// pins.
//
// INTERPRETABLE-ZERO arm: LazyRegisterSkipStats must carry the enabled-state
// alongside the count so a zero cannot be misread as health when the pre-check
// is switched off.

package cache_test

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestLazyRegisterSkipCounter_UnservedGroupBumpsByOne is the primary arm: a
// single EnsureResourceType for a GVR under a group absent from ServerGroups()
// must skip AND bump the counter by exactly 1.
func TestLazyRegisterSkipCounter_UnservedGroupBumpsByOne(t *testing.T) {
	cache.ResetLazyRegisterSkipCountersForTest()
	t.Cleanup(cache.ResetLazyRegisterSkipCountersForTest)

	rw := newSkip119Watcher(t) // pre-check ON (default)
	disco := &skip119Disco{servedGroups: map[string]bool{"dead.krateo.io": false}}
	rw.SetDiscoveryClient(disco)

	before := cache.LazyRegisterSkippedUnservedGroup()
	added, _ := rw.EnsureResourceType(skip119UnservedGVR)
	if added {
		t.Fatalf("precondition: dead-group GVR must be skipped (added=false), got added=true")
	}
	after := cache.LazyRegisterSkippedUnservedGroup()

	if delta := after - before; delta != 1 {
		t.Fatalf("unserved-group skip: want counter delta=1, got %d (before=%d after=%d) — "+
			"the #215 counter is not bumped on the real unserved-GROUP skip path", delta, before, after)
	}
	t.Logf("unserved-group skip bumped lazy_register_skipped_unserved_group_total by 1 (before=%d after=%d)", before, after)
}

// TestLazyRegisterSkipCounter_ServedGroupDoesNotBump: a registration under a
// SERVED group must NOT bump the counter (it is not a skip).
func TestLazyRegisterSkipCounter_ServedGroupDoesNotBump(t *testing.T) {
	cache.ResetLazyRegisterSkipCountersForTest()
	t.Cleanup(cache.ResetLazyRegisterSkipCountersForTest)

	rw := newSkip119Watcher(t)
	disco := &skip119Disco{servedGroups: map[string]bool{"served.krateo.io": true}}
	rw.SetDiscoveryClient(disco)

	before := cache.LazyRegisterSkippedUnservedGroup()
	added, _ := rw.EnsureResourceType(skip119ServedCustom)
	if !added {
		t.Fatalf("precondition: served-group GVR must register (added=true), got added=false")
	}
	if delta := cache.LazyRegisterSkippedUnservedGroup() - before; delta != 0 {
		t.Fatalf("served-group registration must NOT bump the skip counter, got delta=%d", delta)
	}
	t.Logf("served-group registration left the counter unchanged (delta=0)")
}

// TestLazyRegisterSkipCounter_FlagOffDoesNotBump: with the pre-check DISABLED,
// the same dead-group GVR registers instead of being skipped, so the counter
// must NOT bump — proving the counter tracks the SKIP, not the mere touch.
func TestLazyRegisterSkipCounter_FlagOffDoesNotBump(t *testing.T) {
	t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "false")
	cache.ResetLazyRegisterSkipCountersForTest()
	t.Cleanup(cache.ResetLazyRegisterSkipCountersForTest)

	rw := newSkip119Watcher(t)
	disco := &skip119Disco{servedGroups: map[string]bool{"dead.krateo.io": false}}
	rw.SetDiscoveryClient(disco)

	before := cache.LazyRegisterSkippedUnservedGroup()
	added, _ := rw.EnsureResourceType(skip119UnservedGVR)
	if !added {
		t.Fatalf("precondition (flag off): dead-group GVR must register (added=true), got added=false")
	}
	if delta := cache.LazyRegisterSkippedUnservedGroup() - before; delta != 0 {
		t.Fatalf("flag-off registration must NOT bump the skip counter, got delta=%d", delta)
	}
	t.Logf("flag-off registration left the counter unchanged (delta=0)")
}

// TestLazyRegisterSkipCounter_NilDiscoveryDoesNotBump: with no discovery client
// (fail-safe open), the pre-check registers rather than skips, so no bump.
func TestLazyRegisterSkipCounter_NilDiscoveryDoesNotBump(t *testing.T) {
	cache.ResetLazyRegisterSkipCountersForTest()
	t.Cleanup(cache.ResetLazyRegisterSkipCountersForTest)

	rw := newSkip119Watcher(t) // no SetDiscoveryClient → rw.disco == nil

	before := cache.LazyRegisterSkippedUnservedGroup()
	added, _ := rw.EnsureResourceType(skip119UnservedGVR)
	if !added {
		t.Fatalf("precondition (nil disco): fail-safe open must register (added=true), got added=false")
	}
	if delta := cache.LazyRegisterSkippedUnservedGroup() - before; delta != 0 {
		t.Fatalf("nil-discovery register (fail-safe open) must NOT bump the skip counter, got delta=%d", delta)
	}
	t.Logf("nil-discovery register left the counter unchanged (delta=0)")
}

// TestLazyRegisterSkipCounter_InterpretableZero: the stats surface must carry
// the enabled-state alongside the count so a zero is not misread as health.
func TestLazyRegisterSkipCounter_InterpretableZero(t *testing.T) {
	cache.ResetLazyRegisterSkipCountersForTest()
	t.Cleanup(cache.ResetLazyRegisterSkipCountersForTest)

	// Feature ON: the pairing must report enabled=true next to the count.
	t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "true")
	stats := cache.LazyRegisterSkipStats()
	if _, ok := stats["skipped_unserved_group_total"]; !ok {
		t.Fatalf("stats must carry skipped_unserved_group_total; got %v", stats)
	}
	enabled, ok := stats["skip_unserved_group_informers_enabled"]
	if !ok {
		t.Fatalf("interpretable-zero: stats MUST carry skip_unserved_group_informers_enabled " +
			"so a zero count can be told apart from a disabled pre-check; it is absent")
	}
	if enabled != true {
		t.Fatalf("interpretable-zero: enabled-state should be true under SKIP_UNSERVED_GROUP_INFORMERS=true, got %v", enabled)
	}

	// Feature OFF: the same surface must now report enabled=false, which is what
	// makes a zero count interpretable (vacuous, not healthy).
	t.Setenv("SKIP_UNSERVED_GROUP_INFORMERS", "false")
	if cache.LazyRegisterSkipStats()["skip_unserved_group_informers_enabled"] != false {
		t.Fatalf("interpretable-zero: enabled-state should flip to false under " +
			"SKIP_UNSERVED_GROUP_INFORMERS=false so the zero reads as vacuous, not health")
	}
	t.Logf("interpretable-zero: count is surfaced alongside enabled-state (true→false tracked)")
}
