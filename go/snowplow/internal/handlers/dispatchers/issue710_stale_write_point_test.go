//go:build integration

// #710 PR-2b — the D2 write-point guarantee at the production dispatchers
// (architect ruling, #710 issuecomment-6195582848): a stale serve without a
// guaranteed push path never happens.
//
//   - D2-15  a stale serve whose Put is declined is never written: the
//     response is the FRESH body (re-run with D2 disabled), withdrawn{unarmable}
//     moves, and the content debt still clears.
//     Mutation → RED: drop the write-point guard (staleWritePoint returns false
//     without withdrawing).
//   - D2-15b the re-run is declined too: still a fresh body (unarmed), and the
//     re-run never re-enters D2 — exactly one stale decision.
//     Mutation → RED: re-run without cache.WithStaleServeDisabled.
//   - D2-15c the stale content is read inside a NESTED RESTAction resolve
//     under a widget; the widget's Put is declined: identical to D2-15 (the
//     outermost dispatcher saw StaleDerived).
//     Mutation → RED: a sink not inherited by nested resolves (e.g. the nested
//     resolve re-scopes to its own sink).
//   - D2-17  invariant sweep: every stale-derived body written in these
//     scenarios had an accepted Put + the armed header + an owed-push mark.
//     Mutation → RED: CommitStaleServe skipped (no owed mark) or a stale body
//     written unarmed.
package dispatchers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// k710Write is one customer body write observed at the write point.
type k710Write struct {
	key                 string
	staleDerived, armed bool
	owed                bool
}

// k710Writes installs the D2-17 write-point observer.
func k710Writes(t *testing.T) func() []k710Write {
	t.Helper()
	var mu sync.Mutex
	var got []k710Write
	prev := staleWriteHookForTest
	staleWriteHookForTest = func(key string, staleDerived, armed bool) {
		w := k710Write{key: key, staleDerived: staleDerived, armed: armed, owed: cache.StaleOwed(key)}
		mu.Lock()
		got = append(got, w)
		mu.Unlock()
	}
	t.Cleanup(func() { staleWriteHookForTest = prev })
	return func() []k710Write {
		mu.Lock()
		defer mu.Unlock()
		return append([]k710Write(nil), got...)
	}
}

// k710KnownStaleFound is F3c's setup: the shared `found` content cell (a
// cluster LIST read by the RA's stage) is KNOWN-STALE (its refresh flipped to
// not-servable) while the informer holds comp-1 at v1. Background resolves are
// allowed afterwards so a D2 debt can re-resolve its dependents.
func k710KnownStaleFound(t *testing.T) (*k547Harness, string) {
	t.Helper()
	h := newK547Harness(t, k547DenyBackground)
	h.contentMode = true
	cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(context.Context, string, cache.ResolvedKeyInputs) error {
		return cache.ErrContentNotServable
	})
	for k := range k547Cohorts {
		for _, m := range []int{0, 2} {
			if r, code := h.get(k, m); code != 200 || r != "v0" {
				t.Fatalf("warm (k=%d,m=%d): code=%d reason=%q", k, m, code, r)
			}
		}
	}
	var contentKey string
	n := 0
	cache.ResolvedCache().RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassApistage {
			contentKey = m.KeyHash
			n++
		}
		return true
	})
	if n != 1 {
		t.Fatalf("PRECONDITION: %d apistage content cells after warm, want exactly the one `found` cluster LIST", n)
	}
	h.update(k547Changed, "v1")
	if !k547WaitFor(t, 30*time.Second, "content cell known-stale", func() bool {
		_, r, ok := cache.KnownStale(contentKey)
		return ok && r == "not_servable"
	}) {
		t.Fatalf("PRECONDITION: the `found` content cell never became known-stale")
	}
	h.allowBackground.Store(true)
	return h, contentKey
}

// k710AssertFreshWithdrawn is D2-15's assertion set, shared by 15/15b/15c.
func k710AssertFreshWithdrawn(t *testing.T, arm, served string, code int, contentKey string,
	ss0, wd0 uint64, writes []k710Write, wantArmed bool) {
	t.Helper()
	if code != 200 || served != "v1" {
		t.Fatalf("%s RED: served %q (code %d), want the FRESH v1 — a stale body whose Put was declined has no push path "+
			"and must never be written", arm, served, code)
	}
	st := restactionsapi.StaleServeStatsSnapshot()
	if d := st.ServeTotal - ss0; d != 1 {
		t.Fatalf("%s: stale_serve_total moved %d, want exactly 1 (the first resolve's decision; the re-run must never re-enter D2)", arm, d)
	}
	if d := st.WithdrawnUnarmable - wd0; d != 1 {
		t.Fatalf("%s: stale_serve_withdrawn_total{unarmable} moved %d, want 1", arm, d)
	}
	for _, w := range writes {
		if w.staleDerived {
			t.Fatalf("%s RED: a stale-derived body was written (key armed=%v owed=%v)", arm, w.armed, w.owed)
		}
	}
	if len(writes) != 1 {
		t.Fatalf("%s: %d body writes observed, want exactly 1 (the re-run's)", arm, len(writes))
	}
	if writes[0].armed != wantArmed {
		t.Fatalf("%s: the re-run's write armed=%v, want %v", arm, writes[0].armed, wantArmed)
	}
	// The content debt is not withdrawn with the dependent: it still clears.
	if !k547WaitFor(t, 10*time.Second, "debt cleared", func() bool {
		_, _, ks := cache.KnownStale(contentKey)
		return restactionsapi.StaleServeStatsSnapshot().DebtOpen == 0 && !ks
	}) {
		t.Fatalf("%s: the content debt never cleared after the withdrawal (%+v)", arm, restactionsapi.StaleServeStatsSnapshot())
	}
}

func TestIssue710_D2_15_ADeclinedStaleServeIsNeverWrittenTheResponseIsFresh(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(1) // the first resolve's Put is declined; the re-run's is accepted
	s0 := restactionsapi.StaleServeStatsSnapshot()
	r, code := h.getRA(0, k547Changed)
	k710AssertFreshWithdrawn(t, "D2-15", r, code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), true)
}

func TestIssue710_D2_15b_TheReRunNeverReEntersD2EvenWhenItIsDeclinedToo(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(-1) // every customer resolve declines, the re-run included
	s0 := restactionsapi.StaleServeStatsSnapshot()
	r, code := h.getRA(0, k547Changed)
	k710AssertFreshWithdrawn(t, "D2-15b", r, code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), false)
}

func TestIssue710_D2_15c_ANestedStaleServeUnderADeclinedWidgetIsNeverWritten(t *testing.T) {
	h, contentKey := k710KnownStaleFound(t)
	writes := k710Writes(t)
	h.customerExternal.Store(1)
	s0 := restactionsapi.StaleServeStatsSnapshot()
	// The widget's resolve runs the RA's `found` stage in-process (a nested
	// RESTAction resolve under the widget dispatcher's ctx and sink).
	r, code := h.get(0, k547Changed)
	k710AssertFreshWithdrawn(t, "D2-15c", r, code, contentKey, s0.ServeTotal, s0.WithdrawnUnarmable, writes(), true)
}

func TestIssue710_D2_17_EveryStaleWriteHasAnAcceptedPutAnArmedHeaderAndAnOwedMark(t *testing.T) {
	h, _ := k710KnownStaleFound(t)
	writes := k710Writes(t)
	// A committed stale serve (widget, cohort 0) …
	if r, code := h.get(0, k547Changed); code != 200 || r != "v0" {
		t.Fatalf("D2-17: cohort 0 served %q (code %d), want the stale v0 shown at once", r, code)
	}
	// … a declined one (RA, cohort 1) …
	h.customerExternal.Store(1)
	if r, code := h.getRA(1, k547Changed); code != 200 || r != "v1" {
		t.Fatalf("D2-17: cohort 1's declined RA served %q (code %d), want fresh v1", r, code)
	}
	// … and the replace converging.
	k547WaitFor(t, 10*time.Second, "widget replaced", func() bool {
		rr, ok := h.residentReason(h.key(0, k547Changed))
		return ok && rr == "v1"
	})
	nStale := 0
	for _, w := range writes() {
		if !w.staleDerived {
			continue
		}
		nStale++
		if !w.armed || !w.owed {
			t.Fatalf("D2-17 RED: a stale-derived body was written with armed=%v owed=%v — every stale write needs an "+
				"accepted Put, the armed header and an owed-push mark", w.armed, w.owed)
		}
		if e, ok := cache.ResolvedCache().GetNoTouch(w.key); ok && e.StaleDerived {
			t.Fatalf("D2-17: the stale-derived cell is still resident after the replace window")
		}
	}
	if nStale == 0 {
		t.Fatalf("D2-17: no stale-derived write observed — the sweep is vacuous")
	}
}
