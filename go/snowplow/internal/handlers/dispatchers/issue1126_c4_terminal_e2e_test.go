// issue1126_c4_terminal_e2e_test.go — 1.12.6 C4 (design §6) falsifiers BELOW
// the RefreshFunc seam: the real refresher loop → the production
// resolveAndPopulateL1 → resolveOnceProd → the wire (or the resolveOnce test
// seam where the arm needs a stage-error / unsupported-kind outcome that no
// hermetic apiserver can produce).
//
// The cache-package arms (refresh_terminal_test.go) prove the drop point and
// the suppression consult with a stub handler. Per
// feedback_seamed_dispatch_cannot_falsify_a_deep_frame those cannot observe
// the decline sites in resolveAndPopulateL1, so the GREEN drivers for rows
// 3/4/6/9 live here.
//
// ARMS (§6.2 outcome-table rows)
//
//	F4   row 3 — a 500 from the entry's own object across the whole budget:
//	     EVICTED at the drop point (default breaker), counted on
//	     drop_evict_total, NEVER on self_notfound_evict; the budget is spent
//	     first (6 invocations). RED on main: entry resident (drop-to-TTL).
//	F6   row 6 (#191) — three consecutive stage-error declines through the
//	     REAL loop → the key is suppressed; the 4th dequeue does not resolve;
//	     one real Put clears it and the 5th dequeue resolves again.
//	     RED on main: the 4th dequeue resolves (the WARN storm).
//	F6n  row 6, the empty-full site (architect N6) — the seam returns
//	     (nil, nil) three times → suppressed with reason "empty_full"; the
//	     4th dequeue does not resolve; ONE decline does NOT suppress (a
//	     transient pre-sync window clears inside K). RED on main: the 4th
//	     dequeue resolves (the site was "skip-to-TTL forever").
//	F6u  row 4 — the seam reports ErrRefreshUnsupported → suppressed on the
//	     FIRST occurrence, not requeued (1 invocation, no refresh_dropped).
//	F7   row 9 — an apistage CONTENT entry whose call is not pivot-servable
//	     (no global watcher): the typed ErrContentNotServable enters the
//	     budget and the entry is EVICTED at the drop point. RED on main:
//	     (nil, nil) skip-to-TTL, entry resident after ONE invocation.
package dispatchers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/client-go/rest"
)

// i1126Loop drives the real refresher for `class` (the production
// resolveAndPopulateL1 behind the RefreshFunc seam) and returns the handler
// invocation counter and a stop that drains the pool.
func i1126Loop(t *testing.T, class string, saEP *endpoints.Endpoint, saRC *rest.Config) (invocations *atomic.Int64, stop func()) {
	t.Helper()
	invocations = &atomic.Int64{}
	cache.RegisterRefreshFunc(class, func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		invocations.Add(1)
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	stop = func() { cancel(); cache.ResetRefresherForTest() }
	return invocations, stop
}

func i1126WaitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestIssue1126_C4_F4_DeterministicNon404IsEvictedAtTheDropPoint(t *testing.T) {
	i187RefresherEnv(t)
	t.Setenv("REFRESH_DROP_EVICT_MAX_PER_MINUTE", "64") // the default, pinned explicitly
	srv := newI187APIServer(t, http.StatusInternalServerError)
	store, _, key, saEP, saRC := i187Fixture(t, srv, true)

	// Own loop rather than i187RunRefreshCycle: the breaker counters live on
	// the refresher singleton, which that helper resets before returning.
	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 25*time.Second, "budget spent", func() bool { return invocations.Load() >= i187MaxRequeues+1 })
	i1126WaitFor(t, 5*time.Second, "drop point", func() bool { _, ok := store.Get(key); return !ok })

	if _, ok := store.Get(key); ok {
		t.Fatalf("F4 RED: entry still resident after %d attempts on a deterministic 500 — "+
			"1.12.6 §6.2 row 3: a failure that exhausts the budget EVICTS (no forget-and-keep)", invocations.Load())
	}
	if invocations.Load() != i187MaxRequeues+1 {
		t.Fatalf("F4: %d invocations, want %d — the budget must be spent BEFORE the eviction "+
			"(a transient that clears inside it never gets there)", invocations.Load(), i187MaxRequeues+1)
	}
	st := cache.RefreshTerminalStatsSnapshot()
	if st.DropEvictTotal != 1 {
		t.Fatalf("F4: drop_evict_total = %d, want 1", st.DropEvictTotal)
	}
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Fatalf("F4: self_notfound_evict_total = %d, want 0 — a 500 must never be counted as a deletion", got)
	}
	// PM condition 1: the drop-point eviction has its OWN tracker counter.
	// evict_self_gone_total keeps meaning "confirmed 404" so an apiserver
	// outage (drop-point evictions climbing) cannot read as mass deletion.
	// Map lookup rather than a typed field so the arm compiles — and reads
	// zero — against a tree without the counter.
	if got := cache.DepsStatsByStat()["evict_drop_point_total"]; got != 1 {
		t.Fatalf("F4: evict_drop_point_total = %d, want 1 — the drop-point eviction must go through the "+
			"tracker (dep edges cleared) and be counted on its own stat", got)
	}
	if got := cache.Deps().Stats().EvictSelfGoneTotal; got != 0 {
		t.Fatalf("F4: evict_self_gone_total = %d, want 0 — a 500 is not a confirmed 404", got)
	}
	if srv.objectGets.Load() != int64(i187MaxRequeues+1) {
		t.Fatalf("F4: apiserver object GETs = %d, want %d", srv.objectGets.Load(), i187MaxRequeues+1)
	}
}

func TestIssue1126_C4_F6_StageErrorDeclinesSuppressThenPutResumes(t *testing.T) {
	i187RefresherEnv(t)
	t.Setenv("REFRESH_SUPPRESS_AFTER_DECLINES", "3")
	srv := newI187APIServer(t, http.StatusOK) // never reached: the seam replaces resolveOnceProd
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

	// The seam models a stage that fails under the SA identity on EVERY
	// refresh (the exportJwt loopback of #191): bytes come back, the sink
	// records one stage error, and resolveAndPopulateL1 declines the Put.
	var resolves atomic.Int64
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		resolves.Add(1)
		cache.StageErrorSinkFromContext(ctx).Bump("exportJwt", "401 no per-user JWT in a background refresh")
		return []byte(`{"children":[]}`), nil
	})
	t.Cleanup(restore)

	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()

	for i := int64(1); i <= 3; i++ {
		cache.EnqueueRefresh(key)
		i1126WaitFor(t, 5*time.Second, fmt.Sprintf("decline %d", i), func() bool { return resolves.Load() >= i })
		if e, ok := store.Get(key); !ok || string(e.RawJSON) == `{"children":[]}` {
			t.Fatalf("F6: decline %d must keep the prior good body resident (ok=%v)", i, ok)
		}
	}

	// 4th dequeue: skipped without a resolve. BEHAVIOUR FIRST — wait for
	// either outcome, then assert the count, so the RED transcript on a tree
	// without the mechanism is the #191 symptom (a 4th resolve), not a
	// marker-API answer.
	before := cache.RefreshTerminalStatsSnapshot().SuppressedSkipsTotal
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "4th dequeue settled (skipped or resolved)", func() bool {
		return cache.RefreshTerminalStatsSnapshot().SuppressedSkipsTotal > before || resolves.Load() >= 4
	})
	time.Sleep(50 * time.Millisecond)
	if resolves.Load() != 3 || invocations.Load() != 3 {
		t.Fatalf("F6 RED: a suppressed key was resolved again (resolves=%d handler=%d, want 3/3) — "+
			"pre-1.12.6 this key re-resolved every dirty-mark forever (#191)", resolves.Load(), invocations.Load())
	}
	if reason, ok := cache.RefreshSuppressedReason(key); !ok || reason != "stage_error" {
		t.Fatalf("F6: key not marked suppressed after 3 consecutive stage-error declines (reason=%q ok=%v)", reason, ok)
	}

	// One real Put (a user /call storing fresh bytes) clears the marker → the
	// 5th dequeue resolves again.
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"children":["fresh"]}`), Inputs: &inputs})
	if _, still := cache.RefreshSuppressedReason(key); still {
		t.Fatalf("F6 RED: the marker survived a real Put — suppress-and-never-recover")
	}
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "resolve after Put", func() bool { return resolves.Load() >= 4 })
}

// F6n — architect N6: the (nil, nil) "empty full" decline site in
// resolveAndPopulateL1 joins the suppression mechanism. One decline must NOT
// suppress (a pre-sync window is transient); K=3 consecutive ones must.
func TestIssue1126_C4_F6n_EmptyFullDeclinesSuppressAfterKNotAfterOne(t *testing.T) {
	i187RefresherEnv(t)
	t.Setenv("REFRESH_SUPPRESS_AFTER_DECLINES", "3")
	srv := newI187APIServer(t, http.StatusOK) // never reached: the seam replaces resolveOnceProd
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

	var resolves atomic.Int64
	restore := setResolveOnceForTest(func(_ context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		resolves.Add(1)
		return nil, nil // the RAFullList "no full produced" sentinel
	})
	t.Cleanup(restore)

	orig, ok := store.Get(key)
	if !ok {
		t.Fatal("F6n: fixture entry missing")
	}
	origBody := string(orig.RawJSON)

	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()

	// ONE decline: resident, NOT suppressed, and the next dequeue resolves.
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "decline 1", func() bool { return resolves.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if _, ok := store.Get(key); !ok {
		t.Fatal("F6n: one empty-full decline must keep the prior good body resident")
	}
	if reason, ok := cache.RefreshSuppressedReason(key); ok {
		t.Fatalf("F6n: ONE empty-full decline suppressed the key (reason=%q) — a transient pre-sync window must clear inside K", reason)
	}
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "decline 2 (not suppressed after one)", func() bool { return resolves.Load() >= 2 })

	// Third decline reaches K=3.
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "decline 3", func() bool { return resolves.Load() >= 3 })
	if e, ok := store.Get(key); !ok || string(e.RawJSON) != origBody {
		t.Fatalf("F6n: after 3 declines the prior good body must still be resident (ok=%v)", ok)
	}

	// 4th dequeue: BEHAVIOUR FIRST — skipped without a resolve.
	before := cache.RefreshTerminalStatsSnapshot().SuppressedSkipsTotal
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "4th dequeue settled (skipped or resolved)", func() bool {
		return cache.RefreshTerminalStatsSnapshot().SuppressedSkipsTotal > before || resolves.Load() >= 4
	})
	time.Sleep(50 * time.Millisecond)
	if resolves.Load() != 3 || invocations.Load() != 3 {
		t.Fatalf("F6n RED: an empty-full key was resolved again after K declines (resolves=%d handler=%d, want 3/3) — "+
			"the (nil, nil) site was skip-to-TTL forever (#191 shape)", resolves.Load(), invocations.Load())
	}
	if reason, ok := cache.RefreshSuppressedReason(key); !ok || reason != "empty_full" {
		t.Fatalf("F6n: suppression reason = (%q, %v), want (\"empty_full\", true)", reason, ok)
	}

	// A real Put clears it and the next dequeue resolves again.
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"children":["fresh"]}`), Inputs: &inputs})
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "resolve after Put", func() bool { return resolves.Load() >= 4 })
}

func TestIssue1126_C4_F6u_UnsupportedKindSuppressesOnFirstOccurrence(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusOK)
	store, inputs, key, saEP, saRC := i187Fixture(t, srv, true)

	restore := setResolveOnceForTest(func(_ context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		return nil, fmt.Errorf("refresh of class %q: %w", in.CacheEntryClass, cache.ErrRefreshUnsupported)
	})
	t.Cleanup(restore)

	// Classification at the site: a decline, not an error (no requeue).
	if err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC); err != nil {
		t.Fatalf("F6u: want nil (suppressed, nothing to retry), got %v", err)
	}
	if reason, ok := cache.RefreshSuppressedReason(key); !ok || reason != "unsupported_kind" {
		t.Fatalf("F6u RED: unsupported kind not suppressed on first occurrence (reason=%q ok=%v)", reason, ok)
	}
	if _, ok := store.Get(key); !ok {
		t.Fatalf("F6u: suppression must keep the entry resident")
	}
	cache.ResetRefresherForTest() // clears the marker state too
	// And through the loop: exactly ONE invocation, no refresh_dropped.
	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 5*time.Second, "one invocation", func() bool { return invocations.Load() >= 1 })
	time.Sleep(100 * time.Millisecond)
	if invocations.Load() != 1 {
		t.Fatalf("F6u: %d invocations, want 1 — an unsupported kind must not burn the requeue budget", invocations.Load())
	}
	if _, ok := store.Get(key); !ok {
		t.Fatalf("F6u: entry evicted; an unsupported kind is a suppression, not an eviction")
	}
}

func TestIssue1126_C4_F7_ApistageNotServableIsEvictedAtTheDropPoint(t *testing.T) {
	i187RefresherEnv(t)
	t.Setenv("REFRESH_DROP_EVICT_MAX_PER_MINUTE", "64")
	srv := newI187APIServer(t, http.StatusOK)
	store, _, _, saEP, saRC := i187Fixture(t, srv, true)

	// An apistage CONTENT entry: its refresh is one informer re-dispatch, and
	// with no global watcher (cache.Global()==nil → informer gate 4) the call
	// is not pivot-servable — exactly the pre-sync / metadata-only shape.
	cache.SetGlobal(nil)
	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassApistage,
		Group:           i187Group, Version: i187Version, Resource: i187Resource,
		Namespace: i187NS, Name: i187Name,
	}
	key := cache.ComputeKey(inputs)
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"kind":"Flex","metadata":{"name":"stale"}}`), Inputs: &inputs})
	cache.Deps().Record(key, i187GVR(), i187NS, i187Name)

	// Classification first: typed, and NOT a self-gone.
	err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)
	if !errors.Is(err, cache.ErrContentNotServable) {
		t.Fatalf("F7 RED: want ErrContentNotServable so the refresher REQUEUES; got %v (pre-1.12.6: (nil, nil) skip-to-TTL)", err)
	}
	if errors.Is(err, cache.ErrSelfObjectGone) {
		t.Fatalf("F7: not-servable classified as a deletion")
	}

	invocations, stop := i1126Loop(t, cache.CacheEntryClassApistage, saEP, saRC)
	defer stop()
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 25*time.Second, "budget spent", func() bool { return invocations.Load() >= i187MaxRequeues+1 })
	i1126WaitFor(t, 5*time.Second, "drop point", func() bool { _, ok := store.Get(key); return !ok })
	if _, ok := store.Get(key); ok {
		t.Fatalf("F7 RED: apistage entry still resident after %d not-servable refreshes — the one carrier with a "+
			"single layer must reach the drop point like every other class", invocations.Load())
	}
	if invocations.Load() != i187MaxRequeues+1 {
		t.Fatalf("F7: %d invocations, want %d", invocations.Load(), i187MaxRequeues+1)
	}
	if st := cache.RefreshTerminalStatsSnapshot(); st.DropEvictTotal != 1 {
		t.Fatalf("F7: drop_evict_total = %d, want 1", st.DropEvictTotal)
	}
}
