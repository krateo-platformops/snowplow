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
// ARMS (§6.2 outcome-table rows, as amended by #547 — the drop-point
// eviction, its REFRESH_DROP_EVICT_MAX_PER_MINUTE breaker and the
// REFRESH_SUPPRESS_AFTER_DECLINES threshold are retired; a refresh that ends
// without a Put leaves the cell resident and KNOWN-STALE)
//
//	F4   row 3 — a 500 from the entry's own object across the whole budget:
//	     the budget is spent (6 invocations), the entry stays RESIDENT and
//	     known-stale, NEVER counted on self_notfound_evict / evict_self_gone.
//	F6   row 6 (#191) — stage-error declines through the REAL loop never
//	     suppress: every dequeue resolves (retry on change), the prior body
//	     stays resident.
//	F6n  row 6, the empty-full site (architect N6) — the (nil, nil) decline
//	     never suppresses either.
//	F6u  row 4 — the seam reports ErrRefreshUnsupported → suppressed on the
//	     FIRST occurrence (structurally permanent), not requeued.
//	F7   row 9 — an apistage CONTENT entry whose call is not pivot-servable
//	     (no global watcher): the typed ErrContentNotServable enters the
//	     budget and the entry stays RESIDENT (read through while known-stale).
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

func TestIssue1126_C4_F4_DeterministicNon404StaysResidentAtTheDropPoint(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusInternalServerError)
	store, _, key, saEP, saRC := i187Fixture(t, srv, true)

	invocations, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()
	cache.EnqueueRefresh(key)
	i1126WaitFor(t, 25*time.Second, "budget spent", func() bool { return invocations.Load() >= i187MaxRequeues+1 })
	i1126WaitFor(t, 5*time.Second, "drop point", func() bool { return cache.RefresherStatsByStat()["dropped"] >= 1 })

	if _, ok := store.Get(key); !ok {
		t.Fatalf("F4: entry EVICTED after %d attempts on a deterministic 500 — #547: a non-404 failure "+
			"is not a deletion; the cell stays resident (zero cold navigations)", invocations.Load())
	}
	if invocations.Load() != i187MaxRequeues+1 {
		t.Fatalf("F4: %d invocations, want %d — the whole budget is spent", invocations.Load(), i187MaxRequeues+1)
	}
	if got := cache.RefresherSelfNotFoundEvictTotal(); got != 0 {
		t.Fatalf("F4: self_notfound_evict_total = %d, want 0 — a 500 must never be counted as a deletion", got)
	}
	if got := cache.Deps().Stats().EvictSelfGoneTotal; got != 0 {
		t.Fatalf("F4: evict_self_gone_total = %d, want 0 — a 500 is not a confirmed 404", got)
	}
	if srv.objectGets.Load() != int64(i187MaxRequeues+1) {
		t.Fatalf("F4: apiserver object GETs = %d, want %d", srv.objectGets.Load(), i187MaxRequeues+1)
	}
}

func TestIssue1126_C4_F6_StageErrorDeclinesNeverSuppress(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusOK) // never reached: the seam replaces resolveOnceProd
	store, _, key, saEP, saRC := i187Fixture(t, srv, true)

	// The seam models a stage that fails under the background identity on
	// EVERY refresh (the exportJwt loopback of #191): bytes come back, the sink
	// records one stage error, and resolveAndPopulateL1 declines the Put.
	var resolves atomic.Int64
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		resolves.Add(1)
		cache.StageErrorSinkFromContext(ctx).Bump("exportJwt", "401 no per-user JWT in a background refresh")
		return []byte(`{"children":[]}`), nil
	})
	t.Cleanup(restore)

	_, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()

	// #547: every change retries — no K, no suppression; the prior body stays.
	for i := int64(1); i <= 4; i++ {
		cache.EnqueueRefresh(key)
		i1126WaitFor(t, 5*time.Second, fmt.Sprintf("decline %d", i), func() bool { return resolves.Load() >= i })
		if e, ok := store.Get(key); !ok || string(e.RawJSON) == `{"children":[]}` {
			t.Fatalf("F6: decline %d must keep the prior body resident (ok=%v)", i, ok)
		}
	}
	if reason, ok := cache.RefreshSuppressedReason(key); ok {
		t.Fatalf("F6: a stage-error decline suppressed the key (reason=%q) — #547 retired "+
			"REFRESH_SUPPRESS_AFTER_DECLINES; a known-stale cell is retried on read or change", reason)
	}
}

// F6n — architect N6: the (nil, nil) "empty full" decline site never
// suppresses since #547 (a transient pre-sync window and a permanent empty full
// both stay retried on read or change).
func TestIssue1126_C4_F6n_EmptyFullDeclinesNeverSuppress(t *testing.T) {
	i187RefresherEnv(t)
	srv := newI187APIServer(t, http.StatusOK) // never reached: the seam replaces resolveOnceProd
	store, _, key, saEP, saRC := i187Fixture(t, srv, true)

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

	_, stop := i1126Loop(t, "widgets", saEP, saRC)
	defer stop()
	for i := int64(1); i <= 4; i++ {
		cache.EnqueueRefresh(key)
		i1126WaitFor(t, 5*time.Second, fmt.Sprintf("decline %d", i), func() bool { return resolves.Load() >= i })
	}
	if e, ok := store.Get(key); !ok || string(e.RawJSON) != origBody {
		t.Fatalf("F6n: after 4 declines the prior body must still be resident (ok=%v)", ok)
	}
	if reason, ok := cache.RefreshSuppressedReason(key); ok {
		t.Fatalf("F6n: an empty-full decline suppressed the key (reason=%q) — #547: never", reason)
	}
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

func TestIssue1126_C4_F7_ApistageNotServableStaysResidentAtTheDropPoint(t *testing.T) {
	i187RefresherEnv(t)
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
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"kind":"Flex","metadata":{"name":"stale"}}`), Inputs: &inputs})
	cache.Deps().Record(context.Background(), key, i187GVR(), i187NS, i187Name)

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
	i1126WaitFor(t, 5*time.Second, "drop point", func() bool { return cache.RefresherStatsByStat()["dropped"] >= 1 })
	if _, ok := store.GetNoTouch(key); !ok {
		t.Fatalf("F7: apistage entry EVICTED after %d not-servable refreshes — #547: it stays resident "+
			"(and is read through while known-stale)", invocations.Load())
	}
	if invocations.Load() != i187MaxRequeues+1 {
		t.Fatalf("F7: %d invocations, want %d", invocations.Load(), i187MaxRequeues+1)
	}
}
