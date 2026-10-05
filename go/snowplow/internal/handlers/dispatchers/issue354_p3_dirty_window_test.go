// issue354_p3_dirty_window_test.go — #354 P3: the invalidation→fresh window
// (brief: #354 issuecomment-5990885016). Falsifiers BELOW the RefreshFunc seam:
// the real refresher loop → the production resolveAndPopulateL1 → the real store,
// with the dep tracker's own OnUpdate/OnDelete as the informer event. Only the
// resolve itself is the resolveOnce seam, because each arm needs an outcome (a
// mid-resolve dep move, a stage error, a deterministic failure) that a hermetic
// apiserver cannot produce on cue.
//
// Every stat is read through the published maps (the refresher's tagged family,
// snowplow_resolved_cache), never a typed field, so on a tree without P3 the
// arms compile and FAIL on the absent series.
//
// ARMS
//
//	F6b  #375 (discriminating) — a dep moves mid-resolve: the Put is accepted AND
//	     remarked → dirty_ended_unfresh_remarked +1, NO sample at that Put; the one
//	     sample lands at the later clean Put and is >= (clean Put − ORIGINAL mark).
//	     An implementation that restarts the window at the remark fails the bound.
//	F6c  decline / evict / drop / floor:
//	     - a stage-error decline → dirty_ended_unfresh_declined +1, no sample;
//	     - a DELETE between mark and dequeue → dirty_ended_unfresh_evicted +1;
//	     - a poison pill → dirty_ended_unfresh_dropped +1 (and NOT evicted, though
//	       the drop point evicts the entry right after);
//	     - a rate-floor deferral → the sample includes the deferral.
//	F6u  (team-lead condition 2) an unchanged-content refresh is FRESH: it closes
//	     the window as a sample, never as a decline.
//	F6e  parked — customer in-flight held 2 s with one resident key queued →
//	     parked_ms Δ >= 1900; held 6 s → capped +1.
//
// The K>1 × M>1 end-to-end arm (F6) is issue354_p3_f6_e2e_test.go; F6d / F6f
// live in the cache package; D-OTLP in internal/metrics.

package dispatchers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var p3GVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}

const p3NS = "p3"

// p3RefStat reads one stat of the refresher's tagged family (the map expvar and
// the OTLP mirror both publish). 0 when the stat does not exist.
func p3RefStat(name string) int64 {
	for _, f := range cache.TaggedStatFamilies() {
		if f.OTelName != "snowplow_refresher" {
			continue
		}
		switch x := f.Values()[name].(type) {
		case int64:
			return x
		case float64:
			return int64(x)
		}
	}
	return 0
}

// p3StoreStat reads one snowplow_resolved_cache stat.
func p3StoreStat(name string) int64 { return cache.ResolvedCacheStatsByStat()[name] }

// p3Unfresh reads the four flattened unfresh outcomes (the names the #354
// driver queries read: sum:snowplow_refresher{stat=dirty_ended_unfresh_<reason>}).
func p3Unfresh() map[string]int64 {
	out := map[string]int64{}
	for _, r := range []string{"remarked", "evicted", "declined", "dropped"} {
		out[r] = p3RefStat("dirty_ended_unfresh_" + r)
	}
	return out
}

// p3Cell is one resident widgets-class cell with its self dep edge recorded, so
// Deps().OnUpdate / OnDelete on (p3GVR, p3NS, name) is the informer event.
type p3Cell struct {
	name   string
	key    string
	inputs cache.ResolvedKeyInputs
}

func p3Env(t *testing.T) *cache.ResolvedCacheStore {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i187RefresherEnv(t)
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)
	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	cache.Deps().SetStore(store)
	return store
}

func p3PutCell(t *testing.T, store *cache.ResolvedCacheStore, name, body string) p3Cell {
	t.Helper()
	in := cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  p3GVR.Group,
		Version:                p3GVR.Version,
		Resource:               p3GVR.Resource,
		Namespace:              p3NS,
		Name:                   name,
		BindingUID:             "uid-p3",
		RepresentativeUsername: "admin",
		RepresentativeGroups:   []string{"admins"},
		SubjectBindingSet:      rbac.SubjectBindingSetDigest("admin", []string{"admins"}),
	}
	key := cache.ComputeKey(in)
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(body), Inputs: &in})
	cache.Deps().Record(context.Background(), key, p3GVR, p3NS, name)
	return p3Cell{name: name, key: key, inputs: in}
}

// p3Loop starts the real refresher with the production RefreshFunc body for the
// widgets class. stop drains the pool (call it before the seam restore runs).
func p3Loop(t *testing.T) (invocations *atomic.Int64, stop func()) {
	t.Helper()
	return i1126Loop(t, "widgets", nil, nil)
}

func p3Wait(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	i1126WaitFor(t, d, what, cond)
}

func p3Body(store *cache.ResolvedCacheStore, key string) string {
	if e, ok := store.GetNoTouch(key); ok {
		return string(e.RawJSON)
	}
	return "<absent>"
}

// --- F6b ---------------------------------------------------------------------

func TestIssue354_P3_F6b_RemarkKeepsTheOriginalWindowOpen(t *testing.T) {
	store := p3Env(t)
	c := p3PutCell(t, store, "f6b", `{"v":"old"}`)
	dep := "f6b-dep"

	var calls atomic.Int64
	var cleanReturn atomic.Int64 // unix nanos of the clean resolve's return
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		n := calls.Add(1)
		key := cache.L1KeyFromContext(ctx)
		cache.Deps().Record(ctx, key, p3GVR, p3NS, dep)
		if n == 1 {
			// The dep this body was built from moves DURING the resolve: the #375
			// Put-then-remark check sees it at the accepted Put.
			cache.Deps().OnUpdate(p3GVR, p3NS, dep)
			time.Sleep(300 * time.Millisecond)
			return []byte(`{"v":"mid"}`), nil
		}
		cleanReturn.Store(time.Now().UnixNano())
		return []byte(`{"v":"new"}`), nil
	})
	t.Cleanup(restore)

	u0, s0 := p3Unfresh(), p3RefStat("dirty_to_fresh_samples")
	_, stop := p3Loop(t)
	defer stop()
	tMark := time.Now()
	if n := cache.Deps().OnUpdate(p3GVR, p3NS, c.name); n == 0 {
		t.Fatalf("PRE: the informer UPDATE must dirty-mark the resident cell")
	}
	p3Wait(t, 10*time.Second, "the clean re-resolve", func() bool {
		return calls.Load() >= 2 && p3Body(store, c.key) == `{"v":"new"}`
	})
	p3Wait(t, 5*time.Second, "the clean Put's sample", func() bool { return p3RefStat("dirty_to_fresh_samples") > s0 })
	time.Sleep(50 * time.Millisecond)

	u1 := p3Unfresh()
	if d := u1["remarked"] - u0["remarked"]; d != 1 {
		t.Fatalf("F6b: dirty_ended_unfresh_remarked Δ=%d, want 1 — the accepted-and-remarked Put must end the "+
			"dequeue unfresh (the cell holds a body built from a moved dep)", d)
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != 1 {
		t.Fatalf("F6b: dirty_to_fresh_samples Δ=%d, want exactly 1 — NO sample at the remarked Put, one at the clean Put", d)
	}
	bound := time.Duration(cleanReturn.Load()-tMark.UnixNano()) / time.Millisecond
	if got := p3RefStat("dirty_to_fresh_ms_max"); got < int64(bound)-1 {
		t.Fatalf("F6b DISCRIMINATING: the sample is %d ms but the clean Put came %d ms after the ORIGINAL mark — "+
			"the window was restarted at the remark instead of kept open from the first mark", got, bound)
	}
}

// --- F6c ---------------------------------------------------------------------

func TestIssue354_P3_F6c_StageErrorDeclineEndsUnfresh(t *testing.T) {
	store := p3Env(t)
	c := p3PutCell(t, store, "f6c-decl", `{"v":"good"}`)
	var calls atomic.Int64
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		calls.Add(1)
		cache.StageErrorSinkFromContext(ctx).Bump("exportJwt", "401")
		return []byte(`{"v":"partial"}`), nil
	})
	t.Cleanup(restore)

	u0, s0 := p3Unfresh(), p3RefStat("dirty_to_fresh_samples")
	_, stop := p3Loop(t)
	defer stop()
	cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
	p3Wait(t, 5*time.Second, "the declined refresh", func() bool { return calls.Load() >= 1 })
	p3Wait(t, 5*time.Second, "the decline outcome", func() bool { return p3Unfresh()["declined"] > u0["declined"] })
	time.Sleep(50 * time.Millisecond)
	if d := p3Unfresh()["declined"] - u0["declined"]; d != 1 {
		t.Fatalf("F6c: dirty_ended_unfresh_declined Δ=%d, want 1", d)
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != 0 {
		t.Fatalf("F6c: a declined refresh must not record a sample (Δ=%d)", d)
	}
	if p3Body(store, c.key) != `{"v":"good"}` {
		t.Fatalf("PRE: the decline must keep the prior body")
	}
}

func TestIssue354_P3_F6c_DeleteBetweenMarkAndDequeueEndsEvicted(t *testing.T) {
	store := p3Env(t)
	c := p3PutCell(t, store, "f6c-del", `{"v":"old"}`)
	var calls atomic.Int64
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"v":"new"}`), nil
	})
	t.Cleanup(restore)

	// Park the worker (customer in flight) so the DELETE lands between the mark
	// and the dequeue's real work.
	var hold atomic.Bool
	hold.Store(true)
	cache.SetCustomerInflightHook(hold.Load)
	u0, s0 := p3Unfresh(), p3RefStat("dirty_to_fresh_samples")
	_, stop := p3Loop(t)
	defer stop()
	cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
	time.Sleep(100 * time.Millisecond)
	cache.Deps().OnDelete(p3GVR, p3NS, c.name)
	if _, ok := store.GetNoTouch(c.key); ok {
		t.Fatalf("PRE: the DELETE must evict the cell")
	}
	hold.Store(false)
	p3Wait(t, 5*time.Second, "the evicted outcome", func() bool { return p3Unfresh()["evicted"] > u0["evicted"] })
	time.Sleep(100 * time.Millisecond)
	if d := p3Unfresh()["evicted"] - u0["evicted"]; d != 1 {
		t.Fatalf("F6c: dirty_ended_unfresh_evicted Δ=%d, want exactly 1", d)
	}
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != 0 || calls.Load() != 0 {
		t.Fatalf("F6c: an evicted window must not sample (Δ=%d) nor resolve (calls=%d)", d, calls.Load())
	}
}

func TestIssue354_P3_F6c_PoisonPillEndsDropped(t *testing.T) {
	store := p3Env(t)
	c := p3PutCell(t, store, "f6c-drop", `{"v":"old"}`)
	var calls atomic.Int64
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		calls.Add(1)
		return nil, errors.New("deterministic failure")
	})
	t.Cleanup(restore)

	u0 := p3Unfresh()
	_, stop := p3Loop(t)
	defer stop()
	cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
	p3Wait(t, 20*time.Second, "the budget spent", func() bool { return calls.Load() >= i187MaxRequeues+1 })
	p3Wait(t, 5*time.Second, "the dropped outcome", func() bool { return p3Unfresh()["dropped"] > u0["dropped"] })
	time.Sleep(100 * time.Millisecond)
	u1 := p3Unfresh()
	if d := u1["dropped"] - u0["dropped"]; d != 1 {
		t.Fatalf("F6c: dirty_ended_unfresh_dropped Δ=%d, want 1 (retries keep the window; only the drop ends it)", d)
	}
	if d := u1["evicted"] - u0["evicted"]; d != 0 {
		t.Fatalf("F6c: the drop-point eviction after a drop must not count the window twice (evicted Δ=%d)", d)
	}
}

func TestIssue354_P3_F6c_RateFloorDeferralIsInsideTheWindow(t *testing.T) {
	store := p3Env(t)
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "1")
	var calls atomic.Int64
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		calls.Add(1)
		return []byte(`{"v":"new"}`), nil
	})
	t.Cleanup(restore)

	s0 := p3RefStat("dirty_to_fresh_samples")
	_, stop := p3Loop(t)
	defer stop()
	tPut := time.Now()
	c := p3PutCell(t, store, "f6c-floor", `{"v":"old"}`)
	tMark := time.Now()
	cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
	p3Wait(t, 5*time.Second, "the deferred refresh", func() bool { return p3RefStat("dirty_to_fresh_samples") > s0 })
	if p3RefStat("floored") == 0 {
		t.Fatalf("PRE: the dequeue must have been floored (the arm cannot fail otherwise)")
	}
	// The floor expires 1 s after the Put; the window opened at the mark.
	bound := (time.Second - tMark.Sub(tPut)) / time.Millisecond
	if got := p3RefStat("dirty_to_fresh_ms_max"); got < int64(bound)-5 {
		t.Fatalf("F6c: the sample (%d ms) must include the rate-floor deferral (>= %d ms)", got, bound)
	}
}

// --- F6u (team-lead condition 2) ---------------------------------------------

func TestIssue354_P3_F6u_UnchangedContentRefreshIsFresh(t *testing.T) {
	store := p3Env(t)
	const body = `{"v":"same"}`
	c := p3PutCell(t, store, "f6u", body)
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(body), nil // byte-identical to the resident body
	})
	t.Cleanup(restore)

	u0, s0 := p3Unfresh(), p3RefStat("dirty_to_fresh_samples")
	_, stop := p3Loop(t)
	defer stop()
	cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
	p3Wait(t, 5*time.Second, "the sample", func() bool { return p3RefStat("dirty_to_fresh_samples") > s0 })
	time.Sleep(50 * time.Millisecond)
	if d := p3RefStat("dirty_to_fresh_samples") - s0; d != 1 {
		t.Fatalf("F6u: an unchanged-content refresh must close the window as ONE sample (Δ=%d)", d)
	}
	for r, v := range p3Unfresh() {
		if v != u0[r] {
			t.Fatalf("F6u: an unchanged-content refresh is fresh, not %s (Δ=%d)", r, v-u0[r])
		}
	}
}

// --- F6e ---------------------------------------------------------------------

func TestIssue354_P3_F6e_ParkedTimeIsAccumulated(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hold   time.Duration
		minMS  int64
		capped int64
	}{
		{name: "2s", hold: 2 * time.Second, minMS: 1900, capped: 0},
		{name: "6s-capped", hold: 6 * time.Second, minMS: 4900, capped: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := p3Env(t)
			c := p3PutCell(t, store, "f6e-"+tc.name, `{"v":"old"}`)
			var calls atomic.Int64
			restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
				calls.Add(1)
				return []byte(`{"v":"new"}`), nil
			})
			t.Cleanup(restore)
			var hold atomic.Bool
			hold.Store(true)
			cache.SetCustomerInflightHook(hold.Load)
			p0, c0 := p3RefStat("parked_ms"), p3RefStat("capped")
			_, stop := p3Loop(t)
			defer stop()
			cache.Deps().OnUpdate(p3GVR, p3NS, c.name)
			time.Sleep(tc.hold)
			hold.Store(false)
			p3Wait(t, 5*time.Second, "the refresh after the park", func() bool { return calls.Load() >= 1 })
			if d := p3RefStat("parked_ms") - p0; d < tc.minMS {
				t.Fatalf("F6e: parked_ms Δ=%d, want >= %d (the worker parked for the whole hold)", d, tc.minMS)
			}
			if d := p3RefStat("capped") - c0; d != tc.capped {
				t.Fatalf("F6e: capped Δ=%d, want %d", d, tc.capped)
			}
		})
	}
}
