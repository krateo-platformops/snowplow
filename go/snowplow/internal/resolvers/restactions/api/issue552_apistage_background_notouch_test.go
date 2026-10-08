// issue552_apistage_background_notouch_test.go — #552: the apistage content read
// primitive must be chosen by WHO is resolving, not by whether a refresh TRIGGER
// GVR happens to be on the ctx.
//
// THE DEFECT. readContent asked `refresherDriven`, which is true only when the
// dispatch CARRIES a trigger GVR. A background dispatch with no trigger — the
// sliceability re-verify (cache/ra_full_list_slice.go, the dominant enqueue
// source), the proactive pass (cache/resolved.go), the no-hook fallback
// (cache/deps.go), and every cohort-seed / engine-boot resolve — fell through to
// `default: store.Get`, which stamps lastRead, moves the LRU front, bumps
// hit_total and trips noteServeWhileDirty → stale_served_total. Four counters
// that gate #354 acceptance, moved by snowplow reading its own cache.
//
// THE FIX. The read primitive keys on background-ness
// (cache.BackgroundResolveFromContext); forceContentMiss KEEPS keying on trigger
// equality. Two questions, two predicates.
//
// BOTH FAILURE DIRECTIONS ARE ARMED, because the cheap wrong fix here is to key
// BOTH on the same flag:
//   - under-reach (the defect): a trigger-less background read stamps the cell →
//     arm A, RED on origin/main with hit_total +1 and lastRead stamped.
//   - over-reach (#544 reintroduced): force-missing stops happening, so a
//     refresher re-resolve whose own GVR IS the trigger serves the STALE cell it
//     was supposed to re-dispatch → arm D, which asserts the FRESH shape comes
//     back. GREEN on origin/main; it exists to go red if this change widens
//     forceContentMiss or drops it.
//
// Reuses the R1 harness (apistage_refresh_bypass_falsifier_test.go) — the same
// stale content cell and live-fresh informer object, so the arms read the
// production shape: a stale snapshot resident, a fresher object behind it.

package api

import (
	"context"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// put552StaleContentEntry seeds the same STALE content cell as
// putR1StaleContentEntry, but marked SeededAtBoot so the cell starts at
// LastReadSeconds == -1 (never read).
//
// WHY THAT MATTERS: a non-seeded Put stamps lastRead = CreatedAt (resolved.go,
// the cold-fill branch) and LastReadSeconds is published in SECONDS, so on a
// plain Put the surface reads 0 both before and after a stamping read and the
// assertion would be vacuous. Seeded, it reads -1 before and >= 0 after, which
// discriminates without sleeping a second to let the clock tick.
func put552StaleContentEntry(store *cache.ResolvedCacheStore) string {
	key := cache.ComputeKey(contentKeyInputs(r1FsaGVR, r1Ns, r1Name))
	stale := map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata":   map[string]any{"namespace": r1Ns, "name": r1Name},
		"status":     map[string]any{"managed": r1ManagedList(r1StaleManaged)},
	}
	raw, _ := json.Marshal(stale)
	store.Put(key, &cache.ResolvedEntry{
		RawJSON:      raw,
		Inputs:       ptr.To(contentKeyInputs(r1FsaGVR, r1Ns, r1Name)),
		SeededAtBoot: true,
	})
	return key
}

func TestIssue552_ApistageContentServe_TriggerlessBackgroundReadIsInternal(t *testing.T) {
	_ = newR1RefreshBypassWatcher(t) // cache.Global watcher + the FRESH live object
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache nil")
	}
	key := put552StaleContentEntry(store)

	lastRead := func(t *testing.T) int64 {
		t.Helper()
		m, ok := store.MetadataForKey(key)
		if !ok {
			t.Fatal("content cell absent")
		}
		return m.LastReadSeconds
	}
	// freshCell re-creates the never-read stale cell before EVERY arm.
	//
	// WHY PER-ARM AND NOT ONCE. lastRead is cell state, so without this the
	// FIRST arm that stamps it poisons every later arm: measured on origin/main,
	// the trigger-carrying arm (which main reads correctly through GetNoTouch)
	// reported a lastRead stamp it had not made, inherited from the trigger-less
	// arm before it. A red for the previous arm's reason is indistinguishable
	// from a red for its own.
	freshCell := func(t *testing.T) {
		t.Helper()
		store.DeleteForTest(key)
		put552StaleContentEntry(store)
		if got := lastRead(t); got != -1 {
			t.Fatalf("premise: the seeded cell must start at LastReadSeconds=-1, got %d", got)
		}
	}
	freshCell(t)

	otherGVR := schema.GroupVersionResource{Group: "other.krateo.io", Version: "v1", Resource: "others"}
	internalReads := []struct {
		name string
		wrap func(context.Context) context.Context
	}{
		{
			// THE #552 CASE: a refresher dequeue with no trigger GVR at all
			// (sliceability re-verify / proactive pass / no-hook fallback), and
			// the shape every cohort-seed and engine-boot resolve has.
			name: "background_no_trigger",
			wrap: cache.WithBackgroundResolve,
		},
		{
			// The sibling-stage read #376 already named: background, carries a
			// trigger, but not for THIS cell's GVR. Served-as-input, internal.
			name: "background_nonmatching_trigger",
			wrap: func(ctx context.Context) context.Context {
				return cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(ctx), otherGVR)
			},
		},
	}
	for _, r := range internalReads {
		t.Run(r.name, func(t *testing.T) {
			freshCell(t)
			h0, m0 := store.Stats().HitTotal, store.Stats().MissTotal
			s0 := store.Stats().StaleServedTotal
			v, served, ok := apistageContentServe(r.wrap(r1Ctx()), store, r1GetCall(), false)
			// ARM-RAN check: a read that never happened proves nothing.
			if !ok || !served {
				t.Fatalf("ARM DID NOT RUN: the content HIT was not served (ok=%v served=%v)", ok, served)
			}
			if got := managedCountOf(t, v); got != r1StaleManaged {
				t.Fatalf("ARM DID NOT RUN as intended: want the stale %d-managed cell (no force-miss on this ctx), got %d",
					r1StaleManaged, got)
			}
			if s := store.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
				t.Errorf("#552: a %s content read moved the ratio counters (hit %d->%d, miss %d->%d) — "+
					"snowplow read its own cache and it was accounted as a customer serve",
					r.name, h0, s.HitTotal, m0, s.MissTotal)
			}
			if s := store.Stats(); s.StaleServedTotal != s0 {
				t.Errorf("#552: a %s content read moved stale_served_total (%d->%d) — "+
					"the #354 window must be measured on customer serves only", r.name, s0, s.StaleServedTotal)
			}
			if got := lastRead(t); got != -1 {
				t.Errorf("#552: a %s content read stamped lastRead (LastReadSeconds -1 -> %d) — "+
					"it faked #315/#316 customer warmth on a cell no customer touched", r.name, got)
			}
		})
	}

	// CONTROL (non-vacuity): the same cell, same call, no marker → a real /call.
	// It MUST do every one of the things the arms above assert did not happen,
	// or those arms were reading a dead instrument.
	t.Run("customer_call_stamps", func(t *testing.T) {
		freshCell(t)
		h0 := store.Stats().HitTotal
		v, served, ok := apistageContentServe(r1Ctx(), store, r1GetCall(), false)
		if !ok || !served {
			t.Fatalf("customer content HIT must serve (ok=%v served=%v)", ok, served)
		}
		if got := managedCountOf(t, v); got != r1StaleManaged {
			t.Fatalf("control: a customer read must serve the stale cell (stale-while-revalidate), got %d managed", got)
		}
		if s := store.Stats(); s.HitTotal != h0+1 {
			t.Errorf("control: a customer content serve must bump hit_total (%d->%d) — "+
				"if it does not, the no-movement arms above prove nothing", h0, s.HitTotal)
		}
		if got := lastRead(t); got < 0 {
			t.Errorf("control: a customer content serve must stamp lastRead (still %d)", got)
		}
	})

	// #544 REGRESSION GUARD. forceContentMiss is TRIGGER EQUALITY and must stay
	// that way: a refresher re-resolve whose own GVR is in the trigger set must
	// still discard the cell and re-dispatch FRESH. Keying the force-miss on
	// background-ness instead (the "simplify it to one flag" mistake) silently
	// serves the stale snapshot here.
	t.Run("matching_trigger_still_force_misses", func(t *testing.T) {
		freshCell(t) // back to the stale snapshot the force-miss must discard
		ctx := cache.WithRefreshTriggerGVR(cache.WithBackgroundResolve(r1Ctx()), r1FsaGVR)
		v, served, ok := apistageContentServe(ctx, store, r1GetCall(), false)
		if !ok || !served {
			t.Fatalf("the force-missed re-dispatch must serve (ok=%v served=%v)", ok, served)
		}
		if got := managedCountOf(t, v); got != r1FreshManaged {
			t.Fatalf("#544: a refresher re-resolve triggered by this cell's OWN GVR must FORCE-MISS and "+
				"re-dispatch FRESH (%d managed from the live informer); got %d — the content-shield "+
				"bypass stopped firing, which is the defect #544 fixed", r1FreshManaged, got)
		}
	})
}
