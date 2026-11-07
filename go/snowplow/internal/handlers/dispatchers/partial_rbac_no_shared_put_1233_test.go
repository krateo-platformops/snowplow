package dispatchers

// #1233 — while the RBAC snapshot is incomplete, no writer stores a shared cache
// entry. Each writer runs its own production code path twice, on a partial
// snapshot and on the same snapshot complete: the partial run stores nothing and
// is counted under its own name, the complete run stores as before.
//
// The partial snapshot here is the complete one marked not Synced: the bindings
// are all present, so every key is the one the complete snapshot derives and no
// identity-class guard has a reason to decline. What refuses the write is the
// store's admission, and nothing else.

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func declined1233(label string) uint64 {
	return cache.PutDeclinedRBACIncompleteSnapshot()[label]
}

// A user's /call (restActionHandler.ServeHTTP) on a partial snapshot serves its
// body and stores nothing.
func TestIssue1233_UserCallStoresNothingOnAPartialSnapshot(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			a := psArm{name: "configmaps", target: psConfigmapsGVR, watchShape: true}
			psBuildWatcher(t, a)
			perUser := map[string]*atomic.Int64{psAlice: {}, psBob: {}}
			srv := psFakeAPIServer(t, a, perUser)
			psSeedClientconfigs(t, srv.URL)
			ctx := psUserCtx(a, psAlice)
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			key, handle, _ := dispatchCacheLookupKey(ctx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			if handle == nil || key == "" {
				t.Fatal("PRE: need a live cache handle and key")
			}
			label := cache.RefDenialOriginServe + "/restactions"
			before := declined1233(label)

			rec := psServe(t, a, srv.URL, ctx)

			if !psHasSentinel(rec.Body.Bytes()) {
				t.Fatalf("PRE: alice's /call must be served her content either way; code=%d body=%s", rec.Code, rec.Body.String())
			}
			_, stored := handle.Get(key)
			got := declined1233(label) - before
			if !partial {
				if !stored || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the /call must store its cell (stored=%v, declines=%d)", stored, got)
				}
				return
			}
			if stored {
				t.Fatal("#1233 RED: a user's /call stored a shared cell on a partial RBAC snapshot")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}

// The refresher's impersonated read (#574, #615), the route the #1230 review
// found: no re-gate covers it, so on a partial snapshot it returned the
// representative's apiserver view unmarked, and the refresh stored it in the
// shared cell. RED on main.
func TestIssue1233_ImpersonatedRefreshStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			const user = "alice-1233"
			k := f608Kinds[0]
			e := f608Setup(t, nil, nil)
			e.srv.addObject(f608Widget(k, "b1233"), k, "b1233")
			key := e.seedCell(t, f608CohortInputs(k, "b1233", user, nil))
			if !e.store.Has(key) {
				t.Fatal("PRE: the shared cell must be resident before the refresh")
			}
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			label := cache.BackgroundOriginRefresher + "/widgets"
			before := declined1233(label)

			done := make(chan error, 4)
			cache.RegisterRefreshFunc("widgets", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
				err := resolveAndPopulateL1(ctx, in, e.saEP, e.saRC)
				done <- err
				return err
			})
			ctx, cancel := context.WithCancel(context.Background())
			cache.StartRefresher(ctx)
			t.Cleanup(func() { cancel(); cache.ResetRefresherForTest() })
			cache.EnqueueRefresh(key)
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("the refresh did not run within 20s")
			}

			objPath := fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/b1233", f608Group, f608Version, f608NS, k.resource)
			asCohort := 0
			for _, r := range e.srv.requests() {
				if r.path == objPath && r.user == user {
					asCohort++
				}
			}
			if asCohort == 0 {
				t.Fatal("PRE: the refresh's data read must go out impersonating the representative (the #1233 route)")
			}
			got := declined1233(label) - before
			if !partial {
				if !e.accepted(key) || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the impersonated refresh must store (accepted=%v, declines=%d)", e.accepted(key), got)
				}
				return
			}
			if e.accepted(key) {
				t.Fatal("#1233 RED: on a partial RBAC snapshot the impersonated refresh stored the representative's view in the shared cell")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}

// The seed's terminal write (seedTerminalPut, the one write of the walk's and the
// seeds' primitives): the boot scope's plain write and every other mode's
// guarded one.
func TestIssue1233_SeedStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, mode := range []seedScopeMode{seedModeBoot, seedModeKeepwarm} {
			t.Run(fmt.Sprintf("partial=%v/mode=%v", partial, mode), func(t *testing.T) {
				t.Setenv("CACHE_ENABLED", "true")
				t.Setenv("RESOLVED_CACHE_ENABLED", "true")
				cache.ResetResolvedCacheForTest()
				t.Cleanup(cache.ResetResolvedCacheForTest)
				cache.ResetPhase1DoneForTest()
				t.Cleanup(cache.ResetPhase1DoneForTest)
				cache.CompleteRBACSnapshotForTest(t)

				ctx := seedCohortCtx("cohort-1233")
				key, handle, inputs := dispatchCacheLookupKey(ctx, "restactions",
					h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, "seeded-1233", -1, -1, nil)
				if handle == nil || key == "" {
					t.Fatal("PRE: need a live cache handle and key")
				}
				if partial {
					cache.PartialRBACSnapshotForTest(t)
				}
				label := cache.BackgroundOriginCohortSeed + "/restactions"
				before := declined1233(label)
				g := seedTerminalGuardFor(mode, handle, key)
				wrote := seedTerminalPut(cache.WithL1KeyContext(ctx, key), handle, fixtureStoreVerdict, key,
					&cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs}, g)
				_, stored := handle.Get(key)
				got := declined1233(label) - before
				if !partial {
					if !wrote || !stored || got != 0 {
						t.Fatalf("CONTROL: on a complete snapshot the seed must store (wrote=%v stored=%v declines=%d)", wrote, stored, got)
					}
					return
				}
				if wrote || stored {
					t.Fatalf("#1233 RED: the seed stored a shared cell on a partial RBAC snapshot (wrote=%v stored=%v)", wrote, stored)
				}
				if got != 1 {
					t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
				}
			})
		}
	}
}

// The content prewarm's write (populateWidgetContentL1, the identity-free
// widgetContent cell the SA walk fills).
func TestIssue1233_ContentPrewarmStoresNothingOnAPartialSnapshot(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			t.Setenv("CACHE_ENABLED", "true")
			t.Setenv("RESOLVED_CACHE_ENABLED", "true")
			t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
			cache.ResetResolvedCacheForTest()
			t.Cleanup(cache.ResetResolvedCacheForTest)
			cache.ResetDepsForTest()
			t.Cleanup(cache.ResetDepsForTest)
			cache.CompleteRBACSnapshotForTest(t)
			if partial {
				cache.PartialRBACSnapshotForTest(t)
			}
			in := h1WidgetUnstructured(map[string]any{})
			res := h1WidgetUnstructured(map[string]any{})
			if err := unstructured.SetNestedField(res.Object, "content", "status", "widgetData", "label"); err != nil {
				t.Fatalf("setup: %v", err)
			}
			key, _ := widgetContentL1Key(h1WidgetGVR, h1NS, h1WName, -1, -1)
			if key == "" {
				t.Fatal("PRE: the widgetContent layer must be live")
			}
			label := cache.RefDenialOriginPrewarmPath + "/" + cache.CacheEntryClassWidgetContent
			before := declined1233(label)
			ctx := cache.WithPrewarmPath(withWidgetContentPutSinks(context.Background()))
			populateWidgetContentL1(ctx, h1WidgetGVR, in, -1, -1, res, 0)
			_, stored := cache.ResolvedCache().Get(key)
			got := declined1233(label) - before
			if !partial {
				if !stored || got != 0 {
					t.Fatalf("CONTROL: on a complete snapshot the content prewarm must store (stored=%v declines=%d)", stored, got)
				}
				return
			}
			if stored {
				t.Fatal("#1233 RED: the content prewarm stored a shared widgetContent cell on a partial RBAC snapshot")
			}
			if got != 1 {
				t.Fatalf("the refusal must be counted once under %q, counted %d", label, got)
			}
		})
	}
}
