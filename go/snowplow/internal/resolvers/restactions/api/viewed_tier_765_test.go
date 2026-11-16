// viewed_tier_765_test.go — #765 viewed tier V4b: the re-resolve the refresher
// runs for a dirty content cell OPEN ON A PAGE is admitted at the debt class (the
// content-fill cap never queues it), and an unviewed one at background. Real
// informer UPDATE → real dirty-mark → real refresher dequeue → the fill class
// apistageContentServe would use for that ctx.
package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestViewedTier765_V4b_ViewedRefreshIsAdmittedAtDebtClass(t *testing.T) {
	for _, tc := range []struct {
		name   string
		viewed bool
		want   string
	}{
		{"viewed", true, "debt"},
		{"unviewed", false, "background"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
			t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
			t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
			t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
			dyn := newFill710Watcher(t, 3)
			store := cache.ResolvedCache()
			fill710Reset(t)
			resetContentFlightsForTest()
			cache.ResetRefresherForTest()
			stamp765Prime(t, store)

			var mu sync.Mutex
			var classes []string
			cache.RegisterRefreshFunc(cache.CacheEntryClassApistage, func(ctx context.Context, _ string, _ cache.ResolvedKeyInputs) error {
				if cache.RefreshTriggerHas(ctx, r1FsaGVR) {
					mu.Lock()
					classes = append(classes, ContentFillClassForTest(ctx))
					mu.Unlock()
				}
				return nil
			})
			rctx, rcancel := context.WithCancel(context.Background())
			cache.StartRefresher(rctx)
			// Join the dep-event workers before the watcher cleanup resets the cache under them.
			t.Cleanup(func() { rcancel(); cache.ResetRefresherForTest(); cache.ResetDepsForTest() })
			if tc.viewed {
				st := cache.SubscribeRefreshStream(map[string]struct{}{fill710ListKey(): {}})
				t.Cleanup(st.Unsub)
				if !cache.HasRefreshSubscriber(fill710ListKey()) {
					t.Fatalf("PRECONDITION: the /refreshes arm did not register the content cell")
				}
			}

			stamp765Change(t, dyn, r1FreshManaged)
			if !fill710WaitFor(20*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(classes) > 0 }) {
				t.Fatalf("PRECONDITION: the refresher never re-resolved the dirty-marked content cell")
			}
			mu.Lock()
			defer mu.Unlock()
			if classes[0] != tc.want {
				t.Fatalf("V4b RED (#765): the refresher's re-resolve of a %s dirty cell is admitted at %q, want %q", tc.name, classes[0], tc.want)
			}
		})
	}
}
