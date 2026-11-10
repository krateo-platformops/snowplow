// refresh_publish_falsifier_test.go — #62: publishIfSubscribed (cold-dispatch
// Put announces to an already-armed /refreshes subscriber) falsifiers.
//
// publishIfSubscribed is the ~6-LOC helper called from the genuine-Put else-if
// of restactions.go + widgets.go AFTER Put+dep-Record. These arms exercise it
// against REAL per-identity keys (DeriveSubscriptionKey, the same seam the
// serve path stamps), reusing the two-distinct-BindingUID RBAC harness from
// refresh_isolation_falsifier_test.go (buildTwoUserRBACWatcher / ctxAs /
// compositionsCoords).
//
//   C62-1 (cross-user leak, HARD): userA cold-fills its key → userB (armed for
//          B's DISTINCT-BindingUID key) receives NOTHING + B's delivered count
//          is unchanged. The RBAC isolation proof made real for the cold-Put
//          path: publishIfSubscribed(keyA) can only fan to a subscriber armed
//          for keyA.
//   C62-2 (RED→GREEN, HARD): an evicted key, a subscriber armed for it, then a
//          cold-fill Put → delivered. RED if the cold Put does NOT publish
//          (the pre-#62 behaviour: PublishRefresh fired only on the refresher
//          path); GREEN after publishIfSubscribed. Asserts refreshDeliveredTotal
//          +1 AND the subscriber channel receives the key.
//   C62-3 (cost-proportional): NO armed subscriber → publishIfSubscribed is a
//          no-op, all broadcaster counters stay FLAT (HasRefreshSubscriber
//          false → no PublishRefresh).
//   C62-5 (-race): concurrent cold-fill publishIfSubscribed + Subscribe/unsub
//          on the same key — no data race, no deliver-after-unsub (both
//          HasRefreshSubscriber and PublishRefresh take the hub RLock).
package dispatchers

import (
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// C62-2 — the RED→GREEN sufficiency arm. RED pre-fix (cold Put doesn't
// publish), GREEN post-fix.
func TestFalsifier62_ColdFillPublishesToArmedSubscriber(t *testing.T) {
	buildTwoUserRBACWatcher(t)
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	coords := compositionsCoords()
	keyA, ok := DeriveSubscriptionKey(ctxAs("userA"), coords)
	if !ok || keyA == "" {
		t.Fatalf("derive keyA failed (ok=%v key=%q)", ok, keyA)
	}

	// The viewer re-armed after its key was TTL-evicted (the #61 residual
	// scenario): an armed subscriber whose L1 entry is currently cold.
	ch, unsub := cache.SubscribeRefresh(map[string]struct{}{keyA: {}})
	defer unsub()

	_, deliveredBefore, _, _ := cache.RefreshBroadcasterCounters()

	// The cold-dispatch Put fires this (restactions.go / widgets.go genuine-Put
	// else-if). Pre-#62 the cold Put did NOT publish → this would be a no-op →
	// the subscriber starves until the next churn.
	publishIfSubscribed(keyA, cache.PutChanged)

	select {
	case got := <-ch:
		if got != keyA {
			t.Fatalf("C62-2: subscriber received %q, want the armed key %q", got, keyA)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("C62-2 RED: armed subscriber received NOTHING within 2s after the cold-fill Put — " +
			"publishIfSubscribed did not announce the cold-fill (pre-#62 behaviour: cold Put never published).")
	}

	_, deliveredAfter, _, _ := cache.RefreshBroadcasterCounters()
	if deliveredAfter != deliveredBefore+1 {
		t.Fatalf("C62-2: refreshDeliveredTotal = %d, want %d", deliveredAfter, deliveredBefore+1)
	}
}

// C62-1 — the cross-user leak arm. userA's cold-fill must NOT deliver to userB
// (distinct BindingUID → distinct key). The RBAC isolation proof for the
// cold-Put path.
func TestFalsifier62_ColdFillNoCrossUserLeak(t *testing.T) {
	buildTwoUserRBACWatcher(t)
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	coords := compositionsCoords()
	keyA, okA := DeriveSubscriptionKey(ctxAs("userA"), coords)
	keyB, okB := DeriveSubscriptionKey(ctxAs("userB"), coords)
	if !okA || !okB || keyA == keyB {
		t.Fatalf("setup: keys not distinct (keyA=%q keyB=%q okA=%v okB=%v)", keyA, keyB, okA, okB)
	}

	// userB arms ONLY its own (distinct-BindingUID) key.
	chB, unsubB := cache.SubscribeRefresh(map[string]struct{}{keyB: {}})
	defer unsubB()

	_, deliveredBefore, _, _ := cache.RefreshBroadcasterCounters()

	// userA's request cold-fills userA's key (the cold-Put path stamps keyA).
	publishIfSubscribed(keyA, cache.PutChanged)

	// userB must receive NOTHING — keyA != keyB.
	select {
	case leaked := <-chB:
		t.Fatalf("C62-1 LEAK: userB received %q from userA's cold-fill — cross-user signal leak on the cold-Put path", leaked)
	case <-time.After(300 * time.Millisecond):
		// correct — no leak
	}

	// And nothing was delivered at all (keyA had no armed subscriber here).
	_, deliveredAfter, _, _ := cache.RefreshBroadcasterCounters()
	if deliveredAfter != deliveredBefore {
		t.Fatalf("C62-1: delivered moved (%d→%d) — userA's cold-fill delivered to a sub it must not have",
			deliveredBefore, deliveredAfter)
	}
}

// C62-3 — cost-proportional: no armed subscriber → publishIfSubscribed is a
// pure no-op (HasRefreshSubscriber false → no PublishRefresh → counters flat).
func TestFalsifier62_NoSubscriberNoPublish(t *testing.T) {
	buildTwoUserRBACWatcher(t)
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	coords := compositionsCoords()
	keyA, ok := DeriveSubscriptionKey(ctxAs("userA"), coords)
	if !ok || keyA == "" {
		t.Fatalf("derive keyA failed")
	}

	pubBefore, delBefore, dropBefore, coalBefore := cache.RefreshBroadcasterCounters()

	// No SubscribeRefresh — nobody armed for keyA. The vast majority of cold
	// fills (no live viewer on that exact widget) hit this path.
	publishIfSubscribed(keyA, cache.PutChanged)

	pubAfter, delAfter, dropAfter, coalAfter := cache.RefreshBroadcasterCounters()
	if pubAfter != pubBefore || delAfter != delBefore || dropAfter != dropBefore || coalAfter != coalBefore {
		t.Fatalf("C62-3: counters moved with NO armed subscriber "+
			"(pub %d→%d, del %d→%d, drop %d→%d, coal %d→%d) — publishIfSubscribed published when it should have no-op'd",
			pubBefore, pubAfter, delBefore, delAfter, dropBefore, dropAfter, coalBefore, coalAfter)
	}
}

// C62-5 — concurrent cold-fill + subscribe/unsub on the same key. -race must
// be clean; no deliver-after-unsub panic. (Both HasRefreshSubscriber and
// PublishRefresh take the hub RLock; unsub does not close the channel.)
func TestFalsifier62_ConcurrentColdFillAndSubscribe(t *testing.T) {
	buildTwoUserRBACWatcher(t)
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	t.Setenv("REFRESH_COALESCE_WINDOW_MS", "0")
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)

	coords := compositionsCoords()
	keyA, ok := DeriveSubscriptionKey(ctxAs("userA"), coords)
	if !ok || keyA == "" {
		t.Fatalf("derive keyA failed")
	}

	var wg sync.WaitGroup
	// Churn of cold-fills.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publishIfSubscribed(keyA, cache.PutChanged)
		}()
	}
	// Concurrent arm/disarm churn on the same key.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, unsub := cache.SubscribeRefresh(map[string]struct{}{keyA: {}})
			// Drain opportunistically so a delivery never blocks the producer.
			select {
			case <-ch:
			default:
			}
			unsub()
		}()
	}
	wg.Wait()
	// Reaching here under -race with no panic IS the assertion (deliver-after-
	// unsub / concurrent-map-access would fail the race detector or panic).
}
