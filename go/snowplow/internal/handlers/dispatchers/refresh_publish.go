// refresh_publish.go — #62: publish a live-refresh signal on a genuine
// COLD-dispatch Put when (and only when) a /refreshes connection is armed for
// that key.
//
// Why (the #61 TTL-evict residual, RCA §C-2 / backlog #62): #61 made a
// dep-change dirty-mark the armed top-level key so the REFRESHER re-resolves
// it and PublishRefreshes. But if the armed key was TTL-EVICTED before the
// resource churned, its dep edges were removed (RemoveL1Key) → the dep-change
// matches nothing → the refresher never re-resolves it → no PublishRefresh →
// that cycle's delivery is missed. The frontend re-arms on the next /call,
// which COLD-fills the key (a genuine Put here, dep edges re-recorded) — but
// that cold Put historically did NOT publish (PublishRefresh fires only on the
// refresher path, resolve_populate.go). So a viewer whose key cold-filled
// after an eviction sees one stale frame until the NEXT churn. This closes
// that gap: announce the cold-fill to any already-armed subscriber.
//
// SCOPE IS THE SAFETY BOUNDARY (C62-4 placement): call this ONLY with the
// outcome of a write that happened — never on the stage-error / external-skip
// / declined branches (no Put there → publishing would signal a non-event; a
// refused write's outcome is PutRefused, which publishes nothing either).
//
// #1321 B1 — and it is called after EVERY write of a cell a /refreshes stream
// can be armed on, not only the cold dispatch: the seed terminal Put
// (seedTerminalPut), the walker's widgetContent shell Put
// (populateWidgetContentL1) and the external-TTL widget Put too
// (cache/put_outcome.go, WRITE SITES). A write that changes an armed cell's
// bytes and does not announce it breaks the rule that lets an identical Put
// withhold its frame. The widgetContent shell key folds no BindingUID; a
// frame on it names the key and nothing else, it reaches only streams armed
// on that key, and each tab's refetch is gated for its own user
// (gateWidgetEnvelope) — the refresher has always published that key the
// same way. apistage content cells are never armed (no response stamps their
// key), and are written through PutContentIfGen, which reports no outcome.
//
// Cost-proportional + leak-safe by construction: HasRefreshSubscriber is an
// O(1) RLock map read; when no connection is armed for the key it returns
// false and we never publish (counters stay flat). When armed, PublishRefresh
// fans out ONLY to subscribers whose armed set contains this exact key — and
// the key is the per-identity DeriveSubscriptionKey (BindingUID folded in for
// identity-bearing classes), so the cold-filler's key matches only a
// subscriber armed under the SAME identity. Both HasRefreshSubscriber and
// PublishRefresh take the hub RLock, so this is safe under concurrent
// Subscribe/unsubscribe (no deliver-after-unsub: PublishRefresh re-checks the
// armed set under the lock).
package dispatchers

import "github.com/krateo-platformops/snowplow/internal/cache"

// publishIfSubscribed announces a genuine cold-dispatch Put of l1Key to any
// already-armed /refreshes subscriber, and is a no-op when none is armed
// (cost-proportional) or when the SSE layer / cache is off (both nil-safe via
// the broadcaster). l1Key MUST be the just-Put genuine cache key (the
// per-identity DeriveSubscriptionKey-equivalent the serve path stamps).
//
// #1321 — put is that Put's outcome, and only a PutChanged is announced
// (cache.PublishPut). The designed case (an evicted key re-filled) has no
// resident prior, so it is always PutChanged; but this PutIfGen also
// re-enters over a resident cell on a #547 demand revalidation, and a
// revalidation that resolved the same bytes announces nothing the
// subscriber does not already hold.
func publishIfSubscribed(l1Key string, put cache.PutOutcome) {
	cache.PublishPutIfSubscribed(l1Key, put)
}
