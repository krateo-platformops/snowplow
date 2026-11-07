// issue1232_stale_settle_test.go — #1232: a background revalidation's slot
// belongs to the dequeue that runs it, not to one that was already running.
//
// THE DEFECT. Every dequeue of a key settles that key's background revalidation
// slot when it ends (refresher.go, settleBackgroundRevalidation). A
// revalidation claimed while an earlier dequeue of the same key is in flight
// enqueues its key, and the workqueue runs it after that dequeue. But the
// earlier dequeue's settle found the new slot and released it, before the
// revalidation had run. Its own dequeue then ran with no slot held, so a read
// that came in meanwhile was not absorbed into it: it claimed a second slot and
// queued a second revalidation, which re-resolved a cell the first one had just
// healed (K6's 2 background attempts, #1232).
//
// The arm forces that order instead of waiting for load to produce it: it parks
// a dirty-mark dequeue in its resolve, claims a revalidation behind it, then
// lets the earlier dequeue end. When the revalidation's own dequeue starts, the
// workqueue has finished the earlier one, and the slot must still be held.
package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue1232_ADequeueThatStartedBeforeTheClaimLeavesTheSlot(t *testing.T) {
	h := newK547Harness(t, k547DenyBackground)
	contentKey, _ := k740Shell(t, h)
	park := k740ParkDoomed(h, true, 2) // the dirty-mark dequeue, then the revalidation's own
	bg0 := h.bgCalls.Load()

	h.update(k547Changed, "v2") // a dirty mark: its dequeue parks in its resolve
	park.await(t, 0)
	cache.RevalidateKnownStaleInBackground(contentKey) // claimed while that dequeue is in flight
	if n := cache.BackgroundRevalidationsInFlight(); n != 1 {
		t.Fatalf("PRECONDITION: the revalidation claimed no slot (%d held)", n)
	}
	close(park.release[0]) // the earlier dequeue ends

	park.await(t, 1) // the revalidation's own dequeue is running
	if n := cache.BackgroundRevalidationsInFlight(); n != 1 {
		t.Fatalf("#1232 RED: the revalidation's slot was released before its dequeue ran (%d held): "+
			"the dequeue that was already in flight when it was claimed settled it", n)
	}
	if got := h.bgCalls.Load() - bg0; got != 2 {
		t.Fatalf("#1232: %d background resolves, want 2: the dirty mark's and the revalidation's", got)
	}
	close(park.release[1])
	k740WaitSlotReleased(t, "#1232")
}
