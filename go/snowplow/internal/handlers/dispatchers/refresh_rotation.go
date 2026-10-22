// refresh_rotation.go — #819: an armed key whose identity class moved is
// announced to its tab.
//
// THE GAP. An identity-bound key folds the requester's class (binding-set
// digest + RBAC sub-generation, the #682 epoch included). When the class moves,
// the cell is refreshed and published under the NEW key while the tab is armed
// on the old one, which nothing publishes on again. The tab learns the new key
// only from the X-Snowplow-Refresh-Key header of its next /call, and a tab that
// is not refetching never makes one: live refresh silently stops (bench run 10,
// the passive tab got 0 pushes after the epoch flipped).
//
// THE FIX. The /refreshes connection keeps, per identity-bound armed key, the
// class the key was derived under. On cache.IdentityClassesMayHaveMoved it
// recomputes its identity's CURRENT class (rbac.IdentityClassOf — a memoised
// digest and a few atomics: no apiserver read, no key derivation) and returns
// every key minted under a different class, ONCE. The handler sends
// one existing `refresh` frame on each; the SPA refetches (refreshSse.ts
// dispatchRefresh), records the new key from the response header
// (useWidgetQuery.ts recordRefreshHeaders) and re-arms under it
// (useWidgetLiveRefresh.ts → a new stream). No new endpoint, no new frame.
//
// ONCE PER ARMED KEY. One RBAC edit can move a class twice in a few
// milliseconds (the binding-set digest at one publish, the sub-gen bump it
// owes at the next, #703), and the tab needs one refetch, not two: the refetch
// returns the key current at THAT moment, and the stream the SPA then opens
// tracks that key's class from scratch. So a key, once announced, is done on
// this connection.
//
// GENERAL. The test is "the class this key folds is not the requester's class
// now" — no resource, class or cause is named. Identity-free classes
// (cache.IsIdentityFreeClass) fold no class and never rotate, so they are not
// tracked. A class that is back at the minted one (a binding added then
// removed between two checks) makes the key live again: no frame.
//
// ISOLATION. Only the connection's own keys, derived under its own identity,
// are ever announced, and only on that connection (RefreshStream.Rotated).

package dispatchers

import (
	"context"
	"sort"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// SubscriptionRotationTracker records the class each identity-bound armed key
// of one /refreshes connection was derived under. Not safe for concurrent use:
// the connection goroutine owns it.
type SubscriptionRotationTracker struct {
	ui   jwtutil.UserInfo
	keys map[string]*trackedArm
}

type trackedArm struct {
	minted    cache.IdentityClass
	announced bool
}

// NewSubscriptionRotationTracker builds the tracker for ctx's identity (the
// connection's authenticated UserInfo, the identity every key is derived
// under).
func NewSubscriptionRotationTracker(ctx context.Context) *SubscriptionRotationTracker {
	ui, _ := xcontext.UserInfo(ctx)
	return &SubscriptionRotationTracker{ui: ui, keys: map[string]*trackedArm{}}
}

// Derive is DeriveSubscriptionKeyWithReason that also tracks an armed
// identity-bound key's class.
func (t *SubscriptionRotationTracker) Derive(ctx context.Context, coords SubscriptionCoordinates) (string, bool, SubscriptionSkipReason) {
	key, inputs, ok, reason := deriveSubscriptionWithReason(ctx, coords)
	if ok && inputs != nil && !cache.IsIdentityFreeClass(coords.Class) {
		t.keys[key] = &trackedArm{minted: inputs.Class()}
	}
	return key, ok, reason
}

// Tracking reports whether any armed key can rotate.
func (t *SubscriptionRotationTracker) Tracking() bool { return len(t.keys) > 0 }

// Rotated returns, sorted, the not-yet-announced armed keys whose class is
// not the identity's current class, and marks them announced.
func (t *SubscriptionRotationTracker) Rotated() []string {
	if len(t.keys) == 0 {
		return nil
	}
	cur := rbac.IdentityClassOf(t.ui)
	var out []string
	for k, a := range t.keys {
		if a.announced || a.minted == cur {
			continue
		}
		a.announced = true
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
