package apiref

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// RAFullListKey is the exported entry to seedFullListRAKey, the single key
// builder for the raFullList class. The producer (raFullListServe stores the
// cell under it; the refresher re-emits ComputeKey of the stored inputs) and the
// /refreshes subscription (dispatchers.DeriveSubscriptionKey, class raFullList)
// both mint through it, so the armed key equals the emitted key by construction
// (#426). Before #426 the subscription minted through dispatchCacheLookupKey,
// which folds the request's normalized pagination (and, at the time, an
// RBACSubGen this key lacked). This cell's key is page-independent, so the armed
// key never matched and no raFullList refresh was ever delivered. Since #435 the
// key folds RBACSubGen too — through THIS builder, so producer, refresher and
// subscription rotate together on a Role edit or grant/revoke.
//
// ok=false when ctx has no identity or EvaluateRBAC fails closed to "". The
// shared empty-identity cell is never populated, so there is nothing to arm.
func RAFullListKey(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, extras map[string]any) (cache.ResolvedKeyInputs, string, bool) {
	return seedFullListRAKey(ctx, gvr, namespace, name, extras)
}
