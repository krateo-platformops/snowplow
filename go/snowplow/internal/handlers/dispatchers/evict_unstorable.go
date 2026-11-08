// evict_unstorable.go — #1255: every resolve-output writer that declines a
// resolve because its key can no longer be stored evicts the cell it could not
// replace (the rule, the epoch race guard, the log line and the subscriber push
// are in internal/cache/evict_unstorable.go).
//
// THE WRITERS (every Put-gate decline site of a resolve-output cell; a site that
// keeps the old body on such a decline serves it until the TTL):
//
//   - the refresher re-Put (resolve_populate.go): the permanent verdict, the
//     identity-free UAF decline and the sensitive-read decline;
//   - the keep-warm / gvr-discovered / reseed seeds (phase1_pip_seed.go,
//     seed_terminal_put_guard.go): the permanent verdict and the sensitive read;
//   - the customer /call cold paths, which a #547 demand revalidation re-enters
//     over a resident known-stale cell (restactions.go, widgets.go): the
//     permanent verdict, the sensitive read and the class-driven UAF decline;
//   - the walker's identity-free widget-content populate (widget_content.go):
//     the permanent verdict, the UAF decline and the sensitive read;
//   - the raFullList cell (resolvers/widgets/apiref/ra_full_list.go, through the
//     store directly): the permanent verdict and the sensitive read.
//
// A transient decline (a stage error alone, an empty full list, a failed or
// drifted resolve, an "unobserved" verdict) keeps the cell at every one of them,
// as before.
package dispatchers

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// unstorableEvicter is the store's #1255 eviction surface (cacheHandle carries
// it; the refresher and the walker hold the concrete store).
type unstorableEvicter interface {
	EvictOnPermanentDecline(ctx context.Context, v cache.PutVerdict, key string) bool
	EvictUnstorable(ctx context.Context, key, reason string) bool
}

// inClassEvicter is h, or nil when the identity on ctx is no longer in the RBAC
// class inputs' key was minted for (#424). A resolve whose class moved mid-resolve
// read another class's view: its decline proves nothing about THIS key, so it
// must not evict the key's cell (#1255 F3, review of #1263). The same predicate
// every identity-bound Put re-checks (identityClassDriftCtx); always in class for
// an identity-free cell.
func inClassEvicter(ctx context.Context, h unstorableEvicter, inputs *cache.ResolvedKeyInputs) unstorableEvicter {
	if h == nil || identityClassDriftCtx(ctx, inputs) != "" {
		return nil
	}
	return h
}

// dispatchEvicter is a customer /call's eviction surface, or nil when the
// request may not write its cell at all — no store, no key, or the #95 ""
// BindingUID collapse (serveFromCacheEligible): a request that may not populate
// the shared cell may not empty it either — or when its identity class moved
// during the resolve (inClassEvicter).
func dispatchEvicter(ctx context.Context, h cacheHandle, key string, inputs *cache.ResolvedKeyInputs) unstorableEvicter {
	if h == nil || key == "" || !serveFromCacheEligible(inputs) {
		return nil
	}
	return inClassEvicter(ctx, h, inputs)
}

// evictOnPermanentDecline evicts key's cell when v permanently declines the
// resolve ctx carries (cache.EvictOnPermanentDecline). Nil-safe on h. Returns
// true iff a cell was evicted.
func evictOnPermanentDecline(ctx context.Context, h unstorableEvicter, v cache.PutVerdict, key string) bool {
	if h == nil || key == "" || !v.Permanent() {
		return false
	}
	return h.EvictOnPermanentDecline(ctx, v, key)
}

// evictUnstorable is evictOnPermanentDecline for the declines outside the
// verdict (cache.UnstorableSensitive, cache.UnstorableUAF).
func evictUnstorable(ctx context.Context, h unstorableEvicter, key, reason string) bool {
	if h == nil || key == "" {
		return false
	}
	return h.EvictUnstorable(ctx, key, reason)
}
