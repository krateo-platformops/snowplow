// evict_unstorable.go — #1255: a resolve that proves its key can no longer be
// stored evicts the cell it could not replace.
//
// THE DEFECT. A writer re-resolves a resident cell (the refresher on a dirty
// mark, the keep-warm seed, a demand revalidation) and its Put-gate declines the
// fresh bytes. Before #1255 every decline kept the prior entry, "TTL is the outer
// net". For a transient decline that is right: the old body is still the best
// answer and a retry may store. For a PERMANENT one it is not: the decline proves
// that the key's current inputs can never be stored (an edit made the RESTAction
// touch a named endpoint, declare a write, read a Secret...), so no writer can
// ever replace the old body, and it is served until its TTL (3600 s). On 057 a
// PageHeader kept the pre-edit output after portal#384 edited its RESTAction.
//
// THE RULE. A resolve whose decline proves the key unstorable evicts the cell, so
// the next request resolves live (and, being declined, is served uncached, as a
// first visit of that spec always was). Which declines prove it is decided by
// the decline's reason, never by a widget, page or resource kind:
//
//   - a permanent Put-gate verdict (PutVerdict.Permanent: a declared write, a
//     genuine external touch, a stage error beside one) — EvictOnPermanentDecline;
//   - the declines outside the verdict that every writer applies alike: a
//     sensitive read (#398) and a userAccessFilter-narrowed identity-free body
//     (1.12.3 A-1) — EvictUnstorable.
//
// A transient verdict (a stage error alone), an "unobserved" verdict (a wiring
// fault, which proves nothing about the key), a failed resolve and a refresher
// that cannot run this kind at all prove nothing about the key either, and keep
// the cell exactly as before.
//
// THE CELL'S OWN CONTRACT. An external-TTL cell (the widget opt-in,
// PutVerdict.WithExternalTTL) is stored precisely BECAUSE its resolve touches an
// external endpoint, bounded by its own short TTL. An external decline of it
// does not prove it unstorable — its writer stores exactly that — and neither
// does a transient failure of that endpoint (a stage error beside the same
// third-party touch: the opt-in stores the clean retry), so it is kept, as
// before (PutVerdict.StorableUnderExternalTTL).
//
// THE RACE (an eviction must never drop a newer valid body). The guard is the
// #375 dep-event epoch every resolve-output Put already carries: a body's
// resolveStart is the sequence its resolve began at, and the declining resolve's
// own start is on its ctx (DepGenStartSeqFromContext). Only a cell whose body was
// resolved STRICTLY BEFORE the declining resolve began is evicted — it read an
// older state than the one that proved the key unstorable. A body whose resolve
// began at or after it (a concurrent writer that stored meanwhile, possibly from
// a reverted spec) is spared. A cell with no resolve start predates the epoch
// guard entirely (every production writer stamps one; the #375 detector counts
// a resolve that does not), so it cannot be newer and is evicted. A declining
// resolve with no start of its own can place nothing, and evicts nothing. The
// generation alone could not decide this: a replace-in-place keeps it.
//
// THE SUBSCRIBER. A cell served stale to an armed tab has an open stale serve
// (stale_push.go). Its eviction runs through removeElementLocked, whose
// evictDirtyWindow marks those serves replaced (a refetch is a fresh fill), and
// the eviction is published (PublishEviction, the refresh frame the DELETE paths
// send), so the armed tab refetches and its serve closes pushed — never the
// silent cache.stale_push_unreplaced the issue observed.
//
// THE TTL IS NOT A BOUND ON ITS OWN. Expiry runs from the entry's CreatedAt
// (itemExpiredLocked), which only an accepted Put stamps (putPreamble): no read,
// touch, decline or known-stale settle moves it. But a body DERIVED from a stale
// cell is re-Put with a fresh CreatedAt: a widget cell Go-sliced from a stale
// raFullList cell (#406, apiref's fast path) while that cell's refresh is still in
// flight stores the pre-edit body anew, and #406 remarks such a widget only on a
// raKey COMMIT — which a declined raKey never makes. So evicting a raFullList cell
// here also remarks its resident layered consumers (raLayerConsumersLocked): each
// re-resolves, misses the raKey, resolves the RESTAction live, and is declined and
// evicted the same way.
//
// Delete-only invalidation allows it: an eviction is a cold, correct refill.
// It never moves evict_delete_total (the informer-DELETE discriminator); it
// counts on evict_unstorable_total.
package cache

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// The non-verdict reasons EvictUnstorable takes (the verdict's own reasons are
// PutReason values).
const (
	UnstorableSensitive = "sensitive_resource"
	UnstorableUAF       = "uaf"
)

// EvictOnPermanentDecline evicts key's cell when v is a PERMANENT decline of a
// resolve that began after the cell's body was resolved, and the cell's own
// storage contract would not have stored that resolve either (see the file
// comment). ctx is the declining resolve's ctx (its #375 dep-gen sink holds the
// resolve's start). Returns true iff a cell was evicted. A transient or storing
// verdict evicts nothing.
func (c *ResolvedCacheStore) EvictOnPermanentDecline(ctx context.Context, v PutVerdict, key string) bool {
	// An inert (dry-run) resolve leaves no trace (#443), an eviction included.
	if !v.Permanent() || Inert(ctx) {
		return false
	}
	return c.evictPredating(ctx, key, string(v.Reason()), func(e *ResolvedEntry) bool {
		// The cell's own contract: an external-TTL cell is stored by the opt-in
		// that turns a third-party external touch into a store — a clean one now,
		// or a clean retry after a transient failure of that same endpoint (#1255
		// F1: a 5xx must never evict a body its own contract still stores).
		return e.ExternalTTL && v.StorableUnderExternalTTL()
	})
}

// EvictUnstorable evicts key's cell for a decline outside the Put-gate verdict
// that equally proves the key unstorable — a sensitive read (#398,
// UnstorableSensitive) or a userAccessFilter-narrowed identity-free body
// (1.12.3 A-1, UnstorableUAF): no writer stores either, whatever the cell. Same
// epoch guard as EvictOnPermanentDecline. Returns true iff a cell was evicted.
func (c *ResolvedCacheStore) EvictUnstorable(ctx context.Context, key, reason string) bool {
	if Inert(ctx) { // #443 — as EvictOnPermanentDecline
		return false
	}
	return c.evictPredating(ctx, key, reason, nil)
}

// evictPredating removes key's cell iff its body was resolved strictly before
// ctx's resolve began and keeps does not claim it, then — with every store lock
// released (the S10 rule PublishEviction's callers keep) — counts it, logs it
// and publishes it. Its two callers have refused an inert ctx already.
func (c *ResolvedCacheStore) evictPredating(ctx context.Context, key, reason string, keeps func(*ResolvedEntry) bool) bool {
	if c == nil || key == "" {
		return false
	}
	start, ok := DepGenStartSeqFromContext(ctx)
	if !ok {
		return false
	}
	c.mu.Lock()
	el, ok := c.index[key]
	if !ok {
		c.mu.Unlock()
		return false
	}
	e := el.Value.(*lruItem).entry
	if e == nil || (e.hasResolveStart && e.resolveStart >= start) || (keeps != nil && keeps(e)) {
		c.mu.Unlock()
		return false
	}
	class := putClassUnknown
	if e.Inputs != nil && e.Inputs.CacheEntryClass != "" {
		class = e.Inputs.CacheEntryClass
	}
	// A widget cell Go-sliced from this raFullList cell (#406) holds a body
	// derived from the stale one; no raKey commit will ever remark it now, and its
	// own accepted re-Put reset its TTL. Remark it, so its refresh resolves the
	// RESTAction live (and, declined the same way, is evicted in turn).
	var consumers []string
	if isRAFullListEntry(e) {
		consumers = c.raLayerConsumersLocked(key)
	}
	c.removeElementLocked(el)
	c.mu.Unlock()
	remarkLayerConsumers(consumers)
	c.evictUnstorableTotal.Add(1)
	noteEvictUnstorable(reason)
	// INFO, not WARN: an eviction is designed work, not a failure. At the chart's
	// default LOG_LEVEL=warn this line does NOT ship; the shipped signal is the
	// counter (snowplow_l1_evict_unstorable_total{reason} on OTLP, and
	// snowplow_resolved_cache{stat=evict_unstorable_total}). Metadata only: the
	// opaque L1 key digest, the writer, the class and the reason; never a body,
	// an identity or a resource name.
	slog.Default().InfoContext(ctx, "cache.unstorable_evicted",
		slog.String("subsystem", "cache"),
		slog.String("writer", RefDenialOrigin(ctx)),
		slog.String("handler", class),
		slog.String("key_hash", key),
		slog.String("reason", reason),
		slog.String("effect", "a resolve of this key proved it can no longer be stored (permanent decline); the prior "+
			"entry was evicted so the next request resolves live instead of serving the pre-change body until its "+
			"TTL, and armed tabs were told to refetch (#1255)"),
	)
	publishEvictionFn(key)
	return true
}

// EvictUnstorableReasons is the closed reason set the per-reason eviction
// counter reports, in export order (the verdict's three permanent reasons, the
// two non-verdict ones, then "other" for anything outside them).
var EvictUnstorableReasons = []string{
	string(PutReasonDeclaredWrite), string(PutReasonExternal), string(PutReasonStageError),
	UnstorableSensitive, UnstorableUAF, "other",
}

// evictUnstorableByReason counts unstorable evictions per EvictUnstorableReasons
// index (process-lifetime, monotonic).
var evictUnstorableByReason [6]atomic.Uint64

func noteEvictUnstorable(reason string) {
	last := len(EvictUnstorableReasons) - 1
	for i, r := range EvictUnstorableReasons[:last] {
		if r == reason {
			evictUnstorableByReason[i].Add(1)
			return
		}
	}
	evictUnstorableByReason[last].Add(1)
}

// EvictUnstorableCount is one reason's unstorable-eviction count.
type EvictUnstorableCount struct {
	Reason string
	Count  uint64
}

// EvictUnstorableByReason returns every reason's count, zeros included, in
// EvictUnstorableReasons order (the OTLP mirror emits the closed set in full).
func EvictUnstorableByReason() []EvictUnstorableCount {
	out := make([]EvictUnstorableCount, len(EvictUnstorableReasons))
	for i, r := range EvictUnstorableReasons {
		out[i] = EvictUnstorableCount{Reason: r, Count: evictUnstorableByReason[i].Load()}
	}
	return out
}

// EvictUnstorableReasonCountForTest reads one reason's count. TEST-ONLY.
func EvictUnstorableReasonCountForTest(reason string) uint64 {
	for _, c := range EvictUnstorableByReason() {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}
