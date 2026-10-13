package rbac

import (
	"context"
	"encoding/hex"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// sa_regate_attribution.go — #574 (owner re-ruling, 2026-10-13): say WHO is
// driving each SA re-gate refusal.
//
// On krateo-057 the branch-E re-gate WARN fired 770 times in ~49 minutes on
// SubjectAccessReview POSTs, and the line could not say whether the refresher,
// the cohort seed, a prewarm pass or the keep-warm sweep produced them, or
// whether a resident cell was behind them. These attributes answer that from
// stamps the ctx already carries. Nothing new is decided here; it is logging
// only.
//
//	sa_regate_origin   cache.RefDenialOrigin: refresher | cohort-seed |
//	                   prewarm-engine-boot | prewarm-path |
//	                   background-unattributed | serve (a live request)
//	sa_regate_trigger  refresher only: dirty-mark (the dequeue carried trigger
//	                   GVRs) | none (a trigger-less dispatch)
//	sa_regate_scope    the prewarm-engine scope kind when one is stamped
//	                   (boot | gvr-discovered | keepwarm | rbac-shift |
//	                   learned-class)
//	key_hash           the L1 cell key the resolve is populating, when there is
//	                   one. It joins with the refresher's own key_hash lines.
//
// No attribute carries identity. key_hash is cache.ComputeKey output, which is
// already a SHA-256 hex of the key inputs; any other shape is replaced by a
// keyed redact label, so a raw key never reaches a log line.

// SARegateOrigin is the sa_regate_origin attribute.
func SARegateOrigin(ctx context.Context) string { return cache.RefDenialOrigin(ctx) }

// SARegateTrigger is the sa_regate_trigger attribute: "dirty-mark" or "none"
// for a refresher dispatch, "" for every other driver.
func SARegateTrigger(ctx context.Context) string {
	if cache.RefDenialOrigin(ctx) != cache.BackgroundOriginRefresher {
		return ""
	}
	if _, ok := cache.RefreshTriggerGVRFromContext(ctx); ok {
		return "dirty-mark"
	}
	return "none"
}

// SARegateScope is the sa_regate_scope attribute ("" when no prewarm-engine
// scope is stamped).
func SARegateScope(ctx context.Context) string {
	s, _ := cache.PrewarmScopeFromContext(ctx)
	return s
}

// SARegateKeyHash is the key_hash attribute ("" when the resolve populates no
// L1 cell).
func SARegateKeyHash(ctx context.Context) string {
	if k := cache.L1KeyFromContext(ctx); k != "" {
		return keyHashLabel(k)
	}
	return ""
}

// keyHashLabel returns k unchanged when it is a SHA-256 hex digest (the
// cache.ComputeKey shape, so the value joins with the refresher's existing
// key_hash lines). Anything else gets a keyed redact label, never the raw key.
func keyHashLabel(k string) string {
	if len(k) == 64 {
		if _, err := hex.DecodeString(k); err == nil {
			return k
		}
	}
	return redact.Prefixed("key", k)
}
