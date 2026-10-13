// refresh_rebuild_identity.go — the instrument that answers #604.
//
// THE QUESTION IT EXISTS TO ANSWER. #603 established that dirty-mark load grows
// with the SQUARE of the population: 99.66% of 16.1M marks come from list_dep
// edges, which are keyed on (GVR, namespace) and so match every object event on
// that GVR whatever moved. #609 measured what that costs — 2,028,429 refresh
// completions against 19,845 customer reads, 102 refreshes per read, while
// still missing the 1s freshness bar at p95 (6,724ms).
//
// The two candidate remedies are not interchangeable:
//
//   - NARROW the invalidation (make a list_dep edge match less). Attacks the
//     exponent and costs nothing in guarantee — zero-cold-navigations is
//     untouched because every cell stays warm.
//   - REFRESH LESS (#567, promote on demand). Also attacks the exponent, but a
//     never-read cell is cold on first read, which weakens the hard
//     requirement. That collision is #604.
//
// WHICH ONE IS RIGHT DEPENDS ON A NUMBER NOBODY HAS: what share of those two
// million refreshes produce the SAME BYTES they replaced. A high share means
// the invalidation is over-broad and precision is the answer. A low share means
// the data genuinely churns, precision buys nothing, and #567 is the only
// lever. This file measures that share.
//
// WHY IT COULD NOT BE MEASURED BEFORE. #579: both indexer read paths enumerated
// a Go map, so a LIST rebuilt from unchanged data produced different bytes every
// time and a no-op refresh was indistinguishable from a real one. That is fixed
// (8073e959) — which is what makes this instrument meaningful rather than noise.
//
// IT ONLY OBSERVES. No Put is skipped, no behaviour changes, nothing is gated on
// the verdict. Skipping the store on an identical rebuild is the obvious
// follow-on and is deliberately NOT done here: it is a behaviour change that
// wants its own falsifier and its own review, and bundling it would make this
// measurement un-trustworthy (you cannot measure a thing with an instrument that
// also alters it).
//
// ⚠ THE IDENTICAL SHARE IS A LOWER BOUND, NOT A POINT ESTIMATE. The comparison
// is over bytes. Any residual non-determinism in the rebuild — a map iteration
// order this codebase has not yet pinned, an embedded timestamp, a JQ output
// ordering — renders as "changed" even when nothing semantically moved. So:
//
//   a HIGH identical share is SOUND evidence that invalidation is over-broad;
//   a LOW identical share is AMBIGUOUS — it means either the data really churns
//   OR some rebuild is still non-deterministic, and that must be ruled out
//   before concluding anything.
//
// #579 removed the one known source. Others may exist. Read the number in that
// direction and no other.
//
// NAMING. The key is deliberately NOT under `snowplow_refresher_`: that prefix
// is an ExpvarPrefix family and C7's parity guard rejects any key under it with
// no tagged refresherStats field, while adding such a tag would auto-mirror this
// to OTLP — which a diagnostic counter must not do. Same constraint #386 hit.

package dispatchers

import (
	"bytes"
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

var (
	// rebuildIdentical — the refresher rebuilt a cell and got byte-identical
	// output to the entry it is about to replace. This refresh changed nothing.
	rebuildIdentical atomic.Uint64
	// rebuildChanged — the rebuilt bytes differ from the stored ones. Either a
	// genuine change or residual non-determinism; the counter cannot tell them
	// apart, which is why identical is a lower bound.
	rebuildChanged atomic.Uint64
	// rebuildNoPrior — nothing to compare against: the cell was evicted between
	// the resolve and this point, or never existed. Published so the two shares
	// above are interpretable as fractions of a known denominator rather than of
	// an assumed one, and so a zero in them cannot be read as health.
	rebuildNoPrior atomic.Uint64

	rebuildPublishOnce sync.Once
)

// rebuildVerdict is the classification of one refresher rebuild.
type rebuildVerdict uint8

const (
	rebuildVerdictNoPrior rebuildVerdict = iota
	rebuildVerdictIdentical
	rebuildVerdictChanged
)

// classifyRebuild is pure: it decides a verdict from the prior entry and the
// freshly encoded bytes. Separated from the counting so the decision can be
// tested without a cache, a refresher or a cluster.
//
// An empty rebuild is NOT treated as identical to an empty prior — both are
// classified NoPrior, because a cell with no bytes is not evidence that a
// refresh changed nothing; it is evidence there is nothing to compare.
func classifyRebuild(prior *cache.ResolvedEntry, hadPrior bool, encoded []byte) rebuildVerdict {
	if !hadPrior || prior == nil || len(prior.RawJSON) == 0 || len(encoded) == 0 {
		return rebuildVerdictNoPrior
	}
	if bytes.Equal(prior.RawJSON, encoded) {
		return rebuildVerdictIdentical
	}
	return rebuildVerdictChanged
}

// noteRefreshRebuild records one verdict. Call it immediately before the
// terminal store, so the comparison is against the entry actually being
// replaced rather than one read earlier in the function.
func noteRefreshRebuild(prior *cache.ResolvedEntry, hadPrior bool, encoded []byte) {
	switch classifyRebuild(prior, hadPrior, encoded) {
	case rebuildVerdictIdentical:
		rebuildIdentical.Add(1)
	case rebuildVerdictChanged:
		rebuildChanged.Add(1)
	default:
		rebuildNoPrior.Add(1)
	}
}

// RefreshRebuildStats is the test/diagnostic accessor.
func RefreshRebuildStats() (identical, changed, noPrior uint64) {
	return rebuildIdentical.Load(), rebuildChanged.Load(), rebuildNoPrior.Load()
}

// publishRefreshRebuildExpvar installs the diagnostic map. Idempotent: expvar
// panics on a duplicate Publish, and the dispatchers package is initialised from
// more than one entry point in tests.
func publishRefreshRebuildExpvar() {
	rebuildPublishOnce.Do(func() {
		expvar.Publish("snowplow_refresh_rebuild", expvar.Func(func() any {
			i, c, n := RefreshRebuildStats()
			total := i + c
			out := map[string]any{
				"identical_total": i,
				"changed_total":   c,
				"no_prior_total":  n,
			}
			// The share is what the decision turns on, so publish it rather
			// than making every reader recompute it. Only defined when
			// something was actually compared.
			if total > 0 {
				out["identical_share"] = float64(i) / float64(total)
			}
			return out
		}))
	})
}

func init() {
	// CFG-1 gate. This counter only moves when the refresher re-resolves, and
	// the refresher only runs under CACHE_ENABLED — so on a cache-off pod the
	// key would publish three permanent zeros, which reads as "no no-op
	// refreshes" rather than "no refresher". Gated like the other dispatcher
	// cache metrics (customer_inflight_metrics.go, l1_lookup_metrics.go); the
	// atomics are still maintained either way, only the expvar exposure is
	// gated. Enforced by TestCFG1_Structural_DerivationHasFloor, which is in
	// e2e/bench/cfg1_probe and therefore only visible to a whole-module run.
	if cache.Disabled() {
		return
	}
	publishRefreshRebuildExpvar()
}
