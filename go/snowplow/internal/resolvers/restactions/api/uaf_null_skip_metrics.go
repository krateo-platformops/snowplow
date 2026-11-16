// uaf_null_skip_metrics.go — #778: how often a userAccessFilter's
// resourcesFrom skipped a null element, and which RESTAction stages are doing
// it NOW.
//
// WHY. Since #767 a null resourcesFrom element is skipped like "": a stage
// whose expression yields only nulls resolves to the empty resource set, which
// denies every item and is cached. That is right while an input has no resource
// yet (a CompositionDefinition not reconciled), and it is also what a typo'd
// path (`[.compdef.staus.resource]`) does — permanently. The only per-stage
// signal is a DEBUG line, invisible at the shipped LOG_LEVEL=warn.
//
// CURRENT STATE, NOT "EVER" (architect ruling, #778). A stage that skipped once
// while its compdef was unreconciled must not read as broken forever, so each
// row carries skipping_now — whether the stage's LATEST evaluation skipped a
// null — and the gauge counts those. A typo stays up; a compdef that
// reconciles brings its stage back to 0.
//
// One count per stage evaluation that skipped at least one null (not per null
// element: the DEBUG line keeps skipped_null for that).
//
//   - /debug/vars snowplow_uaf_resources_null_skipped_total — the grand total;
//   - /debug/vars snowplow_uaf_resources_null_skipped_by_stage — map
//     "<namespace>/<name>|<stage>" → {count, skipping_now}. A row exists only
//     for a stage that has skipped; both halves of its key come from the CR
//     (its name and its spec's stage names), never from resolved data. Bounded
//     by uafNullSkipStageCap: a stage beyond it folds into the
//     uafNullSkipOtherKey row. A stage no Resolve attributed is keyed
//     contentFillUnattributedRA.
//
// OTLP (hand-wired in metrics.go, no expvar bridge), NO attributes: the total
// and snowplow_uaf_resources_null_skipping_stages, the rows whose latest
// evaluation skipped. The by-stage map stays expvar-only (JWT-gated
// /debug/vars): its keys name RESTActions, and the closed OTLP attribute-key
// set (design §3.4, #311) admits no object name — the same split as the
// content-fill _by_ra table.
//
// Metadata only: never the expression, never a value it yielded, never an
// identity.
//
// CFG-1: gated on cache.Disabled(), like its siblings.
package api

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// uafNullSkipStageCap bounds the by-stage map. The live corpus has tens of
// userAccessFilter stages; the cap only stops a pathological CR set from
// growing the map without limit.
const uafNullSkipStageCap = 1024

// uafNullSkipOtherKey is the overflow row for stages beyond the cap. Per-stage
// state past the cap is exactly what the cap gives up, so once anything folds
// the skipping-stages gauge is APPROXIMATE in both directions: every folded
// stage together counts as at most 1 (an undercount), and the row carries the
// latest folded SKIP — a folded stage that later resolves has no row of its
// own, so it cannot clear __other__.skipping_now (an overstatement). The
// total stays exact.
const uafNullSkipOtherKey = "__other__"

// UAFNullSkipRow is one stage's row of the by-stage map.
type UAFNullSkipRow struct {
	Count       uint64 `json:"count"`
	SkippingNow bool   `json:"skipping_now"`
}

var (
	uafNullSkippedTotal atomic.Uint64

	// uafNullSkipMu guards uafNullSkipByStage.
	uafNullSkipMu      sync.Mutex
	uafNullSkipByStage = map[string]*UAFNullSkipRow{}
)

// recordUAFResourcesEvaluated records one resourcesFrom evaluation of stage:
// skipped reports whether it resolved AND skipped at least one null (an
// unresolved set is a stage error, not a skip, so it clears skipping_now). A
// stage that never skipped gets no row. The owning RESTAction comes from the stage ctx
// (withContentFillOwner, stamped once per stage by the resolver).
func recordUAFResourcesEvaluated(ctx context.Context, stage string, skipped bool) {
	ra := contentFillOwnerFrom(ctx).ra
	if ra == "" {
		ra = contentFillUnattributedRA
	}
	key := ra + "|" + stage
	uafNullSkipMu.Lock()
	defer uafNullSkipMu.Unlock()
	row := uafNullSkipByStage[key]
	if row == nil {
		if !skipped {
			return
		}
		if len(uafNullSkipByStage) >= uafNullSkipStageCap {
			key = uafNullSkipOtherKey
			row = uafNullSkipByStage[key]
		}
		if row == nil {
			row = &UAFNullSkipRow{}
			uafNullSkipByStage[key] = row
		}
	}
	if skipped {
		row.Count++
		uafNullSkippedTotal.Add(1)
	}
	row.SkippingNow = skipped
}

// UAFNullSkippedTotal is the grand total, the single source both /debug/vars
// and OTLP read.
func UAFNullSkippedTotal() uint64 { return uafNullSkippedTotal.Load() }

// UAFNullSkippedByStage returns a copy of the "<ns>/<name>|<stage>" rows.
func UAFNullSkippedByStage() map[string]UAFNullSkipRow {
	uafNullSkipMu.Lock()
	defer uafNullSkipMu.Unlock()
	out := make(map[string]UAFNullSkipRow, len(uafNullSkipByStage))
	for k, r := range uafNullSkipByStage {
		out[k] = *r
	}
	return out
}

// UAFNullSkippingStages is how many rows' latest evaluation skipped a null —
// the OTLP gauge an alert keys on.
func UAFNullSkippingStages() int {
	uafNullSkipMu.Lock()
	defer uafNullSkipMu.Unlock()
	n := 0
	for _, r := range uafNullSkipByStage {
		if r.SkippingNow {
			n++
		}
	}
	return n
}

// UAFNullSkipStageCap exposes the cap so the falsifier asserts against the
// production constant.
func UAFNullSkipStageCap() int { return uafNullSkipStageCap }

// RecordUAFResourcesEvaluatedForTest records one evaluation of stage of
// RESTAction ns/name through the production recorder — the OTLP falsifier's way
// to move the real series. Production MUST NOT use it.
func RecordUAFResourcesEvaluatedForTest(ns, name, stage string, skipped bool) {
	recordUAFResourcesEvaluated(withContentFillOwner(context.Background(), ns, name, stage), stage, skipped)
}

// ResetUAFNullSkippedForTest zeroes the family. Production MUST NOT use it.
func ResetUAFNullSkippedForTest() {
	uafNullSkippedTotal.Store(0)
	uafNullSkipMu.Lock()
	uafNullSkipByStage = map[string]*UAFNullSkipRow{}
	uafNullSkipMu.Unlock()
}

var uafNullSkippedExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerUAFNullSkippedExpvar()
}

func registerUAFNullSkippedExpvar() {
	uafNullSkippedExpvarOnce.Do(func() {
		expvar.Publish("snowplow_uaf_resources_null_skipped_total", expvar.Func(func() any {
			return UAFNullSkippedTotal()
		}))
		expvar.Publish("snowplow_uaf_resources_null_skipped_by_stage", expvar.Func(func() any {
			return UAFNullSkippedByStage()
		}))
	})
}

// RegisterUAFNullSkippedExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterUAFNullSkippedExpvarForTest() { registerUAFNullSkippedExpvar() }
