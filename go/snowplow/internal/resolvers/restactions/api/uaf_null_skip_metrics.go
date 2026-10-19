// uaf_null_skip_metrics.go — #778: how often a userAccessFilter's
// resourcesFrom skipped a null element, and in which RESTAction stage.
//
// WHY. Since #767 a null resourcesFrom element is skipped like "": a stage
// whose expression yields only nulls resolves to the empty resource set, which
// denies every item and is cached. That is right while an input has no resource
// yet (a CompositionDefinition not reconciled), and it is also what a typo'd
// path (`[.compdef.staus.resource]`) does — permanently. The only per-stage
// signal is a DEBUG line, invisible at the shipped LOG_LEVEL=warn. This makes
// it a reading: the compdef's status tells transient from typo.
//
// One count per stage evaluation that skipped at least one null (not per null
// element: the DEBUG line keeps skipped_null for that).
//
//   - /debug/vars snowplow_uaf_resources_null_skipped_total — the grand total;
//   - /debug/vars snowplow_uaf_resources_null_skipped_by_stage — map
//     "<namespace>/<name>|<stage>" → count. Bounded by the RESTAction CRs: both
//     halves come from the CR (its name and its spec's stage names), never from
//     resolved data. A stage no Resolve attributed is keyed
//     contentFillUnattributedRA.
//
// OTLP (hand-wired in metrics.go, no expvar bridge): the total and the number
// of distinct stages that ever skipped, with NO attributes. The by-stage map
// stays expvar-only (JWT-gated /debug/vars): its keys name RESTActions, and the
// closed OTLP attribute-key set (design §3.4, #311) admits no object name —
// the same split as the content-fill _by_ra table.
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

var (
	uafNullSkippedTotal atomic.Uint64

	// uafNullSkippedMu guards uafNullSkippedByStage.
	uafNullSkippedMu      sync.Mutex
	uafNullSkippedByStage = map[string]uint64{}
)

// recordUAFNullSkipped counts one stage evaluation whose resourcesFrom skipped
// at least one null element. The owning RESTAction comes from the stage ctx
// (withContentFillOwner, stamped once per stage by the resolver).
func recordUAFNullSkipped(ctx context.Context, stage string) {
	ra := contentFillOwnerFrom(ctx).ra
	if ra == "" {
		ra = contentFillUnattributedRA
	}
	uafNullSkippedTotal.Add(1)
	uafNullSkippedMu.Lock()
	uafNullSkippedByStage[ra+"|"+stage]++
	uafNullSkippedMu.Unlock()
}

// UAFNullSkippedTotal is the grand total, the single source both /debug/vars
// and OTLP read.
func UAFNullSkippedTotal() uint64 { return uafNullSkippedTotal.Load() }

// UAFNullSkippedByStage returns a copy of the "<ns>/<name>|<stage>" → count map.
func UAFNullSkippedByStage() map[string]uint64 {
	uafNullSkippedMu.Lock()
	defer uafNullSkippedMu.Unlock()
	out := make(map[string]uint64, len(uafNullSkippedByStage))
	for k, v := range uafNullSkippedByStage {
		out[k] = v
	}
	return out
}

// UAFNullSkippedStages is how many distinct stages have skipped a null since
// boot — the OTLP gauge an alert keys on.
func UAFNullSkippedStages() int {
	uafNullSkippedMu.Lock()
	defer uafNullSkippedMu.Unlock()
	return len(uafNullSkippedByStage)
}

// RecordUAFNullSkippedForTest records one skip for stage of RESTAction ns/name
// through the production recorder — the OTLP falsifier's way to move the real
// counters. Production MUST NOT use it.
func RecordUAFNullSkippedForTest(ns, name, stage string) {
	recordUAFNullSkipped(withContentFillOwner(context.Background(), ns, name, stage), stage)
}

// ResetUAFNullSkippedForTest zeroes the family. Production MUST NOT use it.
func ResetUAFNullSkippedForTest() {
	uafNullSkippedTotal.Store(0)
	uafNullSkippedMu.Lock()
	uafNullSkippedByStage = map[string]uint64{}
	uafNullSkippedMu.Unlock()
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
