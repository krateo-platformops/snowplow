// phase1_walk_pagination_metrics.go — #156 (P9-A) / 0.30.256 prewarm
// page-depth coverage observability.
//
// PURPOSE. #156 raises the per-widget apiRef page backstop
// (phase1MaxApiRefPages) from a 5%-sample cap (500) to a liveness-only
// anti-runaway ceiling so the pagination drain covers the FULL declared
// list (terminated by the blueprint's `.slice.continue==false`, not by a
// magic-number sample). These three expvars are the ACCEPTANCE signal for
// that change:
//
//   - snowplow_phase1_units_planned
//       Grand total of widgetContent cells the apiRef pagination walk
//       PLANNED to seed — one per page cell handed to populateWidgetContentL1
//       PLUS one per recursed child the page revealed (a child the walk
//       dispatched w.walk for). This is the HEADLINE acceptance number: at
//       the 50K cluster it must climb toward the real reachable widget-CR
//       count (the design's "> ~40,000", vs ~1,078–2,500 pinned at the old
//       500-page cap). A value still pinned near `500 × widget-count` after
//       the drain settles means the cap is still the binding constraint —
//       the falsifier for #156.
//
//   - snowplow_phase1_units_seeded
//       Grand total of page cells the pagination walk handed to
//       populateWidgetContentL1 with a non-nil resolved envelope (the walk's
//       intent to seed reached the populate call). This is a LOWER BOUND on
//       actual L1 writes: populateWidgetContentL1 may still decline a Put
//       internally (RBAC-sensitive routing / transient-empty poison shell /
//       encode failure) — those declines are already counted by
//       snowplow_widget_content_skipped_{rbac_sensitive,empty_shell}_total
//       (widget_content_metrics.go). We deliberately do NOT reach into
//       populateWidgetContentL1 to observe the Put (that file is the shared
//       serve-adjacent path, out of #156's scope); units_seeded counts the
//       pagination loop's seed ATTEMPTS, and the existing skip counters
//       account for the gap between attempts and writes. units_planned is the
//       acceptance signal; units_seeded + the skip counters reconcile it.
//
//   - snowplow_phase1_apiref_pages_total
//       Grand total of EXTRA apiRef pages (page 2..N) the walk resolved
//       across all paginated widgets. The companion to the per-widget
//       `pages_walked` field on phase1.walk.apiref_pagination.completed. A
//       value >> 500 × widget-count confirms the backstop raise took effect.
//
// PRIOR ART — phase1_pip_metrics.go / phase1_walk_metrics.go /
// widget_content_metrics.go: the SAME atomic.Uint64 + expvar.Func +
// sync.Once idiom, same CFG-1 cache-off init gate
// (project_cache_off_is_transparent_fallback). Read-only, lazy at scrape
// time, zero per-/call cost. Under CACHE_ENABLED=false the prewarm walk
// does not run and these gauges MUST NOT be registered (so /debug/vars does
// not advertise always-zero prewarm counters in the transparent-fallback
// posture).

package dispatchers

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// Grand totals across every paginated apiRef widget. Bumped from
// iterateApiRefPages (phase1_walk_pagination.go); read lazily at scrape.
var (
	// prewarmUnitsPlanned — one per cell the pagination walk plans to seed
	// (page cell + each recursed child). THE acceptance signal for #156.
	prewarmUnitsPlanned atomic.Uint64

	// prewarmUnitsSeeded — one per page cell handed to populateWidgetContentL1
	// with a non-nil resolved envelope (lower bound on actual L1 writes; see
	// the file header for the relationship to the skip counters).
	//
	// NAME-vs-SEMANTIC WARNING (counters-hygiene 2026-07-04): despite the
	// published expvar key `snowplow_phase1_units_seeded` reading like "seed
	// units", this counts apiRef CONTENT-PAGINATION page cells (the
	// widgetContent walk), NOT the top-level per-identity restactions/widgets
	// SEED units. The per-identity seed-unit signal is
	// snowplow_phase1_bindingset_seed_resolves_total (phase1_pip_metrics.go).
	// The expvar KEY is deliberately NOT renamed — breaking a published
	// observability key to fix a naming nit is the wrong trade (a 50K debugger
	// keys dashboards/greps on the string); this comment + the increment-site
	// comment kill the red-herring risk instead.
	prewarmUnitsSeeded atomic.Uint64

	// prewarmApiRefPagesTotal — one per EXTRA apiRef page (page 2..N) the
	// walk resolved. Confirms the backstop raise took effect when >> 500×.
	prewarmApiRefPagesTotal atomic.Uint64

	// prewarmApiRefCursorPagesUnstoredTotal — #874: extra pages resolved with
	// the data source's cursor, walked for their children but NOT stored (the
	// cell key has no cursor dimension, so no caller could look the page up).
	prewarmApiRefCursorPagesUnstoredTotal atomic.Uint64

	// prewarmEligibleNoContinueTotal — Task #318 Step 1 (collection
	// robustness). One per DISTINCT eligible widget (isApiRefTemplateDriven)
	// whose page-1 resolve produced NO continuation at collect time — the
	// detectable post-storm zero-collection condition. On a healthy boot this
	// is the normal end-of-list and the value tracks the count of
	// genuinely-single-page apiRef widgets. A SPIKE on a post-storm boot (the
	// datagrid's page-1 saw a transiently short/empty apiRef RA) is the signal
	// that the re-collection pass (phase1.pagination_recollect.*) had work to
	// retry — making the previously-SILENT zero-collection condition
	// observable. Read lazily at scrape; zero per-/call cost.
	prewarmEligibleNoContinueTotal atomic.Uint64
)

// bumpPrewarmUnitsPlanned increments the planned-units grand total by n.
// Called once per page (the page cell) and once per recursed child.
func bumpPrewarmUnitsPlanned(n uint64) { prewarmUnitsPlanned.Add(n) }

// bumpPrewarmUnitsSeeded increments the seeded-units grand total. Called
// once per page cell handed to populateWidgetContentL1.
func bumpPrewarmUnitsSeeded() { prewarmUnitsSeeded.Add(1) }

// bumpPrewarmApiRefPagesTotal increments the extra-pages grand total.
// Called once per resolved page 2..N.
func bumpPrewarmApiRefPagesTotal() { prewarmApiRefPagesTotal.Add(1) }

// bumpPrewarmApiRefCursorPagesUnstored counts a cursor page walked but not
// stored (#874).
func bumpPrewarmApiRefCursorPagesUnstored() { prewarmApiRefCursorPagesUnstoredTotal.Add(1) }

// bumpPrewarmEligibleNoContinue increments the eligible-but-no-continuation
// grand total (Task #318 Step 1). Called once per DISTINCT eligible widget the
// collector saw with no page-1 continuation (collect()'s no-continue branch).
func bumpPrewarmEligibleNoContinue() { prewarmEligibleNoContinueTotal.Add(1) }

// eligibleNoContinueCount returns the current eligible-but-no-continuation
// grand total. Used by tests (the #318 falsifier reads the delta).
func eligibleNoContinueCount() uint64 { return prewarmEligibleNoContinueTotal.Load() }

// phase1PaginationMetricsOnce guards expvar.Publish against the double-
// publish panic (mirrors phase1WalkMetricsOnce / widgetContentMetricsOnce).
var phase1PaginationMetricsOnce sync.Once

func init() {
	// CFG-1 mirror: under CACHE_ENABLED=false the prewarm walk does not run
	// and these gauges MUST NOT be registered (cache-off transparent-
	// fallback contract — project_cache_off_is_transparent_fallback.md).
	// cache.Disabled() is authoritative at init() time (the Go runtime
	// populates env before package init runs).
	if cache.Disabled() {
		return
	}
	registerPhase1PaginationMetrics()
}

// registerPhase1PaginationMetrics performs the expvar.Publish calls for the
// #156 pagination-coverage counters. Guarded by phase1PaginationMetricsOnce
// so it is safe to call from both init() and a test helper.
func registerPhase1PaginationMetrics() {
	phase1PaginationMetricsOnce.Do(func() {
		expvar.Publish("snowplow_phase1_units_planned", expvar.Func(func() any {
			return prewarmUnitsPlanned.Load()
		}))
		expvar.Publish("snowplow_phase1_units_seeded", expvar.Func(func() any {
			return prewarmUnitsSeeded.Load()
		}))
		expvar.Publish("snowplow_phase1_apiref_pages_total", expvar.Func(func() any {
			return prewarmApiRefPagesTotal.Load()
		}))
		expvar.Publish("snowplow_phase1_apiref_cursor_pages_unstored_total", expvar.Func(func() any {
			return prewarmApiRefCursorPagesUnstoredTotal.Load()
		}))
		expvar.Publish("snowplow_phase1_eligible_no_continue_total", expvar.Func(func() any {
			return prewarmEligibleNoContinueTotal.Load()
		}))
	})
}

// RegisterPhase1PaginationMetricsForTest forces registration under tests
// that flip CACHE_ENABLED=true via t.Setenv after init() already ran with
// the var unset. Idempotent (sync.Once). Production callers MUST NOT use
// this. Mirrors RegisterWidgetContentMetricsForTest.
func RegisterPhase1PaginationMetricsForTest() {
	registerPhase1PaginationMetrics()
}
