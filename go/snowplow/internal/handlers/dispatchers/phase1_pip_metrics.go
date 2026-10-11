// phase1_pip_metrics.go — Ship PIP (0.30.173) observability counters.
//
// Two grand-total counters + two per-cohort maps, exposed via expvar so
// the existing snowplow expvar pipeline (the tester scrapes /debug/vars)
// picks them up automatically. Mirrors the existing pattern in
// internal/cache/fallthrough_meter_expvar.go (the prior art for this
// shape).
//
// REGISTERED VIA init(). Same gate as the fallthrough meter: under
// CACHE_ENABLED=false the counters MUST NOT be registered so they don't
// appear at /debug/vars even with zero values (cache-off transparent-
// fallback contract — project_cache_off_is_transparent_fallback.md).
//
// PER-COHORT MAPS are exposed as a single expvar.Func returning a
// map[string]uint64 — same shape as snowplow_apiserver_fallthrough_cells.
// The cohort label is the canonical cohortLogLabel (Username for
// user-kind cohorts, "group:<name>" for group-kind cohorts). PIP itself
// is gated such that under PREWARM_PIP_ENABLED=false the counters
// register at zero and never increment — operator sees fresh zeros
// rather than a missing key.
//
// Ship 0.30.187 D1: two additional observability surfaces address the
// 0.30.186 silent widget-seed-failure mode where a single cohort's
// widget seed errors (e.g. RBAC denial on a particular widget for the
// group:devs cohort) were logged but produced no operator-visible
// signal of WHICH widget broke WHICH cohort.
//
//   - snowplow_phase1_widget_seed_failure_total
//       expvar.Func returning map["cohort|widget_name|gvr"] -> uint64
//       Bumped at every seedOneWidget error.
//   - snowplow_phase1_restaction_seed_failure_total
//       Symmetric counter for restaction failures (same defect class).
//
// (snowplow_phase1_cohort_seed_status was DELETED in the 2026-07-04
// counters-hygiene pass — its backing map had zero writers post the
// 2026-07-03 prewarm-family fold and published {} forever; see the deletion
// note at the var block below.)

package dispatchers

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// Grand totals across all cohorts.
var (
	// Ship A.3 / 0.30.179 — binding-set enumeration counters.
	//
	// INCREMENTED-BY (audited 2026-07-04, counters-hygiene pass — the next
	// 50K debugger reads these cold):
	//   - pipBindingSetSeedResolvesTotal: incremented once per successful seed
	//     UNIT Put — the handle.Put success path in seedOneRestaction +
	//     seedOneWidget (phase1_pip_seed.go). Means "seed units resolved +
	//     written to per-user L1". (Its only historical incrementer, runPIPSeed,
	//     was DELETED in the 2026-07-03 prewarm-family fold, leaving it
	//     dead-at-0 — which made a 287-real-seed boot read as "seed didn't run";
	//     re-wired in the 2026-07-04 counters-hygiene pass.)
	//   - pipBindingSetSeedFailuresTotal: the back-compat grand total (=
	//     rbac_deny + operational). Incremented ONLY from classifyEngineSeedErr
	//     (prewarm_engine_boot.go:450) — the engine seed loop's per-target
	//     failure path. (runPIPSeed, the other historical feeder, is deleted.)
	pipBindingSetSeedResolvesTotal atomic.Uint64
	pipBindingSetSeedFailuresTotal atomic.Uint64

	// #158 (P9-B) — the SPLIT of pipBindingSetSeedFailuresTotal by error
	// class. pipBindingSetSeedFailuresTotal is KEPT as the back-compat
	// grand total (= rbac_deny + operational) so existing dashboards do
	// not break; these two carry the discriminated signal:
	//
	//   - pipSeedRBACDenyTotal (snowplow_phase1_seed_rbac_deny_total):
	//     EXPECTED narrow-RBAC denies (403/401). A non-zero value here is
	//     normal — these cohorts genuinely cannot read the seed target and
	//     need no L1 entry. NOT re-enqueued.
	//   - pipSeedOperationalFailTotal (snowplow_phase1_seed_operational_fail_total):
	//     UNEXPECTED operational failures (ctx timeout/cancel, 5xx,
	//     transport, panic, fail-loud default). A non-zero value is a real
	//     hole the operator can act on; these ARE re-enqueued (legacy:
	//     bounded retry; engine: coalesced boot re-walk).
	//
	// INCREMENTED-BY: classifyEngineSeedErr (prewarm_engine_boot.go:452/463)
	// via classifySeedErr (phase1_seed_classify.go) — the engine seed loop's
	// per-target failure classifier. (The historical runPIPSeed feeder was
	// deleted in the 2026-07-03 prewarm-family fold; the engine seed loop is
	// now the sole feeder.)
	pipSeedRBACDenyTotal        atomic.Uint64
	pipSeedOperationalFailTotal atomic.Uint64

	// #102 GTTL-1 — pipSeedSkippedStageErrorTotal counts seed Puts the
	// error-aware Put-gate (declineSeedPutOnError, phase1_pip_seed.go) declined
	// because the seed re-resolve observed a swallowed stage error OR touched an
	// external endpoint — the SAME backstop the refresher applies
	// (resolve_populate.go). SEED-SCOPED and distinct from the refresher's
	// refresherSkippedStageError so a keepwarm-sweep / boot-seed decline is
	// attributable in /debug/vars (the falsifier keys on this DELTA, not the
	// refresher's counter). Exposed as snowplow_phase1_seed_skipped_stage_error_total.
	pipSeedSkippedStageErrorTotal atomic.Uint64

	// F.4 (design §3.2) — pipSeedFreshSkipTotal counts boot-scope seed targets
	// fast-forwarded by the boot-only fresh-skip: a live (non-expired) L1 cell
	// already existed under the production key, so the resolve + Put were
	// skipped and the target counted as processed. This is what makes a
	// deadline-cut boot chunk's continuation cost-proportional (chunk N+1 ≈
	// preamble + only the cold remainder). SEED-SCOPED and DISTINCT from every
	// other seed counter: freshSkip does NOT bump seed_resolves (no Put) nor
	// skipped_stage_error (no resolve at all). A boot re-drive over a fully-warm
	// set drives this ≈ the chunk-1 seeded count while seed_resolves stays flat.
	// Boot-only: keepwarm / gvr-discovered never fresh-skip (F4-C3), so this
	// stays flat outside boot chunks. Exposed as
	// snowplow_phase1_seed_fresh_skip_total.
	pipSeedFreshSkipTotal atomic.Uint64

	// keepwarm c2 (design §4.2) — keepwarmAgeSkipTotal counts KEEPWARM-scope seed
	// targets elided by the AGE-SKIP: a live L1 cell already existed under the
	// production key AND was YOUNG (age < TTL−sweepInterval = TTL/4), so the
	// resolve + Put were skipped. This is what makes a deadline-cut keepwarm
	// chunk's continuation cost-proportional (chunk N+1 skips the prefix the
	// earlier chunk re-Put) AND lets the sweep dedup churny cells the refresher /
	// a customer Put already refreshed this window. DISTINCT from
	// pipSeedFreshSkipTotal (that is BOOT-scope, bare-liveness): a keepwarm sweep
	// over a set of OLD live cells drives keepwarmAgeSkip ≈ 0 (they are re-Put,
	// bumping seed_resolves), while a sweep over recently-refreshed cells drives
	// it up (seed_resolves flat). Keepwarm-only: boot / gvr-discovered never
	// age-skip (F4-C3 per-mode boundary). Exposed as
	// snowplow_phase1_keepwarm_age_skip_total.
	keepwarmAgeSkipTotal atomic.Uint64

	// 1.12.7 observability — harvestForgottenNavTotal / harvestForgottenApiRefTotal
	// count HARVESTED ENTRIES ACTUALLY REMOVED from each Phase-1 harvester
	// because the object was confirmed gone: the dep tracker's ABSENT verdict
	// (F6b, via cache.RegisterGoneForgetHook) or the walk's confirmed 404 (F6c).
	//
	// COUNT THE EFFECT, NEVER THE INVOCATION. These bump inside each
	// harvester's forgetCoordinate, once per entry dropped — NOT where the hook
	// fires. Almost every gone verdict names a coordinate that was never
	// harvested, so counting hook invocations would report a large, steadily
	// climbing number that says nothing about whether any replay was actually
	// stopped. A non-zero value here means a specific harvested copy stopped
	// being re-resolved into L1. Incrementing at the removal also makes the
	// count structural: a future third caller of forgetCoordinate cannot forget
	// to count.
	//
	// SPLIT PER HARVESTER, because they fail independently: the nav-widget
	// harvester feeds the per-binding widget seed and the content-prewarm
	// harvester feeds the apiRef RESTAction pass. A forget landing on one and
	// not the other is the shape worth seeing. The nav counter can move by more
	// than one per coordinate — one widget may be harvested under several
	// pagination tuples, and all of them go.
	//
	// Exposed as snowplow_phase1_harvest_forgotten_total, a map keyed by
	// harvester ("nav_widget" / "apiref").
	harvestForgottenNavTotal    atomic.Uint64
	harvestForgottenApiRefTotal atomic.Uint64

	// 1.12.7 review §2(a) — goneVerdictsTotal counts GONE VERDICTS DELIVERED to
	// the harvesters: one per invocation of the forget hook, whether or not
	// anything was harvested under that coordinate.
	//
	// WHY IT IS NOT REDUNDANT WITH harvest_forgotten_total. That counter reports
	// forgets that HAPPENED, so a regressed F6b hook reads 0 — identical to a
	// quiet week with no deletions. The pair disambiguates:
	//   forgotten == 0 && verdicts == 0  → a quiet cluster; nothing was deleted.
	//   forgotten == 0 && verdicts  > 0  → deletions ARE arriving and none is
	//                                      being forgotten: the mechanism or the
	//                                      instrument is dead.
	// A silence that excludes nothing is not information; this is its denominator.
	goneVerdictsTotal atomic.Uint64
)

// Ship 0.30.187 D1 — per-(cohort, target) failure maps. Keyed by
// "cohort|name|gvr" (widgets) or "cohort|namespace/name" (restactions);
// value is *atomic.Uint64. The composite key keeps the per-cohort and
// per-target dimensions independent: an operator inspecting
// /debug/vars sees the exact widget that broke the exact cohort.
var (
	pipWidgetSeedFailureByKey     sync.Map
	pipRestactionSeedFailureByKey sync.Map
)

// counters-hygiene 2026-07-04: DELETED the orphaned pipCohortSeedStatus map +
// its recordCohortSeedStatus writer + cohortStatus{Success,Partial,Failed}
// constants + the snowplow_phase1_cohort_seed_status expvar publish. The map
// had ZERO writers (its only caller was the runPIPSeed errgroup task, deleted
// in the 2026-07-03 prewarm-family fold) → the expvar published {} forever =
// the same always-empty red-herring observability this pass is killing
// (it read `phase1_cohort_seed_status:{}` and looked like "no cohort seed
// ran"). A sensible per-cohort success/partial/failed status would need the
// engine seed LOOP (prewarm_engine_boot.go) to aggregate per-cohort outcome —
// that file is FROZEN for arch Track 1, and wiring it there is out of this
// pass's scope — so DELETE (not wire) is the honest choice. observability.md +
// the two design-doc references updated in the same change.

// incFailureCounter bumps the per-(cohort, target) failure counter. Lazy
// allocation on first observation via the LoadOrStore + Add pattern (same
// shape as the fallthrough meter's recordFallthroughCounter prior art).
func incFailureCounter(m *sync.Map, compositeKey string) {
	if v, ok := m.Load(compositeKey); ok {
		v.(*atomic.Uint64).Add(1)
		return
	}
	fresh := new(atomic.Uint64)
	actual, loaded := m.LoadOrStore(compositeKey, fresh)
	if loaded {
		actual.(*atomic.Uint64).Add(1)
		return
	}
	fresh.Add(1)
}

// pipMetricsOnce guards the expvar.Publish calls — same pattern as
// fallthroughExpvarOnce (sync.Once prevents double-publish panic if
// init() runs twice, e.g. in some test harnesses).
var pipMetricsOnce sync.Once

func init() {
	// CFG-1 mirror: under CACHE_ENABLED=false the cache subsystem does
	// not exist and PIP counters MUST NOT be registered. cache.Disabled()
	// is authoritative at init() time (Go runtime populates env vars
	// before package init() runs).
	if cache.Disabled() {
		return
	}
	registerPIPMetrics()
	registerWalkCoverageExpvar()
}

// registerPIPMetrics performs the expvar.Publish calls for the PIP
// counters. Guarded by pipMetricsOnce so it is safe to call from both
// init() and a test helper.
func registerPIPMetrics() {
	pipMetricsOnce.Do(func() {
		// Task #101 (hygiene) — the four legacy seed totals
		// (snowplow_phase1_seed_{restactions,widgets}_total and their two
		// _by_cohort maps) DELETED. Their only incrementer was runPIPSeed
		// (the legacy seed loop, PrewarmEngineEnabled()==false). Production
		// runs the unified engine path (PrewarmEngineEnabled()==true), which
		// bypasses runPIPSeed entirely (phase1_pip_seed.go:494, phase1_walk.go:409)
		// — so these four counters were permanently zero in production
		// (confirmed live: 0/0/{}/{} while prewarm_engine_processed_total>0).
		// Always-zero expvars are misleading observability. The LIVE seed
		// signals — the failure-classification family (rbac_deny /
		// operational_fail / per-(cohort,target) maps) + the now-wired
		// bindingset_seed_resolves total (counters-hygiene 2026-07-04) —
		// published below are populated by the engine seed path and stay.
		// (cohort_seed_status was ALSO deleted in that pass — same always-{}
		// class; its writer was runPIPSeed. See the var-block note.)

		// Ship 0.30.242 H.c-layered Phase 2b — binding-set classes /
		// powerset-skipped counters DELETED. The underlying functions
		// (cache.Phase1EnumBindingsetClassesTotal,
		// cache.Phase1EnumPowersetSkippedTotal) lived in the deleted
		// binding_set_enumeration.go; their counters incremented from
		// code paths that no longer exist. Keeping the expvar
		// registrations would publish always-zero values (misleading
		// observability — design-gap fix Gap 4).
		//
		// The per-target seed resolves / failures counters KEPT (they
		// now count per-binding-target instead of per-cohort, same shape
		// — see runPIPSeed for the seedTarget loop).
		expvar.Publish("snowplow_phase1_bindingset_seed_resolves_total", expvar.Func(func() any {
			return pipBindingSetSeedResolvesTotal.Load()
		}))
		expvar.Publish("snowplow_phase1_bindingset_seed_failures_total", expvar.Func(func() any {
			return pipBindingSetSeedFailuresTotal.Load()
		}))

		// #158 (P9-B) — the discriminated split of the failures total. The
		// back-compat grand total above stays = rbac_deny + operational so
		// existing dashboards don't break; these two let an operator tell an
		// EXPECTED narrow-RBAC deny from a REAL operational hole.
		expvar.Publish("snowplow_phase1_seed_rbac_deny_total", expvar.Func(func() any {
			return pipSeedRBACDenyTotal.Load()
		}))
		expvar.Publish("snowplow_phase1_seed_operational_fail_total", expvar.Func(func() any {
			return pipSeedOperationalFailTotal.Load()
		}))

		// #102 GTTL-1 — seed Puts declined by the error-aware Put-gate (stage
		// error / external touch). SEED-SCOPED; distinct from the refresher's
		// refresherSkippedStageError so a keepwarm-sweep decline is attributable.
		expvar.Publish("snowplow_phase1_seed_skipped_stage_error_total", expvar.Func(func() any {
			return pipSeedSkippedStageErrorTotal.Load()
		}))

		// F.4 (design §3.2) — boot-scope seed targets fresh-skipped (live cell
		// already under the production key; resolve+Put skipped, counted
		// processed). SEED-SCOPED, boot-only; distinct from seed_resolves.
		expvar.Publish("snowplow_phase1_seed_fresh_skip_total", expvar.Func(func() any {
			return pipSeedFreshSkipTotal.Load()
		}))

		// keepwarm c2 (design §4.2) — keepwarm-scope seed targets elided by the
		// age-skip (live cell younger than TTL/4; resolve+Put skipped). SEED-SCOPED,
		// keepwarm-only; distinct from fresh_skip (boot, bare-liveness).
		expvar.Publish("snowplow_phase1_keepwarm_age_skip_total", expvar.Func(func() any {
			return keepwarmAgeSkipTotal.Load()
		}))

		// #106 — config-vars UPDATE events suppressed by the data-change gate
		// (metadata-only CDC churn; Data+BinaryData byte-identical). Steady-state
		// rate == the CDC churn rate, so a climbing value proves the gate is
		// actively firing (gate-working vs informer-dead/churn-stopped).
		expvar.Publish("snowplow_phase1_configvars_skipped_total", expvar.Func(func() any {
			return configVarsSkippedTotal.Load()
		}))

		// 1.12.7 observability — harvested entries actually DROPPED because the
		// object was confirmed gone, split by harvester. Zero on a cluster where
		// nothing is deleted; a climbing value is the proof that the 1.12.7
		// replay fix is doing work, and the only production-visible evidence
		// that a coordinate was forgotten rather than merely reported gone.
		// See the counter var block for why this counts removals, not hook
		// invocations.
		expvar.Publish("snowplow_phase1_harvest_forgotten_total", expvar.Func(func() any {
			return map[string]uint64{
				"nav_widget": harvestForgottenNavTotal.Load(),
				"apiref":     harvestForgottenApiRefTotal.Load(),
				// The denominator: verdicts delivered, forgotten or not.
				"gone_verdicts": goneVerdictsTotal.Load(),
			}
		}))

		// Ship 0.30.187 D1 — per-(cohort, target) seed-failure maps so
		// operators see WHICH widget/restaction broke WHICH cohort.
		// Composite key shape: "cohort|name|gvr" (widgets) /
		// "cohort|namespace/name" (restactions).
		expvar.Publish("snowplow_phase1_widget_seed_failure_total", expvar.Func(func() any {
			out := map[string]uint64{}
			pipWidgetSeedFailureByKey.Range(func(k, v any) bool {
				out[k.(string)] = v.(*atomic.Uint64).Load()
				return true
			})
			return out
		}))
		expvar.Publish("snowplow_phase1_restaction_seed_failure_total", expvar.Func(func() any {
			out := map[string]uint64{}
			pipRestactionSeedFailureByKey.Range(func(k, v any) bool {
				out[k.(string)] = v.(*atomic.Uint64).Load()
				return true
			})
			return out
		}))
		// counters-hygiene 2026-07-04: snowplow_phase1_cohort_seed_status
		// DELETED — its backing map had zero writers post prewarm-family fold
		// (published {} forever). See the deletion note at the var block above.
	})
}
