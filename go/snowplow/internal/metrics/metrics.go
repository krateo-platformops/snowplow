// Package metrics mirrors snowplow's existing expvar counters/gauges onto
// an OpenTelemetry OTLP/HTTP MeterProvider so they can be scraped by the
// OTel collector alongside traces.
//
// ADDITIVE + GATED (load-bearing): the expvar surface at /debug/vars is
// UNTOUCHED — these OTLP instruments are a parallel export of the SAME
// underlying counters, read through the same exported accessors the expvar
// closures use. The whole pipeline is gated by OTEL_METRICS_ENABLED, which
// DEFAULTS to the value of the OTEL_ENABLED master switch when unset (so
// OTEL_ENABLED=true turns metrics on; a per-signal OTEL_METRICS_ENABLED
// still overrides). With both unset it is false. When the gate is off,
// Setup registers NOTHING (no MeterProvider, no exporter, no instruments)
// and returns a no-op shutdown, so the off-path is byte-identical to the
// pre-OTel binary.
//
// This matches the canonical platform OTel enablement contract (the
// chart-inspector internal/telemetry ConfigFromEnv and the runtimes):
//
//	OTEL_ENABLED            master switch                (default false)
//	OTEL_TRACING_ENABLED    gate tracing  (default: value of OTEL_ENABLED)
//	OTEL_METRICS_ENABLED    gate metrics  (default: value of OTEL_ENABLED)
//	OTEL_EXPORTER_OTLP_ENDPOINT   collector OTLP/HTTP endpoint
//
// Every instrument DECLARED HERE is OBSERVABLE (async): a single registered
// callback reads the live counter snapshots at collection time. This matches
// the expvar.Func "computed-on-read" semantics exactly and adds zero cost to
// the hot path — the callback only runs when the collector reads, and only
// when metrics are enabled.
//
// # THE PROCESS IS NOT FREE, EVEN THOUGH THESE INSTRUMENTS ARE (1.12.4)
//
// The sentence above is about snowplow's OWN instruments and is true of
// them. It is NOT true of the process, and the difference is what the
// serving path experiences. Enabling metrics costs three SYNCHRONOUS,
// UNSAMPLED histogram Records on EVERY non-filtered request:
//
//   - otelhttp captures the GLOBAL MeterProvider at handler-construction
//     time (its newConfig defaults MeterProvider to otel.GetMeterProvider()).
//   - Setup below calls otel.SetMeterProvider BEFORE main constructs the
//     otelhttp handler, so the REAL provider is what gets captured — not
//     the no-op.
//   - Each request then performs http.server.request.body.size,
//     .response.body.size and .request.duration Records, each with a
//     freshly built attribute set.
//
// Metrics do NOT honour the trace sampler, so every request pays this even
// at 5% trace sampling. OTEL_METRICS_ENABLED="false" is the rollback and
// needs no binary change. F11 pins the contract; the real acceptance is the
// Phase-6 bench at SCALE=50000.
//
// The compensation is worth claiming: those three histograms include
// http.server.request.duration, a per-route server-side latency
// distribution for /call — unsampled, complete, and the FIRST server-side
// latency distribution snowplow has ever exported.
//
// Not a 1.12.4 delta: otelhttp already built its metric attribute set per
// request in 1.12.3 under the no-op meter. That cost is already shipped.
package metrics

import (
	"context"

	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/otelresource"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	// EnvEnabled is the master OTel switch. Default false. It supplies the
	// default for EnvMetricsEnabled so a single flag turns the whole pipeline
	// on.
	EnvEnabled = "OTEL_ENABLED"

	// EnvMetricsEnabled gates the entire OTLP metrics pipeline. It DEFAULTS to
	// the value of EnvEnabled (OTEL_ENABLED) when unset; a per-signal value
	// overrides the master. Default (with the master unset) is false.
	EnvMetricsEnabled = "OTEL_METRICS_ENABLED"

	// EnvOTLPEndpoint is the OTLP/HTTP collector endpoint, shared with the
	// trace pipeline via the standard OTEL_EXPORTER_OTLP_ENDPOINT contract.
	EnvOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"

	meterName = "github.com/krateo-platformops/snowplow"
)

// ShutdownFunc flushes and stops the metrics pipeline. Always non-nil; a
// no-op when metrics were not enabled.
type ShutdownFunc func(context.Context) error

// metricsEnabled resolves the metrics gate from the canonical contract:
// OTEL_METRICS_ENABLED, defaulting to the value of the OTEL_ENABLED master
// when the per-signal var is unset. With both unset it is false, preserving
// the default-off byte-identical guarantee.
func metricsEnabled() bool {
	return env.Bool(EnvMetricsEnabled, env.Bool(EnvEnabled, false))
}

// Setup wires the OTLP/HTTP metric exporter + a periodic-reader
// MeterProvider when metrics resolve to enabled (OTEL_METRICS_ENABLED,
// defaulting to OTEL_ENABLED), registers it as the global MeterProvider,
// and registers the observable instruments that mirror snowplow's expvar
// counters.
//
// No-op (registers nothing) when the gate is off.
func Setup(ctx context.Context, build string) (ShutdownFunc, error) {
	noop := func(context.Context) error { return nil }

	if !metricsEnabled() {
		return noop, nil
	}

	opts := []otlpmetrichttp.Option{}
	if ep := env.String(EnvOTLPEndpoint, ""); ep != "" {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(otelresource.SignalEndpointURL(ep, otelresource.MetricsPath)))
	}

	exp, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return noop, err
	}

	// 1.12.4 — the shared resource (internal/otelresource). Identical by
	// construction to the trace and log pipelines' resource, including
	// the downward-API pod identity that keeps the collector's
	// k8sattributes association deterministic.
	res, err := otelresource.Build(ctx, build, kubeutil.ServiceAccountNamespace)
	if err != nil {
		return noop, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	if err := registerInstruments(mp.Meter(meterName), build); err != nil {
		// Best-effort: shut the provider down so we don't leak the reader
		// goroutine, then surface the error.
		_ = mp.Shutdown(ctx)
		return noop, err
	}

	return mp.Shutdown, nil
}

// registerInstruments declares every observable instrument and a single
// callback that reads the live counter snapshots. Instrument names mirror
// the expvar keys (with the conventional `_total` suffix on monotonic
// counters) so dashboards can correlate the two surfaces.
func registerInstruments(m metric.Meter, build string) error {
	// --- cache: apiserver fallthrough + assertion violations ---
	fallthroughTotal, err := m.Int64ObservableCounter(
		"snowplow_apiserver_fallthrough_total",
		metric.WithDescription("Cumulative apiserver fall-throughs (cache miss path)."),
	)
	if err != nil {
		return err
	}
	assertionViolations, err := m.Int64ObservableCounter(
		"snowplow_assertion_violations_total",
		metric.WithDescription("Cumulative read-path-scoped assertion violations."),
	)
	if err != nil {
		return err
	}

	// --- cache: RBAC snapshot publish sequence ---
	rbacPublishSeq, err := m.Int64ObservableCounter(
		"snowplow_rbac_publish_seq",
		metric.WithDescription("RBAC snapshot publish sequence number."),
	)
	if err != nil {
		return err
	}
	// --- cache: #691 stuck-rebuild detectors ---
	rbacRebuildPanics, err := m.Int64ObservableCounter(
		"snowplow_rbac_rebuild_panics_total",
		metric.WithDescription("Recovered panics of the RBAC snapshot rebuild (#691). Each one leaves the previous snapshot in force; a rebuild that panics on every attempt never publishes again. Warn on any increase."),
	)
	if err != nil {
		return err
	}
	rbacUnpublishedAge, err := m.Float64ObservableGauge(
		"snowplow_rbac_unpublished_event_age_seconds",
		metric.WithDescription("Age in seconds of the OLDEST RBAC event not yet reflected in a completed snapshot rebuild; 0 when everything seen has been rebuilt (#691). > 0 and growing = a STUCK rebuild (revokes unpublished, in-process access kept); 0 = healthy or quiet. Page when > 60 for 5m."),
	)
	if err != nil {
		return err
	}
	rbacPublishAge, err := m.Float64ObservableGauge(
		"snowplow_rbac_last_successful_publish_age_seconds",
		metric.WithDescription("Seconds since the last successful RBAC snapshot publish; 0 before the first (#691). Context only: it also grows on a quiet cluster, so it is not an alert on its own."),
	)
	if err != nil {
		return err
	}
	// --- cache: #687 RBAC rebuild post-release pickups ---
	rbacRebuildPickups, err := m.Int64ObservableCounter(
		"snowplow_rbac_rebuild_post_release_pickups_total",
		metric.WithDescription("RBAC snapshot rebuilds the #687 post-release re-check picked up: an RBAC event set dirty while the rebuild loop still held its slot, the scheduler returned, and the loop re-acquired the slot to publish it. Before #687 each of these waited for the next RBAC event (a revoke could stay unpublished)."),
	)
	if err != nil {
		return err
	}

	// --- cache: per-subject RBAC sub-generation (#247) ---
	// rbacPublishSeq above is GLOBAL — one bump per snapshot publish, one link
	// too early on the chain publish -> per-subject bump -> key minted ->
	// resident excess (each link necessary for the next, sufficient for none;
	// see internal/cache/rbac_subgen.go). A publish count therefore cannot say
	// whether any key rotated. bumps_total closes that second link: whether
	// keys rotate at all, and how fast. subjects_tracked is its one-time blast
	// radius denominator (12k bumps on 3 subjects vs on 12k subjects are
	// different systems) — a saturating high-water mark, never a rate.
	rbacSubGenBumps, err := m.Int64ObservableCounter(
		"snowplow_rbac_subgen_bumps_total",
		metric.WithDescription("Cumulative per-subject RBAC sub-generation bumps."),
	)
	if err != nil {
		return err
	}
	rbacSubGenSubjects, err := m.Int64ObservableGauge(
		"snowplow_rbac_subgen_subjects_tracked",
		metric.WithDescription("Number of distinct subjects with an RBAC sub-generation counter."),
	)
	if err != nil {
		return err
	}

	// --- #260 change-4: RBAC sub-gen churn attribution on OTLP. Decision-critical
	// for the #253/#258/#259 keying call (correlate rotation drivers vs ClickStack
	// cold-fills on one time axis). Bounded cardinality: 4 GVRs / 6 sources / scalars.
	rbacReestablished, err := m.Int64ObservableCounter(
		"snowplow_rbac_reflector_reestablished_total",
		metric.WithDescription("Reflector re-establishments (a fresh watchlist/list re-delivery) per typed-RBAC GVR — the sub-gen churn driver. Excludes the #263-tagged forced-verification LIST by construction."))
	if err != nil {
		return err
	}
	rbacWatchError, err := m.Int64ObservableCounter(
		"snowplow_rbac_reflector_watch_errors_total",
		metric.WithDescription("Reflector ListAndWatch errors per typed-RBAC GVR — the watch DISRUPTION rate. Its relationship to reestablished_total tells watch-driven RBAC churn (fixable) from apiserver-inherent churn (a 410/compaction re-list with no watch error)."))
	if err != nil {
		return err
	}
	rbacBumpsBySource, err := m.Int64ObservableCounter(
		"snowplow_rbac_subgen_bumps_by_source_total",
		metric.WithDescription("RBAC sub-gen bumps by source (binding/role add/update/delete). Promoted from expvar so the keying decision can correlate drivers on ClickStack."))
	if err != nil {
		return err
	}
	rbacRoleNoop, err := m.Int64ObservableCounter(
		"snowplow_rbac_role_noop_updates_total",
		metric.WithDescription("Role UPDATE events that were relist fan-out (same resourceVersion) — a re-establishment redelivering roles, not a real change."))
	if err != nil {
		return err
	}
	rbacRoleSemanticNoop, err := m.Int64ObservableCounter(
		"snowplow_rbac_role_semantic_noop_updates_total",
		metric.WithDescription("Role UPDATE events whose rules were unchanged, order-insensitive (#257's skip predicate) — label-only role churn."))
	if err != nil {
		return err
	}
	rbacBindingUidChanged, err := m.Int64ObservableCounter(
		"snowplow_rbac_binding_uid_changed_updates_total",
		metric.WithDescription("Binding UPDATE events whose metadata.uid changed (delete+recreate seen via relist) — excluded from semantic_noop so a real rotation is not credited as a no-op."))
	if err != nil {
		return err
	}

	// --- cache: binding UPDATE events that rotated keys for nothing (#247) ---
	// onBindingUpdate bumps every subject on every UPDATE with no old-vs-new
	// comparison, and a watch re-establishment redelivers every binding as an
	// OnUpdate. noop (same resourceVersion) isolates that relist fan-out;
	// semantic_noop (same subjects + roleRef at any RV) is the superset. Their
	// DIFFERENCE — rewrites that touched nothing RBAC-relevant — is what decides
	// whether a fix keys on resourceVersion or on semantics.
	bindingNoopUpdates, err := m.Int64ObservableCounter(
		"snowplow_rbac_binding_noop_updates_total",
		metric.WithDescription("Cumulative (Cluster)RoleBinding UPDATE events redelivering the same resourceVersion."),
	)
	if err != nil {
		return err
	}
	bindingSemanticNoopUpdates, err := m.Int64ObservableCounter(
		"snowplow_rbac_binding_semantic_noop_updates_total",
		metric.WithDescription("Cumulative (Cluster)RoleBinding UPDATE events with unchanged subjects and roleRef."),
	)
	if err != nil {
		return err
	}

	// --- cache: registered GVR count ---
	registeredGVRs, err := m.Int64ObservableGauge(
		"snowplow_plurals_registered_gvrs",
		metric.WithDescription("Number of GVRs with a registered informer."),
	)
	if err != nil {
		return err
	}

	// --- cache: prewarm completion boundary ---
	prewarmDone, err := m.Int64ObservableGauge(
		"snowplow_prewarm_complete",
		metric.WithDescription("1 once Phase1Done has flipped, else 0."),
	)
	if err != nil {
		return err
	}
	prewarmElapsed, err := m.Int64ObservableGauge(
		"snowplow_prewarm_complete_elapsed_ms",
		metric.WithDescription("Process-start to Phase1Done wall-clock (ms); -1 until flip."),
	)
	if err != nil {
		return err
	}

	// --- rbac: snapshot authz memo ---
	memoHits, err := m.Int64ObservableCounter("snowplow_authz_memo_hits",
		metric.WithDescription("Snapshot authz memo hits."))
	if err != nil {
		return err
	}
	memoMisses, err := m.Int64ObservableCounter("snowplow_authz_memo_misses",
		metric.WithDescription("Snapshot authz memo misses."))
	if err != nil {
		return err
	}
	memoSwaps, err := m.Int64ObservableCounter("snowplow_authz_memo_swaps",
		metric.WithDescription("Snapshot authz memo generation swaps."))
	if err != nil {
		return err
	}
	memoRefused, err := m.Int64ObservableCounter("snowplow_authz_memo_refused",
		metric.WithDescription("Snapshot authz memo cap-breach refused inserts."))
	if err != nil {
		return err
	}
	memoDenyUncached, err := m.Int64ObservableCounter("snowplow_authz_memo_deny_uncached_total",
		metric.WithDescription("Deny verdicts not cached by the memo."))
	if err != nil {
		return err
	}
	memoEntries, err := m.Int64ObservableGauge("snowplow_authz_memo_entries",
		metric.WithDescription("Live entry count of the current memo shard."))
	if err != nil {
		return err
	}

	// --- prewarm engine: the unified walk/seed worker (production path) ---
	prewarmEngEnqueued, err := m.Int64ObservableCounter("snowplow_prewarm_engine_enqueued_total",
		metric.WithDescription("Cumulative enqueueScope calls (every enqueue, even dedup-coalesced)."))
	if err != nil {
		return err
	}
	prewarmEngProcessed, err := m.Int64ObservableCounter("snowplow_prewarm_engine_processed_total",
		metric.WithDescription("Prewarm-engine scopes fully processed by the worker."))
	if err != nil {
		return err
	}
	prewarmEngYield, err := m.Int64ObservableCounter("snowplow_prewarm_engine_yield_total",
		metric.WithDescription("Prewarm-engine worker parked for a customer /call (customer-priority yield)."))
	if err != nil {
		return err
	}
	prewarmEngPending, err := m.Int64ObservableGauge("snowplow_prewarm_engine_pending_depth",
		metric.WithDescription("Live prewarm-engine pending-scope depth; 0 once the worker drains."))
	if err != nil {
		return err
	}

	// --- #368: v7 wildcard digest-collision probe (the 4th dark detector). ---
	shadowWildcardDigestCollision, err := m.Int64ObservableCounter("snowplow_v7_shadow_wildcard_digest_collision_total",
		metric.WithDescription("#368 DETECTOR (zero-means-safe): shareable, UNGATED wildcard cells whose digest failed to distinguish two different-access identities (evaluator-truth) — a Step-3 share leak. ALERT on > 0. 0 today because every ClassWildcard is gated; certifies the real encoding once the enumerate projection ungates."))
	if err != nil {
		return err
	}
	shadowWildcardDigestObserved, err := m.Int64ObservableCounter("snowplow_v7_shadow_wildcard_digest_observed_total",
		metric.WithDescription("#368 denominator: distinct (cell,digest) wildcard pairs observed with >=2 identities (shareable+ungated). Certified = collision_total == 0 over observed_total > 0; observed_total == 0 = not-yet-exercised, NOT certified."))
	if err != nil {
		return err
	}
	shadowWildcardDigestEvicted, err := m.Int64ObservableCounter("snowplow_v7_shadow_wildcard_digest_evicted_total",
		metric.WithDescription("#368 cap-eviction counter: dropped (cell,digest) observations (LRU + identity/coord-cap truncations). A dropped observation could MISS a collision, so certification requires evicted_total == 0 too: certified = collision==0 AND observed>0 AND evicted==0. Non-zero → raise caps/shard and re-measure."))
	if err != nil {
		return err
	}

	// --- prewarm phase-1: apiRef pagination coverage ---
	phase1UnitsPlanned, err := m.Int64ObservableGauge("snowplow_phase1_units_planned",
		metric.WithDescription("widgetContent cells the apiRef pagination walk planned to seed."))
	if err != nil {
		return err
	}
	phase1UnitsSeeded, err := m.Int64ObservableGauge("snowplow_phase1_units_seeded",
		metric.WithDescription("Page cells handed to populateWidgetContentL1 with a non-nil envelope."))
	if err != nil {
		return err
	}
	phase1ApiRefPages, err := m.Int64ObservableCounter("snowplow_phase1_apiref_pages_total",
		metric.WithDescription("Extra apiRef pages (page 2..N) resolved across all paginated widgets."))
	if err != nil {
		return err
	}
	phase1EligibleNoContinue, err := m.Int64ObservableCounter("snowplow_phase1_eligible_no_continue_total",
		metric.WithDescription("Distinct eligible widgets whose page-1 resolve produced no continuation."))
	if err != nil {
		return err
	}

	// --- prewarm phase-1: boot-walk fan-out ---
	phase1WalkZeroChildren, err := m.Int64ObservableCounter("snowplow_phase1_walk_zero_children_total",
		metric.WithDescription("Boot-walk observations that found zero children."))
	if err != nil {
		return err
	}
	phase1WalkObservations, err := m.Int64ObservableCounter("snowplow_phase1_walk_observations_total",
		metric.WithDescription("Total boot-walk children-count observations (zero-children denominator)."))
	if err != nil {
		return err
	}

	// --- prewarm phase-1: per-target seed outcomes ---
	phase1SeedResolves, err := m.Int64ObservableCounter("snowplow_phase1_bindingset_seed_resolves_total",
		metric.WithDescription("Per-binding-target phase-1 seed resolves."))
	if err != nil {
		return err
	}
	phase1SeedFailures, err := m.Int64ObservableCounter("snowplow_phase1_bindingset_seed_failures_total",
		metric.WithDescription("Grand-total phase-1 seed failures (= rbac_deny + operational)."))
	if err != nil {
		return err
	}
	phase1SeedRBACDeny, err := m.Int64ObservableCounter("snowplow_phase1_seed_rbac_deny_total",
		metric.WithDescription("EXPECTED narrow-RBAC seed denies (403/401); cohort genuinely can't read the target."))
	if err != nil {
		return err
	}
	phase1SeedOpFail, err := m.Int64ObservableCounter("snowplow_phase1_seed_operational_fail_total",
		metric.WithDescription("UNEXPECTED seed failures (ctx timeout/cancel, 5xx, transport, panic); should be 0."))
	if err != nil {
		return err
	}

	// --- 1.12.6 C7: tag-derived families (crd_discovery, refresh_broadcaster,
	// refresher). ONE loop: the instrument set, names, kinds and descriptions
	// all come from the snapshot structs' `stat`/`kind`/`desc` tags
	// (internal/cache/stats_by_tag.go), so a counter added there reaches OTLP
	// with no edit here. Three drifts shipped in 1.12.6 before this loop
	// existed, every one in a hand-written field list this replaces; the C12
	// broadcaster instruments were even created and observed but never
	// registered with the callback, so the SDK dropped every observation.
	// TestC7_OTLP_EveryDerivedStatLeavesTheProcess pins all of it.
	derived, derivedObservables, err := newDerivedFamilies(m)
	if err != nil {
		return err
	}

	// --- #354 P3: the invalidation→fresh window distribution ---
	// The one SYNCHRONOUS instrument here: a real histogram, not an observable
	// mirror. The refresher records each dirty→fresh sample (ms) into it through
	// the hook installed below (cache cannot import this package). No
	// attributes: one series per pod. The explicit bounds bracket the 1 s
	// north-star and the 10 s AC-98.12 SLA. The windowed p95/max gauges ride the
	// refresher's tagged family (dirty_to_fresh_ms_p95/_max) for /debug/vars
	// parity and alerting without a histogram query.
	dirtyToFresh, err := m.Int64Histogram("snowplow_refresher_dirty_to_fresh_ms",
		metric.WithDescription("Invalidation-to-fresh window of a resident L1 cell (ms): first dirty-mark to the refresher's accepted, un-remarked re-Put (#354)."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(cache.DirtyToFreshBoundsMS...),
	)
	if err != nil {
		return err
	}
	cache.SetDirtyToFreshRecorder(func(ms int64) { dirtyToFresh.Record(context.Background(), ms) })

	// --- discovery: SA-discovery client (one counter keyed by stat) ---
	saDiscovery, err := m.Int64ObservableCounter("snowplow_sa_discovery",
		metric.WithDescription("SA-discovery client counters, labelled by stat."))
	if err != nil {
		return err
	}

	// --- #608: refresh failures by closed-set (err_class, stage). Outside the
	// C7 snowplow_refresher_ family (that prefix is reserved for tag-derived
	// stats). Alert on err_class="unclassified" > 0 and on
	// err_class="forbidden",stage="validate" > 0 — the #608 signature.
	refreshFailedByClass, err := m.Int64ObservableCounter("snowplow_refresh_failed",
		metric.WithDescription("Background refresh failures by closed-set err_class and resolve stage (#608)."))
	if err != nil {
		return err
	}

	// --- discovery: compiled-CRD-schema memo (one counter keyed by stat) ---
	crdSchemaMemo, err := m.Int64ObservableCounter("snowplow_crd_schema_memo",
		metric.WithDescription("Compiled-CRD-schema memo counters, labelled by stat."))
	if err != nil {
		return err
	}

	// --- #608: refresher impersonation capability (one counter keyed by stat)
	// and the startup probe's kind-wide answers (one gauge keyed by resource).
	// impersonation_fallback_sa is non-zero while a cohort's group is NOT
	// allowlisted — it reads the defect's condition, not a health zero.
	impersonation, err := m.Int64ObservableCounter("snowplow_impersonation",
		metric.WithDescription("Refresher impersonation capability counters (#608), labelled by stat: impersonation_permitted, impersonation_fallback_sa, impersonation_capability_check_error, reviews, memo_*."))
	if err != nil {
		return err
	}
	impersonationProbe, err := m.Int64ObservableGauge("snowplow_impersonation_probe",
		metric.WithDescription("Startup impersonation probe (#608): 1 when snowplow may impersonate every name of the resource, else 0; labelled by stat (users, groups_all_names, serviceaccounts_all_names)."))
	if err != nil {
		return err
	}

	// --- upstream health: aggregate controller + webhook gauges ---
	upstreamControllers, err := m.Int64ObservableGauge("snowplow_upstream_controllers",
		metric.WithDescription("Auto-discovered upstream controllers, labelled by health (healthy/unhealthy)."))
	if err != nil {
		return err
	}
	upstreamWebhooks, err := m.Int64ObservableGauge("snowplow_upstream_webhooks",
		metric.WithDescription("Discovered admission webhooks, labelled by policy (total/fail)."))
	if err != nil {
		return err
	}

	// --- dispatch: L1 resolved-output lookups.
	//
	// 1.12.4 WIDENED IN PLACE. This instrument kept its name but gained
	// the `class` and `gvr` attributes and the `seed_hit` outcome; the
	// aggregate-only declaration it replaces observed a single
	// process-wide hit/miss pair. Declared below with the other 1.12.4
	// instruments as dispatchL1Cells.

	// --- RAFullList: cheap Go-slice serve outcomes + index drift canary ---
	raFullListServe, err := m.Int64ObservableCounter("snowplow_ra_full_list_serve",
		metric.WithDescription("RAFullList serve-outcome counters, labelled by outcome."))
	if err != nil {
		return err
	}
	bindingsDeltaSkipped, err := m.Int64ObservableCounter("snowplow_bindings_by_gvr_delta_skipped_non_typed",
		metric.WithDescription("Delta-event objects neither typed nor convertible, DROPPED (index drift canary); should be 0."))
	if err != nil {
		return err
	}

	// --- #624: informer serve reads failed CLOSED on an unrenderable store slot ---
	// Hand-wired twin of the expvar pair snowplow_informer_unrenderable_store_items_
	// {total,cells} (no expvar->OTLP bridge). One series per (site, gvr) cell plus
	// nothing when healthy: a non-zero value means a store holds a shape the serve
	// paths cannot render and every such read fell through to the apiserver.
	unrenderableStoreItems, err := m.Int64ObservableCounter("snowplow_informer_unrenderable_store_items_total",
		metric.WithDescription("Informer serve reads (site=list_decoded|list_raw|get) failed CLOSED to the apiserver because a store slot could not be rendered, by gvr; should be 0. #624."))
	if err != nil {
		return err
	}

	// --- #647: RESTAction units the resident enumeration (RBAC-shift reseed,
	// learned-class seed, #258 rotated filter) DROPPED for want of a target GVR.
	// Hand-wired twin of expvar snowplow_prewarm_resident_units_dropped_total.
	// The full closed reason set is emitted (zeros included). get_failed is the
	// #647 signature (one per RESTAction per run while the lookup had no
	// identity) and is 0 when healthy; no_static_target is expected and stable.
	residentUnitsDropped, err := m.Int64ObservableCounter("snowplow_prewarm_resident_units_dropped_total",
		metric.WithDescription("RESTAction units the resident enumeration dropped (reseeded for nobody), by reason=get_failed|decode_failed|no_static_target; get_failed and decode_failed should be 0. #647."))
	if err != nil {
		return err
	}

	// ===== 1.12.4 (design §3.3) — the instrument gap =====
	//
	// Everything below already existed as a live counter inside the
	// process, reachable only through expvar at the JWT-gated
	// /debug/vars, or (worse) only through an INFO log line the chart's
	// LOG_LEVEL=warn suppresses. None of it is a new measurement; all of
	// it is an existing measurement finally leaving the pod.

	// --- A-1 UAF Put declines (1.12.3). Non-zero is CORRECT in 1.12.x:
	// the A-1 cross-tenant mitigation is active and declining the Puts.
	// They return to 0 in 1.13.0 when the v7 key re-enables them. The
	// dashboard panel MUST carry that annotation or the series reads as
	// an error.
	uafRestactionsDeclined, err := m.Int64ObservableCounter(
		"snowplow_restactions_uaf_put_declined_total",
		metric.WithDescription("restactions-class L1 Puts declined because the RESTAction declares a userAccessFilter (A-1 mitigation). Non-zero is CORRECT in 1.12.x."))
	if err != nil {
		return err
	}
	uafWidgetsDeclined, err := m.Int64ObservableCounter(
		"snowplow_widgets_uaf_put_declined_total",
		metric.WithDescription("widgets-class L1 Puts declined under the A-1 UAF mitigation. Non-zero is CORRECT in 1.12.x."))
	if err != nil {
		return err
	}
	uafRAFullListCached, err := m.Int64ObservableCounter(
		"snowplow_ra_full_list_uaf_cached_total",
		metric.WithDescription("raFullList cells stored holding a userAccessFilter-narrowed body (#592; replaces snowplow_ra_full_list_uaf_bypass_total, which #592 pinned at zero)."))
	if err != nil {
		return err
	}

	// --- dispatch L1, WIDENED. The aggregate above collapses every
	// class and GVR into one hit/miss pair, in which a widgets hit-rate
	// of 99% and a widgetContent hit-rate of 0% average to a
	// healthy-looking number while the portal is broken.
	dispatchL1Cells, err := m.Int64ObservableCounter(
		"snowplow_dispatch_l1_lookups_total",
		metric.WithDescription("Dispatch-L1 resolved-output lookups by class, gvr and outcome (hit/miss/seed_hit)."))
	if err != nil {
		return err
	}
	seedAttributableHits, err := m.Int64ObservableCounter(
		"snowplow_resolved_cache_hits_seed_attributable_total",
		metric.WithDescription("Resolved-cache hits served from a boot-seeded cell — did the boot seed warm anything a browser actually hit."))
	if err != nil {
		return err
	}

	// --- the fall-through family, per cell. The grand total was already
	// instrument #1; what was missing is the path|gvr|reason breakdown,
	// which is the half that carries the diagnostic value. Capped on the
	// observation path (see cache.FallthroughCellsSnapshot).
	fallthroughCells, err := m.Int64ObservableCounter(
		"snowplow_apiserver_fallthrough_cells_total",
		metric.WithDescription("GENUINE apiserver fall-throughs by path, gvr and reason."))
	if err != nil {
		return err
	}
	diagnosticTotal, err := m.Int64ObservableCounter(
		"snowplow_cache_diagnostic_total",
		metric.WithDescription("Cache-diagnostic observations that reach NO apiserver (1.12.4 reclassification). Expect this to dwarf the fall-through total ~8:1."))
	if err != nil {
		return err
	}
	diagnosticCells, err := m.Int64ObservableCounter(
		"snowplow_cache_diagnostic_cells_total",
		metric.WithDescription("Cache-diagnostic observations by path, gvr and reason."))
	if err != nil {
		return err
	}
	seriesTruncated, err := m.Int64ObservableCounter(
		"snowplow_metrics_series_truncated_total",
		metric.WithDescription("Cell series folded into gvr=__other__ by the OTLP cardinality cap, by family. Non-zero means a cardinality regression — alert."))
	if err != nil {
		return err
	}

	// --- boot SLI. Log-only before 1.12.4: an unexported expvar.Int
	// with no accessor, so nothing could alert on it. A healthy boot
	// leaves it 0.
	readyzBackstop, err := m.Int64ObservableCounter(
		"snowplow_readyz_backstop_fired_total",
		metric.WithDescription("Boots whose /readyz flipped Ready via the C2 backstop instead of firstNav-complete — a FAILED-but-serving boot. Alert on > 0."))
	if err != nil {
		return err
	}

	// --- #397: phase-1 deadline-release DETECTOR. 0 or 1 per process: 1 iff
	// readiness was released by the seed ctx / PHASE1_TIMEOUT deadline before
	// the first-nav latch fired (prewarm.phase1.readiness_exit outcome=deadline).
	// Hand-wired: there is no expvar->OTLP bridge.
	phase1DeadlineReleased, err := m.Int64ObservableCounter(
		"snowplow_phase1_deadline_released_total",
		metric.WithDescription("1 iff this process's /readyz was released by the boot-seed / PHASE1_TIMEOUT deadline before the first-nav latch fired (nav widgets still unseeded at Ready); 0 on a latch-released boot. Healthy = 0. #397."))
	if err != nil {
		return err
	}

	// --- #736: WHICH outcome released readiness, by outcome (the closed set
	// dispatchers.Phase1ReadinessExitOutcomes). 1 at the releasing outcome, 0
	// at every other. The F5 backstop counter has no outcome and the #397
	// detector is deadline-only; none-configured and seed_returned moved
	// nothing. Alert on any outcome other than latch (none-configured only
	// matters with the cache on).
	phase1ExitByOutcome, err := m.Int64ObservableCounter(
		"snowplow_phase1_readiness_exit_total",
		metric.WithDescription("Which outcome released this process's /readyz, by outcome {latch, deadline, boot_error, seed_panic, seed_returned, boot_aborted, none-configured}: 1 at the releasing outcome, 0 elsewhere. Healthy = latch. Any other outcome = a Ready-degraded or nothing-to-warm boot — alert. #736."))
	if err != nil {
		return err
	}

	// --- L1 store occupancy + lifetime, keyed by stat. Computed by
	// Stats() every N seconds since forever and emitted only into an
	// INFO line the production LOG_LEVEL=warn discards.
	resolvedCache, err := m.Int64ObservableGauge(
		"snowplow_resolved_cache",
		metric.WithDescription("Resolved-output L1 store occupancy, ceilings, hit/miss/store and evict counters, labelled by stat."))
	if err != nil {
		return err
	}

	// --- 1.12.5 / #187: the dependency tracker + informer->DepTracker
	// bridge. DELETE-driven invalidation lives here, and until 1.12.5 its
	// counters went only into an INFO line the production LOG_LEVEL=warn
	// discards — which is why #187 could not be answered from the live pod.
	// Alert on stat=delete_worker_panics_total > 0 (an eviction was LOST)
	// and on stat=dropped_cap > 0 (dep edges are being silently dropped).
	depsStats, err := m.Int64ObservableGauge(
		"snowplow_deps",
		metric.WithDescription("Dependency-tracker and informer-bridge counters, labelled by stat: dep-record occupancy, evict/dirty-mark totals, and the DELETE eviction worker's queue depth, inline-fallback and panic counts."))
	if err != nil {
		return err
	}

	// --- #239: dirty-mark ATTRIBUTION. snowplow_deps{stat=dirty_mark_total} is
	// the grand total; these decompose it so a rise can be told from benign
	// fan-out — {path,cause,class}, the fan-out denominator, and the pre-dedup
	// submit source. Bounded cardinality (~15 buckets x 3 sources).
	dirtyMarkAttributed, err := m.Int64ObservableCounter(
		"snowplow_deps_dirty_mark_attributed_total",
		metric.WithDescription("Dirty-marks by path (object_event/type_event), cause and entry class. Their sum equals snowplow_deps{stat=dirty_mark_total}."))
	if err != nil {
		return err
	}
	dirtyMarkEvents, err := m.Int64ObservableCounter(
		"snowplow_deps_dirty_mark_events_total",
		metric.WithDescription("Object/type events that dirty-marked >=1 entry — the fan-out denominator (mean fan-out = dirty_mark_total / this)."))
	if err != nil {
		return err
	}
	dirtyMarkSubmits, err := m.Int64ObservableCounter(
		"snowplow_deps_dirty_mark_submits_total",
		metric.WithDescription("Dep-event submissions by source (watch/relist_bridge/reconcile), counted PRE-dedup — which mechanism drives the queue."))
	if err != nil {
		return err
	}
	dirtyMarkUnattributed, err := m.Int64ObservableCounter(
		"snowplow_deps_dirty_mark_unattributed_total",
		metric.WithDescription("Dirty-marks whose {path,cause,class} combo was not pre-registered — a classifier bug. Alert on > 0."))
	if err != nil {
		return err
	}
	// #479 1b / #603 PR-0 — the same dirty-marks split by the ENTRY CLASS of the
	// marked key (restactions/widgets/widgetContent/apistage/raFullList, plus
	// unknown and the unregistered guard). Closed label set; Σ == dirty_mark_total.
	dirtyMarkByEntryClass, err := m.Int64ObservableCounter(
		"snowplow_deps_dirty_mark_by_entry_class_total",
		metric.WithDescription("Dirty-marks by the entry class of the marked key (restactions/widgets/widgetContent/apistage/raFullList; unknown = class not learned; unregistered = guard, alert on > 0). Their sum equals snowplow_deps{stat=dirty_mark_total}."))
	if err != nil {
		return err
	}

	// --- informer servability, the leading indicator for the
	// informer-fallthrough-not-synced cell.
	informerServable, err := m.Int64ObservableGauge(
		"snowplow_informer_servable",
		metric.WithDescription("Informer counts by state (registered/synced/servable/watch_broken/confirmed). watch_broken > 0 is the stale-delete latch."))
	if err != nil {
		return err
	}

	// --- 1.12.5 / #187: informer FRESHNESS, the half servability does not
	// express. HasSynced says an informer finished its initial LIST once and
	// stays true forever; it cannot say whether the indexer still matches the
	// cluster. Aggregate, not per-GVR: ~169 informers x 3 gauges would be a
	// ~500-series cardinality regression of exactly the class 1.12.4 had to
	// cap. The per-GVR rows stay on /debug/servable.
	informerFreshness, err := m.Int64ObservableGauge(
		"snowplow_informer_freshness",
		metric.WithDescription("Informer freshness aggregates, labelled by stat: total indexed objects, GVRs that have never received an event, the oldest last-event age, and GVRs stale over an hour."))
	if err != nil {
		return err
	}

	// --- #233: unparseable-CA delegation DETECTOR. Non-zero = an endpoint's CA
	// bundle is present + endpoint-owned-CA shape but UNPARSEABLE, so snowplow
	// delegated to plumbing (which almost certainly fails x509:unknown-authority).
	// A monotonic rate (the WARN is one-shot; this counter is never bounded) —
	// alertable, which is why it is OTLP-native, not expvar-only (#311, opposite
	// to an expected-value counter that /debug/vars suffices for).
	unparseableCADelegations, err := m.Int64ObservableCounter(
		"snowplow_unparseable_ca_delegations_total",
		metric.WithDescription("Count of TLS-client construction delegations caused by an UNPARSEABLE endpoint-owned CA bundle (neither raw PEM nor (double-)base64 PEM). Non-zero is a misconfiguration DETECTOR: the operator gave a CA snowplow cannot parse, so the dial delegates to plumbing and almost certainly fails x509:unknown-authority — fix the CA bundle. #233."))
	if err != nil {
		return err
	}

	// #311/#293: malformed-dial skip detector, per BOUNDED reason. Promoted from
	// the expvar family so a mis-interpolation / garbage-path / jq-error spike is
	// alertable on ClickStack. One counter + a `reason` attribute drawn from a
	// fixed code-defined enum (cardinality bounded by construction — the #260
	// bumps_by_source pattern).
	malformedDialSkipped, err := m.Int64ObservableCounter(
		"snowplow_malformed_dial_skipped_total",
		metric.WithDescription("Malformed single-object apiserver dials SKIPPED by the #288/#293/#302 guard, by bounded reason {empty_interp, unrendered_template, jq_path_error, jq_payload_error, jq_header_error}. Non-zero is a DETECTOR: an upstream render defect was caught (collapsed interpolation / unrendered ${ template / jq error in path|payload|header) — fix the RESTAction or the key-minting. #311."))
	if err != nil {
		return err
	}

	// --- #375: dep-generation guard DETECTOR. Non-zero = an ACCEPTED gen-guarded L1
	// Put whose resolve ctx carried no dep-gen sink (a resolve entry outside
	// WithL1KeyContext / WithDepGenSink) — the PUT-THEN-REMARK guard could not tell
	// whether a dependency moved during the resolve, so the Put was fail-fresh remarked.
	// Expected 0; a detector, so OTLP-native and alertable (#311), not expvar-only.
	unguardedPutTotal, err := m.Int64ObservableCounter(
		"snowplow_deps_unguarded_put_total",
		metric.WithDescription("Count of ACCEPTED generation-guarded L1 Puts (PutIfGen/ReplaceIfGen/PutRAFullListIfGen) whose resolve context carried NO dependency-generation sink — a resolve entry outside the #375 PUT-THEN-REMARK guard. Each was remarked once (fail-fresh). Expected 0; non-zero is #375 drift (a resolve entry that does not install the sink). #375."))
	if err != nil {
		return err
	}

	// --- #408: moved PUT-THEN-REMARKs, by carrier (boot = the pre-readyz boot plain Put,
	// guarded = the IfGen methods). Each is one refresher enqueue. A diagnostic sizing
	// input for the boot remark cost (#419), OTLP so it is readable on the cluster.
	movedRemarkTotal, err := m.Int64ObservableCounter(
		"snowplow_deps_moved_remark_total",
		metric.WithDescription("Count of #375 PUT-THEN-REMARKs fired because a dependency the resolve recorded moved during it (one refresher enqueue each), by carrier: boot = the pre-readyz boot seed plain Put (PutThenRemark, #408), guarded = the generation-guarded Put methods. Diagnostic; sizes the boot remark cost. #408."))
	if err != nil {
		return err
	}

	// --- #244: factory-built GVR divergence age — the observable bound on the
	// passive watch-reconnect self-heal. A factory (shared-informer) GVR whose
	// indexer diverged from the apiserver cannot be relist-repaired; it clears on
	// the reflector's next watch re-establishment, bounded by the cluster's
	// --min-request-timeout. This gauge is the age of the oldest such unrepaired
	// divergence: within the reconnect window = expected self-heal, beyond it =
	// genuinely stuck.
	storeFactoryDivergenceAge, err := m.Int64ObservableGauge(
		"snowplow_store_factory_divergence_age_seconds",
		metric.WithDescription("Age in seconds of the oldest unrepaired factory-built (shared-informer, !ownsInformer) GVR divergence — 0 when none. It self-heals on the reflector's next watch re-establishment, bounded by the apiserver --min-request-timeout (cluster-config-dependent); a value beyond that cadence is a genuinely stuck divergence, within it an expected passive self-heal (#244)."))
	if err != nil {
		return err
	}

	// --- build identity, so every other panel can be pinned to a commit.
	buildInfo, err := m.Int64ObservableGauge(
		"snowplow_build_info",
		metric.WithDescription("Constant 1, labelled with the snowplow build (the full 40-character git commit); the per-pod release is the resource service.version."))
	if err != nil {
		return err
	}

	// --- #386 M3 (PR2): resolve-path customer in-flight, as an OTLP CORRELATION
	// gauge. The expvar twin snowplow_customer_resolve_inflight (PR1) is
	// /debug/vars-only; this hand-wires it to ClickStack (no expvar->OTLP bridge),
	// scalars-only. An Int64ObservableGauge (current count, up/down — like
	// rbacSubGenSubjects), NOT a counter: it is a correlation signal
	// (0 = no customers = healthy), NOT a fault detector. Reads the SAME atomic the
	// refresher's customer-priority yield / #384 serveReserve key off.
	customerResolveInflight, err := m.Int64ObservableGauge(
		"snowplow_customer_resolve_inflight",
		metric.WithDescription("Customer /call dispatches currently on the RESOLVE path (GET /call + POST /call/read reaching a restactions/widgets handler). CORRELATION gauge (0 = idle = healthy), NOT a detector. Excludes the direct-proxy Call()/CallRead() fallthrough, GET /list, and write verbs. #386 M3."))
	if err != nil {
		return err
	}

	// Single callback reading every snapshot at collection time.
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(fallthroughTotal, int64(cache.FallthroughTotal()))
		o.ObserveInt64(assertionViolations, int64(cache.AssertionViolationsTotal()),
			metric.WithAttributes(attribute.String("check", "read_paths_scoped")))
		o.ObserveInt64(rbacPublishSeq, int64(cache.RBACGen()))
		o.ObserveInt64(rbacRebuildPickups, int64(cache.RBACRebuildPostReleasePickups()))
		o.ObserveInt64(rbacRebuildPanics, int64(cache.RBACRebuildPanicsTotal()))
		o.ObserveFloat64(rbacUnpublishedAge, cache.RBACUnpublishedEventAge().Seconds())
		o.ObserveFloat64(rbacPublishAge, cache.RBACLastPublishAge().Seconds())
		o.ObserveInt64(rbacSubGenBumps, int64(cache.RBACSubGenBumpsTotal()))
		o.ObserveInt64(rbacSubGenSubjects, int64(cache.RBACSubGenSubjectsTracked()))
		// #260 change-4 — RBAC sub-gen churn attribution (re-establishment {gvr},
		// bumps_by_source {source}, and the role/uid no-op scalars).
		for gvr, n := range cache.RBACReestablishmentSnapshot() {
			o.ObserveInt64(rbacReestablished, int64(n), metric.WithAttributes(attribute.String("gvr", gvr)))
		}
		for gvr, n := range cache.RBACWatchErrorSnapshot() {
			o.ObserveInt64(rbacWatchError, int64(n), metric.WithAttributes(attribute.String("gvr", gvr)))
		}
		for source, n := range cache.RBACSubGenBumpsBySourceSnapshot() {
			o.ObserveInt64(rbacBumpsBySource, int64(n), metric.WithAttributes(attribute.String("source", source)))
		}
		o.ObserveInt64(rbacRoleNoop, int64(cache.RBACRoleNoopUpdatesTotal()))
		o.ObserveInt64(rbacRoleSemanticNoop, int64(cache.RBACRoleSemanticNoopUpdatesTotal()))
		o.ObserveInt64(rbacBindingUidChanged, int64(cache.RBACBindingUidChangedUpdatesTotal()))
		o.ObserveInt64(bindingNoopUpdates, int64(cache.RBACBindingNoopUpdatesTotal()))
		o.ObserveInt64(bindingSemanticNoopUpdates, int64(cache.RBACBindingSemanticNoopUpdatesTotal()))

		// 1.12.6 C7: every tag-derived family, one loop.
		for _, d := range derived {
			d.observe(o)
		}

		o.ObserveInt64(registeredGVRs, registeredGVRCount())

		done, elapsed := cache.PrewarmCompleteSnapshot()
		o.ObserveInt64(prewarmDone, done)
		o.ObserveInt64(prewarmElapsed, elapsed)

		hits, misses, swaps, refused, denyUncached, entries := rbac.AuthzMemoSnapshot()
		o.ObserveInt64(memoHits, int64(hits))
		o.ObserveInt64(memoMisses, int64(misses))
		o.ObserveInt64(memoSwaps, int64(swaps))
		o.ObserveInt64(memoRefused, int64(refused))
		o.ObserveInt64(memoDenyUncached, int64(denyUncached))
		o.ObserveInt64(memoEntries, int64(entries))

		// --- prewarm engine ---
		engEnq, engProc, engYield, engPending := dispatchers.PrewarmEngineSnapshot()
		o.ObserveInt64(prewarmEngEnqueued, int64(engEnq))
		o.ObserveInt64(prewarmEngProcessed, int64(engProc))
		o.ObserveInt64(prewarmEngYield, int64(engYield))
		o.ObserveInt64(prewarmEngPending, engPending)

		// --- #368 wildcard digest-collision detector (hand-wired, #311) ---
		wcCollision, wcObserved, wcEvicted := dispatchers.ShadowWildcardDigestCounts()
		o.ObserveInt64(shadowWildcardDigestCollision, wcCollision)
		o.ObserveInt64(shadowWildcardDigestObserved, wcObserved)
		o.ObserveInt64(shadowWildcardDigestEvicted, wcEvicted)

		// --- #386 M3 (PR2): resolve-path customer in-flight (correlation gauge) ---
		o.ObserveInt64(customerResolveInflight, dispatchers.CustomerResolveInFlightCount())

		// --- phase-1 pagination ---
		planned, seeded, apiRefPages, eligibleNoCont := dispatchers.Phase1PaginationSnapshot()
		o.ObserveInt64(phase1UnitsPlanned, int64(planned))
		o.ObserveInt64(phase1UnitsSeeded, int64(seeded))
		o.ObserveInt64(phase1ApiRefPages, int64(apiRefPages))
		o.ObserveInt64(phase1EligibleNoContinue, int64(eligibleNoCont))

		// --- phase-1 boot walk ---
		zeroChildren, walkObservations := dispatchers.Phase1WalkSnapshot()
		o.ObserveInt64(phase1WalkZeroChildren, int64(zeroChildren))
		o.ObserveInt64(phase1WalkObservations, int64(walkObservations))

		// --- phase-1 seed outcomes ---
		seedResolves, seedFailures, seedRBACDeny, seedOpFail := dispatchers.Phase1SeedSnapshot()
		o.ObserveInt64(phase1SeedResolves, int64(seedResolves))
		o.ObserveInt64(phase1SeedFailures, int64(seedFailures))
		o.ObserveInt64(phase1SeedRBACDeny, int64(seedRBACDeny))
		o.ObserveInt64(phase1SeedOpFail, int64(seedOpFail))

		// --- SA-discovery ---
		sa := dynamic.SADiscoveryStatsSnapshot()
		for stat, v := range map[string]uint64{
			"builds":        sa.Builds,
			"invalidations": sa.Invalidations,
			"fallbacks":     sa.Fallbacks,
			// #608 — an impersonating config refused by the SA singleton.
			"impersonated_rejected": sa.ImpersonatedRejected,
		} {
			o.ObserveInt64(saDiscovery, int64(v),
				metric.WithAttributes(attribute.String("stat", stat)))
		}

		// --- #608: refresh failures by (err_class, stage) ---
		for _, c := range cache.RefreshFailedByClassSnapshot() {
			o.ObserveInt64(refreshFailedByClass, int64(c.N), metric.WithAttributes(
				attribute.String("err_class", c.Class), attribute.String("stage", c.Stage)))
		}

		// --- CRD-schema memo ---
		csHits, csMisses, csStale, csInval := schema.CRDSchemaMemoSnapshot()
		for stat, v := range map[string]uint64{
			"hits":          csHits,
			"misses":        csMisses,
			"stale_dropped": csStale,
			"invalidations": csInval,
		} {
			o.ObserveInt64(crdSchemaMemo, int64(v),
				metric.WithAttributes(attribute.String("stat", stat)))
		}

		// --- #608 impersonation capability ---
		for stat, v := range cache.ImpersonationCapabilityStats() {
			o.ObserveInt64(impersonation, int64(v),
				metric.WithAttributes(attribute.String("stat", stat)))
		}
		for res, v := range cache.ImpersonationProbeResult() {
			o.ObserveInt64(impersonationProbe, v,
				metric.WithAttributes(attribute.String("stat", res)))
		}

		// --- upstream controller / webhook health ---
		ctrlHealthy, ctrlUnhealthy, whTotal, whFail := cache.UpstreamHealthSnapshot()
		o.ObserveInt64(upstreamControllers, ctrlHealthy,
			metric.WithAttributes(attribute.String("health", "healthy")))
		o.ObserveInt64(upstreamControllers, ctrlUnhealthy,
			metric.WithAttributes(attribute.String("health", "unhealthy")))
		o.ObserveInt64(upstreamWebhooks, whTotal,
			metric.WithAttributes(attribute.String("policy", "total")))
		o.ObserveInt64(upstreamWebhooks, whFail,
			metric.WithAttributes(attribute.String("policy", "fail")))

		// --- dispatch L1, per (class, gvr, outcome) ---
		//
		// 1.12.4: emit one point per cell instead of one process-wide
		// hit/miss pair. seed_hit is a SUBSET of hit, not a third
		// disjoint outcome — a ratio panel must divide seed_hit by hit,
		// and hit by (hit+miss). Cardinality is 3 classes x registered
		// GVRs; the GVR set does not grow with composition count.
		for _, c := range dispatchers.DispatchL1LookupCells() {
			base := []attribute.KeyValue{
				attribute.String("class", c.Class),
				attribute.String("gvr", c.GVR),
			}
			for outcome, v := range map[string]uint64{
				"hit":      c.Hit,
				"miss":     c.Miss,
				"seed_hit": c.SeedHit,
			} {
				o.ObserveInt64(dispatchL1Cells, int64(v),
					metric.WithAttributes(append(append([]attribute.KeyValue{}, base...),
						attribute.String("outcome", outcome))...))
			}
		}
		o.ObserveInt64(seedAttributableHits, int64(dispatchers.HitsSeedAttributable()))

		// --- 1.12.4: A-1 UAF Put declines ---
		o.ObserveInt64(uafRestactionsDeclined, int64(cache.RestactionsUAFPutDeclined()))
		o.ObserveInt64(uafWidgetsDeclined, int64(cache.WidgetsUAFPutDeclined()))
		o.ObserveInt64(uafRAFullListCached, int64(cache.RAFullListUAFCached()))

		// --- 1.12.4: the two cell families, capped ---
		//
		// The cap is applied inside the snapshot accessors, on THIS path
		// only: /debug/vars keeps the full uncapped maps so the bench
		// harness is unaffected. Overflow lands on gvr="__other__" and
		// is counted by snowplow_metrics_series_truncated_total below,
		// so a cardinality regression is visible rather than silent.
		ftCells, _ := cache.FallthroughCellsSnapshot()
		for _, c := range ftCells {
			o.ObserveInt64(fallthroughCells, int64(c.Count),
				metric.WithAttributes(
					attribute.String("path", c.Path),
					attribute.String("gvr", c.GVR),
					attribute.String("reason", c.Reason)))
		}
		o.ObserveInt64(diagnosticTotal, int64(cache.DiagnosticTotal()))
		diagCells, _ := cache.CacheDiagnosticCellsSnapshot()
		for _, c := range diagCells {
			o.ObserveInt64(diagnosticCells, int64(c.Count),
				metric.WithAttributes(
					attribute.String("path", c.Path),
					attribute.String("gvr", c.GVR),
					attribute.String("reason", c.Reason)))
		}
		for family, n := range cache.SeriesTruncatedSnapshot() {
			o.ObserveInt64(seriesTruncated, int64(n),
				metric.WithAttributes(attribute.String("family", family)))
		}

		// --- 1.12.4: boot SLI ---
		o.ObserveInt64(readyzBackstop, dispatchers.ReadinessBackstopFired())
		// --- #397: phase-1 deadline-release detector ---
		o.ObserveInt64(phase1DeadlineReleased, dispatchers.Phase1DeadlineReleasedTotal())
		// --- #736: the releasing outcome, by outcome (closed set) ---
		byOutcome := dispatchers.Phase1ReadinessExitByOutcome()
		for _, oc := range dispatchers.Phase1ReadinessExitOutcomes {
			o.ObserveInt64(phase1ExitByOutcome, byOutcome[oc], metric.WithAttributes(attribute.String("outcome", oc)))
		}

		// --- 1.12.4: L1 store occupancy + lifetime ---
		for stat, v := range cache.ResolvedCacheStatsByStat() {
			o.ObserveInt64(resolvedCache, v,
				metric.WithAttributes(attribute.String("stat", stat)))
		}

		// --- 1.12.5: dep tracker + informer bridge ---
		for stat, v := range cache.DepsStatsByStat() {
			o.ObserveInt64(depsStats, v,
				metric.WithAttributes(attribute.String("stat", stat)))
		}

		// --- #239: dirty-mark attribution (Σ attributed == dirty_mark_total) ---
		for _, b := range cache.DirtyMarkAttributionSnapshot() {
			o.ObserveInt64(dirtyMarkAttributed, int64(b.Count),
				metric.WithAttributes(
					attribute.String("path", b.Path),
					attribute.String("cause", b.Cause),
					attribute.String("class", b.Class)))
		}
		o.ObserveInt64(dirtyMarkEvents, int64(cache.DirtyMarkEventsTotal()))
		o.ObserveInt64(dirtyMarkUnattributed, int64(cache.DirtyMarkUnattributedTotal()))
		for class, n := range cache.DirtyMarkByEntryClassSnapshot() {
			o.ObserveInt64(dirtyMarkByEntryClass, int64(n),
				metric.WithAttributes(attribute.String("class", class)))
		}
		for source, n := range cache.DirtyMarkSubmitSourceSnapshot() {
			o.ObserveInt64(dirtyMarkSubmits, int64(n),
				metric.WithAttributes(attribute.String("source", source)))
		}

		// --- 1.12.4: informer servability ---
		reg, syncedN, servableN, brokenN, confirmedN := cache.ServableCountsSnapshot()
		for state, v := range map[string]int{
			"registered":   reg,
			"synced":       syncedN,
			"servable":     servableN,
			"watch_broken": brokenN,
			"confirmed":    confirmedN,
		} {
			o.ObserveInt64(informerServable, int64(v),
				metric.WithAttributes(attribute.String("state", state)))
		}

		// --- 1.12.5: informer freshness aggregates ---
		fr := cache.InformerFreshnessSnapshotGlobal()
		for stat, v := range map[string]int64{
			"indexer_objects":       fr.IndexerObjects,
			"gvrs_never_event":      fr.GVRsNeverEvent,
			"max_event_age_seconds": fr.MaxEventAgeSecs,
			"gvrs_stale_over_hour":  fr.GVRsStaleOverHour,
		} {
			o.ObserveInt64(informerFreshness, v,
				metric.WithAttributes(attribute.String("stat", stat)))
		}

		// --- #233: unparseable-CA delegation detector (uncapped rate) ---
		o.ObserveInt64(unparseableCADelegations, int64(restactionsapi.UnparseableCADelegationTotal()))

		// --- #311/#293: malformed-dial skip detector, per bounded reason ---
		for reason, n := range restactionsapi.MalformedDialSkippedByReasonSnapshot() {
			o.ObserveInt64(malformedDialSkipped, int64(n),
				metric.WithAttributes(attribute.String("reason", reason)))
		}

		// --- #375: dep-generation guard detector (expected 0) ---
		o.ObserveInt64(unguardedPutTotal, int64(cache.UnguardedPutTotal()))
		// --- #408: moved remarks by carrier ---
		// Each carrier is its own monotonic atomic (never derived as total−boot), so neither
		// ObservableCounter series can decrease between scrapes.
		movedGuarded, movedBoot := cache.MovedRemarkByCarrier()
		o.ObserveInt64(movedRemarkTotal, int64(movedBoot), metric.WithAttributes(attribute.String("carrier", "boot")))
		o.ObserveInt64(movedRemarkTotal, int64(movedGuarded), metric.WithAttributes(attribute.String("carrier", "guarded")))

		// --- #244: factory-built divergence age (bounded self-heal detector) ---
		o.ObserveInt64(storeFactoryDivergenceAge, cache.FactoryDivergenceMaxAgeSeconds())

		// --- 1.12.4: build identity, constant 1 ---
		o.ObserveInt64(buildInfo, 1,
			metric.WithAttributes(attribute.String("version", buildLabel(build))))

		// --- RAFullList serve outcomes + index drift canary ---
		ra := cache.RAFullListServeSnapshot()
		for outcome, v := range map[string]uint64{
			"hit":            ra.Hit,
			"repopulate":     ra.Repopulate,
			"verified_slice": ra.VerifiedSlice,
			"fallback":       ra.Fallback,
		} {
			o.ObserveInt64(raFullListServe, int64(v),
				metric.WithAttributes(attribute.String("outcome", outcome)))
		}
		o.ObserveInt64(bindingsDeltaSkipped, int64(cache.BindingsIndexDeltaSkippedNonTyped()))
		for _, c := range cache.UnrenderableStoreItemsSnapshot() {
			o.ObserveInt64(unrenderableStoreItems, int64(c.Count), metric.WithAttributes(
				attribute.String("site", c.Site), attribute.String("gvr", c.GVR)))
		}
		for _, c := range dispatchers.ResidentUnitsDroppedCells() {
			o.ObserveInt64(residentUnitsDropped, int64(c.Count), metric.WithAttributes(
				attribute.String("reason", c.Reason)))
		}
		return nil
	}, append([]metric.Observable{
		fallthroughTotal, assertionViolations, rbacPublishSeq, rbacRebuildPickups,
		rbacRebuildPanics, rbacUnpublishedAge, rbacPublishAge,
		rbacSubGenBumps, rbacSubGenSubjects,
		// --- #260 change-4 ---
		rbacReestablished, rbacWatchError, rbacBumpsBySource, rbacRoleNoop, rbacRoleSemanticNoop, rbacBindingUidChanged,
		bindingNoopUpdates, bindingSemanticNoopUpdates,
		registeredGVRs, prewarmDone, prewarmElapsed,
		memoHits, memoMisses, memoSwaps, memoRefused, memoDenyUncached, memoEntries,
		prewarmEngEnqueued, prewarmEngProcessed, prewarmEngYield, prewarmEngPending,
		// --- #368 (registered since #455; observed but unregistered before, so
		// the SDK dropped every observation) ---
		shadowWildcardDigestCollision, shadowWildcardDigestObserved, shadowWildcardDigestEvicted,
		phase1UnitsPlanned, phase1UnitsSeeded, phase1ApiRefPages, phase1EligibleNoContinue,
		phase1WalkZeroChildren, phase1WalkObservations,
		phase1SeedResolves, phase1SeedFailures, phase1SeedRBACDeny, phase1SeedOpFail,
		saDiscovery, crdSchemaMemo, refreshFailedByClass, impersonation, impersonationProbe,
		upstreamControllers, upstreamWebhooks,
		raFullListServe, bindingsDeltaSkipped,
		// --- #624 ---
		unrenderableStoreItems,
		// --- #647 ---
		residentUnitsDropped,
		// --- 1.12.4 ---
		uafRestactionsDeclined, uafWidgetsDeclined, uafRAFullListCached,
		dispatchL1Cells, seedAttributableHits,
		// --- #386 M3 (PR2) ---
		customerResolveInflight,
		fallthroughCells, diagnosticTotal, diagnosticCells, seriesTruncated,
		readyzBackstop, phase1DeadlineReleased, phase1ExitByOutcome, resolvedCache, informerServable, buildInfo,
		// --- 1.12.5 ---
		depsStats, informerFreshness,
		// --- #233 ---
		unparseableCADelegations,
		// --- #311/#293 ---
		malformedDialSkipped,
		// --- #375 ---
		unguardedPutTotal,
		// --- #408 ---
		movedRemarkTotal,
		// --- #244 ---
		storeFactoryDivergenceAge,
		// --- #239: dirty-mark attribution ---
		dirtyMarkAttributed, dirtyMarkEvents, dirtyMarkSubmits, dirtyMarkUnattributed,
		// --- #479 1b / #603 PR-0 ---
		dirtyMarkByEntryClass,
	}, derivedObservables...)...)
	if err != nil {
		return err
	}
	// #448: the security/verification counters. CFG-1: every one of them is a
	// cache surface whose expvar key is published only from a Disabled()-gated
	// init, so under cache-off its OTLP series is absent too (not present-zero).
	if cache.Disabled() {
		return nil
	}
	if err := registerListQueryInstrument(m); err != nil {
		return err
	}
	if err := registerUAFNullSkipInstruments(m); err != nil {
		return err
	}
	if err := registerContentFillInstruments(m); err != nil {
		return err
	}
	if err := registerNestedResolveAdmissionInstruments(m); err != nil {
		return err
	}
	if err := registerStaleServeInstruments(m); err != nil {
		return err
	}
	if err := registerStaleRetryInstruments(m); err != nil {
		return err
	}
	if err := registerPrewarmPassInstruments(m); err != nil {
		return err
	}
	if err := registerDenialOnlyInstruments(m); err != nil {
		return err
	}
	if err := registerListNarrowInstruments(m); err != nil {
		return err
	}
	if err := registerUndeclaredExtrasExemptInstrument(m); err != nil {
		return err
	}
	return registerSecurityInstruments(m)
}

// registerUndeclaredExtrasExemptInstrument hand-wires #777 b′'s exemption
// counter onto OTLP (no expvar->OTLP bridge): it reads the typed accessor its
// expvar closure reads. No attributes. Registered only when the cache is on
// (CFG-1: the expvar key is published from a Disabled()-gated init).
func registerUndeclaredExtrasExemptInstrument(m metric.Meter) error {
	exempt, err := m.Int64ObservableCounter("snowplow_widget_undeclared_extras_exempt_total",
		metric.WithDescription("Widget requests that carried undeclared, non-identity extras and were cached anyway because the widget has no extras-reading spec surface (no apiRef, widgetDataTemplate or resourcesRefsTemplate; #777 b′). Without the exemption each would have been an F6 undeclared-extras decline."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(exempt, int64(dispatchers.WidgetUndeclaredExtrasExemptTotal()))
		return nil
	}, exempt)
	return err
}

// registerListNarrowInstruments hand-wires the #603 PR-2 / #765 option B
// SHADOW onto OTLP (no expvar->OTLP bridge). Each instrument reads the typed
// accessor its expvar closure (snowplow_deps_list_narrow) reads, so the two
// surfaces cannot diverge. Attributes are closed, code-defined enums only —
// never a GVR, a key or an identity. Registered only when the cache is on
// (CFG-1: the expvar key is published from a Disabled()-gated init).
//
// THE ENFORCEMENT GATE (#603 Q2) READS:
//   - would_avoid  = snowplow_deps_list_narrow_total{outcome="avoided"}
//   - would_change = snowplow_deps_list_narrow_audit_total{outcome="changed"}
//
// (attribute keys from the closed F8 set: outcome + reason)
//
// with the audit's other verdicts as the denominator, so a zero would_change
// always reads as "N audited, 0 changed". The denominator of the AUDIT is
// snowplow_deps_list_narrow_audit_samples_total (avoid-only REFRESHES); the
// would-avoid count is in MARKS. Read the gate as would_change == 0 against a
// minimum of outcome="identical" audits, and publish outcome="unaudited" /
// samples as the coverage share (expvar snowplow_deps_list_narrow.gate is the
// same readout for one process).
//
// ACROSS RESTARTS AND REPLICAS. Every instrument here is an Int64Observable
// COUNTER read from a process-lifetime atomic, exported with the SDK default
// CUMULATIVE temporality and monotonic (pinned by
// TestListNarrow765_GateCountersExportCumulativeMonotonic): each series carries
// its StartTimeUnix, and a restart starts a NEW series at zero. The 7-day gate
// therefore sums, per (pod, StartTimeUnix), the LAST value of each series,
// never a sum across exports and never one pod's /debug/vars (which forgets
// everything before its own start).
//
// A DYING POD LOSES ITS LAST INTERVAL. Whatever a pod counted after its last
// successful export (up to one export interval, or more on an unclean exit)
// never reaches OTLP, so a would_change counted there is invisible to the
// counter sum. Every would_change is ALSO a WARN log line
// (deps.list_narrow.would_change, written at the moment of the verdict, with
// the key hash and the two digests), so the gate ALSO requires ZERO such lines
// in ClickHouse otel_logs over the same window. Both must be zero.
func registerListNarrowInstruments(m metric.Meter) error {
	decisions, err := m.Int64ObservableCounter("snowplow_deps_list_narrow_total",
		metric.WithDescription("SHADOW (#603/#765): every list_dep dirty mark of an object event, classified by whether enforcing the JQ read-set narrowing would have skipped it. outcome=avoided reason=unread_field is a WOULD-AVOID (the UPDATE changed no field the cell can observe); outcome=kept by reason (read_field, membership, interest_all, interest_pending, registry_late, undecided, no_mask, degraded). Every mark is still made: nothing is skipped. Their sum equals the object-path list_dep dirty marks."))
	if err != nil {
		return err
	}
	audit, err := m.Int64ObservableCounter("snowplow_deps_list_narrow_audit_total",
		metric.WithDescription("SHADOW (#603/#765) audit of refreshes whose every mark since the last refresh was a would-avoid, by outcome: identical (the skip would have been right), changed (WOULD-CHANGE: enforcement would have left the cell stale; the gate requires 0), no_prior (nothing to compare), unaudited (the refresh reached no accepted Put; its marks carry over). Residual rebuild non-determinism reads as changed."))
	if err != nil {
		return err
	}
	verdicts, err := m.Int64ObservableCounter("snowplow_deps_interest_verdict_total",
		metric.WithDescription("SHADOW (#603/#765) record-side outcome per list edge of a RESTAction resolve: outcome=narrowed (reason=ok) or outcome=all by reason (stage_error, not_informer_served, reads_whole_item, registry_full, sticky_all, list_level_read, query_unapplied, parse, jq_unprovable). Says which templates block narrowing."))
	if err != nil {
		return err
	}
	samples, err := m.Int64ObservableCounter("snowplow_deps_list_narrow_audit_samples_total",
		metric.WithDescription("SHADOW (#603/#765) avoid-only refreshes taken at dequeue: the REFRESHES the would-avoid marks would have saved, and the denominator of snowplow_deps_list_narrow_audit_total (each sample ends in exactly one outcome). unaudited / samples is the share the audit could not check."))
	if err != nil {
		return err
	}
	replays, err := m.Int64ObservableCounter("snowplow_deps_list_narrow_replay_total",
		metric.WithDescription("SHADOW (#603/#765 B) LIST edges replayed onto a widget that did not resolve its RESTAction itself, by site (memo = a resolve-memo sibling, full_list = a slice of the shared RAFullList cell) and outcome = the PRODUCER edge's state carried: narrowed (the receiver narrows to the producer's read set), or all / pending / absent (a plain, sticky-all record). pending and absent are the lossy fall-backs."))
	if err != nil {
		return err
	}
	eval, err := m.Int64ObservableCounter("snowplow_deps_interest_eval_total",
		metric.WithDescription("SHADOW (#603/#765) informer-handler cost of the per-transition projection compare, by stat: transitions (UPDATEs evaluated against >= 1 live interest) and ns (total nanoseconds they took)."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, c := range cache.ListNarrowDecisionsSnapshot() {
			o.ObserveInt64(decisions, int64(c.Count), metric.WithAttributes(
				attribute.String("outcome", c.Decision), attribute.String("reason", c.Reason)))
		}
		for verdict, n := range cache.ListNarrowAuditSnapshot() {
			o.ObserveInt64(audit, int64(n), metric.WithAttributes(attribute.String("outcome", verdict)))
		}
		for _, v := range cache.ListNarrowCommitVerdictsSnapshot() {
			o.ObserveInt64(verdicts, int64(v.Count), metric.WithAttributes(
				attribute.String("outcome", v.Verdict), attribute.String("reason", v.Why)))
		}
		o.ObserveInt64(samples, int64(cache.ListNarrowAuditSamples()))
		for _, c := range cache.ListNarrowReplaySnapshot() {
			o.ObserveInt64(replays, int64(c.Count), metric.WithAttributes(
				attribute.String("outcome", c.Outcome), attribute.String("site", c.Site)))
		}
		tr, ns := cache.ListNarrowEvalStats()
		o.ObserveInt64(eval, int64(tr), metric.WithAttributes(attribute.String("stat", "transitions")))
		o.ObserveInt64(eval, int64(ns), metric.WithAttributes(attribute.String("stat", "ns")))
		return nil
	}, decisions, audit, verdicts, samples, replays, eval)
	return err
}

// registerDenialOnlyInstruments hand-wires #681 / #682 / #677 detectors onto
// OTLP (no expvar->OTLP bridge). Each instrument reads the typed accessor its
// expvar closure reads, so the surfaces cannot diverge. Attributes are closed,
// code-defined enums only: site {call_ra, call_widget, refresher, seed} and
// outcome {allowed, denied, scope_fallback} — never a GVR, an object name, an
// identity or a key. Registered only when the cache is on (CFG-1: the expvar
// keys are published from Disabled()-gated inits).
//
// #681 REMOVED the four snowplow_refresher_regate_declined_* instruments #677
// added: a denial-only result is now Put, so the known-declined mark they
// described can never be set.
func registerDenialOnlyInstruments(m metric.Meter) error {
	denialOnly, err := m.Int64ObservableCounter("snowplow_resolved_cache_denial_only_put_total",
		metric.WithDescription("Resolved RESTAction / widget results Put although they carried stage errors, because EVERY stage error was a per-identity forbidden (snowplow's re-gate deny, or the apiserver's 403 on the requester's own dial), by site: call_ra, call_widget, refresher, seed (#681). The cell is keyed per identity class exactly like any other; its error bodies are identity-free."))
	if err != nil {
		return err
	}
	epoch, err := m.Int64ObservableGauge("snowplow_rbac_subgen_epoch",
		metric.WithDescription("The process-wide RBAC sub-generation epoch (#682). 0 or 1: it is bumped at most once, at the first bindings-by-GVR index build, iff an RBAC event landed after the first RBAC publish and before that build; the bump rotates every per-identity L1 key."))
	if err != nil {
		return err
	}
	predial, err := m.Int64ObservableCounter("snowplow_internal_dispatch_predial_total",
		metric.WithDescription("Branch C per-identity GET-by-name authorizations decided BEFORE the SA dial (#677), by outcome: allowed (dialled), denied (typed re-gate Forbidden, no dial), scope_fallback (empty namespace on a GVR not known to be cluster-scoped: decided after the fetch, as before #677)."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, c := range cache.DenialOnlyPutSnapshot() {
			o.ObserveInt64(denialOnly, int64(c.Count), metric.WithAttributes(attribute.String("site", c.Site)))
		}
		o.ObserveInt64(epoch, int64(cache.RBACSubGenEpoch()))
		for _, c := range restactionsapi.PreDialSnapshot() {
			o.ObserveInt64(predial, int64(c.Count), metric.WithAttributes(attribute.String("outcome", c.Outcome)))
		}
		return nil
	}, denialOnly, epoch, predial)
	return err
}

// registerPrewarmPassInstruments hand-wires #643's background-pass detectors
// onto OTLP (no expvar->OTLP bridge). Each instrument reads the typed accessor
// its expvar closure reads, so the two surfaces cannot diverge. Attributes are
// closed, code-defined enums only: kind (the prewarm scope kinds), outcome
// {complete, cut} and origin {background, request} — never a GVR, an object
// name or a key. Registered only when the cache is on (CFG-1: the expvar keys
// are published from Disabled()-gated inits).
func registerPrewarmPassInstruments(m metric.Meter) error {
	passTotal, err := m.Int64ObservableCounter("snowplow_prewarm_pass_total",
		metric.WithDescription("Prewarm-engine passes processed, by kind {boot, gvr-discovered, keepwarm, rbac-shift, learned-class} and outcome {complete, cut}. cut = the handler returned an error (the engine requeues it) or the pass declared a unit plan and did not get through it. A gvr-discovered cut rate that never falls is the #643 loop."))
	if err != nil {
		return err
	}
	gauge := func(name, desc string) (metric.Int64ObservableGauge, error) {
		return m.Int64ObservableGauge(name, metric.WithDescription(desc))
	}
	planned, err := gauge("snowplow_prewarm_pass_units_planned",
		"Units the LAST pass of this kind planned to seed (base units its loops attempt + the admitted learned units), by kind. A pass cut before the learned-class bound decides reports an UPPER BOUND (every learned unit, admitted or not). #643.")
	if err != nil {
		return err
	}
	done, err := gauge("snowplow_prewarm_pass_units_done",
		"Units the LAST pass of this kind got through (resolved + every skip), by kind. planned - done > 0 is the work the pass's budget did not cover. #643.")
	if err != nil {
		return err
	}
	budget, err := gauge("snowplow_prewarm_pass_budget_ms",
		"Wall-clock budget of the LAST pass of this kind (the per-scope timeout), by kind. #643.")
	if err != nil {
		return err
	}
	elapsed, err := gauge("snowplow_prewarm_pass_elapsed_ms",
		"Wall-clock time of the LAST pass of this kind, by kind. #643.")
	if err != nil {
		return err
	}
	rWidgetUnits, err := gauge("snowplow_prewarm_pass_resolved_widget_units",
		"Widget units the LAST pass of this kind really resolved (skips excluded), by kind. Per-unit widget cost = resolved_widget_us / resolved_widget_units. #643.")
	if err != nil {
		return err
	}
	rWidgetUS, err := gauge("snowplow_prewarm_pass_resolved_widget_us",
		"Summed wall cost in microseconds of the widget units the LAST pass of this kind resolved, by kind. #643.")
	if err != nil {
		return err
	}
	rRAUnits, err := gauge("snowplow_prewarm_pass_resolved_ra_units",
		"RESTAction units the LAST pass of this kind really resolved (skips excluded), by kind. Per-unit RA cost = resolved_ra_us / resolved_ra_units. #643.")
	if err != nil {
		return err
	}
	rRAUS, err := gauge("snowplow_prewarm_pass_resolved_ra_us",
		"Summed wall cost in microseconds of the RESTAction units the LAST pass of this kind resolved, by kind. #643.")
	if err != nil {
		return err
	}
	skipDeclined, err := m.Int64ObservableCounter("snowplow_prewarm_skip_declined_external_total",
		metric.WithDescription("Seed units skipped because the same key was already resolved and declined for an external touch, by kind. POSITIVE CONTROL for the #643 declined-external skip: must read > 0 for every kind the skip covers, or the skip never fired."))
	if err != nil {
		return err
	}
	skipResume, err := m.Int64ObservableCounter("snowplow_prewarm_skip_resume_done_total",
		metric.WithDescription("Seed units skipped on a resumed pass because an earlier attempt of the same scope already re-resolved them at the current discovery epoch, by kind. POSITIVE CONTROL for the #643 resume skip."))
	if err != nil {
		return err
	}
	extFetch, err := m.Int64ObservableCounter("snowplow_external_fetch_total",
		metric.WithDescription("LOGICAL external (non-apiserver) fetches issued by the RESTAction resolver, one per stage call, by origin {background, request} and driver (the resolver that dialled: refresher, cohort-seed, prewarm-engine-boot = the discovery walk, prewarm-path = the content prewarm, background-unattributed, serve); retries inside plumbing's RetryClient are NOT counted, so this does not compare 1:1 with a server-side attempt log (ClickHouse query_log). background = any non-customer resolve (prewarm engine, content prewarm, refresher). A background rate that does not fall after boot is the #643 re-fetch loop."))
	if err != nil {
		return err
	}
	extAbandoned, err := m.Int64ObservableCounter("snowplow_external_fetch_abandoned_total",
		metric.WithDescription("LOGICAL external fetches whose caller's ctx was done before the response was read (the boot-readiness bound, a cut scope, a client that left), by origin and driver (same closed sets as snowplow_external_fetch_total); RetryClient attempts are not counted separately. The server may still be executing each one. #643."))
	if err != nil {
		return err
	}
	declinedMarks, err := m.Int64ObservableGauge("snowplow_prewarm_declined_external_marks",
		metric.WithDescription("Keys currently held in the engine-wide declined-external set (#643): (unit, cohort) keys whose seed resolve touched an external endpoint, skipped by every background pass until the mark retires itself (an L1 Put of the key, a RESTAction / endpointRef Secret change, GONE, a captured dep moving, or a served input cell being re-Put). Process-lived; its growth over a long run is the reading."))
	if err != nil {
		return err
	}
	declinedRetired, err := m.Int64ObservableCounter("snowplow_prewarm_declined_external_retired_total",
		metric.WithDescription("Declined-external marks retired, by reason (closed set): nil_mark, served_cell_changed, deps_moved, ra_unread_at_mark, ra_unreadable, ra_changed, endpoint_secret_changed (re-checked at a skip consult), put_unmark (an L1 Put of the key), gone (the widget or RESTAction was deleted), config_redrive (a config-vars redrive cleared the set), widget_changed (the discovery walk re-read the walked widget at another uid/resourceVersion, #643 PR-B2). A reason climbing with skip_declined_external_total staying low means marks retire every pass and the re-dials are back (#643)."))
	if err != nil {
		return err
	}
	gvrEpoch, err := m.Int64ObservableGauge("snowplow_gvr_discovery_epoch",
		metric.WithDescription("The prewarm engine's GVR-discovery epoch: bumped once per GVR discovered after boot (#643). Every discovery folds into ONE pending gvr-discovered pass; a resumed attempt of that pass skips only units re-Put in the current epoch, so a rising epoch invalidates every resume record."))
	if err != nil {
		return err
	}
	walkSkip, err := m.Int64ObservableCounter("snowplow_prewarm_walk_skip_declined_external_total",
		metric.WithDescription("Discovery-walk widget resolves skipped because the widget's last walk resolve touched an external endpoint and the declined-external mark still holds; the walk replayed the recorded children instead (#643 PR-B2). POSITIVE CONTROL: must read > 0 after the first walk on a pod with external widgets, or the walk skip never fired."))
	if err != nil {
		return err
	}
	walkUnmarkable, err := m.Int64ObservableCounter("snowplow_prewarm_walk_errored_unmarkable_total",
		metric.WithDescription("Discovery-walk resolves that touched an external endpoint and then errored on a widget that DECLARES children (spec.resourcesRefs items or spec.resourcesRefsTemplate): not marked, so a later walk can still discover the subtree, and re-dialled on every walk (#643 PR-B2 ruling (c)). Its rate is that residual re-dial load."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(walkSkip, int64(dispatchers.WalkSkipDeclinedExternalTotal()))
		o.ObserveInt64(walkUnmarkable, int64(dispatchers.WalkErroredUnmarkableTotal()))
		o.ObserveInt64(gvrEpoch, int64(dispatchers.GVRDiscoveryEpoch()))
		o.ObserveInt64(declinedMarks, cache.EngineDeclinedExternalSet().Len())
		for _, c := range cache.DeclinedExternalRetiredSnapshot() {
			o.ObserveInt64(declinedRetired, int64(c.Count), metric.WithAttributes(attribute.String("reason", c.Reason)))
		}
		kindAttr := func(kind string) metric.ObserveOption {
			return metric.WithAttributes(attribute.String("kind", kind))
		}
		for _, c := range dispatchers.PrewarmPassTotalSnapshot() {
			o.ObserveInt64(passTotal, int64(c.Count), metric.WithAttributes(
				attribute.String("kind", c.Kind), attribute.String("outcome", c.Outcome)))
		}
		for _, l := range dispatchers.PrewarmPassLastSnapshot() {
			a := kindAttr(l.Kind)
			o.ObserveInt64(planned, l.Planned, a)
			o.ObserveInt64(done, l.Done, a)
			o.ObserveInt64(budget, l.BudgetMS, a)
			o.ObserveInt64(elapsed, l.ElapsedMS, a)
			o.ObserveInt64(rWidgetUnits, l.ResolvedWidgetUnits, a)
			o.ObserveInt64(rWidgetUS, l.ResolvedWidgetUS, a)
			o.ObserveInt64(rRAUnits, l.ResolvedRAUnits, a)
			o.ObserveInt64(rRAUS, l.ResolvedRAUS, a)
		}
		for _, c := range dispatchers.PrewarmSkipDeclinedExternalSnapshot() {
			o.ObserveInt64(skipDeclined, int64(c.Count), kindAttr(c.Kind))
		}
		for _, c := range dispatchers.PrewarmSkipResumeDoneSnapshot() {
			o.ObserveInt64(skipResume, int64(c.Count), kindAttr(c.Kind))
		}
		// #643 PR-B2 — one series per {origin, driver}: driver determines
		// origin, so sum-by-origin is the pre-B2 {origin} reading unchanged.
		for _, c := range restactionsapi.ExternalFetchByDriverSnapshot() {
			o.ObserveInt64(extFetch, int64(c.Count), metric.WithAttributes(
				attribute.String("origin", c.Origin), attribute.String("driver", c.Driver)))
		}
		for _, c := range restactionsapi.ExternalFetchAbandonedByDriverSnapshot() {
			o.ObserveInt64(extAbandoned, int64(c.Count), metric.WithAttributes(
				attribute.String("origin", c.Origin), attribute.String("driver", c.Driver)))
		}
		return nil
	}, passTotal, planned, done, budget, elapsed, rWidgetUnits, rWidgetUS, rRAUnits, rRAUS,
		skipDeclined, skipResume, extFetch, extAbandoned, declinedMarks, declinedRetired, gvrEpoch,
		walkSkip, walkUnmarkable)
	return err
}

// registerListQueryInstrument hand-wires #618's list-query outcome counter onto
// OTLP (no expvar->OTLP bridge). It reads restactionsapi.ListQueryOutcomeSnapshot,
// the same accessor the snowplow_list_query_total expvar closure reads, so the
// two surfaces cannot diverge. Both attributes come from a closed, code-defined
// enum: outcome {in_process, fallthrough} and reason (selector, or the
// fall-through reason) — never from request data. Registered only when the cache
// is on (CFG-1: the expvar key is published from a Disabled()-gated init).
func registerListQueryInstrument(m metric.Meter) error {
	listQuery, err := m.Int64ObservableCounter(
		"snowplow_list_query_total",
		metric.WithDescription("Informer-servable calls that carried a query string, by outcome {in_process, fallthrough} and bounded reason. in_process/selector: a labelSelector and/or metadata.name (or namespaced metadata.namespace) fieldSelector was applied to the informer's items. fallthrough/<reason>: the call went to the apiserver with its query intact (pagination, resource_version, watch, unsupported_field, namespace_field_cluster_path, invalid_selector, repeated_param, query_on_get, unknown_param, malformed_query). Calls without a query are not counted. #618."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, s := range restactionsapi.ListQueryOutcomeSnapshot() {
			o.ObserveInt64(listQuery, int64(s.Count), metric.WithAttributes(
				attribute.String("outcome", s.Outcome), attribute.String("reason", s.Reason)))
		}
		return nil
	}, listQuery)
	return err
}

// registerUAFNullSkipInstruments hand-wires #778's userAccessFilter null-skip
// counter onto OTLP (no expvar->OTLP bridge). Both read the accessors the
// expvar closures read, so the surfaces cannot diverge. NO attributes: the
// by-stage map names RESTActions, which the closed attribute-key set admits
// nowhere, so it stays expvar-only (JWT-gated /debug/vars) — the content-fill
// _by_ra split. Registered only when the cache is on (CFG-1).
func registerUAFNullSkipInstruments(m metric.Meter) error {
	total, err := m.Int64ObservableCounter("snowplow_uaf_resources_null_skipped_total",
		metric.WithDescription("userAccessFilter stage evaluations whose resourcesFrom yielded at least one null element, skipped like \"\" (#767): an all-null set denies every item, and the answer is cached. Transient while an input has no resource yet (a CompositionDefinition not reconciled); permanent for a typo'd path. Which stage: /debug/vars snowplow_uaf_resources_null_skipped_by_stage. #778."))
	if err != nil {
		return err
	}
	stages, err := m.Int64ObservableGauge("snowplow_uaf_resources_null_skipping_stages",
		metric.WithDescription("RESTAction stages whose LATEST userAccessFilter resourcesFrom evaluation skipped a null element: each is serving a deny-all now. A stage whose input reconciles drops out; one that stays is likely a typo'd path. /debug/vars snowplow_uaf_resources_null_skipped_by_stage (skipping_now) names it. #778."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(total, int64(restactionsapi.UAFNullSkippedTotal()))
		o.ObserveInt64(stages, int64(restactionsapi.UAFNullSkippingStages()))
		return nil
	}, total, stages)
	return err
}

// registerContentFillInstruments hand-wires #710's apistage content-fill
// series onto OTLP (no expvar->OTLP bridge). The observables read
// restactionsapi.ContentFillStatsSnapshot, the accessor the expvar closures
// read, so the surfaces cannot diverge. The only attribute is `reason`, a
// closed code-defined set {absent, known_stale, refresh_forced}; the by-GVR
// and by-RA tables stay expvar-only (data-derived keys). The duration is a
// synchronous histogram fed through the api package's recorder hook (api
// cannot import this package). Registered only when the cache is on (CFG-1).
func registerContentFillInstruments(m metric.Meter) error {
	total, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_total",
		metric.WithDescription("Apistage content fills started (a reader took the content-cell MISS branch: whole-list envelope build + decode + Put), by reason {absent, known_stale, refresh_forced}. #710."))
	if err != nil {
		return err
	}
	dup, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_duplicate_total",
		metric.WithDescription("Content fills started while another fill of the SAME content key was already in flight (the herd), by reason. ≈0 once fills are coalesced. #710."))
	if err != nil {
		return err
	}
	bytesTotal, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_bytes_total",
		metric.WithDescription("Raw list-envelope bytes produced by content fills, by reason. #710."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	joined, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_joined_total",
		metric.WithDescription("Readers that joined an in-flight content fill of their key instead of filling (single-flight), by reason. #710 PR-1."))
	if err != nil {
		return err
	}
	joinRefused, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_join_refused_total",
		metric.WithDescription("Readers that found an in-flight fill of their key but did not join it because the cell's coordinate moved after the flight started (epoch-at-entry rule); they lead the next flight. By reason. #710 PR-1."))
	if err != nil {
		return err
	}
	joinWaiting, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_join_waiting",
		metric.WithDescription("Readers waiting on an in-flight content fill now. #710 PR-1."))
	if err != nil {
		return err
	}
	refreshReused, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_refresh_reused_total",
		metric.WithDescription("Refresher re-resolve reads whose trigger set held the content cell's GVR but whose stored cell was read after every trigger (content stamp): served from the cell, no re-fill. Each one is a re-fill of the shared list avoided. #765 option A."))
	if err != nil {
		return err
	}
	refreshCoalesced, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_refresh_coalesced_total",
		metric.WithDescription("Background refresher readers that joined their content key's pending fill (queued behind the in-flight one, started after their trigger) instead of starting a concurrent re-fill of the same list. #765 option A."))
	if err != nil {
		return err
	}
	notServable, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_not_servable_total",
		metric.WithDescription("Content fills whose dispatch the informer could not serve (no bytes; the call falls through). #710."))
	if err != nil {
		return err
	}
	inflight, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_inflight",
		metric.WithDescription("Content fills in flight now. #710."))
	if err != nil {
		return err
	}
	inflightBytes, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_inflight_bytes",
		metric.WithDescription("Raw envelope bytes held by the content fills in flight now. #710."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	duration, err := m.Int64Histogram("snowplow_apistage_content_fill_duration_ms",
		metric.WithDescription("One apistage content fill (ms): dispatch start to the end of its parse and Put. #710 (PR-2's predicted admission wait)."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(restactionsapi.ContentFillDurationBoundsMS)...),
	)
	if err != nil {
		return err
	}
	restactionsapi.SetContentFillDurationRecorder(func(ms int64) { duration.Record(context.Background(), ms) })
	// #710 PR-2a — the memory admission gate. `class` is the closed admission
	// class set {background, customer}.
	admWait, err := m.Int64Histogram("snowplow_apistage_content_fill_admission_wait_ms",
		metric.WithDescription("One content-fill flight's wait for memory admission under the shared AdmissionCeiling (ms), by the class it admitted at {background, customer}. #710 PR-2a."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(restactionsapi.ContentFillDurationBoundsMS)...),
	)
	if err != nil {
		return err
	}
	restactionsapi.SetContentFillAdmissionWaitRecorder(func(ms int64, class string) {
		admWait.Record(context.Background(), ms, metric.WithAttributes(attribute.String("class", class)))
	})
	admQueued, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_admission_queued",
		metric.WithDescription("Content-fill flights waiting for memory admission now, by class (only background queues: users never wait). #710 PR-2a."))
	if err != nil {
		return err
	}
	admTimeout, err := m.Int64ObservableCounter("snowplow_apistage_content_fill_admission_timeout_total",
		metric.WithDescription("Background content-fill flights not admitted before the write deadline (their refresher/prewarm readers get a 503 stage error and the cell keeps serving; D1). Customer and debt fills never queue. By class. #710 PR-2a."))
	if err != nil {
		return err
	}
	admWeight, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_admission_inflight_weight",
		metric.WithDescription("Estimated bytes (raw envelope × calibrated decode expansion) of the content fills admitted now. #710 PR-2a."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	admOver, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_admission_over_ceiling_bytes",
		metric.WithDescription("How far the admitted content-fill weight sits past the shared ceiling now: customer and debt fills never queue (owner ruling: no one waits in queue) but still count, so this is the excursion they cause. The early warning for the accepted memory residual (#710 ruling 2); 0 when they fit."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	admOverPeak, err := m.Int64ObservableGauge("snowplow_apistage_content_fill_admission_over_ceiling_bytes_peak",
		metric.WithDescription("High-water mark of snowplow_apistage_content_fill_admission_over_ceiling_bytes since boot. #710."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		a := restactionsapi.ContentFillAdmissionStatsSnapshot()
		for _, c := range restactionsapi.ContentFillAdmissionClasses() {
			attrs := metric.WithAttributes(attribute.String("class", c))
			o.ObserveInt64(admQueued, a.Queued[c], attrs)
			o.ObserveInt64(admTimeout, int64(a.Timeout[c]), attrs)
		}
		o.ObserveInt64(admWeight, a.InflightWeight)
		o.ObserveInt64(admOver, a.OverCeiling)
		o.ObserveInt64(admOverPeak, a.OverCeilingPk)
		return nil
	}, admQueued, admTimeout, admWeight, admOver, admOverPeak)
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := restactionsapi.ContentFillStatsSnapshot()
		for _, r := range restactionsapi.ContentFillReasons() {
			attrs := metric.WithAttributes(attribute.String("reason", r))
			o.ObserveInt64(total, int64(s.Total[r]), attrs)
			o.ObserveInt64(dup, int64(s.Duplicate[r]), attrs)
			o.ObserveInt64(bytesTotal, int64(s.Bytes[r]), attrs)
			o.ObserveInt64(joined, int64(s.Joined[r]), attrs)
			o.ObserveInt64(joinRefused, int64(s.JoinRefused[r]), attrs)
		}
		o.ObserveInt64(notServable, int64(s.NotServable))
		o.ObserveInt64(inflight, s.Inflight)
		o.ObserveInt64(inflightBytes, s.InflightBytes)
		o.ObserveInt64(joinWaiting, s.JoinWaiting)
		o.ObserveInt64(refreshReused, int64(s.RefreshReused))
		o.ObserveInt64(refreshCoalesced, int64(s.RefreshCoalesced))
		return nil
	}, total, dup, bytesTotal, joined, joinRefused, joinWaiting, refreshReused, refreshCoalesced, notServable, inflight, inflightBytes)
	return err
}

// registerNestedResolveAdmissionInstruments hand-wires #720's nested-resolve
// over-ceiling gauges onto OTLP (no expvar->OTLP bridge). The callback reads
// restactionsapi.NestedResolveAdmissionStatsSnapshot, the accessor the expvar
// closures read, so the surfaces cannot diverge. No attributes. Registered only
// when the cache is on (CFG-1: the expvar keys are published from a
// Disabled()-gated init).
func registerNestedResolveAdmissionInstruments(m metric.Meter) error {
	over, err := m.Int64ObservableGauge("snowplow_nested_resolve_admission_over_ceiling_bytes",
		metric.WithDescription("How far CUSTOMER nested-resolve trees, which never wait on the nested-resolve admission gate, push its in-flight weight past the ceiling it admits background trees by (bytes, now). 0 while customer trees fit. The early warning for the 1.5.27 OOMKill shape re-exposed by owner decision. #720."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	peak, err := m.Int64ObservableGauge("snowplow_nested_resolve_admission_over_ceiling_bytes_peak",
		metric.WithDescription("High-water mark since boot of snowplow_nested_resolve_admission_over_ceiling_bytes, so a burst shorter than the export interval is not missed. #720."),
		metric.WithUnit("By"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := restactionsapi.NestedResolveAdmissionStatsSnapshot()
		o.ObserveInt64(over, s.OverCeilingBytes)
		o.ObserveInt64(peak, s.OverCeilingBytesPeak)
		return nil
	}, over, peak)
	return err
}

// registerStaleRetryInstruments hand-wires #740's viewer-gated retry engine onto
// OTLP (no expvar->OTLP bridge). The callback reads cache.StaleRetryStatsSnapshot,
// the accessor the expvar closures read, so the surfaces cannot diverge. Labels
// are closed: kind {transient, genuine}, source {background, demand, debt},
// reason {no_viewer, evicted}; every cell of each set is emitted, zeros
// included. Registered only when the cache is on (CFG-1).
func registerStaleRetryInstruments(m metric.Meter) error {
	retries, err := m.Int64ObservableCounter("snowplow_stale_retry_total",
		metric.WithDescription("Viewer-gated retries scheduled for a known-stale cell after a failed refresh, by kind {transient: the Put was refused (a newer change exists) — retried at once; genuine: 1 s doubling to 60 s, only while a /refreshes tab views the cell} and source {background, demand, debt}. #740."))
	if err != nil {
		return err
	}
	chains, err := m.Int64ObservableGauge("snowplow_stale_retry_chains_active",
		metric.WithDescription("Known-stale cells with a retry chain scheduled or running (one per cell; abandoned chains excluded). #740."))
	if err != nil {
		return err
	}
	viewers, err := m.Int64ObservableGauge("snowplow_stale_retry_viewers",
		metric.WithDescription("Armed /refreshes dependent keys across the active retry chains: someone is looking at a stale cell whose refresh keeps failing. #740."))
	if err != nil {
		return err
	}
	abandoned, err := m.Int64ObservableCounter("snowplow_stale_retry_abandoned_total",
		metric.WithDescription("Retry chains stopped, by reason {no_viewer: nobody views the cell — it stays known-stale and served, and an arm resumes it; evicted: the cell left the store}. #740."))
	if err != nil {
		return err
	}
	resumed, err := m.Int64ObservableCounter("snowplow_stale_retry_resumed_total",
		metric.WithDescription("Abandoned retry chains resumed, with an immediate attempt, because a /refreshes tab armed the cell. #740."))
	if err != nil {
		return err
	}
	backoff, err := m.Int64ObservableGauge("snowplow_stale_retry_backoff_ms",
		metric.WithDescription("The longest retry wait currently pending (ms). Sitting at 60000 is a persistent upstream failure that someone is looking at. #740."),
		metric.WithUnit("ms"))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := cache.StaleRetryStatsSnapshot()
		for k, kind := range cache.StaleRetryKinds {
			for src, source := range cache.StaleRetrySources {
				o.ObserveInt64(retries, int64(s.Retries[k][src]), metric.WithAttributes(
					attribute.String("kind", kind), attribute.String("source", source)))
			}
		}
		o.ObserveInt64(chains, s.ChainsActive)
		o.ObserveInt64(viewers, s.Viewers)
		o.ObserveInt64(abandoned, int64(s.AbandonedNoViewer), metric.WithAttributes(attribute.String("reason", "no_viewer")))
		o.ObserveInt64(abandoned, int64(s.AbandonedEvicted), metric.WithAttributes(attribute.String("reason", "evicted")))
		o.ObserveInt64(resumed, int64(s.Resumed))
		o.ObserveInt64(backoff, s.BackoffMS)
		return nil
	}, retries, chains, viewers, abandoned, resumed, backoff)
	return err
}

// registerStaleServeInstruments hand-wires #710 PR-2b's D2 stale-serve family
// onto OTLP (no expvar->OTLP bridge). The observables read
// restactionsapi.StaleServeStatsSnapshot; the two histograms are synchronous,
// fed through the api package's recorder hooks. The only attribute is the
// closed reason {known_stale}. Registered only when the cache is on (CFG-1).
func registerStaleServeInstruments(m metric.Meter) error {
	serveTotal, err := m.Int64ObservableCounter("snowplow_apistage_stale_serve_total",
		metric.WithDescription("Customer reads served a KNOWN-STALE identity-free content cell at once under an armable resolve (D2, owner ruling: show then replace), by reason {known_stale}. Each opens or joins the cell's debt: an immediate refill, then a push to the customer cell. #710 PR-2b."))
	if err != nil {
		return err
	}
	toPush, err := m.Int64Histogram("snowplow_apistage_stale_to_push_ms",
		metric.WithDescription("Stale serve → the /refreshes push of the dependent customer cell (ms). The owner bar: p95 ≤ 3000, max ≤ 10000, target 1000. #710 PR-2b."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(restactionsapi.StaleToPushBoundsMS)...))
	if err != nil {
		return err
	}
	restactionsapi.SetStaleToPushRecorder(func(ms int64) { toPush.Record(context.Background(), ms) })
	serveAge, err := m.Int64Histogram("snowplow_apistage_stale_serve_age_ms",
		metric.WithDescription("Age of the content body a stale serve delivered: since the content cell's known-stale window opened (ms). #710 PR-2b."),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(int64sToFloat64s(restactionsapi.StaleToPushBoundsMS)...))
	if err != nil {
		return err
	}
	restactionsapi.SetStaleServeAgeRecorder(func(ms int64) { serveAge.Record(context.Background(), ms) })
	debtOpen, err := m.Int64ObservableGauge("snowplow_apistage_stale_debt_open",
		metric.WithDescription("Open ACTIVE stale-serve debts (content cells served stale whose refill has not committed and someone views); parked debts are snowplow_apistage_stale_debt_parked. 0 when healthy; growing = the guarantee failing. #710 PR-2b, #740."))
	if err != nil {
		return err
	}
	debtParked, err := m.Int64ObservableGauge("snowplow_apistage_stale_debt_parked",
		metric.WithDescription("D2 debts PARKED: their refill keeps failing and no /refreshes tab views a dependent, so they are not retried (the cell is kept and served); an arm of a dependent or a new stale serve resumes them. Not an alert: nobody is looking. #740."))
	if err != nil {
		return err
	}
	oldest, err := m.Int64ObservableGauge("snowplow_apistage_stale_debt_oldest_age_ms",
		metric.WithDescription("Age of the oldest ACTIVE stale-serve debt (ms), measured from its current viewed episode (parked debts excluded). #710 PR-2b, #740."), metric.WithUnit("ms"))
	if err != nil {
		return err
	}
	retries, err := m.Int64ObservableCounter("snowplow_apistage_stale_debt_retries_total",
		metric.WithDescription("Failed debt refill attempts retried (non-droppable, #690 backoff). #710 PR-2b."))
	if err != nil {
		return err
	}
	overdue, err := m.Int64ObservableCounter("snowplow_apistage_stale_overdue_total",
		metric.WithDescription("Stale-serve debts that stayed open past the 10 s bar, counted once each. Alert on rate > 0. #710 PR-2b."))
	if err != nil {
		return err
	}
	pushes, err := m.Int64ObservableCounter("snowplow_apistage_stale_push_total",
		metric.WithDescription("/refreshes pushes delivered to stale-served customer cells after their debt cleared. #710 PR-2b."))
	if err != nil {
		return err
	}
	unarmed, err := m.Int64ObservableCounter("snowplow_apistage_stale_push_unarmed_total",
		metric.WithDescription("Replacement publishes of a stale-served customer cell that found no armed tab yet; the owed-push mark announces it when a tab arms. Informational, not a guarantee failure. #710 PR-2b."))
	if err != nil {
		return err
	}
	unresolved, err := m.Int64ObservableCounter("snowplow_apistage_stale_dependent_unresolved_total",
		metric.WithDescription("Debt-lane re-resolves of a stale-served customer cell that completed but whose Put was declined; the stale-derived cell is evicted so the pushed refetch is a fresh fill. #710 PR-2b."))
	if err != nil {
		return err
	}
	replaceRetries, err := m.Int64ObservableCounter("snowplow_apistage_stale_replace_retries_total",
		metric.WithDescription("Failed re-resolves of a stale-served customer cell, retried on backoff; the cell is kept (never evicted on failure). #710 PR-2b."))
	if err != nil {
		return err
	}
	withdrawn, err := m.Int64ObservableCounter("snowplow_apistage_stale_serve_withdrawn_total",
		metric.WithDescription("Stale serves withdrawn at the write point because the response's Put was declined (no push could reach it): the stale body was never written and the resolve re-ran fresh. #710 PR-2b."))
	if err != nil {
		return err
	}
	owedOpen, err := m.Int64ObservableGauge("snowplow_apistage_stale_owed_open",
		metric.WithDescription("Keys a stale-derived body was written under whose fresh value has not yet reached an armed tab. #710 PR-2b."))
	if err != nil {
		return err
	}
	owedDropped, err := m.Int64ObservableCounter("snowplow_apistage_stale_owed_dropped_total",
		metric.WithDescription("Owed-push marks dropped by the L1-capacity bound before reaching a tab: a guarantee failure, expected 0 (a bench gate). #710 PR-2b."))
	if err != nil {
		return err
	}
	owedArm, err := m.Int64ObservableCounter("snowplow_apistage_stale_owed_arm_emits_total",
		metric.WithDescription("Refresh events emitted at /refreshes arm time for a key whose replacement was published before the tab armed. #710 PR-2b."))
	if err != nil {
		return err
	}
	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := restactionsapi.StaleServeStatsSnapshot()
		o.ObserveInt64(serveTotal, int64(s.ServeTotal), metric.WithAttributes(attribute.String("reason", "known_stale")))
		o.ObserveInt64(debtOpen, s.DebtOpen)
		o.ObserveInt64(debtParked, s.DebtParked)
		o.ObserveInt64(oldest, s.DebtOldestAgeMS)
		o.ObserveInt64(retries, int64(s.DebtRetries))
		o.ObserveInt64(overdue, int64(s.OverdueTotal))
		o.ObserveInt64(pushes, int64(s.PushTotal))
		o.ObserveInt64(unarmed, int64(s.PushUnarmed))
		o.ObserveInt64(unresolved, int64(s.DependentUnresolved))
		o.ObserveInt64(withdrawn, int64(s.WithdrawnUnarmable), metric.WithAttributes(attribute.String("reason", "unarmable")))
		o.ObserveInt64(owedOpen, int64(s.Owed.Open))
		o.ObserveInt64(owedDropped, int64(s.Owed.Dropped))
		o.ObserveInt64(owedArm, int64(s.Owed.ArmEmits))
		o.ObserveInt64(replaceRetries, int64(s.ReplaceRetries))
		return nil
	}, serveTotal, debtOpen, debtParked, oldest, retries, overdue, pushes, unarmed, unresolved, withdrawn, owedOpen, owedDropped, owedArm, replaceRetries)
	return err
}

// int64sToFloat64s converts histogram bounds to the OTel bucket type.
func int64sToFloat64s(in []int64) []float64 {
	out := make([]float64, len(in))
	for i, v := range in {
		out[i] = float64(v)
	}
	return out
}

// learnedCapacityStats is the CLOSED set of numeric inputs of the last
// learned-class bound decision (dispatchers' SetLearnedAdmissionStats map),
// mirrored as snowplow_learned_classes_capacity{stat}. Booleans export as 0/1.
// The string-valued "bound" is not here; it is one-hot over learnedBounds.
var learnedCapacityStats = []string{
	"t_widget_us", "t_ra_us", "t_widget_measured", "t_ra_measured",
	"keepwarm_interval_us", "base_widget_units", "base_ra_units",
	"budget_us", "admitted_cost_us", "distinct_classes",
	"prelatch_budget_us", "prelatch_classes", "prelatch_nav_cost_us",
	"memory_headroom_bytes", "entry_headroom", "avg_entry_bytes",
}

// learnedBounds is the CLOSED set of learnedDecision.bound values.
var learnedBounds = []string{"none", "memory", "engine"}

// registerSecurityInstruments hand-wires the #448 post-roll verification
// counters (previously /debug/vars-only, which needs a user JWT) onto OTLP.
// No expvar->OTLP bridge: each instrument reads the typed accessor the expvar
// closure reads. Attribute hygiene (F8): every attribute is drawn from a
// closed, code-defined set (site/reason, outcome, stat, bound) and none carries an
// identity, a username or a Secret name. Monotonic counters are
// ObservableCounters over their own atomics; nothing is derived by subtracting
// two series (the #413 moved_remark lesson). Registered only when the cache is
// on (see the caller).
func registerSecurityInstruments(m metric.Meter) error {
	// --- #424: identity-class drift declines, per {site, reason} ---
	driftDeclined, err := m.Int64ObservableCounter(
		"snowplow_l1_identity_class_drift_declined_total",
		metric.WithDescription("L1 Puts / re-Puts declined because the writer's identity no longer belongs to the RBAC class the key was minted for, by site {restactions, widgets, seed, refresher} and reason {binding_set, rbac_subgen, no_identity}. Non-zero is expected and benign (a grant/revoke landing mid-resolve); it is the evidence the #424 guard fires."))
	if err != nil {
		return err
	}

	// --- #444: refresher representative re-pick outcomes ---
	repick, err := m.Int64ObservableCounter(
		"snowplow_l1_representative_repick_total",
		metric.WithDescription("Refresher outcomes when an identity-bound cell's recorded representative drifted out of the key's RBAC class (#444), by outcome {group, hitter, evicted}: re-picked the canonical group representative, re-picked a recent hitter, or evicted the cell (no in-class representative). The evictions also count on snowplow_resolved_cache{stat=evict_no_representative_total}."))
	if err != nil {
		return err
	}

	// --- #378: refresher-terminal re-mints (BornAt resets), per class. The
	// closed class set (cache.RemintClasses) is emitted in full, zeros included;
	// the sum is snowplow_resolved_cache{stat=remint_total}. ---
	remint, err := m.Int64ObservableCounter(
		"snowplow_resolved_cache_remint_total",
		metric.WithDescription("L1 refresher-terminal writes that re-minted the cell (reset its BornAt under the same key) because it was inside the lead window [maxAge - L, ...), L = min(TTL, maxAge/2), by class {restactions, widgets, widgetContent, apistage, raFullList}. Read with snowplow_resolved_cache{stat=evict_max_age_warm_customer_total}, which must stay 0 (#378)."))
	if err != nil {
		return err
	}

	// --- #547: customer hits that delivered a KNOWN-STALE body (a cell whose
	// latest dirty mark was not closed by an accepted re-Put), per reason of the
	// closed set (cache.KnownStaleReasons), zeros included. Hand-wired: there is
	// no expvar->OTLP bridge. It is bumped by the very hits that deliver the
	// stale body, so it cannot read 0 while a user reloads a frozen card; the
	// sum is snowplow_resolved_cache{stat=known_stale_served_total}. ---
	knownStaleServed, err := m.Int64ObservableCounter(
		"snowplow_resolved_cache_known_stale_served_total",
		metric.WithDescription("Customer L1 hits that delivered a KNOWN-STALE body - its latest dirty mark ended without an accepted re-Put - by reason {declined, dropped, suppressed, not_servable}. Each such hit also starts (or joins) a demand revalidation under the requester. Healthy = flat; a rise names cells the refresher could not refresh (#547)."))
	if err != nil {
		return err
	}

	// --- #740: reruns an ABSORBED read earned — a read that found the key's
	// revalidation slot held while the cell was still known-stale arms one more
	// attempt — per slot of the closed set (cache.KnownStaleRevalidationSlots),
	// zeros included. Hand-wired (no expvar->OTLP bridge); the expvar twins are
	// snowplow_resolved_cache{stat=known_stale_revalidation_rerun_<slot>_total}. ---
	knownStaleRerun, err := m.Int64ObservableCounter(
		"snowplow_resolved_cache_known_stale_revalidation_rerun_total",
		metric.WithDescription("Known-stale revalidation reruns earned by a read absorbed into an attempt already in flight (that attempt may have read its inputs before the state the reader needs), by slot {background, demand}. One per settle with an absorbed read; never self-looping. #740."))
	if err != nil {
		return err
	}

	// --- #424: subject binding-set digest memo ---
	bsHits, err := m.Int64ObservableCounter("snowplow_binding_set_memo_hits",
		metric.WithDescription("Subject binding-set digest memo hits (#424). Hit ratio = hits / (hits + misses)."))
	if err != nil {
		return err
	}
	bsMisses, err := m.Int64ObservableCounter("snowplow_binding_set_memo_misses",
		metric.WithDescription("Subject binding-set digest memo misses, i.e. cold digest builds (#424)."))
	if err != nil {
		return err
	}
	bsRefused, err := m.Int64ObservableCounter("snowplow_binding_set_memo_refused",
		metric.WithDescription("Subject binding-set digest memo cap-breach inserts (digest computed, not cached) (#424)."))
	if err != nil {
		return err
	}
	bsEntries, err := m.Int64ObservableGauge("snowplow_binding_set_memo_entries",
		metric.WithDescription("Live entries in the current binding-set memo shard (#424)."))
	if err != nil {
		return err
	}

	// --- #262: learned identity classes (all current-state gauges) ---
	gauge := func(name, desc string) (metric.Int64ObservableGauge, error) {
		return m.Int64ObservableGauge(name, metric.WithDescription(desc))
	}
	lRegistered, err := gauge("snowplow_learned_classes_registered",
		"Learned identity classes in the registry (#262).")
	if err != nil {
		return err
	}
	lSeeded, err := gauge("snowplow_learned_classes_seeded",
		"Learned classes the last bound decision admitted with at least one distinct target (#262). ~0 on group-only clusters.")
	if err != nil {
		return err
	}
	lUnseeded, err := gauge("snowplow_learned_classes_unseeded_capacity",
		"Distinct learned classes the last bound decision left out for capacity (#262).")
	if err != nil {
		return err
	}
	lNavOnly, err := gauge("snowplow_learned_classes_nav_only",
		"Learned classes the last boot pass seeded pre-latch whose RA-phase admission then dropped them (#262).")
	if err != nil {
		return err
	}
	lFromSecrets, err := gauge("snowplow_learned_classes_from_secrets",
		"Learned classes backed by a clientconfig Secret (#262). ALARM: 0 while snowplow_learned_clientconfig_secrets > 0 (authn's certificate format moved).")
	if err != nil {
		return err
	}
	lUnparseable, err := gauge("snowplow_learned_classes_secrets_unparseable",
		"clientconfig Secrets no class could be read from (#262).")
	if err != nil {
		return err
	}
	lClientconfig, err := gauge("snowplow_learned_clientconfig_secrets",
		"Every *-clientconfig Secret the registry sees: the denominator of the from_secrets alarm (#262).")
	if err != nil {
		return err
	}
	lCapacity, err := gauge("snowplow_learned_classes_capacity",
		"Measured inputs of the last learned-class bound decision, labelled by stat (closed set; booleans as 0/1) (#262).")
	if err != nil {
		return err
	}
	lBound, err := gauge("snowplow_learned_classes_capacity_bound",
		"Which bound limited the last learned-class decision: 1 on the active bound {none, memory, engine}, 0 on the others; no series before the first decision (#262).")
	if err != nil {
		return err
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, c := range dispatchers.IdentityClassDriftDeclinedCells() {
			o.ObserveInt64(driftDeclined, c.Count, metric.WithAttributes(
				attribute.String("site", c.Site), attribute.String("reason", c.Reason)))
		}
		for _, c := range dispatchers.RepresentativeRepickCells() {
			o.ObserveInt64(repick, c.Count, metric.WithAttributes(attribute.String("outcome", c.Outcome)))
		}
		for _, c := range cache.ResolvedCacheRemintByClass() {
			o.ObserveInt64(remint, int64(c.Count), metric.WithAttributes(attribute.String("class", c.Class)))
		}
		byReason := cache.KnownStaleServedByReason()
		for _, reason := range cache.KnownStaleReasons {
			o.ObserveInt64(knownStaleServed, int64(byReason[reason]), metric.WithAttributes(attribute.String("reason", reason)))
		}
		bySlot := cache.KnownStaleRevalidationRerunBySlot()
		for _, slot := range cache.KnownStaleRevalidationSlots {
			o.ObserveInt64(knownStaleRerun, int64(bySlot[slot]), metric.WithAttributes(attribute.String("slot", slot)))
		}

		hits, misses, refused, entries := rbac.BindingSetMemoSnapshot()
		o.ObserveInt64(bsHits, int64(hits))
		o.ObserveInt64(bsMisses, int64(misses))
		o.ObserveInt64(bsRefused, int64(refused))
		o.ObserveInt64(bsEntries, int64(entries))

		registered, fromSecrets, unparseable := cache.LearnedClassCounts()
		seeded, unseeded := cache.LearnedAdmissionStats()
		o.ObserveInt64(lRegistered, int64(registered))
		o.ObserveInt64(lSeeded, int64(seeded))
		o.ObserveInt64(lUnseeded, int64(unseeded))
		o.ObserveInt64(lNavOnly, int64(cache.LearnedNavOnly()))
		o.ObserveInt64(lFromSecrets, int64(fromSecrets))
		o.ObserveInt64(lUnparseable, int64(unparseable))
		o.ObserveInt64(lClientconfig, int64(cache.LearnedClientconfigSecrets()))
		if capacity := cache.LearnedAdmissionCapacity(); capacity != nil {
			for _, stat := range learnedCapacityStats {
				if v, ok := capacityInt64(capacity[stat]); ok {
					o.ObserveInt64(lCapacity, v, metric.WithAttributes(attribute.String("stat", stat)))
				}
			}
			active, _ := capacity["bound"].(string)
			for _, b := range learnedBounds {
				var v int64
				if b == active {
					v = 1
				}
				o.ObserveInt64(lBound, v, metric.WithAttributes(attribute.String("bound", b)))
			}
		}
		return nil
	}, driftDeclined, repick, remint, knownStaleServed, knownStaleRerun, bsHits, bsMisses, bsRefused, bsEntries,
		lRegistered, lSeeded, lUnseeded, lNavOnly, lFromSecrets, lUnparseable, lClientconfig,
		lCapacity, lBound)
	return err
}

// capacityInt64 converts one learned-capacity map value to int64; ok is false
// for a type the map never carries (the stat is then not observed).
func capacityInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// derivedFamily is one tag-derived stats family's instruments: a shared,
// stat-labelled counter for the family's counters when it has one
// (snowplow_crd_discovery, snowplow_refresher), and one instrument per stat
// otherwise (snowplow_refresh_broadcaster_*, the refresher gauges).
type derivedFamily struct {
	fam     cache.StatFamily
	shared  metric.Int64ObservableCounter
	perStat map[string]metric.Observable
}

// newDerivedFamilies creates every instrument the tagged families declare
// and returns them with the flat observable list RegisterCallback needs —
// an instrument observed but not registered is silently dropped by the SDK,
// which is how the C12 mirror shipped dead.
func newDerivedFamilies(m metric.Meter) ([]derivedFamily, []metric.Observable, error) {
	var out []derivedFamily
	var observables []metric.Observable
	for _, f := range cache.TaggedStatFamilies() {
		d := derivedFamily{fam: f, perStat: map[string]metric.Observable{}}
		for _, s := range f.Specs {
			if f.OTelShared(s) {
				if d.shared == nil {
					inst, err := m.Int64ObservableCounter(f.OTelName, metric.WithDescription(f.Desc))
					if err != nil {
						return nil, nil, err
					}
					d.shared = inst
					observables = append(observables, inst)
				}
				continue
			}
			name := f.OTelInstrumentName(s)
			desc := metric.WithDescription(s.Desc)
			var inst metric.Observable
			var err error
			switch {
			case s.Float && s.Kind == "gauge":
				inst, err = m.Float64ObservableGauge(name, desc)
			case s.Float:
				inst, err = m.Float64ObservableCounter(name, desc)
			case s.Kind == "gauge":
				inst, err = m.Int64ObservableGauge(name, desc)
			default:
				inst, err = m.Int64ObservableCounter(name, desc)
			}
			if err != nil {
				return nil, nil, err
			}
			d.perStat[s.Stat] = inst
			observables = append(observables, inst)
		}
		out = append(out, d)
	}
	return out, observables, nil
}

// observe reads the family's live values once and records every stat on
// its instrument.
func (d derivedFamily) observe(o metric.Observer) {
	vals := d.fam.Values()
	for _, s := range d.fam.Specs {
		v := vals[s.Stat]
		if d.fam.OTelShared(s) {
			o.ObserveInt64(d.shared, statInt64(v), metric.WithAttributes(attribute.String("stat", s.Stat)))
			continue
		}
		switch inst := d.perStat[s.Stat].(type) {
		case metric.Int64ObservableCounter:
			o.ObserveInt64(inst, statInt64(v))
		case metric.Int64ObservableGauge:
			o.ObserveInt64(inst, statInt64(v))
		case metric.Float64ObservableCounter:
			o.ObserveFloat64(inst, statFloat64(v))
		case metric.Float64ObservableGauge:
			o.ObserveFloat64(inst, statFloat64(v))
		}
	}
}

func statInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

func statFloat64(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return 0
}

// buildLabel normalises the build string for the snowplow_build_info
// version attribute. An empty build — a local `go build` without the
// linker flag — becomes "unknown" rather than "", so a dashboard panel
// never renders a blank label that reads as a scrape failure. Matches
// the expvar route's normalisation in build_info_expvar.go, so the two
// surfaces report the same thing for the same binary.
func buildLabel(build string) string {
	if build == "" {
		return "unknown"
	}
	return build
}

// registeredGVRCount returns the number of GVRs with a registered informer,
// mirroring the `count` field of snowplow_plurals_registered_gvrs. Returns
// 0 when the global watcher is nil (cache off / not wired).
func registeredGVRCount() int64 {
	rw := cache.Global()
	if rw == nil {
		return 0
	}
	return int64(len(rw.RegisteredGVRs()))
}
