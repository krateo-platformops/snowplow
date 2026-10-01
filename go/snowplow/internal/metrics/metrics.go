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
		opts = append(opts, otlpmetrichttp.WithEndpointURL(ep))
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

	// --- discovery: SA-discovery client (one counter keyed by stat) ---
	saDiscovery, err := m.Int64ObservableCounter("snowplow_sa_discovery",
		metric.WithDescription("SA-discovery client counters, labelled by stat."))
	if err != nil {
		return err
	}

	// --- discovery: compiled-CRD-schema memo (one counter keyed by stat) ---
	crdSchemaMemo, err := m.Int64ObservableCounter("snowplow_crd_schema_memo",
		metric.WithDescription("Compiled-CRD-schema memo counters, labelled by stat."))
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
	uafRAFullListBypass, err := m.Int64ObservableCounter(
		"snowplow_ra_full_list_uaf_bypass_total",
		metric.WithDescription("raFullList serves that bypassed the UAF-narrowed cache path (A-1 mitigation)."))
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
		metric.WithDescription("Constant 1, labelled with the snowplow build (git short commit)."))
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
		} {
			o.ObserveInt64(saDiscovery, int64(v),
				metric.WithAttributes(attribute.String("stat", stat)))
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
		o.ObserveInt64(uafRAFullListBypass, int64(cache.RAFullListUAFBypass()))

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
		return nil
	}, append([]metric.Observable{
		fallthroughTotal, assertionViolations, rbacPublishSeq,
		rbacSubGenBumps, rbacSubGenSubjects,
		// --- #260 change-4 ---
		rbacReestablished, rbacWatchError, rbacBumpsBySource, rbacRoleNoop, rbacRoleSemanticNoop, rbacBindingUidChanged,
		bindingNoopUpdates, bindingSemanticNoopUpdates,
		registeredGVRs, prewarmDone, prewarmElapsed,
		memoHits, memoMisses, memoSwaps, memoRefused, memoDenyUncached, memoEntries,
		prewarmEngEnqueued, prewarmEngProcessed, prewarmEngYield, prewarmEngPending,
		phase1UnitsPlanned, phase1UnitsSeeded, phase1ApiRefPages, phase1EligibleNoContinue,
		phase1WalkZeroChildren, phase1WalkObservations,
		phase1SeedResolves, phase1SeedFailures, phase1SeedRBACDeny, phase1SeedOpFail,
		saDiscovery, crdSchemaMemo,
		upstreamControllers, upstreamWebhooks,
		raFullListServe, bindingsDeltaSkipped,
		// --- 1.12.4 ---
		uafRestactionsDeclined, uafWidgetsDeclined, uafRAFullListBypass,
		dispatchL1Cells, seedAttributableHits,
		// --- #386 M3 (PR2) ---
		customerResolveInflight,
		fallthroughCells, diagnosticTotal, diagnosticCells, seriesTruncated,
		readyzBackstop, phase1DeadlineReleased, resolvedCache, informerServable, buildInfo,
		// --- 1.12.5 ---
		depsStats, informerFreshness,
		// --- #233 ---
		unparseableCADelegations,
		// --- #311/#293 ---
		malformedDialSkipped,
		// --- #244 ---
		storeFactoryDivergenceAge,
		// --- #239: dirty-mark attribution ---
		dirtyMarkAttributed, dirtyMarkEvents, dirtyMarkSubmits, dirtyMarkUnattributed,
	}, derivedObservables...)...)
	return err
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
