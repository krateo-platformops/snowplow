---
type: Architecture
title: snowplow — observability
description: The runtime observability surface — expvars at /debug/vars, structured slog events, pprof, the OTel export (traces/metrics/logs + audit; default-on since 1.12.4), and the probe endpoints.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [observability, expvar, otel, metrics, logging, pprof]
timestamp: 2026-08-06T00:00:00Z
---

# Observability

Snowplow's runtime observability surface is four things, all on the single HTTP
port the server listens on (`main.go` `server.Addr = :<port>`):

1. **expvars** at `GET /debug/vars` — the metric surface.
2. **structured `slog` events** on stdout — the event log.
3. **pprof** at `GET /debug/pprof/*` — runtime profiling.
4. **OTel export** — traces, metrics and logs to an OTLP collector,
   **default-on since 1.12.4** behind the `OTEL_ENABLED` master switch (see below).

Plus the diagnostic endpoints `GET /debug/servable`, `GET /debug/apistage`, `GET /debug/harvest`,
`GET /debug/refreshes` (the live-refresh subscription registry) and
`GET /debug/reconcile` (1.12.6 C3 — the on-demand full reconcile audit, see the
dependency-tracker section), and two probe endpoints used by the chart.

> **Every `/debug/*` route needs a JWT (since 1.12.3).** The whole surface —
> pprof, vars, servable, apistage, refreshes — is registered by
> `registerDebugRoutes` (`debug_routes.go`) behind `middleware.RefreshAuth`,
> the stateless header-or-cookie RS256 gate `/refreshes` uses. Before 1.12.3
> only `/debug/refreshes` was gated (#69) and the rest were world-readable on
> the chart's LoadBalancer Service. Pass the token explicitly:
>
> ```bash
> curl -H "Authorization: Bearer $TOKEN" http://pod:8081/debug/vars
> ```
>
> No token, an expired token, or a token supplied only in the query string →
> `401`. A JWKS the pod cannot fetch → `503`. `/health` and `/readyz` stay
> anonymous — the kubelet presents no credentials. Operator recipes are in
> [operating.md](../../howto/operating.md).

The probe endpoints:

| Path | Handler | Returns | Meaning |
|---|---|---|---|
| `GET /health` | `internal/handlers/health.go` | always `200 {"status":"alive"}` — a static pre-encoded body, zero allocation, no apiserver read | liveness only — process is up; a still-warming pod is alive and must NOT be restarted |
| `GET /readyz` | `internal/handlers/readyz.go` | `200 {"status":"ready"}` once `cache.IsPhase1Done()`, else `503 {"status":"warming"}` | readiness — safe to receive traffic. Since the prewarm-gated-readiness reversal this flips only after the synchronous boot seed (or its fire-regardless backstop) — a pod can be **Ready-degraded** (flipped via backstop with cold cells); `snowplow_readyz_backstop_fired` distinguishes that from a healthy boot |

The chart wires **livenessProbe → `/health`**, **startupProbe → `/health`**
(long `failureThreshold` budget), and **readinessProbe → `/readyz`**
(`helm/snowplow/values.yaml`).

---

## OTel export (default-on since 1.12.4)

`main.go` wires three gated pipelines at boot, all no-ops unless enabled:

| Env | Meaning |
|---|---|
| `OTEL_ENABLED` | master switch (default `false`) |
| `OTEL_TRACING_ENABLED` | gate tracing (defaults to the value of `OTEL_ENABLED`) |
| `OTEL_METRICS_ENABLED` | gate metrics (defaults to the value of `OTEL_ENABLED`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | collector OTLP/HTTP endpoint (standard contract) |

- **Traces** (`internal/tracing/tracing.go` `Setup`): registers a global
  TracerProvider + the W3C trace-context/baggage propagators; the whole mux is
  wrapped in `otelhttp.NewHandler` (no-op spans when disabled). The CORS
  options in `main.go` allow the browser to send
  `traceparent`/`tracestate`/`baggage`.
- **Metrics** (`internal/metrics/metrics.go` `Setup`): an **OTLP mirror of the
  expvar surface** — observable instruments read the same live counter
  snapshots the expvar closures read (families: fallthrough, dispatch L1,
  prewarm/phase-1, refresher, discovery, upstream health, …). The expvar
  surface at `/debug/vars` is unchanged and remains the scrape-free ground
  truth; the OTLP export is additive.
- **Logs + audit** (`internal/logging/logging.go` + `internal/support/audit`):
  an OTLP LogRecord bridge on the shared `otel_logs` plane, plus the audit
  middleware. The audit correlation id (D19a) rides **W3C `baggage`
  (`session.id`)** — minted/accepted by `audit.Middleware()` and serialized
  downstream by the Baggage propagator. It **replaces** the old
  `X-Krateo-Correlation-Id` header entirely; the shortid `X-Krateo-TraceId`
  and the OTel `traceparent` coexist.

---

## Cache-off contract (read before interpreting any expvar)

Under `CACHE_ENABLED=false` the cache subsystem does not exist
(transparent-fallback). Almost every `snowplow_*` expvar is registered in an
`init()` guarded by `if cache.Disabled() { return }`, so under cache-off **the
key is absent from `/debug/vars` entirely** — not present-but-zero. An absent
key under cache-off is expected, not a defect.

The exceptions, **registered unconditionally** in `main.go`'s HTTP bootstrap so
a bench probe gets `0` rather than a missing-key error under cache-off:

- `snowplow_rbac_publish_seq` — `cache.RegisterRBACSnapshotExpvar()`
- `snowplow_authz_memo_*` — `rbac.RegisterAuthzMemoExpvar()`

---

## expvars at `/debug/vars`

Every value is an `expvar.Func` (or counter) evaluated lazily at scrape time
(zero per-`/call` cost). Names are stable for grep/Prometheus tooling. Grouped
by subsystem.

### Fallthrough meter — "is the cache actually serving, or punting to the apiserver?"
Defined in `internal/cache/fallthrough_meter_expvar.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_apiserver_fallthrough_total` | grand-total `uint64` of read requests that **genuinely reached the live apiserver** (client-build, secret-get, the informer-fallthrough-* gate misses, apistage partial-shape GETs, the discovery hop) | climbs during boot/cold; should plateau once warm. A steadily climbing total on a warm pod = cache not covering the live request mix. **Reclassified in 1.12.4:** in-process cache HITs (`resolver-plurals-hit`, `widget-content-hit`, …) no longer count here — on the krateo-057 corpus the lifetime value drops **936,320 → 107,795** (of which customer-scope **22,015 ≈ 2.9/min**; the rest is prewarm-scope). That drop is the fix, not a regression |
| `snowplow_apiserver_fallthrough_cells` | per-cell `map["path\|gvr\|reason"]→uint64` breakdown of the above | use to attribute fallthrough to a specific path/GVR/reason. `resolver-plurals-hit` and the other diagnostic reasons now live **only** in `snowplow_cache_diagnostic_cells` |
| `snowplow_cache_diagnostic_total` / `snowplow_cache_diagnostic_cells` (`fallthrough_meter.go`, 1.12.4) | the six reasons that reach **no** apiserver (`resolver-plurals-hit`, `resolver-plurals-miss` — double-counted against the discovery hop, `widget-content-hit`, `widget-content-miss-per-user-fallback`, `cluster-list-dispatch`, `cluster-list-shape-fallback`), same cell shape | should **dwarf** the fallthrough total ~8:1 on a warm pod — the reclassification visible at a glance. Routing is by enum membership inside the meter; call sites are unchanged |
| `snowplow_resolved_cache` (`resolved_cache_expvar.go`, 1.12.4) | L1 store `Stats()` as `map{stat → value}`; per-stat table below. Mirrored to OTLP as `snowplow_resolved_cache{stat}` | previously reachable only through an INFO summary line the chart's `LOG_LEVEL=warn` suppresses |
| `snowplow_informer_servable` (`servable.go` `ServableCounts`, 1.12.4) | `{registered, synced, servable, watch_broken, confirmed}` | the leading indicator for the `informer-fallthrough-not-synced` cell; `watch_broken > 0` = the stale-delete latch |
| `snowplow_build_info` (`build_info_expvar.go`, 1.12.4) | `{version}` = `main.build` (git short commit; `unknown` for an unstamped local build) | pins every other metric to a commit. Also the `service.version` OTel resource attribute |
| `snowplow_metrics_series_truncated_total` (OTLP only, 1.12.4) | per-family count of `path\|gvr\|reason` series folded into `gvr="__other__"` by the 5000-series OTLP cap | **0**. Non-zero = a cardinality regression (the cap kept it from becoming an incident); `/debug/vars` keeps the uncapped maps |
| `snowplow_assertion_violations_total` | per-check `map[string]→uint64` of architectural-invariant breaches. Keys: `read_paths_scoped` (a `/call`-class route not wrapped with `FallthroughScopeMiddleware`, asserted at boot by `cache.AssertReadPathsScoped()`), `serve_requires_servable` (an authoritative cache HIT was about to be served from a not-servable informer — asserted per-serve in `internal/cache/serve_assert.go`) | **0** for every key. Non-zero = an invariant is broken in prod (logged ERROR, pod stays up). **`serve_requires_servable` > 0 is P1** — never-serve-from-not-synced is the most load-bearing cache guarantee |

#### `snowplow_resolved_cache` stats

| stat | meaning | healthy range |
|---|---|---|
| `entries`, `bytes` | live L1 entries and their byte footprint | under the ceilings below; a flat line at the ceiling = the budget is binding |
| `max_entries`, `max_bytes` | the two ceilings (`RESOLVED_CACHE_MAX_ENTRIES`, `RESOLVED_CACHE_MAX_BYTES`) | configuration echo |
| `hit_total`, `miss_total`, `store_total` | lifetime `Get` hits / misses and `Put` count | hit rate → 100 % warm (`feedback_l1_hit_invariant_is_100_percent`); a miss after a mutation is a defect, not a cold fill |
| `evict_lru_total` | entries evicted because a ceiling was hit — the budget is the binding constraint | **0** on a right-sized pod; climbing = raise the ceiling or the store is oversubscribed |
| `evict_ttl_total` | entries evicted at `RESOLVED_CACHE_TTL_SECONDS` (3600 s) on `Get` — staleness, the outer net | low; every TTL eviction is a body the pipeline did not refresh in an hour |
| `evict_max_age_total` | **1.12.6 C5.** entries evicted on `Get` because they were born more than `RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS` (86400 s) ago, however many times they were re-Put since | low and steady (≈ one per resident cell per day) |
| `evict_delete_total` | entries evicted by invalidation (the dep tracker's DELETE route) — the store-side twin of `snowplow_deps.evict_delete_total` | tracks object deletions |
| `resident_entries`, `resident_bytes`, `max_resident_bytes` | the pinned resident region (Ship 4a): entries the keep-warm sweep keeps out of LRU, and its own byte ceiling | `resident_bytes` under `max_resident_bytes` |
| `resident_pin_total`, `resident_demote_total` | entries pinned into / demoted out of the resident region | demotions low; a burst = the resident ceiling is binding |
| `apistage_store_total`, `apistage_evict_total` | per-class store / evict for `apistage` cells | evicts ≪ stores |
| `widget_content_store_total`, `widget_content_evict_total` | per-class store / evict for `widgetContent` cells | evicts ≪ stores |
| `ra_full_list_store_total`, `ra_full_list_evict_total` | per-class store / evict for `RAFullList` cells | evicts ≪ stores |


### Dispatch L1 lookups — resolved-output cache hit rate
Defined in `internal/handlers/dispatchers/l1_lookup_metrics.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_dispatch_l1_lookups` | `map["<handlerKind>\|<gvr>"→{"hit_total","miss_total"}]`; handlerKind ∈ {restactions, widgets, widgetContent} | high hit ratio on a warm pod. Sustained low hit ratio = prewarm not covering the served mix |
| `snowplow_widget_content_skipped_rbac_sensitive_total` / `snowplow_widget_content_skipped_empty_shell_total` (`widget_content_metrics.go`) | widgetContent fast-path skip counters (RBAC-sensitive widget; empty-shell decline) | informational — attribute content-cell misses |
| `snowplow_widget_skipped_undeclared_extras_put_total` | Puts quarantined by the F6 undeclared-request-extras allowlist | non-zero means a client sends extras the widget author has not declared in `spec.keyExtras` |

### RAFullList serve path — the cheap Go-slice serve for big LISTs
Defined in `internal/cache/bindings_by_gvr_metrics.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_ra_full_list_serve` | `map{hit, repopulate, verified_slice, fallback}` serve-outcome counters for the RAFullList cell | admin's first compositions `/call` should drive `hit`+1 over a warm prewarm-pinned cell; rising `fallback` = the cheap path is not engaging |
| `snowplow_ra_full_list_memo` | per-(RA × sliceShape) sliceability verdict snapshot | diagnostic for the RAFullList failure modes |
| `snowplow_sliceability_reverify` | async sliceability-reverify worker counters | evidence the stuck-false reverify path is firing (informer event → re-verify) |
| `snowplow_bindings_by_gvr_delta_skipped_non_typed` | `uint64` — delta-event objects neither typed nor convertible, and DROPPED | **0**. Non-zero = the bindings-by-GVR index is drifting until the next boot rebuild (a silent-data-staleness canary) |

### Prewarm — boot warm-up completion + walk coverage

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_prewarm_complete` (`internal/cache/prewarm_complete_metric.go`) | `map{done:0/1, elapsed_ms}` — `done=1` once `Phase1Done` flips (same atomic `/readyz` reads); `elapsed_ms` = process-start→done, `-1` until flip | `done` reaches `1`; `elapsed_ms` is the cold-start-to-ready time |
| `snowplow_readyz_backstop_fired` (`internal/handlers/dispatchers/readiness_backstop_metrics.go`) | `int` — readiness flipped via a backstop arm (seed error / timeout / panic / boot error) instead of the first-nav happy path; each fire also emits a `readyz.backstop.fired` ERROR with the reason | **0** on a healthy boot; any non-zero = a FAILED-but-serving (Ready-degraded) boot — alert |
| `snowplow_phase1_units_planned` / `snowplow_phase1_units_seeded` / `snowplow_phase1_apiref_pages_total` / `snowplow_phase1_eligible_no_continue_total` (`phase1_walk_pagination_metrics.go`) | apiRef-pagination walk planning/seeding counters (widgetContent cells planned, seeded, extra pages resolved, single-page widgets) | `units_seeded` reconciles with `units_planned` minus skips; pages ≫ page-cap × widget-count confirms the backstop raise |
| `snowplow_phase1_walk_children` / `snowplow_phase1_walk_zero_children_total` / `snowplow_phase1_walk_observations_total` (`phase1_walk_metrics.go`) | per-root boot-walk fan-out + zero-children observations | high zero-children ratio = roots resolving empty (RBAC/data gap, or the pre-barrier pass — the engine re-walk covers it) |
| `snowplow_phase1_configvars_skipped_total` (`phase1_configvars_watch.go`) | config-vars watch events skipped by the data-change gate (metadata-only churn, e.g. CDC traceparent re-stamps) | climbs harmlessly under CR churn; a boot re-drive fires only on real `config.json` change |

### Prewarm engine — the unified walk/seed worker
Defined in `internal/handlers/dispatchers/prewarm_engine_metrics.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_prewarm_engine_enqueued_total` | `uint64` — cumulative `enqueueScope` calls (every enqueue, even dedup-coalesced) | climbs during boot/re-walk/keepwarm |
| `snowplow_prewarm_engine_processed_total` | `uint64` — scopes fully processed by the worker | `processed ≈ enqueued − dedups` means the queue drained |
| `snowplow_prewarm_engine_requeued_total` | `uint64` — engine-owned boot-scope requeues (the F.4 deadline-cut resume) | small; unbounded climb = the resume bound regressed |
| `snowplow_prewarm_engine_yield_total` | `uint64` — worker parked because a customer `/call` was in flight (customer-priority yield) | >0 under customer load = the yield hook is working |
| `snowplow_prewarm_engine_pending_depth` | live queue depth | **0** once the worker drains; sustained non-zero across many scrapes = worker dead |

### Phase-1 seed — per-target seed outcomes
Defined in `internal/handlers/dispatchers/phase1_pip_metrics.go` (+ siblings).

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_phase1_bindingset_seed_resolves_total` | `uint64` — seed UNITS resolved + written to per-binding L1 | climbs during seed; `0` after boot means no seed unit was written |
| `snowplow_phase1_bindingset_seed_failures_total` | `uint64` — grand-total seed failures (= rbac_deny + operational; back-compat) | interpret via the split below |
| `snowplow_phase1_seed_rbac_deny_total` | `uint64` — EXPECTED narrow-RBAC denies; cohort genuinely can't read the target | non-zero is **normal**; not re-enqueued |
| `snowplow_phase1_seed_operational_fail_total` | `uint64` — UNEXPECTED failures (ctx timeout/cancel, 5xx, transport, panic) | **0**. Non-zero = a real hole; these ARE re-enqueued |
| `snowplow_phase1_seed_fresh_skip_total` | `uint64` — seed units skipped because the cell is still fresh (boot-resume / keepwarm passes) | evidence the resume/sweep is incremental, not redundant |
| `snowplow_phase1_keepwarm_age_skip_total` | `uint64` — keepwarm sweep age-skips (cell young enough) | climbs with sweeps over a warm store |
| `snowplow_phase1_seed_skipped_stage_error_total` | `uint64` — seed Puts declined by the error-aware Put-gate | low; a climb = a systematically-degraded seed target |
| `prewarm.coverage` | map — `{harvested_widgets, harvested_restactions, walked_roots, max_harvested_widgets, regressed_total}` (#220). `max_harvested_widgets` is the process-lived high-water mark of DISTINCT widgets a COMPLETED walk reached; it is the self-adapting baseline, so there is no threshold to tune | `harvested_widgets` should track `max_harvested_widgets`. A gap between them means the walk stopped reaching part of the navigation tree and those pages are a cold first load. `regressed_total` > 0 ⇒ read the `prewarm.coverage.regressed` WARN, which carries both numbers |
| `snowplow_phase1_harvest_forgotten_total` | map keyed by harvester (`nav_widget` / `apiref`) — harvested ENTRIES DROPPED because the object was confirmed gone (1.12.7 F6b/F6c) | `0` on a cluster where nothing is deleted. Climbs only when a deletion actually stopped a replay; counts removals, **not** hook firings, so it does not climb for gone objects that were never harvested. `nav_widget` can move by more than 1 per object (one entry per pagination tuple) |
| `snowplow_phase1_widget_seed_failure_total` / `snowplow_phase1_restaction_seed_failure_total` | per-cohort×object failure maps | pinpoint which widget/RA broke which cohort |
| `snowplow_resolved_cache_hits_seed_attributable` | hits on cells the seed wrote (seed attribution) | the seed-is-actually-useful signal |

### Refresher — background re-resolve worker pool
Defined in `internal/cache/refresher_metrics.go`.

All keys below are derived from the `stat` tags on `refresherStats` /
`RefreshTerminalStats` (1.12.6 C7): counters are `snowplow_refresher_<stat>_total`,
gauges `snowplow_refresher_<stat>`; the OTLP mirror publishes the counters as
`snowplow_refresher{stat=<stat>}` and the gauges under their expvar name. None of them
constructs the refresher: before the first enqueue, and on a cache-off pod, every value is 0
(#203).

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_refresher_enqueue_total`, `snowplow_refresher_completed_total`, `snowplow_refresher_failed_total`, `snowplow_refresher_retried_total`, `snowplow_refresher_dropped_total` | task lifecycle counters. **1.12.6 C4:** `dropped` is terminal — a key dropped after its requeue budget is evicted at the drop point (see `_drop_evict_total`) or, for a confirmed 404, via `evict_self_gone_total` | completed tracks enqueue under steady state; failed/retried/dropped low |
| `snowplow_refresher_skipped_no_entry_total`, `snowplow_refresher_skipped_no_handler_total`, `snowplow_refresher_skipped_stage_error_total` | skip reasons | informational |
| `snowplow_refresher_queue_depth` | live workqueue `Len()` (gauge) | near 0; climbing depth with stagnant `completed_total` = workers stuck (back-pressure) |
| `snowplow_refresher_yielded_total` | worker yield-parked for a customer `/call` | >0 under customer burst (if 0, hook broken) |
| `snowplow_refresher_capped_total` | yield max-parked cap fired (proceeded anyway) | **near 0**; steady climb = inflight counter leaking or sustained pressure |
| `snowplow_refresher_floored_total` | dequeue rate-floor deferred a key (entry younger than floor) | >0 under install-churn storm = the floor gate is protecting against re-resolve storms |
| `snowplow_refresher_drop_evict_total` | **1.12.6 C4.** entries evicted at the drop point after a deterministic **non-404** failure exhausted the requeue budget and the breaker granted a token; tracker-side twin `evict_drop_point_total` under `snowplow_deps` | **0** on a healthy apiserver |
| `snowplow_refresher_drop_evict_suspended_total` | **1.12.6 C4 — ALERT.** drop-point evictions the breaker (`REFRESH_DROP_EVICT_MAX_PER_MINUTE`, default 64) refused: a mass failure is in progress, the entries stayed resident and are still served (availability over freshness; bounded by the TTL) | **0**. Non-zero = an outage-shaped failure burst, not a cache defect |
| `snowplow_refresher_suppressed_set_total` | **1.12.6 C4 / #191.** keys marked refresh-by-traffic-only after `REFRESH_SUPPRESS_AFTER_DECLINES` (default 3) consecutive identity-bound declines (first occurrence for external-endpoint and UAF cells) | low; each is one WARN → DEBUG transition |
| `snowplow_refresher_suppressed_skips_total` | **1.12.6 C4 — ALERT pair.** refresh ticks skipped on a suppressed key (the #191 cure working) | proportional to suppressed keys × keep-warm ticks |
| `snowplow_refresher_suppressed_keys` | live count of suppressed keys (gauge; cleared by the next real Put or eviction) | bounded by the store |

### Live refresh (SSE)
Defined in `internal/cache/refresh_broadcaster_expvar.go`; one expvar key,
`snowplow_refresh_broadcaster`, a `map{stat → value}` derived from the `RefreshBroadcasterStats`
tags (1.12.6 C7). The OTLP mirror publishes each stat as
`snowplow_refresh_broadcaster_<stat>` (counters gain `_total`).

| stat | meaning | healthy range |
|---|---|---|
| `published` | live-refresh signals published by the resolver / refresher on an L1 commit | tracks L1 commits under churn |
| `delivered` | signals delivered into a subscriber's sink | tracks published × armed subscribers |
| `dropped` | signals dropped because a subscriber's sink was full (slow consumer); the subscriber receives a terminal degrade signal instead and must refetch | **near 0**; sustained > 0 = a wedged consumer |
| `coalesced` | signals collapsed by the per-key 250 ms coalesce window | informational |
| `subscribers` | live `/refreshes` connections (gauge) | tracks open tabs |
| `armed_keys` | **1.12.6 C11.** distinct L1 keys with ≥1 armed subscriber — the reverse-index size (gauge) | ≈ subscribers × keys per page; ≈95 MB at 3000 × 93 (0.4 % of the pod) |
| `max_sink_depth` | **1.12.6 C12.** high-water mark of a subscriber sink after a send — consumer lag, 0..64 (gauge) | low; near 64 = a consumer about to drop |
| `evict_published` | **1.12.6 C10.** DELETE-semantics evictions (informer DELETE, confirmed self-404) that reached ≥1 armed subscriber as a `refresh` frame. TTL, LRU, max-age and non-404 drop-point evictions stay silent by design | moves with deletions on watched pages |
| `evict_deferred` | **1.12.6 C10.** eviction frames the per-subscriber token bucket (`REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND` 5, `_BURST` 10 — provisional) deferred into the pending set; never dropped, drained at the pace | > 0 only during a bulk delete — the proof the bound engaged |
| `stream_seconds_total` | **1.12.6 C12.** accumulated `/refreshes` stream lifetime (seconds) | ÷ `streams_closed_total` = mean stream lifetime (live: ≈3,336 s; a collapse means something upstream is cutting streams, see the delivery-failure matrix row G1) |
| `streams_closed_total` | **1.12.6 C12.** streams that ended (client close, hub reset, pod restart) | tracks reconnects |

The key-space mismatch detector (loss modes L7/L8): `delivered == 0` while `subscribers > 0`
and `published > 0` for longer than the coalesce window means the armed keys and the published
keys are not the same keys — an identity drift on a long stream, or an SPA key-derivation
change. No arm pins it (see the matrix); alert on the rule.

### RBAC snapshot + authz memo — subject-index freshness and serve-time eval cache

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_rbac_publish_seq` (`internal/cache/rbac_snapshot_expvar.go`) | `uint64` — incremented once per successful RBAC-snapshot publish (also the point per-subject sub-generation bumps land) | bumps within ~30s of a RoleBinding ADD/DELETE; `0` = no snapshot published (cache-off or pre-readiness) |
| `snowplow_authz_memo_hits` / `_misses` / `_swaps` / `_refused` / `_entries` (`internal/rbac/snapshot_authz_memo.go` via `RegisterAuthzMemoExpvar`) | memo hit/miss, generation shard swaps, cap-breach refusals, live entry count | hit rate ≥0.85 warm; swaps bump on snapshot generation change; refused low |
| `snowplow_authz_memo_deny_uncached_total` | `uint64` — denies (never cached, by design) | informational; should be > 0 and rising on a live cluster — a flat 0 with denied traffic would suggest the PERMITS-only rule regressed |

### Informer / discovery surface

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_plurals_registered_gvrs` (`internal/cache/registered_gvrs_expvar.go`) | `{count, gvrs:[…], last_register_unix_ns}` — live set of GVRs with a registered informer | `count` tracks the cluster's served GVR set; two scrapes with identical `last_register_unix_ns` = informer set quiesced |
| `snowplow_crd_discovery` (`internal/cache/crd_discovery_expvar.go`) | `map{stat → value}` derived from the `CRDDiscoveryStats` tags (1.12.6 C7); mirrored to OTLP as `snowplow_crd_discovery{stat}`. Per-stat table below | `events_dropped`/`*_skipped_ng`/`panics_recovered` should be **0** |
| `snowplow_crd_schema_memo_hits_total` / `_misses_total` / `_stale_dropped_total` / `_invalidations_total` (`internal/resolvers/crds/schema/schema_cache_metrics.go`) | compiled-CRD-schema memo counters | high hit ratio warm; stale-drops expected under concurrent CRD install |
| `snowplow_sa_discovery_builds_total` / `_invalidations_total` / `_fallbacks_total` (`internal/dynamic/cached_client_metrics.go`) | SA-discovery client lifecycle | fallbacks low; climb = discovery degrading |

#### `snowplow_informer_watch` stats

Two 1.12.7 counters for informer failures that previously left no trace anywhere but a log line.
Tag-derived (`InformerWatchStats`), so they reach expvar, OTLP, this doc's guard and the C7 parity
arms from one struct tag each.

| stat | meaning | healthy range |
|---|---|---|
| `watch_errors_total` | reflector `ListAndWatch` errors across **every** informer family (per-GVR, secrets, controller-health). Counts EVERY invocation — our handlers replace client-go's default and each logs only its FIRST failure, so before this a watch failing once and a watch failing every second produced the same single WARN | **0**. A climbing value is the retry RATE, which is the thing the one-shot WARN hides. Read it next to `cache.watch.broken` / `cache.secrets.watch.broken` |
| `confirm_retracted_total` | GVRs whose servability confirmation was retracted **after having been granted**. A retracted GVR silently stops serving from the informer and falls through to the apiserver; #217 took a day to characterise because the retraction left no trace. Counted only when a confirmation actually existed, so ordinary teardowns of never-confirmed GVRs do not inflate it | **0** in steady state. Non-zero is expected around a CRD upgrade or delete — use the by-reason map below to tell which |

`snowplow_informer_confirm_retracted_by_reason` is the `{reason}` breakdown, a map keyed by the
code path that retracted: `discovery_refresh` / `scoped_confirm` / `walk_confirm` (the apiserver no
longer serves the type) and `schema_relist` / `stale_version_pruned` / `crd_deleted` (the informer
was torn down). It rides alongside rather than inside the tagged family because the C7 tag system
has no label facility — a `stat` tag yields exactly one scalar.

#### `snowplow_crd_discovery` stats

| stat | meaning | healthy range |
|---|---|---|
| `events_enqueued`, `events_processed` | CRD lifecycle events (ADD/UPDATE/DELETE) enqueued to / processed by the single discovery worker | processed tracks enqueued |
| `events_parked` | **1.12.5.** submits that had to wait on a full queue (the worker fell 256 events behind; the informer processor goroutine parks up to 30 s) | 0 on a stable cluster; bursts during a bulk CRD install |
| `events_dropped` | lifecycle events dropped after the park deadline — the last resort. A dropped DELETE means an informer is never torn down and its dependent L1 entries stay resident until TTL | **0** |
| `discovery_invoked` | ADD/UPDATE passes that ran `DiscoverGroupResources` | tracks CRD churn |
| `discovery_skipped_ng` | ADD/UPDATE decode-skip / no-group / no-SA-rc | **0** — a non-zero value is the silent-skip defect class |
| `deletes_processed` | successful DELETE teardowns (informer removed, dependents dirty-marked) | tracks CRD deletions |
| `delete_skipped_ng` | DELETE decode-skip / no-served-versions / no-plural | **0** |
| `panics_recovered` | discovery passes that panicked (recovered; the worker survives) | **0** |
| `schema_relists_fired` | ADD/UPDATE passes that relisted ≥1 GVR on a detected structural-schema change | tracks CRD schema churn |
| `schema_unchanged` | ADD/UPDATE where the schema fingerprint was unchanged (thrash guard; no relist) | informational |
| `stale_version_pruned_total` | **1.12.7 F2 (#219).** per-GVR state (informer, sync channel, confirmation, watch-broken, last-sync RV, dep edges) torn down because the CRD stopped serving that version — the informer is **stopped**, not merely forgotten | non-zero is **normal** on a cluster that upgrades components: every upgrade mints a new API version and retires the previous one. Read it against `watch_broken`, which before 1.12.7 climbed monotonically (measured 4 → 40 over 24 h on krateo-057, zero decreases) because retired versions were never pruned |
| `relist_dirtymark_postsync_total` | **1.12.5.** post-sync re-fires of the relist dirty-mark (the containment for the teardown window; stays until the bridge below has soaked) | tracks `schema_relists_fired`; lagging it = relisted informers are not syncing |
| `relist_postsync_timeout_total` | **1.12.5.** relisted GVRs whose new informer did not sync in time (the re-fire could not run) | **0** |
| `relist_bridge_runs_total` | **1.12.6 C2.** relist delta bridges spawned (one per relisted GVR with a registered indexer): the old indexer's key set is snapshotted before teardown and diffed against the fresh LIST | tracks `schema_relists_fired` |
| `relist_bridge_enqueued_total` | **1.12.6 C2.** coordinates the bridge synthesised (old keys the fresh LIST omits) and handed to the dep-event worker, which probes ABSENT and evicts — the DELETEs a fresh informer never emits | moves only when objects vanish inside a teardown window |
| `relist_bridge_timeout_total` | **1.12.6 C2 — the SOAK SIGNAL.** bridges that gave up because the fresh informer never synced (`RELIST_BRIDGE_TIMEOUT_SECONDS`). The 1.12.5 re-fire is retired only after this has stayed at **0** across real CRD schema changes on a healthy cluster; > 0 means the bridge is not covering the window and the re-fire must stay | **0** |
| `relist_bridge_aborted_total` | **1.12.6 C2.** bridges with nothing to diff (no sync channel / GVR removed while waiting) | 0; small on CRD deletions |

### Dependency tracker + DELETE eviction bridge — "is invalidation actually happening?" (1.12.5)
Defined in `internal/cache/deps_expvar.go`; counters in `deps.go` (`DepStats`) and
`deps_watch.go` (`DepWatchStats`).

Before 1.12.5 every number below was computed and then emitted **only** into the periodic
`resolved_cache.summary` INFO line, which the chart's `LOG_LEVEL=warn` discards. That is why
issue #187 — a CR deleted and recreated under the same name kept serving its pre-delete
resolved body — could not be diagnosed from the live pod: the eviction counter, the
dirty-mark counter and the DELETE queue depth all existed and none of them were readable.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_deps` | `map{stat → value}`; one gauge keyed by stat, same idiom as `snowplow_resolved_cache` | see the three rows below |

The stats that answer an invalidation question:

| stat | meaning | healthy range |
|---|---|---|
| `records`, `max_records`, `record_total` | live dep records (an L1 key ↔ object edge), the `DEPS_MAX_RECORDS` ceiling, and the cumulative number recorded | `records` well under `max_records` |
| `dropped_cap` | dep edges **silently dropped** because `records` hit `max_records`: the entry becomes dirty-markable-but-not-evictable (the #187 H4 shape) and only its TTL or the max-age bound can remove it. The reconcile audit counts these under `reconcile_skipped_no_edge_total`; it cannot repair them | **0** — alert on it and raise `DEPS_MAX_RECORDS` |
| `dropped_no_key` | edge registrations that arrived with an empty L1 key (a caller bug; counted, never stored) | **0** |
| `remove_l1_total` | L1 keys whose dep records were removed (entry evicted or re-Put with a new edge set) | tracks evictions |
| `evict_delete_total` | L1 entries evicted by an **informer DELETE** (`OnDelete` bucket 1). Deliberately does NOT include the refresher's self-404 evictions — one counter, one meaning | moves whenever a CR with a live L1 entry is deleted. **Frozen across a known deletion = DELETE handling is not reaching the store** — the strongest single test, and it only works because this counter is not folded |
| `evict_self_gone_total` | L1 entries evicted because the refresher's re-fetch of their own object returned a confirmed **404** across the **whole** requeue budget — 404 ONLY; the 1.12.6 non-404 drop-point evictions are `evict_drop_point_total` below, so this counter keeps meaning "the object is confirmed gone" even during an apiserver outage | non-zero is **normal** on a cluster that deletes CRs |
| `evict_drop_point_total` | **1.12.6 C4.** L1 entries evicted at the refresher drop point after a deterministic **non-404** failure (403/500/timeout/parse/not-servable) exhausted the requeue budget and the breaker granted a token. Tracker-side twin of `snowplow_refresher_drop_evict_total` (they differ only when the entry had already gone by another route). Counted apart from both `evict_delete_total` and `evict_self_gone_total` — one counter, one meaning | **0** on a healthy apiserver. Climbing = deterministic non-404 failures are being evicted; if `snowplow_refresher_drop_evict_suspended_total` climbs with it, a mass failure is in progress and the breaker is holding the rest resident |
| `relist_dirtymark_postsync_total` | CRD schema relists that re-fired the dependent dirty-mark after the new informer synced | tracks CRD schema churn; 0 on a stable cluster |
| `delete_worker_panics_total` | DELETE events whose `OnDelete` panicked. Each one is exactly one LOST eviction; the worker survives and drains the next event | **0**. Non-zero means at least one object's L1 entry is stale until TTL, and the `deps.delete_worker.panic` WARN names which |
| `dep_event_queue_depth` | coordinates pending on the dep-event workqueue (1.12.6 C1; typed, dedup'd, unbounded — the 1.12.5 `delete_queue_depth`/`_cap`/`_full_total` surface is retired, there is no overflow case) | near 0. Persistently high = the drain is behind |
| `events_submitted_total` | coordinates enqueued by the three informer handlers | tracks cluster churn |
| `probe_exists_total` / `probe_absent_total` | actions derived from an authoritative indexer read: exists → dirty-mark, absent → evict self | track churn; `absent` moves with deletions |
| `probe_unknown_total` | probes where the indexer was **not authoritative** for the GVR (relist teardown window, unsynced informer, broken watch, unconfirmed type) → requeued with backoff | small bursts around CRD schema churn are normal |
| `probe_unknown_degraded_total` | coordinates that stayed unknown for the whole requeue budget and were degraded to a dirty-mark (the refresher then decides against the apiserver) | **0**. Non-zero means an informer is not recovering — alert on it |
| `on_object_event_degraded_no_evict_total` | **1.12.7** — the CONSEQUENCE of the row above, which that counter does not carry: a degraded verdict that reached at least one dependent entry and therefore evicted **nothing**, deferring the eviction decision to the refresher. Counted per event, and only when the match set was non-empty — a degraded verdict naming a coordinate nothing depends on cost nothing | **0**. Read it against `probe_unknown_degraded_total`: that one counts budget exhaustions, this one counts the ones that had something at stake. Non-zero means entries are being held resident on an informer's say-so that the informer could not give |
| `self_notfound_evict_total` | refresher-side count of times the drop-point eviction branch fired. Differs from `evict_self_gone_total` only when the entry had already gone by another route | non-zero is **normal**. It is the healthy replacement for the `refresher.refresh_failed` ×5 + `refresher.refresh_dropped` pair that used to leave the body resident |
| `evict_max_age_total` (under `snowplow_resolved_cache`) | **1.12.6.** entries evicted on `Get` because they were born more than `RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS` ago, however many times they were re-Put since | low and steady (≈ one per resident cell per day at the default). A burst right after a pod's first 24 h is the boot seed's cohort aging out together and is expected |
| `snowplow_refresher_drop_evict_total` | **1.12.6.** entries evicted at the drop point after a deterministic **non-404** failure exhausted the requeue budget (403/500/timeout/parse/not-servable); the `refresher.refresh_dropped` WARN now says `entry EVICTED`. OTLP twin: `snowplow_refresher{stat="drop_evict"}` | non-zero is normal on a cluster with dead objects or broken RESTActions. Every one is an entry that used to sit stale until the 1 h TTL |
| `snowplow_refresher_drop_evict_suspended_total` | **1.12.6.** drop-point evictions refused by the breaker (`REFRESH_DROP_EVICT_MAX_PER_MINUTE`); the key kept the old drop-to-TTL. OTLP twin: `snowplow_refresher{stat="drop_evict_suspended"}` — the alert lives on that series, not on `/debug/vars` | **0** on a healthy apiserver. Climbing = a mass failure is in progress (one `refresher.drop_evict_suspended` WARN per window names it) or the budget is too low |
| `snowplow_refresher_suppressed_set_total` / `_suppressed_keys` / `_suppressed_skips_total` | **1.12.6 (#191).** keys marked refresh-by-traffic-only after K consecutive declines (or one permanent decline), the live count of such keys, and dequeues skipped because of the marker. OTLP twins: `snowplow_refresher{stat="suppressed_set"}` / `{stat="suppressed_skips"}` (counters) and the `snowplow_refresher_suppressed_keys` gauge (it drops on `Put` and eviction, so it cannot ride the cumulative counter) | `suppressed_skips_total` climbing while the stage-error WARN is **flat** is the #191 cure working; `suppressed_keys` is bounded by the store — it drops on `Put` and eviction |
| `dirty_mark_total` / `enqueue_update_total` | stale-while-revalidate marks from ADD/UPDATE and from DELETE buckets 2/3 | tracks cluster churn |
| `add_propagated` / `add_dropped_pre_sync` / `add_nil_syncch` | the ADD initial-replay gate | `add_nil_syncch` should be **0** (a registration path skipped the syncCh allocation; dep marks for that GVR degrade to TTL) |
| `reconcile_divergence_total` | **1.12.6 C3** — coordinates the sampled reconcile audit found whose own object is ABSENT from a synced indexer, that a resident entry still holds a self dep edge for, and that no event had evicted: each one is a DELETE the pipeline lost, re-derived from the indexer and handed to the dep-event worker (`internal/cache/deps_reconcile.go`). Entries the worker could NOT evict (no edge — see `reconcile_skipped_no_edge_total`) are kept out of it | **0** on a healthy cluster. A rate that does not fall back to zero between ticks means a handler, the queue or the relist bridge is dropping events — alert on it. The per-tick `cache.deps_reconcile.divergence` WARN names up to three coordinates |
| `reconcile_sampled_total` / `reconcile_probed_total` / `reconcile_ticks_total` | resident entries visited by the audit / unique self coordinates probed (the divergence denominator — cohort copies of one widget share one probe; LIST entries are visited, not probed) / ticks run. Defaults `DEPS_RECONCILE_SAMPLE=512` every `DEPS_RECONCILE_PERIOD_SECONDS=30` (`"0"` disables the ticker) | `sampled` grows by ≤ 512 per tick, `probed` by ≤ `sampled`; `ticks` grows by 2/min |
| `reconcile_skipped_no_edge_total` | ABSENT entries the audit found that hold **no self dep edge**, so the worker's ABSENT verdict cannot reach them: the worker's match set for their coordinate is empty and submitting it would evict nothing. Counted here rather than as divergence. **Two distinct causes, and they need different fixes.** (1) `Record` was dropped at `DEPS_MAX_RECORDS` — read `dropped_cap` alongside; the fix is raising the cap. (2) Pre-1.12.7, the edges were stripped by an eviction racing a re-Put, leaving a resident entry with no edge; 1.12.7 F6a closed that by stripping under the same lock as the index delete, so newly-created entries of this shape should not appear. **NOT a statement about repairability** — these entries are unreachable by the *current* repair path, which submits through the worker; a repair that deletes from the store directly reaches them, and that is what F1 is for | **0**. Non-zero with `dropped_cap` also climbing = raise `DEPS_MAX_RECORDS`. Non-zero with `dropped_cap` flat, on 1.12.7 or later, is the population F1 exists to remove and should be reported |
| `reconcile_unknown_total` | coordinates the audit SKIPPED because the indexer was not authoritative for their GVR (unregistered, unsynced, watch broken, relist window). Never guessed | small; persistently large means entries are resident for GVRs that are not watched |
| `reconcile_panics_total` | audit ticks that panicked (recovered; the ticker survives) | **0** |

The audit is the safety net UNDER the event pipeline, not a replacement for it: it runs
O(sample) under the store mutex per tick (`RangeMetadataSample`, a random window of the index —
never the LRU head), probes OUTSIDE that lock (the store and watcher locks are never nested),
and evicts only through the worker's own ABSENT verdict, so `evict_delete_total` moves for its
evictions too.

**Coverage — it is a divergence DETECTOR, not a staleness bound.** Each tick visits a random
window of `DEPS_RECONCILE_SAMPLE` entries, so against a residency of N the chance one entry is
still unvisited after T ticks is ≈ (1 − sample/N)^T. At the defaults (512 every 30 s) and
N = 100K entries the expected first visit is ≈ 195 ticks ≈ **1.6 h**, and 99 % coverage needs
≈ 900 ticks ≈ **7.5 h** — longer than the 3600 s TTL. So for most entries the TTL fires first;
the audit is the backstop for entries that keep being re-Put (keep-warm cells, entries C4
suppression keeps alive) and the detector that turns a lost DELETE into
`reconcile_divergence_total > 0`. Do not expect it to catch a lost DELETE "within a tick". A
tighter bound costs `DEPS_RECONCILE_SAMPLE` (O(sample) under the store mutex per tick): at
4096 / 30 s the 99 % figure is ≈ 56 min at 100K. Go's map iteration samples a random *window*
rather than 512 independent draws, so these figures are slightly optimistic.

`GET /debug/reconcile` (JWT-gated like its siblings) runs ONE full walk on demand and returns
the divergent set as metadata only (key hash / class / gvr / namespace / name — never a body).
It is a reconcile, not a dry run: the entries it lists are evicted by the worker right after.
The full walk is **chunked** (`RangeMetadataBatched`): the store mutex is held once for a key
snapshot and then per batch of 512 entries, each batch is probed outside the lock before the
next is collected, and wall time is capped at 60 s (`truncated: true` past it). A customer
`/call` therefore waits at most one batch, never the whole residency; the body reports
`batches`, `snapshotHoldMicros` and `maxBatchHoldMicros` (also on the
`cache.deps_reconcile.full_walk` INFO line) so the number is measured on every call. Measured
under `-race` on a 20K-entry store: see `TestIssue1126_C3_FU4` in the developer report. Still
something to call when a stranded entry is suspected, not something to poll.

All of it is mirrored to OTLP as the `snowplow_deps` observable gauge, labelled by `stat`
(`internal/metrics/metrics.go`).

### Inspecting the Phase-1 harvesters (1.12.7)
`GET /debug/harvest` answers **"does the harvester still hold this coordinate?"** — the question
the 1.12.7 acceptance step has to settle and that no surface could answer.

    GET /debug/harvest
        counts only: navEntries (harvested ENTRIES, one per pagination tuple, not per widget)
        and apiRefs (distinct RESTActions).
    GET /debug/harvest?group=&version=&resource=&namespace=&name=
        the same counts plus heldByNavHarvester / heldByApiRefHarvester for that coordinate.

`available:false` means **no harvester has been published** — prewarm is off, or boot has not
reached the engine. It is deliberately distinct from "the harvesters are empty", which is a
healthy post-forget state; conflating them would let a misconfigured pod read as a successful
forget.

Why it exists: 1.12.7 F6b/F6c drop a confirmed-gone object from both harvesters, which is what
stops a deleted widget being re-resolved into L1 on every seed pass. Until this endpoint that
state was verifiable in tests and nowhere else.
`snowplow_phase1_harvest_forgotten_total` says a forget HAPPENED; this says what is held NOW.

**Metadata only, structurally.** Every field is an int or a bool. The harvested value is a whole
widget CR and what the seed makes of it is per-identity resolved output, so the same boundary
that keeps bodies off `/debug/apistage` applies here — and the response type has no field that
could carry one.

### Inspecting ONE resolved entry (1.12.5)
`GET /debug/apistage?key_hash=<hex>` returns the metadata row for a single resident entry
instead of the full walk, with two fields populated only on that path: `bodySha256` and the
opaque `bindingUID`. It is how you answer "how old is this entry, and is its body the same one
as before?" in one request — the question #187 had to infer from the client's subsequent child
fetches because no surface could answer it directly.

**`lifetimeSeconds` vs `ageSeconds` (1.12.7).** `ageSeconds` is the age of the BODY, measured
from `CreatedAt` and reset by every refresh re-Put — so a cell the refresher keeps warm reports a
small `ageSeconds` forever, however long it has been resident. `lifetimeSeconds` is the age of the
KEY, measured from `BornAt`, which a re-Put inherits and never resets. It is the value
`RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS` bounds, **and that bound is enforced ON READ** — the check
lives in `Get`, so the entry is evicted by the request that hits it. An entry that is never read
therefore sits past the bound indefinitely without breaching anything. Before this field an
operator could see that an entry was past its TTL but not whether it was past the age the bound
would enforce on its next read.

**It returns a hash, never the body, and that is a security boundary rather than a size
choice.** L1 cells are per-identity: the key folds `BindingUID`, so on krateo-057 one widget
was resident under `admin`, `system:gke-common-webhooks` and `system:kubestore-collector`.
Returning a body to whoever holds the debug JWT would be a cross-identity read of rows that
requester's own RBAC would have filtered. The hash still discriminates "re-resolved" from
"unchanged".

The lookup does NOT route through `Get`: that would enforce TTL, bump the hit counters and move
the entry to the LRU front, so inspecting an entry would change it.

### Informer freshness — "is this indexer still current?" (1.12.5)
Per-GVR on `/debug/servable` (`ServableGVRStatus`); aggregated to OTLP as
`snowplow_informer_freshness`, labelled by stat.

The four servability conjuncts say whether an informer is ALLOWED to serve. None of them says
whether what it holds is CURRENT — `hasSynced` latches true the moment the initial LIST
completes and never goes back. During #187 the central question, "does this indexer still hold
the object the apiserver says is deleted?", had no field on any surface.

| field (per GVR) | meaning | healthy range |
|---|---|---|
| `indexerCount` | objects the informer's store holds right now | compare against `kubectl get <resource> -A`; a divergence **is** the staleness, no inference needed |
| `lastSyncResourceVersion` | RV the most recent discovery refresh observed. Tracked internally since 0.30.x to clear `watchBroken` on a successful relist; never published until 1.12.5 | advances across relists |
| `lastEventAgeSeconds` | seconds since the bridge last delivered ANY event for this GVR; **-1 means never** | a GVR whose objects churn but whose age keeps climbing has a dead watch `watchBroken` did not catch |

The OTLP mirror is **aggregate, not per-GVR** — a production cluster carries ~169 registered
informers, so three per-GVR gauges would add ~500 series per collection interval, the same
cardinality class 1.12.4 had to cap. Stats: `indexer_objects`, `gvrs_never_event`,
`max_event_age_seconds`, `gvrs_stale_over_hour`. The dashboard says *something* is stale; the
JWT-gated `/debug/servable` route says which.

### Upstream controller health — "is snowplow broken, or is an upstream controller crash-looping?"
Defined in `internal/cache/controller_health_expvar.go` / `controller_health.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_upstream_controller_health` | `map["<ns>/<name>"→{Healthy, Reason, PodRestartCount, EndpointReadyCount, …}]` for auto-discovered controllers | every entry `Healthy=1`, `Reason=""`. `Reason` enum: `pod-restart-within-window`, `endpoints-zero-ready`, `both`, `unwired` |
| `snowplow_upstream_webhook_failurepolicy` | `map["<webhookName>"→{Policy:"Fail"/"Ignore", Configuration, Type}]` | a `Fail`-policy webhook on a crash-looping controller explains apiserver pressure / write hangs |

---

## Delivery-failure matrix (1.12.6 C8)

One row per way a change to a Kubernetes object can fail to reach the browser, along the whole
path **informer → dep-event worker → refresher → L1 store → `/refreshes` broadcaster → agent
gateway → SPA**. Each row names the counter that DETECTS the loss and the arm (test name) that
PINS the fix. A row with no counter or no arm is **OPEN**, never omitted — the table exists to
show where the pipeline is still blind, not to list what works.
`TestC8_DeliveryFailureMatrix_EveryRowNamesLiveCountersAndArms` parses this table and fails the
build when a named counter is not published or a named arm does not exist, so the matrix cannot
rot. Detector names are `<expvar>.<stat>` for map families or a bare expvar key.

<!-- c8-matrix:begin -->
| # | hop | loss mode | detector | arm | status |
|---|---|---|---|---|---|
| I1 | informer → worker | the DELETE handler's worker dies on a panic; every later eviction is lost (the 1.12.5 #187 H1 defect) | `snowplow_deps.delete_worker_panics_total` (each = one lost eviction), `snowplow_deps.evict_delete_total` frozen across a known deletion | `TestIssue187_A4_DeleteWorkerSurvivesAPanic`, `TestIssue187_A4b_DeleteWorkerSurvivesAConcurrentPanicStorm` | PINNED — the worker is supervised; a panic costs one event, counted |
| I2 | informer → worker | a CRD schema relist swaps the informer; an object deleted inside the teardown window never produces a DELETE | `snowplow_crd_discovery.relist_bridge_enqueued_total` (the bridge saw it), `snowplow_crd_discovery.relist_bridge_timeout_total` (the bridge could not run — the soak signal), `snowplow_crd_discovery.relist_dirtymark_postsync_total` (the 1.12.5 containment) | `TestIssue1126_E1_RelistBridgeEvictsTheObjectTheFreshListOmits`, `TestIssue1126_E3_RelistBridgeTimeoutIsCountedAndEnqueuesNothing`, `TestIssue187_B5_RelistTeardownWindowStrandsSelfEntry` | PINNED — additive: bridge + re-fire until the timeout counter has soaked at 0 |
| I3 | informer → worker | the CRD lifecycle queue overflows and drops a DELETE (an informer is never torn down; dependents stay resident until TTL) | `snowplow_crd_discovery.events_dropped`, `snowplow_crd_discovery.events_parked` | `TestCRDLifecycleQueue_OverflowParksAndNeverDrops`, `TestCRDLifecycleQueue_ParkGivesUpOnShutdown` | PINNED — park-and-drain; a drop is a last resort after 30 s and counted |
| W1 | worker | the indexer is not authoritative (unsynced, watch broken, relist window) and the coordinate stays UNKNOWN past the requeue budget | `snowplow_deps.probe_unknown_total`, `snowplow_deps.probe_unknown_degraded_total` | `TestIssue1126_C2_UnknownDegradesToDirtyMarkAfterTheBudget`, `TestIssue1126_E2_TeardownWindowEvictsNothing` | PINNED — degrades to a dirty-mark; the refresher decides against the apiserver |
| W2 | worker | the entry holds no dep edge, so the worker's ABSENT verdict cannot reach it; the body is served until TTL / max age. Two causes: `Record` dropped at the cap, or (pre-1.12.7) edges stripped by an eviction racing a re-Put | `snowplow_deps.dropped_cap`, `snowplow_deps.reconcile_skipped_no_edge_total` | `TestIssue1126_C3_FU2_NoEdgeEntryIsCountedApartAndNeverPinsDivergence`, `TestIssue216_F6a_ConcurrentRewriteKeepsItsEdges` | OPEN — detected, and not reachable by the CURRENT repair path, which submits through the worker and would evict nothing. NOT inherently unrepairable: 1.12.7 F6a stops new ones being created, and a repair that deletes from the store directly reaches the rest (F1). Raise `DEPS_MAX_RECORDS` for the cap half |
| W4 | worker | divergence is DETECTED but nothing is repaired: `reconcile_divergence_total` climbs while the entries stay resident, because every diverged entry is skipped before the submit (no edge) or its submit evicts nothing | `snowplow_deps.reconcile_divergence_total` climbing with `snowplow_deps.reconcile_skipped_no_edge_total` non-zero and `snowplow_deps.evict_delete_total` flat across the same window | `TestIssue1126_C3_FU2_NoEdgeEntryIsCountedApartAndNeverPinsDivergence` | OPEN — this is the combination that made #216 cost a day: detection and repair are counted separately and nothing named the gap between them. The direct-delete repair and its own repaired / repair_failed counters ship with F1; until then read the three counters together |
| W3 | worker | any lost DELETE not covered above (unknown cause) leaves a stranded entry | `snowplow_deps.reconcile_divergence_total` (detector; ≈1.6 h to first visit, ≈7.5 h to 99 % at 512/30 s — NOT a staleness bound) | `TestIssue1126_D2_ReconcileTickerEvictsAStrandedEntry`, `TestIssue1126_D2c_ReconcileOnceCountsExactlyAndSkipsUnknown` | PINNED as a detector; the TTL (3600 s) remains the bound |
| R1 | refresher | the re-fetch of the entry's own object returns a confirmed 404 | `snowplow_deps.evict_self_gone_total`, `snowplow_deps.self_notfound_evict_total` | `TestIssue187_B3b_SelfGoneEvictsAtTheDropPoint`, `TestIssue187_B2E2E_RefresherSelfNotFoundEvictsThroughTheRealLoop` | PINNED — evicted at the drop point after the full budget |
| R2 | refresher | a deterministic non-404 failure (403 / 500 / timeout / parse / not-servable) used to drop the key and keep the body until TTL | `snowplow_refresher_drop_evict_total`, `snowplow_deps.evict_drop_point_total` | `TestIssue1126_C4_F4_DeterministicNon404IsEvictedAtTheDropPoint`, `TestRefreshTerminal_F5a_FewDeterministicFailuresAreEvicted`, `TestIssue1126_C4_F7_ApistageNotServableIsEvictedAtTheDropPoint` | PINNED — evicted behind the breaker (`REFRESH_DROP_EVICT_MAX_PER_MINUTE`) |
| R3 | refresher | a mass failure (apiserver outage): the breaker refuses eviction and stale bodies are served on purpose | `snowplow_refresher_drop_evict_suspended_total` (ALERT) | `TestRefreshTerminal_F5b_MassFailureIsSuspendedAndStillServed` | PINNED by design — availability over freshness, bounded by the TTL; the counter is the alert |
| R4 | refresher | an identity-bound inner stage keeps declining under the service account; the key is refreshed only by traffic (#191) | `snowplow_refresher_suppressed_set_total`, `snowplow_refresher_suppressed_skips_total`, `snowplow_refresher_suppressed_keys` | `TestIssue1126_C4_F6_StageErrorDeclinesSuppressThenPutResumes`, `TestIssue1126_C4_F6n_EmptyFullDeclinesSuppressAfterKNotAfterOne` | PINNED — suppression after `REFRESH_SUPPRESS_AFTER_DECLINES`, cleared by the next Put |
| R5 | refresher | a poison key is dropped after the requeue cap | `snowplow_refresher_dropped_total` | `TestRefresher_PoisonPillDroppedAfterCap` | PINNED — since C4 a drop is terminal (R1/R2), never "forget the key, keep the body" |
| S1 | store | keep-warm re-Puts let an entry outlive its object forever | `snowplow_resolved_cache.evict_max_age_total` | `TestMaxEntryAge_G1_RePutsDoNotExtendTheLifetime`, `TestMaxEntryAge_G1c_BornAtInheritedAndCountedSeparately` | PINNED — 24 h bound (`RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS`) |
| S2 | store | an in-flight cold Put lands after the DELETE and resurrects the pre-delete body | — | `TestIssue187_A5_InFlightColdDispatchCannotResurrectPreDeleteBody` | OPEN detector — pinned by the arm for the cold-dispatch shape; the general race needs a generation counter (#189) and has no counter |
| B1 | broadcaster | a DELETE-semantics eviction published nothing, so the browser never learned (1.12.5 and earlier) | `snowplow_refresh_broadcaster.evict_published` | `TestRefreshes_S1_DeleteEvictionWritesRefreshFrame`, `TestRefreshes_S1b_SelfGoneEvictionWritesRefreshFrame`, `TestRefreshEviction_S1d_TTLAndLRU_Silent` | PINNED — 1.12.6 C10; TTL / LRU / max-age / drop-point stay silent by design |
| B2 | broadcaster | a bulk delete would fan out an unbounded cold-refetch burst; excess frames are deferred by the per-subscriber bucket | `snowplow_refresh_broadcaster.evict_deferred` | `TestRefreshes_S9_BurstIsPacedAndComplete`, `TestRefreshes_S9b_PendingNeverExceedsArmed`, `TestRefreshEviction_Pace_DeferredNeverDroppedDedup` | PINNED — deferred, never dropped; pending ≤ armed |
| B3 | broadcaster | a slow consumer's sink fills and a signal is dropped | `snowplow_refresh_broadcaster.dropped`, `snowplow_refresh_broadcaster.max_sink_depth` | `TestRefreshBroadcaster_SlowConsumerNeverStalls`, `TestRefreshBroadcaster_DroppedTerminalSignalDegrades` | PINNED — the subscriber gets a terminal degrade signal and must refetch; the refresher never stalls |
| B4 | broadcaster | key-space mismatch on a long stream (identity drift, SPA key-derivation change): frames are published but never match an armed key (L7/L8) | rule: `snowplow_refresh_broadcaster.delivered` == 0 while `snowplow_refresh_broadcaster.subscribers` > 0 and `snowplow_refresh_broadcaster.published` > 0 | — | OPEN — the rule is the only detector; no arm drives a real drift |
| B5 | broadcaster | hub reset / pod restart ends every stream; frames published before the client reconnects are gone (no replay, by design) | `snowplow_refresh_broadcaster.streams_closed_total`, `snowplow_refresh_broadcaster.stream_seconds_total` | `TestRefreshes_S5_HubResetOldStreamIdleNewStreamDelivers`, `TestRefreshes_S5_ReconnectArmsAfreshAndDelivers` | OPEN on the client half — the server re-arms afresh (pinned); recovery is the SPA's re-validation (frontend#256, howto §9) |
| G1 | gateway | a proxy-side cut, buffer or timeout silently ends or stalls every stream (`traffic.timeouts.request`, `traffic.buffer`, `traffic.retry`, `rateLimit` on the route) | — (indirect only: the mean stream lifetime `snowplow_refresh_broadcaster.stream_seconds_total` ÷ `snowplow_refresh_broadcaster.streams_closed_total` collapsing) | — | OPEN — no snowplow-side counter can see the hop; S8 measured the route clean today; a configuration hazard with a never-add table in the howto §11 and the chart review checklist |
| C1 | SPA | reconnect gap: a frame published while the tab was disconnected is gone (L4) | — | — | OPEN — the client owns recovery: re-validate on transport loss through the single bounded queue (frontend#256, howto §9) |
| C2 | SPA | the 5 s per-widget throttle discards a second frame inside the window instead of deferring it (L5) | — | — | OPEN — frontend#256 (trailing catch-up) |
| C3 | SPA | a 401 on `/refreshes` is retried forever at the backoff ceiling instead of re-authenticating (S13; 553 gateway rejections in one window) | — (gateway-side JwtAuth rejection logs only) | — | OPEN — frontend#256 (session resume) |
| C4 | SPA | the subscription is truncated at the 16 KiB `?sub=` cap (≈93 coordinates); tail widgets silently never arm and miss every frame | — (the cap is enforced silently on both sides today) | — | OPEN — frontend#256 makes the truncation loud; design rev 4.1 A2.5 (coarse subscribe / precise publish) removes the cap |
<!-- c8-matrix:end -->

Rows marked OPEN on the SPA side are tracked in `krateo-platformops/frontend#256`; the
integration contract they implement is `docs/howto-frontend-live-refresh-sse.md` §9–§11.

## Key `slog` events

JSON structured logs on stdout. Message strings are stable, dotted, and greppable. The
operator-notable ones:

| Event (message) | Level | Site | What it tells an operator |
|---|---|---|---|
| `phase1.warmup.completed` | Info | `dispatchers/phase1_walk.go` | the prewarm walk finished — the boundary `/readyz` and `snowplow_prewarm_complete` track |
| `phase1.warmup.roots_list_failed` | Warn | `dispatchers/phase1_walk.go` | the frontend config-vars ConfigMap was absent at boot; the config-vars informer will re-drive a boot re-walk when it lands (self-heal, no restart) |
| `readyz.backstop.fired` | Error | `dispatchers/readiness_backstop_metrics.go` | readiness flipped **Ready-degraded** via a backstop arm (reason attached); pairs with `snowplow_readyz_backstop_fired` — alert |
| `phase1.seed.sync_incomplete` / `phase1.seed.panic` | Warn / Error | `dispatchers/phase1_walk.go` | the synchronous boot seed erred/panicked; readiness still flipped (backstop) with cold cells |
| `phase1.seed.cohort.operational_failure` | Warn+ | dispatchers phase1 seed | a cohort hit an UNEXPECTED seed failure — pairs with `snowplow_phase1_seed_operational_fail_total`; actionable |
| `phase1.seed.cohort.expected_deny` | Info | dispatchers phase1 seed | EXPECTED narrow-RBAC deny — normal, pairs with `snowplow_phase1_seed_rbac_deny_total` |
| `phase1.walk.apiref_pagination.backstop_hit` | Warn | `dispatchers/phase1_walk_pagination.go` | a widget's apiRef pagination hit the anti-runaway page ceiling — coverage may be capped |
| `apiserver_fallthrough` | Debug | `internal/cache/fallthrough_meter.go` | a read punted to the apiserver — the log companion to `snowplow_apiserver_fallthrough_total`; includes path/gvr/reason. **Demoted Warn→Debug in 1.12.2** (commit `52eb46d`): a warm pod still punts routinely, so at Warn it drowned the log. Track the counter, not the line; raise `LOG_LEVEL=debug` to see it. |
| `cache.read_paths_scoped.violation` | Error | `internal/cache/fallthrough_assert.go` | architectural invariant breach — a `/call` route is not scope-wrapped |
| `cache.serve_requires_servable.violation` | Error | `internal/cache/serve_assert.go` | **P1** invariant breach — a cache HIT was about to be served from a not-servable informer (names the gvr + serve_path) |
| `deps.delete_worker.panic` | Warn | `internal/cache/deps_watch.go` | **1.12.5** — an `OnDelete` panicked; names the `gvr`/`ns`/`name` whose L1 eviction was LOST (stale until TTL) and the panic value. The worker survives and drains the next event. Pairs with `snowplow_deps` stat `delete_worker_panics_total`. Before 1.12.5 this was an Error with no coordinates, and the first occurrence killed DELETE handling process-wide until restart (#187 H1) |
| `deps.delete_queue_full` | Warn | `internal/cache/deps_watch.go` | a DELETE storm outran the eviction worker; `OnDelete` ran inline. Not data loss |
| `refresher.self_object_gone_evicted` | Warn | `internal/cache/refresher.go` | **1.12.5** — a background refresh re-fetched the entry's own object and got a confirmed apiserver 404 on every attempt across the full requeue budget, so the entry was evicted rather than dropped-to-TTL with its stale body resident. REPLACES `refresher.refresh_dropped` for that key (#187) |
| `cache.bindings_by_gvr.delta_skipped_non_typed` | Warn | `internal/cache/bindings_by_gvr_delta.go` | an index delta event was dropped — index is drifting; pairs with the same-named expvar |
| `cache.crd_discovery.event_dropped` | Warn | `internal/cache/crd_discovery_side_effect.go` | a CRD-discovery event was dropped (queue full / shutdown) |
| `cache.controller_health.watch.broken` | Warn | `internal/cache/controller_health.go` | an upstream controller-health watch broke — the health gauge may go stale |
| `cache.rbac.snapshot.published` | Info | `internal/cache/` rbac snapshot | a new RBAC subject-index snapshot published; pairs with `snowplow_rbac_publish_seq` |
| `config.retired_flag_ignored` | Warn/Info | `internal/cache/retired_flags.go` | a retired prewarm/pivot env var is still set; Warn when the value is `false` (a silent behavior change the operator did not consent to — the flag is implicit-on-cache now) |
| `dispatcher.call.complete` | Info | `dispatchers/per_call_log.go` | per-dispatch structured timing (the per-call diagnostic) |
| `cache.secrets.informer.assertion_violation` | Error | `internal/cache/secrets_informer.go` | secrets-informer invariant breach |

There are many more dotted events in the `phase1.*`, `prewarm.engine.*`,
`cache.crd_discovery.*`, `cache.rbac.snapshot.*`, and `cache.discovery.*`
families (full set is greppable with
`grep -rhoE '"(phase1|prewarm|cache)\.[a-z_.]+"' internal main.go`); the table
above lists the ones that map to an alarm-worthy condition or pair directly
with an expvar.

---

## pprof

Registered on the custom server mux (the server does **not** use
`http.DefaultServeMux`), `main.go`:

| Path | Profile |
|---|---|
| `GET /debug/pprof/` | index (links to goroutine, heap, allocs, threadcreate, block, mutex, …) |
| `GET /debug/pprof/cmdline` | process command line |
| `GET /debug/pprof/profile` | 30s CPU profile |
| `GET /debug/pprof/symbol` | symbol lookup |
| `GET /debug/pprof/trace` | execution trace |

Mutex + block profiling fractions are set at startup so `/debug/pprof/mutex`
and `/debug/pprof/block` return non-empty data.

Typical use:

```
go tool pprof http://<pod>:<port>/debug/pprof/heap          # memory
go tool pprof http://<pod>:<port>/debug/pprof/profile       # CPU (30s)
curl http://<pod>:<port>/debug/pprof/goroutine?debug=2      # goroutine dump (deadlock/leak)
```

Note: responses are gzip-compressed only when the client sends
`Accept-Encoding: gzip` (the `gzhttp` wrapper); plain `curl` diagnostics are
byte-identical to pre-compression behaviour, and the SSE routes are excluded
from compression buffering.

---

## Notes / discrepancies vs. earlier revisions

- Several phase-1 seed expvars referenced in older notes
  (`snowplow_phase1_seed_restactions_total`, `snowplow_phase1_seed_widgets_total`
  and their `_by_cohort` maps; the binding-set classes / powerset-skipped
  counters; the cohort_seed_status gauge) were **deleted** as always-zero or
  orphaned and are intentionally NOT in the code today — they are excluded
  here.
- An earlier revision of this doc claimed the chart wires the startupProbe to
  `/readyz`; the chart actually points **startupProbe → `/health`** (with a
  long failure budget) and only the readinessProbe at `/readyz`.
- The observability surface used to be expvar/slog/pprof only; the OTel export
  (traces + expvar-mirror metrics + log/audit bridge) is additive and, since
  1.12.4, default-on — disabling it changes no expvar semantics.
- **1.12.6 C7.** Three metric families (`snowplow_crd_discovery`,
  `snowplow_refresh_broadcaster`, `snowplow_refresher_*`) are now DERIVED from
  `stat` tags on their snapshot structs (`internal/cache/stats_by_tag.go`):
  expvar, the OTLP mirror, this document and the parity arms all read the same
  tag set, so a counter cannot again reach `/debug/vars` and miss ClickStack
  (three did in 1.12.6 before C7: the five C4 refresher counters, seven
  `snowplow_crd_discovery` stats including the whole relist bridge, and the six
  C12 broadcaster instruments, which were created but never registered with
  the observable callback). `TestC7_Docs_EveryPublishedStatIsDocumented` fails
  the build when a published stat is not named in this file.
