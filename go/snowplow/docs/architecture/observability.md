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
`GET /debug/refreshes` (the live-refresh subscription registry),
`GET /debug/reconcile` (1.12.6 C3 — the on-demand full reconcile audit, see the
dependency-tracker section) and `GET /debug/store` (#237 — the per-object informer-store read),
and two probe endpoints used by the chart.

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
| `GET /readyz` | `internal/handlers/readyz.go` | `200 {"status":"ready","phase1Done":true,"outcome":…,"elapsed_ms":…,"walk_ms":…,"sync_wait_ms":…,"content_prewarm_ms":…,"cluster_list_prewarm_ms":…,"seed_ms":…}` once `cache.IsPhase1Done()`, else `503 {"status":"warming","phase1Done":false,"reason":…,"elapsed_s":…}`. **#397**: `reason` = the phase-1 stage (`phase1_not_started` / `roots_walk` / `informer_sync` / `content_prewarm` / `cluster_list_prewarm` / `boot_seed_in_progress` / `readiness_released` / `phase1_aborted_no_sa_endpoint`); `elapsed_s` = seconds since process start; `outcome` = the `prewarm.phase1.readiness_exit` outcome (`latch` / `deadline` / `boot_error` / `seed_panic` / `seed_returned` / `none-configured` / **#401** `boot_aborted` = Phase1Warmup could not start the walk, Ready-degraded with nothing prewarmed). **#407**: the ready body also carries that record's `elapsed_ms` (Phase1Warmup start to flip) and per-step `walk_ms` / `sync_wait_ms` / `content_prewarm_ms` / `cluster_list_prewarm_ms` / `seed_ms` (`-1` = step not run), the same values as `snowplow_phase1_readiness_exit`; omitted while warming and when no exit was recorded. Status codes and the other fields are unchanged | readiness — safe to receive traffic. Since the prewarm-gated-readiness reversal this flips only after the synchronous boot seed (or its fire-regardless backstop) — a pod can be **Ready-degraded** (flipped via backstop with cold cells); `snowplow_readyz_backstop_fired` distinguishes that from a healthy boot |

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
- `snowplow_rbac_subgen_bumps_total`, `snowplow_rbac_subgen_subjects_tracked` —
  `cache.RegisterRBACSubGenExpvar()`
- `snowplow_rbac_binding_noop_updates_total`,
  `snowplow_rbac_binding_semantic_noop_updates_total` —
  `cache.RegisterRBACBindingNoopExpvar()`
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
| `snowplow_build_info` (`build_info_expvar.go`, 1.12.4) | `{version}` = `main.build` (the full 40-character git commit the Dockerfile stamps; `unknown` for an unstamped local build) | pins every other metric to a commit. On OTLP the same commit is the resource attribute `vcs.ref.head.revision`; the resource `service.version` is the pod's release label (`app.kubernetes.io/version`, the chart appVersion), the value the collector stamps on its own rows for the pod (#462) |
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
| `evict_max_age_warm_customer_total` | **#378 — the owner's P5 trigger.** the subset of `evict_max_age_total` that evicted a WARM cell (`SeededAtBoot`, or read within the TTL) on the CUSTOMER read path (`Get`): a cold navigation of the working set at the C5 cap. Since #378 the refresher terminal re-mints (resets `BornAt` of) a warm cell it refreshes inside the lead window `[maxAge − L, …)`, `L = min(TTL, maxAge/2)`, so a warm cell should never reach the cap under a customer — EXCEPT the #496 residual: the re-mint is gated on warmth, so a cell whose in-window refresh landed BEFORE its first read is not re-minted, and if no further refresh arrives before the cap that read cold-navigates | **0**. Read only on a pod whose `oldest_warm_born_age_seconds` exceeded `maxAge − L` (an exercised pod); on a younger pod a zero is "not exercised", not evidence. **a small non-zero is now EXPECTED** — the #496 late-first-read residual, bounded by the refresh cadence (order 1–5% of in-window first reads). Zero is still the healthy reading; what changed is that a handful is no longer an escalation. **A sustained rise, or a value that scales with traffic, = back to the owner (P5)** |
| `evict_max_age_warm_internal_total` | **#378.** the same warm-cell max-age evict on the INTERNAL read path (`GetNoTouch`: refresher dequeue, seed sweeps) — no customer saw it | low; rising with `remint_total` flat = refreshes are landing after the cap instead of in the window |
| `evict_ttl_warm_customer_total` | **#378.** the subset of `evict_ttl_total` that evicted a WARM cell under a customer `Get`: a warm cell whose BODY lapsed (no refresh within the TTL) | **0**; non-zero = a warm cell missed its refresh (missed dirty-mark and #316) |
| `oldest_warm_born_age_seconds` | **#378.** a GAUGE: the oldest `BornAt` age (whole seconds) over the WARM cells of the last reaper walk, 0 if none. The SCOPE of the three detectors above | saw-tooths below `maxAge` once re-mint runs (each re-mint resets the age). Above `maxAge` = warm cells are crossing the cap un-re-minted (read with `warm_past_max_age`) |
| `remint_total` | **#378.** accepted refresher-terminal writes that RE-MINTED (reset `BornAt` under the same key) because the cell was inside the lead window AND (#496) was WARM. The by-class split is the OTLP counter `snowplow_resolved_cache_remint_total{class}` (see the #448 table) | **#496 CHANGED THIS READING — a zero is no longer a FAIL on its own.** Since the re-mint is gated on warmth, 0 is the CORRECT reading on a pod with no customer-warm working set (a lightly-browsed 057, or after boot-seeded cells lose `SeededAtBoot` at their first refresh). Read it as a PAIR with `remint_refused_cold_total`: `remint_total > 0` = the carrier works; both 0 on a pod past `maxAge − L` = **the window is not being reached at all (the real inert case, FAIL)** |
| `remint_refused_cold_total` | **#496.** in-window refreshes the warmth gate REFUSED to re-mint because the cell was COLD. Those cells stay reclaimable by the reaper's cold-evict, which is the 24h bound #259 names and the #191 terminator — before #496 the re-mint extended a cold cell's max-age clock forever and nothing could reclaim it | > 0 on a long-lived pod with churn = the gate is doing its job; read next to `evict_max_age_total`, which RISES by design as those cells are finally reclaimed. Both this and `remint_total` at 0 on a pod past `maxAge − L` = the window is not being reached |
| `evict_no_representative_total` | **#444.** identity-bound cells (restactions / widgets / raFullList) the refresher EVICTED because their recorded representative had drifted out of the key's RBAC class and no in-class replacement was found (neither the canonical group representative nor a recent hitter). The cell refills cold and correct on its next request instead of serving stale until the TTL. Replacement outcomes per source are on `snowplow_l1_representative_repick_total` (`group` / `hitter` / `evicted`) | low; tracks personal (User-subject) RBAC changes on representatives of classes no group representative can stand in for |
| `evict_delete_total` | entries evicted by invalidation (the dep tracker's DELETE route) — the store-side twin of `snowplow_deps.evict_delete_total` | tracks object deletions |
| `suppressed_resident` | **#345 / #248.** a GAUGE (current count, up/down — not a total) of resident cells the refresher has permanently refresh-SUPPRESSED (a UAF decline suppresses on first occurrence). #248's own validation signal: the reaper reads it every summary tick and evicts the past-`maxEntryAge` members; the number was previously computed and discarded into the `LOG_LEVEL=warn`-suppressed INFO line. Distinguishes decline-FROZEN cells from never-yet-refreshed ones | **0** in steady state; a persistent non-zero plateau = cells frozen by UAF-decline that only the reaper (past maxEntryAge) or a keying fix will clear — read next to `evict_max_age_total` (the reaper draining them) |
| `warm_past_max_age` | **#315 C4.** a GAUGE of resident WARM cells (read within TTL or `SeededAtBoot`) that are ALSO past `maxEntryAge`. The read-independent pass deliberately does NOT cold-evict these (evicting a warm cell manufactures a cold nav — C3); it keeps them body-fresh via `proactive_refresh_total` and (#378) re-mints their key when a refresh lands inside the lead window, so a cell is counted here only if no refresh was accepted in its window. The AT-RISK population (structural proxy, not a leak count) | **0** on a fast-roll pod (nothing outlives a day); a non-zero plateau on a long-lived pod = warm cells past the cap that no in-window refresh re-minted — read next to `evict_max_age_total` (the cold ones being reaped) |
| `warm_seeded` | **#376.** a GAUGE of resident WARM cells that are warm because they were `SeededAtBoot` (the boot-prewarm set), recomputed on the same reaper walk as `warm_past_max_age`. Together with `warm_lastread` it decomposes the warm working set by SOURCE so `GetNoTouch`'s effect is observable | holds at the boot-prewarm set on an unbrowsed pod; read next to `warm_lastread` — if the lastRead bucket collapses toward 0 while this holds, internal reads are no longer faking warmth (the #376 fix is working) |
| `warm_lastread` | **#376.** a GAUGE of resident WARM cells that are warm via a read-within-TTL `lastRead` and are NOT `SeededAtBoot` — i.e. kept warm by a genuine customer Get. Before #376, internal cache reads stamped `lastRead` and inflated this; after #376 (internal callers use `GetNoTouch`) only real customer reads keep a cell here | **collapses toward ~0 on an unbrowsed cluster** post-#376 (nothing but customer traffic keeps a cell in it); a healthy non-zero value on a browsed pod = the genuinely-served working set. A high value with no customer traffic = a regression reintroducing warmth-faking internal reads |
| `proactive_refresh_total` | **#316.** a monotonic count of proactive refreshes ENQUEUED by the read-independent pass — warm, approaching-TTL cells (`TTLRemaining < TTL/4`) handed to the existing refresher, the read-independent backstop for a MISSED dirty-mark (fresh indexer, enqueue missed; a stale indexer is #244). Scoped to the warm working set, never refresh-everything | climbs steadily on a busy pod (the working set cycling through 3/4-TTL); read next to `snowplow_refresher.completed_total` (the refresher's dedup/rate-floor collapses these enqueues into far fewer re-resolves). A flat **0** on a pod serving traffic = the pass is inert |
| `stale_served_total` | **#354 P3.** customer L1 hits on a cell whose invalidation→fresh window is OPEN (dirty-marked, the refresher's fresh re-Put not landed yet), the in-flight re-resolve included. Counted in `ResolvedCacheStore.Get`, the one funnel every `hit_total` hit goes through, so it shares `hit_total`'s denominator by construction. Scalar: no key, identity or body | read per 1k hits (`Δstale_served_total / Δhit_total × 1000`); ≈0 on a quiet cluster, rises with churn × traffic |
| `stale_served_age_ms_max` | **#354 P3.** GAUGE — the max age (now − the window's first mark, ms) of a stale serve, over roughly the last OTLP export interval (`OTEL_METRIC_EXPORT_INTERVAL`, default 60 s) | under the 1 s fresh north-star; above 10 s = past the AC-98.12 SLA |
| `warm_keyed_page_restactions`, `warm_keyed_page_widgets`, `warm_keyed_page_ra_full_list`, `warm_keyed_extras_restactions`, `warm_keyed_extras_widgets`, `warm_keyed_extras_ra_full_list` | **#354 P3 (B3, sizes #477).** GAUGES recomputed on the reaper walk: WARM cells (same `warm` as `warm_seeded` / `warm_lastread`) of each identity-bound class whose key folds a page (`page > 0` or `perPage > 0`) or request extras. Cold cells are excluded. Denominator: `warm_seeded + warm_lastread` | sizing input, no alert |
| `resident_entries`, `resident_bytes`, `max_resident_bytes` | the pinned resident region (Ship 4a): entries the keep-warm sweep keeps out of LRU, and its own byte ceiling | `resident_bytes` under `max_resident_bytes` |
| `resident_pin_total`, `resident_demote_total` | entries pinned into / demoted out of the resident region | demotions low; a burst = the resident ceiling is binding |
| `apistage_store_total`, `apistage_evict_total` | per-class store / evict for `apistage` cells | evicts ≪ stores |
| `widget_content_store_total`, `widget_content_evict_total` | per-class store / evict for `widgetContent` cells | evicts ≪ stores |
| `ra_full_list_store_total`, `ra_full_list_evict_total` | per-class store / evict for `RAFullList` cells | evicts ≪ stores |
| `put_refused_generation_moved_total` | **#189.** request-path `PutIfGen` writes REFUSED because the key's generation moved between the dispatcher's pre-resolve capture and the tail write — i.e. a DELETE-eviction (or any removal) landed during the in-flight resolve, so the pre-delete body was NOT resurrected. Reads non-zero exactly when the write-side resurrection race is being closed | **0** in steady state; a low rate around object churn is healthy (the guard doing its job). A sustained climb = many resolves are racing evictions — read next to `snowplow_deps.evict_delete_total` |
| `serve_missed_rotation_atrisk_roleref_unresolved` | **#261 case-3 serve-time detector.** restactions serve HITs (hit-gated) of a cell whose serve-time first-permitting `BindingUID` was roleRef-UNRESOLVED at the last `bindings_by_gvr` index build — so its grant is not reflected in the L1 keying and a later role create/edit could have shifted the grant with no sub-gen bump (the bounded self-stale-leak carrier). Measures the **AT-RISK SERVE POPULATION** (structural proxy), NOT a confirmed-leak count | **0** in steady state (matches the 057 0/2517 baseline). Off-zero => an at-risk-keyed cell was served → REASSESS (the keying fix is v7-deferred); NEVER read as "N leaks" |
| `serve_missed_rotation_atrisk_implicit_group` | **#261 case-2 serve-time detector.** restactions serve HITs (hit-gated) of a cell whose serve-time first-permitting binding's WINNING subject matched only through an IMPLICIT group (`system:authenticated`, or a synthetic `system:serviceaccounts[:ns]` group) — `RBACSubGenForSubject` sums only the requester's PRESENTED groups, so a grant/revoke through such a binding shifts the serve-time first-match `BindingUID` with no sub-gen bump (the bounded self-stale-leak carrier). The class is computed at KEY-MINT (the evaluator's winning-subject fact, surfaced via `WinningSubjectClassOut`) and merely READ at the hit. Measures the **AT-RISK SERVE POPULATION** (structural proxy), NOT a confirmed-leak count | **0** in steady state (none of the 4 implicit-group CRBs on 057 grants `get`). Off-zero => an at-risk-keyed cell was served → REASSESS (the keying fix is v7-deferred); NEVER read as "N leaks" |


### RA resolve guard — malformed single-object dials skipped (#288 / #293 / #302)
Defined in `internal/resolvers/restactions/api/malformed_dial_metrics.go`. The
guard runs on **every** RA resolve (cache on or off), so the family is never
cache-mode-gated. A non-zero is a **DETECTOR**: an upstream render/keying/jq fault
was caught before it could be dialed as a garbage single-object apiserver request
(empty/DNS-invalid name, an unrendered `${...}` template, or a jq
path/payload/header error). Each reason has a distinct owner/urgency, so the count
is broken out **per reason** on both surfaces.

| metric | meaning | healthy range |
|---|---|---|
| `snowplow_malformed_dial_skipped_total` (expvar) | `/debug/vars` `map{reason → uint64}` over the fixed reason set `{empty_interp, unrendered_template, jq_path_error, jq_payload_error, jq_header_error}`. Single published KEY (the CFG-1 `nonCacheInitPublishers` exception); the inner reason set grows, the key name never does | **0** in steady state. Non-zero = the guard fired — read the per-reason breakdown: `empty_interp` (#288) = a key-minting/interpolation regression; `unrendered_template` (#293) = an RA-authoring/unresolved-template fault; `jq_*_error` (#293/#302) = a jq fault that would otherwise have dialed a garbage path / body / header |
| `snowplow_malformed_dial_skipped_total{reason}` (OTLP, **#311**) | the same counter mirrored to ClickStack as one `{reason}` series per enum member, observed in `metrics.go` by ranging `MalformedDialSkippedByReasonSnapshot()`. Previously `/debug/vars`-only (observable-if-you-look); #311 makes it **alertable** | **0**. The `reason` attribute is bounded **by construction** — it is the fixed code-defined `MalformedDialReasonEnum()`, NEVER request-derived (the #260 bounded-attribute discipline), so it cannot blow up OTLP cardinality. Expvar and OTLP read the SAME snapshot, so the two surfaces never diverge (guarded by `metrics_311_malformed_dial_otlp_test.go`: `len(series) == len(enum)` + every emitted reason ∈ enum) |

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
| `snowplow_readyz_backstop_fired` (`internal/handlers/dispatchers/readiness_backstop_metrics.go`) | `int` — readiness flipped via a backstop arm instead of the first-nav happy path. **#402: exactly ONE per readiness release** (recorded only by the Step 7.6 exit recorder, from the same classification as the `prewarm.phase1.readiness_exit` outcome), with ONE `readyz.backstop.fired` ERROR carrying the most specific `reason`: `phase1_timeout` / `pip_global_timeout` / `canceled` (deadline release, = the `deadline_cause`), `boot_error`, `seed_panic` (also when the latch had fired: the seed aborted mid-flight), and **#401** `no_sa_endpoint` / `no_dyn_client` (Phase1Warmup could not start the walk; outcome `boot_aborted`). `seed_incomplete` is **retired** (it was the second count of a deadline / boot error) | **0** on a healthy boot; any non-zero = a FAILED-but-serving (Ready-degraded) boot — alert |
| `snowplow_phase1_readiness_exit` (`internal/handlers/dispatchers/phase1_readiness_exit.go`, **#407**; expvar only, no OTLP mirror) | JSON object, **set once per process** at the readiness flip, from the same record (same sync.Once, same values) as the `prewarm.phase1.readiness_exit` line: `outcome`, `deadline_cause`, `abort_cause` (**#401** enum, set only for `outcome=boot_aborted`: `no_sa_endpoint` / `no_dyn_client`; `deadline_cause` is then empty, and `abort_cause` is empty on every other outcome), `latch_fired`, `elapsed_ms`, `since_process_start_ms`, `nav_latch_armed`, `nav_boot_passes`, `nav_units_total` / `nav_units_seeded` / `nav_units_remaining`, `cohorts`, the bounded failure counts `nav_units_expected_deny` / `nav_units_operational_failure` / `nav_units_aborted`, and per-step `walk_ms` / `sync_wait_ms` / `content_prewarm_ms` / `cluster_list_prewarm_ms` / `seed_ms` (`-1` = step not run). **Before the exit it is the empty object `{}`** (no keys; it is never `{"outcome":""}`), so "not yet exited" = no `outcome` key. Ungated on `CACHE_ENABLED` (a readiness surface; the cache-off exit is `none-configured`). Exists because the line is INFO on a latch exit and production runs `LOG_LEVEL=warn`: on the normal path this map and the ready `/readyz` body are the only places the per-step timings and nav counts can be read. Counts, ms and code-defined enums only, never an identity or widget name. `nav_units_seeded` here is the BOOT pass's count frozen at the flip; it is not the cumulative keepwarm `units_seeded` | healthy = `outcome=latch`, `nav_units_remaining=0`. Read `*_ms` to see where boot time went |
| `snowplow_phase1_deadline_released_total` (`internal/handlers/dispatchers/phase1_readiness_exit.go`; OTLP mirror `snowplow_phase1_deadline_released_total`, hand-wired in `internal/metrics/metrics.go`, **#397**) | `int`, **0 or 1 per process**: 1 iff readiness was released by the boot-seed ctx (`pipGlobalTimeout`) or the `PHASE1_TIMEOUT_SECONDS` parent expiring BEFORE the first-nav latch fired (`prewarm.phase1.readiness_exit` `outcome=deadline`). Narrower than `snowplow_readyz_backstop_fired`, which also counts seed errors / panics (one per readiness release, #402). This one answers exactly "was Ready time-released?". **Caveat:** a shutdown mid-boot (SIGTERM cancelling the seed / PHASE1 ctx before the latch, `deadline_cause=canceled`) is also recorded as `outcome=deadline` and ALSO sets this to 1. Check `deadline_cause` before reading a 1 as a slow boot: only `phase1_timeout` / `pip_global_timeout` mean the budget ran out | **0**. A 1 = the pod went Ready with nav-widget units still unseeded. Read the `prewarm.phase1.readiness_exit` WARN for `nav_units_remaining`, the failure-class counts and the per-step `*_ms` |
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
| `snowplow_phase1_seed_widget_uaf_preresolve_skip_total` | `uint64` (expvar + OTLP) — #403: widget seed units skipped BEFORE the resolve because the widget's `spec.apiRef` RESTAction declares a `userAccessFilter` (the widgets cell would be declined for every identity). Each is also counted by `snowplow_widgets_uaf_put_declined_total` | ≈ the boot's widget UAF declines (908 per boot on 057 at 1.12.36); the widget UAF decline count stays unchanged, but these units no longer pay a resolve |
| `prewarm.coverage` | map — `{reached, lost, completed_passes_total, last_completed_pass_unix, reuse_passes_total, no_roots_passes_total, regressed_total, harvested_widgets, harvested_restactions, walked_roots}` (#220). **`reached` is the measure**: DISTINCT widgets the last COMPLETED walk pass actually reached — an absolute gauge, independent of pod uptime, so it is comparable across pods, restarts and versions. **`harvested_widgets` is NOT the measure and must never be read as coverage**: it is the harvester's cumulative UNION across every pass this pod ever ran, monotonic by design (it drops only on a confirmed deletion), so it **cannot fall when reachability collapses** — during the original #220 it read a healthy 177 throughout. It is published only for continuity with `/debug/harvest`. A pass that did not walk records NOTHING (so `walked_roots` is always the last REAL walk) and bumps one of two counters, kept apart on purpose: `reuse_passes_total` is an F.4 resume deliberately reusing the snapshot (**healthy**), `no_roots_passes_total` is a driver that meant to walk and found no roots (**a pod covering nothing**) | **`reached` is the row to watch.** The alarm is a set difference between adjacent COMPLETED passes — `lost = reached(t-1) − reached(t) − forgotten_recent` — and it is a **TRANSITION** alarm: it fires ONCE, on the pass that lost the widgets, then goes quiet while the loss persists. So **`lost = 0` means "no change since the previous pass", NOT "healthy"** — read `reached` for the standing level and `regressed_total` (which latches at ≥ 1 for the life of the process) for whether this pod ever observed a collapse; the `prewarm.coverage.regressed` WARN carries both levels and up to 5 example coordinates. Only a COMPLETED pass is evaluated and only a COMPLETED pass moves the baseline, so a partial pass and a no-roots pass can neither alarm nor become the thing the next pass is compared against. A legitimately DELETED widget is discounted by the gone-forget set and does not alarm. **TWO BLIND SPOTS, both by construction:** (1) a pod that **BOOTED already collapsed never alarms** — the alarm compares this process's own passes, so with `completed_passes_total` ≥ 1 and `regressed_total` = 0 the walk has been stable *since this pod started*, which says nothing about whether it started at the right level; (2) a **sustained run of PARTIAL passes means the alarm never evaluates at all** — `completed_passes_total` stops advancing while `reached` and `last_completed_pass_unix` go stale, so **watch `completed_passes_total` for movement**, a flat one with `walked_roots` > 0 is an alarm that has gone blind rather than quiet. Neither is solved in-process and neither should be: that comparison lives in the SLI dashboard, against the published `reached` gauge for the current version vs the prior version on the same cluster (correlated with the deletion rate to separate CRUD shrinkage from a reachability collapse). **Cost:** the forget sets are TWO generations, each bounded by the navigation widget set (tens) and replaced wholesale at each completed evaluation — deliberately **no cap and no timer**. A cap would silently drop explanations and manufacture false alarms on ordinary deletes; a timer is the started-once-never-stopped goroutine pattern this release has two open issues about |
| `snowplow_phase1_harvest_forgotten_total` | map keyed by harvester (`nav_widget` / `apiref`), plus `gone_verdicts` — harvested ENTRIES DROPPED because the object was confirmed gone (1.12.7 F6b/F6c), and the count of gone verdicts DELIVERED to the harvesters whether or not anything was held | `0` on a cluster where nothing is deleted. Climbs only when a deletion actually stopped a replay; counts removals, **not** hook firings, so it does not climb for gone objects that were never harvested. `nav_widget` can move by more than 1 per object (one entry per pagination tuple). **Read the forget counts against `gone_verdicts`:** both zero is a quiet cluster with nothing deleted; `gone_verdicts` climbing while the forget counts stay flat means deletions ARE arriving and none is being forgotten — the mechanism or the instrument is dead |
| `snowplow_phase1_widget_seed_failure_total` / `snowplow_phase1_restaction_seed_failure_total` | per-cohort×object failure maps | pinpoint which widget/RA broke which cohort |
| `snowplow_resolved_cache_hits_seed_attributable` | hits on cells the seed wrote (seed attribution) | the seed-is-actually-useful signal |
| `snowplow_prewarm_ref_denied_total` | `uint64` — #214: resourceRef RBAC denials observed on the PREWARM path (the content-prewarm walk resolves write-verb refs the identity may not hold) | non-zero is **normal** and expected. It is the `prewarm-path` cell of the map below, kept as its own key for continuity with what #214 published |
| `snowplow_ref_denied_by_origin` | map keyed by the DRIVER of the resolve that saw the denial (#563): `refresher`, `cohort-seed`, `prewarm-engine-boot`, `prewarm-path`, `background-unattributed`, `serve`. All six cells are always present, so a `0` is a reading and not a missing instrument | The five non-`serve` cells are **expected to be non-zero** — background walks resolve write-verb refs their identity does not hold, and those denials are Debug-logged, not WARN'd. **`serve` is the one to watch**: it counts denials hit by a real customer `/call`, each of which also WARNs with the requester's redact label, so `serve` climbing is a real access problem with an identity attached to it. **`background-unattributed` must stay 0** — it means a background driver reached the resolver without declaring itself, i.e. the attribution has a hole; the static census in `internal/handlers/dispatchers/background_origin_census_563_test.go` is what keeps that cell's zero meaningful |

### Refresher — background re-resolve worker pool
Defined in `internal/cache/refresher_metrics.go`.

All keys below are derived from the `stat` tags on `refresherStats` /
`RefreshTerminalStats` (1.12.6 C7): counters are `snowplow_refresher_<stat>_total`,
gauges `snowplow_refresher_<stat>`; the OTLP mirror publishes the counters as
`snowplow_refresher{stat=<stat>}` and the gauges under their expvar name. None of them
constructs the refresher: before the first enqueue, and on a cache-off pod, every value is 0
(#203). The refresher's real-resolve p95 (#386 M1) is published SEPARATELY as
`snowplow_resolve_latency_p95_ms` — a standalone expvar scalar kept OUTSIDE this
`snowplow_refresher_` family ON PURPOSE: a C7-tagged stat would auto-mirror it to OTLP
(the ruling excludes that), and a `snowplow_refresher_`-prefixed literal fails the C7 parity
guard. So it stays expvar-only; see its row below.

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
| `snowplow_refresher_residency_cheapen` (map, expvar only; diagnostics, not a C7-tagged family) | two residency counters. `pickup_noop_no_park` (**#374 Part B**): dequeues where the worker found the key not resident and skipped the customer-priority yield park, since processing it is a no-op. `enqueue_dropped_non_resident` (**#383**): dirty-marks whose queue slot and trigger GVRs were DROPPED at the refresher hook because the key was not resident in L1 at the mark. `SubmitSliceabilityInvalidate` still fired for each of them (#91 Lever C), and #375/#408 remark the key once its in-flight Put lands. Before #383 those marks were enqueued and later counted in `snowplow_refresher_skipped_no_entry_total` | both are load-dependent. `enqueue_dropped_non_resident` per mark is the non-resident share of churn, which swings roughly 39–97% across load (#386-M2), so always quote it with the load. Zero `enqueue_dropped_non_resident` under churn with non-zero `skipped_no_entry_total` means the drop gate is not running |
| `snowplow_resolve_latency_p95_ms` (**#386 M1** — standalone scalar OUTSIDE the `snowplow_refresher_` C7 family) | p95 of the REAL resolve latency (the handler re-resolve+Put in `processOne`), a P² streaming estimate sampled ONLY on the ok=true path AFTER `yieldToCustomer` returns — so it EXCLUDES the dequeue, the customer-priority park, the `skipped_no_entry`/`skipped_no_handler` skips and the rate-floor defer | a DIAGNOSTIC sizing input for #384/#365 (≈≲31ms observed). **expvar-only by design** — a sizing input read on demand, not an alert (the backlog `queue_depth`/`completed` is the alert), so deliberately NOT OTLP-mirrored and kept out of the auto-mirrored C7 family |
| `snowplow_refresher_dirty_to_fresh_samples_total` | **#354 P3.** invalidation→fresh windows CLOSED FRESH: a resident cell's first dirty-mark (an informer event fanned out to the key, or a #375 remark) to the refresher's accepted, un-remarked re-Put. Each sample is also one observation of the OTLP histogram `snowplow_refresher_dirty_to_fresh_ms` (synchronous `Int64Histogram`, no attributes, bounds 50/100/250/500/750/1000/2000/5000/10000/30000/60000 ms — bracketing the 1 s north-star and the 10 s AC-98.12 SLA). A retried error and a rate-floor deferral stay inside the window | must be > 0 on a stage with churn, or the stage is invalid (no window measured) |
| `snowplow_refresher_dirty_to_fresh_ms_p95`, `snowplow_refresher_dirty_to_fresh_ms_max` | **#354 P3.** GAUGES — p95 (P²) and max of the window (ms) over roughly the last OTLP export interval (two estimators staggered by half an interval, the older read). For `/debug/vars` parity and alerting without a histogram query; the distribution is the histogram | p95 under 1 s (fresh north-star); max under 10 s (AC-98.12) |
| `snowplow_refresher_dirty_ended_unfresh_remarked_total`, `snowplow_refresher_dirty_ended_unfresh_evicted_total`, `snowplow_refresher_dirty_ended_unfresh_declined_total`, `snowplow_refresher_dirty_ended_unfresh_dropped_total` | **#354 P3.** dequeues of a dirty key that did NOT end its window fresh, FLATTENED by reason on the shared counter (OTLP `snowplow_refresher{stat=dirty_ended_unfresh_<reason>}`; closed set). `remarked`: the re-Put was accepted but #375 remarked it (a dep moved mid-resolve) — the window stays open from the ORIGINAL mark and the next clean Put samples it. `evicted`: the cell left the store while dirty (DELETE, TTL/LRU/max-age, #444 no representative, a refused replace). `declined`: the refresh returned without a Put (stage error, external, UAF, sensitive, empty full, unsupported, suppressed, no handler). `dropped`: the poison-pill bound gave up | `remarked` tracks churn during resolves; `declined` steady = cells the refresher cannot freshen (read with `suppressed_keys`); `dropped` ≈ 0 |
| `snowplow_refresher_dirty_to_fresh_*` / `stale_served_*` — **read with their two CONSERVATIVE biases** (#354 P3; both over-report, never under-report) | (a) only the refresher's terminal write closes a window: a non-refresher replace of a dirty cell (keep-warm sweep, seed, gvr-discovered, #258 reseed) leaves it open, so a window can outlast the moment a fresh body actually landed; (b) `stale_served_total` counts hits on a dirty-marked cell even when its refresh then proves the content unchanged (an unchanged refresh is accepted as FRESH), so it means "served while potentially stale". A driver report must state both | — |
| `snowplow_refresher_parked_ms_total` | **#354 P3 (B2/Q3).** wall time (ms) refresher workers spent parked in the customer-priority yield, first park to release, a capped exit included. OTLP `snowplow_refresher{stat=parked_ms}` | read with `yielded` / `capped`: parked time per window is the yield's share of the dirty→fresh window |

**#386 M2 — real drain rate (reader-computed; NO new metric).** The refresher's real
re-resolve throughput is `Δ(snowplow_refresher_completed_total − snowplow_refresher_skipped_no_entry_total)/interval`,
computed by the reader (the #365 model / the #384 control loop) at its own interval — it is
NOT a stored rate metric (that would hard-code one interval and need a background sampler).
Why the subtraction: `completed_total` increments on **every** non-error dequeue, and a
`skipped_no_entry` dequeue (a non-resident key the refresher picked up) hits the success
branch and bumps `completed_total` too — so raw `completed/s` **over-estimates** real work
(on 057 ~92% of completions are cheap skips → ~12.5× over-estimate). **Source
`skipped_no_entry_total`, NOT the L1 `miss_total`:** post-#376 (GetNoTouch) the refresher's
dequeue read no longer contaminates `miss_total`, so `miss_total` is miss-neutral and the
refresher's own skip counter is the independent, #376-surviving source.

### Customer resolve-path in-flight (#386 M3)
Defined in `internal/handlers/dispatchers/customer_inflight_metrics.go`.

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_customer_resolve_inflight` | gauge — customer `/call` dispatches currently executing on the **RESOLVE path**: GET `/call` + POST `/call/read` that reached a restactions/widgets handler, where `markCustomerInFlight` brackets `ServeHTTP`. This is the SAME population the refresher's customer-priority yield keys off, so #384 serveReserve sizes against the same customer definition the live yield uses | 0 at idle (no customers in flight = healthy — a correlation signal, not an alarm); rises with concurrent resolve-path load. **SCOPE — NOT "all customer activity":** EXCLUDES the direct-proxy `Call()`/`CallRead()` fallthrough, `GET /list`, and all write verbs (POST/PUT/PATCH/DELETE `/call` route straight to `Call()` with no Dispatcher) — those are I/O-bound apiserver proxies that do not contend for the refresher's resolve-CPU. Counted ONCE per OUTERMOST call — `markCustomerInFlight` is at `ServeHTTP` entry only; a nested resolve runs via the in-process `apiref.Resolve` path and never re-enters `ServeHTTP`, so it does not re-mark. Cache-gated expvar, mirrored to OTLP as the `snowplow_customer_resolve_inflight` `Int64ObservableGauge` (#386 M3 PR2 — hand-wired in `metrics.go` on #311's pattern, no expvar→OTLP bridge, scalars-only) for ClickStack correlation (refresher/cache behavior vs customer load). A correlation gauge, NOT a detector |

### v7 wildcard digest-collision probe (#368)

A dark probe (`internal/handlers/dispatchers/shadow_wildcard_digest_probe.go`)
that certifies the ClassWildcard projection digest before v7 shares cells
across identities. Its three counters are hand-wired OTLP counters (sums).
Before #455 they were observed but never registered with the callback, so the
SDK dropped every observation and none of them reached ClickStack.

| OTLP instrument | meaning | healthy |
|---|---|---|
| `snowplow_v7_shadow_wildcard_digest_collision_total` | **DETECTOR.** Shareable, ungated wildcard cells whose digest failed to tell apart two identities with different evaluator access (a Step-3 share leak) | **0**. Alert on > 0 |
| `snowplow_v7_shadow_wildcard_digest_observed_total` | the denominator: distinct (cell, digest) pairs seen with ≥ 2 identities, shareable and ungated | > 0 once the enumerate projection ungates. 0 means not yet exercised, **not** certified |
| `snowplow_v7_shadow_wildcard_digest_evicted_total` | observations dropped by the probe's caps (LRU, per-entry coordinate or identity cap). A dropped observation can miss a collision | **0**. Any value voids certification for the window: raise the caps and re-measure |

The certification rule is read in ClickStack as step 6 of the
[POST-ROLL CHECKLIST](#post-roll-checklist-clickstack).

### Live refresh (SSE)
Defined in `internal/cache/refresh_broadcaster_expvar.go`; one expvar key,
`snowplow_refresh_broadcaster`, a `map{stat → value}` derived from the `RefreshBroadcasterStats`
tags (1.12.6 C7). The OTLP mirror publishes each stat as
`snowplow_refresh_broadcaster_<stat>` (counters gain `_total`).

| stat | meaning | healthy range |
|---|---|---|
| `published` | live-refresh signals published by the resolver / refresher on an L1 commit | tracks L1 commits under churn |
| `delivered` | signals delivered into a subscriber's sink | tracks published × armed subscribers |
| `dropped` | signals dropped on a full subscriber sink. **Structurally 0 since #484**: a full sink defers the key (`deferred`) instead; nothing increments it | **0**; any value is a regression of the never-drop invariant |
| `deferred` | **#484.** refresher-path signals a full subscriber sink deferred into that subscriber's refresh pending set (dedup'd, ⊆ the armed set, un-paced); delivered as soon as the sink has room | > 0 only while a consumer is behind; sustained growth with `pending_overflow_resync` = wedged consumers |
| `coalesced` | signals suppressed by the per-key 250 ms coalesce window. Since #484 a window that suppressed a signal owes exactly one trailing emit (`trailing_emitted`), so a coalesced signal is deferred to the window end, not lost | informational |
| `trailing_emitted` | **#484.** trailing-edge emits: coalesce windows that suppressed a signal and re-announced the key at the window end | ≤ `coalesced`; > 0 wherever `coalesced` > 0 |
| `pending_coalesced` | **#484.** refresher-path signals absorbed because the key was already queued for that subscriber (refresh- or eviction-pending) — that queued frame delivers it once | informational; moves with `deferred` |
| `pending_overflow_resync` | **#484.** subscribers force-resynced by the pending-drain stall rule: the consumer accepted **no** frame for a full liveness interval (20 s, the heartbeat), so its backlog was released and the stream ended with `event: resync`; the SPA re-validates every armed widget on reconnect (`refreshSse.ts` scheduleRetry → revalidateArmed) | **near 0**; sustained > 0 = clients behind a stalled hop (see G1) |
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
| `snowplow_rbac_subgen_bumps_total` (`internal/cache/rbac_subgen_expvar.go`) | `uint64` — cumulative PER-SUBJECT sub-generation bumps (one per subject per bump, so a publish flushing 40 subjects adds 40). This, not `publish_seq`, is the rate at which identity-bound L1 keys rotate | rises with RBAC churn; `0` = no identity-bound key has ever rotated for an RBAC reason. The key is always present, so `0` never means "not instrumented" |
| `snowplow_rbac_subgen_subjects_tracked` (`internal/cache/rbac_subgen_expvar.go`) | `uint64` — distinct subjects that have a sub-generation counter; the blast-radius denominator for `bumps_total`. **HIGH-WATER MARK, NOT A RATE** — entries are never removed, so it ratchets up then saturates | read the RATIO, never a delta: `bumps_total` high with `subjects_tracked` low = churn concentrated on a few subjects (one tenant); both high = fleet-wide rotation. A **flat line means saturation, not quiet** — it reads the same healthy and broken |
| `snowplow_rbac_binding_noop_updates_total` (`internal/cache/rbac_binding_noop_counters.go`) | `uint64` — (Cluster)RoleBinding UPDATE events redelivering the SAME per-object `resourceVersion`. A watch re-establishment dispatches a Sync delta as `OnUpdate(old,new)` with old == new, so this isolates RELIST FAN-OUT. Since #253 such an event records **no** sub-generation bump (it is a semantic no-op, below) — this counter still counts it | rising in steps of roughly the binding count = watch churn. **A zero means "no relists", NOT "no label/annotation churn"** — a label/annotation write changes the RV and is counted only by the semantic key below. Both no-op counters are **UPDATE-only and structurally blind to creates/deletes** (`onBindingAdd`/`onBindingDelete` bump unconditionally; an ADD has no old side), so on a cluster whose binding count never grows they say nothing about a create-dominated production workload |
| `snowplow_rbac_binding_semantic_noop_updates_total` (`internal/cache/rbac_binding_noop_counters.go`) | `uint64` — UPDATE events whose subject set (order-insensitive multiset of Kind/Name/Namespace) and roleRef (+ binding namespace) were both unchanged, at the same `metadata.uid`, at any `resourceVersion`. A SUPERSET of the key above (a uid-changed UPDATE is counted by `snowplow_rbac_binding_uid_changed_updates_total` instead, #260). **Since #253 this is exactly the set of binding UPDATEs that record NO sub-generation bump** — the skip and this counter share one predicate (`bindingUpdateSemanticallyUnchanged`) | climbing on label/annotation churn and relists is healthy: each increment is a key rotation #253 avoided. To confirm the skip in production, read it against the `binding_update` bucket of `snowplow_rbac_subgen_bumps_by_source_total` (which must not move for these events), **never** against `snowplow_rbac_subgen_bumps_total`, which also moves on binding ADD/DELETE, the role path (#257) and ServiceAccount churn |
| `snowplow_authz_memo_hits` / `_misses` / `_swaps` / `_refused` / `_entries` (`internal/rbac/snapshot_authz_memo.go` via `RegisterAuthzMemoExpvar`) | memo hit/miss, generation shard swaps, cap-breach refusals, live entry count | hit rate ≥0.85 warm; swaps bump on snapshot generation change; refused low |
| `snowplow_authz_memo_deny_uncached_total` | `uint64` — denies (never cached, by design) | informational; should be > 0 and rising on a live cluster — a flat 0 with denied traffic would suggest the PERMITS-only rule regressed |

### Security / verification counters on OTLP (#448)

These counters used to be `/debug/vars`-only, which needs a user JWT, so the
post-roll checks of three security releases went unread. Since #448 each one is
hand-wired in `internal/metrics/metrics.go` (`registerSecurityInstruments`, no
expvar→OTLP bridge) and reads the same typed accessor its expvar closure reads.
Rules the mirror follows:

- **Closed attributes (F8).** Every attribute comes from a code-defined set:
  `site` ∈ {restactions, widgets, seed, refresher}, `reason` ∈ {binding_set,
  rbac_subgen, no_identity}, `outcome` ∈ {group, hitter, evicted}, `class` ∈ {restactions, widgets,
  widgetContent, apistage, raFullList} (#378 re-mint), `stat` ∈
  the learned-capacity inputs, `bound` ∈ {none, memory, engine}. No username,
  group or Secret name is ever an attribute. The drift, re-pick and re-mint
  counters emit their full closed sets (zeros included), so their series sets never grow
  with traffic.
- **Cache-off.** Same CFG-1 rule as the expvar keys: under `CACHE_ENABLED`
  off, none of these series is registered (absent, not zero).
- **Monotonic counters** are `ObservableCounter`s over their own atomics, and
  no series is derived by subtracting two others. Gauges are current-state.

| OTLP instrument | kind | meaning | healthy |
|---|---|---|---|
| `snowplow_l1_identity_class_drift_declined_total{site,reason}` | counter (sum) | L1 Puts / re-Puts the #424 guard declined because the writer's identity no longer belongs to the RBAC class the key was minted for | low, non-zero rate on a cluster with RBAC churn (a grant/revoke landing mid-resolve). `no_identity` should stay 0 |
| `snowplow_l1_representative_repick_total{outcome}` | counter (sum) | #444 refresher outcomes when a cell's recorded representative drifted out of its RBAC class: `group` (re-picked the canonical group representative), `hitter` (re-picked a recent hitter), `evicted` (no in-class representative; the cell was evicted). The evictions are also `snowplow_resolved_cache{stat=evict_no_representative_total}` (a gauge row, already on OTLP through the `snowplow_resolved_cache` mirror) | low; follows personal (User-subject) RBAC changes on representatives. `evicted` should stay well below `group` + `hitter` |
| `snowplow_resolved_cache_remint_total{class}` | counter (sum) | **#378.** refresher-terminal re-mints (BornAt resets under the same key, inside the lead window `[maxAge − L, …)`), by `class` ∈ {restactions, widgets, widgetContent, apistage, raFullList} — the full closed set is emitted (zeros included). Their sum is `snowplow_resolved_cache{stat=remint_total}` | > 0 per class with a **customer-warm** working set once a pod is older than `maxAge − L`; since #496 a zero per class is correct when nothing of that class is warm, so read it with `snowplow_resolved_cache{stat=remint_refused_cold_total}` before calling it inert; `evict_max_age_warm_customer_total` stays 0 apart from the #496 late-first-read residual (see its row above) — a sustained rise is still P5 |
| `snowplow_binding_set_memo_hits` / `_misses` / `_refused` | counter (sum) | subject binding-set digest memo (#424). The memo shard swaps on every RBAC snapshot publish | hit ratio `hits/(hits+misses)` high once warm. `refused` = 0 (cap 4096 per shard) |
| `snowplow_binding_set_memo_entries` | gauge | live entries in the current memo shard | ≈ active identities |
| `snowplow_learned_classes_registered` / `_seeded` / `_unseeded_capacity` / `_nav_only` | gauge | learned identity classes (#262): in the registry, admitted with ≥1 distinct target, left out by the capacity bound, seeded nav-only at boot then dropped | `seeded` ≈ 0 on group-only RBAC clusters (a group-only member is not a distinct target) |
| `snowplow_learned_classes_from_secrets` / `_secrets_unparseable`, `snowplow_learned_clientconfig_secrets` | gauge | classes backed by a `*-clientconfig` Secret, Secrets no class could be read from, and every `*-clientconfig` Secret seen | **ALARM: `from_secrets == 0` while `clientconfig_secrets > 0`** (authn's certificate format moved) |
| `snowplow_learned_classes_capacity{stat}` | gauge | the measured inputs of the last learned-class bound decision (booleans as 0/1) | informational |
| `snowplow_learned_classes_capacity_bound{bound}` | gauge | 1 on the bound that limited the last decision, 0 on the others; no series before the first decision | informational |

The parity arm is `TestC7_OTLP_EveryDerivedStatLeavesTheProcess` (each series
driven to a distinct value through its production recorder, then read off the
OTLP/HTTP wire). The `metrics_448_security_otlp_test.go` arms pin cache-off
absence, attribute hygiene, the clientconfig alarm, and that the closed sets
match what the producers write.

### Informer / discovery surface

| expvar | meaning | healthy range |
|---|---|---|
| `snowplow_plurals_registered_gvrs` (`internal/cache/registered_gvrs_expvar.go`) | `{count, gvrs:[…], last_register_unix_ns}` — live set of GVRs with a registered informer | `count` tracks the cluster's served GVR set; two scrapes with identical `last_register_unix_ns` = informer set quiesced |
| `snowplow_crd_discovery` (`internal/cache/crd_discovery_expvar.go`) | `map{stat → value}` derived from the `CRDDiscoveryStats` tags (1.12.6 C7); mirrored to OTLP as `snowplow_crd_discovery{stat}`. Per-stat table below | `events_dropped`/`*_skipped_ng`/`panics_recovered` should be **0** |
| `snowplow_crd_schema_memo_hits_total` / `_misses_total` / `_stale_dropped_total` / `_invalidations_total` (`internal/resolvers/crds/schema/schema_cache_metrics.go`) | compiled-CRD-schema memo counters | high hit ratio warm; stale-drops expected under concurrent CRD install |
| `snowplow_sa_discovery_builds_total` / `_invalidations_total` / `_fallbacks_total` (`internal/dynamic/cached_client_metrics.go`) | SA-discovery client lifecycle | fallbacks low; climb = discovery degrading |
| `snowplow_discovery` (`internal/dynamic/partial_discovery.go`, #517) | `{total, partial_total, fatal_total, failed_groups:[…]}` for `Client.Discover` (the `GET /list` discovery). `partial_total` counts PARTIAL discoveries honoured as degraded (one or more API group/versions failed, the healthy groups were still served); `fatal_total` counts the calls `/list` answers 500 for; `failed_groups` names the group/versions of the most recent degradation. NOT gated on `CACHE_ENABLED` — `/list` performs discovery either way | `partial_total`/`fatal_total` **0** while `total > 0` = healthy. `total == 0` means no `/list` traffic, NOT health — always read the pair. A climbing `partial_total` with a group named in `failed_groups` = a stale aggregated APIService: `/list` is serving a SHORTER list than the cluster has |

#### `snowplow_informer_watch` stats

Two 1.12.7 counters for informer failures that previously left no trace anywhere but a log line.
Tag-derived (`InformerWatchStats`), so they reach expvar, OTLP, this doc's guard and the C7 parity
arms from one struct tag each.

| stat | meaning | healthy range |
|---|---|---|
| `watch_errors_total` | reflector `ListAndWatch` errors across **every** informer family (per-GVR, secrets, controller-health). Counts EVERY invocation — our handlers replace client-go's default and each logs only its FIRST failure, so before this a watch failing once and a watch failing every second produced the same single WARN | **0**. A climbing value is the retry RATE, which is the thing the one-shot WARN hides. Read it next to `cache.watch.broken` / `cache.secrets.watch.broken` |
| `confirm_retracted_total` | GVRs whose servability confirmation was retracted **after having been granted**, on a **definite-absent** discovery answer — either a **successful** `ServerResourcesForGroupVersion` whose list omits the resource, or an **authoritative** `NotFound(404)`/`Gone(410)` error (the apiserver stating the group/version is gone). A retracted GVR silently stops serving from the informer and falls through to the apiserver; #217 took a day to characterise because the retraction left no trace. Counted only when a confirmation actually existed, so ordinary teardowns of never-confirmed GVRs do not inflate it. **#217:** a *transient* discovery error (timeout/5xx/throttle/transport/nil list) no longer retracts — it fails open and increments `confirm_retained_unknown_total` instead. An authoritative 404/410 still retracts, so `RefreshDiscovery` stays the reconciling backstop for a genuine removal whose CRD-DELETE event was missed | **0** in steady state. Non-zero is expected around a CRD upgrade or delete — use the by-reason map below to tell which |
| `confirm_retained_unknown_total` | **#217 detector — pairs with `confirm_retracted_total`.** Retain-on-**unknown** decisions: a granted confirmation **held open** because discovery was genuinely uncertain — a *transient* error (timeout/5xx/throttle/transport) or a nil list, where conjunct-4 could not be evaluated — rather than retracted. Fail-open on uncertainty ONLY; an authoritative `NotFound`/`Gone` is definite-absent and **retracts** instead (never held). Mirrors the #119 group pre-check registering when `ServerGroups()` cannot answer. Incremented **per decision** (per group/version per refresh), so it carries the flap RATE. Unlike `confirm_retracted_total`, which reads-as-*health* during flaky discovery (retractions stop once errors stop), this reads **non-zero DURING** the defect, so it is the real detector | **0** in steady state. `retained_unknown` climbing while `retracted` stays **flat** = discovery is blipping and fail-open is holding a healthy GVR (working as designed, **not** an error). Both climbing = a genuine type change amid discovery noise. Use the by-reason map below |

`snowplow_informer_confirm_retracted_by_reason` is the `{reason}` breakdown, a map keyed by the
code path that retracted: `discovery_refresh` / `scoped_confirm` / `walk_confirm` (a **definite-absent**
successful discovery) and `schema_relist` / `stale_version_pruned` / `crd_deleted` (the informer
was torn down). It rides alongside rather than inside the tagged family because the C7 tag system
has no label facility — a `stat` tag yields exactly one scalar.

`snowplow_informer_confirm_retained_unknown_by_reason` is the matching `{reason}` breakdown for the
retain-on-unknown detector, keyed by the discovery-error **class**: `discovery_error` (a generic
`ServerResourcesForGroupVersion` error), `timeout` (an apiserver timeout / unavailable / throttle /
internal-error class) and `nil_list` (no error but a nil resource list). Same shape and gating as the
retracted map — one scalar per key, published outside the tagged family for the same reason.

#### `snowplow_reflector_path` stats

**#237 A3.** Which path our informers' LISTs and WATCHes take on the wire, per process, plus a
per-GVR breakdown. Before this the question "is this GVR being served from the apiserver watch
cache, or is it paging etcd?" was answerable only from a `slog.Info` — and the chart ships
`LOG_LEVEL=warn`, so the live pod's 20 h log was 217 WARN + 30 ERROR and **zero INFO**. Absence
there was a statement about the instrument, not about the code path. Tag-derived
(`ReflectorPathStats`), so the counters reach expvar, OTLP, this doc's guard and the C7 parity
arms from one struct tag each.

Classification is on the request **path shape**: only GETs to a collection endpoint
(`/apis/<g>/<v>/<r>`, `/api/<v>/<r>`, with or without an interposed `/namespaces/<ns>/`) are
bucketed. Named GETs, subresources, discovery, non-resource paths and every non-GET verb are
**not counted at all** rather than swept into a catch-all — the wrapper rides the shared
`rest.Config`, so a catch-all would mix "the reflector listed" with "a customer request fell
through to the apiserver", which is two regimes in one number.

| stat | meaning | healthy range |
|---|---|---|
| `reflector_transport_requests_total` | every outbound request the wrapper delegated | **DENOMINATOR, not a detector.** `0` means the wrapper is not installed (or the process issues no requests) and makes every zero below meaningless |
| `reflector_collection_requests_total` | collection GETs (LIST or WATCH) classified into the five buckets below | climbing. `transport > 0` with **this at 0** is the alarmable reading: the wrapper sees traffic but recognises no LIST at all — a broken classifier masquerading as a quiet reflector |
| `reflector_watchlist_established_total` | WATCHes carrying `sendInitialEvents`: initial state streamed from the apiserver watch cache | climbing on a server that honours watch-list. **Flat or 0 means nothing on its own** — read it against the two `list_*` counters below; the PAIR is the detector |
| `reflector_watch_plain_total` | WATCHes without `sendInitialEvents` — the ordinary re-watch after a clean timeout | climbs under **both** regimes, so it is not a discriminator; it is the proof the wrapper is wired at all |
| `reflector_list_page_total` | LISTs carrying a `continue` token | a continue page is an etcd read by definition and its page size bounds it — correct traffic, not a bypass |
| `reflector_list_etcd_delegated_total` | LISTs carrying a `limit` **together with** a `resourceVersion` that is neither empty nor `0` — delegated to etcd, **skipping the watch cache** | **non-zero by construction on today's binary** (`listOptionsTweak` sets `Limit` unconditionally, inside the ListFunc, after the pager has already decided). This is the C1 follow-up's only production proof: it must read **0** once the tweak mirrors client-go's rule |
| `reflector_list_cache_eligible_total` | every other LIST (`resourceVersion` empty or `0`, or no `limit`) — the cacher can serve these | climbing at boot, ~0 afterwards |
| `reflector_path_transitions_total` | per-GVR path changes, including each GVR's first observation | one per GVR at boot, then **flat**. A later increment means a GVR flipped between `watchlist` and `list` and is paired with a `cache.reflector.path` WARN naming the GVR and both sides |

`snowplow_reflector_path_by_gvr` is the per-GVR breakdown, a map of GVR → current path
(`watchlist` or `list`). It rides alongside rather than inside the tagged family for the same
reason the retraction breakdown does — a `stat` tag yields exactly one scalar. This map is what
answers "has **this** GVR relisted, and how?", which #237 recorded as unanswerable from outside
the process: `watch_errors_total` is a single global total that cannot separate one GVR failing in
a loop from every GVR recycling normally.

**The map is narrower than the counters, on purpose.** A GVR appears only while it has a
registered informer, and only a request shaped like a reflector establishment updates it —
a watch carrying `sendInitialEvents`, or a LIST carrying a `resourceVersion` (`r.list()` always
sets one; our own passthrough and fallthrough LISTs do not). Without those two rules the map
would record for LISTs no reflector issued: in `CACHE_ENABLED=false` there are **no reflectors at
all**, yet a passthrough LIST would raise a `cache.reflector.path` WARN saying the apiserver
stopped honouring `SendInitialEvents`; and a fallthrough LIST for a registered-but-unservable GVR
would flip an established `watchlist` to `list` and back on the next re-establishment. The
counters are deliberately *not* narrowed — an inflated total is a number you can caveat, an alarm
naming a cause that did not happen is not. The row is dropped when the informer is torn down, so
after a CRD version is retired no path is reported for it; a rebuild logs a fresh first
observation, which is what you want confirmed after a relist.

The log line is `cache.reflector.path`, at **WARN** and **only on a transition** (first
observation, and any change after). Warn because the chart ships at warn and an INFO line would be
invisible exactly when it is needed; transition-only because a per-request line would flood the
log it is meant to be readable in.

#### `snowplow_store_verification` stats

**#237 B.** Does the informer store still agree with the apiserver? Before this, nothing in
snowplow handled *"still there, but different"*: `DepTracker.OnUpdate` fires on the watch UPDATE
event (for a LOST event the signal and the data share one failure), the C2 relist bridge diffs
`ns/name` key SETS (a key present on both sides takes the `continue` no matter how much its
content changed — which is why `relist_bridge_enqueued_total` read 0 across 108 runs, correctly),
and the C3 reconcile audit acts only on `objAbsent` (a present-but-stale object is skipped before
any field is compared, which is why the live capture read `divergent: 0` with the defect active).
Every safety net was a **deletion detector**.

The comparison runs where truth already arrives: every relist fetches the complete authoritative
object set for its GVR, and a decorator on the ListerWatcher diffs it against the store an instant
before `Replace()`. Total coverage per GVR, **zero additional apiserver load**, and it fires on the
event rather than on a cadence. The deadline walker below is the backstop for GVRs whose watch only
ever recycles and therefore never snapshot.

**READ THIS BEFORE READING A ZERO.** The oracle is *the snapshot we just received*, not the
apiserver. These counters answer "does our store match the object set the apiserver last handed
us", **not** "does our store match the apiserver". If the incoming snapshot is itself a stale
watch-cache read — #237's *"the relist happened and did not fix it"* candidate — the comparison
finds equality and **every divergence counter reads 0 with the defect active**. The forced pass is
only partly immune: `ResourceVersionMatch=NotOlderThan` at our own `lastSyncRV` makes the cacher
block until it reaches OUR resourceVersion, which is a guarantee about our store's position, not
about etcd's. This is not closed, because the only thing that would close it is a quorum read per
snapshot — precisely the apiserver load the cache exists to avoid.

**So what this deliverable may be said to have done, with the qualifier attached every time:** it
closes invisibility for every loss class **except a stale cacher**. Root cause on #237 is still
**open** and the stale-cacher candidate is live, so this must not be described as having made #237
detectable — only its other candidate classes.

The four divergence classes carry a **site**. `snapshot` divergences are already repaired by
client-go (`processDeltas` writes the store and *then* calls the handler), so
`store_repairs_fired_total` correctly stays 0 for them; only `forced` divergences need snowplow's
own repair verb. Without the split, "divergence climbing while repairs flat" would fire on every
correctly repaired snapshot divergence — and an alarm that cries wolf gets muted, taking the
signal it exists for with it.

| stat | meaning | healthy range |
|---|---|---|
| `store_verifications_total` | completed comparisons (snapshot plus forced) | **DENOMINATOR, not a detector.** Climbs in both regimes. A zero here makes every zero below meaningless |
| `store_objects_verified_total` | objects compared across all verifications | **DENOMINATOR.** Per forced pass it must advance by the GVR's FULL `indexerCount` — a shortfall means sampling crept in and the coverage guarantee (*the deadline chooses WHEN, never WHAT*) is silently gone |
| `store_verification_forced_lists_total` | `PartialObjectMetadata` LISTs issued by the deadline walker | **DENOMINATOR for the forced path.** At most one per GVR per `maxVerificationAge` (= `ResolvedCacheTTL`, 3600 s default). Above that rate the deadline or the pacing is broken — the failure mode that could pressure the apiserver |
| `store_divergent_lost_update_snapshot_total` | same uid, different `resourceVersion`, found at a snapshot — a lost UPDATE | **0**. Non-zero during #237. Already repaired by client-go when counted here |
| `store_divergent_lost_update_forced_total` | same class, found by the deadline pass | **0**. NOT self-repairing: climbing while `store_repairs_fired_total` stays flat is detection without repair |
| `store_divergent_lost_delete_snapshot_total` | in the store, absent upstream, found at a snapshot | **0**. Repaired by client-go's synthesised `Deleted` delta |
| `store_divergent_lost_delete_forced_total` | same class, found by the deadline pass | **0**. Needs the repair verb |
| `store_divergent_lost_add_snapshot_total` | upstream, absent from the store, found at a snapshot | **0**. NOT counted on a GVR's first sync — an empty store against a full set is the normal case, and that skip is `skipped_by_reason{initial_sync}` |
| `store_divergent_lost_add_forced_total` | same class, found by the deadline pass | **0** |
| `store_divergent_uid_mismatch_snapshot_total` | same `(namespace,name)`, **different uid** — a delete-and-recreate phantom | **0**. The class every existing repair path is structurally blind to: the C2 bridge diffs `ns/name`, and `ResolvedKeyInputs` carries no object uid, so a recreate under the same name reuses the byte-identical L1 cell |
| `store_divergent_uid_mismatch_forced_total` | same class, found by the deadline pass | **0**. Load-bearing rather than defensive — #237's ten-day staleness IS a uid phantom |
| `store_divergence_candidates_total` | RAW divergences, before the bounded re-read confirmed them | ~0. A large candidates-minus-confirmed gap is a **DeltaFIFO backlog** (the indexer lagging the queue), not a stale store — which is why the raw number is published rather than hidden |
| `store_gvrs_unverified` | servable, synced, reachable GVRs past `maxVerificationAge` | **0**. During #237 with B working this reads **0** — the deadline reached the GVR and found the divergence. It is NOT the detector; it is the DENOMINATOR that makes a zero divergence readable: `(unverified 0, divergent 0)` = *I looked and found nothing*; `(unverified >0, divergent 0)` = **I did not look**. That is exactly the distinction the 1.12.6 audit's `divergent: 0` could not express |
| `store_gvrs_unverifiable` | registered GVRs no mechanism reaches right now — breakdown in `snowplow_store_verification_unverifiable_by_reason` (`retracted` / `not_synced` / `unreachable`) | **0**. Split from `store_gvrs_unverified` so a retracted GVR cannot be quietly dropped from that gauge's set and produce a zero that reads as health |
| `store_gvrs_undecorated` | registered GVRs whose informer never got the verifying ListerWatcher, so only the deadline pass covers them | **not 0** — the healthy value is the size of the non-streaming class (GVRs whose informer client-go constructs internally, where snowplow cannot decorate the ListerWatcher). Read it as a CLASS, not a list: membership follows from informer routing, which H5 has already changed once. Watch for the JUMP: with `RESOLVER_COMPOSITION_STREAMING_LIST=false` this goes to nearly every GVR while every `*_snapshot_total` silently drops to 0 |
| `store_verification_max_age_seconds` | oldest age-since-last-verified across the `store_gvrs_unverified` set | at or below `maxVerificationAge`. **At boot it reads age-since-REGISTRATION and never 0** — a never-verified GVR excluded from the max would read 0 while nothing had been verified at all |
| `store_verification_skipped_total` | comparisons declined — breakdown in `snowplow_store_verification_skipped_by_reason` | read the **breakdown**, never this total: it deliberately spans expected reasons (`initial_sync`, once per GVR per informer lifetime; `undecorated_informer`, once per undecorated registration) and unexpected ones (`torn_down`, `metaclient_nil`, `list_error`, `no_sync_position` — the last meaning the watcher held no recorded sync position, so the pass could not issue a LIST whose result is provably no older than the store, and **skipped rather than downgrading to** `resourceVersion=0`, which can answer from behind our store and invert every divergence class). It is what gives *"no divergence found"* its scope — without it, a pass that never ran and a pass that found nothing are the same zero |
| `store_verification_invalidated_total` | verifications ended by a servability retraction | **CONTEXT, NOT A DETECTOR** — it climbs in both regimes (krateo-057 measured 10,011 `discovery_refresh` retractions in 18 h). It is what explains a GVR's age resetting with no verification having run |
| `store_repairs_fired_total` | targeted relists fired to repair a divergence the deadline pass found | **0**. Read it AGAINST `store_divergent_*_forced_total`: divergence climbing while this stays flat is a green counter over a still-wrong store |
| `store_repair_ineffective_total` | repairs after which the NEXT verification still reported divergence | **0, and ALARMABLE.** The only stat here whose non-zero means the **detector** is wrong rather than the store: a full relist that does not clear a divergence does not indicate a lost event |
| `store_repair_unsupported_total` | divergences whose GVR cannot be repaired by a relist at all, because its informer comes from the **shared dynamic factory** — which caches by GVR with no eviction API, so a teardown plus re-register hands back the **stopped** informer and kills the cache for that GVR instead of repairing it | **0** in production, where the streaming path owns the informer for the overwhelming majority of GVRs. Non-zero is the honest statement that this GVR is **detected and never repaired** — a state that would otherwise be indistinguishable from a divergence nobody acted on, and one the permanence bound below does NOT cover. The refused set is a CLASS — *a GVR whose informer is factory-built rather than owned* — determined by construction and recorded at registration, never inferred from the GVR's group. Naming today's members here would make this row wrong the next time informer routing changes |
| `store_repair_suppressed_total` | GVRs the circuit breaker has latched | **0**. Non-zero means the staleness bound no longer holds for that GVR — it is **unbounded** and the alarm is the only mitigation. Detection continues; the breaker never produces silence |
| `store_repairs_pending` | GVRs waiting in the serialised repair queue | **0**. Repairs run ONE AT A TIME across all GVRs, so a correlated divergence cannot take the cache non-servable everywhere at once; the price is queue wait, and this is where it shows |
| `store_repair_queue_age_seconds` | age of the oldest queued repair, measured from detection | **0**. Rising without bound means the serialised queue is not draining — the regime in which the staleness bound (`maxVerificationAge` + **queue wait** + one relist) stops being minutes and becomes hours |

A repair **retracts the GVR's confirmation for the whole rebuild window** — minutes at 50K — so
every resolve for it falls through to the apiserver until the replacement informer syncs. That is
the deliberate trade of *fast-but-wrong* for *slow-but-correct*, it is the largest single cost in
this deliverable, and it is why repairs are globally serialised (one in flight across all GVRs,
waiting for the replacement informer to sync — serialising only the teardown *calls* would leave
the rebuild *windows* overlapping, and the window is what makes a GVR non-servable) and
circuit-broken. The log lines are `cache.store.divergent` and `cache.store.repair` at **WARN**, and
`cache.store.repair_ineffective` at **ERROR**.

**What this does and does not close.** Invisibility is closed for every loss class **except a stale
apiserver watch cache** — see the scope limit above; that qualifier travels with every claim made
about this deliverable. Staleness is bounded by `maxVerificationAge` + repair-queue wait + one
relist for a GVR that is repairable and unsuppressed. It is **unbounded** for two classes: a GVR the
circuit breaker has latched, and **a GVR whose informer is factory-built rather than owned**, which
is detected, refused out loud, and never repaired. Neither is a bound and neither may be described
as one.

#### `snowplow_crd_discovery` stats

| stat | meaning | healthy range |
|---|---|---|
| `events_enqueued`, `events_processed` | CRD lifecycle events (ADD/UPDATE/DELETE) enqueued to / processed by the single discovery worker | processed tracks enqueued |
| `events_parked` | **1.12.5.** submits that had to wait on a full queue (the worker fell 256 events behind; the informer processor goroutine parks up to 30 s) | 0 on a stable cluster; bursts during a bulk CRD install |
| `events_dropped` | lifecycle events dropped after the park deadline — the last resort. A dropped DELETE means an informer is never torn down and its dependent L1 entries stay resident until TTL | **0** |
| `discovery_invoked` | ADD/UPDATE passes that ran `DiscoverGroupResources` (**#218:** only when the schema OR discovery-identity fingerprint changed — no longer on an idle ~9.5min re-list) | tracks real CRD churn; **flat** across idle re-lists |
| `discovery_skipped_ng` | ADD/UPDATE decode-skip / no-group / no-SA-rc | **0** — a non-zero value is the silent-skip defect class |
| `deletes_processed` | successful DELETE teardowns (informer removed, dependents dirty-marked) | tracks CRD deletions |
| `delete_skipped_ng` | DELETE decode-skip / no-served-versions / no-plural | **0** |
| `panics_recovered` | discovery passes that panicked (recovered; the worker survives) | **0** |
| `schema_relists_fired` | ADD/UPDATE passes that relisted ≥1 GVR on a detected structural-schema change | tracks CRD schema churn |
| `schema_unchanged` | ADD/UPDATE where the schema fingerprint was unchanged (thrash guard; no relist) | informational |
| `crd_discovery_noop` | **#218.** ADD/UPDATE where NEITHER the schema nor the discovery-identity fingerprint (`spec.group`/`names`/`scope`/per-version `served`,`storage`,`deprecated`,status/scale subresource) changed → no discovery memcache wipe, no SA `RESTMapper` rebuild, no schema-memo reset ran (the ~9.5min idle CRD re-list amplifier removed) | climbs ~1 per CRD per reflector re-list at steady state — that rising count IS the healthy signal the amplifier is gone, paired with a flat `discovery_invoked` |
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
| `coordinates` | **#239.** DISTINCT dependency coordinates — the `(GVR, namespace, name)` tuples the edges point at, counted rather than derived. `records` counts EDGES; the ratio `records / coordinates` is the dirty-mark fan-out multiplier | **not a health signal, and its zero is not a health reading** — zero coordinates with zero `records` is simply an empty tracker, so read the PAIR. #239 measured fan-out at ~250 dirty-marks per object event and *derived* a coordinate count of ~364 from it; the scaling conclusion (fan-out grows **linearly with cohort count**, because coordinates are cluster-scoped while L1 entries are cohort-scoped) had no falsifier until this number existed. Coordinates flat while `records` grows with cohorts CONFIRMS it; coordinates growing with cohorts KILLS it. Read against `dropped_cap`: a cap rollback leaves an empty forward bucket, so this over-reads while `dropped_cap > 0` |
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
| `unguarded_put_total` | **#375 DETECTOR.** accepted gen-guarded L1 Puts (`PutIfGen` / `ReplaceIfGen` / `PutRAFullListIfGen`) whose resolve context carried **no dep-generation sink**, i.e. a resolve entry that went through neither `WithL1KeyContext` nor `WithDepGenSink`/`WithContentDepGenSink`, so the PUT-THEN-REMARK guard could not tell whether a dependency moved during the resolve. Each such Put is remarked once (fail-fresh: one refresh of that key), so the cell still converges, but the path is outside the guard. Also exported as its own OTLP counter `snowplow_deps_unguarded_put_total` | **0**. Any non-zero value is #375 drift (a new or changed resolve entry that does not install the sink); alert on it. The rate equals one extra refresh per drifted Put |
| `moved_remark_total` / `moved_remark_boot_total` | **#408 diagnostic.** PUT-THEN-REMARKs fired because a dependency the resolve recorded moved during it (reasons `moved` and `moved_after_put`; the nil-sink drift is `unguarded_put_total`). Each one is one refresher enqueue, rate-floored and coalesced per key. `moved_remark_boot_total` is the subset from the pre-readyz boot seed plain Put (`PutThenRemark`), the cost #408 added at boot. Under the cold-bucket GVR floor a cell is remarked when *any* object of a GVR it depends on moved during its resolve, so on a busy boot this approaches one per cell that depends on a churning GVR. Also exported as the OTLP counter `snowplow_deps_moved_remark_total{carrier=boot\|guarded}`. Each carrier is its own monotonic counter, and `moved_remark_total` = guarded + boot | tracks churn during resolves; **0** on a quiet cluster. Read `moved_remark_boot_total` across a boot to size the boot remark cost (#419) |

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

`available:false` means **no harvester has been published** — prewarm is off, boot has not
reached the engine, or only one of the two harvesters exists (the surface publishes both or
neither, so a half-configured pod refuses to answer rather than reporting an absent harvester as
an empty one). It is deliberately distinct from "the harvesters are empty", which is a
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

### Inspecting the informer store for ONE object (#237)
`GET /debug/store` answers **"is the STORE stale, or is the L1 cell stale?"** — the question
#237 spent two sessions failing to answer, because every other surface is DOWNSTREAM of the
informer store and renders the two states identically. `/debug/apistage` reports L1 entry
metadata; `/debug/reconcile` probes L1 against the informer's **own** indexer, i.e. against the
layer under suspicion, and its `probeObjectState` oracle answers `objExists` for a present object
whatever its `resourceVersion`.

    GET /debug/store?gvr=<group>/<version>/<resource>&namespace=<ns>&name=<name>
        gvr takes 2 segments for the core group (v1/configmaps) or 3 otherwise.
        namespace is omitted for a cluster-scoped object.

The answer is the store's own `resourceVersion`, `uid`, `generation` and `creationTimestamp` for
that coordinate, a `bodySHA256` of the stored bytes, the stored `representation`
(`bytesObject` / `unstructured` / `partialObjectMetadata`), the GVR's four servability conjuncts,
its `indexerCount`, `gvrLastSyncResourceVersion` and `gvrLastEventAgeSeconds`. Compare the
`resourceVersion` against `kubectl get <resource> <name> -o jsonpath='{.metadata.resourceVersion}'`
and the divergence IS the staleness — no inference.

**It does NOT read the apiserver, and that is deliberate.** Serving the comparison here would
read as the snowplow ServiceAccount, telling any valid-JWT holder the existence and
`resourceVersion` of objects their own RBAC forbids. The operator runs that half themselves, under
their own identity.

**It does NOT gate on servability.** The four conjuncts are reported as fields and gate nothing —
a retracted or watch-broken GVR is exactly when the store needs reading, and `confirm_retracted_total`
was **10158** on the pod #237 was filed from. This is the one behaviour that separates it from
`probeObjectState`, which returns `objUnknown` in that state by design.

**Status codes.** `400` for a missing or malformed `gvr`/`name`. `200` for everything else,
including a coordinate the store does not hold (`found:false`) and a GVR with no informer
(`registered:false`) — never `404`, because `404` is what an unregistered route looks like.

**`bodySHA256` is comparable only between two snowplow reads** — never against
`kubectl get -o json | sha256sum`. The informer's `SetTransform` has already stripped
`managedFields` and the last-applied-configuration annotation, so a kubectl comparison reports a
divergence that is not one.

**Metadata only, structurally.** Every field is a string, bool or number; the response type has
no `[]byte`, map or slice, so it cannot carry an object body. Same boundary as `/debug/apistage`.

There is deliberately **no per-object "last seen"**: tracking one would be a 50K-entry map per GVR
written on the informer's processor goroutine. The per-object `resourceVersion` answers "is this
object stale"; `gvrLastEventAgeSeconds` answers "is this GVR receiving events at all", which is
what the #237 capture actually needed.

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

## POST-ROLL CHECKLIST (ClickStack)

Run after every snowplow roll. It needs no user credential: everything below is
on OTLP in ClickHouse (ClickStack default schema: `otel_metrics_sum` for
counters, `otel_metrics_gauge` for gauges). The only parameter is `{since}`, the
roll time (for example `toDateTime64('2026-10-04 09:00:00', 9)`). Which pods and
which `service.version` to read is DERIVED, never typed: every query starts
from the `roster` CTE, the pods whose first `snowplow_build_info` row is at or
after `{since}`, each with the `service.version` and build commit it reports
(#462). A typed version can silently select the wrong row set: before #462 one
pod exported two `service.version` values (the SDK's 40-character commit, and
the chart release on the rows the collector's kubeletstats and filelog
receivers produce for the pod), and typing the release selected rows without
`snowplow_build_info`, so step 0 returned nothing and read as a pass. Since
#462 both carry the release, and the commit is `vcs.ref.head.revision` and
`snowplow_build_info{version}`.

> **Rollout note (1.12.36, #462).** From 1.12.36 on, `service.version` on
> snowplow's own OTLP rows (metrics, traces, logs) is the **release** (the pod's
> `app.kubernetes.io/version`, e.g. `1.12.36`), no longer the git commit. A
> saved query, alert or dashboard that filters `service.version` on a commit
> SHA matches nothing for 1.12.36+ pods. Filter on the resource attribute
> `vcs.ref.head.revision`, or on the `snowplow_build_info{version}` metric
> attribute, instead. Both carry the full 40-character commit.

Counters are cumulative per pod, so a window delta is `max(Value) - min(Value)`
per pod-and-attribute series (a restarted pod is a new `k8s.pod.name`, so a
reset never produces a negative delta). The delta omits what a pod counted
before its first export (one export interval); for a pod that started after
`{since}`, `max(Value)` alone is its lifetime total.

**0. The series exist on the rolled pods, in the right table.** Every check
below passes vacuously when its series is missing, and a counter exported as a
gauge (or the reverse) lands in the other table, where checks 2-5 would match
nothing. So run this first. **Pass = zero rows.** Each row names a rolled pod
(the roster is `snowplow_build_info`, which is always exported) that is missing
one of the 16 (table, metric) pairs, or that has fewer than the full 4×3 drift,
3 repick or 5 re-mint class series, or fewer than the 5 #378 P1 store stats. `missing` lists what is absent. Step 0 is the single
presence gate for every later step, including the three #368 counters step 6
reads.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  ),
  ['sum:snowplow_l1_identity_class_drift_declined_total',
   'sum:snowplow_l1_representative_repick_total',
   'sum:snowplow_binding_set_memo_hits',
   'sum:snowplow_binding_set_memo_misses',
   'sum:snowplow_binding_set_memo_refused',
   'sum:snowplow_deps_unguarded_put_total',
   'sum:snowplow_v7_shadow_wildcard_digest_collision_total',
   'sum:snowplow_v7_shadow_wildcard_digest_observed_total',
   'sum:snowplow_v7_shadow_wildcard_digest_evicted_total',
   'sum:snowplow_resolved_cache_remint_total',
   'gauge:snowplow_resolved_cache',
   'gauge:snowplow_learned_classes_registered',
   'gauge:snowplow_learned_classes_seeded',
   'gauge:snowplow_learned_classes_from_secrets',
   'gauge:snowplow_learned_classes_secrets_unparseable',
   'gauge:snowplow_learned_clientconfig_secrets'] AS expected
SELECT r.pod,
       arrayFilter(x -> NOT has(s.present, x), expected) AS missing,
       s.drift_series, s.repick_series, s.remint_series, s.p1_stats
FROM roster AS r
LEFT JOIN (
  SELECT pod,
         groupUniqArray(concat(tbl, ':', MetricName)) AS present,
         uniqExactIf(toString(Attributes), MetricName = 'snowplow_l1_identity_class_drift_declined_total') AS drift_series,
         uniqExactIf(toString(Attributes), MetricName = 'snowplow_l1_representative_repick_total') AS repick_series,
         -- #378 P1: the 5-class re-mint counter and the five store stats.
         uniqExactIf(toString(Attributes), MetricName = 'snowplow_resolved_cache_remint_total') AS remint_series,
         uniqExactIf(Attributes['stat'], MetricName = 'snowplow_resolved_cache' AND Attributes['stat'] IN
           ('evict_max_age_warm_customer_total', 'evict_max_age_warm_internal_total',
            'evict_ttl_warm_customer_total', 'oldest_warm_born_age_seconds', 'remint_total',
            'remint_refused_cold_total')) AS p1_stats
  FROM (
    SELECT 'sum' AS tbl, ResourceAttributes['k8s.pod.name'] AS pod, MetricName, Attributes
    FROM otel_metrics_sum
    WHERE ServiceName = 'snowplow'
      AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
      AND TimeUnix >= {since}
    UNION ALL
    SELECT 'gauge' AS tbl, ResourceAttributes['k8s.pod.name'] AS pod, MetricName, Attributes
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
      AND TimeUnix >= {since}
  )
  WHERE has(expected, concat(tbl, ':', MetricName))
  GROUP BY pod
) AS s ON s.pod = r.pod
WHERE length(arrayFilter(x -> NOT has(s.present, x), expected)) > 0
   OR s.drift_series < 12
   OR s.repick_series < 3
   OR s.remint_series < 5
   OR s.p1_stats < 5
UNION ALL
-- An empty roster is a FAILURE row, never a silent pass: no pod has exported
-- snowplow_build_info since {since} (nothing rolled, wrong {since}, or
-- metrics export off).
SELECT concat('NO ROLLED POD since ', toString({since})) AS pod, expected AS missing,
       toUInt64(0) AS drift_series, toUInt64(0) AS repick_series,
       toUInt64(0) AS remint_series, toUInt64(0) AS p1_stats
WHERE (SELECT count() FROM roster) = 0
```

`SELECT pod, version, build FROM roster` (the CTE alone) shows which pods the
checklist is reading, the `service.version` each reports and its commit.

**1. Learned classes seeded ≈ 0 on group-only clusters.** On a cluster whose
RBAC is bound to groups only, no learned class has a distinct target, so
`seeded` stays ≈ 0 while `registered` tracks logins. A sizable `seeded` there
means distinct-target detection regressed. On a cluster with per-user bindings,
`seeded` > 0 is expected.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT ResourceAttributes['k8s.pod.name'] AS pod, MetricName,
       argMax(Value, TimeUnix) AS latest
FROM otel_metrics_gauge
WHERE ServiceName = 'snowplow'
  AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
  AND TimeUnix >= {since}
  AND MetricName IN ('snowplow_learned_classes_registered', 'snowplow_learned_classes_seeded',
                     'snowplow_learned_classes_unseeded_capacity', 'snowplow_learned_classes_nav_only')
GROUP BY pod, MetricName
ORDER BY pod, MetricName
```

**2. The clientconfig alarm.** `from_secrets == 0` while
`clientconfig_secrets > 0` means authn's certificate format moved and no class
is learned from Secrets. Expect **zero rows**.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT pod, from_secrets, clientconfig_secrets, unparseable
FROM (
  SELECT ResourceAttributes['k8s.pod.name'] AS pod,
         argMaxIf(Value, TimeUnix, MetricName = 'snowplow_learned_classes_from_secrets') AS from_secrets,
         argMaxIf(Value, TimeUnix, MetricName = 'snowplow_learned_clientconfig_secrets') AS clientconfig_secrets,
         argMaxIf(Value, TimeUnix, MetricName = 'snowplow_learned_classes_secrets_unparseable') AS unparseable
  FROM otel_metrics_gauge
  WHERE ServiceName = 'snowplow'
    AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
    AND TimeUnix >= {since}
    AND MetricName IN ('snowplow_learned_classes_from_secrets', 'snowplow_learned_clientconfig_secrets',
                       'snowplow_learned_classes_secrets_unparseable')
  GROUP BY pod
)
WHERE from_secrets = 0 AND clientconfig_secrets > 0
```

**3. `unguarded_put == 0`.** A non-zero value is #375 drift (a resolve entry
that does not install the dep-generation sink). Expect **zero rows**.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT ResourceAttributes['k8s.pod.name'] AS pod, argMax(Value, TimeUnix) AS unguarded_put
FROM otel_metrics_sum
WHERE ServiceName = 'snowplow'
  AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
  AND TimeUnix >= {since}
  AND MetricName = 'snowplow_deps_unguarded_put_total'
GROUP BY pod
HAVING unguarded_put > 0
```

**4. Drift-decline rate.** Declines per minute by site and reason. Expect a low
rate that follows RBAC churn, and `reason = 'no_identity'` at 0. A rate that
climbs without RBAC churn means keys and identities disagree outside a
grant/revoke race.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT site, reason,
       sum(delta) AS declines,
       round(sum(delta) / greatest(dateDiff('minute', {since}, now()), 1), 3) AS per_minute
FROM (
  SELECT ResourceAttributes['k8s.pod.name'] AS pod,
         Attributes['site'] AS site, Attributes['reason'] AS reason,
         max(Value) - min(Value) AS delta
  FROM otel_metrics_sum
  WHERE ServiceName = 'snowplow'
    AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
    AND TimeUnix >= {since}
    AND MetricName = 'snowplow_l1_identity_class_drift_declined_total'
  GROUP BY pod, site, reason
)
GROUP BY site, reason
ORDER BY declines DESC
```

**5. Binding-set memo hit ratio.** Every RBAC snapshot publish swaps the memo
shard, so the ratio reads how often `/call` pays the cold digest build. Expect
it high once warm, and `refused` at 0.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT pod,
       hits, misses, refused,
       round(hits / greatest(hits + misses, 1), 4) AS hit_ratio
FROM (
  SELECT ResourceAttributes['k8s.pod.name'] AS pod,
         maxIf(Value, MetricName = 'snowplow_binding_set_memo_hits')
           - minIf(Value, MetricName = 'snowplow_binding_set_memo_hits') AS hits,
         maxIf(Value, MetricName = 'snowplow_binding_set_memo_misses')
           - minIf(Value, MetricName = 'snowplow_binding_set_memo_misses') AS misses,
         maxIf(Value, MetricName = 'snowplow_binding_set_memo_refused')
           - minIf(Value, MetricName = 'snowplow_binding_set_memo_refused') AS refused
  FROM otel_metrics_sum
  WHERE ServiceName = 'snowplow'
    AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
    AND TimeUnix >= {since}
    AND MetricName IN ('snowplow_binding_set_memo_hits', 'snowplow_binding_set_memo_misses',
                       'snowplow_binding_set_memo_refused')
  GROUP BY pod
)
ORDER BY pod
```

**6. #368 certification.** The ClassWildcard projection is certified on a pod
iff `collision == 0 AND observed > 0 AND evicted == 0`. The counters are per
process, so read each pod's lifetime value (`max(Value)`). Once the enumerate
projection ungates, expect `verdict = 'certified'` on every pod. Until then
`observed = 0` on every pod and the verdict reads `not_exercised`. That is
expected, not a pass. `COLLISION` is a share leak (alert); `void_evicted`
voids the window (raise the probe caps and re-measure). Their presence is
checked by step 0; if step 6 returns no rows at all, step 0 has already failed.

```sql
WITH
  roster AS (
    -- The rolled pods: every pod whose FIRST snowplow_build_info row is at or
    -- after {since}, with the service.version and build commit it reports.
    -- Derived, never typed (#462).
    SELECT ResourceAttributes['k8s.pod.name'] AS pod,
           argMax(ResourceAttributes['service.version'], TimeUnix) AS version,
           argMax(Attributes['version'], TimeUnix) AS build
    FROM otel_metrics_gauge
    WHERE ServiceName = 'snowplow'
      AND MetricName = 'snowplow_build_info'
      AND TimeUnix >= {since} - INTERVAL 1 HOUR
    GROUP BY pod
    HAVING min(TimeUnix) >= {since}
  )
SELECT pod, collision, observed, evicted,
       multiIf(collision > 0, 'COLLISION',
               evicted > 0, 'void_evicted',
               observed = 0, 'not_exercised',
               'certified') AS verdict
FROM (
  SELECT ResourceAttributes['k8s.pod.name'] AS pod,
         maxIf(Value, MetricName = 'snowplow_v7_shadow_wildcard_digest_collision_total') AS collision,
         maxIf(Value, MetricName = 'snowplow_v7_shadow_wildcard_digest_observed_total') AS observed,
         maxIf(Value, MetricName = 'snowplow_v7_shadow_wildcard_digest_evicted_total') AS evicted,
         uniqExactIf(MetricName, MetricName LIKE 'snowplow_v7_shadow_wildcard_digest_%') AS present
  FROM otel_metrics_sum
  WHERE ServiceName = 'snowplow'
    AND (ResourceAttributes['k8s.pod.name'], ResourceAttributes['service.version']) IN (SELECT pod, version FROM roster)
    AND TimeUnix >= {since}
    AND MetricName LIKE 'snowplow_v7_shadow_wildcard_digest_%'
  GROUP BY pod
)
WHERE present = 3
ORDER BY pod
```

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
| B3 | broadcaster | a slow consumer's sink fills and a signal is dropped — the key's LAST change never reaches the tab (#484; the SPA does not poll) | `snowplow_refresh_broadcaster.deferred`, `snowplow_refresh_broadcaster.pending_coalesced`, `snowplow_refresh_broadcaster.pending_overflow_resync`, `snowplow_refresh_broadcaster.max_sink_depth` | `TestRefresh484_FDrop_FullSinkChangeReachesClient`, `TestRefreshes484_FDrop_Wire`, `TestRefreshes484_StallForcesResyncAndReconnectRepairs`, `TestRefreshBroadcaster_SlowConsumerNeverStalls`, `TestRefreshBroadcaster_DroppedTerminalSignalDegrades` | PINNED (#484) — a full sink defers into a per-subscriber pending set, never drops; a wedged consumer is force-resynced (stream ends, SPA re-validates); the refresher never stalls |
| B3b | broadcaster | a second commit inside the 250 ms coalesce window is suppressed and was the key's last change (#484) | `snowplow_refresh_broadcaster.coalesced`, `snowplow_refresh_broadcaster.trailing_emitted` | `TestRefresh484_FCoalesce_TrailingChangeReachesClient`, `TestRefreshes484_FCoalesce_Wire` | PINNED (#484) — trailing-edge coalescing emits exactly one trailing signal per window that suppressed one |
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
| `prewarm.phase1.readiness_exit` | Info (`latch` / `none-configured` / `seed_returned`), **Warn** (`deadline` / `boot_error` / `seed_panic` / `boot_aborted`) | `dispatchers/phase1_readiness_exit.go` | **#397.** ONE line per process at the readiness flip: `outcome`, `deadline_cause` (`phase1_timeout` / `pip_global_timeout` = budget exhausted; `canceled` = the ctx was cancelled, e.g. a SIGTERM mid-boot, which also sets `snowplow_phase1_deadline_released_total` to 1 and is NOT a slow boot), `abort_cause` (**#401**, set only for `outcome=boot_aborted`: `no_sa_endpoint` = the SA endpoint could not be built, an empty token file or missing ca.crt; `no_dyn_client` = the roots-read dynamic client could not be built), `latch_fired`, `elapsed_ms` (Phase1Warmup start to flip), `since_process_start_ms`, `nav_latch_armed`, `nav_boot_passes`, `nav_units_total` / `nav_units_seeded` / `nav_units_remaining`, `cohorts`, the bounded failure-class counts `nav_units_expected_deny` / `nav_units_operational_failure` / `nav_units_aborted`, and per-step `walk_ms` / `sync_wait_ms` / `content_prewarm_ms` / `cluster_list_prewarm_ms` / `seed_ms` (`-1` = step not run). Counts and code-defined enums only, never an identity or widget name. Visible at `LOG_LEVEL=warn` whenever readiness was NOT latch-released; pairs with `snowplow_phase1_deadline_released_total`. **#407**: the same record (including `abort_cause`) is always readable at `LOG_LEVEL=warn` as the expvar map `snowplow_phase1_readiness_exit`, and its timings in the ready `/readyz` body |
| `readyz.backstop.fired` | Error | `dispatchers/readiness_backstop_metrics.go` | readiness flipped **Ready-degraded** via a backstop arm; ONE line per release (#402) with `reason` = `phase1_timeout` / `pip_global_timeout` / `canceled` / `boot_error` / `seed_panic` / `no_sa_endpoint` / `no_dyn_client` (#401) (`seed_incomplete` retired); pairs with `snowplow_readyz_backstop_fired` — alert |
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
