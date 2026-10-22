---
type: Architecture
title: snowplow — prewarm
description: How snowplow makes the first page-load warm — the Phase-1 navigation walk, the sync barrier, the prewarm-gated readiness, the dynamic per-binding seed, and the steady-state keepwarm/refresh mechanisms.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [prewarm, boot, readiness, seed, cohort]
timestamp: 2026-08-06T00:00:00Z
---

# Prewarm

How snowplow makes the *first* page-load warm: it replays the frontend's own
navigation under the service-account identity at boot, populates the informer
set and the per-binding L1 cache, and keeps those cells fresh as objects change —
without ever hardcoding a navigation path, a cohort list, or a GVR.

This is a maintainer deep-dive. It leads with the flow, then traces the named
files, then states the invariants and the known failure modes. Re-derived from
the current tree (docs-standard, 2026-08-06); anchors are file + function names.

---

## ASCII flow

```
 BOOT (Phase1Warmup → phase1WarmupWith, internal/handlers/dispatchers/phase1_walk.go)
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ Step 1  RegisterMetaQuerySeeds()           ── the ONLY hardcoded GVRs at boot  │
 │           (cache/phase1.go — bare meta-query anchors, no business GVR)         │
 │ Step 3  lister(ctx)  ── READ nav roots from the frontend ConfigMap            │
 │           config.json .api.INIT + .api.ROUTES_LOADER → 2 widget CRs           │
 │           (phase1_roots.go listNavigationRootsFromConfigMap)                  │
 │           roots missing? LOG + PROCEED — the config-vars ConfigMap informer   │
 │           (phase1_configvars_watch.go) re-drives a boot re-walk when it lands │
 │ Step 4  for each root:  resolveNavigationRoot → phase1Walker.walk()           │
 │           recursive replay of frontend nav (phase1_walk.go)                   │
 │             • harvestApiRef + harvestNavWidget on every walked widget         │
 │             • widgets.Resolve → status.resourcesRefs.items[]                  │
 │             • recurse ONLY items where verb=="GET"  (walkShouldRecurse)       │
 │               — load-bearing read-only gate                                   │
 │             • lazyRegister + DiscoverGroupResources register informers        │
 │ Step 6  settleRegisteredSet  ── let the discovered informer set stop growing  │
 │ Step 7  WaitAllInformersSynced  ── the SYNC BARRIER (cache/phase1.go)         │
 │ Step 7.5 content pre-warm + cluster_list pre-warm   (behind 503 readiness)    │
 │ Step 7.6 ENGINE BOOT SEED — SYNCHRONOUS, before readiness:                    │
 │           StartPrewarmEngine → scopeKindBoot → rePrewarmBoot                  │
 │             ① RE-WALK both roots, FRESH visited map, AFTER the sync barrier   │
 │             ② settleRegisteredSet  ③ BuildBindingsByGVRIndex                  │
 │             ④ seedScopeYielding: rank-major, class-interleaved per-binding    │
 │                seed from the live index, yielding to customer /call           │
 │           /readyz unblocks on the FIRST of:                                   │
 │             • firstNav latch — every cohort's NAV WIDGETS warm (happy path)   │
 │             • bootDone (early end) / pipGlobalTimeout / PHASE1_TIMEOUT        │
 │               → Ready-DEGRADED backstop (snowplow_readyz_backstop_fired)      │
 │ Step 8  MarkPhase1Done()        ── /readyz flips 200 (deferred, fire-         │
 │           regardless: normal return, error, timeout, even seed panic)         │
 │ Step 7.7 apiRef pagination drain (background, post-Ready)                     │
 └───────────────────────────────────────────────┬──────────────────────────────┘
                                                  │  (BACKGROUND, post-Ready)
                                                  ▼
 ENGINE (implicit-on-cache; worker outlives boot on the process-lifetime ctx):
   scopeKindBoot           ─ boot seed + any config-vars-driven re-drive
   scopeKindGVRDiscovered  ─ new GVR registered post-boot → re-walk + seed
   scopeKindKeepwarm       ─ TTL×3/4-cadenced quiet-page re-seed sweep
   (scopeKindWidgetCR / scopeKindRBACShift: declared, still NOT wired)

 COHORT MODEL: one dynamic engine. Targets come from the LIVE BindingsByGVR
   index per (GVR, verb) — NO static cohort list, and when the index yields
   nothing for a GVR the seed SKIPS it (NO global fallback, NO lazy cold-fill).
   A skipped/runtime-discovered target is resolved cold-then-warm at first /call.

 STEADY STATE — CRUD-triggered re-prewarm:
   informer ADD/UPDATE ─ DepTracker.onChange → dirty-mark dependent L1 keys
                          → refresher RE-RESOLVES (never evicts)
   informer DELETE     ─ self entry evicted; LIST/dependent deps dirty-marked
   new GVR discovered  ─ cache GVR-discovered hook → engine scopeKindGVRDiscovered
   quiet cells         ─ keepwarm sweep re-Puts before TTL expiry
```

---

## Trace

### One knob: the whole prewarm family is implicit-on-cache

There is **no prewarm env flag any more**. The formerly-standalone gates —
`PREWARM_ENABLED`, `PREWARM_CONTENT_ENABLED`, `PREWARM_PIP_ENABLED`,
`PREWARM_ENGINE_ENABLED`, `PROACTIVE_RA_SEED_ENABLED` — are **retired**: each
helper now returns `cache.PrewarmEnabled()` (== cache on), and a stale value in
the environment is ignored with a one-shot audit log (`cache/retired_flags.go`;
`=false` logs a WARN because the operator asked for OFF and now gets ON).
The legacy `runPIPSeed` global-cohort seed path is **deleted**
(`prewarm_engine_boot.go`); the engine is the only seed path. Back-out for the
whole family is `CACHE_ENABLED=false`, which nils the harvesters and makes the
walk/seed a no-op.

### Boot seed — the only hardcoded GVRs

`phase1WarmupWith` (`internal/handlers/dispatchers/phase1_walk.go`) is the boot
orchestrator. **Step 1** is the entire "boot seed": `rw.RegisterMetaQuerySeeds()`
(`internal/cache/phase1.go`) registers only the bare meta-query anchors —
*"every business GVR is discovered by resolution"*. Ship 0/0.5 deleted the
`customresourcedefinitions` seed and the CRD informer entirely; composition
GVRs are discovered as a synchronous side-effect of the walk
(`cache.DiscoverGroupResources`), not pre-seeded.

### Phase-1 walker roots — read from the frontend ConfigMap, not hardcoded

The roots are NOT `navmenus` / `routesloaders` literals. **Step 3** calls the
lister, which is `listNavigationRootsFromConfigMap` (`phase1_roots.go`). It:

1. Reads the frontend ConfigMap — name from env `FRONTEND_CONFIG_CONFIGMAP`
   (default `frontend-config-vars`), namespace = `AUTHN_NAMESPACE`.
2. Parses its `config.json` key, reading `.api.INIT` and `.api.ROUTES_LOADER` —
   the two `/call?...` URLs the frontend itself dispatches on login.
3. Decodes each URL into an `ObjectReference` via
   `objects.ParseCallPathToObjectRef` — the same generic `/call` decoder the
   recursive walk uses; not a path special-case.
4. Fetches each named root widget CR via `objects.Get` and returns
   `navigationRoot{Root, GVR}` pairs.

The strings `navmenus`/`routesloaders` appear **nowhere** as Go literals driving
root selection. If the frontend changes its INIT widget, Phase 1 follows with
zero Go change.

**A missing ConfigMap is self-healing, not just degraded.** When snowplow boots
before the frontend (fresh-install boot race), the eager read finds nothing; the
warmup logs `phase1.warmup.roots_list_failed`, proceeds through the sync barrier
and the normal readiness flip — and a dedicated single-object ConfigMap informer
(`phase1_configvars_watch.go`, registered on the process-lifetime ctx) enqueues
a `scopeKindBoot` re-walk the instant the ConfigMap appears or its
`config.json` data changes (a data-change gate ignores metadata-only churn such
as CDC traceparent re-stamps). The re-drive works before OR after the readiness
backstop, with zero pod restart.

### The recursive walk — replaying navigation, GET-only

**Step 4** calls `resolve(ctx, root)` per root, which threads into
`phase1Walker.walk()` (`phase1_walk.go`). Per walked widget:

- `harvestApiRef` + `harvestNavWidget` collect the widget's apiRef RESTAction
  and the widget CR + its (GVR, pagination) tuple into the shared harvesters the
  engine seed later drains.
- `widgets.Resolve` runs the widget under the SA identity; its inner-call walk
  auto-registers an informer per touched GVR and invokes
  `cache.DiscoverGroupResources` for templated apiserver paths.
- It reads `status.resourcesRefs.items[]` — the child widget endpoints — via
  `extractResourcesRefsItems`.

**The recursion gate is `verb == "GET"` only.** Before descending into a child,
the loop calls `walkShouldRecurse(child)`, which is exactly:

```go
func walkShouldRecurse(child navChildRef) bool {
    return strings.EqualFold(child.Verb, "GET") && child.Path != ""
}
```

This is load-bearing for both correctness and safety: a non-GET
`resourcesRefs` item is a mutation/action endpoint (POST/PUT/PATCH/DELETE)
bound to a widget's `actions`, and because the walk runs with the SA's
*privileged* credentials, following one would issue a destructive apiserver
mutation. The `allowed` RBAC flag is **deliberately not** a recursion gate:
Phase 1 is identity-independent informer *discovery*; the per-user `allowed`
render gate is applied later, at real request time. Recursion bounds are the
visited-set and `phase1MaxWalkDepth`; child fan-out is bounded by the declared
`slice` or `prewarmPageLimit()`.

### The sync barrier

After the walk, `settleRegisteredSet` (Step 6) waits for the discovered
informer set to stop growing, then `WaitAllInformersSynced` (Step 7,
`cache/phase1.go`) blocks until every registered informer `HasSynced` AND no
new informer appeared during the wait (the re-snapshot loop).

### Readiness gates on prewarm-complete (the 2026-07-02 reversal)

This is the part that **reversed** relative to older revisions of this doc.
The per-cohort seed used to run in the background *after* `MarkPhase1Done`
("readiness precedes the seed; boot is cohort-count-independent"). Empirically,
an informer-synced-but-cold pod serializes/503s the compositions-page fan-out
under the aggregate-OOM admission gate — "synced ≠ safe to route". So the seed
now runs **synchronously at Step 7.6, before `MarkPhase1Done`** (shape A,
`docs/readiness-gate-prewarm-complete-2026-07-02.md`), and `/readyz` gates on
prewarm-complete:

- `engineSeed` (`phase1_walk.go`) starts the engine on the **process-lifetime
  ctx** (`SetEngineProcessContext`, wired from `main.go` — the fix for the
  worker dying at boot-scope completion), enqueues one `scopeKindBoot`, and
  waits on the **first** of:
  - the **first-nav latch** (`prewarm_first_nav_latch.go`): fires the instant
    every cohort's **nav widgets** have seeded (or provably has none) — the
    happy path. Ready flips with every cohort's navigation warm while the boot
    scope keeps seeding the RESTAction content tail in background.
  - `bootDone` — the boot scope finished/failed before the latch could fire
    (e.g. roots-list failure); a genuine boot error still surfaces.
  - the `pipGlobalTimeout` / `PHASE1_TIMEOUT` backstop.
- `MarkPhase1Done` is a **defer placed before the seed call**, so it fires on
  every exit — normal return, error, timeout, even a seed panic (the recover
  runs first). Readiness is therefore **never withheld forever**; a failed or
  timed-out seed flips the pod **Ready-degraded** and the failure is surfaced
  loudly (`recordReadinessBackstop` → `snowplow_readyz_backstop_fired` +
  ERROR logs — the F5 "backstop-Ready is an explicit failure signal" ship).
- With prewarm off entirely (cache off), a nil seed flips Ready right after the
  sync barrier — the byte-identical no-seed behaviour.

**Step 7.7** — the deferred apiRef pagination drain — runs *after* the flip on
a bounded background goroutine (page 2..N of paginated apiRef widgets, plus the
post-storm re-collection pass); it fills widgetContent page cells without
blocking `/readyz`.

### The seed — one dynamic engine, rank-major and class-interleaved

The engine worker (`prewarm_engine.go`) yields to any in-flight customer
`/call` before and during each scope (`yieldToCustomer`,
`engineYieldCheckpoint`) and runs `rePrewarmBoot` (`prewarm_engine_boot.go`):

1. **RE-WALK both roots with a FRESH `phase1Walker` per root** (new visited
   map). This is the boot-race fix: the Step-4 walk runs *before* the sync
   barrier and is single-pass, so the navmenu's *dynamic* children
   (`resourcesRefsTemplate` over the apiRef) resolve to 0 while fallthrough
   data is empty. The re-walk runs *after* the barrier, when the data is
   present, so the full nav tree is discovered and the harvesters are
   populated. Reusing the old visited map would short-circuit and descend
   nothing. (F4b Lever B additionally reuses the boot discovery snapshot on a
   resume re-walk, and Lever A marks declined-external targets so a resume does
   not re-resolve external whales in a loop.)
2. `settleRegisteredSet` once.
3. `cache.BuildBindingsByGVRIndex(rw.RegisteredGVRs())` — the cohort-scoping
   substrate, built over the *navigated* GVRs.
4. `seedScopeYielding` drains the harvested widgets + restactions and seeds the
   per-binding top-level L1 cells, in a **rank-major, class-interleaved**
   order: identities are ranked widget-capable-first (deterministic rank
   hygiene over both classes' target sets — Fix-3), and for each rank the
   widgets seed in first-nav walk order, then that rank's RESTActions (cheap
   RAs first by target count). The F3 login-cohort coverage work makes every
   login-cohort-shaped identity reachable (with an intended ServiceAccount
   exclusion), and the all-nav-widget latch (F3b-r2) is what feeds the
   first-nav readiness latch above. A per-seed-pass RA-resolve memo
   (`cache/seed_resolve_memo.go`, F4) dedups repeated RA resolves within one
   pass.

Seed-unit resource use is bounded adaptively: `seed_bound.go` applies the
shared `cache.AdmissionCeiling()` headroom gate (GOMEMLIMIT − live heap −
reserve) at the `seedOneWidget` / `seedOneRestaction` choke points — a separate
semaphore instance from the nested-resolve bound so the stacked seed→nested
case cannot deadlock. External fetches inside the boot seed carry a wall-clock
bound (`api/external_seed_bound.go`). The error-aware Put-gate declines a
degraded re-resolve rather than overwrite a good warm entry.

### The cohort model — dynamic, no static list, no cold-fill

`seedScopeYielding` does **not** iterate a cohort list. For each harvested
widget it scopes on the widget's GVR; for each harvested RESTAction it scopes
on the RA's *target* GVR derived from its `userAccessFilter`
(`restActionTargetGVR`). It then asks the **live index** for the per-binding
targets via `cache.EnumeratePrewarmTargetsForGVR(gvr, "list")`
(`cache/prewarm_enumeration.go`). Each authorising RBAC binding yields exactly
one `PrewarmTarget`; the representative subject identity is drawn from the
binding's subjects.

**Only identities a real requester can hit (#841).** The production seam
`enumeratePrewarmTargetsWithLearned` keeps a representative only if a
**learned class** (a known requester, from the stored clientconfig
certificates or live traffic) has the same identity signature, and therefore
the same key. A representative that no known user matches is dropped, because a
real user's key folds their whole binding set. In bench run 12 such
representatives used the whole readiness window and drew zero hits. Learned
classes that differ from every kept representative are seeded as their own
targets, newest first, within the self-adapting bound. When no unit cost has
been measured yet, the newest class is admitted alone as the measuring probe.
The first-nav latch waits for every nav widget × every identity the pass seeds
before it, including learned classes. `snowplow_prewarm_seed_origin` counts the
cells seeded per class (base / learned), the hits on them, and the distinct
cells hit; `snowplow_prewarm_base_targets` counts representatives kept and
dropped.

Three "no static / no cold-fill" facts, verified in code:

- **No static cohort list.** Targets are read from `BindingsByGVR` per
  snapshot; there is no Go-literal cohort set.
- **No global fallback.** The v3 `EnumerateBindingSetClasses()` global-cohort
  fallback was **removed**. When the per-GVR enumerator returns empty (no
  authorising binding, or the target GVR is runtime-discovered), the seed
  **skips** that GVR rather than widening to a universe of identities that
  can't authorise it.
- **No lazy cold-fill.** A skipped target is not back-filled by a background
  job; runtime-discovered RA targets are resolved cold-then-warm via the
  on-demand dispatcher at first `/call`.

An empty-`BindingUID` target is never seeded (`prewarm_seed_empty_binding_skip`
— the #95 leak closure), and the seed Puts stamp `RBACSubGen==0` (a known
warm-miss for subjects whose sub-generation has moved; dispatch keys stay
correct).

Customer priority is enforced throughout: `engineYieldCheckpoint(ctx)` runs
between every target, so a customer burst arriving mid-seed defers the
remaining work.

### Steady state — three re-prewarm mechanisms

1. **Existing cells stay fresh via the refresher (object CRUD).** An informer
   ADD/UPDATE event calls `DepTracker.OnAdd`/`OnUpdate` → `onChange`
   (`internal/cache/deps.go`), which dirty-marks every dependent L1 key
   (exact-object + LIST-scope deps) into the refresher. The refresher
   **re-resolves** the entry — never evicts on ADD/UPDATE. DELETE evicts only
   the deleted object's own self entry and dirty-marks its dependents. Each
   refresher commit publishes a `/refreshes` SSE signal.
2. **New navigation structure via the engine (CRD/GVR CRUD).** When a genuinely
   new GVR is first registered post-boot, the cache fires its GVR-discovered
   hook (`cache.RegisterGVRDiscoveredHook`), which the engine wires at start to
   enqueue a `scopeKindGVRDiscovered` scope. That scope runs the same
   re-walk + seed core; re-reading the now-widened index records the dep edge
   against the new GVR, so subsequent CR events propagate via the normal
   `onChange` → dirty-mark → refresher path. The queue keys distinct GVRs as
   distinct work and coalesces repeats.
3. **Quiet cells via the keepwarm sweep (#102).** A cell whose data goes quiet
   dies at TTL by design (CreatedAt slides only on Put; there is no store
   janitor), so a >TTL-returning user would pay a full cold resolve. A ticker
   (`prewarm_keepwarm_sweep.go`) enqueues `scopeKindKeepwarm` at **TTL×3/4**
   (derived from `RESOLVED_CACHE_TTL_SECONDS` — no new env knob);
   `rePrewarmKeepwarm` re-runs the walk + seed for the **widget-capable prefix
   of the identity ranking** (every login-cohort-shaped identity — the c2
   coverage fix), with an age-skip for still-fresh cells. Each sweep Put
   re-resolves fresh bytes (never TTL-extends), preserving the staleness
   backstop by construction; the queue coalesces to at most one pending sweep.

The placeholder scope kinds `scopeKindWidgetCR` / `scopeKindRBACShift` are
**declared but not wired** (`prewarm_engine.go`): a widget/RESTAction *CR*
add/update/delete does not enqueue an engine re-walk of its subtree. Today such
changes are caught by the refresher (for already-cached dependent keys) and by
lazy resolve-on-navigation at `/call`.

---

## Invariants

- **No special cases / no hardcoded navigation.** The only hardcoded GVRs at
  boot are the meta-query seeds. Navigation roots, the cohort set, and every
  business GVR are read from config / the live RBAC index / the walk — never Go
  literals.
- **The walk is strictly read-only.** Recursion is gated on `verb == "GET"`
  alone (`walkShouldRecurse`); the SA's privileged credentials never reach a
  mutation endpoint.
- **Discovery is identity-independent; rendering is not.** `allowed` is never a
  recursion gate during prewarm. The per-user `allowed` render verdict is
  re-derived at request time — prewarmed SA-evaluated `allowed=true` flags are
  never served verbatim.
- **Readiness gates on prewarm-complete, with a fire-regardless backstop.**
  `/readyz` flips 200 at min(first-nav-warm, seed-complete, backstop); a
  failed/stuck/panicking seed yields **Ready-degraded** (loudly surfaced via
  `snowplow_readyz_backstop_fired`), never not-Ready-forever. This *reverses*
  the earlier "readiness precedes the seed" design — boot wall-clock is no
  longer cohort-count-independent by construction; the latch + backstops bound
  it instead.
- **The re-walk MUST use a fresh visited map and the same `walk()`.** Reusing
  the boot pass's visited set descends nothing.
- **Customer `/call` has absolute priority.** Both the engine worker and the
  steady-state refresher yield on `customerInFlight()`.
- **The whole feature is toggle-off-able — via `CACHE_ENABLED` only.** The
  per-stage prewarm envs are retired; setting one is ignored (audited). The
  seed's memory use is bounded by the shared adaptive admission ceiling, not by
  capacity knobs.

## Known failure modes

- **Pre-sync single-pass dynamic-children miss (the boot-race).** The Step-4
  walk runs before the sync barrier, so a navmenu whose children are
  `resourcesRefsTemplate`-driven sees empty fallthrough data and harvests only
  the roots. *Mitigation:* the engine's post-barrier re-walk (always on).
- **Missing / late frontend ConfigMap.** No longer a permanent cold pod: the
  config-vars informer re-drives `scopeKindBoot` when the ConfigMap appears or
  its data changes, before or after the readiness backstop, no restart.
  Until then, lazy register-on-navigation covers GVRs on first request.
- **Ready-degraded boot.** A seed error, `pipGlobalTimeout`/`PHASE1_TIMEOUT`
  expiry, or a seed panic flips readiness with cold cells;
  `snowplow_readyz_backstop_fired` (+ `readyz.backstop.fired` ERROR) is the
  signal. First `/call` per affected cohort falls back to per-user resolve.
- **Engine worker lifetime.** The worker is bound to the process-lifetime ctx
  via `SetEngineProcessContext` — binding it to the boot-seed ctx (the 0.30.247
  regression) would drop post-boot `scopeKindGVRDiscovered`/keepwarm work.
- **Six HARD REVERTs from a missing `rc` field.** A `phase1Walker` struct
  literal that omitted `rc` (the SA `*rest.Config`) fail-closed at
  `discoverPluralInfo`. *Fix:* `newPhase1Walker` makes `rc` a required
  positional parameter (`phase1_walker_new.go`).
- **Empty per-GVR index → skipped seed.** If `BuildBindingsByGVRIndex` runs
  before the RBAC snapshot is ready, or a navigated GVR has no authorising
  binding, `EnumeratePrewarmTargetsForGVR` returns nil and the seed skips that
  GVR by design. Those targets fall back to cold-then-warm at first `/call` —
  never to a global-cohort universe (that fallback was an RBAC-leak risk and
  was removed).
- **Transient apiserver pressure during seed.** An operational (non-RBAC-deny)
  per-target seed failure re-enqueues a single coalesced `scopeKindBoot` to
  retry after pressure clears (`phase1_seed_classify.go`); the F.4 boot-scope
  resume chunks the re-walk with a fresh-skip so a deadline-cut pass resumes
  instead of restarting, and the re-enqueue is bounded against deterministic
  seed failures (no infinite redrive). RBAC-deny failures are expected and not
  retried.
- **External whales.** Boot-seed external fetches are wall-clock-bounded, and
  declined-external markers (F4b Lever A) stop a resume loop from re-resolving
  the same external target; an external RA is never L1-cached anyway (see
  caching.md §4) unless TTL-annotated.
- **Widget/RESTAction CR edits do not re-walk their subtree (gap, not bug).**
  `scopeKindWidgetCR`/`scopeKindRBACShift` are declared but unwired; such edits
  are covered only by the refresher (cached dependents) and lazy
  resolve-on-navigation until a future ship wires them.
