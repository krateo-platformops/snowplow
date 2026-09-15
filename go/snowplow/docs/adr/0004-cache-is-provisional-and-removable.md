---
type: Decision
title: "ADR 0004 — All caching is provisional and removable; CACHE_ENABLED=false is a transparent fallback"
description: >-
  CACHE_ENABLED is the single master gate over snowplow's whole cache
  subsystem; turning it off routes every read straight to the apiserver with
  identical data, UI, and RBAC — the correctness baseline and the documented
  incident lever. Sub-tier back-out knobs and bounded-TTL tunables layer under
  it without changing the invariant.
resource: snowplow
tags:
  - adr
  - cache
  - operations
  - flags
status: implemented
timestamp: 2026-08-06T00:00:00Z
---

# ADR 0004 — All caching is provisional and removable; `CACHE_ENABLED=false` is a transparent fallback

- **Status:** Accepted and implemented as decided; the knob inventory has grown (bounded-TTL
  tunables) without changing the invariant.
- **Related:** ADR 0002, ADR 0003 (the cache layers this invariant governs).
  Deep dive: [`docs/architecture/caching.md`](../architecture/caching.md).

## Context

Snowplow's entire cache subsystem — the L3 informer cache, the L1 resolved-entry cache, the
dispatcher seam, prewarm, the typed-RBAC snapshot — exists for one reason: to serve the Krateo
frontend fast enough at 50K scale without re-hitting the apiserver on every `/call`. It is an
optimisation, not the source of truth. The apiserver is.

Two risks follow from a cache that becomes load-bearing for *correctness*:

1. If the cache ever returns data, UI, or RBAC verdicts that differ from a direct apiserver read,
   it is a bug surface that is hard to reason about and hard to roll back.
2. Kubernetes may eventually ship better server-side caching (consistent watch caches, etc.),
   at which point snowplow's in-process cache should be removable without a rewrite.

## Decision

**The cache is provisional and must stay cleanly removable.** This is enforced by a single master
toggle and the invariant that turning it off is a *transparent fallback*, not a degraded mode.

- `CACHE_ENABLED` is the master gate (`cache.Disabled()`, `internal/cache/cache.go`). The code
  default is **off** — only an explicit truthy value enables the subsystem (the chart sets
  `CACHE_ENABLED: "true"` for production). With it off, all tiers vanish and **every read goes
  straight to the apiserver — same data, same UI, same RBAC, just slower.** This is not a
  reduced-functionality path; it is the correctness baseline.
- The fallback is enforced at every tier, by construction, not by special-casing:
  - **L3** switches to `modePassthrough`: `GetObject` issues a live `Get` and `ListObjects` a
    fresh paged LIST (`internal/cache/watcher.go`) instead of reading the indexer. Same
    `GetObject`/`ListObjects` surface, two backing implementations chosen once at construction
    (`NewResourceWatcher`).
  - **L1** `cache.ResolvedCache()` returns `nil` and every consumer nil-checks and resolves
    directly (`internal/cache/resolved.go`).
  - **RBAC** routes through `UserCan` → `SelfSubjectAccessReview` against the user's own
    kubeconfig (`internal/rbac/rbac.go`), and the dispatcher's in-process `EvaluateRBAC` gate is
    skipped because the per-user apiserver fetch enforces RBAC inline
    (`internal/handlers/dispatchers/restactions.go`). There is **zero
    `SubjectAccessReview` to the apiserver in cache-on mode**, and `userCanViaSAR` MUST be
    reachable only when `cache.Disabled() == true` (asserted by the F7 invariant tests).
  - **Prewarm** is implicit under the master gate (`cache.PrewarmEnabled()`,
    `internal/cache/phase1.go`): with it off, the walk never runs, its harvesters are nil, and
    the seed is a no-op (`internal/handlers/dispatchers/phase1_walk.go`).
- `CACHE_ENABLED` is the single master gate; formerly-standalone flags are folded into it (#57,
  `project_single_cache_flag_direction`): `PREWARM_ENABLED`, `RESOLVER_USE_INFORMER`,
  `PREWARM_CONTENT_ENABLED`, `PREWARM_PIP_ENABLED`, and `RESOLVED_CACHE_APISTAGE_ENABLED` are
  retired names, implicit-on under the subsystem (main.go's retired-flag audit warns once on
  stale values). What remains under it:
  - **Back-out knobs** for narrowing a regression to one tier without losing the others:
    `RESOLVED_CACHE_ENABLED` (the L1 store + refresher; the api-stage L1 is implicit under it)
    and `WIDGET_CONTENT_L1_ENABLED` (the identity-free widget layer only).
  - **Capacity/TTL tunables**, not feature flags: `RESOLVED_CACHE_MAX_ENTRIES` /
    `RESOLVED_CACHE_MAX_BYTES` / `RESOLVED_CACHE_TTL_SECONDS` /
    `RESOLVED_CACHE_MAX_RESIDENT_BYTES` (the pinned-resident region; `0` disables pinning —
    the Ship 4a kill-switch), and the bounded-staleness backstops
    `CATALOG_UNSERVABLE_TTL_SECONDS` (#36) and `UAF_RESOLVED_TTL_SECONDS` (#118 (d)) — each
    default `0` = off, purely additive. (`PARTIAL_RESULT_TTL_SECONDS` (#313 D) was retired in
    1.12.6 C9; it is audited as a retired flag.)
- The subsystem is kept structurally removable (`internal/cache/cache.go` package contract,
  `project_caching_is_provisional`).

## Consequences

- **Instant, safe rollback.** A cache-introduced regression is mitigated by one helm `--set
  env.CACHE_ENABLED=false`; the pod keeps serving correct content from the apiserver, just
  slower. This is the documented incident lever.
- **The cache can never be the correctness story.** Because cache-off is the baseline, any
  cache-on/cache-off behavioural divergence is by definition a bug — which makes the cache testable
  against a known-correct reference (the same `/call` with the toggle flipped).
- **Removability is preserved.** When Kubernetes offers adequate server-side caching, snowplow can
  drop the in-process tiers without changing the request contract, because the passthrough path
  already *is* that world.
- **Performance is the only thing you lose by turning it off** — cold reads, no prewarm, a
  per-request SAR per RBAC check. That is the intended trade: correctness is constant, latency is
  the variable.
- **Boundary for contributors.** A change that makes cache-off return *different* data/UI/RBAC, or
  that makes any correctness path depend on the cache being on, violates this ADR.
