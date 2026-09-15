---
type: Architecture
title: snowplow — caching (the three-tier cache)
description: The read-path cache that lets /call serve resolved RESTAction/Widget JSON without re-hitting the apiserver — L3 informer, L1 resolved-entry store, dispatcher seam, keying, invalidation, invariants.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [cache, l1, informer, keying, invalidation]
timestamp: 2026-08-06T00:00:00Z
---

# Caching architecture — the three-tier cache

> **Audience:** snowplow maintainers.
> **Scope:** the read-path cache that lets `/call` serve resolved `RESTAction` / `Widget`
> JSON without re-hitting the apiserver on every request.
> **Status:** re-derived from the current tree (docs-standard, 2026-08-06). Anchors are
> `file` + function names; where a prior revision of this doc disagreed with the code, the
> code won.

Snowplow stacks three caches in the request path. They are layered, not alternatives:

- **L3 — informer cache** (`internal/cache/watcher.go`): an in-process Kubernetes informer
  store. Resolvers read raw cluster objects from here instead of the apiserver.
- **L1 — resolved-entry cache** (`internal/cache/resolved.go`): a bounded LRU of the
  *already-resolved, pre-encoded JSON* a `/call` returns. A hit skips the whole resolver.
- **Dispatcher cache seam** (`internal/handlers/dispatchers/`): the request-handler logic that
  computes the L1 key, consults L1, and on a miss resolves + populates it. This is the "tier"
  where keying, the RBAC gate ordering, and the widget two-key fast-path live; the *store* it
  uses is L1.

The whole stack is gated behind one master toggle, `CACHE_ENABLED`
(`internal/cache/cache.go` `Disabled()`). With it off, all three tiers vanish and every read goes
straight to the apiserver — same data, same UI, same RBAC, just slower (see *Invariants*).

---

## 1. Data flow

```
                         GET /call?resource=widgets&name=…&extras={…}
                                         │
                                         ▼
                    ┌──────────────────────────────────────────────┐
                    │  DISPATCHER SEAM  (handlers/dispatchers/)      │
                    │  widgets.go / restactions.go                   │
                    │                                                │
                    │  1. fetch dispatched CR (objects → L3)         │
                    │  2. EvaluateRBAC gate  (cache=on only)         │ checkDispatchRBAC
                    │     ── deny → 403, never cached ───────────────│
                    │  3. compute L1 key + consult L1                │
                    └──────────────────────────────────────────────┘
                                         │
        ┌────────────────────────────────┼─────────────────────────────────┐
        │ WIDGET fast-path (widgets.go)   │                                  │
        │                                 │                                  │
   (a) widgetContent key  ──hit──▶ gateWidgetEnvelope (re-stamp `allowed`)   │
       identity-FREE               ──▶ write per-user body, return           │
       (gvr,ns,name,page,extras)                                             │
            │ miss / RBAC-sensitive widget → fall through                    │
            ▼                                                                │
   (b) widgets key (per-binding  ──hit──▶ write RawJSON, return              │
       UID + RBAC sub-gen)                                                   │
            │ miss                                                           │
        ────┴────────────────────────────────────────────────────────────┐ │
                                         │ (restactions: single per-binding key)
                                         ▼                                  │
                    ┌──────────────────────────────────────────────┐       │
                    │  L1  ResolvedCacheStore  (resolved.go)         │◀──────┘
                    │  Get(key) → (*ResolvedEntry, bool)             │
                    └──────────────────────────────────────────────┘
                                         │ miss
                                         ▼
                    ┌──────────────────────────────────────────────┐
                    │  RESOLVER  (resolvers/…)                       │
                    │  reads cluster objects from ──────────────────┼──▶ L3 informer
                    │                                                │   GetObject / ListObjects
                    │  records dep edges as it reads (WithL1Key)     │   Deps().Record / RecordList
                    └──────────────────────────────────────────────┘
                                         │
                                         ▼
                    encode once → write to response
                    → L1.Put(key, entry)  + Deps().Record(self-edge)
                       (Put gated — see §4 step 4)

  ── invalidation (async, informer event → DepTracker) ────────────────────────
     L3 informer event ──▶ depEventHandlers ──▶ enqueue (gvr, ns, name)   [1.12.6 C1]
       dep-event worker ──▶ probeObjectState (informer indexer, tri-state)
         EXISTS  → dirty-mark deps (refresher re-resolve), never evict
         ABSENT  → evict self-entry, dirty-mark deps
         UNKNOWN → requeue ×maxRefreshRequeues, then degrade to a dirty-mark
     L1 commit    ──▶ cache.PublishRefresh(key) → /refreshes SSE subscribers
```

---

## 2. L3 — the informer cache (`watcher.go`)

`ResourceWatcher` (`internal/cache/watcher.go` `NewResourceWatcher`) owns a
`dynamicinformer.DynamicSharedInformerFactory` plus a per-GVR map of `GenericInformer`s.
Resolvers do not call the apiserver directly; they call:

- `GetObject(gvr, ns, name)` — in `modeInformer` it reads
  `gi.Informer().GetIndexer().GetByKey(...)` — an in-memory map lookup, no network. In
  `modePassthrough` (cache off) it falls back to a live `rw.dyn.Resource(gvr).Get` apiserver
  call.
- `ListObjects(gvr, ns)` — `modeInformer` materialises the namespace partition from the indexer
  (`listFromIndexer`); `modePassthrough` issues a fresh paged apiserver LIST
  (`listPassthrough`) with **no** in-process caching.

The mode split is the L3 half of the toggle invariant: same `GetObject`/`ListObjects` surface,
two backing implementations chosen once at construction. `NewResourceWatcher` logs
`"CACHE_ENABLED=false — typed-RBAC + informer cache + L1 ALL disabled"` and sets
`informer.get_list_path=apiserver` when the toggle is off.

Every serve from L3 is additionally guarded by the **servability assertion**
(`internal/cache/serve_assert.go`): an authoritative cache hit about to be served from a
not-synced / watch-broken informer trips `serve_requires_servable` (P1) rather than serving
silently stale data.

Each informer also registers DepTracker event handlers (`depEventHandlers`) so cluster
mutations drive L1 invalidation (§5).

---

## 3. L1 — the resolved-entry cache (`resolved.go`)

`ResolvedCacheStore` is a single-mutex bounded LRU: a `container/list` for recency order + a
`map[string]*list.Element` index, with three caps — `maxEntries`, `maxBytes`, `ttl` — plus a
separate *pinned* resident byte budget (`maxResidentBytes`) for expensive prewarmed entries that
LRU pressure must not evict.

The value is a `ResolvedEntry`: pre-encoded `RawJSON` bytes ready to write, a `CreatedAt` for
TTL, the canonical `Inputs *ResolvedKeyInputs` the refresher re-resolves from, and an optional
per-entry `TTLOverride` (see §3.4). Storing the *encoded* form (not the live object) keeps the
hit path race-free — readers get an immutable `[]byte`.

- `Get`: index lookup; a TTL-expired entry is dropped and counted as a miss in the same call; a
  hit moves the element to the LRU front. The effective TTL is
  `min(entry.TTLOverride, store.ttl)` when an override is set.
- `Put`: stamps `CreatedAt`, computes the entry byte cost, resolves final pin status under the
  mutex (a `Pinned` entry that does not fit the resident budget is demoted to transient), then
  inserts and evicts the LRU tail until under caps.

### 3.1 Key structure — `ComputeKey`

`ComputeKey(in ResolvedKeyInputs) string` (`resolved.go`) is a hex-encoded SHA-256 over a
versioned, NUL-delimited byte stream. The fields folded in, in order:

1. `resolvedKeyVersion` salt — currently **`"v6"`** (`resolved.go`). Bumping it rotates the
   entire key space on a rolling restart so no stale-shape entry ever serves as a hit. The
   lineage is documented on the constant: v4 = per-binding `BindingUID` replaced the per-cohort
   `BindingSetHash`; v5 = `RBACSubGen` folded in (#118 (c)); v6 = the sub-gen bump moved from
   RBAC-delta-time to snapshot-publish-time (#118 (c)-v2).
2. `CacheEntryClass` — the entry-class discriminant, one of `"restactions"`, `"widgets"`,
   `"apistage"`, `"widgetContent"`, `"raFullList"`. The string *values* are load-bearing
   (hashed into the key + used as refresher registry keys).
3. The dispatched object's `Group / Version / Resource / Namespace / Name`.
4. **Identity** — the `BindingUID` and `RBACSubGen` fields are mechanically hashed for *every
   class except* `widgetContent`, but what they carry is per-class:
   - `restactions` / `widgets` — **two live terms**, stamped at dispatch by
     `dispatchCacheLookupKey` (`helpers.go` — the only `RBACSubGen` stamping site in the tree):
     `BindingUID`, the first-match binding that authorised this layer's GET (§3.2), and
     `RBACSubGen`, the requesting identity's per-subject RBAC **sub-generation**
     (`cache.RBACSubGenForSubject` over the user + groups + SA counters). A grant/revoke that
     touches this user's own bindings bumps a subject counter at snapshot publish
     (`rbac_subgen_pending.go`) → the term changes → cold miss → fresh resolve + fresh UAF
     refilter. Blast radius is herd-proportional (only subjects whose own bindings changed),
     which is why this replaced a global RBAC generation. The seed writes `RBACSubGen==0`
     (a warm-miss perf gap for a moved-sub-gen subject, not an authz bug — de-scoped as
     seed-reachability work).
   - `raFullList` — identity-bound via **`BindingUID` only**. Its single key source
     (`seedFullListRAKey`, `resolvers/widgets/apiref/ra_full_list.go` → `RAFullListKeyInputs`,
     `cache/ra_full_list_slice.go`) never sets `RBACSubGen`, so every `raFullList` cell hashes
     `RBACSubGen==0` on Put *and* Get — the sub-gen term is folded as a constant, semantically
     inert. These cells rotate only when the `BindingUID` itself changes; there is no
     grant/revoke sub-gen rotation for this class.
   - `apistage` — **identity-free content cells**: `contentKeyInputs`
     (`resolvers/restactions/api/apistage.go`) never sets either field, so both fold as
     empty/zero constants and the cell is identity-invariant. It is populated SA-maximal and
     RBAC-narrowed per request at serve time, fail-closed (the ADR 0003 pattern).
5. `PerPage` / `Page`.
6. `Stage` — only for `apistage` entries; written with a `0x01` sentinel and skipped when empty
   so non-apistage keys hash byte-identically across the field's introduction.
7. `Extras` — canonicalised via `canonicaliseExtras`: a recursively sorted-key JSON surrogate,
   so two requests with the same extras content but different map iteration order produce the
   same key, and distinct extras produce distinct entries. On marshal failure it falls back to a
   deterministic `fmt.Sprintf("%v", …)`. **Which request extras are allowed to reach the key at
   all is governed by the F6 allowlist** (§3.5).

Fields carried on `Inputs` but **excluded from `ComputeKey`**:
`RepresentativeUsername` / `RepresentativeGroups` (the refresher's re-resolve identity — two
members of the same equivalence class must not shift the cell's identity) and `HasUAF` (a
bookkeeping bit that re-stamps the UAF short-TTL on refresher re-Puts, §3.4).

### 3.2 The two widget L1 keys

A widget `/call` can hit L1 by two different keys, tried in order in `widgets.go`:

1. **Identity-free content key** (`CacheEntryClassWidgetContent`). Built by
   `dispatchWidgetContentKey` (`helpers.go`) with `Username`/`Groups` left zero; `ComputeKey`
   skips the identity fold for this class. So admin and a narrow-RBAC user hit the **same
   cell**, keyed only on `(gvr, ns, name, perPage, page, extras)`. The stored body is a *shell*
   with SA-evaluated `status.resourcesRefs.items[].allowed` flags; it is **never served
   verbatim** — on hit, `gateWidgetEnvelope` (`widget_content.go`) re-stamps every `allowed`
   flag under the request's own identity before the body is written. This path is skipped for
   RBAC-sensitive apiRef widgets (those whose `status.widgetData` is RBAC-narrowed and would
   leak the SA-maximal aggregate) — `isRBACSensitiveApiRefWidget`.

2. **Per-binding key** (`CacheEntryClass=="widgets"`). Built by `dispatchCacheLookupKey`
   (`helpers.go`), which calls `rbac.EvaluateRBAC` to derive the **`BindingUID`** of the
   first-match binding that authorised this layer's GET, reads the subject's `RBACSubGen`, and
   folds both into the key. Two users granted by the *same* binding (at the same sub-gen) share
   one cell; a deny or error fails closed to `bindingUID=""` — and an empty-`BindingUID` cell is
   **neither served nor written** on the dispatch path (`serveFromCacheEligible`, the #95
   cross-identity-leak closure; see §6.3). RESTActions use only this per-binding path.

The widget fast-path falls from (1) to (2) to a full resolve on successive misses. The
`BindingUID` derivation site of record is `cache.BindingUIDFromCRB` / `BindingUIDFromRB` in
`match_subject.go`; prefixes `"C:<uid>"` / `"R:<ns>/<uid>"` keep ClusterRoleBinding and
RoleBinding UIDs from aliasing and carry namespace scope into the identifier.

### 3.3 Value-dedup

Dedup here is **cell-sharing by equivalence class**, not byte-interning:

- *Across users*: per-binding keying means every user authorised by the same binding resolves to
  byte-identical output and lands on one cell — the per-user-keyed-never-cohort invariant
  satisfied at binding granularity.
- *Across pages*: the `raFullList` class caches the RA's full unpaginated result with
  `PerPage/Page` forced to 0; every paginated `/call` differing only in slice shares that one
  cell and the page is applied as a cheap Go-slice at serve time (`ra_full_list_slice.go`).
  Widgets feeding the same RA under the same binding share the same cell — the chokepoint
  dedupe across widgets.
- *Across cohorts (widgets)*: the identity-free content cell collapses all cohorts onto one
  stored shell, re-personalised at serve time (§3.2).

### 3.4 Per-entry TTL overrides

Three mechanisms shorten (never lengthen) an entry's lifetime via `TTLOverride`; the effective
TTL is always `min(override, store TTL)`:

- **UAF short-TTL (interim #118 (d))** — a cell whose RESTAction declares a `userAccessFilter`
  carries `Inputs.HasUAF`; when `UAF_RESOLVED_TTL_SECONDS > 0` both Put sites (customer
  dispatch and refresher re-Put) stamp the short override (`uaf_shortttl.go`), capping the
  RBAC-staleness window the per-object refilter can otherwise accumulate. Default 0 =
  disabled — the durable fix is the `RBACSubGen` key fold (§3.1).
- **External-widget bounded-TTL (opt-in)** — see §4 step 4 and `external_ttl.go`.

(A third mechanism, the `PARTIAL_RESULT_TTL_SECONDS` bounded Put of a partial-with-errors
body, was retired in 1.12.6 C9. It was default-off and never enabled on any deployment; a
partial body is exactly the content §5.3 and the lifetime bound below exist to keep out of L1.
The env var is now a retired flag: setting it to anything but `0` logs a startup WARN.)

**Bounded lifetime (1.12.6, `RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS`, default `"86400"`).** The
TTL above is measured from `CreatedAt`, which *every* `Put` resets — a refresher re-Put or a
keep-warm sweep included — so an entry the event pipeline keeps re-Putting never expires, and a
wrong refresh (a stale body, a missed dep edge) is served for the life of the pod. Nothing in
the pipeline can catch a mistake the pipeline itself made; a lifetime bound measured from the
**first** `Put` can. `ResolvedEntry.BornAt` is set on the first `Put` under a key and
**inherited** by every replace-in-place `Put`; `Get` evicts an entry older than the max age
(`evict_max_age_total`, distinct from `evict_ttl_total`) and the request that hit it re-resolves
from scratch. It is a backstop, not a refresh cadence: at the default every L1 cell is
guaranteed one from-scratch resolve per day, and the eviction lands on a customer request path
only when that cell was not otherwise re-resolved in 24 h. `"0"` disables. Arms:
`resolved_max_entry_age_test.go` (G1 is RED on 1.12.5: a key re-Put every 400 ms under a 1 s
bound is still evicted).

---

## 4. The dispatcher seam (`handlers/dispatchers/`)

`/call` routes to a per-kind handler from the `dispatchers.All()` registry (`dispatchers.go`).
The handler (`restactions.go`, `widgets.go`) is where the cache is *consulted* — the ordering
here is load-bearing:

1. Fetch + convert the dispatched CR (reads L3).
2. **EvaluateRBAC gate runs BEFORE the L1 lookup** (`checkDispatchRBAC`) so a cache hit can
   never short-circuit the permission check. Cache-off skips this in-process gate because the
   per-user apiserver fetch enforces RBAC inline.
3. Compute key + `Get` — but only when `serveFromCacheEligible(inputs)` holds (a non-empty
   `BindingUID`; the #95 guard). On hit, `writeResolvedJSON(entry.RawJSON)` and return.
4. On miss: attach `WithL1KeyContext(ctx, cacheKey)` so the resolver records dep edges against
   this key as it reads L3; attach a `StageErrorSink` and an `ExternalTouchedSink`; resolve;
   encode once; serve; then **Put only when every gate passes**:
   - `stageErrSink.Count() == 0` — a partial-with-errors body is served (200) but never
     persisted, so transient item failures self-heal on the next resolve;
   - `extTouchedSink.Count() == 0` — a resolve that reached a genuine external endpoint
     (`httpFetchAllowingNonJSON`, ADR 0006) has **no dep edge to invalidate it**, so the Put is
     declined and every `/call` re-fetches live (`external_touched_sink.go`). *Exception:* a
     widget annotated `krateo.io/external-cache-ttl-seconds: <n>` opts into a bounded-staleness
     Put with `TTLOverride = min(n, 120s)` (`external_ttl.go`) — general opt-in, no hardcoded
     widget names;
   - `serveFromCacheEligible(inputs)` — never write the shared empty-`BindingUID` row from the
     dispatch path;
   - the **F6 undeclared-extras quarantine** — request extras must all be declared in the
     widget's `spec.keyExtras` or belong to the identity axis (`spec.identityContext`
     username/groups), else the Put is skipped
     (`filterDeclaredKeyExtras` / `snowplow_widget_skipped_undeclared_extras_put_total`).
   Finally `Deps().Record(cacheKey, gvr, ns, name)` records the self-edge after
   `ensureWatcherInformerForGVR` guarantees the GVR's informer is wired, and
   `cache.PublishRefresh(key)` signals any `/refreshes` SSE subscriber of the committed entry.

The refresher hooks are registered once at startup (`dispatchers.go` `RegisterRefreshFunc` for
each class), all pointing at the shared `resolveAndPopulateL1` (`resolve_populate.go`), which
re-resolves an entry from its own `Inputs` and re-`Put`s — it only ever `Put`s, never `Get`s.

---

## 5. Invalidation (`deps.go`)

L1 entries are invalidated by L3 informer events flowing through `DepTracker` (`Deps()`). The
tracker holds a forward index (DepKey → set of L1 keys) and a reverse index (L1 key → set of
DepKeys). Dependencies are recorded at resolve time:

- `Record(l1Key, gvr, ns, name)` — exact-object edge.
- `RecordList(l1Key, gvr, ns)` — list-scope edge encoded as the `(gvr, ns, "*")` wildcard
  bucket.

**Since 1.12.6 C1 the action is derived from STATE, not from the event type.** All three
informer handlers enqueue the coordinate `(gvr, ns, name)` on one typed, deduplicating,
rate-limiting workqueue; a single worker probes the informer (`probeObjectState`, tri-state:
`EXISTS` / `ABSENT` / `UNKNOWN`) and calls `OnObjectEvent(gvr, ns, name, state)`, the one
decision site. `OnAdd`/`OnUpdate`/`OnDelete` survive only as shims over it for the test surface.
Duplicates are idempotent, reordering is harmless (a late DELETE after a same-name recreate finds
the object `EXISTS` and dirty-marks the correct fresh entry instead of evicting it), coalescing
is safe, and a lost event is recoverable by the next enqueue for the same coordinate. `UNKNOWN`
(informer torn down, not synced, watch broken, type unconfirmed, passthrough) means the indexer
is not authoritative: the coordinate is requeued with backoff on the refresher's
`maxRefreshRequeues` budget and, on exhaustion, degraded to a dirty-mark so the refresher's
re-fetch decides against the apiserver (a definite 404 takes the self-gone route and evicts at
the drop point — seconds; since 1.12.6 C4 a 403, a 500, a timeout or a parse failure also
evicts there after the budget, behind the `REFRESH_DROP_EVICT_MAX_PER_MINUTE` breaker, and only
a suspended breaker leaves the body bounded by `RESOLVED_CACHE_TTL_SECONDS`, 3600 s). It never means
"keep forever", and it never evicts by itself — that is what keeps a schema-relist teardown window
from evicting a whole GVR. The probe reads the watcher directly and never calls
`EnsureResourceType`, so it is not the inert `IsRegistered` conjunct 1.12.5 removed (different
predicate — `IsServable` is four conjuncts — different call site, deferral instead of verdict).
Evictions are applied before dirty-marks, so a panic in the refresh hook cannot lose a due
eviction. The ADD pre-sync gate stays ADD-only at the handler.

The **invalidation rules**, stated by state:

- **`EXISTS` (ADD / UPDATE / a stale DELETE for an object that is back) → dirty-mark only,
  never evict.** Every dependent L1 key is enqueued into the refresher for stale-while-revalidate.
  ADD is treated identically to UPDATE because a freshly-created object can satisfy a LIST-dep that
  previously resolved empty.
- **`ABSENT` (DELETE) → three-way classification** (the `OnDelete` shim maps to it):
  1. *self-representation* — the entry's own dispatched object is the deleted object →
     **EVICT** (the only authorised eviction trigger). Classified by `isSelfRepresentation`,
     which reads the entry's `Inputs` and compares GVR/ns/name.
  2. *LIST-dep* — matched via the `(gvr, ns, "*")` wildcard; the entry's own object still
     exists but a list member went away → **DIRTY-MARK**.
  3. *dependent-GET-dep* — matched via an exact bucket but the entry's own object is a
     *different* object (e.g. a widget GET-depending on a deleted RESTAction) → **DIRTY-MARK**.
  Buckets 2 and 3 take the identical action, so `OnDelete` only needs the self-vs-non-self
  split; it returns the evicted count and dirty-marks the rest. `isSelfRepresentation` fails
  conservatively to `false` when the store/entry/Inputs is missing — missing an eviction merely
  leaves a stale entry until TTL, whereas over-eviction is the regression the falsifier catches.

This is the precise statement of the rule: **DELETE evicts only the deleted object's own entry;
LIST-deps and dependent GET-deps are dirty-marked; ADD/UPDATE dirty-mark.** TTL is the outer
safety net for any change the dep tracker cannot see (external data being the structural case —
hence the external Put-decline in §4).

The tracker and the store are kept in lock-step: `Deps().SetStore(...)` wires the L1 store, and
every L1 eviction path (LRU/TTL/DELETE) calls `Deps().RemoveL1Key` so dep records never outlive
their entry. The dep-record forward map is bounded (`DEPS_MAX_RECORDS`); on cap it drops new
records silently and relies on TTL for correctness.

### 5.1 The refresher's self-object 404 (1.12.5, #187)

An informer DELETE is not the only way snowplow learns an object is gone. A background refresh
re-fetches the entry's own CR before re-resolving it; when that re-fetch returns a **definite
apiserver 404**, the object is gone and the entry is the resolved representation of something
that no longer exists.

Since 1.12.5 the refresher **EVICTS at the DROP POINT**. `resolveOnceProd` wraps the 404 in the
`cache.ErrSelfObjectGone` sentinel (preserving the error text, so log greps are unchanged);
`resolveAndPopulateL1` returns it; `processNext` requeues as before, and only when the key
exhausts `maxRefreshRequeues` does it test the sentinel and call
`DepTracker.EvictSelfGone(key)` instead of dropping to TTL.

**The requeue budget is the bound, deliberately.** Five requeues is about 15.5 s of backoff at
the 500 ms/60 s defaults, so any window that clears inside it — a CRD re-registration, an
apiserver blip that happens to answer 404 — never reaches the eviction. Evicting on the *first*
404 would have no such bound.

**The type-absent case is not separately bound, and that is deliberate.** When a CRD itself is
gone the apiserver answers a genuine 404 for every object of that GVR, and all of them classify
as self-gone. A type-exists conjunct (`cache.Global().IsRegistered(gvr)`) was tried and
**removed**. It was inert where it mattered: `objects.Get`'s own not-servable branch calls
`EnsureResourceType(gvr)` **before** falling through to the apiserver, so the GVR is registered
again by the time the error is classified, and `EnsureResourceType` refuses only at **group**
granularity — a single deleted CRD whose group survives passes it. Where it was inert it was
worse than absent, because the next reader would trust it as a bound; and where it would have
bound (an entire group gone) the objects genuinely are gone, so it would have blocked a correct
eviction and stranded those entries to TTL. Do not re-add it. The `BCRD` arm pins the real
behaviour, with the `cache.lazy_register` line as the evidence.

The budget still covers the case that matters: a CRD re-registration that clears inside ~15.5 s
never evicts, and a type absent for longer than that has taken its objects with it. The cost is
bounded and worth stating — a deleted object is re-fetched up to six times over about 15 s
before its entry goes, so a 102-CR deletion batch costs on the order of 600 apiserver GETs,
spread by the refresher rate floor.

Two further bounds, each with its own arm
(`dispatchers/issue187_self_notfound_evict_test.go`, `cache/issue187_recreate_falsifier_test.go`):

- **Only a definite 404 takes the self-gone route.** A 403, a 500, a timeout or a parse
  failure never carries the sentinel and is never counted as a deletion. Until 1.12.5 such a
  key dropped to TTL with its stale body resident; since 1.12.6 a failure that exhausts the
  budget is evicted at the drop point too, behind the breaker of §5.3 — so an apiserver
  hiccup or an RBAC blip still cannot evict a slice of L1 at once.
- **Never a synthesised 404.** Under `cache.WithInformerOnlyReads`, `objects.Get` fabricates a
  NotFound without asking the apiserver; that means "absent from the indexer", not "deleted".
- **Only the self object, structurally.** The sentinel is wrapped at exactly one site — the
  re-fetch of the CR named by the entry's own `Inputs`, which runs before the resolver — and the
  gate is `errors.Is`, never string matching. An inner call's NotFound stays bucket 2/3: a child
  vanishing dirty-marks the parent, it never evicts it.

The eviction routes through the tracker, never `store.deleteForDep`, so `RemoveL1Key` clears
the dep edges alongside the store delete. It counts on **`evict_self_gone_total`, not
`evict_delete_total`**: the latter stays informer-DELETE-driven because it is the live
discriminator for a dead DELETE bridge (delete a throwaway CR and watch it move), and a folded
counter would move for two unrelated reasons.

This widens the set of authorised eviction *triggers* without widening the rule: a repeatedly
confirmed 404 on the entry's own object is a deletion observation, the same fact the informer
event carries, arriving by a different route.

Two operational notes from #187: the dep-event worker (1.12.5: the DELETE hand-off worker)
recovers **per event**, so one panicking event costs exactly one action rather than killing
invalidation process-wide; and the tracker's counters are published at `/debug/vars` under `snowplow_deps`
(see `docs/architecture/observability.md`), because at `LOG_LEVEL=warn` the old INFO-only
summary made "did invalidation stop?" unanswerable from a live pod.

### 5.2 The CRD schema-relist teardown window (1.12.5, #187)

`triggerCRDSchemaRelist` tears the per-GVR informer down (`RemoveResourceType` closes its stop
channel and purges its state, so anything in its DeltaFIFO or in flight is dropped with no
handler run) and builds a **fresh** informer. A fresh informer has an empty `knownObjects`, so
`DeltaFIFO.Replace()` has nothing to diff against and **synthesises no `Deleted` delta** for an
object that vanished before the new LIST. An object deleted inside that window therefore leaves
its self entry stranded with no future informer trigger.

`OnResourceTypeSchemaRelisted` dirty-marks every dependent entry from the **forward dep index**,
not from the indexer, so it covers entries whose objects are absent. But it ran **once**, at the
start of the window: an entry dirty-marked at that instant re-resolves successfully while its
object still exists, and is then stranded when the delete lands a moment later. That is the
#187 burst shape — relist at ~09:21, deletes 09:21–09:24.

Since 1.12.5 the relist **re-fires the same dirty-mark after the new informer has synced**,
in the goroutine that already waits on the sync channel. Every stranded entry is then handed to
the self-404 eviction above. The pre-sync fire is kept as well; both run. Counted as
`relist_dirtymark_postsync_total` under `snowplow_deps`.

This is containment, not the precise form: it evicts by re-resolving and observing a 404, so it
costs the requeue budget and depends on the refresh path being healthy. Since 1.12.6 C2 the
relist delta bridge re-derives the missing DELETEs from the indexer (`relist_bridge.go`), and
the C3 **sampled reconcile audit** (`deps_reconcile.go`) is the safety net under the whole
pipeline: every `DEPS_RECONCILE_PERIOD_SECONDS` (30) it probes `DEPS_RECONCILE_SAMPLE` (512)
resident entries against the indexer and hands ABSENT ones to the dep-event worker.

**Coverage — the audit is a divergence detector, not a staleness bound.** With a random window
of 512 per tick against 100K resident entries, the expected first visit of a given entry is
≈ 1.6 h and 99 % coverage takes ≈ 900 ticks ≈ **7.5 h** — longer than the 3600 s TTL, so for
most entries the TTL still fires first. What the audit guarantees is that a lost DELETE shows
up as `reconcile_divergence_total > 0` (alertable) and that entries which keep being re-Put
(keep-warm cells, C4-suppressed keys) are eventually evicted; it does not bound how long a
stranded entry can be served. Raise `DEPS_RECONCILE_SAMPLE` for a tighter bound (O(sample)
under the store mutex per tick). Full arithmetic next to the counter rows in observability.md.

### 5.3 Terminal refresh semantics (1.12.6, #191)

**No refresh outcome may end in "forget the key and keep the entry".** Every path through the
refresher ends in exactly one of three states, all counted:

| Outcome | When | Counter |
|---|---|---|
| **re-Put** | the re-resolve succeeded | `completed_total` |
| **evict** | the entry's basis is gone or unverifiable: the self-object 404 (§5.1), and since 1.12.6 **every** deterministic failure that exhausts `maxRefreshRequeues` — 403, 500, timeout, parse failure, an apistage content call that is not servable | `self_notfound_evict_total`, `drop_evict_total` |
| **suppress** | the decline sites in `resolveAndPopulateL1` (stage error, external endpoint, `userAccessFilter` cell, unsupported kind) — the key is marked *refresh-by-traffic-only*, skipped by the refresher without a resolve, and cleared by the next real `Put` or by any eviction | `suppressed_set_total`, `suppressed_skips_total`, `suppressed_keys` |

Before 1.12.6 the drop point in `processNext` forgot a non-404 key and left the stale body
resident until the 1 h TTL, and the decline gates re-resolved the same key on every dirty-mark
forever — #191 was 956 WARNs from 23 keys in 4.5 h, every one a deterministic `exportJwt`
401 under the SA identity.

**The breaker (`REFRESH_DROP_EVICT_MAX_PER_MINUTE`, default `"64"`).** Evicting is right for a
handful of dead objects and wrong for a mass failure: an apiserver outage fails every refresh,
so after the budget every dirty-marked key reaches the drop point, and evicting them all turns a
stale portal into a broken one on top of the outage. The knob is a token bucket on drop-point
evictions, **not a rate limit** — it is the discriminator between the correctness regime (a few
deterministic failures, evict) and the availability regime (a mass failure, keep serving).
Over budget the key keeps the pre-1.12.6 drop-to-TTL, `drop_evict_suspended_total` ticks and
**one** WARN per suspension window (`refresher.drop_evict_suspended`) names the suspected
outage. `"0"` disables drop-point eviction for non-404 failures entirely — byte-identical to
1.12.5 — and is the kill switch. The self-object 404 route is not subject to the breaker.

**Suppression (`REFRESH_SUPPRESS_AFTER_DECLINES`, default `"3"`).** A stage-error decline is
suppressed after K consecutive declines of the same key; the structurally permanent declines
(external endpoint, UAF cell, unsupported kind) suppress on the first. The marker is package
state in `refresher_terminal.go`, never a field on the resident entry (`Get` hands out the live
pointer; mutating it is a data race), so `Put` and every eviction path can clear it without
constructing the refresher. A suppressed entry is still served and still correct — real user
traffic is what writes it; `RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS` (§3.4) bounds how long it can
live without one.

The arms are `cache/refresh_terminal_test.go` (F5a/F5b are the two-regime pin: three failures
under the default budget all evict with the breaker idle; six failures under budget `"1"` evict
one and keep five resident **and served** — an unbounded evict passes the first and fails the
second, a never-evict the reverse) and
`dispatchers/issue1126_c4_terminal_e2e_test.go` (rows 3/4/6/9 driven through the production
`resolveAndPopulateL1`).

---

### 5.4 Where the pipeline is still blind — the delivery-failure matrix (1.12.6 C8)

Every loss mode along informer → worker → refresher → store → broadcaster → gateway → SPA, each
with the counter that detects it and the arm that pins it, and the rows that have neither
listed as OPEN, is in `observability.md` § "Delivery-failure matrix". Read it before claiming a
change "reaches the browser": the server half is pinned through the broadcaster; the gateway hop
and the SPA's recovery are OPEN rows owned by the chart review checklist and frontend#256.

## 6. Invariants

1. **Provisionality / toggle (transparent fallback).** `CACHE_ENABLED=false` (`cache.go`
   `Disabled()`) must be a transparent fallback to the direct apiserver path — **same data,
   same UI, same RBAC, only slower** — not a degraded mode. It is enforced at every tier: L3
   switches to `modePassthrough` live apiserver reads; L1's `ResolvedCache()` returns `nil` and
   every consumer nil-checks and resolves directly; the dispatcher's in-process EvaluateRBAC
   gate is skipped because per-user apiserver fetches enforce RBAC inline. The whole subsystem
   stays cleanly removable. `CACHE_ENABLED` is the single master gate — prewarm (the whole
   family), the informer-serve pivot, and the api-stage L1 are implicit under it; the retired
   per-feature envs (`PREWARM_ENABLED`, `RESOLVER_USE_INFORMER`,
   `RESOLVED_CACHE_APISTAGE_ENABLED`, `PREWARM_CONTENT_ENABLED`, `PREWARM_PIP_ENABLED`,
   `PREWARM_ENGINE_ENABLED`, `PROACTIVE_RA_SEED_ENABLED`) are ignored with a one-shot audit log
   (`retired_flags.go`). Fine-grained back-out knobs that remain explicit:
   `RESOLVED_CACHE_ENABLED`, `WIDGET_CONTENT_L1_ENABLED`, `REFRESH_SSE_ENABLED`,
   `UAF_RESOLVED_TTL_SECONDS`.
2. **RBAC is never short-circuited by a hit.** The EvaluateRBAC gate runs *before* the L1
   lookup; the identity-free widget cell is re-personalised per request by `gateWidgetEnvelope`
   and is bypassed entirely for RBAC-sensitive `widgetData` widgets. The cached body is the
   shell; the body that leaves the pod is per-user.
3. **Per-user (per-binding) keying, never cohort-only — and never the empty row.** Identity-bound
   classes fold `BindingUID` (`restactions`/`widgets` additionally `RBACSubGen`); the
   identity-free classes — `widgetContent` and `apistage` — are so only
   because their served bodies are re-narrowed per request. Every member of a `BindingUID` equivalence
   class resolves to byte-identical output. An empty `BindingUID` (deny / no snapshot / cache
   off) is **not cache-eligible** on the dispatch path — `serveFromCacheEligible` blocks both
   the read and the write, closing the #95 cross-identity leak through the shared `""` row.
4. **DELETE-only eviction.** UPDATE/ADD use stale-while-revalidate dirty-marking; eviction is
   reserved for a DELETE of an entry's own object.
5. **Never persist an under-served or un-invalidatable result.** `Put` is gated on zero per-item
   stage errors AND zero external touches (unless the bounded-TTL opt-in applies), so a partial
   or dep-edge-less body is served but never pins itself for the TTL.
6. **Author-declared key surface.** Request extras partition the key only where the widget
   author declared them (`spec.keyExtras`) or on the identity axis (`spec.identityContext`);
   an undeclared-extras resolve is served-but-not-cached, never a silent cell fork.
7. **No per-resource special cases.** Key shape is per *class*, uniform across every GVR;
   behaviour is expressed via the entry-class discriminant (or a general annotation like the
   external TTL), never hardcoded resource names.

---

## 7. Known failure modes

- **Seed→serve key divergence.** If the prewarm seed `Put`s under a different `BindingUID`,
  `RBACSubGen`, extras or page than the dispatcher `Get` computes, the warm cell is missed and
  the request falls through to a cold resolve. The `emitDispatchCacheKeyDiag` lines
  (`helpers.go`) exist specifically to diff the folded components for one object. Symptom:
  `l1=miss` on a request you expected to be warm; check the `binding_uid` field across the
  sites. (The seed's `RBACSubGen==0` is a known warm-miss for subjects whose sub-gen has
  moved.)
- **Stale content past dirty-mark.** Dirty-marking only *enqueues* a refresher re-resolve; until
  the refresher runs, a hit serves the prior bytes (stale-while-revalidate by design). A wedged
  or back-pressured refresher leaves stale content until TTL.
- **Breaker suspension during an outage.** While `refresher.drop_evict_suspended` is firing,
  keys past the budget keep the old drop-to-TTL and serve stale until a later dirty-mark or the
  max entry age; that is the intended availability posture, not a stuck refresher. A steadily
  climbing `drop_evict_suspended_total` with a healthy apiserver means the budget is too low.
- **Dep-record cap drop.** Past the cap, new dep edges are dropped silently and a one-shot WARN
  (`deps.cache.cap_reached`) fires; affected entries then rely on TTL rather than event-driven
  invalidation.
- **Conservative under-eviction on DELETE.** When `isSelfRepresentation` cannot read the entry's
  `Inputs` it returns `false` and the entry is dirty-marked instead of evicted —
  correct-but-slower; the entry clears at TTL.
- **Pin demotion under resident-budget pressure.** A `Put` that requests `Pinned` but does not
  fit the resident byte budget is demoted to transient — an expensive prewarmed cell can then be
  LRU-evicted, reintroducing a cold navigation.
- **External-TTL staleness window.** An annotated external widget serves up to
  `min(annotation, 120s)`-stale external data by design; a fat-fingered large value is clamped
  by the cap.

---

## 8. File map

| Concern | File | Key anchors |
|---|---|---|
| Master toggle | `internal/cache/cache.go` | `Disabled()` |
| Retired-flag audit | `internal/cache/retired_flags.go` | `AuditRetiredFlags` |
| L1 store, keys, dedup, TTL overrides | `internal/cache/resolved.go` | `ComputeKey`, `canonicaliseExtras`, `ResolvedKeyInputs` (`BindingUID`, `RBACSubGen`, `HasUAF`), `resolvedKeyVersion="v6"`, entry classes, `Get`, `Put` |
| RBAC sub-generation | `internal/cache/rbac_subgen.go`, `rbac_subgen_pending.go` | `RBACSubGenForSubject`, publish-deferred bumps |
| Terminal refresh semantics (breaker, suppression) | `internal/cache/refresher_terminal.go` | `dropEvictBreaker`, `NoteRefreshDecline`, `RefreshTerminalStatsSnapshot`; consulted in `refresher.go` `processNext` |
| Tag-derived stats families (1.12.6 C7) | `internal/cache/stats_by_tag.go`, `internal/cache/stats_families.go` | `StatSpec`, `StatFamily`, `TaggedStatFamilies`; `stat`/`kind`/`desc` tags on `CRDDiscoveryStats`, `RefreshBroadcasterStats`, `refresherStats`, `RefreshTerminalStats` drive expvar, the OTLP mirror (`internal/metrics/metrics.go` `newDerivedFamilies`) and the parity arms |
| Invalidation | `internal/cache/deps.go`, `deps_watch.go` | `Record`, `RecordList`, `OnObjectEvent` (state-derived; `OnAdd`/`OnUpdate`/`OnDelete` are shims), `isSelfRepresentation`; bridge: `depEventHandlers` → `submitDepEvent` → `probeObjectState` |
| External Put-gate | `internal/cache/external_touched_sink.go` | `WithExternalTouchedSink` |
| L3 informer | `internal/cache/watcher.go` | `NewResourceWatcher`, `GetObject`, `ListObjects`, `depEventHandlers` |
| Servability tripwire | `internal/cache/serve_assert.go` | `serve_requires_servable` |
| Dispatcher seam — keys | `internal/handlers/dispatchers/helpers.go` | `dispatchCacheLookupKey`, `dispatchWidgetContentKey`, `serveFromCacheEligible`, `filterDeclaredKeyExtras` |
| Dispatcher seam — RESTAction | `internal/handlers/dispatchers/restactions.go` | RBAC gate, L1 lookup, gated Put |
| Dispatcher seam — Widget | `internal/handlers/dispatchers/widgets.go` | content fast-path, per-binding lookup, external-TTL Put arm |
| External bounded-TTL opt-in | `internal/handlers/dispatchers/external_ttl.go` | `krateo.io/external-cache-ttl-seconds`, 120s cap |
| UAF short-TTL | `internal/handlers/dispatchers/uaf_shortttl.go` | `UAF_RESOLVED_TTL_SECONDS` |
| Refresher wiring | `internal/handlers/dispatchers/dispatchers.go`, `resolve_populate.go` | `RegisterRefreshFunc`, `resolveAndPopulateL1` |
| Live-refresh signal | `internal/cache/refresh_broadcaster.go` | `PublishRefresh`, `PublishEviction` (paced per subscriber: `REFRESH_EVICTION_PUBLISH_RATE_PER_SECOND` / `_BURST`, provisional), `REFRESH_SSE_ENABLED` |
