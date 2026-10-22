---
type: Architecture
title: snowplow — RBAC & user-aware filtering (UAF)
description: How every byte that leaves the pod is narrowed to the requesting user without per-request SubjectAccessReviews — the in-process evaluator, the subject index, per-binding+sub-generation L1 keying, and the serve-time per-item gates.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [rbac, uaf, security, cache-keying, evaluator]
timestamp: 2026-08-06T00:00:00Z
---

# RBAC & User-Aware Filtering (UAF)

**Audience:** maintainer · **Status:** re-derived from the current tree (docs-standard,
2026-08-06) · **Subsystem:** `internal/rbac/` (evaluator) + `internal/cache/` (snapshot, subject
index, L1 keying, sub-generation) + the serve-time filter sites in `internal/resolvers/` and
`internal/objects/`.

Snowplow serves Krateo frontend content out of an in-process cache. Every byte that leaves the pod
must be exactly what the *requesting user* is permitted to see — no more. This document traces how
that guarantee is enforced without a per-request `SubjectAccessReview` to the apiserver, and
captures the one invariant whose violation caused a six-revert retrospective: **the L1 resolved
cache must be keyed by the binding that authorised the request (plus the subject's RBAC
sub-generation), never by a cohort/content key alone.**

**Precondition (#835):** this design assumes authorization is a function of Kubernetes RBAC
bindings. That means no authorizer in the apiserver chain that can deny a read RBAC allows,
ahead of RBAC. Authorizers that only grant (such as GKE IAM, after RBAC) are fine. A
deny-capable authorizer before RBAC is a supported configuration only with the cache off.
See [docs/configuration.md](../../../../docs/configuration.md#supported-authorization-configuration).

---

## 1. Flow diagram

```
                                  /call  (request carries UserInfo: Username + Groups)
                                    │
        ┌───────────────────────────┴───────────────────────────────┐
        │                                                             │
   (A) CACHE-KEY DERIVATION                                  (B) SERVE-TIME UAF
   dispatchCacheLookupKey                                    per-item gate on the served body
   (dispatchers/helpers.go)                                  (list/get drop, widget allowed-flag)
        │                                                             │
        │ EvaluateRBAC(verb=get, …, SkipBindingUID=false)             │ EvaluateRBAC(verb, …,
        │   → (allowed, matchedBindingUID)                            │   SkipBindingUID=true)
        │ + cache.RBACSubGenForSubject(user, groups)                  │   → allowed only
        ▼                                                             ▼
   BindingUID + RBACSubGen folded into ComputeKey ──► L1 cell     keep / drop each item
   (cache/resolved.go ComputeKey, v6)                keyed by     (refilter.go, informer_serve.go,
        │                                            binding+     informer_dispatch_rbac.go,
        │                                            sub-gen      cluster_list.go)
        ▼                                            NOT cohort       │
   two users on the SAME binding share a cell                        │
   two users on DIFFERENT bindings get DIFFERENT cells               │
   a grant/revoke on YOUR bindings bumps YOUR sub-gen → cold miss    ▼
                                                              body that leaves the pod
                                                              is per-user-correct

        ┌──────────────────────── EvaluateRBAC (evaluate.go) ─────────────────────────────┐
        │  cache.Disabled()?  ──yes──►  UserCan → SelfSubjectAccessReview (SAR baseline)   │
        │       │ no                                  (rbac.go userCanViaSAR)              │
        │       ▼                                                                          │
        │  rw.Snapshot()  (cache.RBACSnapshot, single Load, threaded through the walk)     │
        │       │ nil → degrade-to-DENY                                                    │
        │       ▼                                                                          │
        │  L2 authz memo lookup  (snapshot_authz_memo.go, keyed by snap.PublishSeq)        │
        │       │ hit → return cached (allowed, uid)                                       │
        │       ▼ miss                                                                     │
        │  evaluateAgainstInformerFirstMatch                                               │
        │       1. selectCRBCandidates (subject index union)  ──► anySubjectMatches        │
        │       2. selectRBCandidates (ns-scoped)             ──► roleRefPermits           │
        │                                                          → rulesPermit           │
        │       first permit wins (RBAC v1 has no deny rules)                              │
        │  store PERMITS only (deny is never cached) → return                              │
        └──────────────────────────────────────────────────────────────────────────────┘
```

The two halves are deliberate. **(A)** decides *which cache cell* a request reads/writes. **(B)**
decides *which items inside a served body* the user keeps. Both call the same evaluator; they differ
only in whether they consume `matchedBindingUID` (A) or just `allowed` (B).

---

## 2. The evaluator — trace through `internal/rbac/`

### 2.1 Entry points: `UserCan` and `EvaluateRBAC`

`UserCan` (`rbac.go`) is the per-item yes/no API. With cache on it delegates to `EvaluateRBAC`
and discards the binding UID; with cache off it falls to `userCanViaSAR`, which issues a
`SelfSubjectAccessReview` against the user's own kubeconfig. That SAR path is the correctness
baseline that keeps `CACHE_ENABLED=false` a transparent fallback, and `userCanViaSAR` MUST be
reachable only when `cache.Disabled() == true`.

`EvaluateRBAC` (`evaluate.go`) is the core. Signature: `(bool allowed, string
matchedBindingUID, error)`. Decision order:

1. **`cache.Disabled()`** → route to `UserCan`/SAR, return an empty UID. No snapshot exists, so
   there is no binding to identify.
2. **`rw := cache.Global()` nil** → cache flagged on but watcher not wired: **degrade to deny**,
   do *not* silently fall back to the apiserver. Falling back here would violate the "zero
   `SubjectAccessReview` in cache=on" rule.
3. **`snap := rw.Snapshot()` nil** → typed-RBAC snapshot not yet published: **degrade to deny**.
   A single `Snapshot()` load is threaded through the entire walk so one evaluation observes one
   coherent snapshot generation.
4. **L2 authz memo lookup** keyed on `snap.PublishSeq` — see §2.5.
5. **Cold walk** via `evaluateAgainstInformerFirstMatch`.
6. **Store PERMITS only** — see §2.5; denies are never cached.

### 2.2 The candidate walk — `evaluateAgainstInformerFirstMatch`

Two phases, mirroring the upstream Kubernetes RBAC authorizer:

- **Phase 1 — ClusterRoleBindings.** `selectCRBCandidates(snap, opts)` returns a superset of the
  matching CRBs from the subject index (§2.3). When the caller consumes the UID
  (`SkipBindingUID == false`) the candidates are stable-sorted by `(Name, UID)` via
  `sortCRBsStable` so the first match is deterministic. Each candidate is gated through
  `anySubjectMatches` (the post-index correctness barrier) and then `roleRefPermits`. **First
  permit wins**, returning `cache.BindingUIDFromCRB(crb)`.
- **Phase 2 — RoleBindings in `opts.Namespace`,** only when `Namespace != ""`. Same structure
  via `selectRBCandidates` / `sortRBsStable` / `BindingUIDFromRB`.

No match in either phase → `(false, "", nil)`. There are **no deny rules in RBAC v1**, so any
single matching permit short-circuits the whole walk — which is why the candidate sort is purely
for UID determinism and never changes the yes/no verdict.

### 2.3 The subject index — `selectCRBCandidates` / `selectRBCandidates`

The index lives on the snapshot (`cache/rbac_snapshot.go`) and is built once per rebuild by
`rebuildSubjectIndexes`. It maps a subject to the bindings that name it:

| Subject kind | CRB index field | RB index field (per-ns) |
|---|---|---|
| `User` | `CRBsByUser[name]` | `RBsByUserByNS[ns][name]` |
| `Group` | `CRBsByGroup[name]` | `RBsByGroupByNS[ns][name]` |
| `ServiceAccount` | `CRBsByServiceAccount["<ns>/<name>"]` | `RBsByServiceAccountByNS[ns]["<ns>/<name>"]` |
| unrecognised kind | `CRBsCatchAll` | `RBsCatchAllByNS[ns]` |

`selectCRBCandidates` unions the routes that apply to the request identity, deduplicating by
pointer (a multi-subject binding appears once):

1. `CRBsByUser[Username]` when `Username != ""`
2. `CRBsByGroup[g]` for every `g` in `effectiveGroups(opts)`
3. `CRBsByGroup["system:authenticated"]`, gated on `Username != ""` so an unauthenticated
   identity never lands the implicit group
4. `CRBsByServiceAccount["<ns>/<name>"]` when the username is a canonical SA
5. `CRBsCatchAll` — always

The index is a **pre-filter only**: it is allowed to over-include, and `anySubjectMatches` is the
authoritative equality barrier after lookup. The hard invariant is the *other* direction: the
index must never **under**-include — a binding the linear scan would match must appear in the
candidate union, or a permit is silently lost. The `CRBsCatchAll` arm exists exactly so an
unrecognised future `Subject.Kind` cannot be dropped. `selectRBCandidates` is the
namespace-scoped analogue; a missing namespace yields an empty candidate set with no allocations.

### 2.4 Predicate symmetry: User AND Group (and ServiceAccount)

The load-bearing symmetry rule is that **every place the verdict depends on a subject kind, both
the index route and the matcher route handle User, Group, and ServiceAccount the same way.** If
the index routes a User but the matcher only checks Groups (or vice-versa), the answer is wrong.
The matcher `anySubjectMatches` is the canonical statement of the predicate:

- `UserKind` → exact `s.Name == opts.Username`
- `GroupKind` → membership test against `effectiveGroups`, **plus** the implicit
  `system:authenticated` for any authenticated identity
- `ServiceAccountKind` → `(Namespace, Name)` match on a parsed canonical SA username
- any other kind → no case → no match

`effectiveGroups` is the single source of group expansion: for a ServiceAccount identity it
appends the two Kubernetes synthetic groups `system:serviceaccounts` and
`system:serviceaccounts:<ns>`; for a non-SA identity it returns `opts.Groups` unchanged with no
allocation. Crucially **`selectCRBCandidates` reuses the same `effectiveGroups`** so the index
and the matcher agree on group expansion — the symmetry is enforced by shared code, not by two
parallel implementations.

`rulesPermit` walks the resolved role's `PolicyRule`s with Kubernetes wildcard semantics
(`stringSliceMatches`: `"*"` matches everything) over Verbs, APIGroups, Resources, and honours
`resourceNames`. `resourceNameMatches` implements the upstream `ResourceNameMatches` rule: a
non-empty `rule.ResourceNames` can only ever grant a **name-specific verb**
(`get`/`update`/`patch`/`delete`) and only when `opts.Name` is in the list — a
`resourceNames`-scoped rule **never** grants `list`. This was added in 0.30.109; its absence had
over-exposed every object on a `resourceNames`-scoped rule — a cross-user leak in
`filterListByRBAC`.

### 2.5 The L2 authz memo — `snapshot_authz_memo.go`

The candidate walk at 50K scale re-walks the same ~18K same-subject CRBs on every call for a
verdict that repeats thousands of times within a generation. The L2 memo collapses that.
Properties that keep it correct:

- **Generation-scoped.** A single `atomic.Pointer[snapshotAuthzShard]`; each shard carries the
  `snap.PublishSeq` it is valid for. Lookup compares the shard gen to the `PublishSeq` of the
  snapshot the caller *already holds* — no second snapshot load, no TTL. A `PublishSeq` change
  CAS-swaps a fresh empty shard, so no entry outlives its snapshot. `Gen` is also in the key to
  close the store-race window.
- **PERMITS only — never cache a deny.** A deny can be transiently wrong under snapshot churn (a
  momentarily-incoherent rebuild yields a fail-closed `false`); caching it would pin the wrong
  deny for the whole generation on a hot key (the snowplow SA's wildcard CRB can never be
  correctly denied), starving the refresher — the #301 incident. Permits are monotone-correct
  within a generation (a binding removal bumps `PublishSeq`), so they are safe to cache. Denies
  fall back to the walk every call and self-heal; `authzMemoDenyUncached` counts them.
- **`SkipBindingUID` is part of the key** so a UID-consumer and a verdict-only consumer never
  share an entry.
- **Capacity-capped** at 16384 with insert-refusal on breach — never evict-to-OOM.
- **Below the cache=off short-circuit** so `CACHE_ENABLED=false` never reaches it.

Groups fold into the key via `canonicalGroupsHash` (`groups_hash.go`): order-independent
(sort-first) FNV-1a with a per-element **length prefix** so distinct set partitions like
`["a","bc"]` vs `["ab","c"]` cannot alias. Hashing the *raw* `opts.Groups` is sufficient because
`effectiveGroups` is a pure function of `(Username, Groups)`. Counters are exposed read-only over
`/debug/vars` via `RegisterAuthzMemoExpvar`.

### 2.6 BindingUID derivation — `cache/match_subject.go`

`BindingUIDFromCRB` returns `"C:<uid>"`; `BindingUIDFromRB` returns `"R:<ns>/<uid>"`. The
`C:`/`R:` prefixes keep CRB and RB UIDs from aliasing and the `R:<ns>/` prefix carries namespace
scope into the identifier. Empty UID (synthetic fixtures / pre-stamp gap) falls back to a content
tuple framed with a `\x1f` separator. Both return `""` iff the pointer is nil.

### 2.7 The per-subject RBAC sub-generation — `cache/rbac_subgen.go`

The single dispatch-authorizing `BindingUID` is blind to a dependency the `userAccessFilter`
refilter has: it re-evaluates RBAC *per object, per that object's own namespace*. An out-of-band
grant/revoke used to evict zero resolved cells — the user's own stale UAF view was served until
the cell left the cache (#118). The durable fix is the **per-subject sub-generation**:

- Every subject (user, group, SA) has a counter; an RBAC delta that touches a subject's own
  bindings marks it pending, and the bump is applied **at snapshot publish**
  (`rbac_subgen_pending.go`) so the counter and the published snapshot move together (the
  (c)-v2 ordering fix — this is what bumped the key salt to v6).
- `cache.RBACSubGenForSubject(username, groups)` folds the subject's effective counters into one
  `uint64`, and `dispatchCacheLookupKey` — the only stamping site — stamps it into
  `ResolvedKeyInputs.RBACSubGen` for the dispatch-keyed classes (`restactions`, `widgets`),
  where `ComputeKey` hashes it (`raFullList` hashes the field as a constant `0` — §3.1). A
  change to *your* RBAC → *your* `restactions`/`widgets` keys rotate → cold miss → fresh
  resolve + fresh refilter. Blast radius is herd-proportional.
- The interim exposure cap — a short TTL override on UAF-bearing cells
  (`UAF_RESOLVED_TTL_SECONDS`, `dispatchers/uaf_shortttl.go`) — remains available,
  default-disabled.

---

## 3. Per-user-keyed L1 — the load-bearing invariant (ADR 0002/0003)

### 3.1 What gets keyed

`ComputeKey` (`cache/resolved.go`) builds the L1 cell key. Mechanically it hashes the
`BindingUID` and `RBACSubGen` fields for every class except `widgetContent`; what those fields
actually *carry* is per-class:

- **`restactions`, `widgets` — identity-bound, two live terms.** `dispatchCacheLookupKey`
  (`dispatchers/helpers.go`) derives the **`BindingUID`** — the first-match binding that
  authorised *this layer's* GET for *this request's* identity — with a direct
  `EvaluateRBAC(verb=get, …)` call leaving `SkipBindingUID` at its safe zero value (so the
  returned UID is the deterministic sorted first-match), and stamps the subject's
  **`RBACSubGen`** (§2.7) alongside it. That is the *only* `RBACSubGen` stamping site; a
  grant/revoke touching the user's own bindings rotates these cells via the sub-gen fold.
- **`raFullList` — identity-bound via `BindingUID` ONLY.** Its single key source is
  `seedFullListRAKey` (`resolvers/widgets/apiref/ra_full_list.go`), which derives the
  first-match `BindingUID` on the RA's own coordinates but never sets `RBACSubGen`
  (`RAFullListKeyInputs`, `cache/ra_full_list_slice.go`), so every `raFullList` cell hashes
  `RBACSubGen == 0` on Put *and* Get. The sub-gen term is mechanically folded but semantically
  inert: these cells rotate only when the `BindingUID` itself changes — there is no
  grant/revoke sub-gen rotation for this class.
- **`apistage` — identity-free content cells, NOT identity-bound.** `contentKeyInputs`
  (`resolvers/restactions/api/apistage.go`) never sets `BindingUID` or `RBACSubGen`, so both
  fold as empty/zero constants and the cell is identity-invariant. It is populated SA-maximal
  and RBAC-narrowed **per request at serve time**, fail-closed — the ADR 0003 pattern, same
  family as `widgetContent` (§3.3).

For the identity-bound classes the result is: **two users granted by the SAME binding (for
`restactions`/`widgets`, at the same sub-generation) share one cell; the same user authorised
by DIFFERENT bindings on different layers gets different cells; a grant/revoke touching a
user's own bindings rotates that user's `restactions`/`widgets` cells.** This is finer-grained
than the v3 cohort hash (`BindingSetHash`) it replaced. The key salt is `resolvedKeyVersion =
"v6"` — see caching.md §3.1 for the v1→v6 lineage.

### 3.2 The cross-user leak it prevents

The invariant is: **an identity-bound L1 cell must never be keyed by a cohort/content key
alone.**

If the cell were keyed only by content (gvr/ns/name/page/extras) and *not* by the authorising
binding, then user A — broadly authorised, e.g. an admin or the wildcard-CRB snowplow SA — would
write a cell holding rows A is permitted to see. User B, narrowly authorised, would then **read
A's cell** because the content key matches, and receive rows B has no grant for. That is a direct
cross-user RBAC leak. Folding `BindingUID` into the key means B's request (authorised by a
different binding, or denied → `BindingUID == ""`) computes a *different* key and never lands on
A's cell. The `feedback_l1_per_user_keyed_never_cohort` retrospective records that attempts to
collapse this to a cohort/content-only key were reverted six times; the binding-keyed cell is the
durable fix and is the substance of ADR 0002/0003.

### 3.3 The one exception that proves the rule: `widgetContent`

`widgetContent` is the **only** class that skips the identity fold. Its cached body is a *shell*:
`status.resourcesRefs.items[].allowed` flags are stored as populated by the SA walker, but the
serve-time gate `gateWidgetEnvelope` (`dispatchers/widget_content.go`) **overwrites every
`allowed` flag per-request via `rbac.UserCan` under the requesting identity before
serialisation.** So the body is shared but the bytes that leave the pod are per-user — the
identity narrowing moves from the *key* to a *serve-time rewrite*. This is the
identity-free-content-key + serve-time-UAF pattern (ADR 0003). `apistage` reaches the same
identity-free end state by different mechanics — its identity fields are never set and fold as
constants (§3.1) — and is governed by the same ADR 0003 rule. The general rule holds: an entry
is identity-free in the key **only if** it is re-narrowed per-user at serve time.

### 3.4 The empty-UID biconditional — and the #95 serve/Put guard

`BindingUID == ""` iff (cache off) OR (deny / no snapshot) — verified both directions by the F7
invariant tests (`internal/rbac/evaltest/empty_binding_uid_invariant_test.go`). A non-empty UID
for a deny would leak the matched-binding identity into the cache key and is treated as a broken
invariant.

The empty-UID *cell* itself proved to be a leak vector (#95): a broad identity could populate the
shared `""` row (e.g. via a legacy seed) and a *different* ""-deriving identity could then read
it. Closure is enforced at the dispatch seam: `serveFromCacheEligible`
(`dispatchers/helpers.go`) blocks **both** the L1 read and the L1 write for empty-`BindingUID`
inputs, and the seed skips empty-binding targets entirely
(`prewarm_seed_empty_binding_skip`). An identity that derives `""` simply resolves per-request —
the same shape as cache-off's transparent fallback.

---

## 4. Serve-time UAF — the per-item gate

Serve-time UAF is the second guarantee: even on a correctly-keyed cell, every item handed out is
re-checked against the requesting identity. The sites, all calling `EvaluateRBAC(…,
SkipBindingUID=true)` and consuming only `allowed`:

- **`refilter.go`** (`internal/resolvers/restactions/api/`) — `applyUserAccessFilter` /
  `refilterSlice` / `evalSingle`: the `userAccessFilter` dispatch path. SA-dispatched list
  results are filtered per-object; a JQ error, an `EvaluateRBAC` error, or a deny **drops** the
  item (fail-closed). This is the authoritative gate — if it drops, the user does not see the
  item. Per item: `NamespaceFrom` jq resolves the namespace (default `.metadata.namespace`);
  the resource-plural set is resolved once per dispatch (static `uaf.Resource` XOR jq-derived
  `ResourcesFrom`, OR-semantics across the set); and — **#123** — when `uaf.Verb` is a
  name-specific verb (`get`/`update`/`patch`/`delete`), the once-compiled `NameFrom` expression
  (default `.metadata.name`) derives the per-object name so a `resourceNames`-scoped grant
  matches the objects it names. `NameFrom` is evaluated *only* for name-specific verbs — for
  `list`/`watch` the evaluator never consults `Name` (a `resourceNames` rule cannot grant
  collection verbs), so an irrelevant `NameFrom` jq error cannot fail-close a list refilter.
- **`informer_dispatch_rbac.go`** — `filterListByRBAC` and `filterGetByRBAC`: post-LIST per-item
  drop and single-object GET gate for the informer-served path. Errors fail closed (item
  dropped).
- **`cluster_list.go`** — the cluster-scoped list gate; a not-yet-published snapshot degrades to
  deny.
- **`objects/informer_serve.go`** — `filterGetByRBAC`: no identity, an `EvaluateRBAC` error, or
  a deny all fail closed.
- **`dispatchers/helpers.go`** — `checkDispatchRBAC`: the per-request dispatch gate.

Because every per-item site discards `matchedBindingUID`, they pass `SkipBindingUID: true` to
skip the CRB/RB stable-sort — the ~43% pod-CPU lever at 50K scale. Correctness is unaffected:
the sort only affects which UID is returned, not the verdict.

The cache-key callers — `dispatchCacheLookupKey`, the `helpers.go` diagnostic, `ra_full_list` —
are the ones that leave `SkipBindingUID` false and keep the deterministic UID.

---

## 5. Invariants

1. **Identity-bound L1 cells are keyed by `BindingUID` (`restactions`/`widgets` additionally by
   `RBACSubGen`), never cohort/content alone.** Violation = cross-user leak (§3.2). The
   identity-free classes — `widgetContent` and `apistage` — are re-narrowed per-user at serve
   time (§3.1, §3.3). (ADR 0002 / 0003.)
2. **Zero `SubjectAccessReview` to the apiserver in cache=on mode.** All checks resolve against
   the informer-cached typed RBAC snapshot. A nil watcher or nil snapshot **degrades to deny**,
   never to apiserver fallback. The hard rollback trigger for the tag.
3. **cache=off is a transparent correctness baseline.** `CACHE_ENABLED=false` routes through
   `UserCan` → `SelfSubjectAccessReview` and returns an empty `BindingUID`; the memo and the
   snapshot are never reached.
4. **One snapshot generation per evaluation.** A single `Snapshot()` load is threaded through
   the whole walk and the memo key, so reads are coherent across a republish.
5. **Memo caches PERMITS only.** A deny can be transiently wrong under churn and must self-heal
   by re-walking every call. Caching a deny is the #301 incident.
6. **Predicate symmetry across subject kinds.** The subject index and `anySubjectMatches` route
   User, Group, and ServiceAccount identically, sharing `effectiveGroups` for group expansion.
7. **The subject index may over-include but never under-include.** `anySubjectMatches` is the
   post-lookup equality barrier; `CRBsCatchAll` guards unrecognised kinds.
8. **`BindingUID == ""` ⟺ (cache off) ∨ (deny / no snapshot)** — both directions
   (`evaltest/empty_binding_uid_invariant_test.go`) — and the empty-UID row is never served
   from or written to on the dispatch path (`serveFromCacheEligible`, #95).
9. **`resourceNames`-scoped rules never grant collection verbs.** A non-empty
   `rule.ResourceNames` matches only name-specific verbs and only the named object; the UAF
   refilter honours such grants per-object via `NameFrom` (#123).
10. **Sub-generation moves with the snapshot.** Subject counters bump at snapshot publish, not
    at raw delta time, so a key never encodes RBAC state the evaluator cannot yet see.

---

## 6. Known failure modes

| Symptom | Likely cause | Where to look |
|---|---|---|
| Cross-user content leak (user sees rows they lack a grant for) | An identity-bound class lost its `BindingUID`/`RBACSubGen` fold, a cohort/content-only key was reintroduced, or the #95 empty-UID guard regressed. | `ComputeKey` (`resolved.go`); `serveFromCacheEligible` (`helpers.go`); the `feedback_l1_per_user_keyed_never_cohort` retrospective; F7 + `compute_key_identity_invariants` tests. |
| Stale UAF view survives a grant/revoke | Sub-gen not bumped (publish-deferral wedge) or the seed's `RBACSubGen==0` cell pinned by a hot refresher without the UAF TTL cap. | `rbac_subgen_pending.go`; `uaf_shortttl.go`; `snowplow_rbac_publish_seq`. |
| Admin list stuck empty under load; refresher starved | A transiently-wrong deny got cached (regression of the PERMITS-only rule). | `evaluate.go` memo-store site; `snowplow_authz_memo_deny_uncached_total` should be > 0 and rising. |
| Everything denied right after pod start | Snapshot not yet published — degrade-to-deny pre-readiness gate firing as designed; self-heals once the first rebuild publishes. | `evaluate.go`; `cluster_list.go`. |
| Permits silently lost for one subject kind | Subject index under-includes (index/matcher predicate drift); or a future `Subject.Kind` not routed to `CRBsCatchAll`. | `selectCRBCandidates`; `anySubjectMatches`; `rebuildSubjectIndexes` (`cache/rbac_snapshot.go`). |
| Wrong verdict for a ServiceAccount caller | Synthetic SA groups not expanded, or index/matcher disagree on expansion. | `effectiveGroups`; `parseServiceAccountUsername` (`evaluate.go`). |
| `resourceNames`-scoped rule over-exposes a list, or its named objects vanish from a UAF list | `resourceNameMatches` regressed; or `NameFrom` mis-derives the per-object name. | `evaluate.go` `resourceNameMatches`; `refilter.go` `compileNameFrom`/`evalSingle`. |
| Stale verdict served across a binding change | Memo generation-binding broke (gen not in key, or shard not swapped on `PublishSeq` change). | `snapshot_authz_memo.go`. |
| Group-set hash collision (two different group sets share an entry) | A second, non-length-prefixed groups hasher was introduced (the 0.30.239 two-hasher drift). | `canonicalGroupsHash` (`groups_hash.go`); do not inline a second hasher. |
| Loud `rbac.indexer.read fallback=true` WARN | Typed transform did not fire on an RBAC object; the defensive `as{Kind}` conversion path ran. | `asClusterRoleBinding` etc. (`evaluate.go`). |

> **Testing caution.** Do **not** run `go test ./internal/rbac/...` against a remote kubeconfig — its
> `TestMain` destructively deletes the RESTAction CRD. Use the `evaltest/` sub-package or a kind
> cluster only, with `KUBECONFIG` unset for unit runs.
