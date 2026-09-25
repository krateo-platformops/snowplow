# L1 key multiplicity — root cause, share, and fix shape

Author: arch (cache-architect) · 2026-09-23

## 0. Refs — every file:line below is pinned to these

| what | ref |
|---|---|
| Deployed pod (source of the capture) | `snowplow-77cfcc8b9-h82f9`, `restarts=0`, `started=2026-09-21T23:33:57Z` (`preroll-237/00-pod.txt`) |
| Build in that pod | `snowplow_build_info.version = f47546e363aceb0c2d31a7dd72bbbe43c2476445` (`preroll-237/vars.json`) |
| Worktree I read | `/Users/diegobraga/krateo/snowplow-cache/snowplow/.claude/worktrees/gate-118cv2` @ `4c66c29e6f04650f60918fd038425389ef486474` (branch `feat/237-b-store-divergence-detection`) |
| Staleness check | `git merge-base --is-ancestor f47546e HEAD` → **YES**, and `git diff --stat f47546e HEAD -- <every file cited below>` → **EMPTY**. Every cited line is byte-identical in the deployed binary and in my checkout. |

Files verified identical deployed↔HEAD:
`internal/cache/resolved.go`, `internal/cache/rbac_subgen.go`,
`internal/handlers/dispatchers/helpers.go`, `.../phase1_pip_seed.go`,
`.../resolve_populate.go`, `.../widget_content.go`.

Capture: `preroll-237/apistage.json` (4,823 entries), `preroll-237/vars.json`.

---

## 1. Answer

**The varying component is `ResolvedKeyInputs.RBACSubGen`** — the per-subject RBAC
sub-generation added by #118 (c) / key version v5, folded into `ComputeKey` at
`internal/cache/resolved.go:844-847` (TRACED).

It is written at **exactly one** non-test site — `dispatchCacheLookupKey`,
`internal/handlers/dispatchers/helpers.go:275` (TRACED, enumerated by grep in §3).
Its value is `cache.RBACSubGenForSubject(ui.Username, ui.Groups)`
(`internal/cache/rbac_subgen.go:96-116`): the **sum of the requesting identity's
own User/SA counter plus every presented Group counter**, each incremented by
`BumpSubjectSubGens` (`rbac_subgen.go:74-78`) from the deferred flush
`flushPendingSubGenBumps` (`rbac_subgen_pending.go:124-140`), which runs on every
RBAC snapshot publish (`internal/cache/rbac_snapshot.go:510`).

Mechanism for a fixed `(object, identity)`: the counter is **monotonic and
global-in-time**. It moves whenever *any* binding naming the requester's user or
any of its groups is added/updated/deleted. When it moves, `ComputeKey` mints a
**new key for the same object, the same BindingUID, the same body**. Invalidation
is DELETE-only (`feedback_l1_invalidation_delete_only`), so the previous-subgen
cell is **never removed** — and because `ResolvedEntry.Inputs` carries the *old*
`RBACSubGen`, the refresher re-`Put`s it under its old key forever
(`resolve_populate.go:116` recomputes `ComputeKey(inputs)` from the **stored**
Inputs). The orphan is therefore kept permanently *fresh*, which is why the 86%
sibling comparison survived the ≤60s staleness restriction.

How hot is the bump source: `snowplow_rbac_publish_seq = 12313` over ~34 h uptime
(~6 publishes/min) — `preroll-237/vars.json` (TRACED).

---

## 2. Empirical attribution — how I excluded every other fold input

`ComputeKey`'s fold set (`resolved.go:786-869`): `resolvedKeyVersion`,
`CacheEntryClass`, `Group/Version/Resource/Namespace/Name`, `[BindingUID +
RBACSubGen]`, `PerPage`, `Page`, `Stage`, `Extras`.

### 2.0 Reproduction of the brief's numbers

My pair definition is `(class, group, version, resource, namespace, name, bindingUID)`.
`bindingUID` **is** in the capture (`ResolvedEntryMeta.BindingUID`,
`resolved.go:1337-1341`), so identity is directly observable, not inferred.

| | brief | mine |
|---|---|---|
| resident cells | 4,823 | 4,823 |
| distinct (object, identity) pairs | 2,860 | 2,899 |
| excess cells | 1,963 | 1,924 |
| pairs holding >1 key | 1,126 | 1,108 |

Small deltas = a slightly different pair tuple. The structure is identical; I use
my own numbers below and they are self-consistent.

### 2.1 `Stage` — EXCLUDED, directly observed

`ResolvedEntryMeta.Stage` is emitted with `json:"stage,omitempty"`
(`resolved.go:1309`). Field-coverage count over all 4,823 entries:

```
python3 -c "import json,collections; d=json.load(open('apistage.json'));
k=collections.Counter();
[k.update(e.keys()) for e in d['entries']]; print(k)"
→ no 'stage' key present on ANY of the 4,823 entries
```
Every resident entry has `Stage == ""`. (TRACED)

### 2.2 The class discriminator — EXCLUDES `PerPage`/`Page`/`Extras` as the driver

`ComputeKey` skips the **whole identity block** — `BindingUID` *and* `RBACSubGen` —
for, and only for, `CacheEntryClassWidgetContent` (`resolved.go:828`,
`if in.CacheEntryClass != CacheEntryClassWidgetContent`). `widgetContent` still
folds `PerPage`, `Page` and `Extras` identically to `widgets`
(`dispatchWidgetContentKey`, `helpers.go:186-203`).

| class | folds identity? | cells | distinct pairs | **excess** |
|---|---|---|---|---|
| `widgets` | yes | 4,433 | 2,509 | **1,924 (100% of all excess)** |
| `widgetContent` | **no** | 262 | 262 | **0** |
| `apistage` | yes | 123 | 123 | **0** |
| `raFullList` | yes | 5 | 5 | **0** |

Every excess cell in the store is in the one class whose key folds
`BindingUID + RBACSubGen` *and* whose sibling class with the identity fold removed
has **zero** excess. 1,086 of the 1,924 excess cells sit on coordinates that hold
**exactly one** `widgetContent` cell — same widget, same `PerPage/Page/Extras`
surface, one cell there, up to 14 here.

Caveat I will not hide: `widgetContent` is a smaller corpus (262 vs 2,509
coordinates) because of its own Put declines
(`snowplow_widget_content_skipped_rbac_sensitive_total = 95,938`), so this is
*strong* evidence, not proof on its own. §2.3 and §2.4 close it.

### 2.3 `PerPage` / `Page` — EXCLUDED by uniformity across all 25 plurals

If the SPA's client-side pagination registry (`arch-tables-div`, portal#235 /
frontend#309) were the driver, excess would concentrate in `tables`. Excess per
widget plural, `widgets` class, pair = (resource, ns, name, bindingUID):

```
tables         215 cells   98 pairs  117 excess  (54.4%)   ← only 6.1% of total excess
cards         1010 cells  571 pairs  439 excess  (43.5%)
flexes         998 cells  618 pairs  380 excess  (38.1%)
buttons        406 cells  192 pairs  214 excess  (52.7%)
…
themes          11 cells    7 pairs    4 excess  (36.4%)
menus           11 cells    7 pairs    4 excess  (36.4%)
images          11 cells    7 pairs    4 excess  (36.4%)
layouts         11 cells    7 pairs    4 excess  (36.4%)
selects         11 cells    7 pairs    4 excess  (36.4%)
TOTAL excess 1924 · non-`tables` excess 1807 (93.9%)
```

Excess is **flat at 33–57% across every plural**, including page-chrome widgets
(`themes`, `menus`, `images`, `layouts`) the SPA fetches once with no query
parameters. A per-URL parameter mechanism cannot produce a uniform rate across
widget kinds that are never paginated. (TRACED)

Corollary that also kills the seed-vs-serve `deriveSeedKeyTuple` hypothesis
(`phase1_pip_seed.go:509-538`): the boot seed ran ~2,043 min before the capture,
and **no resident cell has a birth time older than 1,226 min** (§2.4). Every boot
cell has already aged out (`evict_max_age_total = 2125`, `evict_ttl_total = 7262`).
None of the 1,924 excess cells is a seed/serve tuple mismatch. (TRACED)

I also confirmed the widget seed does **not** take a different key path: it calls
the same `dispatchCacheLookupKey` (`phase1_pip_seed.go:1295`) under a cohort
`WithUserInfo` context (`withCohortSeedContext`, `phase1_pip_seed.go:650-660`), so
it stamps a real `RBACSubGen`. The `resolved.go:452-456` note "the SEED does NOT
stamp it" does **not** apply to the `widgets` class.

### 2.4 The positive signature — discrete, per-binding, whole-cohort re-fill waves

Cell birth time is `now − lifetimeSeconds` (`LifetimeSeconds` = now − `BornAt`,
*not* reset by a refresh — `resolved.go:1311-1328`). Bucketed per minute over the
4,433 `widgets` cells: **92.1% fall in 25 one-minute buckets**, which group into
~9 epochs (≈25, 76, 107, 185, 598, 793, 1076, 1153, 1226 min ago).

Broken out per `bindingUID`:

```
bindingUID                                     cells  birth waves (minutes ago, count)
C:1dd07773-9981-4cf8-a6dc-a3fc5c18147f          1671  25:370  76:374  107:119 185:370 793:34 1076:87 1153:73 (+244)
C:9edd1d03-7f6c-49b3-87f7-aa3bab8950ae           805  1076:375  1225:430
R:krateo-system/d3d9aaca-ce6b-421a-b27b-…        736  76:368    1076:368
C:84c69e81-f2e2-4092-a407-50f806b00279           368  598:359   1076:7
C:a27ef2cd-e843-40ca-ae43-9ec9edaf0854           368  598:359   1076:7
C:69730c1d-e69d-4731-9add-68f6c1a55778           368  598:359   1076:7
R:krateo-system/dd056ebf-0273-4c4a-877e-…        117  598:114   1076:2
```

`R:…d3d9aaca` holds **736 cells = 368 + 368**: its *entire* 368-coordinate working
set materialised **twice**, wholesale, at two discrete instants 1,000 minutes apart.
`C:9edd1d03` is the same shape (375 + 430). Three different ClusterRoleBindings
(`84c69e81`, `a27ef2cd`, `69730c1d`) show a byte-identical wave profile.

A *whole* identity's key space re-minting at an instant, with the body unchanged,
is the signature of a key component that is a property of the **requester** and
changes **discretely**. Of the fold set, after §2.1–§2.3, only `RBACSubGen`
qualifies. `BindingUID` is equal by construction within every pair. (TRACED)

### 2.5 Two failed checks I am reporting because they were in my working set

- I tried to use `itemsCount` to separate "list drift" from "identity-dependent
  body". **It is vacuous here**: `itemsCount == 0` for all 4,433 `widgets` cells
  (the field is only populated for parsed-list apistage entries —
  `resolved.go:1330-1332`). Do not cite it.
- Byte **length** equality is not body equality. Every "identical" count below is
  therefore an **upper** bound on waste and every "distinct" count a **lower**
  bound on genuine separation. `/debug/apistage?key_hash=…` returns `bodySha256`
  per key (`resolved.go:1344-1357`) and would tighten both — that is in the
  falsifier (§5).

---

## 3. The enumeration, with the command

```
$ grep -rn "RBACSubGen:" --include='*.go' . | grep -v _test.go
go/snowplow/internal/handlers/dispatchers/helpers.go:275:		RBACSubGen: cache.RBACSubGenForSubject(ui.Username, ui.Groups),
```
**One** production write site. Every other `ResolvedKeyInputs` construction leaves
it zero:

```
$ grep -rn "ResolvedKeyInputs{" --include='*.go' . | grep -v _test.go
internal/cache/ra_full_list_slice.go:88
internal/resolvers/widgets/apiref/ra_full_list.go:156, :168
internal/handlers/dispatchers/phase1_content_prewarm.go:433
internal/handlers/dispatchers/widget_content.go:87
internal/handlers/dispatchers/helpers.go:193          (widgetContent — identity-free)
internal/handlers/dispatchers/helpers.go:259          (dispatchCacheLookupKey — the one above)
internal/resolvers/restactions/api/apistage.go:66

$ grep -rn "ComputeKey(" --include='*.go' . | grep -v _test.go
internal/cache/resolved.go:786 (def)
internal/resolvers/restactions/api/cluster_list.go:281
internal/resolvers/restactions/api/cluster_list_prewarm.go:292
internal/resolvers/restactions/api/apistage.go:488
internal/resolvers/widgets/apiref/ra_full_list.go:172
internal/handlers/dispatchers/phase1_content_prewarm.go:433
internal/handlers/dispatchers/widget_content.go:103
internal/handlers/dispatchers/resolve_populate.go:116
internal/handlers/dispatchers/helpers.go:205, :289

$ grep -rn "dispatchCacheLookupKey(" --include='*.go' . | grep -v _test.go
dispatchers/restactions.go:192            (customer serve, class restactions)
dispatchers/phase1_pip_seed.go:899        (boot seed, restactions)
dispatchers/phase1_pip_seed.go:1295       (boot seed, widgets)
dispatchers/widgets.go:221                (customer serve, widgets)
dispatchers/refresh_subscription.go:242, :263
dispatchers/helpers.go:238 (def)
```
Only `RBACSubGen` is read by `ComputeKey` and written nowhere else — removing the
fold cannot break another reader. (TRACED)

---

## 4. Share of the excess, and the 86/14 question

Per `(resource, ns, name, bindingUID)` pair, `dup = cells − distinct byte lengths`
(subgen-attributable), `legit = distinct byte lengths − 1` (must stay separated).

| population | cells | pairs | excess | byte-identical dup | distinct-body |
|---|---|---|---|---|---|
| all resident `widgets` | 4,433 | 2,509 | 1,924 | **1,377 (71.6%)** | 547 (28.4%) |
| concurrently fresh (`ageSeconds ≤ 300`) | 926 | 435 | **491** | **423 (86.2%)** | 68 (13.8%) |

The fresh-restricted row **reproduces the brief's 86/14 exactly** from a different
method (byte-length classes per pair, not pairwise sibling comparison), which is a
useful independent confirmation. Among fresh pairs, **405 of 435 (93.1%) hold
exactly one distinct body**, replicated 2–14×.

**So: is 86/14 one mechanism or several? It is ONE mechanism plus a much smaller
second one, and they COMPOSE rather than split the excess.**

- `RBACSubGen` replicates *each distinct body variant* N times (N = the number of
  sub-gen epochs the coordinate has survived, 2–14 here).
- `PerPage/Page` (and, for a few coordinates, `Extras`) is what creates the *two*
  distinct body variants in the first place — the portal#235 / frontend#309 story,
  confined to `tables` and a handful of `pageheaders`.

The capture shows the composition directly. `tables/incident-auditrecords`,
single `bindingUID` `C:1dd07773…`:

```
one cell   life= 8827  3908 bytes   ← the ?page=1&perPage=50 variant
nine cells life= 6337 … 69215  2251 bytes each  ← the (-1,-1) variant, ×9 sub-gen epochs
```

One legitimate key split; nine illegitimate copies of one side of it. That is why
the pairwise sibling method reads "14% differ" — it is counting the cross-variant
*pairs*, which the ×9 replication inflates. **The 14% is 1 extra cell per affected
coordinate, not 14% of the 1,963 waste.**

**Bound on the fix's yield:** removing `RBACSubGen` from the key collapses
**1,377 of the 1,924 excess cells (71.6%) as an upper bound**, and at the
steady-state (fresh) rate **86.2%** of new excess. The 547-cell residue over the
full resident set is inflated by staleness — a never-re-read old-epoch cell holds a
stale body variant of the same lineage — so the true collapse sits between those
two figures. I am deliberately not reporting a single number: `bodySha256` is what
settles it (§5).

---

## 5. Prior-art check, and answers to the brief's two routes

### Prior art (k8s / client-go)
- client-go has no resolved-output cache and no RBAC-keyed cache; there is nothing
  to reuse for the key itself.
- The relevant prior art is the **apiserver's own RBAC authorizer**
  (`k8s.io/apiserver/plugin/pkg/authorizer/rbac`): it does **not** version-stamp a
  cache key with an RBAC generation. It computes the decision from a **live
  informer-backed lister** on every call, and where it *does* cache (the webhook
  authorizer, `authorizerfactory`) it uses `cache.NewLRUExpireCache` keyed on the
  `SubjectAccessReview` spec with a **TTL**, never a generation folded into the key.
- The general k8s idiom for "this cached thing may be stale w.r.t. a generation" is
  **stamp the observed generation on the record and validate it on read**
  (`status.observedGeneration`; `Reflector`/`DeltaFIFO` resourceVersion checks) —
  *not* to put the generation in the identity of the record. Folding a monotonic
  counter into a cache key is a known anti-pattern precisely because it turns an
  invalidation into an allocation.

That idiom is the fix shape.

### Route 2 — can `dispatch.cache_key.computed` attribute a duplicate today?

Emit site: `helpers.go:744-758` (TRACED). Fields: `site`, `key_hash`,
`binding_uid`, `username`, `groups`, `handler_kind`, `gvr`, `namespace`, `name`,
`per_page`, `page`, **`extras_len`**.

- `per_page` / `page`: **present** → attributable today, no new code.
- `binding_uid`, `username`, `groups`: **present**.
- `Extras`: only its **length**. A same-cardinality value change is invisible.
- **`RBACSubGen`: absent entirely.**

So today the line proves *"the difference is `RBACSubGen` or an extras value"* by
elimination, but cannot name it. **Minimal addition (design note, 2 lines, at
`helpers.go:757`):**

```go
slog.Uint64("rbac_subgen", func() uint64 { if inputs != nil { return inputs.RBACSubGen }; return 0 }()),
slog.String("extras_hash", cache.HashExtras(extras)),
```

`cache.HashExtras` **already exists** (`resolved.go:868-880`) and routes through the
*same* `canonicaliseExtras` that `ComputeKey` folds — so it is the exact pre-hash
extras identity, with no new derivation to drift and no content in the log (it is a
hash; `feedback_debug_surface_never_dumps_per_identity_cache_bodies` is satisfied).
With those two fields the line carries **every** `ComputeKey` input except the
class-constant `resolvedKeyVersion`, and a duplicate is attributable by a
`join`-and-`diff` on the log alone.

I recommend adding them regardless of the fix — they are the falsifier instrument.

---

## 6. Fix shape

### 6.1 What makes the symptom disappear

**Stop folding `RBACSubGen` into the key; carry it on the entry and validate it on
read.** `RBACSubGen` becomes exactly what `HasUAF` already is
(`resolved.go:437-442`: *"EXCLUDED FROM COMPUTEKEY … bookkeeping carried on Inputs,
NOT key material"*).

- `internal/cache/resolved.go:844-847` — delete the 4-line subgen fold (and its
  comment block, 830-843). `BindingUID` + its `0xff` terminator stay.
- `internal/cache/resolved.go:518` — bump `resolvedKeyVersion` `"v6"` → `"v7"`
  (mandatory: the key shape changes; a v6 cell must never serve as a v7 hit).
- `internal/handlers/dispatchers/helpers.go:275` — **keep** the assignment. The
  value now rides on `ResolvedEntry.Inputs` as a stamp.
- Serve path, at the L1 HIT branch (`widgets.go:239`, `restactions.go:~200`): after
  `cacheHandle.Get(cacheKey)` returns a hit, compare the entry's stamp against a
  freshly computed one; on mismatch treat it as a **MISS** and fall through to the
  existing re-resolve + `Put`, which overwrites **the same key**.

Net: an RBAC change now **invalidates a cell in place** instead of **allocating a
new one and orphaning the old**. One key per `(object, binding, perPage, page,
extras)`. The `RBACSubGen` cell-count multiplier goes to 1.

LOC bound: **~25 production lines** (4 deleted in `ComputeKey`, 1 version constant,
~8 for the revalidation helper, 2 call sites × ~4 lines, 2 diagnostic fields).

### 6.2 The 14% is preserved, by construction

`PerPage`, `Page` and `Extras` are **untouched** — they stay folded at
`resolved.go:849-868`. Every pair that holds genuinely different bodies in the
capture is separated by one of those three (the `tables/incident-auditrecords`
2251-vs-3908 pagination split; the `pageheaders/incident-detail-page-header`
2156/2191/2198 extras split). Nothing that holds a different body is merged. This
fix does **not** touch the wall that killed normalisation on #231.

### 6.3 THE STRATEGIC CHOICE — what the stamp is compared against

This is the one decision I will not make unilaterally. The naive version is wrong
and I want that on the record:

**Option A — compare against the REQUESTER's live sub-gen. ✗ DO NOT SHIP.**
`RBACSubGenForSubject(ui.Username, ui.Groups)` is a **per-user** value, but the cell
is **per-binding**. Two users sharing a `BindingUID` with different sums (5 and 7)
**ping-pong**: A hits, B mismatches and re-Puts 7, A mismatches and re-Puts 5, …
L1 hit rate for every shared cohort cell collapses toward 0. This trades a space
defect for a far worse latency defect and breaks
`feedback_l1_hit_invariant_is_100_percent`.

**Option B — compare against the CELL's REPRESENTATIVE sub-gen. ✓ RECOMMENDED.**
`ResolvedEntry.Inputs` already carries `RepresentativeUsername` /
`RepresentativeGroups` (`helpers.go:276-288` — the first writer's identity, already
used by the refresher to drive the re-resolve). Revalidation recomputes
`RBACSubGenForSubject(entry.Inputs.RepresentativeUsername,
entry.Inputs.RepresentativeGroups)` and compares it to `entry.Inputs.RBACSubGen`.
This is a **property of the cell**, identical for every reader → **no ping-pong**;
and it rotates on exactly the event #118 (c) targeted, with the same herd-
proportional blast radius (`rbac_subgen.go:11-21`). The refresher gets the same
check for free at `resolve_populate.go:116` (re-stamp on every re-Put).

**Why Option B does not reopen #118.** The residual worry is user B, sharing the
binding, whose *own* RBAC moved while the representative's did not. That case is
already closed by two existing mechanisms, both TRACED:

1. If B's change altered **which binding authorises B's GET**, `BindingUID` itself
   changes → different key → cold miss → correct. `RBACSubGen` was never needed
   for this limb.
2. If B's change altered a **per-object `userAccessFilter` narrowing**, the cell
   cannot exist: `declineWidgetUAFPut` gates **all three** `widgets` Put sites —
   customer `widgets.go:408` (sink installed `widgets.go:327`), boot seed
   `phase1_pip_seed.go:1461`, refresher `resolve_populate.go:385` — and the
   sibling `declineUAFPut` gates the `restactions` sites (`restactions.go:399`,
   `phase1_pip_seed.go:1040`, `resolve_populate.go:387`). `widgets.go:519` states
   the invariant outright: *"A refilter-touched envelope can no longer REACH this
   Put … every cell written here is refilter-free."* The gate is firing live:
   `snowplow_widgets_uaf_put_declined_total = 266,393`,
   `snowplow_restactions_uaf_put_declined_total = 54,276` (`vars.json`, TRACED).

So for every cell that can actually exist in L1 today, the body depends on identity
**only** through `BindingUID`. `RBACSubGen` is guarding a body shape the UAF
Put-decline has already made unreachable. It is pure key-space fan-out.

**This is exactly why I am surfacing it rather than deciding it.** (2) is a *1.12.3
mitigation*, and `uaf_shortttl.go:273-275` says the mitigation is removed in 1.13.0
when the UAF scope is folded into the key (v7). If 1.13.0 lands the UAF-scope
digest, Option B's safety argument for limb (2) must be **re-derived against that
digest** — the digest, not `RBACSubGen`, becomes the separator. That coupling is
the PM/TL call: *ship Option B now and re-verify at 1.13.0*, or *hold key
multiplicity until 1.13.0 and fix both in one key-version bump*.

**My recommendation: ship Option B now, at v7, and make the 1.13.0 UAF-digest
design own the re-derivation.** 40% of L1 and ~40% of the dirty-mark fan-out is
too large a standing tax to hold behind an unscheduled ship, the change is ~25
lines with a clean version-bump break, and the UAF limb is independently closed by
a Put-decline that 266K live events prove is working.

### 6.4 BLOCKING OBLIGATION ON 1.13.0 — carried forward, named owner

*(TL ruling 3 condition, 2026-09-23. This is an obligation, not a note. It must be
transcribed verbatim into the v7 implementation as a code comment at
`resolved.go` `resolvedKeyVersion` and into the 1.13.0 UAF-scope design brief.)*

> **v7 removed `RBACSubGen` from `ComputeKey`. Its safety argument (§6.3 limb 2)
> rests on the 1.12.3 A-1 UAF Put-decline gate — `declineWidgetUAFPut` /
> `declineUAFPut` — which keeps every refilter-touched body OUT of L1, so a
> per-binding cell can never hold a per-user narrowing.
> `uaf_shortttl.go:273-275` schedules those helpers and their call sites for
> DELETION in 1.13.0, when the UAF-scope digest is folded into the key. The moment
> that digest lands, the separator changes from "the cell cannot exist" to "the
> digest separates it", and §6.3 limb 2 is NO LONGER ESTABLISHED.**
>
> **OWNER: the 1.13.0 UAF-scope-digest design author.**
> **OBLIGATION: re-derive §6.3 limb 2 against the digest before deleting a single
> decline site. Specifically — demonstrate that the digest separates two co-bound
> users with different per-object narrowings, with a golden that models both
> sources independently and asserts per-item bytes. Deleting the decline gate
> without that derivation re-opens #118 (c) silently, because v7 removed the
> component that would otherwise have separated them.**
> **GATE: the 1.13.0 PM gate MUST refuse a brief that does not carry this
> re-derivation.**

Rationale for stating it this loudly: a safety argument that depends on a
mitigation scheduled for removal is precisely the thing that gets inherited as
settled. The v7 change is what makes the decline gate load-bearing for a *second*
property it was not originally shipped to hold.

**Option C — fan refresh out across all keys.** Explicitly rejected, per the brief
and independently: it keeps 40% of L1 as duplicates, pays ~40% of the dirty-mark
fan-out to keep waste fresh, and does nothing for the `max_entries`/`max_bytes`
headroom. I do not find an argument for it. The only thing it has over B is that it
needs no key-version bump, which is not a benefit.

---

## 7. Falsifier — diffs PRE-HASH INPUTS through the real derivation on both sides

Per `feedback_key_parity_golden_real_inputs_prehash_diff` and
`feedback_consultation_mutation_is_not_key_correctness`: a hash comparison proves
inequality and names nothing. All three arms below diff **inputs**, not hashes.

**F1 — RED-before / GREEN-after, on the live boundary (the load-bearing arm).**
Add the two `dispatch.cache_key.computed` fields from §5 **first**, ship them
alone, and capture ≥2 real `/call` invocations for one widget spanning a real
sub-gen bump (`feedback_falsifier_must_drive_real_boundary_not_install_crossed_state`).
- **RED (pre-fix, must be observed before any fix lands):** two lines with
  identical `binding_uid`, `per_page`, `page`, `extras_hash` and **different**
  `rbac_subgen` and **different** `key_hash`. That is the pre-hash input diff
  naming the component. If this is not observed, **this whole analysis is wrong**
  and the fix must not ship.
- **GREEN (post-fix):** the same two lines — identical `binding_uid`, `per_page`,
  `page`, `extras_hash`, **different** `rbac_subgen` — now carry the **same**
  `key_hash`.

**F2 — the store-level outcome, production workload shape
(`feedback_falsifier_must_exercise_production_workload_shape`).** Re-run the
`/debug/apistage` capture on the fixed build after an equivalent soak (≥2 sub-gen
epochs — confirm via ≥2 `flushPendingSubGenBumps` effects, i.e.
`snowplow_rbac_publish_seq` advanced and a `rbac_subgen` change observed in F1).
Compute, with the §4 script:
- `excess / cells` for class `widgets` drops from **43.4% (1,924/4,433)** toward the
  `PerPage/Page/Extras` floor. Acceptance: **fresh-population byte-identical
  duplicates = 0** (today 423/491 = 86.2%).
- `widgetContent`, `apistage`, `raFullList` excess stays **0**.

**F3 — the 14% must NOT merge (the separation arm).** Using
`/debug/apistage?key_hash=…` → `bodySha256` (`resolved.go:1344-1357`), enumerate
every post-fix `(resource, ns, name, bindingUID)` pair that still holds >1 key and
assert **every one holds a DISTINCT `bodySha256`**. Any pair holding >1 key with the
*same* `bodySha256` is residual waste; any *merged* key whose body should have
differed is a **leak** and blocks the ship. Concretely: `tables/incident-auditrecords`
under `C:1dd07773…` must still hold **exactly 2** keys post-fix (the 2251-byte and
3908-byte variants), **not 1 and not 10**. Run this same query pre-fix to establish
the baseline set.

**F4 — the ping-pong guard (Option B specific).** `snowplow_dispatch_l1_lookups`
`hit_total`/`miss_total` per class, before vs after, on a cohort with ≥2 distinct
users sharing one `BindingUID`. `widgets` hit ratio must not regress. A regression
here is the Option-A failure mode leaking into B and means the stamp is being
compared against the requester somewhere.

**F5 — key-version break.** `resolvedKeyVersion` v6→v7 must force a clean rolling
break (the v3→v4→v5→v6 precedent, `resolved.go:501-518`): post-restart,
`seed_hit_total` on a v6 residue is 0. Plus the mandatory
`TestBootSeedCoverage_*` seed falsifier — this is an L1 key change
(`feedback_seed_falsifier_required_on_l1_key_changes`).

---

## 8. What would make this analysis wrong

The single biggest risk: **§2.4's wave signature is consistent with, but does not by
itself prove, `RBACSubGen`.** Any per-identity component that changes discretely and
wholesale would produce the same picture. I excluded `Stage` by direct observation
(§2.1), `PerPage/Page` by cross-plural uniformity (§2.3), and `Extras` by the
`widgetContent` class discriminator (§2.2) — but `Extras` is the weakest of the
three exclusions, because `widgetContent`'s smaller corpus leaves room for a
declared-`keyExtras` value that rotates on the same cadence.

**F1's RED arm is precisely the instrument that closes it**, which is why it must be
observed *before* the fix lands and why I am recommending the two diagnostic fields
ship first, on their own. Do not skip to the fix.

---

## 9. Does `RBACSubGen` explain the #237 STALENESS symptom? — **NO. KILLED.**

TL's chain: per-user sum in a per-binding key → N parallel cells per (object,
binding) → *"each ageing independently, refreshed only when their own dep edge
fires"* → a user is served their own subject's unrefreshed cell.

**The premise is right and I confirm it.** `helpers.go:275` does stamp a *per-user*
sum into a key whose other identity component is the *per-binding* `BindingUID`, and
that asymmetry is the current serve path, not a hypothetical. Two co-bound users do
derive different keys for the same object. That is exactly why Option A is dead.

**The load-bearing link — "refreshed only when their own dep edge fires", i.e.
duplicates are orphaned from the refresh path — is FALSE.** A cell that has never
been refreshed since its first Put has `ageSeconds == lifetimeSeconds` (`CreatedAt`
is reset by every refresh, `BornAt` is not — `resolved.go:1311-1328`). That makes
the claim directly measurable:

```
widgets cells NEVER refreshed since first Put:            143 of 4,433  (3.2%)

Among multi-cell (coord, bindingUID) pairs:
  ALL cells refreshed at least once                     1,105 of 1,108 (99.7%)
  MIXED (some refreshed, some never)                        3 of 1,108  (0.3%)
```

**99.7% of duplicate cells are on the refresh path.** `recordWidgetDeps`
(`deps_extract.go:96-142`) records edges **per L1 key** at every Put, so each
sub-gen sibling gets its own full edge set. The dirty-mark fans out to all of them
— which is the ~40% amplification tax, and is also why your own 60s-restricted
sibling comparison found them co-fresh. Multiplicity costs refresh work; it does
not withhold it. (TRACED)

The refresher is also not saturated, so there is no "fan-out overloads the
refresher → it drops keys" back-door either: `queue_depth = 0`,
`capped_total = 0`, `dropped_total = 89` against `completed_total = 36,599,293`
(`vars.json`, TRACED). At 50K this is worth re-checking; today it is not the
mechanism.

### What actually produces seven-fresh-three-stale under one coordinate

**The split is by BODY VARIANT, not by subject** — and the same `bindingUID`
appears on both sides of it. `portal-builder-page-header`, full cell list:

```
bindingUID                     life    age   bytes
R:krateo-system/d3d9aaca…     64453    126    2454   fresh
C:9edd1d03…                   73592    126    2456   fresh
C:9edd1d03…                   64726    126    2456   fresh
C:a27ef2cd…                   35790    126    2456   fresh
C:69730c1d…                   35790    126    2456   fresh
C:84c69e81…                   35790    126    2456   fresh
C:1dd07773…                   11169    126    2456   fresh
C:1dd07773…                    1568   1126    1568   stale ← same binding as a fresh cell
C:1dd07773…                    4419   3534    1568   stale ← same binding as a fresh cell
R:krateo-system/d3d9aaca…      4419   1126    1568   stale ← same binding as a fresh cell
```

Every stale cell has `age < lifetime` — **all three have been refreshed.** None is
orphaned. And `C:1dd07773` holds a fresh cell *and* two stale ones, so "that
subject's cell went stale while other subjects' stayed fresh" does not describe
this coordinate. Across the whole store, of the 61 coordinates holding both a fresh
and a stale cell:

```
separated ONLY by body variant (bindingUID overlaps fresh & stale):  22
separated ONLY by bindingUID   (body variant overlaps):               0
both or neither separate (ambiguous):                                39
```

**Zero** coordinates are separated only by subject. (TRACED)

### Two leads of my own that died in this check — reporting both

1. **"Stale cells are systematically smaller, so a degraded/truncated resolve emits
   fewer `resourcesRefs` → fewer dep edges (`deps_extract.go:124-142`) → fewer
   dirty-marks → stays degraded."** It looked strong: stale median 1,760 bytes vs
   fresh median 2,169. **KILLED** — among cells that *have* been refreshed, median
   `ageSeconds` is FLAT across every body-size quartile:
   `496–1584 → 1300s · 1584–1916 → 1298s · 1919–2478 → 1292s · 2478–93934 → 1286s`.
   Refresh cadence is not size-dependent. The apparent correlation comes entirely
   from the never-refreshed population contaminating the stale bucket.
2. **"The refresher rate floor sets a global staleness plateau"** (59.1% of widgets
   cells sit in the 901–1800s age band). **KILLED** —
   `defaultRefresherRateFloorSeconds = 2` (`refresher.go:107`), two seconds, and the
   floored branch is explicitly lossless/deferred (`refresher.go:812-835`). A 2s
   floor cannot produce a ~1,290s median. I have no traced explanation for the
   plateau and will not invent one.

> **CORRECTION (2026-09-23, same day).** An earlier revision of this section said
> the 765 past-TTL cells "all have `ageSeconds == lifetimeSeconds` exactly (never
> refreshed)". **That was wrong.** I read it off equal min/max *ranges* for the two
> fields across the set and did not check it per cell. The per-cell cross-tab:
>
> ```
> never-refreshed=False  past-TTL=False   3654
> never-refreshed=False  past-TTL=True     636   ← refreshed, and still past TTL
> never-refreshed=True   past-TTL=False     14
> never-refreshed=True   past-TTL=True     129
> ```
>
> **636 of the 765 past-TTL cells HAVE been refreshed.** Past-TTL and
> never-refreshed are overlapping, not identical, populations. The §9 conclusion is
> unaffected — it rests on the 99.7% all-refreshed figure for multi-cell pairs and
> the 22-vs-0 variant-vs-subject split, neither of which this touches — but the
> sentence was a claim about a set made from a truncated look, and it is retracted.

### 9.1 The refresh-decline freeze — TL hypothesis, TRACED, and BOUNDED at 143 cells

TL's proposal: *a refresh that runs but whose Put is declined leaves the old body
resident — refreshed by my measure, stale to the user.*

**The mechanism is real and I confirm it at file:line.** `resolve_populate.go:381-393`:
on a UAF decline the refresher keeps the existing entry and declines the write —
the comment says it outright, *"keep whatever entry exists, decline the write, TTL
is the outer net"* — and then calls `cache.NoteRefreshDecline(key, "uaf", true)`.
The marker clears only on a real Put or an eviction (`clearRefreshSuppression`,
`refresher_terminal.go:229-233`). `REFRESH_SUPPRESS_AFTER_DECLINES: "3"` is set
(`helm/snowplow/values.yaml:405`; mechanism `refresher_terminal.go:67,191`). Live:
`suppressed_keys = 47`, `suppressed_set_total = 1547`, `suppressed_skips_total = 61785`.

**Two corrections to the hypothesis as stated, both load-bearing:**

1. **It is worse than described.** That `true` is `permanent` — `NoteRefreshDecline`
   (`refresher_terminal.go:195-215`) marks the key on the **FIRST** decline for a
   permanent reason. `REFRESH_SUPPRESS_AFTER_DECLINES=3` does not gate the UAF path
   at all. One decline freezes the cell.
2. **It does NOT slip past my §9 check.** `CreatedAt` is stamped *only* by a
   successful Put — `refresher.go:828-831` states this explicitly, citing
   `resolved.go:767-768`. A declined re-Put leaves `CreatedAt` old, so `ageSeconds`
   keeps growing while `lifetimeSeconds` does too: a decline-frozen cell reads as
   **`age == lifetime`, i.e. NEVER REFRESHED**. It lands in the population §9
   measured and set aside, not in the "refreshed" one.

**That bounds it.** The never-refreshed population is the ceiling for this class:

```
NEVER-REFRESHED widgets cells: 143 of 4,433 (3.2%)
  median age 68,801s (19 hours) · range 125..121,750s
  by resource: pageheaders 62, flexes 46, rows 26, tables 6, tags 2, cards 1
  129 of 143 are also past TTL; only 13 of 143 share a coordinate with a refreshed cell
```

So the decline-freeze can account for **at most 143 cells (3.2%)** — consistent in
order of magnitude with the 47 live suppressions. It is a **real defect and worth
its own fix**: 19-hour-median frozen cells, `maxEntryAge` enforced on read
(`resolved.go:1311-1328`) so a cell nobody reads never even gets evicted. But it
**cannot** be the 59.1% of widgets cells sitting in the 901–1800s age band — those
are being refreshed.

**The uncomfortable implication is correct.** The 1.12.3 A-1 mitigation buys
cross-tenant correctness by never caching what the key cannot separate, and pays
for it with unbounded staleness on the declined cell. Mitigation and defect are the
same mechanism. This tightens §6.4: the 1.13.0 UAF-digest work does not merely owe a
re-derived safety argument — if the digest makes these cells separable, the decline
disappears and this staleness class disappears with it; if it does not, the class is
entrenched. **§6.4's owner owes BOTH answers.**

### Bottom line

**`RBACSubGen` explains the 40% waste. It does not explain the #237 staleness, and
I could not substitute another traced cause from this capture.** The capture holds
`ageSeconds`/`lifetimeSeconds` — two points of refresh history per cell — which is
enough to *kill* the orphaning hypothesis but not enough to *build* a staleness
root cause. That needs per-key refresh history, which is a different instrument
(the refresher's own emit sites joined on `key_hash`, alongside the F1 fields).
**#237's staleness should be re-dispatched as its own trace, not closed by this
one.** I would rather hand that back than let a 40%-waste finding be inherited as
a staleness fix it does not support.

---

## 10. #237 staleness trace — Q1/Q2/Q3, and why the question cannot be answered from L1

Same refs as §0. Deployed `f47546e`; every file below verified identical
deployed↔HEAD **except** where §10.4 says otherwise — that exception is the finding.

### 10.1 Q1 — does `CreatedAt` slide on a declined Put? **NO. TRACED IN CODE.**

I previously cited a *comment* (`refresher.go:826-829`) for this. The TL is right
that it is load-bearing for my own kill, so here is the code.

`CreatedAt` is stamped at `resolved.go:1019-1021`, **inside the body of
`func (c *ResolvedCacheStore) Put` (`resolved.go:1015`)**:

```go
func (c *ResolvedCacheStore) Put(key string, entry *ResolvedEntry) {
	if c == nil || entry == nil { return }
	if entry.CreatedAt.IsZero() { entry.CreatedAt = time.Now() }
	if entry.BornAt.IsZero()   { entry.BornAt = entry.CreatedAt }
```

It is reachable **only by invoking `Put`**. A declined re-Put returns before
calling it (`resolve_populate.go:381-393`), so `CreatedAt` cannot slide.
`BornAt` is separately *inherited* from the prior entry on a replace
(`resolved.go:1086-1087`), so `lifetime` survives a re-Put while `age` resets.

Enumerated rather than taken from a named set:
```
$ grep -rn "CreatedAt" --include='*.go' go/snowplow/internal/ | grep -v _test.go
```
→ exactly TWO assignment sites, `resolved.go:1020` and `resolved.go:1026`
(the latter assigning `BornAt`). Every other hit is a read or a comment.

**`age < lifetime` therefore does discriminate "refresh ran and the Put
succeeded" from "refresh ran and was refused". §9's kill stands unqualified.**

### 10.2 Q2 — what suppression does, and at what granularity

- **Granularity: per L1 KEY**, not per coordinate. `refreshSuppressed sync.Map`
  keyed by `l1Key` (`refresher_terminal.go:178-179`). (TRACED)
- **Behaviour: skip WITH a counter; it does NOT stop scheduling.**
  `refresher.go:799-810` — on dequeue of a suppressed key: `q.Forget(key)`,
  `refreshSuppressedSkipsTotal.Add(1)`, consume and discard the trigger GVR,
  `return true`. Future dirty-marks still enqueue the key; each is dequeued and
  skipped. That is why `suppressed_skips_total = 61,785` vastly exceeds
  `suppressed_keys = 47`. (TRACED)
- **Clearing:** `clearRefreshSuppression` (`refresher_terminal.go:229-233`) on
  every real Put and every eviction. A customer `/call` that re-Puts the cell
  unfreezes it.
- **The UAF path does not use the threshold at all.**
  `resolve_populate.go:390` passes `permanent=true`, and
  `NoteRefreshDecline` (`refresher_terminal.go:195-215`) marks immediately on a
  permanent reason. `REFRESH_SUPPRESS_AFTER_DECLINES: "3"`
  (`helm/snowplow/values.yaml:405`) gates only the non-permanent reasons.

### 10.3 Q3 — does the decline rate have the right shape? **NO.**

It has the wrong shape in two independent ways.

**Wrong magnitude.** The 266,393 `declineWidgetUAFPut` calls are dominated by the
**customer serve** site (`widgets.go:408`), which fires on *every* `/call` for a
UAF-backed widget — a per-request counter, not a per-cell one. The per-cell figure
is `suppressed_set_total = 1547` (distinct keys ever frozen), against 22
variant-split coordinates.

**Wrong signature, and this is decisive.** A decline-frozen cell reads
`age == lifetime` (§10.1). At the 22 coordinates split by body variant:

```
stale cells at those coordinates:                              65
  age == lifetime (never refreshed / decline-frozen):           1
  age <  lifetime (refresh RAN and the Put SUCCEEDED):         64
```

**64 of 65.** The anomaly is not declined Puts. The refresh ran, the Put
succeeded, and the cell still holds bytes that differ from its fresh sibling.
The decline-freeze survives as its own bounded defect (§9.1, ≤143 cells) but it
does **not** explain what the TL asked about. (TRACED)

### 10.4 Why the question cannot be answered from L1 at all — and what the floor really is

**I have no evidence that any cell holds a body older than its object.** That is
not a gap in the analysis; the L1 vantage is structurally blind to it.
`ageSeconds` is *time since Put*, not *body-vs-object divergence*. If the L3
informer store holds a stale object, a **successful** refresh reads the stale
store, resolves stale bytes, and Puts them with a fresh `CreatedAt`: `age` resets,
`lifetime` grows, and every L1 counter reads healthy **with the defect active**.
L1 is faithfully caching what L3 handed it.

That is the live #237 candidate, and this repository already says so. From
`internal/cache/store_verify_stats.go:14-40` (HEAD):

> *"B's oracle is THE SNAPSHOT WE JUST RECEIVED, not the apiserver … if the
> incoming snapshot is itself a stale watch-cache read … the comparison finds
> equality and every divergence counter reads 0 WITH THE DEFECT ACTIVE … Root
> cause on #237 is still OPEN and the stale-cacher candidate is live."*

And `servable.go:589-592`: *"divergent:0 was read as 'the store is fine' when it
meant 'nothing compared the store to anything'."*

**Which is exactly the trap this capture sets.** `reconcile.json` reports
`divergent: 0, probed: 564` and `snowplow_deps.reconcile_divergence_total = 4`
over 1,168,628 probes. **Do not read those as health.** They are the dep-tracker
reconcile (L1 key ↔ dep-edge existence), not a store-vs-authority comparison —
and on this pod the store comparison does not exist at all:

```
$ git cat-file -e f47546e:go/snowplow/internal/cache/store_verify_stats.go
  → ABSENT  (present on disk at HEAD, not in the deployed commit)
$ git show f47546e:go/snowplow/internal/cache/servable.go | grep -c lastVerifiedAgeSeconds
  → 0        (HEAD: 1)
```

Confirmed empirically in the capture: `servable.json` carries
`gvr, hasSynced, watchBroken, confirmed, servable, indexerCount,
lastSyncResourceVersion, lastEventAgeSeconds` and **none** of
`lastVerifiedAgeSeconds / lastVerifiedObjects / divergentSinceBoot /
verificationDecorated / repairSuppressed` — **0 of 209 GVRs**. (TRACED)

**So: deliverable B — the instrument that would begin to answer this — is already
built, sitting on this branch (`21519d0`, `3f25429`, `4c66c29`), and is NOT in the
running binary.**

### 10.5 I retract my previous answer on the minimum instrument

Last message I said the floor was *"per-key refresh history joined on
`key_hash`"*. **That is wrong.** Per-key refresh history tells me *when* each cell
was Put — which §10.1 shows I can already derive from `age`/`lifetime` — and it is
blind to whether the bytes were *correct*. It would have cost a build and answered
nothing.

The floor is an **object-level oracle**, in this order:

1. **Ship deliverable B** (already written). It gives per-GVR
   `lastVerifiedAgeSeconds`, `divergentSinceBoot`, `verificationDecorated`,
   `repairSuppressed`, and the four `store_divergent_*` classes. Cheapest possible
   next step: the code exists and is unshipped. **Carry B's own qualifier
   verbatim** — it closes every loss class EXCEPT a stale cacher, and that
   candidate is live, so B may detect nothing and that will not be proof of health.
2. **Only if B reads clean:** the remaining candidate is a stale cacher, and B's
   header states the only oracle that closes it is a **quorum read per snapshot** —
   precisely the apiserver load this programme exists to avoid. That is a
   design decision for Diego, not something to build on an architect's say-so.
3. The L1-side addition worth having either way is one field, not a subsystem:
   stamp the **resolved object's `resourceVersion`** on `ResolvedEntry` at Put and
   expose it in `ResolvedEntryMeta`. Then `/debug/apistage` can be joined against
   the informer store per cell, and "this cell's body is older than the object"
   becomes directly measurable instead of inferred. ~6 lines, no key change, no
   content exposure (an RV is not a body).

**Recommendation: do not dispatch more L1 analysis on #237 staleness. Ship B, read
it with its qualifier, and put the quorum-read question to Diego if B reads clean.**

---

## 11. Scoping the approved instrument — and why the approved standard kills it

TL approved *"per-key refresh history, the refresher emit sites joined on
`key_hash`"* as deliverable 1, and required (a) two-regime descriptions per field —
what it reads DURING the defect and when healthy — and (b) an explicit statement of
what it cannot distinguish.

**I am flagging a sequencing problem before scoping further: this is the instrument
I retracted in §10.5, and the approval does not engage with the retraction.** It may
be a deliberate override; if so I will build it. But I have applied the standard the
approval itself imposes, and the instrument does not survive it. Per
`feedback_verify_the_facts_inside_an_option_before_putting_it_to_the_decider`, I am
re-putting the decision rather than building on a premise I have already shown false.

### 11.1 The two-regime table, per field

Defect regime = the live #237 candidate named on HEAD
(`store_verify_stats.go:14-40`): a **stale cacher** — the informer store holds an
object older than etcd, so a successful refresh resolves stale bytes from it.

| field | reads DURING the defect | reads healthy | detector? |
|---|---|---|---|
| `key_hash` | the key | the key | **No.** Identity, never non-zero-able. |
| `outcome` (`put_ok` / `declined_uaf` / `floored` / `suppressed_skip` / `skipped_no_entry` / `failed`) | **`put_ok`** | **`put_ok`** | **No. Identical.** The refresh succeeds; that is the whole point of §9/§10.3. |
| `interval_since_last_put` | normal cadence | normal cadence | **No. Identical.** And already readable per-cell today as `/debug/apistage` `ageSeconds`. |
| `trigger_gvr` | fires normally | fires normally | **No. Identical.** The watch delivers; the content it delivers is what is stale. |

**Every field reads the same in both regimes.** By the rule the approval invokes —
`store_verify_stats.go:4-11`, *"A stat that cannot be non-zero is not an instrument
and does not belong here"* — this instrument does not belong. It would be the fifth
two-regimes-one-number on this investigation, and it would ship *as* the
falsification arm, which is the worst place to put one.

**Against the OTHER candidate class (a lost watch event)** it is only marginally
better, and still not the floor:
- If the event is lost, the key is never enqueued, so **no row is emitted at all**.
  Absence is the signal — and per `feedback_negative_evidence_needs_its_scope`, "no
  rows" is a statement about the instrument until a counter gives it a period.
- The per-cell version of that signal (`ageSeconds` keeps growing) is **already in
  the capture**, and the per-GVR version (`lastEventAgeSeconds`) is **already
  deployed**: `servable.json` carries it for all 209 GVRs — 12 `watchBroken=true`,
  12 `hasSynced=false`, 99 that have never delivered an event. Nobody has read it.

### 11.2 What it cannot distinguish — the required sentence

> **Per-key refresh history cannot distinguish a refresh that wrote CORRECT bytes
> from a refresh that wrote STALE bytes faithfully copied out of a stale informer
> store, because it observes neither the bytes nor the object. During the live
> #237 candidate every one of its fields reads healthy. A clean read from this
> instrument is therefore not evidence of a healthy cache and must never be
> reported as one.**

If that sentence has to ship with the instrument, the instrument is not worth
shipping.

### 11.3 The counter-proposal, held to the same standard

One field, ~6 lines, no key change, no content exposure (a resourceVersion is not a
body — `feedback_debug_surface_never_dumps_per_identity_cache_bodies` is satisfied):

**Stamp the resolved object's `resourceVersion` on `ResolvedEntry` at Put; expose it
as `objectResourceVersion` in `ResolvedEntryMeta` (`resolved.go:1292-1358`).**

| field | DURING the defect | healthy | detector? |
|---|---|---|---|
| `objectResourceVersion` vs an **authoritative** read of the same object | cell's RV **strictly below** the object's | equal | **Yes.** Differs between regimes. |
| `objectResourceVersion` vs the **informer store's** copy | **equal** (the store is the stale thing) | equal | **No** — and this is the trap. It must be compared against an authority, never against the store. |

That second row is the whole reason B's header refuses to claim #237 is detectable.
The stamp is **necessary but not sufficient**: it makes "this cell's body is older
than the object" *expressible*, and B (or a quorum read) supplies the oracle. Ship
it alongside B, not instead of it.

### 11.4 Recommendation

1. **Ship deliverable B** — written, on this branch, absent from the running binary.
2. **Ship the `objectResourceVersion` stamp** with it (~6 lines).
3. **Read `servable.json`'s existing `lastEventAgeSeconds` / `watchBroken` /
   `hasSynced`** against object churn per GVR — zero new code, data already in hand.
4. **If B reads clean, the stale-cacher candidate survives and the only closing
   oracle is a quorum read per snapshot — Diego's call.**

If the TL confirms the per-key refresh history after reading §11.1, I will scope and
build it. I am not refusing it; I am declining to let it be the arm that a
conclusion rests on, because it cannot carry that weight.

---

## 12. The 99 "never delivered an event" GVRs — characterised

TL: *"'never delivered an event' and 'never had an event to deliver' are the same
reading of the same field, and that is the distinction this entire investigation has
been about."* Correct, and the answer is in the emit site.

### 12.1 The initial LIST does NOT stamp the clock — TRACED

`noteInformerEvent` (`watcher.go:2712`) is called from exactly three sites,
enumerated:

```
$ grep -rn "noteInformerEvent" --include='*.go' go/snowplow/ | grep -v _test.go
internal/cache/deps_watch.go:361   AddFunc
internal/cache/deps_watch.go:371   UpdateFunc
internal/cache/deps_watch.go:387   DeleteFunc
internal/cache/watcher.go:2707,2712  (definition)
```

In `AddFunc` the stamp sits **after** the pre-sync gate (`deps_watch.go:354-361`):

```go
AddFunc: func(obj interface{}) {
	if !rw.addEventPostSync(gvr, w) {
		w.counters.addDroppedPreSync.Add(1)
		return                        // ← returns BEFORE noteInformerEvent
	}
	...
	rw.noteInformerEvent(gvr)
```

and `addEventPostSync` (`deps_watch.go:448-473`) returns `false` for the initial
replay — `default: return false // open → initial LIST replay → drop`.

**So a GVR whose objects all arrived in the initial LIST, and which has had no
UPDATE/DELETE since, reads `lastEventAgeSeconds = -1` with a perfectly healthy
watch and a correctly populated store.** Corroborated by
`snowplow_deps.add_dropped_pre_sync = 4670`.

### 12.2 The 99, classified

| n | `indexerCount` | `hasSynced` | `watchBroken` | `servable` | reading |
|---|---|---|---|---|---|
| **56** | 0 | true | false | true | **Never had an event to deliver.** Empty kind. Benign. |
| **41** | >0 | true | false | true | **LISTed at boot, no post-sync event since.** Benign *by construction* (§12.1). |
| **2** | 0 | **false** | **true** | **false** | **Genuinely broken** — and already flagged by `watchBroken`, not by this field. Both are in the existing 12. |

The 41 are static-by-nature kinds: 27 `github.krateo.io/v1alpha1` config CRs at 1
object each, the Gateway API set (`httproutes` 34, `gateways` 2, `gatewayclasses`
1), `agentgatewaypolicies` 31, `restdefinitions` 29, `modelconfigs` 3,
`widgets…/piecharts` 1. Nothing in that list is expected to churn. **None is the
#217 confirm-retraction class** — all 41 read `confirmed: true, servable: true`,
and `snowplow_informer_confirm_retracted_by_reason` is a separate counter.

**Verdict: 97 of 99 benign, 2 already caught by another field. No new signal here.**

### 12.3 But the field is the SIXTH two-regimes-one-number — and that is the finding

`lastEventAgeSeconds == -1` reads **identically** for:

- **(a)** a static kind that has had nothing to deliver — healthy; and
- **(b)** a kind whose watch died before ever delivering a post-sync event — the
  defect.

Its own doc (`servable.go:580-584`) claims the detector property — *"a GVR whose
objects churn but whose age keeps climbing has a dead watch that `watchBroken` did
not catch"* — and that holds for **positive** values. It does **not** hold for -1,
which is the value 99 of 209 GVRs carry.

**The discriminator already exists and needs no new code: read `indexerCount`
alongside it.** `idx == 0 && age < 0` → nothing to deliver. `idx > 0 && age < 0` →
LISTed, nothing since. That is exactly the split in §12.2, done from data already
captured. The fix is one `desc:`/doc change so nobody reads -1 alone — the same
remedy `store_verify_stats.go` applies to its own counters.

### 12.4 Confirming the TL's `pageheaders` kill, with its boundary

`pageheaders`: `hasSynced: true, watchBroken: false, confirmed: true,
servable: true, lastEventAgeSeconds: 126.7`. **A positive value is not subject to
the -1 ambiguity** — it means the bridge genuinely delivered a post-sync
Add/Update/Delete 126 s before the capture. The kill is sound.

Boundary, stated because it should travel with the claim: a positive value proves an
event **was** delivered, not that none was **missed**. What closes that gap here is
the join the TL already made — the seven `pageheaders` cells refreshed at
`ageSeconds ≈ 126`, i.e. that delivered event is the one that drove their refresh.
Field plus join is a kill; the field alone would not have been.
