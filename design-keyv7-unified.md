# design-keyv7-unified — does one key version absorb #180 and #224?

Date: 2026-09-18. Tree: `feat/prewarm-coverage-harness` @ c6ba82f (worktree `gate-118cv2`).
Tracking: snowplow#231. Inputs: #180 (UAF scope not in key), #224 (seed/browser key divergence).

> `scratchpad/plan-audit-and-prewarm.md` does **not exist on disk** anywhere under
> `/Users/diegobraga/krateo/snowplow-cache` (checked). This design is written from source +
> the runtime numbers quoted in the dispatch and in
> `go/snowplow/internal/cache/uaf_put_decline_metrics.go:19-22,71-73`.
>
> `kubectl config current-context` returns **`error: current-context is not set`**. Per the
> standing rule I did **not** switch contexts, so there are **no fresh live expvar reads in this
> document**. Every runtime number here is quoted from a code header that recorded it or from the
> dispatch. Flagged inline as TRACED-FROM-HEADER vs TRACED-FROM-SOURCE.

---

## VERDICT

**No. One key version does not absorb both — because #224 should not be fixed with a key term at
all, and #180's fix must not be *only* a key term.**

The honest statement is stronger and simpler than "unify the hash":

> **Both defects are the same *category* error — a non-content dimension is being carried in the
> content key — but they are opposite instances of it.**
> **#224 is a dimension that is in the key and should not be (it is a *freshness* signal, and a
> lossy one).**
> **#180 is a dimension that is not in the key and cannot merely be added (it is a *body* signal,
> and it is not derivable before the resolve that produces it).**

Forcing them into one `v7` hash fold is the failure mode the dispatch already anticipates in
question 2: it makes the hot class *cacheable-but-unhittable*, which burns memory for nothing and
is worse than the 1.12.3 decline.

There **is** one shipping unit, and it **is** one `resolvedKeyVersion` bump (`v6` → `v7`), because
both changes touch `ComputeKey`'s identity block. But it is **two mechanisms in one version**, not
one mechanism. Calling that "unified" would be a naming convenience, not an engineering claim.

> **Read §3.4 and §2.5 before treating `v7` as the path to 100%.** `v7` fixes *keys*. Three of the
> eight app-shell widgets are **unreachable by the enumerator** (§3.4) and a **third, independent
> decline mechanism** suppresses the identity-free widget cell (§2.5). `v7` alone delivers **at best
> 5 of 8 shell widgets**. Keys are no longer the binding constraint; reachability is.

---

## 0. Prior art (opened first, per standing rule)

| Upstream thing | What it solves | Does it apply |
|---|---|---|
| **HTTP `Vary` + secondary cache keys, RFC 9111 §4.1** | primary key = content identity (URI); a *secondary* key selected from request dimensions the primary key does not carry | **Directly.** This is the K1/K2 two-phase shape below. It is the only mature answer to "one resource, many per-requester renderings" and it is the model snowplow has been approximating by folding everything into one SHA-256. |
| **`k8s.io/apiserver/plugin/pkg/authorizer/webhook`** — LRU over authorization decisions keyed by the `authorizer.Attributes` record, split allow/deny TTLs | cache the *decision*, not the *narrowed body* | **Directly**, and snowplow already sits on the seam: `rbac.EvaluateOptions` is documented as mirroring `authorizationv1.ResourceAttributes` (`internal/rbac/evaluate.go:53-55`). Phase-2 memoisation below is this, in-process. |
| **`plugin/pkg/auth/authorizer/rbac` `RuleResolver.RulesFor(user, ns)`** | the canonical *union-of-rules projection* for an identity | **Directly** — it is the input domain for the scope digest in §2. Upstream computes it per request from the same informer-cached RBAC types snowplow reads. |
| **client-go informer `ResourceVersion` "not older than" semantics / HTTP `Last-Modified` + `If-Modified-Since`** | a **validator** carried on the cached object, checked at read, rather than a term in the key | **The shape only — NOT the semantics.** It supplies "put the signal on the entry, not in the key", which is what #224 needs (§1.3). Its *failure* behaviour (serve stale, revalidate in background) is **inverted** here: the signal is an authorization signal, and serving stale is a leak. See the corrected §1.3. Copying this prior art wholesale is precisely the mistake the first draft made. |
| **client-go `ExpirationCache` / `UndeltaStore`** | TTL'd shared cache | No. No identity dimension. |
| **Anything in client-go that caches a per-identity *narrowed response body*** | — | **Does not exist.** There is no upstream precedent to copy for the body. `Vary` is the closest, and it is HTTP, not Kubernetes. This is the part snowplow genuinely has to build. |

Nothing here needs inventing except the body layer, and for that the design below deliberately
copies `Vary` rather than inventing a third thing.

---

## 1. #224 — the sub-gen does not belong in the key

### 1.1 Root cause, TRACED

`ComputeKey` folds `RBACSubGen` as 8 LE bytes for every identity-bound class —
`internal/cache/resolved.go:844-847`.

`RBACSubGen` is a **sum of per-subject monotonic counters** —
`internal/cache/rbac_subgen.go:96-116`:

```
RBACSubGenForSubject(u, G) = c[User:u] + Σ_{g∈G} c[Group:g]     (+ the SA limb)
```

It has **exactly one production stamping site**: `internal/handlers/dispatchers/helpers.go:275`.
A grep for `RBACSubGen` across `internal/handlers/` and `internal/resolvers/` (excluding tests)
returns that one assignment; every other hit is a comment. **The seed never stamps it**, which
`internal/cache/resolved.go:451-456` states outright ("the identity-bound seed Put writes
`RBACSubGen==0` … de-scoped this as a #42-class seed-reachability perf gap"). TRACED-FROM-SOURCE.

So the two halves of #224 are:

- **Temporal.** Seeded cell carries `0`. Any binding event touching *any* subject the browsing user
  presents moves the browser's sum off `0`. At 50K composition installs creating RoleBindings
  continuously (`rbac_subgen.go:18-21` names this exact workload), the sum moves constantly. The
  seeded cell is unhittable from the first relevant event onward.
- **Structural.** Even if the seed *did* stamp it, it would stamp
  `RBACSubGenForSubject("", ["devs"])` — the cohort representative, `Username: ""` plus the single
  group `pickRepresentativeFromSubjectKeys` picked
  (`internal/cache/prewarm_enumeration.go:210-213`). The browser presents a real username and every
  group. **Different subject sets ⇒ different sums**, by construction of the formula above.

### 1.2 Why "seed every distinct effective generation" is sound but is the wrong shape

Diego's ruling is correct *as a repair of the symptom* and it is cheap, because `RBACSubGen` is a
**pure invalidation salt**: the code says so — "the key only needs to CHANGE on any relevant RBAC
event, not carry a meaningful absolute value" (`rbac_subgen.go:72-73`). The body is identical across
every value, so N puts cost N hash-and-store, one resolve. That is real and it works.

What it does not do is remove the mechanism that keeps generating the divergence. Three concrete
costs of keeping the salt in the key, all TRACED-FROM-SOURCE:

1. **The seeder must re-enumerate on every RBAC event.** The values are *history*, not *content*, so
   every binding add/update/delete invalidates the enumeration. At 50K installs that is a treadmill.
2. **The enumeration is bounded by distinct sums, and distinct sums are only bounded below user
   count by a data-dependent accident.** If any binding names users directly (`subjectKindUser`,
   `rbac_subgen.go:106`), `c[User:u]` becomes non-zero per-user and the sum becomes per-user. The
   bound is a property of the customer's RBAC topology, not of the design. That is exactly the
   "bounded by user count" failure the constraint forbids, sitting one topology change away.
3. **The salt over-invalidates by design.** It bumps on *any* binding event touching the subject,
   including thousands that grant nothing on the GVR being keyed. Each bump rotates every one of that
   subject's cells across every GVR. *(§1.3 does **not** fix this — see the out-of-scope note there.
   It is listed as a cost of the current shape, not as a benefit claimed for `v7`.)*

### 1.3 The fix: `RBACSubGen` leaves the key and becomes a **fail-closed** entry predicate

> **CORRECTED 2026-09-18 after PM gate — this section previously reopened #118 (c) as a leak.**
> The first draft routed a failed predicate into stale-while-refresh and called that a win. That was
> a category error, and the citation that kills it is in the tree:
> `internal/rbac/evaltest/rbac_subgen_race_test.go:246-251` — *"A stale (unrotated) key would serve
> the pre-edit resolved cell on a warm hit, never re-entering EvaluateRBAC, so the revoke would be
> invisible to the browser until TTL."* Corroborated at `internal/cache/resolved.go:84-86`: the
> `UAF_RESOLVED_TTL_SECONDS` stopgap "does NOT fix the cache key (a within-TTL RBAC change is still
> served stale — that is #118 (c)'s job)". **TRACED-FROM-SOURCE, both.**
>
> **`RBACSubGen` in the key is not load-bearing for freshness. It is load-bearing for revocation.**
> Those have opposite failure semantics: serving stale is *safe* for freshness and is a *leak* for
> authorization. An `If-Modified-Since` validator is the wrong instrument for an authz signal, and
> applying one here would have served post-revoke users their pre-revoke rows.

Replace the summed counter with a **global monotonic RBAC sequence**, stamped on the entry, checked
at read — as a **fail-closed admission predicate**, not a freshness validator. The client-go
`ResourceVersion` / `If-Modified-Since` prior art in §0 supplies the *shape* (a validator on the
object rather than a term in the key) and **not** the *failure semantics*, which must be inverted.

- Per subject, record `lastChangeSeq[subject]` — the value of one global monotonic counter at the
  moment that subject's grants last changed. Same `sync.Map[subjectKey]*atomic.Uint64` already in
  `rbac_subgen.go:51`, same informer-hook bump site (`BumpSubjectSubGens`, `rbac_subgen.go:74-78`);
  only the stored value changes from `+1` to `globalSeq.Add(1)`. Same concurrency contract, same
  lock-free read.
- `ResolvedEntry` carries `WrittenAtSeq uint64` — the global sequence at Put time. **Not key
  material.** Same class of field as `RepresentativeUsername` / `HasUAF`, which
  `internal/cache/resolved.go:412-414,437-440` already document as carried-on-`Inputs`,
  excluded-from-`ComputeKey`.
- At lookup: `requesterSeq = max over the requester's subjects of lastChangeSeq[subj]`. The entry is
  **servable** to this requester iff `entry.WrittenAtSeq >= requesterSeq`. Otherwise **do not serve
  it — re-resolve.** Not stale-while-refresh. Not a TTL extension. The cell is
  *findable-but-not-servable*, and the request takes the same full resolve it takes today.

**This costs nothing against `v6`.** Today a bump makes the cell *unfindable*, so the request
resolves. Fail-closed `v7` makes it *findable-but-not-servable*, so the request resolves. Identical
work, identical latency, identical authorization outcome. The change buys the structural property
below and nothing else — which is the correct scope.

What this buys, point by point against #224:

- **Structural half: gone.** The requester's subject set appears only in the *predicate*, never in the
  *key*. Seed and browser derive a byte-identical key **on the path where no RBAC event intervenes**
  — which is the seed→first-navigation path, i.e. exactly #224. This is the whole win and it is
  intact under fail-closed.
- **Temporal half: NOT fixed, and must not be.** A binding event between seed and navigation that
  changes this requester's grants *must* invalidate; that is #118 (c)'s entire purpose. The first
  draft claimed this half as a win. **Withdrawn.** Under fail-closed the temporal behaviour is
  byte-for-byte what `v6` does today.
- **Diego's "N puts per distinct generation" becomes unnecessary in this dimension.** N collapses to 1
  for the no-event path. That is the test of whether this is the right mechanism, and it passes.

**The one temporal gain that is still available — and is NOT part of this design.** `v6` rotates on
*any* bump to *any* presented subject, including the thousands of composition-install RoleBindings
that grant nothing on the widget's GVR (`rbac_subgen.go:18-21` names this workload). Scoping
`requesterSeq` to the layer's own (verb, GVR) domain would stop the 50K-install storm from
invalidating unrelated cells. **INFERRED, and deliberately out of scope for `v7`:** narrowing what
counts as a relevant RBAC change is a *security-relevant* narrowing — it is only sound if the scoped
signal is provably a superset of the events that can change this layer's authorization outcome. That
needs its own design and its own RED arm (a revoke inside the domain that the scoped signal misses).
**Do not let a dev brief fold it in as an optimisation.**

> **INFERRED** (falsifier in §5): that no consumer of `ResolvedKeyInputs.RBACSubGen` outside
> `ComputeKey` depends on it being key material. The one stamping site is `helpers.go:275` and the
> one reader is `resolved.go:844-847`; the refresher path reads `Inputs` wholesale. A `grep -n
> 'RBACSubGen' --include='*.go' internal/` enumerating **every** site (per
> `feedback_enumerate_all_write_sites_not_the_designs_named_set`) is a prerequisite of the dev brief,
> not an assumption this design gets to make.

---

## 2. #180 — the UAF scope cannot merely be folded in

### 2.1 Root cause, TRACED

The narrowing is a **per-item RBAC verdict**. `refilterSlice`
(`internal/resolvers/restactions/api/refilter.go:311-368`) walks items and calls `evalSingle`
(`:387-470`), which resolves `NamespaceFrom` / `NameFrom` per item and calls
`rbac.EvaluateRBAC{Username, Groups, Verb, Group, Resource, Namespace, Name}` with OR semantics over
the resolved resource set.

Two properties of that code are load-bearing for everything below, both TRACED-FROM-SOURCE:

- **(P1) The refilter is a pure drop filter.** `kept = append(kept, item)` — kept items are the
  *same* items, byte-identical; nothing is rewritten (`refilter.go:321,357-361`). The narrowed body
  is a **subsequence** of the pre-refilter superset. That makes the outcome expressible as a bitmask
  over superset positions.
- **(P2) The refilter's input is the SA-dispatched, identity-free superset.** The UAF path dispatches
  the inner K8s call with snowplow's **own ServiceAccount** (`internal/dynamic/sa_client.go:4-8`),
  and `jsonHandlerCore` runs the refilter on the **raw envelope before the stage filter**
  (`internal/resolvers/restactions/api/handler.go:145-147` refilter, then `:152` filter). So there
  exists a well-defined, genuinely identity-free object at a specific point in the pipeline.

The defect is then exactly as the header states
(`internal/cache/uaf_put_decline_metrics.go:3-10`): the key folds `BindingUID + RBACSubGen`, and
neither separates requesters by narrowing scope. Two users sharing the first-match binding for
`get restactions` derive the same key; the hit path serves `entry.RawJSON` verbatim.

### 2.2 Why "fold a UAF-scope digest into the key" is a chicken-and-egg, not a design

The *exact* narrowing scope — the verdict vector — is **only known after the refilter has run**.
A key term you can only compute after doing the work you were trying to avoid cannot be used for the
**lookup**. Fold it in naively and every customer request misses on GET and succeeds on PUT: the
cache fills perfectly and is never read. The dispatch's question 2 names this, and it is not a
hypothetical — it is the arithmetic of a post-resolve key term.

So a digest must be either (a) **pre-resolve derivable**, or (b) reached by a **two-phase lookup**.
Both are viable, they have different costs, and I recommend shipping (b) with (a) layered on top.

### 2.3 Answer to question 2 — **yes, a naive #180 fold recreates #224, and worse**

Take the pre-resolve digest form (a): digest the identity's **effective grant set over the layer's
UAF domain** (verb, group, resolved resources), computed from the same informer-cached RBAC types
`rbac.RulesFor` reads upstream. Equal grant projection ⟹ equal verdicts on *any* item set, so it is a
**safe over-approximation**: it can over-separate (extra cells) but can never under-separate (leak).

Now ask the seed to reproduce it. The seed's representative is `(Username: "", Groups: [g])`
(`prewarm_enumeration.go:210-213`). The browser presents `(alice, [devs, ops, sales, …])`. The
digest of a **union over subjects** differs between those two identities whenever any extra group the
user presents grants anything in the domain. **So yes — the fold recreates #224 in a new dimension.**

Two mitigations, in order of how much they buy:

- **Domain-scope the fold.** Combine only subjects that actually carry a grant in *this layer's*
  (verb, GVR) domain. A user in `{devs, sales}` where `sales` grants nothing on this GVR folds
  exactly what the `devs` representative folds. This collapses most of the divergence, because in the
  narrow RBAC shape the overwhelming majority of a user's groups are irrelevant to any given widget.
- **Digest the resulting rule union, canonicalised — not the subject set.** `{devs}` and
  `{devs, ops}` collapse to one digest iff `ops` adds no rule beyond `devs`. This is `RulesFor`
  semantics (§0) and it is what makes the bound *content*-shaped rather than *subject-set*-shaped.

Even with both, the residue is real: **distinct grant unions over the domain remain distinct
digests**, and the seed cannot enumerate which unions real users will present without either a
powerset walk (forbidden — `feedback_rbac_natural_warmup`, "no cartesian") or a user-count-bounded
enumeration (forbidden by the dispatch). **This is why (a) alone must not ship.** It is a strictly
better key, and a cache whose hot cells are still unhittable.

### 2.4 The fix: `Vary`-shaped two-phase cache, with the digest as the secondary key

Copy RFC 9111 §4.1 rather than inventing.

**K1 — the shared substrate cell, identity-free.** Stores the **pre-refilter** superset (P2) *plus*
the per-item derived triples `(resource, namespace, name)` that `evalSingle` computes. The triples
are cached because deriving them is the per-item `NamespaceFrom`/`NameFrom` **JQ** evaluation
(`refilter.go:332,344,399,432`), and caching them is what makes phase 2 pure `EvaluateRBAC` with no
JQ. Key: every existing dimension **except** identity — literally the `widgetContent` key shape
already in `ComputeKey` (`resolved.go:822-826`: "widgetContent is identity-free — the widget envelope
is shared, the per-user `allowed` flag is re-derived at serve time"). **This pattern is already in
the codebase and already shipped.** #180's fix is generalising it, not introducing it.

**Phase 2 — derive the secondary key.** Per request, run `EvaluateRBAC` over the cached triples.
No apiserver call, no JQ, no decode. `refilter.go:26-30` records this as amortised **<1µs per check**
against a per-site result set of **≤50 items** — call it ~50µs. Memoise it with the
webhook-authorizer pattern from §0, keyed by the decision record, invalidated on snapshot publish.
The digest is the commutative fold over the verdict records `(resource, ns, name, allowed)` — a
**wrapping sum of `fnv64a` per record**, not XOR, because the refilter bumps from errgroup workers
(`internal/cache/uaf_touched_sink.go:94-99` documents exactly this concurrency) and XOR cancels on
duplicates while a sum does not. Identity is **not** in the record: that is what makes the digest
bounded by distinct access patterns rather than by users.

**K2 — the narrowed cell.** `K1 ⊕ digest`. Stores the post-refilter, post-stage-filter body — the
thing that is served today.

**Where it plugs in.** The accumulator is already built and already plumbed. `UAFTouchedSink`
(`internal/cache/uaf_touched_sink.go:94-160`) is a ctx-carried atomic accumulator installed at
**every** Put-capable resolve entry — `restactions.go`, `widgets.go`, `resolve_populate.go`,
`phase1_pip_seed.go` (both seeders), `phase1_walk.go` — and inherited through every nested frame
"which is exactly why it sees a refilter the Put site's own frame cannot" (`:124-128`). Every Put
site already reads it, via one shared helper pair `declineUAFPut` / `declineWidgetUAFPut`
(`internal/handlers/dispatchers/uaf_shortttl.go:279-307`). **`v7` widens that sink's payload from a
count to a digest and flips the same helper from "decline" to "discriminate".** No new plumbing, no
new context key, no new Put site.

**Why this is the recommendation, in one line:** a K2 miss falls through to a **K1 hit**, never to a
cold apiserver LIST. The failure mode of imperfect digest coverage is **CPU, not a cold navigation**
— which is what makes it compatible with `feedback_zero_cold_navigations_hard_requirement`, and which
is the property (a)-alone does not have.

**Cost honestly stated.** A K2 miss re-pays the stage filter (JQ) because the filter runs *after* the
refilter (`handler.go:145-152`) and does not distribute over it in general (`length`, `group_by`).
`docs/boot-walk-deadline-rootcause-2026-07-09.md §4` is cited in `refilter.go:326-331` for an
8854-item filter paying ~0.3s in pure recompile — so a K2 miss on a large cell is not cheap. It is
still strictly cheaper than today's decline, which pays the SA LIST **and** the decode
(`feedback_cluster_list_decode_irreducibility`: ~10ms/MB) **and** the JQ, on **every single request**.

### 2.5 The THIRD decline mechanism — `v7` as scoped does NOT fix it

**Added 2026-09-18 after PM gate. Nobody should read "`v7` re-enables caching" as covering this.**

There is a third, **independent** suppression that has nothing to do with `userAccessFilter`:
`isRBACSensitiveApiRefWidget` (`internal/handlers/dispatchers/widget_content.go:213-227`), counter
`bumpWidgetContentSkippedRBACSensitive` / `widget_content_skipped_rbac_sensitive_total`.
**TRACED-FROM-SOURCE.** Its own words (`:202-212`):

> *an apiRef-driven render-template widget (piechart/table over an aggregating apiRef RA) renders
> from `status.widgetData`, which the serve-time gate NEVER narrows per-user — so the identity-free
> cell would serve every user the SA-maximal aggregate (a cross-user leak).*

Live: **21,338**, against the UAF decline's 2,470 — an order of magnitude larger
(TRACED-FROM-DISPATCH; no kube context, not re-read).

**Does `v7` fix it? As scoped, no.** Two things must be said separately, because conflating them is
how this gets mis-read:

1. **It is not the same defect.** #180 is "the key cannot separate narrowing scope". This is "the
   identity-free `widgetContent` cell has no narrowing *step* at all" — the serve-time gate that
   narrows the normal envelope does not reach `status.widgetData`. Different mechanism, different
   file, different counter.
2. **It is not 21,338 uncacheable renders.** The skip **routes** the widget to the per-cohort
   `widgets` L1 rather than dropping it (`:205-210`, `:219-225`). So the counter measures
   *re-routing*, not loss. **Reading 21,338 as "10× bigger than the thing you are fixing" overstates
   it**, and the design should not carry that framing.

**But there is a real interaction, and it is the part worth acting on.** `declineWidgetUAFPut` gates
on `isWidgetClass(class)`, which is `"widgets" || widgetContent`
(`internal/handlers/dispatchers/uaf_shortttl.go:315-317`). So a widget that is **both** RBAC-sensitive
**and** UAF-carrying is skipped out of `widgetContent` by mechanism 3 and then **declined out of
`widgets` by mechanism 1 — it is cacheable nowhere.** That intersection is very likely where the
three uncacheable app-shell widgets (40 requests / 40 misses / 0 stores) actually live.

**`v7`'s §2.4 mechanism generalises to it cleanly** — the identity-free `widgetContent` cell *is* K1,
and per-requester narrowed `status.widgetData` *is* what K2 would carry. But generalising it is a
scoping decision with its own blast radius, not something this design gets to assume.
**Recommendation: measure the intersection first.** Instrument how many of the 21,338 skips are on
widgets that *also* trip `declineWidgetUAFPut`. If the intersection is the three shell widgets, `v7`
must be scoped to cover mechanism 3 or it will not move the headline number at all.

---

## 3. Answer to question 3 — does "one put per distinct generation" generalise?

**It generalises in form and inverts in cost, and the cross-product does not have to be covered.**

| | #224 sub-gen | #180 UAF scope |
|---|---|---|
| Is the term body-affecting? | **No** — pure salt (`rbac_subgen.go:72-73`) | **Yes** — a different scope *is* a different body |
| Cost of N distinct values | 1 resolve, N hash+store | 1 K1 resolve, **N refilters + N filters + N stores** |
| Seeder can enumerate the domain? | Yes, from counters | Only the *granting subjects*; not the *unions users present* |
| Bound | distinct sums (data-dependent) | distinct grant-unions over the domain |

So Diego's answer was given for the cheap case and is right there. Transplanted to #180 it does
**not** hold, for the single reason that the body is not identical across values.

**And under §2.4 it does not need to.** The seed does the following, and stops:

1. Seed **K1 once per cell**. Identity-free, so seed and browser derive the same key *by
   construction* — this is the `widgetContent` invariant, already shipped and already relied upon.
   This is the part that must be complete, and it is the part that is cheap and enumerable.
2. Seed **K2 for each representative identity the existing enumerator already produces**
   (`EnumeratePrewarmTargetsForGVR`, `prewarm_enumeration.go:104+`) — one refilter per target over
   the *already-resolved* K1 superset. **No extra resolve.** This is the exact shape of Diego's
   ruling, applied to the layer where it is affordable.
3. **Do not walk the powerset.** An identity whose grant-union the seed did not anticipate takes a K2
   miss and a K1 hit: it pays a refilter plus a filter, in-process, and then *writes its own K2 cell*
   which every subsequent member of its equivalence class hits. Self-adapting, no knob, no cap
   (`feedback_self_adapt_no_magic_env_knobs`, `feedback_prewarm_walk_no_sampling_caps`).

### 3.4 The reachability ceiling — `v7` alone delivers at best 5 of 8 shell widgets

**Added 2026-09-18 after PM gate. §3 step 1 above says "seed K1 once per cell" and silently assumes
the enumerator produces the cell. That assumption is false today, and it is now the binding
constraint — not keys.**

Per the gate: **three of the app shell's eight widgets are absent from the walk map** (531 of ~533
coordinates reached). **TRACED-FROM-GATE — I have not independently measured this** (no kube context;
see the preamble), and it should be re-confirmed against the `prewarm-coverage` run-dir before it is
load-bearing in a dev brief.

A cell the enumerator never visits is **never written under any key version**. Key correctness is
necessary and not sufficient: `v7` makes a seeded cell *hittable*, and does nothing whatsoever for a
cell that was never *seeded*. So:

> **`v7` alone raises the ceiling from "3 of 8 shell widgets uncacheable by construction" to
> "3 of 8 shell widgets unseeded by reachability". The headline number does not move without a
> reachability fix.** Against a goal of 100% cache-hit on login-and-navigate, `v7` alone delivers
> **at best 5 of 8**.

This is not an argument against `v7` — the three unreached widgets are *also* UAF-declined, so they
need both fixes and `v7` is one of them. It is an argument against sequencing `v7` first and
expecting the north-star to move. **Reachability and `v7` are independent, and reachability is the
cheaper of the two.**

**Cardinality bound, stated plainly.** Cells ≤ (cells today) × (1 + number of distinct grant-unions
over the layer's UAF domain actually presented). K1 is one per cell. K2 is one per *observed
equivalence class*, created on demand. Neither is a function of user count: 1,000 users in the narrow
RBAC shape share one grant-union and therefore one K2. The second factor is data-dependent and I will
not claim it is structurally bounded — **it is bounded by observation**, which is the honest bound and
the only one that self-adapts.

---

## 4. The `v6` → `v7` bump: what happens to in-flight entries

**Nothing, and this is a much smaller question than it sounds.**

`project_redis_removal` / `project_caching_is_provisional`: **L1 is in-process.** `ResolvedCache()`
is a per-process singleton (`resolved.go:715-751`). There is **no shared key space** between pods.
A key-version bump therefore does not "rotate" anything that outlives a process:

- The old pod keeps serving its own `v6` store until it terminates. Its entries are private; no `v7`
  reader can reach them, which is precisely the clean-break property `v3→v4`, `v4→v5`, `v5→v6` were
  each bumped for (`resolved.go:487-516`).
- The new pod boots with an **empty store regardless of the version**, runs the boot seed, and does
  not take traffic until `/readyz` reports prewarm-complete
  (`project_readyz_gates_on_prewarm_complete`; `internal/handlers/readyz.go`,
  `internal/cache/prewarm_complete_metric.go`).
- Chart strategy is RollingUpdate surge 1 by design
  (`feedback_snowplow_upgrade_needs_recreate_strategy`, superseded note), so old and new overlap and
  the old pod covers the new pod's seed window.

**Cold window = the existing boot-seed window. The bump adds zero to it.** The `v7` salt must still
be bumped — not for cold-window management but for the cross-regime correctness reason every prior
bump cites: a `v6` cell's key does not encode the UAF scope, so it must never be readable as a `v7`
hit under a pod that believes it does. That is a *safety* bump, and it is free.

Per `feedback_seed_falsifier_required_on_l1_key_changes`, a `TestBootSeedCoverage_*` arm is mandatory
alongside any of this.

---

## 5. Falsifiers

### 5.1 The security falsifier (worst-case; two dimensions; anti-trivial)

`TestKeyV7_UAFScopeSeparation_SharedBinding_CrossUser` — **must fail red on today's tree with the
decline removed, and on any key-only fix.**

**Setup — this is where the arm is usually wrong.** A real published RBAC snapshot with **one**
binding `B` granting `get restactions` to `Group/devs`, so alice and bob derive the **identical**
`BindingUID`. Both in `devs`. A second pair of bindings grants alice the UAF verb in `ns-a` only and
bob in `ns-b` only. The K1 superset holds **≥1 item in `ns-a` AND ≥1 in `ns-b`**.

| # | Dimension | Assertion |
|---|---|---|
| A0 | **anti-setup-collapse** | `bindingUID(alice) == bindingUID(bob)` and **non-empty**. Without this the arm is green because the users were never in the same cell — the #132 "install-crossed state" trap. |
| A1 | **key** | `ComputeKey(alice) != ComputeKey(bob)` — and assert the *differing byte range* is the scope-digest field, not an incidental one (pre-hash diff, per `feedback_key_parity_golden_real_inputs_prehash_diff`). |
| A2 | **resolved output** | Drive the **real dispatch** (not a seam — `feedback_seamed_dispatch_cannot_falsify_a_deep_frame`), alice first. alice's served body contains the `ns-a` item and **not** the `ns-b` item. Then bob, **against the same store**: bob's body contains `ns-b` and **not** `ns-a`, and `dispatch_l1_lookups` records a **MISS** for bob's first request. |
| A3 | **anti-trivial / the cache is actually on** | alice's **second** request is an **L1 HIT** returning the `ns-a`-only body. Without A3, "never cache anything" passes A1+A2. |
| A4 | **anti-trivial / the decline is off** | `cache.WidgetsUAFPutDeclined()` delta over the arm == **0**, and `RestactionsUAFPutDeclined()` delta == 0 (`uaf_put_decline_metrics.go:79-98`). **This is the single most important line in the arm.** Without it the 1.12.3 mitigation satisfies A0–A3 trivially and the arm proves nothing about `v7`. |
| A5 | **per-item bytes** | Assert `ns-a`/`ns-b` membership **by item bytes**, not by name set or count (`feedback_byte_parity_golden_must_assert_peritem_bytes`). |

**Carrier symmetry (`feedback_leak_needs_acceptance_arm_per_carrier`).** The above must run **twice**:
once with the RA dispatched directly (`restactions` class, `restactions.go:399`) and once via
`widgets.Resolve → resolveApiRef → apiref.Resolve → restactions.Resolve` folding into
`status.widgetData` (`widgets` class) — **the R-1 carrier, the one with 298,064 hits**
(TRACED-FROM-HEADER: `uaf_put_decline_metrics.go:19-22`). A single-carrier arm has already missed
this once, which is why there are two counters and not one (`:12-22`).

**Group symmetry (`feedback_predicate_subject_kind_symmetry`).** Repeat with the narrowing grant
arriving via `Group` rather than `User`.

### 5.2 #224 falsifier

Per `feedback_curl_probes_inadmissible_for_seed_hit_acceptance` a curl probe is **inadmissible**.
Chrome → portal → ingress, `K>1 × M>1` (`feedback_falsifier_shape_must_discriminate`): ≥2 distinct
users in ≥2 distinct grant-unions, ≥2 widgets.

> **CORRECTED 2026-09-18 after PM gate.** The first draft asserted `l1:HIT` *after* a binding event.
> That is a **green light for the #118 (c) revocation leak** — it would have passed an
> implementation that serves post-revoke users their pre-revoke rows and called it a win. The arm is
> split in two below, and the two halves assert **opposite** things.

**Arm A — the no-event path. This is #224, and this is where `l1:HIT` is the green.**
Boot seed, then navigate, with **no RBAC event in between**.
Green: every app-shell request on first navigation reports `l1:HIT` with a `key_hash` **equal to a
hash the seeder wrote**. The existing `prewarm-coverage` harness on this branch already emits the
per-request key-hash table (c6ba82f) — that table **is** the instrument. Under `v6` this is red
(the seed writes `RBACSubGen==0`, the browser does not); under `v7` §1.3 it is green because the
subject set has left the key.

**Arm B — the revoke path. `l1:HIT` here is a FAILURE, and the assertion is on CONTENT, not status.**
Seed, then **revoke** a grant that narrows this user, then navigate.

| # | Assertion |
|---|---|
| B1 | The served **body does not contain the revoked rows**. Per-item bytes (`feedback_byte_parity_golden_must_assert_peritem_bytes`), not a count and not a name set. **This is the only assertion that actually tests the security property**; every status-level assertion below is supporting evidence. |
| B2 | The request re-entered `EvaluateRBAC` — assert a delta on `rbac.EvaluateRBACCallCount()` (`internal/rbac/evaluate.go:40-45`). A warm serve that never re-enters the evaluator is the exact shape `rbac_subgen_race_test.go:246-251` describes. |
| B3 | **Anti-trivial:** the revoked rows were **present** in the pre-revoke body. Without this, a user who could never see them passes B1 vacuously — the empty/unreached-state trap. |
| B4 | **Anti-trivial:** a *second*, **unrelated** user in the same cell — one whose grants the revoke did not touch — still gets `l1:HIT` on the seeded `key_hash`. Without B4, "invalidate everything on every RBAC event" passes B1-B3 and silently reintroduces the herd-wide rotation §1.2 cost 3 is about. |

Arm B must go **red** against a stale-while-refresh implementation of §1.3 — that is the arm's
purpose, and it is the arm the first draft did not have.

### 5.3 The counters that must move, and the direction

- `snowplow_widgets_uaf_put_declined_total` → **0 delta** under `v7`
  (`uaf_put_decline_metrics.go:30-32` commits to exactly this: "these counters return to their
  pre-1.12.3 zero"). Live-057 baseline quoted in the dispatch: **2,470** — TRACED-FROM-DISPATCH, not
  re-read (no kube context; §preamble).
- `widget_content_skipped_rbac_sensitive_total` (live **21,338**) → **NO expected delta from `v7` as
  scoped.** This is the third, independent mechanism (§2.5). Listing it as a `v7` success metric
  would be the mis-read §2.5 exists to prevent. It belongs on the **pre-work** measurement — how much
  of it intersects `declineWidgetUAFPut` — not on the acceptance scorecard.
- `snowplow_ra_full_list_uaf_bypass_total` → 0 delta (`ra_full_list_store.go:63`'s
  `uaf-scope-waiver` comment is retired with the bypass).
- The three app-shell widgets measured at **40 requests / 40 misses / 0 stores** → **40 requests /
  ≤N misses / ≥1 store**, with the steady-state hit rate the acceptance number.

---

## 6. Options, with a recommendation

| | Mechanism | #180 | #224 | Bound | Risk |
|---|---|---|---|---|---|
| **1** | Pre-resolve grant-projection digest folded into `ComputeKey`; keep `RBACSubGen` in the key; seed N puts per distinct value | fixed | fixed *only* for enumerated values | distinct grant-unions **× distinct sub-gens** — a real cross-product | **Recreates #224 in the new dimension.** Hot class cacheable-but-unhittable. Smallest diff, worst outcome. |
| **2** | Serve-time narrowing only: cache the K1 superset, refilter **and re-filter** on every serve. No narrowed body in cache at all | fixed **structurally** — nothing narrowed is ever stored | n/a in this dimension (K1 is identity-free) | one cell per logical cell | Re-pays the JQ stage filter on **every** request, incl. the 8854-item case. Safest; slowest. |
| **3** ✅ | **§1.3 fail-closed predicate (#224) + §2.4 two-phase K1/K2 (#180), shipped as one `v7`** | fixed structurally at K1, optimised at K2 | **structural half only** — the subject set leaves the key; the revoke path is unchanged and fail-closed | K1: 1/cell. K2: 1 per **observed** equivalence class | Largest diff. Two new concepts (admission predicate; secondary key). K2 miss re-pays the filter. **Neither option moves the north-star without the §3.4 reachability fix.** |

**Recommended: 3.** It is the only one where the failure mode of imperfect seed coverage is CPU
rather than a cold navigation, and the only one where the security property is structural at the
substrate (K1 holds nothing narrowed) *and* enforced at the optimisation layer (K2's digest). Option
2 is the honest fallback if K2 proves not to be worth its complexity — and note that **2 is a strict
subset of 3**, so shipping 3 in two steps (K1 first, K2 second) is available and is how I would
sequence it.

---

## 7. Risks, ranked

> **RE-RANKED 2026-09-18 after PM gate.** The first draft named the cardinality distribution as the
> top risk. It is now second.

### 7.1 `v7` ships, is correct, and the north-star does not move

**The top risk.** Keys were never the binding constraint (§3.4, §2.5). A key fix that lands against
an unreachable cell is invisible, and the team will have spent a `v7` bump to learn it.
**Confirm the 3-of-8 reachability gap against the `prewarm-coverage` run-dir before a dev brief is
written**, per `feedback_baseline_before_speculation`. If reachability is the real gap, it is both
cheaper than `v7` and a prerequisite for `v7` being measurable.

### 7.2 The grant-union digest's cardinality

**Bounded by observation, not by construction — and no one has measured the distribution.**

Every claim in §3 that "1,000 users collapse to one K2" rests on the narrow RBAC shape
(`project_narrow_rbac_shape`, `project_composition_install_rbac_scale`), not on anything `v7`
enforces. If the real 057 identity population presents many distinct grant-unions over the app
shell's UAF domain, K2 fragments, the seed cannot cover it, and the design degrades to Option 2 —
correct and hittable, but paying the filter on most requests.

**This is measurable before a line of code is written, and it should be.** Per
`feedback_baseline_before_speculation` and `feedback_falsifier_first_before_ship`: take the RBAC
snapshot, take the three uncacheable app-shell widgets, compute the domain-scoped grant-union digest
for every identity that has hit the pod, and **plot the distribution of distinct digests**. If the
answer is "1 to 3", ship 3 with confidence. If it is "hundreds", ship K1 (Option 2) and stop, because
K2 is not paying for itself.

### 7.3 The `RBACSubGen` removal sweep

**Smaller but sharper:** §1.3 requires `RBACSubGen` to be removed from `ComputeKey`, and
the removal must be verified by enumerating **every** read site, not the set this design named
(`feedback_enumerate_all_write_sites_not_the_designs_named_set`). The grep I ran returns one write
(`helpers.go:275`) and one read (`resolved.go:844-847`). Two greps are not an enumeration; the dev
brief must require the full sweep including `_test.go` (`rbac_subgen_test.go:115` hard-pins
`resolvedKeyVersion == "v6"` and **will go red** — that is expected and must be *edited*, not
deleted; `feedback_diff_for_deleted_tests_not_just_added`).
