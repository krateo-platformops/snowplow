All load-bearing claims are now verified against source this session. I have what I need. Returning the gated design doc.

---

# v7 Step 2 — DARK requester-profile R, projection P, shadow-parity — ARCHITECT DESIGN (revised for PM gate)

**Stage:** architect (ship loop). **Output: design only. No code. Nothing committed. Authorizes no code yet — this doc goes to the PM gate.**
**Owned repo:** `krateo-platformops/snowplow` (hyphen org; no-hyphen org OFF-LIMITS).

**Sources verified this session.** Every citation is tagged **TRACED** (I read it at the cited file:line this session — `origin/main` = `a6b9d348` for `internal/rbac/*`, `internal/cache/rbac_snapshot.go`, `internal/resolvers/restactions/api/refilter.go`, and the EvaluateRBAC call-site grep; `origin/feat/v7-access-domain-deriver` for `access_domain.go`; the 057 dumps in scratchpad) or **INFERRED** (reasoned, or a seam I did not personally re-open — the dev grep-confirms before coding). Files dumped to `…/scratchpad/s2-arch-*.go`.

**Dark invariant (holds for all of Step 2; every seam is re-proven against it in §4):** R / P / digest / shareability are computed per served resolve, emitted as counters and hashed logs, **never folded into any cache key, never stored on any cache entry, never able to change any verdict or any served byte.**

**Out of scope (Step 3 — seams in §5.2, not designed here):** the v6→v7 `ComputeKey` change; the `EvaluateRBAC` pin branch; `declineUAFPut` / `declineWidgetUAFPut` removal; the clientconfig-cert subject reader. Step 2 leaves `ComputeKey`, the decline helpers, and the key version untouched.

---

## 1. Adversarial findings — resolution ledger

Both reviewers concurred: **R (the second RBAC reading) is sound by construction; dark-safety and the acceptance mechanism were not.** All blockers and majors are resolved below. IDs: R1 = reviewer 1, R2 = reviewer 2.

| # | Severity | Finding (one line) | Verdict | Where handled |
|---|---|---|---|---|
| **R1‑B1** | BLOCKER | Shadow‑parity validates R (full‑domain, real name), never P (the name‑blind per‑class projection that feeds the digest). A name‑specific verb covered by a name‑free class → P computed at `Name=""` under‑reports a `resourceNames`‑scoped grant; F‑verdict & F‑coverage stay green; two identities get an identical digest → cross‑tenant leak once Step 3 shares the cell. | **RESOLVED** | §2.4 structural name‑ambiguity rule (leak made structurally impossible) + §2.5 F‑proj shadow arm + §6 falsifier |
| **R2‑B1** | BLOCKER | The dark hook and the inline #265 deriver run on the live serving path with **no panic isolation** and an unspecified `ctx.Value` assertion → an uncaught panic crashes the real request. | **RESOLVED** | §4.1 recover‑wrapped hook + comma‑ok ctx read + `dark_panic_total`; §6 panic‑injection arm |
| **R1‑M1** | MAJOR | "Caught before any digest is trusted" overreaches — shadow‑parity is runtime‑only; counters==0 over one acceptance run is not domain coverage; unexercised or coincidentally‑covered paths pass and leak in Step 3. | **RESOLVED** | §5.1 two‑part unlock: **offline completeness** over the 057 corpus + **runtime confirmation**, fail‑closed default for any unverified coordinate; per‑resolve coverage attribution |
| **R2‑M1** | MAJOR | Cost‑bound (no‑op without shadow context) contradicts the denominator gate (`checks_total` tracks `evaluateRBACCallCount`): the CPU‑dominant refresher path carries no shadow context, so the gate can read green while the comparator never ran on served‑byte checks. | **RESOLVED** | §4.3 denominator redefined to *served‑path checks*; served sites enumerated; refresher deliberately excluded with proof |
| **R2‑M2** | MAJOR | The `routeRBSubjects` extraction edits production `selectRBCandidates` but has **no behavior‑preserving arm** (F‑C1 compares two callers of the same refactored routing → cannot catch a regression in the extraction). | **RESOLVED** | §6 **F‑C0** byte‑identical arm, symmetric to F‑D1 |
| **R2‑M3** | MAJOR | Hook‑on doubles authz work on the hottest served path; running it in the north‑star latency window corrupts latency acceptance; budget was "I lean full‑during‑acceptance," unmeasured. | **RESOLVED** | §5.1 correctness‑acceptance and latency‑acceptance are **separate runs, hook never on in a latency window**; §7 build‑step E requires a measured pprof per‑check bound |
| **R1‑m1** | MINOR | `ClassWildcard` projection under‑specified on `resourceNames`; could silently repeat R1‑B1 for a name‑specific‑verb wildcard. | **RESOLVED** | §2.4 the name‑ambiguity rule subsumes it: a name‑specific‑verb **or verb‑wildcard** wildcard class is name‑ambiguous ⇒ not shareable (no atom‑canonicalization change needed) |
| **R2‑m1** | MINOR | Mismatch WARN / counter labels risk emitting identity (`opts` carries Username, Groups). | **RESOLVED** | §4.4 WARN emits only non‑identity coordinate + hashed identity; counter labels carry `{verb,group,resource,scope‑kind}` only |
| **R2‑m2** | MINOR | `EvaluateRBAC` has **unnamed returns** (evaluate.go:167); the defer needs a named‑return + `snap`‑hoist refactor, presented as if it already existed. | **RESOLVED** | §4.2 refactor called out explicitly; §6 **F‑D6** extended to sha256 the verdict+UID table *before/after the signature change itself* |

**Verified against source (TRACED this session):** `EvaluateRBAC` returns `(bool, string, error)` **unnamed** at evaluate.go:167; `snap` first assigned at evaluate.go:206 (after the cache‑off early return at :171‑183 and nil‑watcher return at :186‑199) — the named‑return + hoist is real. `rebuildRBACSnapshot`'s own goroutine already `recover()`s at rbac_snapshot.go:324‑333 — the hook lacking one is the asymmetry R2‑B1 names. `evaluateRBACCallCount.Add(1)` at evaluate.go:169 counts **every** call including the refresher; the SkipBindingUID comment (evaluate.go:107‑111) records the SA‑refilter/refresher path as ~43% of pod CPU and (evaluate.go:277‑278) the 160M‑hit population as permit‑dominated — R2‑M1's denominator problem is real.

---

## 2. Profile R, projection P, and the leak‑closing name‑fidelity rule

### 2.1 R is a second RBAC reading assembled ONLY from evaluator‑owned functions

R must agree with `EvaluateRBAC` over the **whole domain, not just observed rows**. The anti‑drift guarantee is: **R calls the evaluator's own unexported functions verbatim**, so it lives in `package rbac` (new file `internal/rbac/profile.go`).

The five reused functions (all **TRACED**): `selectCRBCandidates` (evaluate.go:453‑500, cluster‑wide; `system:authenticated` gated on `Username!=""` at :488‑490; `CRBsCatchAll` always at :497); `anySubjectMatches` (evaluate.go:709‑738; `system:authenticated` gate at :728); `effectiveGroups` (evaluate.go:758‑769) + `parseServiceAccountUsername` (evaluate.go:774‑785); `rulesPermit` (evaluate.go:597‑614) → `stringSliceMatches` (:685) + `resourceNameMatches` (:666‑681); and the resolve‑half split out of `roleRefPermits` (§2.2).

Two mechanical splits, each behavior‑preserving:
- **`rulesForRef(snap, ns, ref, recordMiss)`** — the resolve half of `roleRefPermits` (evaluate.go:558‑585). Mirrors all four arms verbatim: `ClusterRole` miss records + denies (:564); `Role`‑in‑CRB (`ns==""`) denies **without** record (:570‑574); `Role` miss records + denies (:577); unknown kind denies without record (:582‑583). `roleRefPermits` becomes `rules,ok := rulesForRef(...,true); if !ok {return false,nil}; return rulesPermit(rules,opts),nil` — byte‑identical. **R calls it with `recordMiss=false`** — the *only* deliberate difference, so the dark second reading never perturbs the AC‑B.10 miss ratio.
- **`routeRBSubjects(...)`** — the User/Group/`system:authenticated`‑gated/SA/catch‑all routing body of `selectRBCandidates` (evaluate.go:527‑543), extracted so `selectRBCandidates` (per‑ns inner maps) and the new `selectRBCandidatesAllNS` (§3, flat maps) share **one** routing function ⇒ V1/V3 drift closed by construction.

```
type RequesterProfile struct {
    Gen             uint64
    ClusterRules    []rbacv1.PolicyRule            // union over matching CRBs — every namespace
    NamespacedRules map[string][]rbacv1.PolicyRule // per-ns union over matching RBs — that ns only
}
// BuildRequesterProfile: for each selectCRBCandidates(snap,id) gated by anySubjectMatches,
//   append rulesForRef(snap,"",crb.RoleRef,false); for each selectRBCandidatesAllNS(snap,id)
//   gated by anySubjectMatches, append rulesForRef(snap,rb.Namespace,...,false) into NamespacedRules[rb.Namespace].
func (p *RequesterProfile) Permits(opts EvaluateOptions) bool {
    return rulesPermit(p.ClusterRules, opts) ||
        (opts.Namespace != "" && rulesPermit(p.NamespacedRules[opts.Namespace], opts))
}
```

**R correctness theorem (INFERRED, discharged by §3 + §6):** `Permits` reuses `rulesPermit`/`resourceNameMatches`/`stringSliceMatches` **verbatim**, and the verdict is order‑independent + any‑match‑permits (TRACED: comment evaluate.go:340‑343; RBAC v1 has no deny rules). Therefore for any coordinate, **`Permits(opts) == EvaluateRBAC(opts).allowed` given equal rule sets**. All R correctness reduces to *rule‑set fidelity* — candidate selection (§3) and the divergence table (§6). **R stores raw `PolicyRule`s and re‑runs `resourceNameMatches` — R itself is fully name‑faithful.** The name‑fidelity leak (R1‑B1) is **not in R; it is in P** (§2.4). **Seam:** R yields the boolean only; it cannot reproduce `matchedBindingUID` (first‑match is order‑dependent) — Step 3 must not derive BindingUID from R.

### 2.2 R memo

Keyed `(Username, canonicalGroupsHash(Groups), snap.PublishSeq)` — the exact identity shape of the L2 authz memo (TRACED evaluate.go:231‑241; `canonicalGroupsHash` order‑independent at :234). Lifetime = one RBAC generation, per‑generation sharded map whole‑shard‑swapped on `PublishSeq` change, mirroring `snapshot_authz_memo.go` (**INFERRED** — reader‑traced pattern; dev confirms the shard‑swap API). R is built ~once per (identity, generation) and reused across every per‑item check; a revoke bumps `PublishSeq` and drops the stale‑gen R atomically.

### 2.3 Projection P and the digest

P projects R onto each `AccessClass c ∈ D.Classes` using the **exact evaluator predicate** — build `opts_c` from `(Verb, Group, Resource, Namespace, Name)` and call `Permits(opts_c)`. Per class kind: `ClassExact` → one bool; `ClassNamespaceSet(v,g,r)` → `(clusterPermit, {ns : rulesPermit(NamespacedRules[ns], opts_c)})`; `ClassWildcard` → the set of R atoms whose `(verb,group,resource)` match under `stringSliceMatches`. `D.Escapes` present → cell not shareable.

**Digest** = `sha256(D.Canonical() ‖ P's per‑class answers in D's sorted class order)`, US/RS‑delimited (`\x1f`/`\x1e`) as in `AccessDomain.Canonical()` (TRACED access_domain.go:184‑193). **The digest is hashed into nothing in Step 2**: it is computed on the resolve's transient shadow context, emitted as `v7_digest_cardinality`, logged beside the real v6 `key_hash`, and discarded. No cache entry field is added; `ComputeKey` is untouched.

### 2.4 The leak‑closing structural rule (resolves R1‑B1 and R1‑m1)

**Root cause (TRACED):** for a name‑specific verb (`get/update/patch/delete`, evaluate.go:624‑629), a `resourceNames`‑scoped rule matches only when `opts.Name ∈ rule.ResourceNames` (evaluate.go:666‑681). But the covering class can be **name‑free**: `nsset`/`wild` canonicals carry no name (access_domain.go:102‑113); a static‑resource UAF emits `nsSet(uaf.Verb,…)` (access_domain.go:420), a `resourcesFrom` UAF emits `wildcard(uaf.Verb,…)` (:418), a templated‑name GET emits `exact(get,…,"")` or `nsSet(get,…)` (:485,:490,:494). P is a pure function of `(R,D)` and is name‑blind for those classes → it computes at `Name=""` → `resourceNameMatches` returns false for a `resourceNames` grant → P under‑reports. Meanwhile the real refilter threads the per‑object name for name‑specific verbs (**TRACED** refilter.go:485‑505 gate on `IsNameSpecificVerb`, EvaluateRBAC with `Name` at :529‑545) → R.Permits(real name) == EvaluateRBAC (F‑verdict green) and the coordinate is covered (F‑coverage green). The old design's two arms both stay green while two identities collapse to one digest.

**Concreteness (TRACED, 057 this session):** 70 UAF stanzas — **64 `list`, 5 `create`, exactly 1 `get`** (`verb=get, group=kagent.dev, resource=agents`, static → `nsSet("get","kagent.dev","agents")`, name‑free). `roles-mf.json` has **97** `resourceNames`‑scoped rules, `clusterroles-mf.json` **23**, verbs including `get/update/patch/delete`. So a `resourceNames:[X]` get grant on `agents` is a live, reachable leak vector. (The 6 `resourcesFrom` UAFs are all `create`/`list` — collection verbs, so `resourceNames` cannot apply today; but R1‑m1 must still be closed structurally for future name‑specific `resourcesFrom`.)

**The rule — a class is *name‑ambiguous* iff its verb is name‑specific (or the verb is `*`) AND it does not pin a literal object name:**
> `nameAmbiguous(c) := (IsNameSpecificVerb(c.Verb) ∨ c.Verb=="*") ∧ (c.Kind∈{ClassNamespaceSet,ClassWildcard} ∨ (c.Kind==ClassExact ∧ c.Name==""))`

A `ClassExact` with a literal `Name!=""` is name‑pinned → P faithful → shareable. A collection‑verb class (list/watch/create/deletecollection) is **never** name‑ambiguous — `resourceNames` cannot apply, so name‑free P at `Name=""` is exactly what the real collection check evaluates. A verb‑`*` wildcard IS name‑ambiguous (it subsumes get) — this is what closes R1‑m1 **without** touching atom canonicalization.

P computes a cell‑level **`shareable`** predicate (the dark answer Step 3 will consume): `shareable := (len(D.Escapes)==0) ∧ (∄ c∈D.Classes: nameAmbiguous(c)) ∧ digestTrusted`. In Step 2 `shareable` is **emitted as a counter only** (`v7_cell_not_shareable_total{reason}`) — it gates nothing yet. Because a name‑ambiguous cell is structurally never shareable, **two identities differing only in a `resourceNames` grant can never share a key** — the leak is impossible even on paths no acceptance run exercises. This is cluster‑state‑independent (no dependence on whether a `resourceNames` rule happens to exist), per the "relax only on structural proof" rule.

**Known conservatism (honest residual, zero Step‑2 cost).** The rule also sidelines cells whose real check is genuinely name‑free — notably widget `resourcesRefs` via `UserCan`, which passes `Name=""` (**TRACED** rbac.go:45‑52), where P at `Name=""` would actually be faithful. In Step 2 this costs nothing (dark). In Step 3 it would shrink subset‑serve sharing. **Refinement path (Step‑3 / Step‑2a coordination, noted not designed):** annotate each `AccessClass` in the deriver with whether its originating step threads a per‑object name (UAF‑name‑specific and dispatch‑GET‑templated‑name do; `UserCan` does not), letting the projection treat only genuinely name‑carrying classes as ambiguous. That is a #265/D change — flagged for `dev-domain-deriver`. §5.1's measurement reports exactly how many cells the conservative rule sidelines, so the PM can decide whether the refinement is worth it.

### 2.5 What shadow‑parity catches vs cannot — explicit

Shadow‑parity runs three arms per served check (§4). **F‑verdict** catches any R↔EvaluateRBAC divergence on an exercised check. **F‑coverage** catches a D under‑approximation on an exercised check (the #265 templated‑prefix blind spot). **F‑proj** (new, resolves R1‑B1's detection half): for every real per‑object check the hook sees — which carries the *real* `(Name,Namespace)` — if every class covering that coordinate is name‑free (`nameAmbiguous`) yet the cell was **not** flagged `shareable==false`, bump `projection_name_ambiguous_leak_total` and mark the digest untrusted (RED in tests). F‑proj is the empirical proof that §2.4's structural rule actually fires on the live `agents` get‑UAF.

**Residuals, named with mitigation:**
- **(residual‑1) A D under‑approximation on a cell+step shape not in the 057 corpus AND coincidentally covered at runtime by another class.** Mitigation: §5.1 makes the unlock **fail‑closed by default** — any D‑class coordinate neither offline‑verified over the full corpus nor runtime‑exercised is treated digest‑untrusted; and the coverage predicate is scoped to the resolve's **own cell D** (not a cross‑cell union), so an unrelated cell cannot mask a miss. The offline direct‑deriver arm (§6 F‑D4‑offline) catches the templated‑prefix under‑approximation at its source over the whole corpus, independent of runtime coincidence.
- **(residual‑2) Within one cell, step X under‑approximates and step Y in the same cell coincidentally covers X's coordinate with a same‑`(v,g,r)` class.** Mitigation: F‑D4‑offline asserts per‑api‑step that a step touching the apiserver is not silently classified external — catching X at derivation, before Y can mask it. Genuinely equal `(v,g,r)` with equal projected answers is not a leak (the digest already reflects that grant); the danger is only mis‑attribution under name‑ambiguity, which §2.4 sidelines wholesale.
- **(residual‑3, accepted) BindingUID is not derivable from R.** Step 3 must not derive it from R (§2.1 seam).

---

## 3. The all‑namespace RoleBinding‑by‑subject reverse index

**Gap (TRACED):** RB subject indexes are namespace‑first — `RBsByUserByNS`/`RBsByGroupByNS`/`RBsByServiceAccountByNS`/`RBsCatchAllByNS` (rbac_snapshot.go:166‑169); `selectRBCandidates` is intrinsically single‑ns (`ns==""`→nil, evaluate.go:508). There is no subject→(all‑their‑RBs) index. R needs O(u's RBs), not O(all RBs).

**Shape** — four flat maps added after rbac_snapshot.go:169, mirroring the CRB indexes: `RBsByUserAllNS`, `RBsByGroupAllNS`, `RBsByServiceAccountAllNS` (key `"<ns>/<name>"`), `RBsCatchAllAllNS`. Values are the **same `*rbacv1.RoleBinding` pointers** already in `RoleBindingsByNS` — no struct copy. Each RB carries its own `.Namespace`, so scope is recoverable per pointer.

**Build site** — inside the **existing** RB switch in `rebuildSubjectIndexes` (**TRACED** rbac_snapshot.go:611‑636): one flat append per arm alongside the per‑ns append (SA arm uses the same `s.Namespace + "/" + s.Name` key as :632, so the flat index matches `selectRBCandidates`'s SA lookup), four maps allocated beside the per‑ns allocations (:599‑602). Same walk, same `s.Kind` routing → the flat index is **structurally the per‑ns index flattened**. An RB has exactly one namespace ⇒ one entry per subject key ⇒ no cross‑ns duplicate, no dedup.

**Consumer** — `selectRBCandidatesAllNS(snap, opts)` calls the shared `routeRBSubjects` (§2.1) with the four flat maps; `BuildRequesterProfile` buckets each returned candidate by `rb.Namespace` after `anySubjectMatches`.

**Write‑site enumeration (TRACED — grep, don't trust the design):** the authz subject maps are written **only** in `rebuildSubjectIndexes` (rbac_snapshot.go:557). The separate `BindingsByGVR` incremental delta (`onBinding{Add,Update,Delete}` / `bindings_by_gvr_delta.go`) is **seed‑targeting only** and does not touch the authz maps (rbac_snapshot.go:867‑871). The flat index rides the wholesale rebuild scheduled on every binding event ⇒ **no new event handler, no incremental‑delta wiring**.

**Additivity (TRACED):** (a) no existing field's construction changes — the per‑ns appends at :611‑636 are untouched; (b) no existing reader references the new fields — `selectCRBCandidates`/`selectRBCandidates` unchanged; (c) same immutability/concurrency — built before `rbacSnap.Store` (:501), read lock‑free, GC'd with the old snapshot (:121‑133); (d) cache‑off: `rebuildRBACSnapshot` early‑returns on `rw.mode == modePassthrough` (:372) so the fields stay nil and no dark reader touches them.

**Scale (INFERRED, anchored on TRACED live figure N₂=63316 RBs at rbac_snapshot.go:356; RBs linear in installs per `project_composition_install_rbac_scale`):** new pointer entries ≈ Σ|RB.Subjects| ≈ 64.6K × 8 B ≈ **0.5 MB** backing arrays; SA‑dimension keys dominate (~up to 50–63K keys × ~48–64 B) ≈ **3–4 MB**; User/Group maps stay at tens of keys (narrow‑RBAC shape). **~4 MB total at 63K RBs, linear, no struct copy** — ~1× the existing per‑ns SA sub‑index flattened. Rebuild delta ≈ 64K appends + 63K map inserts on the existing RB‑subject walk, INFERRED well inside the ≤100 ms envelope; **F‑C2** benchmarks it. **Data to confirm before dev (§5.1 Q5):** #namespaces and RB‑per‑namespace distribution at 50K validates the SA‑key estimate.

---

## 4. Dark‑safety — enumerated seams + proof none changes behavior or leaks identity

### 4.1 The hook is recover‑isolated (resolves R2‑B1)

A `defer` inside `EvaluateRBAC` capturing named returns, with the read‑only hook loaded atomically (nil in production):

```
var snap *cache.RBACSnapshot
defer func() {
    if h := shadowHook.Load(); h == nil || snap == nil || err != nil { return }
    defer func() {                        // panic isolation — REQUIRED (R2-B1)
        if r := recover(); r != nil {
            darkPanicTotal.Add(1)
            markResolveDigestUntrusted(ctx)   // comma-ok read inside, never panics
            // swallow: the live verdict already returned; request is unaffected
        }
    }()
    h(ctx, snap, opts, allowed)           // read-only; CANNOT mutate the return
}()
```

`shadowHook` is `atomic.Pointer[func(...)]`, **nil in production** (one atomic load then return). The **same recover discipline wraps the inline D‑derivation** at the dispatcher entry (which calls `DeriveRESTActionAccessDomain`/`DeriveWidgetAccessDomain` — the widget path does map access + `schema.ParseGroupVersion`, TRACED access_domain.go:305‑334): on recover, bump `darkPanicTotal`, mark digest‑untrusted, proceed with the un‑derived (empty) domain. **The shadow context is read with the comma‑ok form** (`v, ok := ctx.Value(k).(shadowCtx); if !ok { return }`) — never a bare assertion. This matches the process‑poisoning defense the codebase already uses for the rebuild goroutine (TRACED rbac_snapshot.go:324‑333). §6 has the panic‑injection arm asserting the served response is byte‑identical.

### 4.2 Named‑return + snap‑hoist refactor (resolves R2‑m2)

`EvaluateRBAC(ctx, opts) (allowed bool, matchedBindingUID string, err error)` — converting the **unnamed** signature at evaluate.go:167, and hoisting `var snap *cache.RBACSnapshot` above the cache‑off return (evaluate.go:171‑183) with assignment moved to the existing `snap = rw.Snapshot()` at :206. The defer no‑ops when `snap==nil` (cache‑off / pre‑readiness) or `err!=nil`. **F‑D6 is extended** to sha256 the full verdict+`matchedBindingUID` table over 057 **before/after the signature refactor itself** (named returns + a mutating defer are a classic silent‑behavior vector), independently of hook‑on‑vs‑off.

### 4.3 The denominator is the served‑path check population (resolves R2‑M1)

The hook fires only when a **resolve shadow context** is present on ctx; it is installed **only at the served dispatcher resolve entry**. `checks_total`'s denominator is therefore *EvaluateRBAC calls that carry a resolve shadow context* — the served‑byte‑determining checks, **not** `evaluateRBACCallCount`. The served‑path check sites (TRACED via grep this session, all real invocations, not seams):

- `internal/resolvers/restactions/api/refilter.go:529` (UAF refilter, per object — the name‑carrying path)
- `internal/resolvers/restactions/api/informer_dispatch_rbac.go:155` (list‑filter), `:267` (get‑by‑name filter)
- `internal/resolvers/restactions/api/cluster_list.go:238` (cluster‑list gate)
- `internal/objects/informer_serve.go:257` (get‑filter)
- `internal/handlers/dispatchers/helpers.go:250`, `:734` (dispatchCacheLookupKey / diagnostic — the key‑minting sites)
- widget `resourcesRefs` via `rbac.UserCan` → `EvaluateRBAC` (TRACED rbac.go:45)

**Deliberately excluded:** the refresher / RAFullList populate path (`internal/resolvers/widgets/apiref/ra_full_list.go:158`, `ra_full_list_slice.go`) — the ~43%‑CPU / 160M‑hit population (TRACED evaluate.go:107‑111, :277‑278). Safe to exclude because it **populates** the cache; it does not determine a browser‑served body. The served bytes' RBAC correctness is re‑decided on the served path (refilter etc.) regardless of the refresher — that is the stale‑while‑refresh architecture. Excluding it also bounds the dark CPU cost (the hook never fires on the 160M‑hit path). The unlock gate (§5.1) asserts `checks_total == (count of served‑path EvaluateRBAC calls over the window)`, an equality with a defined RHS — never "tracks `evaluateRBACCallCount`."

### 4.4 Identity is never emitted raw (resolves R2‑m1)

`EvaluateOptions` carries `Username` and `Groups` (TRACED evaluate.go:56‑86). The mismatch WARN emits only the non‑identity coordinate `{verb, group, resource, scope‑kind}` plus a **hashed** identity (`canonicalGroupsHash(opts.Groups)` already at evaluate.go:234, and a `sha256(username)` prefix), and a hashed `(namespace,name)` if needed for debugging — **never** raw groups/username/name and **never** a resolved body (consistent with the memory rule that the debug surface never dumps per‑identity cache bodies — inspector = metadata + body sha256 only). Counter labels carry `{verb, group, resource, scope‑kind}` only — no namespace, no name, no username (bounded cardinality).

### 4.5 Seam table — every Step‑2 change and why it is dark‑safe

| Seam (file) | Step‑2 change | Why dark‑safe / non‑leaking |
|---|---|---|
| `evaluate.go` — `rulesForRef` split + `roleRefPermits` rewrite | additive fn; `roleRefPermits = rulesForRef(...,true)+rulesPermit` | byte‑identical verdict path (**F‑D1**; sha256 of full verdict+UID over 057) |
| `evaluate.go` — named returns + `snap` hoist + deferred recover‑wrapped `shadowHook` | signature refactor; one atomic load, nil in prod; read‑only; recover‑isolated | cannot mutate the return; fires only post‑verdict on served checks; **F‑D6** (both refactor‑only and hook‑on‑vs‑off) + panic‑injection arm |
| `rbac_snapshot.go` — 4 flat maps + same‑switch appends | additive fields, additive appends | §3 (a)–(d): no existing field/reader/concurrency change; nil in cache‑off |
| `internal/rbac/profile.go` (new) | R type, builder, `Permits`, `selectRBCandidatesAllNS`, `routeRBSubjects` | reads only; no serving path calls it except the dark hook |
| shadow‑parity subsystem (new file) | hook impl, R memo, F‑verdict/F‑coverage/F‑proj, coverage predicate, P + digest + `shareable`, ctx shadow context, counters | pure computation + counters; no key/entry write; recover‑isolated; identity hashed |
| dispatcher entry (UAF‑touched‑sink install sites — **INFERRED**, dev greps `WithUAFTouchedSink` in `restactions`/`widgets`) | install shadow context; compute `D(CR)` (memoized per CR UID/RV) under recover; thread on ctx | returned ctx carries one extra value key; D is identity‑free (access_domain.go) |
| `helpers.go` `dispatchCacheLookupKey` (**INFERRED**, reader‑traced) | compute P+digest+`shareable` from R+D; emit; **key unchanged** (`cache.ComputeKey` inputs untouched) | digest folded nowhere |
| per‑item check sites (refilter, dispatch get/list, `resourcesRefs`, apiRef) | **none** — they already call `EvaluateRBAC`; the hook fires automatically | funnel means no per‑site edit; kept/dropped/served sets untouched |

**No seam touches `ComputeKey` inputs, any served byte, or any verdict.** Step 2 is a recover‑isolated read‑only observer riding the existing funnel and the existing rebuild.

---

## 5. Open questions for the PM gate, and the Step‑3 seam

### 5.1 The Step‑2 unlock is a coverage‑completeness statement, not counters==0 (resolves R1‑M1, R2‑M3)

Two independent parts; both required to declare a digest trustworthy for Step 3:

1. **Offline completeness (over the full 057 widget/RA corpus + the identity matrix).** Enumerate every D‑class coordinate across the corpus; for each, prove `R.Permits(opts) == EvaluateRBAC(opts).allowed` (F‑D2, exhaustive‑over‑domain, not observed rows). Enumerate every api‑step and assert it is not silently classified external when it hits the apiserver (F‑D4‑offline — catches the #265 blind spot at the deriver). Enumerate every class and assert the `nameAmbiguous`/escape classification (F‑proj‑golden). This proves R‑parity and D‑coverage over the **whole declared domain**.
2. **Runtime confirmation (057 acceptance, correctness window).** `verdict_mismatch_total==0 ∧ coverage_miss_total==0 ∧ projection_name_ambiguous_leak_total==0 ∧ dark_panic_total==0`, with `checks_total == (served‑path EvaluateRBAC count)` (§4.3) and `checks_total>0` for each class coordinate the corpus is expected to exercise. **Fail‑closed default:** any coordinate neither offline‑verified nor runtime‑exercised is treated digest‑untrusted.

**Separation of windows (R2‑M3):** the shadow hook is **NEVER enabled during a window used to accept north‑star latency.** Correctness‑acceptance (hook on) and latency‑acceptance (hook off, production config) are distinct runs. **Acceptance vantage** (per the measurement rules): drive the correctness window through **Chrome → portal → ingress** so resolves fire; read the mechanism from **expvar** counters (not log tails, not kubectl).

**PM gate — technical, team‑decidable:**
1. **Index shape** — recommend four flat maps + factored `routeRBSubjects` (lowest drift; O(routing) per build; clean F‑C0/F‑C1). Alternative (single `RBsBySubjectAllNS` + `RBNamespacesForSubject` projection driving `selectRBCandidates` per ns) is O(#u's‑namespaces × routing) and moves routing into the projection — rejected on cost and drift. **Confirm.**
2. **Hook vs 6 call‑site wrappers** — recommend the deferred nil‑guarded recover‑isolated post‑verdict hook (single funnel; same‑`snap` kills skew; covers `UserCan`; read‑only). Fallback if defer‑cost‑sensitive: explicit hook calls at the two return sites (evaluate.go:249, :301). **Confirm.**
3. **Dark‑cost budget at 50K** — R‑per‑(identity,gen) memo makes R ~once per identity per generation; F‑verdict/F‑coverage/F‑proj run per served check. §7 step E delivers a **measured** pprof per‑check bound before any Step‑3 promotion (no magic env knob to gate key logic). Recommend full shadow‑parity during the correctness window, off in steady state. **Confirm.**
4. **PR #265 sequencing** — F‑coverage/F‑proj depend on D. #265 is draft (`feat/v7-access-domain-deriver`). Step‑2 correctness needs it merged or the deriver imported. **Confirm sequencing** (coordinate with `dev-domain-deriver`).
5. **Name‑ambiguity conservatism vs the §2.4 refinement** — the conservative rule sidelines genuinely‑name‑free cells (e.g. widget `resourcesRefs`/`UserCan`). Step 2 measures `v7_cell_not_shareable_total{reason}`; the PM decides whether the per‑origin `NameThreaded` annotation (a #265/D change) is worth doing before Step 3. **Confirm** whether Step 2 ships the conservative floor (recommended) or blocks on the refinement.
6. **Data to confirm before dev** (baseline‑before‑speculation): #namespaces + RB‑per‑ns distribution at 50K (validates §3's ~4 MB SA‑key estimate); steady‑state `RecordRBACSnapshotMiss` rate <1%; (Username, GroupsHash) identity cardinality from 057 (sizes the R memo).

### 5.2 Step‑3 seams (noted, NOT designed)

The v6→v7 `ComputeKey` change + a `WrittenAtSeq` fail‑closed predicate that promotes a **shareable** digest to key material; the `EvaluateRBAC` pin branch; `declineUAFPut`/`declineWidgetUAFPut` removal (`uaf_shortttl.go`) — gated by the `shareable` flag so name‑ambiguous cells keep declining; the clientconfig‑cert subject reader (Q3: authn writes `CN=username, Organization=groups`; snowplow caches the Secret but does not yet parse it) feeding prewarm representative identities — where the representative‑vs‑browser identity divergence (#224/#180) must be retired; the §2.4 per‑origin `NameThreaded` refinement. `matchedBindingUID` is not derivable from R.

---

## 6. Falsifier plan — RED‑first control matrix

Each arm goes RED when its one defect is injected, GREEN otherwise, sha256‑clean between arms (arm‑that‑cannot‑fail is not coverage), driven on the **real dispatch** not a seam, with User **and** Group carriers (subject‑kind symmetry). New/changed arms vs the prior design are marked **[NEW]**.

- **F‑D1** (split behavior‑preserving) — over 057, sha256 of the full `EvaluateRBAC` verdict+UID table is byte‑identical before/after the `rulesForRef` split; `rulesPermit(rulesForRef(snap,ns,ref,true),opts) == roleRefPermits_old(...)` over a `(ref,ns,opts)` corpus.
- **[NEW] F‑C0** (routeRBSubjects extraction behavior‑preserving — resolves R2‑M2) — over the 057 candidate corpus, sha256 of the **ordered** candidate slice from the refactored `selectRBCandidates` == the pre‑refactor `selectRBCandidates`, for {User, Group, multi‑Group, SA, empty‑username, catch‑all‑kind}. Symmetric to F‑D1; only then does F‑C1 (AllNS equivalence) rely on the shared routing.
- **F‑C1** (reverse‑index correctness) — `selectRBCandidatesAllNS(snap,id)` (pointer set) == `⋃_ns selectRBCandidates(snap,ns,id)` over 057 for the identity matrix; plant one RB in a dropped namespace → RED.
- **F‑C2** (index cost) — benchmark `rebuildSubjectIndexes` at 63,316 RBs; assert <100 ms and flat‑index delta <~50% over the pre‑index RB‑subject build. Hermetic, all cluster paths isolated.
- **F‑D2** (parity golden, exhaustive over D) — for K>1 identities (User + Group) × the class coordinates of D across the widget/RA corpus (**not** observed rows), assert per‑coordinate `R.Permits(opts) == EvaluateRBAC(opts).allowed`. Carries V1/V3/V4/V6/V7/V8/V11/V12 sub‑arms, each injected alone.
- **F‑D2div** (the divergence arm) — a naive R that flattens `ResourceNames` answers `allow` for `list` and `get bar` where EvaluateRBAC denies both; assert `verdict_mismatch_total` **fires** (RED) against naive R and is 0 (GREEN) against the real `Permits`. Proves shadow‑parity *catches* a real divergence.
- **[NEW] F‑proj** (name‑fidelity leak arm — resolves R1‑B1 detection) — two identities differing **only** in a `resourceNames:[X]` get grant on the live `agents` get‑UAF cell: assert **both cells are flagged `shareable==false` (nameAmbiguous)** and that their would‑be digests are therefore never treated as shareable. RED if either cell is marked shareable OR if a name‑carrying real check (refilter, `Name!=""`) is covered only by a name‑free class in a cell the projection treated as shareable. This is the arm that proves §2.4 fires.
- **[NEW] F‑proj‑golden** (R1‑m1) — a byte‑level golden asserting a name‑specific‑verb wildcard class and a verb‑`*` wildcard class are both classified `nameAmbiguous`; RED if a wildcard class with a name‑specific/`*` verb is treated shareable.
- **F‑D4** (runtime #265 templated blind spot) — a RESTAction whose api‑step builds the whole `/apis/` prefix inside `${…}` (TRACED access_domain.go:600‑621) → D emits no class → resolve it → `coverage_miss_total` increments while `verdict_mismatch_total` stays 0. RED against a shadow impl that only compares in‑domain coordinates.
- **[NEW] F‑D4‑offline** (coverage completeness at the source — resolves R1‑M1 / residual‑2) — over every api‑step in the 057 corpus, re‑derive the step path and assert an apiserver‑touching step is not classified external. Catches the under‑approximation over the whole corpus, independent of runtime coincidental coverage.
- **F‑D5** (dark miss‑counter neutrality) — deleted‑role arm: R contributes ∅ for a dangling roleRef AND the `RecordRBACSnapshotMiss` delta attributable to dark R == 0 (`recordMiss=false`).
- **F‑D6** (dark no‑behavior‑change, **extended**) — (a) over the dispatchers' `a1_uaf_*` corpus, sha256 of served bodies **and** the exact minted‑key set, hook ON vs OFF, identical; `WidgetsUAFPutDeclined`/`RestactionsUAFPutDeclined` deltas unchanged; **(b) [NEW]** the same verdict+UID table byte‑identical before/after the named‑return + `snap`‑hoist signature refactor itself (R2‑m2).
- **[NEW] F‑panic** (recover isolation — resolves R2‑B1) — inject a panic in the hook body and in the inline D‑derivation; assert the served response is byte‑identical to hook‑off, the request does not fail, and `dark_panic_total` increments with the digest marked untrusted. Include a ctx key/type‑collision arm proving the comma‑ok read does not panic.
- **F‑D7 / F‑D7b** — memo invalidates on `PublishSeq` (a revoke bumps gen → stale‑gen R not reused); digest deterministic across N goroutines.
- **F‑2mutant** (two‑mutant control) — inject V7 (resourceNames flattening in R) **and** V9 (a templated‑prefix step) in one run; assert `verdict_mismatch_total>0` **and** `coverage_miss_total>0` fire independently, with single‑defect controls showing exactly one arm each.
- **F‑H5** (counter‑as‑detector) — on a real‑resolve run assert `checks_total>0` so a zero mismatch reads as health only when the comparator ran; and `checks_total == served‑path EvaluateRBAC count` over the window (§4.3).
- **Seed falsifier** (mandatory on L1‑key‑adjacent snapshot changes) — `TestBootSeedCoverage_ShadowParityDark`: the boot snapshot builds the four flat maps (guards a half‑done migration) and the seeded corpus yields `coverage_miss_total==0 ∧ projection_name_ambiguous_leak_total==0`.

**Divergence table (R vs EvaluateRBAC) — each row's catch:** V1/V3/V4/V5/V6/V11/V12 closed **by construction** (R reuses the evaluator's own functions); V2 (new AllNS path) → **F‑C0 + F‑C1**; V7/V8/V9 caught empirically over the domain (F‑D2/F‑D4/F‑D4‑offline); **V‑proj (name‑free class under‑reports name‑specific grant, R1‑B1)** → structurally sidelined (§2.4) + **F‑proj**; V10 (generation skew) impossible — the hook builds R from the check's own `snap`.

---

## 7. Build order for the dev within Step 2 — smallest safe first, each with its own gate

Every sub‑step is independently dark and additive; each is one SHA on a clean tested tree (`go test` after commit), diff‑reviewed with architect + PM before commit, with an explicit ACK step. **This doc authorizes none of it yet — it is the plan the PM gate rules on.**

- **Step A — all‑namespace RB reverse index** (`rbac_snapshot.go` flat fields + same‑switch appends; `routeRBSubjects` extraction; `selectRBCandidatesAllNS`). Pure additive + dark; no R yet. **Gate:** F‑C0 (behavior‑preserving `selectRBCandidates`), F‑C1 (union equivalence), F‑C2 (63K cost), plus a verdict‑table sha256 proving no serving change.
- **Step B — `rulesForRef` split + R builder + `Permits` + R memo** (`evaluate.go` split; new `profile.go`). Library only; no serving path calls R. **Gate:** F‑D1, F‑D2, F‑D2div, F‑D5, F‑D7.
- **Step C — projection P + digest + `shareable` (name‑ambiguity/escape)** (pure functions in the shadow‑parity subsystem file). Still nothing wired. **Gate:** F‑D7b (digest determinism), F‑proj‑golden + F‑proj offline half (two‑identity `resourceNames` discriminates or both non‑shareable), F‑D4‑offline (D‑completeness over the corpus).
- **Step D — shadow‑parity hook wired dark** (named‑return + `snap`‑hoist refactor; recover‑isolated `shadowHook`; comma‑ok ctx read; dispatcher entry installs shadow context + recover‑isolated inline D; `helpers.go` computes P+digest, **key unchanged**). **Gate:** F‑D6 (both halves), F‑panic, F‑verdict/F‑coverage/F‑proj RED‑first, F‑2mutant, F‑H5, seed falsifier.
- **Step E — measure (authorizes Step‑3 *discussion*, not Step‑2 code beyond D)** — 057 correctness‑acceptance run (hook on, Chrome→portal→ingress, expvar); a **separate** latency run (hook off) confirms north‑star is uncorrupted; pprof per‑check F‑verdict+F‑coverage+F‑proj cost at 50K with a stated bound. **Gate:** the §5.1 two‑part unlock — offline completeness + runtime counters all zero + `checks_total` denominator equality + measured cost bound. Report `v7_digest_cardinality` and `v7_cell_not_shareable_total{reason}` as the Step‑3 go/no‑go inputs.

---

**Verdict:** buildable, and every blocker/major from both reviews is resolved. R is assembled only from reused evaluator functions plus two behavior‑preserving splits (`rulesForRef` with `recordMiss`; `routeRBSubjects` shared by both selectors, now guarded by F‑C0). The name‑fidelity leak (R1‑B1) is closed **structurally** — name‑specific‑verb name‑free classes are non‑shareable, cluster‑state‑independent, verified by F‑proj on the live `agents` get‑UAF — which also subsumes R1‑m1. The hook is recover‑isolated with a comma‑ok ctx read (R2‑B1); the denominator is the enumerated served‑path population with the refresher deliberately excluded (R2‑M1); the extraction has a byte‑identical arm (R2‑M2); correctness and latency acceptance are separate runs with a measured cost bound (R2‑M3); identity is only ever hashed (R2‑m1); the named‑return refactor is called out and F‑D6‑guarded (R2‑m2); the unlock is offline completeness + fail‑closed runtime confirmation, not counters==0 (R1‑M1). Named residuals (§2.5) all have mitigations. **Design only — nothing committed; no code authorized until the PM gate rules on §5.1's questions.**