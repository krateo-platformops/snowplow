---
type: Architecture
title: Snowplow `/rbac` endpoint — RESTAction→GVR inspect-only enumeration (REVISED design)
description: "Revised design for GET /rbac: RESTAction-to-GVR inspect-only read-set enumeration for core-provider RBAC."
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [archive]
timestamp: 2026-06-23T00:00:00Z
---

# Snowplow `/rbac` endpoint — RESTAction→GVR inspect-only enumeration (REVISED design)

Date: 2026-06-23
Anchor: canonical `~/krateo/snowplow-cache/snowplow` @ `1.4.3-1-g4062db0`.
Status: REVISED per PM gate (REVISE) + Diego auth ruling. Doc-only; no code in this pass.
Supersedes the prior finalized design on exactly two premises (Corrections 1 and 2 below);
everything else is carried forward verbatim (PM-blessed).

> Note for §4 / §7.1 of the prior draft: the directive "omit the UAF stages from the
> enumerated read-set" is **REVERSED** — UAF stages are **EMITTED** (with their verb).
> See Correction 1.

---

## 0. Purpose

core-provider needs to know, for a given RESTAction, the full set of
`(group, resource, verb)` tuples that resolving it will read — so it can pre-generate
the RBAC (Roles/RoleBindings) a user needs *before* the first `/call`. `/rbac` enumerates
that read-set from the RA's `api[]` stages **without dispatching** (no apiserver reads of
the referenced data, no per-user creds required) and returns a canonical, deduped,
sorted list.

---

## 1. Prior-art check (k8s / client-go)

- `SelfSubjectRulesReview` (authorization/v1) enumerates what *a caller* may do, not what a
  *RESTAction* will read — wrong axis. We need the RA's static read-set, derived from the
  blueprint, before any binding exists.
- `RESTMapper` / discovery (`discoveryClientFor`, discovery_dispatch.go:114) maps
  group/version → resource plurals; we **reuse** it (already in-process). It does not, on its
  own, walk an RA's `api[]` + `dependsOn` graph — that walk is snowplow-specific.
- No client-go primitive walks a snowplow RA blueprint. So the *enumeration* is new; every
  *primitive it calls* is existing snowplow code (§3). Nothing reinvented.

---

## 2. Root-axis facts (all TRACED against the anchor)

| Fact | Where | Status |
|---|---|---|
| HTTP-stage `self.verb` is CEL-bound to GET/HEAD (case-insensitive) | `apis/templates/v1/core.go:30` | TRACED |
| `self.userAccessFilter.verb` is **only** CEL-required non-empty — NOT member-bound | `apis/templates/v1/core.go:32` | TRACED |
| `UserAccessFilterSpec.Verb` is a **free-form lower-case string** ("get, list, watch, etc.") | `apis/templates/v1/core.go:139-141` | TRACED |
| refilter passes `Verb: uaf.Verb` **verbatim** into `EvaluateRBAC` | `internal/resolvers/restactions/api/refilter.go:257-267` (line 260) | TRACED |
| `/call` mounts on `middleware.UserConfig(*signKey, *authnNS)` | `main.go:813-814` | TRACED |
| `UserConfig` = JWT-validate (authn) + load `<user>-clientconfig` Endpoint into ctx | `internal/handlers/middleware/userconfig.go:151,229-233` | TRACED |
| `/refreshes` mounts WITHOUT `RegisterScopedRoute` (zero apiserver reads → outside read-path-scoped invariant) | `main.go:825,831-833` | TRACED |
| `RegisterScopedRoute` is what enrolls a route in the per-user apiserver-read scope invariant | `internal/cache/fallthrough_middleware.go:142-158` | TRACED |
| Enumeration primitives all exist in-process | topologicalSort `sort.go:9`; ParseAPIServerPathToDep `internal/cache/inventory.go:251`; discoveryClientFor `discovery_dispatch.go:114`; createRequestOptions `setup.go:28`; `ServiceAccountRESTConfig` `internal/dynamic/sa_client.go:158` | TRACED |

---

## 3. Reuse map — KEPT UNCHANGED (PM-blessed)

The enumeration body is unchanged from the prior finalized design:

- **No-dispatch path-eval**: `createRequestOptions(ctx, log, in, dict)` (`setup.go:28`) resolves a
  stage's `path` from the (possibly empty) dict WITHOUT issuing the HTTP/apiserver call —
  use it to materialize each stage's concrete path, then classify.
- **Path → GVR**: `ParseAPIServerPathToDep(path)` (`inventory.go:251`) yields
  `(gvr, namespace, name, ok)` for an in-cluster apiserver-shaped path.
- **Discovery**: `discoveryClientFor(rc)` (`discovery_dispatch.go:114`) on
  `ServiceAccountRESTConfig()` (`sa_client.go:158`) resolves bare `/apis/<g>/<v>` discovery-shaped
  stages to their resource plural set — **SA discovery, dispatch-free**.
- **Dependency order**: `topologicalSort(items)` (`sort.go:9`) — **corrected location** (prior
  draft mis-cited `resolve.go:1480`). Used to evaluate `dependsOn` edges in order so a
  stage's path that templates off an upstream stage's (empty-dict) output is still walkable.
- **dependsOn cutoff**: empty-dict eval + parser rejection — a path that cannot be
  materialized from the empty dict (because it genuinely needs upstream *data*, not just
  shape) is reported, not silently dropped.
- **Permissive default**, **fail-loud non-200 enumeration**, **canonical dedupe/sort**: unchanged.
- **Zero dispatch-path modification**: `/rbac` is a read-only sibling endpoint; it does not
  touch `Resolve`, the dispatcher, or the refilter.

The ~280-LOC sketch is carried forward **minus the ServiceAuth middleware** (see Correction 2):
`serviceauth.go` is **dropped**; `/rbac` mounts on the existing `UserConfig`.

---

## 4. Correction 1 — UAF stages are EMITTED (with their verb); they are NOT omitted

### Root cause of the prior error (TRACED)
The prior draft asserted "CRD CEL restricts `userAccessFilter.verb` to get/head/list, so a
verb-less response is provably safe, and UAF stages can be omitted from the read-set." That is
**FALSE**:
- `core.go:30` bounds **`self.verb`** — the *HTTP stage method* — to GET/HEAD.
- `core.go:32` only requires **`self.userAccessFilter.verb != ''`** (non-empty); it does **not**
  constrain it to any member set.
- `core.go:139-141`: `UserAccessFilterSpec.Verb` is a free-form lower-case string
  ("get, list, watch, etc.").
- `refilter.go:260`: the refilter calls `EvaluateRBAC(Verb: uaf.Verb, …)` **verbatim**.

So `uaf.Verb` can be `watch`, `deletecollection`, or anything else. A verb-less `/rbac`
response paired with a consumer that grants only a fixed `{get,list,watch}` read-set would
**UNDER-GRANT** any UAF stage whose verb falls outside that set → silent under-read at first
`/call`. The "CEL guarantees it" claim is **struck entirely**.

### Fix — emit `uaf.Verb` on UAF rows (PM-preferred)
A UAF stage contributes a read-set row carrying its **own** verb so the consumer grants
exactly that verb. The verb the SA-dispatch+refilter actually checks is `uaf.Verb`
(refilter.go:260) on `(uaf.Group, resource∈resolveUAFResources)` — that is precisely what
the user must hold, so that is precisely what we emit.

### Response shape (build-ready)

```json
{
  "restaction": { "name": "compositions-get-ns-and-crd", "namespace": "krateo-system" },
  "readSet": [
    { "group": "",                         "resource": "namespaces",     "verb": "get" },
    { "group": "apiextensions.k8s.io",     "resource": "customresourcedefinitions", "verb": "list" },
    { "group": "composition.krateo.io",    "resource": "fireworksapps",  "verb": "watch" }
  ],
  "unresolved": [
    { "stage": "detail", "reason": "unrenderedTemplate", "message": "stage path is not enumerable: unrendered ${...} template (#293)" }
  ]
}
```

### Status and the `unresolved` entries (#1619)

- **200**: every stage was enumerated. The body is `{restaction, readSet}`; there is **no**
  `unresolved` key.
- **422**: at least one stage was not. The body is **the same JSON** plus `unresolved`, and
  `readSet` holds the rows of the stages that **were** enumerated, in exactly the 200's row
  shape (one decoder reads both). The status is what says the read-set is incomplete: do not
  grant from a 422's `readSet` as though it were whole. (Before #1619 the 422 body was a prose
  string and the resolved rows were dropped.)
- Every `unresolved` entry is `{stage, reason, message}`. `reason` is a **stable, closed token**
  to switch on; it is never reworded and never parsed from `message`. `message` is prose for a
  person and may change.
- Status codes are otherwise unchanged: 400 (missing/invalid parameters, reserved extras keys),
  401, 500 (the inspect pass itself failed, e.g. no ServiceAccount rest.Config).

| `reason` | Cause |
|---|---|
| `stageDataInResourcesFrom` | a `userAccessFilter.resourcesFrom` reads upstream stage output, which does not exist before dispatch |
| `resourceNotServed` | the apiserver serves no such resource, group or version; the stage would 404 at `/call` |
| `discoveryFailed` | discovery could not be read, so the resource could not be validated (retryable; says nothing about the stage) |
| `noRequestPath` | the stage plans no request (an iterator that yields nothing) |
| `unrenderedTemplate` | a literal `${...}` survived rendering (#293) |
| `pathNotAsServed` | the rendered path is one the apiserver would read as a different path (#1137): no leading `/`, an empty segment, or more than one `${...}` embedded in one path |
| `pathValueRefused` | a value spliced into the path could not be one segment or query value, the path has a `.`/`..` segment, or an apiserver path carries a percent-escape (#1123) |
| `jqPathError`, `jqPayloadError`, `jqHeaderError` | the stage's path, payload or header jq expression errored |
| `templatedResource` | the RESOURCE segment of the path is an expression, so what is read is undetermined (#504) |
| `noStaticResource` | the path template has no static (group, version, resource) to enumerate from |
| `notEnumerablePath` | the rendered path is not an in-cluster apiserver resource path (residual template, upstream-dependent, or non-kube) |
| `unknownDependency` | the stage's `dependsOn` names a stage the RESTAction does not declare |
| `cyclicDependency` | the stage is on a `dependsOn` cycle, or depends on a stage that is |

A `dependsOn` naming a stage that does not exist is an `unresolved` entry (`unknownDependency`),
not a 400 and not a 500: the request is fine and the server is fine, and the RESTAction is what
is invalid, which is what 422 says. The other stages are still enumerated, so the consumer gets
every row that can be known plus the stage to fix. (A cycle used to share the "cyclic dependency
detected" 500 with it.)

Field rules (the digest/dedupe key is the full triple, so this stays stable):

- **`verb` is ALWAYS present** on every row. This is the deliberate, documented divergence
  from chart-inspector byte-parity, and it is the entire correctness point of Correction 1.
- **Non-UAF rows** (plain in-cluster stages, classified via `ParseAPIServerPathToDep`)
  carry the verb dictated by the path SHAPE: a **COLLECTION** read
  (`.../resource`, no object name) → **`"list"`**; a **BY-NAME** read
  (`.../resource/<name>`) → **`"get"`**. ✅ **FIXED** (2026-06-25, #44 verb bug):
  the prior draft asserted a *constant* `"get"` on the (mistaken) rationale that
  "`core.go:30` bounds the HTTP-stage method to GET/HEAD, so `get` is exact". That
  conflated the HTTP *method* with the RBAC *verb*: the apiserver authorizes a
  collection GET under the **`list`** verb, so a constant `get` UNDER-GRANTED every
  collection stage (the dominant catalog shape, e.g. the `/blueprints`
  `compositiondefinitions` LIST) and 403'd the real LIST at `/call`. The fix
  (`inspect.go` `inspectInClusterStage`) captures the object `name` from
  `ParseAPIServerPathToDep` and emits `verb := "list"; if name != "" { verb = "get" }`.
  Resource granularity is unchanged — `Resource.Name` stays `""` (a resource-level
  grant, per §3.2); only the verb reflects the read shape.
- **UAF rows** carry **`uaf.Verb`** verbatim (the value at `core.go:141`, passed at
  refilter.go:260). For a `resourcesFrom` UAF the row fans out to one row per discovered
  plural (each with the same `uaf.Verb` and `uaf.Group`), matching the OR-semantics loop at
  refilter.go:254-285.

> Prior-draft note: a "verb-less response with chart-inspector byte-parity" is **abandoned**.
> Byte-parity on the *non-UAF* subset is preserved only at the (group, resource) level;
> the always-present `verb` field is an additive, documented field on the snowplow response.
> The consumer (core-provider) keys on the triple.

### Dedupe / sort (unchanged mechanism, now over the triple)
Canonical-dedupe and -sort operate on `(group, resource, verb)` lexicographically. Two stages
that read the same resource under different verbs (e.g. an HTTP-GET stage on `namespaces` and
a UAF `watch` on `namespaces`) yield **two** distinct rows — correct, because the user needs
both verbs granted.

### Falsifier (PM-required)
Construct an RA with a UAF stage whose `uaf.Verb` ∉ {get,list,watch} — e.g.
`userAccessFilter: { verb: deletecollection, group: "", resource: namespaces }` (admission
accepts it: `core.go:32` only checks non-empty + XOR). Call `/rbac`. **PASS** iff the
`readSet` contains exactly `{ "group": "", "resource": "namespaces", "verb": "deletecollection" }`.
A verb-less or omitted row = FAIL (the under-grant the prior design would have produced).

---

## 5. Correction 2 — `/rbac` REUSES the existing `UserConfig` auth (no new ServiceAuth)

### Root cause of the prior error
The prior draft invented a dedicated-key `ServiceAuth` middleware (`serviceauth.go`) on the
premise that core-provider arrives **unauthenticated**. Diego confirms that premise is FALSE:
**core-provider already presents the standard `/call` auth** (a JWT + a `<user>-clientconfig`).
So no new middleware and no new signing key.

### Fix — mount `/rbac` on `middleware.UserConfig(*signKey, *authnNS)`
Identical to `/call` (`main.go:813-814`). Concretely, mount in the same block:

```go
// GET /rbac — RESTAction read-set enumeration for core-provider RBAC pre-gen.
// Authenticated with the SAME JWT+clientconfig as /call (middleware.UserConfig).
// DELIBERATELY NOT cache.RegisterScopedRoute'd: like /refreshes (main.go:831-833),
// it issues ZERO per-user apiserver reads — enumeration runs under SA discovery —
// so it sits outside the read-path-scoped invariant (fallthrough_middleware.go:142).
mux.Handle("GET /rbac", chain.Append(
    middleware.UserConfig(*signKey, *authnNS)).
    Then(handlers.RBAC( /* deps: SA rest.Config + RA lookup */ )))
```

### The load-bearing framing: authentication ≠ the caller's RBAC permissions (TRACED)
This is the point to get exactly right:

- `UserConfig` does two things (userconfig.go): (a) **validate the JWT** → `userInfo`
  (`:151`); (b) **load `<user>-clientconfig`** Endpoint into ctx via `WithUserConfig`
  (`:229-233`). (a) is authentication. (b) is the per-user dispatch *capability*.
- `/rbac` consumes **(a) only** for the authn gate. It does **NOT** use (b): the enumeration
  reads nothing through the caller's clientconfig — discovery runs under
  `ServiceAccountRESTConfig()` (`sa_client.go:158`). Loading the clientconfig is **harmless**:
  core-provider has one, and `/rbac` simply never dispatches with it.
- Therefore `/rbac` needs **NONE of the caller's RBAC permissions** → the read-set can be
  generated **before** the first resolve / before any binding exists. This is the whole reason
  the endpoint exists, and it survives the auth-reuse: requiring a JWT for *authentication* is
  orthogonal to needing the caller's *permissions*.
- Loading the clientconfig is a Secret read served from the informer cache
  (`api.FromInformerSecret`, userconfig.go:169) — **not** a per-user apiserver RBAC read.
  This is why `/rbac` stays correctly **un-`RegisterScopedRoute`'d** (mirrors `/refreshes`,
  whose comment at main.go:824-826 states the same "zero apiserver reads → outside the
  invariant" rationale).

### Privileged-oracle tradeoff (addressed, not over-engineered)
With `UserConfig`, **any** authenticated caller (any holder of a valid JWT + clientconfig) can
call `/rbac` and learn a RESTAction's GVR read-set. That is **read-only metadata** about a
blueprint; it confers no access — the actual privilege (writing RBAC) lives in core-provider,
not in `/rbac`. The data leaked is "which GVRs does RA X read", which is already discoverable
by anyone who can `GET` the RESTAction CR. **Accepted tradeoff; default is plain `UserConfig`
per Diego.**

**OPTIONAL hardening (offered, not default):** if `/rbac` should be restricted to
core-provider specifically, the minimal add is a caller-identity/group check inside
`handlers.RBAC` — gate on the core-provider SA's group (`system:serviceaccounts:<ns>` or its
specific SA username from `userInfo`, already in ctx from `WithUserInfo`). ~5 LOC, no new
middleware. Recommendation: **ship plain `UserConfig`** (Diego's ruling); add the group check
later only if a concrete need to restrict the oracle appears.

---

## 6. LOC bound & file:line targets

- `internal/handlers/rbac.go` (**new**, ~180 LOC): `func RBAC(...) http.Handler` — load RA by
  name/ns, walk `api[]` in `topologicalSort` order, classify each stage
  (`createRequestOptions` → `ParseAPIServerPathToDep` for in-cluster GET; `discoveryClientFor`
  on SA RC for bare discovery paths; UAF branch reads `uaf.Group`/`uaf.Verb`/resource-set),
  dedupe+sort the triples, marshal §4 shape, fail-loud on non-200 discovery.
- `main.go` (~5 LOC at the §5 mount site, adjacent to main.go:818): mount `GET /rbac` on
  `UserConfig`; **do not** `RegisterScopedRoute`.
- **Dropped vs prior draft**: `serviceauth.go` (entire file) — not created.
- Net: ~280 LOC → ~185 LOC after dropping the middleware.

---

## 7. Falsifiers (consolidated; PM re-gate checklist)

1. **UAF verb emit (§4):** RA with UAF `verb: deletecollection` on `namespaces` →
   `/rbac` readSet contains `{group:"", resource:"namespaces", verb:"deletecollection"}`.
   Verb-less/omitted = FAIL.
2. **Non-UAF verb:** plain in-cluster GET stage on `/api/v1/namespaces` → row
   `{group:"", resource:"namespaces", verb:"get"}`.
3. **resourcesFrom fan-out:** UAF with `resourcesFrom` yielding 3 plurals → 3 rows, each
   carrying `uaf.Verb` + `uaf.Group`.
4. **Auth reuse (§5):** a request with the **same** JWT+clientconfig that succeeds on `/call`
   succeeds on `/rbac`; a request with no/invalid Authorization header → 401 (UserConfig
   `:135-158`). No new key needed.
5. **Dispatch-free / before-resolve:** `/rbac` returns a correct read-set when the caller holds
   **zero** RBAC on the enumerated GVRs (enumeration under SA discovery; the caller's
   clientconfig is loaded but never used for the reads).
6. **Not scoped:** `AssertReadPathsScoped` / `requiredScopedRoutes`
   (`fallthrough_assert.go`) startup assertion still passes with `/rbac` mounted and
   un-registered (mirrors `/refreshes`).

---

## 8. Strategic choice surfaced (one)

**Oracle restriction** (§5): default `UserConfig` (any authenticated caller) vs optional
core-provider-group check. **Recommendation: default `UserConfig`** per Diego — the leak is
read-only blueprint metadata already obtainable via `GET` on the RA CR; add the group check
only on a concrete need. Everything else in this design is non-strategic and TRACED.
