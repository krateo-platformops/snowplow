---
type: Architecture
title: snowplow — request lifecycle
description: How a /call request becomes JSON — routes and middleware, dispatch, object fetch, RBAC gates, L1 lookup, resolvers, serialization, and the invariants of the read path.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [request-path, dispatch, resolver, rbac, serialization]
timestamp: 2026-08-06T00:00:00Z
---

# Request lifecycle: `/call` → dispatch → resolve → RBAC → serialize

How a single `/call` request becomes JSON the Krateo frontend renders. This
traces the *current* code (docs-standard, 2026-08-06); anchors are file +
function names. Where a verb-specific or cache-mode-specific branch matters, it
is called out — the path is not uniform.

snowplow resolves two CR kinds into JSON over `/call`: a `RESTAction`
(`templates.krateo.io/restactions`) emits raw, *unordered* data assembled from
one or more API stages; a `Widget` (`*.widgets.templates.krateo.io`)
canonicalizes data into the render-ready shape the frontend consumes. Every
other GVR falls through to a raw apiserver passthrough.

---

## 1. Data-flow diagram

```
                         HTTP GET/POST/PUT/PATCH/DELETE /call?apiVersion=&resource=&namespace=&name=&page=&perPage=&extras=
                                              │
            ┌─────────────────────────────────┴──────────────────────────────────┐
            │  main.go  mux.Handle("GET /call", chain.Append(                      │
            │     middleware.UserConfig(signKey, authnNS)   ← authn: JWT → UserInfo│
            │     cache.FallthroughScopeMiddleware(ScopeCallGeneric)               │
            │     handlers.Dispatcher(dispatchers.All()))   ← routes by GVR group  │
            │   .Then(handlers.Call()))                     ← fallthrough handler  │
            │  (outermost wrappers: CORS → otelhttp → gzip → audit middleware)     │
            └─────────────────────────────────┬──────────────────────────────────┘
                                              │
                       proxy.go Dispatcher: GET only; key by gv.Group
                       (restactions → "restactions."+group; widget → group)
                                              │
        ┌──────────────────────┬──────────────┴───────────────────┬──────────────────────────┐
        │ key in All() map?    │                                  │ NOT a GET, or no handler  │
        │ "restactions.        │ "widgets.templates.krateo.io"    │ for this group            │
        │  templates.krateo.io"│                                  │                           │
        ▼                      ▼                                  ▼                           ▼
  RESTAction()           Widgets()                          handlers.Call()             (write verbs:
  restactions.go         widgets.go                         call.go                       POST/PUT/PATCH/
        │                      │                            raw apiserver passthrough     DELETE skip the
        │                      │                            under the USER's own token    dispatcher entirely)
        │                      │                            RecordApiserverFallthrough
        ▼                      ▼
  fetchObject (helpers.go) ─ objects.Get (objects/get.go)
   cache-on: informer-served + filterGetByRBAC
   cache-off: getFromAPIServer under user token
        │
        ▼
  checkDispatchRBAC  (helpers.go) — cache-on ONLY; gate the GET on the dispatch-target CR
   rbac.EvaluateRBAC; deny → 403
        │
        ▼
  L1 resolved-output cache lookup (cache-on, and only when serveFromCacheEligible)
   widgets: identity-FREE content cell FIRST (gated by isRBACSensitiveApiRefWidget)
            then per-binding cell (BindingUID + RBACSubGen, dispatchCacheLookupKey)
   restactions: per-binding cell only
        │ HIT → write cached bytes (widgets content-hit re-runs serve-time UAF gate first)
        │ MISS ↓
        ▼
  RESOLVE
   RESTAction: restactions.Resolve → api.Resolve (per-stage)
               per-stage userAccessFilter → refilter.go (per-item EvaluateRBAC, fail-closed)
               → spec.Filter jq projection → status.Raw
   Widget:    widgets.Resolve
               A2 identity injection → apiRef → widgetDataTemplate → resourcesRefs
               (per-item rbac.UserCan → allowed flag) → resourcesRefsTemplate
               → CRD-schema validate status
        │
        ▼
  SERIALIZE  encodeResolvedJSON (helpers.go, json.Encoder, no indent)
   Put gates: zero stage errors AND zero external touches (or TTL-annotated)
   AND cache-eligible AND declared extras — else serve without persisting
        │
        ▼
  writeResolvedJSON (helpers.go) — Content-Type: application/json, 200, single buffered Write
  → cache.PublishRefresh(key) on a committed Put (the /refreshes SSE signal)
```

For a widget served from the identity-free content cell the cached bytes are a
*shell*: `gateWidgetEnvelope` re-derives every
`status.resourcesRefs.items[].allowed` flag under the requester before the
bytes leave the pod (`widget_content.go`). The shell is never served verbatim.

---

## 2. Trace through the packages

### 2.1 Server wiring and routes — `main.go`

`main()` builds one `http.ServeMux` and registers the full route table:

- `GET /health` → `handlers.HealthCheck(...)` — liveness only, a static
  `{"status":"alive"}` with zero allocation per probe.
- `GET /readyz` → `handlers.ReadyCheck()` — returns 503 `{"status":"warming"}`
  until `cache.MarkPhase1Done()` fires. Since the prewarm-gated-readiness
  reversal this flips only after the synchronous boot seed (or its backstop) —
  see [prewarm.md](prewarm.md).
- `GET /call` — the canonical read path. Its middleware chain is
  `middleware.UserConfig(signKey, authnNS)` → `FallthroughScopeMiddleware(ScopeCallGeneric)`
  → `handlers.Dispatcher(dispatchers.All())`, finally `.Then(handlers.Call())`.
- `POST/PUT/PATCH/DELETE /call` — same `UserConfig` middleware and a
  write-scope `FallthroughScopeMiddleware`, but **no** `Dispatcher` in the
  chain; they go straight to `handlers.Call()`.
- `GET /list` — the list surface (`handlers.List()`), same UserConfig + scope
  chain (`ScopeList`).
- `GET /export` — generic export: any `/call`-resolvable list serialized as a
  CSV/JSON attachment. It **re-dispatches in-process through the same
  dispatcher lane** the GET `/call` route uses (identical auth, RBAC gate and
  serve-time UAF — export can never see more than the caller's own `/call`),
  then serializes the extracted rows (`handlers.Export`).
- `GET /api-info/names` — plural-name resolution (`handlers.Plurals()`),
  scope-classified as `plurals`.
- `GET /refreshes` — the per-subject live-refresh **SSE** stream. Uses
  `middleware.RefreshAuth` (cookie-or-header JWT; a browser `EventSource`
  cannot set `Authorization`) and issues **zero** apiserver reads. The
  connection re-derives each subscription key under the caller's own JWT
  (`DeriveSubscriptionKey`, `dispatchers/refresh_subscription.go`), so a
  `?sub=` coordinate cannot forge another subject's stream. The cache calls
  `PublishRefresh(key)` after an L1 commit; the matching subscriber receives a
  signal and refetches a guaranteed-fresh entry. Gated by
  `REFRESH_SSE_ENABLED` (a clean idle stream when off). Deliberately NOT
  scope-registered — with no apiserver reads it sits outside the
  read-path-scoped invariant.
- `GET /rbac` — RESTAction read-set enumeration for core-provider RBAC
  pre-generation: resolves a referenced RESTAction's `api[]` stages to the
  (group, version, resource, verb) tuples a `/call` WOULD read, without
  dispatching; runs under the SA so it is computable before any binding
  exists.
- `POST /jq` — a jq evaluation helper (`handlers.JQ()`).
- `GET /swagger/` — swagger UI; `GET /debug/vars` → `expvar.Handler()`;
  `GET /debug/pprof/*` registered on this mux directly (the server does not
  use `http.DefaultServeMux`); `GET /debug/servable` and `GET /debug/apistage`
  — cache diagnostics; `GET /debug/refreshes` — the refresh-subscription
  registry, auth-gated with the same `RefreshAuth` as `/refreshes`.

The mux is wrapped (inner → outer) by **gzip** (`gzhttp` via
`middleware/compression.go` — Accept-Encoding-gated, SSE-excluded so
`/refreshes` keeps per-event flush; non-gzip clients are byte-identical), then
**otelhttp** (no-op spans unless tracing is enabled — see
[observability.md](observability.md)), then the **audit middleware**
(`internal/support/audit` — session correlation rides W3C `baggage`
`session.id`), then **CORS** (`snowplowCORSOptions` in `main.go`: exposes the
live-refresh signalling headers `X-Snowplow-Refresh-Key` /
`X-Snowplow-Refresh-Class` + `Link`, and allows the W3C `traceparent` /
`tracestate` / `baggage` request headers).

The `http.Server` uses `WriteTimeout = 300s` (`main.go`). That deadline is
anchored to request-read time, sized to clear the cache-OFF heavy compute path
(measured ~159s at 50K) before the handler's single buffered `Write`; cache-ON
is sub-4s and never approaches it.

### 2.2 Authentication — `middleware.UserConfig` (`internal/handlers/middleware/userconfig.go`)

`UserConfig` is snowplow's cache-aware sibling of plumbing's `use.UserConfig`,
pinned to a transcribed copy of plumbing's control flow. It validates the JWT
with `jwtutil.Validate(signingKey, token)` and installs `WithAccessToken` /
`WithUserInfo` / the per-user endpoint onto the request context. Downstream the
identity is read with `xcontext.UserInfo(ctx)` and the per-user apiserver
endpoint with `xcontext.UserConfig(ctx)`.

### 2.3 Dispatch — `handlers.Dispatcher` (`internal/handlers/proxy.go`)

`Dispatcher` is a middleware, not a handler. It:

1. Passes any **non-GET** request straight to `next` (= `handlers.Call()`)
   without routing. Write verbs are therefore never resolved — they are raw
   apiserver passthroughs.
2. Parses `apiVersion` + `resource` query params into a GVR.
3. Computes the lookup key: `key = gv.Group`, except for
   `resource == "restactions"` where `key = "restactions." + gv.Group`. This is
   the "Hack caused by new Widgets handlers" — a widget CR's group *is*
   `widgets.templates.krateo.io`, so it keys on the group; a RESTAction's group
   is `templates.krateo.io`, so the `restactions.`-prefix disambiguates.
4. Looks the key up in the `dispatchers.All()` map:
   `"restactions.templates.krateo.io"` → `RESTAction()`,
   `"widgets.templates.krateo.io"` → `Widgets()`. On a hit it forwards to that
   handler; on a miss it falls through to `next` = `handlers.Call()`.

### 2.4 Fallthrough — `handlers.Call` (`internal/handlers/call.go`)

`handlers.Call()` is the raw passthrough for every GVR not in the dispatch map
and for all write verbs. It validates the request, builds an apiserver URI path
(`/apis/<g>/<v>/namespaces/<ns>/<res>[/<name>]`), then issues the call under
the **user's own** endpoint via `request.Do` — RBAC is enforced inline by the
apiserver. It records `cache.RecordApiserverFallthrough(..., ReasonClientBuild,
"")` *before* the call so a panicking plumbing call still counts. The response
is decoded into a dict (list responses land under `items`) and JSON-encoded to
the wire (with two-space indent — see §5).

### 2.5 The two dispatchers — `internal/handlers/dispatchers/{restactions,widgets}.go`

Both handlers are constructed once (`RESTAction()` / `Widgets()`) and capture
the snowplow ServiceAccount transport pair (`saEP`, `saRC`) at construction
time, not per request — the per-request `snowplowSACtx()` was found to
serialize dispatches on the SA-singleton mutexes. Out-of-cluster runs get
`(nil, nil)` and skip the attach.

Both `ServeHTTP` methods follow the same skeleton:

1. `beginPerCall` / deferred `pcEmit` — structured `dispatcher.call.complete`
   timing log (`per_call_log.go`).
2. `defer markCustomerInFlight()()` — signals the prewarm engine and refresher
   to yield background work for the dispatch's duration.
3. `util.ParseExtras(req)` — parses the `?extras=<json>` query param into a
   `map[string]any`.
4. `fetchObject(req)` — fetch the dispatch-target CR (`helpers.go` →
   `objects.Get`, §2.6).
5. **RBAC dispatch gate** — cache-on only (§2.7).
6. **L1 cache lookup** — cache-on only, guarded by `serveFromCacheEligible`
   (§2.8).
7. On miss, build the resolve context (attach SA transport, the L1 key, a
   stage-error sink, an external-touched sink), call the resolver, encode,
   conditionally cache, write.

### 2.6 Object fetch — `objects.Get` (`internal/objects/get.go`)

`fetchObject` delegates to `objects.Get`. The fetch is cache-mode-routed:

- **cache-off** (`cache.Disabled()`): `getFromAPIServer` under the user's own
  endpoint/token. RBAC is enforced inline by the apiserver.
- **cache-on**: served from the in-process informer cache *iff* the GVR is
  servable (registered + `HasSynced`) **and** the requester passes
  `filterGetByRBAC` (`objects/informer_serve.go`). A miss, a not-yet-synced
  informer, or an RBAC-denied GET all fall through to `getFromAPIServer` under
  the user's token, which returns the authoritative 403/404.

When `ctx` carries an L1 key (`cache.WithL1KeyContext`), a successful Get
records a dependency edge `(GVR, ns, name)` so a later DELETE/ADD/UPDATE of
that object invalidates the L1 entry.

### 2.7 The RBAC dispatch gate — `checkDispatchRBAC` (`dispatchers/helpers.go`)

In cache-on mode `objects.Get`'s informer branch bypasses the per-user token,
so the GET on the dispatch-target CR is re-checked explicitly. Both dispatchers
call `checkDispatchRBAC` and return **403** on deny. It extracts `UserInfo`,
calls `rbac.EvaluateRBAC` with verb `get` and `SkipBindingUID: true` (it
discards the binding UID), and returns the allow bit; any error fails closed to
deny. In cache-off mode the gate is skipped — the per-user apiserver fetch in
`objects.Get` already enforced it.

### 2.8 L1 lookup keys

The dispatch-target lookup runs **strictly after** the RBAC gate so a cache hit
can never short-circuit the permission check.

- `dispatchCacheLookupKey` (`helpers.go`) builds the per-request key. It reads
  `UserInfo`; **a missing/unparseable identity makes the request uncacheable**
  (nil handle) — the resolve still runs, but nothing is read from or written to
  L1. Identity is folded as **two terms**: the **per-binding UID** — a direct
  `rbac.EvaluateRBAC(verb=get, …)` returns the first-match binding UID that
  authorized the GET, and that UID (*not* the literal username) is the
  `BindingUID` field in the key — and the subject's **RBAC sub-generation**
  (`cache.RBACSubGenForSubject`), which rotates the user's keys when their own
  bindings change. Two users authorized by the same binding (at the same
  sub-gen) share the cell. `Username`/`Groups` are carried only as the
  refresher's `Representative*` re-resolve identity, **not** folded into
  `ComputeKey`. A deny/error derives `BindingUID == ""`, and an empty-UID cell
  is **neither read nor written** on the dispatch path (`serveFromCacheEligible`
  — the #95 leak closure).
- Widgets additionally try an **identity-free** content cell first
  (`dispatchWidgetContentKey`). Its key is `(gvr, ns, name, perPage, page,
  extras)` with username/groups omitted entirely
  (`CacheEntryClassWidgetContent`). This shared cell is **skipped** for an
  apiRef-driven render widget classified RBAC-sensitive by
  `isRBACSensitiveApiRefWidget` (`widget_content.go`), because its
  `status.widgetData` aggregate is not narrowed at serve time and would leak
  cross-user. On a content-cell hit, `gateWidgetEnvelope` re-derives every
  `items[].allowed` flag under the requester before writing.

`extras` fold into every L1 key via `canonicaliseExtras` (a sorted-key JSON
encoding) inside `ComputeKey` — so distinct `extras` values never collide on
one cell. **Which extras may reach the key** is author-declared: the effective
key extras are the union of the widget's inline template extras and the request
extras filtered by `spec.keyExtras` (F6, `filterDeclaredKeyExtras`) plus the
`spec.identityContext` identity axis (username/groups — server-injected, so a
client-supplied `extras.username` can never spoof it). A resolve fed
undeclared request extras is served but its Put is quarantined.

### 2.9 Resolve — RESTAction (`internal/resolvers/restactions/`)

`restactions.Resolve` runs the typed CR's `spec.api[]` stages through
`api.Resolve`, producing a `dict` of per-stage outputs. Each stage that
declares `userAccessFilter` is RBAC-filtered **per item** by the api package's
refilter (`refilter.go`). Then `spec.Filter` (if present) is a single jq
projection over the dict; otherwise the dict is marshalled as-is. The result is
written to `status.Raw` as a `runtime.RawExtension`, with
`last-applied-configuration` and `managedFields` stripped.

A stage that targets an external `endpointRef` is dispatched through snowplow's
own HTTP fetch (`api/external_fetch.go` `httpFetchAllowingNonJSON`), which
reuses plumbing's client / auth roundtrippers / retry but drops the JSON-only
`406` gate — so the stage may receive **YAML or JSON**, and a YAML body is
converted to JSON before the stage jq. See
[ADR 0006](../adr/0006-snowplow-owned-external-fetch.md). A resolve that
touched an external endpoint is not L1-cached (no dep edge can invalidate it)
unless the widget carries the bounded-TTL annotation — see caching.md §4. An
api-step's `endpointRef.name` may itself be templated from request extras (the
hub-spoke pattern).

A stage that fetches a snowplow RESTAction/Widget CR from a **direct apiserver
path** may set `resolve: true` (`spec.api[].resolve`, **default false / opt-in**
— the 2026-07-02 contract flip aligned with progressive rendering) to run the
fetched CR through the resolver **in-process** and substitute the resolved
envelope for the stage output. The old `/call?resource=…` HTTP-loopback
dispatch branch is **retired** (2026-06-22 corpus audit: zero live loopback
paths); the in-process resolver behind the seam
(`dispatchers/nested_call.go` `ResolveNestedCall`, wired via
`api.RegisterNestedCallResolver`) replicates the dispatcher minus the HTTP
edge — and its explicit `checkDispatchRBAC` call is the single most important
correctness line on that path (the in-process resolve bypasses the per-user
apiserver edge). Nested resolves are cycle-stopped by an ancestor set, depth-
gated, and admission-bounded by the process-wide adaptive headroom gate
(`api/nested_resolve_bound.go`: GOMEMLIMIT − live heap; blocks, never drops;
on ctx expiry the outer `/call` returns an honest 503-class error).

The RESTAction emits *unordered* data. It contains **no widget-shaping logic**
— the layering boundary is asserted in the refilter package doc: RBAC narrowing
lives in the resolver/per-API-stage layer, never in widget canonicalization.

### 2.10 Resolve — RBAC refilter (`internal/resolvers/restactions/api/refilter.go`)

`userAccessFilter` is the per-item RBAC filter for a SA-dispatched LIST. The
list is fetched under the snowplow SA, then narrowed to what the *requesting
user* may see. For each item:

- `NamespaceFrom` jq resolves the namespace (default `.metadata.namespace`).
- The resource-plural set is resolved once per dispatch — static `uaf.Resource`
  or jq-derived `ResourcesFrom`. An item is kept iff `rbac.EvaluateRBAC`
  permits the user for **any** resource in the set (OR-semantics).
- For a **name-specific verb** (`get`/`update`/`patch`/`delete`), the
  once-compiled `NameFrom` expression (default `.metadata.name`) derives the
  per-object name so `resourceNames`-scoped grants match (#123); for
  `list`/`watch` the name is never consulted.

Every failure mode fails **closed**: missing `UserInfo` drops all items; an
unresolvable resource set drops all items; a jq error or an unrecognized item
shape denies that item. The production callsite is
`applyUserAccessFilterOnPig`, which resolves `ResourcesFrom` against the full
resolver `dict` (so it can reference upstream stage outputs like `.crds`)
while refiltering the per-stage items.

### 2.11 Resolve — Widget (`internal/resolvers/widgets/resolve.go`)

`widgets.Resolve` canonicalizes into the render-ready envelope, in fixed
phases (each wall-clocked for the seed's timing sink):

1. **A2 identity injection** — for a widget declaring `spec.identityContext`,
   the declared subset of the *authenticated* principal (`DeclaredIdentity`) is
   folded into `opts.Extras`, **overwriting** any client-supplied value for the
   declared keys (the anti-spoof quarantine). Runs cache-on and cache-off
   identically; inert for the identity-free corpus.
2. `resolveApiRef` — fetch the widget's apiRef RESTAction (under SA
   transport), yielding the data source `ds`. `extras` flows
   widget → apiref → restactions → api here.
3. `injectSlice(ds, perPage, page)` — re-inject the pagination triple stripped
   by the RA's `spec.Filter` projection.
4. `mergeExtras(ds, extras)` — fold extras into `ds`, **non-overwriting**
   (apiRef results and the slice triple win on collision); this also makes
   extras available to apiRef-less widgets.
5. `resolveWidgetData` → `status.widgetData`. A `widgetDataTemplate` read error
   **fails soft** to static-only data — kept symmetric with
   `isRBACSensitiveApiRefWidget`'s de-classification so a read error never
   lands a SA-maximal aggregate in the shared identity-free cell.
6. `resolveResourceRefs` → `status.resourcesRefs.items`. Each ref gets an
   `allowed` flag from `rbac.UserCan` under the *resolving* identity
   (`resourcesrefs/resolve.go`). The #72 inline-rendered-children producer
   (`widgets_inline.go`, opt-in) can additionally attach resolved child
   envelopes.
7. `crdschema.ValidateObjectStatus` validates the status against the widget's
   CRD schema; a failure returns a 400 `StatusError`.

### 2.12 RBAC evaluator — `internal/rbac/`

`rbac.UserCan` routes through `rbac.EvaluateRBAC` in cache-on mode and through
a SubjectAccessReview only in cache-off mode. `EvaluateRBAC` is the in-process
evaluator — snapshot-based candidate walk, L2 permit-only memo, degrade-to-deny
on a missing snapshot. See [rbac-uaf.md](rbac-uaf.md) for the full trace.

### 2.13 Serialize and conditional cache write

Both dispatchers encode with `encodeResolvedJSON` (`helpers.go`) — a
`json.Encoder` with **no** indentation, deliberately matched between the
cache-Put bytes and the wire bytes so "cache-on warm == cache-off" holds (note
`handlers.Call` does indent — that path is the raw passthrough, not a resolver
result). The write is a single buffered `writeResolvedJSON` — no streaming.

The L1 **Put** is gated (see caching.md §4): `stageErrSink.Count() == 0` (a
partial body is served (200) but not persisted), `extTouchedSink.Count() == 0`
(external results have no invalidation edge; the per-widget
`krateo.io/external-cache-ttl-seconds` annotation opts into a bounded-TTL Put
instead), `serveFromCacheEligible` (never write the empty-`BindingUID` row),
and the F6 declared-extras check. On a clean resolve the bytes are Put under
the computed key, dependency edges are recorded — the self-dep on the
dispatch-target CR and, for widgets, the apiRef→RESTAction and render-eligible
resourcesRefs deps (`recordWidgetDeps`) — and `cache.PublishRefresh` signals
`/refreshes` subscribers.

---

## 3. Invariants

1. **RBAC gate precedes the cache.** The L1 lookup is always after
   `checkDispatchRBAC`, so a cache hit can never bypass a permission check.
2. **Layering: RESTAction emits unordered data; the widget canonicalizes.**
   RBAC narrowing lives in the resolver / per-API-stage layer; the RESTAction
   has no widget-shaping logic.
3. **Per-binding(+sub-gen) L1 keying — never identity-free for sensitive
   content, never the empty row.** The identity-bound cell keys on the
   authorizing binding UID + the subject's RBAC sub-generation, not username.
   The identity-free widget-content cell is used only for non-RBAC-sensitive
   widgets and is always passed through serve-time `gateWidgetEnvelope`. A
   request with no identity is uncacheable; a deny (`BindingUID==""`) is
   neither served from nor written to L1.
4. **Cache-off is a transparent fallback.** `CACHE_ENABLED=false` makes
   `objects.Get`, `EvaluateRBAC`, and `UserCan` route to the apiserver /
   SubjectAccessReview under the user's own token — same data and RBAC, just
   slower. No dispatch RBAC gate runs because the per-user fetch already
   enforced it.
5. **Fail-closed everywhere RBAC is evaluated.** Missing identity, evaluator
   error, nil snapshot, unparseable item shape → deny / drop, never allow-all.
6. **Serve bytes == cache bytes.** Resolver responses are encoded identically
   on the Put path and the write path.
7. **Only clean, invalidatable, declared resolves are cached.** A per-item
   stage error, an external touch (unless TTL-annotated), an empty binding
   UID, or undeclared request extras serve the body but decline the Put.
8. **A per-item feed/decode error does not truncate the resolve.** On every
   served dispatch branch (external fetch, in-process nested resolve,
   informer-pivot, internal-rest-config), a stage whose body fails to decode /
   jq-filter records the error into `errorKey` (or surfaces it per
   `continueOnError`) and returns `nil` for that item — remaining items and
   downstream stages still run (the #313 Option C-A contract). When the
   RESTAction's filter then cannot produce a value, the first stage that
   failed without `continueOnError` is the answer, with its upstream status
   (#709) — never a JSON-decode 500.
9. **In-process resolves are explicitly RBAC-gated.** Every `resolve: true`
   nested resolve passes `checkDispatchRBAC` before resolving — the in-process
   path must reconstruct the RBAC enforcement the HTTP edge would have
   provided.
10. **Server-injected identity beats client extras.** For
    `spec.identityContext` widgets the JWT-derived identity overwrites any
    client-supplied identity extras — extras can shape content only where the
    author declared them (`spec.keyExtras`).

---

## 4. Known failure modes

| Symptom | Likely cause | Where it surfaces in code |
|---|---|---|
| `/call` returns 403 for a widget/RESTAction the user expects | `checkDispatchRBAC` denied the GET on the dispatch-target CR (cache-on); or in-process snapshot not yet built (degrade-to-deny) | `dispatchers/restactions.go`, `widgets.go`; `rbac/evaluate.go` |
| All list items vanish for a narrow-RBAC user | `userAccessFilter` fail-closed: missing `UserInfo`, unresolvable `ResourcesFrom`, or a `NamespaceFrom` jq error dropped every item | `api/refilter.go` |
| Widget panel renders 0 rows for one user, full for admin | working as designed: `gateWidgetEnvelope` set `allowed=false` per requester; the frontend renders only `allowed==true` items | `dispatchers/widget_content.go` |
| `/call` hangs then client gets HTTP 0 under cache-OFF at scale | heavy compute exceeded `WriteTimeout`; the t0-anchored 300s deadline is the bound | `main.go` |
| A cold compositions-page fan-out returns 503-class errors | the adaptive aggregate admission bound serialized the outermost nested resolves and the ctx expired — honest backpressure, not silent truncation | `api/nested_resolve_bound.go` |
| Cross-user data leak from a shared widget cell | an RBAC-sensitive apiRef widget incorrectly routed to the identity-free content cell — the `isRBACSensitiveApiRefWidget` / `resolveWidgetData` error-direction symmetry was broken; or the #95 empty-UID guard regressed | `dispatchers/widgets.go`; `widgets/resolve.go`; `helpers.go` `serveFromCacheEligible` |
| A nested `resolve: true` stage returns raw instead of resolved (or vice versa) | the stage's `resolve` field vs the opt-in default-false contract; the resolver's `ptr.Deref(…Resolve, false)` is the single default site | `apis/templates/v1/core.go`; `api/resolve.go` |
| Stale content after an object change | dependency edges not recorded (Get ran without an L1 key in ctx, or cache off); TTL is the outer safety net | `objects/get.go`; `dispatchers/restactions.go` |
| External-API widget hammers the upstream on every `/call` | by design: external resolves are never cached; opt into the bounded-TTL annotation if staleness ≤120s is acceptable | `cache/external_touched_sink.go`; `dispatchers/external_ttl.go` |
| Cache never serves; everything hits apiserver | cache disabled, informer not servable (`!HasSynced`), or a nil/passthrough/metadata-only watcher | `objects/get.go`; `rbac/evaluate.go` |
| `/readyz` stuck `{"status":"warming"}` | Phase-1 warmup (incl. the sync seed) never flipped `Phase1Done` — should be impossible past the fire-regardless backstop; check `snowplow_readyz_backstop_fired` and the `phase1.*` logs | `dispatchers/phase1_walk.go` |

---

## 5. Notes where code diverged from earlier documentation

- **"per-user L1 keying."** Older prose describes the L1 cell as *per-user*-keyed.
  The current code keys the cell on the **first-match binding UID** plus the
  subject's **RBAC sub-generation**, with username/groups deliberately removed
  from `ComputeKey`. Username/groups survive only as the refresher's
  `Representative*` re-resolve identity. "Per-binding-UID (+sub-gen)" is the
  accurate description; "per-user" is correct only in the sense that the key is
  still identity-derived and never shared across permission boundaries.
- **`handlers.Call` is the fallthrough, not the resolver entry.** The resolvers
  are reached only through the `Dispatcher` middleware's `All()` map. Write
  verbs bypass the dispatcher entirely and are never resolved.
- **The `/call` HTTP loopback is retired.** Earlier revisions documented a
  nested `/call?resource=…` loopback dispatch branch (with its forge-guard and
  self-host matching). The 2026-06-22 corpus audit found zero live loopback
  paths and the branch was retired; nested resolution is now the direct
  apiserver path + `resolve: true` through the in-process seam
  (`nested_call.go`), which is where the RBAC gate and the adaptive OOM bound
  live.
- **`internal/handlers/dispatchers/` is not a thin layer.** This package owns
  the entire L1 key/lookup/Put logic, the prewarm engine, the refresher hooks,
  and the live-refresh subscription derivation. The pure routing decision is
  the small `proxy.go` middleware in `internal/handlers/`.
- **Widget serialization indents differently from the passthrough.**
  `encodeResolvedJSON` (resolver path) emits compact JSON deliberately for
  cache-byte parity; `handlers.Call` (passthrough) emits two-space-indented
  JSON. The two `/call` response shapes are not byte-identical across the
  resolver vs. passthrough lanes.
