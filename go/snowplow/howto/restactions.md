---
type: Usage
title: RESTAction — declarative chained REST calls
description: The RESTAction resource — declaratively define chained HTTP calls with jq filters, iterators, endpointRef credentials, in-process resolve of referenced CRs and per-item RBAC refiltering, executed via the snowplow /call endpoint.
resource: snowplow
tags:
  - snowplow
  - restaction
  - jq
  - call
timestamp: 2026-08-06T00:00:00Z
---

# `RESTAction`

**API Group:** `templates.krateo.io`  
**Kind:** `RESTAction`  
**Version:** `v1`  
**Scope:** Namespaced (short name: `ra`)  

## Overview

The `RESTAction` is a Krateo PlatformOps resource that enables users to **declaratively define one or more REST API calls** within Kubernetes.

It allows you to chain HTTP requests, handle dependencies between them, extract data, and use filters to process results — all through a Kubernetes-native manifest.

This approach is particularly useful for integrating external systems or Kubernetes APIs into workflows managed by Krateo PlatformOps.

> `RESTAction` defines one or more declarative HTTP (REST) calls that can optionally depend on other calls.

It allows you to orchestrate a chain of API requests across multiple endpoints using Kubernetes resources.

A `RESTAction` resource declaratively defines one or more HTTP calls (`spec.api`) that can depend on each other.

Each call can produce a JSON response that becomes part of a **shared global context**, enabling subsequent calls to reference previous results using **JQ expressions**, iterators, and filters.

To fully leverage these advanced capabilities — such as resolving JQ expressions, using custom JQ functions or modules, and managing interdependent API calls — the `RESTAction` must be executed through the `snowplow` service endpoint (`/call`).  

Only this endpoint implements the orchestration logic that:
- Executes all HTTP requests defined under `spec.api`, respecting their declared dependencies (`dependsOn`).
- Stores all API responses in a global JSON context.
- Evaluates and resolves any JQ expressions or iterators defined within the resource.
- Returns the computed output in the resource’s `status` field.

When a `RESTAction` is retrieved directly via Kubernetes (e.g. `kubectl get restaction <name>`), the resource is shown **as-is**, without JQ resolution or execution of any API calls.


## `spec`

The `spec` field defines the configuration for the REST action workflow.

| Field | Type | Description | Required |
|--------|------|-------------|-----------|
| `api` | `array` | List of API requests to execute. Each item defines one HTTP call. (Not enforced as required by the CRD schema, but a `RESTAction` without it resolves to nothing.) | ❌ |
| `filter` | `string` | Optional filter to apply to the overall output or results. | ❌ |


### `spec.api[]`

Defines a single HTTP request and its dependencies.

| Field | Type | Description | Required |
|--------|------|-------------|-----------|
| `name` | `string` | A unique identifier for this API call. | ✅ |
| `verb` | `string` | The HTTP method (e.g., `GET`, `POST`, `PUT`, `DELETE`). Defaults to `GET`. | ❌ |
| `path` | `string` | The URI path of the request. | ❌ |
| `payload` | `string` | The request body (for methods like `POST`, `PUT`, etc.). | ❌ |
| `headers` | `array` | Array of custom request headers to include in the request. | ❌ |
| `filter` | `string` | Optional filter to process or extract data from the response. | ❌ |
| `errorKey` | `string` | Key to identify error fields in the response. | ❌ |
| `exportJwt` | `boolean` | If `true`, exports a JWT token from this request for later use. | ❌ |
| `continueOnError` | `boolean` | Changes the answer when this stage fails (see [When a stage fails](#when-a-stage-fails)). On a write stage it also means "send me even if the stage I depend on did not succeed". | ❌ |
| `endpointRef` | `object` | Reference (`name` + `namespace`) to a Kubernetes [`Endpoint`](endpoints.md) object defining the target service. | ❌ |
| `dependsOn` | `object` | Declares a dependency on another API call defined in this spec. | ❌ |
| `resolve` | `boolean` | When this stage's `path` is a **direct apiserver path to a `RESTAction` or `Widget` CR**, controls whether snowplow runs the fetched CR through the resolver in-process. **Opt-in — defaults to `false`** (the raw stored CR is returned). See [resolving a referenced RESTAction/Widget in-process](#resolving-a-referenced-restactionwidget-in-process). | ❌ |
| `userAccessFilter` | `object` | Dispatch this read stage via snowplow's ServiceAccount and RBAC-refilter the result per item against the requesting user. Read-verb stages only. See below. | ❌ |

### `spec.api[].endpointRef`

Defines the reference to an [`Endpoint`](endpoints.md) resource that this API should call.

| Field | Type | Description | Required |
|--------|------|-------------|-----------|
| `name` | `string` | Name of the referenced [`Endpoint`](endpoints.md) object. | ✅ |
| `namespace` | `string` | Namespace of the referenced [`Endpoint`](endpoints.md) object. | ✅ |

### `spec.api[].dependsOn`

Defines a dependency on another API call within the same `RESTAction` definition.  
Useful for chaining calls where one must complete before another.

| Field | Type | Description | Required |
|--------|------|-------------|-----------|
| `name` | `string` | Name of another API call in the list that this call depends on. | ✅ |
| `iterator` | `string` | Optional field on which to iterate (used for loop-like behavior). | ❌ |

### Resolving a referenced `RESTAction`/`Widget` in-process

A stage may reference **another `RESTAction` or `Widget`** by pointing its
`path` at the CR's **direct Kubernetes apiserver path**, e.g.

```yaml
api:
  - name: inner
    path: /apis/templates.krateo.io/v1/namespaces/krateo-system/restactions/my-inner-ra
    verb: GET
    resolve: true   # opt-in — omitted means false (raw CR)
```

With `resolve: true` snowplow fetches that CR from the in-process informer
(cacheable, dependency-tracked) and then **runs it through the resolver
in-process** — `RESTAction`s through the RESTAction resolver, `Widget`s through
the widget resolver — substituting the **resolved** envelope for the stage
output. The result is byte-identical to what an HTTP `/call` of that referenced
CR would return, but with **no outbound HTTP round-trip**. The referenced CR
becomes a cache dependency of the entry, so editing it re-resolves the
dependent entry.

| `resolve` value | Behaviour |
|---|---|
| `false` / omitted (**the default**) | Return the **raw** stored CR, unresolved. Aligned with the frontend's progressive rendering — the server returns raw refs/CRs unless a stage explicitly asks for resolution. |
| `true` | Fetch the CR, then resolve it in-process; the stage output is the **resolved** envelope. |
| any value, on a non-`RESTAction`/`Widget` path (e.g. a `ConfigMap`) | No-op — the raw fetched object is returned (resolution is meaningful only for `RESTAction`/`Widget`). |

The in-process resolve is RBAC-gated (the requesting identity must be allowed
to `get` the referenced CR) and depth-capped (a cyclic reference chain
terminates with a bounded error, never a hang).

`resolve: true` is a **transparent fallback**: it returns the **same resolved
data whether the snowplow cache is on or off**. With the cache off, the
referenced CR is fetched over the requesting user's own token (the user's
apiserver RBAC is the authoritative gate) and resolved in-process all the same —
only slower (no informer serve, no memoisation), never a different result.

> **Release note — `resolve` is opt-in (defaults to `false`).** A stage that
> consumes a referenced `RESTAction`/`Widget`'s **resolved** data (e.g. reads a
> child's resolved-only `.status.<field>`) MUST set `resolve: true` explicitly.
> An omitted field is never persisted or defaulted by the apiserver; the
> resolver treats `nil` as `false`.

> **Note — the `/call?resource=…` loopback path is retired.** Earlier versions
> let a stage reference another `RESTAction` by a `/call?resource=…&apiVersion=…`
> loopback URL. That loopback dispatch path has been removed; use the direct
> apiserver path + `resolve: true` form above instead. (The `/call?resource=…`
> URL shape is still used by the frontend SPA and the prewarm walker for
> navigation — only the *RESTAction-stage* loopback path is retired.)

### `spec.api[].userAccessFilter`

When set, the stage is dispatched under snowplow's ServiceAccount (cluster-wide
read) and the returned items are **refiltered per item** through the in-process
RBAC evaluator before being returned to the caller — so a narrow user sees only
the items they may access. Allowed only on read verbs (`GET`/`HEAD`); admission
CEL also forbids `exportJwt: true` on such a stage. Type `UserAccessFilterSpec`
(`apis/templates/v1/core.go`).

| Field | Type | Description | Required |
|--------|------|-------------|-----------|
| `verb` | `string` | Kubernetes RBAC verb checked per item (e.g. `get`, lower-case). | ✅ |
| `group` | `string` | API group of the checked resource (`""` = core group). | ✅ |
| `resource` | `string` | Static plural resource name. Exactly one of `resource` / `resourcesFrom`. | conditional |
| `resourcesFrom` | `string` | jq over the resolve dict yielding a `[]string` of plurals; an item is kept if the user may act on **any** of them (OR). | conditional |
| `namespaceFrom` | `string` | jq evaluated per item to derive the namespace for the RBAC check. Defaults to `.metadata.namespace`. | ❌ |
| `nameFrom` | `string` | jq evaluated per item to derive the object **name** for the RBAC check, so `resourceNames`-scoped grants are honoured on name-specific verbs. Defaults to `.metadata.name`; use `"."` when the items are bare name strings. No effect on collection verbs (`list`/`watch`/…). | ❌ |

## Passing per-request context

A `/call` may carry `?extras={…}` — a per-request JSON object that **seeds the
resolve dict** (the API stage outputs overwrite it) and is folded into the cache
key. See [extras.md](extras.md).

### The per-stage `filter` sees `extras` as a reserved sibling key

A stage `filter` runs against the wrapped envelope `pig`, which holds the stage
response under the stage `name` plus two **reserved sibling keys**: `slice`
(pagination) and `extras` (the *pure* per-request `?extras=` object). So a
filter can read the request extras directly via `.extras.<key>`:

```yaml
filter: '{items: .things.items, tenant: .extras.tenant}'
```

`extras` is present only when the request carried a non-empty `?extras=` (so a
no-extras resolve is unchanged). The value is the **pure request extras**, not
the accumulated run dict.

**Known asymmetry.** If a stage is literally named `extras`, the **stage
response wins** the collision (a stage's own output is the more specific datum).
This is the opposite of a stage named `slice`, which **loses** to the synthetic
pagination `slice`. The difference is historical, documented, and acceptable —
naming a stage `extras` or `slice` is degenerate. See [extras.md](extras.md) for
the full collision/precedence rules.

## Response body: JSON or YAML

An api stage that targets an external `endpointRef` may receive a **YAML *or*
JSON** response body. snowplow owns the external fetch for these stages
(`api/external_fetch.go`) and accepts a non-JSON `Content-Type` — a plain
upstream client would reject it `406` before the body is read. A YAML body is
converted to JSON **before** any stage `filter`/jq runs, so the jq sees the same
structure either way; a JSON body is passed through unchanged (byte-identical).
This lets a `RESTAction` consume e.g. a Helm repo `index.yaml`. See
[ADR 0006](../docs/adr/0006-snowplow-owned-external-fetch.md).

An **empty** successful body (a `204`, an accepted write) means "no content":
the stage's value is `null`, and it is not an error.

## When a stage fails

A failing **read** stage never stops the stages after it. Its error is
recorded under its `errorKey` (default `error`): the upstream's `Status`
(`code`, `reason`, `message`) when it answered one, otherwise a message. A
non-2xx answer always keeps its HTTP code, even with an empty body or a body
that is not a `Status`.

### A write stage waits for the stage it depends on

A **write** stage (any verb but `GET`, `HEAD`, `OPTIONS`, `TRACE`) whose
`dependsOn` stage **failed or was skipped** is **not sent**, unless the write
stage itself sets `continueOnError: true` ("send me even if what I depend on
did not succeed"). `continueOnError` on the stage it depends on does not
change this.

Each stage ends in exactly one outcome:

- **succeeded**: it ran, and every item it sent succeeded. An `iterator` that
  yields **zero items** succeeded: it had nothing to do;
- **failed**: at least one item failed, or the stage could not be built (its
  iterator errored, its endpoint could not be resolved);
- **skipped**: a write stage not sent because of this rule. A skipped stage
  leaves no value in the result and blocks its own dependent writes, so a
  chain `read → detach → delete` stops at the first failure.

Example: a delete lifecycle where `delete.dependsOn` is `detachVolumes`
(an iterator over the server's data volumes), and `delete` has no
`continueOnError`:

- a detach fails → `delete` is skipped;
- the server has no data volumes (empty iterator) → `detachVolumes` succeeded
  → `delete` is sent.

### The stage-outcome header

Every `/call` answer of a RESTAction carries `X-Snowplow-Stage-Outcomes`: a
compact JSON array, in dependency order, of every stage that did **not**
succeed. `[]` means every stage succeeded.

The header has exactly two shapes:

- a JSON **array** of rows (above); `[]` is the only "all succeeded";
- the object `{"truncated":true,"failed":N}` when the array would exceed
  4 KiB: N stages did not succeed, and which ones cannot be told.

A reader must treat anything else — a header it cannot parse, a JSON value
that is neither, a missing header — as **not** succeeded, never as `[]`.

```
[{"name":"detachVolumes","ok":false,"reason":"Error"},
 {"name":"delete","ok":false,"reason":"Skipped","dependsOn":"detachVolumes"}]
```

- `reason` is a code: `Forbidden`, `NotFound`, `Unauthorized`, `Error`,
  `Skipped`, `NotRun` (the resolve stopped before this stage),
  `StageNotExecuted` (a dry run). `dependsOn` is set on a `Skipped` row.
- A resolve that runs no stage at all (its `dependsOn` graph names a stage
  that does not exist or has a cycle) answers a resolve-level row
  `{"name":"","ok":false,"reason":"Aborted"}` followed by every stage as
  `NotRun`, never `[]`.
- It carries stage names and codes only: no error text, path, identity or
  data.
- The HTTP status is unchanged (see below), so a controller that drives a
  write lifecycle through `/call` reads this header to tell a half-done
  sequence from a done one, even when the RESTAction's `filter` hides the
  error keys. A cached answer carries the header of the resolve that stored
  it. Most cached answers carry `[]`; two kinds of resolve are stored with
  a non-`[]` header: one whose only failures are per-identity forbiddens
  (#681), and one that ran no stage (`Aborted`) — it is stored with its
  `Aborted` / `NotRun` header and answered with it until the RESTAction is
  edited (an edit evicts the cell).

### The HTTP status

What `continueOnError` changes is what happens when the top-level `filter`
**cannot produce a value**: it yields nothing (jq `empty`), or it fails because
it read the stage that has no data.

| | the filter yields a value | the filter yields nothing or fails |
|---|---|---|
| a stage **without** `continueOnError` failed | `200`, the error under `errorKey` | the **first** such stage's upstream status: a `404` stays a `404` (`reason: NotFound`), with a message that names the RESTAction, the stage and the missing resource |
| no such stage failed | `200` | yields nothing: `200`, `status: null`. Fails: `500` with the filter error |

The same answer reaches a widget whose `apiRef` is that RESTAction. An error
answer is never cached. Its message is built from the status code and the
stage's own request (resource, namespace, name), never from the upstream's
text, which can name the requester. The upstream's full text stays under the
stage's `errorKey` and in the logs.

## Write stages run only on a user's request

A **write** stage (any verb but `GET`, `HEAD`, `OPTIONS`, `TRACE`) is sent
only when a user (or a controller, with its own credential) calls `/call`.
Snowplow's own background work — the cache refresher, the prewarm walks and
seeds, the content prewarm, the pagination drain, and the revalidation of a
stale cached answer — resolves the same RESTActions but **never sends their
write stages**, whatever credential the stage uses (snowplow's
ServiceAccount, an `endpointRef` Secret, an external URL). The refused stage
gets an error under its `errorKey`, and that background result is never
stored or served.

- A RESTAction that **declares** a write stage is never stored, whether or
  not a given `/call` sent the write (an iterator with no items, a resolve
  that stopped early because an `endpointRef` Secret is missing). Every
  `/call` runs it, and the background never prewarms or refreshes it. This
  holds for a widget that uses it too, including a widget that opted into the
  external cache (`krateo.io/external-cache-ttl-seconds`, #915).
- A `/call` that snowplow makes to itself on behalf of background work
  carries `X-Snowplow-Background: 1`, so that call refuses write stages too.
  Snowplow sends the header only to itself and strips it from every other
  request.

## jq templating of `path`, `payload` and `headers`

A stage's `path`, `payload` and each `headers` entry may embed a **jq template**
delimited by `${ ... }`. The template is evaluated against the shared resolve
dict (previous stage outputs + `extras`) before the request is issued; a plain
string without the delimiter is passed through verbatim. (`endpointRef.name`
supports the same templating, with a guardrail: a request-templated name may
never resolve to a reserved `<user>-clientconfig` credential Secret.)

## Example

```yaml
apiVersion: templates.krateo.io/v1
kind: RESTAction
metadata:
  name: example-restaction
spec:
  api:
    - name: get-user
      endpointRef:
        name: user-endpoint
        namespace: default
      verb: GET
      path: /users
      continueOnError: false

    - name: update-user
      dependsOn:
        name: get-user
      endpointRef:
        name: user-endpoint
        namespace: default
      verb: PUT
      path: '${ "/users/" + (.["get-user"].items[0].id | tostring) }'
      headers:
        - "Content-Type: application/json"
      payload: '${ {status: "active"} }'

  filter: ""
```
