---
type: Architecture
title: snowplow — audit coverage
description: Which snowplow actions emit an AuditEvent, which do not and why, what a record must contain, and how to count the trail without miscounting it. The checked-in inventory AUDIT-4 (krateo-platformops/installer#410) asks for.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [audit, observability, security, restactions]
timestamp: 2026-10-25T00:00:00Z
---

# Audit coverage

Snowplow writes one **AuditEvent** per action it performs for a person, as an
OTLP LogRecord on the shared logs plane (`internal/support/audit`). This page is
the inventory of snowplow's paths that change something: each one is marked
**audited**, **not audited** (an open gap) or **out of scope** (a decision, with
its reason). Keep it current: a new route or a new emit site changes this page in
the same pull request.

## What the trail can and cannot promise

The Kubernetes apiserver's audit log is written by the thing being protected, so
nothing reaches an object without passing it. Snowplow's trail is written by the
component that performs the action. Its **content** can match the apiserver's
(who, what, which object, with what outcome). Its **completeness** cannot: a
path that does not emit leaves no trace, and nothing else will notice. Present
the trail as "what snowplow did on a person's behalf", never as an
apiserver-equivalent record of everything that happened to an object. Only
admission-side enforcement (AUDIT-5, installer#411) can give that guarantee.

## What every record contains

A record that cannot answer "who did what to which target" is worse than no
record, because it looks like coverage. The emitter refuses one, and counts the
refusal in `snowplow_audit_incomplete_total{actor,action,method,target}`
(`/debug/vars`). A record is emitted only when it has:

| field | attribute | source |
|---|---|---|
| actor | `enduser.id`, `user.name` (+ `user.roles`) | the request's authenticated user, read from the request context, never reconstructed |
| action | `krateo.action` | the emit site: `call`, `call.dryRun`, `export`, `restaction` |
| method | `http.request.method` | the verb actually sent |
| target | `k8s.resource.*` + `k8s.namespace.name`, or `server.address` | the object acted on, or the host a RESTAction stage wrote to |

Every record also carries `outcome` (`success` / `failure`),
`http.response.status_code`, `session.id` (W3C baggage) and the request's
TraceId/SpanId.

## Inventory

| path | what changes | status |
|---|---|---|
| `POST/PUT/PATCH/DELETE /call` | an object, on the caller's own credential | **audited** — `call`, one record per request, including `failure` with the apiserver's code when it refuses the write |
| `POST/PUT/PATCH /call/dry-run` | nothing persisted; the apiserver admits or refuses the write | **audited** — `call.dryRun` |
| a RESTAction `api[]` stage with a write verb, reached by `GET /call`, `POST /call/read` or `GET /export` | whatever the stage's target does with the request: the caller's own apiserver (no `endpointRef`) or a named endpoint | **audited** — `restaction`, one record per dialled call (an iterator stage of N calls is N records). See below |
| `GET /export` | nothing; a file is produced from the caller's own `/call` | **audited** — `export` |
| a RESTAction write stage in a background resolve (refresher, cohort seed, prewarm) | as above | **out of scope** — see "Writes nobody requested" |
| a RESTAction write stage in a dry run (inline object on `POST /call/read`) | nothing: the stage is refused before any request leaves (#443) | **out of scope** — no authority was asked, nothing was done |
| authentication failures (bad, expired or missing token) on any route | nothing: answered 401 by the UserConfig middleware before any handler | **out of scope** — see "Authentication failures" |
| reads: `GET /call`, `POST /call/read`, `GET /list`, `GET /rbac`, `GET /api-info/names` | nothing | **out of scope** for AUDIT-4, which covers mutations. Note that the request log and trace id these were once delegated to do not cover them today (installer#410: the gateway access log has no outcome, resource or session id, and the trace is sampled) |
| `GET/POST /refreshes` | nothing: the live-refresh SSE subscription | **out of scope** — cache machinery, no user action |
| `POST /jq` | nothing: evaluates a jq program over the request body; no I/O | **out of scope** |
| `POST /convert` | not mounted (commented out in `main.go`) | **out of scope** — unreachable |
| `POST /debug/shadow-parity` | toggles a dark parity measurement; cannot change an authorization verdict | **not audited** — open hygiene gap (an authenticated state change with no record) |
| snowplow's own writes to the cluster | none: its ClusterRole grants `get/list/watch`, `create` on (Self)SubjectAccessReviews (non-persistent queries) and `impersonate` | **out of scope** — there are no controller writes to record |

### RESTAction stage records

A RESTAction stage may carry any HTTP verb. A stage whose verb is not a safe
method (RFC 9110 §9.2.1: `GET`, `HEAD`, `OPTIONS`, `TRACE`) is a write for the
trail. The verb decides, never the path or the endpoint: a `POST` that only
evaluates something (an access review, a template render) is recorded like any
other `POST`. Each record carries:

- `krateo.restaction.namespace`, `krateo.restaction.name`,
  `krateo.restaction.stage`: the RESTAction and the stage that acted;
- `server.address` (host and port, never userinfo) and `url.path` (never the
  query, which can carry credentials);
- for a stage with no `endpointRef` (the caller's own clientconfig, i.e. the
  apiserver): `k8s.resource.{group,version,resource,subresource,name}` and
  `k8s.namespace.name` parsed from the path. A named endpoint may be any HTTP
  API, so its record claims no Kubernetes resource;
- `outcome`/`http.response.status_code` from the authority that answered,
  read before the stage's own filter runs. A refused write is `failure` with the
  upstream's code and message, the same shape as a `/call` write record.

### Writes nobody requested

The refresher, the cohort seed and the prewarm drivers re-resolve RESTActions
with no person behind them. When one of them dials a write stage (an
`endpointRef` stage uses the endpoint's own credential, so it is not stopped by
the ServiceAccount's read-only grant), it emits no record:

- there is no actor. The context carries a cohort's identity, so a record would
  attribute to a real user an action that user did not take;
- it is a write that originates from a reconcile, not a request. The
  controller-write boundary (installer#413, decided 2026-10-24) records such a
  write only when it targets `*.krateo.io`, changes `.spec` and differs from the
  previous reconcile. Snowplow holds no previous state of a stage's target, so
  it cannot establish the last two; the predicate is not met.

That background drivers dial write stages at all is a separate question from the
trail, tracked outside this page.

### Authentication failures

Decided 2026-10-25: **not in the trail.** A request whose token is missing,
malformed, expired or not signed by a trusted key is answered 401 by the
UserConfig middleware before any handler runs. Recording it would make the trail
writable by anyone able to send a bad token: the trail would hold whatever volume
and content a stranger chose to put there. These requests stay visible in the
agent-gateway access log. `TestAudit410_C4_AuthenticationFailure_IsNotAudited`
(`main_audit_410_test.go`) pins this on the real route table and middleware, so
adding an emit there fails a test and has to change this page.

### Not snowplow's

These rows of installer#410 are emitted, or not, by other components:

- controller writes by cdc, core-provider and oasgen (the installer#413
  predicate belongs at their emit sites);
- authn login, logout, token and certificate issuance;
- Composition / CompositionDefinition writes through the installer;
- kagent tool calls (installer#412);
- direct `kubectl` against `*.krateo.io` (AUDIT-5, installer#411).

## Counting the trail

Filter on the scope **and** the service. The `krateo_audit` table also holds the
agent-gateway access log (`ScopeName='request'`, ~99.5% of rows on krateo-057 in
October 2026), and on 2026-10-23 it held one row under snowplow's scope name
written by `ServiceName='audit-filter-probe'`, carrying only `event.name`,
`outcome` and `session.id`.

```sql
SELECT LogAttributes['krateo.action'] AS action,
       LogAttributes['http.request.method'] AS method,
       LogAttributes['outcome'] AS outcome,
       count() AS n
FROM krateo_audit
WHERE ScopeName = 'github.com/krateo-platformops/snowplow/audit'
  AND ServiceName = 'snowplow'
  AND Timestamp >= '<window start>' AND Timestamp < '<window end>'
GROUP BY action, method, outcome
ORDER BY n DESC
```

Report a volume with its window and its filter. "12 rows" and "N rows" compare
only when both state the same window, scope, service and version
(`ResourceAttributes['service.version']`).
