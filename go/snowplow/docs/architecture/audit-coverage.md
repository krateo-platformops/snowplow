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

A record should answer "who did what to which target":

| field | attribute | source |
|---|---|---|
| actor | `enduser.id`, `user.name` (+ `user.roles`) | the request's authenticated user, read from the request context, never reconstructed |
| action | `krateo.action` | the emit site: `call`, `call.dryRun`, `export`, `restaction` |
| method | `http.request.method` | the verb actually sent |
| target | `k8s.resource.*` + `k8s.namespace.name`, or `server.address` | the object acted on, or the host a RESTAction stage wrote to |

The emitter enforces it, for every emit site:

- **no actor, no record.** A record without an authenticated user is worse than
  none, because it looks like coverage. It is refused and counted in
  `snowplow_audit_incomplete_total{actor}` (`/debug/vars`).
- **an actor but a missing action, method or target**: the record is still
  emitted, marked `krateo.audit.incomplete=<fields>` (e.g. `method,target`), and
  each missing field is counted in `snowplow_audit_incomplete_total`. The person
  is known, so the action is not hidden; the mark says what the site failed to
  supply.

Every record also carries `outcome` (`success` / `failure`),
`http.response.status_code` (when the authority answered), `session.id` (W3C
baggage) and the request's TraceId/SpanId.

**What a record never holds.** The trail is kept 365 days, so nothing that can
carry a credential goes into it: no request path or query (a webhook path such
as Slack's `/services/T/B/<secret>`, Discord's `/api/webhooks/<id>/<token>` or
Telegram's `/bot<token>/…`, or a path built by jq from an earlier stage's token,
is a secret in its own right), and no upstream response body (an error body can
echo a token). A RESTAction stage record's body is a reason class from a closed
set (below).

## Inventory

| path | what changes | status |
|---|---|---|
| `POST/PUT/PATCH/DELETE /call` | an object, on the caller's own credential | **audited** — `call`, one record per request, including `failure` with the apiserver's code when it refuses the write |
| `POST/PUT/PATCH /call/dry-run` | nothing persisted; the apiserver admits or refuses the write | **audited** — `call.dryRun` |
| a RESTAction `api[]` stage with a write verb, reached by `GET /call`, `POST /call/read` or `GET /export` | whatever the stage's target does with the request: the caller's own apiserver (no `endpointRef`) or a named endpoint | **audited** — `restaction`, one record per dialled call (an iterator stage of N calls is N records). See below |
| `GET /export` | nothing; a file is produced from the caller's own `/call` | **audited** — `export` |
| a RESTAction write stage in a background resolve (refresher, cohort seed, prewarm, content prewarm, pagination drain, a #547 demand revalidation, or a `/call` snowplow makes to itself for one of them) | nothing: the stage is refused before any request leaves (#903) | **out of scope** — nothing was sent; see "Writes nobody requested" |
| a RESTAction write stage in a dry run (inline object on `POST /call/read`) | nothing: the stage is refused before any request leaves (#443) | **out of scope** — no authority was asked, nothing was done |
| a RESTAction write stage abandoned before the dial: its endpoint Secret cannot be resolved, its iterator or path template fails, the #288 malformed-dial guard refuses the path, the resolve is not a customer request (#903, the row above), the stage it depends on did not succeed (#904: reported `Skipped` in `X-Snowplow-Stage-Outcomes`), or the pass was served stale content and its fresh re-run sends the write instead (#905: the withdrawn pass sends nothing, the re-run's dial is the one recorded) | nothing: no request leaves | **out of scope** — nothing was sent; the stage error is in the response and the log, not in the trail |
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
- `server.address`: host and port only — never userinfo, a path or a query;
- for a stage with no `endpointRef` (the caller's own clientconfig, i.e. the
  apiserver): `k8s.resource.{group,version,resource,subresource,name}` and
  `k8s.namespace.name`, the object's coordinates as a `/call` record holds them.
  A named endpoint may be any HTTP API: its record names no Kubernetes resource
  and nothing of its path;
- `outcome` and `http.response.status_code`: what the authority answered,
  observed at the dial — not the resolver's reshaped result, and not what the
  stage's own filter makes of it:

  | the authority answered | outcome | code | record body |
  |---|---|---|---|
  | a 2xx | `success` | the real code (`201`, `204` are not reported as `200`) | empty |
  | a 2xx whose body snowplow cannot read or convert to JSON/YAML | `success` — the write was accepted | the real code | `accepted; the response body could not be read` / `accepted; the response body is not JSON or YAML` |
  | any other status | `failure` | the real code | `upstream answered <code> <status text>` — never the upstream's body |
  | nothing (connection refused, reset, timeout) | `failure` | absent | `no response from the endpoint` |

### Writes nobody requested

The refresher, the cohort seed and the prewarm drivers re-resolve RESTActions
with no person behind them. Since #903 they never send a write stage, whatever
credential the stage uses: the stage is refused before the dial, so there is
nothing to record. Before #903 such a write (an `endpointRef` stage uses the
endpoint's own credential, so it was not stopped by the ServiceAccount's
read-only grant) was sent and emitted no record, for two reasons that still
hold for any write without a requester:

- there is no actor. The context carries a cohort's identity, so a record would
  attribute to a real user an action that user did not take;
- it is a write that originates from a reconcile, not a request. The
  controller-write boundary (installer#413, decided 2026-10-24) records such a
  write only when it targets `*.krateo.io`, changes `.spec` and differs from the
  previous reconcile. Snowplow holds no previous state of a stage's target, so
  it cannot establish the last two; the predicate is not met.

`TestAudit410_C3_BackgroundDriverWrite_EmitsNoRecord` pins it: per background
driver, 0 sends and 0 records; a customer control sends once and records once.

What counts as a write is one classifier for the trail and for #903, #904 and
#905 (`isSafeMethod`): anything but `GET`, `HEAD`, `OPTIONS`, `TRACE`, and an
empty verb is `GET` (it is sent as one).

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

### Effect-free POSTs dominate the `restaction` rows

The verb decides what is recorded, so RESTAction stages that `POST` only to
evaluate something are in the trail, once per customer view. In the
portal-platformops chart (October 2026), each view of these RESTActions emits:

| RESTAction | `restaction` records per view | what the POSTs do |
|---|---|---|
| `access-preview` | 10 | one SubjectAccessReview per capability row (nothing persisted) |
| `blueprint-palette` | 2 | two SelfSubjectAccessReviews |
| `blueprint-render`, `blueprint-render-draft`, `controller-render-draft` | 1 each | a helm-render-service `/render` |
| `upgrade-impact` | 1 | a helm-render-service `/diff` |

So after this ships, a rise in `restaction` rows is mostly page views of those
RESTActions, not changes to anything. To read real mutations, group by
`krateo.restaction.name` and `krateo.restaction.stage` and set these apart,
rather than reading the total.
