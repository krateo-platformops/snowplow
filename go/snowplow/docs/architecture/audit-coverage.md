---
type: Architecture
title: snowplow — audit coverage
description: Which snowplow actions emit an AuditEvent, which do not and why, what a record must contain, and how to count the trail without miscounting it. The checked-in inventory AUDIT-4 (krateo-platformops/installer#410) asks for.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [audit, observability, security, restactions]
timestamp: 2026-11-11T00:00:00Z
---

# Audit coverage

Snowplow writes one **AuditEvent** per action it performs for a person, as an
OTLP LogRecord on the shared logs plane. Since #1291 the record is written by
the shared Krateo writer, `github.com/krateo-platformops/audit` (v0.13.0,
schema v1), declared as the `request` writer on the scope
`github.com/krateo-platformops/snowplow/audit`; `internal/support/audit`
supplies what only snowplow knows (the actor from the validated token, the
request it was made on, the preview flag). This page is
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

A record should answer "who did what to which target", in the terms of the
shared schema (the library's `docs/schema.md`):

| field | attribute | source |
|---|---|---|
| actor | `user.name` (+ `user.roles`), and its v0.3 twin `enduser.id` | the validated JWT on the request context, never reconstructed: the token's actor (`UserInfo.Actor()`), i.e. its outermost `act` subject on a delegated token, else its subject |
| actor class | `krateo.audit.actor_class` | authn's signed `principal` claim of that actor (`UserInfo.ActorPrincipal()`, through `audit.ParseClaim`): `human`, `agent`, `controller` or `service`. A token with no claim (issued before authn 0.39.0) or an unknown value gets none, and the record is marked `actor_class`: **never defaulted** (Diego's ruling, #1291) |
| delegation | `krateo.audit.on_behalf_of.user.name`, `krateo.audit.delegation` | only on a delegated (RFC 8693) token: the person the actor acts for (the token's `sub`), and the signed `act` chain, outermost actor first and ending with the subject, each `<class>:<subject>` (`:<subject>` when the token gives no class). One record names one principal |
| action | `krateo.action` | the emit site: `call`, `call.dryRun`, `export`, `restaction`, `restaction.dryRun` (snowplow's own literals; readers use `krateo.audit.dry_run`) |
| verb | `krateo.audit.verb` | the verb of what snowplow dialled, as its authority derives it: for a Kubernetes target the apiserver's own `RequestInfoFactory` on the method and URI that were dialled (`create`, `update`, `patch`, `delete`, `deletecollection`; for an export, the verb of the read its `/call` lane dialled, which is `get`: that lane always names its object), else the lowercase method (`post`, `put`, ...) |
| method | `http.request.method` | the method actually sent |
| target | exactly one: `krateo.audit.object_ref.*` + `k8s.namespace.name` (and the v0.3 twins `k8s.resource.*`), or `server.address` | the Kubernetes object acted on, or the host a RESTAction stage wrote to |

**A record is never dropped** (the shared writer's rule, #1291). One that lacks
a key snowplow's writer class must write is emitted without it, marked
`krateo.audit.incomplete=<markers>` (in the schema's order: `actor`,
`actor_class`, `action`, `verb`, `method`, `preview`, `target`), and each marker
is counted in `krateo.audit.records.incomplete{marker}`: on OTLP, the shared
writer's own counter on the process MeterProvider (when metrics are on), and on
`/debug/vars` in its mirror, `snowplow_audit_incomplete_total{<marker>}`, one
cell per marker of the schema, counted from the record itself. Until #1291 a
record with no actor was refused (counted in that cell `actor`); it is now
emitted, marked `actor`. A gap is in the trail, never hidden.

Every record also carries `krateo.audit.schema_version` (`1`),
`krateo.audit.outcome` and its v0.3 twin `outcome` (`success`, `failure`, or
`denied` when snowplow itself refused), `http.response.status_code` (when the
authority answered; never on a denied record), `krateo.audit.dry_run` on every
write (`true` for a dry run), `krateo.audit.preview` for a Kubernetes target,
`session.id` (W3C baggage), the request's TraceId/SpanId, and what snowplow
received with the request (see "Where the request came from" below). Since
#1291, `TestAudit1291_Handlers_*` (`internal/handlers`) and
`TestAudit1291_Stage_*` (`internal/resolvers/restactions/api`) drive every
record shape through the real emit sites and check it with the library's
`Check` (`audittest.AssertConformant`, `AssertRequiredKeysDetected`,
`AssertEnumsDetected`) for the request writer; an agent's `/call` record
conforms as the action writer too (`TestAudit1291_C_*`).

### The preview flag

`krateo.audit.preview` is `true` when the target is a namespaced object whose
Namespace carries the label `krateo.io/purpose=preview-sandbox`, read from
apiserver state, and `false` for any other Kubernetes target, a cluster-scoped
one included. It is never read from the object itself (whoever creates an
object writes its labels). Snowplow keeps a metadata-only informer on
namespaces over its own ServiceAccount (its grant reads every resource) and
reads a Namespace the informer does not hold with one GET. So until the
informer has synced, and for a write into a Namespace that does not exist, the
preview read is a synchronous GET on the request's context, made before the
record is emitted (a NotFound answer is `false`). When the Namespace
cannot be read, the flag is omitted and the record marked `preview`. A record
that names only an external endpoint carries none. `TestAudit1291_P_*` and
`TestAudit1291_R_*` pin it.

### The recorded span marker

The Server span of a request that wrote an audit record carries
`krateo.audit.recorded=true` (`audit.RecordedMiddleware`, inside the span in
`newServerHandler`), so the tail sampler keeps that trace whole; a RESTAction
stage's record, emitted on its own span's context, marks the Server span too.
It is a boolean: no identity ever goes on a span (#848).
`TestS1291_RecordedMarkerOnlyOnTheServerSpanOfARequestThatWroteARecord`
(`main` package) pins it.

### Where the request came from

Decided 2026-11-07 (#1251, AUDIT-3, installer#409). Every record carries three
facts of the request it was made on, with the Kubernetes apiserver's audit
semantics (`sourceIPs`, `userAgent`, `requestURI`):

| attribute | k8s audit field | value |
|---|---|---|
| `krateo.request.source_ips` | `sourceIPs` | a string list, built by `k8s.io/apimachinery` `utilnet.SourceIPs`, the function the apiserver's audit calls. In order: each entry of the first `X-Forwarded-For` header line, trimmed, and dropped when it is not an IP (repeated entries are **kept**); then `X-Real-Ip`, when it is an IP not already in the list; then the address of the TCP peer, unless it equals the last entry so far |
| `user_agent.original` | `userAgent` | the `User-Agent` header, bounded as the apiserver bounds it (`k8s.io/apiserver` `pkg/audit/request.go`, `maybeTruncateUserAgent`): a value longer than 1024 bytes keeps at most 1024 bytes and gets `...TRUNCATED`. The cut steps back to the start of a character rather than split one (the apiserver cuts at the byte and its JSON encoder repairs the split; OTLP does not repair, see "Every string is valid UTF-8") |
| `krateo.request.uri` | `requestURI` | `req.URL.RequestURI()`, as the apiserver records it: the path and query snowplow received, never a scheme or host, even for an absolute-form request target (e.g. `/call/dry-run?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x`). A proxy that rewrites the path changes it first: on krateo-057 the gateway's HTTPRoute rewrites the `/content` prefix to `/`, so a browser's `/content/call/…` is recorded as `/call/…`. The apiserver does not bound it and neither does snowplow: Go's HTTP server bounds the request line together with the headers (`http.Server.MaxHeaderBytes`, unset in `main.go`, so `http.DefaultMaxHeaderBytes`, 1 MiB) and answers 431 beyond it |

**Which source IP to believe.** Only the **last** entry of
`krateo.request.source_ips` is established by snowplow: it is the address of
the connection the request arrived on. On krateo-057 that is the agentgateway
hop, not the person's browser. Every earlier entry was written into a header by
a client or a proxy, and snowplow cannot tell a true one from a forged one: a
caller can send any `X-Forwarded-For` it likes, and the gateway appends to it.
There is no trusted-proxy list and no special case for the gateway. Read the
entries from the end: the last is a fact, the one before it is what the hop you
trust (if you trust it) says it received from, and so on. If the peer's address
equals the last forwarded entry it is not repeated, so the last entry is the
peer's either way. Earlier entries can repeat, the peer's address included: a
chain that claims the peer first records it twice, first and last.

The three facts are captured once, by `audit.Middleware` at the edge of the
request chain, before any handler runs: the raw header values, the peer address
and a copy of the URL. Nothing is parsed there, so a request that never emits a
record (most are reads) pays only the capture. The emitter derives the
attributes from that capture on every record made for the request: a `/call`
write, an export, and each RESTAction stage write the request caused. An export
never sends a write stage (#903), so a stage record carries the URI of the
`/call` snowplow received, never the stage's own outbound path (see "What a
record never holds"). No emit site supplies them, so none can take them
from a field of a request body. `TestRequest1251_*` (`internal/support/audit`)
and `TestCall1251_*` / `TestExport1251_*` (`internal/handlers`) pin this.

**Every string is valid UTF-8.** The emitter replaces every invalid UTF-8
sequence with U+FFFD in every string an attribute or the record body holds, at
any depth (a string list, a nested list, a map's keys and values), in one place
(the shared writer's `audit.Sanitize`, applied by its `Emit`), for every emit
site. The rule is total over the value kinds of the otel attribute API: a byte
list is left as it is (OTLP carries it as protobuf `bytes`, which need not be
UTF-8), and a kind an otel upgrade adds fails `TestUTF8_1251_K` until it gets a
sample. The reason: the
OTLP exporter marshals with protobuf, which refuses a string that is not valid
UTF-8, and the batch processor then drops the **whole batch**. So one request
with a raw `0xFF` in its User-Agent or query, or a `name=cm-%FF` that decodes to
that byte, would otherwise lose every other person's record in the same batch,
and nothing in the trail would show the gap. The apiserver's audit is JSON, and
its encoder makes the same replacement. `TestUTF8_1251_*`
(`internal/support/audit`) pins it through the real `otlploghttp` exporter and
batch processor. The trace plane has the same exposure, tracked in #1269.

**Identity.** The request URI can carry identity in its query (a name, a
filter, an owner). The audit trail stores identity on purpose; snowplow's log
lines never do (#1192). The facts live only on the request context and are
written only to the audit log pipeline, never to a log line:
`TestS1251_RequestFactsReachTheAuditRecordAndNoLogLine` (`main` package) sends
a sentinel identity in the query, the User-Agent and `X-Forwarded-For` through
the pod's real chain and log handler, and wants it in the record and in no
line.

A successful write to the Kubernetes apiserver also carries the **written
object's identity**, as the apiserver returned it (see "Written object
identity" below). These attributes are optional: they are never listed as
missing, and a record without them is not incomplete.

### Every attribute

Each attribute is written only when it has a value. The names are the shared
schema's (`github.com/krateo-platformops/audit`, schema v1), so a join with
another component's records uses the same keys. The v0.3 keys snowplow wrote
before #1291 are all still written, with the same values (the migration window
W1: each beside its replacement, until the library closes W1).
`TestWrittenObject957_D_AuditCoverageDocNamesEveryAttribute`
(`internal/support/audit`) and the #1291 parity arms of the emit sites fail
when a record carries an attribute this table does not name.

| attribute | value |
|---|---|
| `event.name` | always `audit` |
| `krateo.audit.schema_version` | always `1` |
| `user.name`, `enduser.id` | the actor (see "What every record contains"); `enduser.id` is its v0.3 twin, always equal |
| `user.roles` | the token's groups |
| `krateo.audit.actor_class` | the actor's signed class: `human`, `agent`, `controller`, `service`; absent (and marked) when the token does not say |
| `krateo.audit.on_behalf_of.user.name`, `krateo.audit.delegation` | a delegated token's subject and signed `act` chain |
| `krateo.action` | `call`, `export`, `restaction`; a dry-run write carries the suffix `.dryRun` (`call.dryRun`, `restaction.dryRun`) |
| `krateo.audit.verb` | the authority's verb for what was dialled |
| `http.request.method` | the method of the authority request snowplow SENT, the method of its verb, never the method of the request snowplow served (schema v1; an export sends GET, whatever it was served with); `_OTHER` for one semconv does not name. The emitter checks every record: one whose method is not its verb's is counted (`snowplow_audit_dial_method_mismatch_total`, zero is the only correct value) and, in a test binary, fails the arm that emitted it (`SetDialMethodGuardForTest`, installed in every package that records), so the falsifier runs in every arm that emits a record |
| `http.request.method_original` | the method as sent, only when `http.request.method` is `_OTHER` |
| `krateo.audit.dry_run` | on every write: `true` for a dry run (`/call/dry-run`, a stage path with `?dryRun=All`), else `false`; never on a read (an export) |
| `krateo.audit.outcome`, `outcome` | `success` / `failure` / `denied` (snowplow itself refused; no status code) |
| `http.response.status_code` | the authority's code, when it answered |
| `krateo.audit.preview` | for a Kubernetes target: whether its Namespace is the preview sandbox (see "The preview flag") |
| `krateo.audit.object_ref.api_group`, `krateo.audit.object_ref.api_version`, `krateo.audit.object_ref.resource`, `krateo.audit.object_ref.name`, `k8s.namespace.name`, and their v0.3 twins `k8s.resource.group`, `k8s.resource.version`, `k8s.resource.resource`, `k8s.resource.name` | the Kubernetes object's coordinates |
| `krateo.audit.object_ref.subresource`, `k8s.resource.subresource` | the addressed subresource (`status`, `scale`), when there is one |
| `krateo.audit.object_ref.uid`, `k8s.object.uid` | **optional**: the written object's `metadata.uid`, from the apiserver's response; on a DELETE, the deleted object's uid |
| `krateo.audit.object_ref.generation`, `k8s.object.generation` | **optional**, an integer: the written object's `metadata.generation`, from the response; never on a DELETE |
| `krateo.audit.object_ref.resource_version`, `k8s.object.resource_version` | **optional**: the written object's `metadata.resourceVersion`, from the response; never on a DELETE |
| `server.address` | host and port a RESTAction stage was sent to, when the record names no Kubernetes object |
| `krateo.restaction.namespace`, `krateo.restaction.name`, `krateo.restaction.stage` | the RESTAction and stage that acted |
| `session.id` | the session id, from W3C baggage |
| `krateo.request.source_ips` | a string list: the request's source IPs; only the last entry (the TCP peer) is established by snowplow (see "Where the request came from") |
| `user_agent.original` | the request's `User-Agent` header, at most 1024 bytes (cut on a character boundary) plus `...TRUNCATED`, as the apiserver bounds it |
| `krateo.request.uri` | the path and query snowplow received (`req.URL.RequestURI()`, as the apiserver records it) |
| `krateo.audit.incomplete` | the markers of the keys the record lacks, e.g. `actor_class,preview` |
| `krateo.audit.no_trace_context` | `true` when the record was emitted with no span (so it has no TraceId) |
| `krateo.audit.client.source`, `krateo.audit.client.agent_session_id`, `krateo.audit.client.confirmation_id`, `krateo.audit.client.confirmed_set.count`, `krateo.audit.client.confirmed_set.irreversible`, `krateo.audit.client.confirmed_set.sha256`, `krateo.audit.client.confirmed_set.op`, `krateo.audit.client.requested_at` | **asserted**, on a `/call` write or dry run only: what the portal's `X-Krateo-Provenance` header said, after snowplow validated it against its closed schema (#1333, see "Client provenance" below); never identity, the actor's class, the verb, the target, the outcome or the event time |
| `krateo.audit.client.rejected` | **asserted**, on a `/call` write or dry run only: why snowplow rejected the header (`oversize`, `encoding`, `version`, `schema`); the record then carries no other `krateo.audit.client.*` key, and the write ran |

Not written by snowplow yet: `krateo.audit.object_ref.content_sha256`. No snowplow refusal has
a `krateo.audit.denial_reason`: the request writer's closed set holds authn's
reasons only, and a denied record without one conforms.

### Client provenance

A `/call` write or dry run reads the request header `X-Krateo-Provenance`
(#1333, `internal/support/audit/provenance.go`) and records what it asserts
under `krateo.audit.client.*`, through the shared library's
`Event.Client`. The header is the client's word: it names no identity, actor
class, agent, verb, target, namespace, outcome, status or time snowplow
records as its own, and a header that tries is rejected whole (`schema`). A
rejected header never blocks or changes the write: the record carries
`krateo.audit.client.rejected` and nothing else from it. No header, no client
key. Reads, exports and RESTAction stage records never carry these keys (the
library allows them on writes only). The header value is never logged, never
put on a span and never forwarded to the apiserver. The format, limits and
rejection rules are the frontend's contract:
[`howto-frontend-call-provenance.md`](../howto-frontend-call-provenance.md).
`TestCall1333_*` (`internal/handlers`) and `TestProvenance1333_*`,
`FuzzDecodeProvenance` (`internal/support/audit`).

**What a record never holds.** The trail is kept 365 days, so nothing that can
carry a credential goes into it: no path or query of a request snowplow **sends**
(a webhook path such as Slack's `/services/T/B/<secret>`, Discord's
`/api/webhooks/<id>/<token>` or Telegram's `/bot<token>/…`, or a path built by
jq from an earlier stage's token, is a secret in its own right), and no upstream
response body (an error body can echo a token). The URI of the request snowplow
**received** (`krateo.request.uri`) is recorded: snowplow reads no credential
from a query (its token travels in the `Authorization` header), and on
krateo-057 no access token was found in any URL field over 6 h or over all time
(installer, measured 2026-11-07, with controls). A failed `/call` record's body and a RESTAction stage record's body are the same reason class from a closed
set (below).

## One record per audited request

Which routes are audited, and as what, is declared once, where the route is
registered (`routeTable` in `debug_routes.go`): `HandleAudited` mounts a route
with its audit descriptor (`audit.Route`: what a request asked for, by the
handler's own rules: `handlers.CallRoute`, `CallDryRunRoute`, `ExportRoute`),
`HandleUnaudited` mounts one that is not, with the reason, and `Handle` panics
at boot for a pattern whose method can change something (POST, PUT, PATCH,
DELETE, or none), so a new route is never silently rowless
(`TestAudit_RouteTable_*`). An audited route is wrapped OUTERMOST in
`audit.OpenRecord`, before any middleware decodes a body or verifies a token,
so every exit emits exactly one record (#1379 review F2):

- the handler emits it (`audit.EmitRoute`) with what the authority answered;
- the login middleware (UserConfig) names the verified caller as soon as the
  token validates (`audit.SetCaller`), before its credential lookup, so its
  own exits are the route's record too: a verified caller whose own
  credential Secret is missing (its 401) is `denied`,
  `session_credential_missing` or, for a `krateo:loa:2` token,
  `step_up_required`, no status code (`audit.DenyRoute`); a lookup that fails
  for any other reason (its 500, or the page budget's 504) is a `failure`
  with NO status code and the body "the caller's credential could not be
  read; nothing was sent" (`audit.FailRoute`): snowplow's own code, and its
  ServiceAccount's read of the credential Secret, are never an answer of the
  authority the record names (never a code snowplow made up, #1298). A
  legitimate server error, not a design error;
- a 4xx exit that emitted none, a refusal by snowplow before anything was
  sent (a body the decoder refuses, a malformed query, a `/call` dial that
  would act on another object), is recorded when the request ends: what it
  asked for, `denied`, `krateo.audit.denial_reason` `invalid_request`, no
  status code. What it asked for is the descriptor the handler stored once
  it parsed the request (`audit.SetAsked`), else the route's descriptor of
  the method, path and query alone (no body is read twice); the target when
  the request names one the handler would dial, else none (marked `target`);
- any other exit that emitted none (a 2xx, 3xx or 5xx answer, or a panic) is
  a design error of the handler, never a description of what was sent: it is
  recorded as a `failure` (the outcome is required) with its own body, "the
  handler answered without recording its outcome" or "panic: …", and
  counted in `snowplow_audit_unrecorded_exit_total{answered,panic}`, whose
  only correct value is zero; a panic is re-raised after its record;
- a request that never got a verified caller (an authentication failure) gets
  none: those are counted, never recorded (see "Authentication failures").

`TestAudit_E2E_*` (`main` package) drives each audited route class through
the real route table and the real login middleware (one row per refusal and
per success, zero for an unverified request, one saying panic for a panic,
and the design-error counter unmoved); `TestAudit_OpenRecord_*` and
`TestOpenRecord_*` pin the parts.

## Inventory

| path | what changes | status |
|---|---|---|
| `POST/PUT/PATCH/DELETE /call` | an object, on the caller's own credential | **audited** — `call`, one record per request, including `failure` with the apiserver's code when it refuses the write; the failure record's outcome, code and body are what was observed at the dial, from the same closed set as a stage record (the outcome table below), never the apiserver's message or body, and never a code snowplow made up (#1298); the portal's `X-Krateo-Provenance` header adds `krateo.audit.client.*` (#1333). The record names the object the apiserver resolves from the dialled path, by construction (#1257): `buildURIPath` sends each query value as exactly one escaped path segment (a `?` or `#` in a name stays in the name), and refuses with a 400, before any dial, a value the apiserver does not accept as one segment (`.`, `..`, or one holding `/` or `%`) in the API version, resource, namespace, name or subresource. Before, `name=cm-x/status` dialled the status subresource and `name=../secrets/s1` another resource, while the record named the query's values. Then the dialled URI is classified with the apiserver's own `RequestInfoFactory` (`audit.KubernetesObject`, `dialledTarget`): a write whose classification differs from what the request named (a group, version, resource or subresource, a namespace it gave, a name the path carries) is refused with a 400 before the dial (`resource=namespaces&namespace=foo&name=bar` would delete namespace `bar`; `resource=finalize` under a namespace is that namespace's subresource), and otherwise the record's target is that classification, so it describes the dial by construction (#1379 review F3). A create (POST) is sent to the collection, so the query's `name` never reaches the apiserver, which names the object from the body or generates it (`k8s.io/apiserver` `createHandler`, `BeforeCreate`): its record names the object the apiserver answered with (`metadata.name`, a generated name included), and on a failure (a 403 included) the body's `metadata.name`, what was asked, when it is one path segment, else no name (#1378). The body's keys are matched exactly, as apimachinery decodes them: `{"metadata":{"Name":"decoy"}}` names nothing. `krateo.request.uri` keeps the query, `name` included, as received. A refusal before the dial (a malformed query, a dial that would act on another object, a `dryRun` on the plain route) is the route's one record too (see "One record per audited request") |
| `POST/PUT/PATCH /call/dry-run` | nothing persisted; the apiserver admits or refuses the write | **audited** — `call.dryRun`, `krateo.audit.dry_run=true`; `krateo.audit.client.*` as on `/call` (#1333); the dialled path is built, classified and refused as on `/call` (#1257, #1379 review F3) |
| a RESTAction `api[]` stage with a write verb, reached by `GET /call`, `POST /call/read` or `GET /export` | whatever the stage's target does with the request: the caller's own apiserver (no `endpointRef`) or a named endpoint | **audited** — `restaction` (`restaction.dryRun` when the stage's apiserver path asks for a dry run), one record per dialled call (an iterator stage of N calls is N records). A stage dialled through a named endpointRef also names which one: `krateo.audit.endpoint_ref.namespace` and `krateo.audit.endpoint_ref.name`, the reference as resolved (a templated name rendered), beside `server.address`; names only, never the Secret's data, and never a per-user credential Secret's name (#1283). A destination the policy refuses is `denied`, `destination_refused`. See below |
| `GET /export`, `POST /export` | nothing; a file is produced from the caller's own `/call` | **audited** — `export`, verb `get` (the `/call` lane it dials always names its object), `http.request.method` `GET` for a GET and a POST export alike: the method of the read it sends (#1379 review F1); `krateo.request.uri` says what it received. Its target is the apiserver's classification of that read's URI. Exactly one record per export, on every exit (#986; the handler's record is emitted once, deferred, by `exportAudit`, and an exit before the handler, a body `BodyExtrasDecode` refuses included, is the route's record, see "One record per audited request"): `success` with the code it answered; `failure` with the code it answered for a page the `/call` lane failed (relayed), a row path that does not evaluate, a pdf over the row bound, or a render or send failure; `denied`, with no status code, for the export's own refusals before any page is dispatched (an unsupported `format`, `header` labels that do not match `fields`), whose body is the rule's fixed phrase, never the caller's value. A target the `/call` lane cannot dial (#1257) names no object, since none was read: the record carries no Kubernetes target and is marked `target` |
| a RESTAction write stage in a background resolve (refresher, cohort seed, prewarm, content prewarm, pagination drain, a #547 demand revalidation, or a `/call` snowplow makes to itself for one of them) | nothing: the stage is refused before any request leaves (#903) | **out of scope** — nothing was sent; see "Writes nobody requested" |
| a RESTAction write stage snowplow refuses, on a request somebody asked for (audit#22 (a)): the endpointRef names a per-user credential Secret, the endpoint Secret is malformed, a caller-supplied draft on an internal identity, an export's write, a dry run's write (#443), the stage it depends on failed (#904), `userAccessFilter` on a write verb, an invalid or unrendered path | nothing: no request leaves | **audited** — `restaction`, `denied`, no status code, `krateo.audit.denial_reason` one of `per_user_credential_ref`, `endpoint_ref_malformed`, `internal_identity_context`, `write_refused`, `dry_run_not_executed`, `dependency_failed`, `uaf_on_write_verb`, `invalid_path`, `template_error`. The target is what was resolved (the object of a rendered apiserver path, the endpoint and its endpointRef); an unresolved one is marked `target`. A `per_user_credential_ref` row names no reference: no attribute and no body of it holds `-clientconfig` |
| a RESTAction write stage whose endpoint Secret read failed (NotFound, Forbidden, nothing answered) | nothing: no request leaves | **audited** — `restaction`, `failure`, the code the apiserver answered that read with (none when nothing answered), target marked |
| a RESTAction write stage the #905 stale-pass hold withdrew | nothing: its fresh re-run sends the write instead | **out of scope** — the re-run's dial is the one recorded |
| a refused READ stage, for any cause | nothing | **out of scope** — reads are not audited, sent or refused |
| authentication failures (bad, expired or missing token) on any route | nothing: answered 401 by the UserConfig middleware before any handler | **out of scope** — see "Authentication failures" |
| a VERIFIED caller UserConfig refuses because their own credential Secret is missing (401 `SessionCredentialMissing` / `StepUpRequired`), on an audited route | nothing: answered before any handler | **audited** — the route's one record (its descriptor names what was asked: action, verb, target), `denied`, `session_credential_missing` or `step_up_required`, no status code; never the credential Secret (its name embeds the username) |
| reads: `GET /call`, `POST /call/read`, `GET /list`, `GET /rbac`, `GET /api-info/names` | nothing | **out of scope** for AUDIT-4, which covers mutations. Note that the request log and trace id these were once delegated to do not cover them today (installer#410: the gateway access log has no outcome, resource or session id, and the trace is sampled) |
| `GET/POST /refreshes` | nothing: the live-refresh SSE subscription | **out of scope** — cache machinery, no user action |
| `POST /jq` | nothing: evaluates a jq program over the request body; no I/O | **out of scope** |
| `POST /telemetry/v1/traces` | nothing on the platform: browser spans forwarded to the trace plane (#1015) | **out of scope** — no platform state changes |
| `POST /convert` | not mounted (commented out in `main.go`) | **out of scope** — unreachable |
| `POST /debug/shadow-parity` | toggles a dark parity measurement; cannot change an authorization verdict | **audited** — `debug.shadowParity`, verb `post`, method `POST`, target `url.path` `/debug/shadow-parity` (the request writer's non-resource target), `dry_run=false`; `success` with the committed state in the body, `denied` `invalid_request` for an unparseable `enabled` (#909); a verified caller snowplow's own RBAC evaluation refuses (DebugAuthz's 403, after RefreshAuth named the caller) is `denied`, `rbac_not_allowed` (audit v0.13.0), no status code; an evaluation that is unavailable (its 503) is a `failure` with no status code. GET `/debug/*` are reads: refused or not, no row |
| snowplow's own writes to the cluster | none: its ClusterRole grants `get/list/watch`, `create` on (Self)SubjectAccessReviews (non-persistent queries) and `impersonate` | **out of scope** — there are no controller writes to record |

### RESTAction stage records

A RESTAction stage may carry any HTTP verb. A stage whose verb is not a safe
method (RFC 9110 §9.2.1: `GET`, `HEAD`, `OPTIONS`, `TRACE`) is a write for the
trail. The verb decides, never the path or the endpoint: a `POST` that only
evaluates something (an access review, a template render) is recorded like any
other `POST`. Each record carries:

- `krateo.restaction.namespace`, `krateo.restaction.name`,
  `krateo.restaction.stage`: the RESTAction and the stage that acted;
- exactly one target (schema v1). For a stage with no `endpointRef` (the
  caller's own clientconfig, i.e. the apiserver) whose path names an object:
  `krateo.audit.object_ref.{api_group,api_version,resource,subresource,name}`
  (and `k8s.resource.*`) and `k8s.namespace.name`, the object's coordinates as
  a `/call` record holds them, with the apiserver's verb for that path. Until
  #1291 such a record also carried `server.address`, the host of the caller's
  own clientconfig (the apiserver's): two targets, which the schema forbids, so
  it is no longer written there. Any other stage (a named endpoint may be any
  HTTP API) names `server.address`, host and port only — never userinfo, a path
  or a query — no Kubernetes resource, nothing of its path, and the lowercase
  method as its verb;
- for the same stages, on success: `k8s.object.uid`, `k8s.object.generation`
  and `k8s.object.resource_version` of the object the apiserver answered with
  (see "Written object identity"). A stage to a named endpoint records none of
  them;
- `krateo.action=restaction.dryRun` instead of `restaction` when a stage with
  no `endpointRef` asks the apiserver for a dry run in its path
  (`…/demo?dryRun=All`, e.g. a validation stage). The rule is the apiserver's
  own: the query key `dryRun` (case-sensitive) is present with any value; any
  value but `All` is refused with a 400, so such a write is a `failure`. A dry-run
  stage names no written object. A named endpoint's `dryRun` query means
  nothing to the trail and is not marked;
- `outcome` and `http.response.status_code`: what the authority answered,
  observed at the dial — not the resolver's reshaped result, and not what the
  stage's own filter makes of it:

  | the authority answered | outcome | code | record body |
  |---|---|---|---|
  | a 2xx | `success` | the real code (`201`, `204` are not reported as `200`) | empty |
  | a 2xx whose body snowplow cannot read or convert to JSON/YAML | `success` — the write was accepted | the real code | `accepted; the response body could not be read` / `accepted; the response body is not JSON or YAML` |
  | a 3xx (snowplow follows no redirect on a credentialed dial, #729/#1106) | `failure` | the real code | `snowplow refused to follow a <code> <status text> redirect` |
  | any other status | `failure` | the real code | `upstream answered <code> <status text>` — never the upstream's body |
  | nothing (connection refused, reset, timeout) | `failure` | absent | `no response from the endpoint` |
  | nothing: snowplow's destination policy refused the address (#1131) | `denied` (since #1291: snowplow itself refused, the schema's denied; it was `failure`) | absent (a denied record never carries one) | `not sent: snowplow's destination policy refused the address` |
  | nothing: the request's page budget ran out (#1115) | `failure` | absent | `not sent: snowplow's time budget for the request ran out first` (before a connection), `snowplow stopped waiting: the request's time budget ran out before an answer`, or `…ran out after the write was sent; it may have been applied` |

  The same table, built from the same observation (`credhttp.Answer`, recorded
  by `credhttp.Send`, the one place an endpoint request leaves), is the outcome
  of a failed `/call` write (#1298). The code the caller receives is often
  snowplow's own (500 for no response, 502 for a refused destination or
  redirect, 504 for the page budget, 406 for a `/call` 2xx whose content type is
  not JSON); the record never carries it as the authority's answer. That 2xx is
  recorded as the accepted write it was: `success`, the real code, `accepted;
  snowplow refused the response body unread: its content type is not JSON`. A
  successful `/call` record is unchanged (`200`, empty body).

### Written object identity

Decided 2026-10-28 (#957, krateo-acmp#59 Option B). A user's write through
snowplow changes a custom resource; a controller later reconciles it and makes
the real external change (a VM, a repository, a bucket). To join the two audit
records, both name the object by its identity: snowplow records the uid,
generation and resourceVersion the apiserver returned for the write, and the
controller records the uid and the generation it acted on
(oasgen-provider#253). Join `krateo_audit` on
`(krateo.audit.object_ref.uid, krateo.audit.object_ref.generation)` (a
controller row carries them as `krateo.audit.cause_ref.*` since audit v0.5.0;
during W1 the v0.3 twins `k8s.object.uid`, `k8s.object.generation` hold the same
values), adding the resourceVersion where both sides have it. Snowplow never
annotates or otherwise changes the user's object for this: an annotation would
name "the last write through snowplow", not the cause of a reconcile, and would
trigger server-side-apply, GitOps and webhook side effects.

The rules, the same for `/call` and for a RESTAction stage
(`audit.RecordWrittenObject`); each key is written with its v0.3 twin:

| the write | `k8s.object.uid` | `k8s.object.generation` | `k8s.object.resource_version` |
|---|---|---|---|
| a successful `POST`, `PUT` or `PATCH` (a subresource included) | the response's `metadata.uid` | the response's `metadata.generation` | the response's `metadata.resourceVersion` |
| a successful `DELETE` | the returned object's `metadata.uid` (deletion pending on finalizers), or the `details.uid` of the `Status` the apiserver answers with once the object is gone | none | none |
| a failed or refused write | none: there is no written object. This is not an error | none | none |
| a dry run: `/call/dry-run` (`call.dryRun`) or a stage path with `?dryRun=All` (`restaction.dryRun`) | none: nothing was persisted. A dry-run update answers with the real uid and the generation the change would reach, which a later real write can reach too, so recording it would join that write's reconcile to this dry run | none | none |
| a RESTAction stage to a named endpoint (`endpointRef`) | none: it may be any HTTP API | none | none |
| `export` (a read) | none | none | none |

The values always come from the apiserver's **response**, never from the
request body: the request carries what the caller sent (often no uid, and the
resourceVersion the write replaces), the response what was stored. A field the
response does not carry is left out. These are object identifiers, not user
data: no identity is added to the record.

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
       LogAttributes['krateo.audit.outcome'] AS outcome,
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
