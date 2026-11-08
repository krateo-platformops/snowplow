---
type: Architecture
title: Audit correlation (session.id baggage + AuditEvent)
description: How snowplow correlates and audits actions — the W3C baggage session.id business-correlation id and the trace-correlated OTLP AuditEvent LogRecord emitted for /call writes and /export downloads.
resource: oci://ghcr.io/krateo-platformops/charts/snowplow
tags: [portal, audit, otel, observability]
timestamp: 2026-08-06T00:00:00Z
---

# Audit correlation (`session.id` baggage + `AuditEvent`)

Snowplow implements a generic end-to-end audit correlation mechanism
(`internal/support/audit`). There is **no bespoke correlation header**: the
cross-request business/session correlation id rides **W3C baggage** (the
`session.id` member), and the audit record itself is a **trace-correlated OTLP
LogRecord** on the shared OTel Collector → ClickHouse `otel_logs` plane — not a
stdout JSON line.

## Session correlation id

- The id tags one *logical business action*; unlike the per-request
  `X-Krateo-TraceId` shortid and the OTel `traceparent` (both untouched — the
  coexistence contract), the same session id is reused across all the requests
  that make up that action.
- **Snowplow is the id origin** (`audit.Middleware`, wired in `main.go`): if
  the inbound `baggage` header already carries a `session.id` member (seeded by
  the portal or any API client), it is kept; otherwise snowplow falls back to
  the request trace id, and failing that mints a random 16-hex-char id — so
  the id is always non-empty. The id is written into the request context's
  baggage.
- It **propagates into every downstream external call** an api-step performs:
  the global `propagation.Baggage` propagator (installed by `tracing.Setup`)
  serializes the context baggage into the outbound `baggage` header on the
  otelhttp transport (`internal/resolvers/restactions/api/external_fetch.go`
  documents the contract), so a downstream service/adapter can log its own
  records under the same id and an auditor can link portal action → resolved
  calls → downstream effect.
- Inbound ids are sanitized (`SanitizeID`: max 128 chars, `[A-Za-z0-9._-]`);
  anything else is rejected and replaced, never used raw — a hostile caller
  cannot inject baggage/pipeline control characters.

## `AuditEvent` records

Snowplow emits a normalized **`AuditEvent`** as an OTLP LogRecord
(`audit.Emit` → the process-wide `Emitter`) for:

- every **write** through `/call` (POST/PUT/PATCH/DELETE — reads are
  deliberately not audited: volume, and they are already covered by the
  request log + trace id), and
- every **`/export`** download (`action=export`).

The record is an `event.name="audit"` LogRecord with semconv-style attributes
(landing in ClickHouse `otel_logs.LogAttributes`):

| Attribute | Content |
|---|---|
| `event.name` | `audit` — the discriminator every audit row is filtered on |
| `krateo.action` | `call` \| `export` |
| `http.request.method` | the HTTP verb |
| `k8s.resource.group` / `.version` / `.resource` / `.name`, `k8s.namespace.name` | the object acted on |
| `enduser.id` / `user.name`, `user.roles` | the authenticated username and groups (from the request JWT) |
| `outcome` | `success` \| `failure` \| `denied` (drives severity: Info on success, Error otherwise) |
| `http.response.status_code` | the outcome HTTP code |
| `session.id` | the business correlation id, read from ctx baggage |
| `krateo.request.source_ips` | the request's source IPs, k8s audit `sourceIPs` order (`X-Forwarded-For`, `X-Real-Ip`, TCP peer); only the last entry is established by snowplow — see [audit coverage](../docs/architecture/audit-coverage.md#where-the-request-came-from) |
| `user_agent.original` | the request's `User-Agent`, at most 1024 bytes (cut on a character boundary) plus `...TRUNCATED`, as the apiserver bounds it |
| `krateo.request.uri` | the path and query snowplow received (`req.URL.RequestURI()`, as the apiserver records it) |
| `krateo.audit.no_trace_context` | `true` only in the pathological case of an emit with no active span |

`TraceId`/`SpanId` are stamped onto the LogRecord by the Logs SDK from the
active request span (snowplow wraps every HTTP request in an otelhttp server
span), so an audit row **joins on `otel_logs.idx_trace_id`** to the traces and
logs the action caused — an id-space parallel to `traceparent` would be
un-joinable in the very index the stack is built on. The record body carries an
optional human-readable message; the timestamp is set by the SDK.

## Enablement / shipping / immutability

The audit pipeline follows the same `OTEL_ENABLED` gate and endpoint contract
as tracing (`logging.Setup` in `main.go`): when the gate is off, no
LoggerProvider is built and `audit.Emit` is a **no-op** — the off path is
byte-identical. When on, records ship as OTLP/HTTP to the collector with the
same `service.name=snowplow` resource as the traces.

Snowplow takes no sink dependency beyond that: routing `event.name=audit`
records to a dedicated audit view **and to an immutable/WORM sink** is
log-pipeline configuration in the observability stack, not application code —
any deployment can add it without touching snowplow. The pre-existing
stdout → otel-daemonset per-call diagnostic log is a separate signal and is
untouched.
